"""End-to-end orchestration: manifest → Bronze → Silver → DQ → Gold → audit.

This is the module a Databricks job (or a local CLI) calls. It wires the
Spark-free core (client, transform, quality, audit) to the Spark I/O layer and
records a complete, auditable trail for every run.

The orchestrator is defensive per-series: one series failing (bad id, DQ error
under a strict profile, network exhaustion) is recorded and the run continues,
finishing as ``PARTIAL`` rather than losing all progress.
"""

from __future__ import annotations

import logging
from typing import Any, Iterable, Optional

from fred_pipeline.audit import EtlRun, RunStatus
from fred_pipeline.bronze import build_bronze_row, write_bronze
from fred_pipeline.config import Environment, PipelineConfig
from fred_pipeline.fred_client import FredClient
from fred_pipeline.gold import build_gold
from fred_pipeline.manifest import SeriesSpec, all_series, load_manifests
from fred_pipeline.quality import run_quality_checks
from fred_pipeline.silver import build_silver_rows, merge_silver
from fred_pipeline.spark_io import append_rows, get_spark

log = logging.getLogger("fred_pipeline")

# Full-vintage real-time window for point-in-time enabled series.
FULL_VINTAGE_START = "1776-07-04"
FULL_VINTAGE_END = "9999-12-31"


class FredPipeline:
    def __init__(
        self,
        config: PipelineConfig,
        *,
        client: Optional[FredClient] = None,
        spark: Any = None,
        persist_audit: bool = True,
    ):
        self.config = config
        self.spark = spark
        self.persist_audit = persist_audit
        self._client = client

    @property
    def client(self) -> FredClient:
        if self._client is None:
            self._client = FredClient(
                api_key=self.config.fred_api_key,
                base_url=self.config.fred_base_url,
                timeout=self.config.request_timeout_seconds,
                max_retries=self.config.max_retries,
                rate_limit_per_minute=self.config.rate_limit_per_minute,
            )
        return self._client

    # ---- run entrypoints ------------------------------------------------

    def run_from_manifest(
        self,
        manifest_path: str,
        *,
        triggered_by: str = "",
        build_gold_layer: bool = True,
    ) -> EtlRun:
        manifests = load_manifests(manifest_path)
        specs = all_series(manifests, active_only=True)
        log.info("Loaded %d active series from %s", len(specs), manifest_path)
        if self.spark is not None:
            try:
                from fred_pipeline.meta import sync_meta

                sync_meta(self.config, manifests, spark=self.spark)
                log.info("Synced %d series to meta layer", len(specs))
            except Exception:  # pragma: no cover - Spark-only path
                log.exception("Meta sync failed (continuing)")
        return self.run(
            specs,
            manifest_path=manifest_path,
            triggered_by=triggered_by,
            build_gold_layer=build_gold_layer,
        )

    def run(
        self,
        specs: Iterable[SeriesSpec],
        *,
        manifest_path: str = "",
        triggered_by: str = "",
        build_gold_layer: bool = True,
    ) -> EtlRun:
        specs = list(specs)
        run = EtlRun(
            environment=self.config.environment.value,
            manifest_path=manifest_path,
            triggered_by=triggered_by,
        )
        log.info("Starting run %s (%d series)", run.run_id, len(specs))

        for spec in specs:
            self._process_series(run, spec)

        run.finalize()

        if build_gold_layer and run.series_succeeded > 0 and self.spark is not None:
            try:
                build_gold(self.config, spark=self.spark)
                log.info("Gold layer refreshed for run %s", run.run_id)
            except Exception:  # pragma: no cover - Spark-only path
                log.exception("Gold refresh failed for run %s", run.run_id)

        self._persist_run(run)
        log.info(
            "Run %s finished: %s (%d ok / %d failed)",
            run.run_id, run.status.value, run.series_succeeded, run.series_failed,
        )
        return run

    # ---- per-series -----------------------------------------------------

    def _process_series(self, run: EtlRun, spec: SeriesSpec) -> None:
        sr = run.start_series(spec.series_id, load_type=spec.load_type.value)
        try:
            payload = self._extract(spec)
            observations = payload.get("observations") or []

            bronze_row = build_bronze_row(
                spec.series_id, "series/observations", payload, run_id=run.run_id
            )
            silver_rows = build_silver_rows(
                spec.series_id, payload, run_id=run.run_id
            )
            report = run_quality_checks(
                spec.series_id, silver_rows, profile=spec.validation_profile
            )

            bronze_written = 0
            silver_merged = 0
            if self.spark is not None:
                bronze_written = write_bronze(
                    self.config, [bronze_row], spark=self.spark
                )
                if report.passed:
                    result = merge_silver(
                        self.config, silver_rows, spark=self.spark
                    )
                    silver_merged = result.get("source_rows", 0)
                self._persist_dq(run.run_id, report)

            if report.passed:
                sr.complete(
                    RunStatus.SUCCEEDED,
                    observations_extracted=len(observations),
                    rows_written_bronze=bronze_written,
                    rows_merged_silver=silver_merged,
                    dq_passed=True,
                )
            else:
                msgs = "; ".join(f.message for f in report.failures)
                sr.complete(
                    RunStatus.FAILED,
                    observations_extracted=len(observations),
                    rows_written_bronze=bronze_written,
                    dq_passed=False,
                    error_message=f"Data quality failed: {msgs}",
                )
                log.warning("DQ failed for %s: %s", spec.series_id, msgs)
        except Exception as exc:  # isolate per-series failures
            sr.complete(RunStatus.FAILED, error_message=str(exc))
            log.exception("Series %s failed", spec.series_id)

    def _extract(self, spec: SeriesSpec) -> dict[str, Any]:
        kwargs: dict[str, Any] = {}
        if spec.vintage_enabled:
            kwargs["realtime_start"] = FULL_VINTAGE_START
            kwargs["realtime_end"] = FULL_VINTAGE_END
        return self.client.get_observations(spec.series_id, **kwargs)

    # ---- audit persistence ----------------------------------------------

    def _persist_run(self, run: EtlRun) -> None:
        if not (self.persist_audit and self.spark is not None):
            return
        try:
            append_rows(
                self.spark, [run.to_row()], self.config.table("audit", "etl_run")
            )
            series_rows = [s.to_row() for s in run.series_runs]
            append_rows(
                self.spark, series_rows,
                self.config.table("audit", "etl_series_run"),
            )
        except Exception:  # pragma: no cover - Spark-only path
            log.exception("Failed to persist audit records for run %s", run.run_id)

    def _persist_dq(self, run_id: str, report) -> None:
        if not (self.persist_audit and self.spark is not None):
            return
        rows = [
            {
                "run_id": run_id,
                "series_id": r.series_id,
                "check_name": r.check,
                "passed": r.passed,
                "severity": r.severity.value,
                "message": r.message,
                "metric_value": r.metric_value,
            }
            for r in report.results
        ]
        try:
            append_rows(
                self.spark, rows,
                self.config.table("audit", "data_quality_result"),
            )
        except Exception:  # pragma: no cover
            log.exception("Failed to persist DQ results for run %s", run_id)


def run_pipeline(
    environment: str = "dev",
    manifest_path: str = "manifests",
    *,
    fred_api_key: Optional[str] = None,
    dbutils: Any = None,
    spark: Any = None,
    triggered_by: str = "cli",
) -> EtlRun:
    """Convenience entrypoint used by the Databricks job and the CLI."""
    config = PipelineConfig.resolve(
        environment=Environment(environment),
        fred_api_key=fred_api_key,
        dbutils=dbutils,
    )
    if spark is None:
        try:
            spark = get_spark()
        except Exception:  # pragma: no cover - allow no-Spark dry runs
            spark = None
    pipeline = FredPipeline(config, spark=spark)
    return pipeline.run_from_manifest(manifest_path, triggered_by=triggered_by)
