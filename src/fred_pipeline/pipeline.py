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
from fred_pipeline.bronze import build_bronze_row
from fred_pipeline.config import Environment, PipelineConfig
from fred_pipeline.fred_client import FredClient
from fred_pipeline.manifest import LoadType, SeriesSpec, all_series, load_manifests
from fred_pipeline.quality import run_quality_checks
from fred_pipeline.silver import build_silver_rows
from fred_pipeline.warehouse import SparkWarehouse, Warehouse

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
        warehouse: Optional[Warehouse] = None,
        persist_audit: bool = True,
        notify_transport: Any = None,
    ):
        self.config = config
        self.persist_audit = persist_audit
        self._client = client
        self._notify_transport = notify_transport
        # Resolve the storage backend: explicit warehouse > Spark > None (dry run).
        if warehouse is not None:
            self.warehouse: Optional[Warehouse] = warehouse
        elif spark is not None:
            self.warehouse = SparkWarehouse(config, spark)
        else:
            self.warehouse = None

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
        series: Optional[list[str]] = None,
        force_full: bool = False,
    ) -> EtlRun:
        manifests = load_manifests(manifest_path)
        specs = all_series(manifests, active_only=True)
        if series:
            wanted = set(series)
            specs = [s for s in specs if s.series_id in wanted]
            missing = wanted - {s.series_id for s in specs}
            if missing:
                log.warning("Requested series not found/active: %s", sorted(missing))
        log.info("Loaded %d active series from %s", len(specs), manifest_path)
        if self.warehouse is not None:
            try:
                self.warehouse.sync_meta(manifests)
                log.info("Synced %d series to meta layer", len(specs))
            except Exception:
                log.exception("Meta sync failed (continuing)")
        return self.run(
            specs,
            manifest_path=manifest_path,
            triggered_by=triggered_by,
            build_gold_layer=build_gold_layer,
            force_full=force_full,
        )

    def run(
        self,
        specs: Iterable[SeriesSpec],
        *,
        manifest_path: str = "",
        triggered_by: str = "",
        build_gold_layer: bool = True,
        force_full: bool = False,
    ) -> EtlRun:
        specs = list(specs)
        run = EtlRun(
            environment=self.config.environment.value,
            manifest_path=manifest_path,
            triggered_by=triggered_by,
        )
        log.info("Starting run %s (%d series)", run.run_id, len(specs))

        for spec in specs:
            self._process_series(run, spec, force_full=force_full)

        run.finalize()

        if build_gold_layer and run.series_succeeded > 0 and self.warehouse is not None:
            try:
                self.warehouse.build_gold()
                log.info("Gold layer refreshed for run %s", run.run_id)
            except Exception:
                log.exception("Gold refresh failed for run %s", run.run_id)

        self._persist_run(run)
        self._notify(run)
        log.info(
            "Run %s finished: %s (%d ok / %d failed)",
            run.run_id, run.status.value, run.series_succeeded, run.series_failed,
        )
        return run

    def _notify(self, run: EtlRun) -> None:
        from fred_pipeline import notify

        try:
            notify.send_notification(
                run,
                webhook_url=self.config.alert_webhook_url,
                notify_on=self.config.notify_on,
                environment=self.config.environment.value,
                transport=self._notify_transport,
            )
        except Exception:  # never let notification issues fail a run
            log.exception("Notification step failed for run %s", run.run_id)

    # ---- per-series -----------------------------------------------------

    def _process_series(
        self, run: EtlRun, spec: SeriesSpec, *, force_full: bool = False
    ) -> None:
        sr = run.start_series(spec.series_id, load_type=spec.load_type.value)
        try:
            observation_start, sr.load_type = self._plan_extract(
                spec, force_full=force_full
            )
            payload = self._extract(spec, observation_start=observation_start)
            observations = payload.get("observations") or []

            bronze_row = build_bronze_row(
                spec.series_id, "series/observations", payload, run_id=run.run_id
            )
            silver_rows = build_silver_rows(
                spec.series_id, payload, run_id=run.run_id,
                track_vintage=spec.vintage_enabled,
            )
            report = run_quality_checks(
                spec.series_id, silver_rows, profile=spec.validation_profile,
                frequency=spec.frequency,
                min_value=spec.min_value, max_value=spec.max_value,
            )

            bronze_written = 0
            silver_merged = 0
            if self.warehouse is not None:
                bronze_written = self.warehouse.write_bronze([bronze_row])
                if report.passed:
                    silver_merged = self.warehouse.merge_silver(silver_rows)
                if self.persist_audit:
                    self.warehouse.persist_dq(run.run_id, report)

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

    def _plan_extract(
        self, spec: SeriesSpec, *, force_full: bool = False
    ) -> tuple[Optional[str], str]:
        """Decide the load window for a series.

        Returns ``(observation_start, effective_load_type)``. A series with no
        data yet (or ``load_type: full``, ``force_full``, or a dry run with no
        backend) is loaded in full (``observation_start=None``). Otherwise only
        the last N observations are re-pulled and MERGEd, restating recent
        revisions.
        """
        if force_full or spec.load_type == LoadType.FULL or self.warehouse is None:
            return None, "full"
        n = spec.restate_records or self.config.restate_last_n
        start = self.warehouse.restate_start(spec.series_id, n)
        if start is None:
            return None, "full"  # first load: series not in the warehouse yet
        return start, f"restate_last_{n}"

    def _extract(
        self, spec: SeriesSpec, *, observation_start: Optional[str] = None
    ) -> dict[str, Any]:
        # Complete-history mode: batch all vintages under FRED's cap.
        if spec.vintage_enabled and self.config.complete_vintage_history:
            return self.client.get_observations_all_vintages(
                spec.series_id, observation_start=observation_start
            )
        kwargs: dict[str, Any] = {}
        if spec.vintage_enabled:
            kwargs["realtime_start"] = FULL_VINTAGE_START
            kwargs["realtime_end"] = FULL_VINTAGE_END
        if observation_start:
            kwargs["observation_start"] = observation_start
        return self.client.get_observations(spec.series_id, **kwargs)

    # ---- audit persistence ----------------------------------------------

    def _persist_run(self, run: EtlRun) -> None:
        if not (self.persist_audit and self.warehouse is not None):
            return
        try:
            self.warehouse.persist_run(run)
        except Exception:
            log.exception("Failed to persist audit records for run %s", run.run_id)


def run_pipeline(
    environment: str = "dev",
    manifest_path: str = "manifests",
    *,
    fred_api_key: Optional[str] = None,
    dbutils: Any = None,
    spark: Any = None,
    warehouse: Optional[Warehouse] = None,
    triggered_by: str = "cli",
) -> EtlRun:
    """Convenience entrypoint used by the Databricks job and the CLI."""
    config = PipelineConfig.resolve(
        environment=Environment(environment),
        fred_api_key=fred_api_key,
        dbutils=dbutils,
    )
    if warehouse is None and spark is None:
        from fred_pipeline.spark_io import get_spark

        try:
            spark = get_spark()
        except Exception:  # pragma: no cover - allow no-Spark dry runs
            spark = None
    pipeline = FredPipeline(config, spark=spark, warehouse=warehouse)
    return pipeline.run_from_manifest(manifest_path, triggered_by=triggered_by)
