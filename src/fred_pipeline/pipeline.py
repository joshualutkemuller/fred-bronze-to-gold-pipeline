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
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Iterable, Optional

from fred_pipeline.audit import EtlRun, RunStatus
from fred_pipeline.bronze import build_bronze_row
from fred_pipeline.config import Environment, PipelineConfig
from fred_pipeline.manifest import LoadType, SeriesSpec, all_series, load_manifests
from fred_pipeline.quality import run_quality_checks
from fred_pipeline.sources.base import SourceClient
from fred_pipeline.sources.bea import BEAClient
from fred_pipeline.sources.bls import BLSClient
from fred_pipeline.sources.census import CensusClient
from fred_pipeline.sources.eia import EIAClient
from fred_pipeline.sources.fred import FredClient
from fred_pipeline.sources.ishares import ISharesClient
from fred_pipeline.sources.sec import SECClient
from fred_pipeline.sources.stooq import StooqClient
from fred_pipeline.sources.treasury import TreasuryClient
from fred_pipeline.sources.worldbank import WorldBankClient
from fred_pipeline.transform import assign_revision_numbers, normalize_observations
from fred_pipeline.warehouse import SparkWarehouse, Warehouse

log = logging.getLogger("fred_pipeline")

# Full-vintage real-time window for point-in-time enabled series.
FULL_VINTAGE_START = "1776-07-04"
FULL_VINTAGE_END = "9999-12-31"


def _make_fred(config: PipelineConfig) -> SourceClient:
    return FredClient(
        api_key=config.fred_api_key,
        base_url=config.fred_base_url,
        timeout=config.request_timeout_seconds,
        max_retries=config.max_retries,
        rate_limit_per_minute=config.rate_limit_per_minute,
    )


def _make_bls(config: PipelineConfig) -> SourceClient:
    # BLS keyless works at a lower quota; a key is used if one is configured.
    return BLSClient(
        api_key=getattr(config, "bls_api_key", "") or None,
        timeout=config.request_timeout_seconds,
        max_retries=config.max_retries,
    )


def _make_eia(config: PipelineConfig) -> SourceClient:
    # EIA requires a key; EIAClient raises if one isn't configured.
    return EIAClient(
        api_key=getattr(config, "eia_api_key", "") or "",
        timeout=config.request_timeout_seconds,
        max_retries=config.max_retries,
    )


def _make_treasury(config: PipelineConfig) -> SourceClient:
    return TreasuryClient(
        timeout=config.request_timeout_seconds,
        max_retries=config.max_retries,
    )


def _make_worldbank(config: PipelineConfig) -> SourceClient:
    return WorldBankClient(
        timeout=config.request_timeout_seconds,
        max_retries=config.max_retries,
    )


def _make_bea(config: PipelineConfig) -> SourceClient:
    # BEA requires a key; BEAClient raises if one isn't configured.
    return BEAClient(
        api_key=getattr(config, "bea_api_key", "") or "",
        timeout=config.request_timeout_seconds,
        max_retries=config.max_retries,
    )


def _make_census(config: PipelineConfig) -> SourceClient:
    # Census works keyless at a lower quota; a key is used if configured.
    return CensusClient(
        api_key=getattr(config, "census_api_key", "") or None,
        timeout=config.request_timeout_seconds,
        max_retries=config.max_retries,
    )


def _make_sec(config: PipelineConfig) -> SourceClient:
    # SEC is keyless but requires a descriptive User-Agent (contact). The target
    # income-statement duration comes from SEC_PERIOD (default quarterly).
    from fred_pipeline.sources.sec import resolve_sec_period

    return SECClient(
        user_agent=getattr(config, "sec_user_agent", "") or None,
        period=resolve_sec_period(),
        timeout=config.request_timeout_seconds,
        max_retries=config.max_retries,
    )


def _make_stooq(config: PipelineConfig) -> SourceClient:
    # Stooq is keyless daily OHLCV (equity price return).
    return StooqClient(
        timeout=config.request_timeout_seconds,
        max_retries=config.max_retries,
    )


def _make_ishares(config: PipelineConfig) -> SourceClient:
    # Keyless ETF-holdings CSV (index constituents / symbol universe).
    return ISharesClient(
        timeout=config.request_timeout_seconds,
        max_retries=config.max_retries,
    )


# Registry of source name -> client factory. Adding a source is one entry here
# plus its client module under fred_pipeline.sources.
SOURCE_FACTORIES = {
    "fred": _make_fred,
    "bls": _make_bls,
    "eia": _make_eia,
    "treasury": _make_treasury,
    "worldbank": _make_worldbank,
    "bea": _make_bea,
    "census": _make_census,
    "sec": _make_sec,
    "stooq": _make_stooq,
    "ishares": _make_ishares,
}

# Sources that require an API key to call, mapped to the PipelineConfig
# attribute holding it. Sources not listed can run keyless (BLS, Census,
# Treasury, World Bank, SEC).
SOURCE_KEY_REQUIREMENTS = {
    "fred": "fred_api_key",
    "eia": "eia_api_key",
    "bea": "bea_api_key",
}


def missing_source_keys(
    config: PipelineConfig, sources: Iterable[str]
) -> dict[str, str]:
    """Return ``{source: config_attr}`` for sources whose required key is unset.

    Lets a caller validate that a run has the keys the *active* sources need,
    instead of demanding a FRED key unconditionally.
    """
    missing: dict[str, str] = {}
    for source in sources:
        attr = SOURCE_KEY_REQUIREMENTS.get(source)
        if attr and not getattr(config, attr, ""):
            missing[source] = attr
    return missing


class FredPipeline:
    def __init__(
        self,
        config: PipelineConfig,
        *,
        client: Optional[SourceClient] = None,
        clients: Optional[dict[str, SourceClient]] = None,
        spark: Any = None,
        warehouse: Optional[Warehouse] = None,
        persist_audit: bool = True,
        notify_transport: Any = None,
    ):
        self.config = config
        self.persist_audit = persist_audit
        # Per-source client cache. ``client`` (back-compat) seeds the default
        # "fred" source; ``clients`` supplies/overrides any source explicitly.
        # Anything not provided is built lazily from SOURCE_FACTORIES.
        self._clients: dict[str, SourceClient] = dict(clients or {})
        if client is not None:
            self._clients.setdefault("fred", client)
        self._notify_transport = notify_transport
        # Resolve the storage backend: explicit warehouse > Spark > None (dry run).
        if warehouse is not None:
            self.warehouse: Optional[Warehouse] = warehouse
        elif spark is not None:
            self.warehouse = SparkWarehouse(config, spark)
        else:
            self.warehouse = None

    @property
    def client(self) -> SourceClient:
        """The default (FRED) source client. Kept for back-compat; per-series
        extraction goes through :meth:`_client_for`."""
        return self._client_for_source("fred")

    def _client_for_source(self, source: str) -> SourceClient:
        client = self._clients.get(source)
        if client is None:
            factory = SOURCE_FACTORIES.get(source)
            if factory is None:
                raise ValueError(
                    f"Unknown source {source!r}; known sources: "
                    f"{sorted(SOURCE_FACTORIES)}"
                )
            client = factory(self.config)
            self._clients[source] = client
        return client

    def _client_for(self, spec: SeriesSpec) -> SourceClient:
        return self._client_for_source(getattr(spec, "source", "fred") or "fred")

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

        # Phase 1 (sequential): decide each series' load window. This reads the
        # warehouse (one SQLite/Delta connection), so it must not run
        # concurrently with itself or with phase 3's writes.
        plans = [self._plan_extract(spec, force_full=force_full) for spec in specs]

        # Phase 2 (thread pool): the network-bound, rate-limited FRED calls.
        # Workers share one FredClient/RateLimiter, so the aggregate request
        # rate is still capped — concurrency here overlaps response wait and
        # per-series retry backoff rather than exceeding the configured rate.
        workers = max(1, self.config.extract_workers)
        with ThreadPoolExecutor(max_workers=workers) as pool:
            outcomes = list(pool.map(self._safe_extract, specs, plans))

        # Phase 3 (sequential): bronze/silver/DQ + all warehouse writes + audit
        # bookkeeping, in original spec order.
        for spec, (_observation_start, load_type), outcome in zip(specs, plans, outcomes):
            self._finish_series(run, spec, load_type, outcome)

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

    def _safe_extract(
        self, spec: SeriesSpec, plan: tuple[Optional[str], str]
    ) -> Any:
        """Run on the thread pool: never raises, so one series' network failure
        can't sink the whole batch or short-circuit ``pool.map``. Returns the
        raw payload, or the caught exception for phase 3 to record.
        """
        observation_start, _load_type = plan
        try:
            return self._extract(spec, observation_start=observation_start)
        except Exception as exc:  # isolate per-series failures
            return exc

    def _finish_series(
        self, run: EtlRun, spec: SeriesSpec, load_type: str, outcome: Any
    ) -> None:
        sr = run.start_series(spec.series_id, load_type=spec.load_type.value)
        sr.load_type = load_type
        try:
            if isinstance(outcome, Exception):
                raise outcome
            payload = outcome
            source = getattr(spec, "source", "fred") or "fred"

            silver_rows = self._normalize(spec, payload, run_id=run.run_id)
            # Count from normalized rows so the metric is source-agnostic (BLS
            # nests observations under Results.series[].data, not a top-level key).
            observations_extracted = len(silver_rows)
            bronze_row = build_bronze_row(
                spec.series_id, self._observations_endpoint(spec), payload,
                run_id=run.run_id, source=source,
                observation_count=observations_extracted,
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
                    observations_extracted=observations_extracted,
                    rows_written_bronze=bronze_written,
                    rows_merged_silver=silver_merged,
                    dq_passed=True,
                )
            else:
                msgs = "; ".join(f.message for f in report.failures)
                sr.complete(
                    RunStatus.FAILED,
                    observations_extracted=observations_extracted,
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
        client = self._client_for(spec)
        # Complete-history mode: batch all vintages under FRED's cap. Only
        # sources that support it (FRED) expose get_observations_all_vintages.
        if (
            spec.vintage_enabled
            and self.config.complete_vintage_history
            and hasattr(client, "get_observations_all_vintages")
        ):
            return client.get_observations_all_vintages(
                spec.series_id, observation_start=observation_start
            )
        kwargs: dict[str, Any] = {}
        if spec.vintage_enabled:
            kwargs["realtime_start"] = FULL_VINTAGE_START
            kwargs["realtime_end"] = FULL_VINTAGE_END
        if observation_start:
            kwargs["observation_start"] = observation_start
        return client.get_observations(spec.series_id, **kwargs)

    def _observations_endpoint(self, spec: SeriesSpec) -> str:
        """The upstream endpoint used for this series, for Bronze lineage.

        Clients advertise it via ``observations_endpoint``; lightweight test
        doubles that don't fall back to the FRED path.
        """
        client = self._client_for(spec)
        fn = getattr(client, "observations_endpoint", None)
        return fn(spec.series_id) if fn else "series/observations"

    def _normalize(
        self, spec: SeriesSpec, payload: dict[str, Any], *, run_id: str
    ) -> list[dict[str, Any]]:
        """Map a raw payload into revision-numbered silver rows.

        Normalization is delegated to the spec's source client (each source
        knows its own response shape); revision numbering is applied uniformly
        here so it stays source-agnostic. Clients that predate the ``normalize``
        contract (e.g. lightweight test doubles) fall back to the FRED
        normalizer.
        """
        client = self._client_for(spec)
        source = getattr(spec, "source", "fred") or "fred"
        normalize = getattr(client, "normalize", None)
        if normalize is not None:
            rows = normalize(
                spec.series_id, payload, run_id=run_id,
                track_vintage=spec.vintage_enabled, source=source,
            )
        else:
            rows = normalize_observations(
                spec.series_id, payload, run_id=run_id,
                track_vintage=spec.vintage_enabled, source=source,
            )
        return assign_revision_numbers(rows)

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
