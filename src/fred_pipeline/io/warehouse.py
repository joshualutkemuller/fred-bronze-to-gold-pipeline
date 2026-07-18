"""Storage-backend abstraction.

The orchestrator (:mod:`fred_pipeline.pipeline`) talks to a *warehouse* rather
than to Spark directly, so the same run logic can persist to either:

  * :class:`SparkWarehouse` — Databricks / Delta Lake (production), or
  * :class:`fred_pipeline.local_store.LocalWarehouse` — a local SQLite file
    (laptop dev, demos, CI), or
  * ``None`` — a pure in-memory dry run (extract + DQ, no writes).

Each backend implements the same small surface used by the pipeline.
"""

from __future__ import annotations

from typing import Any, Iterable, Optional, Protocol, runtime_checkable

from fred_pipeline.audit import EtlRun
from fred_pipeline.config import PipelineConfig
from fred_pipeline.manifest import Manifest
from fred_pipeline.quality import QualityReport


@runtime_checkable
class Warehouse(Protocol):
    """Persistence surface required by the pipeline."""

    def sync_meta(self, manifests: Iterable[Manifest]) -> dict[str, int]: ...
    def restate_start(self, series_id: str, n: int) -> Optional[str]: ...
    def write_bronze(self, rows: list[dict[str, Any]]) -> int: ...
    def read_bronze(
        self, series_ids: Optional[list[str]] = None
    ) -> list[dict[str, Any]]: ...
    def merge_silver(self, rows: list[dict[str, Any]]) -> int: ...
    def build_gold(self) -> dict[str, str]: ...
    def write_lifecycle(self, rows: list[dict[str, Any]]) -> int: ...
    def write_drift(self, rows: list[dict[str, Any]]) -> int: ...
    def write_release_calendar(self, rows: list[dict[str, Any]]) -> int: ...
    def persist_run(self, run: EtlRun) -> None: ...
    def persist_dq(self, run_id: str, report: QualityReport) -> None: ...
    def close(self) -> None: ...


def dq_rows(run_id: str, report: QualityReport) -> list[dict[str, Any]]:
    """Flatten a QualityReport into audit.data_quality_result rows (shared)."""
    return [
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


class SparkWarehouse:
    """Delta Lake backend for Databricks. Thin adapter over the layer writers."""

    def __init__(self, config: PipelineConfig, spark: Any):
        self.config = config
        self.spark = spark

    def sync_meta(self, manifests: Iterable[Manifest]) -> dict[str, int]:
        from fred_pipeline.meta import sync_meta

        return sync_meta(self.config, list(manifests), spark=self.spark)

    def restate_start(self, series_id: str, n: int) -> Optional[str]:
        """Earliest observation_date among the N most recent for this series.

        Returns ``None`` when the series has no data yet (→ full load). Used to
        set ``observation_start`` for incremental "restate last N" pulls.
        """
        from fred_pipeline.spark_io import table_exists

        table = self.config.table("silver", "fred_observation")
        if not table_exists(self.spark, table):
            return None
        safe_id = series_id.replace("'", "''")
        df = self.spark.sql(
            f"""
            SELECT MIN(observation_date) AS start FROM (
                SELECT DISTINCT observation_date FROM {table}
                WHERE series_id = '{safe_id}'
                ORDER BY observation_date DESC
                LIMIT {int(n)}
            )
            """
        )
        rows = df.collect()
        val = rows[0]["start"] if rows else None
        return None if val is None else str(val)

    def write_bronze(self, rows: list[dict[str, Any]]) -> int:
        from fred_pipeline.bronze import write_bronze

        return write_bronze(self.config, rows, spark=self.spark)

    def read_bronze(
        self, series_ids: Optional[list[str]] = None
    ) -> list[dict[str, Any]]:
        table = self.config.table("bronze", "fred_api_response")
        where = ""
        if series_ids:
            ids = ", ".join("'" + s.replace("'", "''") + "'" for s in series_ids)
            where = f"WHERE series_id IN ({ids})"
        df = self.spark.sql(
            f"SELECT series_id, response_payload, run_id, ingested_at "
            f"FROM {table} {where} ORDER BY ingested_at"
        )
        return [row.asDict() for row in df.collect()]

    def merge_silver(self, rows: list[dict[str, Any]]) -> int:
        from fred_pipeline.silver import merge_silver

        return merge_silver(self.config, rows, spark=self.spark).get("source_rows", 0)

    def build_gold(self) -> dict[str, str]:
        from fred_pipeline.gold import build_gold

        return build_gold(self.config, spark=self.spark)

    def write_lifecycle(self, rows: list[dict[str, Any]]) -> int:
        from fred_pipeline.spark_io import append_rows

        return append_rows(
            self.spark, rows, self.config.table("meta", "fred_series_lifecycle")
        )

    def write_drift(self, rows: list[dict[str, Any]]) -> int:
        from fred_pipeline.spark_io import append_rows

        return append_rows(
            self.spark, rows, self.config.table("meta", "fred_series_drift")
        )

    def write_release_calendar(self, rows: list[dict[str, Any]]) -> int:
        # Full-refresh (not append): it's a re-fetched forward schedule, not
        # an accumulating observation history, so overwrite each run.
        from pyspark.sql.types import (
            BooleanType, IntegerType, StringType, StructField, StructType,
        )

        schema = StructType([
            StructField("release_id", IntegerType()),
            StructField("release_name", StringType()),
            StructField("release_date", StringType()),
            StructField("importance", StringType()),
            StructField("econ_category", StringType()),
            StructField("representative_series_id", StringType()),
            StructField("is_future", BooleanType()),
            StructField("fetched_at", StringType()),
        ])
        self.spark.createDataFrame(rows, schema=schema).selectExpr(
            "release_id", "release_name",
            "CAST(release_date AS DATE) AS release_date",
            "importance", "econ_category", "representative_series_id",
            "is_future", "fetched_at",
        ).write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).saveAsTable(self.config.table("gold", "release_calendar"))
        return len(rows)

    def persist_run(self, run: EtlRun) -> None:
        from fred_pipeline.spark_io import append_rows

        append_rows(self.spark, [run.to_row()], self.config.table("audit", "etl_run"))
        append_rows(
            self.spark,
            [s.to_row() for s in run.series_runs],
            self.config.table("audit", "etl_series_run"),
        )

    def persist_dq(self, run_id: str, report: QualityReport) -> None:
        from fred_pipeline.spark_io import append_rows

        append_rows(
            self.spark,
            dq_rows(run_id, report),
            self.config.table("audit", "data_quality_result"),
        )

    def close(self) -> None:  # Spark session is managed by the runtime.
        pass
