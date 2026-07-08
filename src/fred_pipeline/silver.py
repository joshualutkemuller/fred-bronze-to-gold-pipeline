"""Silver layer: normalized, deduplicated observations.

Silver is the clean, queryable form of every observation. Loads are idempotent
via a Delta MERGE on (series_id, observation_date, realtime_start), so re-runs
never create duplicates and revisions land as new real-time rows.
"""

from __future__ import annotations

from typing import Any

from fred_pipeline.config import PipelineConfig
from fred_pipeline.spark_io import get_spark, merge_delta
from fred_pipeline.transform import (
    SILVER_COLUMNS,
    assign_revision_numbers,
    normalize_observations,
)

# Natural key that makes a silver row unique.
SILVER_MERGE_KEYS = ("series_id", "observation_date", "realtime_start")


def build_silver_rows(
    series_id: str,
    payload: dict[str, Any],
    *,
    run_id: str,
    ingested_at: str | None = None,
    track_vintage: bool = True,
) -> list[dict[str, Any]]:
    """Normalize a raw payload into silver rows with revision numbers."""
    rows = normalize_observations(
        series_id, payload, run_id=run_id, ingested_at=ingested_at,
        track_vintage=track_vintage,
    )
    return assign_revision_numbers(rows)


# Source rows carry ISO strings; cast to the Delta column types before MERGE.
_SILVER_CASTS = {
    "observation_date": "date",
    "realtime_start": "date",
    "realtime_end": "date",
    "ingested_at": "timestamp",
}


def merge_silver(
    config: PipelineConfig,
    rows: list[dict[str, Any]],
    *,
    spark: Any = None,
) -> dict[str, int]:
    """MERGE normalized rows into ``<catalog>.silver.fred_observation``."""
    if not rows:
        return {"source_rows": 0}
    spark = get_spark(spark)
    table = config.table("silver", "fred_observation")
    df = spark.createDataFrame(rows)
    # Empty strings (e.g. open-ended realtime windows) must become NULL dates.
    exprs = []
    for col in df.columns:
        cast = _SILVER_CASTS.get(col)
        if cast in ("date", "timestamp"):
            exprs.append(
                f"CAST(NULLIF({col}, '') AS {cast}) AS {col}"
            )
        else:
            exprs.append(col)
    df = df.selectExpr(*exprs)
    return merge_delta(spark, df, table, SILVER_MERGE_KEYS)


__all__ = ["SILVER_COLUMNS", "SILVER_MERGE_KEYS", "build_silver_rows", "merge_silver"]
