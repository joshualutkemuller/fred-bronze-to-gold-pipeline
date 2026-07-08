"""Bronze layer: raw payload retention.

Bronze stores the FRED API response *verbatim* (as a JSON string) plus minimal
ingestion metadata. Nothing is parsed or reshaped here — Bronze is the system
of record we can always re-derive Silver/Gold from.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Optional

from fred_pipeline.config import PipelineConfig
from fred_pipeline.spark_io import append_rows, get_spark
from fred_pipeline.transform import payload_summary


def build_bronze_row(
    series_id: str,
    endpoint: str,
    payload: dict[str, Any],
    *,
    run_id: str,
    request_params: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Assemble a single ``bronze.fred_api_response`` row from a raw payload."""
    summary = payload_summary(payload)
    return {
        "run_id": run_id,
        "series_id": series_id,
        "endpoint": endpoint,
        "request_params": json.dumps(request_params or {}, sort_keys=True),
        "response_payload": json.dumps(payload),
        "observation_count": summary["observation_count"],
        "payload_bytes": summary["payload_bytes"],
        "ingested_at": datetime.now(timezone.utc),
    }


def write_bronze(
    config: PipelineConfig,
    rows: list[dict[str, Any]],
    *,
    spark: Any = None,
) -> int:
    """Append bronze rows to ``<catalog>.bronze.fred_api_response``."""
    if not rows:
        return 0
    spark = get_spark(spark)
    table = config.table("bronze", "fred_api_response")
    return append_rows(spark, rows, table)
