"""Meta layer sync: register manifests + series into ``meta.*`` tables.

Keeps Unity Catalog's ``meta.fred_series`` / ``meta.fred_manifest`` /
``meta.fred_series_manifest_map`` in lock-step with the YAML manifests, so the
catalog is self-describing and discoverable (lineage, ownership, use-case) even
for consumers who never read the repo.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable

from fred_pipeline.config import PipelineConfig
from fred_pipeline.manifest import Manifest
from fred_pipeline.spark_io import get_spark, merge_delta


def build_meta_rows(manifests: Iterable[Manifest]) -> dict[str, list[dict[str, Any]]]:
    """Produce the three meta table row-sets from parsed manifests (pure)."""
    now = datetime.now(timezone.utc)
    series_rows: list[dict[str, Any]] = []
    manifest_rows: list[dict[str, Any]] = []
    map_rows: list[dict[str, Any]] = []

    for man in manifests:
        manifest_rows.append(
            {
                "manifest_name": man.name,
                "description": man.description,
                "version": man.version,
                "source_path": man.source_path,
                "series_count": len(man.series),
                "loaded_at": now,
            }
        )
        for spec in man.series:
            row = spec.to_dict()
            row["updated_at"] = now
            series_rows.append(row)
            map_rows.append(
                {
                    "series_id": spec.series_id,
                    "manifest_name": man.name,
                    "updated_at": now,
                }
            )
    return {
        "fred_series": series_rows,
        "fred_manifest": manifest_rows,
        "fred_series_manifest_map": map_rows,
    }


def sync_meta(
    config: PipelineConfig,
    manifests: Iterable[Manifest],
    *,
    spark: Any = None,
) -> dict[str, int]:
    """Upsert manifest/series metadata into the ``meta`` schema via MERGE."""
    spark = get_spark(spark)
    rows = build_meta_rows(list(manifests))
    keys = {
        "fred_series": ("series_id",),
        "fred_manifest": ("manifest_name",),
        "fred_series_manifest_map": ("series_id", "manifest_name"),
    }
    counts: dict[str, int] = {}
    for table_name, table_rows in rows.items():
        if not table_rows:
            counts[table_name] = 0
            continue
        df = spark.createDataFrame(table_rows)
        merge_delta(spark, df, config.table("meta", table_name), keys[table_name])
        counts[table_name] = len(table_rows)
    return counts
