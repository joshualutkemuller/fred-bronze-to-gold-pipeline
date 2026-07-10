"""Gold layer: analytics-ready feature tables and point-in-time views.

Gold is rebuilt from Silver with pure SQL so it stays reproducible and cheap to
reason about:

  * ``gold.fred_latest_observation`` — one row per (series, date) at its latest
    revision (the "as revised today" view).
  * ``gold.fred_point_in_time``       — full vintage history for ALFRED-style
    "what was known on date X" queries.
  * ``gold.fred_macro_feature_daily``  — a daily, forward-filled feature matrix
    suitable for optimizer inputs and ML.

The SQL templates live here so the orchestrator can refresh Gold in one call;
equivalent DDL/DML also ships in ``sql/50_gold.sql`` and ``sql/60_views.sql``.
"""

from __future__ import annotations

from typing import Any

from fred_pipeline.config import PipelineConfig
from fred_pipeline.spark_io import get_spark


def _latest_observation_sql(config: PipelineConfig) -> str:
    silver = config.table("silver", "fred_observation")
    gold = config.table("gold", "fred_latest_observation")
    return f"""
    CREATE OR REPLACE TABLE {gold} AS
    WITH ranked AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY series_id, observation_date
                ORDER BY realtime_start DESC
            ) AS rn
        FROM {silver}
    )
    SELECT
        series_id,
        observation_date,
        value,
        realtime_start,
        realtime_end,
        is_missing,
        revision_number,
        ingested_at
    FROM ranked
    WHERE rn = 1
    """


def _point_in_time_sql(config: PipelineConfig) -> str:
    silver = config.table("silver", "fred_observation")
    gold = config.table("gold", "fred_point_in_time")
    return f"""
    CREATE OR REPLACE TABLE {gold} AS
    SELECT
        series_id,
        observation_date,
        realtime_start,
        realtime_end,
        value,
        revision_number,
        is_missing,
        ingested_at
    FROM {silver}
    """


def _macro_feature_daily_sql(config: PipelineConfig) -> str:
    """Daily, forward-filled feature matrix built from latest observations."""
    latest = config.table("gold", "fred_latest_observation")
    gold = config.table("gold", "fred_macro_feature_daily")
    return f"""
    CREATE OR REPLACE TABLE {gold} AS
    WITH bounds AS (
        SELECT MIN(observation_date) AS min_d, MAX(observation_date) AS max_d
        FROM {latest}
    ),
    calendar AS (
        SELECT explode(sequence(
            (SELECT min_d FROM bounds),
            (SELECT max_d FROM bounds),
            INTERVAL 1 DAY
        )) AS as_of_date
    ),
    series_list AS (
        SELECT DISTINCT series_id FROM {latest}
    ),
    grid AS (
        SELECT c.as_of_date, s.series_id
        FROM calendar c CROSS JOIN series_list s
    ),
    joined AS (
        SELECT
            g.as_of_date,
            g.series_id,
            l.value,
            last_value(l.value, true) OVER (
                PARTITION BY g.series_id ORDER BY g.as_of_date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS value_ffill
        FROM grid g
        LEFT JOIN {latest} l
          ON g.series_id = l.series_id AND g.as_of_date = l.observation_date
    )
    SELECT as_of_date, series_id, value AS raw_value, value_ffill AS value
    FROM joined
    """


def _feature_transforms_sql(config: PipelineConfig) -> str:
    """MoM / diff / YoY / z-score per series from latest observations.

    Note: YoY here matches the exact −12-month observation (works for monthly/
    quarterly month-end series). The Local/SQLite backend uses nearest-on-or-
    before within a tolerance; results agree for regular monthly/quarterly data.

    z-score is an *expanding* (point-in-time safe) mean/std — the window frame
    is bounded to UNBOUNDED PRECEDING..CURRENT ROW, so each row's mean/std only
    reflects observations at-or-before its own date, never later ones. A plain
    ``PARTITION BY series_id`` (whole-partition) aggregate would leak future
    values into every historical row.
    """
    latest = config.table("gold", "fred_latest_observation")
    gold = config.table("gold", "fred_feature_transforms")
    return f"""
    CREATE OR REPLACE TABLE {gold} AS
    WITH base AS (
        SELECT series_id, observation_date, value
        FROM {latest} WHERE is_missing = false
    ),
    w AS (
        SELECT series_id, observation_date, value,
            LAG(value) OVER (PARTITION BY series_id ORDER BY observation_date) AS prev_value,
            AVG(value) OVER (
                PARTITION BY series_id ORDER BY observation_date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS mean_v,
            STDDEV_POP(value) OVER (
                PARTITION BY series_id ORDER BY observation_date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS std_v
        FROM base
    ),
    ya AS (
        SELECT a.*, b.value AS year_ago
        FROM w a
        LEFT JOIN base b
          ON b.series_id = a.series_id
         AND b.observation_date = add_months(a.observation_date, -12)
    )
    SELECT series_id, observation_date, value,
        CASE WHEN prev_value IS NOT NULL AND prev_value <> 0
             THEN (value - prev_value) / prev_value END AS mom,
        CASE WHEN prev_value IS NOT NULL THEN value - prev_value END AS diff,
        CASE WHEN year_ago IS NOT NULL AND year_ago <> 0
             THEN (value - year_ago) / year_ago END AS yoy,
        CASE WHEN std_v IS NOT NULL AND std_v <> 0
             THEN (value - mean_v) / std_v END AS zscore
    FROM ya
    """


def _curve_spread_sql(config: PipelineConfig) -> str:
    """Cross-series spreads/ratios, defined in ``config/spreads.yml`` (see
    :func:`fred_pipeline.spread_config.load_spread_defs`) rather than
    hardcoded, so a reviewer can add new pairs without touching this file."""
    from fred_pipeline.spread_config import load_spread_defs

    latest = config.table("gold", "fred_latest_observation")
    gold = config.table("gold", "fred_curve_spread")
    legs = []
    for sd in load_spread_defs():
        if sd.op == "ratio":
            value_sql = "a.value / b.value"
            zero_guard = "\n          AND b.value <> 0"
        else:
            value_sql = "a.value - b.value"
            zero_guard = ""
        legs.append(f"""
        SELECT '{sd.name}' AS spread_name, a.observation_date,
               '{sd.long_leg}' AS long_leg, '{sd.short_leg}' AS short_leg,
               {value_sql} AS value
        FROM {latest} a JOIN {latest} b ON a.observation_date = b.observation_date
        WHERE a.series_id = '{sd.long_leg}' AND b.series_id = '{sd.short_leg}'
          AND a.is_missing = false AND b.is_missing = false{zero_guard}
        """)
    union = "\nUNION ALL\n".join(legs)
    return f"CREATE OR REPLACE TABLE {gold} AS\n{union}"


def _revision_stats_sql(config: PipelineConfig) -> str:
    """How much each observation moved between its first print and today.

    Unlike the other Gold tables (built from ``gold.fred_latest_observation``),
    this reads raw Silver (every vintage), since it exists to measure revision
    behavior itself. Non-vintage series (blank ``realtime_start``) always have
    ``revision_count = 1`` — no vintage history is tracked for them.
    """
    silver = config.table("silver", "fred_observation")
    gold = config.table("gold", "fred_revision_stats")
    return f"""
    CREATE OR REPLACE TABLE {gold} AS
    WITH base AS (
        SELECT series_id, observation_date, realtime_start, value, revision_number
        FROM {silver}
        WHERE is_missing = false AND value IS NOT NULL
    ),
    bounds AS (
        SELECT series_id, observation_date,
            MIN(revision_number) AS min_rev, MAX(revision_number) AS max_rev,
            COUNT(*) AS revision_count
        FROM base
        GROUP BY series_id, observation_date
    )
    SELECT b.series_id, b.observation_date, b.revision_count,
        f.value AS first_value, f.realtime_start AS first_realtime_start,
        l.value AS latest_value, l.realtime_start AS latest_realtime_start,
        l.value - f.value AS revision_delta,
        CASE WHEN f.value <> 0 THEN (l.value - f.value) / f.value END AS revision_pct
    FROM bounds b
    JOIN base f ON f.series_id = b.series_id AND f.observation_date = b.observation_date
              AND f.revision_number = b.min_rev
    JOIN base l ON l.series_id = b.series_id AND l.observation_date = b.observation_date
              AND l.revision_number = b.max_rev
    """


def point_in_time_features_sql(config: PipelineConfig, as_of: str) -> str:
    """Ad-hoc SQL: each series' value as known on ``as_of`` (leakage-free)."""
    silver = config.table("silver", "fred_observation")
    return f"""
    WITH known AS (
        SELECT series_id, observation_date, value, realtime_start
        FROM {silver}
        WHERE is_missing = false
          AND realtime_start <= DATE '{as_of}'
          AND (realtime_end IS NULL OR realtime_end > DATE '{as_of}')
    ),
    ranked AS (
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY series_id
            ORDER BY observation_date DESC, realtime_start DESC
        ) AS rn
        FROM known
    )
    SELECT DATE '{as_of}' AS as_of_date, series_id, observation_date, value
    FROM ranked WHERE rn = 1
    """


def _build_cross_series(config: PipelineConfig, spark: Any) -> None:
    """Build ``gold.fred_cross_series_feature`` on Spark by reusing the pure-Python
    reference (:func:`fred_pipeline.features.compute_cross_series_features`).

    Cross-series features involve only the handful of leg series named in
    ``config/cross_series.yml``, so we read just those from the (already-built)
    latest-observation table, collect them, compute in Python — guaranteeing
    parity with the local backend — and overwrite the table.
    """
    from pyspark.sql.types import (
        DoubleType, StringType, StructField, StructType,
    )

    from fred_pipeline.cross_series_config import load_cross_series_defs
    from fred_pipeline.features import compute_cross_series_features

    gold = config.table("gold", "fred_cross_series_feature")
    defs = load_cross_series_defs()
    leg_ids = sorted({sid for d in defs for (sid, _w) in d.legs})

    feats: list[dict[str, Any]] = []
    if leg_ids:
        latest = config.table("gold", "fred_latest_observation")
        in_list = ", ".join("'" + s.replace("'", "''") + "'" for s in leg_ids)
        df = spark.sql(
            f"SELECT series_id, CAST(observation_date AS STRING) AS observation_date, "
            f"value, is_missing FROM {latest} WHERE series_id IN ({in_list})"
        )
        rows = [r.asDict() for r in df.collect()]
        feats = compute_cross_series_features(rows, defs)

    # Explicit schema so the empty case still creates a well-typed table.
    schema = StructType([
        StructField("feature_name", StringType()),
        StructField("op", StringType()),
        StructField("observation_date", StringType()),
        StructField("value", DoubleType()),
    ])
    out = spark.createDataFrame(feats, schema=schema).selectExpr(
        "feature_name", "op",
        "CAST(observation_date AS DATE) AS observation_date",
        "CAST(value AS DOUBLE) AS value",
    )
    out.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(gold)


def build_gold(config: PipelineConfig, *, spark: Any = None) -> dict[str, str]:
    """Rebuild all Gold tables from Silver. Returns table -> 'ok' map."""
    spark = get_spark(spark)
    steps = {
        "fred_point_in_time": _point_in_time_sql(config),
        "fred_latest_observation": _latest_observation_sql(config),
        "fred_macro_feature_daily": _macro_feature_daily_sql(config),
        "fred_feature_transforms": _feature_transforms_sql(config),
        "fred_curve_spread": _curve_spread_sql(config),
        "fred_revision_stats": _revision_stats_sql(config),
    }
    results: dict[str, str] = {}
    for name, sql in steps.items():
        spark.sql(sql)
        results[name] = "ok"
    # Cross-series features are computed in Python (reused by both backends) and
    # written after latest_observation exists.
    _build_cross_series(config, spark)
    results["fred_cross_series_feature"] = "ok"
    return results
