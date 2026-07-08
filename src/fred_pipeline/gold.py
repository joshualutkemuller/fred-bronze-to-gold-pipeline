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
            AVG(value) OVER (PARTITION BY series_id) AS mean_v,
            STDDEV_POP(value) OVER (PARTITION BY series_id) AS std_v
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
    from fred_pipeline.features import DEFAULT_CURVE_SPREADS

    latest = config.table("gold", "fred_latest_observation")
    gold = config.table("gold", "fred_curve_spread")
    legs = []
    for name, long_leg, short_leg in DEFAULT_CURVE_SPREADS:
        legs.append(f"""
        SELECT '{name}' AS spread_name, a.observation_date,
               '{long_leg}' AS long_leg, '{short_leg}' AS short_leg,
               a.value - b.value AS value
        FROM {latest} a JOIN {latest} b ON a.observation_date = b.observation_date
        WHERE a.series_id = '{long_leg}' AND b.series_id = '{short_leg}'
          AND a.is_missing = false AND b.is_missing = false
        """)
    union = "\nUNION ALL\n".join(legs)
    return f"CREATE OR REPLACE TABLE {gold} AS\n{union}"


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


def build_gold(config: PipelineConfig, *, spark: Any = None) -> dict[str, str]:
    """Rebuild all Gold tables from Silver. Returns table -> 'ok' map."""
    spark = get_spark(spark)
    steps = {
        "fred_point_in_time": _point_in_time_sql(config),
        "fred_latest_observation": _latest_observation_sql(config),
        "fred_macro_feature_daily": _macro_feature_daily_sql(config),
        "fred_feature_transforms": _feature_transforms_sql(config),
        "fred_curve_spread": _curve_spread_sql(config),
    }
    results: dict[str, str] = {}
    for name, sql in steps.items():
        spark.sql(sql)
        results[name] = "ok"
    return results
