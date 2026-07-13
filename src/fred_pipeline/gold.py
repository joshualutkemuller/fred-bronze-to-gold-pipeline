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


def _build_cross_series_pit(config: PipelineConfig, spark: Any) -> None:
    """Build ``gold.fred_cross_series_feature_pit`` — the point-in-time
    (as-first-reported) cross-series features — by reusing the pure-Python
    reference (:func:`fred_pipeline.features.compute_cross_series_features_pit`).

    Reads the configured leg series' **full vintage history** from Silver (this
    variant needs ``realtime_start``), collects them, computes in Python, and
    overwrites the table."""
    from pyspark.sql.types import (
        DoubleType, StringType, StructField, StructType,
    )

    from fred_pipeline.cross_series_config import load_cross_series_defs
    from fred_pipeline.features import compute_cross_series_features_pit

    gold = config.table("gold", "fred_cross_series_feature_pit")
    defs = load_cross_series_defs()
    leg_ids = sorted({sid for d in defs for (sid, _w) in d.legs})

    rows_out: list[dict[str, Any]] = []
    if leg_ids:
        silver = config.table("silver", "fred_observation")
        in_list = ", ".join("'" + s.replace("'", "''") + "'" for s in leg_ids)
        df = spark.sql(
            f"SELECT series_id, CAST(observation_date AS STRING) AS observation_date, "
            f"CAST(realtime_start AS STRING) AS realtime_start, value, is_missing "
            f"FROM {silver} WHERE series_id IN ({in_list})"
        )
        rows = [r.asDict() for r in df.collect()]
        rows_out = compute_cross_series_features_pit(rows, defs)

    schema = StructType([
        StructField("feature_name", StringType()),
        StructField("op", StringType()),
        StructField("observation_date", StringType()),
        StructField("value", DoubleType()),
        StructField("basis", StringType()),
    ])
    out = spark.createDataFrame(rows_out, schema=schema).selectExpr(
        "feature_name", "op",
        "CAST(observation_date AS DATE) AS observation_date",
        "CAST(value AS DOUBLE) AS value", "basis",
    )
    out.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(gold)


def _build_company_financials(config: PipelineConfig, spark: Any) -> None:
    """Build ``gold.fred_company_fundamentals`` + ``gold.fred_company_ratios`` by
    reusing the pure-Python standardizer (:mod:`fred_pipeline.sec_standardization`).

    Reads the ``source='sec'`` slice of Silver (bounded by the active SEC series),
    standardizes raw XBRL tags into canonical concepts, computes ratios, and
    overwrites both tables. Priority tag-coalescing is impractical in SQL, so —
    like the other cross-cutting Gold tables — both backends share the one
    Python engine."""
    from pyspark.sql.types import DoubleType, StringType, StructField, StructType

    from fred_pipeline.sec_standardization import (
        compute_sec_ratios, standardize_sec_statements,
    )

    silver = config.table("silver", "fred_observation")
    df = spark.sql(
        f"SELECT source, series_id, "
        f"CAST(observation_date AS STRING) AS observation_date, "
        f"CAST(realtime_start AS STRING) AS realtime_start, value, is_missing "
        f"FROM {silver} WHERE source = 'sec'"
    )
    rows = [r.asDict() for r in df.collect()]
    fundamentals = standardize_sec_statements(rows)
    ratios = compute_sec_ratios(fundamentals)

    fund_schema = StructType([
        StructField("cik", StringType()), StructField("concept", StringType()),
        StructField("statement", StringType()),
        StructField("observation_date", StringType()),
        StructField("value", DoubleType()),
    ])
    spark.createDataFrame(fundamentals, schema=fund_schema).selectExpr(
        "cik", "concept", "statement",
        "CAST(observation_date AS DATE) AS observation_date",
        "CAST(value AS DOUBLE) AS value",
    ).write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(config.table("gold", "fred_company_fundamentals"))

    ratio_schema = StructType([
        StructField("cik", StringType()), StructField("ratio_name", StringType()),
        StructField("observation_date", StringType()),
        StructField("value", DoubleType()),
    ])
    spark.createDataFrame(ratios, schema=ratio_schema).selectExpr(
        "cik", "ratio_name",
        "CAST(observation_date AS DATE) AS observation_date",
        "CAST(value AS DOUBLE) AS value",
    ).write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(config.table("gold", "fred_company_ratios"))


def _build_source_reconciliation(config: PipelineConfig, spark: Any) -> None:
    """Build ``gold.fred_source_reconciliation`` on Spark by reusing the pure-Python
    reference (:func:`fred_pipeline.features.compute_source_reconciliation`) — same
    collect-legs-and-compute pattern as :func:`_build_cross_series`, so both
    backends stay in parity."""
    from pyspark.sql.types import (
        BooleanType, DoubleType, StringType, StructField, StructType,
    )

    from fred_pipeline.features import compute_source_reconciliation
    from fred_pipeline.reconciliation_config import load_reconciliation_defs

    gold = config.table("gold", "fred_source_reconciliation")
    defs = load_reconciliation_defs()
    series = sorted({s for d in defs for s in (d.series_a, d.series_b)})

    rows_out: list[dict[str, Any]] = []
    if series:
        latest = config.table("gold", "fred_latest_observation")
        in_list = ", ".join("'" + s.replace("'", "''") + "'" for s in series)
        df = spark.sql(
            f"SELECT series_id, CAST(observation_date AS STRING) AS observation_date, "
            f"value, is_missing FROM {latest} WHERE series_id IN ({in_list})"
        )
        rows = [r.asDict() for r in df.collect()]
        rows_out = compute_source_reconciliation(rows, defs)

    schema = StructType([
        StructField("name", StringType()),
        StructField("observation_date", StringType()),
        StructField("series_a", StringType()),
        StructField("value_a", DoubleType()),
        StructField("series_b", StringType()),
        StructField("value_b", DoubleType()),
        StructField("abs_diff", DoubleType()),
        StructField("pct_diff", DoubleType()),
        StructField("diverged", BooleanType()),
    ])
    out = spark.createDataFrame(rows_out, schema=schema).selectExpr(
        "name", "CAST(observation_date AS DATE) AS observation_date",
        "series_a", "CAST(value_a AS DOUBLE) AS value_a",
        "series_b", "CAST(value_b AS DOUBLE) AS value_b",
        "CAST(abs_diff AS DOUBLE) AS abs_diff",
        "CAST(pct_diff AS DOUBLE) AS pct_diff",
        "diverged",
    )
    out.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(gold)


def _collect_latest(config: PipelineConfig, spark: Any, series_ids: list[str]) -> list[dict[str, Any]]:
    """Collect the named series from ``gold.fred_latest_observation`` as plain
    dicts (dates as ISO strings) — the input shape the pure-Python
    terminal-view engines expect."""
    if not series_ids:
        return []
    latest = config.table("gold", "fred_latest_observation")
    in_list = ", ".join("'" + s.replace("'", "''") + "'" for s in series_ids)
    df = spark.sql(
        f"SELECT series_id, CAST(observation_date AS STRING) AS observation_date, "
        f"CAST(realtime_start AS STRING) AS realtime_start, value, is_missing "
        f"FROM {latest} WHERE series_id IN ({in_list})"
    )
    return [r.asDict() for r in df.collect()]


def _build_terminal_views(config: PipelineConfig, spark: Any) -> None:
    """Build the market-terminal analytical views (dimensions + ECON macro
    dashboard + Treasury Curve Lab; see ``docs/market_terminal_gold_views.md``)
    by reusing the pure-Python engines in :mod:`fred_pipeline.terminal_views` —
    the same collect-and-compute pattern as :func:`_build_cross_series`, so
    both backends stay in parity. Inputs are bounded: only cataloged series,
    curve tenors, spread legs, and USREC are collected."""
    from pyspark.sql.types import (
        BooleanType, DoubleType, IntegerType, StringType, StructField, StructType,
    )

    from fred_pipeline.catalog_config import load_series_catalog
    from fred_pipeline.curve_config import load_curve_defs
    from fred_pipeline.spread_config import load_spread_defs
    from fred_pipeline.inflation_config import load_inflation_items
    from fred_pipeline.rates_complex_config import (
        load_benchmark_board, load_credit_config, load_funding_config,
    )
    from fred_pipeline.terminal_views import (
        RECESSION_SERIES,
        build_dim_date,
        build_dim_series,
        compute_benchmark_rate_board,
        compute_credit_spread_daily,
        compute_curve_spread_daily,
        compute_funding_features,
        compute_inflation_explorer,
        compute_macro_dashboard,
        compute_spread_inversion_episodes,
        compute_treasury_curve,
    )

    def _write(name: str, rows: list[dict[str, Any]], schema: Any, casts: list[str]) -> None:
        spark.createDataFrame(rows, schema=schema).selectExpr(*casts).write.format(
            "delta"
        ).mode("overwrite").option("overwriteSchema", "true").saveAsTable(
            config.table("gold", name)
        )

    catalog = load_series_catalog()
    tenors = load_curve_defs()
    spreads = load_spread_defs()

    # dim_series: catalog semantics + title/frequency/units from meta.
    meta_df = spark.sql(
        f"SELECT series_id, title, frequency, units FROM "
        f"{config.table('meta', 'fred_series')}"
    )
    dim_series = build_dim_series(catalog, [r.asDict() for r in meta_df.collect()])
    _write("dim_series", dim_series, StructType([
        StructField("series_id", StringType()), StructField("title", StringType()),
        StructField("source", StringType()), StructField("frequency", StringType()),
        StructField("units", StringType()), StructField("econ_category", StringType()),
        StructField("polarity", IntegerType()),
        StructField("default_transform", StringType()),
        StructField("scale", StringType()), StructField("decimals", IntegerType()),
        StructField("notes", StringType()),
    ]), ["*"])

    # dim_date: calendar bounds from latest observations + the USREC overlay.
    latest = config.table("gold", "fred_latest_observation")
    bounds = spark.sql(
        f"SELECT CAST(MIN(observation_date) AS STRING) AS lo, "
        f"CAST(MAX(observation_date) AS STRING) AS hi FROM {latest} "
        f"WHERE is_missing = false"
    ).collect()[0]
    usrec_rows = _collect_latest(config, spark, [RECESSION_SERIES])
    dim_date = (
        build_dim_date(bounds["lo"], bounds["hi"], usrec_rows)
        if bounds["lo"] else []
    )
    _write("dim_date", dim_date, StructType([
        StructField("date", StringType()), StructField("year", IntegerType()),
        StructField("quarter", IntegerType()), StructField("month", IntegerType()),
        StructField("month_name", StringType()),
        StructField("is_month_end", BooleanType()),
        StructField("fiscal_year", IntegerType()),
        StructField("is_recession", BooleanType()),
    ]), ["CAST(date AS DATE) AS date", "year", "quarter", "month", "month_name",
         "is_month_end", "fiscal_year", "is_recession"])

    # ECON macro dashboard (+ sparkline + category summary) over the catalog.
    dash = compute_macro_dashboard(
        _collect_latest(config, spark, sorted(e.series_id for e in catalog)),
        catalog,
    )
    _write("macro_indicator_dashboard", dash["dashboard"], StructType([
        StructField("series_id", StringType()),
        StructField("econ_category", StringType()),
        StructField("polarity", IntegerType()),
        StructField("default_transform", StringType()),
        StructField("as_of_date", StringType()),
        StructField("latest_date", StringType()),
        StructField("latest_value", DoubleType()),
        StructField("prior_date", StringType()),
        StructField("prior_value", DoubleType()),
        StructField("change_abs", DoubleType()),
        StructField("change_pct", DoubleType()),
        StructField("yoy_pct", DoubleType()),
        StructField("zscore", DoubleType()),
        StructField("percentile", DoubleType()),
        StructField("surprise", DoubleType()),
        StructField("surprise_z", DoubleType()),
        StructField("direction_is_good", BooleanType()),
        StructField("spark_min", DoubleType()),
        StructField("spark_max", DoubleType()),
        StructField("staleness_days", IntegerType()),
        StructField("realtime_start", StringType()),
    ]), ["series_id", "econ_category", "polarity", "default_transform",
         "CAST(as_of_date AS DATE) AS as_of_date",
         "CAST(latest_date AS DATE) AS latest_date", "latest_value",
         "CAST(prior_date AS DATE) AS prior_date", "prior_value", "change_abs",
         "change_pct", "yoy_pct", "zscore", "percentile", "surprise",
         "surprise_z", "direction_is_good", "spark_min", "spark_max",
         "staleness_days", "realtime_start"])
    _write("macro_indicator_sparkline", dash["sparkline"], StructType([
        StructField("series_id", StringType()),
        StructField("point_index", IntegerType()),
        StructField("observation_date", StringType()),
        StructField("value", DoubleType()),
    ]), ["series_id", "point_index",
         "CAST(observation_date AS DATE) AS observation_date", "value"])
    _write("macro_category_summary", dash["category_summary"], StructType([
        StructField("econ_category", StringType()),
        StructField("as_of_date", StringType()),
        StructField("n_series", IntegerType()),
        StructField("n_improving", IntegerType()),
        StructField("n_deteriorating", IntegerType()),
        StructField("breadth_pct", DoubleType()),
        StructField("avg_zscore", DoubleType()),
        StructField("surprise_index", DoubleType()),
    ]), ["econ_category", "CAST(as_of_date AS DATE) AS as_of_date", "n_series",
         "n_improving", "n_deteriorating", "breadth_pct", "avg_zscore",
         "surprise_index"])

    # Treasury Curve Lab: tenor series + USREC.
    curve_rows = _collect_latest(
        config, spark,
        sorted({t.series_id for t in tenors} | {RECESSION_SERIES}),
    )
    curve = compute_treasury_curve(curve_rows, tenors)
    _write("treasury_curve", curve["curve"], StructType([
        StructField("as_of_date", StringType()),
        StructField("tenor_label", StringType()),
        StructField("tenor_months", IntegerType()),
        StructField("series_id", StringType()),
        StructField("yield_pct", DoubleType()),
    ]), ["CAST(as_of_date AS DATE) AS as_of_date", "tenor_label",
         "tenor_months", "series_id", "yield_pct"])
    _write("treasury_curve_metrics", curve["metrics"], StructType([
        StructField("as_of_date", StringType()),
        StructField("level", DoubleType()),
        StructField("slope_10y2y", DoubleType()),
        StructField("slope_10y3m", DoubleType()),
        StructField("curvature_2_5_10", DoubleType()),
        StructField("butterfly_2_10_30", DoubleType()),
        StructField("is_inverted_10y2y", BooleanType()),
        StructField("is_inverted_10y3m", BooleanType()),
        StructField("is_recession", BooleanType()),
        StructField("curve_move", StringType()),
    ]), ["CAST(as_of_date AS DATE) AS as_of_date", "level", "slope_10y2y",
         "slope_10y3m", "curvature_2_5_10", "butterfly_2_10_30",
         "is_inverted_10y2y", "is_inverted_10y3m", "is_recession", "curve_move"])

    # Enriched spread history + inversion episodes: spread legs + USREC.
    leg_ids = sorted(
        {s for sd in spreads for s in (sd.long_leg, sd.short_leg)}
        | {RECESSION_SERIES}
    )
    leg_rows = _collect_latest(config, spark, leg_ids)
    spread_daily = compute_curve_spread_daily(leg_rows, spreads)
    _write("curve_spread_daily", spread_daily, StructType([
        StructField("spread_name", StringType()),
        StructField("observation_date", StringType()),
        StructField("long_leg", StringType()),
        StructField("short_leg", StringType()),
        StructField("value", DoubleType()),
        StructField("value_bps", DoubleType()),
        StructField("zscore", DoubleType()),
        StructField("percentile", DoubleType()),
        StructField("is_inverted", BooleanType()),
        StructField("inversion_run", IntegerType()),
        StructField("is_recession", BooleanType()),
    ]), ["spread_name", "CAST(observation_date AS DATE) AS observation_date",
         "long_leg", "short_leg", "value", "value_bps", "zscore", "percentile",
         "is_inverted", "inversion_run", "is_recession"])
    episodes = compute_spread_inversion_episodes(leg_rows, spreads)
    _write("spread_inversion_episode", episodes, StructType([
        StructField("spread_name", StringType()),
        StructField("long_leg", StringType()),
        StructField("short_leg", StringType()),
        StructField("episode_number", IntegerType()),
        StructField("start_date", StringType()),
        StructField("end_date", StringType()),
        StructField("last_inverted_date", StringType()),
        StructField("observation_count", IntegerType()),
        StructField("calendar_days", IntegerType()),
        StructField("trough_value", DoubleType()),
        StructField("trough_bps", DoubleType()),
        StructField("trough_date", StringType()),
        StructField("is_ongoing", BooleanType()),
        StructField("recession_overlap", BooleanType()),
    ]), ["spread_name", "long_leg", "short_leg", "episode_number",
         "CAST(start_date AS DATE) AS start_date",
         "CAST(end_date AS DATE) AS end_date",
         "CAST(last_inverted_date AS DATE) AS last_inverted_date",
         "observation_count", "calendar_days", "trough_value", "trough_bps",
         "CAST(trough_date AS DATE) AS trough_date", "is_ongoing",
         "recession_overlap"])

    # Phase 4 rates complex: BMRK benchmark board, FUND funding tape + stress
    # gauge, CRDT credit spreads (configs under config/).
    board = load_benchmark_board()
    board_ids = sorted(
        {rd.series_id for rd in board.rates}
        | {rd.benchmark for rd in board.rates if rd.benchmark}
    )
    board_rows = compute_benchmark_rate_board(
        _collect_latest(config, spark, board_ids), board
    )
    _write("benchmark_rate_board", board_rows, StructType([
        StructField("series_id", StringType()),
        StructField("rate_label", StringType()),
        StructField("rate_category", StringType()),
        StructField("benchmark_series", StringType()),
        StructField("as_of_date", StringType()),
        StructField("latest_date", StringType()),
        StructField("latest_value", DoubleType()),
        StructField("prior_value", DoubleType()),
        StructField("change_bps", DoubleType()),
        StructField("trend", StringType()),
        StructField("spread_to_benchmark_bps", DoubleType()),
        StructField("zscore", DoubleType()),
        StructField("percentile", DoubleType()),
        StructField("regime", StringType()),
        StructField("staleness_days", IntegerType()),
    ]), ["series_id", "rate_label", "rate_category", "benchmark_series",
         "CAST(as_of_date AS DATE) AS as_of_date",
         "CAST(latest_date AS DATE) AS latest_date", "latest_value",
         "prior_value", "change_bps", "trend", "spread_to_benchmark_bps",
         "zscore", "percentile", "regime", "staleness_days"])

    funding_cfg = load_funding_config()
    funding_ids = sorted(
        {m.series_id for m in funding_cfg.metrics}
        | {s for sp in funding_cfg.spreads for s in (sp.long_leg, sp.short_leg)}
    )
    funding = compute_funding_features(
        _collect_latest(config, spark, funding_ids), funding_cfg
    )
    _write("funding_tape_daily", funding["tape"], StructType([
        StructField("metric_name", StringType()),
        StructField("metric_type", StringType()),
        StructField("observation_date", StringType()),
        StructField("value", DoubleType()),
        StructField("zscore", DoubleType()),
        StructField("percentile", DoubleType()),
    ]), ["metric_name", "metric_type",
         "CAST(observation_date AS DATE) AS observation_date", "value",
         "zscore", "percentile"])
    _write("funding_stress_daily", funding["stress"], StructType([
        StructField("observation_date", StringType()),
        StructField("composite_z", DoubleType()),
        StructField("stress_score", DoubleType()),
        StructField("stress_bucket", StringType()),
        StructField("n_components", IntegerType()),
    ]), ["CAST(observation_date AS DATE) AS observation_date", "composite_z",
         "stress_score", "stress_bucket", "n_components"])

    credit_cfg = load_credit_config()
    credit_ids = sorted(
        {cd.series_id for cd in credit_cfg.instruments} | {RECESSION_SERIES}
    )
    credit_rows = compute_credit_spread_daily(
        _collect_latest(config, spark, credit_ids), credit_cfg
    )
    _write("credit_spread_daily", credit_rows, StructType([
        StructField("instrument", StringType()),
        StructField("series_id", StringType()),
        StructField("category", StringType()),
        StructField("observation_date", StringType()),
        StructField("oas_pct", DoubleType()),
        StructField("oas_bps", DoubleType()),
        StructField("change_bps", DoubleType()),
        StructField("zscore", DoubleType()),
        StructField("percentile", DoubleType()),
        StructField("is_stress_episode", BooleanType()),
        StructField("is_recession", BooleanType()),
    ]), ["instrument", "series_id", "category",
         "CAST(observation_date AS DATE) AS observation_date", "oas_pct",
         "oas_bps", "change_bps", "zscore", "percentile",
         "is_stress_episode", "is_recession"])

    # Phase 2 Inflation Explorer (config/inflation_items.yml).
    infl_items = load_inflation_items()
    inflation = compute_inflation_explorer(
        _collect_latest(
            config, spark, sorted({i.series_id for i in infl_items})
        ),
        infl_items,
    )
    _write("inflation_explorer", inflation["explorer"], StructType([
        StructField("series_id", StringType()),
        StructField("item_label", StringType()),
        StructField("parent_item", StringType()),
        StructField("hierarchy_level", IntegerType()),
        StructField("basket", StringType()),
        StructField("sa_nsa", StringType()),
        StructField("observation_date", StringType()),
        StructField("index_value", DoubleType()),
        StructField("mom_pct", DoubleType()),
        StructField("yoy_pct", DoubleType()),
        StructField("mom_accel", DoubleType()),
        StructField("yoy_accel", DoubleType()),
        StructField("three_month_annualized", DoubleType()),
        StructField("weight", DoubleType()),
        StructField("contribution_pp", DoubleType()),
    ]), ["series_id", "item_label", "parent_item", "hierarchy_level",
         "basket", "sa_nsa",
         "CAST(observation_date AS DATE) AS observation_date", "index_value",
         "mom_pct", "yoy_pct", "mom_accel", "yoy_accel",
         "three_month_annualized", "weight", "contribution_pp"])
    _write("inflation_contribution", inflation["contribution"], StructType([
        StructField("observation_date", StringType()),
        StructField("basket", StringType()),
        StructField("sa_nsa", StringType()),
        StructField("series_id", StringType()),
        StructField("item_label", StringType()),
        StructField("contribution_pp", DoubleType()),
        StructField("rank_in_month", IntegerType()),
        StructField("is_headline_total", BooleanType()),
    ]), ["CAST(observation_date AS DATE) AS observation_date", "basket",
         "sa_nsa", "series_id", "item_label", "contribution_pp",
         "rank_in_month", "is_headline_total"])


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
    # Cross-series features + source reconciliation are computed in Python (reused
    # by both backends) and written after latest_observation exists.
    _build_cross_series(config, spark)
    results["fred_cross_series_feature"] = "ok"
    _build_cross_series_pit(config, spark)
    results["fred_cross_series_feature_pit"] = "ok"
    _build_source_reconciliation(config, spark)
    results["fred_source_reconciliation"] = "ok"
    _build_company_financials(config, spark)
    results["fred_company_fundamentals"] = "ok"
    results["fred_company_ratios"] = "ok"
    _build_terminal_views(config, spark)
    for name in (
        "dim_series", "dim_date",
        "macro_indicator_dashboard", "macro_indicator_sparkline",
        "macro_category_summary",
        "treasury_curve", "treasury_curve_metrics", "curve_spread_daily",
        "spread_inversion_episode",
        "benchmark_rate_board", "funding_tape_daily", "funding_stress_daily",
        "credit_spread_daily",
        "inflation_explorer", "inflation_contribution",
    ):
        results[name] = "ok"
    return results
