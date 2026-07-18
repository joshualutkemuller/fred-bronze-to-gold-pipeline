"""Spark + Delta integration tests.

These exercise the *actual* Delta code paths (the production backend), which the
pure-Python + SQLite suite cannot. They auto-skip when PySpark/Delta are not
installed, and run in CI's `spark-integration` job (see .github/workflows/ci.yml)
which installs a pinned pyspark + delta-spark on a JDK.

They validate the primitives the SparkWarehouse relies on — an idempotent Delta
MERGE, an append, and the Gold "latest revision" SQL — using two-level table
names (vanilla Spark has no Unity Catalog three-level namespace; that is a
Databricks-only concern).
"""

from __future__ import annotations

import pytest

pyspark = pytest.importorskip("pyspark")
delta = pytest.importorskip("delta")

from delta import configure_spark_with_delta_pip  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402

SILVER_KEYS = ["series_id", "observation_date", "realtime_start"]


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    warehouse = tmp_path_factory.mktemp("warehouse")
    builder = (
        SparkSession.builder.master("local[1]")
        .appName("fred-integration")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()


def _silver_df(spark, rows):
    return spark.createDataFrame(rows)


def test_merge_delta_upserts_idempotently(spark):
    from fred_pipeline.spark_io import merge_delta

    rows = [
        {"series_id": "X", "observation_date": "2024-01-01",
         "realtime_start": "2024-02-01", "value": 1.0},
        {"series_id": "X", "observation_date": "2024-01-02",
         "realtime_start": "2024-02-01", "value": 2.0},
    ]
    df = _silver_df(spark, rows)
    df.write.format("delta").mode("overwrite").saveAsTable("silver_it")

    # Re-merging the same rows must not create duplicates.
    merge_delta(spark, df, "silver_it", SILVER_KEYS)
    assert spark.table("silver_it").count() == 2

    # A changed value updates in place (same key).
    changed = _silver_df(spark, [
        {"series_id": "X", "observation_date": "2024-01-01",
         "realtime_start": "2024-02-01", "value": 9.9},
    ])
    merge_delta(spark, changed, "silver_it", SILVER_KEYS)
    assert spark.table("silver_it").count() == 2
    val = spark.sql(
        "SELECT value FROM silver_it WHERE observation_date = '2024-01-01'"
    ).collect()[0][0]
    assert val == 9.9

    # A new key inserts.
    new = _silver_df(spark, [
        {"series_id": "X", "observation_date": "2024-01-03",
         "realtime_start": "2024-02-01", "value": 3.0},
    ])
    merge_delta(spark, new, "silver_it", SILVER_KEYS)
    assert spark.table("silver_it").count() == 3


def test_append_rows(spark):
    from fred_pipeline.spark_io import append_rows

    n = append_rows(spark, [{"a": 1}, {"a": 2}], "append_it")
    assert n == 2
    assert spark.table("append_it").count() == 2


def test_gold_latest_revision_sql(spark):
    """The Gold 'latest revision per date' logic must pick the newest vintage."""
    rows = [
        {"series_id": "X", "observation_date": "2024-01-01",
         "realtime_start": "2024-02-01", "value": 100.0},
        {"series_id": "X", "observation_date": "2024-01-01",
         "realtime_start": "2024-03-01", "value": 101.5},  # revised, newer
    ]
    _silver_df(spark, rows).write.format("delta").mode("overwrite").saveAsTable(
        "silver_gold_it"
    )
    latest = spark.sql(
        """
        WITH ranked AS (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY series_id, observation_date
                ORDER BY realtime_start DESC
            ) AS rn
            FROM silver_gold_it
        )
        SELECT value FROM ranked WHERE rn = 1
        """
    ).collect()
    assert len(latest) == 1
    assert latest[0][0] == 101.5


def test_feature_transforms_zscore_is_expanding_not_full_sample(spark):
    """gold.fred_feature_transforms.zscore must be point-in-time safe: each
    row's mean/std may only reflect observations at-or-before its own date.
    A whole-partition STDDEV_POP (no ORDER BY / frame) would leak the later
    outlier's magnitude into the z-score of every earlier row — this asserts
    the earliest rows see zero variance (only one/two identical points known
    so far) rather than a value shaped by data from the future."""
    rows = [
        {"series_id": "X", "observation_date": "2024-01-01", "value": 100.0},
        {"series_id": "X", "observation_date": "2024-02-01", "value": 100.0},
        {"series_id": "X", "observation_date": "2024-03-01", "value": 1000.0},  # outlier
    ]
    _silver_df(spark, rows).write.format("delta").mode("overwrite").saveAsTable(
        "latest_zscore_it"
    )
    out = spark.sql(
        """
        WITH base AS (
            SELECT series_id, observation_date, value FROM latest_zscore_it
        ),
        w AS (
            SELECT series_id, observation_date, value,
                AVG(value) OVER (
                    PARTITION BY series_id ORDER BY observation_date
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS mean_v,
                STDDEV_POP(value) OVER (
                    PARTITION BY series_id ORDER BY observation_date
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS std_v
            FROM base
        )
        SELECT observation_date,
            CASE WHEN std_v IS NOT NULL AND std_v <> 0
                 THEN (value - mean_v) / std_v END AS zscore
        FROM w ORDER BY observation_date
        """
    ).collect()

    by_date = {r["observation_date"]: r["zscore"] for r in out}
    # Only one/two identical values known so far -> zero variance -> null,
    # regardless of the huge outlier that arrives later.
    assert by_date["2024-01-01"] is None
    assert by_date["2024-02-01"] is None
    # By the third (outlier) row, variance is nonzero and a real zscore exists.
    assert by_date["2024-03-01"] is not None


def test_revision_stats_sql(spark):
    """gold.fred_revision_stats: first-release vs. latest-revised value,
    revision count, and magnitude — computed from raw Silver (every vintage),
    not latest-revision rows."""
    rows = [
        # revised twice: 100.0 (first print) -> 101.5 -> 99.0 (latest)
        {"series_id": "G", "observation_date": "2024-01-01",
         "realtime_start": "2024-02-01", "value": 100.0, "revision_number": 1,
         "is_missing": False},
        {"series_id": "G", "observation_date": "2024-01-01",
         "realtime_start": "2024-03-01", "value": 101.5, "revision_number": 2,
         "is_missing": False},
        {"series_id": "G", "observation_date": "2024-01-01",
         "realtime_start": "2024-04-01", "value": 99.0, "revision_number": 3,
         "is_missing": False},
        # never revised
        {"series_id": "G", "observation_date": "2024-02-01",
         "realtime_start": "2024-03-15", "value": 102.0, "revision_number": 1,
         "is_missing": False},
    ]
    _silver_df(spark, rows).write.format("delta").mode("overwrite").saveAsTable(
        "silver_revision_it"
    )
    out = spark.sql(
        """
        WITH base AS (
            SELECT series_id, observation_date, realtime_start, value, revision_number
            FROM silver_revision_it
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
            f.value AS first_value, l.value AS latest_value,
            l.value - f.value AS revision_delta,
            CASE WHEN f.value <> 0 THEN (l.value - f.value) / f.value END AS revision_pct
        FROM bounds b
        JOIN base f ON f.series_id = b.series_id AND f.observation_date = b.observation_date
                  AND f.revision_number = b.min_rev
        JOIN base l ON l.series_id = b.series_id AND l.observation_date = b.observation_date
                  AND l.revision_number = b.max_rev
        """
    ).collect()

    by_date = {r["observation_date"]: r for r in out}
    revised = by_date["2024-01-01"]
    assert revised["revision_count"] == 3
    assert revised["first_value"] == 100.0
    assert revised["latest_value"] == 99.0
    assert revised["revision_delta"] == pytest.approx(-1.0)

    unrevised = by_date["2024-02-01"]
    assert unrevised["revision_count"] == 1
    assert unrevised["revision_delta"] == 0.0


def test_curve_spread_sql_supports_ratio_op_with_zero_guard(spark):
    """gold.fred_curve_spread supports config-driven spread ('long - short')
    and ratio ('long / short') ops (see spread_config.load_spread_defs) —
    a zero short leg must drop that row entirely, not divide by zero."""
    rows = [
        {"series_id": "A", "observation_date": "2024-01-01", "value": 10.0, "is_missing": False},
        {"series_id": "B", "observation_date": "2024-01-01", "value": 4.0, "is_missing": False},
        {"series_id": "A", "observation_date": "2024-01-02", "value": 10.0, "is_missing": False},
        {"series_id": "B", "observation_date": "2024-01-02", "value": 0.0, "is_missing": False},
    ]
    _silver_df(spark, rows).write.format("delta").mode("overwrite").saveAsTable(
        "latest_curve_spread_it"
    )
    out = spark.sql(
        """
        SELECT s.spread_name, a.observation_date, s.long_leg, s.short_leg,
               CASE WHEN s.op = 'ratio' THEN a.value / b.value
                    ELSE a.value - b.value END AS value
        FROM (VALUES ('A_OVER_B', 'A', 'B', 'ratio')) AS s(spread_name, long_leg, short_leg, op)
        JOIN latest_curve_spread_it a ON a.series_id = s.long_leg  AND a.is_missing = false
        JOIN latest_curve_spread_it b ON b.series_id = s.short_leg AND b.is_missing = false
                                      AND b.observation_date = a.observation_date
                                      AND (s.op <> 'ratio' OR b.value <> 0)
        """
    ).collect()

    assert len(out) == 1
    assert out[0]["observation_date"] == "2024-01-01"
    assert out[0]["value"] == pytest.approx(2.5)


def test_fomc_tables_build_end_to_end_on_spark(spark, monkeypatch):
    """Unlike the primitive-level tests above, this calls the real
    ``fred_pipeline.writer.gold._build_regime_stats`` (not hand-written
    equivalent SQL) — the two ``gold.fomc_probability`` / ``gold.
    fomc_meeting_path`` ``_write(...)`` calls (StructType + selectExpr casts)
    are new enough, and specific enough to Spark's DataFrame API, that a
    pure-Python unit test of ``compute_fomc_probability`` alone (see
    tests/test_fomc_probability.py) can't catch a schema/cast mismatch.
    ``config.table()`` normally returns 3-level Unity-Catalog names
    (``catalog.schema.table``); vanilla local Spark has no such catalog
    registered, so ``PipelineConfig.catalog`` is monkeypatched to
    ``spark_catalog`` (Spark's built-in default catalog, which *is*
    resolvable), matching how the rest of this module avoids 3-level
    naming."""
    from fred_pipeline.config import Environment, PipelineConfig
    from fred_pipeline.writer.gold import _build_regime_stats

    monkeypatch.setattr(PipelineConfig, "catalog", property(lambda self: "spark_catalog"))
    monkeypatch.setenv("FRED_FOMC_CONFIG_FILE", "config/fomc.yml")
    monkeypatch.setenv("FRED_REGIME_FILE", "/nonexistent/regime.yml")
    monkeypatch.setenv("FRED_STATS_PAIRS_FILE", "/nonexistent/stats_pairs.yml")

    config = PipelineConfig(environment=Environment.DEV, fred_api_key="k")
    spark.sql("CREATE DATABASE IF NOT EXISTS spark_catalog.gold")

    rows = [
        {"series_id": "EFFR", "observation_date": "2026-07-17", "realtime_start": "2026-07-17", "value": 4.33, "is_missing": False},
        {"series_id": "DFEDTARL", "observation_date": "2026-07-17", "realtime_start": "2026-07-17", "value": 4.25, "is_missing": False},
        {"series_id": "DFEDTARU", "observation_date": "2026-07-17", "realtime_start": "2026-07-17", "value": 4.50, "is_missing": False},
        {"series_id": "DGS1MO", "observation_date": "2026-07-17", "realtime_start": "2026-07-17", "value": 4.30, "is_missing": False},
        {"series_id": "DGS3MO", "observation_date": "2026-07-17", "realtime_start": "2026-07-17", "value": 4.10, "is_missing": False},
        {"series_id": "DGS6MO", "observation_date": "2026-07-17", "realtime_start": "2026-07-17", "value": 3.95, "is_missing": False},
        {"series_id": "DGS1", "observation_date": "2026-07-17", "realtime_start": "2026-07-17", "value": 3.70, "is_missing": False},
    ]
    _silver_df(spark, rows).write.format("delta").mode("overwrite").saveAsTable(
        config.table("gold", "fred_latest_observation")
    )

    _build_regime_stats(config, spark)

    prob = spark.sql(
        f"SELECT meeting_date, SUM(probability) AS total "
        f"FROM {config.table('gold', 'fomc_probability')} GROUP BY meeting_date"
    ).collect()
    assert len(prob) == 12  # config/fomc.yml has 12 scheduled meetings
    for row in prob:
        assert row["total"] == pytest.approx(1.0, abs=1e-6)

    path = spark.sql(
        f"SELECT COUNT(*) AS n FROM {config.table('gold', 'fomc_meeting_path')}"
    ).collect()
    assert path[0]["n"] == 12
