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
