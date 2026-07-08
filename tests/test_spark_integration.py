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
