"""Local SQLite backend — run the whole pipeline on a laptop, no Spark.

``LocalWarehouse`` implements the same :class:`fred_pipeline.warehouse.Warehouse`
surface as the Spark/Delta backend but persists to a single SQLite ``.db`` file.
It is intended for local development, demos, CI, and quickly inspecting results
without a Databricks workspace.

Design notes
------------
* Delta schemas/catalogs don't exist in SQLite, so tables are named
  ``{schema}_{name}`` (e.g. ``silver_fred_observation``) in one file.
* Silver upserts use ``INSERT ... ON CONFLICT`` on the same natural key the
  Delta MERGE uses, so re-runs are idempotent here too.
* Gold is rebuilt with the same semantics as the pure-Python spec functions in
  :mod:`fred_pipeline.transform` / :mod:`fred_pipeline.features`. When
  ``polars`` is installed (``pip install -e ".[local]"``), the vectorized
  implementations in :mod:`fred_pipeline.gold_polars` are used instead — they
  are output-identical (see ``tests/test_gold_polars_parity.py``) but far
  faster once the series universe grows past a few dozen series, since the
  daily feature matrix is a dense ``series x calendar_day`` panel. Falls back
  to the pure-Python versions automatically if polars isn't installed.
"""

from __future__ import annotations

import datetime as _dt
import json
import sqlite3
from typing import Any, Iterable, Optional, Sequence

from fred_pipeline.audit import EtlRun
from fred_pipeline.config import PipelineConfig
from fred_pipeline.manifest import Manifest
from fred_pipeline.meta import build_meta_rows
from fred_pipeline.quality import QualityReport
from fred_pipeline.transform import latest_by_observation
from fred_pipeline.warehouse import dq_rows


def _gold_feature_impls():
    """("polars" | "python", daily_feature_matrix, compute_feature_transforms,
    compute_curve_spreads, compute_revision_stats).

    Prefers the polars-accelerated implementations (output-identical to the
    pure-Python spec — see tests/test_gold_polars_parity.py) when polars is
    installed; falls back to the pure-Python versions otherwise, exactly as
    Spark is an optional, lazily-imported dependency elsewhere in this repo.
    In "polars" mode the functions return DataFrames (see
    ``LocalWarehouse._insert_frame``); in "python" mode they return
    ``list[dict]`` (see ``LocalWarehouse._insert``).
    """
    try:
        from fred_pipeline.gold_polars import (
            compute_curve_spreads_frame,
            compute_feature_transforms_frame,
            compute_revision_stats_frame,
            daily_feature_matrix_frame,
        )

        return (
            "polars", daily_feature_matrix_frame, compute_feature_transforms_frame,
            compute_curve_spreads_frame, compute_revision_stats_frame,
        )
    except ImportError:
        from fred_pipeline.features import (
            compute_curve_spreads,
            compute_feature_transforms,
            compute_revision_stats,
        )
        from fred_pipeline.transform import daily_feature_matrix

        return (
            "python", daily_feature_matrix, compute_feature_transforms,
            compute_curve_spreads, compute_revision_stats,
        )


# DDL for the SQLite mirror of the Delta tables. Kept in one place for clarity.
_SCHEMA = """
CREATE TABLE IF NOT EXISTS meta_fred_series (
    series_id TEXT PRIMARY KEY, title TEXT, category TEXT, frequency TEXT,
    units TEXT, active INTEGER, load_type TEXT, expected_update_frequency TEXT,
    vintage_enabled INTEGER, validation_profile TEXT, business_owner TEXT,
    technical_owner TEXT, downstream_use_case TEXT, priority INTEGER,
    restate_records INTEGER, min_value REAL, max_value REAL,
    tags TEXT, updated_at TEXT
);
CREATE TABLE IF NOT EXISTS meta_fred_manifest (
    manifest_name TEXT PRIMARY KEY, description TEXT, version INTEGER,
    source_path TEXT, series_count INTEGER, loaded_at TEXT
);
CREATE TABLE IF NOT EXISTS meta_fred_series_manifest_map (
    series_id TEXT, manifest_name TEXT, updated_at TEXT,
    PRIMARY KEY (series_id, manifest_name)
);
CREATE TABLE IF NOT EXISTS bronze_fred_api_response (
    run_id TEXT, source TEXT NOT NULL DEFAULT 'fred', series_id TEXT,
    endpoint TEXT, request_params TEXT,
    response_payload TEXT, observation_count INTEGER, payload_bytes INTEGER,
    ingested_at TEXT
);
CREATE TABLE IF NOT EXISTS silver_fred_observation (
    source TEXT NOT NULL DEFAULT 'fred',
    series_id TEXT, observation_date TEXT, realtime_start TEXT,
    realtime_end TEXT, value REAL, raw_value TEXT, is_missing INTEGER,
    row_hash TEXT, revision_number INTEGER, ingested_at TEXT, run_id TEXT,
    PRIMARY KEY (source, series_id, observation_date, realtime_start)
);
CREATE TABLE IF NOT EXISTS gold_fred_latest_observation (
    series_id TEXT, observation_date TEXT, value REAL, realtime_start TEXT,
    realtime_end TEXT, is_missing INTEGER, revision_number INTEGER, ingested_at TEXT
);
CREATE TABLE IF NOT EXISTS gold_fred_point_in_time (
    series_id TEXT, observation_date TEXT, realtime_start TEXT, realtime_end TEXT,
    value REAL, revision_number INTEGER, is_missing INTEGER, ingested_at TEXT
);
CREATE TABLE IF NOT EXISTS gold_fred_macro_feature_daily (
    as_of_date TEXT, series_id TEXT, raw_value REAL, value REAL
);
CREATE TABLE IF NOT EXISTS gold_fred_feature_transforms (
    series_id TEXT, observation_date TEXT, value REAL,
    mom REAL, diff REAL, yoy REAL, zscore REAL
);
CREATE TABLE IF NOT EXISTS gold_fred_curve_spread (
    spread_name TEXT, observation_date TEXT, long_leg TEXT, short_leg TEXT, value REAL
);
CREATE TABLE IF NOT EXISTS gold_fred_cross_series_feature (
    feature_name TEXT, op TEXT, observation_date TEXT, value REAL
);
CREATE TABLE IF NOT EXISTS gold_fred_cross_series_feature_pit (
    feature_name TEXT, op TEXT, observation_date TEXT, value REAL, basis TEXT
);
CREATE TABLE IF NOT EXISTS gold_fred_source_reconciliation (
    name TEXT, observation_date TEXT, series_a TEXT, value_a REAL,
    series_b TEXT, value_b REAL, abs_diff REAL, pct_diff REAL, diverged INTEGER
);
CREATE TABLE IF NOT EXISTS gold_fred_company_fundamentals (
    cik TEXT, concept TEXT, statement TEXT, observation_date TEXT, value REAL
);
CREATE TABLE IF NOT EXISTS gold_fred_company_ratios (
    cik TEXT, ratio_name TEXT, observation_date TEXT, value REAL
);
CREATE TABLE IF NOT EXISTS gold_fred_revision_stats (
    series_id TEXT, observation_date TEXT, revision_count INTEGER,
    first_value REAL, first_realtime_start TEXT,
    latest_value REAL, latest_realtime_start TEXT,
    revision_delta REAL, revision_pct REAL
);

-- Market-terminal analytical views (docs/market_terminal_gold_views.md):
-- star-schema dimensions + the ECON macro dashboard + the Treasury Curve Lab,
-- shaped for Power BI. Built by fred_pipeline.terminal_views (pure Python,
-- shared with the Spark backend).
CREATE TABLE IF NOT EXISTS gold_dim_series (
    series_id TEXT PRIMARY KEY, title TEXT, source TEXT, frequency TEXT,
    units TEXT, econ_category TEXT, polarity INTEGER, default_transform TEXT,
    scale TEXT, decimals INTEGER, notes TEXT
);
CREATE TABLE IF NOT EXISTS gold_dim_date (
    date TEXT PRIMARY KEY, year INTEGER, quarter INTEGER, month INTEGER,
    month_name TEXT, is_month_end INTEGER, fiscal_year INTEGER,
    is_recession INTEGER
);
CREATE TABLE IF NOT EXISTS gold_macro_indicator_dashboard (
    series_id TEXT, econ_category TEXT, polarity INTEGER,
    default_transform TEXT, as_of_date TEXT, latest_date TEXT,
    latest_value REAL, prior_date TEXT, prior_value REAL, change_abs REAL,
    change_pct REAL, yoy_pct REAL, zscore REAL, percentile REAL,
    surprise REAL, surprise_z REAL, direction_is_good INTEGER,
    spark_min REAL, spark_max REAL, staleness_days INTEGER, realtime_start TEXT
);
CREATE TABLE IF NOT EXISTS gold_macro_indicator_sparkline (
    series_id TEXT, point_index INTEGER, observation_date TEXT, value REAL
);
CREATE TABLE IF NOT EXISTS gold_macro_category_summary (
    econ_category TEXT, as_of_date TEXT, n_series INTEGER,
    n_improving INTEGER, n_deteriorating INTEGER, breadth_pct REAL,
    avg_zscore REAL, surprise_index REAL
);
CREATE TABLE IF NOT EXISTS gold_treasury_curve (
    as_of_date TEXT, tenor_label TEXT, tenor_months INTEGER,
    series_id TEXT, yield_pct REAL
);
CREATE TABLE IF NOT EXISTS gold_treasury_curve_metrics (
    as_of_date TEXT, level REAL, slope_10y2y REAL, slope_10y3m REAL,
    curvature_2_5_10 REAL, butterfly_2_10_30 REAL,
    is_inverted_10y2y INTEGER, is_inverted_10y3m INTEGER,
    is_recession INTEGER, curve_move TEXT
);
CREATE TABLE IF NOT EXISTS gold_curve_spread_daily (
    spread_name TEXT, observation_date TEXT, long_leg TEXT, short_leg TEXT,
    value REAL, value_bps REAL, zscore REAL, percentile REAL,
    is_inverted INTEGER, inversion_run INTEGER, is_recession INTEGER
);
CREATE TABLE IF NOT EXISTS gold_spread_inversion_episode (
    spread_name TEXT, long_leg TEXT, short_leg TEXT, episode_number INTEGER,
    start_date TEXT, end_date TEXT, last_inverted_date TEXT,
    observation_count INTEGER, calendar_days INTEGER,
    trough_value REAL, trough_bps REAL, trough_date TEXT,
    is_ongoing INTEGER, recession_overlap INTEGER
);
CREATE TABLE IF NOT EXISTS gold_benchmark_rate_board (
    series_id TEXT, rate_label TEXT, rate_category TEXT,
    benchmark_series TEXT, as_of_date TEXT, latest_date TEXT,
    latest_value REAL, prior_value REAL, change_bps REAL, trend TEXT,
    spread_to_benchmark_bps REAL, zscore REAL, percentile REAL,
    regime TEXT, staleness_days INTEGER
);
CREATE TABLE IF NOT EXISTS gold_funding_tape_daily (
    metric_name TEXT, metric_type TEXT, observation_date TEXT,
    value REAL, zscore REAL, percentile REAL
);
CREATE TABLE IF NOT EXISTS gold_funding_stress_daily (
    observation_date TEXT, composite_z REAL, stress_score REAL,
    stress_bucket TEXT, n_components INTEGER
);
CREATE TABLE IF NOT EXISTS gold_credit_spread_daily (
    instrument TEXT, series_id TEXT, category TEXT, observation_date TEXT,
    oas_pct REAL, oas_bps REAL, change_bps REAL, zscore REAL,
    percentile REAL, is_stress_episode INTEGER, is_recession INTEGER
);
CREATE TABLE IF NOT EXISTS gold_inflation_explorer (
    series_id TEXT, item_label TEXT, parent_item TEXT,
    hierarchy_level INTEGER, basket TEXT, sa_nsa TEXT, observation_date TEXT,
    index_value REAL, mom_pct REAL, yoy_pct REAL, mom_accel REAL,
    yoy_accel REAL, three_month_annualized REAL, weight REAL,
    contribution_pp REAL
);
CREATE TABLE IF NOT EXISTS gold_inflation_contribution (
    observation_date TEXT, basket TEXT, sa_nsa TEXT, series_id TEXT,
    item_label TEXT, contribution_pp REAL, rank_in_month INTEGER,
    is_headline_total INTEGER
);
CREATE TABLE IF NOT EXISTS gold_curve_spread_rolling (
    spread_name TEXT, observation_date TEXT, window INTEGER,
    value REAL, change REAL, pct_change REAL, zscore REAL
);
CREATE TABLE IF NOT EXISTS gold_credit_spread_rolling (
    instrument TEXT, series_id TEXT, observation_date TEXT, window INTEGER,
    oas_bps REAL, change_bps REAL, pct_change REAL, zscore REAL
);
CREATE TABLE IF NOT EXISTS gold_treasury_curve_rolling (
    tenor_label TEXT, tenor_months INTEGER, series_id TEXT,
    observation_date TEXT, window INTEGER,
    yield_pct REAL, change REAL, pct_change REAL, zscore REAL
);
CREATE TABLE IF NOT EXISTS gold_macro_regime_daily (
    observation_date TEXT, growth_score REAL, inflation_score REAL,
    liquidity_score REAL, credit_score REAL, policy_score REAL,
    composite_score REAL, regime_name TEXT, regime_confidence REAL
);
CREATE TABLE IF NOT EXISTS gold_series_correlation (
    series_a TEXT, series_b TEXT, transform_a TEXT, transform_b TEXT,
    window INTEGER, observation_date TEXT, correlation REAL, n_obs INTEGER
);
CREATE TABLE IF NOT EXISTS gold_series_lead_lag (
    series_a TEXT, series_b TEXT, transform_a TEXT, transform_b TEXT,
    lag INTEGER, cross_correlation REAL, n_obs INTEGER, best_lag INTEGER,
    granger_f_ab REAL, granger_p_ab REAL, granger_f_ba REAL,
    granger_p_ba REAL, as_of_date TEXT
);
CREATE TABLE IF NOT EXISTS gold_global_inflation (
    country TEXT, iso3 TEXT, region TEXT, series_id TEXT,
    observation_date TEXT, cpi_yoy_pct REAL, change_pp REAL, trend TEXT,
    streak INTEGER, target_pct REAL, vs_target_pp REAL
);
CREATE TABLE IF NOT EXISTS gold_global_policy_rates (
    country TEXT, iso3 TEXT, region TEXT, series_id TEXT,
    observation_date TEXT, policy_rate_pct REAL, change_bps REAL,
    last_move_bps REAL, stance TEXT, real_rate_pct REAL
);
CREATE TABLE IF NOT EXISTS gold_powerbi_catalog (
    object_name TEXT PRIMARY KEY, object_type TEXT, module TEXT,
    grain TEXT, intended_visual TEXT, description TEXT
);
CREATE TABLE IF NOT EXISTS audit_etl_run (
    run_id TEXT PRIMARY KEY, environment TEXT, manifest_path TEXT,
    triggered_by TEXT, status TEXT, started_at TEXT, ended_at TEXT,
    duration_seconds REAL, series_total INTEGER, series_succeeded INTEGER,
    series_failed INTEGER, error_message TEXT
);
CREATE TABLE IF NOT EXISTS audit_etl_series_run (
    run_id TEXT, series_id TEXT, status TEXT, load_type TEXT, started_at TEXT,
    ended_at TEXT, duration_seconds REAL, observations_extracted INTEGER,
    rows_written_bronze INTEGER, rows_merged_silver INTEGER, dq_passed INTEGER,
    error_message TEXT
);
CREATE TABLE IF NOT EXISTS audit_data_quality_result (
    run_id TEXT, series_id TEXT, check_name TEXT, passed INTEGER,
    severity TEXT, message TEXT, metric_value REAL
);
CREATE TABLE IF NOT EXISTS meta_fred_series_lifecycle (
    series_id TEXT, fred_title TEXT, fred_frequency TEXT, fred_units TEXT,
    seasonal_adjustment TEXT, observation_start TEXT, observation_end TEXT,
    last_updated TEXT, popularity INTEGER, discontinued INTEGER,
    days_since_last_observation INTEGER, is_stale INTEGER, checked_at TEXT
);
CREATE TABLE IF NOT EXISTS meta_fred_series_drift (
    series_id TEXT, field TEXT, manifest_value TEXT, fred_value TEXT,
    kind TEXT, severity TEXT, detected_at TEXT
);

-- SQLite equivalents of the Delta-only gold.v_* views in sql/60_views.sql
-- (SQLite has no CREATE OR REPLACE VIEW, so these use IF NOT EXISTS and are
-- expected to stay in sync with that file by hand).
CREATE VIEW IF NOT EXISTS gold_v_latest_revised AS
WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY series_id, observation_date
        ORDER BY realtime_start DESC
    ) AS rn
    FROM silver_fred_observation
)
SELECT series_id, observation_date, value, realtime_start, realtime_end,
       is_missing, revision_number, ingested_at
FROM ranked
WHERE rn = 1;

CREATE VIEW IF NOT EXISTS gold_v_point_in_time AS
SELECT series_id, observation_date, realtime_start, realtime_end, value,
       revision_number, is_missing, ingested_at
FROM silver_fred_observation;

CREATE VIEW IF NOT EXISTS gold_v_series_latest_value AS
WITH latest AS (
    SELECT series_id, observation_date, value,
        ROW_NUMBER() OVER (
            PARTITION BY series_id ORDER BY observation_date DESC
        ) AS rn
    FROM gold_v_latest_revised
    WHERE is_missing = false
)
SELECT series_id, observation_date, value
FROM latest
WHERE rn = 1;

CREATE VIEW IF NOT EXISTS gold_v_series_revision_summary AS
SELECT series_id,
    COUNT(*)               AS observation_count,
    AVG(revision_count)    AS avg_revision_count,
    MAX(revision_count)    AS max_revision_count,
    AVG(ABS(revision_pct)) AS avg_abs_revision_pct,
    MAX(ABS(revision_pct)) AS max_abs_revision_pct
FROM gold_fred_revision_stats
GROUP BY series_id;

-- Multi-source coverage & freshness dashboard: latest observation, count, and a
-- staleness verdict per (source, series_id), using the manifest cadence from
-- meta. Mirrors gold.v_source_coverage in sql/60_views.sql.
CREATE VIEW IF NOT EXISTS gold_v_source_coverage AS
WITH per_series AS (
    SELECT source, series_id,
           MAX(observation_date)            AS latest_observation_date,
           COUNT(DISTINCT observation_date) AS observation_count
    FROM silver_fred_observation
    GROUP BY source, series_id
),
aged AS (
    SELECT p.source, p.series_id, m.category, m.frequency,
           p.latest_observation_date, p.observation_count,
           CAST(julianday('now') - julianday(p.latest_observation_date) AS INTEGER)
               AS days_since_last
    FROM per_series p
    LEFT JOIN meta_fred_series m ON m.series_id = p.series_id
)
SELECT source, series_id, category, frequency, latest_observation_date,
       observation_count, days_since_last,
       CASE
         WHEN frequency IN ('d','daily')       AND days_since_last > 10  THEN 1
         WHEN frequency IN ('w','weekly')      AND days_since_last > 21  THEN 1
         WHEN frequency IN ('bw','biweekly')   AND days_since_last > 30  THEN 1
         WHEN frequency IN ('m','monthly')     AND days_since_last > 75  THEN 1
         WHEN frequency IN ('q','quarterly')   AND days_since_last > 200 THEN 1
         WHEN frequency IN ('sa','semiannual') AND days_since_last > 380 THEN 1
         WHEN frequency IN ('a','annual')      AND days_since_last > 550 THEN 1
         ELSE 0
       END AS is_stale
FROM aged;

-- Cross-company ranks/percentiles of each SEC-derived ratio within each period.
-- Mirrors gold.v_company_ratio_ranks in sql/60_views.sql.
CREATE VIEW IF NOT EXISTS gold_v_company_ratio_ranks AS
SELECT cik, ratio_name, observation_date, value,
       PERCENT_RANK() OVER (
           PARTITION BY ratio_name, observation_date ORDER BY value
       ) AS pct_rank,
       ROW_NUMBER() OVER (
           PARTITION BY ratio_name, observation_date ORDER BY value DESC
       ) AS rank_desc
FROM gold_fred_company_ratios;
"""


def _encode(value: Any) -> Any:
    """Coerce a Python value into something SQLite can store."""
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (_dt.datetime, _dt.date)):
        return value.isoformat()
    if isinstance(value, (list, dict)):
        return json.dumps(value)
    return value


class LocalWarehouse:
    """A SQLite-file implementation of the Warehouse protocol."""

    def __init__(self, config: PipelineConfig, db_path: str = "fred_local.db"):
        self.config = config
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(_SCHEMA)
        self.conn.commit()

    # ---- low-level helpers ---------------------------------------------

    def _insert(
        self,
        table: str,
        rows: Sequence[dict[str, Any]],
        upsert_keys: Optional[Sequence[str]] = None,
    ) -> int:
        if not rows:
            return 0
        cols = list(rows[0].keys())
        collist = ", ".join(cols)
        placeholders = ", ".join(["?"] * len(cols))
        sql = f"INSERT INTO {table} ({collist}) VALUES ({placeholders})"
        if upsert_keys:
            updates = ", ".join(
                f"{c}=excluded.{c}" for c in cols if c not in upsert_keys
            )
            conflict = ", ".join(upsert_keys)
            sql += f" ON CONFLICT({conflict}) DO UPDATE SET {updates}"
        data = [tuple(_encode(r.get(c)) for c in cols) for r in rows]
        self.conn.executemany(sql, data)
        self.conn.commit()
        return len(rows)

    def _insert_frame(
        self,
        table: str,
        df: Any,
        upsert_keys: Optional[Sequence[str]] = None,
    ) -> int:
        """Like :meth:`_insert` but for a polars DataFrame.

        Inserts straight from ``df.iter_rows()`` (plain tuples), skipping the
        per-row dict allocation ``_insert`` does — that dict-building step is
        what dominates wall-clock time at large row counts (see the
        gold_polars module docstring). Callers must ensure the DataFrame has
        no bool/date/datetime columns (cast to Utf8/int first), since this
        path skips ``_encode``.
        """
        if df.is_empty():
            return 0
        cols = df.columns
        collist = ", ".join(cols)
        placeholders = ", ".join(["?"] * len(cols))
        sql = f"INSERT INTO {table} ({collist}) VALUES ({placeholders})"
        if upsert_keys:
            updates = ", ".join(
                f"{c}=excluded.{c}" for c in cols if c not in upsert_keys
            )
            conflict = ", ".join(upsert_keys)
            sql += f" ON CONFLICT({conflict}) DO UPDATE SET {updates}"
        n = df.height
        self.conn.executemany(sql, df.iter_rows())
        self.conn.commit()
        return n

    def _read(self, table: str) -> list[dict[str, Any]]:
        cur = self.conn.execute(f"SELECT * FROM {table}")
        return [dict(row) for row in cur.fetchall()]

    # ---- Warehouse surface ---------------------------------------------

    def sync_meta(self, manifests: Iterable[Manifest]) -> dict[str, int]:
        rows = build_meta_rows(list(manifests))
        counts = {}
        counts["fred_series"] = self._insert(
            "meta_fred_series", rows["fred_series"], upsert_keys=["series_id"]
        )
        counts["fred_manifest"] = self._insert(
            "meta_fred_manifest", rows["fred_manifest"], upsert_keys=["manifest_name"]
        )
        counts["fred_series_manifest_map"] = self._insert(
            "meta_fred_series_manifest_map",
            rows["fred_series_manifest_map"],
            upsert_keys=["series_id", "manifest_name"],
        )
        return counts

    def restate_start(self, series_id: str, n: int) -> Optional[str]:
        """Earliest observation_date among the N most recent for this series.

        Returns ``None`` when the series has no rows yet (→ full load).
        """
        cur = self.conn.execute(
            """
            SELECT MIN(observation_date) FROM (
                SELECT DISTINCT observation_date FROM silver_fred_observation
                WHERE series_id = ?
                ORDER BY observation_date DESC
                LIMIT ?
            )
            """,
            (series_id, int(n)),
        )
        row = cur.fetchone()
        return row[0] if row else None

    def write_bronze(self, rows: list[dict[str, Any]]) -> int:
        return self._insert("bronze_fred_api_response", rows)

    def read_bronze(
        self, series_ids: Optional[list[str]] = None
    ) -> list[dict[str, Any]]:
        sql = ("SELECT source, series_id, response_payload, run_id, ingested_at "
               "FROM bronze_fred_api_response")
        params: tuple = ()
        if series_ids:
            placeholders = ", ".join("?" * len(series_ids))
            sql += f" WHERE series_id IN ({placeholders})"
            params = tuple(series_ids)
        sql += " ORDER BY ingested_at"
        return self.query(sql, params)

    def merge_silver(self, rows: list[dict[str, Any]]) -> int:
        return self._insert(
            "silver_fred_observation",
            rows,
            upsert_keys=["source", "series_id", "observation_date", "realtime_start"],
        )

    def build_gold(self) -> dict[str, str]:
        silver = self._read("silver_fred_observation")
        for r in silver:
            r["is_missing"] = bool(r.get("is_missing"))

        # point-in-time = every vintage row
        self.conn.execute("DELETE FROM gold_fred_point_in_time")
        pit = [
            {
                "series_id": r["series_id"],
                "observation_date": r["observation_date"],
                "realtime_start": r["realtime_start"],
                "realtime_end": r["realtime_end"],
                "value": r["value"],
                "revision_number": r["revision_number"],
                "is_missing": r["is_missing"],
                "ingested_at": r["ingested_at"],
            }
            for r in silver
        ]
        self._insert("gold_fred_point_in_time", pit)

        # latest revision per (series, date)
        latest = latest_by_observation(silver)
        self.conn.execute("DELETE FROM gold_fred_latest_observation")
        latest_rows = [
            {
                "series_id": r["series_id"],
                "observation_date": r["observation_date"],
                "value": r["value"],
                "realtime_start": r["realtime_start"],
                "realtime_end": r["realtime_end"],
                "is_missing": r["is_missing"],
                "revision_number": r.get("revision_number"),
                "ingested_at": r["ingested_at"],
            }
            for r in latest
        ]
        self._insert("gold_fred_latest_observation", latest_rows)

        # daily forward-filled feature matrix; quant transforms (mom/yoy/diff/
        # zscore); curve spreads; revision-magnitude stats. Prefers the
        # polars-accelerated versions (output-identical, see gold_polars
        # module docstring) when available, inserting straight from the
        # DataFrame to skip Python dict overhead.
        mode, build_daily_matrix, build_transforms, build_spreads, build_revision_stats = (
            _gold_feature_impls()
        )
        insert = self._insert_frame if mode == "polars" else self._insert

        self.conn.execute("DELETE FROM gold_fred_macro_feature_daily")
        insert("gold_fred_macro_feature_daily", build_daily_matrix(latest))

        self.conn.execute("DELETE FROM gold_fred_feature_transforms")
        insert("gold_fred_feature_transforms", build_transforms(latest))
        self.conn.execute("DELETE FROM gold_fred_curve_spread")
        insert("gold_fred_curve_spread", build_spreads(latest))

        # cross-series features (frequency-aware, N-leg): a small output, so use
        # the pure-Python reference directly (same function the Spark path reuses).
        from fred_pipeline.features import (
            compute_cross_series_features,
            compute_cross_series_features_pit,
            compute_source_reconciliation,
        )
        self.conn.execute("DELETE FROM gold_fred_cross_series_feature")
        self._insert("gold_fred_cross_series_feature",
                     compute_cross_series_features(latest))
        # Point-in-time (as-first-reported) variant: leak-free, reads raw Silver
        # (all vintages), not latest-revision rows.
        self.conn.execute("DELETE FROM gold_fred_cross_series_feature_pit")
        self._insert("gold_fred_cross_series_feature_pit",
                     compute_cross_series_features_pit(silver))
        self.conn.execute("DELETE FROM gold_fred_source_reconciliation")
        self._insert("gold_fred_source_reconciliation",
                     compute_source_reconciliation(latest))

        # SEC company financials: standardize raw XBRL tags into canonical line
        # items, then derived ratios (reads raw Silver for source='sec' rows).
        from fred_pipeline.sec_standardization import (
            compute_sec_ratios,
            standardize_sec_statements,
        )
        fundamentals = standardize_sec_statements(silver)
        self.conn.execute("DELETE FROM gold_fred_company_fundamentals")
        self._insert("gold_fred_company_fundamentals", fundamentals)
        self.conn.execute("DELETE FROM gold_fred_company_ratios")
        self._insert("gold_fred_company_ratios", compute_sec_ratios(fundamentals))

        # revision stats read raw Silver (every vintage), not latest-revision
        # rows — they exist to measure how much observations get revised.
        self.conn.execute("DELETE FROM gold_fred_revision_stats")
        insert("gold_fred_revision_stats", build_revision_stats(silver))

        # Market-terminal analytical views (docs/market_terminal_gold_views.md):
        # dimensions, the ECON macro dashboard, and the Treasury Curve Lab.
        # All pure-Python engines shared verbatim with the Spark backend.
        from fred_pipeline.terminal_views import (
            build_dim_date,
            build_dim_series,
            compute_benchmark_rate_board,
            compute_credit_spread_daily,
            compute_credit_spread_rolling,
            compute_curve_spread_daily,
            compute_curve_spread_rolling,
            compute_funding_features,
            compute_inflation_explorer,
            compute_macro_dashboard,
            compute_spread_inversion_episodes,
            compute_treasury_curve,
            compute_treasury_curve_rolling,
        )
        meta_rows = self.query(
            "SELECT series_id, title, frequency, units FROM meta_fred_series"
        )
        self.conn.execute("DELETE FROM gold_dim_series")
        self._insert("gold_dim_series", build_dim_series(meta_rows=meta_rows))

        obs_dates = [
            r["observation_date"] for r in latest
            if not r["is_missing"] and r.get("observation_date")
        ]
        usrec = [r for r in latest if r["series_id"] == "USREC"]
        self.conn.execute("DELETE FROM gold_dim_date")
        if obs_dates:
            self._insert(
                "gold_dim_date",
                build_dim_date(min(obs_dates), max(obs_dates), usrec),
            )

        dash = compute_macro_dashboard(latest)
        self.conn.execute("DELETE FROM gold_macro_indicator_dashboard")
        self._insert("gold_macro_indicator_dashboard", dash["dashboard"])
        self.conn.execute("DELETE FROM gold_macro_indicator_sparkline")
        self._insert("gold_macro_indicator_sparkline", dash["sparkline"])
        self.conn.execute("DELETE FROM gold_macro_category_summary")
        self._insert("gold_macro_category_summary", dash["category_summary"])

        curve = compute_treasury_curve(latest)
        self.conn.execute("DELETE FROM gold_treasury_curve")
        self._insert("gold_treasury_curve", curve["curve"])
        self.conn.execute("DELETE FROM gold_treasury_curve_metrics")
        self._insert("gold_treasury_curve_metrics", curve["metrics"])
        self.conn.execute("DELETE FROM gold_curve_spread_daily")
        self._insert("gold_curve_spread_daily", compute_curve_spread_daily(latest))
        self.conn.execute("DELETE FROM gold_spread_inversion_episode")
        self._insert("gold_spread_inversion_episode",
                     compute_spread_inversion_episodes(latest))

        # Phase 4 rates complex: BMRK benchmark board, FUND funding tape +
        # stress gauge, CRDT credit spreads (configs under config/).
        self.conn.execute("DELETE FROM gold_benchmark_rate_board")
        self._insert("gold_benchmark_rate_board",
                     compute_benchmark_rate_board(latest))
        funding = compute_funding_features(latest)
        self.conn.execute("DELETE FROM gold_funding_tape_daily")
        self._insert("gold_funding_tape_daily", funding["tape"])
        self.conn.execute("DELETE FROM gold_funding_stress_daily")
        self._insert("gold_funding_stress_daily", funding["stress"])
        self.conn.execute("DELETE FROM gold_credit_spread_daily")
        self._insert("gold_credit_spread_daily",
                     compute_credit_spread_daily(latest))

        # Phase 2 Inflation Explorer (config/inflation_items.yml).
        inflation = compute_inflation_explorer(latest)
        self.conn.execute("DELETE FROM gold_inflation_explorer")
        self._insert("gold_inflation_explorer", inflation["explorer"])
        self.conn.execute("DELETE FROM gold_inflation_contribution")
        self._insert("gold_inflation_contribution", inflation["contribution"])

        # Rolling-window stats companions (windows 1/5/10/21/63/126/252 obs)
        # for the spread, credit, and curve daily tables.
        self.conn.execute("DELETE FROM gold_curve_spread_rolling")
        self._insert("gold_curve_spread_rolling",
                     compute_curve_spread_rolling(latest))
        self.conn.execute("DELETE FROM gold_credit_spread_rolling")
        self._insert("gold_credit_spread_rolling",
                     compute_credit_spread_rolling(latest))
        self.conn.execute("DELETE FROM gold_treasury_curve_rolling")
        self._insert("gold_treasury_curve_rolling",
                     compute_treasury_curve_rolling(latest))

        # Phase 5: regime playbook + statistical lab (config/regime.yml,
        # config/stats_pairs.yml).
        from fred_pipeline.regime_stats import (
            compute_macro_regime,
            compute_series_correlation,
            compute_series_lead_lag,
        )
        self.conn.execute("DELETE FROM gold_macro_regime_daily")
        self._insert("gold_macro_regime_daily", compute_macro_regime(latest))
        self.conn.execute("DELETE FROM gold_series_correlation")
        self._insert("gold_series_correlation",
                     compute_series_correlation(latest))
        self.conn.execute("DELETE FROM gold_series_lead_lag")
        self._insert("gold_series_lead_lag", compute_series_lead_lag(latest))

        # Phase 6: global inflation / policy rates + the Power BI catalog
        # (config/global_series.yml; catalog from global_views.POWERBI_CATALOG).
        from fred_pipeline.global_views import (
            compute_global_inflation,
            compute_global_policy_rates,
            powerbi_catalog_rows,
        )
        self.conn.execute("DELETE FROM gold_global_inflation")
        self._insert("gold_global_inflation", compute_global_inflation(latest))
        self.conn.execute("DELETE FROM gold_global_policy_rates")
        self._insert("gold_global_policy_rates",
                     compute_global_policy_rates(latest))
        self.conn.execute("DELETE FROM gold_powerbi_catalog")
        self._insert("gold_powerbi_catalog", powerbi_catalog_rows())

        self.conn.commit()
        return {k: "ok" for k in (
            "fred_point_in_time", "fred_latest_observation",
            "fred_macro_feature_daily", "fred_feature_transforms",
            "fred_curve_spread", "fred_cross_series_feature",
            "fred_cross_series_feature_pit", "fred_source_reconciliation",
            "fred_company_fundamentals", "fred_company_ratios",
            "fred_revision_stats",
            "dim_series", "dim_date",
            "macro_indicator_dashboard", "macro_indicator_sparkline",
            "macro_category_summary",
            "treasury_curve", "treasury_curve_metrics", "curve_spread_daily",
            "spread_inversion_episode",
            "benchmark_rate_board", "funding_tape_daily",
            "funding_stress_daily", "credit_spread_daily",
            "inflation_explorer", "inflation_contribution",
            "curve_spread_rolling", "credit_spread_rolling",
            "treasury_curve_rolling",
            "macro_regime_daily", "series_correlation", "series_lead_lag",
            "global_inflation", "global_policy_rates", "powerbi_catalog",
        )}

    def point_in_time_features(self, as_of: str) -> list[dict[str, Any]]:
        """Each series' value as known on ``as_of`` (leakage-free snapshot)."""
        from fred_pipeline.features import point_in_time_snapshot

        silver = self._read("silver_fred_observation")
        for r in silver:
            r["is_missing"] = bool(r.get("is_missing"))
        return point_in_time_snapshot(silver, as_of)

    def write_lifecycle(self, rows: list[dict[str, Any]]) -> int:
        return self._insert("meta_fred_series_lifecycle", rows)

    def write_drift(self, rows: list[dict[str, Any]]) -> int:
        return self._insert("meta_fred_series_drift", rows)

    def persist_run(self, run: EtlRun) -> None:
        self._insert("audit_etl_run", [run.to_row()], upsert_keys=["run_id"])
        self._insert("audit_etl_series_run", [s.to_row() for s in run.series_runs])

    def persist_dq(self, run_id: str, report: QualityReport) -> None:
        self._insert("audit_data_quality_result", dq_rows(run_id, report))

    def close(self) -> None:
        self.conn.close()

    # ---- convenience for interactive/local use -------------------------

    def query(self, sql: str, params: Sequence[Any] = ()) -> list[dict[str, Any]]:
        """Run an ad-hoc SQL query and return rows as dicts."""
        cur = self.conn.execute(sql, params)
        return [dict(row) for row in cur.fetchall()]

    def tables(self) -> list[str]:
        cur = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        return [row[0] for row in cur.fetchall()]
