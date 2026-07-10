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
CREATE TABLE IF NOT EXISTS gold_fred_revision_stats (
    series_id TEXT, observation_date TEXT, revision_count INTEGER,
    first_value REAL, first_realtime_start TEXT,
    latest_value REAL, latest_realtime_start TEXT,
    revision_delta REAL, revision_pct REAL
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
        from fred_pipeline.features import compute_cross_series_features
        self.conn.execute("DELETE FROM gold_fred_cross_series_feature")
        self._insert("gold_fred_cross_series_feature",
                     compute_cross_series_features(latest))

        # revision stats read raw Silver (every vintage), not latest-revision
        # rows — they exist to measure how much observations get revised.
        self.conn.execute("DELETE FROM gold_fred_revision_stats")
        insert("gold_fred_revision_stats", build_revision_stats(silver))

        self.conn.commit()
        return {k: "ok" for k in (
            "fred_point_in_time", "fred_latest_observation",
            "fred_macro_feature_daily", "fred_feature_transforms",
            "fred_curve_spread", "fred_cross_series_feature", "fred_revision_stats",
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
