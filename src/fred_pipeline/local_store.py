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
* Gold is rebuilt in pure Python (reusing :mod:`fred_pipeline.transform`) rather
  than Spark SQL, keeping exact parity with the tested core functions.
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
from fred_pipeline.transform import daily_feature_matrix, latest_by_observation
from fred_pipeline.warehouse import dq_rows

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
    run_id TEXT, series_id TEXT, endpoint TEXT, request_params TEXT,
    response_payload TEXT, observation_count INTEGER, payload_bytes INTEGER,
    ingested_at TEXT
);
CREATE TABLE IF NOT EXISTS silver_fred_observation (
    series_id TEXT, observation_date TEXT, realtime_start TEXT,
    realtime_end TEXT, value REAL, raw_value TEXT, is_missing INTEGER,
    row_hash TEXT, revision_number INTEGER, ingested_at TEXT, run_id TEXT,
    PRIMARY KEY (series_id, observation_date, realtime_start)
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

    def merge_silver(self, rows: list[dict[str, Any]]) -> int:
        return self._insert(
            "silver_fred_observation",
            rows,
            upsert_keys=["series_id", "observation_date", "realtime_start"],
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

        # daily forward-filled feature matrix
        self.conn.execute("DELETE FROM gold_fred_macro_feature_daily")
        self._insert("gold_fred_macro_feature_daily", daily_feature_matrix(latest))
        self.conn.commit()
        return {k: "ok" for k in
                ("fred_point_in_time", "fred_latest_observation", "fred_macro_feature_daily")}

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
