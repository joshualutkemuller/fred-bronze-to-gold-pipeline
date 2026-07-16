"""Point-in-time historical backfill engine.

For each snapshot date D in a date range, filters Silver to rows that were
visible as of D (``realtime_start <= D`` or null/empty), collapses to the
latest revision per (series_id, observation_date), runs a focused subset of
Gold engines, and writes results tagged with ``as_of_date = D`` into a
separate backfill SQLite database.

Tables written (all prefixed ``pit_``):

* ``pit_fred_feature_transforms``       — quant transforms (mom/diff/yoy/zscore)
* ``pit_ml_feature_matrix``             — ML-0 wide feature matrix
* ``pit_macro_factor_scores``           — ML-2 PCA factor scores
* ``pit_macro_factor_loadings``         — ML-2 PCA factor loadings
* ``pit_macro_anomaly_scores``          — ML-4 Mahalanobis anomaly scores
* ``pit_macro_regime_daily``            — regime playbook composite scores
* ``pit_recession_probability_daily``   — ML-3 recession probabilities

Example CLI::

    fred-pipeline backfill \\
        --db-path fred_local.db \\
        --backfill-db fred_backfill.db \\
        --from 2010-01-31 --to 2024-12-31
"""

from __future__ import annotations

import calendar
import sqlite3
from datetime import date
from typing import Any, Optional

from fred_pipeline.transform import latest_by_observation

# ---- Schema ----------------------------------------------------------------

_BACKFILL_SCHEMA = """
CREATE TABLE IF NOT EXISTS pit_fred_feature_transforms (
    as_of_date TEXT NOT NULL,
    series_id TEXT, observation_date TEXT, value REAL,
    mom REAL, diff REAL, yoy REAL, zscore REAL,
    PRIMARY KEY (as_of_date, series_id, observation_date)
);
CREATE TABLE IF NOT EXISTS pit_ml_feature_matrix (
    as_of_date TEXT NOT NULL,
    observation_date TEXT, feature_name TEXT, series_id TEXT,
    transform TEXT, value REAL,
    PRIMARY KEY (as_of_date, observation_date, feature_name)
);
CREATE TABLE IF NOT EXISTS pit_macro_factor_scores (
    as_of_date TEXT NOT NULL,
    observation_date TEXT, factor INTEGER, score REAL,
    explained_variance_ratio REAL, cumulative_variance_ratio REAL, n_obs INTEGER,
    PRIMARY KEY (as_of_date, observation_date, factor)
);
CREATE TABLE IF NOT EXISTS pit_macro_factor_loadings (
    as_of_date TEXT NOT NULL,
    observation_date TEXT, factor INTEGER, feature_name TEXT, loading REAL,
    PRIMARY KEY (as_of_date, observation_date, factor, feature_name)
);
CREATE TABLE IF NOT EXISTS pit_macro_anomaly_scores (
    as_of_date TEXT NOT NULL,
    observation_date TEXT, mahalanobis_d2 REAL, chi2_df INTEGER,
    p_value REAL, is_anomaly INTEGER NOT NULL DEFAULT 0, n_factors_used INTEGER,
    PRIMARY KEY (as_of_date, observation_date)
);
CREATE TABLE IF NOT EXISTS pit_macro_regime_daily (
    as_of_date TEXT NOT NULL,
    observation_date TEXT, growth_score REAL, inflation_score REAL,
    liquidity_score REAL, credit_score REAL, policy_score REAL,
    composite_score REAL, regime_name TEXT, regime_confidence REAL,
    PRIMARY KEY (as_of_date, observation_date)
);
CREATE TABLE IF NOT EXISTS pit_recession_probability_daily (
    as_of_date TEXT NOT NULL,
    observation_date TEXT, recession_prob REAL, prob_recession_3m REAL,
    prob_recession_6m REAL, prob_recession_12m REAL, logit_score REAL,
    n_features INTEGER, n_obs_training INTEGER, model_vintage TEXT,
    is_backfilled INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (as_of_date, observation_date)
);
CREATE TABLE IF NOT EXISTS pit_backfill_log (
    as_of_date TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    n_silver_rows INTEGER,
    n_latest_rows INTEGER,
    completed_at TEXT
);
"""

ALL_TABLES = (
    "feature_transforms",
    "ml_feature_matrix",
    "macro_factor_scores",
    "macro_factor_loadings",
    "macro_anomaly_scores",
    "macro_regime_daily",
    "recession_probability_daily",
)

# ---- Helpers ----------------------------------------------------------------


def _pit_silver(silver: list[dict[str, Any]], cutoff: date) -> list[dict[str, Any]]:
    """Silver rows visible as of ``cutoff``.

    Non-vintage rows (null/empty realtime_start) are always included because
    they represent series that don't carry vintage metadata — they should
    appear in every snapshot regardless of the cutoff.
    """
    cutoff_str = cutoff.isoformat()
    return [
        r for r in silver
        if not r.get("realtime_start") or r["realtime_start"] <= cutoff_str
    ]


def _month_end_dates(from_date: date, to_date: date) -> list[date]:
    """Last calendar day of each month in [from_date, to_date] (inclusive)."""
    results = []
    y, m = from_date.year, from_date.month
    while True:
        last_day = calendar.monthrange(y, m)[1]
        d = date(y, m, last_day)
        if d > to_date:
            break
        if d >= from_date:
            results.append(d)
        if m == 12:
            y, m = y + 1, 1
        else:
            m += 1
    return results


def _week_end_dates(from_date: date, to_date: date) -> list[date]:
    """Last calendar day of each week (Sunday) in [from_date, to_date]."""
    from datetime import timedelta
    results = []
    # advance to first Sunday on or after from_date
    d = from_date
    while d.isoweekday() != 7:  # 7 = Sunday
        d += timedelta(days=1)
    while d <= to_date:
        results.append(d)
        d += timedelta(days=7)
    return results


def _snapshot_dates(from_date: date, to_date: date, step: str) -> list[date]:
    if step == "monthly":
        return _month_end_dates(from_date, to_date)
    if step == "weekly":
        return _week_end_dates(from_date, to_date)
    if step == "daily":
        from datetime import timedelta
        dates = []
        d = from_date
        while d <= to_date:
            dates.append(d)
            d += timedelta(days=1)
        return dates
    raise ValueError(f"Unknown step {step!r}; choose monthly / weekly / daily")


def _insert(conn: sqlite3.Connection, table: str, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    cols = list(rows[0].keys())
    collist = ", ".join(cols)
    placeholders = ", ".join(["?"] * len(cols))
    sql = (
        f"INSERT OR REPLACE INTO {table} ({collist}) VALUES ({placeholders})"
    )
    conn.executemany(sql, ([r.get(c) for c in cols] for r in rows))
    return len(rows)


def _tag(rows: list[dict[str, Any]], as_of_date: str) -> list[dict[str, Any]]:
    """Prepend as_of_date to every row dict."""
    return [{"as_of_date": as_of_date, **r} for r in rows]


# ---- Core engine ------------------------------------------------------------


def _load_ml_cfg():
    try:
        from fred_pipeline.ml_features import load_ml_feature_config
        return load_ml_feature_config()
    except Exception:
        return None


def _run_snapshot(
    silver: list[dict[str, Any]],
    cutoff: date,
    tables: tuple[str, ...],
) -> tuple[dict[str, list[dict[str, Any]]], int, int]:
    """Compute Gold rows for a single PIT snapshot.

    Returns ``(out, n_silver_rows, n_latest_rows)`` where ``out`` is keyed by
    the short table name (without ``pit_`` prefix) and values are rows already
    tagged with ``as_of_date``.
    """
    as_of = cutoff.isoformat()
    pit_rows = _pit_silver(silver, cutoff)
    latest = latest_by_observation(pit_rows)
    for r in latest:
        r["is_missing"] = bool(r.get("is_missing"))

    out: dict[str, list[dict[str, Any]]] = {}
    tables_set = set(tables)

    needs_transforms = tables_set & {
        "feature_transforms", "ml_feature_matrix",
        "macro_factor_scores", "macro_factor_loadings",
        "macro_anomaly_scores", "recession_probability_daily",
    }
    needs_ml_matrix = tables_set & {
        "ml_feature_matrix", "macro_factor_scores",
        "macro_factor_loadings", "macro_anomaly_scores",
    }
    needs_pca = tables_set & {
        "macro_factor_scores", "macro_factor_loadings", "macro_anomaly_scores",
    }
    needs_regime = tables_set & {"macro_regime_daily", "recession_probability_daily"}

    # ---- feature transforms -------------------------------------------------
    feature_transform_rows: list[dict[str, Any]] = []
    if needs_transforms:
        from fred_pipeline.features import compute_feature_transforms
        feature_transform_rows = list(compute_feature_transforms(latest))
        if "feature_transforms" in tables_set:
            out["feature_transforms"] = _tag(feature_transform_rows, as_of)

    # ---- ML-0 feature matrix ------------------------------------------------
    ml_matrix: list[dict[str, Any]] = []
    if needs_ml_matrix:
        from fred_pipeline.ml_features import compute_ml_feature_matrix
        ml_cfg = _load_ml_cfg()
        ml_matrix = compute_ml_feature_matrix(feature_transform_rows, ml_cfg)
        if "ml_feature_matrix" in tables_set:
            out["ml_feature_matrix"] = _tag(ml_matrix, as_of)

    # ---- ML-2 PCA factor scores / loadings ----------------------------------
    pca: dict[str, Any] = {}
    if needs_pca:
        from fred_pipeline.macro_pca import compute_macro_factor_scores
        ml_cfg = _load_ml_cfg()
        n_comp = ml_cfg.n_components if ml_cfg else 5
        pca = compute_macro_factor_scores(ml_matrix, n_components=n_comp)
        if "macro_factor_scores" in tables_set:
            out["macro_factor_scores"] = _tag(pca["scores"], as_of)
        if "macro_factor_loadings" in tables_set:
            out["macro_factor_loadings"] = _tag(pca["loadings"], as_of)

    # ---- ML-4 Mahalanobis anomaly scores ------------------------------------
    if "macro_anomaly_scores" in tables_set and pca:
        from fred_pipeline.anomaly import compute_macro_anomaly_scores
        ml_cfg = _load_ml_cfg()
        anom_thresh = ml_cfg.anomaly_threshold if ml_cfg else 0.99
        out["macro_anomaly_scores"] = _tag(
            compute_macro_anomaly_scores(pca["scores"], anomaly_threshold=anom_thresh),
            as_of,
        )

    # ---- regime playbook ----------------------------------------------------
    regime_rows: list[dict[str, Any]] = []
    if needs_regime:
        from fred_pipeline.regime_stats import compute_macro_regime
        regime_rows = compute_macro_regime(latest)
        if "macro_regime_daily" in tables_set:
            out["macro_regime_daily"] = _tag(regime_rows, as_of)

    # ---- ML-3 recession probability (needs NS factors + credit + funding) ---
    if "recession_probability_daily" in tables_set:
        from fred_pipeline.terminal_views import (
            compute_credit_spread_daily,
            compute_funding_features,
            compute_treasury_curve,
        )
        from fred_pipeline.ns_model import compute_yield_curve_ns_factors
        from fred_pipeline.recession_model import (
            compute_recession_probability,
            load_recession_model_config,
        )

        curve = compute_treasury_curve(latest)
        ns_factor_rows = compute_yield_curve_ns_factors(curve["curve"])
        credit_rows = compute_credit_spread_daily(latest)
        funding = compute_funding_features(latest)

        rec_cfg = None
        try:
            rec_cfg = load_recession_model_config()
        except Exception:
            pass
        out["recession_probability_daily"] = _tag(
            compute_recession_probability(
                latest,
                ns_factor_rows=ns_factor_rows,
                feature_transform_rows=feature_transform_rows,
                credit_spread_rows=credit_rows,
                funding_stress_rows=funding["stress"],
                regime_rows=regime_rows,
                cfg=rec_cfg,
            ),
            as_of,
        )

    return out, len(pit_rows), len(latest)


# ---- Public entry point -----------------------------------------------------


def run_backfill(
    db_path: str,
    backfill_db_path: str,
    from_date: date,
    to_date: date,
    step: str = "monthly",
    tables: Optional[tuple[str, ...]] = None,
    resume: bool = True,
) -> dict[str, Any]:
    """Run the PIT backfill over the given date range.

    Parameters
    ----------
    db_path:
        Path to the existing pipeline SQLite database (source of Silver data).
    backfill_db_path:
        Path to the backfill output database (created if it doesn't exist).
    from_date / to_date:
        Inclusive date range for snapshots.
    step:
        Snapshot cadence: ``"monthly"`` (month-end), ``"weekly"`` (Sunday),
        or ``"daily"``.
    tables:
        Subset of :data:`ALL_TABLES` to compute. ``None`` means all.
    resume:
        Skip snapshot dates already present in ``pit_backfill_log``.

    Returns
    -------
    dict with keys: ``snapshots_computed``, ``snapshots_skipped``,
    ``snapshots_failed``, ``errors``.
    """
    import datetime as _dt

    wanted = tuple(tables) if tables else ALL_TABLES
    unknown = set(wanted) - set(ALL_TABLES)
    if unknown:
        raise ValueError(f"Unknown table(s): {unknown}. Valid: {ALL_TABLES}")

    # Translate short names to pit_ table names
    pit_tables = {t: f"pit_{t}" for t in wanted}
    # Feature transforms always written to pit_fred_feature_transforms
    pit_tables_renamed = {
        "feature_transforms": "pit_fred_feature_transforms",
        "ml_feature_matrix": "pit_ml_feature_matrix",
        "macro_factor_scores": "pit_macro_factor_scores",
        "macro_factor_loadings": "pit_macro_factor_loadings",
        "macro_anomaly_scores": "pit_macro_anomaly_scores",
        "macro_regime_daily": "pit_macro_regime_daily",
        "recession_probability_daily": "pit_recession_probability_daily",
    }

    # ---- Read all Silver from source DB -------------------------------------
    src_conn = sqlite3.connect(db_path)
    src_conn.row_factory = sqlite3.Row
    try:
        silver = [
            dict(r) for r in src_conn.execute(
                "SELECT * FROM silver_fred_observation"
            ).fetchall()
        ]
    finally:
        src_conn.close()

    # ---- Open / create backfill DB ------------------------------------------
    bfill_conn = sqlite3.connect(backfill_db_path)
    bfill_conn.row_factory = sqlite3.Row
    for stmt in _BACKFILL_SCHEMA.strip().split(";"):
        stmt = stmt.strip()
        if stmt:
            bfill_conn.execute(stmt)
    bfill_conn.commit()

    # ---- Discover already-computed dates (for resume) -----------------------
    done_dates: set[str] = set()
    if resume:
        done_dates = {
            r[0] for r in bfill_conn.execute(
                "SELECT as_of_date FROM pit_backfill_log WHERE status='ok'"
            ).fetchall()
        }

    # ---- Iterate snapshots --------------------------------------------------
    snapshots = _snapshot_dates(from_date, to_date, step)
    computed = 0
    skipped = 0
    failed = 0
    errors: list[str] = []

    for cutoff in snapshots:
        as_of = cutoff.isoformat()
        if as_of in done_dates:
            skipped += 1
            continue

        try:
            result, n_silver, n_latest = _run_snapshot(silver, cutoff, wanted)

            for short_name, rows in result.items():
                pit_table = pit_tables_renamed[short_name]
                _insert(bfill_conn, pit_table, rows)

            bfill_conn.execute(
                """INSERT OR REPLACE INTO pit_backfill_log
                   (as_of_date, status, n_silver_rows, n_latest_rows, completed_at)
                   VALUES (?, 'ok', ?, ?, ?)""",
                (as_of, n_silver, n_latest,
                 _dt.datetime.utcnow().isoformat(timespec="seconds")),
            )
            bfill_conn.commit()
            computed += 1
        except Exception as exc:
            bfill_conn.execute(
                """INSERT OR REPLACE INTO pit_backfill_log
                   (as_of_date, status, n_silver_rows, n_latest_rows, completed_at)
                   VALUES (?, 'error', NULL, NULL, ?)""",
                (as_of, _dt.datetime.utcnow().isoformat(timespec="seconds")),
            )
            bfill_conn.commit()
            failed += 1
            errors.append(f"{as_of}: {exc}")

    bfill_conn.close()
    return {
        "snapshots_total": len(snapshots),
        "snapshots_computed": computed,
        "snapshots_skipped": skipped,
        "snapshots_failed": failed,
        "errors": errors,
    }
