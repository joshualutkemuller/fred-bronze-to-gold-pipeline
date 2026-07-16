"""Tests for the historical backfill engine (backfill.py)."""

import sqlite3
from datetime import date

import pytest

from fred_pipeline.backfill import (
    ALL_TABLES,
    _month_end_dates,
    _pit_silver,
    _snapshot_dates,
    _week_end_dates,
    run_backfill,
)


# ---- Unit tests for helpers -------------------------------------------------


def test_pit_silver_excludes_future_vintage():
    rows = [
        {"series_id": "X", "observation_date": "2020-01-01",
         "realtime_start": "2020-03-01", "value": 1.0},
        {"series_id": "X", "observation_date": "2020-01-01",
         "realtime_start": "2020-01-15", "value": 0.9},
    ]
    result = _pit_silver(rows, date(2020, 2, 1))
    # Only the Jan-15 vintage is visible as of Feb-1
    assert len(result) == 1
    assert result[0]["realtime_start"] == "2020-01-15"


def test_pit_silver_includes_null_realtime_start():
    """Non-vintage series (null realtime_start) appear in every snapshot."""
    rows = [
        {"series_id": "Y", "observation_date": "2023-06-01",
         "realtime_start": None, "value": 5.0},
        {"series_id": "Y", "observation_date": "2023-07-01",
         "realtime_start": "", "value": 6.0},
    ]
    # Even with cutoff before the dates, both rows are included
    result = _pit_silver(rows, date(2023, 1, 1))
    assert len(result) == 2


def test_pit_silver_includes_same_day_vintage():
    rows = [
        {"series_id": "Z", "observation_date": "2021-01-01",
         "realtime_start": "2021-06-30", "value": 1.0},
    ]
    # Cutoff == realtime_start → included (<=)
    assert len(_pit_silver(rows, date(2021, 6, 30))) == 1
    # Cutoff one day before → excluded
    assert len(_pit_silver(rows, date(2021, 6, 29))) == 0


def test_month_end_dates_basic():
    # March 31 > to_date (March 15), so only Jan and Feb month-ends are returned.
    result = _month_end_dates(date(2024, 1, 1), date(2024, 3, 15))
    assert result == [date(2024, 1, 31), date(2024, 2, 29)]


def test_month_end_dates_includes_to_month_if_on_last_day():
    result = _month_end_dates(date(2024, 1, 1), date(2024, 3, 31))
    assert result == [date(2024, 1, 31), date(2024, 2, 29), date(2024, 3, 31)]


def test_month_end_dates_feb_non_leap():
    result = _month_end_dates(date(2023, 2, 1), date(2023, 2, 28))
    assert result == [date(2023, 2, 28)]


def test_month_end_dates_same_month():
    result = _month_end_dates(date(2024, 6, 1), date(2024, 6, 30))
    assert result == [date(2024, 6, 30)]


def test_month_end_dates_from_after_end():
    assert _month_end_dates(date(2024, 6, 1), date(2024, 5, 31)) == []


def test_week_end_dates_basic():
    # 2024-01-07 is a Sunday; 2024-01-14, 2024-01-21 follow
    result = _week_end_dates(date(2024, 1, 1), date(2024, 1, 21))
    for d in result:
        assert d.isoweekday() == 7  # Sunday
    assert date(2024, 1, 7) in result
    assert date(2024, 1, 14) in result
    assert date(2024, 1, 21) in result


def test_snapshot_dates_monthly():
    result = _snapshot_dates(date(2024, 1, 1), date(2024, 3, 1), "monthly")
    assert result[0] == date(2024, 1, 31)
    assert result[1] == date(2024, 2, 29)


def test_snapshot_dates_daily():
    result = _snapshot_dates(date(2024, 1, 29), date(2024, 1, 31), "daily")
    assert result == [date(2024, 1, 29), date(2024, 1, 30), date(2024, 1, 31)]


def test_snapshot_dates_unknown_step():
    with pytest.raises(ValueError, match="Unknown step"):
        _snapshot_dates(date(2024, 1, 1), date(2024, 12, 31), "yearly")


# ---- Integration test -------------------------------------------------------


def _make_silver_db(path: str) -> None:
    """Create a minimal source SQLite DB with Silver rows."""
    conn = sqlite3.connect(path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS silver_fred_observation (
            source TEXT NOT NULL DEFAULT 'fred',
            series_id TEXT, observation_date TEXT, realtime_start TEXT,
            realtime_end TEXT, value REAL, raw_value TEXT, is_missing INTEGER,
            row_hash TEXT, revision_number INTEGER, ingested_at TEXT, run_id TEXT,
            PRIMARY KEY (source, series_id, observation_date, realtime_start)
        )
    """)
    # Non-vintage series (always visible)
    rows = [
        ("fred", "UNRATE", f"2023-0{m:01d}-01", None, "9999-12-31",
         3.0 + m * 0.1, str(3.0 + m * 0.1), 0, None, 0, "2023-01-01", "r1")
        for m in range(1, 13)
    ]
    # Vintage-enabled series: first vintage released in 2023-03
    rows += [
        ("fred", "GDPC1", "2022-10-01", "2023-03-15", "9999-12-31",
         25000.0, "25000.0", 0, None, 0, "2023-03-15", "r1"),
        ("fred", "GDPC1", "2022-10-01", "2023-06-15", "9999-12-31",
         25100.0, "25100.0", 0, None, 0, "2023-06-15", "r2"),
    ]
    conn.executemany(
        "INSERT OR REPLACE INTO silver_fred_observation VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        rows,
    )
    conn.commit()
    conn.close()


def test_run_backfill_feature_transforms_only(tmp_path):
    db = str(tmp_path / "src.db")
    bfill_db = str(tmp_path / "bfill.db")
    _make_silver_db(db)

    result = run_backfill(
        db_path=db,
        backfill_db_path=bfill_db,
        from_date=date(2023, 1, 1),
        to_date=date(2023, 3, 31),
        step="monthly",
        tables=("feature_transforms",),
        resume=True,
    )

    assert result["snapshots_computed"] == 3
    assert result["snapshots_failed"] == 0

    conn = sqlite3.connect(bfill_db)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT DISTINCT as_of_date FROM pit_fred_feature_transforms ORDER BY as_of_date").fetchall()
    conn.close()

    as_of_dates = [r["as_of_date"] for r in rows]
    assert "2023-01-31" in as_of_dates
    assert "2023-02-28" in as_of_dates
    assert "2023-03-31" in as_of_dates


def test_run_backfill_vintage_filtering(tmp_path):
    """Vintage GDPC1 row (realtime_start 2023-03-15) is excluded in Jan snapshot."""
    db = str(tmp_path / "src.db")
    bfill_db = str(tmp_path / "bfill.db")
    _make_silver_db(db)

    run_backfill(
        db_path=db,
        backfill_db_path=bfill_db,
        from_date=date(2023, 1, 1),
        to_date=date(2023, 6, 30),
        step="monthly",
        tables=("feature_transforms",),
        resume=True,
    )

    conn = sqlite3.connect(bfill_db)
    conn.row_factory = sqlite3.Row

    # Jan snapshot: GDPC1 should be absent (realtime_start 2023-03-15 > 2023-01-31)
    jan_ids = {
        r["series_id"]
        for r in conn.execute(
            "SELECT DISTINCT series_id FROM pit_fred_feature_transforms "
            "WHERE as_of_date='2023-01-31'"
        ).fetchall()
    }
    assert "GDPC1" not in jan_ids
    assert "UNRATE" in jan_ids

    # Apr snapshot: GDPC1 appears (realtime_start 2023-03-15 <= 2023-04-30)
    apr_ids = {
        r["series_id"]
        for r in conn.execute(
            "SELECT DISTINCT series_id FROM pit_fred_feature_transforms "
            "WHERE as_of_date='2023-04-30'"
        ).fetchall()
    }
    assert "GDPC1" in apr_ids

    conn.close()


def test_run_backfill_resume(tmp_path):
    """Dates already in pit_backfill_log with status='ok' are skipped."""
    db = str(tmp_path / "src.db")
    bfill_db = str(tmp_path / "bfill.db")
    _make_silver_db(db)

    # First pass: compute Jan–Feb
    r1 = run_backfill(
        db_path=db, backfill_db_path=bfill_db,
        from_date=date(2023, 1, 1), to_date=date(2023, 2, 28),
        step="monthly", tables=("feature_transforms",), resume=True,
    )
    assert r1["snapshots_computed"] == 2
    assert r1["snapshots_skipped"] == 0

    # Second pass: same range → both dates skipped
    r2 = run_backfill(
        db_path=db, backfill_db_path=bfill_db,
        from_date=date(2023, 1, 1), to_date=date(2023, 3, 31),
        step="monthly", tables=("feature_transforms",), resume=True,
    )
    assert r2["snapshots_skipped"] == 2
    assert r2["snapshots_computed"] == 1  # only March is new


def test_run_backfill_no_resume_recomputes(tmp_path):
    """--no-resume recomputes already-done dates."""
    db = str(tmp_path / "src.db")
    bfill_db = str(tmp_path / "bfill.db")
    _make_silver_db(db)

    run_backfill(
        db_path=db, backfill_db_path=bfill_db,
        from_date=date(2023, 1, 1), to_date=date(2023, 1, 31),
        step="monthly", tables=("feature_transforms",), resume=True,
    )

    r = run_backfill(
        db_path=db, backfill_db_path=bfill_db,
        from_date=date(2023, 1, 1), to_date=date(2023, 1, 31),
        step="monthly", tables=("feature_transforms",), resume=False,
    )
    assert r["snapshots_skipped"] == 0
    assert r["snapshots_computed"] == 1


def test_run_backfill_invalid_table(tmp_path):
    db = str(tmp_path / "src.db")
    _make_silver_db(db)
    with pytest.raises(ValueError, match="Unknown table"):
        run_backfill(
            db_path=db,
            backfill_db_path=str(tmp_path / "b.db"),
            from_date=date(2023, 1, 1),
            to_date=date(2023, 3, 31),
            tables=("nonexistent_table",),
        )


def test_backfill_log_populated(tmp_path):
    db = str(tmp_path / "src.db")
    bfill_db = str(tmp_path / "bfill.db")
    _make_silver_db(db)

    run_backfill(
        db_path=db, backfill_db_path=bfill_db,
        from_date=date(2023, 1, 1), to_date=date(2023, 2, 28),
        step="monthly", tables=("feature_transforms",), resume=True,
    )

    conn = sqlite3.connect(bfill_db)
    log_rows = conn.execute(
        "SELECT as_of_date, status FROM pit_backfill_log ORDER BY as_of_date"
    ).fetchall()
    conn.close()

    assert len(log_rows) == 2
    assert all(r[1] == "ok" for r in log_rows)
    assert log_rows[0][0] == "2023-01-31"
    assert log_rows[1][0] == "2023-02-28"


# ---- CLI integration --------------------------------------------------------


def test_cli_backfill_basic(tmp_path):
    from fred_pipeline.cli import main

    db = str(tmp_path / "src.db")
    bfill_db = str(tmp_path / "bfill.db")
    _make_silver_db(db)

    rc = main([
        "backfill",
        "--db-path", db,
        "--backfill-db", bfill_db,
        "--from", "2023-01-01",
        "--to", "2023-02-28",
        "--step", "monthly",
        "--tables", "feature_transforms",
    ])
    assert rc == 0


def test_cli_backfill_bad_date(tmp_path, capsys):
    from fred_pipeline.cli import main

    rc = main([
        "backfill",
        "--db-path", str(tmp_path / "x.db"),
        "--backfill-db", str(tmp_path / "b.db"),
        "--from", "not-a-date",
        "--to", "2023-12-31",
    ])
    assert rc == 2
    captured = capsys.readouterr()
    assert "ERROR" in captured.err


def test_cli_backfill_from_after_to(tmp_path, capsys):
    from fred_pipeline.cli import main

    rc = main([
        "backfill",
        "--db-path", str(tmp_path / "x.db"),
        "--backfill-db", str(tmp_path / "b.db"),
        "--from", "2023-12-31",
        "--to", "2023-01-01",
    ])
    assert rc == 2
    captured = capsys.readouterr()
    assert "--from must be" in captured.err


def test_cli_backfill_unknown_table(tmp_path, capsys):
    from fred_pipeline.cli import main

    db = str(tmp_path / "src.db")
    _make_silver_db(db)

    rc = main([
        "backfill",
        "--db-path", db,
        "--backfill-db", str(tmp_path / "b.db"),
        "--from", "2023-01-01",
        "--to", "2023-01-31",
        "--tables", "bad_table",
    ])
    assert rc == 2
    captured = capsys.readouterr()
    assert "unknown table" in captured.err.lower()
