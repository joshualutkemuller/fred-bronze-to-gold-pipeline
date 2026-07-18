"""Tests for the economic release calendar (gold.release_calendar, item 1 of
docs/handoffs/terminal_phase0_gaps.md): config/release_calendar.yml loading,
compute_release_calendar filtering/shaping, and the LocalWarehouse write path.
"""

from __future__ import annotations

import textwrap
from datetime import date

import pytest

from fred_pipeline.config import Environment, PipelineConfig
from fred_pipeline.gold_config.release_calendar_config import (
    ReleaseCalendarConfigError,
    ReleaseCalendarEntry,
    load_release_calendar_config,
)
from fred_pipeline.local_store import LocalWarehouse
from fred_pipeline.terminal_views import compute_release_calendar


def _config():
    return PipelineConfig(environment=Environment.DEV, fred_api_key="k")


ENTRIES = [
    ReleaseCalendarEntry(10, "Consumer Price Index", "HIGH", "inflation", "CPIAUCSL"),
    ReleaseCalendarEntry(50, "Employment Situation", "HIGH", "labor", "PAYEMS"),
]


# ---------------------------------------------------------------------------
# Config loader
# ---------------------------------------------------------------------------


def test_load_release_calendar_config(tmp_path):
    p = tmp_path / "release_calendar.yml"
    p.write_text(textwrap.dedent("""
        releases:
          - {release_id: 10, release_name: Consumer Price Index, importance: HIGH, econ_category: inflation, representative_series_id: CPIAUCSL}
    """))
    entries = load_release_calendar_config(str(p))
    assert len(entries) == 1
    assert entries[0].release_id == 10
    assert entries[0].representative_series_id == "CPIAUCSL"


def test_load_release_calendar_config_missing_file_returns_empty():
    assert load_release_calendar_config("/nonexistent/path.yml") == []


def test_release_calendar_entry_rejects_bad_importance():
    with pytest.raises(ReleaseCalendarConfigError):
        ReleaseCalendarEntry(10, "CPI", "URGENT", "inflation", "CPIAUCSL")


def test_load_release_calendar_config_rejects_duplicate_id(tmp_path):
    p = tmp_path / "release_calendar.yml"
    p.write_text(textwrap.dedent("""
        releases:
          - {release_id: 10, release_name: A, importance: HIGH, econ_category: x, representative_series_id: S1}
          - {release_id: 10, release_name: B, importance: LOW, econ_category: y, representative_series_id: S2}
    """))
    with pytest.raises(ReleaseCalendarConfigError):
        load_release_calendar_config(str(p))


# ---------------------------------------------------------------------------
# compute_release_calendar
# ---------------------------------------------------------------------------


def test_compute_release_calendar_filters_to_curated_set():
    release_dates = [
        {"release_id": 10, "release_name": "Consumer Price Index", "date": "2026-08-12"},
        {"release_id": 441, "release_name": "Coinbase Cryptocurrencies", "date": "2026-07-17"},
    ]
    rows = compute_release_calendar(release_dates, ENTRIES, as_of=date(2026, 7, 17))
    assert len(rows) == 1
    assert rows[0]["release_id"] == 10
    assert rows[0]["representative_series_id"] == "CPIAUCSL"


def test_compute_release_calendar_is_future_flag():
    release_dates = [
        {"release_id": 10, "release_name": "CPI", "date": "2026-07-01"},
        {"release_id": 50, "release_name": "Employment", "date": "2026-08-07"},
    ]
    rows = compute_release_calendar(release_dates, ENTRIES, as_of=date(2026, 7, 17))
    by_id = {r["release_id"]: r for r in rows}
    assert by_id[10]["is_future"] is False
    assert by_id[50]["is_future"] is True


def test_compute_release_calendar_dedupes_same_release_and_date():
    release_dates = [
        {"release_id": 10, "release_name": "CPI", "date": "2026-08-12"},
        {"release_id": 10, "release_name": "CPI", "date": "2026-08-12"},
    ]
    rows = compute_release_calendar(release_dates, ENTRIES)
    assert len(rows) == 1


def test_compute_release_calendar_stamps_fetched_at():
    rows = compute_release_calendar(
        [{"release_id": 10, "release_name": "CPI", "date": "2026-08-12"}],
        ENTRIES, fetched_at="2026-07-17T12:00:00+00:00",
    )
    assert rows[0]["fetched_at"] == "2026-07-17T12:00:00+00:00"


def test_compute_release_calendar_no_config_returns_empty():
    assert compute_release_calendar(
        [{"release_id": 10, "release_name": "CPI", "date": "2026-08-12"}], [],
    ) == []


# ---------------------------------------------------------------------------
# Warehouse round-trip
# ---------------------------------------------------------------------------


def test_local_warehouse_write_release_calendar_overwrites(tmp_path):
    wh = LocalWarehouse(_config(), db_path=str(tmp_path / "f.db"))
    rows = [{
        "release_id": 10, "release_name": "CPI", "release_date": "2026-08-12",
        "importance": "HIGH", "econ_category": "inflation",
        "representative_series_id": "CPIAUCSL", "is_future": True,
        "fetched_at": "2026-07-17T00:00:00+00:00",
    }]
    n = wh.write_release_calendar(rows)
    assert n == 1
    got = wh.conn.execute("SELECT * FROM gold_release_calendar").fetchall()
    assert len(got) == 1

    # A second call fully replaces the prior snapshot (no accumulation).
    wh.write_release_calendar(rows)
    got = wh.conn.execute("SELECT * FROM gold_release_calendar").fetchall()
    assert len(got) == 1
