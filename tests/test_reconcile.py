from datetime import date

from fred_pipeline.config import Environment, PipelineConfig
from fred_pipeline.fred_client import FredAPIError
from fred_pipeline.local_store import LocalWarehouse
from fred_pipeline.manifest import Manifest, SeriesSpec
from fred_pipeline.reconcile import (
    compare_metadata,
    lifecycle_snapshot,
    persist_report,
    reconcile,
    reconcile_staleness,
    staleness_snapshot,
)


def _spec(series_id="DGS10", **kw):
    kw.setdefault("title", "10-Year Treasury")
    kw.setdefault("frequency", "d")
    return SeriesSpec(series_id=series_id, **kw)


def _meta(**kw):
    base = {
        "id": "DGS10",
        "title": "10-Year Treasury Constant Maturity Rate",
        "frequency_short": "D",
        "units": "Percent",
        "seasonal_adjustment_short": "NSA",
        "observation_start": "1962-01-02",
        "observation_end": "2024-06-28",
        "last_updated": "2024-07-01 15:20:00-05",
        "popularity": 85,
    }
    base.update(kw)
    return base


class FakeMetaClient:
    def __init__(self, metas, missing=()):
        self.metas = metas
        self.missing = set(missing)

    def get_series_metadata(self, series_id):
        if series_id in self.missing:
            raise FredAPIError("not found", 400)
        return self.metas[series_id]


# ---- compare_metadata ----------------------------------------------------

def test_no_drift_when_aligned():
    assert compare_metadata(_spec(units="Percent"), _meta()) == []


def test_frequency_mismatch_is_error():
    drifts = compare_metadata(_spec(frequency="d"), _meta(frequency_short="M"))
    assert len(drifts) == 1
    assert drifts[0].kind == "frequency_mismatch"
    assert drifts[0].severity == "error"
    assert drifts[0].fred_value == "m"


def test_discontinued_is_warning():
    drifts = compare_metadata(
        _spec(), _meta(title="Foo (DISCONTINUED) Series")
    )
    kinds = {d.kind: d for d in drifts}
    assert "discontinued" in kinds
    assert kinds["discontinued"].severity == "warning"


def test_units_change_is_info():
    drifts = compare_metadata(_spec(units="Percent"), _meta(units="Index 2017=100"))
    assert [d.kind for d in drifts] == ["units_changed"]
    assert drifts[0].severity == "info"


# ---- lifecycle_snapshot --------------------------------------------------

def test_lifecycle_flags_stale_daily_series():
    snap = lifecycle_snapshot(
        _spec(frequency="d"), _meta(observation_end="2024-01-01"),
        today=date(2024, 6, 1),
    )
    assert snap["is_stale"] is True          # >10 days old for a daily series
    assert snap["days_since_last_observation"] == 152
    assert snap["popularity"] == 85
    assert snap["discontinued"] is False


def test_lifecycle_not_stale_when_fresh():
    snap = lifecycle_snapshot(
        _spec(frequency="d"), _meta(observation_end="2024-05-30"),
        today=date(2024, 6, 1),
    )
    assert snap["is_stale"] is False


def test_lifecycle_marks_discontinued():
    snap = lifecycle_snapshot(_spec(), _meta(title="X (DISCONTINUED)"),
                              today=date(2024, 6, 1))
    assert snap["discontinued"] is True


# ---- orchestrator + persistence -----------------------------------------

def _manifest(*specs):
    return Manifest.from_dict({
        "name": "t",
        "series": [s.to_dict() for s in specs],
    })


def test_reconcile_collects_drift_and_not_found():
    man = _manifest(
        _spec("DGS10", frequency="d", units="Percent"),
        _spec("BADID", frequency="m"),
    )
    client = FakeMetaClient({"DGS10": _meta(frequency_short="M")}, missing=["BADID"])
    report = reconcile([man], client, today=date(2024, 6, 1))

    assert report.series_checked == 2
    assert "BADID" in report.not_found
    assert report.has_errors  # freq mismatch + not_found are errors
    kinds = {d.kind for d in report.drifts}
    assert {"frequency_mismatch", "not_found"} <= kinds


def test_reconcile_skips_non_fred_sources():
    man = _manifest(
        _spec("DGS10", frequency="d", units="Percent"),
        _spec("CUUR0000SA0", frequency="m", source="bls"),
    )
    # The client only knows FRED metadata; reconciling the BLS series against
    # FRED would spuriously flag it not_found. It must be skipped instead.
    client = FakeMetaClient({"DGS10": _meta()})
    report = reconcile([man], client, today=date(2024, 6, 1))

    assert report.series_checked == 1      # only the FRED series is reconciled
    assert report.not_found == []          # BLS series skipped, not flagged


def test_persist_report_writes_meta_tables(tmp_path):
    cfg = PipelineConfig(environment=Environment.DEV, fred_api_key="k")
    man = _manifest(_spec("DGS10", frequency="d", units="Percent"))
    client = FakeMetaClient({"DGS10": _meta(units="Index 2017=100")})
    report = reconcile([man], client, today=date(2024, 6, 1))

    wh = LocalWarehouse(cfg, db_path=str(tmp_path / "g.db"))
    counts = persist_report(cfg, report, wh)
    assert counts["lifecycle_rows"] == 1
    assert counts["drift_rows"] >= 1

    life = wh.query("SELECT * FROM meta_fred_series_lifecycle")
    assert life[0]["series_id"] == "DGS10"
    drift = wh.query("SELECT kind FROM meta_fred_series_drift")
    assert any(r["kind"] == "units_changed" for r in drift)
    wh.close()


# ---- staleness_snapshot (source-agnostic) --------------------------------

def test_staleness_snapshot_flags_stale_daily_series():
    snap = staleness_snapshot(
        _spec(frequency="d"), "2024-01-01", today=date(2024, 6, 1)
    )
    assert snap["is_stale"] is True           # >10 days old for a daily series
    assert snap["days_since_last_observation"] == 152
    assert snap["has_data"] is True
    assert snap["source"] == "fred"


def test_staleness_snapshot_not_stale_when_fresh():
    snap = staleness_snapshot(
        _spec(frequency="d"), "2024-05-30", today=date(2024, 6, 1)
    )
    assert snap["is_stale"] is False


def test_staleness_snapshot_flags_no_data_as_not_stale():
    # A series with zero ingested observations isn't "stale" (nothing to
    # compare against) — it's a distinct "no_data"/has_data=False case, so a
    # never-run series doesn't get silently lumped in with a broken feed.
    snap = staleness_snapshot(_spec(frequency="d"), None, today=date(2024, 6, 1))
    assert snap["has_data"] is False
    assert snap["is_stale"] is False
    assert snap["days_since_last_observation"] is None


def test_staleness_snapshot_covers_non_fred_source():
    snap = staleness_snapshot(
        _spec("AAPL", frequency="d", source="tiingo"),
        "2024-01-01",
        today=date(2024, 6, 1),
    )
    assert snap["source"] == "tiingo"
    assert snap["is_stale"] is True


# ---- reconcile_staleness (orchestrator, all sources) ---------------------

class FakeStalenessWarehouse:
    def __init__(self, latest: dict):
        self._latest = latest

    def latest_observation_dates(self, series_ids):
        return {sid: d for sid, d in self._latest.items() if sid in series_ids}


def test_reconcile_staleness_covers_every_source_not_just_fred():
    man = _manifest(
        _spec("DGS10", frequency="d", units="Percent"),
        _spec("CUUR0000SA0", frequency="m", source="bls"),
        _spec("AAPL", frequency="d", source="tiingo"),
    )
    wh = FakeStalenessWarehouse({
        "DGS10": "2024-05-30",       # fresh
        "CUUR0000SA0": "2020-01-01",  # very stale
        "AAPL": "2024-05-30",         # fresh
        # no entry for a series with no ingested data yet is handled elsewhere
    })
    rows = reconcile_staleness([man], wh, today=date(2024, 6, 1))

    assert {r["series_id"] for r in rows} == {"DGS10", "CUUR0000SA0", "AAPL"}
    by_id = {r["series_id"]: r for r in rows}
    assert by_id["CUUR0000SA0"]["is_stale"] is True
    assert by_id["CUUR0000SA0"]["source"] == "bls"
    assert by_id["DGS10"]["is_stale"] is False
    assert by_id["AAPL"]["source"] == "tiingo"
    assert by_id["AAPL"]["is_stale"] is False


def test_reconcile_staleness_marks_never_ingested_series():
    man = _manifest(_spec("NEWSERIES", frequency="d", source="eia"))
    wh = FakeStalenessWarehouse({})
    rows = reconcile_staleness([man], wh, today=date(2024, 6, 1))

    assert rows[0]["has_data"] is False
    assert rows[0]["is_stale"] is False


def test_local_warehouse_latest_observation_dates_and_write_staleness(tmp_path):
    cfg = PipelineConfig(environment=Environment.DEV, fred_api_key="k")
    wh = LocalWarehouse(cfg, db_path=str(tmp_path / "s.db"))
    wh.merge_silver([
        {
            "source": "tiingo", "series_id": "AAPL", "observation_date": "2024-01-01",
            "realtime_start": "2024-01-01", "realtime_end": "2024-01-01",
            "value": 190.0, "raw_value": "190.0", "is_missing": 0,
            "row_hash": "h1", "revision_number": 1, "ingested_at": "2024-01-02",
            "run_id": "r1",
        },
        {
            "source": "tiingo", "series_id": "AAPL", "observation_date": "2024-01-02",
            "realtime_start": "2024-01-02", "realtime_end": "2024-01-02",
            "value": 191.0, "raw_value": "191.0", "is_missing": 0,
            "row_hash": "h2", "revision_number": 1, "ingested_at": "2024-01-03",
            "run_id": "r1",
        },
    ])

    latest = wh.latest_observation_dates(["AAPL", "MISSING"])
    assert latest == {"AAPL": "2024-01-02"}

    man = _manifest(_spec("AAPL", frequency="d", source="tiingo"))
    rows = reconcile_staleness([man], wh, today=date(2024, 6, 1))
    n = wh.write_staleness(rows)
    assert n == 1

    persisted = wh.query("SELECT * FROM meta_series_staleness")
    assert persisted[0]["series_id"] == "AAPL"
    assert persisted[0]["source"] == "tiingo"
    assert bool(persisted[0]["is_stale"]) is True
    wh.close()
