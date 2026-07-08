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
