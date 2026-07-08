"""Tests for the full-on-first-load / restate-last-N incremental strategy."""

from fred_pipeline.config import Environment, PipelineConfig
from fred_pipeline.local_store import LocalWarehouse
from fred_pipeline.manifest import SeriesSpec
from fred_pipeline.pipeline import FredPipeline
from fred_pipeline.silver import build_silver_rows


def _config(**kw):
    return PipelineConfig(environment=Environment.DEV, fred_api_key="k", **kw)


def _spec(series_id, **kw):
    kw.setdefault("title", series_id)
    kw.setdefault("frequency", "d")
    return SeriesSpec(series_id=series_id, **kw)


def _payload(dates):
    return {
        "count": len(dates),
        "observations": [
            {"date": d, "value": "1.0", "realtime_start": "2024-02-01",
             "realtime_end": "9999-12-31"}
            for d in dates
        ],
    }


DATES = ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"]


class RecordingClient:
    """Captures the kwargs of each get_observations call."""

    def __init__(self, payload):
        self.payload = payload
        self.calls = []

    def get_observations(self, series_id, **kwargs):
        self.calls.append(kwargs)
        return self.payload


# ---- warehouse watermark -------------------------------------------------

def test_restate_start_local(tmp_path):
    wh = LocalWarehouse(_config(), db_path=str(tmp_path / "f.db"))
    wh.merge_silver(build_silver_rows("X", _payload(DATES), run_id="r"))

    # earliest of the 2 most recent dates (04, 05) -> 04
    assert wh.restate_start("X", 2) == "2024-01-04"
    # fewer than N present -> earliest available
    assert wh.restate_start("X", 99) == "2024-01-01"
    # unknown series -> None (signals full load)
    assert wh.restate_start("MISSING", 5) is None
    wh.close()


# ---- pipeline planning ---------------------------------------------------

def test_first_run_full_then_restate(tmp_path):
    cfg = _config(restate_last_n=2)
    client = RecordingClient(_payload(DATES))
    wh = LocalWarehouse(cfg, db_path=str(tmp_path / "f.db"))
    pipe = FredPipeline(cfg, client=client, warehouse=wh)

    run1 = pipe.run([_spec("X", load_type="incremental")])
    assert "observation_start" not in client.calls[0]        # first load = full
    assert run1.series_runs[0].load_type == "full"

    run2 = pipe.run([_spec("X", load_type="incremental")])
    assert client.calls[1]["observation_start"] == "2024-01-04"  # last 2 records
    assert run2.series_runs[0].load_type == "restate_last_2"
    wh.close()


def test_load_type_full_always_full(tmp_path):
    cfg = _config(restate_last_n=2)
    client = RecordingClient(_payload(DATES))
    wh = LocalWarehouse(cfg, db_path=str(tmp_path / "f.db"))
    pipe = FredPipeline(cfg, client=client, warehouse=wh)

    pipe.run([_spec("X", load_type="full")])
    pipe.run([_spec("X", load_type="full")])  # data now present, but forced full
    assert all("observation_start" not in c for c in client.calls)
    wh.close()


def test_per_series_restate_override(tmp_path):
    cfg = _config(restate_last_n=90)   # global default large
    client = RecordingClient(_payload(DATES))
    wh = LocalWarehouse(cfg, db_path=str(tmp_path / "f.db"))
    pipe = FredPipeline(cfg, client=client, warehouse=wh)

    pipe.run([_spec("X", load_type="incremental", restate_records=1)])
    pipe.run([_spec("X", load_type="incremental", restate_records=1)])
    # override wins: only the single most-recent record restated
    assert client.calls[1]["observation_start"] == "2024-01-05"
    wh.close()


def test_dry_run_without_backend_is_full():
    client = RecordingClient(_payload(DATES))
    pipe = FredPipeline(_config(), client=client, warehouse=None, spark=None)
    pipe.run([_spec("X", load_type="incremental")])
    pipe.run([_spec("X", load_type="incremental")])
    assert all("observation_start" not in c for c in client.calls)
