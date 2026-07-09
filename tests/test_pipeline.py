from fred_pipeline.audit import RunStatus
from fred_pipeline.config import Environment, PipelineConfig
from fred_pipeline.fred_client import FredAPIError
from fred_pipeline.manifest import SeriesSpec, ValidationProfile
from fred_pipeline.pipeline import FredPipeline


def _config():
    return PipelineConfig(environment=Environment.DEV, fred_api_key="k")


def _spec(series_id, **kw):
    kw.setdefault("title", series_id)
    kw.setdefault("frequency", "d")
    return SeriesSpec(series_id=series_id, **kw)


def test_run_without_spark_records_audit(observations_payload, fake_client_cls):
    client = fake_client_cls({"DGS10": observations_payload})
    pipe = FredPipeline(_config(), client=client, spark=None, persist_audit=False)
    run = pipe.run([_spec("DGS10")])

    assert run.status == RunStatus.SUCCEEDED
    assert run.series_total == 1
    assert run.series_succeeded == 1
    sr = run.series_runs[0]
    assert sr.status == RunStatus.SUCCEEDED
    assert sr.observations_extracted == 4
    assert sr.dq_passed is True
    assert sr.duration_seconds is not None


def test_concurrent_extraction_preserves_order_and_isolation(
    observations_payload, fake_client_cls
):
    """Threaded extraction (phase 2) must still yield deterministic,
    per-series-isolated results in original spec order once phase 3 replays
    them sequentially — regardless of worker count or completion order."""
    client = fake_client_cls(
        {sid: observations_payload for sid in ("A", "B", "C", "D", "BAD_E", "F")},
        errors={"BAD_E": FredAPIError("boom", 400)},
    )
    config = PipelineConfig(
        environment=Environment.DEV, fred_api_key="k", extract_workers=4
    )
    pipe = FredPipeline(config, client=client, spark=None, persist_audit=False)
    specs = [_spec(sid) for sid in ("A", "B", "C", "D", "BAD_E", "F")]
    run = pipe.run(specs)

    assert run.status == RunStatus.PARTIAL
    assert run.series_succeeded == 5
    assert run.series_failed == 1
    # audit rows preserve the original spec order regardless of thread timing
    assert [sr.series_id for sr in run.series_runs] == ["A", "B", "C", "D", "BAD_E", "F"]
    bad = run.series_runs[4]
    assert bad.status == RunStatus.FAILED
    assert "boom" in bad.error_message
    for sid in ("A", "B", "C", "D", "F"):
        sr = [s for s in run.series_runs if s.series_id == sid][0]
        assert sr.status == RunStatus.SUCCEEDED


def test_series_failure_is_isolated(observations_payload, fake_client_cls):
    client = fake_client_cls(
        {"GOOD": observations_payload},
        errors={"BAD": FredAPIError("boom", 400)},
    )
    pipe = FredPipeline(_config(), client=client, spark=None, persist_audit=False)
    run = pipe.run([_spec("BAD"), _spec("GOOD")])

    assert run.status == RunStatus.PARTIAL
    assert run.series_succeeded == 1
    assert run.series_failed == 1
    bad = [s for s in run.series_runs if s.series_id == "BAD"][0]
    assert bad.status == RunStatus.FAILED
    assert "boom" in bad.error_message


def test_strict_profile_fails_run_on_dq(fake_client_cls):
    all_missing = {"observations": [
        {"date": "2024-01-01", "value": ".", "realtime_start": "2024-01-02",
         "realtime_end": "9999-12-31"},
        {"date": "2024-01-02", "value": ".", "realtime_start": "2024-01-03",
         "realtime_end": "9999-12-31"},
    ]}
    client = fake_client_cls({"CPIAUCSL": all_missing})
    pipe = FredPipeline(_config(), client=client, spark=None, persist_audit=False)
    run = pipe.run([_spec("CPIAUCSL", validation_profile=ValidationProfile.STRICT)])

    assert run.status == RunStatus.FAILED
    sr = run.series_runs[0]
    assert sr.status == RunStatus.FAILED
    assert sr.dq_passed is False
    assert "Data quality failed" in sr.error_message


def test_vintage_series_requests_full_realtime_window(observations_payload):
    captured = {}

    class RecordingClient:
        def get_observations(self, series_id, **kwargs):
            captured.update(kwargs)
            return observations_payload

    pipe = FredPipeline(_config(), client=RecordingClient(), spark=None, persist_audit=False)
    pipe.run([_spec("GDP", vintage_enabled=True)])
    assert captured.get("realtime_start") == "1776-07-04"
    assert captured.get("realtime_end") == "9999-12-31"


def test_run_from_manifest_series_filter(observations_payload, fake_client_cls):
    client = fake_client_cls({sid: observations_payload for sid in
                              ("DGS10", "UNRATE", "GDP")})
    pipe = FredPipeline(_config(), client=client, spark=None, persist_audit=False)
    run = pipe.run_from_manifest(
        "manifests", series=["DGS10", "GDP"], build_gold_layer=False,
    )
    assert {s.series_id for s in run.series_runs} == {"DGS10", "GDP"}
    assert set(client.requested) == {"DGS10", "GDP"}


def test_force_full_ignores_watermark(observations_payload):
    from fred_pipeline.local_store import LocalWarehouse
    import tempfile, os

    captured = []

    class RecordingClient:
        def get_observations(self, series_id, **kwargs):
            captured.append(kwargs)
            return observations_payload

    with tempfile.TemporaryDirectory() as d:
        cfg = PipelineConfig(environment=Environment.DEV, fred_api_key="k",
                             restate_last_n=2)
        wh = LocalWarehouse(cfg, db_path=os.path.join(d, "f.db"))
        pipe = FredPipeline(cfg, client=RecordingClient(), warehouse=wh)
        pipe.run([_spec("DGS10")])              # first load (full)
        pipe.run([_spec("DGS10")], force_full=True)  # force full despite data
        assert all("observation_start" not in c for c in captured)
        wh.close()


def test_config_table_naming():
    cfg = PipelineConfig(environment=Environment.PROD, fred_api_key="k")
    assert cfg.catalog == "macro_prod"
    assert cfg.table("silver", "fred_observation") == "macro_prod.silver.fred_observation"
