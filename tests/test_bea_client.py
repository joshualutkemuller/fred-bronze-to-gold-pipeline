"""Tests for the BEA source client."""

import pytest

from fred_pipeline.audit import RunStatus
from fred_pipeline.config import Environment, PipelineConfig
from fred_pipeline.local_store import LocalWarehouse
from fred_pipeline.manifest import SeriesSpec
from fred_pipeline.pipeline import FredPipeline
from fred_pipeline.quality import run_quality_checks
from fred_pipeline.sources.base import SourceClient
from fred_pipeline.sources.bea import BEAAPIError, BEAClient, normalize_bea_observations
from fred_pipeline.transform import SILVER_COLUMNS

SID = "NIPA:T10101:1:Q"


def _payload(data):
    return {"BEAAPI": {"Results": {"Statistic": "GDP", "Data": data}}}


DATA = [
    {"TableName": "T10101", "LineNumber": "1", "TimePeriod": "2024Q1", "DataValue": "1.6"},
    {"TableName": "T10101", "LineNumber": "1", "TimePeriod": "2024Q2", "DataValue": "3.0"},
    {"TableName": "T10101", "LineNumber": "2", "TimePeriod": "2024Q1", "DataValue": "9.9"},
]


def _client(session, **kw):
    return BEAClient(api_key="bea-key", session=session, sleep=lambda _s: None, **kw)


def _config():
    return PipelineConfig(environment=Environment.DEV, fred_api_key="k")


# ---- transport / auth / errors -------------------------------------------

def test_requires_api_key():
    with pytest.raises(BEAAPIError):
        BEAClient(api_key="", session=object())


def test_get_observations_builds_params(fake_session_cls, fake_response_cls):
    session = fake_session_cls([fake_response_cls(_payload(DATA))])
    _client(session).get_observations(SID)
    params = session.calls[0]["params"]
    assert params["UserID"] == "bea-key"
    assert params["datasetname"] == "NIPA"
    assert params["TableName"] == "T10101"
    assert params["Frequency"] == "Q"
    assert params["method"] == "GetData"


def test_error_block_raises(fake_session_cls, fake_response_cls):
    body = {"BEAAPI": {"Error": {"APIErrorDescription": "Invalid table name"}}}
    session = fake_session_cls([fake_response_cls(body)])
    with pytest.raises(BEAAPIError) as exc:
        _client(session).get_observations(SID)
    assert "Invalid table name" in str(exc.value)


# ---- normalization -------------------------------------------------------

def test_normalize_selects_line_and_maps_schema():
    rows = normalize_bea_observations(SID, _payload(DATA), run_id="r")
    # only LineNumber == 1 rows for this series (line 2 dropped)
    assert len(rows) == 2
    for row in rows:
        assert set(row.keys()) == set(SILVER_COLUMNS)
        assert row["source"] == "bea"
    by_date = {r["observation_date"]: r for r in rows}
    assert by_date["2024-01-01"]["value"] == 1.6   # Q1 -> Jan
    assert by_date["2024-04-01"]["value"] == 3.0   # Q2 -> Apr


def test_bad_series_id_raises():
    with pytest.raises(BEAAPIError):
        normalize_bea_observations("NIPA:T10101", {"BEAAPI": {}})


# ---- downstream + orchestrator routing -----------------------------------

def test_bea_rows_pass_dq_and_merge(tmp_path):
    rows = normalize_bea_observations(SID, _payload(DATA), run_id="r")
    report = run_quality_checks(SID, rows, profile="standard", frequency="q")
    assert report.passed, [f.message for f in report.failures]

    wh = LocalWarehouse(_config(), db_path=str(tmp_path / "f.db"))
    assert wh.merge_silver(rows) == 2
    wh.merge_silver(rows)  # idempotent
    assert wh.query("SELECT count(*) c FROM silver_fred_observation")[0]["c"] == 2
    wh.close()


def test_pipeline_routes_bea_end_to_end(tmp_path, fake_session_cls, fake_response_cls):
    session = fake_session_cls([fake_response_cls(_payload(DATA))])
    client = BEAClient(api_key="bea-key", session=session, sleep=lambda _s: None)
    wh = LocalWarehouse(_config(), db_path=str(tmp_path / "f.db"))
    pipe = FredPipeline(_config(), clients={"bea": client}, warehouse=wh,
                        persist_audit=False)

    spec = SeriesSpec(series_id=SID, title="Real GDP", frequency="q",
                      source="bea", vintage_enabled=False)
    run = pipe.run([spec], build_gold_layer=False)

    assert run.status == RunStatus.SUCCEEDED
    rows = wh.query("SELECT source, count(*) c FROM silver_fred_observation "
                    "GROUP BY source")
    assert rows == [{"source": "bea", "c": 2}]
    wh.close()


def test_bea_client_satisfies_source_protocol():
    assert isinstance(BEAClient(api_key="k", session=object()), SourceClient)
