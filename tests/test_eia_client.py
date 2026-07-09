"""Tests for the EIA source client — a third source proving the abstraction
extends cleanly (one client module + one SOURCE_FACTORIES entry)."""

import pytest

from fred_pipeline.audit import RunStatus
from fred_pipeline.config import Environment, PipelineConfig
from fred_pipeline.local_store import LocalWarehouse
from fred_pipeline.manifest import SeriesSpec
from fred_pipeline.pipeline import FredPipeline
from fred_pipeline.quality import run_quality_checks
from fred_pipeline.sources.base import SourceClient
from fred_pipeline.sources.eia import (
    EIAAPIError,
    EIAClient,
    _eia_period_to_date,
    normalize_eia_observations,
)
from fred_pipeline.transform import SILVER_COLUMNS


def _payload(data):
    return {"response": {"total": len(data), "data": data}}


MONTHLY = [
    {"period": "2024-12", "seriesId": "PET.RWTC.M", "value": 74.2, "units": "$/BBL"},
    {"period": "2024-11", "seriesId": "PET.RWTC.M", "value": 75.1, "units": "$/BBL"},
]


def _client(session, **kw):
    return EIAClient(api_key="eia-key", session=session, sleep=lambda _s: None, **kw)


def _config():
    return PipelineConfig(environment=Environment.DEV, fred_api_key="k")


# ---- transport / auth / errors -------------------------------------------

def test_requires_api_key():
    with pytest.raises(EIAAPIError):
        EIAClient(api_key="", session=object())


def test_get_observations_injects_key_and_bounds(fake_session_cls, fake_response_cls):
    session = fake_session_cls([fake_response_cls(_payload(MONTHLY))])
    client = _client(session)
    client.get_observations("PET.RWTC.M", observation_start="2024-01-01",
                            observation_end="2024-12-31")
    params = session.calls[0]["params"]
    assert params["api_key"] == "eia-key"
    assert params["start"] == "2024-01-01"
    assert params["end"] == "2024-12-31"
    assert session.calls[0]["url"].endswith("/seriesid/PET.RWTC.M")


def test_error_detail_reads_error_field(fake_session_cls, fake_response_cls):
    session = fake_session_cls([
        fake_response_cls({"error": "invalid series id", "code": 404},
                          status_code=404),
    ])
    with pytest.raises(EIAAPIError) as exc:
        _client(session).get_observations("NOPE")
    assert "invalid series id" in str(exc.value)


# ---- period mapping / normalization --------------------------------------

@pytest.mark.parametrize("period, expected", [
    ("2024", "2024-01-01"),
    ("2024-06", "2024-06-01"),
    ("2024-06-15", "2024-06-15"),
    ("2024-Q3", "2024-07-01"),
    ("2024Q1", "2024-01-01"),
    ("nonsense", None),
])
def test_period_mapping(period, expected):
    assert _eia_period_to_date(period) == expected


def test_normalize_matches_canonical_silver_schema():
    rows = normalize_eia_observations("PET.RWTC.M", _payload(MONTHLY), run_id="r")
    assert len(rows) == 2
    for row in rows:
        assert set(row.keys()) == set(SILVER_COLUMNS)
        assert row["source"] == "eia"
    by_date = {r["observation_date"]: r for r in rows}
    assert by_date["2024-12-01"]["value"] == 74.2


# ---- downstream + orchestrator routing -----------------------------------

def test_eia_rows_pass_dq_and_merge(tmp_path):
    rows = normalize_eia_observations("PET.RWTC.M", _payload(MONTHLY), run_id="r")
    report = run_quality_checks("PET.RWTC.M", rows, profile="standard",
                                frequency="m", min_value=0)
    assert report.passed, [f.message for f in report.failures]

    wh = LocalWarehouse(_config(), db_path=str(tmp_path / "f.db"))
    assert wh.merge_silver(rows) == 2
    wh.merge_silver(rows)  # idempotent
    assert wh.query("SELECT count(*) c FROM silver_fred_observation")[0]["c"] == 2
    wh.close()


def test_pipeline_routes_eia_series_end_to_end(tmp_path, fake_session_cls,
                                               fake_response_cls):
    session = fake_session_cls([fake_response_cls(_payload(MONTHLY))])
    eia = EIAClient(api_key="eia-key", session=session, sleep=lambda _s: None)
    wh = LocalWarehouse(_config(), db_path=str(tmp_path / "f.db"))
    pipe = FredPipeline(_config(), clients={"eia": eia}, warehouse=wh,
                        persist_audit=False)

    spec = SeriesSpec(series_id="PET.RWTC.M", title="WTI", frequency="m",
                      source="eia", vintage_enabled=False)
    run = pipe.run([spec], build_gold_layer=False)

    assert run.status == RunStatus.SUCCEEDED
    assert session.calls[0]["url"].endswith("/seriesid/PET.RWTC.M")
    rows = wh.query("SELECT source, count(*) c FROM silver_fred_observation "
                    "GROUP BY source")
    assert rows == [{"source": "eia", "c": 2}]
    wh.close()


def test_eia_client_satisfies_source_protocol():
    assert isinstance(EIAClient(api_key="k", session=object()), SourceClient)
