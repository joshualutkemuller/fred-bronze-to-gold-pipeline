"""Tests for the World Bank (Indicators) source client."""

import pytest

from fred_pipeline.audit import RunStatus
from fred_pipeline.config import Environment, PipelineConfig
from fred_pipeline.local_store import LocalWarehouse
from fred_pipeline.manifest import SeriesSpec
from fred_pipeline.pipeline import FredPipeline
from fred_pipeline.quality import run_quality_checks
from fred_pipeline.sources.base import SourceClient
from fred_pipeline.sources.worldbank import (
    WorldBankAPIError,
    WorldBankClient,
    _wb_date,
    normalize_worldbank_observations,
)
from fred_pipeline.transform import SILVER_COLUMNS

SID = "USA:NY.GDP.MKTP.CD"


def _payload(data, pages=1):
    return [{"page": 1, "pages": pages, "per_page": 1000, "total": len(data)}, data]


DATA = [
    {"indicator": {"id": "NY.GDP.MKTP.CD"}, "country": {"id": "US"},
     "date": "2023", "value": 27360935000000.0},
    {"indicator": {"id": "NY.GDP.MKTP.CD"}, "country": {"id": "US"},
     "date": "2022", "value": 25744100000000.0},
]


def _client(session, **kw):
    return WorldBankClient(session=session, sleep=lambda _s: None, **kw)


def _config():
    return PipelineConfig(environment=Environment.DEV, fred_api_key="k")


# ---- transport / errors --------------------------------------------------

def test_get_observations_builds_endpoint_and_format(fake_session_cls, fake_response_cls):
    session = fake_session_cls([fake_response_cls(_payload(DATA))])
    _client(session).get_observations(SID, observation_start="2000-01-01")
    call = session.calls[0]
    assert call["url"].endswith("/country/USA/indicator/NY.GDP.MKTP.CD")
    assert call["params"]["format"] == "json"
    assert call["params"]["date"] == "2000:2100"


def test_error_message_body_raises_on_http_200(fake_session_cls, fake_response_cls):
    # World Bank returns HTTP 200 with a message envelope on a bad request.
    err = [{"message": [{"id": "120", "key": "Parameter 'indicator'",
                         "value": "The provided parameter value is not valid"}]}]
    session = fake_session_cls([fake_response_cls(err, status_code=200)])
    with pytest.raises(WorldBankAPIError) as exc:
        _client(session).get_observations("USA:BOGUS")
    assert "not valid" in str(exc.value)


# ---- date mapping / normalization ----------------------------------------

@pytest.mark.parametrize("period, expected", [
    ("2023", "2023-01-01"),
    ("2023Q2", "2023-04-01"),
    ("2023M03", "2023-03-01"),
    ("nonsense", None),
])
def test_wb_date_mapping(period, expected):
    assert _wb_date(period) == expected


def test_normalize_matches_canonical_silver_schema():
    rows = normalize_worldbank_observations(SID, _payload(DATA), run_id="r")
    assert len(rows) == 2
    for row in rows:
        assert set(row.keys()) == set(SILVER_COLUMNS)
        assert row["source"] == "worldbank"
    by_date = {r["observation_date"]: r for r in rows}
    assert by_date["2023-01-01"]["value"] == pytest.approx(27360935000000.0)


def test_null_value_is_missing():
    data = [{"date": "2021", "value": None}]
    rows = normalize_worldbank_observations(SID, _payload(data))
    assert rows[0]["is_missing"] is True
    assert rows[0]["value"] is None


# ---- downstream + orchestrator routing -----------------------------------

def test_worldbank_rows_pass_dq_and_merge(tmp_path):
    rows = normalize_worldbank_observations(SID, _payload(DATA), run_id="r")
    report = run_quality_checks(SID, rows, profile="standard", frequency="a",
                                min_value=0)
    assert report.passed, [f.message for f in report.failures]

    wh = LocalWarehouse(_config(), db_path=str(tmp_path / "f.db"))
    assert wh.merge_silver(rows) == 2
    wh.merge_silver(rows)  # idempotent
    assert wh.query("SELECT count(*) c FROM silver_fred_observation")[0]["c"] == 2
    wh.close()


def test_pipeline_routes_worldbank_end_to_end(tmp_path, fake_session_cls,
                                              fake_response_cls):
    session = fake_session_cls([fake_response_cls(_payload(DATA))])
    client = WorldBankClient(session=session, sleep=lambda _s: None)
    wh = LocalWarehouse(_config(), db_path=str(tmp_path / "f.db"))
    pipe = FredPipeline(_config(), clients={"worldbank": client}, warehouse=wh,
                        persist_audit=False)

    spec = SeriesSpec(series_id=SID, title="US GDP", frequency="a",
                      source="worldbank", vintage_enabled=False)
    run = pipe.run([spec], build_gold_layer=False)

    assert run.status == RunStatus.SUCCEEDED
    assert session.calls[0]["url"].endswith("/country/USA/indicator/NY.GDP.MKTP.CD")
    rows = wh.query("SELECT source, count(*) c FROM silver_fred_observation "
                    "GROUP BY source")
    assert rows == [{"source": "worldbank", "c": 2}]
    wh.close()


def test_worldbank_client_satisfies_source_protocol():
    assert isinstance(WorldBankClient(session=object()), SourceClient)
