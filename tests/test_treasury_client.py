"""Tests for the US Treasury (Fiscal Data) source client."""

import pytest

from fred_pipeline.audit import RunStatus
from fred_pipeline.config import Environment, PipelineConfig
from fred_pipeline.local_store import LocalWarehouse
from fred_pipeline.manifest import SeriesSpec
from fred_pipeline.pipeline import FredPipeline
from fred_pipeline.quality import run_quality_checks
from fred_pipeline.sources.base import SourceClient
from fred_pipeline.sources.treasury import (
    TreasuryAPIError,
    TreasuryClient,
    normalize_treasury_observations,
)
from fred_pipeline.transform import SILVER_COLUMNS

SID = "v2/accounting/od/debt_to_penny:tot_pub_debt_out_amt"


def _payload(records, total_pages=1):
    return {"data": records, "meta": {"total-pages": total_pages, "count": len(records)}}


RECORDS = [
    {"record_date": "2024-12-31", "tot_pub_debt_out_amt": "36218605031090.31"},
    {"record_date": "2024-12-30", "tot_pub_debt_out_amt": "36160380870997.60"},
]


def _client(session, **kw):
    return TreasuryClient(session=session, sleep=lambda _s: None, **kw)


def _config():
    return PipelineConfig(environment=Environment.DEV, fred_api_key="k")


# ---- transport / parsing / errors ----------------------------------------

def test_get_observations_builds_endpoint_and_filter(fake_session_cls, fake_response_cls):
    session = fake_session_cls([fake_response_cls(_payload(RECORDS))])
    _client(session).get_observations(SID, observation_start="2024-01-01")
    call = session.calls[0]
    assert call["url"].endswith("/v2/accounting/od/debt_to_penny")
    assert call["params"]["fields"] == "record_date,tot_pub_debt_out_amt"
    assert call["params"]["filter"] == "record_date:gte:2024-01-01"
    assert call["params"]["format"] == "json"


def test_bad_series_id_raises():
    with pytest.raises(TreasuryAPIError):
        normalize_treasury_observations("no_colon_here", {"data": []})


def test_error_body_surfaced(fake_session_cls, fake_response_cls):
    session = fake_session_cls([
        fake_response_cls({"error": "Field not found"}, status_code=400),
    ])
    with pytest.raises(TreasuryAPIError) as exc:
        _client(session).get_observations(SID)
    assert "Field not found" in str(exc.value)


def test_pagination_merges_pages(fake_session_cls, fake_response_cls):
    session = fake_session_cls([
        fake_response_cls(_payload([RECORDS[0]], total_pages=2)),
        fake_response_cls(_payload([RECORDS[1]], total_pages=2)),
    ])
    out = _client(session).get_observations(SID)
    assert len(out["data"]) == 2
    assert len(session.calls) == 2
    assert session.calls[1]["params"]["page[number]"] == 2


# ---- normalization -------------------------------------------------------

def test_normalize_matches_canonical_silver_schema():
    rows = normalize_treasury_observations(SID, _payload(RECORDS), run_id="r")
    assert len(rows) == 2
    for row in rows:
        assert set(row.keys()) == set(SILVER_COLUMNS)
        assert row["source"] == "treasury"
    by_date = {r["observation_date"]: r for r in rows}
    assert by_date["2024-12-31"]["value"] == pytest.approx(36218605031090.31)


# ---- downstream + orchestrator routing -----------------------------------

def test_treasury_rows_pass_dq_and_merge(tmp_path):
    rows = normalize_treasury_observations(SID, _payload(RECORDS), run_id="r")
    report = run_quality_checks(SID, rows, profile="standard", frequency="d",
                                min_value=0)
    assert report.passed, [f.message for f in report.failures]

    wh = LocalWarehouse(_config(), db_path=str(tmp_path / "f.db"))
    assert wh.merge_silver(rows) == 2
    wh.merge_silver(rows)  # idempotent
    assert wh.query("SELECT count(*) c FROM silver_fred_observation")[0]["c"] == 2
    wh.close()


def test_pipeline_routes_treasury_end_to_end(tmp_path, fake_session_cls,
                                             fake_response_cls):
    session = fake_session_cls([fake_response_cls(_payload(RECORDS))])
    client = TreasuryClient(session=session, sleep=lambda _s: None)
    wh = LocalWarehouse(_config(), db_path=str(tmp_path / "f.db"))
    pipe = FredPipeline(_config(), clients={"treasury": client}, warehouse=wh,
                        persist_audit=False)

    spec = SeriesSpec(series_id=SID, title="Debt", frequency="d",
                      source="treasury", vintage_enabled=False)
    run = pipe.run([spec], build_gold_layer=False)

    assert run.status == RunStatus.SUCCEEDED
    assert session.calls[0]["url"].endswith("/v2/accounting/od/debt_to_penny")
    # source-accurate Bronze lineage + Silver rows tagged with their source
    bronze = wh.query("SELECT source, endpoint, observation_count "
                      "FROM bronze_fred_api_response")
    assert bronze == [{"source": "treasury",
                       "endpoint": "v2/accounting/od/debt_to_penny",
                       "observation_count": 2}]
    rows = wh.query("SELECT source, count(*) c FROM silver_fred_observation "
                    "GROUP BY source")
    assert rows == [{"source": "treasury", "c": 2}]
    wh.close()


def test_treasury_client_satisfies_source_protocol():
    assert isinstance(TreasuryClient(session=object()), SourceClient)
