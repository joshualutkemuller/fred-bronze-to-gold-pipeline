"""Tests for the BLS source client and its normalizer.

The point of these tests is to show a *second*, differently-shaped API drops
into the same pipeline plumbing: the BLS payload normalizes into the canonical
silver schema, passes the shared DQ checks, and MERGEs into the warehouse the
same way FRED rows do.
"""

import pytest

from fred_pipeline.config import Environment, PipelineConfig
from fred_pipeline.local_store import LocalWarehouse
from fred_pipeline.quality import run_quality_checks
from fred_pipeline.sources.bls import BLSAPIError, BLSClient, normalize_bls_observations
from fred_pipeline.transform import SILVER_COLUMNS


def _payload(data, status="REQUEST_SUCCEEDED", series_id="CUUR0000SA0"):
    return {
        "status": status,
        "responseTime": 12,
        "message": [] if status == "REQUEST_SUCCEEDED" else ["invalid series"],
        "Results": {"series": [{"seriesID": series_id, "data": data}]},
    }


MONTHLY = [
    {"year": "2024", "period": "M12", "periodName": "December", "value": "315.6"},
    {"year": "2024", "period": "M11", "periodName": "November", "value": "314.4"},
    {"year": "2024", "period": "M13", "periodName": "Annual", "value": "313.0"},  # avg
]


def _client(session, **kw):
    return BLSClient(session=session, sleep=lambda _s: None, **kw)


# ---- transport / auth / errors -------------------------------------------

def test_get_observations_success_injects_key(fake_session_cls, fake_response_cls):
    session = fake_session_cls([fake_response_cls(_payload(MONTHLY))])
    client = _client(session, api_key="bls-key")
    out = client.get_observations("CUUR0000SA0", observation_start="2024-01-01",
                                  observation_end="2024-12-31")
    assert out["status"] == "REQUEST_SUCCEEDED"
    params = session.calls[0]["params"]
    assert params["registrationkey"] == "bls-key"
    # year window derived from ISO observation bounds
    assert params["startyear"] == "2024"
    assert params["endyear"] == "2024"
    # series id is in the path, not the query
    assert session.calls[0]["url"].endswith("/timeseries/data/CUUR0000SA0")


def test_keyless_omits_registrationkey(fake_session_cls, fake_response_cls):
    session = fake_session_cls([fake_response_cls(_payload(MONTHLY))])
    _client(session).get_observations("CUUR0000SA0")
    assert "registrationkey" not in session.calls[0]["params"]


def test_logical_failure_raises_even_on_http_200(fake_session_cls, fake_response_cls):
    # BLS returns HTTP 200 with a failure status; the client must surface it.
    session = fake_session_cls([
        fake_response_cls(_payload([], status="REQUEST_NOT_PROCESSED"), status_code=200),
    ])
    with pytest.raises(BLSAPIError):
        _client(session).get_observations("BAD")


# ---- normalization -------------------------------------------------------

def test_normalize_matches_canonical_silver_schema():
    rows = normalize_bls_observations("CUUR0000SA0", _payload(MONTHLY), run_id="r")
    # M13 (annual average) is dropped; two monthly points remain
    assert len(rows) == 2
    for row in rows:
        assert set(row.keys()) == set(SILVER_COLUMNS)
    by_date = {r["observation_date"]: r for r in rows}
    assert by_date["2024-12-01"]["value"] == 315.6
    assert by_date["2024-11-01"]["value"] == 314.4
    # non-vintage convention: realtime blanked so the MERGE key collapses
    assert all(r["realtime_start"] == "" for r in rows)


def test_period_mapping_quarterly_and_missing():
    data = [
        {"year": "2023", "period": "Q02", "value": "5.1"},    # -> Apr 1
        {"year": "2023", "period": "A01", "value": "5.0"},    # -> Jan 1
        {"year": "2023", "period": "M05", "value": "-"},      # -> May 1, missing
    ]
    rows = normalize_bls_observations("X", _payload(data))
    by_date = {r["observation_date"]: r for r in rows}
    assert by_date["2023-04-01"]["value"] == 5.1   # quarter start month
    assert by_date["2023-01-01"]["value"] == 5.0   # annual -> Jan
    # unparseable "-" sentinel -> is_missing, value None
    assert by_date["2023-05-01"]["is_missing"] is True
    assert by_date["2023-05-01"]["value"] is None


# ---- downstream compatibility (the payoff) -------------------------------

def test_bls_rows_pass_dq_and_merge_into_warehouse(tmp_path):
    rows = normalize_bls_observations("CUUR0000SA0", _payload(MONTHLY), run_id="r")

    report = run_quality_checks("CUUR0000SA0", rows, profile="standard",
                                frequency="m")
    assert report.passed, [f.message for f in report.failures]

    cfg = PipelineConfig(environment=Environment.DEV, fred_api_key="k")
    wh = LocalWarehouse(cfg, db_path=str(tmp_path / "f.db"))
    merged = wh.merge_silver(rows)
    assert merged == 2
    # idempotent: merging the same BLS rows again does not duplicate
    wh.merge_silver(rows)
    n = wh.query("SELECT count(*) c FROM silver_fred_observation")[0]["c"]
    assert n == 2
    wh.close()
