"""Tests for the BIS Central Bank Policy Rates source client."""

import pytest

from fred_pipeline.audit import RunStatus
from fred_pipeline.config import Environment, PipelineConfig
from fred_pipeline.local_store import LocalWarehouse
from fred_pipeline.manifest import SeriesSpec
from fred_pipeline.pipeline import SOURCE_FACTORIES, FredPipeline
from fred_pipeline.quality import run_quality_checks
from fred_pipeline.sources.base import SourceClient
from fred_pipeline.sources.bis import (
    BIS_ACCEPT,
    BISAPIError,
    BISClient,
    _bis_period_to_date,
    normalize_bis_observations,
)
from fred_pipeline.transform import SILVER_COLUMNS

SID = "BIS:GB"

CSV_DATA = (
    "DATAFLOW,FREQ,REF_AREA,TIME_PERIOD,OBS_VALUE\n"
    "BIS:WS_CBPOL(1.0),M,GB,2024-05,5.25\n"
    "BIS:WS_CBPOL(1.0),M,GB,2024-06,5.00\n"
)


def _payload(csv_text=CSV_DATA):
    return {
        "data": csv_text,
        "meta": {
            "series_id": SID,
            "dataflow": "WS_CBPOL",
            "frequency": "M",
            "ref_area": "GB",
            "format": "sdmx-csv",
        },
    }


def _client(session, **kw):
    return BISClient(session=session, sleep=lambda _s: None, **kw)


def _config():
    return PipelineConfig(environment=Environment.DEV, fred_api_key="k")


# ---- transport / errors --------------------------------------------------


def test_get_observations_builds_endpoint_headers_and_periods(
    fake_session_cls, fake_response_cls
):
    session = fake_session_cls([fake_response_cls({}, text=CSV_DATA)])
    payload = _client(session).get_observations(
        SID, observation_start="2024-05-15", observation_end="2024-06-30"
    )

    call = session.calls[0]
    assert call["url"].endswith("/data/dataflow/BIS/WS_CBPOL/1.0/M.GB")
    assert call["params"] == {"startPeriod": "2024-05", "endPeriod": "2024-06"}
    assert call["headers"]["Accept"] == BIS_ACCEPT
    assert payload["data"] == CSV_DATA
    assert payload["meta"]["ref_area"] == "GB"


def test_bad_series_id_raises():
    with pytest.raises(BISAPIError):
        BISClient(session=object()).observations_endpoint("GB")


# ---- date mapping / normalization ----------------------------------------


@pytest.mark.parametrize(
    "period, expected",
    [
        ("2024-05", "2024-05-01"),
        ("2024-05-17", "2024-05-17"),
        ("2024M5", "2024-05-01"),
        ("202405", "2024-05-01"),
        ("nonsense", None),
    ],
)
def test_bis_period_mapping(period, expected):
    assert _bis_period_to_date(period) == expected


def test_normalize_matches_canonical_silver_schema():
    rows = normalize_bis_observations(SID, _payload(), run_id="r")
    assert len(rows) == 2
    for row in rows:
        assert set(row.keys()) == set(SILVER_COLUMNS)
        assert row["source"] == "bis"
        assert row["realtime_start"] == ""
        assert row["realtime_end"] == ""
    by_date = {r["observation_date"]: r for r in rows}
    assert by_date["2024-05-01"]["value"] == pytest.approx(5.25)
    assert by_date["2024-05-01"]["raw_value"] == "5.25"


def test_blank_value_is_missing():
    payload = _payload("FREQ,REF_AREA,TIME_PERIOD,OBS_VALUE\nM,GB,2024-05,\n")
    rows = normalize_bis_observations(SID, payload)
    assert rows[0]["is_missing"] is True
    assert rows[0]["value"] is None


# ---- downstream + orchestrator routing -----------------------------------


def test_bis_rows_pass_dq_and_merge(tmp_path):
    rows = normalize_bis_observations(SID, _payload(), run_id="r")
    report = run_quality_checks(
        SID, rows, profile="standard", frequency="m", min_value=-5, max_value=25
    )
    assert report.passed, [f.message for f in report.failures]

    wh = LocalWarehouse(_config(), db_path=str(tmp_path / "f.db"))
    assert wh.merge_silver(rows) == 2
    wh.merge_silver(rows)  # idempotent
    assert wh.query("SELECT count(*) c FROM silver_fred_observation")[0]["c"] == 2
    wh.close()


def test_pipeline_routes_bis_end_to_end(tmp_path, fake_session_cls, fake_response_cls):
    session = fake_session_cls([fake_response_cls({}, text=CSV_DATA)])
    client = BISClient(session=session, sleep=lambda _s: None)
    wh = LocalWarehouse(_config(), db_path=str(tmp_path / "f.db"))
    pipe = FredPipeline(
        _config(), clients={"bis": client}, warehouse=wh, persist_audit=False
    )

    spec = SeriesSpec(
        series_id=SID,
        title="UK Policy Rate",
        frequency="m",
        source="bis",
        vintage_enabled=False,
    )
    run = pipe.run([spec], build_gold_layer=False)

    assert run.status == RunStatus.SUCCEEDED
    assert session.calls[0]["url"].endswith("/data/dataflow/BIS/WS_CBPOL/1.0/M.GB")
    rows = wh.query(
        "SELECT source, count(*) c FROM silver_fred_observation GROUP BY source"
    )
    assert rows == [{"source": "bis", "c": 2}]
    wh.close()


def test_bis_client_satisfies_source_protocol():
    assert isinstance(BISClient(session=object()), SourceClient)


def test_bis_source_is_registered():
    assert "bis" in SOURCE_FACTORIES
