"""Equity slice: Stooq CSV client, iShares holdings ingester, the symbol-
universe generator, and the equity Gold engines."""

from __future__ import annotations

import pytest

from fred_pipeline.sources.ishares import (
    build_equity_manifest,
    normalize_ishares_holdings,
)
from fred_pipeline.sources.stooq import (
    StooqAPIError,
    _parse_series_id,
    _stooq_symbol,
    normalize_stooq_observations,
)
from fred_pipeline.equity_views import (
    compute_equity_return_daily,
    compute_index_constituents,
)


# ---- Stooq ------------------------------------------------------------------

def test_stooq_series_id_parsing_and_symbol():
    assert _parse_series_id("AAPL") == ("AAPL", "close")
    assert _parse_series_id("aapl:volume") == ("AAPL", "volume")
    assert _stooq_symbol("AAPL") == "aapl.us"
    assert _stooq_symbol("CBA.au") == "cba.au"   # already-suffixed left alone
    with pytest.raises(StooqAPIError):
        _parse_series_id(":close")
    with pytest.raises(StooqAPIError):
        _parse_series_id("AAPL:adjclose")        # unsupported field


_STOOQ_CSV = (
    "Date,Open,High,Low,Close,Volume\n"
    "2024-01-02,185.0,186.0,184.0,185.64,50000\n"
    "2024-01-03,184.0,185.5,183.0,184.25,60000\n"
    "2024-01-04,184.5,188.0,184.2,187.10,72000\n"
)


def test_stooq_normalize_selects_field():
    payload = {"format": "csv", "field": "close", "text": _STOOQ_CSV}
    rows = normalize_stooq_observations("AAPL:close", payload)
    assert [r["observation_date"] for r in rows] == [
        "2024-01-02", "2024-01-03", "2024-01-04"]
    assert [r["value"] for r in rows] == pytest.approx([185.64, 184.25, 187.10])
    assert all(r["source"] == "stooq" and r["realtime_start"] == "" for r in rows)
    # volume field selects the Volume column
    vol = normalize_stooq_observations(
        "AAPL:volume", {"format": "csv", "field": "volume", "text": _STOOQ_CSV})
    assert [r["value"] for r in vol] == pytest.approx([50000, 60000, 72000])


def test_stooq_normalize_tolerates_bad_body():
    for bad in ("N/D", "", "No data\n", "garbage,header\n1,2\n"):
        assert normalize_stooq_observations(
            "AAPL:close", {"format": "csv", "field": "close", "text": bad}) == []
    assert normalize_stooq_observations("AAPL:close", None) == []


def test_stooq_get_observations_uses_text_transport():
    class FakeResp:
        status_code = 200
        text = _STOOQ_CSV

        def json(self):  # must NOT be called for CSV
            raise AssertionError("json() called on a CSV response")

    class FakeSession:
        def __init__(self):
            self.last = None

        def get(self, url, params=None, timeout=None, headers=None):
            self.last = (url, params)
            return FakeResp()

    from fred_pipeline.sources.stooq import StooqClient
    sess = FakeSession()
    client = StooqClient(session=sess, sleep=lambda _s: None)
    payload = client.get_observations("AAPL:close", observation_start="2024-01-01")
    assert payload["field"] == "close" and "185.64" in payload["text"]
    url, params = sess.last
    assert params["s"] == "aapl.us" and params["d1"] == "20240101"
    rows = client.normalize("AAPL:close", payload)
    assert len(rows) == 3


# ---- iShares holdings -------------------------------------------------------

_IVV_CSV = (
    'iShares Core S&P 500 ETF\n'
    '"Fund Holdings as of","Jul 31, 2024"\n'
    '"Inception Date","May 15, 2000"\n'
    '\n'
    'Ticker,Name,Sector,Asset Class,Weight (%),Shares\n'
    'AAPL,APPLE INC,Information Technology,Equity,6.95,1000000\n'
    'MSFT,MICROSOFT CORP,Information Technology,Equity,6.55,500000\n'
    'NVDA,NVIDIA CORP,Information Technology,Equity,5.10,300000\n'
    'USD,US DOLLAR,Cash and/or Derivatives,Cash,0.05,0\n'
)


def test_ishares_normalize_explodes_constituents():
    payload = {"format": "csv", "etf": "IVV", "text": _IVV_CSV}
    rows = normalize_ishares_holdings("IVV", payload)
    assert [r["series_id"] for r in rows] == [
        "IVV:AAPL", "IVV:MSFT", "IVV:NVDA"]          # USD cash row dropped
    assert all(r["observation_date"] == "2024-07-31" for r in rows)
    assert rows[0]["value"] == pytest.approx(6.95)
    assert rows[0]["shares"] == pytest.approx(1000000)
    assert rows[0]["source"] == "ishares"


def test_ishares_normalize_requires_asof_and_header():
    # no "as of" line -> skip (won't guess a date)
    no_date = _IVV_CSV.replace('"Fund Holdings as of","Jul 31, 2024"\n', "")
    assert normalize_ishares_holdings(
        "IVV", {"format": "csv", "etf": "IVV", "text": no_date}) == []
    # no header row -> skip
    assert normalize_ishares_holdings(
        "IVV", {"format": "csv", "etf": "IVV",
                "text": '"Fund Holdings as of","Jul 31, 2024"\nfoo,bar\n1,2\n'}) == []


def test_build_equity_manifest_from_holdings():
    man = build_equity_manifest(_IVV_CSV, include_etf="IVV")
    ids = [s["series_id"] for s in man["series"]]
    assert ids == ["IVV:close", "AAPL:close", "MSFT:close", "NVDA:close"]
    assert all(s["source"] == "stooq" and s["active"] is False
               for s in man["series"])
    assert man["name"] == "equity_stooq"


# ---- Gold engines -----------------------------------------------------------

def _close_row(ticker, date, close, is_missing=False):
    return {"series_id": f"{ticker}:close", "observation_date": date,
            "value": close, "is_missing": is_missing}


def test_equity_return_daily_and_index():
    rows = [
        _close_row("AAPL", "2024-01-02", 100.0),
        _close_row("AAPL", "2024-01-03", 101.0),   # +1%
        _close_row("AAPL", "2024-01-04", 99.99),   # -1% -> back near 100
        _close_row("SPY", "2024-01-02", 470.0),
    ]
    out = compute_equity_return_daily(rows)
    aapl = [r for r in out if r["ticker"] == "AAPL"]
    assert aapl[0]["price_return"] is None and aapl[0]["price_return_index"] == 100.0
    assert aapl[1]["price_return"] == pytest.approx(0.01)
    assert aapl[1]["price_return_index"] == pytest.approx(101.0)
    assert aapl[2]["price_return"] == pytest.approx((99.99 - 101.0) / 101.0)
    assert aapl[2]["price_return_index"] == pytest.approx(100.0 * 1.01 * (99.99 / 101.0))
    # SPY single obs -> one row, null return
    spy = [r for r in out if r["ticker"] == "SPY"]
    assert len(spy) == 1 and spy[0]["price_return"] is None


def test_equity_return_ignores_non_close_and_missing():
    rows = [
        _close_row("AAPL", "2024-01-02", 100.0),
        {"series_id": "AAPL:volume", "observation_date": "2024-01-02",
         "value": 5000.0, "is_missing": False},         # not a :close series
        _close_row("AAPL", "2024-01-03", 200.0, is_missing=True),  # missing
    ]
    out = compute_equity_return_daily(rows)
    assert [r["ticker"] for r in out] == ["AAPL"]       # one valid close row
    assert out[0]["close"] == pytest.approx(100.0)


def _weight_row(etf, constituent, date, weight):
    return {"series_id": f"{etf}:{constituent}", "observation_date": date,
            "value": weight, "is_missing": False}


def test_index_constituents_ranks_and_latest_flag():
    rows = [
        _weight_row("IVV", "AAPL", "2024-06-30", 6.5),
        _weight_row("IVV", "MSFT", "2024-06-30", 6.9),
        _weight_row("IVV", "AAPL", "2024-07-31", 7.0),   # newer snapshot
        _weight_row("IVV", "MSFT", "2024-07-31", 6.6),
    ]
    out = compute_index_constituents(rows)
    jul = [r for r in out if r["observation_date"] == "2024-07-31"]
    jun = [r for r in out if r["observation_date"] == "2024-06-30"]
    # ranked by weight within snapshot
    assert [(r["constituent"], r["weight_rank"]) for r in jul] == [
        ("AAPL", 1), ("MSFT", 2)]
    assert [(r["constituent"], r["weight_rank"]) for r in jun] == [
        ("MSFT", 1), ("AAPL", 2)]
    # only the newest snapshot is flagged latest
    assert all(r["is_latest_snapshot"] for r in jul)
    assert not any(r["is_latest_snapshot"] for r in jun)


def test_index_constituents_bounds_by_etf_set():
    # a FOO:close price series must not be mistaken for a constituent
    rows = [_weight_row("IVV", "AAPL", "2024-07-31", 7.0),
            {"series_id": "FOO:close", "observation_date": "2024-07-31",
             "value": 12.0, "is_missing": False}]
    out = compute_index_constituents(rows, etfs=("IVV",))
    assert [r["constituent"] for r in out] == ["AAPL"]


# ---- manifests + registry ---------------------------------------------------

def test_equity_manifests_parse_and_register():
    from fred_pipeline.manifest import load_manifests
    from fred_pipeline.pipeline import SOURCE_FACTORIES

    assert "stooq" in SOURCE_FACTORIES and "ishares" in SOURCE_FACTORIES
    mans = load_manifests("manifests")
    by_name = {m.name: m for m in mans}
    assert "equity_stooq" in by_name and "etf_holdings" in by_name
    stooq_series = by_name["equity_stooq"].series
    assert all(s.source == "stooq" and not s.active for s in stooq_series)
    assert any(s.series_id == "SPY:close" for s in stooq_series)


def test_equity_local_build_gold(tmp_path):
    from fred_pipeline.config import Environment, PipelineConfig
    from fred_pipeline.local_store import LocalWarehouse

    wh = LocalWarehouse(
        PipelineConfig(environment=Environment.DEV, fred_api_key="k"),
        db_path=str(tmp_path / "eq.db"),
    )
    silver = []
    for i, px in enumerate([100.0, 101.0, 99.99]):
        silver.append({
            "source": "stooq", "series_id": "AAPL:close",
            "observation_date": f"2024-01-0{i + 2}", "realtime_start": "",
            "realtime_end": "", "value": px, "raw_value": str(px),
            "is_missing": 0, "row_hash": f"a{i}", "revision_number": 1,
            "ingested_at": "2024-02-01T00:00:00", "run_id": "r1",
        })
    for tk, wt in (("AAPL", 7.0), ("MSFT", 6.6)):
        silver.append({
            "source": "ishares", "series_id": f"IVV:{tk}",
            "observation_date": "2024-07-31", "realtime_start": "",
            "realtime_end": "", "value": wt, "raw_value": str(wt),
            "is_missing": 0, "row_hash": f"h{tk}", "revision_number": 1,
            "ingested_at": "2024-08-01T00:00:00", "run_id": "r1",
        })
    wh.merge_silver(silver)
    results = wh.build_gold()
    assert results["equity_return_daily"] == "ok"
    assert results["index_constituents"] == "ok"

    ret = wh.query("SELECT * FROM gold_equity_return_daily ORDER BY observation_date")
    assert [r["ticker"] for r in ret] == ["AAPL", "AAPL", "AAPL"]
    assert ret[1]["price_return"] == pytest.approx(0.01)
    cons = wh.query("SELECT * FROM gold_index_constituents "
                    "WHERE is_latest_snapshot = 1 ORDER BY weight_rank")
    assert [r["constituent"] for r in cons] == ["AAPL", "MSFT"]
    wh.close()
