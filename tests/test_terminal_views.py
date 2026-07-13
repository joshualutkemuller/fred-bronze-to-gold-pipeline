"""Market-terminal analytical views: catalog/curve configs, the pure-Python
engines (dimensions, ECON dashboard, Curve Lab, enriched spreads), and their
integration into the local backend's build_gold."""

from __future__ import annotations

import pytest

from fred_pipeline.catalog_config import (
    CatalogConfigError,
    CatalogEntry,
    load_series_catalog,
)
from fred_pipeline.curve_config import (
    CurveConfigError,
    FALLBACK_TENORS,
    TenorDef,
    load_curve_defs,
)
from fred_pipeline.spread_config import SpreadDef
from fred_pipeline.terminal_views import (
    build_dim_date,
    build_dim_series,
    compute_curve_spread_daily,
    compute_macro_dashboard,
    compute_spread_inversion_episodes,
    compute_treasury_curve,
)


def _row(series_id, date, value, realtime_start="2024-01-01", is_missing=False):
    return {
        "series_id": series_id,
        "observation_date": date,
        "value": value,
        "realtime_start": realtime_start,
        "is_missing": is_missing,
    }


def _monthly(series_id, start_year, values):
    """Monthly rows starting January of start_year."""
    rows = []
    y, m = start_year, 1
    for v in values:
        rows.append(_row(series_id, f"{y:04d}-{m:02d}-01", v))
        m += 1
        if m > 12:
            y, m = y + 1, 1
    return rows


# ---- config loaders ---------------------------------------------------------

def test_load_series_catalog_missing_file_is_empty(tmp_path):
    assert load_series_catalog(str(tmp_path / "nope.yml")) == []


def test_load_series_catalog_parses_and_validates(tmp_path):
    p = tmp_path / "catalog.yml"
    p.write_text(
        "series:\n"
        "  - {series_id: UNRATE, econ_category: LABOR, polarity: -1,"
        " default_transform: level}\n"
        "  - {series_id: PAYEMS, econ_category: LABOR, polarity: 1,"
        " default_transform: chg, surprise_window: 6}\n"
    )
    entries = load_series_catalog(str(p))
    assert [e.series_id for e in entries] == ["UNRATE", "PAYEMS"]
    assert entries[0].polarity == -1
    assert entries[1].surprise_window == 6


@pytest.mark.parametrize("body", [
    "series:\n  - {series_id: X, econ_category: NOPE}\n",           # bad category
    "series:\n  - {series_id: X, econ_category: LABOR, polarity: 2}\n",
    "series:\n  - {series_id: X, econ_category: LABOR, default_transform: yoy}\n",
    "series:\n  - {series_id: X, econ_category: LABOR, bogus: 1}\n",  # unknown field
    ("series:\n  - {series_id: X, econ_category: LABOR}\n"
     "  - {series_id: X, econ_category: RATES}\n"),                  # duplicate
])
def test_load_series_catalog_rejects_malformed(tmp_path, body):
    p = tmp_path / "catalog.yml"
    p.write_text(body)
    with pytest.raises(CatalogConfigError):
        load_series_catalog(str(p))


def test_repo_series_catalog_parses():
    entries = load_series_catalog("config/series_catalog.yml")
    assert len(entries) > 40
    assert any(e.series_id == "UNRATE" and e.polarity == -1 for e in entries)


def test_load_curve_defs_fallback_and_repo_file(tmp_path):
    assert load_curve_defs(str(tmp_path / "nope.yml")) == list(FALLBACK_TENORS)
    tenors = load_curve_defs("config/curve.yml")
    assert [t.label for t in tenors][:3] == ["1M", "3M", "6M"]
    assert tenors == sorted(tenors, key=lambda t: t.months)


def test_load_curve_defs_rejects_duplicates(tmp_path):
    p = tmp_path / "curve.yml"
    p.write_text(
        "tenors:\n"
        "  - {label: 2Y, months: 24, series_id: DGS2}\n"
        "  - {label: 2Y, months: 25, series_id: DGS2X}\n"
    )
    with pytest.raises(CurveConfigError):
        load_curve_defs(str(p))


# ---- dimensions -------------------------------------------------------------

def test_build_dim_series_merges_meta():
    catalog = [CatalogEntry("UNRATE", "LABOR", polarity=-1)]
    meta = [{"series_id": "UNRATE", "title": "Unemployment Rate",
             "frequency": "m", "units": "Percent"}]
    (row,) = build_dim_series(catalog, meta)
    assert row["title"] == "Unemployment Rate"
    assert row["econ_category"] == "LABOR"
    assert row["polarity"] == -1
    # not in meta -> blank descriptive fields, catalog fields intact
    (bare,) = build_dim_series([CatalogEntry("X", "RATES")], [])
    assert bare["title"] == "" and bare["econ_category"] == "RATES"


def test_build_dim_date_calendar_attributes():
    rows = build_dim_date("2024-09-29", "2024-10-02")
    assert [r["date"] for r in rows] == [
        "2024-09-29", "2024-09-30", "2024-10-01", "2024-10-02"]
    sep30 = rows[1]
    assert sep30["is_month_end"] is True
    assert sep30["quarter"] == 3 and sep30["fiscal_year"] == 2024
    oct1 = rows[2]
    assert oct1["is_month_end"] is False
    assert oct1["quarter"] == 4 and oct1["fiscal_year"] == 2025  # US federal FY
    # no USREC ingested -> unknown, not false
    assert all(r["is_recession"] is None for r in rows)


def test_build_dim_date_recession_flag():
    usrec = [_row("USREC", "2020-03-01", 1.0), _row("USREC", "2020-05-01", 0.0)]
    rows = {r["date"]: r for r in build_dim_date("2020-03-01", "2020-05-02", usrec)}
    assert rows["2020-03-15"]["is_recession"] is True
    assert rows["2020-04-30"]["is_recession"] is True   # carries March/April print
    assert rows["2020-05-02"]["is_recession"] is False
    # before the first USREC print -> unknown
    early = build_dim_date("2020-02-01", "2020-02-01", usrec)
    assert early[0]["is_recession"] is None


# ---- ECON macro dashboard ---------------------------------------------------

def _labor_catalog(window=12):
    return [
        CatalogEntry("UNRATE", "LABOR", polarity=-1, surprise_window=window),
        CatalogEntry("PAYEMS", "LABOR", polarity=1, default_transform="chg",
                     surprise_window=window),
    ]


def test_macro_dashboard_core_columns():
    # 14 months of UNRATE: falls from 5.0 to 3.7 then ticks UP 0.2 at the end.
    unrate = [5.0 - 0.1 * i for i in range(13)] + [3.9]
    rows = _monthly("UNRATE", 2023, unrate)
    out = compute_macro_dashboard(rows, _labor_catalog())
    (d,) = out["dashboard"]
    assert d["series_id"] == "UNRATE"
    assert d["latest_value"] == pytest.approx(3.9)
    assert d["prior_value"] == pytest.approx(3.8)
    assert d["change_abs"] == pytest.approx(0.1)
    # date-based YoY: Feb 2024 vs Feb 2023
    assert d["yoy_pct"] == pytest.approx((3.9 - 4.9) / 4.9)
    # unemployment rising with polarity -1 -> deteriorating
    assert d["direction_is_good"] is False
    # surprise: latest minus trailing-12 mean (months 2..13 of the decline)
    window = unrate[1:13]
    assert d["surprise"] == pytest.approx(3.9 - sum(window) / len(window))
    assert d["staleness_days"] == 0
    assert d["as_of_date"] == "2024-02-01"


def test_macro_dashboard_polarity_and_summary():
    unrate = _monthly("UNRATE", 2023, [4.0, 3.9, 3.8])       # falling -> good
    payems = _monthly("PAYEMS", 2023, [157000, 157200, 157400])  # rising -> good
    out = compute_macro_dashboard(unrate + payems, _labor_catalog(window=2))
    by_id = {r["series_id"]: r for r in out["dashboard"]}
    assert by_id["UNRATE"]["direction_is_good"] is True
    assert by_id["PAYEMS"]["direction_is_good"] is True
    (cat,) = out["category_summary"]
    assert cat["econ_category"] == "LABOR"
    assert cat["n_series"] == 2
    assert cat["n_improving"] == 2 and cat["n_deteriorating"] == 0
    assert cat["breadth_pct"] == pytest.approx(1.0)


def test_macro_dashboard_sparkline_capped_and_ordered():
    rows = _monthly("UNRATE", 2020, [4.0 + 0.01 * i for i in range(50)])
    out = compute_macro_dashboard(
        rows, [CatalogEntry("UNRATE", "LABOR", polarity=-1)])
    spark = out["sparkline"]
    assert len(spark) == 36  # capped at SPARK_POINTS
    assert [p["point_index"] for p in spark] == list(range(36))
    assert spark[-1]["value"] == pytest.approx(4.49)  # newest = last point
    (d,) = out["dashboard"]
    assert d["spark_min"] == pytest.approx(spark[0]["value"])
    assert d["spark_max"] == pytest.approx(4.49)


def test_macro_dashboard_staleness_across_series():
    # PAYEMS is a month behind UNRATE; as_of is the global max date.
    unrate = _monthly("UNRATE", 2023, [4.0, 3.9, 3.8])
    payems = _monthly("PAYEMS", 2023, [157000, 157200])
    out = compute_macro_dashboard(unrate + payems, _labor_catalog(window=2))
    by_id = {r["series_id"]: r for r in out["dashboard"]}
    assert by_id["UNRATE"]["staleness_days"] == 0
    assert by_id["PAYEMS"]["staleness_days"] == 28  # 2023-02-01 -> 2023-03-01


def test_macro_dashboard_ignores_uncataloged_and_missing():
    rows = _monthly("UNRATE", 2023, [4.0, 3.9])
    rows += _monthly("NOT_IN_CATALOG", 2023, [1.0, 2.0])
    rows.append(_row("UNRATE", "2023-03-01", 99.0, is_missing=True))
    out = compute_macro_dashboard(
        rows, [CatalogEntry("UNRATE", "LABOR", polarity=-1)])
    (d,) = out["dashboard"]
    assert d["latest_value"] == pytest.approx(3.9)  # missing row excluded
    assert {r["series_id"] for r in out["sparkline"]} == {"UNRATE"}


def test_macro_dashboard_empty_catalog():
    out = compute_macro_dashboard(_monthly("UNRATE", 2023, [4.0]), [])
    assert out == {"dashboard": [], "sparkline": [], "category_summary": []}


# ---- Treasury Curve Lab -----------------------------------------------------

_TENORS = [
    TenorDef("3M", 3, "DGS3MO"), TenorDef("2Y", 24, "DGS2"),
    TenorDef("5Y", 60, "DGS5"), TenorDef("10Y", 120, "DGS10"),
    TenorDef("30Y", 360, "DGS30"),
]


def _curve_day(d, y3m, y2, y5, y10, y30):
    return [
        _row("DGS3MO", d, y3m), _row("DGS2", d, y2), _row("DGS5", d, y5),
        _row("DGS10", d, y10), _row("DGS30", d, y30),
    ]


def test_treasury_curve_rows_and_metrics():
    rows = _curve_day("2024-01-02", 5.4, 4.3, 4.0, 4.0, 4.2)
    out = compute_treasury_curve(rows, _TENORS)
    assert [c["tenor_label"] for c in out["curve"]] == ["3M", "2Y", "5Y", "10Y", "30Y"]
    assert out["curve"][0]["yield_pct"] == pytest.approx(5.4)
    (m,) = out["metrics"]
    assert m["level"] == pytest.approx((5.4 + 4.3 + 4.0 + 4.0 + 4.2) / 5)
    assert m["slope_10y2y"] == pytest.approx(-0.3)
    assert m["slope_10y3m"] == pytest.approx(-1.4)
    assert m["is_inverted_10y2y"] is True and m["is_inverted_10y3m"] is True
    assert m["curvature_2_5_10"] == pytest.approx(2 * 4.0 - 4.3 - 4.0)
    assert m["butterfly_2_10_30"] == pytest.approx(2 * 4.0 - 4.3 - 4.2)
    assert m["curve_move"] is None          # first date has no prior
    assert m["is_recession"] is None        # no USREC ingested


def test_treasury_curve_move_classification():
    rows = (
        _curve_day("2024-01-02", 5.4, 4.3, 4.0, 4.0, 4.2)
        # everything down, 2s10s wider -> bull steepener
        + _curve_day("2024-01-03", 5.2, 4.0, 3.9, 3.9, 4.1)
        # everything up, 2s10s tighter -> bear flattener
        + _curve_day("2024-01-04", 5.4, 4.5, 4.2, 4.1, 4.3)
    )
    out = compute_treasury_curve(rows, _TENORS)
    moves = [m["curve_move"] for m in out["metrics"]]
    assert moves == [None, "bull-steepener", "bear-flattener"]


def test_treasury_curve_skips_absent_tenors():
    # Only 3 of 5 tenors ingested: curve emits what exists; 2-5-10 curvature
    # is defined, 30Y-dependent metrics are not.
    rows = [_row("DGS2", "2024-01-02", 4.3), _row("DGS5", "2024-01-02", 4.0),
            _row("DGS10", "2024-01-02", 4.0)]
    out = compute_treasury_curve(rows, _TENORS)
    assert len(out["curve"]) == 3
    (m,) = out["metrics"]
    assert m["slope_10y3m"] is None
    assert m["butterfly_2_10_30"] is None
    assert m["curvature_2_5_10"] is not None


# ---- enriched spread history --------------------------------------------------

def test_curve_spread_daily_enrichment():
    spreads = [SpreadDef("T10Y2Y", "DGS10", "DGS2")]
    rows = []
    # 4 days: positive, inverts for 2 days, re-steepens
    for d, y10, y2 in [
        ("2024-01-02", 4.0, 3.8), ("2024-01-03", 4.0, 4.1),
        ("2024-01-04", 4.0, 4.2), ("2024-01-05", 4.3, 4.0),
    ]:
        rows += [_row("DGS10", d, y10), _row("DGS2", d, y2)]
    out = compute_curve_spread_daily(rows, spreads)
    assert [r["value"] for r in out] == pytest.approx([0.2, -0.1, -0.2, 0.3])
    assert [r["value_bps"] for r in out] == pytest.approx([20.0, -10.0, -20.0, 30.0])
    assert [r["is_inverted"] for r in out] == [False, True, True, False]
    assert [r["inversion_run"] for r in out] == [0, 1, 2, 0]
    # expanding stats: first obs has no percentile, zscore null (std=0)
    assert out[0]["zscore"] is None and out[0]["percentile"] is None
    assert out[3]["percentile"] == pytest.approx(1.0)  # highest value so far
    assert all(r["is_recession"] is None for r in out)  # no USREC


def test_curve_spread_daily_ratio_has_no_inversion_semantics():
    spreads = [SpreadDef("RATIO", "DGS10", "DGS2", op="ratio")]
    rows = [_row("DGS10", "2024-01-02", 4.0), _row("DGS2", "2024-01-02", 2.0)]
    (r,) = compute_curve_spread_daily(rows, spreads)
    assert r["value"] == pytest.approx(2.0)
    assert r["value_bps"] is None
    assert r["is_inverted"] is None and r["inversion_run"] is None


def test_curve_spread_daily_recession_overlay():
    spreads = [SpreadDef("T10Y2Y", "DGS10", "DGS2")]
    rows = [
        _row("DGS10", "2020-03-15", 0.8), _row("DGS2", "2020-03-15", 0.4),
        _row("USREC", "2020-03-01", 1.0),
    ]
    (r,) = compute_curve_spread_daily(rows, spreads)
    assert r["is_recession"] is True


# ---- inversion episodes -------------------------------------------------------

def _spread_days(pairs):
    """(date, y10, y2) triples -> latest rows for DGS10/DGS2."""
    rows = []
    for d, y10, y2 in pairs:
        rows += [_row("DGS10", d, y10), _row("DGS2", d, y2)]
    return rows


def test_inversion_episodes_split_on_resteepening():
    spreads = [SpreadDef("T10Y2Y", "DGS10", "DGS2")]
    rows = _spread_days([
        ("2024-01-01", 4.0, 3.8),   # +0.2
        ("2024-01-02", 4.0, 4.1),   # -0.1  episode 1 starts
        ("2024-01-03", 4.0, 4.3),   # -0.3  trough
        ("2024-01-04", 4.0, 4.2),   # -0.2
        ("2024-01-05", 4.2, 4.1),   # +0.1  episode 1 ends here
        ("2024-01-06", 4.0, 4.1),   # -0.1  episode 2 starts (new unique period)
        ("2024-01-07", 4.3, 4.0),   # +0.3  episode 2 ends here
    ])
    eps = compute_spread_inversion_episodes(rows, spreads)
    assert len(eps) == 2
    e1, e2 = eps
    assert (e1["episode_number"], e2["episode_number"]) == (1, 2)
    assert e1["start_date"] == "2024-01-02"
    assert e1["end_date"] == "2024-01-05"          # first non-negative print
    assert e1["last_inverted_date"] == "2024-01-04"
    assert e1["observation_count"] == 3
    assert e1["calendar_days"] == 3                # Jan 2 -> Jan 5
    assert e1["trough_value"] == pytest.approx(-0.3)
    assert e1["trough_bps"] == pytest.approx(-30.0)
    assert e1["trough_date"] == "2024-01-03"
    assert e1["is_ongoing"] is False
    assert e2["start_date"] == "2024-01-06" and e2["end_date"] == "2024-01-07"
    assert e2["observation_count"] == 1


def test_inversion_episode_ongoing_at_end_of_history():
    spreads = [SpreadDef("T10Y2Y", "DGS10", "DGS2")]
    rows = _spread_days([
        ("2024-01-01", 4.0, 4.1),   # inverted from the first observation
        ("2024-01-03", 4.0, 4.2),   # still inverted at end of history
    ])
    (ep,) = compute_spread_inversion_episodes(rows, spreads)
    assert ep["start_date"] == "2024-01-01"
    assert ep["end_date"] is None and ep["is_ongoing"] is True
    assert ep["last_inverted_date"] == "2024-01-03"
    assert ep["calendar_days"] == 2                # measured to last inverted obs
    assert ep["observation_count"] == 2


def test_inversion_episode_zero_is_not_inverted():
    # value == 0 matches is_inverted (v < 0) elsewhere: it closes an episode.
    spreads = [SpreadDef("T10Y2Y", "DGS10", "DGS2")]
    rows = _spread_days([
        ("2024-01-01", 4.0, 4.1),   # -0.1
        ("2024-01-02", 4.0, 4.0),   #  0.0 -> episode ends
        ("2024-01-03", 4.0, 4.1),   # -0.1 -> new episode, ongoing
    ])
    eps = compute_spread_inversion_episodes(rows, spreads)
    assert [e["episode_number"] for e in eps] == [1, 2]
    assert eps[0]["end_date"] == "2024-01-02"
    assert eps[1]["is_ongoing"] is True


def test_inversion_episodes_never_inverted_and_ratio_excluded():
    spreads = [
        SpreadDef("T10Y2Y", "DGS10", "DGS2"),
        SpreadDef("RATIO", "DGS10", "DGS2", op="ratio"),
    ]
    rows = _spread_days([("2024-01-01", 4.2, 4.0), ("2024-01-02", 4.3, 4.0)])
    assert compute_spread_inversion_episodes(rows, spreads) == []


def test_inversion_episode_recession_overlap():
    spreads = [SpreadDef("T10Y2Y", "DGS10", "DGS2")]
    rows = _spread_days([
        ("2020-02-15", 1.2, 1.3),   # inverted, pre-recession print unknown
        ("2020-03-15", 1.0, 1.2),   # inverted, in recession
        ("2020-04-15", 1.5, 1.0),   # re-steepens
    ]) + [_row("USREC", "2020-03-01", 1.0)]
    (ep,) = compute_spread_inversion_episodes(rows, spreads)
    assert ep["recession_overlap"] is True
    # without USREC ingested the overlap is unknown, not false
    (ep2,) = compute_spread_inversion_episodes(
        _spread_days([("2020-02-15", 1.2, 1.3), ("2020-04-15", 1.5, 1.0)]),
        spreads,
    )
    assert ep2["recession_overlap"] is None


# ---- local backend integration ------------------------------------------------

def test_local_build_gold_populates_terminal_views(tmp_path, monkeypatch):
    from fred_pipeline.config import Environment, PipelineConfig
    from fred_pipeline.local_store import LocalWarehouse

    # Point the dashboard at a temp catalog covering our test series.
    catalog = tmp_path / "catalog.yml"
    catalog.write_text(
        "series:\n"
        "  - {series_id: UNRATE, econ_category: LABOR, polarity: -1,"
        " surprise_window: 2}\n"
    )
    monkeypatch.setenv("FRED_SERIES_CATALOG_FILE", str(catalog))

    wh = LocalWarehouse(
        PipelineConfig(environment=Environment.DEV, fred_api_key="k"),
        db_path=str(tmp_path / "t.db"),
    )
    silver = []
    for i, v in enumerate([4.0, 3.9, 3.8, 3.9]):
        silver.append({
            "source": "fred", "series_id": "UNRATE",
            "observation_date": f"2024-0{i + 1}-01",
            "realtime_start": "", "realtime_end": "",
            "value": v, "raw_value": str(v), "is_missing": 0,
            "row_hash": f"h{i}", "revision_number": 1,
            "ingested_at": "2024-05-01T00:00:00", "run_id": "r1",
        })
    for d, y10, y2 in [("2024-01-02", 4.0, 3.8), ("2024-01-03", 4.0, 4.1)]:
        for sid, v in (("DGS10", y10), ("DGS2", y2)):
            silver.append({
                "source": "fred", "series_id": sid, "observation_date": d,
                "realtime_start": "", "realtime_end": "",
                "value": v, "raw_value": str(v), "is_missing": 0,
                "row_hash": f"{sid}{d}", "revision_number": 1,
                "ingested_at": "2024-05-01T00:00:00", "run_id": "r1",
            })
    wh.merge_silver(silver)
    results = wh.build_gold()
    for key in ("dim_series", "dim_date", "macro_indicator_dashboard",
                "macro_indicator_sparkline", "macro_category_summary",
                "treasury_curve", "treasury_curve_metrics",
                "curve_spread_daily", "spread_inversion_episode"):
        assert results[key] == "ok"

    (dash,) = wh.query("SELECT * FROM gold_macro_indicator_dashboard")
    assert dash["series_id"] == "UNRATE"
    assert dash["direction_is_good"] == 0  # ticked up, polarity -1 -> stored int
    (dim,) = wh.query("SELECT * FROM gold_dim_series")
    assert dim["econ_category"] == "LABOR"
    curve = wh.query(
        "SELECT * FROM gold_treasury_curve ORDER BY as_of_date, tenor_months")
    assert {c["tenor_label"] for c in curve} == {"2Y", "10Y"}
    metrics = wh.query(
        "SELECT * FROM gold_treasury_curve_metrics ORDER BY as_of_date")
    assert metrics[0]["is_inverted_10y2y"] == 0
    assert metrics[1]["is_inverted_10y2y"] == 1
    spread = wh.query(
        "SELECT * FROM gold_curve_spread_daily WHERE spread_name='T10Y2Y' "
        "ORDER BY observation_date")
    assert [r["inversion_run"] for r in spread] == [0, 1]
    (ep,) = wh.query(
        "SELECT * FROM gold_spread_inversion_episode "
        "WHERE spread_name='T10Y2Y'")
    assert ep["start_date"] == "2024-01-03"
    assert ep["end_date"] is None and ep["is_ongoing"] == 1  # stored int
    # dim_date spans the observed range with month attributes
    days = wh.query("SELECT COUNT(*) AS n, MIN(date) AS lo, MAX(date) AS hi "
                    "FROM gold_dim_date")[0]
    assert days["lo"] == "2024-01-01" and days["hi"] == "2024-04-01"
    wh.close()


# ---- Phase 4: rates complex ----------------------------------------------------

from fred_pipeline.rates_complex_config import (  # noqa: E402
    BenchmarkBoardConfig,
    BenchmarkRateDef,
    CreditConfig,
    CreditInstrumentDef,
    FundingConfig,
    FundingMetricDef,
    FundingSpreadDef,
    RatesComplexConfigError,
    StressComponent,
    load_benchmark_board,
    load_credit_config,
    load_funding_config,
)
from fred_pipeline.terminal_views import (  # noqa: E402
    compute_benchmark_rate_board,
    compute_credit_spread_daily,
    compute_funding_features,
)


def test_rates_complex_loaders_missing_files_are_empty(tmp_path):
    assert load_benchmark_board(str(tmp_path / "n.yml")).rates == ()
    cfg = load_funding_config(str(tmp_path / "n.yml"))
    assert cfg.metrics == () and cfg.spreads == ()
    assert load_credit_config(str(tmp_path / "n.yml")).instruments == ()


def test_rates_complex_repo_configs_parse():
    board = load_benchmark_board("config/benchmark_rates.yml")
    assert any(r.series_id == "SOFR" and r.benchmark == "EFFR" for r in board.rates)
    funding = load_funding_config("config/funding.yml")
    assert {s.name for s in funding.spreads} >= {"SOFR_EFFR", "SOFR_IORB"}
    assert all(
        c.spread in {s.name for s in funding.spreads}
        for c in funding.stress_components
    )
    credit = load_credit_config("config/credit.yml")
    assert any(c.instrument == "HY_OAS" for c in credit.instruments)
    assert credit.stress_percentile == pytest.approx(0.90)


def test_rates_complex_loaders_reject_malformed(tmp_path):
    p = tmp_path / "bad.yml"
    p.write_text("rates:\n  - {series_id: X, label: L, category: C, bogus: 1}\n")
    with pytest.raises(RatesComplexConfigError):
        load_benchmark_board(str(p))
    p.write_text(
        "metrics:\n  - {name: A, series_id: X, metric_type: nope}\n"
    )
    with pytest.raises(RatesComplexConfigError):
        load_funding_config(str(p))
    p.write_text(  # stress component referencing an unconfigured spread
        "metrics:\n  - {name: A, series_id: X, metric_type: rate}\n"
        "spreads:\n  - {name: S, long_leg: X, short_leg: Y}\n"
        "stress:\n  components:\n    - {spread: NOPE}\n"
    )
    with pytest.raises(RatesComplexConfigError):
        load_funding_config(str(p))
    p.write_text("instruments:\n  - {instrument: I, series_id: X}\n"
                 "stress_percentile: 1.5\n")
    with pytest.raises(RatesComplexConfigError):
        load_credit_config(str(p))


def _board():
    return BenchmarkBoardConfig(rates=(
        BenchmarkRateDef("SOFR", "SOFR", "secured_overnight", benchmark="EFFR"),
        BenchmarkRateDef("EFFR", "EFFR", "policy"),
        BenchmarkRateDef("IORB", "IORB", "policy"),  # not ingested -> no row
    ), trend_window=2)


def test_benchmark_rate_board_columns():
    rows = []
    for i, (d, sofr, effr) in enumerate([
        ("2024-01-02", 5.31, 5.33), ("2024-01-03", 5.32, 5.33),
        ("2024-01-04", 5.34, 5.33), ("2024-01-05", 5.38, 5.33),
    ]):
        rows += [_row("SOFR", d, sofr), _row("EFFR", d, effr)]
    out = compute_benchmark_rate_board(rows, _board())
    by_id = {r["series_id"]: r for r in out}
    assert set(by_id) == {"SOFR", "EFFR"}   # IORB absent -> no row
    sofr = by_id["SOFR"]
    assert sofr["latest_value"] == pytest.approx(5.38)
    assert sofr["change_bps"] == pytest.approx(4.0)
    # trend: latest (5.38) vs 2 obs ago (5.32) -> rising -> tightening
    assert sofr["trend"] == "rising" and sofr["regime"] == "tightening"
    assert sofr["spread_to_benchmark_bps"] == pytest.approx(5.0)
    assert sofr["staleness_days"] == 0
    effr = by_id["EFFR"]
    # flat within the 1bp dead-band -> stable, no benchmark spread
    assert effr["trend"] == "flat" and effr["regime"] == "stable"
    assert effr["spread_to_benchmark_bps"] is None
    assert effr["benchmark_series"] is None


def test_benchmark_rate_board_short_history_has_no_trend():
    rows = [_row("SOFR", "2024-01-02", 5.31), _row("SOFR", "2024-01-03", 5.32)]
    (r,) = compute_benchmark_rate_board(rows, _board())
    assert r["trend"] is None and r["regime"] is None
    assert r["prior_value"] == pytest.approx(5.31)


def _funding_cfg():
    return FundingConfig(
        metrics=(
            FundingMetricDef("SOFR", "SOFR", "rate"),
            FundingMetricDef("EFFR", "EFFR", "rate"),
            FundingMetricDef("RESERVES", "WRESBAL", "balance"),
        ),
        spreads=(FundingSpreadDef("SOFR_EFFR", "SOFR", "EFFR"),),
        stress_components=(StressComponent("SOFR_EFFR", 1.0),),
    )


def test_funding_tape_and_stress():
    rows = []
    # SOFR prints daily; EFFR too; reserves weekly. Last day SOFR spikes.
    for d, sofr, effr in [
        ("2024-01-02", 5.31, 5.33), ("2024-01-03", 5.31, 5.33),
        ("2024-01-04", 5.32, 5.33), ("2024-01-05", 5.45, 5.33),
    ]:
        rows += [_row("SOFR", d, sofr), _row("EFFR", d, effr)]
    rows.append(_row("WRESBAL", "2024-01-03", 3500.0))
    out = compute_funding_features(rows, _funding_cfg())
    tape = out["tape"]
    types = {(r["metric_name"], r["metric_type"]) for r in tape}
    assert ("SOFR", "rate") in types and ("RESERVES", "balance") in types
    assert ("SOFR_EFFR", "spread") in types
    spread_rows = [r for r in tape if r["metric_name"] == "SOFR_EFFR"]
    assert [r["value"] for r in spread_rows] == pytest.approx(
        [-0.02, -0.02, -0.01, 0.12])
    # stress: one row per date where the (only) component prints
    stress = out["stress"]
    assert [s["observation_date"] for s in stress] == [
        "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"]
    # first obs has no z (expanding std 0) -> neutral 50 / "normal"
    assert stress[0]["stress_score"] == pytest.approx(50.0)
    assert stress[0]["stress_bucket"] == "normal"
    # the spike maps to the top bucket, clamped at 100
    assert stress[-1]["stress_score"] > 80.0
    assert stress[-1]["stress_bucket"] == "stressed"
    assert all(s["n_components"] == 1 for s in stress)


def test_funding_stress_requires_all_components():
    cfg = FundingConfig(
        metrics=(),
        spreads=(
            FundingSpreadDef("SOFR_EFFR", "SOFR", "EFFR"),
            FundingSpreadDef("SOFR_IORB", "SOFR", "IORB"),
        ),
        stress_components=(
            StressComponent("SOFR_EFFR"), StressComponent("SOFR_IORB"),
        ),
    )
    rows = [_row("SOFR", "2024-01-02", 5.31), _row("EFFR", "2024-01-02", 5.33)]
    out = compute_funding_features(rows, cfg)  # IORB absent
    assert [r["metric_name"] for r in out["tape"]] == ["SOFR_EFFR"]
    assert out["stress"] == []   # gauge needs every component


def test_credit_spread_daily_stress_and_recession():
    cfg = CreditConfig(
        instruments=(CreditInstrumentDef("HY_OAS", "BAMLH0A0HYM2", "headline"),),
        stress_percentile=0.75,
    )
    rows = [
        _row("BAMLH0A0HYM2", "2020-02-01", 3.5),
        _row("BAMLH0A0HYM2", "2020-03-01", 4.0),
        _row("BAMLH0A0HYM2", "2020-03-20", 8.7),
        _row("BAMLH0A0HYM2", "2020-04-10", 7.5),
        _row("USREC", "2020-03-01", 1.0),
    ]
    out = compute_credit_spread_daily(rows, cfg)
    assert [r["oas_bps"] for r in out] == pytest.approx([350.0, 400.0, 870.0, 750.0])
    assert out[2]["change_bps"] == pytest.approx(470.0)
    # first obs has no percentile -> unknown, not a stress episode verdict
    assert out[0]["is_stress_episode"] is None
    assert out[2]["is_stress_episode"] is True     # highest so far (pct 1.0)
    assert out[3]["is_stress_episode"] is False    # pct 2/3 < 0.75
    assert out[0]["is_recession"] is None          # before first USREC print
    assert out[2]["is_recession"] is True


def test_credit_spread_daily_absent_series_emit_nothing():
    cfg = CreditConfig(
        instruments=(CreditInstrumentDef("IG_OAS", "BAMLC0A0CM"),))
    assert compute_credit_spread_daily(
        [_row("DGS10", "2024-01-02", 4.0)], cfg) == []
