"""Phase-6 global inflation/policy engines + the Power BI catalog."""

from __future__ import annotations

import pytest

from fred_pipeline.global_config import (
    GlobalConfig,
    GlobalConfigError,
    GlobalInflationDef,
    GlobalPolicyRateDef,
    load_global_config,
)
from fred_pipeline.global_views import (
    POWERBI_CATALOG,
    compute_global_inflation,
    compute_global_policy_rates,
    powerbi_catalog_rows,
)
from tests.test_terminal_views import _monthly, _row


# ---- config loader ---------------------------------------------------------

def test_global_loader_missing_and_repo_file(tmp_path):
    cfg = load_global_config(str(tmp_path / "nope.yml"))
    assert cfg.inflation == () and cfg.policy_rates == ()
    cfg = load_global_config("config/global_series.yml")
    us = next(d for d in cfg.inflation if d.iso3 == "USA")
    assert us.series_id == "CPIAUCSL" and us.transform == "yoy_from_index"
    assert us.target == pytest.approx(2.0)
    assert any(d.series_id == "EMU:FP.CPI.TOTL.ZG" for d in cfg.inflation)
    assert {d.region for d in cfg.inflation} == {"AMER", "EMEA", "APAC"}
    assert any(d.series_id == "FEDFUNDS" for d in cfg.policy_rates)


@pytest.mark.parametrize("body", [
    "inflation:\n  - {country: X, iso3: XXX, region: MARS, series_id: S}\n",
    "inflation:\n  - {country: X, iso3: XXX, region: AMER, series_id: S, transform: log}\n",
    ("inflation:\n  - {country: X, iso3: XXX, region: AMER, series_id: S}\n"
     "  - {country: Y, iso3: XXX, region: AMER, series_id: T}\n"),  # dup iso3
    "policy_rates:\n  - {country: X, iso3: XXX, region: AMER, series_id: S, bogus: 1}\n",
])
def test_global_loader_rejects_malformed(tmp_path, body):
    p = tmp_path / "global.yml"
    p.write_text(body)
    with pytest.raises(GlobalConfigError):
        load_global_config(str(p))


# ---- global inflation --------------------------------------------------------

def _annual(series_id, values, start_year=2018):
    return [
        _row(series_id, f"{start_year + i}-01-01", v)
        for i, v in enumerate(values)
    ]


def test_global_inflation_trend_streaks_and_target():
    cfg = GlobalConfig(inflation=(
        GlobalInflationDef("Euro Area", "EMU", "EMEA",
                           "EMU:FP.CPI.TOTL.ZG", target=2.0),
    ))
    # 1.5 -> 1.5 (flat) -> 2.5 -> 5.0 (accelerating x2) -> 3.0 (cooling)
    rows = _annual("EMU:FP.CPI.TOTL.ZG", [1.5, 1.5, 2.5, 5.0, 3.0])
    out = compute_global_inflation(rows, cfg)
    assert [r["cpi_yoy_pct"] for r in out] == pytest.approx(
        [1.5, 1.5, 2.5, 5.0, 3.0])
    assert [r["trend"] for r in out] == [
        None, "flat", "accelerating", "accelerating", "cooling"]
    assert [r["streak"] for r in out] == [0, 0, 1, 2, -1]
    assert out[3]["vs_target_pp"] == pytest.approx(3.0)
    assert out[0]["region"] == "EMEA" and out[0]["change_pp"] is None


def test_global_inflation_yoy_from_index():
    cfg = GlobalConfig(inflation=(
        GlobalInflationDef("United States", "USA", "AMER", "CPIAUCSL",
                           transform="yoy_from_index", target=2.0),
    ))
    # 13 months of an index rising 0.25%/mo -> YoY ~ 1.0025^12 - 1
    rows = _monthly("CPIAUCSL", 2023, [100.0 * 1.0025 ** i for i in range(13)])
    out = compute_global_inflation(rows, cfg)
    (r,) = out  # only the 13th month has a year-ago match
    assert r["observation_date"] == "2024-01-01"
    assert r["cpi_yoy_pct"] == pytest.approx((1.0025 ** 12 - 1) * 100.0)
    assert r["vs_target_pp"] == pytest.approx(r["cpi_yoy_pct"] - 2.0)


def test_global_inflation_absent_series_and_empty_config():
    cfg = GlobalConfig(inflation=(
        GlobalInflationDef("Japan", "JPN", "APAC", "JPN:FP.CPI.TOTL.ZG"),
    ))
    assert compute_global_inflation([_row("OTHER", "2024-01-01", 1.0)], cfg) == []
    assert compute_global_inflation([], GlobalConfig()) == []


# ---- global policy rates -------------------------------------------------------

def test_global_policy_rates_stance_and_real_rate():
    cfg = GlobalConfig(
        inflation=(
            GlobalInflationDef("United States", "USA", "AMER",
                               "USA:FP.CPI.TOTL.ZG", target=2.0),
        ),
        policy_rates=(
            GlobalPolicyRateDef("United States", "USA", "AMER", "FEDFUNDS"),
        ),
    )
    rows = _annual("USA:FP.CPI.TOTL.ZG", [3.0, 4.0], start_year=2022)
    rows += [
        _row("FEDFUNDS", "2022-06-01", 1.00),
        _row("FEDFUNDS", "2022-12-01", 4.00),   # +300bp -> hiking
        _row("FEDFUNDS", "2023-06-01", 4.00),   # hold, stance carried
        _row("FEDFUNDS", "2023-12-01", 3.50),   # -50bp -> cutting
    ]
    out = compute_global_policy_rates(rows, cfg)
    assert [r["change_bps"] for r in out] == [None, 300.0, 0.0, -50.0]
    assert [r["stance"] for r in out] == [None, "hiking", "hiking", "cutting"]
    assert [r["last_move_bps"] for r in out] == [None, 300.0, 300.0, -50.0]
    # real rate: policy minus the latest CPI print on-or-before each date
    assert out[0]["real_rate_pct"] == pytest.approx(1.00 - 3.0)
    assert out[3]["real_rate_pct"] == pytest.approx(3.50 - 4.0)


def test_global_policy_rates_no_inflation_pairing():
    cfg = GlobalConfig(policy_rates=(
        GlobalPolicyRateDef("Euro Area", "EMU", "EMEA", "ECBDFR"),
    ))
    rows = [_row("ECBDFR", "2024-01-01", 4.0), _row("ECBDFR", "2024-02-01", 4.0)]
    out = compute_global_policy_rates(rows, cfg)
    assert all(r["real_rate_pct"] is None for r in out)
    # flat print before any move -> on-hold
    assert out[1]["stance"] == "on-hold"


# ---- Power BI catalog ------------------------------------------------------------

def test_powerbi_catalog_rows_are_unique_and_typed():
    rows = powerbi_catalog_rows()
    names = [r["object_name"] for r in rows]
    assert len(names) == len(set(names))
    assert all(r["object_type"] in {"dimension", "fact", "reference"}
               for r in rows)
    assert all(r["grain"] and r["intended_visual"] and r["description"]
               for r in rows)


def test_powerbi_catalog_covers_gold_tables():
    """Every gold_* table in the local schema must have a catalog row — this
    is the guard that keeps POWERBI_CATALOG current as Gold objects are added.
    Legacy/internal objects are explicitly exempted below."""
    import re

    from fred_pipeline import local_store

    exempt = {
        # superseded by curve_spread_daily / macro_feature views, kept for
        # backward compatibility; or PIT/audit-oriented rather than report-facing
        "fred_point_in_time", "fred_macro_feature_daily", "fred_curve_spread",
        "fred_cross_series_feature", "fred_cross_series_feature_pit",
        "fred_source_reconciliation", "fred_company_fundamentals",
        "fred_company_ratios", "fred_revision_stats",
    }
    tables = set(re.findall(
        r"CREATE TABLE IF NOT EXISTS gold_(\w+)", local_store._SCHEMA))
    cataloged = {r["object_name"] for r in POWERBI_CATALOG}
    missing = tables - cataloged - exempt
    assert not missing, f"gold tables missing from POWERBI_CATALOG: {sorted(missing)}"
