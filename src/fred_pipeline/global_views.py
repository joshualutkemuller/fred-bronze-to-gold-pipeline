"""Phase-6 engines: global inflation (GCPI), global policy rates (GPOL), and
the Power BI object catalog.

Pure Python, shared by both backends like the other terminal-view engines.
The catalog is a single Python constant (one source of truth) written as a
Gold *table* — a deliberate refinement of the plan's ``v_powerbi_catalog``
view sketch, since a literal-rows view would have to be hand-duplicated in
two SQL dialects.
"""

from __future__ import annotations

from bisect import bisect_right
from datetime import date
from typing import Any, Iterable, Optional

from fred_pipeline.features import _group_sorted, _pct_change, _year_ago_value
from fred_pipeline.global_config import GlobalConfig, load_global_config

# A move smaller than this counts as flat: 0.05pp for YoY inflation prints,
# 1bp for policy-rate moves.
INFLATION_FLAT_EPS_PP = 0.05
POLICY_FLAT_EPS_BPS = 1.0

# Only pair a policy rate with an inflation print at most this old (real
# rate on annual World Bank data is a coarse but honest approximation).
REAL_RATE_MAX_STALENESS_DAYS = 400


def _yoy_pct_series(
    series: list[tuple[date, float]], transform: str
) -> list[tuple[date, float]]:
    """Reduce a configured series to (date, YoY % in percent)."""
    if transform == "level":
        return list(series)
    dates = [d for d, _v in series]
    values = [v for _d, v in series]
    out = []
    for i in range(len(series)):
        pc = _pct_change(values[i], _year_ago_value(dates, values, i))
        if pc is not None:
            out.append((dates[i], pc * 100.0))
    return out


def compute_global_inflation(
    latest_rows: Iterable[dict[str, Any]],
    cfg: Optional[GlobalConfig] = None,
) -> list[dict[str, Any]]:
    """``gold.global_inflation``: one row per country × print — CPI YoY in
    percent, the change vs. the prior print, a trend verdict
    (accelerating / cooling / flat, ±0.05pp dead-band), the signed
    consecutive-print streak (+n = n accelerating prints in a row, −n =
    cooling; flat resets to 0), and the gap to the central-bank target.
    Countries whose series aren't ingested emit no rows."""
    if cfg is None:
        cfg = load_global_config()
    if not cfg.inflation:
        return []
    by_series = _group_sorted(
        r for r in latest_rows
        if r.get("series_id") in {d.series_id for d in cfg.inflation}
    )
    out: list[dict[str, Any]] = []
    for d in cfg.inflation:
        series = _yoy_pct_series(by_series.get(d.series_id, []), d.transform)
        streak = 0
        for i, (obs_date, yoy) in enumerate(series):
            change = yoy - series[i - 1][1] if i > 0 else None
            if change is None or abs(change) <= INFLATION_FLAT_EPS_PP:
                trend = "flat" if change is not None else None
                streak = 0
            elif change > 0:
                trend = "accelerating"
                streak = streak + 1 if streak > 0 else 1
            else:
                trend = "cooling"
                streak = streak - 1 if streak < 0 else -1
            out.append({
                "country": d.country,
                "iso3": d.iso3,
                "region": d.region,
                "series_id": d.series_id,
                "observation_date": obs_date.isoformat(),
                "cpi_yoy_pct": yoy,
                "change_pp": change,
                "trend": trend,
                "streak": streak,
                "target_pct": d.target,
                "vs_target_pp": (yoy - d.target) if d.target is not None else None,
            })
    return out


def compute_global_policy_rates(
    latest_rows: Iterable[dict[str, Any]],
    cfg: Optional[GlobalConfig] = None,
) -> list[dict[str, Any]]:
    """``gold.global_policy_rates``: one row per country × print — the policy
    rate in percent, the change vs. the prior print in bps, the most recent
    nonzero move (carried), a stance verdict from that move's sign
    (hiking / cutting / on-hold), and the ex-post real rate (policy − the
    country's latest CPI YoY print on-or-before the date, when an inflation
    entry for the same ``iso3`` is configured and fresh enough)."""
    if cfg is None:
        cfg = load_global_config()
    if not cfg.policy_rates:
        return []
    wanted = {d.series_id for d in cfg.policy_rates} | {
        d.series_id for d in cfg.inflation
    }
    by_series = _group_sorted(
        r for r in latest_rows if r.get("series_id") in wanted
    )
    # iso3 -> date-sorted CPI YoY prints, for the real-rate join.
    yoy_by_iso = {
        d.iso3: _yoy_pct_series(by_series.get(d.series_id, []), d.transform)
        for d in cfg.inflation
    }

    def _yoy_asof(iso3: str, on: date) -> Optional[float]:
        s = yoy_by_iso.get(iso3)
        if not s:
            return None
        pos = bisect_right(s, (on, float("inf"))) - 1
        if pos < 0 or (on - s[pos][0]).days > REAL_RATE_MAX_STALENESS_DAYS:
            return None
        return s[pos][1]

    out: list[dict[str, Any]] = []
    for d in cfg.policy_rates:
        series = by_series.get(d.series_id, [])
        last_move: Optional[float] = None
        for i, (obs_date, rate) in enumerate(series):
            change_bps = (rate - series[i - 1][1]) * 100.0 if i > 0 else None
            if change_bps is not None and abs(change_bps) > POLICY_FLAT_EPS_BPS:
                last_move = change_bps
            if last_move is None:
                stance = None
            else:
                stance = "hiking" if last_move > 0 else "cutting"
            if change_bps is not None and abs(change_bps) <= POLICY_FLAT_EPS_BPS \
                    and stance is None:
                stance = "on-hold"
            yoy = _yoy_asof(d.iso3, obs_date)
            out.append({
                "country": d.country,
                "iso3": d.iso3,
                "region": d.region,
                "series_id": d.series_id,
                "observation_date": obs_date.isoformat(),
                "policy_rate_pct": rate,
                "change_bps": change_bps,
                "last_move_bps": last_move,
                "stance": stance,
                "real_rate_pct": (rate - yoy) if yoy is not None else None,
            })
    return out


# ---- Power BI object catalog ----------------------------------------------------

def _entry(name, otype, module, grain, visual, description):
    return {
        "object_name": name, "object_type": otype, "module": module,
        "grain": grain, "intended_visual": visual, "description": description,
    }


# One source of truth for what the Gold layer offers a report author.
# Update this list when a Gold object is added — test_powerbi_catalog_covers_
# gold_tables in tests/test_global_views.py fails if a gold_* table exists in
# the local schema without a catalog row.
POWERBI_CATALOG: tuple[dict[str, Any], ...] = (
    _entry("dim_series", "dimension", "ALL", "1 / series",
           "slicers + relationships",
           "Star-schema hub: catalog semantics (category, polarity, transform) + meta titles/units."),
    _entry("dim_date", "dimension", "ALL", "1 / calendar day",
           "date table (mark as such) + recession shading",
           "Full time-intelligence calendar: date key, period start/end anchors, ISO week, "
           "day-of-week (ISO + DAX Sunday-first), US Federal fiscal calendar (Oct start), "
           "leap-year flag, NBER recession flag."),
    _entry("macro_indicator_dashboard", "fact", "ECON", "1 / series (latest)",
           "KPI grid / table with conditional formatting",
           "Latest/prior/change/YoY, PIT z-score & percentile, surprise proxy, polarity, staleness."),
    _entry("macro_indicator_sparkline", "fact", "ECON", "1 / series x point",
           "sparkline small multiples", "Last 36 observations per cataloged series."),
    _entry("macro_category_summary", "fact", "ECON", "1 / category",
           "breadth bar / cards", "Improving vs deteriorating breadth and surprise index per category."),
    _entry("inflation_explorer", "fact", "INFL", "1 / item x month",
           "decomposition tree + line drill",
           "CPI/PCE item trees: index, MoM/YoY, acceleration, 3m-annualized, weight, contribution."),
    _entry("inflation_contribution", "fact", "INFL", "1 / item x month",
           "waterfall", "Ranked weight x MoM contributions vs the headline-total bar."),
    _entry("treasury_curve", "fact", "CURV", "1 / date x tenor",
           "line chart over tenor (play axis on date)", "The tidy constant-maturity curve."),
    _entry("treasury_curve_metrics", "fact", "CURV", "1 / date",
           "line + recession shading",
           "Level/slope/curvature/butterfly, inversion flags, bull/bear x steepener/flattener move."),
    _entry("curve_spread_daily", "fact", "CURV", "1 / spread x date",
           "line + zero line + recession shading",
           "Configured spreads with PIT z-score/percentile, inversion flag/run."),
    _entry("spread_inversion_episode", "fact", "CURV", "1 / spread x episode",
           "episode table / Gantt bands",
           "Unique inversion periods: start on first negative print, end on re-steepening; trough, duration, recession overlap."),
    _entry("curve_spread_rolling", "fact", "CURV", "1 / spread x date x window",
           "line with window slicer", "Trailing change/pct-change/z over 1-252 obs windows."),
    _entry("treasury_curve_rolling", "fact", "CURV", "1 / tenor x date x window",
           "line with window slicer", "Per-tenor trailing change/pct-change/z."),
    _entry("yield_curve_ns_factors", "fact", "CURV", "1 / date",
           "3-panel factor time series / fitted-curve line",
           "Nelson-Siegel β₀ (level) / β₁ (slope) / β₂ (curvature) fitted daily; "
           "λ decay, fit RMSE, and fit_valid flag. "
           "β₁ (negative when normally shaped) is the Estrella-Mishkin recession predictor."),
    _entry("benchmark_rate_board", "fact", "BMRK", "1 / rate (latest)",
           "board table with trend arrows",
           "43-rate board: change bps, trend, spread-to-benchmark, regime tag."),
    _entry("funding_tape_daily", "fact", "FUND", "1 / metric x date",
           "faceted lines by metric_type", "Corridor rates, balances, funding spreads with expanding stats."),
    _entry("funding_stress_daily", "fact", "FUND", "1 / date",
           "gauge / area with bucket bands", "0-100 stress score blended from component spread z-scores."),
    _entry("credit_spread_daily", "fact", "CRDT", "1 / instrument x date",
           "line + stress-episode markers",
           "ICE BofA OAS levels/changes with percentile stress episodes and recession overlay."),
    _entry("credit_spread_rolling", "fact", "CRDT", "1 / instrument x date x window",
           "line with window slicer", "OAS trailing change/pct-change/z in bps."),
    _entry("macro_regime_daily", "fact", "REGIME", "1 / date",
           "regime ribbon + pillar small multiples",
           "Five pillar z-scores, signed composite, named regime with confidence."),
    _entry("fred_series_zscore_rolling", "fact", "STAT", "1 / series x date x window",
           "multi-window fan chart / z-score band",
           "Rolling z-score and percentile rank for every FRED macro series at 12 / 36 / 60 / 120 "
           "observation windows (≈ 1 / 3 / 5 / 10 years). Fan-chart: filter to one series and "
           "overlay each window band to gauge how extreme the current reading is on different "
           "historical horizons."),
    _entry("zscore_heatmap", "fact", "STAT", "1 / series x date",
           "heatmap (filter to date) / fan chart (filter to series)",
           "Wide-format cross-series z-score snapshot: expanding z-score plus rolling z-scores and "
           "percentile ranks at 12 / 36 / 60 / 120 observations per (series_id, date). "
           "Filter to a single date for a cross-category heat matrix; filter to one series for "
           "a multi-window z-score fan chart over time."),
    _entry("series_correlation", "fact", "STAT", "1 / pair x window x date",
           "heatmap (latest) / rolling line", "Rolling & expanding Pearson correlation for curated pairs."),
    _entry("series_lead_lag", "fact", "EDA", "1 / pair x lag",
           "CCF bar chart + Granger cards",
           "Cross-correlation by lag (+lag = a leads), best lag, two-direction Granger F/p."),
    _entry("series_structural_breaks", "fact", "EDA", "1 / pair x test_type",
           "break-date scatter / table",
           "Chow and CUSUM structural-break tests for each configured pair: estimated break date, "
           "Chow F-stat / p-value, pre/post segment means, CUSUM max deviation, and significance flag. "
           "Chow scans all candidate break dates (15% trim) under y ~ 1 + x and picks the max-F date; "
           "CUSUM accumulates full-sample OLS residuals and reports the first 5%-boundary crossing."),
    _entry("global_inflation", "fact", "GCPI", "1 / country x print",
           "map / heat table by region",
           "CPI YoY by country: change, trend, streaks, vs-target gap."),
    _entry("global_policy_rates", "fact", "GPOL", "1 / country x print",
           "board table by region", "Policy rate, last move, stance, ex-post real rate."),
    _entry("equity_return_daily", "fact", "EQUITY", "1 / ticker x date",
           "line / return heatmap",
           "Daily price return per ticker from split-adjusted Stooq close, with a cumulative price-return index."),
    _entry("index_constituents", "fact", "EQUITY", "1 / ETF x constituent x snapshot",
           "weight treemap / table (filter is_latest_snapshot)",
           "ETF constituent weights exploded from daily holdings, with rank and latest-snapshot flag."),
    _entry("equity_total_return_index", "fact", "EQUITY", "1 / ticker x date",
           "line (TR vs PR index) + yield",
           "True total return from Tiingo raw close+dividend+split (reinvested), with price/total indices, trailing-12m dividend and yield."),
    _entry("equity_price_reconciliation", "reference", "EQUITY", "1 / ticker x date",
           "divergence scatter / table",
           "Cross-vendor close comparison: Stooq split-adjusted vs Tiingo adjClose; flags dates where sources diverge beyond tolerance."),
    _entry("equity_factor_attribution", "fact", "EQUITY", "1 / ticker x factor x window x date",
           "factor exposure heatmap / rolling beta lines",
           "Rolling OLS of monthly equity price returns on ML-2 PCA macro factor scores: "
           "beta and t-stat per factor, alpha, R², and observation count per (ticker, window, date). "
           "Configurable windows (default 12 / 36 / 60 months); tickers filter in config/equity_factor_attribution.yml."),
    _entry("fred_latest_observation", "fact", "CORE", "1 / series x date",
           "generic line", "Latest-revised observation per (series, date)."),
    _entry("fred_feature_transforms", "fact", "CORE", "1 / series x date",
           "generic line", "MoM/diff/YoY/expanding-z transforms per series."),
    _entry("powerbi_catalog", "reference", "ALL", "1 / gold object",
           "documentation page", "This catalog."),
    _entry("ml_feature_matrix", "fact", "ML", "1 / date x feature",
           "generic table / heatmap",
           "Tidy ML feature matrix: one row per (date, feature_name) with the configured transform value, feeding the PCA and anomaly engines."),
    _entry("macro_factor_scores", "fact", "ML", "1 / date x factor",
           "line (factor scores over time)",
           "Expanding monthly PCA factor scores: score, explained/cumulative variance ratio, and training-obs count per factor."),
    _entry("macro_factor_loadings", "fact", "ML", "1 / date x factor x feature",
           "heatmap (latest loadings) / animated loading bar chart",
           "PCA factor loadings per snapshot: feature contributions to each factor, sign-anchored for stability."),
    _entry("macro_anomaly_scores", "fact", "ML", "1 / date",
           "line + anomaly markers",
           "Mahalanobis D² in PCA factor space with χ²(k) p-value; is_anomaly flags the top-1% statistical outliers."),
    _entry("recession_probability_daily", "fact", "ML", "1 / date",
           "probability fan chart + recession shading",
           "Expanding IRLS logistic recession probability: P(recession in next 3/6/12m) "
           "re-estimated at each USREC print; logit score, feature count, training-obs count, "
           "and backfill flag for early dates below the min-obs threshold."),
)


def powerbi_catalog_rows() -> list[dict[str, Any]]:
    """``gold.powerbi_catalog``: the report author's manifest of Gold objects."""
    return [dict(e) for e in POWERBI_CATALOG]
