"""Market-terminal analytical views (pure Python).

Gold objects that recreate the ``market_terminal`` project's economic-analysis
surfaces for Power BI (plan: ``docs/market_terminal_gold_views.md``):

  * **dim_series / dim_date** — the star-schema dimensions every fact joins to;
  * **macro_indicator_dashboard** (+ sparkline + category summary) — the ECON
    macro grid: latest/prior/change/YoY/z-score/percentile/surprise/polarity
    per cataloged series, with category breadth and a surprise index;
  * **treasury_curve** (+ metrics) — the Curve Lab: the tidy tenor×date curve,
    level/slope/curvature/butterfly, inversion flags, recession overlay, and
    bull/bear × steepen/flatten curve-move classification;
  * **curve_spread_daily** — the configured spreads enriched with expanding
    (point-in-time safe) z-score/percentile, inversion flags/runs, recession;
  * **spread_inversion_episode** — one row per *unique inversion episode* per
    spread: opens on the first negative observation, closes when the spread
    turns non-negative again (with trough, duration, and recession overlap);
  * **benchmark_rate_board** — the BMRK rate board: latest/prior/change,
    trend, spread-to-benchmark, z-score/percentile, regime tag per rate;
  * **funding_tape_daily / funding_stress_daily** — the FUND tape (corridor
    rates, balances, spreads with expanding stats) and the 0–100 stress gauge;
  * **credit_spread_daily** — the CRDT OAS history with expanding stats and
    percentile-based stress-episode flags;
  * **inflation_explorer / inflation_contribution** — the INFL item trees
    (CPI SA/NSA, PCE): index/MoM/YoY/acceleration/3m-annualized per item, and
    the weight × MoM contribution waterfall against the headline print.

Kept pure (dict-in → dict-out, no Spark, no SQLite) so the Local and
Databricks backends share the same tested logic — the same pattern as
:mod:`fred_pipeline.features`. Rolling statistics are expanding-window only,
so nothing here leaks future information into historical rows.
"""

from __future__ import annotations

import calendar
from bisect import bisect_right
from datetime import date, timedelta
from typing import Any, Iterable, Optional

from fred_pipeline.catalog_config import CatalogEntry, load_series_catalog
from fred_pipeline.curve_config import TenorDef, load_curve_defs
from fred_pipeline.inflation_config import InflationItemDef, load_inflation_items
from fred_pipeline.rates_complex_config import (
    BenchmarkBoardConfig,
    CreditConfig,
    FundingConfig,
    load_benchmark_board,
    load_credit_config,
    load_funding_config,
)
from fred_pipeline.features import (
    _expanding_mean_std,
    _group_sorted,
    _parse,
    _pct_change,
    _year_ago_value,
    compute_curve_spreads,
)
from fred_pipeline.spread_config import SpreadDef, load_spread_defs

# Series used for the recession overlay (NBER USREC, 1 = recession month).
RECESSION_SERIES = "USREC"

# Sparkline length the ECON dashboard renders (terminal shows 36 points).
SPARK_POINTS = 36


# ---- shared helpers ---------------------------------------------------------

def _recession_flags(
    latest_rows: Iterable[dict[str, Any]], series_id: str = RECESSION_SERIES
) -> list[tuple[date, bool]]:
    """Date-sorted ``(observation_date, in_recession)`` from the USREC series.

    Empty when USREC isn't ingested — callers then emit ``None`` for
    ``is_recession`` (unknown), not ``False`` (known expansion).
    """
    flags = [
        (d, v >= 1.0)
        for d, v in _group_sorted(
            r for r in latest_rows if r.get("series_id") == series_id
        ).get(series_id, [])
    ]
    return flags


def _recession_at(flags: list[tuple[date, bool]], d: date) -> Optional[bool]:
    """USREC is monthly; a date's flag is the latest USREC obs on-or-before it."""
    if not flags:
        return None
    pos = bisect_right(flags, (d, True)) - 1
    if pos < 0:
        return None
    # Don't extrapolate more than ~2 months past the last USREC print.
    if (d - flags[pos][0]).days > 62:
        return None
    return flags[pos][1]


def _expanding_percentile(values: list[float]) -> list[Optional[float]]:
    """Percent-rank of each value within the history up to and including it
    (0 = lowest seen so far, 1 = highest). PIT-safe: rank ``i`` uses only
    ``values[0..i]``. First observation has no rank (``None``)."""
    out: list[Optional[float]] = []
    for i, v in enumerate(values):
        if i == 0:
            out.append(None)
            continue
        below = sum(1 for x in values[: i + 1] if x < v)
        out.append(below / i)
    return out


# ---- dimensions -------------------------------------------------------------

def build_dim_series(
    catalog: Optional[Iterable[CatalogEntry]] = None,
    meta_rows: Iterable[dict[str, Any]] = (),
) -> list[dict[str, Any]]:
    """``gold.dim_series``: one row per cataloged series, presentation semantics
    from ``config/series_catalog.yml`` merged with title/frequency/units from
    the ``meta`` layer (blank when the series isn't in meta yet)."""
    if catalog is None:
        catalog = load_series_catalog()
    meta = {m["series_id"]: m for m in meta_rows if m.get("series_id")}
    out: list[dict[str, Any]] = []
    for e in catalog:
        m = meta.get(e.series_id, {})
        out.append({
            "series_id": e.series_id,
            "title": m.get("title") or "",
            "source": e.source,
            "frequency": m.get("frequency") or "",
            "units": m.get("units") or "",
            "econ_category": e.econ_category,
            "polarity": e.polarity,
            "default_transform": e.default_transform,
            "scale": e.scale,
            "decimals": e.decimals,
            "notes": e.notes,
        })
    return sorted(out, key=lambda r: (r["econ_category"], r["series_id"]))


def build_dim_date(
    start: Any, end: Any, recession_rows: Iterable[dict[str, Any]] = ()
) -> list[dict[str, Any]]:
    """``gold.dim_date``: one row per calendar day in [start, end], with the
    calendar attributes Power BI time-intelligence needs plus the NBER
    recession flag (``None`` until USREC is ingested). ``fiscal_year`` is the
    US federal fiscal year (October start)."""
    lo, hi = _parse(start), _parse(end)
    if lo is None or hi is None or lo > hi:
        return []
    flags = _recession_flags(recession_rows)
    out: list[dict[str, Any]] = []
    d = lo
    while d <= hi:
        out.append({
            "date": d.isoformat(),
            "year": d.year,
            "quarter": (d.month - 1) // 3 + 1,
            "month": d.month,
            "month_name": calendar.month_name[d.month],
            "is_month_end": d.month != (d + timedelta(days=1)).month,
            "fiscal_year": d.year + 1 if d.month >= 10 else d.year,
            "is_recession": _recession_at(flags, d),
        })
        d += timedelta(days=1)
    return out


# ---- ECON macro dashboard ---------------------------------------------------

def compute_macro_dashboard(
    latest_rows: Iterable[dict[str, Any]],
    catalog: Optional[Iterable[CatalogEntry]] = None,
    *,
    as_of: Optional[str] = None,
    spark_points: int = SPARK_POINTS,
) -> dict[str, list[dict[str, Any]]]:
    """The ECON macro grid over cataloged series, from latest-revision rows.

    Returns three row sets keyed ``dashboard`` / ``sparkline`` /
    ``category_summary``. ``as_of`` defaults to the latest observation date
    across the cataloged series (deterministic — no wall-clock dependency), and
    drives ``staleness_days``. ``surprise`` is the no-consensus proxy the plan
    documents: latest value minus the trailing ``surprise_window`` mean
    (``surprise_z`` divides by that window's std). z-score/percentile are
    expanding (PIT-safe).
    """
    if catalog is None:
        catalog = load_series_catalog()
    entries = {e.series_id: e for e in catalog}
    if not entries:
        return {"dashboard": [], "sparkline": [], "category_summary": []}

    # Group per series, keeping realtime_start for the provenance column.
    per_series: dict[str, list[tuple[date, float, str]]] = {}
    for r in latest_rows:
        sid = r.get("series_id")
        if sid not in entries or r.get("is_missing"):
            continue
        v, d = r.get("value"), _parse(r.get("observation_date"))
        if v is None or d is None:
            continue
        per_series.setdefault(sid, []).append(
            (d, float(v), str(r.get("realtime_start") or "")[:10])
        )
    for pts in per_series.values():
        pts.sort(key=lambda t: t[0])

    as_of_date = _parse(as_of) if as_of else max(
        (pts[-1][0] for pts in per_series.values()), default=None
    )
    if as_of_date is None:
        return {"dashboard": [], "sparkline": [], "category_summary": []}

    dashboard: list[dict[str, Any]] = []
    sparkline: list[dict[str, Any]] = []
    for sid, pts in sorted(per_series.items()):
        e = entries[sid]
        dates = [d for d, _v, _rt in pts]
        values = [v for _d, v, _rt in pts]
        i = len(pts) - 1
        latest_d, latest_v, latest_rt = pts[i]
        prior_d, prior_v = (pts[i - 1][0], pts[i - 1][1]) if i > 0 else (None, None)

        change_abs = (latest_v - prior_v) if prior_v is not None else None
        yoy = _pct_change(latest_v, _year_ago_value(dates, values, i))
        means, stds = _expanding_mean_std(values)
        zscore = ((latest_v - means[i]) / stds[i]) if stds[i] else None
        percentile = _expanding_percentile(values)[i]

        window = values[max(0, i - e.surprise_window):i]  # excludes latest
        surprise = surprise_z = None
        if len(window) >= 2:
            w_mean = sum(window) / len(window)
            surprise = latest_v - w_mean
            w_std = (sum((x - w_mean) ** 2 for x in window) / len(window)) ** 0.5
            surprise_z = (surprise / w_std) if w_std else None

        direction_is_good: Optional[bool] = None
        if e.polarity and change_abs:
            direction_is_good = (e.polarity * change_abs) > 0

        spark = values[-spark_points:]
        dashboard.append({
            "series_id": sid,
            "econ_category": e.econ_category,
            "polarity": e.polarity,
            "default_transform": e.default_transform,
            "as_of_date": as_of_date.isoformat(),
            "latest_date": latest_d.isoformat(),
            "latest_value": latest_v,
            "prior_date": prior_d.isoformat() if prior_d else None,
            "prior_value": prior_v,
            "change_abs": change_abs,
            "change_pct": _pct_change(latest_v, prior_v),
            "yoy_pct": yoy,
            "zscore": zscore,
            "percentile": percentile,
            "surprise": surprise,
            "surprise_z": surprise_z,
            "direction_is_good": direction_is_good,
            "spark_min": min(spark),
            "spark_max": max(spark),
            "staleness_days": (as_of_date - latest_d).days,
            "realtime_start": latest_rt,
        })
        for idx, (d, v, _rt) in enumerate(pts[-spark_points:]):
            sparkline.append({
                "series_id": sid,
                "point_index": idx,
                "observation_date": d.isoformat(),
                "value": v,
            })

    by_cat: dict[str, list[dict[str, Any]]] = {}
    for row in dashboard:
        by_cat.setdefault(row["econ_category"], []).append(row)
    category_summary: list[dict[str, Any]] = []
    for cat in sorted(by_cat):
        rows = by_cat[cat]
        directional = [r for r in rows if r["direction_is_good"] is not None]
        improving = sum(1 for r in directional if r["direction_is_good"])
        zscores = [r["zscore"] for r in rows if r["zscore"] is not None]
        surprises = [r["surprise_z"] for r in rows if r["surprise_z"] is not None]
        category_summary.append({
            "econ_category": cat,
            "as_of_date": as_of_date.isoformat(),
            "n_series": len(rows),
            "n_improving": improving,
            "n_deteriorating": len(directional) - improving,
            "breadth_pct": (improving / len(directional)) if directional else None,
            "avg_zscore": (sum(zscores) / len(zscores)) if zscores else None,
            "surprise_index": (sum(surprises) / len(surprises)) if surprises else None,
        })
    return {
        "dashboard": dashboard,
        "sparkline": sparkline,
        "category_summary": category_summary,
    }


# ---- Treasury Curve Lab -----------------------------------------------------

def _tenor_yield(day: dict[int, float], months: int) -> Optional[float]:
    return day.get(months)


def compute_treasury_curve(
    latest_rows: Iterable[dict[str, Any]],
    tenors: Optional[Iterable[TenorDef]] = None,
) -> dict[str, list[dict[str, Any]]]:
    """The Curve Lab tables from daily constant-maturity history.

    Returns ``curve`` (tidy: one row per as-of date × tenor with data) and
    ``metrics`` (one row per as-of date): level (mean of available tenors),
    2s10s / 3m10s slopes, 2-5-10 curvature, 2-10-30 butterfly, inversion
    flags, the NBER recession flag (``None`` until USREC is ingested), and the
    bull/bear × steepen/flatten classification of the move vs the prior curve
    date (level down = bull; 2s10s wider = steepen).
    """
    if tenors is None:
        tenors = load_curve_defs()
    tenor_list = sorted(tenors, key=lambda t: t.months)
    by_series = _group_sorted(
        r for r in latest_rows
        if r.get("series_id") in {t.series_id for t in tenor_list}
    )
    flags = _recession_flags(latest_rows)

    # date -> {tenor_months: yield}
    by_date: dict[date, dict[int, float]] = {}
    curve: list[dict[str, Any]] = []
    for t in tenor_list:
        for d, v in by_series.get(t.series_id, []):
            by_date.setdefault(d, {})[t.months] = v
            curve.append({
                "as_of_date": d.isoformat(),
                "tenor_label": t.label,
                "tenor_months": t.months,
                "series_id": t.series_id,
                "yield_pct": v,
            })
    curve.sort(key=lambda r: (r["as_of_date"], r["tenor_months"]))

    metrics: list[dict[str, Any]] = []
    prev_level: Optional[float] = None
    prev_slope: Optional[float] = None
    for d in sorted(by_date):
        day = by_date[d]
        level = sum(day.values()) / len(day)
        y3m, y2, y5 = day.get(3), day.get(24), day.get(60)
        y10, y30 = day.get(120), day.get(360)
        slope_10y2y = (y10 - y2) if (y10 is not None and y2 is not None) else None
        slope_10y3m = (y10 - y3m) if (y10 is not None and y3m is not None) else None
        curvature = (
            2 * y5 - y2 - y10
            if (y5 is not None and y2 is not None and y10 is not None) else None
        )
        butterfly = (
            2 * y10 - y2 - y30
            if (y10 is not None and y2 is not None and y30 is not None) else None
        )

        curve_move: Optional[str] = None
        if prev_level is not None and prev_slope is not None and slope_10y2y is not None:
            d_level, d_slope = level - prev_level, slope_10y2y - prev_slope
            rally = "bull" if d_level < 0 else "bear"
            shape = "steepener" if d_slope > 0 else "flattener"
            if d_level and d_slope:
                curve_move = f"{rally}-{shape}"
            elif d_level:
                curve_move = f"parallel-{rally}"
            elif d_slope:
                curve_move = f"twist-{shape}"
        metrics.append({
            "as_of_date": d.isoformat(),
            "level": level,
            "slope_10y2y": slope_10y2y,
            "slope_10y3m": slope_10y3m,
            "curvature_2_5_10": curvature,
            "butterfly_2_10_30": butterfly,
            "is_inverted_10y2y": (slope_10y2y < 0) if slope_10y2y is not None else None,
            "is_inverted_10y3m": (slope_10y3m < 0) if slope_10y3m is not None else None,
            "is_recession": _recession_at(flags, d),
            "curve_move": curve_move,
        })
        prev_level = level
        if slope_10y2y is not None:
            prev_slope = slope_10y2y
    return {"curve": curve, "metrics": metrics}


# ---- enriched spread history -------------------------------------------------

def compute_curve_spread_daily(
    latest_rows: Iterable[dict[str, Any]],
    spreads: Optional[Iterable[SpreadDef]] = None,
) -> list[dict[str, Any]]:
    """``gold.curve_spread_daily``: the configured spreads/ratios
    (``config/spreads.yml``) enriched with expanding (PIT-safe) z-score and
    percentile, inversion flag + consecutive-inverted-observation run (spreads
    only — a ratio has no zero line), value in bps, and the recession flag."""
    if spreads is None:
        spreads = load_spread_defs()
    spread_list = list(spreads)
    ops = {sd.name: sd.op for sd in spread_list}
    base = compute_curve_spreads(latest_rows, spread_list)
    flags = _recession_flags(latest_rows)

    by_name: dict[str, list[dict[str, Any]]] = {}
    for row in base:
        by_name.setdefault(row["spread_name"], []).append(row)

    out: list[dict[str, Any]] = []
    for name in sorted(by_name):
        rows = sorted(by_name[name], key=lambda r: r["observation_date"])
        values = [r["value"] for r in rows]
        means, stds = _expanding_mean_std(values)
        pcts = _expanding_percentile(values)
        is_spread = ops.get(name) == "spread"
        run = 0
        for i, r in enumerate(rows):
            v = r["value"]
            inverted = (v < 0) if is_spread else None
            run = (run + 1) if inverted else 0
            d = _parse(r["observation_date"])
            out.append({
                "spread_name": name,
                "observation_date": r["observation_date"],
                "long_leg": r["long_leg"],
                "short_leg": r["short_leg"],
                "value": v,
                "value_bps": (v * 100.0) if is_spread else None,
                "zscore": ((v - means[i]) / stds[i]) if stds[i] else None,
                "percentile": pcts[i],
                "is_inverted": inverted,
                "inversion_run": run if is_spread else None,
                "is_recession": _recession_at(flags, d) if d else None,
            })
    return out


def compute_spread_inversion_episodes(
    latest_rows: Iterable[dict[str, Any]],
    spreads: Optional[Iterable[SpreadDef]] = None,
) -> list[dict[str, Any]]:
    """``gold.spread_inversion_episode``: one row per unique inversion episode
    per configured spread (``op: spread`` only — a ratio has no zero line).

    An episode **opens** on the first observation where the spread is negative
    and **closes** on the first later observation where it is non-negative
    again (``end_date`` = that re-steepening date; a single positive print
    between two inversions therefore splits them into two distinct episodes).
    An episode still negative at the end of history is **ongoing**:
    ``end_date`` is ``None`` and duration is measured to ``last_inverted_date``.
    Each row carries the trough (most negative value and its date), the
    inverted-observation count, calendar duration, and whether any inverted
    date overlapped an NBER recession (``None`` until USREC is ingested).
    """
    if spreads is None:
        spreads = load_spread_defs()
    spread_list = [sd for sd in spreads if sd.op == "spread"]
    base = compute_curve_spreads(latest_rows, spread_list)
    flags = _recession_flags(latest_rows)

    by_name: dict[str, list[dict[str, Any]]] = {}
    for row in base:
        by_name.setdefault(row["spread_name"], []).append(row)

    out: list[dict[str, Any]] = []
    for name in sorted(by_name):
        rows = sorted(by_name[name], key=lambda r: r["observation_date"])
        episode: Optional[dict[str, Any]] = None
        number = 0

        def _close(end_date: Optional[str]) -> None:
            ep = episode
            last = _parse(ep["last_inverted_date"])
            start = _parse(ep["start_date"])
            end = _parse(end_date) if end_date else None
            ep["end_date"] = end_date
            ep["is_ongoing"] = end_date is None
            ep["calendar_days"] = ((end or last) - start).days
            ep["trough_bps"] = ep["trough_value"] * 100.0
            out.append(ep)

        for r in rows:
            v, d = r["value"], str(r["observation_date"])[:10]
            if v < 0:
                rec = _recession_at(flags, _parse(d))
                if episode is None:  # first negative print opens the episode
                    number += 1
                    episode = {
                        "spread_name": name,
                        "long_leg": r["long_leg"],
                        "short_leg": r["short_leg"],
                        "episode_number": number,
                        "start_date": d,
                        "end_date": None,
                        "last_inverted_date": d,
                        "observation_count": 1,
                        "calendar_days": 0,
                        "trough_value": v,
                        "trough_bps": v * 100.0,
                        "trough_date": d,
                        "is_ongoing": True,
                        "recession_overlap": rec,
                    }
                else:
                    episode["last_inverted_date"] = d
                    episode["observation_count"] += 1
                    if v < episode["trough_value"]:
                        episode["trough_value"] = v
                        episode["trough_date"] = d
                    if rec is not None:
                        episode["recession_overlap"] = (
                            bool(episode["recession_overlap"]) or rec
                        )
            elif episode is not None:
                _close(d)  # re-steepened: this date ends the episode
                episode = None
        if episode is not None:
            _close(None)  # still inverted at the end of history
    return out


# ---- BMRK benchmark rate board ------------------------------------------------

# A move smaller than this (in the rate's native percent units) counts as flat
# for the trend verdict: 1bp.
TREND_EPSILON = 0.01


def compute_benchmark_rate_board(
    latest_rows: Iterable[dict[str, Any]],
    board: Optional[BenchmarkBoardConfig] = None,
) -> list[dict[str, Any]]:
    """``gold.benchmark_rate_board``: one row per configured rate
    (``config/benchmark_rates.yml``) at its latest observation — latest/prior,
    change in bps, a trend verdict (latest vs. ``trend_window`` observations
    ago, ±1bp dead-band), expanding (PIT-safe) z-score/percentile, the spread
    to the configured benchmark (benchmark's last value on-or-before the
    rate's date), a regime tag from the trend (rising→tightening,
    falling→easing, flat→stable), and staleness vs. the board's as-of date.
    Rates whose series aren't ingested emit no row."""
    if board is None:
        board = load_benchmark_board()
    if not board.rates:
        return []
    wanted = {rd.series_id for rd in board.rates} | {
        rd.benchmark for rd in board.rates if rd.benchmark
    }
    by_series = _group_sorted(
        r for r in latest_rows if r.get("series_id") in wanted
    )
    as_of = max((s[-1][0] for s in by_series.values()), default=None)
    if as_of is None:
        return []

    out: list[dict[str, Any]] = []
    for rd in board.rates:
        series = by_series.get(rd.series_id)
        if not series:
            continue
        dates = [d for d, _v in series]
        values = [v for _d, v in series]
        i = len(series) - 1
        latest_d, latest_v = series[i]
        prior_v = values[i - 1] if i > 0 else None

        back = i - board.trend_window
        trend = None
        if back >= 0:
            delta = latest_v - values[back]
            if abs(delta) <= TREND_EPSILON:
                trend = "flat"
            else:
                trend = "rising" if delta > 0 else "falling"
        regime = {"rising": "tightening", "falling": "easing", "flat": "stable"}.get(trend)

        spread_bps = None
        if rd.benchmark:
            bench = by_series.get(rd.benchmark)
            if bench:
                bdates = [d for d, _v in bench]
                pos = bisect_right(bdates, latest_d) - 1
                if pos >= 0:
                    spread_bps = (latest_v - bench[pos][1]) * 100.0

        means, stds = _expanding_mean_std(values)
        out.append({
            "series_id": rd.series_id,
            "rate_label": rd.label,
            "rate_category": rd.category,
            "benchmark_series": rd.benchmark or None,
            "as_of_date": as_of.isoformat(),
            "latest_date": latest_d.isoformat(),
            "latest_value": latest_v,
            "prior_value": prior_v,
            "change_bps": ((latest_v - prior_v) * 100.0) if prior_v is not None else None,
            "trend": trend,
            "spread_to_benchmark_bps": spread_bps,
            "zscore": ((latest_v - means[i]) / stds[i]) if stds[i] else None,
            "percentile": _expanding_percentile(values)[i],
            "regime": regime,
            "staleness_days": (as_of - latest_d).days,
        })
    return out


# ---- FUND funding tape + stress gauge -------------------------------------------

# Gauge mapping: stress_score = clamp(50 + STRESS_Z_SCALE * composite_z, 0, 100).
STRESS_Z_SCALE = 20.0
STRESS_BUCKETS = ((40.0, "calm"), (60.0, "normal"), (80.0, "elevated"))


def _stress_bucket(score: float) -> str:
    for bound, label in STRESS_BUCKETS:
        if score < bound:
            return label
    return "stressed"


def compute_funding_features(
    latest_rows: Iterable[dict[str, Any]],
    cfg: Optional[FundingConfig] = None,
) -> dict[str, list[dict[str, Any]]]:
    """The FUND surfaces from ``config/funding.yml``.

    Returns ``tape`` (one row per metric × date: corridor rates and balances
    as configured, plus each funding spread on the dates both legs print, all
    with expanding PIT-safe z-score/percentile) and ``stress`` (one row per
    date where **every** stress component spread has a value:
    ``composite_z`` = weighted mean of the component spreads' expanding
    z-scores, mapped to the 0–100 ``stress_score`` and bucketed calm/normal/
    elevated/stressed). Metrics whose series aren't ingested emit no rows."""
    if cfg is None:
        cfg = load_funding_config()
    if not cfg.metrics and not cfg.spreads:
        return {"tape": [], "stress": []}
    wanted = {m.series_id for m in cfg.metrics} | {
        s for sp in cfg.spreads for s in (sp.long_leg, sp.short_leg)
    }
    by_series = _group_sorted(
        r for r in latest_rows if r.get("series_id") in wanted
    )

    tape: list[dict[str, Any]] = []
    # spread name -> {date_iso: zscore} for the gauge
    spread_z: dict[str, dict[str, Optional[float]]] = {}

    def _emit(name: str, metric_type: str, series: list[tuple[date, float]]) -> None:
        values = [v for _d, v in series]
        means, stds = _expanding_mean_std(values)
        pcts = _expanding_percentile(values)
        zmap: dict[str, Optional[float]] = {}
        for i, (d, v) in enumerate(series):
            z = ((v - means[i]) / stds[i]) if stds[i] else None
            zmap[d.isoformat()] = z
            tape.append({
                "metric_name": name,
                "metric_type": metric_type,
                "observation_date": d.isoformat(),
                "value": v,
                "zscore": z,
                "percentile": pcts[i],
            })
        if metric_type == "spread":
            spread_z[name] = zmap

    for m in cfg.metrics:
        series = by_series.get(m.series_id)
        if series:
            _emit(m.name, m.metric_type, series)
    for sp in cfg.spreads:
        long_s, short_s = by_series.get(sp.long_leg), by_series.get(sp.short_leg)
        if not long_s or not short_s:
            continue
        short_map = {d: v for d, v in short_s}
        series = [(d, v - short_map[d]) for d, v in long_s if d in short_map]
        if series:
            _emit(sp.name, "spread", series)

    stress: list[dict[str, Any]] = []
    if cfg.stress_components and all(
        c.spread in spread_z for c in cfg.stress_components
    ):
        common = set.intersection(
            *(set(spread_z[c.spread]) for c in cfg.stress_components)
        )
        total_w = sum(c.weight for c in cfg.stress_components)
        for d in sorted(common):
            # An early observation with no z yet (expanding std = 0) is
            # neutral, not missing — it contributes 0 to the composite.
            composite = sum(
                c.weight * (spread_z[c.spread][d] or 0.0)
                for c in cfg.stress_components
            ) / total_w
            score = min(100.0, max(0.0, 50.0 + STRESS_Z_SCALE * composite))
            stress.append({
                "observation_date": d,
                "composite_z": composite,
                "stress_score": score,
                "stress_bucket": _stress_bucket(score),
                "n_components": len(cfg.stress_components),
            })
    return {"tape": tape, "stress": stress}


# ---- CRDT credit spreads ---------------------------------------------------------

def compute_credit_spread_daily(
    latest_rows: Iterable[dict[str, Any]],
    cfg: Optional[CreditConfig] = None,
) -> list[dict[str, Any]]:
    """``gold.credit_spread_daily``: OAS history per configured instrument
    (``config/credit.yml``; FRED publishes ICE BofA OAS in percent —
    ``oas_bps`` is ×100) with change vs. prior print, expanding (PIT-safe)
    z-score/percentile, a stress-episode flag (expanding percentile at/above
    ``stress_percentile``), and the NBER recession overlay (``None`` until
    USREC is ingested). Instruments whose series aren't ingested emit no rows."""
    if cfg is None:
        cfg = load_credit_config()
    if not cfg.instruments:
        return []
    by_series = _group_sorted(
        r for r in latest_rows
        if r.get("series_id") in {c.series_id for c in cfg.instruments}
    )
    flags = _recession_flags(latest_rows)

    out: list[dict[str, Any]] = []
    for cd in cfg.instruments:
        series = by_series.get(cd.series_id)
        if not series:
            continue
        values = [v for _d, v in series]
        means, stds = _expanding_mean_std(values)
        pcts = _expanding_percentile(values)
        for i, (d, v) in enumerate(series):
            pct = pcts[i]
            out.append({
                "instrument": cd.instrument,
                "series_id": cd.series_id,
                "category": cd.category,
                "observation_date": d.isoformat(),
                "oas_pct": v,
                "oas_bps": v * 100.0,
                "change_bps": ((v - values[i - 1]) * 100.0) if i > 0 else None,
                "zscore": ((v - means[i]) / stds[i]) if stds[i] else None,
                "percentile": pct,
                "is_stress_episode": (
                    (pct >= cfg.stress_percentile) if pct is not None else None
                ),
                "is_recession": _recession_at(flags, d),
            })
    return sorted(out, key=lambda r: (r["instrument"], r["observation_date"]))


# ---- INFL inflation explorer -------------------------------------------------

def _month_index(d: date) -> int:
    return d.year * 12 + (d.month - 1)


def compute_inflation_explorer(
    latest_rows: Iterable[dict[str, Any]],
    items: Optional[Iterable[InflationItemDef]] = None,
) -> dict[str, list[dict[str, Any]]]:
    """The INFL surfaces from ``config/inflation_items.yml``.

    Returns ``explorer`` (one row per item × month: index level, MoM %, YoY %,
    ΔMoM/ΔYoY acceleration, trailing-3-month annualized rate, the item's
    relative-importance weight, and ``weight × MoM`` contribution in headline
    percentage points) and ``contribution`` (the waterfall: per month and
    tree, one row per ``waterfall: true`` item ranked by contribution, plus an
    ``is_headline_total`` row carrying the headline's own MoM in pp).

    All month arithmetic is calendar-based (``year × 12 + month``), so a
    publication gap yields nulls rather than comparing the wrong months.
    Items whose series aren't ingested emit no rows; the waterfall for a month
    appears only when the tree's headline printed that month.
    """
    if items is None:
        items = load_inflation_items()
    item_list = list(items)
    if not item_list:
        return {"explorer": [], "contribution": []}
    by_series = _group_sorted(
        r for r in latest_rows
        if r.get("series_id") in {i.series_id for i in item_list}
    )

    explorer: list[dict[str, Any]] = []
    # (basket, sa_nsa) -> {month_index: {...}} for the waterfall pass
    mom_by_tree: dict[tuple[str, str], dict[str, dict[int, Any]]] = {}

    for item in item_list:
        series = by_series.get(item.series_id)
        if not series:
            continue
        # month_index -> (date, value); ascending input, last obs in a month wins
        monthly: dict[int, tuple[date, float]] = {
            _month_index(d): (d, v) for d, v in series
        }
        mom_map: dict[int, Optional[float]] = {}
        for m in monthly:
            prev = monthly.get(m - 1)
            mom_map[m] = (
                _pct_change(monthly[m][1], prev[1]) if prev else None
            )
        tree = mom_by_tree.setdefault((item.basket, item.sa_nsa), {})
        tree[item.series_id] = mom_map

        for m in sorted(monthly):
            d, v = monthly[m]
            year_ago = monthly.get(m - 12)
            three_back = monthly.get(m - 3)
            prev_mom, cur_mom = mom_map.get(m - 1), mom_map[m]
            yoy = _pct_change(v, year_ago[1]) if year_ago else None
            prev_yoy = None
            if monthly.get(m - 1) and monthly.get(m - 13):
                prev_yoy = _pct_change(monthly[m - 1][1], monthly[m - 13][1])
            explorer.append({
                "series_id": item.series_id,
                "item_label": item.label,
                "parent_item": item.parent or None,
                "hierarchy_level": item.level,
                "basket": item.basket,
                "sa_nsa": item.sa_nsa,
                "observation_date": d.isoformat(),
                "index_value": v,
                "mom_pct": cur_mom,
                "yoy_pct": yoy,
                "mom_accel": (
                    cur_mom - prev_mom
                    if cur_mom is not None and prev_mom is not None else None
                ),
                "yoy_accel": (
                    yoy - prev_yoy
                    if yoy is not None and prev_yoy is not None else None
                ),
                "three_month_annualized": (
                    (v / three_back[1]) ** 4 - 1
                    if three_back and three_back[1] > 0 and v > 0 else None
                ),
                "weight": item.weight,
                "contribution_pp": (
                    item.weight * cur_mom
                    if item.weight is not None and cur_mom is not None else None
                ),
            })

    # Waterfall: per tree × month where the headline printed, the waterfall
    # items' contributions ranked largest-first, plus the headline-total row.
    contribution: list[dict[str, Any]] = []
    items_by_tree: dict[tuple[str, str], list[InflationItemDef]] = {}
    for item in item_list:
        items_by_tree.setdefault((item.basket, item.sa_nsa), []).append(item)
    for key in sorted(items_by_tree):
        basket, sa_nsa = key
        tree_items = items_by_tree[key]
        head = next((i for i in tree_items if i.level == 0), None)
        wf = [i for i in tree_items if i.waterfall]
        head_moms = mom_by_tree.get(key, {}).get(head.series_id, {}) if head else {}
        for m in sorted(head_moms):
            head_mom = head_moms[m]
            if head_mom is None:
                continue
            d = date(m // 12, m % 12 + 1, 1)
            rows = []
            for i in wf:
                mom = mom_by_tree[key].get(i.series_id, {}).get(m)
                if mom is None:
                    continue
                rows.append({
                    "observation_date": d.isoformat(),
                    "basket": basket,
                    "sa_nsa": sa_nsa,
                    "series_id": i.series_id,
                    "item_label": i.label,
                    "contribution_pp": i.weight * mom,
                    "rank_in_month": 0,
                    "is_headline_total": False,
                })
            rows.sort(key=lambda r: -r["contribution_pp"])
            for rank, r in enumerate(rows, start=1):
                r["rank_in_month"] = rank
            contribution.extend(rows)
            contribution.append({
                "observation_date": d.isoformat(),
                "basket": basket,
                "sa_nsa": sa_nsa,
                "series_id": head.series_id,
                "item_label": head.label,
                "contribution_pp": head_mom * 100.0,  # headline MoM in pp
                "rank_in_month": None,
                "is_headline_total": True,
            })
    return {"explorer": explorer, "contribution": contribution}


# ---- rolling-window stats companions -------------------------------------------

# Trailing observation-count windows (~trading-day horizons: day, week,
# 2 weeks, month, quarter, half-year, year).
ROLLING_WINDOWS = (1, 5, 10, 21, 63, 126, 252)


def _rolling_window_rows(
    series: list[tuple[date, float]],
    windows: tuple[int, ...] = ROLLING_WINDOWS,
) -> list[dict[str, Any]]:
    """Per observation × window: trailing change, percent change, and rolling
    z-score, computed with prefix sums (O(n × windows), not O(n × w)).

    A (observation, window) row is emitted only once the window is **fully
    populated** (observation index ≥ w) — no partial-window stats. ``change``
    is ``v_t − v_{t−w}`` in the series' native units; ``pct_change`` is
    relative to ``v_{t−w}`` (``None`` at a zero base); ``zscore`` is against
    the trailing-w rolling mean/std *including* the current value (``None``
    when the window std is 0 — always the case for w=1). Windows are
    observation counts, so for daily series they approximate trading-day
    horizons; the stats are trailing-only (point-in-time safe).
    """
    n = len(series)
    values = [v for _d, v in series]
    s = [0.0] * (n + 1)   # prefix sums of x and x²
    s2 = [0.0] * (n + 1)
    for i, v in enumerate(values):
        s[i + 1] = s[i] + v
        s2[i + 1] = s2[i] + v * v

    out: list[dict[str, Any]] = []
    for i in range(n):
        d, v = series[i]
        for w in windows:
            if i < w:
                continue
            base = values[i - w]
            mean = (s[i + 1] - s[i + 1 - w]) / w
            var = max((s2[i + 1] - s2[i + 1 - w]) / w - mean * mean, 0.0)
            std = var ** 0.5
            out.append({
                "observation_date": d.isoformat(),
                "window": w,
                "value": v,
                "change": v - base,
                "pct_change": ((v - base) / base) if base != 0 else None,
                "zscore": ((v - mean) / std) if std > 1e-12 else None,
            })
    return out


def compute_curve_spread_rolling(
    latest_rows: Iterable[dict[str, Any]],
    spreads: Optional[Iterable[SpreadDef]] = None,
    windows: tuple[int, ...] = ROLLING_WINDOWS,
) -> list[dict[str, Any]]:
    """``gold.curve_spread_rolling``: rolling-window companions to
    ``curve_spread_daily`` — per configured spread/ratio, the trailing change,
    percent change, and rolling z-score over each window. ``value``/``change``
    are in the spread's native units (percent points for rate spreads); note
    ``pct_change`` on a spread that crosses zero is of limited meaning and is
    provided for uniformity."""
    if spreads is None:
        spreads = load_spread_defs()
    base = compute_curve_spreads(latest_rows, spreads)
    by_name: dict[str, list[tuple[date, float]]] = {}
    for r in base:
        d = _parse(r["observation_date"])
        if d is not None:
            by_name.setdefault(r["spread_name"], []).append((d, r["value"]))
    out: list[dict[str, Any]] = []
    for name in sorted(by_name):
        series = sorted(by_name[name], key=lambda t: t[0])
        for row in _rolling_window_rows(series, windows):
            out.append({"spread_name": name, **row})
    return out


def compute_credit_spread_rolling(
    latest_rows: Iterable[dict[str, Any]],
    cfg: Optional[CreditConfig] = None,
    windows: tuple[int, ...] = ROLLING_WINDOWS,
) -> list[dict[str, Any]]:
    """``gold.credit_spread_rolling``: rolling-window companions to
    ``credit_spread_daily`` — per configured OAS instrument, over the spread
    in **bps** (credit convention), so ``change`` is a bps move."""
    if cfg is None:
        cfg = load_credit_config()
    by_series = _group_sorted(
        r for r in latest_rows
        if r.get("series_id") in {c.series_id for c in cfg.instruments}
    )
    out: list[dict[str, Any]] = []
    for cd in cfg.instruments:
        series = by_series.get(cd.series_id)
        if not series:
            continue
        bps = [(d, v * 100.0) for d, v in series]
        for row in _rolling_window_rows(bps, windows):
            out.append({
                "instrument": cd.instrument,
                "series_id": cd.series_id,
                "observation_date": row["observation_date"],
                "window": row["window"],
                "oas_bps": row["value"],
                "change_bps": row["change"],
                "pct_change": row["pct_change"],
                "zscore": row["zscore"],
            })
    return out


def compute_treasury_curve_rolling(
    latest_rows: Iterable[dict[str, Any]],
    tenors: Optional[Iterable[TenorDef]] = None,
    windows: tuple[int, ...] = ROLLING_WINDOWS,
) -> list[dict[str, Any]]:
    """``gold.treasury_curve_rolling``: rolling-window companions to
    ``treasury_curve`` — per tenor, over the constant-maturity yield in
    percent, so ``change`` is a percent-point move (×100 for bps)."""
    if tenors is None:
        tenors = load_curve_defs()
    tenor_list = sorted(tenors, key=lambda t: t.months)
    by_series = _group_sorted(
        r for r in latest_rows
        if r.get("series_id") in {t.series_id for t in tenor_list}
    )
    out: list[dict[str, Any]] = []
    for t in tenor_list:
        series = by_series.get(t.series_id)
        if not series:
            continue
        for row in _rolling_window_rows(series, windows):
            out.append({
                "tenor_label": t.label,
                "tenor_months": t.months,
                "series_id": t.series_id,
                "observation_date": row["observation_date"],
                "window": row["window"],
                "yield_pct": row["value"],
                "change": row["change"],
                "pct_change": row["pct_change"],
                "zscore": row["zscore"],
            })
    return out
