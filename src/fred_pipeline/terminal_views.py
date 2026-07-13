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
    turns non-negative again (with trough, duration, and recession overlap).

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
                if episode is None:
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
