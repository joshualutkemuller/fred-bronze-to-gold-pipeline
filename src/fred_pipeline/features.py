"""Quant feature engineering (pure Python).

Derived Gold features for research / optimizer / ML inputs, computed from the
latest-revision series and the full vintage history:

  * **transforms** — period-over-period % change (MoM-style), first difference,
    year-over-year % change (date-based), and an **expanding, point-in-time
    safe** z-score, per series;
  * **curve spreads** — differences between series (e.g. 10Y-2Y);
  * **as-of-date point-in-time snapshot** — each series' value *as it was known*
    on a given date (leakage-free feature vector for backtests).

Kept pure so the Local (SQLite) and Databricks backends share the same tested
logic; the Spark equivalents live in :mod:`fred_pipeline.gold`.
"""

from __future__ import annotations

from bisect import bisect_right
from datetime import date, datetime, timedelta
from typing import Any, Iterable, Optional

# Common Treasury curve spreads over the seed universe (name, long_leg, short_leg).
DEFAULT_CURVE_SPREADS: tuple[tuple[str, str, str], ...] = (
    ("T10Y2Y", "DGS10", "DGS2"),
    ("T10Y3M", "DGS10", "DGS3MO"),
    ("T2Y3M", "DGS2", "DGS3MO"),
    ("T30Y10Y", "DGS30", "DGS10"),
)

# How far back a "year ago" match may be before YoY is left null (daily series
# won't have an exact −365d point).
YOY_TOLERANCE_DAYS = 40


def _parse(d: Any) -> Optional[date]:
    try:
        return datetime.strptime(str(d)[:10], "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


def _pct_change(cur: Optional[float], base: Optional[float]) -> Optional[float]:
    if cur is None or base is None or base == 0:
        return None
    return (cur - base) / base


def _group_sorted(rows: Iterable[dict[str, Any]]) -> dict[str, list[tuple[date, float]]]:
    by_series: dict[str, list[tuple[date, float]]] = {}
    for r in rows:
        if r.get("is_missing"):
            continue
        v = r.get("value")
        d = _parse(r.get("observation_date"))
        if v is None or d is None:
            continue
        by_series.setdefault(r["series_id"], []).append((d, float(v)))
    for series in by_series.values():
        series.sort(key=lambda t: t[0])
    return by_series


def _year_ago_value(
    dates: list[date], values: list[float], i: int
) -> Optional[float]:
    """Value at the observation nearest on-or-before ~1 year before dates[i]."""
    target = dates[i] - timedelta(days=365)
    pos = bisect_right(dates, target) - 1  # latest date <= target
    if pos < 0:
        return None
    if (target - dates[pos]).days > YOY_TOLERANCE_DAYS:
        return None
    return values[pos]


def _expanding_mean_std(values: list[float]) -> tuple[list[float], list[float]]:
    """Expanding (point-in-time safe) population mean/std, via Welford's
    online algorithm.

    ``means[i]``/``stds[i]`` are computed from ``values[0..i]`` only — never
    later values — so a z-score built from them can't leak future
    information into earlier rows the way a full-sample mean/std would.
    """
    means: list[float] = []
    stds: list[float] = []
    mean = 0.0
    m2 = 0.0
    for n, v in enumerate(values, start=1):
        delta = v - mean
        mean += delta / n
        m2 += delta * (v - mean)
        means.append(mean)
        stds.append((m2 / n) ** 0.5 if n > 1 else 0.0)
    return means, stds


def compute_feature_transforms(
    latest_rows: Iterable[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Per-series transforms from latest-revision rows (one per obs date)."""
    grouped = _group_sorted(latest_rows)
    out: list[dict[str, Any]] = []
    for series_id, series in grouped.items():
        dates = [d for d, _ in series]
        values = [v for _, v in series]
        exp_means, exp_stds = _expanding_mean_std(values)
        for i, (d, v) in enumerate(series):
            prev = values[i - 1] if i > 0 else None
            year_ago = _year_ago_value(dates, values, i)
            std = exp_stds[i]
            out.append({
                "series_id": series_id,
                "observation_date": d.isoformat(),
                "value": v,
                "mom": _pct_change(v, prev),
                "diff": (v - prev) if prev is not None else None,
                "yoy": _pct_change(v, year_ago),
                "zscore": ((v - exp_means[i]) / std) if std else None,
            })
    return out


def compute_curve_spreads(
    latest_rows: Iterable[dict[str, Any]],
    spreads: Iterable[tuple[str, str, str]] = DEFAULT_CURVE_SPREADS,
) -> list[dict[str, Any]]:
    """Compute spread series (long_leg − short_leg) where both legs exist."""
    values: dict[tuple[str, str], float] = {}
    for r in latest_rows:
        if r.get("is_missing") or r.get("value") is None:
            continue
        values[(r["series_id"], str(r["observation_date"])[:10])] = float(r["value"])

    dates_by_series: dict[str, set[str]] = {}
    for (sid, d) in values:
        dates_by_series.setdefault(sid, set()).add(d)

    out: list[dict[str, Any]] = []
    for name, long_leg, short_leg in spreads:
        common = dates_by_series.get(long_leg, set()) & dates_by_series.get(short_leg, set())
        for d in sorted(common):
            out.append({
                "spread_name": name,
                "observation_date": d,
                "long_leg": long_leg,
                "short_leg": short_leg,
                "value": values[(long_leg, d)] - values[(short_leg, d)],
            })
    return out


def compute_revision_stats(silver_rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """How much each observation moved between its first print and today.

    Unlike the other transforms (which read *latest-revision* rows),
    this reads raw **Silver** rows — every vintage — since it exists to
    measure revision behavior itself. Groups by ``(series_id,
    observation_date)`` and compares the vintage with the lowest
    ``revision_number`` (first release) against the one with the highest
    (latest as of this run).

    Non-vintage series (``vintage_enabled: false``) always have
    ``revision_count == 1`` — no vintage history is tracked for them, so
    there's nothing to compare; that's a legitimate "not revised" signal,
    not a data gap.
    """
    groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for r in silver_rows:
        if r.get("is_missing") or r.get("value") is None:
            continue
        key = (r["series_id"], str(r["observation_date"])[:10])
        groups.setdefault(key, []).append(r)

    out: list[dict[str, Any]] = []
    for (series_id, obs_date), rows in groups.items():
        rows.sort(key=lambda r: r.get("revision_number") or 0)
        first, latest = rows[0], rows[-1]
        first_value = float(first["value"])
        latest_value = float(latest["value"])
        delta = latest_value - first_value
        out.append({
            "series_id": series_id,
            "observation_date": obs_date,
            "revision_count": len(rows),
            "first_value": first_value,
            "first_realtime_start": first.get("realtime_start") or "",
            "latest_value": latest_value,
            "latest_realtime_start": latest.get("realtime_start") or "",
            "revision_delta": delta,
            "revision_pct": (delta / first_value) if first_value else None,
        })
    return sorted(out, key=lambda r: (r["series_id"], r["observation_date"]))


def point_in_time_snapshot(
    silver_rows: Iterable[dict[str, Any]], as_of: str
) -> list[dict[str, Any]]:
    """Each series' latest value *known as of* ``as_of`` (leakage-free vector).

    Uses the vintage windows: a row is "known" when
    ``realtime_start <= as_of`` and (``realtime_end`` empty/open or ``> as_of``).
    Among known rows per series, the latest ``observation_date`` wins, and among
    that date's vintages, the latest ``realtime_start``. Non-vintage rows
    (blank realtime) are always considered known.
    """
    as_of = str(as_of)[:10]
    best: dict[str, dict[str, Any]] = {}
    for r in silver_rows:
        if r.get("is_missing"):
            continue
        rt_start = (r.get("realtime_start") or "")[:10]
        rt_end = (r.get("realtime_end") or "")[:10]
        if rt_start and rt_start > as_of:
            continue  # not yet known
        if rt_end and rt_end not in ("", "9999-12-31") and rt_end <= as_of:
            continue  # superseded before as_of
        sid = r["series_id"]
        obs = str(r["observation_date"])[:10]
        cur = best.get(sid)
        key = (obs, rt_start)
        if cur is None or key > (cur["observation_date"], cur.get("_rt", "")):
            best[sid] = {
                "as_of_date": as_of,
                "series_id": sid,
                "observation_date": obs,
                "value": r.get("value"),
                "_rt": rt_start,
            }
    result = [{k: v for k, v in row.items() if k != "_rt"} for row in best.values()]
    return sorted(result, key=lambda r: r["series_id"])
