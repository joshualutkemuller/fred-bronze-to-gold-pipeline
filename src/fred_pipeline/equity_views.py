"""Equity Gold engines (pure Python), shared by both backends.

Equity sub-plan (handoff.md → "Equity Price & Total Return — Two-Source
Sub-Plan"):

  * :func:`compute_equity_return_daily` → ``gold.equity_return_daily``: daily
    price return per ticker from the exploded Stooq ``<ticker>:close`` series
    (split-adjusted close → clean price return).
  * :func:`compute_index_constituents` → ``gold.index_constituents``: the
    per-snapshot ETF constituent weights from ``<ETF>:<constituent>`` series.
  * :func:`compute_equity_total_return_index` →
    ``gold.equity_total_return_index``: true total return per ticker from the
    Tiingo raw inputs (``close`` + ``divCash`` + ``splitFactor``), dividends
    reinvested — derived from raw so it survives dividend restatements.

**Source isolation.** Stooq and Tiingo both name a ``<ticker>:close`` series,
so the callers pass **source-filtered** Silver rows (Stooq → price return,
Tiingo → total return, iShares → constituents) rather than the merged
latest-observation table — which would otherwise collapse the two ``:close``
sources for the same ticker/date onto one row. The engines themselves stay
source-agnostic (they just consume the rows handed to them).
"""

from __future__ import annotations

from bisect import bisect_left
from datetime import date, timedelta
from typing import Any, Iterable

from fred_pipeline.features import _parse, _pct_change

# Silver series_id suffix carrying the (split-adjusted) close.
CLOSE_FIELD = "close"


def _close_ticker(series_id: str) -> str | None:
    """``AAPL:close`` → ``AAPL``; anything not a ``:close`` series → None."""
    ticker, sep, field = series_id.partition(":")
    if sep and field == CLOSE_FIELD and ticker:
        return ticker
    return None


def compute_equity_return_daily(
    latest_rows: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    """``gold.equity_return_daily``: one row per ticker × date from the
    ``<ticker>:close`` Silver series — the close, the day-over-day price change
    and simple return, and a cumulative price-return index (=100 at each
    ticker's first observation). Stooq close is split-adjusted, so the simple
    return is a clean price return (dividends excluded — that's the Tiingo
    total-return slice). Multi-source note: only ``source='stooq'`` (or any
    ``:close`` series) participates; a ticker with a single observation emits
    that row with null return."""
    by_ticker: dict[str, list[tuple[date, float]]] = {}
    for r in latest_rows:
        ticker = _close_ticker(r.get("series_id", ""))
        if ticker is None or r.get("is_missing"):
            continue
        v, d = r.get("value"), _parse(r.get("observation_date"))
        if v is None or d is None:
            continue
        by_ticker.setdefault(ticker, []).append((d, float(v)))

    out: list[dict[str, Any]] = []
    for ticker in sorted(by_ticker):
        series = sorted(by_ticker[ticker], key=lambda t: t[0])
        index = 100.0
        for i, (d, close) in enumerate(series):
            prev = series[i - 1][1] if i > 0 else None
            ret = _pct_change(close, prev)
            if ret is not None:
                index *= 1.0 + ret
            out.append({
                "ticker": ticker,
                "observation_date": d.isoformat(),
                "close": close,
                "price_change": (close - prev) if prev is not None else None,
                "price_return": ret,
                "price_return_index": index,
            })
    return out


def compute_index_constituents(
    latest_rows: Iterable[dict[str, Any]],
    etfs: Iterable[str] = ("IVV", "SPY"),
) -> list[dict[str, Any]]:
    """``gold.index_constituents``: one row per ETF × constituent × snapshot
    date from the ``<ETF>:<constituent>`` holdings weight series — the weight
    (percent), a within-snapshot weight rank, and an ``is_latest_snapshot``
    flag on the most recent as-of date per ETF (the row set a report filters to
    for "current membership"). ``etfs`` bounds which prefixes are treated as
    holdings series so a normal ``FOO:close`` price series is never mistaken
    for a constituent."""
    etf_set = {e.upper() for e in etfs}
    # etf -> obs_date -> list[(constituent, weight)]
    snapshots: dict[str, dict[str, list[tuple[str, float]]]] = {}
    for r in latest_rows:
        if r.get("is_missing") or r.get("value") is None:
            continue
        sid = r.get("series_id", "")
        etf, sep, constituent = sid.partition(":")
        if not sep or etf.upper() not in etf_set or not constituent:
            continue
        d = _parse(r.get("observation_date"))
        if d is None:
            continue
        snapshots.setdefault(etf.upper(), {}).setdefault(
            d.isoformat(), []
        ).append((constituent, float(r["value"])))

    out: list[dict[str, Any]] = []
    for etf in sorted(snapshots):
        by_date = snapshots[etf]
        latest_date = max(by_date)
        for obs_date in sorted(by_date):
            ranked = sorted(by_date[obs_date], key=lambda t: -t[1])
            for rank, (constituent, weight) in enumerate(ranked, start=1):
                out.append({
                    "index_etf": etf,
                    "constituent": constituent,
                    "observation_date": obs_date,
                    "weight_pct": weight,
                    "weight_rank": rank,
                    "is_latest_snapshot": obs_date == latest_date,
                })
    return out


# ---- Tiingo total return ---------------------------------------------------

def _field_maps(
    tiingo_rows: Iterable[dict[str, Any]]
) -> dict[str, dict[str, dict[date, float]]]:
    """Group exploded Tiingo rows into ``ticker -> field -> {date: value}``
    for the ``close`` / ``divCash`` / ``splitFactor`` fields."""
    wanted = {"close", "divCash", "splitFactor"}
    out: dict[str, dict[str, dict[date, float]]] = {}
    for r in tiingo_rows:
        if r.get("is_missing") or r.get("value") is None:
            continue
        ticker, sep, field = r.get("series_id", "").partition(":")
        if not sep or field not in wanted:
            continue
        d = _parse(r.get("observation_date"))
        if d is None:
            continue
        out.setdefault(ticker, {}).setdefault(field, {})[d] = float(r["value"])
    return out


def compute_equity_total_return_index(
    tiingo_rows: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    """``gold.equity_total_return_index``: true total return per ticker × date
    reconstructed from the Tiingo raw inputs (``close`` + ``divCash`` +
    ``splitFactor``), dividends reinvested.

    Per day, using the split-adjusted convention (``close`` is the raw close;
    ``splitFactor`` is that day's split ratio; ``divCash`` the per-share cash
    dividend):

        price_return_t = close_t / close_{t-1} × splitFactor_t − 1
        total_return_t = (close_t + divCash_t) / close_{t-1} × splitFactor_t − 1

    A split day (price halves, factor 2) nets ~0; a dividend day adds
    ``div/close_{t-1}``. ``total_return_index`` / ``price_return_index`` are
    cumulative (=100 at each ticker's first date); their gap is the reinvested
    income. ``trailing_12m_dividend`` sums ``divCash`` over the trailing 365
    days and ``dividend_yield_pct`` divides it by the close. Deriving from raw
    inputs (not Tiingo's ``adjClose``) means the series can be rebuilt and
    diffed when a dividend is restated. Same-day split+dividend (rare) uses the
    formula as written.
    """
    maps = _field_maps(tiingo_rows)
    out: list[dict[str, Any]] = []
    for ticker in sorted(maps):
        closes = maps[ticker].get("close")
        if not closes:
            continue
        divs = maps[ticker].get("divCash", {})
        splits = maps[ticker].get("splitFactor", {})
        dates = sorted(closes)
        div_dates = sorted(divs)
        tr_index = pr_index = 100.0
        prev_close: float | None = None
        for d in dates:
            close = closes[d]
            div = divs.get(d, 0.0)
            split = splits.get(d, 1.0) or 1.0
            if prev_close and prev_close > 0:
                pr = close / prev_close * split - 1.0
                tr = (close + div) / prev_close * split - 1.0
                pr_index *= 1.0 + pr
                tr_index *= 1.0 + tr
            else:
                pr = tr = None
            # trailing-365d dividend sum (inclusive), via the sorted div dates.
            cutoff = d - timedelta(days=365)
            ttm_div = sum(
                divs[dd] for dd in div_dates[bisect_left(div_dates, cutoff):]
                if dd <= d
            )
            out.append({
                "ticker": ticker,
                "observation_date": d.isoformat(),
                "close": close,
                "dividend": div,
                "split_factor": split,
                "price_return": pr,
                "total_return": tr,
                "price_return_index": pr_index,
                "total_return_index": tr_index,
                "trailing_12m_dividend": ttm_div,
                "dividend_yield_pct": (ttm_div / close * 100.0) if close else None,
            })
            prev_close = close
    return out
