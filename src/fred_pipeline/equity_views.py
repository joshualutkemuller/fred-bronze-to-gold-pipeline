"""Equity Gold engines (pure Python), shared by both backends.

First slice of the equity sub-plan (handoff.md → "Equity Price & Total Return
— Two-Source Sub-Plan"): the licensing-free Stooq price-return and the ETF
constituent tables. The Tiingo total-return path
(``gold.equity_total_return_index``) is the planned second slice.

  * :func:`compute_equity_return_daily` → ``gold.equity_return_daily``: daily
    price return per ticker from the exploded ``<ticker>:close`` Silver series
    (Stooq close is split-adjusted, so this is a clean price return).
  * :func:`compute_index_constituents` → ``gold.index_constituents``: the
    per-snapshot constituent weights exploded from ``<ETF>:<constituent>``
    holdings series, with each ETF's latest snapshot flagged.

Both read the scalar Silver directly — the ``<ticker>:<field>`` /
``<ETF>:<constituent>`` composite-id convention lets equity bars and holdings
lists fit the single-``value`` schema with no new tables in Bronze/Silver.
"""

from __future__ import annotations

from datetime import date
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
