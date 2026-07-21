"""Dynamic Tiingo pricing plans for ETF constituents.

iShares/BlackRock holdings are the source of truth for membership and weights;
Tiingo is only used to fetch price/return inputs for those live constituents.
This module keeps that relationship explicit by deriving a small Tiingo batch
from the latest ``gold_index_constituents`` rows and the freshness of already
persisted Tiingo ``:adjClose`` Silver rows.
"""

from __future__ import annotations

import datetime as dt
import re
from dataclasses import dataclass
from typing import Any, Iterable, Optional

from fred_pipeline.manifest import SeriesSpec


_PRICEABLE_TICKER = re.compile(r"^[A-Z][A-Z.\-]*$")


@dataclass(frozen=True)
class ConstituentPriceCandidate:
    ticker: str
    reason: str
    weight_rank: int
    weight_pct: Optional[float] = None
    latest_price_date: Optional[str] = None


@dataclass(frozen=True)
class ConstituentPricingPlan:
    index_etf: str
    as_of_date: str
    stale_days: int
    total_constituents: int
    already_fresh: int
    skipped_unpriceable: tuple[str, ...]
    candidates: tuple[ConstituentPriceCandidate, ...]
    batch: tuple[ConstituentPriceCandidate, ...]


def is_tiingo_priceable_ticker(ticker: str) -> bool:
    """Return whether a holdings symbol is safe to send to Tiingo as a ticker.

    This intentionally excludes symbols containing digits, such as the
    futures-like ``ESU6`` seen in IVV holdings snapshots. It allows letters,
    dots, and hyphens so class-share tickers can still be reviewed/run.
    """
    t = str(ticker or "").strip().upper()
    return bool(_PRICEABLE_TICKER.fullmatch(t))


def _parse_date(value: Any) -> Optional[dt.date]:
    if value in (None, ""):
        return None
    try:
        return dt.date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def plan_tiingo_constituent_pricing(
    constituent_rows: Iterable[dict[str, Any]],
    latest_price_rows: Iterable[dict[str, Any]],
    *,
    index_etf: str = "IVV",
    as_of_date: Optional[dt.date] = None,
    stale_days: int = 7,
    limit: int = 25,
) -> ConstituentPricingPlan:
    """Select missing/stale constituent tickers for the next Tiingo batch.

    ``constituent_rows`` should be the latest rows from
    ``gold_index_constituents`` for one ETF. ``latest_price_rows`` should be one
    row per ticker with its latest Tiingo ``:adjClose`` date. Candidates are
    sorted by holdings weight rank so the most material missing prices are
    pulled first.
    """
    if stale_days < 0:
        raise ValueError("stale_days must be >= 0")
    if limit < 1:
        raise ValueError("limit must be >= 1")

    today = as_of_date or dt.date.today()
    stale_before = today - dt.timedelta(days=stale_days)

    prices = {
        str(r.get("ticker", "")).strip().upper(): _parse_date(
            r.get("latest_price_date")
        )
        for r in latest_price_rows
    }

    by_ticker: dict[str, dict[str, Any]] = {}
    skipped: set[str] = set()
    for row in constituent_rows:
        ticker = str(row.get("ticker") or row.get("constituent") or "").strip().upper()
        if not ticker:
            continue
        if not is_tiingo_priceable_ticker(ticker):
            skipped.add(ticker)
            continue

        rank = int(row.get("weight_rank") or 999999)
        existing = by_ticker.get(ticker)
        if existing is None or rank < int(existing.get("weight_rank") or 999999):
            by_ticker[ticker] = {
                "ticker": ticker,
                "weight_rank": rank,
                "weight_pct": row.get("weight_pct"),
            }

    candidates: list[ConstituentPriceCandidate] = []
    already_fresh = 0
    for ticker, row in by_ticker.items():
        latest = prices.get(ticker)
        if latest is None:
            reason = "missing"
        elif latest < stale_before:
            reason = "stale"
        else:
            already_fresh += 1
            continue
        candidates.append(
            ConstituentPriceCandidate(
                ticker=ticker,
                reason=reason,
                weight_rank=int(row["weight_rank"]),
                weight_pct=(
                    None if row.get("weight_pct") is None else float(row["weight_pct"])
                ),
                latest_price_date=latest.isoformat() if latest else None,
            )
        )

    candidates.sort(key=lambda c: (c.reason == "stale", c.weight_rank, c.ticker))
    batch = tuple(candidates[:limit])
    return ConstituentPricingPlan(
        index_etf=index_etf.upper(),
        as_of_date=today.isoformat(),
        stale_days=stale_days,
        total_constituents=len(by_ticker) + len(skipped),
        already_fresh=already_fresh,
        skipped_unpriceable=tuple(sorted(skipped)),
        candidates=tuple(candidates),
        batch=batch,
    )


def specs_for_tiingo_candidates(
    candidates: Iterable[ConstituentPriceCandidate],
) -> list[SeriesSpec]:
    """Convert planned candidates into active Tiingo ``SeriesSpec`` objects."""
    specs: list[SeriesSpec] = []
    for c in candidates:
        specs.append(
            SeriesSpec(
                series_id=c.ticker,
                title=f"{c.ticker} ETF constituent daily price and total-return inputs",
                category="equity",
                frequency="d",
                units="USD",
                active=True,
                source="tiingo",
                vintage_enabled=False,
                validation_profile="standard",
                downstream_use_case="equity_constituent_total_return",
                priority=3,
                min_value=0,
                tags=["equity", "stock", "etf_constituent", "tiingo", "total_return"],
            )
        )
    return specs
