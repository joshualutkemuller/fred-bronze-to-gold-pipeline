"""Stooq source client — free, keyless daily OHLCV for stocks and ETFs.

Stooq publishes per-ticker daily history as CSV with no key and no account
(``https://stooq.com/q/d/l/?s=<symbol>&i=d`` → ``Date,Open,High,Low,Close,
Volume``). It is the pipeline's **price-return** equity source (split-adjusted
close; **no dividends**, so total return needs the Tiingo half). Coverage is
broad — thousands of US tickers — at essentially no rate cost.

A manifest ``series_id`` encodes the ticker plus which OHLCV field this series
carries, so a stock bar (multi-field) fits the scalar-``value`` Silver schema
by exploding into one series per field — the same composite-id convention
Treasury (``<dataset>:<field>``) and World Bank (``<country>:<indicator>``)
use::

    AAPL:close     SPY:close     AAPL:volume
    └tk┘ └field┘

Field defaults to ``close`` (``AAPL`` alone means ``AAPL:close``). US tickers
get the ``.us`` market suffix Stooq expects appended automatically. The
transport (retry/backoff/rate-limit) is inherited from
:class:`fred_pipeline.sources.base.HTTPSource`; only the CSV shape is
Stooq-specific.
"""

from __future__ import annotations

import csv
import io
import logging
import time
from typing import Any, Callable, Optional

from fred_pipeline.sources.base import HTTPSource, SourceError
from fred_pipeline.transform import _row_hash, _utc_now_iso, parse_value

log = logging.getLogger("fred_pipeline.sources.stooq")

# OHLCV fields a series_id may select, mapped to the Stooq CSV header.
FIELD_TO_HEADER = {
    "open": "Open", "high": "High", "low": "Low",
    "close": "Close", "volume": "Volume",
}
DEFAULT_FIELD = "close"


class StooqAPIError(SourceError):
    """Raised when Stooq returns an unrecoverable error / unparseable body."""


def _parse_series_id(series_id: str) -> tuple[str, str]:
    """Split ``<ticker>[:<field>]`` into ``(ticker, field)`` (field default
    ``close``). The ticker is upper-cased; the field lower-cased and validated."""
    ticker, sep, field = series_id.partition(":")
    ticker = ticker.strip().upper()
    field = (field.strip().lower() or DEFAULT_FIELD) if sep else DEFAULT_FIELD
    if not ticker:
        raise StooqAPIError(
            f"Stooq series_id must be '<ticker>[:<field>]', got {series_id!r}"
        )
    if field not in FIELD_TO_HEADER:
        raise StooqAPIError(
            f"Stooq series_id {series_id!r} has invalid field {field!r}; "
            f"expected one of {sorted(FIELD_TO_HEADER)}"
        )
    return ticker, field


def _stooq_symbol(ticker: str) -> str:
    """Map a bare US ticker to the Stooq symbol (``aapl`` → ``aapl.us``).
    A ticker that already carries a market suffix (contains a dot) is left as-is
    so non-US symbols still work."""
    t = ticker.lower()
    return t if "." in t else f"{t}.us"


def normalize_stooq_observations(
    series_id: str,
    payload: Any,
    *,
    run_id: Optional[str] = None,
    ingested_at: Optional[str] = None,
    source: str = "stooq",
) -> list[dict[str, Any]]:
    """Convert a raw Stooq CSV payload into canonical silver rows for the one
    field named in ``series_id``.

    ``payload`` is the envelope :meth:`StooqClient.get_observations` returns:
    ``{"format": "csv", "field": <field>, "text": <csv>}``. A body Stooq
    returns for an unknown symbol (no ``Date`` header, or ``N/D``) yields no
    rows rather than raising — one bad ticker never fails a run.
    """
    ingested_at = ingested_at or _utc_now_iso()
    if not isinstance(payload, dict):
        return []
    text = payload.get("text") or ""
    field = payload.get("field") or DEFAULT_FIELD
    header_name = FIELD_TO_HEADER.get(field)
    if not header_name or not text.strip():
        return []

    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames or "Date" not in reader.fieldnames \
            or header_name not in reader.fieldnames:
        return []

    rows: list[dict[str, Any]] = []
    for rec in reader:
        obs_date = (rec.get("Date") or "").strip()
        if len(obs_date) != 10 or obs_date[4] != "-":
            continue  # skip a malformed/footer line
        raw_value = rec.get(header_name)
        value = parse_value(raw_value)
        rows.append({
            "source": source,
            "series_id": series_id,
            "observation_date": obs_date,
            "realtime_start": "",
            "realtime_end": "",
            "value": value,
            "raw_value": None if raw_value is None else str(raw_value),
            "is_missing": value is None,
            "row_hash": _row_hash(series_id, obs_date, "", raw_value),
            "ingested_at": ingested_at,
            "run_id": run_id,
        })
    return rows


class StooqClient(HTTPSource):
    """Retrying, rate-limited Stooq CSV client (keyless)."""

    source_name = "STOOQ"
    error_cls = StooqAPIError

    def __init__(
        self,
        base_url: str = "https://stooq.com",
        *,
        session: Any = None,
        timeout: int = 30,
        max_retries: int = 5,
        rate_limit_per_minute: int = 60,
        sleep: Callable[[float], None] = time.sleep,
    ):
        super().__init__(
            base_url=base_url,
            session=session,
            timeout=timeout,
            max_retries=max_retries,
            rate_limit_per_minute=rate_limit_per_minute,
            sleep=sleep,
        )

    def observations_endpoint(self, series_id: str) -> str:
        """Lineage string recorded in Bronze (the download path)."""
        ticker, _field = _parse_series_id(series_id)
        return f"q/d/l?s={_stooq_symbol(ticker)}&i=d"

    # ---- SourceClient contract ------------------------------------------

    def get_observations(
        self,
        series_id: str,
        *,
        observation_start: Optional[str] = None,
        observation_end: Optional[str] = None,
        **_ignored: Any,
    ) -> dict[str, Any]:
        """Fetch a ticker's daily CSV, returning a small JSON-serializable
        envelope (Bronze archives it verbatim). ``d1``/``d2`` bound the range
        when a start/end is supplied (Stooq wants ``YYYYMMDD``)."""
        ticker, field = _parse_series_id(series_id)
        params: dict[str, Any] = {"s": _stooq_symbol(ticker), "i": "d"}
        if observation_start:
            params["d1"] = str(observation_start).replace("-", "")[:8]
        if observation_end:
            params["d2"] = str(observation_end).replace("-", "")[:8]
        text = self._request("q/d/l", params, as_text=True)
        return {"format": "csv", "field": field, "text": text}

    def normalize(
        self,
        series_id: str,
        payload: Any,
        *,
        run_id: Optional[str] = None,
        track_vintage: bool = False,  # market prices carry no vintages
        source: str = "stooq",
        **_ignored: Any,
    ) -> list[dict[str, Any]]:
        return normalize_stooq_observations(
            series_id, payload, run_id=run_id, source=source
        )
