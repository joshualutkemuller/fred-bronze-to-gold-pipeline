"""ETF constituent-holdings source client (iShares / State Street CSV).

Index *membership* is licensed data with no good free API. The free,
defensible workaround: iShares and State Street publish full ETF holdings as
daily CSV. This client fetches a fund's holdings file (keyless) and explodes
it into per-constituent **weight** rows, so ``gold.index_constituents`` can be
built and the same list can seed the equity symbol universe
(:func:`build_equity_manifest`).

A manifest ``series_id`` is the ETF ticker (e.g. ``IVV``, ``SPY``); the
download URL is resolved from :data:`HOLDINGS_URLS`. One fetch explodes into
one scalar Silver series per constituent — ``series_id = <ETF>:<constituent>``,
``value = weight %``, ``observation_date = the file's "as of" date`` — so
membership/weight history accumulates through the normal incremental path with
no schema change. The holdings CSV has a preamble; the parser locates the
"as of" date and the header row tolerantly rather than assuming fixed offsets.

⚠️ The URLs in :data:`HOLDINGS_URLS` are product-specific and change; verify
each against the fund's page before activating.
"""

from __future__ import annotations

import csv
import io
import logging
import re
import time
from datetime import datetime
from typing import Any, Callable, Optional

from fred_pipeline.sources.base import HTTPSource, SourceError
from fred_pipeline.transform import _row_hash, _utc_now_iso, parse_value

log = logging.getLogger("fred_pipeline.sources.ishares")

# ETF ticker -> holdings-CSV URL. Starter set; VERIFY before activating.
HOLDINGS_URLS: dict[str, str] = {
    # iShares Core S&P 500 ETF (IVV) — "Detailed Holdings and Analytics" CSV.
    "IVV": ("https://www.ishares.com/us/products/239726/ishares-core-sp-500-etf/"
            "1467271812596.ajax?fileType=csv&fileName=IVV_holdings&dataType=fund"),
    # State Street SPDR S&P 500 ETF (SPY) — SSGA holds-daily CSV.
    "SPY": ("https://www.ssga.com/us/en/intermediary/etfs/library-content/"
            "products/fund-data/etfs/us/holdings-daily-us-en-spy.xlsx"),
}

# "as of" date formats seen in iShares/SSGA preambles.
_DATE_FORMATS = ("%b %d, %Y", "%d-%b-%Y", "%m/%d/%Y", "%Y-%m-%d")
_ASOF_RE = re.compile(r"as of[\"',]*\s*[\"']?([A-Za-z0-9 ,/\-]+?)[\"']?\s*$", re.I)

# Asset-class / ticker values that are not equity constituents.
_NON_EQUITY = {"", "-", "CASH", "USD", "MARGIN_USD", "BRL", "FX"}


def _parse_asof(text: str) -> Optional[str]:
    """Pull the holdings 'as of' date from the CSV preamble → ISO date."""
    for line in text.splitlines()[:15]:
        m = _ASOF_RE.search(line.strip())
        if not m:
            continue
        raw = m.group(1).strip()
        for fmt in _DATE_FORMATS:
            try:
                return datetime.strptime(raw, fmt).date().isoformat()
            except ValueError:
                continue
    return None


def _find_header(lines: list[str]) -> int:
    """Index of the holdings header row (contains 'Ticker' and a 'Weight'
    column), or -1."""
    for i, line in enumerate(lines):
        low = line.lower()
        if "ticker" in low and "weight" in low:
            return i
    return -1


def normalize_ishares_holdings(
    series_id: str,
    payload: Any,
    *,
    run_id: Optional[str] = None,
    ingested_at: Optional[str] = None,
    source: str = "ishares",
) -> list[dict[str, Any]]:
    """Explode a holdings CSV into one weight row per constituent.

    ``payload`` is :meth:`ISharesClient.get_observations`'s envelope
    ``{"format": "csv", "etf": <ETF>, "text": <csv>}``. Emits
    ``series_id = <ETF>:<constituent-ticker>``, ``value = weight %``. A file
    with no parseable "as of" date or header yields no rows (logged) rather
    than guessing a date.
    """
    ingested_at = ingested_at or _utc_now_iso()
    if not isinstance(payload, dict):
        return []
    text = payload.get("text") or ""
    etf = (payload.get("etf") or series_id).upper()
    as_of = _parse_asof(text)
    if not as_of or not text.strip():
        log.warning("ishares %s: no parseable 'as of' date; skipping", etf)
        return []

    lines = text.splitlines()
    hi = _find_header(lines)
    if hi < 0:
        return []
    reader = csv.DictReader(io.StringIO("\n".join(lines[hi:])))
    cols = {c.lower().strip(): c for c in (reader.fieldnames or [])}
    tk_col = cols.get("ticker")
    wt_col = next((orig for low, orig in cols.items() if low.startswith("weight")), None)
    if not tk_col or not wt_col:
        return []
    sh_col = next((orig for low, orig in cols.items() if low.startswith("shares")), None)

    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for rec in reader:
        ticker = (rec.get(tk_col) or "").strip().upper()
        if ticker in _NON_EQUITY or ticker in seen:
            continue
        weight = parse_value(rec.get(wt_col))
        if weight is None:
            continue
        seen.add(ticker)
        constituent_series = f"{etf}:{ticker}"
        raw = rec.get(wt_col)
        row = {
            "source": source,
            "series_id": constituent_series,
            "observation_date": as_of,
            "realtime_start": "",
            "realtime_end": "",
            "value": weight,
            "raw_value": None if raw is None else str(raw),
            "is_missing": False,
            "row_hash": _row_hash(constituent_series, as_of, "", raw),
            "ingested_at": ingested_at,
            "run_id": run_id,
        }
        if sh_col is not None:
            row["shares"] = parse_value(rec.get(sh_col))
        rows.append(row)
    return rows


class ISharesClient(HTTPSource):
    """Keyless ETF-holdings CSV client (iShares / State Street)."""

    source_name = "ISHARES"
    error_cls = SourceError

    def __init__(
        self,
        base_url: str = "https://www.ishares.com",
        *,
        holdings_urls: Optional[dict[str, str]] = None,
        session: Any = None,
        timeout: int = 30,
        max_retries: int = 5,
        rate_limit_per_minute: int = 30,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self.holdings_urls = holdings_urls or dict(HOLDINGS_URLS)
        super().__init__(
            base_url=base_url,
            session=session,
            timeout=timeout,
            max_retries=max_retries,
            rate_limit_per_minute=rate_limit_per_minute,
            sleep=sleep,
        )

    def _resolve_url(self, series_id: str) -> str:
        etf = series_id.strip().upper()
        url = self.holdings_urls.get(etf)
        if not url:
            raise SourceError(
                f"ishares: no holdings URL registered for ETF {etf!r}; add it to "
                f"HOLDINGS_URLS or pass holdings_urls="
            )
        return url

    def observations_endpoint(self, series_id: str) -> str:
        return self._resolve_url(series_id)

    # ---- SourceClient contract ------------------------------------------

    def get_observations(self, series_id: str, **_ignored: Any) -> dict[str, Any]:
        """Fetch a fund's holdings CSV verbatim. The full URL is absolute, so
        it is requested directly (bypassing ``base_url``)."""
        etf = series_id.strip().upper()
        url = self._resolve_url(series_id)
        # Absolute URL: temporarily swap base_url so the shared retry/rate-limit
        # transport still applies.
        saved, self.base_url = self.base_url, ""
        try:
            text = self._request(url, {}, as_text=True)
        finally:
            self.base_url = saved
        return {"format": "csv", "etf": etf, "text": text}

    def normalize(
        self,
        series_id: str,
        payload: Any,
        *,
        run_id: Optional[str] = None,
        track_vintage: bool = False,
        source: str = "ishares",
        **_ignored: Any,
    ) -> list[dict[str, Any]]:
        return normalize_ishares_holdings(
            series_id, payload, run_id=run_id, source=source
        )


def build_equity_manifest(
    holdings_csv: str,
    *,
    name: str = "equity_stooq",
    field: str = "close",
    source: str = "stooq",
    active: bool = False,
    include_etf: Optional[str] = None,
) -> dict[str, Any]:
    """Generate a Stooq price manifest from an ETF holdings CSV — the symbol
    universe generator (analogous to ``sources.sec.build_sec_manifest``).

    Parses the holdings file for its constituent tickers and emits one manifest
    series per ticker (``<ticker>:<field>``), optionally prepending the ETF
    itself (``include_etf``) so the fund's own price is pulled too. Shipped
    ``active=False`` by default — review before enabling.
    """
    payload = {"format": "csv", "etf": include_etf or "ETF", "text": holdings_csv}
    holdings = normalize_ishares_holdings("_", payload, source="ishares")
    tickers = [r["series_id"].split(":", 1)[1] for r in holdings]
    if include_etf:
        tickers = [include_etf.upper(), *tickers]

    series = [
        {
            "series_id": f"{t}:{field}",
            "title": f"{t} daily {field}",
            "category": "equity",
            "frequency": "d",
            "units": "USD" if field != "volume" else "Shares",
            "source": source,
            "active": active,
            "vintage_enabled": False,
            "validation_profile": "standard",
            "downstream_use_case": "equity_price_return",
            "priority": 3,
            "tags": ["equity", field, source],
        }
        for t in tickers
    ]
    return {
        "name": name,
        "description": (
            f"Equity price manifest generated from an ETF holdings file "
            f"({len(series)} tickers, {field}). VERIFY tickers before activating."
        ),
        "version": 1,
        "series": series,
    }
