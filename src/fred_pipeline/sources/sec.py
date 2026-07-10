"""SEC EDGAR source client (XBRL company financials).

Company fundamentals from SEC filings, via the free ``data.sec.gov`` XBRL
``companyconcept`` API. A manifest `series_id` names one concept for one
company::

    CIK0000320193:us-gaap/Assets:USD
    │             │              └ unit
    │             └──────────────── taxonomy/tag
    └────────────────────────────── zero-padded CIK (CIK##########)

SEC-specific bits: a **required descriptive User-Agent** header (SEC returns 403
without one), and a response where each concept carries many filings per period
end. Each filing's ``filed`` date becomes ``realtime_start`` — so restatements
and amendments are captured as genuine point-in-time vintages (set
``vintage_enabled: true``).

> Scope note: this client fetches a raw concept series. Turning heterogeneous
> XBRL tags into *standardized* statements, and disambiguating duration facts
> (quarterly vs. YTD) for income-statement concepts, is the follow-on
> standardization layer. **Instant (balance-sheet) concepts** — Assets,
> StockholdersEquity, CashAndCashEquivalents… — have no duration ambiguity and
> are the clean fit for a manifest today.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Callable, Optional

from fred_pipeline.sources.base import HTTPSource, SourceError
from fred_pipeline.transform import _row_hash, _utc_now_iso, parse_value

log = logging.getLogger("fred_pipeline.sources.sec")

DEFAULT_USER_AGENT = "fred-bronze-to-gold-pipeline (set SEC_USER_AGENT to your contact email)"


class SECAPIError(SourceError):
    """Raised when the SEC API returns an unrecoverable error."""


def sec_cik(cik: Any) -> str:
    """Normalize any CIK form to the zero-padded ``CIK##########`` used by EDGAR."""
    digits = "".join(ch for ch in str(cik) if ch.isdigit())
    if not digits:
        raise SECAPIError(f"Invalid CIK: {cik!r}")
    return f"CIK{int(digits):010d}"


def build_sec_series_id(cik: Any, taxonomy: str, tag: str, unit: str = "USD") -> str:
    """Assemble a SEC ``series_id`` from its parts."""
    return f"{sec_cik(cik)}:{taxonomy}/{tag}:{unit}"


def build_sec_manifest(
    companies: Any,
    concepts: Any,
    *,
    name: str = "sec_financials",
    frequency: str = "q",
    active: bool = False,
) -> dict[str, Any]:
    """Generate a manifest dict for the (company x concept) grid.

    This is the seed of the SEC manifest generator: at ~1,000 companies it is
    impractical to hand-author series, so they are produced programmatically
    from a company list and a concept list (analogous to FRED ``discover``).

    ``companies``: iterable of ``(cik, label)``.
    ``concepts``: iterable of ``(taxonomy, tag, unit, title)``.
    """
    series = []
    for cik, label in companies:
        for taxonomy, tag, unit, title in concepts:
            series.append(
                {
                    "series_id": build_sec_series_id(cik, taxonomy, tag, unit),
                    "title": f"{label} — {title}",
                    "category": "company_financials",
                    "frequency": frequency,
                    "source": "sec",
                    "vintage_enabled": True,
                    "active": active,
                    "tags": ["sec", "fundamentals"],
                }
            )
    return {
        "name": name,
        "description": "Generated SEC company-financials manifest.",
        "version": 1,
        "series": series,
    }


def _parse_series_id(series_id: str) -> tuple[str, str, str, str]:
    """Split ``CIK##########:taxonomy/tag:unit`` into its parts."""
    cik, sep1, rest = series_id.partition(":")
    concept, sep2, unit = rest.partition(":")
    taxonomy, sep3, tag = concept.partition("/")
    if not (sep1 and sep2 and sep3) or not all([cik, taxonomy, tag, unit]):
        raise SECAPIError(
            f"SEC series_id must be '<CIK>:<taxonomy>/<tag>:<unit>', "
            f"got {series_id!r}"
        )
    return cik.strip(), taxonomy.strip(), tag.strip(), unit.strip()


def normalize_sec_observations(
    series_id: str,
    payload: dict[str, Any],
    *,
    run_id: Optional[str] = None,
    ingested_at: Optional[str] = None,
    track_vintage: bool = True,
    source: str = "sec",
) -> list[dict[str, Any]]:
    """Convert a raw SEC companyconcept payload into canonical silver rows.

    One row per (period end, filing). With ``track_vintage`` on (the default),
    ``realtime_start`` is the filing's ``filed`` date, so amendments and
    restatements land as distinct vintages — the same point-in-time model FRED
    uses. With it off, realtime is blanked and only the latest filing per period
    survives the MERGE.
    """
    ingested_at = ingested_at or _utc_now_iso()
    _cik, _tax, _tag, unit = _parse_series_id(series_id)
    entries = (payload.get("units") or {}).get(unit) or []
    rows: list[dict[str, Any]] = []
    for e in entries:
        obs_date = e.get("end")
        if not obs_date:
            continue
        if track_vintage:
            rt_start = e.get("filed", "") or ""
            rt_end = ""
        else:
            rt_start = ""
            rt_end = ""
        raw_value = e.get("val")
        value = parse_value(raw_value)
        rows.append(
            {
                "source": source,
                "series_id": series_id,
                "observation_date": str(obs_date)[:10],
                "realtime_start": rt_start,
                "realtime_end": rt_end,
                "value": value,
                "raw_value": None if raw_value is None else str(raw_value),
                "is_missing": value is None,
                "row_hash": _row_hash(series_id, str(obs_date)[:10], rt_start, raw_value),
                "ingested_at": ingested_at,
                "run_id": run_id,
            }
        )
    return rows


class SECClient(HTTPSource):
    """Retrying, rate-limited SEC EDGAR XBRL client (keyless; UA required)."""

    source_name = "SEC"
    error_cls = SECAPIError

    def __init__(
        self,
        user_agent: Optional[str] = None,
        base_url: str = "https://data.sec.gov",
        *,
        session: Any = None,
        timeout: int = 30,
        max_retries: int = 5,
        # SEC asks for <= 10 requests/second.
        rate_limit_per_minute: int = 300,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self.user_agent = user_agent or DEFAULT_USER_AGENT
        super().__init__(
            base_url=base_url,
            session=session,
            timeout=timeout,
            max_retries=max_retries,
            rate_limit_per_minute=rate_limit_per_minute,
            sleep=sleep,
        )

    def _request_headers(self) -> dict[str, str]:
        # SEC rejects requests without a descriptive User-Agent (403).
        return {"User-Agent": self.user_agent, "Accept-Encoding": "gzip, deflate"}

    def observations_endpoint(self, series_id: str) -> str:
        cik, taxonomy, tag, _unit = _parse_series_id(series_id)
        return f"api/xbrl/companyconcept/{cik}/{taxonomy}/{tag}.json"

    # ---- SourceClient contract ------------------------------------------

    def get_observations(
        self,
        series_id: str,
        *,
        observation_start: Optional[str] = None,
        observation_end: Optional[str] = None,
        **_ignored: Any,
    ) -> dict[str, Any]:
        """Fetch one concept's full filing history for a company.

        The companyconcept endpoint has no server-side date filter, so
        ``observation_start`` is ignored; the Silver MERGE dedupes on re-runs.
        """
        return self._request(self.observations_endpoint(series_id), {})

    def normalize(
        self,
        series_id: str,
        payload: dict[str, Any],
        *,
        run_id: Optional[str] = None,
        track_vintage: bool = True,
        source: str = "sec",
    ) -> list[dict[str, Any]]:
        return normalize_sec_observations(
            series_id, payload, run_id=run_id, track_vintage=track_vintage,
            source=source,
        )
