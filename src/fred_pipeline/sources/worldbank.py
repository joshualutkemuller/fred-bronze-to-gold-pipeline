"""World Bank source client (Indicators API) — a keyless `SourceClient`.

A World Bank series is a (country, indicator) pair; a manifest `series_id`
encodes both, separated by the first colon::

    USA:NY.GDP.MKTP.CD      WLD:SP.POP.TOTL
    └cty┘ └── indicator ──┘

World Bank breaks two FRED assumptions, handled here:
  * the response is a **top-level JSON array** ``[meta, data]`` (not an object);
  * a bad request returns **HTTP 200** with a single-element ``[{"message": …}]``
    body, surfaced as an error.

Everything else — rate limiting, retry, request engine — is inherited from
:class:`fred_pipeline.sources.base.HTTPSource`.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Callable, Optional

from fred_pipeline.sources.base import HTTPSource, SourceError
from fred_pipeline.transform import _row_hash, _utc_now_iso, parse_value

log = logging.getLogger("fred_pipeline.sources.worldbank")

PAGE_SIZE = 1000


class WorldBankAPIError(SourceError):
    """Raised when the World Bank API returns an unrecoverable error."""


def _parse_series_id(series_id: str) -> tuple[str, str]:
    """Split ``country:indicator`` into ``(country, indicator)``."""
    country, sep, indicator = series_id.partition(":")
    if not sep or not country or not indicator:
        raise WorldBankAPIError(
            f"World Bank series_id must be '<country>:<indicator>', got {series_id!r}"
        )
    return country, indicator


def _wb_date(period: Any) -> Optional[str]:
    """Map a World Bank ``date`` (usually ``YYYY``) to an ISO period start."""
    if not period:
        return None
    p = str(period).strip().upper()
    digits = p.replace("-", "").replace("Q", "").replace("M", "")
    if not digits.isdigit():
        return None
    if "Q" in p:
        year, _, q = p.partition("Q")
        try:
            n = int(q)
        except ValueError:
            return None
        if not 1 <= n <= 4:
            return None
        return f"{int(year):04d}-{(n - 1) * 3 + 1:02d}-01"
    if "M" in p:
        year, _, m = p.partition("M")
        try:
            n = int(m)
        except ValueError:
            return None
        if not 1 <= n <= 12:
            return None
        return f"{int(year):04d}-{n:02d}-01"
    if len(p) == 4:
        return f"{p}-01-01"
    return None


def normalize_worldbank_observations(
    series_id: str,
    payload: Any,
    *,
    run_id: Optional[str] = None,
    ingested_at: Optional[str] = None,
    source: str = "worldbank",
) -> list[dict[str, Any]]:
    """Convert a raw World Bank ``[meta, data]`` payload into silver rows.

    Annual indicators, so no vintages — ``realtime_start``/``realtime_end`` are
    blanked, as with BLS/EIA/Treasury.
    """
    ingested_at = ingested_at or _utc_now_iso()
    data = payload[1] if isinstance(payload, list) and len(payload) >= 2 else []
    rows: list[dict[str, Any]] = []
    for rec in data or []:
        obs_date = _wb_date(rec.get("date"))
        if not obs_date:
            continue
        raw_value = rec.get("value")
        value = parse_value(raw_value)
        rows.append(
            {
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
            }
        )
    return rows


class WorldBankClient(HTTPSource):
    """Retrying, rate-limited World Bank Indicators client (keyless)."""

    source_name = "WORLDBANK"
    error_cls = WorldBankAPIError

    def __init__(
        self,
        base_url: str = "https://api.worldbank.org/v2",
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

    def _default_query(self) -> dict[str, Any]:
        return {"format": "json"}

    def observations_endpoint(self, series_id: str) -> str:
        """The endpoint hit for observations (recorded in Bronze lineage)."""
        country, indicator = _parse_series_id(series_id)
        return f"country/{country}/indicator/{indicator}"

    # ---- SourceClient contract ------------------------------------------

    def get_observations(
        self,
        series_id: str,
        *,
        observation_start: Optional[str] = None,
        observation_end: Optional[str] = None,
        **_ignored: Any,
    ) -> Any:
        """Fetch an indicator's history for a country, following pagination."""
        endpoint = self.observations_endpoint(series_id)
        base_params: dict[str, Any] = {"per_page": PAGE_SIZE}
        if observation_start:
            start_year = str(observation_start)[:4]
            end_year = str(observation_end)[:4] if observation_end else "2100"
            base_params["date"] = f"{start_year}:{end_year}"

        records: list[dict[str, Any]] = []
        page = 1
        while True:
            payload = self._request(endpoint, {**base_params, "page": page})
            meta, data = self._split(series_id, payload)
            records.extend(data)
            pages = meta.get("pages")
            if not pages or page >= int(pages):
                break
            page += 1
        return [{"page": 1, "pages": 1, "per_page": len(records),
                 "total": len(records)}, records]

    def _split(self, series_id: str, payload: Any) -> tuple[dict[str, Any], list]:
        """Validate the ``[meta, data]`` envelope, raising on an error body."""
        if isinstance(payload, list) and payload and isinstance(payload[0], dict) \
                and payload[0].get("message"):
            msgs = payload[0]["message"]
            detail = "; ".join(
                m.get("value", str(m)) for m in msgs
            ) if isinstance(msgs, list) else str(msgs)
            raise WorldBankAPIError(
                f"World Bank request failed for {series_id!r}: {detail}"
            )
        if not isinstance(payload, list) or len(payload) < 2:
            return {}, []
        meta = payload[0] if isinstance(payload[0], dict) else {}
        data = payload[1] if isinstance(payload[1], list) else []
        return meta, data

    def normalize(
        self,
        series_id: str,
        payload: Any,
        *,
        run_id: Optional[str] = None,
        track_vintage: bool = False,  # World Bank indicators carry no vintages
        source: str = "worldbank",
    ) -> list[dict[str, Any]]:
        return normalize_worldbank_observations(series_id, payload, run_id=run_id,
                                                source=source)
