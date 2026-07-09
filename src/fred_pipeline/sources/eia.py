"""EIA (U.S. Energy Information Administration) source client — a third
implementation of the :class:`SourceClient` contract.

Like BLS, only the source-specific bits live here; the rate limiter, retry, and
request engine come from :class:`fred_pipeline.sources.base.HTTPSource`.

EIA v2 specifics:

  * **Auth** — an ``api_key`` query param (required; EIA has no keyless tier).
  * **Route** — the ``seriesid/{id}`` compatibility route returns a single
    series' history in the v2 envelope.
  * **Errors** — a failed request returns a JSON body with an ``error`` field
    (surfaced via ``_error_detail``).
  * **Response shape** — observations are nested under ``response.data[]`` and
    dated by a ``period`` string whose granularity follows the series
    frequency (``YYYY``, ``YYYY-MM``, ``YYYY-MM-DD``, ``YYYY-Qn``).
    ``normalize`` maps that into the same canonical silver rows FRED/BLS
    produce.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Callable, Optional

from fred_pipeline.sources.base import HTTPSource, SourceError
from fred_pipeline.transform import _row_hash, _utc_now_iso, parse_value

log = logging.getLogger("fred_pipeline.sources.eia")


class EIAAPIError(SourceError):
    """Raised when the EIA API returns an unrecoverable error."""


def _eia_period_to_date(period: Any) -> Optional[str]:
    """Map an EIA ``period`` string to an ISO observation date (period start).

    Handles annual (``YYYY``), monthly (``YYYY-MM``), daily (``YYYY-MM-DD``),
    and quarterly (``YYYY-Qn`` / ``YYYYQn``). Returns ``None`` for anything
    unrecognized.
    """
    if not period:
        return None
    p = str(period).strip()

    # Quarterly: 2024-Q1 or 2024Q1
    q = p.upper().replace("-", "")
    if "Q" in q:
        year_part, _, quarter_part = q.partition("Q")
        try:
            y = int(year_part)
            n = int(quarter_part)
        except ValueError:
            return None
        if not 1 <= n <= 4:
            return None
        return f"{y:04d}-{(n - 1) * 3 + 1:02d}-01"

    digits_dashes = p.replace("-", "")
    if not digits_dashes.isdigit():
        return None

    if len(p) == 4:                 # YYYY
        return f"{p}-01-01"
    if len(p) == 7:                 # YYYY-MM
        return f"{p}-01"
    if len(p) == 6:                 # YYYYMM
        return f"{p[:4]}-{p[4:6]}-01"
    if len(p) == 10:                # YYYY-MM-DD
        return p
    if len(p) == 8:                 # YYYYMMDD
        return f"{p[:4]}-{p[4:6]}-{p[6:8]}"
    return None


def normalize_eia_observations(
    series_id: str,
    payload: dict[str, Any],
    *,
    run_id: Optional[str] = None,
    ingested_at: Optional[str] = None,
    source: str = "eia",
) -> list[dict[str, Any]]:
    """Convert a raw EIA seriesid payload into canonical silver rows.

    EIA's feed carries only current values (no real-time/vintage window), so
    ``realtime_start``/``realtime_end`` are blanked — the same convention BLS
    and FRED's non-vintage series use.
    """
    ingested_at = ingested_at or _utc_now_iso()
    data = (payload.get("response") or {}).get("data") or []
    rows: list[dict[str, Any]] = []
    for obs in data:
        obs_date = _eia_period_to_date(obs.get("period"))
        if not obs_date:
            continue
        raw_value = obs.get("value")
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


class EIAClient(HTTPSource):
    """Retrying, rate-limited EIA API client (v2, seriesid route)."""

    source_name = "EIA"
    error_cls = EIAAPIError

    def __init__(
        self,
        api_key: str,
        base_url: str = "https://api.eia.gov/v2",
        *,
        session: Any = None,
        timeout: int = 30,
        max_retries: int = 5,
        rate_limit_per_minute: int = 60,
        sleep: Callable[[float], None] = time.sleep,
    ):
        if not api_key:
            raise EIAAPIError("An EIA API key is required (got empty string)")
        self.api_key = api_key
        super().__init__(
            base_url=base_url,
            session=session,
            timeout=timeout,
            max_retries=max_retries,
            rate_limit_per_minute=rate_limit_per_minute,
            sleep=sleep,
        )

    def _default_query(self) -> dict[str, Any]:
        return {"api_key": self.api_key}

    def _error_detail(self, resp: Any) -> str:
        try:
            body = resp.json()
            if isinstance(body, dict) and body.get("error"):
                return str(body["error"])
            return str(body)
        except Exception:
            return getattr(resp, "text", "<no body>")

    # ---- SourceClient contract ------------------------------------------

    def get_observations(
        self,
        series_id: str,
        *,
        observation_start: Optional[str] = None,
        observation_end: Optional[str] = None,
        **_ignored: Any,
    ) -> dict[str, Any]:
        """Fetch a single series' observations, returning the raw JSON payload.

        ``observation_start``/``observation_end`` are forwarded as EIA
        ``start``/``end``. EIA matches these against the series' period format
        (e.g. ``YYYY-MM`` for monthly); the pipeline passes ISO dates, which EIA
        accepts by prefix. FRED-only kwargs (``realtime_start`` etc.) are
        ignored — EIA has no point-in-time window here.
        """
        params: dict[str, Any] = {}
        if observation_start:
            params["start"] = observation_start
        if observation_end:
            params["end"] = observation_end
        return self._request(f"seriesid/{series_id}", params)

    def normalize(
        self,
        series_id: str,
        payload: dict[str, Any],
        *,
        run_id: Optional[str] = None,
        track_vintage: bool = False,  # EIA feed carries no vintages
        source: str = "eia",
    ) -> list[dict[str, Any]]:
        return normalize_eia_observations(series_id, payload, run_id=run_id,
                                          source=source)
