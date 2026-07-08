"""Thin, resilient client for the FRED REST API.

Responsibilities:
  * build correctly-parameterized requests,
  * respect a client-side rate limit (FRED allows 120 req/min by default),
  * retry transient failures with exponential backoff + jitter,
  * return *raw* JSON payloads unchanged so Bronze can archive them verbatim.

The client keeps zero Spark/Delta knowledge — it only knows how to talk HTTP.
The ``session`` is injectable so tests can supply a fake without patching
globals.
"""

from __future__ import annotations

import random
import time
from dataclasses import dataclass
from typing import Any, Callable, Optional

try:  # requests is a light, ubiquitous dependency; import guarded for clarity.
    import requests
except ImportError:  # pragma: no cover
    requests = None  # type: ignore


class FredAPIError(RuntimeError):
    """Raised when the FRED API returns an unrecoverable error."""

    def __init__(self, message: str, status_code: Optional[int] = None):
        super().__init__(message)
        self.status_code = status_code


# HTTP statuses worth retrying (transient/server-side/throttling).
RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})


@dataclass
class RateLimiter:
    """Simple token-free rate limiter based on a minimum inter-call interval."""

    per_minute: int
    _last_call: float = 0.0
    _sleep: Callable[[float], None] = time.sleep
    _now: Callable[[], float] = time.monotonic

    def acquire(self) -> None:
        if self.per_minute <= 0:
            return
        min_interval = 60.0 / self.per_minute
        elapsed = self._now() - self._last_call
        if elapsed < min_interval:
            self._sleep(min_interval - elapsed)
        self._last_call = self._now()


class FredClient:
    """Retrying, rate-limited FRED API client."""

    def __init__(
        self,
        api_key: str,
        base_url: str = "https://api.stlouisfed.org/fred",
        *,
        session: Any = None,
        timeout: int = 30,
        max_retries: int = 5,
        rate_limit_per_minute: int = 120,
        sleep: Callable[[float], None] = time.sleep,
    ):
        if not api_key:
            raise FredAPIError("A FRED API key is required (got empty string)")
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self._sleep = sleep
        self._rate_limiter = RateLimiter(rate_limit_per_minute, _sleep=sleep)
        if session is not None:
            self._session = session
        elif requests is not None:
            self._session = requests.Session()
        else:  # pragma: no cover
            raise FredAPIError("`requests` is not installed and no session provided")

    # ---- public API -----------------------------------------------------

    def get_series_metadata(self, series_id: str) -> dict[str, Any]:
        """Return the ``seriess`` metadata block for a series (title, units...)."""
        payload = self._request("series", {"series_id": series_id})
        seriess = payload.get("seriess") or []
        if not seriess:
            raise FredAPIError(f"No metadata returned for series {series_id!r}")
        return seriess[0]

    def get_observations(
        self,
        series_id: str,
        *,
        observation_start: Optional[str] = None,
        observation_end: Optional[str] = None,
        realtime_start: Optional[str] = None,
        realtime_end: Optional[str] = None,
        units: Optional[str] = None,
    ) -> dict[str, Any]:
        """Fetch observations for a series, returning the raw JSON payload.

        Passing ``realtime_start``/``realtime_end`` (e.g. both set to a past
        date, or ``1776-07-04``/``9999-12-31`` for full vintage history) is how
        point-in-time / ALFRED data is retrieved.
        """
        params: dict[str, Any] = {"series_id": series_id}
        if observation_start:
            params["observation_start"] = observation_start
        if observation_end:
            params["observation_end"] = observation_end
        if realtime_start:
            params["realtime_start"] = realtime_start
        if realtime_end:
            params["realtime_end"] = realtime_end
        if units:
            params["units"] = units
        return self._request("series/observations", params)

    # ---- discovery (for generating manifests) ---------------------------

    def list_series(
        self,
        endpoint: str,
        params: dict[str, Any],
        *,
        max_results: Optional[int] = 1000,
        order_by: str = "popularity",
        sort_order: str = "desc",
        page_size: int = 1000,
    ) -> list[dict[str, Any]]:
        """Page through an endpoint that returns a ``seriess`` list.

        Used by category / release / search discovery below. FRED caps each page
        at 1000 rows; this transparently follows ``offset`` until exhausted or
        ``max_results`` is reached.
        """
        collected: list[dict[str, Any]] = []
        offset = 0
        while True:
            page = dict(params)
            page.update(
                {
                    "limit": min(page_size, 1000),
                    "offset": offset,
                    "order_by": order_by,
                    "sort_order": sort_order,
                }
            )
            payload = self._request(endpoint, page)
            batch = payload.get("seriess") or []
            collected.extend(batch)
            offset += len(batch)
            if not batch or len(batch) < page["limit"]:
                break
            if max_results and len(collected) >= max_results:
                break
        return collected[:max_results] if max_results else collected

    def get_category_series(self, category_id: int, **kwargs: Any) -> list[dict[str, Any]]:
        """All series belonging to a FRED category id."""
        return self.list_series("category/series", {"category_id": category_id}, **kwargs)

    def get_release_series(self, release_id: int, **kwargs: Any) -> list[dict[str, Any]]:
        """All series belonging to a FRED release id."""
        return self.list_series("release/series", {"release_id": release_id}, **kwargs)

    def search_series(self, search_text: str, **kwargs: Any) -> list[dict[str, Any]]:
        """Full-text search over the FRED catalog."""
        return self.list_series("series/search", {"search_text": search_text}, **kwargs)

    # ---- internals ------------------------------------------------------

    def _request(self, endpoint: str, params: dict[str, Any]) -> dict[str, Any]:
        url = f"{self.base_url}/{endpoint}"
        query = {**params, "api_key": self.api_key, "file_type": "json"}

        last_exc: Optional[Exception] = None
        for attempt in range(self.max_retries + 1):
            self._rate_limiter.acquire()
            try:
                resp = self._session.get(url, params=query, timeout=self.timeout)
            except Exception as exc:  # network error, DNS, timeout, etc.
                last_exc = exc
                self._backoff(attempt)
                continue

            status = getattr(resp, "status_code", 200)
            if status == 200:
                return resp.json()
            if status in RETRYABLE_STATUS and attempt < self.max_retries:
                last_exc = FredAPIError(f"HTTP {status} from {endpoint}", status)
                self._backoff(attempt)
                continue
            # Non-retryable (e.g. 400 bad series id, 403 bad key) or retries done.
            detail = self._error_detail(resp)
            raise FredAPIError(
                f"FRED API error on {endpoint} (HTTP {status}): {detail}", status
            )

        raise FredAPIError(
            f"FRED API request to {endpoint} failed after "
            f"{self.max_retries + 1} attempts: {last_exc}"
        )

    def _backoff(self, attempt: int) -> None:
        # Exponential backoff with full jitter, capped at 30s.
        delay = min(2 ** attempt, 30) + random.uniform(0, 0.5)
        self._sleep(delay)

    @staticmethod
    def _error_detail(resp: Any) -> str:
        try:
            body = resp.json()
            return str(body.get("error_message", body))
        except Exception:
            return getattr(resp, "text", "<no body>")
