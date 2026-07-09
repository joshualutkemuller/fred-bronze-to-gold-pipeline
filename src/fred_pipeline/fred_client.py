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

import logging
import random
import threading
import time
from datetime import date, timedelta
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

try:  # requests is a light, ubiquitous dependency; import guarded for clarity.
    import requests
except ImportError:  # pragma: no cover
    requests = None  # type: ignore

log = logging.getLogger("fred_pipeline.fred_client")

# Progressive real-time lookbacks (years) tried when a full-vintage request
# exceeds FRED's cap of 2000 vintage dates per request.
VINTAGE_FALLBACK_YEARS = (10, 3, 1)


class FredAPIError(RuntimeError):
    """Raised when the FRED API returns an unrecoverable error."""

    def __init__(self, message: str, status_code: Optional[int] = None):
        super().__init__(message)
        self.status_code = status_code


# HTTP statuses worth retrying (transient/server-side/throttling).
RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})


def _is_vintage_cap_error(exc: "FredAPIError") -> bool:
    """True for FRED's 400 'exceeded maximum number of vintage dates' error."""
    return exc.status_code == 400 and "vintage date" in str(exc).lower()


def _coalesce_observations(observations: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Collapse consecutive equal-value vintages per observation date.

    Batched real-time windows clip each row's realtime window to the batch
    bounds, splitting an unchanged value into multiple rows at batch seams. A
    genuine revision changes the value; a clipping artifact repeats it — so
    merging adjacent equal-value rows (extending realtime_end) reconstructs the
    true revision history. Rows are keyed/ordered by (date, realtime_start).
    """
    by_date: dict[str, list[dict[str, Any]]] = {}
    for obs in observations:
        by_date.setdefault(obs.get("date"), []).append(obs)

    out: list[dict[str, Any]] = []
    for _date, rows in by_date.items():
        rows.sort(key=lambda o: o.get("realtime_start", ""))
        run: Optional[dict[str, Any]] = None
        for obs in rows:
            if run is not None and obs.get("value") == run.get("value"):
                # same value continuing -> extend the vintage window
                run["realtime_end"] = obs.get("realtime_end", run.get("realtime_end"))
            else:
                if run is not None:
                    out.append(run)
                run = dict(obs)
        if run is not None:
            out.append(run)
    return sorted(out, key=lambda o: (o.get("date", ""), o.get("realtime_start", "")))


@dataclass
class RateLimiter:
    """Token-free rate limiter based on a minimum inter-call interval.

    Safe to share across threads: the whole wait-then-stamp sequence is
    serialized under a lock, so concurrent extraction workers still respect a
    single aggregate ``per_minute`` ceiling against the FRED API.
    """

    per_minute: int
    _last_call: float = 0.0
    _sleep: Callable[[float], None] = time.sleep
    _now: Callable[[], float] = time.monotonic
    _lock: "threading.Lock" = field(default_factory=threading.Lock)

    def acquire(self) -> None:
        if self.per_minute <= 0:
            return
        min_interval = 60.0 / self.per_minute
        with self._lock:
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
        try:
            return self._request("series/observations", params)
        except FredAPIError as exc:
            if realtime_start and _is_vintage_cap_error(exc):
                return self._observations_bounded_vintage(series_id, params)
            raise

    def _observations_bounded_vintage(
        self, series_id: str, params: dict[str, Any]
    ) -> dict[str, Any]:
        """Recover from the 'maximum vintage dates' cap by narrowing the window.

        FRED caps a real-time window at 2000 vintage dates. For a series with
        more revisions than that, progressively shorten ``realtime_start`` to
        recent years (capturing recent revisions), and finally fall back to the
        latest-only view. Full history for such series would require per-vintage
        batching; recent vintages are what feature pipelines actually use.
        """
        for years in VINTAGE_FALLBACK_YEARS:
            start = (date.today() - timedelta(days=365 * years)).isoformat()
            attempt = {**params, "realtime_start": start}
            log.warning(
                "Series %s exceeded FRED vintage-date cap; retrying with "
                "realtime_start=%s (last %d years of vintages)",
                series_id, start, years,
            )
            try:
                return self._request("series/observations", attempt)
            except FredAPIError as exc:
                if _is_vintage_cap_error(exc):
                    continue
                raise
        # Last resort: latest revision only (no vintage history).
        latest = {k: v for k, v in params.items()
                  if k not in ("realtime_start", "realtime_end")}
        log.warning(
            "Series %s still over the cap; falling back to latest-only "
            "(no vintage history for this run)", series_id,
        )
        return self._request("series/observations", latest)

    # ---- complete vintage history (batched) -----------------------------

    def get_vintage_dates(self, series_id: str, *, page_size: int = 10000) -> list[str]:
        """Return every vintage (release) date for a series, ascending."""
        out: list[str] = []
        offset = 0
        limit = min(page_size, 10000)
        while True:
            payload = self._request(
                "series/vintagedates",
                {"series_id": series_id, "limit": limit, "offset": offset},
            )
            batch = payload.get("vintage_dates") or []
            out.extend(batch)
            offset += len(batch)
            if not batch or len(batch) < limit:
                break
        return out

    def get_observations_all_vintages(
        self,
        series_id: str,
        *,
        batch_size: int = 2000,
        observation_start: Optional[str] = None,
        observation_end: Optional[str] = None,
        units: Optional[str] = None,
    ) -> dict[str, Any]:
        """Fetch the *complete* vintage history, batched under FRED's cap.

        FRED allows at most 2000 vintage dates per observations request. This
        enumerates the series' vintage dates and pulls observations in
        contiguous real-time windows of ``batch_size`` vintages each, then
        coalesces adjacent equal-value vintages — which removes the artificial
        "revisions" that window-clipping introduces at batch seams — so the
        merged result is equivalent to one uncapped request.
        """
        base: dict[str, Any] = {"series_id": series_id}
        if observation_start:
            base["observation_start"] = observation_start
        if observation_end:
            base["observation_end"] = observation_end
        if units:
            base["units"] = units

        vintages = self.get_vintage_dates(series_id)
        if not vintages:  # brand-new series with no releases yet
            return self._request(
                "series/observations",
                {**base, "realtime_start": "1776-07-04", "realtime_end": "9999-12-31"},
            )
        if len(vintages) <= batch_size:
            return self._request(
                "series/observations",
                {**base, "realtime_start": vintages[0], "realtime_end": vintages[-1]},
            )

        merged: list[dict[str, Any]] = []
        for i in range(0, len(vintages), batch_size):
            chunk = vintages[i:i + batch_size]
            payload = self._request(
                "series/observations",
                {**base, "realtime_start": chunk[0], "realtime_end": chunk[-1]},
            )
            merged.extend(payload.get("observations") or [])
        log.info(
            "Series %s: assembled %d vintages in %d batch(es)",
            series_id, len(vintages), (len(vintages) + batch_size - 1) // batch_size,
        )
        return {"observations": _coalesce_observations(merged)}

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
