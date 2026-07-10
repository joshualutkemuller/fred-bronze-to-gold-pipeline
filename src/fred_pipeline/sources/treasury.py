"""US Treasury source client (Fiscal Data API) — a keyless `SourceClient`.

The Fiscal Data API exposes *datasets* (e.g. `debt_to_penny`) whose rows are
keyed by `record_date`, rather than a flat series catalog. To fit the pipeline's
series model, a manifest `series_id` encodes both the dataset path and the value
column, separated by the last colon::

    v2/accounting/od/debt_to_penny:tot_pub_debt_out_amt
    └────────── dataset path ─────────┘ └──── value field ────┘

Only the source-specific bits live here (no auth; error body carries an `error`
field; records nested under `data[]`, dated by `record_date`); the shared HTTP
transport comes from :class:`fred_pipeline.sources.base.HTTPSource`.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Callable, Optional

from fred_pipeline.sources.base import HTTPSource, SourceError
from fred_pipeline.transform import _row_hash, _utc_now_iso, parse_value

log = logging.getLogger("fred_pipeline.sources.treasury")

# Every Fiscal Data dataset used here is keyed by this date column.
DATE_FIELD = "record_date"
# The API caps page size at 10000 rows per request.
PAGE_SIZE = 10000


class TreasuryAPIError(SourceError):
    """Raised when the Treasury Fiscal Data API returns an unrecoverable error."""


def _parse_series_id(series_id: str) -> tuple[str, str]:
    """Split ``dataset/path:field`` into ``(dataset_path, value_field)``."""
    dataset, sep, field = series_id.rpartition(":")
    if not sep or not dataset or not field:
        raise TreasuryAPIError(
            f"Treasury series_id must be '<dataset_path>:<field>', got {series_id!r}"
        )
    return dataset, field


def normalize_treasury_observations(
    series_id: str,
    payload: dict[str, Any],
    *,
    run_id: Optional[str] = None,
    ingested_at: Optional[str] = None,
    source: str = "treasury",
) -> list[dict[str, Any]]:
    """Convert a raw Fiscal Data payload into canonical silver rows.

    The Treasury feed carries current values only (no vintages), so
    ``realtime_start``/``realtime_end`` are blanked, as with BLS/EIA.
    """
    ingested_at = ingested_at or _utc_now_iso()
    _dataset, field = _parse_series_id(series_id)
    rows: list[dict[str, Any]] = []
    for rec in payload.get("data") or []:
        obs_date = rec.get(DATE_FIELD)
        if not obs_date:
            continue
        obs_date = str(obs_date)[:10]
        raw_value = rec.get(field)
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


class TreasuryClient(HTTPSource):
    """Retrying, rate-limited US Treasury Fiscal Data client (keyless)."""

    source_name = "TREASURY"
    error_cls = TreasuryAPIError

    def __init__(
        self,
        base_url: str = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service",
        *,
        session: Any = None,
        timeout: int = 30,
        max_retries: int = 5,
        rate_limit_per_minute: int = 120,
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

    def _error_detail(self, resp: Any) -> str:
        try:
            body = resp.json()
            if isinstance(body, dict):
                return str(body.get("error") or body.get("message") or body)
            return str(body)
        except Exception:
            return getattr(resp, "text", "<no body>")

    def observations_endpoint(self, series_id: str) -> str:
        """The dataset path hit for observations (recorded in Bronze lineage)."""
        dataset, _field = _parse_series_id(series_id)
        return dataset

    # ---- SourceClient contract ------------------------------------------

    def get_observations(
        self,
        series_id: str,
        *,
        observation_start: Optional[str] = None,
        observation_end: Optional[str] = None,
        **_ignored: Any,
    ) -> dict[str, Any]:
        """Fetch a dataset column as observations, following pagination."""
        dataset, field = _parse_series_id(series_id)
        params: dict[str, Any] = {
            "fields": f"{DATE_FIELD},{field}",
            "sort": DATE_FIELD,
            "page[size]": PAGE_SIZE,
        }
        filters = []
        if observation_start:
            filters.append(f"{DATE_FIELD}:gte:{observation_start}")
        if observation_end:
            filters.append(f"{DATE_FIELD}:lte:{observation_end}")
        if filters:
            params["filter"] = ",".join(filters)

        records: list[dict[str, Any]] = []
        page = 1
        while True:
            payload = self._request(dataset, {**params, "page[number]": page})
            batch = payload.get("data") or []
            records.extend(batch)
            total_pages = (payload.get("meta") or {}).get("total-pages")
            if total_pages is not None:
                if page >= int(total_pages):
                    break
            elif len(batch) < PAGE_SIZE:
                break
            page += 1
        return {"data": records, "meta": {"series_id": series_id, "pages": page}}

    def normalize(
        self,
        series_id: str,
        payload: dict[str, Any],
        *,
        run_id: Optional[str] = None,
        track_vintage: bool = False,  # Treasury feed carries no vintages
        source: str = "treasury",
    ) -> list[dict[str, Any]]:
        return normalize_treasury_observations(series_id, payload, run_id=run_id,
                                               source=source)
