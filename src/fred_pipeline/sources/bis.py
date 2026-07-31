"""BIS policy-rate source client (WS_CBPOL) — a keyless `SourceClient`.

The BIS Central Bank Policy Rates dataflow is a single-purpose dataset keyed by
monthly frequency and reference area. Manifest ``series_id`` values encode the
BIS reference area with a stable source prefix::

    BIS:GB      BIS:JP      BIS:XM
    └src┘└ref_area┘

BIS serves SDMX CSV, so Bronze keeps the raw CSV body in a small JSON envelope
and Silver normalization reads ``TIME_PERIOD`` / ``OBS_VALUE`` into the
canonical row schema. The feed carries current values only (no vintages).
"""

from __future__ import annotations

import csv
import io
import logging
import re
import time
from collections.abc import Callable
from typing import Any

from fred_pipeline.sources.base import HTTPSource, SourceError
from fred_pipeline.transform import _row_hash, _utc_now_iso, parse_value

log = logging.getLogger("fred_pipeline.sources.bis")

BIS_DATAFLOW = "WS_CBPOL"
BIS_FREQUENCY = "M"
BIS_ACCEPT = "application/vnd.sdmx.data+csv;version=1.0.0"


class BISAPIError(SourceError):
    """Raised when the BIS API returns an unrecoverable error."""


def _parse_series_id(series_id: str) -> str:
    """Extract the BIS reference area from ``BIS:<ref_area>``."""
    prefix, sep, ref_area = series_id.partition(":")
    if prefix.upper() != "BIS" or not sep or not ref_area:
        raise BISAPIError(f"BIS series_id must be 'BIS:<ref_area>', got {series_id!r}")
    return ref_area.upper()


def _date_to_bis_period(value: str | None) -> str | None:
    """Map an ISO-ish date to the monthly period string BIS expects."""
    if not value:
        return None
    text = str(value).strip()
    if re.fullmatch(r"\d{4}-\d{2}(-\d{2})?", text):
        return text[:7]
    if re.fullmatch(r"\d{4}", text):
        return text
    return text


def _bis_period_to_date(period: Any) -> str | None:
    """Map a BIS ``TIME_PERIOD`` to an ISO observation date (period start)."""
    if not period:
        return None
    text = str(period).strip().upper()
    if re.fullmatch(r"\d{4}-\d{2}", text):
        return f"{text}-01"
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        return text
    match = re.fullmatch(r"(\d{4})-?M(\d{1,2})", text)
    if match:
        year, month = match.groups()
        month_n = int(month)
        if 1 <= month_n <= 12:
            return f"{int(year):04d}-{month_n:02d}-01"
        return None
    if re.fullmatch(r"\d{6}", text):
        return f"{text[:4]}-{text[4:6]}-01"
    if re.fullmatch(r"\d{4}", text):
        return f"{text}-01-01"
    return None


def _csv_records(csv_text: str) -> list[dict[str, str]]:
    """Parse SDMX CSV rows, tolerating blank/comment preamble lines."""
    lines = [
        line
        for line in (csv_text or "").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]
    if not lines:
        return []
    return list(csv.DictReader(io.StringIO("\n".join(lines))))


def _field(rec: dict[str, Any], *names: str) -> Any:
    """Case-insensitive field lookup for SDMX CSV column-name variants."""
    lookup = {str(k).upper(): k for k in rec}
    for name in names:
        key = lookup.get(name.upper())
        if key is not None:
            return rec.get(key)
    return None


def normalize_bis_observations(
    series_id: str,
    payload: dict[str, Any] | str,
    *,
    run_id: str | None = None,
    ingested_at: str | None = None,
    source: str = "bis",
) -> list[dict[str, Any]]:
    """Convert raw BIS SDMX CSV into canonical silver rows."""
    ingested_at = ingested_at or _utc_now_iso()
    csv_text = payload.get("data", "") if isinstance(payload, dict) else str(payload)

    rows: list[dict[str, Any]] = []
    for rec in _csv_records(csv_text):
        obs_date = _bis_period_to_date(_field(rec, "TIME_PERIOD", "PERIOD"))
        if not obs_date:
            continue
        raw_value = _field(rec, "OBS_VALUE", "VALUE")
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


class BISClient(HTTPSource):
    """Retrying, rate-limited BIS Central Bank Policy Rates client."""

    source_name = "BIS"
    error_cls = BISAPIError

    def __init__(
        self,
        base_url: str = "https://stats.bis.org/api/v2",
        *,
        session: Any = None,
        timeout: int = 30,
        max_retries: int = 5,
        rate_limit_per_minute: int = 30,
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

    def _request_headers(self) -> dict[str, str]:
        return {"Accept": BIS_ACCEPT}

    def _error_detail(self, resp: Any) -> str:
        return getattr(resp, "text", "<no body>")

    def observations_endpoint(self, series_id: str) -> str:
        """The endpoint hit for observations (recorded in Bronze lineage)."""
        ref_area = _parse_series_id(series_id)
        return f"data/dataflow/BIS/{BIS_DATAFLOW}/1.0/{BIS_FREQUENCY}.{ref_area}"

    # ---- SourceClient contract ------------------------------------------

    def get_observations(
        self,
        series_id: str,
        *,
        observation_start: str | None = None,
        observation_end: str | None = None,
        **_ignored: Any,
    ) -> dict[str, Any]:
        """Fetch one reference area's policy-rate history as raw SDMX CSV."""
        ref_area = _parse_series_id(series_id)
        params: dict[str, Any] = {}
        start_period = _date_to_bis_period(observation_start)
        end_period = _date_to_bis_period(observation_end)
        if start_period:
            params["startPeriod"] = start_period
        if end_period:
            params["endPeriod"] = end_period

        csv_text = self._request(
            self.observations_endpoint(series_id), params, as_text=True
        )
        return {
            "data": csv_text,
            "meta": {
                "series_id": series_id,
                "dataflow": BIS_DATAFLOW,
                "frequency": BIS_FREQUENCY,
                "ref_area": ref_area,
                "format": "sdmx-csv",
            },
        }

    def normalize(
        self,
        series_id: str,
        payload: dict[str, Any],
        *,
        run_id: str | None = None,
        track_vintage: bool = False,  # BIS WS_CBPOL carries no vintages
        source: str = "bis",
    ) -> list[dict[str, Any]]:
        return normalize_bis_observations(
            series_id, payload, run_id=run_id, source=source
        )
