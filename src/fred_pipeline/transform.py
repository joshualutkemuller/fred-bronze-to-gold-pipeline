"""Pure-Python transformation of raw FRED payloads into normalized rows.

Kept separate from the Spark writers so the trickiest logic — parsing FRED's
string values, handling the ``"."`` missing-value sentinel, computing revision
numbers, and deriving point-in-time keys — is fully unit-testable without a
SparkSession.

A "silver row" here is a plain ``dict`` with a stable schema. The Spark layer
(:mod:`fred_pipeline.silver`) simply wraps these into a DataFrame and MERGEs.
"""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timezone
from typing import Any, Iterable, Optional

# FRED encodes "no value for this date" as a literal single period.
MISSING_VALUE = "."

# Sentinel FRED uses for open-ended real-time windows.
REALTIME_OPEN_END = "9999-12-31"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_value(raw: Any) -> Optional[float]:
    """Parse a FRED observation value string into a float or ``None``.

    FRED returns values as strings, using ``"."`` for missing data. Anything
    that cannot be parsed becomes ``None`` (recorded as a DQ concern later)
    rather than raising, so one bad cell never fails an entire load.
    """
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        return float(raw)
    s = str(raw).strip()
    if s == "" or s == MISSING_VALUE:
        return None
    try:
        return float(s.replace(",", ""))
    except ValueError:
        return None


def _row_hash(series_id: str, observation_date: str, realtime_start: str, value: Any) -> str:
    """Deterministic hash used to detect genuine changes across loads."""
    payload = f"{series_id}|{observation_date}|{realtime_start}|{value}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def normalize_observations(
    series_id: str,
    payload: dict[str, Any],
    *,
    ingested_at: Optional[str] = None,
    run_id: Optional[str] = None,
) -> list[dict[str, Any]]:
    """Convert a raw FRED observations payload into normalized silver rows.

    Parameters
    ----------
    series_id:
        The series the payload belongs to (FRED does not echo it per-row).
    payload:
        The exact JSON dict returned by ``series/observations``.
    ingested_at / run_id:
        Audit stamps threaded through from the pipeline run.

    Returns
    -------
    A list of dicts with a stable schema (see :data:`SILVER_COLUMNS`).
    """
    ingested_at = ingested_at or _utc_now_iso()
    observations = payload.get("observations")
    if observations is None:
        raise ValueError(
            f"Payload for {series_id!r} has no 'observations' key "
            f"(keys: {sorted(payload.keys())})"
        )

    rows: list[dict[str, Any]] = []
    for obs in observations:
        obs_date = obs.get("date")
        if not obs_date:
            continue  # skip structurally broken rows
        rt_start = obs.get("realtime_start", "")
        rt_end = obs.get("realtime_end", "")
        raw_value = obs.get("value")
        value = parse_value(raw_value)

        rows.append(
            {
                "series_id": series_id,
                "observation_date": obs_date,
                "realtime_start": rt_start,
                "realtime_end": rt_end,
                "value": value,
                "raw_value": None if raw_value is None else str(raw_value),
                "is_missing": value is None,
                "row_hash": _row_hash(series_id, obs_date, rt_start, raw_value),
                "ingested_at": ingested_at,
                "run_id": run_id,
            }
        )
    return rows


# Canonical silver column order (also documented in docs/data_dictionary.md).
SILVER_COLUMNS = (
    "series_id",
    "observation_date",
    "realtime_start",
    "realtime_end",
    "value",
    "raw_value",
    "is_missing",
    "row_hash",
    "ingested_at",
    "run_id",
)


def assign_revision_numbers(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Add a monotonically increasing ``revision_number`` per observation_date.

    For vintage-enabled series, FRED returns multiple rows per
    ``observation_date`` (one per real-time window). Ordering by
    ``realtime_start`` gives revision 1, 2, 3, ... — the point-in-time history
    of how a given data point was revised over time.
    """
    rows = list(rows)
    by_date: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for r in rows:
        by_date.setdefault((r["series_id"], r["observation_date"]), []).append(r)

    for group in by_date.values():
        group.sort(key=lambda r: (r.get("realtime_start") or ""))
        for i, r in enumerate(group, start=1):
            r["revision_number"] = i
    return rows


def latest_by_observation(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Collapse vintage rows to the single latest revision per observation_date.

    This is the transformation behind ``gold.fred_latest_observation`` /
    the ``latest_revised`` view.
    """
    rows = list(rows)
    latest: dict[tuple[str, str], dict[str, Any]] = {}
    for r in rows:
        key = (r["series_id"], r["observation_date"])
        cur = latest.get(key)
        if cur is None or (r.get("realtime_start") or "") >= (cur.get("realtime_start") or ""):
            latest[key] = r
    return sorted(latest.values(), key=lambda r: (r["series_id"], r["observation_date"]))


def payload_summary(payload: dict[str, Any]) -> dict[str, Any]:
    """Extract lightweight metadata about a payload for Bronze/audit rows."""
    obs = payload.get("observations") or []
    return {
        "observation_count": len(obs),
        "response_realtime_start": payload.get("realtime_start"),
        "response_realtime_end": payload.get("realtime_end"),
        "response_count": payload.get("count"),
        "payload_bytes": len(json.dumps(payload)),
    }
