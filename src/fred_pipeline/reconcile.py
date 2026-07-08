"""Metadata governance: reconcile manifests against live FRED metadata.

Manifests declare *intent* (what we think a series is); FRED is the source of
*truth* (what it currently is). Over time these drift — a series changes
frequency, gets discontinued, stops updating, or is renamed. This module fetches
FRED's ``/series`` metadata for each manifest series and:

  * emits **drift** findings (frequency mismatch, units change, discontinued,
    not-found), and
  * records a **lifecycle snapshot** (observation range, last_updated,
    popularity, staleness) so the series' health can be tracked over time.

The comparison functions are pure (spec + metadata dict in, findings out) and
fully unit-testable; the orchestrator takes an injected client + warehouse.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any, Iterable, Optional

from fred_pipeline.config import PipelineConfig
from fred_pipeline.fred_client import FredAPIError
from fred_pipeline.manifest import FREQUENCY_MAX_AGE_DAYS, SeriesSpec, all_series

DISCONTINUED_MARKER = "DISCONTINUED"

# Alias kept for readability at call sites.
STALE_MAX_AGE_DAYS = FREQUENCY_MAX_AGE_DAYS


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_date(value: Any) -> Optional[date]:
    if not value:
        return None
    try:
        return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


@dataclass
class Drift:
    series_id: str
    field: str
    manifest_value: str
    fred_value: str
    kind: str
    severity: str  # info | warning | error

    def to_row(self, detected_at: str) -> dict[str, Any]:
        return {
            "series_id": self.series_id,
            "field": self.field,
            "manifest_value": self.manifest_value,
            "fred_value": self.fred_value,
            "kind": self.kind,
            "severity": self.severity,
            "detected_at": detected_at,
        }


def compare_metadata(spec: SeriesSpec, meta: dict[str, Any]) -> list[Drift]:
    """Diff a manifest spec against a FRED ``/series`` metadata dict.

    Focused on machine-meaningful drift, not cosmetic wording:
      * frequency mismatch  -> error   (breaks processing assumptions)
      * discontinued        -> warning (lifecycle)
      * units changed       -> info    (interpretation)
    Titles are intentionally paraphrased in manifests, so they are captured in
    the lifecycle snapshot rather than flagged as drift.
    """
    drifts: list[Drift] = []
    sid = spec.series_id

    fred_freq = (meta.get("frequency_short") or "").strip().lower()
    if fred_freq and fred_freq != spec.frequency:
        drifts.append(
            Drift(sid, "frequency", spec.frequency, fred_freq,
                  "frequency_mismatch", "error")
        )

    fred_title = meta.get("title") or ""
    if DISCONTINUED_MARKER in fred_title.upper():
        drifts.append(
            Drift(sid, "title", spec.title, fred_title, "discontinued", "warning")
        )

    fred_units = (meta.get("units") or "").strip()
    if spec.units and fred_units and fred_units.lower() != spec.units.strip().lower():
        drifts.append(
            Drift(sid, "units", spec.units, fred_units, "units_changed", "info")
        )

    return drifts


def lifecycle_snapshot(
    spec: SeriesSpec, meta: dict[str, Any], today: Optional[date] = None
) -> dict[str, Any]:
    """Capture FRED-reported lifecycle facts + a staleness verdict for a series."""
    today = today or date.today()
    obs_end = _parse_date(meta.get("observation_end"))
    days_since = (today - obs_end).days if obs_end else None
    threshold = STALE_MAX_AGE_DAYS.get(spec.frequency)
    is_stale = bool(
        threshold is not None and days_since is not None and days_since > threshold
    )
    title = meta.get("title") or ""
    popularity = meta.get("popularity")
    try:
        popularity = int(popularity) if popularity is not None else None
    except (TypeError, ValueError):
        popularity = None

    return {
        "series_id": spec.series_id,
        "fred_title": title,
        "fred_frequency": (meta.get("frequency_short") or "").strip().lower(),
        "fred_units": (meta.get("units") or "").strip(),
        "seasonal_adjustment": meta.get("seasonal_adjustment_short") or "",
        "observation_start": meta.get("observation_start"),
        "observation_end": meta.get("observation_end"),
        "last_updated": meta.get("last_updated"),
        "popularity": popularity,
        "discontinued": DISCONTINUED_MARKER in title.upper(),
        "days_since_last_observation": days_since,
        "is_stale": is_stale,
        "checked_at": _utc_now_iso(),
    }


@dataclass
class ReconcileReport:
    drifts: list[Drift] = field(default_factory=list)
    lifecycles: list[dict[str, Any]] = field(default_factory=list)
    not_found: list[str] = field(default_factory=list)
    series_checked: int = 0

    @property
    def has_errors(self) -> bool:
        return any(d.severity == "error" for d in self.drifts) or bool(self.not_found)

    @property
    def stale(self) -> list[str]:
        return [lc["series_id"] for lc in self.lifecycles if lc["is_stale"]]

    def by_severity(self, severity: str) -> list[Drift]:
        return [d for d in self.drifts if d.severity == severity]

    def summary(self) -> dict[str, Any]:
        return {
            "series_checked": self.series_checked,
            "drifts": len(self.drifts),
            "errors": len(self.by_severity("error")) + len(self.not_found),
            "warnings": len(self.by_severity("warning")),
            "info": len(self.by_severity("info")),
            "not_found": len(self.not_found),
            "stale": len(self.stale),
        }


def reconcile(
    manifests: Iterable[Any],
    client: Any,
    *,
    today: Optional[date] = None,
    active_only: bool = False,
    series_ids: Optional[list[str]] = None,
) -> ReconcileReport:
    """Fetch FRED metadata for each series and diff it against the manifests.

    ``client`` needs a ``get_series_metadata(series_id)`` method. Network/lookup
    failures for one series are recorded (``not_found``) without aborting the
    run. ``series_ids`` restricts reconciliation to a subset.
    """
    specs = all_series(list(manifests), active_only=active_only)
    if series_ids:
        wanted = set(series_ids)
        specs = [s for s in specs if s.series_id in wanted]
    report = ReconcileReport(series_checked=len(specs))
    for spec in specs:
        try:
            meta = client.get_series_metadata(spec.series_id)
        except FredAPIError:
            report.not_found.append(spec.series_id)
            report.drifts.append(
                Drift(spec.series_id, "series_id", spec.series_id, "<not found>",
                      "not_found", "error")
            )
            continue
        report.drifts.extend(compare_metadata(spec, meta))
        report.lifecycles.append(lifecycle_snapshot(spec, meta, today))
    return report


def persist_report(
    config: PipelineConfig, report: ReconcileReport, warehouse: Any
) -> dict[str, int]:
    """Write lifecycle snapshots + drift findings to the meta layer."""
    detected_at = _utc_now_iso()
    drift_rows = [d.to_row(detected_at) for d in report.drifts]
    n_life = warehouse.write_lifecycle(report.lifecycles)
    n_drift = warehouse.write_drift(drift_rows)
    return {"lifecycle_rows": n_life, "drift_rows": n_drift}
