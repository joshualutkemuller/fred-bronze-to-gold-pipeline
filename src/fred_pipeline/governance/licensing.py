"""Per-source data-licensing register
(``docs/handoffs/governance_and_access_control.md`` item 1).

Manifests declare what data this pipeline pulls; this module tracks what the
pipeline is actually *allowed to do* with it once pulled -- redistribute it,
use it commercially, or neither without further review. Config-driven, same
pattern as ``gold_config/*.py``: ``config/data_licensing.yml`` -> a frozen
dataclass per source -> a pure check function ``fred_pipeline validate
--commercial`` calls at validate time.

The register was populated from each source's generally-published policy
(U.S. federal statistical agencies are public domain; World Bank Open Data
is CC BY 4.0; Tiingo/Stooq/iShares terms per this project's own prior
research) rather than a live fetch of every terms page in one pass -- see
each entry's ``notes`` in ``config/data_licensing.yml`` and spot-check
``terms_url``/``last_reviewed_date`` before treating this as a final legal
answer for a real compliance decision.
"""

from __future__ import annotations

import os
from collections import Counter
from dataclasses import dataclass
from datetime import date
from typing import Any, Iterable, Optional

import yaml

DEFAULT_LICENSING_PATH = "config/data_licensing.yml"

VALID_LICENSE_TYPES = {
    "public-domain",
    "open-data",
    "free-tier-personal-use",
    "free-tier-commercial-ok",
    "requires-agreement",
}


class DataLicensingConfigError(ValueError):
    """Raised when the data-licensing config file is malformed."""


@dataclass(frozen=True)
class SourceLicense:
    source: str
    license_type: str
    redistribution_allowed: bool
    commercial_use_allowed: bool
    attribution_required: bool
    terms_url: str
    last_reviewed_date: Optional[date]
    notes: str = ""

    def __post_init__(self) -> None:
        if not self.source:
            raise DataLicensingConfigError("A licensing entry is missing 'source'")
        if self.license_type not in VALID_LICENSE_TYPES:
            raise DataLicensingConfigError(
                f"Source {self.source!r} has invalid license_type "
                f"{self.license_type!r}; expected one of "
                f"{sorted(VALID_LICENSE_TYPES)}"
            )


def _parse_sources(raw: Any, *, source_file: str) -> dict[str, SourceLicense]:
    if not isinstance(raw, list):
        raise DataLicensingConfigError(
            f"{source_file} must contain a top-level 'sources' list"
        )
    known = {
        "source", "license_type", "redistribution_allowed",
        "commercial_use_allowed", "attribution_required", "terms_url",
        "last_reviewed_date", "notes",
    }
    out: dict[str, SourceLicense] = {}
    for entry in raw:
        if not isinstance(entry, dict):
            raise DataLicensingConfigError(
                f"Each source entry in {source_file} must be a mapping, "
                f"got {entry!r}"
            )
        unknown = set(entry) - known
        if unknown:
            raise DataLicensingConfigError(
                f"Source entry {entry.get('source')!r} in {source_file} has "
                f"unknown field(s): {sorted(unknown)}. Allowed: {sorted(known)}"
            )
        src = str(entry.get("source", "")).strip().lower()
        if src in out:
            raise DataLicensingConfigError(
                f"Duplicate source {src!r} in {source_file}"
            )
        raw_date = entry.get("last_reviewed_date")
        reviewed = (
            raw_date if isinstance(raw_date, date)
            else date.fromisoformat(str(raw_date)) if raw_date else None
        )
        out[src] = SourceLicense(
            source=src,
            license_type=str(entry.get("license_type", "")),
            redistribution_allowed=bool(entry.get("redistribution_allowed", False)),
            commercial_use_allowed=bool(entry.get("commercial_use_allowed", False)),
            attribution_required=bool(entry.get("attribution_required", False)),
            terms_url=str(entry.get("terms_url", "")),
            last_reviewed_date=reviewed,
            notes=str(entry.get("notes", "")).strip(),
        )
    return out


def load_data_licensing_config(path: Optional[str] = None) -> dict[str, SourceLicense]:
    """Load the per-source licensing register from YAML, keyed by source name.

    Resolution: explicit ``path``, else ``FRED_DATA_LICENSING_FILE`` env
    var, else ``config/data_licensing.yml``. A missing file returns ``{}``
    (no sources registered -- callers should treat an unregistered source as
    *unknown*, not *safe*); a malformed file raises.
    """
    resolved = (
        path or os.environ.get("FRED_DATA_LICENSING_FILE")
        or DEFAULT_LICENSING_PATH
    )
    if not resolved or not os.path.isfile(resolved):
        return {}
    with open(resolved, "r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    if not isinstance(data, dict):
        raise DataLicensingConfigError(
            f"{resolved} must be a mapping at the top level"
        )
    return _parse_sources(data.get("sources"), source_file=resolved)


@dataclass(frozen=True)
class LicensingViolation:
    source: str
    series_count: int
    reason: str


def check_commercial_use(
    active_specs: Iterable[Any],
    licensing: dict[str, SourceLicense],
) -> list[LicensingViolation]:
    """Flag active sources that don't clear commercial use.

    ``active_specs`` are ``SeriesSpec``-like objects (need a ``.source``
    attribute) -- pass the active series from ``all_series()``. An
    **unregistered** source (no entry in the licensing register) is flagged
    too: fail closed, not open, so a newly onboarded source can't silently
    skip the licensing review this register exists to force.
    """
    counts = Counter(getattr(s, "source", "fred") for s in active_specs)
    violations: list[LicensingViolation] = []
    for src in sorted(counts):
        lic = licensing.get(src)
        if lic is None:
            violations.append(LicensingViolation(
                source=src, series_count=counts[src],
                reason="no licensing register entry -- unreviewed",
            ))
        elif not lic.commercial_use_allowed:
            violations.append(LicensingViolation(
                source=src, series_count=counts[src],
                reason=(
                    f"license_type={lic.license_type!r} does not allow "
                    "commercial use"
                ),
            ))
    return violations
