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

#: How much authority an entry carries. Default is deliberately the weakest:
#: an entry nobody has explicitly verified must not look like one that has been.
#:   verified    a human read terms_url and signed off (requires reviewed_by)
#:   provisional researched from secondary sources; good enough to operate on
#:               internally, NOT good enough to redistribute externally
#:   unreviewed  placeholder; treat the source as unknown
VALID_REVIEW_STATUSES = ("verified", "provisional", "unreviewed")

#: A verified read older than this is stale -- terms change, and an entry that
#: was right in 2019 is not evidence about today.
DEFAULT_REVIEW_MAX_AGE_DAYS = 730  # two years

VALID_LICENSE_TYPES = {
    "public-domain",
    "open-data",
    "free-tier-personal-use",
    "free-tier-commercial-ok",
    # Copyright retained by the publisher, who permits redistribution for
    # NON-commercial purposes without written permission but requires it for
    # commercial use. Distinct from 'open-data' (commercial use is fine) and
    # from 'requires-agreement' (nothing is permitted without one). BIS
    # statistics are the reference case.
    "attribution-noncommercial",
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
    #: See VALID_REVIEW_STATUSES. Defaults to 'provisional' so an entry that
    #: says nothing about its provenance is not mistaken for a signed-off one.
    review_status: str = "provisional"
    #: Who signed off. Required for review_status='verified' -- "verified" with
    #: nobody's name on it is not a review, it is an assertion.
    reviewed_by: str = ""
    #: How the terms were established, e.g. 'primary terms page read' or
    #: 'secondary reporting of the published terms'.
    source_of_truth: str = ""

    def __post_init__(self) -> None:
        if not self.source:
            raise DataLicensingConfigError("A licensing entry is missing 'source'")
        if self.review_status not in VALID_REVIEW_STATUSES:
            raise DataLicensingConfigError(
                f"Source {self.source!r} has invalid review_status "
                f"{self.review_status!r}; expected one of "
                f"{list(VALID_REVIEW_STATUSES)}"
            )
        if self.review_status == "verified" and not self.reviewed_by:
            raise DataLicensingConfigError(
                f"Source {self.source!r} claims review_status='verified' but "
                f"has no 'reviewed_by'. A verification nobody's name is on is "
                f"an assertion, not a review."
            )
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
        "last_reviewed_date", "notes", "review_status", "reviewed_by",
        "source_of_truth",
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
            review_status=str(entry.get("review_status", "provisional")).lower(),
            reviewed_by=str(entry.get("reviewed_by", "")).strip(),
            source_of_truth=str(entry.get("source_of_truth", "")).strip(),
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


def check_redistribution_review(
    active_specs: Iterable[Any],
    licensing: dict[str, SourceLicense],
    *,
    as_of: Optional[date] = None,
    max_age_days: int = DEFAULT_REVIEW_MAX_AGE_DAYS,
) -> list[LicensingViolation]:
    """Flag sources this pipeline would redistribute on unverified authority.

    The dangerous combination is not "we don't know" -- an unknown source is
    already caught by :func:`check_commercial_use` failing closed. It is an
    entry that says **yes, you may redistribute** on authority nobody has
    established. That reads identically to a lawyer-reviewed entry unless the
    register records the difference, which is what ``review_status`` is for.

    ``public-domain`` entries are exempt. U.S. federal works are public domain
    by statute (17 U.S.C. 105), not by a terms page someone has to read, so
    requiring a terms review there would be noise that trains people to ignore
    the check. Everything else that permits redistribution -- CC BY, BIS's
    non-commercial grant, anything negotiated -- rests on published terms, and
    those need a human to have actually read them.

    A ``verified`` review older than ``max_age_days`` is flagged as stale:
    terms change, and a read from three years ago is not evidence about today.
    """
    as_of = as_of or date.today()
    counts = Counter(getattr(s, "source", "fred") for s in active_specs)
    violations: list[LicensingViolation] = []
    for src in sorted(counts):
        lic = licensing.get(src)
        if lic is None or not lic.redistribution_allowed:
            continue  # unregistered is check_commercial_use's job
        if lic.license_type == "public-domain":
            continue
        if lic.review_status != "verified":
            violations.append(LicensingViolation(
                source=src, series_count=counts[src],
                reason=(
                    f"permits redistribution but review_status="
                    f"{lic.review_status!r} -- nobody has confirmed this "
                    f"against {lic.terms_url or 'its published terms'}"
                ),
            ))
        elif lic.last_reviewed_date is None:
            violations.append(LicensingViolation(
                source=src, series_count=counts[src],
                reason="review_status='verified' but no last_reviewed_date",
            ))
        else:
            age = (as_of - lic.last_reviewed_date).days
            if age > max_age_days:
                violations.append(LicensingViolation(
                    source=src, series_count=counts[src],
                    reason=(
                        f"last reviewed {age} days ago by "
                        f"{lic.reviewed_by or 'unknown'} (max {max_age_days}) "
                        f"-- terms may have changed"
                    ),
                ))
    return violations


def unverified_sources(licensing: dict[str, SourceLicense]) -> list[str]:
    """Every registered source whose terms nobody has signed off on."""
    return sorted(
        name for name, lic in licensing.items() if lic.review_status != "verified"
    )
