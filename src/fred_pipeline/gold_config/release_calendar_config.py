"""Config-driven curated release set for the Gold economic release calendar.

``config/release_calendar.yml`` declares which of FRED's ~300 releases the
terminal's CAL module tracks: each entry names the FRED ``release_id`` /
``release_name``, an ``importance`` tier, an ``econ_category``, and a
``representative_series_id`` so a release can join to its headline print.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Optional

import yaml

DEFAULT_RELEASE_CALENDAR_PATH = "config/release_calendar.yml"

VALID_IMPORTANCE = {"HIGH", "MEDIUM", "LOW"}


class ReleaseCalendarConfigError(ValueError):
    """Raised when the release-calendar config file is malformed."""


@dataclass(frozen=True)
class ReleaseCalendarEntry:
    release_id: int
    release_name: str
    importance: str
    econ_category: str
    representative_series_id: str

    def __post_init__(self) -> None:
        if not self.release_name:
            raise ReleaseCalendarConfigError(
                f"Release {self.release_id} is missing release_name"
            )
        if self.importance not in VALID_IMPORTANCE:
            raise ReleaseCalendarConfigError(
                f"Release {self.release_id} has invalid importance "
                f"{self.importance!r}; expected one of {sorted(VALID_IMPORTANCE)}"
            )
        if not self.representative_series_id:
            raise ReleaseCalendarConfigError(
                f"Release {self.release_id} is missing representative_series_id"
            )


def _parse_releases(raw: Any, *, source: str) -> list[ReleaseCalendarEntry]:
    if not isinstance(raw, list):
        raise ReleaseCalendarConfigError(
            f"{source} must contain a top-level 'releases' list"
        )
    known = {
        "release_id", "release_name", "importance", "econ_category",
        "representative_series_id",
    }
    entries: list[ReleaseCalendarEntry] = []
    seen: set[int] = set()
    for entry in raw:
        if not isinstance(entry, dict):
            raise ReleaseCalendarConfigError(
                f"Each release entry in {source} must be a mapping, got {entry!r}"
            )
        unknown = set(entry) - known
        if unknown:
            raise ReleaseCalendarConfigError(
                f"Release entry {entry.get('release_id')!r} in {source} has "
                f"unknown field(s): {sorted(unknown)}. Allowed: {sorted(known)}"
            )
        release_id = int(entry.get("release_id", 0))
        if release_id in seen:
            raise ReleaseCalendarConfigError(
                f"Duplicate release_id {release_id} in {source}"
            )
        seen.add(release_id)
        entries.append(
            ReleaseCalendarEntry(
                release_id=release_id,
                release_name=entry.get("release_name", ""),
                importance=entry.get("importance", ""),
                econ_category=entry.get("econ_category", ""),
                representative_series_id=entry.get("representative_series_id", ""),
            )
        )
    return entries


def load_release_calendar_config(
    path: Optional[str] = None,
) -> list[ReleaseCalendarEntry]:
    """Load curated release-calendar entries from YAML.

    Resolution: explicit ``path``, else ``FRED_RELEASE_CALENDAR_FILE`` env
    var, else ``config/release_calendar.yml``. A missing file returns ``[]``
    (the calendar table is then simply empty); a malformed file raises.
    """
    resolved = (
        path or os.environ.get("FRED_RELEASE_CALENDAR_FILE")
        or DEFAULT_RELEASE_CALENDAR_PATH
    )
    if not resolved or not os.path.isfile(resolved):
        return []
    with open(resolved, "r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    if not isinstance(data, dict):
        raise ReleaseCalendarConfigError(
            f"{resolved} must be a mapping at the top level"
        )
    return _parse_releases(data.get("releases"), source=resolved)
