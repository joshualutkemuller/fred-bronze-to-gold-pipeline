"""Config for the FOMC rate-probability engine (option A: no CME connector).

``config/fomc.yml`` declares the scheduled FOMC meeting decision dates, the
25bp outcome-bucket step, the anchor series for the current target
range/effective rate, and the short end of the Treasury curve used to
bootstrap the implied forward-rate path between meetings. See
:mod:`fred_pipeline.writer.terminal_views` (``compute_fomc_probability``)
and ``docs/handoffs/terminal_phase0_gaps.md`` item 3 for the full design.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date
from typing import Any, Optional

import yaml

DEFAULT_FOMC_PATH = "config/fomc.yml"


class FOMCConfigError(ValueError):
    """Raised when the FOMC config file is malformed."""


@dataclass(frozen=True)
class FOMCTenorDef:
    """One point on the short end of the Treasury curve used for bootstrap."""

    series_id: str
    tenor_months: int

    def __post_init__(self) -> None:
        if not self.series_id:
            raise FOMCConfigError("FOMC tenor entry is missing series_id")
        if self.tenor_months <= 0:
            raise FOMCConfigError(
                f"FOMC tenor {self.series_id!r} must have tenor_months > 0"
            )


@dataclass(frozen=True)
class FOMCConfig:
    meeting_dates: tuple[date, ...]
    bucket_step_bps: int
    target_low_series: str
    target_high_series: str
    effective_rate_series: str
    tenors: tuple[FOMCTenorDef, ...]

    def __post_init__(self) -> None:
        if not self.meeting_dates:
            raise FOMCConfigError("fomc.yml must declare at least one meeting_date")
        if list(self.meeting_dates) != sorted(self.meeting_dates):
            raise FOMCConfigError("fomc.yml meeting_dates must be ascending")
        if self.bucket_step_bps <= 0:
            raise FOMCConfigError("fomc.yml bucket_step_bps must be > 0")
        if len(self.tenors) < 2:
            raise FOMCConfigError(
                "fomc.yml must declare at least 2 tenors to bootstrap a forward rate"
            )
        months = [t.tenor_months for t in self.tenors]
        if months != sorted(months):
            raise FOMCConfigError("fomc.yml tenors must be ascending by tenor_months")


def _parse_fomc(raw: dict[str, Any], *, source: str) -> FOMCConfig:
    known = {
        "meeting_dates", "bucket_step_bps", "target_low_series",
        "target_high_series", "effective_rate_series", "tenors",
    }
    unknown = set(raw) - known
    if unknown:
        raise FOMCConfigError(
            f"{source} has unknown top-level field(s): {sorted(unknown)}. "
            f"Allowed: {sorted(known)}"
        )

    raw_dates = raw.get("meeting_dates") or []
    try:
        meeting_dates = tuple(
            d if isinstance(d, date) else date.fromisoformat(str(d))
            for d in raw_dates
        )
    except ValueError as exc:
        raise FOMCConfigError(f"{source} has an invalid meeting_date: {exc}") from exc

    raw_tenors = raw.get("tenors") or []
    if not isinstance(raw_tenors, list):
        raise FOMCConfigError(f"{source} 'tenors' must be a list")
    tenors = tuple(
        FOMCTenorDef(
            series_id=t.get("series_id", ""),
            tenor_months=int(t.get("tenor_months", 0)),
        )
        for t in raw_tenors
    )

    return FOMCConfig(
        meeting_dates=meeting_dates,
        bucket_step_bps=int(raw.get("bucket_step_bps", 25)),
        target_low_series=raw.get("target_low_series", ""),
        target_high_series=raw.get("target_high_series", ""),
        effective_rate_series=raw.get("effective_rate_series", ""),
        tenors=tenors,
    )


def load_fomc_config(path: Optional[str] = None) -> Optional[FOMCConfig]:
    """Load the FOMC config from YAML.

    Resolution: explicit ``path``, else ``FRED_FOMC_CONFIG_FILE`` env var,
    else ``config/fomc.yml``. A missing file returns ``None`` (the FOMC
    tables are then simply empty); a malformed file raises.
    """
    resolved = path or os.environ.get("FRED_FOMC_CONFIG_FILE") or DEFAULT_FOMC_PATH
    if not resolved or not os.path.isfile(resolved):
        return None
    with open(resolved, "r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    if not isinstance(data, dict):
        raise FOMCConfigError(f"{resolved} must be a mapping at the top level")
    return _parse_fomc(data, source=resolved)
