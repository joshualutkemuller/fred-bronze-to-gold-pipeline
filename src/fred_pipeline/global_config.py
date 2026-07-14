"""Config loader for the Phase-6 global tables (GCPI / GPOL).

``config/global_series.yml`` maps countries to their inflation and
policy-rate series. Entries can point at any ingested source: World Bank
``ISO3:FP.CPI.TOTL.ZG`` (already an annual YoY %, ``transform: level``) or a
FRED index mirror (``transform: yoy_from_index`` computes the YoY % from the
index). Missing file → empty config; malformed → raises.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Optional

import yaml

DEFAULT_GLOBAL_PATH = "config/global_series.yml"

VALID_REGIONS = {"AMER", "EMEA", "APAC"}
VALID_INFLATION_TRANSFORMS = {"level", "yoy_from_index"}


class GlobalConfigError(ValueError):
    """Raised when a global-series config file is malformed."""


@dataclass(frozen=True)
class GlobalInflationDef:
    """One country's CPI series. ``transform: level`` = the series already is
    a YoY inflation rate in percent (e.g. World Bank FP.CPI.TOTL.ZG);
    ``yoy_from_index`` = compute date-based YoY % from a CPI index.
    ``target`` is the central bank's inflation target in percent."""

    country: str
    iso3: str
    region: str
    series_id: str
    transform: str = "level"
    target: Optional[float] = None

    def __post_init__(self) -> None:
        if not self.country or not self.iso3 or not self.series_id:
            raise GlobalConfigError(
                f"Global inflation entry missing required field(s): {self}"
            )
        if self.region not in VALID_REGIONS:
            raise GlobalConfigError(
                f"Entry {self.country!r} has invalid region {self.region!r}; "
                f"expected one of {sorted(VALID_REGIONS)}"
            )
        if self.transform not in VALID_INFLATION_TRANSFORMS:
            raise GlobalConfigError(
                f"Entry {self.country!r} has invalid transform "
                f"{self.transform!r}; expected one of "
                f"{sorted(VALID_INFLATION_TRANSFORMS)}"
            )


@dataclass(frozen=True)
class GlobalPolicyRateDef:
    """One country's policy rate series (percent)."""

    country: str
    iso3: str
    region: str
    series_id: str

    def __post_init__(self) -> None:
        if not self.country or not self.iso3 or not self.series_id:
            raise GlobalConfigError(
                f"Global policy-rate entry missing required field(s): {self}"
            )
        if self.region not in VALID_REGIONS:
            raise GlobalConfigError(
                f"Entry {self.country!r} has invalid region {self.region!r}; "
                f"expected one of {sorted(VALID_REGIONS)}"
            )


@dataclass(frozen=True)
class GlobalConfig:
    inflation: tuple[GlobalInflationDef, ...] = ()
    policy_rates: tuple[GlobalPolicyRateDef, ...] = ()


def _entries(raw: Any, known: set[str], source: str, kind: str) -> list[dict]:
    if raw is None:
        return []
    if not isinstance(raw, list):
        raise GlobalConfigError(f"'{kind}' in {source} must be a list")
    out = []
    for item in raw:
        if not isinstance(item, dict):
            raise GlobalConfigError(
                f"Each {kind} entry in {source} must be a mapping, got {item!r}"
            )
        unknown = set(item) - known
        if unknown:
            raise GlobalConfigError(
                f"{kind} entry {item.get('country')!r} in {source} has "
                f"unknown field(s): {sorted(unknown)}. Allowed: {sorted(known)}"
            )
        out.append(item)
    return out


def load_global_config(path: Optional[str] = None) -> GlobalConfig:
    resolved = path or os.environ.get("FRED_GLOBAL_FILE") or DEFAULT_GLOBAL_PATH
    if not resolved or not os.path.isfile(resolved):
        return GlobalConfig()
    with open(resolved, "r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    if not isinstance(data, dict):
        raise GlobalConfigError(f"{resolved} must be a mapping at the top level")

    inflation: list[GlobalInflationDef] = []
    seen: set[str] = set()
    for item in _entries(
        data.get("inflation"),
        {"country", "iso3", "region", "series_id", "transform", "target"},
        resolved, "inflation",
    ):
        target = item.get("target")
        d = GlobalInflationDef(
            country=item.get("country", ""),
            iso3=item.get("iso3", ""),
            region=item.get("region", ""),
            series_id=item.get("series_id", ""),
            transform=item.get("transform", "level"),
            target=float(target) if target is not None else None,
        )
        if d.iso3 in seen:
            raise GlobalConfigError(
                f"Duplicate inflation iso3 {d.iso3!r} in {resolved}"
            )
        seen.add(d.iso3)
        inflation.append(d)

    policy: list[GlobalPolicyRateDef] = []
    seen_p: set[str] = set()
    for item in _entries(
        data.get("policy_rates"),
        {"country", "iso3", "region", "series_id"},
        resolved, "policy_rates",
    ):
        d = GlobalPolicyRateDef(
            country=item.get("country", ""),
            iso3=item.get("iso3", ""),
            region=item.get("region", ""),
            series_id=item.get("series_id", ""),
        )
        if d.iso3 in seen_p:
            raise GlobalConfigError(
                f"Duplicate policy-rate iso3 {d.iso3!r} in {resolved}"
            )
        seen_p.add(d.iso3)
        policy.append(d)

    return GlobalConfig(inflation=tuple(inflation), policy_rates=tuple(policy))
