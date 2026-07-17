"""Config-driven spread/ratio definitions for ``gold.fred_curve_spread``.

Cross-series spreads (``long_leg - short_leg``) and ratios (``long_leg /
short_leg``) are defined in a reviewable YAML file (``config/spreads.yml`` by
default) rather than hardcoded in Python, so a reviewer can add new
cross-series features (real yields, credit spreads, PCE vs. CPI divergence,
...) across the expanded series universe without touching code — the same
pattern the manifest system uses for the series universe itself.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Optional

import yaml

# Default location for the (checked-in, reviewable) spreads config file.
DEFAULT_SPREADS_PATH = "config/spreads.yml"

VALID_OPS = {"spread", "ratio"}


class SpreadConfigError(ValueError):
    """Raised when a spreads config file is missing/malformed."""


@dataclass(frozen=True)
class SpreadDef:
    """One cross-series feature: ``name`` from ``long_leg`` and ``short_leg``.

    ``op`` is ``"spread"`` (``long_leg - short_leg``, e.g. yield curve legs)
    or ``"ratio"`` (``long_leg / short_leg``, e.g. relative-value ratios).
    """

    name: str
    long_leg: str
    short_leg: str
    op: str = "spread"
    description: str = ""

    def __post_init__(self) -> None:
        if not self.name or not self.long_leg or not self.short_leg:
            raise SpreadConfigError(f"Spread definition missing required field(s): {self}")
        if self.op not in VALID_OPS:
            raise SpreadConfigError(
                f"Spread {self.name!r} has invalid op {self.op!r}; "
                f"expected one of {sorted(VALID_OPS)}"
            )


# Used only if the config file is missing (e.g. an ad-hoc script importing
# this module without the repo's config/ directory) -- mirrors the pipeline's
# original hardcoded Treasury curve spreads so behavior never silently
# regresses to "no spreads at all".
FALLBACK_SPREADS: tuple[SpreadDef, ...] = (
    SpreadDef("T10Y2Y", "DGS10", "DGS2"),
    SpreadDef("T10Y3M", "DGS10", "DGS3MO"),
    SpreadDef("T2Y3M", "DGS2", "DGS3MO"),
    SpreadDef("T30Y10Y", "DGS30", "DGS10"),
)


def _parse_spread_defs(raw: Any, *, source: str) -> list[SpreadDef]:
    if not isinstance(raw, list):
        raise SpreadConfigError(f"{source} must contain a top-level 'spreads' list")
    defs: list[SpreadDef] = []
    seen: set[str] = set()
    for item in raw:
        if not isinstance(item, dict):
            raise SpreadConfigError(
                f"Each spread entry in {source} must be a mapping, got {item!r}"
            )
        known = {"name", "long_leg", "short_leg", "op", "description"}
        unknown = set(item) - known
        if unknown:
            raise SpreadConfigError(
                f"Spread entry {item.get('name')!r} in {source} has unknown "
                f"field(s): {sorted(unknown)}. Allowed: {sorted(known)}"
            )
        sd = SpreadDef(
            name=item.get("name", ""),
            long_leg=item.get("long_leg", ""),
            short_leg=item.get("short_leg", ""),
            op=item.get("op", "spread"),
            description=item.get("description", ""),
        )
        if sd.name in seen:
            raise SpreadConfigError(f"Duplicate spread name {sd.name!r} in {source}")
        seen.add(sd.name)
        defs.append(sd)
    return defs


def load_spread_defs(path: Optional[str] = None) -> list[SpreadDef]:
    """Load spread/ratio definitions from YAML.

    Resolution: explicit ``path`` argument, else ``FRED_SPREADS_FILE`` env
    var, else ``config/spreads.yml``. Falls back to :data:`FALLBACK_SPREADS`
    (the historical hardcoded Treasury curve spreads) if no file is found at
    any of those locations -- a *malformed* file still raises, since silently
    dropping a reviewer's edits would be worse than failing loudly.
    """
    resolved = path or os.environ.get("FRED_SPREADS_FILE") or DEFAULT_SPREADS_PATH
    if not resolved or not os.path.isfile(resolved):
        return list(FALLBACK_SPREADS)
    with open(resolved, "r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    if not isinstance(data, dict):
        raise SpreadConfigError(f"{resolved} must be a mapping at the top level")
    return _parse_spread_defs(data.get("spreads"), source=resolved)
