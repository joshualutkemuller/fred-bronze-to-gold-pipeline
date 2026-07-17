"""Config-driven Treasury-curve tenor map for the Curve Lab Gold tables.

``config/curve.yml`` maps curve tenors (1M … 30Y) to their FRED constant-
maturity series (``DGS1MO`` … ``DGS30``), so ``gold.treasury_curve`` /
``gold.treasury_curve_metrics`` can be re-pointed or extended (e.g. real-yield
TIPS curve) without touching code — the same reviewable-YAML pattern as
``config/spreads.yml``.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Optional

import yaml

# Default location for the (checked-in, reviewable) curve config file.
DEFAULT_CURVE_PATH = "config/curve.yml"


class CurveConfigError(ValueError):
    """Raised when a curve config file is malformed."""


@dataclass(frozen=True)
class TenorDef:
    """One curve point: display ``label``, sortable ``months``, source series."""

    label: str
    months: int
    series_id: str

    def __post_init__(self) -> None:
        if not self.label or not self.series_id:
            raise CurveConfigError(f"Tenor definition missing required field(s): {self}")
        if self.months <= 0:
            raise CurveConfigError(
                f"Tenor {self.label!r} has invalid months {self.months!r}; must be > 0"
            )


# Used only if the config file is missing — the standard FRED constant-maturity
# curve. DGS3/DGS7/DGS20 are included so the curve is complete once those
# manifest entries are activated; tenors with no ingested data simply emit no
# rows (the engine skips absent series).
FALLBACK_TENORS: tuple[TenorDef, ...] = (
    TenorDef("1M", 1, "DGS1MO"),
    TenorDef("3M", 3, "DGS3MO"),
    TenorDef("6M", 6, "DGS6MO"),
    TenorDef("1Y", 12, "DGS1"),
    TenorDef("2Y", 24, "DGS2"),
    TenorDef("3Y", 36, "DGS3"),
    TenorDef("5Y", 60, "DGS5"),
    TenorDef("7Y", 84, "DGS7"),
    TenorDef("10Y", 120, "DGS10"),
    TenorDef("20Y", 240, "DGS20"),
    TenorDef("30Y", 360, "DGS30"),
)


def _parse_tenor_defs(raw: Any, *, source: str) -> list[TenorDef]:
    if not isinstance(raw, list):
        raise CurveConfigError(f"{source} must contain a top-level 'tenors' list")
    defs: list[TenorDef] = []
    seen_labels: set[str] = set()
    seen_months: set[int] = set()
    known = {"label", "months", "series_id"}
    for item in raw:
        if not isinstance(item, dict):
            raise CurveConfigError(
                f"Each tenor entry in {source} must be a mapping, got {item!r}"
            )
        unknown = set(item) - known
        if unknown:
            raise CurveConfigError(
                f"Tenor entry {item.get('label')!r} in {source} has unknown "
                f"field(s): {sorted(unknown)}. Allowed: {sorted(known)}"
            )
        td = TenorDef(
            label=item.get("label", ""),
            months=int(item.get("months", 0)),
            series_id=item.get("series_id", ""),
        )
        if td.label in seen_labels:
            raise CurveConfigError(f"Duplicate tenor label {td.label!r} in {source}")
        if td.months in seen_months:
            raise CurveConfigError(
                f"Duplicate tenor months {td.months!r} in {source}"
            )
        seen_labels.add(td.label)
        seen_months.add(td.months)
        defs.append(td)
    return sorted(defs, key=lambda t: t.months)


def load_curve_defs(path: Optional[str] = None) -> list[TenorDef]:
    """Load curve tenor definitions from YAML, sorted by months.

    Resolution: explicit ``path``, else ``FRED_CURVE_FILE`` env var, else
    ``config/curve.yml``. Falls back to :data:`FALLBACK_TENORS` (the standard
    FRED constant-maturity curve) if no file is found; a *malformed* file
    still raises.
    """
    resolved = path or os.environ.get("FRED_CURVE_FILE") or DEFAULT_CURVE_PATH
    if not resolved or not os.path.isfile(resolved):
        return list(FALLBACK_TENORS)
    with open(resolved, "r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    if not isinstance(data, dict):
        raise CurveConfigError(f"{resolved} must be a mapping at the top level")
    return _parse_tenor_defs(data.get("tenors"), source=resolved)
