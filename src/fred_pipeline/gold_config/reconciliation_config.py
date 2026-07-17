"""Config-driven cross-source reconciliation pairs for
``gold.fred_source_reconciliation``.

Different sources sometimes measure the *same concept* (FRED ``UNRATE`` and BLS
``LNS14000000`` are both the SA unemployment rate; FRED ``GDP`` and a BEA NIPA
line are both nominal GDP). Their series ids differ, so there's no automatic
join — a reviewer declares the concept pairs here, and the pipeline aligns and
compares them, flagging divergence beyond a tolerance. A governance/lineage
check, not a research feature.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Optional

import yaml

DEFAULT_RECONCILIATIONS_PATH = "config/reconciliations.yml"

_FREQ_ALIASES = {
    "d": "d", "daily": "d", "w": "w", "weekly": "w", "m": "m", "monthly": "m",
    "q": "q", "quarterly": "q", "a": "a", "annual": "a",
}


class ReconciliationConfigError(ValueError):
    """Raised when a reconciliations config file is missing/malformed."""


@dataclass(frozen=True)
class ReconciliationDef:
    """Compare ``series_a`` vs ``series_b`` aligned to ``frequency``.

    ``tolerance_pct`` is the |percent difference| above which a period is flagged
    ``diverged`` (relative to ``series_b``).
    """

    name: str
    frequency: str
    series_a: str
    series_b: str
    tolerance_pct: float = 1.0
    description: str = ""

    def __post_init__(self) -> None:
        if not (self.name and self.series_a and self.series_b):
            raise ReconciliationConfigError(
                f"Reconciliation missing required field(s): {self}"
            )
        if self.frequency not in _FREQ_ALIASES.values():
            raise ReconciliationConfigError(
                f"Reconciliation {self.name!r} has invalid frequency {self.frequency!r}"
            )
        if float(self.tolerance_pct) < 0:
            raise ReconciliationConfigError(
                f"Reconciliation {self.name!r} tolerance_pct must be >= 0"
            )


def _parse_defs(raw: Any, *, source: str) -> list[ReconciliationDef]:
    if not isinstance(raw, list):
        raise ReconciliationConfigError(
            f"{source} must contain a top-level 'reconciliations' list"
        )
    defs: list[ReconciliationDef] = []
    seen: set[str] = set()
    for item in raw:
        if not isinstance(item, dict):
            raise ReconciliationConfigError(
                f"Each reconciliation entry in {source} must be a mapping, got {item!r}"
            )
        known = {"name", "frequency", "series_a", "series_b", "tolerance_pct",
                 "description"}
        unknown = set(item) - known
        if unknown:
            raise ReconciliationConfigError(
                f"Reconciliation {item.get('name')!r} in {source} has unknown "
                f"field(s): {sorted(unknown)}. Allowed: {sorted(known)}"
            )
        name = item.get("name", "")
        freq = _FREQ_ALIASES.get(str(item.get("frequency", "")).strip().lower())
        if freq is None:
            raise ReconciliationConfigError(
                f"Reconciliation {name!r} in {source} has invalid/missing frequency "
                f"{item.get('frequency')!r}"
            )
        rc = ReconciliationDef(
            name=name,
            frequency=freq,
            series_a=str(item.get("series_a", "")),
            series_b=str(item.get("series_b", "")),
            tolerance_pct=float(item.get("tolerance_pct", 1.0)),
            description=item.get("description", ""),
        )
        if rc.name in seen:
            raise ReconciliationConfigError(f"Duplicate reconciliation name {rc.name!r}")
        seen.add(rc.name)
        defs.append(rc)
    return defs


def load_reconciliation_defs(path: Optional[str] = None) -> list[ReconciliationDef]:
    """Load reconciliation pairs from YAML. Resolution: explicit ``path`` →
    ``FRED_RECONCILIATIONS_FILE`` env → ``config/reconciliations.yml``. Missing
    file → no pairs (additive Gold table); malformed → raises."""
    resolved = (path or os.environ.get("FRED_RECONCILIATIONS_FILE")
                or DEFAULT_RECONCILIATIONS_PATH)
    if not resolved or not os.path.isfile(resolved):
        return []
    with open(resolved, "r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    if not isinstance(data, dict):
        raise ReconciliationConfigError(f"{resolved} must be a mapping at the top level")
    return _parse_defs(data.get("reconciliations") or [], source=resolved)
