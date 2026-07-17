"""Config-driven inflation item tree for the Inflation Explorer Gold tables.

``config/inflation_items.yml`` declares the CPI/PCE item hierarchy the INFL
surfaces render: each entry names a series, its basket (CPI or PCE), whether
it's the SA or NSA variant, its parent item and level in the tree, an optional
relative-importance ``weight`` (percent of the headline basket — drives the
contribution waterfall), and a ``waterfall`` flag marking the items whose
contributions make up the headline decomposition.

One file carries hierarchy *and* weights (a deliberate refinement of the plan's
two-file sketch): a weight sits next to the item it belongs to, so there's no
join between files to get wrong. Weights are BLS relative importance — they
change annually, and updating them is a config edit here, never code.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Optional

import yaml

# Default location for the (checked-in, reviewable) items file.
DEFAULT_INFLATION_ITEMS_PATH = "config/inflation_items.yml"

VALID_BASKETS = {"CPI", "PCE"}
VALID_SA_NSA = {"SA", "NSA"}


class InflationConfigError(ValueError):
    """Raised when an inflation-items config file is malformed."""


@dataclass(frozen=True)
class InflationItemDef:
    """One node of an inflation basket tree.

    ``level`` 0 is the basket headline (one per ``(basket, sa_nsa)`` tree);
    ``parent`` names another entry's series_id (empty at the root).
    ``weight`` is the item's relative importance in percent of the headline
    basket; ``waterfall: true`` marks the additive decomposition set (e.g. the
    8 CPI major groups) whose ``weight × MoM`` contributions the waterfall
    chart stacks against the headline print.
    """

    series_id: str
    label: str
    basket: str
    sa_nsa: str
    parent: str = ""
    level: int = 0
    weight: Optional[float] = None
    waterfall: bool = False

    def __post_init__(self) -> None:
        if not self.series_id or not self.label:
            raise InflationConfigError(
                f"Inflation item missing required field(s): {self}"
            )
        if self.basket not in VALID_BASKETS:
            raise InflationConfigError(
                f"Item {self.series_id!r} has invalid basket {self.basket!r}; "
                f"expected one of {sorted(VALID_BASKETS)}"
            )
        if self.sa_nsa not in VALID_SA_NSA:
            raise InflationConfigError(
                f"Item {self.series_id!r} has invalid sa_nsa {self.sa_nsa!r}; "
                f"expected one of {sorted(VALID_SA_NSA)}"
            )
        if self.level < 0:
            raise InflationConfigError(
                f"Item {self.series_id!r} has negative level {self.level}"
            )
        if self.level == 0 and self.parent:
            raise InflationConfigError(
                f"Item {self.series_id!r} is level 0 but has a parent "
                f"{self.parent!r}"
            )
        if self.level > 0 and not self.parent:
            raise InflationConfigError(
                f"Item {self.series_id!r} is level {self.level} but has no parent"
            )
        if self.weight is not None and not 0 < self.weight <= 100:
            raise InflationConfigError(
                f"Item {self.series_id!r} weight must be in (0, 100], "
                f"got {self.weight}"
            )
        if self.waterfall and self.weight is None:
            raise InflationConfigError(
                f"Item {self.series_id!r} is in the waterfall but has no weight"
            )


def _parse_items(raw: Any, *, source: str) -> list[InflationItemDef]:
    if not isinstance(raw, list):
        raise InflationConfigError(f"{source} must contain a top-level 'items' list")
    known = {
        "series_id", "label", "basket", "sa_nsa", "parent", "level",
        "weight", "waterfall",
    }
    items: list[InflationItemDef] = []
    seen: set[str] = set()
    for entry in raw:
        if not isinstance(entry, dict):
            raise InflationConfigError(
                f"Each item entry in {source} must be a mapping, got {entry!r}"
            )
        unknown = set(entry) - known
        if unknown:
            raise InflationConfigError(
                f"Item entry {entry.get('series_id')!r} in {source} has unknown "
                f"field(s): {sorted(unknown)}. Allowed: {sorted(known)}"
            )
        weight = entry.get("weight")
        item = InflationItemDef(
            series_id=entry.get("series_id", ""),
            label=entry.get("label", ""),
            basket=entry.get("basket", ""),
            sa_nsa=entry.get("sa_nsa", ""),
            parent=entry.get("parent", "") or "",
            level=int(entry.get("level", 0)),
            weight=float(weight) if weight is not None else None,
            waterfall=bool(entry.get("waterfall", False)),
        )
        if item.series_id in seen:
            raise InflationConfigError(
                f"Duplicate item series_id {item.series_id!r} in {source}"
            )
        seen.add(item.series_id)
        items.append(item)

    # Referential integrity: parents must exist; one root per tree.
    by_id = {i.series_id for i in items}
    roots: dict[tuple[str, str], str] = {}
    for i in items:
        if i.parent and i.parent not in by_id:
            raise InflationConfigError(
                f"Item {i.series_id!r} in {source} references unknown parent "
                f"{i.parent!r}"
            )
        if i.level == 0:
            key = (i.basket, i.sa_nsa)
            if key in roots:
                raise InflationConfigError(
                    f"Tree {key} in {source} has two level-0 roots: "
                    f"{roots[key]!r} and {i.series_id!r}"
                )
            roots[key] = i.series_id
    return items


def load_inflation_items(path: Optional[str] = None) -> list[InflationItemDef]:
    """Load inflation-item definitions from YAML.

    Resolution: explicit ``path``, else ``FRED_INFLATION_ITEMS_FILE`` env var,
    else ``config/inflation_items.yml``. A missing file returns ``[]`` (the
    explorer tables are then simply empty); a malformed file raises.
    """
    resolved = (
        path or os.environ.get("FRED_INFLATION_ITEMS_FILE")
        or DEFAULT_INFLATION_ITEMS_PATH
    )
    if not resolved or not os.path.isfile(resolved):
        return []
    with open(resolved, "r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    if not isinstance(data, dict):
        raise InflationConfigError(f"{resolved} must be a mapping at the top level")
    return _parse_items(data.get("items"), source=resolved)
