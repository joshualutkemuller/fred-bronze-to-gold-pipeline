"""Config-driven series catalog for the market-terminal analytical views.

``config/series_catalog.yml`` assigns presentation semantics to series already
ingested by the pipeline — the ``econ_category`` bucket (the terminal's 10
dashboard groups), a ``polarity`` (is a rise bullish, bearish, or neutral?),
the default display transform, and scaling hints. It is the single place those
semantics live: ``gold.dim_series`` materializes it (joined with ``meta``) and
``gold.macro_indicator_dashboard`` computes only over cataloged series.

Like ``config/spreads.yml`` / ``config/cross_series.yml``, this is a reviewable
YAML file: adding a series to the ECON dashboard is a config edit, not code.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Optional

import yaml

# Default location for the (checked-in, reviewable) catalog file.
DEFAULT_CATALOG_PATH = "config/series_catalog.yml"

# The terminal's 10 EconCategory buckets.
VALID_CATEGORIES = {
    "GROWTH", "INFLATION", "LABOR", "RATES", "CREDIT",
    "HOUSING", "CONSUMER", "MONEY", "ACTIVITY", "FX",
}

# FRED-style display transforms the terminal uses: pc1 = YoY %, pch = period %,
# chg = first difference, bps = value shown in basis points, level = as-is.
VALID_TRANSFORMS = {"pc1", "pch", "chg", "bps", "level"}

VALID_POLARITY = {-1, 0, 1}


class CatalogConfigError(ValueError):
    """Raised when a series-catalog config file is malformed."""


@dataclass(frozen=True)
class CatalogEntry:
    """Presentation semantics for one dashboard series.

    ``polarity``: +1 a rise is bullish (payrolls), −1 a rise is bearish
    (unemployment, inflation), 0 neutral (FX, policy rates).
    ``surprise_window``: trailing observations used for the dashboard's
    no-consensus "surprise" proxy (latest − trailing mean).
    """

    series_id: str
    econ_category: str
    polarity: int = 0
    default_transform: str = "level"
    source: str = "fred"
    scale: str = ""
    decimals: int = 2
    surprise_window: int = 12
    notes: str = ""

    def __post_init__(self) -> None:
        if not self.series_id:
            raise CatalogConfigError(f"Catalog entry missing series_id: {self}")
        if self.econ_category not in VALID_CATEGORIES:
            raise CatalogConfigError(
                f"Series {self.series_id!r} has invalid econ_category "
                f"{self.econ_category!r}; expected one of {sorted(VALID_CATEGORIES)}"
            )
        if self.polarity not in VALID_POLARITY:
            raise CatalogConfigError(
                f"Series {self.series_id!r} has invalid polarity {self.polarity!r}; "
                f"expected one of {sorted(VALID_POLARITY)}"
            )
        if self.default_transform not in VALID_TRANSFORMS:
            raise CatalogConfigError(
                f"Series {self.series_id!r} has invalid default_transform "
                f"{self.default_transform!r}; expected one of {sorted(VALID_TRANSFORMS)}"
            )
        if self.surprise_window < 2:
            raise CatalogConfigError(
                f"Series {self.series_id!r} has surprise_window "
                f"{self.surprise_window}; must be >= 2"
            )


def _parse_entries(raw: Any, *, source: str) -> list[CatalogEntry]:
    if not isinstance(raw, list):
        raise CatalogConfigError(f"{source} must contain a top-level 'series' list")
    entries: list[CatalogEntry] = []
    seen: set[str] = set()
    known = {
        "series_id", "econ_category", "polarity", "default_transform",
        "source", "scale", "decimals", "surprise_window", "notes",
    }
    for item in raw:
        if not isinstance(item, dict):
            raise CatalogConfigError(
                f"Each catalog entry in {source} must be a mapping, got {item!r}"
            )
        unknown = set(item) - known
        if unknown:
            raise CatalogConfigError(
                f"Catalog entry {item.get('series_id')!r} in {source} has unknown "
                f"field(s): {sorted(unknown)}. Allowed: {sorted(known)}"
            )
        entry = CatalogEntry(
            series_id=item.get("series_id", ""),
            econ_category=item.get("econ_category", ""),
            polarity=int(item.get("polarity", 0)),
            default_transform=item.get("default_transform", "level"),
            source=item.get("source", "fred"),
            scale=item.get("scale", ""),
            decimals=int(item.get("decimals", 2)),
            surprise_window=int(item.get("surprise_window", 12)),
            notes=item.get("notes", ""),
        )
        if entry.series_id in seen:
            raise CatalogConfigError(
                f"Duplicate series_id {entry.series_id!r} in {source}"
            )
        seen.add(entry.series_id)
        entries.append(entry)
    return entries


def load_series_catalog(path: Optional[str] = None) -> list[CatalogEntry]:
    """Load dashboard-catalog entries from YAML.

    Resolution: explicit ``path``, else ``FRED_SERIES_CATALOG_FILE`` env var,
    else ``config/series_catalog.yml``. A missing file returns ``[]`` — the
    dashboard tables are then simply empty (nothing has been cataloged), which
    is the honest state, unlike spreads where a hardcoded history existed to
    preserve. A *malformed* file still raises.
    """
    resolved = path or os.environ.get("FRED_SERIES_CATALOG_FILE") or DEFAULT_CATALOG_PATH
    if not resolved or not os.path.isfile(resolved):
        return []
    with open(resolved, "r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    if not isinstance(data, dict):
        raise CatalogConfigError(f"{resolved} must be a mapping at the top level")
    return _parse_entries(data.get("series"), source=resolved)
