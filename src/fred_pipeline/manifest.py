"""Manifest loading and validation.

Manifests are the single source of truth for *what* the pipeline ingests and
*how* each series should be treated. They are plain YAML files so that quant
researchers and engineers can review series universes in pull requests without
touching Python.

This module is pure Python (no Spark, no network) and is the most heavily
tested part of the codebase.
"""

from __future__ import annotations

import glob
import os
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Any, Iterable, Optional

import yaml


class LoadType(str, Enum):
    FULL = "full"          # re-pull the entire history every run
    INCREMENTAL = "incremental"  # only pull observations since last watermark


class ValidationProfile(str, Enum):
    STRICT = "strict"      # any DQ failure fails the series run
    STANDARD = "standard"  # warnings recorded, run continues
    LENIENT = "lenient"    # minimal checks (e.g. brand-new/experimental series)


# Frequencies as reported by FRED.
VALID_FREQUENCIES = {
    "d", "w", "bw", "m", "q", "sa", "a",  # daily..annual (FRED short codes)
    "daily", "weekly", "biweekly", "monthly", "quarterly", "semiannual", "annual",
}

# Max age (days since the latest observation) before a series is "stale" — the
# expected release almost certainly hasn't landed. Shared by data-quality
# freshness checks and metadata reconciliation. Keyed by frequency code.
FREQUENCY_MAX_AGE_DAYS = {
    "d": 10, "daily": 10,
    "w": 21, "weekly": 21,
    "bw": 30, "biweekly": 30,
    "m": 75, "monthly": 75,
    "q": 200, "quarterly": 200,
    "sa": 380, "semiannual": 380,
    "a": 550, "annual": 550,
}

REQUIRED_FIELDS = ("series_id", "title", "frequency")


class ManifestError(ValueError):
    """Raised when a manifest is structurally or semantically invalid."""


@dataclass
class SeriesSpec:
    """Specification for a single FRED series, mirroring the handoff doc.

    Only ``series_id``, ``title`` and ``frequency`` are strictly required; the
    rest carry sensible defaults so a minimal manifest stays readable while a
    rich one remains fully expressible.
    """

    series_id: str
    title: str
    frequency: str
    category: str = "uncategorized"
    units: str = ""
    active: bool = True
    load_type: LoadType = LoadType.INCREMENTAL
    expected_update_frequency: str = ""
    # Revision-sensitive by default: capture full point-in-time (ALFRED) history
    # so backtests never suffer look-ahead bias. Collapsing to "latest revised"
    # is always possible downstream; recovering un-captured vintages is not.
    # For provably non-revised market/price series (yields, SOFR, breakevens)
    # this is a cheap no-op (one vintage per date) and may be set to false.
    vintage_enabled: bool = True
    validation_profile: ValidationProfile = ValidationProfile.STANDARD
    business_owner: str = ""
    technical_owner: str = ""
    downstream_use_case: str = ""
    priority: int = 3  # 1 (highest) .. 5 (lowest)
    # Per-series override for the incremental "restate last N observations"
    # window. None -> use the pipeline-level PipelineConfig.restate_last_n.
    # Tune higher for series with deep benchmark revisions (e.g. GDP, payrolls).
    restate_records: Optional[int] = None
    # Optional data-quality value bounds (inclusive). When set, non-missing
    # observations outside [min_value, max_value] fail the value_bounds check.
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    # Free-form tags for grouping in the feature store (e.g. ["rates", "curve"]).
    tags: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        self._validate()

    def _validate(self) -> None:
        if not self.series_id or not str(self.series_id).strip():
            raise ManifestError("series_id must be a non-empty string")
        self.series_id = str(self.series_id).strip()
        if not self.title or not str(self.title).strip():
            raise ManifestError(f"{self.series_id}: title is required")

        # Normalize enums (accept plain strings from YAML).
        self.load_type = LoadType(self.load_type)
        self.validation_profile = ValidationProfile(self.validation_profile)

        freq = str(self.frequency).strip().lower()
        if freq not in VALID_FREQUENCIES:
            raise ManifestError(
                f"{self.series_id}: unknown frequency {self.frequency!r}; "
                f"expected one of {sorted(VALID_FREQUENCIES)}"
            )
        self.frequency = freq

        if not (1 <= int(self.priority) <= 5):
            raise ManifestError(
                f"{self.series_id}: priority must be between 1 and 5, "
                f"got {self.priority}"
            )
        self.priority = int(self.priority)

        if self.restate_records is not None:
            if int(self.restate_records) < 1:
                raise ManifestError(
                    f"{self.series_id}: restate_records must be >= 1, "
                    f"got {self.restate_records}"
                )
            self.restate_records = int(self.restate_records)

        if self.min_value is not None:
            self.min_value = float(self.min_value)
        if self.max_value is not None:
            self.max_value = float(self.max_value)
        if (
            self.min_value is not None
            and self.max_value is not None
            and self.min_value > self.max_value
        ):
            raise ManifestError(
                f"{self.series_id}: min_value ({self.min_value}) must be "
                f"<= max_value ({self.max_value})"
            )

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["load_type"] = self.load_type.value
        d["validation_profile"] = self.validation_profile.value
        return d


@dataclass
class Manifest:
    """A named collection of series specs (typically one YAML file)."""

    name: str
    description: str
    series: list[SeriesSpec]
    version: int = 1
    source_path: Optional[str] = None

    @property
    def active_series(self) -> list[SeriesSpec]:
        return [s for s in self.series if s.active]

    def series_ids(self, active_only: bool = True) -> list[str]:
        src = self.active_series if active_only else self.series
        return [s.series_id for s in src]

    @classmethod
    def from_dict(cls, data: dict[str, Any], source_path: Optional[str] = None) -> "Manifest":
        if not isinstance(data, dict):
            raise ManifestError(f"Manifest must be a mapping, got {type(data).__name__}")
        raw_series = data.get("series")
        if not raw_series or not isinstance(raw_series, list):
            raise ManifestError(
                f"Manifest {data.get('name', source_path)!r} must contain a "
                "non-empty 'series' list"
            )
        specs = [SeriesSpec(**_clean_spec(item)) for item in raw_series]
        _check_duplicates(specs, source_path)
        return cls(
            name=data.get("name", os.path.basename(source_path or "unnamed")),
            description=data.get("description", ""),
            series=specs,
            version=int(data.get("version", 1)),
            source_path=source_path,
        )

    @classmethod
    def from_yaml(cls, path: str) -> "Manifest":
        with open(path, "r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh)
        return cls.from_dict(data, source_path=path)


def _clean_spec(item: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(item, dict):
        raise ManifestError(f"Each series entry must be a mapping, got {item!r}")
    missing = [f for f in REQUIRED_FIELDS if f not in item]
    if missing:
        raise ManifestError(
            f"Series entry {item.get('series_id', item)!r} missing required "
            f"field(s): {missing}"
        )
    # Drop unknown keys with a clear error rather than silently ignoring them.
    known = set(SeriesSpec.__dataclass_fields__.keys())
    unknown = set(item) - known
    if unknown:
        raise ManifestError(
            f"Series {item.get('series_id')!r} has unknown field(s): "
            f"{sorted(unknown)}. Allowed: {sorted(known)}"
        )
    return item


def _check_duplicates(specs: Iterable[SeriesSpec], source_path: Optional[str]) -> None:
    seen: set[str] = set()
    dupes: set[str] = set()
    for spec in specs:
        if spec.series_id in seen:
            dupes.add(spec.series_id)
        seen.add(spec.series_id)
    if dupes:
        raise ManifestError(
            f"Duplicate series_id(s) in {source_path or 'manifest'}: {sorted(dupes)}"
        )


def load_manifests(path: str, pattern: str = "*.y*ml") -> list[Manifest]:
    """Load one manifest file or every manifest in a directory.

    Duplicate ``series_id`` values *across* files are also rejected, since the
    pipeline treats the series universe as a single logical set.
    """
    if os.path.isdir(path):
        files = sorted(glob.glob(os.path.join(path, pattern)))
        if not files:
            raise ManifestError(f"No manifest files matching {pattern!r} under {path}")
    elif os.path.isfile(path):
        files = [path]
    else:
        raise ManifestError(f"Manifest path does not exist: {path}")

    manifests = [Manifest.from_yaml(f) for f in files]

    # Cross-file duplicate detection.
    origin: dict[str, str] = {}
    for man in manifests:
        for sid in man.series_ids(active_only=False):
            if sid in origin:
                raise ManifestError(
                    f"series_id {sid!r} defined in both {origin[sid]} and "
                    f"{man.source_path}"
                )
            origin[sid] = man.source_path or man.name
    return manifests


def all_series(manifests: Iterable[Manifest], active_only: bool = True) -> list[SeriesSpec]:
    """Flatten a list of manifests into a single ordered series list."""
    out: list[SeriesSpec] = []
    for man in manifests:
        out.extend(man.active_series if active_only else man.series)
    return out
