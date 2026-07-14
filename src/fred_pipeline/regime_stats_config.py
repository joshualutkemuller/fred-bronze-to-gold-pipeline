"""Config loaders for the Phase-5 regime playbook + statistical lab.

Two reviewable YAML files:

  * ``config/regime.yml`` → ``gold.macro_regime_daily`` (REGIME): the five
    pillars (growth / inflation / liquidity / credit / policy), each a
    weighted blend of direction-adjusted, expanding z-scores of input series;
    per-pillar composite weights; and an **ordered rule table** that names the
    regime (first matching rule wins, else the default).
  * ``config/stats_pairs.yml`` → ``gold.series_correlation`` +
    ``gold.series_lead_lag`` (STAT/EDA): the curated series pairs to
    precompute, each leg's transform, the rolling-correlation windows, the
    lead-lag range, and the Granger lag order.

Missing files load as empty configs (tables stay empty); malformed raise.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Any, Optional

import yaml

DEFAULT_REGIME_PATH = "config/regime.yml"
DEFAULT_STATS_PAIRS_PATH = "config/stats_pairs.yml"

# The five pillar columns of gold.macro_regime_daily — fixed table shape.
REGIME_PILLARS = ("growth", "inflation", "liquidity", "credit", "policy")

# Input transforms: level (as published), diff (first difference), mom
# (period % change), yoy (date-based year-over-year % change).
VALID_TRANSFORMS = {"level", "diff", "mom", "yoy"}

_CONDITION_RE = re.compile(r"^\s*(<=|>=|<|>)\s*(-?\d+(?:\.\d+)?)\s*$")


class RegimeStatsConfigError(ValueError):
    """Raised when a regime/stats config file is malformed."""


def _load_yaml(path: Optional[str], env_var: str, default: str) -> tuple[Optional[dict], str]:
    resolved = path or os.environ.get(env_var) or default
    if not resolved or not os.path.isfile(resolved):
        return None, resolved
    with open(resolved, "r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    if not isinstance(data, dict):
        raise RegimeStatsConfigError(f"{resolved} must be a mapping at the top level")
    return data, resolved


# ---- regime playbook ---------------------------------------------------------

@dataclass(frozen=True)
class RegimeInputDef:
    """One pillar input: a series, its transform, direction, and weight.

    ``direction`` +1 means a higher (transformed) value pushes the pillar
    score up, −1 the opposite — e.g. NFCI enters *liquidity* with −1 (a
    higher NFCI is tighter conditions, i.e. less liquidity).
    """

    series_id: str
    transform: str = "level"
    direction: int = 1
    weight: float = 1.0

    def __post_init__(self) -> None:
        if not self.series_id:
            raise RegimeStatsConfigError(f"Regime input missing series_id: {self}")
        if self.transform not in VALID_TRANSFORMS:
            raise RegimeStatsConfigError(
                f"Regime input {self.series_id!r} has invalid transform "
                f"{self.transform!r}; expected one of {sorted(VALID_TRANSFORMS)}"
            )
        if self.direction not in (-1, 1):
            raise RegimeStatsConfigError(
                f"Regime input {self.series_id!r} direction must be 1 or -1"
            )
        if self.weight <= 0:
            raise RegimeStatsConfigError(
                f"Regime input {self.series_id!r} weight must be > 0"
            )


@dataclass(frozen=True)
class RegimePillarDef:
    name: str
    inputs: tuple[RegimeInputDef, ...]
    # Sign/weight of this pillar in the composite score.
    composite_weight: float = 1.0

    def __post_init__(self) -> None:
        if self.name not in REGIME_PILLARS:
            raise RegimeStatsConfigError(
                f"Pillar {self.name!r} is not one of {REGIME_PILLARS}"
            )
        if not self.inputs:
            raise RegimeStatsConfigError(f"Pillar {self.name!r} has no inputs")


@dataclass(frozen=True)
class RegimeCondition:
    """One rule condition: ``pillar <op> threshold`` (parsed from e.g. '> 0.25')."""

    pillar: str
    op: str
    threshold: float

    def matches(self, score: float) -> bool:
        return {
            "<": score < self.threshold,
            "<=": score <= self.threshold,
            ">": score > self.threshold,
            ">=": score >= self.threshold,
        }[self.op]


@dataclass(frozen=True)
class RegimeRuleDef:
    """A named regime and its conditions (ALL must hold). Rules are ordered;
    the first match wins."""

    name: str
    conditions: tuple[RegimeCondition, ...]

    def __post_init__(self) -> None:
        if not self.name:
            raise RegimeStatsConfigError(f"Regime rule missing name: {self}")
        if not self.conditions:
            raise RegimeStatsConfigError(f"Regime rule {self.name!r} has no conditions")


@dataclass(frozen=True)
class RegimeConfig:
    pillars: tuple[RegimePillarDef, ...] = ()
    rules: tuple[RegimeRuleDef, ...] = ()
    default_regime: str = "Neutral"
    # An input whose latest (transformed) observation is older than this many
    # days as of a given date drops out of its pillar for that date.
    max_staleness_days: int = 200


def _parse_condition(pillar: str, raw: Any, source: str) -> RegimeCondition:
    m = _CONDITION_RE.match(str(raw))
    if not m:
        raise RegimeStatsConfigError(
            f"Condition {raw!r} for pillar {pillar!r} in {source} must look "
            f"like '> 0.25' / '<= -1' (op + number)"
        )
    return RegimeCondition(pillar=pillar, op=m.group(1), threshold=float(m.group(2)))


def load_regime_config(path: Optional[str] = None) -> RegimeConfig:
    data, resolved = _load_yaml(path, "FRED_REGIME_FILE", DEFAULT_REGIME_PATH)
    if data is None:
        return RegimeConfig()

    raw_pillars = data.get("pillars")
    if not isinstance(raw_pillars, dict):
        raise RegimeStatsConfigError(
            f"{resolved} must contain a top-level 'pillars' mapping"
        )
    unknown = set(raw_pillars) - set(REGIME_PILLARS)
    if unknown:
        raise RegimeStatsConfigError(
            f"{resolved} has unknown pillar(s) {sorted(unknown)}; "
            f"allowed: {list(REGIME_PILLARS)}"
        )
    missing = set(REGIME_PILLARS) - set(raw_pillars)
    if missing:
        raise RegimeStatsConfigError(
            f"{resolved} is missing pillar(s) {sorted(missing)} — the regime "
            f"table has a fixed column per pillar"
        )

    pillars: list[RegimePillarDef] = []
    known_input = {"series_id", "transform", "direction", "weight"}
    for name in REGIME_PILLARS:
        spec = raw_pillars[name] or {}
        if not isinstance(spec, dict):
            raise RegimeStatsConfigError(
                f"Pillar {name!r} in {resolved} must be a mapping"
            )
        inputs: list[RegimeInputDef] = []
        for item in spec.get("inputs") or []:
            if not isinstance(item, dict):
                raise RegimeStatsConfigError(
                    f"Pillar {name!r} input in {resolved} must be a mapping"
                )
            bad = set(item) - known_input
            if bad:
                raise RegimeStatsConfigError(
                    f"Pillar {name!r} input {item.get('series_id')!r} in "
                    f"{resolved} has unknown field(s): {sorted(bad)}"
                )
            inputs.append(RegimeInputDef(
                series_id=item.get("series_id", ""),
                transform=item.get("transform", "level"),
                direction=int(item.get("direction", 1)),
                weight=float(item.get("weight", 1.0)),
            ))
        pillars.append(RegimePillarDef(
            name=name,
            inputs=tuple(inputs),
            composite_weight=float(spec.get("composite_weight", 1.0)),
        ))

    rules: list[RegimeRuleDef] = []
    for entry in data.get("rules") or []:
        if not isinstance(entry, dict) or "name" not in entry or "when" not in entry:
            raise RegimeStatsConfigError(
                f"Each rule in {resolved} needs 'name' and 'when', got {entry!r}"
            )
        when = entry["when"]
        if not isinstance(when, dict) or not when:
            raise RegimeStatsConfigError(
                f"Rule {entry['name']!r} 'when' in {resolved} must be a "
                f"non-empty mapping of pillar -> condition"
            )
        bad = set(when) - set(REGIME_PILLARS)
        if bad:
            raise RegimeStatsConfigError(
                f"Rule {entry['name']!r} in {resolved} references unknown "
                f"pillar(s): {sorted(bad)}"
            )
        rules.append(RegimeRuleDef(
            name=str(entry["name"]),
            conditions=tuple(
                _parse_condition(p, c, resolved) for p, c in when.items()
            ),
        ))

    max_staleness = int(data.get("max_staleness_days", 200))
    if max_staleness < 1:
        raise RegimeStatsConfigError(
            f"max_staleness_days in {resolved} must be >= 1"
        )
    return RegimeConfig(
        pillars=tuple(pillars),
        rules=tuple(rules),
        default_regime=str(data.get("default_regime", "Neutral")),
        max_staleness_days=max_staleness,
    )


# ---- correlation / lead-lag pairs ---------------------------------------------

@dataclass(frozen=True)
class StatsPairDef:
    """One precomputed pair. Transforms default to ``diff`` — correlations on
    the *levels* of trending macro series are spurious more often than not."""

    series_a: str
    series_b: str
    transform_a: str = "diff"
    transform_b: str = "diff"

    def __post_init__(self) -> None:
        if not self.series_a or not self.series_b:
            raise RegimeStatsConfigError(f"Stats pair missing series id(s): {self}")
        if self.series_a == self.series_b:
            raise RegimeStatsConfigError(
                f"Stats pair {self.series_a!r} correlates a series with itself"
            )
        for t in (self.transform_a, self.transform_b):
            if t not in VALID_TRANSFORMS:
                raise RegimeStatsConfigError(
                    f"Stats pair {self.series_a!r}/{self.series_b!r} has "
                    f"invalid transform {t!r}; expected one of "
                    f"{sorted(VALID_TRANSFORMS)}"
                )


@dataclass(frozen=True)
class StatsConfig:
    pairs: tuple[StatsPairDef, ...] = ()
    # Rolling-correlation windows in observations; 0 = expanding (full sample
    # up to each date).
    windows: tuple[int, ...] = (63, 252, 0)
    # Lead-lag range: cross-correlation at lags -max_lag..+max_lag.
    max_lag: int = 12
    # Granger F-test lag order.
    granger_lags: int = 4


def load_stats_config(path: Optional[str] = None) -> StatsConfig:
    data, resolved = _load_yaml(
        path, "FRED_STATS_PAIRS_FILE", DEFAULT_STATS_PAIRS_PATH
    )
    if data is None:
        return StatsConfig(pairs=())

    pairs: list[StatsPairDef] = []
    seen: set[tuple[str, str]] = set()
    known = {"series_a", "series_b", "transform_a", "transform_b"}
    raw_pairs = data.get("pairs")
    if not isinstance(raw_pairs, list):
        raise RegimeStatsConfigError(
            f"{resolved} must contain a top-level 'pairs' list"
        )
    for item in raw_pairs:
        if not isinstance(item, dict):
            raise RegimeStatsConfigError(
                f"Each pair in {resolved} must be a mapping, got {item!r}"
            )
        bad = set(item) - known
        if bad:
            raise RegimeStatsConfigError(
                f"Pair {item!r} in {resolved} has unknown field(s): {sorted(bad)}"
            )
        pd = StatsPairDef(
            series_a=item.get("series_a", ""),
            series_b=item.get("series_b", ""),
            transform_a=item.get("transform_a", "diff"),
            transform_b=item.get("transform_b", "diff"),
        )
        key = (pd.series_a, pd.series_b)
        if key in seen or (pd.series_b, pd.series_a) in seen:
            raise RegimeStatsConfigError(
                f"Duplicate stats pair {key!r} in {resolved}"
            )
        seen.add(key)
        pairs.append(pd)

    windows = tuple(int(w) for w in data.get("windows", [63, 252, 0]))
    for w in windows:
        if w < 0 or (0 < w < 3):
            raise RegimeStatsConfigError(
                f"Correlation window {w} in {resolved} must be 0 (expanding) "
                f"or >= 3"
            )
    max_lag = int(data.get("max_lag", 12))
    granger_lags = int(data.get("granger_lags", 4))
    if max_lag < 1 or granger_lags < 1:
        raise RegimeStatsConfigError(
            f"max_lag/granger_lags in {resolved} must be >= 1"
        )
    return StatsConfig(
        pairs=tuple(pairs), windows=windows,
        max_lag=max_lag, granger_lags=granger_lags,
    )
