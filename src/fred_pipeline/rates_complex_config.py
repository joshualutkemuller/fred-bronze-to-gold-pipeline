"""Config loaders for the Phase-4 rates-complex Gold tables.

Three reviewable YAML files (same pattern as ``config/spreads.yml`` /
``config/series_catalog.yml``), one per surface:

  * ``config/benchmark_rates.yml`` → ``gold.benchmark_rate_board`` (BMRK):
    the rate list, its category buckets, and which benchmark each rate is
    spread against;
  * ``config/funding.yml`` → ``gold.funding_tape_daily`` +
    ``gold.funding_stress_daily`` (FUND): corridor rates and balances, the
    funding spreads, and the weighted components of the 0–100 stress gauge;
  * ``config/credit.yml`` → ``gold.credit_spread_daily`` (CRDT): the ICE BofA
    OAS instruments and the percentile threshold for stress-episode flags.

Missing files load as empty configs (the tables are then simply empty —
nothing has been configured); malformed files raise.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Optional

import yaml

DEFAULT_BENCHMARK_PATH = "config/benchmark_rates.yml"
DEFAULT_FUNDING_PATH = "config/funding.yml"
DEFAULT_CREDIT_PATH = "config/credit.yml"

VALID_METRIC_TYPES = {"rate", "balance"}


class RatesComplexConfigError(ValueError):
    """Raised when a rates-complex config file is malformed."""


def _load_yaml(path: Optional[str], env_var: str, default: str) -> tuple[Optional[dict], str]:
    resolved = path or os.environ.get(env_var) or default
    if not resolved or not os.path.isfile(resolved):
        return None, resolved
    with open(resolved, "r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    if not isinstance(data, dict):
        raise RatesComplexConfigError(f"{resolved} must be a mapping at the top level")
    return data, resolved


def _entries(raw: Any, known: set[str], source: str, kind: str) -> list[dict]:
    if not isinstance(raw, list):
        raise RatesComplexConfigError(
            f"{source} must contain a top-level '{kind}' list"
        )
    out = []
    for item in raw:
        if not isinstance(item, dict):
            raise RatesComplexConfigError(
                f"Each {kind} entry in {source} must be a mapping, got {item!r}"
            )
        unknown = set(item) - known
        if unknown:
            raise RatesComplexConfigError(
                f"{kind} entry {item!r} in {source} has unknown field(s): "
                f"{sorted(unknown)}. Allowed: {sorted(known)}"
            )
        out.append(item)
    return out


# ---- benchmark rate board (BMRK) --------------------------------------------

@dataclass(frozen=True)
class BenchmarkRateDef:
    """One board rate: display label, category bucket, optional benchmark to
    spread against (empty = no spread column for this rate)."""

    series_id: str
    label: str
    category: str
    benchmark: str = ""

    def __post_init__(self) -> None:
        if not self.series_id or not self.label or not self.category:
            raise RatesComplexConfigError(
                f"Benchmark rate definition missing required field(s): {self}"
            )


@dataclass(frozen=True)
class BenchmarkBoardConfig:
    rates: tuple[BenchmarkRateDef, ...] = ()
    # Observations back for the trend slope (latest vs. N obs ago).
    trend_window: int = 5


def load_benchmark_board(path: Optional[str] = None) -> BenchmarkBoardConfig:
    data, resolved = _load_yaml(path, "FRED_BENCHMARK_RATES_FILE", DEFAULT_BENCHMARK_PATH)
    if data is None:
        return BenchmarkBoardConfig()
    rates: list[BenchmarkRateDef] = []
    seen: set[str] = set()
    for item in _entries(
        data.get("rates"), {"series_id", "label", "category", "benchmark"},
        resolved, "rates",
    ):
        rd = BenchmarkRateDef(
            series_id=item.get("series_id", ""),
            label=item.get("label", ""),
            category=item.get("category", ""),
            benchmark=item.get("benchmark", "") or "",
        )
        if rd.series_id in seen:
            raise RatesComplexConfigError(
                f"Duplicate benchmark rate {rd.series_id!r} in {resolved}"
            )
        seen.add(rd.series_id)
        rates.append(rd)
    trend_window = int(data.get("trend_window", 5))
    if trend_window < 1:
        raise RatesComplexConfigError(
            f"trend_window in {resolved} must be >= 1, got {trend_window}"
        )
    return BenchmarkBoardConfig(rates=tuple(rates), trend_window=trend_window)


# ---- funding tape + stress gauge (FUND) --------------------------------------

@dataclass(frozen=True)
class FundingMetricDef:
    """One tape line: a corridor ``rate`` or a balance-sheet ``balance``."""

    name: str
    series_id: str
    metric_type: str

    def __post_init__(self) -> None:
        if not self.name or not self.series_id:
            raise RatesComplexConfigError(
                f"Funding metric missing required field(s): {self}"
            )
        if self.metric_type not in VALID_METRIC_TYPES:
            raise RatesComplexConfigError(
                f"Funding metric {self.name!r} has invalid metric_type "
                f"{self.metric_type!r}; expected one of {sorted(VALID_METRIC_TYPES)}"
            )


@dataclass(frozen=True)
class FundingSpreadDef:
    """One funding spread (long − short, both corridor series ids)."""

    name: str
    long_leg: str
    short_leg: str

    def __post_init__(self) -> None:
        if not self.name or not self.long_leg or not self.short_leg:
            raise RatesComplexConfigError(
                f"Funding spread missing required field(s): {self}"
            )


@dataclass(frozen=True)
class StressComponent:
    """A stress-gauge component: a configured spread name and its weight."""

    spread: str
    weight: float = 1.0

    def __post_init__(self) -> None:
        if not self.spread:
            raise RatesComplexConfigError(f"Stress component missing spread: {self}")
        if self.weight <= 0:
            raise RatesComplexConfigError(
                f"Stress component {self.spread!r} weight must be > 0"
            )


@dataclass(frozen=True)
class FundingConfig:
    metrics: tuple[FundingMetricDef, ...] = ()
    spreads: tuple[FundingSpreadDef, ...] = ()
    stress_components: tuple[StressComponent, ...] = ()


def load_funding_config(path: Optional[str] = None) -> FundingConfig:
    data, resolved = _load_yaml(path, "FRED_FUNDING_FILE", DEFAULT_FUNDING_PATH)
    if data is None:
        return FundingConfig()
    metrics: list[FundingMetricDef] = []
    seen: set[str] = set()
    for item in _entries(
        data.get("metrics"), {"name", "series_id", "metric_type"}, resolved, "metrics"
    ):
        md = FundingMetricDef(
            name=item.get("name", ""),
            series_id=item.get("series_id", ""),
            metric_type=item.get("metric_type", "rate"),
        )
        if md.name in seen:
            raise RatesComplexConfigError(
                f"Duplicate funding metric {md.name!r} in {resolved}"
            )
        seen.add(md.name)
        metrics.append(md)

    spreads: list[FundingSpreadDef] = []
    for item in _entries(
        data.get("spreads") or [], {"name", "long_leg", "short_leg"},
        resolved, "spreads",
    ):
        sd = FundingSpreadDef(
            name=item.get("name", ""),
            long_leg=item.get("long_leg", ""),
            short_leg=item.get("short_leg", ""),
        )
        if sd.name in seen:
            raise RatesComplexConfigError(
                f"Funding spread {sd.name!r} in {resolved} collides with another "
                f"metric/spread name"
            )
        seen.add(sd.name)
        spreads.append(sd)

    spread_names = {s.name for s in spreads}
    components: list[StressComponent] = []
    for item in _entries(
        (data.get("stress") or {}).get("components") or [],
        {"spread", "weight"}, resolved, "stress components",
    ):
        sc = StressComponent(
            spread=item.get("spread", ""),
            weight=float(item.get("weight", 1.0)),
        )
        if sc.spread not in spread_names:
            raise RatesComplexConfigError(
                f"Stress component {sc.spread!r} in {resolved} is not a "
                f"configured funding spread"
            )
        components.append(sc)
    return FundingConfig(
        metrics=tuple(metrics), spreads=tuple(spreads),
        stress_components=tuple(components),
    )


# ---- credit spreads (CRDT) ----------------------------------------------------

@dataclass(frozen=True)
class CreditInstrumentDef:
    """One OAS instrument (ICE BofA index series, values in percent)."""

    instrument: str
    series_id: str
    category: str = ""

    def __post_init__(self) -> None:
        if not self.instrument or not self.series_id:
            raise RatesComplexConfigError(
                f"Credit instrument missing required field(s): {self}"
            )


@dataclass(frozen=True)
class CreditConfig:
    instruments: tuple[CreditInstrumentDef, ...] = ()
    # Expanding-percentile threshold at/above which an observation is flagged
    # a stress episode.
    stress_percentile: float = 0.90


def load_credit_config(path: Optional[str] = None) -> CreditConfig:
    data, resolved = _load_yaml(path, "FRED_CREDIT_FILE", DEFAULT_CREDIT_PATH)
    if data is None:
        return CreditConfig()
    instruments: list[CreditInstrumentDef] = []
    seen: set[str] = set()
    for item in _entries(
        data.get("instruments"), {"instrument", "series_id", "category"},
        resolved, "instruments",
    ):
        cd = CreditInstrumentDef(
            instrument=item.get("instrument", ""),
            series_id=item.get("series_id", ""),
            category=item.get("category", "") or "",
        )
        if cd.instrument in seen:
            raise RatesComplexConfigError(
                f"Duplicate credit instrument {cd.instrument!r} in {resolved}"
            )
        seen.add(cd.instrument)
        instruments.append(cd)
    stress_percentile = float(data.get("stress_percentile", 0.90))
    if not 0.0 < stress_percentile <= 1.0:
        raise RatesComplexConfigError(
            f"stress_percentile in {resolved} must be in (0, 1], "
            f"got {stress_percentile}"
        )
    return CreditConfig(
        instruments=tuple(instruments), stress_percentile=stress_percentile
    )
