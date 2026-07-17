"""SEC statement standardization + derived ratios (Gold layer).

The SEC source lands one XBRL concept per series in Silver, keyed by
``series_id = CIK##########:taxonomy/tag:unit``. Different companies report the
same economic line item under different tags, so this layer:

  1. **standardizes** raw tags into canonical concepts via a priority-ordered
     mapping (``config/sec_concepts.yml``) → ``gold.fred_company_fundamentals``;
  2. computes **derived ratios** from those concepts (``config/sec_ratios.yml``)
     → ``gold.fred_company_ratios``.

Both are pure-Python and reused by the local and Spark backends. Restatement
analytics come free from ``gold.fred_revision_stats`` (SEC vintages already flow
through it). Standardization is done in Python because priority-coalescing across
candidate tags is awkward in SQL.

Scope note: balance-sheet (instant) concepts are clean. Ratios mixing an income
(duration) concept with an instant one (ROE, margins) inherit the unresolved
quarterly-vs-YTD duration ambiguity — flagged in the config.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Iterable, Optional

import yaml

DEFAULT_CONCEPTS_PATH = "config/sec_concepts.yml"
DEFAULT_RATIOS_PATH = "config/sec_ratios.yml"


class SECStandardizationConfigError(ValueError):
    """Raised when a SEC standardization config file is malformed."""


@dataclass(frozen=True)
class ConceptDef:
    """A canonical line item and its candidate XBRL tags (priority order)."""

    name: str
    tags: tuple[str, ...]
    statement: str = ""

    def __post_init__(self) -> None:
        if not self.name or not self.tags:
            raise SECStandardizationConfigError(
                f"Concept missing name/tags: {self}"
            )


@dataclass(frozen=True)
class RatioDef:
    """``numerator`` concept / ``denominator`` concept."""

    name: str
    numerator: str
    denominator: str
    description: str = ""

    def __post_init__(self) -> None:
        if not (self.name and self.numerator and self.denominator):
            raise SECStandardizationConfigError(
                f"Ratio missing required field(s): {self}"
            )


def _load_yaml(path: Optional[str], env: str, default: str, key: str) -> Any:
    resolved = path or os.environ.get(env) or default
    if not resolved or not os.path.isfile(resolved):
        return []
    with open(resolved, "r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    if not isinstance(data, dict):
        raise SECStandardizationConfigError(f"{resolved} must be a mapping")
    return data.get(key) or []


def load_concept_defs(path: Optional[str] = None) -> list[ConceptDef]:
    """Load canonical concepts from ``config/sec_concepts.yml`` (missing → none)."""
    raw = _load_yaml(path, "FRED_SEC_CONCEPTS_FILE", DEFAULT_CONCEPTS_PATH, "concepts")
    defs, seen = [], set()
    for item in raw:
        if not isinstance(item, dict):
            raise SECStandardizationConfigError(f"Concept entry must be a mapping: {item!r}")
        name = item.get("name", "")
        tags = item.get("tags") or []
        if not isinstance(tags, list) or not tags:
            raise SECStandardizationConfigError(f"Concept {name!r} needs a non-empty 'tags' list")
        cd = ConceptDef(name=name, tags=tuple(str(t) for t in tags),
                        statement=item.get("statement", ""))
        if cd.name in seen:
            raise SECStandardizationConfigError(f"Duplicate concept {cd.name!r}")
        seen.add(cd.name)
        defs.append(cd)
    return defs


def load_ratio_defs(path: Optional[str] = None) -> list[RatioDef]:
    """Load derived ratios from ``config/sec_ratios.yml`` (missing → none)."""
    raw = _load_yaml(path, "FRED_SEC_RATIOS_FILE", DEFAULT_RATIOS_PATH, "ratios")
    defs, seen = [], set()
    for item in raw:
        if not isinstance(item, dict):
            raise SECStandardizationConfigError(f"Ratio entry must be a mapping: {item!r}")
        rd = RatioDef(name=item.get("name", ""), numerator=item.get("numerator", ""),
                      denominator=item.get("denominator", ""),
                      description=item.get("description", ""))
        if rd.name in seen:
            raise SECStandardizationConfigError(f"Duplicate ratio {rd.name!r}")
        seen.add(rd.name)
        defs.append(rd)
    return defs


def _tag_priority(concept_defs: Iterable[ConceptDef]) -> dict[str, tuple[str, int, str]]:
    """Map each candidate tag -> (concept_name, priority, statement).

    Lower priority number wins. If a tag appears under multiple concepts, the
    first concept's mapping is kept (config order)."""
    out: dict[str, tuple[str, int, str]] = {}
    for cd in concept_defs:
        for i, tag in enumerate(cd.tags):
            out.setdefault(tag, (cd.name, i, cd.statement))
    return out


def standardize_sec_statements(
    silver_rows: Iterable[dict[str, Any]],
    concept_defs: Optional[Iterable[ConceptDef]] = None,
) -> list[dict[str, Any]]:
    """Map raw SEC Silver rows into canonical ``(cik, concept, date, value)`` rows.

    Only ``source == 'sec'`` rows are considered. For each
    ``(cik, concept, observation_date)`` the value is taken from the
    highest-priority candidate tag the company reports, and (within a tag) the
    latest-filed vintage (``realtime_start``) — i.e. the latest-revised value.
    """
    from fred_pipeline.sources.sec import _parse_series_id

    concept_defs = list(concept_defs) if concept_defs is not None else load_concept_defs()
    tag_map = _tag_priority(concept_defs)

    # (cik, concept, date) -> best (priority, realtime_start, value, statement)
    best: dict[tuple[str, str, str], tuple[int, str, float, str]] = {}
    for r in silver_rows:
        if r.get("source") != "sec" or r.get("is_missing"):
            continue
        value = r.get("value")
        obs_date = r.get("observation_date")
        if value is None or not obs_date:
            continue
        try:
            cik, _tax, tag, _unit = _parse_series_id(str(r.get("series_id", "")))
        except Exception:
            continue
        mapped = tag_map.get(tag)
        if mapped is None:
            continue
        concept, priority, statement = mapped
        rt = str(r.get("realtime_start") or "")
        key = (cik, concept, str(obs_date)[:10])
        cur = best.get(key)
        # prefer the higher-priority tag (lower number); within a tag, the
        # later filing (latest-revised value).
        if cur is None or priority < cur[0] or (priority == cur[0] and rt > cur[1]):
            best[key] = (priority, rt, float(value), statement)

    out = [
        {"cik": cik, "concept": concept, "statement": statement,
         "observation_date": date, "value": value}
        for (cik, concept, date), (_p, _rt, value, statement) in best.items()
    ]
    return sorted(out, key=lambda r: (r["cik"], r["concept"], r["observation_date"]))


def compute_sec_ratios(
    standardized_rows: Iterable[dict[str, Any]],
    ratio_defs: Optional[Iterable[RatioDef]] = None,
) -> list[dict[str, Any]]:
    """Compute ``gold.fred_company_ratios`` from standardized concept rows.

    Each ratio is ``numerator / denominator`` per ``(cik, observation_date)``
    where both concepts exist and the denominator is nonzero.
    """
    ratio_defs = list(ratio_defs) if ratio_defs is not None else load_ratio_defs()

    by_cik_date: dict[tuple[str, str], dict[str, float]] = {}
    for r in standardized_rows:
        by_cik_date.setdefault((r["cik"], r["observation_date"]), {})[r["concept"]] = r["value"]

    out: list[dict[str, Any]] = []
    for (cik, date), concepts in by_cik_date.items():
        for rd in ratio_defs:
            num = concepts.get(rd.numerator)
            den = concepts.get(rd.denominator)
            if num is None or den in (None, 0):
                continue
            out.append({"cik": cik, "ratio_name": rd.name,
                        "observation_date": date, "value": num / den})
    return sorted(out, key=lambda r: (r["ratio_name"], r["cik"], r["observation_date"]))
