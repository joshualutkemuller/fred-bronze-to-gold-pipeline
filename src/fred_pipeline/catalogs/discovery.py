"""API-driven manifest discovery.

Turn a FRED *category*, *release*, or *search* into a validated manifest so the
series universe can scale without hand-listing hundreds of ids. The network call
lives on :class:`fred_pipeline.fred_client.FredClient`; everything here is pure
(metadata dict in, spec/manifest out) and therefore unit-testable.

The output is deliberately run through the same :class:`SeriesSpec` validation
the pipeline uses, so a generated manifest is guaranteed loadable.
"""

from __future__ import annotations

from typing import Any, Iterable, Optional

import yaml

from fred_pipeline.manifest import (
    VALID_FREQUENCIES,
    Manifest,
    ManifestError,
    SeriesSpec,
)

# FRED marks retired series with this token in the title.
DISCONTINUED_MARKER = "DISCONTINUED"


def priority_from_popularity(popularity: Any) -> int:
    """Bucket FRED's 0–100 popularity into our 1 (highest) … 5 priority scale."""
    try:
        pop = float(popularity)
    except (TypeError, ValueError):
        return 3
    if pop >= 80:
        return 1
    if pop >= 60:
        return 2
    if pop >= 40:
        return 3
    if pop >= 20:
        return 4
    return 5


def _frequency_code(meta: dict[str, Any]) -> Optional[str]:
    """Return a manifest-valid frequency code from FRED metadata, or None."""
    code = (meta.get("frequency_short") or "").strip().lower()
    if code in VALID_FREQUENCIES:
        return code
    freq = (meta.get("frequency") or "").strip().lower()
    return freq if freq in VALID_FREQUENCIES else None


def series_meta_to_spec_dict(
    meta: dict[str, Any],
    *,
    category: str,
    tags: Optional[list[str]] = None,
    downstream_use_case: str = "",
) -> dict[str, Any]:
    """Map a single FRED series metadata dict to a manifest spec dict."""
    spec = {
        "series_id": meta.get("id"),
        "title": (meta.get("title") or "").strip(),
        "frequency": _frequency_code(meta),
        "category": category,
        "units": (meta.get("units") or "").strip(),
        "priority": priority_from_popularity(meta.get("popularity")),
        "downstream_use_case": downstream_use_case,
        "tags": tags if tags is not None else [category],
    }
    return spec


def discover_specs(
    metas: Iterable[dict[str, Any]],
    *,
    category: str,
    frequencies: Optional[Iterable[str]] = None,
    exclude_discontinued: bool = True,
    min_popularity: float = 0.0,
    exclude_ids: Optional[Iterable[str]] = None,
    downstream_use_case: str = "",
) -> tuple[list[SeriesSpec], list[dict[str, Any]]]:
    """Filter + validate raw FRED metadata into SeriesSpec objects.

    Returns ``(specs, skipped)`` where ``skipped`` is a list of
    ``{"series_id", "reason"}`` records for transparency (bad frequency,
    discontinued, below popularity floor, already-existing, or failed
    validation). Never raises on a single bad series — the whole point is to
    survive the long tail of the FRED catalog.
    """
    freq_filter = {f.strip().lower() for f in frequencies} if frequencies else None
    exclude = {s for s in (exclude_ids or [])}
    specs: list[SeriesSpec] = []
    skipped: list[dict[str, Any]] = []
    seen: set[str] = set()

    for meta in metas:
        sid = meta.get("id")
        title = meta.get("title") or ""
        if not sid:
            skipped.append({"series_id": None, "reason": "missing id"})
            continue
        if sid in exclude:
            skipped.append({"series_id": sid, "reason": "already in existing manifest"})
            continue
        if sid in seen:
            skipped.append({"series_id": sid, "reason": "duplicate in result set"})
            continue
        if exclude_discontinued and DISCONTINUED_MARKER in title.upper():
            skipped.append({"series_id": sid, "reason": "discontinued"})
            continue
        try:
            if float(meta.get("popularity", 0) or 0) < min_popularity:
                skipped.append({"series_id": sid, "reason": "below min_popularity"})
                continue
        except (TypeError, ValueError):
            pass

        spec_dict = series_meta_to_spec_dict(
            meta, category=category, downstream_use_case=downstream_use_case
        )
        if spec_dict["frequency"] is None:
            skipped.append(
                {"series_id": sid, "reason": f"unsupported frequency "
                 f"{meta.get('frequency_short') or meta.get('frequency')!r}"}
            )
            continue
        if freq_filter and spec_dict["frequency"] not in freq_filter:
            skipped.append({"series_id": sid, "reason": "frequency filtered out"})
            continue
        try:
            specs.append(SeriesSpec(**spec_dict))
            seen.add(sid)
        except ManifestError as exc:
            skipped.append({"series_id": sid, "reason": f"validation: {exc}"})

    return specs, skipped


def build_manifest_dict(
    name: str,
    specs: Iterable[SeriesSpec],
    *,
    description: str = "",
    version: int = 1,
) -> dict[str, Any]:
    """Assemble a manifest dict (the same shape ``Manifest.from_dict`` expects)."""
    return {
        "name": name,
        "description": description,
        "version": version,
        "series": [s.to_dict() for s in specs],
    }


def manifest_to_yaml(manifest_dict: dict[str, Any]) -> str:
    """Serialize a manifest dict to YAML, validating it round-trips first."""
    # Fail fast if we somehow produced an invalid manifest.
    Manifest.from_dict(manifest_dict)
    return yaml.safe_dump(manifest_dict, sort_keys=False, default_flow_style=False)
