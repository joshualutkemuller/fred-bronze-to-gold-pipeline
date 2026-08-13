"""Cross-check: every series id referenced by a Gold-feature config must be
declared in a manifest.

The Gold configs (``config/funding.yml``, ``curve.yml``, ``spreads.yml``, …)
name series by id, and the engines match those ids against ingested rows. When
a config names an id that no manifest declares, nothing errors — the engine
simply finds no rows and the affected feature silently emits nothing.

That failure mode shipped once already: ``config/funding.yml`` referenced
``TGCR`` while ``manifests/fed_funding.yml`` declared FRED's actual id,
``TGCRRATE``. The ``SOFR_TGCR`` spread never computed, and because
``gold.funding_stress_daily`` only emits on dates where *every* component
spread has a value, the entire 0–100 funding stress gauge produced zero rows.
Nothing failed; the table was just empty.

These tests fail on an id no manifest knows about — the typo/mismatch case.
They deliberately tolerate ids declared but inactive: shipping a series
inactive is a coverage decision (it emits no rows, by design), whereas naming
an id that exists nowhere is always a defect.
"""

from __future__ import annotations

import glob
import os

import pytest
import yaml

CONFIG_DIR = "config"
MANIFEST_GLOB = "manifests/*.yml"


def _declared_series() -> dict[str, bool]:
    """Every series id any manifest declares -> whether it is active."""
    declared: dict[str, bool] = {}
    for path in glob.glob(MANIFEST_GLOB):
        with open(path, encoding="utf-8") as fh:
            doc = yaml.safe_load(fh) or {}
        if not isinstance(doc, dict):
            continue
        for spec in doc.get("series") or []:
            if isinstance(spec, dict) and spec.get("series_id"):
                declared[spec["series_id"]] = bool(spec.get("active", True))
    return declared


def _load(name: str):
    path = os.path.join(CONFIG_DIR, name)
    if not os.path.isfile(path):
        pytest.skip(f"{name} not present")
    with open(path, encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def _referenced() -> dict[str, list[str]]:
    """series_id -> the config surfaces that reference it."""
    refs: dict[str, list[str]] = {}

    def add(series_id, where: str) -> None:
        if isinstance(series_id, str) and series_id:
            refs.setdefault(series_id, []).append(where)

    for tenor in _load("curve.yml").get("tenors") or []:
        add(tenor.get("series_id"), "curve.yml tenors")

    for spread in _load("spreads.yml").get("spreads") or []:
        add(spread.get("long_leg"), "spreads.yml long_leg")
        add(spread.get("short_leg"), "spreads.yml short_leg")

    funding = _load("funding.yml")
    for metric in funding.get("metrics") or []:
        add(metric.get("series_id"), "funding.yml metrics")
    for spread in funding.get("spreads") or []:
        # legs are series ids, not the `name` labels on metrics
        add(spread.get("long_leg"), "funding.yml spread long_leg")
        add(spread.get("short_leg"), "funding.yml spread short_leg")

    for rate in _load("benchmark_rates.yml").get("rates") or []:
        add(rate.get("series_id"), "benchmark_rates.yml series_id")
        add(rate.get("benchmark"), "benchmark_rates.yml benchmark")

    for inst in _load("credit.yml").get("instruments") or []:
        add(inst.get("series_id"), "credit.yml instruments")

    for pillar, conf in (_load("regime.yml").get("pillars") or {}).items():
        for item in conf.get("inputs") or []:
            add(item.get("series_id"), f"regime.yml pillar {pillar}")

    for pair in _load("stats_pairs.yml").get("pairs") or []:
        add(pair.get("series_a"), "stats_pairs.yml series_a")
        add(pair.get("series_b"), "stats_pairs.yml series_b")

    for feature in _load("ml_features.yml").get("features") or []:
        add(feature.get("series_id"), "ml_features.yml features")

    for item in _load("inflation_items.yml").get("items") or []:
        add(item.get("series_id"), "inflation_items.yml items")

    fomc = _load("fomc.yml")
    add(fomc.get("target_low_series"), "fomc.yml target_low_series")
    add(fomc.get("target_high_series"), "fomc.yml target_high_series")
    add(fomc.get("effective_rate_series"), "fomc.yml effective_rate_series")
    for tenor in fomc.get("tenors") or []:
        add(tenor.get("series_id"), "fomc.yml tenors")

    for feature in _load("cross_series.yml").get("features") or []:
        for leg in feature.get("legs") or []:
            # a leg is either a bare series id or {series_id, weight}
            add(leg if isinstance(leg, str) else leg.get("series_id"),
                "cross_series.yml legs")

    for rec in _load("reconciliations.yml").get("reconciliations") or []:
        add(rec.get("series_a"), "reconciliations.yml series_a")
        add(rec.get("series_b"), "reconciliations.yml series_b")

    for key, value in _load("global_series.yml").items():
        if isinstance(value, list):
            for country in value:
                if isinstance(country, dict):
                    add(country.get("series_id"), f"global_series.yml {key}")

    for release in _load("release_calendar.yml").get("releases") or []:
        add(release.get("representative_series_id"),
            "release_calendar.yml representative_series_id")

    for series_id in _load("inflation_forecast.yml").get("series") or []:
        add(series_id, "inflation_forecast.yml series")

    add((_load("recession_model.yml").get("features") or {}).get("hy_oas_instrument"),
        "recession_model.yml hy_oas_instrument")

    return refs


def test_every_config_series_is_declared_in_a_manifest():
    """An id no manifest declares is a typo or a stale rename -- the feature
    that references it can never emit a row."""
    declared = _declared_series()
    unknown = {
        series_id: sorted(set(where))
        for series_id, where in _referenced().items()
        if series_id not in declared
    }
    assert unknown == {}, (
        "Gold config(s) reference series ids that no manifest declares; the "
        "features that use them will silently emit nothing:\n"
        + "\n".join(f"  {sid}: referenced by {w}" for sid, w in sorted(unknown.items()))
    )


def test_funding_stress_gauge_components_are_resolvable():
    """gold.funding_stress_daily emits a row only on dates where EVERY
    component spread has a value, so one unresolvable leg empties the whole
    0-100 gauge. Guard the components specifically -- this is the exact
    regression that shipped with the TGCR/TGCRRATE mismatch."""
    funding = _load("funding.yml")
    declared = _declared_series()
    spreads = {s["name"]: s for s in funding.get("spreads") or []}

    problems: list[str] = []
    for component in (funding.get("stress") or {}).get("components") or []:
        name = component.get("spread")
        spread = spreads.get(name)
        if spread is None:
            problems.append(f"{name}: no such spread in funding.yml spreads")
            continue
        for role in ("long_leg", "short_leg"):
            leg = spread.get(role)
            if leg not in declared:
                problems.append(f"{name}.{role}={leg}: not declared in any manifest")
            elif not declared[leg]:
                problems.append(f"{name}.{role}={leg}: declared but inactive")

    assert problems == [], (
        "funding stress gauge components are not fully resolvable, so "
        "gold.funding_stress_daily will emit no rows:\n  "
        + "\n  ".join(problems)
    )
