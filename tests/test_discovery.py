import pytest
import yaml

from fred_pipeline.discovery import (
    build_manifest_dict,
    discover_specs,
    manifest_to_yaml,
    priority_from_popularity,
    series_meta_to_spec_dict,
)
from fred_pipeline.manifest import Manifest


def _meta(id, title="A title", freq="D", pop=50, units="Percent"):
    return {
        "id": id,
        "title": title,
        "frequency_short": freq,
        "frequency": {"D": "Daily", "M": "Monthly", "Q": "Quarterly"}.get(freq, ""),
        "units": units,
        "popularity": pop,
    }


@pytest.mark.parametrize(
    "pop, expected", [(95, 1), (70, 2), (50, 3), (25, 4), (5, 5), (None, 3), ("x", 3)]
)
def test_priority_from_popularity(pop, expected):
    assert priority_from_popularity(pop) == expected


def test_series_meta_to_spec_dict_maps_fields():
    spec = series_meta_to_spec_dict(_meta("DGS10", pop=85), category="rates")
    assert spec["series_id"] == "DGS10"
    assert spec["frequency"] == "d"          # lowercased short code
    assert spec["category"] == "rates"
    assert spec["priority"] == 1             # popularity 85 -> priority 1
    assert spec["tags"] == ["rates"]


def test_discover_specs_happy_path_returns_valid_specs():
    metas = [_meta("A", pop=90), _meta("B", freq="M", pop=40)]
    specs, skipped = discover_specs(metas, category="rates")
    assert [s.series_id for s in specs] == ["A", "B"]
    assert skipped == []
    # they are real, validated SeriesSpec objects
    assert specs[0].frequency == "d"
    assert specs[1].frequency == "m"
    # discovered series inherit the safe, revision-sensitive default
    assert all(s.vintage_enabled for s in specs)


def test_discover_specs_excludes_discontinued():
    metas = [_meta("A"), _meta("B", title="Foo (DISCONTINUED)")]
    specs, skipped = discover_specs(metas, category="c")
    assert [s.series_id for s in specs] == ["A"]
    assert skipped == [{"series_id": "B", "reason": "discontinued"}]


def test_discover_specs_min_popularity_and_freq_filter():
    metas = [_meta("A", freq="D", pop=10), _meta("B", freq="M", pop=90),
             _meta("C", freq="D", pop=90)]
    specs, skipped = discover_specs(
        metas, category="c", frequencies=["d"], min_popularity=20
    )
    ids = [s.series_id for s in specs]
    assert ids == ["C"]  # A dropped (low pop), B dropped (freq), C kept
    reasons = {s["series_id"]: s["reason"] for s in skipped}
    assert reasons["A"] == "below min_popularity"
    assert reasons["B"] == "frequency filtered out"


def test_discover_specs_excludes_existing_and_dupes():
    metas = [_meta("A"), _meta("A"), _meta("EXIST")]
    specs, skipped = discover_specs(metas, category="c", exclude_ids={"EXIST"})
    assert [s.series_id for s in specs] == ["A"]
    reasons = [s["reason"] for s in skipped]
    assert "duplicate in result set" in reasons
    assert "already in existing manifest" in reasons


def test_discover_specs_skips_unsupported_frequency():
    metas = [_meta("A", freq="ZZ")]  # unknown frequency
    specs, skipped = discover_specs(metas, category="c")
    assert specs == []
    assert "unsupported frequency" in skipped[0]["reason"]


def test_generated_manifest_is_loadable():
    metas = [_meta("A", pop=90), _meta("B", freq="Q", pop=30)]
    specs, _ = discover_specs(metas, category="growth")
    manifest = build_manifest_dict("growth_extra", specs, description="disc")
    text = manifest_to_yaml(manifest)
    # round-trips through YAML and the real manifest validator
    reloaded = Manifest.from_dict(yaml.safe_load(text))
    assert reloaded.name == "growth_extra"
    assert reloaded.series_ids() == ["A", "B"]
