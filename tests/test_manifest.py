import pytest

from fred_pipeline.manifest import (
    LoadType,
    Manifest,
    ManifestError,
    SeriesSpec,
    ValidationProfile,
    all_series,
    load_manifests,
)

MANIFESTS_DIR = "manifests"


def test_series_spec_minimal_defaults():
    spec = SeriesSpec(series_id="DGS10", title="10Y", frequency="d")
    assert spec.active is True
    assert spec.load_type is LoadType.INCREMENTAL
    assert spec.validation_profile is ValidationProfile.STANDARD
    assert spec.priority == 3
    # Revision-sensitive by default (point-in-time safe).
    assert spec.vintage_enabled is True


def test_series_spec_normalizes_frequency_and_enums():
    spec = SeriesSpec(
        series_id="GDP", title="GDP", frequency="Q",
        load_type="full", validation_profile="strict",
    )
    assert spec.frequency == "q"
    assert spec.load_type is LoadType.FULL
    assert spec.validation_profile is ValidationProfile.STRICT


@pytest.mark.parametrize(
    "kwargs, message",
    [
        ({"series_id": "", "title": "x", "frequency": "d"}, "series_id"),
        ({"series_id": "X", "title": "", "frequency": "d"}, "title"),
        ({"series_id": "X", "title": "x", "frequency": "zzz"}, "frequency"),
        ({"series_id": "X", "title": "x", "frequency": "d", "priority": 9}, "priority"),
        ({"series_id": "X", "title": "x", "frequency": "d", "restate_records": 0},
         "restate_records"),
    ],
)
def test_series_spec_invalid(kwargs, message):
    with pytest.raises(ManifestError) as exc:
        SeriesSpec(**kwargs)
    assert message in str(exc.value)


def test_manifest_from_dict_rejects_unknown_field():
    data = {"name": "m", "series": [
        {"series_id": "X", "title": "x", "frequency": "d", "bogus": 1}
    ]}
    with pytest.raises(ManifestError) as exc:
        Manifest.from_dict(data)
    assert "unknown field" in str(exc.value)


def test_manifest_missing_required_field():
    data = {"name": "m", "series": [{"series_id": "X", "frequency": "d"}]}
    with pytest.raises(ManifestError) as exc:
        Manifest.from_dict(data)
    assert "title" in str(exc.value)


def test_manifest_rejects_intra_file_duplicates():
    data = {"name": "m", "series": [
        {"series_id": "X", "title": "a", "frequency": "d"},
        {"series_id": "X", "title": "b", "frequency": "d"},
    ]}
    with pytest.raises(ManifestError) as exc:
        Manifest.from_dict(data)
    assert "Duplicate" in str(exc.value)


def test_active_series_filtering():
    data = {"name": "m", "series": [
        {"series_id": "A", "title": "a", "frequency": "d"},
        {"series_id": "B", "title": "b", "frequency": "d", "active": False},
    ]}
    man = Manifest.from_dict(data)
    assert man.series_ids() == ["A"]
    assert man.series_ids(active_only=False) == ["A", "B"]


def test_load_shipped_manifests_are_valid():
    manifests = load_manifests(MANIFESTS_DIR)
    assert len(manifests) >= 4  # at least: rates, inflation, labor, growth
    specs = all_series(manifests, active_only=False)
    ids = [s.series_id for s in specs]
    # spot-check a few from each category
    for sid in ("DGS10", "CPIAUCSL", "UNRATE", "GDP"):
        assert sid in ids
    # no cross-file duplicates
    assert len(ids) == len(set(ids))


def test_cross_file_duplicate_detection(tmp_path):
    (tmp_path / "a.yml").write_text(
        "name: a\nseries:\n  - {series_id: X, title: x, frequency: d}\n"
    )
    (tmp_path / "b.yml").write_text(
        "name: b\nseries:\n  - {series_id: X, title: x2, frequency: d}\n"
    )
    with pytest.raises(ManifestError) as exc:
        load_manifests(str(tmp_path))
    assert "both" in str(exc.value)
