from fred_pipeline.manifest import all_series, load_manifests
from fred_pipeline.meta import build_meta_rows


def test_build_meta_rows_from_shipped_manifests():
    manifests = load_manifests("manifests")
    rows = build_meta_rows(manifests)
    n_series = len(all_series(manifests, active_only=False))

    assert set(rows) == {"fred_series", "fred_manifest", "fred_series_manifest_map"}
    assert len(rows["fred_manifest"]) == len(manifests)
    assert len(rows["fred_series"]) == n_series
    assert len(rows["fred_series_manifest_map"]) == n_series

    # series rows carry serialized enums + updated_at stamp
    sample = rows["fred_series"][0]
    assert isinstance(sample["load_type"], str)
    assert isinstance(sample["validation_profile"], str)
    assert "updated_at" in sample

    # every mapping points at a real manifest
    manifest_names = {m["manifest_name"] for m in rows["fred_manifest"]}
    for m in rows["fred_series_manifest_map"]:
        assert m["manifest_name"] in manifest_names
