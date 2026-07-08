from fred_pipeline.config import Environment, PipelineConfig
from fred_pipeline.features import (
    compute_curve_spreads,
    compute_feature_transforms,
    point_in_time_snapshot,
)
from fred_pipeline.local_store import LocalWarehouse


def _monthly(series_id, start_year=2023):
    # 13 month-start points: value 100..112
    rows = []
    for i in range(13):
        month = (i % 12) + 1
        year = start_year + (i // 12)
        rows.append({
            "series_id": series_id,
            "observation_date": f"{year}-{month:02d}-01",
            "value": 100.0 + i,
            "is_missing": False,
        })
    return rows


def test_feature_transforms_mom_diff_yoy():
    out = compute_feature_transforms(_monthly("X"))
    by_date = {r["observation_date"]: r for r in out}

    # second point: 101 vs 100
    assert by_date["2023-02-01"]["diff"] == 1.0
    assert abs(by_date["2023-02-01"]["mom"] - 0.01) < 1e-9
    assert by_date["2023-01-01"]["mom"] is None   # no prior

    # YoY at 2024-01-01 (112) vs 2023-01-01 (100) = 0.12
    assert abs(by_date["2024-01-01"]["yoy"] - 0.12) < 1e-9
    # z-score is populated
    assert by_date["2024-01-01"]["zscore"] is not None


def test_curve_spreads():
    rows = [
        {"series_id": "DGS10", "observation_date": "2024-01-01", "value": 4.0,
         "is_missing": False},
        {"series_id": "DGS2", "observation_date": "2024-01-01", "value": 3.0,
         "is_missing": False},
        {"series_id": "DGS2", "observation_date": "2024-01-02", "value": 3.1,
         "is_missing": False},  # no DGS10 on this date -> no spread
    ]
    out = compute_curve_spreads(rows)
    t10y2y = [r for r in out if r["spread_name"] == "T10Y2Y"]
    assert len(t10y2y) == 1
    assert t10y2y[0]["observation_date"] == "2024-01-01"
    assert abs(t10y2y[0]["value"] - 1.0) < 1e-9


def _vintage_silver():
    return [
        {"series_id": "G", "observation_date": "2024-01-01", "value": 100.0,
         "realtime_start": "2024-02-01", "realtime_end": "9999-12-31", "is_missing": False},
        {"series_id": "G", "observation_date": "2024-01-01", "value": 101.5,
         "realtime_start": "2024-03-01", "realtime_end": "9999-12-31", "is_missing": False},
        {"series_id": "G", "observation_date": "2024-02-01", "value": 102.0,
         "realtime_start": "2024-03-15", "realtime_end": "9999-12-31", "is_missing": False},
    ]


def test_point_in_time_snapshot_respects_vintage():
    # As of mid-Feb, only the first release is known.
    early = point_in_time_snapshot(_vintage_silver(), "2024-02-15")
    assert early == [{"as_of_date": "2024-02-15", "series_id": "G",
                      "observation_date": "2024-01-01", "value": 100.0}]

    # By late March, the newer observation (Feb) is known and wins.
    late = point_in_time_snapshot(_vintage_silver(), "2024-03-20")
    assert late[0]["observation_date"] == "2024-02-01"
    assert late[0]["value"] == 102.0


def test_local_backend_builds_feature_tables(tmp_path):
    cfg = PipelineConfig(environment=Environment.DEV, fred_api_key="k")
    wh = LocalWarehouse(cfg, db_path=str(tmp_path / "f.db"))
    rows = [
        {"series_id": "DGS10", "observation_date": "2024-01-01", "realtime_start": "",
         "realtime_end": "", "value": 4.0, "raw_value": "4.0", "is_missing": False,
         "row_hash": "h1", "revision_number": 1, "ingested_at": "2024-01-02", "run_id": "r"},
        {"series_id": "DGS2", "observation_date": "2024-01-01", "realtime_start": "",
         "realtime_end": "", "value": 3.0, "raw_value": "3.0", "is_missing": False,
         "row_hash": "h2", "revision_number": 1, "ingested_at": "2024-01-02", "run_id": "r"},
    ]
    wh.merge_silver(rows)
    wh.build_gold()

    spreads = wh.query("SELECT * FROM gold_fred_curve_spread WHERE spread_name='T10Y2Y'")
    assert len(spreads) == 1 and abs(spreads[0]["value"] - 1.0) < 1e-9
    transforms = wh.query("SELECT * FROM gold_fred_feature_transforms")
    assert len(transforms) == 2
    snap = wh.point_in_time_features("2024-06-01")
    assert {r["series_id"] for r in snap} == {"DGS10", "DGS2"}
    wh.close()
