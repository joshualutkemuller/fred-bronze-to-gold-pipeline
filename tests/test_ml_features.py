"""Tests for ML-0: feature matrix engine and config loader."""

import pytest

from fred_pipeline.ml_features import (
    MLFeatureConfig,
    MLFeatureConfigError,
    MLFeatureDef,
    compute_ml_feature_matrix,
    load_ml_feature_config,
)


# ---- helpers -----------------------------------------------------------------

def _ft(sid, obs, value=None, mom=None, diff=None, yoy=None, zscore=None):
    return {
        "series_id": sid, "observation_date": obs,
        "value": value, "mom": mom, "diff": diff, "yoy": yoy, "zscore": zscore,
    }


def _fd(name, sid, transform="level"):
    return MLFeatureDef(name=name, series_id=sid, transform=transform)


def _cfg(*fds):
    return MLFeatureConfig(features=tuple(fds))


# ---- config loader -----------------------------------------------------------

def test_repo_config_loads():
    cfg = load_ml_feature_config()
    assert len(cfg.features) > 0
    for fd in cfg.features:
        assert fd.transform in {"level", "mom", "diff", "yoy", "zscore"}
        assert fd.name
        assert fd.series_id


def test_config_missing_file():
    with pytest.raises(MLFeatureConfigError, match="not found"):
        load_ml_feature_config("/nonexistent/path/ml.yml")


def test_config_unknown_key(tmp_path):
    p = tmp_path / "ml.yml"
    p.write_text("features: []\nunknown_param: 1\n")
    with pytest.raises(MLFeatureConfigError, match="Unknown"):
        load_ml_feature_config(str(p))


def test_config_duplicate_name(tmp_path):
    p = tmp_path / "ml.yml"
    p.write_text(
        "features:\n"
        "  - {name: X, series_id: A, transform: level}\n"
        "  - {name: X, series_id: B, transform: yoy}\n"
    )
    with pytest.raises(MLFeatureConfigError, match="Duplicate"):
        load_ml_feature_config(str(p))


def test_config_invalid_transform(tmp_path):
    p = tmp_path / "ml.yml"
    p.write_text("features:\n  - {name: X, series_id: A, transform: bad}\n")
    with pytest.raises(MLFeatureConfigError, match="Invalid transform"):
        load_ml_feature_config(str(p))


def test_config_missing_series_id(tmp_path):
    p = tmp_path / "ml.yml"
    p.write_text("features:\n  - {name: X}\n")
    with pytest.raises(MLFeatureConfigError, match="series_id"):
        load_ml_feature_config(str(p))


def test_config_defaults(tmp_path):
    p = tmp_path / "ml.yml"
    p.write_text("features:\n  - {name: X, series_id: A, transform: level}\n")
    cfg = load_ml_feature_config(str(p))
    assert cfg.min_features_for_pca == 5
    assert cfg.n_components == 5
    assert cfg.anomaly_threshold == pytest.approx(0.99)


# ---- compute_ml_feature_matrix -----------------------------------------------

def test_level_transform_picks_value_column():
    rows = [_ft("NFCI", "2020-01-03", value=-0.5, yoy=0.1)]
    cfg = _cfg(_fd("NFCI", "NFCI", "level"))
    out = compute_ml_feature_matrix(rows, cfg)
    assert len(out) == 1
    assert out[0]["value"] == pytest.approx(-0.5)
    assert out[0]["transform"] == "level"
    assert out[0]["feature_name"] == "NFCI"
    assert out[0]["series_id"] == "NFCI"


def test_yoy_transform_picks_yoy_column():
    rows = [_ft("M2SL", "2020-01-01", value=20_000.0, yoy=0.05)]
    cfg = _cfg(_fd("M2SL_YOY", "M2SL", "yoy"))
    out = compute_ml_feature_matrix(rows, cfg)
    assert len(out) == 1
    assert out[0]["value"] == pytest.approx(0.05)
    assert out[0]["transform"] == "yoy"


def test_mom_transform():
    rows = [_ft("X", "2020-03-01", value=100.0, mom=0.02)]
    cfg = _cfg(_fd("X_MOM", "X", "mom"))
    out = compute_ml_feature_matrix(rows, cfg)
    assert out[0]["value"] == pytest.approx(0.02)


def test_zscore_transform():
    rows = [_ft("X", "2020-03-01", value=100.0, zscore=-1.3)]
    cfg = _cfg(_fd("X_Z", "X", "zscore"))
    out = compute_ml_feature_matrix(rows, cfg)
    assert out[0]["value"] == pytest.approx(-1.3)


def test_null_transform_value_skipped():
    rows = [_ft("M2SL", "2020-01-01", value=100.0, yoy=None)]
    cfg = _cfg(_fd("M2SL_YOY", "M2SL", "yoy"))
    assert compute_ml_feature_matrix(rows, cfg) == []


def test_unrecognised_series_skipped():
    rows = [_ft("OTHER", "2020-01-01", value=1.0)]
    cfg = _cfg(_fd("NFCI", "NFCI", "level"))
    assert compute_ml_feature_matrix(rows, cfg) == []


def test_multiple_features_same_series():
    rows = [_ft("NFCI", "2020-01-03", value=-0.5, yoy=0.1, zscore=-1.2)]
    cfg = _cfg(
        _fd("NFCI_LEVEL", "NFCI", "level"),
        _fd("NFCI_YOY", "NFCI", "yoy"),
        _fd("NFCI_Z", "NFCI", "zscore"),
    )
    out = compute_ml_feature_matrix(rows, cfg)
    assert len(out) == 3
    names = {r["feature_name"] for r in out}
    assert names == {"NFCI_LEVEL", "NFCI_YOY", "NFCI_Z"}


def test_multiple_dates():
    rows = [
        _ft("NFCI", "2020-01-03", value=-0.5),
        _ft("NFCI", "2020-02-07", value=-0.3),
        _ft("NFCI", "2020-03-06", value=0.1),
    ]
    cfg = _cfg(_fd("NFCI", "NFCI", "level"))
    out = compute_ml_feature_matrix(rows, cfg)
    assert len(out) == 3
    dates = [r["observation_date"] for r in out]
    assert "2020-01-03" in dates and "2020-03-06" in dates


def test_empty_input():
    cfg = _cfg(_fd("NFCI", "NFCI", "level"))
    assert compute_ml_feature_matrix([], cfg) == []


def test_empty_config():
    rows = [_ft("NFCI", "2020-01-03", value=1.0)]
    assert compute_ml_feature_matrix(rows, MLFeatureConfig(features=())) == []


def test_missing_observation_date_skipped():
    rows = [{"series_id": "NFCI", "observation_date": None, "value": 1.0}]
    cfg = _cfg(_fd("NFCI", "NFCI", "level"))
    assert compute_ml_feature_matrix(rows, cfg) == []
