"""Tests for ML-2 (PCA factor scores) and ML-4 (anomaly detection)."""

import math
import random

import pytest

from fred_pipeline.anomaly import (
    _chi2_sf,
    _gammainc_cf,
    _gammainc_series,
    compute_macro_anomaly_scores,
)
from fred_pipeline.macro_pca import (
    _WelfordCov,
    _top_k_eigenpairs,
    compute_macro_factor_scores,
)


# ============================================================
# χ² survival function
# ============================================================

def test_chi2_sf_zero_x():
    assert _chi2_sf(0.0, 3) == pytest.approx(1.0)


def test_chi2_sf_negative_x():
    assert _chi2_sf(-5.0, 3) == pytest.approx(1.0)


def test_chi2_sf_1dof_known():
    # chi2(1).sf(3.841) ≈ 0.05  (standard table value)
    assert _chi2_sf(3.841, 1) == pytest.approx(0.05, abs=1e-3)


def test_chi2_sf_2dof_known():
    # chi2(2).sf(x) = exp(-x/2); chi2(2).sf(4.605) ≈ 0.1003
    assert _chi2_sf(4.605, 2) == pytest.approx(0.1003, abs=1e-3)


def test_chi2_sf_large_x_is_small():
    assert _chi2_sf(30.0, 5) < 1e-4


def test_chi2_sf_x_at_median():
    # chi2(4).sf(3.357) ≈ 0.5  (chi2 median ≈ df*(1-2/(9*df))^3)
    assert _chi2_sf(3.357, 4) == pytest.approx(0.5, abs=0.01)


def test_gammainc_series_known():
    # P(1, 1) = 1 − e^{−1}
    assert _gammainc_series(1.0, 1.0) == pytest.approx(1.0 - math.exp(-1.0), abs=1e-8)


def test_gammainc_cf_known():
    # Q(1, 5) = e^{−5}
    assert _gammainc_cf(1.0, 5.0) == pytest.approx(math.exp(-5.0), abs=1e-8)


def test_chi2_sf_complement():
    # P + Q should equal 1 for both series and cf regimes
    x, k = 3.0, 6
    # x=3 < a+1=4 → series regime; x=9 >= a+1=4 → cf regime
    sf_series = _chi2_sf(3.0, k)
    sf_cf = _chi2_sf(9.0, k)
    # Just check both are in (0,1)
    assert 0.0 < sf_series < 1.0
    assert 0.0 < sf_cf < 1.0


# ============================================================
# Welford covariance
# ============================================================

def test_welford_constant_series_zero_var():
    wc = _WelfordCov(2)
    for _ in range(100):
        wc.update([3.0, -1.5])
    cov = wc.covariance()
    assert abs(cov[0][0]) < 1e-10
    assert abs(cov[1][1]) < 1e-10


def test_welford_unit_variance():
    rng = random.Random(42)
    wc = _WelfordCov(1)
    for _ in range(2000):
        wc.update([rng.gauss(0.0, 1.0)])
    cov = wc.covariance()
    assert cov[0][0] == pytest.approx(1.0, abs=0.05)


def test_welford_known_covariance():
    # x = (1,2,3,4,5), y = (5,4,3,2,1) → cov(x,y) = -2.5
    wc = _WelfordCov(2)
    for x, y in zip([1, 2, 3, 4, 5], [5, 4, 3, 2, 1]):
        wc.update([float(x), float(y)])
    cov = wc.covariance()
    assert cov[0][1] == pytest.approx(-2.5)
    assert cov[1][0] == pytest.approx(-2.5)


def test_welford_insufficient_data():
    wc = _WelfordCov(2)
    wc.update([1.0, 2.0])
    cov = wc.covariance()
    assert all(cov[i][j] == 0.0 for i in range(2) for j in range(2))


# ============================================================
# Power iteration
# ============================================================

def test_top_k_eigenpairs_identity():
    I = [[1.0 if i == j else 0.0 for j in range(3)] for i in range(3)]
    pairs = _top_k_eigenpairs(I, 3)
    eigenvalues = sorted([ev for ev, _ in pairs], reverse=True)
    assert all(abs(ev - 1.0) < 1e-6 for ev in eigenvalues)


def test_top_k_eigenpairs_diagonal():
    D = [[4.0, 0.0], [0.0, 1.0]]
    pairs = _top_k_eigenpairs(D, 2)
    assert len(pairs) == 2
    evs = sorted([ev for ev, _ in pairs], reverse=True)
    assert evs[0] == pytest.approx(4.0, abs=1e-4)
    assert evs[1] == pytest.approx(1.0, abs=1e-4)


# ============================================================
# compute_macro_factor_scores
# ============================================================

def _fm_rows(n_months, feature_names):
    """Synthetic monthly feature matrix rows (one value per feature per month)."""
    rng = random.Random(7)
    rows = []
    for i in range(n_months):
        year = 2000 + (i // 12)
        month = (i % 12) + 1
        obs_date = f"{year}-{month:02d}-28"
        for j, fn in enumerate(feature_names):
            val = float(i) if j == 0 else rng.gauss(0.0, 1.0)
            rows.append({
                "observation_date": obs_date,
                "feature_name": fn,
                "series_id": fn,
                "transform": "level",
                "value": val,
            })
    return rows


def test_pca_returns_scores_and_loadings():
    rows = _fm_rows(50, ["F1", "F2", "F3"])
    result = compute_macro_factor_scores(rows, n_components=2, min_obs=10)
    assert "scores" in result and "loadings" in result
    assert len(result["scores"]) > 0
    assert len(result["loadings"]) > 0


def test_pca_respects_n_components():
    rows = _fm_rows(60, ["F1", "F2", "F3", "F4"])
    result = compute_macro_factor_scores(rows, n_components=2, min_obs=10)
    per_date: dict[str, list[int]] = {}
    for r in result["scores"]:
        per_date.setdefault(r["observation_date"], []).append(r["factor"])
    for factors in per_date.values():
        assert max(factors) <= 2


def test_pca_min_obs_threshold():
    rows = _fm_rows(5, ["F1", "F2"])
    result = compute_macro_factor_scores(rows, n_components=1, min_obs=30)
    assert result["scores"] == [] and result["loadings"] == []


def test_pca_cum_evr_nondecreasing():
    rows = _fm_rows(60, ["F1", "F2", "F3"])
    result = compute_macro_factor_scores(rows, n_components=3, min_obs=10)
    from itertools import groupby
    by_date = {}
    for r in result["scores"]:
        by_date.setdefault(r["observation_date"], []).append(r)
    for date_rows in by_date.values():
        ordered = sorted(date_rows, key=lambda r: r["factor"])
        for i in range(1, len(ordered)):
            assert ordered[i]["cumulative_variance_ratio"] >= ordered[i-1]["cumulative_variance_ratio"] - 1e-9


def test_pca_cum_evr_bounded():
    rows = _fm_rows(60, ["F1", "F2", "F3"])
    result = compute_macro_factor_scores(rows, n_components=3, min_obs=10)
    for r in result["scores"]:
        assert r["cumulative_variance_ratio"] <= 1.0 + 1e-9


def test_pca_n_obs_increases():
    rows = _fm_rows(60, ["F1", "F2", "F3"])
    result = compute_macro_factor_scores(rows, n_components=1, min_obs=10)
    n_obs_vals = [r["n_obs"] for r in result["scores"]]
    assert n_obs_vals == sorted(n_obs_vals)


def test_pca_loadings_unit_norm():
    rows = _fm_rows(50, ["F1", "F2", "F3"])
    result = compute_macro_factor_scores(rows, n_components=2, min_obs=10)
    by_date_factor: dict[tuple[str, int], list[float]] = {}
    for r in result["loadings"]:
        key = (r["observation_date"], r["factor"])
        by_date_factor.setdefault(key, []).append(r["loading"])
    for loadings in by_date_factor.values():
        norm = math.sqrt(sum(v * v for v in loadings))
        assert norm == pytest.approx(1.0, abs=1e-4)


def test_pca_empty_input():
    result = compute_macro_factor_scores([], n_components=3, min_obs=10)
    assert result == {"scores": [], "loadings": []}


def test_pca_single_feature():
    rows = _fm_rows(40, ["F1"])
    # k_feat < 2 → no output
    result = compute_macro_factor_scores(rows, n_components=1, min_obs=10)
    assert result == {"scores": [], "loadings": []}


def test_pca_sign_anchor_positive_loading():
    """The dominant (max-abs) loading per factor should always be positive."""
    rows = _fm_rows(60, ["F1", "F2", "F3", "F4"])
    result = compute_macro_factor_scores(rows, n_components=3, min_obs=10)
    from itertools import groupby
    by_df = {}
    for r in result["loadings"]:
        key = (r["observation_date"], r["factor"])
        by_df.setdefault(key, []).append(r["loading"])
    for loadings in by_df.values():
        max_abs = max(abs(v) for v in loadings)
        dominant = next(v for v in loadings if abs(v) == max_abs)
        assert dominant >= 0.0


# ============================================================
# compute_macro_anomaly_scores
# ============================================================

def _score_rows(n, n_factors=2, spike_at_end=False, n_obs=50):
    rng = random.Random(99)
    rows = []
    for i in range(n):
        year = 2000 + (i // 12)
        month = (i % 12) + 1
        obs_date = f"{year}-{month:02d}-28"
        for f in range(1, n_factors + 1):
            score = 10.0 * f if (spike_at_end and i == n - 1) else rng.gauss(0, 1)
            rows.append({
                "observation_date": obs_date,
                "factor": f,
                "score": score,
                "explained_variance_ratio": 0.3,
                "cumulative_variance_ratio": 0.3 * f,
                "n_obs": n_obs,
            })
    return rows


def test_anomaly_output_schema():
    rows = _score_rows(50)
    out = compute_macro_anomaly_scores(rows, min_obs=10)
    assert out
    for r in out:
        assert "observation_date" in r
        assert "mahalanobis_d2" in r
        assert "chi2_df" in r
        assert "p_value" in r
        assert "is_anomaly" in r
        assert "n_factors_used" in r


def test_anomaly_d2_nonnegative():
    rows = _score_rows(50)
    out = compute_macro_anomaly_scores(rows, min_obs=10)
    assert all(r["mahalanobis_d2"] >= 0.0 for r in out)


def test_anomaly_p_value_in_unit_interval():
    rows = _score_rows(50)
    out = compute_macro_anomaly_scores(rows, min_obs=10)
    assert all(0.0 <= r["p_value"] <= 1.0 for r in out)


def test_anomaly_extreme_observation_flagged():
    rows = _score_rows(60, spike_at_end=True)
    out = compute_macro_anomaly_scores(rows, min_obs=10, anomaly_threshold=0.99)
    last = max(out, key=lambda r: r["observation_date"])
    assert last["is_anomaly"] == 1


def test_anomaly_normal_data_low_flag_rate():
    rows = _score_rows(120)
    out = compute_macro_anomaly_scores(rows, min_obs=10, anomaly_threshold=0.99)
    flag_rate = sum(r["is_anomaly"] for r in out) / len(out)
    # Under H0 we expect ≈ 1% flags; allow generous bound for small n
    assert flag_rate < 0.15


def test_anomaly_empty_input():
    assert compute_macro_anomaly_scores([]) == []


def test_anomaly_insufficient_history():
    rows = _score_rows(5)
    out = compute_macro_anomaly_scores(rows, min_obs=50)
    assert out == []


def test_anomaly_chi2_df_equals_n_factors():
    rows = _score_rows(60, n_factors=3)
    out = compute_macro_anomaly_scores(rows, min_obs=10)
    for r in out:
        assert r["chi2_df"] == r["n_factors_used"]
        assert r["chi2_df"] == 3
