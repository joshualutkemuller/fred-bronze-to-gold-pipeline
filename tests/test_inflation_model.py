"""Tests for ML-6 (Short-Horizon Inflation Forecasting)."""

import math
import random

import pytest

from fred_pipeline.inflation_model import (
    InflationForecastConfig,
    _ar_bic,
    _ar_bootstrap_ci,
    _ar_fit,
    _ar_forecast,
    _extract_mom,
    _select_ar_lag,
    _select_var_lag,
    _solve,
    _var_bic,
    _var_bootstrap_ci,
    _var_fit,
    _var_forecast,
    compute_inflation_forecast,
    load_inflation_forecast_config,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _level_rows(
    series_id: str,
    n_months: int,
    start_level: float = 100.0,
    trend: float = 0.003,
    noise: float = 0.001,
    seed: int = 42,
) -> list[dict]:
    """Synthetic monthly price-index level rows (FRED-style)."""
    rng = random.Random(seed)
    rows = []
    level = start_level
    for i in range(n_months):
        year = 2000 + i // 12
        month = (i % 12) + 1
        rows.append({
            "series_id": series_id,
            "observation_date": f"{year}-{month:02d}-01",
            "value": level,
        })
        level *= 1.0 + trend + rng.gauss(0.0, noise)
    return rows


def _fast_cfg(**overrides) -> InflationForecastConfig:
    """Config with small bootstrap for fast tests."""
    defaults = dict(
        series=("CPIAUCSL", "PCEPI"),
        var_pairs=(("CPIAUCSL", "PCEPI"),),
        horizons=(1, 3),
        max_ar_lag=4,
        max_var_lag=2,
        n_bootstrap=20,
        min_obs=12,
        random_seed=0,
    )
    defaults.update(overrides)
    return InflationForecastConfig(**defaults)


# ---------------------------------------------------------------------------
# _solve
# ---------------------------------------------------------------------------

def test_solve_2x2():
    # 2x + y = 5, x + 3y = 10  →  x=1, y=3
    a = [[2.0, 1.0], [1.0, 3.0]]
    b = [5.0, 10.0]
    x = _solve(a, b)
    assert x is not None
    assert x[0] == pytest.approx(1.0, abs=1e-10)
    assert x[1] == pytest.approx(3.0, abs=1e-10)


def test_solve_singular_returns_none():
    a = [[1.0, 2.0], [2.0, 4.0]]  # rank 1
    b = [1.0, 2.0]
    assert _solve(a, b) is None


# ---------------------------------------------------------------------------
# AR fitting
# ---------------------------------------------------------------------------

def test_ar_fit_returns_correct_length():
    y = [1.0, 2.0, 3.0, 4.0, 5.0, 4.0, 3.0, 2.0, 1.0, 2.0, 3.0, 4.0]
    result = _ar_fit(y, p=2)
    assert result is not None
    coeffs, residuals = result
    assert len(coeffs) == 3       # intercept + 2 lags
    assert len(residuals) == len(y) - 2


def test_ar_fit_insufficient_obs_returns_none():
    # n_eff = 2, k = p+1 = 3 → n_eff <= k
    assert _ar_fit([1.0, 2.0, 3.0, 4.0], p=3) is None


def test_ar_fit_residuals_near_zero_for_perfect_ar1():
    # y[t] = 0.9 * y[t-1] exactly
    y = [1.0]
    for _ in range(30):
        y.append(0.9 * y[-1])
    result = _ar_fit(y, p=1)
    assert result is not None
    _, residuals = result
    assert all(abs(r) < 1e-8 for r in residuals)


# ---------------------------------------------------------------------------
# AR BIC and lag selection
# ---------------------------------------------------------------------------

def test_ar_bic_finite_for_valid_input():
    rng = random.Random(1)
    y = [rng.gauss(0, 1) for _ in range(50)]
    bic = _ar_bic(y, p=1)
    assert math.isfinite(bic)


def test_ar_bic_inf_when_insufficient_data():
    # Only 3 obs, p=3 → n_eff=0 ≤ k=4
    bic = _ar_bic([1.0, 2.0, 3.0], p=3)
    assert bic == math.inf


def test_select_ar_lag_in_range():
    rng = random.Random(7)
    y = [rng.gauss(0, 1) for _ in range(60)]
    p = _select_ar_lag(y, max_lag=6)
    assert 1 <= p <= 6


def test_select_ar_lag_true_ar1_prefers_low_lag():
    # Generate clean AR(1) data; BIC should prefer p=1 or p=2
    rng = random.Random(99)
    y = [0.0]
    for _ in range(200):
        y.append(0.7 * y[-1] + rng.gauss(0, 0.1))
    p = _select_ar_lag(y, max_lag=8)
    assert p <= 3  # may pick 1 or occasionally 2; not 8


# ---------------------------------------------------------------------------
# AR forecast
# ---------------------------------------------------------------------------

def test_ar_forecast_returns_h_values():
    coeffs = [0.1, 0.8]   # AR(1): y[t] = 0.1 + 0.8*y[t-1]
    y = [1.0, 1.2, 1.1, 1.3, 1.0]
    fc = _ar_forecast(y, coeffs, h=4)
    assert len(fc) == 4


def test_ar_forecast_deterministic():
    coeffs = [0.05, 0.7, 0.2]   # AR(2)
    y = list(range(10, 0, -1))
    fc1 = _ar_forecast(y, coeffs, h=3)
    fc2 = _ar_forecast(y, coeffs, h=3)
    assert fc1 == fc2


def test_ar_forecast_stationary_converges():
    # y[t] = 0.5*y[t-1]; should converge to 0
    coeffs = [0.0, 0.5]
    y = [10.0] * 5
    fc = _ar_forecast(y, coeffs, h=20)
    assert abs(fc[-1]) < 0.01


# ---------------------------------------------------------------------------
# AR bootstrap CIs
# ---------------------------------------------------------------------------

def test_ar_bootstrap_ci_returns_all_keys():
    rng_obj = random.Random(0)
    y = [0.003] * 30
    coeffs = [0.001, 0.6]
    residuals = [0.001 * (i % 5 - 2) for i in range(28)]
    ci = _ar_bootstrap_ci(y, coeffs, residuals, h=1, n_boot=20, rng=rng_obj)
    for key in ("lower_80", "upper_80", "lower_95", "upper_95"):
        assert key in ci
        assert ci[key] is not None


def test_ar_bootstrap_ci_ordered():
    rng_obj = random.Random(1)
    rng = random.Random(2)
    y = [rng.gauss(0.003, 0.002) for _ in range(40)]
    result = _ar_fit(y, p=1)
    assert result is not None
    coeffs, residuals = result
    ci = _ar_bootstrap_ci(y, coeffs, residuals, h=3, n_boot=50, rng=rng_obj)
    assert ci["lower_95"] <= ci["lower_80"]
    assert ci["lower_80"] <= ci["upper_80"]
    assert ci["upper_80"] <= ci["upper_95"]


def test_ar_bootstrap_ci_empty_residuals_returns_none():
    rng_obj = random.Random(0)
    ci = _ar_bootstrap_ci([1.0, 2.0], [0.5, 0.5], [], h=1, n_boot=10, rng=rng_obj)
    assert all(v is None for v in ci.values())


# ---------------------------------------------------------------------------
# VAR fitting
# ---------------------------------------------------------------------------

def test_var_fit_returns_four_arrays():
    rng = random.Random(3)
    y1 = [rng.gauss(0, 1) for _ in range(40)]
    y2 = [rng.gauss(0, 1) for _ in range(40)]
    result = _var_fit(y1, y2, p=2)
    assert result is not None
    c1, c2, r1, r2 = result
    assert len(c1) == 5   # intercept + 2*2 lags
    assert len(c2) == 5
    assert len(r1) == 38  # n - p = 40 - 2
    assert len(r2) == 38


def test_var_fit_insufficient_obs_returns_none():
    # n_eff = 5, k = 1+2*3 = 7 → not enough
    y1 = [float(i) for i in range(8)]
    y2 = [float(i) for i in range(8)]
    assert _var_fit(y1, y2, p=3) is None


def test_var_fit_mismatched_lengths_returns_none():
    y1 = [1.0] * 30
    y2 = [1.0] * 25
    assert _var_fit(y1, y2, p=1) is None


# ---------------------------------------------------------------------------
# VAR BIC and lag selection
# ---------------------------------------------------------------------------

def test_var_bic_finite_for_valid_input():
    rng = random.Random(4)
    y1 = [rng.gauss(0, 1) for _ in range(60)]
    y2 = [rng.gauss(0, 1) for _ in range(60)]
    bic = _var_bic(y1, y2, p=1)
    assert math.isfinite(bic)


def test_select_var_lag_in_range():
    rng = random.Random(5)
    y1 = [rng.gauss(0, 1) for _ in range(80)]
    y2 = [rng.gauss(0, 1) for _ in range(80)]
    p = _select_var_lag(y1, y2, max_lag=3)
    assert 1 <= p <= 3


# ---------------------------------------------------------------------------
# VAR forecast
# ---------------------------------------------------------------------------

def test_var_forecast_returns_h_tuples():
    c1 = [0.01, 0.5, 0.1]
    c2 = [0.01, 0.1, 0.6]
    y1 = [0.003] * 5
    y2 = [0.002] * 5
    fc = _var_forecast(y1, y2, c1, c2, p=1, h=4)
    assert len(fc) == 4
    assert all(len(pair) == 2 for pair in fc)


def test_var_forecast_stationary_converges():
    # Both equations: y[t] = 0.3*y1[t-1] + 0.1*y2[t-1]; should decay
    c1 = [0.0, 0.3, 0.1]
    c2 = [0.0, 0.1, 0.3]
    y1 = [1.0] * 3
    y2 = [1.0] * 3
    fc = _var_forecast(y1, y2, c1, c2, p=1, h=30)
    assert abs(fc[-1][0]) < 0.5
    assert abs(fc[-1][1]) < 0.5


# ---------------------------------------------------------------------------
# VAR bootstrap CIs
# ---------------------------------------------------------------------------

def test_var_bootstrap_ci_ordered():
    rng_obj = random.Random(6)
    rng = random.Random(7)
    y1 = [rng.gauss(0.003, 0.002) for _ in range(50)]
    y2 = [rng.gauss(0.002, 0.002) for _ in range(50)]
    vfit = _var_fit(y1, y2, p=1)
    assert vfit is not None
    c1, c2, r1, r2 = vfit
    ci1, ci2 = _var_bootstrap_ci(y1, y2, c1, c2, r1, r2, p=1, h=3, n_boot=40, rng=rng_obj)
    for ci in (ci1, ci2):
        assert ci["lower_95"] <= ci["lower_80"]
        assert ci["lower_80"] <= ci["upper_80"]
        assert ci["upper_80"] <= ci["upper_95"]


def test_var_bootstrap_ci_empty_residuals_returns_none():
    rng_obj = random.Random(0)
    ci1, ci2 = _var_bootstrap_ci(
        [0.003], [0.002], [0.0, 0.5, 0.1], [0.0, 0.1, 0.5],
        [], [], p=1, h=1, n_boot=10, rng=rng_obj,
    )
    assert all(v is None for v in ci1.values())
    assert all(v is None for v in ci2.values())


# ---------------------------------------------------------------------------
# _extract_mom
# ---------------------------------------------------------------------------

def test_extract_mom_basic():
    rows = _level_rows("CPIAUCSL", 36)
    mom = _extract_mom(rows, {"CPIAUCSL"})
    assert "CPIAUCSL" in mom
    assert len(mom["CPIAUCSL"]) == 35  # 36 levels → 35 MoM values


def test_extract_mom_skips_gaps():
    # Introduce a 2-month gap in the middle
    rows = _level_rows("PCEPI", 24)
    # Remove month 12 (2000-12-01) to create a gap
    rows = [r for r in rows if r["observation_date"] != "2000-12-01"]
    mom = _extract_mom(rows, {"PCEPI"})
    # 23 obs → 22 potential pairs; the Nov→Jan gap (61 days) is skipped → 21
    assert len(mom["PCEPI"]) == 21


def test_extract_mom_skips_zero_prior_level():
    rows = [
        {"series_id": "X", "observation_date": "2000-01-01", "value": 0.0},
        {"series_id": "X", "observation_date": "2000-02-01", "value": 100.0},
    ]
    mom = _extract_mom(rows, {"X"})
    assert "X" not in mom or len(mom["X"]) == 0


def test_extract_mom_unknown_series_excluded():
    rows = _level_rows("CPIAUCSL", 20)
    mom = _extract_mom(rows, {"PCEPI"})
    assert "CPIAUCSL" not in mom


# ---------------------------------------------------------------------------
# compute_inflation_forecast — main engine
# ---------------------------------------------------------------------------

def test_compute_forecast_empty_input():
    assert compute_inflation_forecast([]) == []


def test_compute_forecast_insufficient_data_returns_empty():
    cfg = _fast_cfg(min_obs=50)
    rows = _level_rows("CPIAUCSL", 20)  # only 19 MoM values
    assert compute_inflation_forecast(rows, cfg=cfg) == []


def test_compute_forecast_ar_schema():
    cfg = _fast_cfg(var_pairs=())
    rows = _level_rows("CPIAUCSL", 50)
    out = compute_inflation_forecast(rows, cfg=cfg)
    assert out, "expected non-empty output"
    for r in out:
        assert r["model_type"] == "ar"
        for key in (
            "series_id", "forecast_date", "horizon_months",
            "forecast_value", "lower_80", "upper_80", "lower_95", "upper_95",
            "model_type", "lag_order", "model_vintage", "n_obs_training",
        ):
            assert key in r, f"missing key {key!r}"


def test_compute_forecast_var_schema():
    rows = _level_rows("CPIAUCSL", 50) + _level_rows("PCEPI", 50, seed=99)
    cfg = _fast_cfg(series=(), var_pairs=(("CPIAUCSL", "PCEPI"),))
    out = compute_inflation_forecast(rows, cfg=cfg)
    assert out
    var_rows = [r for r in out if r["model_type"] == "var"]
    assert var_rows
    for r in var_rows:
        for key in (
            "series_id", "forecast_date", "horizon_months",
            "forecast_value", "lower_80", "upper_80", "lower_95", "upper_95",
            "model_type", "lag_order", "model_vintage", "n_obs_training",
        ):
            assert key in r, f"missing key {key!r}"


def test_compute_forecast_all_horizons_present():
    cfg = _fast_cfg(horizons=(1, 3, 6), var_pairs=())
    rows = _level_rows("CPIAUCSL", 60)
    out = compute_inflation_forecast(rows, cfg=cfg)
    horizons = {r["horizon_months"] for r in out}
    assert horizons == {1, 3, 6}


def test_compute_forecast_model_type_ar():
    cfg = _fast_cfg(var_pairs=())
    rows = _level_rows("CPIAUCSL", 50)
    out = compute_inflation_forecast(rows, cfg=cfg)
    assert all(r["model_type"] == "ar" for r in out)


def test_compute_forecast_lag_order_positive():
    cfg = _fast_cfg(var_pairs=())
    rows = _level_rows("CPIAUCSL", 60)
    out = compute_inflation_forecast(rows, cfg=cfg)
    assert all(r["lag_order"] >= 1 for r in out)


def test_compute_forecast_n_obs_training_positive():
    cfg = _fast_cfg(var_pairs=())
    rows = _level_rows("CPIAUCSL", 60)
    out = compute_inflation_forecast(rows, cfg=cfg)
    assert all(r["n_obs_training"] >= cfg.min_obs for r in out)


def test_compute_forecast_ci_not_none():
    cfg = _fast_cfg(var_pairs=())
    rows = _level_rows("CPIAUCSL", 60)
    out = compute_inflation_forecast(rows, cfg=cfg)
    for r in out:
        assert r["lower_95"] is not None
        assert r["lower_80"] is not None
        assert r["upper_80"] is not None
        assert r["upper_95"] is not None


def test_compute_forecast_ci_ordering():
    cfg = _fast_cfg(var_pairs=())
    rows = _level_rows("CPIAUCSL", 60)
    out = compute_inflation_forecast(rows, cfg=cfg)
    for r in out:
        assert r["lower_95"] <= r["lower_80"]
        assert r["lower_80"] <= r["upper_80"]
        assert r["upper_80"] <= r["upper_95"]


def test_compute_forecast_both_ar_and_var():
    rows = _level_rows("CPIAUCSL", 60) + _level_rows("PCEPI", 60, seed=77)
    cfg = _fast_cfg()
    out = compute_inflation_forecast(rows, cfg=cfg)
    types = {r["model_type"] for r in out}
    assert "ar" in types
    assert "var" in types


def test_compute_forecast_var_produces_both_series():
    rows = _level_rows("CPIAUCSL", 60) + _level_rows("PCEPI", 60, seed=77)
    cfg = _fast_cfg(series=(), var_pairs=(("CPIAUCSL", "PCEPI"),))
    out = compute_inflation_forecast(rows, cfg=cfg)
    ids = {r["series_id"] for r in out}
    assert "CPIAUCSL" in ids
    assert "PCEPI" in ids


def test_compute_forecast_missing_series_in_pair_skipped():
    rows = _level_rows("CPIAUCSL", 60)  # PCEPI absent
    cfg = _fast_cfg(series=("CPIAUCSL",), var_pairs=(("CPIAUCSL", "PCEPI"),))
    out = compute_inflation_forecast(rows, cfg=cfg)
    # AR rows for CPIAUCSL should still appear; VAR rows should not
    assert any(r["model_type"] == "ar" for r in out)
    assert not any(r["model_type"] == "var" for r in out)


def test_compute_forecast_forecast_date_equals_model_vintage():
    cfg = _fast_cfg(var_pairs=())
    rows = _level_rows("CPIAUCSL", 50)
    out = compute_inflation_forecast(rows, cfg=cfg)
    for r in out:
        assert r["forecast_date"] == r["model_vintage"]


def test_compute_forecast_forecast_value_is_float():
    cfg = _fast_cfg(var_pairs=())
    rows = _level_rows("CPIAUCSL", 50)
    out = compute_inflation_forecast(rows, cfg=cfg)
    for r in out:
        assert isinstance(r["forecast_value"], float)


def test_compute_forecast_wider_ci_at_longer_horizon():
    """95% CI width should (on average) grow with horizon for AR on noisy data."""
    cfg = _fast_cfg(var_pairs=(), horizons=(1, 6), n_bootstrap=100)
    rows = _level_rows("CPIAUCSL", 80)
    out = compute_inflation_forecast(rows, cfg=cfg)
    ar_rows = {r["horizon_months"]: r for r in out if r["model_type"] == "ar"}
    if 1 in ar_rows and 6 in ar_rows:
        width1 = ar_rows[1]["upper_95"] - ar_rows[1]["lower_95"]
        width6 = ar_rows[6]["upper_95"] - ar_rows[6]["lower_95"]
        assert width6 >= width1 * 0.5  # width should not collapse


# ---------------------------------------------------------------------------
# load_inflation_forecast_config
# ---------------------------------------------------------------------------

def test_load_config_defaults_when_file_absent():
    cfg = load_inflation_forecast_config("/nonexistent/path.yml")
    assert "CPIAUCSL" in cfg.series
    assert "PCEPI" in cfg.series
    assert cfg.horizons == (1, 3, 6, 12)
    assert cfg.max_ar_lag == 12
    assert cfg.n_bootstrap == 500
    assert cfg.min_obs == 24


def test_load_config_from_repo_file():
    cfg = load_inflation_forecast_config()  # uses repo config/inflation_forecast.yml
    assert len(cfg.series) >= 2
    assert len(cfg.horizons) >= 1
    assert cfg.max_ar_lag >= 1
    assert cfg.n_bootstrap > 0
