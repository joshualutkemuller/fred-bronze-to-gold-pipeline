"""Tests for fred_pipeline.ns_model (Nelson-Siegel yield curve fitting)."""

import math

import pytest

from fred_pipeline.ns_model import (
    LAMBDA_FIXED,
    MIN_TENORS,
    RMSE_GRID_THRESHOLD,
    _ns_loadings,
    _ols3,
    compute_yield_curve_ns_factors,
)

# Standard Treasury tenor set in months.
_TENORS_MO = (1, 3, 6, 12, 24, 60, 120, 360)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _curve_rows(as_of_date: str, yields_by_months: dict[int, float]) -> list[dict]:
    return [
        {"as_of_date": as_of_date, "tenor_months": m, "yield_pct": v}
        for m, v in yields_by_months.items()
    ]


def _exact_ns_yields(beta0, beta1, beta2, lam, tenors_mo=_TENORS_MO) -> dict[int, float]:
    """Generate yields that are *exactly* NS(β₀,β₁,β₂,λ)."""
    out = {}
    for m in tenors_mo:
        L, C = _ns_loadings(m / 12.0, lam)
        out[m] = beta0 + beta1 * L + beta2 * C
    return out


# ---------------------------------------------------------------------------
# Unit tests for _ns_loadings
# ---------------------------------------------------------------------------

def test_ns_loadings_tau_zero():
    L, C = _ns_loadings(0.0, LAMBDA_FIXED)
    assert L == pytest.approx(1.0)
    assert C == pytest.approx(0.0)


def test_ns_loadings_long_tenor():
    # L decays as 1/x = λ/τ; at τ=10000 that's 1.7/10000 ≈ 1.7e-4.
    L, C = _ns_loadings(10000.0, LAMBDA_FIXED)
    assert abs(L) < 1e-3
    assert abs(C) < 1e-3
    # And both are much smaller than the short-tenor values.
    L_short, _ = _ns_loadings(0.1, LAMBDA_FIXED)
    assert L < L_short / 100


def test_ns_loadings_curvature_peak():
    # C(τ,λ) has a single interior maximum (the hump); verify it is positive
    # and that values at both short and long ends are smaller.
    lam = LAMBDA_FIXED
    short = _ns_loadings(0.1, lam)[1]
    medium = _ns_loadings(3.0, lam)[1]   # near the hump for λ=1.7
    long_ = _ns_loadings(30.0, lam)[1]
    assert medium > short
    assert medium > long_
    assert medium > 0.0


# ---------------------------------------------------------------------------
# Unit tests for _ols3
# ---------------------------------------------------------------------------

def test_ols3_recovers_exact_ns():
    """OLS with the true λ recovers (β₀, β₁, β₂) exactly (up to float eps)."""
    b0, b1, b2, lam = 4.0, -1.5, 0.8, LAMBDA_FIXED
    yields_by_months = _exact_ns_yields(b0, b1, b2, lam)
    taus = [m / 12.0 for m in sorted(yields_by_months)]
    yields = [yields_by_months[m] for m in sorted(yields_by_months)]
    beta, rmse = _ols3(taus, yields, lam)
    assert beta is not None
    assert beta[0] == pytest.approx(b0, abs=1e-8)
    assert beta[1] == pytest.approx(b1, abs=1e-8)
    assert beta[2] == pytest.approx(b2, abs=1e-8)
    assert rmse == pytest.approx(0.0, abs=1e-8)


def test_ols3_rmse_nonneg():
    taus = [m / 12.0 for m in _TENORS_MO]
    yields = [2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 4.8, 5.0]
    _, rmse = _ols3(taus, yields, LAMBDA_FIXED)
    assert rmse >= 0.0


# ---------------------------------------------------------------------------
# compute_yield_curve_ns_factors
# ---------------------------------------------------------------------------

def test_empty_input():
    assert compute_yield_curve_ns_factors([]) == []


def test_output_schema():
    rows = _curve_rows("2024-01-02", _exact_ns_yields(4.0, -1.0, 0.5, LAMBDA_FIXED))
    result = compute_yield_curve_ns_factors(rows)
    assert len(result) == 1
    expected_keys = {
        "observation_date", "beta0", "beta1", "beta2",
        "lambda", "lambda_estimated", "fit_rmse", "n_tenors", "fit_valid",
    }
    assert set(result[0].keys()) == expected_keys


def test_observation_date_uses_as_of_date():
    rows = _curve_rows("2023-06-30", _exact_ns_yields(3.5, -0.5, 0.3, LAMBDA_FIXED))
    result = compute_yield_curve_ns_factors(rows)
    assert result[0]["observation_date"] == "2023-06-30"


def test_insufficient_tenors_fit_invalid():
    """Fewer than MIN_TENORS tenors → fit_valid=False, betas=None."""
    partial = dict(list(_exact_ns_yields(4.0, -1.0, 0.5, LAMBDA_FIXED).items())[:2])
    rows = _curve_rows("2024-01-02", partial)
    result = compute_yield_curve_ns_factors(rows)
    assert len(result) == 1
    r = result[0]
    assert r["fit_valid"] is False
    assert r["beta0"] is None
    assert r["beta1"] is None
    assert r["beta2"] is None
    assert r["lambda"] is None
    assert r["lambda_estimated"] is None
    assert r["fit_rmse"] is None
    assert r["n_tenors"] == 2


def test_exactly_min_tenors_is_valid():
    """MIN_TENORS tenors should be enough for a valid fit."""
    partial = dict(list(_exact_ns_yields(4.0, -1.0, 0.0, LAMBDA_FIXED).items())[:MIN_TENORS])
    rows = _curve_rows("2024-01-02", partial)
    result = compute_yield_curve_ns_factors(rows)
    assert result[0]["fit_valid"] is True
    assert result[0]["n_tenors"] == MIN_TENORS


def test_n_tenors_count():
    """n_tenors reflects the number of non-null tenors."""
    rows = _curve_rows("2024-01-02", _exact_ns_yields(4.0, -1.0, 0.5, LAMBDA_FIXED))
    r = compute_yield_curve_ns_factors(rows)[0]
    assert r["n_tenors"] == len(_TENORS_MO)


def test_flat_curve_beta1_beta2_near_zero():
    """A flat yield curve is β₀ = yield, β₁ ≈ 0, β₂ ≈ 0."""
    flat_level = 3.5
    flat = {m: flat_level for m in _TENORS_MO}
    rows = _curve_rows("2024-01-02", flat)
    r = compute_yield_curve_ns_factors(rows)[0]
    assert r["fit_valid"] is True
    assert r["beta0"] == pytest.approx(flat_level, abs=1e-6)
    assert r["beta1"] == pytest.approx(0.0, abs=1e-6)
    assert r["beta2"] == pytest.approx(0.0, abs=1e-6)
    assert r["fit_rmse"] == pytest.approx(0.0, abs=1e-6)


def test_exact_ns_zero_rmse():
    """Yields drawn from a perfect NS curve → fit_rmse ≈ 0."""
    rows = _curve_rows(
        "2024-01-02",
        _exact_ns_yields(4.5, -2.0, 1.0, LAMBDA_FIXED),
    )
    r = compute_yield_curve_ns_factors(rows)[0]
    assert r["fit_rmse"] == pytest.approx(0.0, abs=1e-7)
    assert r["lambda"] == pytest.approx(LAMBDA_FIXED)
    assert r["lambda_estimated"] is False


def test_beta0_approx_long_rate():
    """As τ → ∞, y → β₀; so β₀ should be close to the 30y yield."""
    b0, b1, b2 = 4.0, -1.5, 0.5
    rows = _curve_rows("2024-01-02", _exact_ns_yields(b0, b1, b2, LAMBDA_FIXED))
    r = compute_yield_curve_ns_factors(rows)[0]
    assert r["beta0"] == pytest.approx(b0, abs=1e-6)


def test_normal_slope_beta1_negative():
    """Normal upward-sloping curve (higher at long end) → β₁ < 0.

    In NS: short rate ≈ β₀ + β₁ (since L(0,λ)=1), long rate ≈ β₀.
    So β₁ = short - long < 0 for a normal curve.
    """
    # Yields rise from 1% (short) to 4% (long).
    yields = {1: 1.0, 3: 1.5, 6: 2.0, 12: 2.5, 24: 3.0, 60: 3.5, 120: 3.8, 360: 4.0}
    rows = _curve_rows("2024-01-02", yields)
    r = compute_yield_curve_ns_factors(rows)[0]
    assert r["fit_valid"] is True
    assert r["beta1"] < 0.0


def test_inverted_curve_beta1_positive():
    """Inverted curve (higher at short end) → β₁ > 0."""
    yields = {1: 5.0, 3: 4.8, 6: 4.5, 12: 4.2, 24: 3.8, 60: 3.5, 120: 3.3, 360: 3.0}
    rows = _curve_rows("2024-01-02", yields)
    r = compute_yield_curve_ns_factors(rows)[0]
    assert r["fit_valid"] is True
    assert r["beta1"] > 0.0


def test_lambda_fixed_no_grid_search():
    """A well-behaved NS curve should not trigger grid search."""
    rows = _curve_rows("2024-01-02", _exact_ns_yields(4.0, -1.0, 0.5, LAMBDA_FIXED))
    r = compute_yield_curve_ns_factors(rows)[0]
    assert r["lambda_estimated"] is False
    assert r["lambda"] == pytest.approx(LAMBDA_FIXED)


def test_grid_search_finds_true_lambda(monkeypatch):
    """When forced to grid-search (threshold patched to -1), the grid finds
    the λ that gives zero RMSE for an exact NS curve.

    The regular threshold (0.10 pp) may not be exceeded for moderate β₂
    values because the wrong-λ OLS can still find betas that minimise
    residuals reasonably well — only a very large β₂ guarantees the
    threshold is exceeded without monkeypatching.  This test verifies the
    grid-search logic directly.
    """
    true_lam = 3.5
    rows = _curve_rows("2024-01-02", _exact_ns_yields(4.0, -2.0, 2.0, true_lam))
    # Force grid search by lowering the RMSE threshold to -1 (always triggers).
    monkeypatch.setattr("fred_pipeline.ns_model.RMSE_GRID_THRESHOLD", -1.0)
    r = compute_yield_curve_ns_factors(rows)[0]
    assert r["fit_valid"] is True
    assert r["lambda_estimated"] is True
    # RMSE should be near zero since the grid hits λ=3.5.
    assert r["fit_rmse"] == pytest.approx(0.0, abs=1e-6)
    # Grid step is 0.1, so best λ should be within 0.1 of true.
    assert abs(r["lambda"] - true_lam) <= 0.1 + 1e-9


def test_multiple_dates_sorted():
    """Two dates produce two rows, sorted chronologically."""
    r1 = _curve_rows("2024-02-01", _exact_ns_yields(4.0, -1.0, 0.5, LAMBDA_FIXED))
    r2 = _curve_rows("2024-01-01", _exact_ns_yields(4.5, -1.5, 0.3, LAMBDA_FIXED))
    result = compute_yield_curve_ns_factors(r1 + r2)
    assert len(result) == 2
    assert result[0]["observation_date"] == "2024-01-01"
    assert result[1]["observation_date"] == "2024-02-01"


def test_skips_rows_with_none_fields():
    """Rows with None as_of_date, tenor_months, or yield_pct are ignored."""
    good = _curve_rows("2024-01-02", _exact_ns_yields(4.0, -1.0, 0.5, LAMBDA_FIXED))
    bad = [
        {"as_of_date": None, "tenor_months": 12, "yield_pct": 4.0},
        {"as_of_date": "2024-01-02", "tenor_months": None, "yield_pct": 4.0},
        {"as_of_date": "2024-01-02", "tenor_months": 12, "yield_pct": None},
    ]
    result = compute_yield_curve_ns_factors(good + bad)
    assert len(result) == 1
    # The bad rows don't corrupt n_tenors (good rows supply all 8 tenors).
    assert result[0]["n_tenors"] == len(_TENORS_MO)


def test_fit_rmse_is_float():
    rows = _curve_rows("2024-01-02", _exact_ns_yields(3.5, -0.5, 0.3, LAMBDA_FIXED))
    r = compute_yield_curve_ns_factors(rows)[0]
    assert isinstance(r["fit_rmse"], float)
    assert math.isfinite(r["fit_rmse"])


def test_fit_valid_is_bool():
    rows = _curve_rows("2024-01-02", _exact_ns_yields(3.5, -0.5, 0.3, LAMBDA_FIXED))
    r = compute_yield_curve_ns_factors(rows)[0]
    # Should be Python bool (or at least truthy).
    assert r["fit_valid"] is True

    partial = dict(list(_exact_ns_yields(3.5, -0.5, 0.3, LAMBDA_FIXED).items())[:1])
    r2 = compute_yield_curve_ns_factors(_curve_rows("2024-01-02", partial))[0]
    assert r2["fit_valid"] is False
