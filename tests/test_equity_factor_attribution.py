"""Tests for ML-5: equity_factor_attribution (rolling OLS on PCA factors)."""

from __future__ import annotations

import math

import pytest

from fred_pipeline.equity_factor_attribution import (
    DEFAULT_WINDOWS,
    EquityFactorConfig,
    _factor_matrix,
    _monthly_returns,
    _ols,
    _solve,
    compute_equity_factor_attribution,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _eq_rows(ticker: str, monthly_returns: list[tuple[str, float | None]]) -> list[dict]:
    """Build synthetic equity_return_daily rows (one per month, single day)."""
    return [
        {"ticker": ticker, "observation_date": obs_date, "price_return": ret}
        for obs_date, ret in monthly_returns
    ]


def _fac_rows(data: list[tuple[str, int, float]]) -> list[dict]:
    """Build synthetic macro_factor_scores rows."""
    return [
        {"observation_date": obs_date, "factor": factor, "score": score}
        for obs_date, factor, score in data
    ]


def _monthly_seq(n: int, start_year: int = 2000, ret: float = 0.01) -> list[tuple[str, float]]:
    """n months of (date_str, ret) starting at start_year-01-15."""
    out = []
    for i in range(n):
        year = start_year + (i // 12)
        month = (i % 12) + 1
        out.append((f"{year}-{month:02d}-15", ret))
    return out


def _factor_seq(n: int, start_year: int = 2000, n_factors: int = 1) -> list[tuple[str, int, float]]:
    """n months of factor scores for n_factors factors."""
    out = []
    for i in range(n):
        year = start_year + (i // 12)
        month = (i % 12) + 1
        date_str = f"{year}-{month:02d}-15"
        for f in range(1, n_factors + 1):
            out.append((date_str, f, float(i + f)))
    return out


# ---------------------------------------------------------------------------
# _solve
# ---------------------------------------------------------------------------

def test_solve_2x2():
    A = [[2.0, 1.0], [1.0, 3.0]]
    b = [5.0, 10.0]
    x = _solve(A, b)
    assert x is not None
    assert x[0] == pytest.approx(1.0, abs=1e-9)
    assert x[1] == pytest.approx(3.0, abs=1e-9)


def test_solve_identity():
    A = [[1.0, 0.0], [0.0, 1.0]]
    b = [3.7, -2.1]
    x = _solve(A, b)
    assert x is not None
    assert x[0] == pytest.approx(3.7)
    assert x[1] == pytest.approx(-2.1)


def test_solve_singular_returns_none():
    A = [[1.0, 2.0], [2.0, 4.0]]  # rank-1
    b = [1.0, 2.0]
    assert _solve(A, b) is None


# ---------------------------------------------------------------------------
# _ols
# ---------------------------------------------------------------------------

def test_ols_perfect_linear_fit():
    x = [float(i) for i in range(20)]
    y = [2.0 + 3.0 * xi for xi in x]  # alpha=2, beta=3
    res = _ols(y, [x])
    assert res is not None
    assert res["alpha"] == pytest.approx(2.0, abs=1e-6)
    assert res["betas"][0] == pytest.approx(3.0, abs=1e-6)
    assert res["r_squared"] == pytest.approx(1.0, abs=1e-6)


def test_ols_r_squared_zero_for_no_relationship():
    y = [1.0, 2.0, 1.0, 2.0, 1.0, 2.0, 1.0, 2.0, 1.0, 2.0,
         1.0, 2.0, 1.0, 2.0, 1.0, 2.0, 1.0, 2.0, 1.0, 2.0]
    x = list(range(20))
    res = _ols(y, [x])
    assert res is not None
    # With a perfectly alternating y, a linear x has near-zero R²
    assert res["r_squared"] < 0.3


def test_ols_returns_none_when_n_le_p():
    y = [1.0, 2.0]     # 2 obs
    x = [0.0, 1.0]     # 1 factor → p=2, n==p → None
    assert _ols(y, [x]) is None


def test_ols_singular_design_returns_none():
    # Two identical predictors → XtX singular
    x = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0,
         11.0, 12.0, 13.0, 14.0, 15.0]
    y = [v + 0.1 for v in x]
    res = _ols(y, [x, x])  # duplicate column → singular
    assert res is None


def test_ols_alpha_intercept_known():
    # y = 5 + 0*x  (constant, no slope)
    x = [float(i) for i in range(20)]
    y = [5.0] * 20
    # SST=0 → we get a degenerate result but alpha should be 5
    res = _ols(y, [x])
    assert res is not None
    assert res["alpha"] == pytest.approx(5.0, abs=1e-6)


def test_ols_t_stat_sign_matches_beta():
    x = [float(i) for i in range(20)]
    y = [3.0 * xi + 0.01 * (-1) ** int(xi) for xi in x]  # positive slope
    res = _ols(y, [x])
    assert res is not None
    assert res["betas"][0] > 0
    assert res["t_stats"][0] is not None and res["t_stats"][0] > 0


def test_ols_r_squared_bounded():
    import random
    rng = random.Random(42)
    x = [rng.gauss(0, 1) for _ in range(30)]
    y = [2.0 * xi + rng.gauss(0, 0.5) for xi in x]
    res = _ols(y, [x])
    assert res is not None
    assert 0.0 <= res["r_squared"] <= 1.0


# ---------------------------------------------------------------------------
# _monthly_returns
# ---------------------------------------------------------------------------

def test_monthly_returns_single_month():
    rows = [{"ticker": "AAPL", "observation_date": "2020-01-10", "price_return": 0.01},
            {"ticker": "AAPL", "observation_date": "2020-01-20", "price_return": 0.02}]
    result = _monthly_returns(rows, frozenset())
    aapl = result["AAPL"]
    assert (2020, 1) in aapl
    # (1.01)(1.02) - 1 = 0.0302
    assert aapl[(2020, 1)] == pytest.approx(0.0302, abs=1e-9)


def test_monthly_returns_none_is_skipped():
    rows = [{"ticker": "AAPL", "observation_date": "2020-01-01", "price_return": None},
            {"ticker": "AAPL", "observation_date": "2020-01-15", "price_return": 0.05}]
    result = _monthly_returns(rows, frozenset())
    # Month exists because of the second row (0.05); first row skipped
    assert (2020, 1) in result["AAPL"]
    assert result["AAPL"][(2020, 1)] == pytest.approx(0.05, abs=1e-9)


def test_monthly_returns_month_with_only_none_excluded():
    rows = [{"ticker": "X", "observation_date": "2020-01-15", "price_return": None}]
    result = _monthly_returns(rows, frozenset())
    assert result == {}


def test_monthly_returns_multiple_months():
    rows = [
        {"ticker": "A", "observation_date": "2020-01-15", "price_return": 0.01},
        {"ticker": "A", "observation_date": "2020-02-15", "price_return": 0.02},
    ]
    result = _monthly_returns(rows, frozenset())
    assert (2020, 1) in result["A"]
    assert (2020, 2) in result["A"]


def test_monthly_returns_wanted_tickers_filter():
    rows = [
        {"ticker": "AAPL", "observation_date": "2020-01-15", "price_return": 0.01},
        {"ticker": "GOOG", "observation_date": "2020-01-15", "price_return": 0.02},
    ]
    result = _monthly_returns(rows, frozenset({"AAPL"}))
    assert "AAPL" in result
    assert "GOOG" not in result


def test_monthly_returns_empty():
    assert _monthly_returns([], frozenset()) == {}


# ---------------------------------------------------------------------------
# _factor_matrix
# ---------------------------------------------------------------------------

def test_factor_matrix_basic():
    rows = [
        {"observation_date": "2020-01-15", "factor": 1, "score": 0.5},
        {"observation_date": "2020-01-15", "factor": 2, "score": -0.3},
    ]
    scores, dates = _factor_matrix(rows)
    assert (2020, 1) in scores
    assert scores[(2020, 1)][1] == pytest.approx(0.5)
    assert scores[(2020, 1)][2] == pytest.approx(-0.3)
    assert dates[(2020, 1)] == "2020-01-15"


def test_factor_matrix_multiple_months():
    rows = _fac_rows(_factor_seq(3, n_factors=2))
    scores, dates = _factor_matrix(rows)
    assert len(scores) == 3


def test_factor_matrix_invalid_date_skipped():
    rows = [{"observation_date": "not-a-date", "factor": 1, "score": 1.0}]
    scores, _ = _factor_matrix(rows)
    assert scores == {}


# ---------------------------------------------------------------------------
# compute_equity_factor_attribution
# ---------------------------------------------------------------------------

def _simple_setup(n_months: int = 36, n_factors: int = 2, ret: float = 0.01):
    """Return (equity_rows, factor_rows, cfg) for a simple single-ticker test."""
    eq = _eq_rows("SPY", _monthly_seq(n_months, ret=ret))
    fac = _fac_rows(_factor_seq(n_months, n_factors=n_factors))
    cfg = EquityFactorConfig(windows=(12,), min_obs=12, tickers=())
    return eq, fac, cfg


def test_attribution_empty_equity():
    _, fac, cfg = _simple_setup()
    assert compute_equity_factor_attribution([], fac, cfg) == []


def test_attribution_empty_factors():
    eq, _, cfg = _simple_setup()
    assert compute_equity_factor_attribution(eq, [], cfg) == []


def test_attribution_too_short():
    eq = _eq_rows("SPY", _monthly_seq(5, ret=0.01))
    fac = _fac_rows(_factor_seq(5, n_factors=1))
    cfg = EquityFactorConfig(windows=(12,), min_obs=12, tickers=())
    assert compute_equity_factor_attribution(eq, fac, cfg) == []


def test_attribution_output_schema():
    eq, fac, cfg = _simple_setup(36, n_factors=2)
    result = compute_equity_factor_attribution(eq, fac, cfg)
    assert len(result) > 0
    r = result[0]
    expected_keys = {"ticker", "observation_date", "window", "factor",
                     "beta", "t_stat", "alpha", "r_squared", "n_obs"}
    assert set(r.keys()) == expected_keys


def test_attribution_factor_ids_match_input():
    eq, fac, cfg = _simple_setup(36, n_factors=3)
    result = compute_equity_factor_attribution(eq, fac, cfg)
    factors = {r["factor"] for r in result}
    assert factors == {1, 2, 3}


def test_attribution_n_obs_equals_window():
    eq, fac, cfg = _simple_setup(36, n_factors=1)
    result = compute_equity_factor_attribution(eq, fac, cfg)
    for r in result:
        assert r["n_obs"] == r["window"]


def test_attribution_r_squared_in_unit_interval():
    eq, fac, cfg = _simple_setup(48, n_factors=2)
    result = compute_equity_factor_attribution(eq, fac, cfg)
    for r in result:
        assert 0.0 <= r["r_squared"] <= 1.0


def test_attribution_alpha_repeats_across_factors():
    eq, fac, cfg = _simple_setup(36, n_factors=2)
    result = compute_equity_factor_attribution(eq, fac, cfg)
    # Group by (ticker, window, observation_date)
    by_twk: dict[tuple, list[dict]] = {}
    for r in result:
        key = (r["ticker"], r["window"], r["observation_date"])
        by_twk.setdefault(key, []).append(r)
    for rows in by_twk.values():
        alphas = {r["alpha"] for r in rows}
        assert len(alphas) == 1, "alpha must be identical across factor rows at same (ticker,window,date)"


def test_attribution_r_squared_repeats_across_factors():
    eq, fac, cfg = _simple_setup(36, n_factors=2)
    result = compute_equity_factor_attribution(eq, fac, cfg)
    by_twk: dict[tuple, set[float]] = {}
    for r in result:
        key = (r["ticker"], r["window"], r["observation_date"])
        by_twk.setdefault(key, set()).add(r["r_squared"])
    for r2_set in by_twk.values():
        assert len(r2_set) == 1


def test_attribution_multiple_tickers_independent():
    eq = (
        _eq_rows("AAA", _monthly_seq(36, ret=0.01))
        + _eq_rows("BBB", _monthly_seq(36, ret=0.02))
    )
    fac = _fac_rows(_factor_seq(36, n_factors=1))
    cfg = EquityFactorConfig(windows=(12,), min_obs=12, tickers=())
    result = compute_equity_factor_attribution(eq, fac, cfg)
    tickers = {r["ticker"] for r in result}
    assert tickers == {"AAA", "BBB"}


def test_attribution_sorted_by_ticker_then_date():
    eq = (
        _eq_rows("ZZZ", _monthly_seq(36, ret=0.01))
        + _eq_rows("AAA", _monthly_seq(36, ret=0.01))
    )
    fac = _fac_rows(_factor_seq(36, n_factors=1))
    cfg = EquityFactorConfig(windows=(12,), min_obs=12, tickers=())
    result = compute_equity_factor_attribution(eq, fac, cfg)
    keys = [(r["ticker"], r["observation_date"]) for r in result]
    assert keys == sorted(keys)


def test_attribution_window_values_in_output():
    eq, fac, _ = _simple_setup(60, n_factors=1)
    cfg = EquityFactorConfig(windows=(12, 36), min_obs=12, tickers=())
    result = compute_equity_factor_attribution(eq, fac, cfg)
    windows = {r["window"] for r in result}
    assert windows == {12, 36}


def test_attribution_tickers_filter():
    eq = (
        _eq_rows("SPY", _monthly_seq(36, ret=0.01))
        + _eq_rows("QQQ", _monthly_seq(36, ret=0.015))
    )
    fac = _fac_rows(_factor_seq(36, n_factors=1))
    cfg = EquityFactorConfig(windows=(12,), min_obs=12, tickers=("SPY",))
    result = compute_equity_factor_attribution(eq, fac, cfg)
    tickers = {r["ticker"] for r in result}
    assert tickers == {"SPY"}
    assert "QQQ" not in tickers


def test_attribution_default_windows():
    eq, fac, _ = _simple_setup(70, n_factors=1)
    cfg = EquityFactorConfig(windows=DEFAULT_WINDOWS, min_obs=12, tickers=())
    result = compute_equity_factor_attribution(eq, fac, cfg)
    windows = {r["window"] for r in result}
    assert windows == set(DEFAULT_WINDOWS)


def test_attribution_known_beta():
    # y = 0.01 + 0.5 * f1  (exact; no noise) → beta_1 ≈ 0.5, alpha ≈ 0.01
    n = 24
    dates = [f"2000-{(i % 12) + 1:02d}-15" if i < 12 else
             f"2001-{(i % 12) + 1:02d}-15" for i in range(n)]
    factor_vals = [float(i) * 0.01 for i in range(n)]

    eq = [{"ticker": "X", "observation_date": dates[i], "price_return": 0.01 + 0.5 * factor_vals[i]}
          for i in range(n)]
    fac = [{"observation_date": dates[i], "factor": 1, "score": factor_vals[i]}
           for i in range(n)]
    cfg = EquityFactorConfig(windows=(12,), min_obs=12, tickers=())
    result = compute_equity_factor_attribution(eq, fac, cfg)
    # All rows should have beta ≈ 0.5 and alpha ≈ 0.01
    for r in result:
        assert r["beta"] == pytest.approx(0.5, abs=1e-6)
        assert r["alpha"] == pytest.approx(0.01, abs=1e-6)


def test_attribution_observation_date_is_string():
    eq, fac, cfg = _simple_setup(24, n_factors=1)
    result = compute_equity_factor_attribution(eq, fac, cfg)
    for r in result:
        assert isinstance(r["observation_date"], str)
