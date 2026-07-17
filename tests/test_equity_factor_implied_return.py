"""Tests for ML-5b: equity_factor_implied_return."""

from __future__ import annotations

import pytest

from fred_pipeline.equity_factor_attribution import (
    EquityFactorConfig,
    compute_equity_factor_implied_return,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _cfg(**overrides) -> EquityFactorConfig:
    defaults = dict(windows=(12,), min_obs=6, tickers=())
    defaults.update(overrides)
    return EquityFactorConfig(**defaults)


def _attrib_rows(
    ticker: str,
    months: list[str],
    alpha: float = 0.001,
    betas: dict[int, float] | None = None,
    window: int = 12,
) -> list[dict]:
    """Synthetic attribution rows (one per factor per month)."""
    if betas is None:
        betas = {1: 0.5, 2: -0.3}
    rows = []
    for obs in months:
        for fid, b in betas.items():
            rows.append({
                "ticker": ticker,
                "observation_date": obs,
                "window": window,
                "factor": fid,
                "beta": b,
                "t_stat": 2.0,
                "alpha": alpha,
                "r_squared": 0.3,
                "n_obs": window,
            })
    return rows


def _score_rows(months: list[str], scores: dict[int, float] | None = None) -> list[dict]:
    """Synthetic macro_factor_scores rows."""
    if scores is None:
        scores = {1: 1.0, 2: -1.0}
    rows = []
    for obs in months:
        for fid, s in scores.items():
            rows.append({
                "observation_date": obs,
                "factor": fid,
                "score": s,
            })
    return rows


def _return_rows(ticker: str, months: list[str], ret: float = 0.02) -> list[dict]:
    """Synthetic equity_return_daily rows (one per month, single day)."""
    return [
        {"ticker": ticker, "observation_date": obs, "price_return": ret}
        for obs in months
    ]


MONTHS_2Y = [f"200{y}-{m:02d}-01" for y in range(2) for m in range(1, 13)]


# ---------------------------------------------------------------------------
# Basic correctness
# ---------------------------------------------------------------------------

def test_implied_return_schema():
    rows = compute_equity_factor_implied_return(
        _attrib_rows("AAPL", MONTHS_2Y[:14]),
        _score_rows(MONTHS_2Y),
        _return_rows("AAPL", MONTHS_2Y),
    )
    assert rows
    for r in rows:
        for key in (
            "ticker", "observation_date", "window",
            "implied_return", "factor_return", "alpha_return",
            "realized_return", "residual_return",
        ):
            assert key in r, f"missing key {key!r}"


def test_implied_return_arithmetic():
    # β₁=0.5, β₂=-0.3; F₁=1.0, F₂=-1.0; α=0.001
    # factor_return = 0.5*1.0 + (-0.3)*(-1.0) = 0.5 + 0.3 = 0.8
    # implied = 0.001 + 0.8 = 0.801
    months = MONTHS_2Y[:14]
    rows = compute_equity_factor_implied_return(
        _attrib_rows("AAPL", months),
        _score_rows(months, {1: 1.0, 2: -1.0}),
        _return_rows("AAPL", months, ret=0.02),
    )
    assert rows
    r = rows[-1]
    assert r["factor_return"] == pytest.approx(0.8, abs=1e-10)
    assert r["implied_return"] == pytest.approx(0.801, abs=1e-10)
    assert r["alpha_return"] == pytest.approx(0.001, abs=1e-10)


def test_residual_return_arithmetic():
    # realized=0.02, implied=0.801 → residual=0.02-0.801=-0.781
    months = MONTHS_2Y[:14]
    rows = compute_equity_factor_implied_return(
        _attrib_rows("AAPL", months),
        _score_rows(months, {1: 1.0, 2: -1.0}),
        _return_rows("AAPL", months, ret=0.02),
    )
    r = rows[-1]
    assert r["residual_return"] == pytest.approx(
        r["realized_return"] - r["implied_return"], abs=1e-10
    )


def test_ticker_correct():
    rows = compute_equity_factor_implied_return(
        _attrib_rows("MSFT", MONTHS_2Y[:14]),
        _score_rows(MONTHS_2Y),
        _return_rows("MSFT", MONTHS_2Y),
    )
    assert all(r["ticker"] == "MSFT" for r in rows)


def test_window_preserved():
    rows = compute_equity_factor_implied_return(
        _attrib_rows("AAPL", MONTHS_2Y[:14], window=36),
        _score_rows(MONTHS_2Y),
        _return_rows("AAPL", MONTHS_2Y),
    )
    assert all(r["window"] == 36 for r in rows)


# ---------------------------------------------------------------------------
# Multiple tickers / windows
# ---------------------------------------------------------------------------

def test_two_tickers_independent():
    attrib = (
        _attrib_rows("AAPL", MONTHS_2Y[:14], betas={1: 0.5}, alpha=0.001)
        + _attrib_rows("MSFT", MONTHS_2Y[:14], betas={1: 0.8}, alpha=0.002)
    )
    scores = _score_rows(MONTHS_2Y, {1: 1.0})
    returns = (
        _return_rows("AAPL", MONTHS_2Y, 0.01)
        + _return_rows("MSFT", MONTHS_2Y, 0.03)
    )
    out = compute_equity_factor_implied_return(attrib, scores, returns)
    aapl = [r for r in out if r["ticker"] == "AAPL"]
    msft = [r for r in out if r["ticker"] == "MSFT"]
    assert aapl and msft
    # AAPL implied = 0.001 + 0.5*1.0 = 0.501
    assert aapl[-1]["implied_return"] == pytest.approx(0.501, abs=1e-9)
    # MSFT implied = 0.002 + 0.8*1.0 = 0.802
    assert msft[-1]["implied_return"] == pytest.approx(0.802, abs=1e-9)


def test_two_windows_both_emitted():
    attrib = (
        _attrib_rows("AAPL", MONTHS_2Y[:14], window=12)
        + _attrib_rows("AAPL", MONTHS_2Y[:14], window=36)
    )
    out = compute_equity_factor_implied_return(
        attrib, _score_rows(MONTHS_2Y), _return_rows("AAPL", MONTHS_2Y)
    )
    windows = {r["window"] for r in out}
    assert 12 in windows
    assert 36 in windows


# ---------------------------------------------------------------------------
# Forward-fill of betas
# ---------------------------------------------------------------------------

def test_forward_fill_uses_latest_betas():
    """Betas estimated through month 14 should be used for months 15+."""
    attrib = _attrib_rows("AAPL", MONTHS_2Y[:14])   # betas through month 14
    scores = _score_rows(MONTHS_2Y)                  # factor scores for all 24 months
    returns = _return_rows("AAPL", MONTHS_2Y)

    out = compute_equity_factor_implied_return(attrib, scores, returns)
    # Should have rows for months 15-24 (forward-filled betas)
    obs_dates = {r["observation_date"] for r in out}
    assert MONTHS_2Y[14] in obs_dates  # month 15 (index 14)
    assert MONTHS_2Y[23] in obs_dates  # month 24 (index 23)


def test_no_rows_before_first_beta():
    """No output for months before the first beta estimate."""
    # Betas start at month 12 (index 12)
    attrib = _attrib_rows("AAPL", MONTHS_2Y[12:14])
    scores = _score_rows(MONTHS_2Y)
    returns = _return_rows("AAPL", MONTHS_2Y)

    out = compute_equity_factor_implied_return(attrib, scores, returns)
    obs_dates = {r["observation_date"] for r in out}
    # Months before index 12 should not appear
    for early_month in MONTHS_2Y[:12]:
        assert early_month not in obs_dates


# ---------------------------------------------------------------------------
# Missing / no realized return
# ---------------------------------------------------------------------------

def test_residual_none_when_no_realized_return():
    """residual_return is None when no realized equity return for that month."""
    attrib = _attrib_rows("AAPL", MONTHS_2Y[:14])
    scores = _score_rows(MONTHS_2Y)
    # No equity return rows at all
    out = compute_equity_factor_implied_return(attrib, scores, [])
    assert out  # still get rows (factor scores exist)
    for r in out:
        assert r["realized_return"] is None
        assert r["residual_return"] is None
        # But implied still computed
        assert r["implied_return"] is not None


# ---------------------------------------------------------------------------
# Empty / degenerate inputs
# ---------------------------------------------------------------------------

def test_empty_attribution_rows_returns_empty():
    out = compute_equity_factor_implied_return(
        [], _score_rows(MONTHS_2Y), _return_rows("AAPL", MONTHS_2Y)
    )
    assert out == []


def test_empty_factor_scores_returns_empty():
    out = compute_equity_factor_implied_return(
        _attrib_rows("AAPL", MONTHS_2Y[:14]), [], _return_rows("AAPL", MONTHS_2Y)
    )
    assert out == []


def test_ticker_filter_via_config():
    """With tickers=("AAPL",), MSFT rows are excluded from both attribution and output."""
    attrib = (
        _attrib_rows("AAPL", MONTHS_2Y[:14])
        + _attrib_rows("MSFT", MONTHS_2Y[:14])
    )
    cfg = _cfg(tickers=("AAPL",))
    out = compute_equity_factor_implied_return(
        attrib, _score_rows(MONTHS_2Y), _return_rows("AAPL", MONTHS_2Y), cfg=cfg
    )
    assert all(r["ticker"] == "AAPL" for r in out)
    assert not any(r["ticker"] == "MSFT" for r in out)


def test_implied_return_is_float():
    out = compute_equity_factor_implied_return(
        _attrib_rows("AAPL", MONTHS_2Y[:14]),
        _score_rows(MONTHS_2Y),
        _return_rows("AAPL", MONTHS_2Y),
    )
    for r in out:
        assert isinstance(r["implied_return"], float)
        assert isinstance(r["factor_return"], float)
        assert isinstance(r["alpha_return"], float)
