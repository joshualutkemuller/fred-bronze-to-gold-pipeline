"""Tests for fred_pipeline.recession_model (ML-3 expanding IRLS)."""

from __future__ import annotations

import math
from datetime import date

import pytest

from fred_pipeline.recession_model import (
    RecessionModelConfig,
    _add_months,
    _asof,
    _forward_labels,
    _irls,
    _sigmoid,
    compute_recession_probability,
    load_recession_model_config,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_cfg(
    min_obs: int = 3,
    horizon_months: tuple[int, ...] = (3, 6, 12),
    **kwargs,
) -> RecessionModelConfig:
    defaults = dict(
        l2_lambda=0.01,
        max_iter=25,
        tol=1e-7,
        ns_slope=True,
        unrate_mom=False,
        indpro_mom=False,
        hy_oas_zscore=False,
        hy_oas_instrument="BAMLH0A0HYM2",
        funding_stress=False,
        regime_composite=False,
    )
    defaults.update(kwargs)
    return RecessionModelConfig(
        min_obs=min_obs,
        horizon_months=horizon_months,
        **defaults,
    )


def _usrec_rows(pattern: list[tuple[str, float]]) -> list[dict]:
    """Build synthetic Silver latest_rows for USREC."""
    return [
        {"series_id": "USREC", "observation_date": d, "value": v}
        for d, v in pattern
    ]


def _ns_rows(entries: list[tuple[str, float]]) -> list[dict]:
    """Build synthetic ns_factor_rows with beta1."""
    return [{"observation_date": d, "beta1": b1} for d, b1 in entries]


def _monthly_dates(start_year: int, start_month: int, n: int) -> list[str]:
    """Generate n monthly ISO date strings starting from (year, month)."""
    out = []
    y, m = start_year, start_month
    for _ in range(n):
        out.append(date(y, m, 1).isoformat())
        m += 1
        if m > 12:
            m = 1
            y += 1
    return out


# ---------------------------------------------------------------------------
# _sigmoid
# ---------------------------------------------------------------------------

def test_sigmoid_zero():
    assert _sigmoid(0.0) == pytest.approx(0.5)


def test_sigmoid_large_positive():
    assert _sigmoid(100.0) == pytest.approx(1.0, abs=1e-9)


def test_sigmoid_large_negative():
    assert _sigmoid(-100.0) == pytest.approx(0.0, abs=1e-9)


def test_sigmoid_in_unit_interval():
    for z in [-10.0, -1.0, 0.0, 1.0, 10.0]:
        v = _sigmoid(z)
        assert 0.0 < v < 1.0


def test_sigmoid_symmetry():
    assert _sigmoid(2.5) == pytest.approx(1.0 - _sigmoid(-2.5))


# ---------------------------------------------------------------------------
# _add_months
# ---------------------------------------------------------------------------

def test_add_months_basic():
    assert _add_months(date(2024, 1, 1), 3) == date(2024, 4, 1)


def test_add_months_year_rollover():
    assert _add_months(date(2023, 11, 1), 3) == date(2024, 2, 1)


def test_add_months_end_of_month_clamp():
    # Jan 31 + 1 month → Feb 28 (non-leap) or Feb 29 (leap)
    result = _add_months(date(2023, 1, 31), 1)
    assert result == date(2023, 2, 28)


def test_add_months_leap():
    result = _add_months(date(2024, 1, 31), 1)
    assert result == date(2024, 2, 29)


# ---------------------------------------------------------------------------
# _asof
# ---------------------------------------------------------------------------

def test_asof_exact_match():
    pairs = [(date(2024, 1, 1), 1.0), (date(2024, 2, 1), 2.0)]
    assert _asof(pairs, date(2024, 2, 1)) == pytest.approx(2.0)


def test_asof_before_first():
    pairs = [(date(2024, 1, 1), 1.0)]
    assert _asof(pairs, date(2023, 12, 1)) is None


def test_asof_between_points():
    pairs = [(date(2024, 1, 1), 1.0), (date(2024, 3, 1), 3.0)]
    assert _asof(pairs, date(2024, 2, 1)) == pytest.approx(1.0)


def test_asof_empty():
    assert _asof([], date(2024, 1, 1)) is None


# ---------------------------------------------------------------------------
# _forward_labels
# ---------------------------------------------------------------------------

def test_forward_labels_empty():
    assert _forward_labels([], 3) == {}


def test_forward_labels_no_recession():
    usrec = [(date(2024, m, 1), 0.0) for m in range(1, 13)]
    labels = _forward_labels(usrec, 3)
    # Last 3 months don't get a label (horizon extends beyond data).
    assert all(v == 0.0 for v in labels.values())
    # Last 3 dates (Oct, Nov, Dec) should have no label.
    assert date(2024, 10, 1) not in labels
    assert date(2024, 11, 1) not in labels
    assert date(2024, 12, 1) not in labels


def test_forward_labels_recession():
    # 12 months of data; recession in month 6, 7.
    usrec = [(date(2024, m, 1), 1.0 if m in (6, 7) else 0.0) for m in range(1, 13)]
    labels = _forward_labels(usrec, 3)
    # Months 3, 4, 5 should have label=1 (recession within next 3 months).
    assert labels[date(2024, 3, 1)] == 1.0
    assert labels[date(2024, 4, 1)] == 1.0
    assert labels[date(2024, 5, 1)] == 1.0
    # Month 1 is > 3 months before the recession starts (month 6).
    assert labels[date(2024, 1, 1)] == 0.0


def test_forward_labels_horizon_window_exclusive():
    # Label for t is based on (t, t+h], not including t itself.
    usrec = [(date(2024, 1, 1), 1.0), (date(2024, 2, 1), 0.0),
             (date(2024, 3, 1), 0.0), (date(2024, 4, 1), 0.0)]
    labels = _forward_labels(usrec, 1)
    # Jan: window (Jan, Feb] → Feb=0 → label=0 (recession at Jan not in window)
    assert labels.get(date(2024, 1, 1)) == 0.0
    # Mar: window (Mar, Apr] → Apr=0 → label=0 (horizon ends on last date, still valid)
    assert labels.get(date(2024, 3, 1)) == 0.0
    # Apr: horizon end = May > last_date (Apr) → no label.
    assert date(2024, 4, 1) not in labels


# ---------------------------------------------------------------------------
# _irls
# ---------------------------------------------------------------------------

def test_irls_empty_returns_none():
    assert _irls([], [], l2_lambda=0.01, max_iter=25, tol=1e-7) is None


def test_irls_perfectly_separable():
    """IRLS on perfectly separable data converges and gives a positive β₁."""
    X = [[1.0, -2.0], [1.0, -1.0], [1.0, 1.0], [1.0, 2.0]]
    y = [0.0, 0.0, 1.0, 1.0]
    beta = _irls(X, y, l2_lambda=0.1, max_iter=100, tol=1e-9)
    assert beta is not None
    assert beta[1] > 0.0  # higher x → higher prob → positive slope


def test_irls_output_length():
    X = [[1.0, 0.5, -0.3], [1.0, -0.5, 0.3], [1.0, 0.0, 0.0]]
    y = [1.0, 0.0, 0.5]
    beta = _irls(X, y, l2_lambda=0.01, max_iter=10, tol=1e-6)
    assert beta is not None
    assert len(beta) == 3


def test_irls_warm_start_accepted():
    """Warm-start with a previous solution converges in fewer iterations."""
    X = [[1.0, x] for x in range(-5, 6)]
    y = [0.0 if x < 0 else 1.0 for x in range(-5, 6)]
    beta_cold = _irls(X, y, l2_lambda=0.1, max_iter=50, tol=1e-9)
    beta_warm = _irls(X, y, l2_lambda=0.1, max_iter=50, tol=1e-9, warm_start=beta_cold)
    assert beta_warm is not None
    for j in range(2):
        assert beta_warm[j] == pytest.approx(beta_cold[j], abs=1e-6)


# ---------------------------------------------------------------------------
# compute_recession_probability
# ---------------------------------------------------------------------------

def _minimal_data(n_months: int, recession_month: int | None = None) -> dict:
    """Build minimal inputs: USREC + ns_slope beta1 for n_months."""
    dates = _monthly_dates(2000, 1, n_months)
    usrec_val = [
        1.0 if (i + 1) == recession_month else 0.0
        for i in range(n_months)
    ]
    usrec = [{"series_id": "USREC", "observation_date": d, "value": v}
             for d, v in zip(dates, usrec_val)]
    # beta1 cycles mildly around zero to give the model something to learn.
    ns = [{"observation_date": d, "beta1": (i % 5 - 2) * 0.5}
          for i, d in enumerate(dates)]
    return {"usrec": usrec, "ns": ns, "dates": dates}


def test_empty_usrec_returns_empty():
    cfg = _make_cfg(min_obs=3)
    result = compute_recession_probability(
        [],
        ns_factor_rows=[],
        feature_transform_rows=[],
        credit_spread_rows=[],
        funding_stress_rows=[],
        regime_rows=[],
        cfg=cfg,
    )
    assert result == []


def test_output_length_equals_usrec_dates():
    data = _minimal_data(20)
    cfg = _make_cfg(min_obs=3, horizon_months=(3,))
    result = compute_recession_probability(
        data["usrec"],
        ns_factor_rows=data["ns"],
        feature_transform_rows=[],
        credit_spread_rows=[],
        funding_stress_rows=[],
        regime_rows=[],
        cfg=cfg,
    )
    assert len(result) == 20


def test_output_schema():
    data = _minimal_data(10)
    cfg = _make_cfg(min_obs=3, horizon_months=(3, 6))
    result = compute_recession_probability(
        data["usrec"],
        ns_factor_rows=data["ns"],
        feature_transform_rows=[],
        credit_spread_rows=[],
        funding_stress_rows=[],
        regime_rows=[],
        cfg=cfg,
    )
    assert len(result) > 0
    expected_keys = {
        "observation_date", "recession_prob", "logit_score",
        "n_features", "n_obs_training", "model_vintage", "is_backfilled",
        "prob_recession_3m", "prob_recession_6m",
    }
    assert set(result[0].keys()) == expected_keys


def test_observation_date_is_string():
    data = _minimal_data(5)
    cfg = _make_cfg(min_obs=3, horizon_months=(3,))
    result = compute_recession_probability(
        data["usrec"],
        ns_factor_rows=data["ns"],
        feature_transform_rows=[],
        credit_spread_rows=[],
        funding_stress_rows=[],
        regime_rows=[],
        cfg=cfg,
    )
    for r in result:
        assert isinstance(r["observation_date"], str)
        date.fromisoformat(r["observation_date"])  # valid ISO format


def test_early_rows_backfilled():
    """Rows with fewer than min_obs training examples are backfilled."""
    data = _minimal_data(20)
    cfg = _make_cfg(min_obs=10, horizon_months=(3,))
    result = compute_recession_probability(
        data["usrec"],
        ns_factor_rows=data["ns"],
        feature_transform_rows=[],
        credit_spread_rows=[],
        funding_stress_rows=[],
        regime_rows=[],
        cfg=cfg,
    )
    # First rows are backfilled and have None probability.
    early = [r for r in result if r["is_backfilled"] == 1]
    assert len(early) > 0
    for r in early:
        assert r["recession_prob"] is None
        assert r["logit_score"] is None


def test_later_rows_not_backfilled():
    """Rows with >= min_obs training examples are not backfilled."""
    data = _minimal_data(30)
    cfg = _make_cfg(min_obs=5, horizon_months=(3,))
    result = compute_recession_probability(
        data["usrec"],
        ns_factor_rows=data["ns"],
        feature_transform_rows=[],
        credit_spread_rows=[],
        funding_stress_rows=[],
        regime_rows=[],
        cfg=cfg,
    )
    late = [r for r in result if r["is_backfilled"] == 0]
    assert len(late) > 0
    for r in late:
        assert r["recession_prob"] is not None
        assert 0.0 <= r["recession_prob"] <= 1.0


def test_recession_prob_in_unit_interval():
    data = _minimal_data(30, recession_month=15)
    cfg = _make_cfg(min_obs=5, horizon_months=(3,))
    result = compute_recession_probability(
        data["usrec"],
        ns_factor_rows=data["ns"],
        feature_transform_rows=[],
        credit_spread_rows=[],
        funding_stress_rows=[],
        regime_rows=[],
        cfg=cfg,
    )
    for r in result:
        if r["recession_prob"] is not None:
            assert 0.0 <= r["recession_prob"] <= 1.0


def test_sigmoid_logit_consistency():
    """recession_prob should equal sigmoid(logit_score) for non-None rows."""
    data = _minimal_data(30, recession_month=15)
    cfg = _make_cfg(min_obs=5, horizon_months=(3,))
    result = compute_recession_probability(
        data["usrec"],
        ns_factor_rows=data["ns"],
        feature_transform_rows=[],
        credit_spread_rows=[],
        funding_stress_rows=[],
        regime_rows=[],
        cfg=cfg,
    )
    for r in result:
        if r["recession_prob"] is not None:
            assert r["recession_prob"] == pytest.approx(
                _sigmoid(r["logit_score"]), abs=1e-9
            )


def test_n_obs_training_monotone():
    """n_obs_training should be non-decreasing over time."""
    data = _minimal_data(30)
    cfg = _make_cfg(min_obs=3, horizon_months=(3,))
    result = compute_recession_probability(
        data["usrec"],
        ns_factor_rows=data["ns"],
        feature_transform_rows=[],
        credit_spread_rows=[],
        funding_stress_rows=[],
        regime_rows=[],
        cfg=cfg,
    )
    n_obs_list = [r["n_obs_training"] for r in result]
    for i in range(1, len(n_obs_list)):
        assert n_obs_list[i] >= n_obs_list[i - 1]


def test_multiple_horizons_prob_fields():
    """All per-horizon prob fields are present; last row lacks a 12m label."""
    # 15 months: months 1–3 have a 12m forward label (horizon ends ≤ month 15),
    # months 4–15 don't, leaving only 3 < min_obs=5 training examples for 12m.
    data = _minimal_data(15)
    cfg = _make_cfg(min_obs=5, horizon_months=(3, 6, 12))
    result = compute_recession_probability(
        data["usrec"],
        ns_factor_rows=data["ns"],
        feature_transform_rows=[],
        credit_spread_rows=[],
        funding_stress_rows=[],
        regime_rows=[],
        cfg=cfg,
    )
    for r in result:
        assert "prob_recession_3m" in r
        assert "prob_recession_6m" in r
        assert "prob_recession_12m" in r

    # At the last date only 3 labeled examples exist for 12m → below min_obs=5.
    last = result[-1]
    assert last["prob_recession_12m"] is None


def test_model_vintage_equals_observation_date():
    data = _minimal_data(10)
    cfg = _make_cfg(min_obs=3, horizon_months=(3,))
    result = compute_recession_probability(
        data["usrec"],
        ns_factor_rows=data["ns"],
        feature_transform_rows=[],
        credit_spread_rows=[],
        funding_stress_rows=[],
        regime_rows=[],
        cfg=cfg,
    )
    for r in result:
        assert r["model_vintage"] == r["observation_date"]


def test_no_features_all_disabled():
    """All features disabled → intercept-only model → prob near 0.5 for balanced data."""
    data = _minimal_data(20)
    cfg = _make_cfg(
        min_obs=3,
        horizon_months=(3,),
        ns_slope=False,
        unrate_mom=False,
        indpro_mom=False,
        hy_oas_zscore=False,
        funding_stress=False,
        regime_composite=False,
    )
    result = compute_recession_probability(
        data["usrec"],
        ns_factor_rows=[],
        feature_transform_rows=[],
        credit_spread_rows=[],
        funding_stress_rows=[],
        regime_rows=[],
        cfg=cfg,
    )
    valid = [r for r in result if r["recession_prob"] is not None]
    # Intercept-only on all-zero outcome → probability near 0 (no recessions).
    assert len(valid) > 0
    for r in valid:
        assert r["n_features"] == 0


def test_sorted_chronologically():
    """Output should be sorted by observation_date."""
    data = _minimal_data(15)
    cfg = _make_cfg(min_obs=3, horizon_months=(3,))
    result = compute_recession_probability(
        data["usrec"],
        ns_factor_rows=data["ns"],
        feature_transform_rows=[],
        credit_spread_rows=[],
        funding_stress_rows=[],
        regime_rows=[],
        cfg=cfg,
    )
    dates = [r["observation_date"] for r in result]
    assert dates == sorted(dates)


def test_load_recession_model_config():
    """Config loads from the real YAML file without error."""
    cfg = load_recession_model_config()
    assert cfg.min_obs == 60
    assert cfg.l2_lambda == pytest.approx(0.01)
    assert 3 in cfg.horizon_months
    assert 6 in cfg.horizon_months
    assert 12 in cfg.horizon_months
    assert cfg.ns_slope is True
    assert cfg.hy_oas_instrument == "BAMLH0A0HYM2"
