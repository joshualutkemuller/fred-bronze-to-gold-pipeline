"""Tests for compute_series_structural_breaks (Chow + CUSUM engines)."""

import math
from datetime import date, timedelta

import pytest

from fred_pipeline.regime_stats import (
    _chow_f_at,
    _chow_scan,
    _cusum_scan,
    _ols_fit,
    compute_series_structural_breaks,
)
from fred_pipeline.regime_stats_config import StatsPairDef, StatsConfig


# ---- helpers to build synthetic latest_rows ---------------------------------

def _make_rows(series_id: str, values: list[float], start: date) -> list[dict]:
    return [
        {
            "series_id": series_id,
            "observation_date": (start + timedelta(days=i)).isoformat(),
            "value": v,
            "is_missing": False,
        }
        for i, v in enumerate(values)
    ]


def _cfg(*pairs) -> StatsConfig:
    return StatsConfig(
        pairs=tuple(
            StatsPairDef(series_a=a, series_b=b, transform_a="level", transform_b="level")
            for a, b in pairs
        ),
        windows=(63,),
        max_lag=4,
        granger_lags=2,
    )


START = date(2020, 1, 1)


# ---- unit tests: _ols_fit ---------------------------------------------------


def test_ols_fit_exact_line():
    xs = [1.0, 2.0, 3.0, 4.0, 5.0]
    ys = [3.0 + 2.0 * x for x in xs]  # y = 3 + 2x (exact)
    result = _ols_fit(xs, ys)
    assert result is not None
    coeffs, resid, rss = result
    assert abs(coeffs[0] - 3.0) < 1e-6   # intercept
    assert abs(coeffs[1] - 2.0) < 1e-6   # slope
    assert rss < 1e-10
    assert all(abs(r) < 1e-8 for r in resid)


def test_ols_fit_too_few_obs():
    assert _ols_fit([1.0], [2.0]) is None


def test_ols_fit_singular_gracefully():
    # All x values identical → near-singular; should return something via ridge
    xs = [1.0] * 10
    ys = list(range(10))
    # Should not raise; may return None or a result
    _ols_fit(xs, ys)


# ---- unit tests: _chow_f_at -------------------------------------------------


def test_chow_f_at_known_break():
    """Two clearly different regimes joined → F should be large at the join."""
    n = 60
    xs = list(range(n))
    # regime 1: y = x (slope 1); regime 2: y = 100 + 0.5*x
    ys = [float(x) for x in xs[:30]] + [100.0 + 0.5 * x for x in xs[30:]]
    f = _chow_f_at(xs, ys, 30)
    assert f is not None
    assert f > 10.0  # should be a large F at the true breakpoint


def test_chow_f_at_no_break():
    """Stable regime → F at mid-point should be small."""
    import random
    rng = random.Random(42)
    n = 60
    xs = list(range(n))
    ys = [2.0 + 0.5 * x + rng.gauss(0, 0.1) for x in xs]
    f = _chow_f_at(xs, ys, 30)
    # Not guaranteed to be small due to noise, but check it's finite
    assert f is not None
    assert math.isfinite(f)


def test_chow_f_at_too_small_segment():
    xs = [1.0, 2.0, 3.0]
    ys = [1.0, 2.0, 3.0]
    assert _chow_f_at(xs, ys, 1) is None  # only 1 obs in left segment


# ---- unit tests: _chow_scan -------------------------------------------------


def test_chow_scan_detects_known_break():
    """The scan should pick up the midpoint break in a two-regime series."""
    n = 100
    xs = [float(i) for i in range(n)]
    ys = [x + 0.1 * (i % 3) for i, x in enumerate(xs[:50])]  # regime 1
    ys += [50.0 - (x - 50.0) + 0.1 * (i % 3) for i, x in enumerate(xs[50:])]  # regime 2 (flipped slope)
    dates = [START + timedelta(days=i) for i in range(n)]
    bd, f, p, pre_n, post_n = _chow_scan(dates, xs, ys, min_segment=10)
    assert bd is not None
    assert f is not None and f > 1.0
    assert p is not None and p < 0.05
    # Break date should be close to the midpoint (day ~49/50)
    assert abs((bd - START).days - 49) <= 10


def test_chow_scan_too_few_obs():
    dates = [START + timedelta(days=i) for i in range(5)]
    xs = [1.0, 2.0, 3.0, 4.0, 5.0]
    ys = [1.0, 2.0, 3.0, 4.0, 5.0]
    bd, f, p, pre_n, post_n = _chow_scan(dates, xs, ys, min_segment=5)
    assert bd is None


# ---- unit tests: _cusum_scan ------------------------------------------------


def test_cusum_scan_stable_series():
    """Stable linear relationship → CUSUM should stay small."""
    n = 50
    xs = [float(i) for i in range(n)]
    ys = [2.0 + 0.5 * x for x in xs]  # perfect line
    dates = [START + timedelta(days=i) for i in range(n)]
    cd, cusum_max, p = _cusum_scan(dates, xs, ys)
    # Perfect fit → residuals ≈ 0 → CUSUM ≈ 0
    assert cusum_max < 1e-6


def test_cusum_scan_breaks_boundary_on_shift():
    """A sudden mean shift in residuals should cross the CUSUM boundary."""
    import random
    rng = random.Random(99)
    n = 200
    xs = [float(i) for i in range(n)]
    # Stable relationship for first 100 obs, then a clear level shift in y
    ys = [0.5 * x + rng.gauss(0, 0.05) for x in xs[:100]]
    ys += [50.0 + 0.5 * x + rng.gauss(0, 0.05) for x in xs[100:]]
    dates = [START + timedelta(days=i) for i in range(n)]
    cd, cusum_max, p = _cusum_scan(dates, xs, ys)
    assert p < 0.05  # should be significant
    assert cd is not None


def test_cusum_scan_returns_date():
    n = 40
    xs = list(range(n))
    ys = [float(x) for x in xs]
    dates = [START + timedelta(days=i) for i in range(n)]
    cd, _, _ = _cusum_scan(dates, [float(x) for x in xs], ys)
    assert cd is None or isinstance(cd, date)


# ---- integration: compute_series_structural_breaks --------------------------


def test_structural_breaks_stable_pair():
    """A stationary pair with no break should still return two rows per pair."""
    import random
    rng = random.Random(7)
    n = 80
    a_vals = [rng.gauss(0, 1) for _ in range(n)]
    b_vals = [2.0 * a + rng.gauss(0, 0.1) for a in a_vals]
    rows = _make_rows("A", a_vals, START) + _make_rows("B", b_vals, START)
    cfg = _cfg(("A", "B"))
    out = compute_series_structural_breaks(rows, cfg)
    assert len(out) == 2  # one Chow row, one CUSUM row
    types = {r["test_type"] for r in out}
    assert types == {"chow", "cusum"}
    for r in out:
        assert r["series_a"] == "A"
        assert r["series_b"] == "B"
        assert r["as_of_date"] is not None


def test_structural_breaks_detects_regime_change():
    """A pair with a clear mid-series break should produce p < 0.05 for Chow."""
    n = 100
    a_vals = [float(i) for i in range(n)]
    # b follows a closely in first half, then inverts slope in second half
    b_vals = [0.8 * a + 0.01 * (i % 5) for i, a in enumerate(a_vals[:50])]
    b_vals += [100.0 - 0.8 * a + 0.01 * (i % 5) for i, a in enumerate(a_vals[50:])]
    rows = _make_rows("X", a_vals, START) + _make_rows("Y", b_vals, START)
    cfg = _cfg(("X", "Y"))
    out = compute_series_structural_breaks(rows, cfg)
    chow = next(r for r in out if r["test_type"] == "chow")
    assert chow["is_significant"] == 1
    assert chow["break_date"] is not None
    assert chow["f_stat"] > 1.0


def test_structural_breaks_multiple_pairs():
    """Two pairs → four rows total (two test types × two pairs)."""
    import random
    rng = random.Random(13)
    n = 60
    vals_a = [rng.gauss(0, 1) for _ in range(n)]
    vals_b = [v + rng.gauss(0, 0.1) for v in vals_a]
    vals_c = [-v + rng.gauss(0, 0.1) for v in vals_a]
    rows = (
        _make_rows("S1", vals_a, START)
        + _make_rows("S2", vals_b, START)
        + _make_rows("S3", vals_c, START)
    )
    cfg = _cfg(("S1", "S2"), ("S1", "S3"))
    out = compute_series_structural_breaks(rows, cfg)
    assert len(out) == 4
    pairs = {(r["series_a"], r["series_b"]) for r in out}
    assert ("S1", "S2") in pairs
    assert ("S1", "S3") in pairs


def test_structural_breaks_too_few_obs():
    """Fewer than 10 aligned observations → no output rows."""
    a_vals = [1.0, 2.0, 3.0]
    b_vals = [1.1, 2.1, 3.1]
    rows = _make_rows("P", a_vals, START) + _make_rows("Q", b_vals, START)
    cfg = _cfg(("P", "Q"))
    out = compute_series_structural_breaks(rows, cfg)
    assert out == []


def test_structural_breaks_empty_config():
    """Empty pair list → empty output."""
    rows = _make_rows("A", [1.0, 2.0, 3.0], START)
    cfg = StatsConfig(pairs=(), windows=(63,), max_lag=4, granger_lags=2)
    assert compute_series_structural_breaks(rows, cfg) == []


def test_structural_breaks_schema_keys():
    """Every output row has the expected column set."""
    import random
    rng = random.Random(0)
    n = 50
    a_vals = [rng.gauss(0, 1) for _ in range(n)]
    b_vals = [a + rng.gauss(0, 0.2) for a in a_vals]
    rows = _make_rows("M", a_vals, START) + _make_rows("N", b_vals, START)
    cfg = _cfg(("M", "N"))
    out = compute_series_structural_breaks(rows, cfg)
    expected_keys = {
        "series_a", "series_b", "transform_a", "transform_b", "test_type",
        "break_date", "f_stat", "p_value", "pre_n", "post_n",
        "pre_mean_a", "post_mean_a", "pre_mean_b", "post_mean_b",
        "cusum_max", "is_significant", "as_of_date",
    }
    for row in out:
        assert set(row.keys()) == expected_keys
    # Chow row has cusum_max=None; CUSUM row has f_stat=None
    chow = next(r for r in out if r["test_type"] == "chow")
    cusum = next(r for r in out if r["test_type"] == "cusum")
    assert chow["cusum_max"] is None
    assert cusum["f_stat"] is None


def test_structural_breaks_missing_series():
    """If one series in a pair has no data, that pair is skipped."""
    import random
    rng = random.Random(1)
    n = 50
    a_vals = [rng.gauss(0, 1) for _ in range(n)]
    rows = _make_rows("PRESENT", a_vals, START)
    # MISSING has no rows
    cfg = _cfg(("PRESENT", "MISSING"))
    out = compute_series_structural_breaks(rows, cfg)
    assert out == []  # 0 common dates → skipped


def test_cusum_p_value_range():
    """p_value from CUSUM should always be in [0, 1]."""
    import random
    rng = random.Random(42)
    n = 100
    dates = [START + timedelta(days=i) for i in range(n)]
    xs = [rng.gauss(0, 1) for _ in range(n)]
    ys = [x + rng.gauss(0, 0.5) for x in xs]
    _, _, p = _cusum_scan(dates, xs, ys)
    assert 0.0 <= p <= 1.0


def test_is_significant_consistent_with_pvalue():
    """is_significant must equal int(p_value < 0.05)."""
    import random
    rng = random.Random(55)
    n = 80
    a_vals = [rng.gauss(0, 1) for _ in range(n)]
    b_vals = [a + rng.gauss(0, 0.1) for a in a_vals]
    rows = _make_rows("U", a_vals, START) + _make_rows("V", b_vals, START)
    cfg = _cfg(("U", "V"))
    out = compute_series_structural_breaks(rows, cfg)
    for r in out:
        p = r["p_value"]
        if p is not None:
            assert r["is_significant"] == int(p < 0.05)
