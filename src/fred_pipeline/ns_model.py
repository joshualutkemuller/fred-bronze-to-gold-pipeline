"""Phase ML-1: Nelson-Siegel yield curve fitting (pure Python).

Fits the three-factor Nelson-Siegel (1987) model to the daily Treasury
constant-maturity curve (``gold.treasury_curve``) and produces
``gold.yield_curve_ns_factors``: β₀ (level), β₁ (slope), β₂ (curvature),
the decay parameter λ, RMSE, and tenor count per date.

Two-step estimation
-------------------
1. Fix λ = ``LAMBDA_FIXED`` (1.7, the Diebold-Li calibration that maximises
   the slope-loading variance at medium maturities).  Solve the closed-form
   linear OLS for (β₀, β₁, β₂).
2. If the fixed-λ RMSE exceeds ``RMSE_GRID_THRESHOLD``, grid-search
   λ ∈ [0.5, 5.0] in steps of 0.1 and pick the minimising λ.

Economic meaning of the three factors
--------------------------------------
* β₀ — long-run level (all maturities converge here as τ → ∞)
* β₁ — slope / short-rate spread (negative when curve is normally shaped)
* β₂ — curvature / hump (positive when medium maturities are rich vs. extremes)
"""

from __future__ import annotations

import math
from typing import Any, Iterable, Optional

# Diebold-Li calibration: maximises slope-loading variance for US Treasuries.
LAMBDA_FIXED: float = 1.7

# Minimum tenors for a valid fit.
MIN_TENORS: int = 4

# Fixed-λ RMSE above this threshold (percentage points) triggers grid search.
RMSE_GRID_THRESHOLD: float = 0.10

# λ grid: 0.5 to 5.0 in steps of 0.1 (46 values).
_LAMBDA_GRID: list[float] = [round(0.5 + 0.1 * i, 10) for i in range(46)]


def _ns_loadings(tau: float, lam: float) -> tuple[float, float]:
    """Nelson-Siegel slope (L) and curvature (C) loadings for tenor τ (years).

    L(τ,λ) = (1 − e^{−τ/λ}) / (τ/λ)
    C(τ,λ) = L(τ,λ) − e^{−τ/λ}
    """
    if tau <= 0.0:
        # Limiting values as τ → 0: L → 1, C → 0.
        return 1.0, 0.0
    x = tau / lam
    exp_neg = math.exp(-x)
    L = (1.0 - exp_neg) / x if x > 1e-9 else 1.0
    return L, L - exp_neg


def _solve(a: list[list[float]], b: list[float]) -> Optional[list[float]]:
    """Gaussian elimination with partial pivoting; None if singular."""
    n = len(b)
    m = [row[:] + [b[i]] for i, row in enumerate(a)]
    for col in range(n):
        piv = max(range(col, n), key=lambda r: abs(m[r][col]))
        if abs(m[piv][col]) < 1e-12:
            return None
        m[col], m[piv] = m[piv], m[col]
        for r in range(col + 1, n):
            f = m[r][col] / m[col][col]
            for c in range(col, n + 1):
                m[r][c] -= f * m[col][c]
    x = [0.0] * n
    for r in range(n - 1, -1, -1):
        x[r] = (
            m[r][n] - sum(m[r][c] * x[c] for c in range(r + 1, n))
        ) / m[r][r]
    return x


def _ols3(
    taus: list[float], yields: list[float], lam: float
) -> tuple[Optional[list[float]], float]:
    """Fit y(τ) = β₀ + β₁·L + β₂·C via normal equations.

    Returns ``(beta, rmse)``; ``(None, inf)`` if the design matrix is
    singular even after a tiny ridge regularisation.
    """
    xtx = [[0.0] * 3 for _ in range(3)]
    xty = [0.0] * 3
    for tau, y in zip(taus, yields):
        L, C = _ns_loadings(tau, lam)
        row = [1.0, L, C]
        for i in range(3):
            for j in range(3):
                xtx[i][j] += row[i] * row[j]
            xty[i] += row[i] * y

    beta = _solve(xtx, xty)
    if beta is None:
        tr = sum(xtx[i][i] for i in range(3))
        ridge = 1e-8 * (tr / 3.0 or 1.0)
        ridged = [
            [xtx[i][j] + (ridge if i == j else 0.0) for j in range(3)]
            for i in range(3)
        ]
        beta = _solve(ridged, xty)
        if beta is None:
            return None, float("inf")

    ss = 0.0
    for tau, y in zip(taus, yields):
        L, C = _ns_loadings(tau, lam)
        ss += (y - (beta[0] + beta[1] * L + beta[2] * C)) ** 2
    return beta, math.sqrt(ss / len(taus))


def compute_yield_curve_ns_factors(
    treasury_curve_rows: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    """``gold.yield_curve_ns_factors``: Nelson-Siegel β₀/β₁/β₂ per date.

    Reads ``gold.treasury_curve`` rows (fields: ``as_of_date``,
    ``tenor_months``, ``yield_pct``).  One output row per date.

    * ``fit_valid = False`` (and betas = ``None``) when fewer than
      ``MIN_TENORS`` (4) non-null tenors are available.
    * ``lambda_estimated = True`` when the grid search improved on the
      fixed-λ RMSE (i.e. the fitted λ differs from ``LAMBDA_FIXED``).
    """
    # Group yields by as_of_date.
    by_date: dict[str, dict[int, float]] = {}
    for r in treasury_curve_rows:
        d = r.get("as_of_date")
        m = r.get("tenor_months")
        v = r.get("yield_pct")
        if d is None or m is None or v is None:
            continue
        by_date.setdefault(d, {})[int(m)] = float(v)

    out: list[dict[str, Any]] = []
    for obs_date in sorted(by_date):
        day = by_date[obs_date]
        sorted_months = sorted(day)
        taus = [m / 12.0 for m in sorted_months]
        yields = [day[m] for m in sorted_months]
        n = len(taus)

        if n < MIN_TENORS:
            out.append({
                "observation_date": obs_date,
                "beta0": None, "beta1": None, "beta2": None,
                "lambda": None, "lambda_estimated": None,
                "fit_rmse": None, "n_tenors": n, "fit_valid": False,
            })
            continue

        # Step 1: fixed λ.
        beta, rmse = _ols3(taus, yields, LAMBDA_FIXED)
        lam_used = LAMBDA_FIXED
        lam_estimated = False

        # Step 2: grid search when fixed-λ RMSE is too large.
        if rmse > RMSE_GRID_THRESHOLD:
            best_rmse, best_beta, best_lam = rmse, beta, LAMBDA_FIXED
            for lam in _LAMBDA_GRID:
                b, r = _ols3(taus, yields, lam)
                if b is not None and r < best_rmse:
                    best_rmse, best_beta, best_lam = r, b, lam
            if best_lam != LAMBDA_FIXED:
                beta, rmse, lam_used = best_beta, best_rmse, best_lam
                lam_estimated = True

        fit_valid = beta is not None
        out.append({
            "observation_date": obs_date,
            "beta0": beta[0] if fit_valid else None,
            "beta1": beta[1] if fit_valid else None,
            "beta2": beta[2] if fit_valid else None,
            "lambda": lam_used if fit_valid else None,
            "lambda_estimated": lam_estimated if fit_valid else None,
            "fit_rmse": rmse if fit_valid else None,
            "n_tenors": n,
            "fit_valid": fit_valid,
        })
    return out
