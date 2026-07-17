"""ML-5: Equity factor attribution — rolling OLS of monthly equity returns on
PCA macro factor scores (NumPy-accelerated, scipy-free).

Inputs
------
* ``gold.equity_return_daily`` — daily price-return per ticker (Stooq close).
* ``gold.macro_factor_scores`` — monthly expanding PCA factor scores (ML-2).

Output
------
``gold.equity_factor_attribution`` — one row per (ticker, factor, window,
observation_date): rolling OLS β/t-stat, plus α/R²/n_obs repeated for context.

Algorithm
---------
1. Compound daily price returns within each calendar month → monthly return
   series per ticker.
2. Match monthly ticker returns to PCA factor scores on (year, month).
3. For each (window w, ending calendar month m), batch ALL tickers that have
   returns for the w consecutive months and solve:
       np.linalg.lstsq(X, Y)   where Y = (n, N_tickers)
   — a single BLAS call replaces N_tickers independent OLS solves and reuses
   the shared (X'X)⁻¹ diagonal across all tickers in the batch.
4. Emit α (intercept), K betas, t-stats, R², n_obs per window end-date.

The public `_solve` / `_ols` helpers are kept for backward compatibility
with the test suite; the hot path inside `compute_equity_factor_attribution`
uses batched NumPy instead.
"""

from __future__ import annotations

import math
from bisect import bisect_right
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Optional

import numpy as np
import yaml

_CONFIG_PATH = (
    Path(__file__).parent.parent.parent / "config" / "equity_factor_attribution.yml"
)

DEFAULT_WINDOWS: tuple[int, ...] = (12, 36, 60)
DEFAULT_MIN_OBS: int = 12


@dataclass(frozen=True)
class EquityFactorConfig:
    windows: tuple[int, ...]
    min_obs: int
    tickers: tuple[str, ...]  # empty = all tickers in equity_return_rows


def load_equity_factor_config(path: Optional[str] = None) -> EquityFactorConfig:
    """Load config from YAML; return sensible defaults if the file is absent."""
    p = Path(path) if path else _CONFIG_PATH
    if not p.exists():
        return EquityFactorConfig(
            windows=DEFAULT_WINDOWS, min_obs=DEFAULT_MIN_OBS, tickers=()
        )
    with open(p) as fh:
        raw = yaml.safe_load(fh) or {}
    return EquityFactorConfig(
        windows=tuple(int(w) for w in raw.get("windows", list(DEFAULT_WINDOWS))),
        min_obs=int(raw.get("min_obs", DEFAULT_MIN_OBS)),
        tickers=tuple(str(t) for t in raw.get("tickers", [])),
    )


# ---------------------------------------------------------------------------
# Gaussian elimination (kept for backward compatibility with tests)
# ---------------------------------------------------------------------------

def _solve(a: list[list[float]], b: list[float]) -> Optional[list[float]]:
    """Solve Ax = b via Gaussian elimination with partial pivoting.
    Returns None if the matrix is singular (pivot < 1e-14).
    """
    n = len(b)
    M = [row[:] + [b[i]] for i, row in enumerate(a)]
    for col in range(n):
        pivot = max(range(col, n), key=lambda r: abs(M[r][col]))
        M[col], M[pivot] = M[pivot], M[col]
        if abs(M[col][col]) < 1e-14:
            return None
        for row in range(col + 1, n):
            f = M[row][col] / M[col][col]
            for j in range(col, n + 1):
                M[row][j] -= f * M[col][j]
    x = [0.0] * n
    for i in range(n - 1, -1, -1):
        x[i] = M[i][n]
        for j in range(i + 1, n):
            x[i] -= M[i][j] * x[j]
        x[i] /= M[i][i]
    return x


# ---------------------------------------------------------------------------
# OLS with t-statistics (NumPy internals, kept for backward compat / tests)
# ---------------------------------------------------------------------------

def _ols(
    y: list[float],
    factor_cols: list[list[float]],
) -> Optional[dict[str, Any]]:
    """OLS: y ~ 1 + factor_cols[0] + factor_cols[1] + …

    Returns a dict with:
      alpha, betas (list), t_stat_alpha (Optional[float]),
      t_stats (list of Optional[float]), r_squared, n_obs.
    Returns None if the design matrix is singular, n ≤ p, or SST is zero.
    """
    n = len(y)
    k = len(factor_cols)
    p = k + 1
    if n <= p:
        return None

    y_arr = np.array(y, dtype=float)
    X = np.column_stack([np.ones(n)] + [np.array(c, dtype=float) for c in factor_cols])

    beta, _, rank, _ = np.linalg.lstsq(X, y_arr, rcond=None)
    if rank < p:
        return None

    y_hat = X @ beta
    resid = y_arr - y_hat
    sse = float(resid @ resid)
    y_mean = float(y_arr.mean())
    sst = float(((y_arr - y_mean) ** 2).sum())
    r_squared = max(0.0, 1.0 - sse / sst) if sst > 1e-14 else 0.0

    sigma2 = sse / (n - p)
    if sigma2 <= 0.0:
        return {
            "alpha": float(beta[0]), "betas": beta[1:].tolist(),
            "t_stat_alpha": None, "t_stats": [None] * k,
            "r_squared": r_squared, "n_obs": n,
        }

    try:
        XtX_inv_diag = np.diag(np.linalg.inv(X.T @ X))
    except np.linalg.LinAlgError:
        return None

    se = np.sqrt(np.maximum(sigma2 * XtX_inv_diag, 0.0))
    t_all: list[Optional[float]] = [
        float(b / s) if s > 1e-14 else None for b, s in zip(beta, se)
    ]

    return {
        "alpha": float(beta[0]),
        "betas": beta[1:].tolist(),
        "t_stat_alpha": t_all[0],
        "t_stats": t_all[1:],
        "r_squared": r_squared,
        "n_obs": n,
    }


# ---------------------------------------------------------------------------
# Monthly return aggregation
# ---------------------------------------------------------------------------

def _monthly_returns(
    equity_return_rows: Iterable[dict[str, Any]],
    wanted_tickers: frozenset[str],
) -> dict[str, dict[tuple[int, int], float]]:
    """Compound daily ``price_return`` within each calendar month per ticker.

    Returns ``{ticker: {(year, month): compounded_monthly_return}}``.
    Days with ``price_return=None`` (the first observation per ticker) are
    skipped — the product starts from the first day that has a return.
    Months in which no day yields a non-null return are excluded.
    """
    cum: dict[str, dict[tuple[int, int], float]] = {}
    for r in equity_return_rows:
        ticker = r.get("ticker", "")
        if not ticker:
            continue
        if wanted_tickers and ticker not in wanted_tickers:
            continue
        ret = r.get("price_return")
        if ret is None:
            continue
        obs = r.get("observation_date", "")
        try:
            d = date.fromisoformat(obs)
        except (ValueError, TypeError):
            continue
        ym = (d.year, d.month)
        ticker_map = cum.setdefault(ticker, {})
        ticker_map[ym] = ticker_map.get(ym, 1.0) * (1.0 + float(ret))

    return {
        ticker: {ym: v - 1.0 for ym, v in months.items()}
        for ticker, months in cum.items()
    }


# ---------------------------------------------------------------------------
# Factor score matrix
# ---------------------------------------------------------------------------

def _factor_matrix(
    factor_score_rows: Iterable[dict[str, Any]],
) -> tuple[dict[tuple[int, int], dict[int, float]], dict[tuple[int, int], str]]:
    """Parse ML-2 factor score rows into:
      * ``scores`` — ``{(year, month): {factor_id: score}}``
      * ``dates``  — ``{(year, month): observation_date_str}`` (for output keys)
    """
    scores: dict[tuple[int, int], dict[int, float]] = {}
    dates: dict[tuple[int, int], str] = {}
    for r in factor_score_rows:
        obs = r.get("observation_date", "")
        factor = r.get("factor")
        score = r.get("score")
        if not obs or factor is None or score is None:
            continue
        try:
            d = date.fromisoformat(obs)
        except (ValueError, TypeError):
            continue
        ym = (d.year, d.month)
        scores.setdefault(ym, {})[int(factor)] = float(score)
        dates[ym] = obs
    return scores, dates


# ---------------------------------------------------------------------------
# Main engine — batched cross-ticker OLS (Tier 1 + 2)
# ---------------------------------------------------------------------------

def compute_equity_factor_attribution(
    equity_return_rows: Iterable[dict[str, Any]],
    factor_score_rows: Iterable[dict[str, Any]],
    cfg: Optional[EquityFactorConfig] = None,
) -> list[dict[str, Any]]:
    """``gold.equity_factor_attribution``: rolling OLS of monthly equity price
    returns on ML-2 PCA macro factor scores.

    One row per (ticker, window, observation_date, factor).
    ``alpha``, ``r_squared``, and ``n_obs`` repeat for every factor row at the
    same (ticker, window, observation_date) for Power BI convenience.

    For each (window, ending calendar month), all tickers that have returns for
    every month in the window are batched into a single ``np.linalg.lstsq``
    call, sharing one (X'X)⁻¹ diagonal computation.
    """
    if cfg is None:
        cfg = load_equity_factor_config()

    wanted = frozenset(cfg.tickers)

    fscores, fdates = _factor_matrix(factor_score_rows)
    if not fscores:
        return []

    monthly_ret = _monthly_returns(equity_return_rows, wanted)
    if not monthly_ret:
        return []

    all_ym = sorted(fscores)
    factor_ids = sorted({fid for v in fscores.values() for fid in v})
    n_factors = len(factor_ids)
    if n_factors == 0:
        return []

    # Pre-build list of (ym, factor_score_ndarray) for months where all factors
    # are present — these are the only months that can anchor a window.
    all_monthly_pairs: list[tuple[tuple[int, int], np.ndarray]] = []
    for ym in all_ym:
        frow = fscores[ym]
        if all(fid in frow for fid in factor_ids):
            all_monthly_pairs.append(
                (ym, np.array([frow[fid] for fid in factor_ids], dtype=float))
            )

    if not all_monthly_pairs:
        return []

    all_tickers = sorted(monthly_ret)
    out: list[dict[str, Any]] = []

    for window in sorted(cfg.windows):
        for i in range(len(all_monthly_pairs)):
            if i + 1 < window:
                continue
            sl = all_monthly_pairs[i + 1 - window: i + 1]
            n = len(sl)
            if n < cfg.min_obs:
                continue

            sl_yms = [p[0] for p in sl]
            obs_date = fdates[sl_yms[-1]]

            # Design matrix X — shared by all tickers (n, p)
            F = np.array([p[1] for p in sl])          # (n, n_factors)
            X = np.column_stack([np.ones(n), F])       # (n, p)
            p_cols = X.shape[1]

            # Batch: tickers that have returns for every month in this window
            batch = [t for t in all_tickers
                     if all(ym in monthly_ret[t] for ym in sl_yms)]
            if not batch:
                continue

            # Y: (n, n_batch) — each column is one ticker's monthly returns
            Y = np.array(
                [[monthly_ret[t][ym] for ym in sl_yms] for t in batch],
                dtype=float,
            ).T

            # Single BLAS solve for all tickers (minimum-norm for rank-deficient X)
            beta_all, _, _, _ = np.linalg.lstsq(X, Y, rcond=None)

            # Shared quantities (same X for all tickers)
            Y_hat = X @ beta_all                               # (n, n_batch)
            resid = Y - Y_hat                                  # (n, n_batch)
            sse = (resid ** 2).sum(axis=0)                    # (n_batch,)
            y_mean = Y.mean(axis=0)                            # (n_batch,)
            sst_arr = ((Y - y_mean) ** 2).sum(axis=0)         # (n_batch,)
            with np.errstate(divide="ignore", invalid="ignore"):
                r2_arr = np.maximum(
                    0.0,
                    np.where(sst_arr > 1e-14, 1.0 - sse / sst_arr, 0.0),
                )
            sigma2_arr = sse / (n - p_cols)                    # (n_batch,)

            # (X'X)⁺ diagonal via pseudoinverse — handles rank-deficient X gracefully
            XtX_inv_diag = np.diag(np.linalg.pinv(X.T @ X))

            for ti, ticker in enumerate(batch):
                alpha_val = float(beta_all[0, ti])
                betas_ti = beta_all[1:, ti]          # shape (n_factors,)
                r2 = float(r2_arr[ti])
                s2 = float(sigma2_arr[ti])

                if s2 > 0.0:
                    se = np.sqrt(np.maximum(s2 * XtX_inv_diag, 0.0))
                    t_all: list[Optional[float]] = [
                        float(b / s) if s > 1e-14 else None
                        for b, s in zip(beta_all[:, ti], se)
                    ]
                else:
                    t_all = [None] * p_cols

                for fi, fid in enumerate(factor_ids):
                    out.append({
                        "ticker": ticker,
                        "observation_date": obs_date,
                        "window": window,
                        "factor": fid,
                        "beta": float(betas_ti[fi]),
                        "t_stat": t_all[fi + 1],   # fi+1: skip intercept
                        "alpha": alpha_val,
                        "r_squared": r2,
                        "n_obs": n,
                    })

    # Sort to match the original (ticker, observation_date, window, factor) order
    out.sort(key=lambda r: (
        r["ticker"], r["observation_date"], r["window"], r["factor"]
    ))
    return out


# ---------------------------------------------------------------------------
# ML-5b: Factor-implied return decomposition
# ---------------------------------------------------------------------------

def compute_equity_factor_implied_return(
    attribution_rows: Iterable[dict[str, Any]],
    factor_score_rows: Iterable[dict[str, Any]],
    equity_return_rows: Iterable[dict[str, Any]],
    cfg: Optional[EquityFactorConfig] = None,
) -> list[dict[str, Any]]:
    """``gold.equity_factor_implied_return``: factor-implied monthly returns.

    For each (ticker, window, month):
        implied_return  = α + Σᵢ βᵢ · Fᵢ   (full model prediction)
        factor_return   = Σᵢ βᵢ · Fᵢ        (pure systematic component)
        alpha_return    = α                   (OLS intercept)
        residual_return = realized − implied  (idiosyncratic / unexplained)

    Betas are forward-filled: the most-recent attribution estimate at or before
    the current month is used, so no new OLS is run here.
    """
    if cfg is None:
        cfg = load_equity_factor_config()

    wanted = frozenset(cfg.tickers)

    fscores, fdates = _factor_matrix(factor_score_rows)
    if not fscores:
        return []

    factor_ids = sorted({fid for v in fscores.values() for fid in v})
    if not factor_ids:
        return []

    # Parse attribution rows: {(ticker, window): {ym: {alpha, betas}}}
    # Multiple factor rows share the same alpha/alpha per (ticker, window, ym).
    attrib: dict[tuple[str, int], dict[tuple[int, int], dict[str, Any]]] = {}
    for r in attribution_rows:
        ticker = r.get("ticker", "")
        obs = r.get("observation_date", "")
        window = r.get("window")
        factor = r.get("factor")
        beta = r.get("beta")
        alpha = r.get("alpha")
        if (not ticker or not obs or window is None
                or factor is None or beta is None or alpha is None):
            continue
        if wanted and ticker not in wanted:
            continue
        try:
            d = date.fromisoformat(obs)
        except (ValueError, TypeError):
            continue
        ym = (d.year, d.month)
        key = (ticker, int(window))
        entry = attrib.setdefault(key, {}).setdefault(
            ym, {"alpha": 0.0, "betas": {}}
        )
        entry["betas"][int(factor)] = float(beta)
        entry["alpha"] = float(alpha)  # same value for every factor row at this ym

    if not attrib:
        return []

    # Pre-sort ym keys per (ticker, window) for O(log T) forward-fill lookup.
    sorted_yms: dict[tuple[str, int], list[tuple[int, int]]] = {
        key: sorted(ym_dict) for key, ym_dict in attrib.items()
    }

    monthly_ret = _monthly_returns(equity_return_rows, wanted)
    all_ym = sorted(fscores)

    out: list[dict[str, Any]] = []
    for (ticker, window), ym_dict in sorted(attrib.items()):
        key_yms = sorted_yms[(ticker, window)]

        for ym in all_ym:
            if ym not in fdates:
                continue

            # Forward-fill: latest attribution date ≤ current month
            pos = bisect_right(key_yms, ym) - 1
            if pos < 0:
                continue
            entry = ym_dict[key_yms[pos]]
            betas = entry["betas"]
            alpha = entry["alpha"]

            if not all(fid in betas for fid in factor_ids):
                continue

            fscore = fscores.get(ym)
            if fscore is None or not all(fid in fscore for fid in factor_ids):
                continue

            factor_ret = sum(betas[fid] * fscore[fid] for fid in factor_ids)
            implied = alpha + factor_ret
            realized = monthly_ret.get(ticker, {}).get(ym)

            out.append({
                "ticker": ticker,
                "observation_date": fdates[ym],
                "window": window,
                "implied_return": implied,
                "factor_return": factor_ret,
                "alpha_return": alpha,
                "realized_return": realized,
                "residual_return": (realized - implied) if realized is not None else None,
            })

    return out
