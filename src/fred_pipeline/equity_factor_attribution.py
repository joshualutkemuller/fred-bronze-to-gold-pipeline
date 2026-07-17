"""ML-5: Equity factor attribution — rolling OLS of monthly equity returns on
PCA macro factor scores (pure Python, scipy-free).

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
3. For each (ticker, window w), slide the trailing-w-months window and run
   OLS:  monthly_return ~ 1 + factor_1 + … + factor_K
4. Emit α (intercept), K betas, t-stats, R², n_obs per window end-date.

The pure-Python OLS uses Gaussian elimination with partial pivoting.
"""

from __future__ import annotations

import math
from bisect import bisect_right
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Optional

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
# Gaussian elimination (shared OLS kernel)
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
# OLS with t-statistics
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
    k = len(factor_cols)  # predictors, excluding intercept
    p = k + 1              # total parameters
    if n <= p:
        return None

    # Design matrix rows: [1, x1_i, x2_i, …]
    X = [[1.0] + [factor_cols[j][i] for j in range(k)] for i in range(n)]

    XtX = [
        [sum(X[i][a] * X[i][b] for i in range(n)) for b in range(p)]
        for a in range(p)
    ]
    Xty = [sum(X[i][a] * y[i] for i in range(n)) for a in range(p)]

    beta = _solve(XtX, Xty)
    if beta is None:
        return None

    y_hat = [sum(X[i][a] * beta[a] for a in range(p)) for i in range(n)]
    resid = [y[i] - y_hat[i] for i in range(n)]
    sse = sum(r * r for r in resid)

    y_mean = sum(y) / n
    sst = sum((yi - y_mean) ** 2 for yi in y)
    r_squared = max(0.0, 1.0 - sse / sst) if sst > 1e-14 else 0.0

    sigma2 = sse / (n - p)
    if sigma2 <= 0.0:
        # Perfect fit — t-stats undefined (would be ±∞)
        return {
            "alpha": beta[0], "betas": beta[1:],
            "t_stat_alpha": None, "t_stats": [None] * k,
            "r_squared": r_squared, "n_obs": n,
        }

    # Var(β_j) = σ²·[(XᵀX)⁻¹]_{jj} — recover via column of the inverse
    t_all: list[Optional[float]] = []
    for j in range(p):
        e_j = [1.0 if i == j else 0.0 for i in range(p)]
        col_j = _solve(XtX, e_j)
        if col_j is None or col_j[j] <= 0.0:
            t_all.append(None)
        else:
            se_j = math.sqrt(sigma2 * col_j[j])
            t_all.append(beta[j] / se_j if se_j > 1e-14 else None)

    return {
        "alpha": beta[0],
        "betas": beta[1:],
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
# Main engine
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

    # All months with at least one factor score, sorted chronologically
    all_ym = sorted(fscores)
    factor_ids = sorted({fid for v in fscores.values() for fid in v})
    n_factors = len(factor_ids)
    if n_factors == 0:
        return []

    out: list[dict[str, Any]] = []
    for ticker in sorted(monthly_ret):
        ticker_returns = monthly_ret[ticker]

        # Build matched list: months where both return and all factors exist
        matched: list[tuple[tuple[int, int], float, list[float]]] = []
        for ym in all_ym:
            if ym not in ticker_returns:
                continue
            frow = fscores[ym]
            if not all(fid in frow for fid in factor_ids):
                continue
            matched.append((ym, ticker_returns[ym], [frow[fid] for fid in factor_ids]))

        if not matched:
            continue

        for window in sorted(cfg.windows):
            for i in range(len(matched)):
                if i + 1 < window:
                    continue  # not enough history yet
                sl = matched[i + 1 - window: i + 1]
                if len(sl) < cfg.min_obs:
                    continue

                y = [s[1] for s in sl]
                Xcols = [[s[2][j] for s in sl] for j in range(n_factors)]

                ols = _ols(y, Xcols)
                if ols is None:
                    continue

                obs_date = fdates[matched[i][0]]
                for fi, fid in enumerate(factor_ids):
                    out.append({
                        "ticker": ticker,
                        "observation_date": obs_date,
                        "window": window,
                        "factor": fid,
                        "beta": ols["betas"][fi],
                        "t_stat": ols["t_stats"][fi],
                        "alpha": ols["alpha"],
                        "r_squared": ols["r_squared"],
                        "n_obs": ols["n_obs"],
                    })

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
