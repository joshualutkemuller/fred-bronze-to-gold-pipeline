"""ML-6: Short-Horizon Inflation Forecasting — pure Python, scipy-free.

Inputs
------
* ``gold.fred_latest_observation`` — monthly price-index levels for the
  configured series (CPIAUCSL, PCEPI by default).

Output
------
``gold.inflation_forecast`` — one row per (series_id, horizon_months,
model_type): point forecast and bootstrap 80%/95% CIs.

Algorithm
---------
1. Extract level time series per series_id from ``latest_rows``; compute
   MoM % (decimal fraction) from consecutive monthly observations.
2. AR(p): BIC lag selection (p ∈ [1, max_ar_lag]), expanding OLS, recursive
   h-step-ahead forecasting, fixed-coefficient residual bootstrap CIs.
3. VAR(p): bivariate equation-by-equation OLS; same BIC and forecasting
   scheme; joint residual resampling preserves cross-series correlation.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Optional

import yaml

_CONFIG_PATH = (
    Path(__file__).parent.parent.parent / "config" / "inflation_forecast.yml"
)

DEFAULT_HORIZONS: tuple[int, ...] = (1, 3, 6, 12)
DEFAULT_MAX_AR_LAG: int = 12
DEFAULT_MAX_VAR_LAG: int = 4
DEFAULT_N_BOOTSTRAP: int = 500
DEFAULT_MIN_OBS: int = 24


@dataclass(frozen=True)
class InflationForecastConfig:
    series: tuple[str, ...]
    var_pairs: tuple[tuple[str, str], ...]
    horizons: tuple[int, ...]
    max_ar_lag: int
    max_var_lag: int
    n_bootstrap: int
    min_obs: int
    random_seed: int


def load_inflation_forecast_config(
    path: Optional[str] = None,
) -> InflationForecastConfig:
    """Load config from YAML; return sensible defaults if the file is absent."""
    p = Path(path) if path else _CONFIG_PATH
    if not p.exists():
        return InflationForecastConfig(
            series=("CPIAUCSL", "PCEPI"),
            var_pairs=(("CPIAUCSL", "PCEPI"),),
            horizons=DEFAULT_HORIZONS,
            max_ar_lag=DEFAULT_MAX_AR_LAG,
            max_var_lag=DEFAULT_MAX_VAR_LAG,
            n_bootstrap=DEFAULT_N_BOOTSTRAP,
            min_obs=DEFAULT_MIN_OBS,
            random_seed=42,
        )
    with open(p) as fh:
        raw = yaml.safe_load(fh) or {}
    return InflationForecastConfig(
        series=tuple(str(s) for s in raw.get("series", ["CPIAUCSL", "PCEPI"])),
        var_pairs=tuple(
            (str(pair[0]), str(pair[1]))
            for pair in raw.get("var_pairs", [["CPIAUCSL", "PCEPI"]])
        ),
        horizons=tuple(int(h) for h in raw.get("horizons", list(DEFAULT_HORIZONS))),
        max_ar_lag=int(raw.get("max_ar_lag", DEFAULT_MAX_AR_LAG)),
        max_var_lag=int(raw.get("max_var_lag", DEFAULT_MAX_VAR_LAG)),
        n_bootstrap=int(raw.get("n_bootstrap", DEFAULT_N_BOOTSTRAP)),
        min_obs=int(raw.get("min_obs", DEFAULT_MIN_OBS)),
        random_seed=int(raw.get("random_seed", 42)),
    )


# ---------------------------------------------------------------------------
# Gaussian elimination (shared linear-algebra kernel)
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
# AR(p) with intercept
# ---------------------------------------------------------------------------

def _ar_fit(
    y: list[float], p: int
) -> Optional[tuple[list[float], list[float]]]:
    """OLS fit of AR(p) with intercept: y[t] = c + φ₁·y[t-1] + … + φₚ·y[t-p].

    Returns (coeffs, residuals) where coeffs = [c, φ₁, …, φₚ], or None if the
    design matrix is singular or there are too few observations.
    """
    n = len(y)
    n_eff = n - p
    k = p + 1  # intercept + p lags
    if n_eff <= k:
        return None

    X = [[1.0] + [y[t - 1 - j] for j in range(p)] for t in range(p, n)]
    Y = [y[t] for t in range(p, n)]

    XtX = [
        [sum(X[i][a] * X[i][b] for i in range(n_eff)) for b in range(k)]
        for a in range(k)
    ]
    Xty = [sum(X[i][a] * Y[i] for i in range(n_eff)) for a in range(k)]

    coeffs = _solve(XtX, Xty)
    if coeffs is None:
        return None

    y_hat = [sum(X[i][a] * coeffs[a] for a in range(k)) for i in range(n_eff)]
    residuals = [Y[i] - y_hat[i] for i in range(n_eff)]
    return coeffs, residuals


def _ar_bic(y: list[float], p: int) -> float:
    """BIC = n·log(RSS/n) + (p+1)·log(n) for AR(p) with intercept."""
    result = _ar_fit(y, p)
    if result is None:
        return math.inf
    _, residuals = result
    n = len(residuals)
    rss = sum(r * r for r in residuals)
    if rss <= 0.0:
        return -math.inf
    return n * math.log(rss / n) + (p + 1) * math.log(n)


def _select_ar_lag(y: list[float], max_lag: int) -> int:
    """Select AR lag p ∈ [1, max_lag] that minimises BIC."""
    best_p, best_bic = 1, math.inf
    for p in range(1, max_lag + 1):
        bic = _ar_bic(y, p)
        if bic < best_bic:
            best_bic = bic
            best_p = p
    return best_p


def _ar_forecast(y: list[float], coeffs: list[float], h: int) -> list[float]:
    """Recursively forecast h steps ahead from the end of y.

    Returns a list of h point forecasts (step 1, 2, …, h).
    """
    p = len(coeffs) - 1
    ext = list(y[-p:]) if p > 0 else []
    forecasts: list[float] = []
    for _ in range(h):
        pred = coeffs[0]
        for j in range(p):
            pred += coeffs[j + 1] * ext[-(j + 1)]
        forecasts.append(pred)
        ext.append(pred)
    return forecasts


def _ar_bootstrap_ci(
    y: list[float],
    coeffs: list[float],
    residuals: list[float],
    h: int,
    n_boot: int,
    rng: random.Random,
) -> dict[str, Optional[float]]:
    """Fixed-coefficient residual bootstrap CIs for the h-step AR forecast.

    Draws ``n_boot`` future residual paths and returns the empirical 10th/90th
    and 2.5th/97.5th percentiles of the h-step-ahead simulated values.
    """
    null: dict[str, Optional[float]] = {
        "lower_80": None, "upper_80": None,
        "lower_95": None, "upper_95": None,
    }
    if not residuals:
        return null

    p = len(coeffs) - 1
    history = list(y[-p:]) if p > 0 else []
    samples: list[float] = []

    for _ in range(n_boot):
        ext = list(history)
        for _ in range(h):
            innov = rng.choice(residuals)
            pred = coeffs[0]
            for j in range(p):
                pred += coeffs[j + 1] * ext[-(j + 1)]
            pred += innov
            ext.append(pred)
        samples.append(ext[-1])

    samples.sort()
    n = len(samples)
    return {
        "lower_80": samples[max(0, int(0.10 * n))],
        "upper_80": samples[min(n - 1, int(0.90 * n))],
        "lower_95": samples[max(0, int(0.025 * n))],
        "upper_95": samples[min(n - 1, int(0.975 * n))],
    }


# ---------------------------------------------------------------------------
# VAR(p) — bivariate, equation-by-equation OLS
# ---------------------------------------------------------------------------

def _var_fit(
    y1: list[float],
    y2: list[float],
    p: int,
) -> Optional[tuple[list[float], list[float], list[float], list[float]]]:
    """Fit VAR(p) with intercepts via equation-by-equation OLS.

    Design matrix row at time t (lags interleaved):
        [1, y1[t-1], y2[t-1], y1[t-2], y2[t-2], …, y1[t-p], y2[t-p]]

    Returns (coeffs1, coeffs2, resid1, resid2), or None if singular / too few obs.
    """
    n = len(y1)
    if len(y2) != n:
        return None
    n_eff = n - p
    k = 1 + 2 * p  # intercept + 2*p cross-lagged terms
    if n_eff <= k:
        return None

    X = [
        [1.0] + [val for j in range(p) for val in (y1[t - 1 - j], y2[t - 1 - j])]
        for t in range(p, n)
    ]
    Y1 = [y1[t] for t in range(p, n)]
    Y2 = [y2[t] for t in range(p, n)]

    XtX = [
        [sum(X[i][a] * X[i][b] for i in range(n_eff)) for b in range(k)]
        for a in range(k)
    ]
    Xty1 = [sum(X[i][a] * Y1[i] for i in range(n_eff)) for a in range(k)]
    Xty2 = [sum(X[i][a] * Y2[i] for i in range(n_eff)) for a in range(k)]

    c1 = _solve(XtX, Xty1)
    c2 = _solve(XtX, Xty2)
    if c1 is None or c2 is None:
        return None

    yhat1 = [sum(X[i][a] * c1[a] for a in range(k)) for i in range(n_eff)]
    yhat2 = [sum(X[i][a] * c2[a] for a in range(k)) for i in range(n_eff)]
    r1 = [Y1[i] - yhat1[i] for i in range(n_eff)]
    r2 = [Y2[i] - yhat2[i] for i in range(n_eff)]

    return c1, c2, r1, r2


def _var_bic(y1: list[float], y2: list[float], p: int) -> float:
    """BIC = n·log(det Σ̂) + k·log(n) for VAR(p); k = total params across equations."""
    result = _var_fit(y1, y2, p)
    if result is None:
        return math.inf
    _, _, r1, r2 = result
    n = len(r1)
    k = 2 * (1 + 2 * p)  # total params across both equations
    s11 = sum(x * x for x in r1) / n
    s22 = sum(x * x for x in r2) / n
    s12 = sum(r1[i] * r2[i] for i in range(n)) / n
    det = s11 * s22 - s12 * s12
    if det <= 0.0:
        return math.inf
    return n * math.log(det) + k * math.log(n)


def _select_var_lag(y1: list[float], y2: list[float], max_lag: int) -> int:
    """Select VAR lag p ∈ [1, max_lag] that minimises BIC."""
    best_p, best_bic = 1, math.inf
    for p in range(1, max_lag + 1):
        bic = _var_bic(y1, y2, p)
        if bic < best_bic:
            best_bic = bic
            best_p = p
    return best_p


def _var_forecast(
    y1: list[float],
    y2: list[float],
    coeffs1: list[float],
    coeffs2: list[float],
    p: int,
    h: int,
) -> list[tuple[float, float]]:
    """Recursively forecast h steps ahead from the end of (y1, y2).

    Returns [(f1_1, f2_1), …, (f1_h, f2_h)].
    """
    ext1 = list(y1[-p:]) if p > 0 else []
    ext2 = list(y2[-p:]) if p > 0 else []
    forecasts: list[tuple[float, float]] = []
    for _ in range(h):
        pred1 = coeffs1[0]
        pred2 = coeffs2[0]
        for j in range(p):
            pred1 += (
                coeffs1[1 + 2 * j] * ext1[-(j + 1)]
                + coeffs1[1 + 2 * j + 1] * ext2[-(j + 1)]
            )
            pred2 += (
                coeffs2[1 + 2 * j] * ext1[-(j + 1)]
                + coeffs2[1 + 2 * j + 1] * ext2[-(j + 1)]
            )
        forecasts.append((pred1, pred2))
        ext1.append(pred1)
        ext2.append(pred2)
    return forecasts


def _var_bootstrap_ci(
    y1: list[float],
    y2: list[float],
    coeffs1: list[float],
    coeffs2: list[float],
    resid1: list[float],
    resid2: list[float],
    p: int,
    h: int,
    n_boot: int,
    rng: random.Random,
) -> tuple[dict[str, Optional[float]], dict[str, Optional[float]]]:
    """Fixed-coefficient bootstrap CIs for VAR(p) h-step forecasts.

    Residual pairs are resampled jointly (same index) to preserve the
    contemporaneous cross-series correlation.  Returns (ci_y1, ci_y2).
    """
    null: dict[str, Optional[float]] = {
        "lower_80": None, "upper_80": None,
        "lower_95": None, "upper_95": None,
    }
    n_resid = len(resid1)
    if n_resid == 0:
        return null, null

    base1 = list(y1[-p:]) if p > 0 else []
    base2 = list(y2[-p:]) if p > 0 else []
    samples1: list[float] = []
    samples2: list[float] = []

    for _ in range(n_boot):
        ext1 = list(base1)
        ext2 = list(base2)
        for _ in range(h):
            idx = rng.randint(0, n_resid - 1)
            pred1 = coeffs1[0]
            pred2 = coeffs2[0]
            for j in range(p):
                pred1 += (
                    coeffs1[1 + 2 * j] * ext1[-(j + 1)]
                    + coeffs1[1 + 2 * j + 1] * ext2[-(j + 1)]
                )
                pred2 += (
                    coeffs2[1 + 2 * j] * ext1[-(j + 1)]
                    + coeffs2[1 + 2 * j + 1] * ext2[-(j + 1)]
                )
            pred1 += resid1[idx]
            pred2 += resid2[idx]
            ext1.append(pred1)
            ext2.append(pred2)
        samples1.append(ext1[-1])
        samples2.append(ext2[-1])

    def _pcts(samples: list[float]) -> dict[str, Optional[float]]:
        s = sorted(samples)
        n = len(s)
        return {
            "lower_80": s[max(0, int(0.10 * n))],
            "upper_80": s[min(n - 1, int(0.90 * n))],
            "lower_95": s[max(0, int(0.025 * n))],
            "upper_95": s[min(n - 1, int(0.975 * n))],
        }

    return _pcts(samples1), _pcts(samples2)


# ---------------------------------------------------------------------------
# MoM extraction helper
# ---------------------------------------------------------------------------

def _extract_mom(
    latest_rows: Iterable[dict[str, Any]],
    wanted: set[str],
) -> dict[str, list[tuple[str, float]]]:
    """Return ``{series_id: [(date_str, mom_decimal), …]}`` sorted by date.

    MoM is computed only for consecutive monthly observations (25–40 day gap).
    Non-positive prior levels and date-parse failures are silently skipped.
    """
    by_id: dict[str, list[tuple[str, float]]] = {}
    for r in latest_rows:
        sid = r.get("series_id", "")
        obs = r.get("observation_date", "")
        val = r.get("value")
        if sid not in wanted or not obs or val is None:
            continue
        try:
            date.fromisoformat(obs)
        except (ValueError, TypeError):
            continue
        by_id.setdefault(sid, []).append((obs, float(val)))

    mom: dict[str, list[tuple[str, float]]] = {}
    for sid, levels in by_id.items():
        levels.sort(key=lambda x: x[0])
        series: list[tuple[str, float]] = []
        for i in range(1, len(levels)):
            d_prev = date.fromisoformat(levels[i - 1][0])
            d_curr = date.fromisoformat(levels[i][0])
            delta = (d_curr - d_prev).days
            prev_val = levels[i - 1][1]
            if 25 <= delta <= 40 and prev_val > 0.0:
                series.append((levels[i][0], (levels[i][1] - prev_val) / prev_val))
        if series:
            mom[sid] = series
    return mom


# ---------------------------------------------------------------------------
# Main engine
# ---------------------------------------------------------------------------

def compute_inflation_forecast(
    latest_rows: Iterable[dict[str, Any]],
    cfg: Optional[InflationForecastConfig] = None,
) -> list[dict[str, Any]]:
    """``gold.inflation_forecast``: AR(p) and VAR(p) MoM inflation forecasts.

    One row per (series_id, horizon_months, model_type).  ``forecast_value``
    and the CI bounds are MoM decimal fractions (e.g. 0.003 = +0.3%).
    ``model_vintage`` equals ``forecast_date`` (re-estimated monthly).
    """
    if cfg is None:
        cfg = load_inflation_forecast_config()

    wanted: set[str] = set(cfg.series) | {s for pair in cfg.var_pairs for s in pair}
    mom_series = _extract_mom(latest_rows, wanted)

    if not mom_series:
        return []

    rng = random.Random(cfg.random_seed)
    out: list[dict[str, Any]] = []

    # --- AR(p) per series ---------------------------------------------------
    for sid in cfg.series:
        if sid not in mom_series:
            continue
        series = mom_series[sid]
        if len(series) < cfg.min_obs:
            continue

        y = [v for _, v in series]
        obs_date = series[-1][0]
        effective_max_lag = max(1, min(cfg.max_ar_lag, len(y) // 4))

        p = _select_ar_lag(y, effective_max_lag)
        fit = _ar_fit(y, p)
        if fit is None:
            continue
        coeffs, residuals = fit

        for h in cfg.horizons:
            fc = _ar_forecast(y, coeffs, h)
            ci = _ar_bootstrap_ci(y, coeffs, residuals, h, cfg.n_bootstrap, rng)
            out.append({
                "series_id": sid,
                "forecast_date": obs_date,
                "horizon_months": h,
                "forecast_value": fc[-1],
                "lower_80": ci["lower_80"],
                "upper_80": ci["upper_80"],
                "lower_95": ci["lower_95"],
                "upper_95": ci["upper_95"],
                "model_type": "ar",
                "lag_order": p,
                "model_vintage": obs_date,
                "n_obs_training": len(y),
            })

    # --- VAR(p) for each pair -----------------------------------------------
    for sid1, sid2 in cfg.var_pairs:
        if sid1 not in mom_series or sid2 not in mom_series:
            continue

        dates1 = {d: v for d, v in mom_series[sid1]}
        dates2 = {d: v for d, v in mom_series[sid2]}
        common_dates = sorted(dates1.keys() & dates2.keys())
        if len(common_dates) < cfg.min_obs:
            continue

        y1 = [dates1[d] for d in common_dates]
        y2 = [dates2[d] for d in common_dates]
        obs_date = common_dates[-1]
        effective_max_lag = max(1, min(cfg.max_var_lag, len(common_dates) // 8))

        p = _select_var_lag(y1, y2, effective_max_lag)
        vfit = _var_fit(y1, y2, p)
        if vfit is None:
            continue
        c1, c2, r1, r2 = vfit

        for h in cfg.horizons:
            fc = _var_forecast(y1, y2, c1, c2, p, h)
            ci1, ci2 = _var_bootstrap_ci(
                y1, y2, c1, c2, r1, r2, p, h, cfg.n_bootstrap, rng
            )
            out.append({
                "series_id": sid1,
                "forecast_date": obs_date,
                "horizon_months": h,
                "forecast_value": fc[-1][0],
                "lower_80": ci1["lower_80"],
                "upper_80": ci1["upper_80"],
                "lower_95": ci1["lower_95"],
                "upper_95": ci1["upper_95"],
                "model_type": "var",
                "lag_order": p,
                "model_vintage": obs_date,
                "n_obs_training": len(y1),
            })
            out.append({
                "series_id": sid2,
                "forecast_date": obs_date,
                "horizon_months": h,
                "forecast_value": fc[-1][1],
                "lower_80": ci2["lower_80"],
                "upper_80": ci2["upper_80"],
                "lower_95": ci2["lower_95"],
                "upper_95": ci2["upper_95"],
                "model_type": "var",
                "lag_order": p,
                "model_vintage": obs_date,
                "n_obs_training": len(y1),
            })

    return out
