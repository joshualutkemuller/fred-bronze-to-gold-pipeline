"""ML-2: Expanding PCA factor scores — pure Python, scipy-free.

Reads the tidy ``gold.ml_feature_matrix`` and computes monthly expanding
principal-component factor scores via:

  * **Welford's online covariance** — running mean and M2 matrix updated once
    per monthly snapshot; no need to store the full history.
  * **Power iteration with deflation** — extracts the top-*k* eigenpairs from
    the running covariance matrix, which is symmetric positive semi-definite.
  * **Sign anchoring** — each eigenvector's sign is fixed so the component with
    the largest absolute loading is always positive (prevents random sign flips
    across time steps).

Outputs ``gold.macro_factor_scores`` (one row per snapshot × factor) and
``gold.macro_factor_loadings`` (one row per snapshot × factor × feature).
"""

from __future__ import annotations

import math
from datetime import date
from typing import Any, Iterable


# ---- Welford online covariance matrix ----------------------------------------

class _WelfordCov:
    """Incremental k×k covariance via Welford's online algorithm."""

    def __init__(self, k: int) -> None:
        self.k = k
        self.n = 0
        self.mean = [0.0] * k
        self.M2 = [[0.0] * k for _ in range(k)]

    def update(self, x: list[float]) -> None:
        self.n += 1
        delta = [x[j] - self.mean[j] for j in range(self.k)]
        for j in range(self.k):
            self.mean[j] += delta[j] / self.n
        delta2 = [x[j] - self.mean[j] for j in range(self.k)]
        for i in range(self.k):
            for j in range(self.k):
                self.M2[i][j] += delta[i] * delta2[j]

    def covariance(self) -> list[list[float]]:
        if self.n < 2:
            return [[0.0] * self.k for _ in range(self.k)]
        d = self.n - 1
        return [[self.M2[i][j] / d for j in range(self.k)] for i in range(self.k)]


# ---- power iteration + deflation for symmetric matrices ----------------------

def _mat_vec(A: list[list[float]], v: list[float]) -> list[float]:
    n = len(A)
    return [sum(A[i][j] * v[j] for j in range(n)) for i in range(n)]


def _power_iter(
    A: list[list[float]], max_iter: int = 500, tol: float = 1e-9
) -> tuple[float, list[float]]:
    """Dominant eigenvalue and unit eigenvector of a symmetric matrix."""
    k = len(A)
    v = [1.0 / math.sqrt(k)] * k
    eigenvalue = 0.0
    for _ in range(max_iter):
        Av = _mat_vec(A, v)
        new_ev = sum(v[j] * Av[j] for j in range(k))
        nrm = math.sqrt(sum(x * x for x in Av))
        if nrm == 0.0:
            break
        v_new = [x / nrm for x in Av]
        if abs(new_ev - eigenvalue) < tol:
            eigenvalue = new_ev
            v = v_new
            break
        eigenvalue = new_ev
        v = v_new
    return eigenvalue, v


def _deflate(
    A: list[list[float]], eigenvalue: float, evec: list[float]
) -> list[list[float]]:
    """A ← A − λ·v·vᵀ (remove one eigenpair from a symmetric matrix)."""
    k = len(A)
    return [
        [A[i][j] - eigenvalue * evec[i] * evec[j] for j in range(k)]
        for i in range(k)
    ]


def _top_k_eigenpairs(
    cov: list[list[float]], k: int
) -> list[tuple[float, list[float]]]:
    """Top-*k* (eigenvalue, unit eigenvector) pairs via power iteration + deflation."""
    pairs: list[tuple[float, list[float]]] = []
    A = [row[:] for row in cov]
    for _ in range(k):
        ev, vec = _power_iter(A)
        if ev <= 0.0:
            break
        pairs.append((ev, vec))
        A = _deflate(A, ev, vec)
    return pairs


# ---- main engine -------------------------------------------------------------

def compute_macro_factor_scores(
    feature_matrix_rows: Iterable[dict[str, Any]],
    n_components: int = 5,
    min_obs: int = 30,
) -> dict[str, list[dict[str, Any]]]:
    """``gold.macro_factor_scores`` + ``gold.macro_factor_loadings``.

    Takes the tidy ``gold.ml_feature_matrix`` and produces monthly expanding
    PCA factor scores and loadings.  Returns a dict with keys ``"scores"`` and
    ``"loadings"``.

    Each calendar month's last available date is the snapshot date; dates with
    fewer than ``k_feat // 2`` features present are skipped.  Missing feature
    values at the snapshot date are imputed with the current running mean so the
    covariance matrix stays full-rank.  At least ``min_obs`` snapshots must have
    been accumulated before scores are emitted.
    """
    # Group rows into {date_str: {feature_name: value}}
    by_date: dict[str, dict[str, float]] = {}
    for r in feature_matrix_rows:
        d = r.get("observation_date", "")
        fn = r.get("feature_name", "")
        v = r.get("value")
        if d and fn and v is not None:
            by_date.setdefault(d, {})[fn] = float(v)

    if not by_date:
        return {"scores": [], "loadings": []}

    # Monthly snapshots: last date per (year, month)
    monthly: dict[tuple[int, int], str] = {}
    for d_str in sorted(by_date):
        try:
            d = date.fromisoformat(d_str)
        except ValueError:
            continue
        monthly[(d.year, d.month)] = d_str

    sorted_months = sorted(monthly)
    if not sorted_months:
        return {"scores": [], "loadings": []}

    # Feature universe = sorted union of all feature names ever seen
    all_features = sorted({fn for row in by_date.values() for fn in row})
    k_feat = len(all_features)
    if k_feat < 2:
        return {"scores": [], "loadings": []}

    k_comp = min(n_components, k_feat)
    feat_idx = {fn: i for i, fn in enumerate(all_features)}

    welford = _WelfordCov(k_feat)
    scores_out: list[dict[str, Any]] = []
    loadings_out: list[dict[str, Any]] = []

    for ym in sorted_months:
        d_str = monthly[ym]
        row_map = by_date[d_str]

        # Build feature vector; skip if too many features are missing
        x_raw = [row_map.get(fn) for fn in all_features]
        n_present = sum(1 for v in x_raw if v is not None)
        if n_present < max(2, k_feat // 2):
            continue

        # Impute missing values with the current running mean (0 initially)
        x = [
            (v if v is not None else welford.mean[i])
            for i, v in enumerate(x_raw)
        ]
        welford.update(x)

        if welford.n < min_obs:
            continue

        cov = welford.covariance()
        total_var = sum(cov[i][i] for i in range(k_feat))
        if total_var <= 0.0:
            continue

        pairs = _top_k_eigenpairs(cov, k_comp)
        if not pairs:
            continue

        # Sign-anchor: flip so the max-abs-loading component is positive
        anchored: list[tuple[float, list[float]]] = []
        for ev, evec in pairs:
            max_i = max(range(k_feat), key=lambda i: abs(evec[i]))
            if evec[max_i] < 0.0:
                evec = [-v for v in evec]
            anchored.append((ev, evec))

        # Project the centred snapshot onto the eigenvectors
        x_c = [x[i] - welford.mean[i] for i in range(k_feat)]
        cum_evr = 0.0
        for comp_idx, (ev, evec) in enumerate(anchored, start=1):
            score = sum(x_c[i] * evec[i] for i in range(k_feat))
            evr = ev / total_var
            cum_evr += evr
            scores_out.append({
                "observation_date": d_str,
                "factor": comp_idx,
                "score": score,
                "explained_variance_ratio": evr,
                "cumulative_variance_ratio": cum_evr,
                "n_obs": welford.n,
            })
            for fn, loading in zip(all_features, evec):
                loadings_out.append({
                    "observation_date": d_str,
                    "factor": comp_idx,
                    "feature_name": fn,
                    "loading": loading,
                })

    return {"scores": scores_out, "loadings": loadings_out}
