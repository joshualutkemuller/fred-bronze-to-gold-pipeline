"""ML-2: Expanding PCA factor scores — NumPy-accelerated, scipy-free.

Reads the tidy ``gold.ml_feature_matrix`` and computes monthly expanding
principal-component factor scores via:

  * **Welford's online covariance** — running mean and M2 matrix updated once
    per monthly snapshot; no need to store the full history.
  * **np.linalg.eigh** — extracts all eigenpairs of the symmetric covariance
    matrix in one LAPACK call (replaces the old pure-Python power iteration).
  * **Sign anchoring** — each eigenvector's sign is fixed so the component with
    the largest absolute loading is always positive (prevents random sign flips
    across time steps).

Outputs ``gold.macro_factor_scores`` (one row per snapshot × factor) and
``gold.macro_factor_loadings`` (one row per snapshot × factor × feature).
"""

from __future__ import annotations

from datetime import date
from typing import Any, Iterable

import numpy as np


# ---- Welford online covariance matrix (NumPy internals) ----------------------

class _WelfordCov:
    """Incremental k×k covariance via Welford's online algorithm.

    Public API is unchanged: ``update(x: list[float])`` / ``covariance()``
    returning an indexable 2-D structure (now a numpy ndarray).
    """

    def __init__(self, k: int) -> None:
        self.k = k
        self.n = 0
        self.mean: np.ndarray = np.zeros(k)
        self.M2: np.ndarray = np.zeros((k, k))

    def update(self, x: Any) -> None:
        """Accept a list or numpy array of length k."""
        self.n += 1
        x_arr = np.asarray(x, dtype=float)
        delta = x_arr - self.mean
        self.mean += delta / self.n
        delta2 = x_arr - self.mean
        self.M2 += np.outer(delta, delta2)

    def covariance(self) -> np.ndarray:
        if self.n < 2:
            return np.zeros((self.k, self.k))
        return self.M2 / (self.n - 1)


# ---- top-k eigenpairs via numpy.linalg.eigh ----------------------------------

def _top_k_eigenpairs(
    cov: Any, k: int
) -> list[tuple[float, np.ndarray]]:
    """Top-k (eigenvalue, unit eigenvector) pairs for a symmetric PSD matrix.

    Uses ``np.linalg.eigh`` (LAPACK dsyevd) which is O(k_feat^3) but runs
    as a single BLAS/LAPACK call — orders of magnitude faster than the old
    pure-Python power iteration for k_feat > ~10.

    ``cov`` may be a list-of-lists or a numpy ndarray; it is converted
    internally so callers do not need to change.
    """
    A = np.asarray(cov, dtype=float)
    # eigh returns eigenvalues in ascending order
    eigvals, eigvecs = np.linalg.eigh(A)
    pairs: list[tuple[float, np.ndarray]] = []
    # Walk from the largest eigenvalue down
    for idx in range(len(eigvals) - 1, -1, -1):
        if len(pairs) >= k:
            break
        ev = float(eigvals[idx])
        if ev <= 0.0:
            break
        pairs.append((ev, eigvecs[:, idx]))
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

    welford = _WelfordCov(k_feat)
    scores_out: list[dict[str, Any]] = []
    loadings_out: list[dict[str, Any]] = []

    for ym in sorted_months:
        d_str = monthly[ym]
        row_map = by_date[d_str]

        # Build feature vector; skip if too many features are missing
        x_raw = np.array([row_map.get(fn, np.nan) for fn in all_features])
        n_present = int(np.sum(~np.isnan(x_raw)))
        if n_present < max(2, k_feat // 2):
            continue

        # Impute missing values with the current running mean (0 initially)
        x = np.where(np.isnan(x_raw), welford.mean, x_raw)
        welford.update(x)

        if welford.n < min_obs:
            continue

        cov = welford.covariance()
        total_var = float(np.trace(cov))
        if total_var <= 0.0:
            continue

        pairs = _top_k_eigenpairs(cov, k_comp)
        if not pairs:
            continue

        # Sign-anchor: flip so the max-abs-loading component is positive
        anchored: list[tuple[float, np.ndarray]] = []
        for ev, evec in pairs:
            max_i = int(np.argmax(np.abs(evec)))
            if evec[max_i] < 0.0:
                evec = -evec
            anchored.append((ev, evec))

        # Project the centred snapshot onto the eigenvectors
        x_c = x - welford.mean
        cum_evr = 0.0
        for comp_idx, (ev, evec) in enumerate(anchored, start=1):
            score = float(np.dot(x_c, evec))
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
            for fn, loading in zip(all_features, evec.tolist()):
                loadings_out.append({
                    "observation_date": d_str,
                    "factor": comp_idx,
                    "feature_name": fn,
                    "loading": loading,
                })

    return {"scores": scores_out, "loadings": loadings_out}
