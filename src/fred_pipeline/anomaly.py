"""ML-4: Mahalanobis anomaly detection in PCA factor space (pure Python).

Reads ``gold.macro_factor_scores`` and computes, for each monthly snapshot:

  * **Mahalanobis D²** = Σ_k ((score_k − μ_k) / σ_k)², where μ_k and σ_k are
    the *expanding* mean and standard deviation of the k-th factor score up to
    that date (Welford's algorithm, PIT-safe).
  * **p-value** = P(χ²(k) > D²), using the regularised upper incomplete gamma
    Q(k/2, D²/2) — the same algorithm family as ``_betainc`` in
    ``regime_stats.py`` (Lentz continued fraction + series expansion, no
    SciPy dependency).
  * **is_anomaly** flag when the p-value falls below (1 − anomaly_threshold).

The χ²(k) assumption is exact when the factor scores are jointly Gaussian and
the expanding estimates have converged; for finite history it is a useful
heuristic calibrated by ``min_obs``.
"""

from __future__ import annotations

import math
from typing import Any, Iterable


# ---- regularised upper incomplete gamma Q(a, x) = Γ(a, x) / Γ(a) -----------

def _gammainc_series(a: float, x: float) -> float:
    """Lower regularised incomplete gamma P(a, x) via series expansion.

    Accurate for x < a + 1 (Numerical Recipes §6.2).
    """
    if x == 0.0:
        return 0.0
    ap = a
    term = 1.0 / a
    total = term
    for _ in range(300):
        ap += 1.0
        term *= x / ap
        total += term
        if abs(term) < abs(total) * 1e-12:
            break
    return math.exp(-x + a * math.log(x) - math.lgamma(a)) * total


def _gammainc_cf(a: float, x: float) -> float:
    """Upper regularised incomplete gamma Q(a, x) via Lentz continued fraction.

    Accurate for x ≥ a + 1 (Numerical Recipes §6.2).
    """
    tiny = 1e-30
    b = x + 1.0 - a
    c = 1.0 / tiny
    d = 1.0 / b if abs(b) >= tiny else 1.0 / tiny
    h = d
    for i in range(1, 300):
        an = -i * (i - a)
        b += 2.0
        d = an * d + b
        if abs(d) < tiny:
            d = tiny
        c = b + an / c
        if abs(c) < tiny:
            c = tiny
        d = 1.0 / d
        delta = d * c
        h *= delta
        if abs(delta - 1.0) < 1e-12:
            break
    return math.exp(-x + a * math.log(x) - math.lgamma(a)) * h


def _chi2_sf(x: float, k: int) -> float:
    """Survival function of χ²(k): P(χ²(k) > x) = Q(k/2, x/2)."""
    if x <= 0.0:
        return 1.0
    a = k / 2.0
    z = x / 2.0
    if z < a + 1.0:
        return 1.0 - _gammainc_series(a, z)
    return _gammainc_cf(a, z)


# ---- anomaly scoring ---------------------------------------------------------

def compute_macro_anomaly_scores(
    factor_scores_rows: Iterable[dict[str, Any]],
    min_obs: int = 20,
    anomaly_threshold: float = 0.99,
) -> list[dict[str, Any]]:
    """``gold.macro_anomaly_scores``: Mahalanobis D² anomaly scores.

    For each date in ``gold.macro_factor_scores``, computes
    D² = Σ_k ((score_k − μ_k) / σ_k)² using *expanding* per-factor mean and
    std (Welford).  Rows are emitted once each factor has accumulated at least
    ``min_obs`` snapshots.

    ``is_anomaly`` is 1 when ``p_value < 1 − anomaly_threshold``
    (default: p-value < 0.01 → top-1 % of χ²(k)).
    """
    # Group by date: {date_str: [(factor, score)]}
    by_date: dict[str, list[tuple[int, float]]] = {}
    for r in factor_scores_rows:
        d = r.get("observation_date", "")
        factor = r.get("factor")
        score = r.get("score")
        if d and factor is not None and score is not None:
            by_date.setdefault(d, []).append((int(factor), float(score)))

    if not by_date:
        return []

    sorted_dates = sorted(by_date)

    # Expanding Welford per factor (1-D)
    f_n: dict[int, int] = {}
    f_mean: dict[int, float] = {}
    f_M2: dict[int, float] = {}

    out: list[dict[str, Any]] = []
    for d_str in sorted_dates:
        factor_scores = by_date[d_str]
        if not factor_scores:
            continue

        # Update expanding stats (including current observation)
        for factor, score in factor_scores:
            n = f_n.get(factor, 0) + 1
            f_n[factor] = n
            delta = score - f_mean.get(factor, 0.0)
            f_mean[factor] = f_mean.get(factor, 0.0) + delta / n
            delta2 = score - f_mean[factor]
            f_M2[factor] = f_M2.get(factor, 0.0) + delta * delta2

        # Skip until every present factor has enough history
        if any(f_n.get(f, 0) < min_obs for f, _ in factor_scores):
            continue

        d2_terms: list[float] = []
        for factor, score in factor_scores:
            n = f_n[factor]
            if n < 2:
                continue
            var = f_M2[factor] / (n - 1)
            if var <= 0.0:
                continue
            z = (score - f_mean[factor]) / math.sqrt(var)
            d2_terms.append(z * z)

        if not d2_terms:
            continue

        d2 = sum(d2_terms)
        k = len(d2_terms)
        p_value = _chi2_sf(d2, k)
        out.append({
            "observation_date": d_str,
            "mahalanobis_d2": d2,
            "chi2_df": k,
            "p_value": p_value,
            "is_anomaly": int(p_value < 1.0 - anomaly_threshold),
            "n_factors_used": k,
        })

    return out
