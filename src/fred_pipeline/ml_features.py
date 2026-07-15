"""ML-0: Feature matrix for the ML pipeline (pure Python).

Reads ``gold_fred_feature_transforms`` rows (one row per series × date with
``value`` / ``mom`` / ``diff`` / ``yoy`` / ``zscore`` columns) and extracts the
configured series/transform pair into a tidy (long-format) feature matrix — one
row per ``(observation_date, feature_name)``.

The tidy output is the canonical ML input for the downstream PCA (ML-2) and
anomaly-detection (ML-4) engines.  The same row format is written to
``gold.ml_feature_matrix`` and read back by those engines.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Optional

import yaml

_CONFIG_PATH = Path(__file__).parent.parent.parent / "config" / "ml_features.yml"
_VALID_TRANSFORMS = frozenset({"level", "mom", "diff", "yoy", "zscore"})
_KNOWN_TOP_KEYS = frozenset(
    {"features", "min_features_for_pca", "n_components", "anomaly_threshold"}
)

# Maps the config transform name to the column in gold_fred_feature_transforms.
_TRANSFORM_COL: dict[str, str] = {
    "level": "value",
    "mom": "mom",
    "diff": "diff",
    "yoy": "yoy",
    "zscore": "zscore",
}


class MLFeatureConfigError(ValueError):
    pass


@dataclass(frozen=True)
class MLFeatureDef:
    name: str
    series_id: str
    transform: str


@dataclass(frozen=True)
class MLFeatureConfig:
    features: tuple[MLFeatureDef, ...]
    min_features_for_pca: int = 5
    n_components: int = 5
    anomaly_threshold: float = 0.99


def load_ml_feature_config(path: Optional[str] = None) -> MLFeatureConfig:
    """Load and validate the ML feature config from YAML."""
    p = Path(path) if path else _CONFIG_PATH
    if not p.exists():
        raise MLFeatureConfigError(f"ML feature config not found: {p}")
    with open(p) as fh:
        raw = yaml.safe_load(fh) or {}
    unknown = set(raw) - _KNOWN_TOP_KEYS
    if unknown:
        raise MLFeatureConfigError(f"Unknown keys in ML feature config: {sorted(unknown)}")
    raw_features = raw.get("features", [])
    features: list[MLFeatureDef] = []
    seen_names: set[str] = set()
    for item in raw_features:
        name = item.get("name", "")
        sid = item.get("series_id", "")
        transform = item.get("transform", "level")
        if not name or not sid:
            raise MLFeatureConfigError(
                f"Feature entry missing 'name' or 'series_id': {item}"
            )
        if transform not in _VALID_TRANSFORMS:
            raise MLFeatureConfigError(
                f"Invalid transform {transform!r} for feature {name!r}; "
                f"allowed: {sorted(_VALID_TRANSFORMS)}"
            )
        if name in seen_names:
            raise MLFeatureConfigError(f"Duplicate feature name: {name!r}")
        seen_names.add(name)
        features.append(MLFeatureDef(name=name, series_id=sid, transform=transform))
    return MLFeatureConfig(
        features=tuple(features),
        min_features_for_pca=int(raw.get("min_features_for_pca", 5)),
        n_components=int(raw.get("n_components", 5)),
        anomaly_threshold=float(raw.get("anomaly_threshold", 0.99)),
    )


def compute_ml_feature_matrix(
    feature_transform_rows: Iterable[dict[str, Any]],
    cfg: Optional[MLFeatureConfig] = None,
) -> list[dict[str, Any]]:
    """``gold.ml_feature_matrix``: tidy ML feature matrix.

    One row per ``(observation_date, feature_name)`` for each configured feature
    that has a non-null value.  The ``transform`` column records which column of
    ``gold_fred_feature_transforms`` was selected (``level`` → ``value``,
    ``yoy`` → ``yoy``, etc.).
    """
    if cfg is None:
        cfg = load_ml_feature_config()
    if not cfg.features:
        return []

    # Index by series_id → list[MLFeatureDef] so one series can yield multiple
    # features (e.g. NFCI at both level and zscore).
    by_series: dict[str, list[MLFeatureDef]] = {}
    for fd in cfg.features:
        by_series.setdefault(fd.series_id, []).append(fd)

    out: list[dict[str, Any]] = []
    for r in feature_transform_rows:
        sid = r.get("series_id", "")
        if sid not in by_series:
            continue
        obs_date = r.get("observation_date")
        if obs_date is None:
            continue
        for fd in by_series[sid]:
            col = _TRANSFORM_COL[fd.transform]
            val = r.get(col)
            if val is None:
                continue
            out.append({
                "observation_date": obs_date,
                "feature_name": fd.name,
                "series_id": fd.series_id,
                "transform": fd.transform,
                "value": float(val),
            })
    return out
