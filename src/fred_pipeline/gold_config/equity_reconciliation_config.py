"""Config loader for equity cross-source price reconciliation.

Reads ``config/equity_reconciliations.yml`` (or a custom path) and returns
an :class:`EquityReconciliationConfig` that drives
:func:`~fred_pipeline.equity_views.compute_equity_price_reconciliation`.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

import yaml

DEFAULT_PATH = "config/equity_reconciliations.yml"


class EquityReconciliationConfigError(ValueError):
    pass


@dataclass(frozen=True)
class EquityReconciliationConfig:
    """Which tickers to reconcile and how strictly.

    ``tolerance_pct`` is the absolute percent-difference above which an
    observation is flagged ``diverged`` (relative to the Tiingo adjClose).
    """

    tickers: tuple[str, ...] = ()
    tolerance_pct: float = 2.0


def load_equity_reconciliation_config(
    path: Optional[str] = None,
) -> EquityReconciliationConfig:
    """Load config from YAML.  Missing file → empty config; malformed → raises."""
    resolved = path or os.environ.get("FRED_EQUITY_RECONCILIATIONS_FILE") or DEFAULT_PATH
    if not resolved or not os.path.isfile(resolved):
        return EquityReconciliationConfig()
    with open(resolved, "r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    if not isinstance(data, dict):
        raise EquityReconciliationConfigError(f"{resolved}: top-level must be a mapping")
    block = data.get("equity_reconciliations")
    if block is None:
        return EquityReconciliationConfig()
    if not isinstance(block, dict):
        raise EquityReconciliationConfigError(
            f"{resolved}: 'equity_reconciliations' must be a mapping"
        )
    known = {"tolerance_pct", "tickers"}
    unknown = set(block) - known
    if unknown:
        raise EquityReconciliationConfigError(
            f"{resolved}: unknown key(s) under equity_reconciliations: {sorted(unknown)}"
        )
    tol = float(block.get("tolerance_pct", 2.0))
    if tol < 0:
        raise EquityReconciliationConfigError(
            f"{resolved}: tolerance_pct must be >= 0, got {tol}"
        )
    raw_tickers = block.get("tickers") or []
    if not isinstance(raw_tickers, list):
        raise EquityReconciliationConfigError(
            f"{resolved}: 'tickers' must be a list"
        )
    tickers = tuple(str(t).strip().upper() for t in raw_tickers if str(t).strip())
    if len(tickers) != len(set(tickers)):
        dupes = [t for t in tickers if tickers.count(t) > 1]
        raise EquityReconciliationConfigError(
            f"{resolved}: duplicate ticker(s): {sorted(set(dupes))}"
        )
    return EquityReconciliationConfig(tickers=tickers, tolerance_pct=tol)
