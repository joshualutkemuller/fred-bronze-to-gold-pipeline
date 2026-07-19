"""One-off remediation: force-full-reload Tiingo tickers stuck at 1 day of
history (pre-fix bug: full loads defaulted to Tiingo's 'no startDate' ==
latest-only behavior). Covers both manifest-defined tickers (equity_tiingo.yml)
and dynamically-added constituent-pricing tickers (never in a manifest, so
`--source tiingo --full` can't reach them). Skips the per-call Gold rebuild;
call `warehouse.build_gold()` once at the end instead.
"""

from __future__ import annotations

import sys

from fred_pipeline.config import Environment, PipelineConfig
from fred_pipeline.local_store import LocalWarehouse
from fred_pipeline.manifest import SeriesSpec
from fred_pipeline.pipeline import FredPipeline

TICKERS = [
    "ABBV", "AMAT", "AMD", "AMZN", "AVGO", "BRKB", "CAT", "COST", "CSCO",
    "EWG", "EWU", "EWZ", "FXI", "GE", "GOOG", "HD", "INDA", "INTC", "KLAC",
    "KO", "LLY", "LRCX", "MA", "MRK", "MU", "NVDA", "PANW", "PG", "PM", "V",
    "VNQ", "WMT",
]


def main() -> int:
    config = PipelineConfig.resolve(environment=Environment.DEV)
    warehouse = LocalWarehouse(config, db_path="fred_local.db")
    pipeline = FredPipeline(config, warehouse=warehouse)

    specs = [
        SeriesSpec(
            series_id=t, title=f"{t} full-history backfill", category="equity",
            frequency="d", units="USD", active=True, source="tiingo",
            vintage_enabled=False, validation_profile="standard",
            downstream_use_case="equity_backfill", priority=3, min_value=0,
            tags=["equity", "stock", "tiingo", "backfill"],
        )
        for t in TICKERS
    ]

    succeeded, failed = 0, 0
    for spec in specs:
        run = pipeline.run(
            [spec], manifest_path="script:backfill_stuck_tiingo",
            triggered_by="backfill-script", build_gold_layer=False,
            force_full=True,
        )
        succeeded += run.series_succeeded
        failed += run.series_failed
        status = "OK" if run.series_succeeded else "FAILED"
        err = ""
        if run.series_runs and run.series_runs[0].error_message:
            err = f" ({run.series_runs[0].error_message[:120]})"
        print(f"{spec.series_id}: {status}{err}", flush=True)

    print(f"\nDone: {succeeded} ok, {failed} failed. Rebuilding Gold...", flush=True)
    warehouse.build_gold()
    warehouse.close()
    print("Gold rebuild complete.", flush=True)
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
