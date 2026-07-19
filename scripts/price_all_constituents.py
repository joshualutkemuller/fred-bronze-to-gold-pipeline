"""Drive constituent_pricing.plan_tiingo_constituent_pricing in a loop until
every IVV constituent has a fresh Tiingo price (or is unpriceable), sleeping
out Tiingo's hourly request-quota resets in between batches. Meant to run
unattended for hours: each batch is its own committed unit of work, and Gold
is rebuilt periodically (not after every ticker) to keep this efficient.
"""

from __future__ import annotations

import sys
import time

from fred_pipeline.config import Environment, PipelineConfig
from fred_pipeline.constituent_pricing import (
    plan_tiingo_constituent_pricing,
    specs_for_tiingo_candidates,
)
from fred_pipeline.local_store import LocalWarehouse
from fred_pipeline.pipeline import FredPipeline

INDEX_ETF = "IVV"
BATCH_SIZE = 25
QUOTA_BACKOFF_SECONDS = 65 * 60  # Tiingo's quota is hourly; pad 5 min for safety


def _quota_limited(run) -> bool:
    for series_run in run.series_runs:
        message = (series_run.error_message or "").lower()
        if "http 429" in message or "hourly request allocation" in message:
            return True
        if "rate limit" in message or "quota" in message:
            return True
    return False


def main() -> int:
    config = PipelineConfig.resolve(environment=Environment.DEV)
    warehouse = LocalWarehouse(config, db_path="fred_local.db")
    pipeline = FredPipeline(config, warehouse=warehouse)

    total_priced = 0
    batch_num = 0
    since_last_gold_rebuild = 0

    try:
        while True:
            constituent_rows = warehouse.query(
                """
                SELECT constituent AS ticker, weight_rank, weight_pct
                FROM gold_index_constituents
                WHERE index_etf = ? AND is_latest_snapshot = 1
                ORDER BY weight_rank, constituent
                """,
                (INDEX_ETF,),
            )
            latest_price_rows = warehouse.query(
                """
                SELECT
                    substr(series_id, 1, instr(series_id, ':') - 1) AS ticker,
                    MAX(observation_date) AS latest_price_date
                FROM silver_fred_observation
                WHERE source = 'tiingo' AND series_id LIKE '%:adjClose'
                GROUP BY ticker
                """
            )
            plan = plan_tiingo_constituent_pricing(
                constituent_rows, latest_price_rows,
                index_etf=INDEX_ETF, limit=BATCH_SIZE,
            )

            print(
                f"\n=== batch {batch_num}: {plan.total_constituents} total, "
                f"{plan.already_fresh} fresh, {len(plan.candidates)} candidates, "
                f"batch={len(plan.batch)} ===", flush=True,
            )

            if not plan.batch:
                print("All constituents fresh or unpriceable. Done.", flush=True)
                break

            batch_num += 1
            quota_hit = False
            for spec in specs_for_tiingo_candidates(plan.batch):
                run = pipeline.run(
                    [spec], manifest_path=f"script:price_all_constituents:{INDEX_ETF}",
                    triggered_by="price-all-constituents-script",
                    build_gold_layer=False,
                )
                ok = run.series_succeeded > 0
                print(f"  {spec.series_id}: {'OK' if ok else 'FAILED'}", flush=True)
                if ok:
                    total_priced += 1
                    since_last_gold_rebuild += 1
                if _quota_limited(run):
                    quota_hit = True
                    print(
                        f"  Quota hit at {spec.series_id}; sleeping "
                        f"{QUOTA_BACKOFF_SECONDS}s.", flush=True,
                    )
                    break

            if since_last_gold_rebuild >= 50:
                print("Rebuilding Gold (checkpoint)...", flush=True)
                warehouse.build_gold()
                since_last_gold_rebuild = 0

            if quota_hit:
                time.sleep(QUOTA_BACKOFF_SECONDS)

        print(f"\nTotal priced this run: {total_priced}. Final Gold rebuild...", flush=True)
        warehouse.build_gold()
    finally:
        warehouse.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
