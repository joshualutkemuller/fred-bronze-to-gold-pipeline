#!/usr/bin/env python
"""Benchmark pipeline performance across stages.

Measures end-to-end runtime and stage-by-stage timing for the FRED pipeline.
Generates a report showing where time is spent and scaling characteristics.

Usage
-----
Run with default series::

    FRED_API_KEY=your_key python scripts/benchmark_pipeline.py

Run with custom series and iterations::

    FRED_API_KEY=your_key python scripts/benchmark_pipeline.py \\
        --series GDPC1,UNRATE,CPIAUCSL \\
        --iterations 3 \\
        --db-path /tmp/bench.db

Run in dry-run mode (no writes)::

    FRED_API_KEY=your_key python scripts/benchmark_pipeline.py --dry-run

Profile a full catalog run (slow, requires many API calls)::

    FRED_API_KEY=your_key python scripts/benchmark_pipeline.py \\
        --full-catalog --db-path /tmp/full_bench.db
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
log = logging.getLogger("benchmark_pipeline")


class BenchmarkResult:
    """Single benchmark run result."""

    def __init__(self, series_count: int, dry_run: bool, total_elapsed: float):
        self.series_count = series_count
        self.dry_run = dry_run
        self.total_elapsed = total_elapsed
        self.timestamp = datetime.now().isoformat()

    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp,
            "series_count": self.series_count,
            "dry_run": self.dry_run,
            "total_elapsed_sec": round(self.total_elapsed, 2),
        }


def run_benchmark(
    series: Optional[list[str]] = None,
    db_path: str = "/tmp/benchmark.db",
    dry_run: bool = False,
    full_catalog: bool = False,
) -> BenchmarkResult:
    """Run pipeline and measure performance.

    Parameters
    ----------
    series : list[str], optional
        Series IDs to ingest. If None and full_catalog=False, uses default set.
    db_path : str
        SQLite database path for local backend.
    dry_run : bool
        If True, run without writes (in-memory only).
    full_catalog : bool
        If True, ingest all active series (slow, requires many API calls).

    Returns
    -------
    BenchmarkResult
        Timing information for the run.
    """
    from fred_pipeline.config import Environment, PipelineConfig
    from fred_pipeline.pipeline import FredPipeline
    from fred_pipeline.manifest import load_manifests, all_series
    from fred_pipeline.io.database_connection import DatabaseConnectionFactory

    config = PipelineConfig.resolve(
        environment=Environment.dev,
    )

    db = DatabaseConnectionFactory.create("local", db_path=db_path) if not dry_run else None

    pipeline = FredPipeline(config, warehouse=db if db else None, persist_audit=not dry_run)

    # Determine which series to run
    if full_catalog:
        log.info("Loading full catalog (all active series)...")
        active = all_series(load_manifests("manifests"), active_only=True)
        series_list = [s.series_id for s in active]
        log.info(f"Full catalog: {len(series_list)} series")
    elif series:
        series_list = series
        log.info(f"Running with {len(series_list)} specified series: {', '.join(series_list[:5])}{'...' if len(series_list) > 5 else ''}")
    else:
        # Default benchmark set: covers all major categories
        series_list = [
            "GDPC1",      # Growth
            "UNRATE",     # Labor
            "CPIAUCSL",   # Inflation
            "DGS10",      # Rates
            "SOFR",       # Funding
            "BAMLH0A0HYM2",  # Credit
        ]
        log.info(f"Running with default benchmark set ({len(series_list)} series)")

    log.info(f"{'DRY RUN' if dry_run else 'WITH WRITES'} to {db_path}")
    log.info("=" * 70)

    # Run pipeline
    start = time.perf_counter()
    try:
        run = pipeline.run_from_manifest(
            "manifests",
            series=series_list if len(series_list) < 100 else None,  # Don't pass full list if huge
            build_gold_layer=not dry_run,
        )
        elapsed = time.perf_counter() - start

        log.info("=" * 70)
        log.info(f"Pipeline completed: {run.status.value}")
        log.info(f"Total elapsed: {elapsed:.2f}s")
        log.info(f"Series succeeded: {run.series_succeeded}, failed: {run.series_failed}")

        # Try to extract stage timings from audit if available
        if db and not dry_run:
            try:
                from fred_pipeline.io.database_connection import DatabaseConnectionFactory
                db_conn = DatabaseConnectionFactory.create("local", db_path=db_path)
                stages = db_conn.query(
                    """
                    SELECT stage, elapsed_sec FROM audit_etl_stage
                    WHERE run_id = ? ORDER BY stage
                    """,
                    {"run_id": run.run_id}
                )
                if stages:
                    log.info("\nStage breakdown:")
                    for stage_name, stage_elapsed in stages:
                        pct = (stage_elapsed / elapsed * 100) if elapsed > 0 else 0
                        log.info(f"  {stage_name:<20} {stage_elapsed:>7.2f}s ({pct:>5.1f}%)")
                db_conn.close()
            except Exception as e:
                log.warning(f"Could not fetch stage timings: {e}")

        return BenchmarkResult(len(series_list), dry_run, elapsed)

    except Exception as e:
        elapsed = time.perf_counter() - start
        log.error(f"Pipeline failed after {elapsed:.2f}s: {e}", exc_info=True)
        raise
    finally:
        if db:
            db.close()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Benchmark FRED pipeline performance.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "--series",
        help="Comma-separated series IDs to benchmark (default: 6-series benchmark set)",
    )
    parser.add_argument(
        "--full-catalog",
        action="store_true",
        help="Benchmark full active catalog (slow, many API calls)",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=1,
        help="Number of benchmark iterations (default: 1)",
    )
    parser.add_argument(
        "--db-path",
        default="/tmp/benchmark.db",
        help="SQLite database path (default: /tmp/benchmark.db)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run without writes (in-memory, faster)",
    )
    parser.add_argument(
        "--output",
        help="Save results to JSON file (optional)",
    )

    args = parser.parse_args()

    series_list = None
    if args.series:
        series_list = [s.strip() for s in args.series.split(",")]

    results = []
    log.info(f"Starting benchmark ({args.iterations} iteration(s))...")
    log.info("")

    for i in range(args.iterations):
        if args.iterations > 1:
            log.info(f"\n{'='*70}")
            log.info(f"Iteration {i+1}/{args.iterations}")
            log.info(f"{'='*70}\n")

        try:
            result = run_benchmark(
                series=series_list,
                db_path=args.db_path,
                dry_run=args.dry_run,
                full_catalog=args.full_catalog,
            )
            results.append(result)
        except Exception as e:
            log.error(f"Iteration {i+1} failed: {e}")
            if args.iterations == 1:
                return 1
            continue

    # Summary
    log.info(f"\n{'='*70}")
    log.info("BENCHMARK SUMMARY")
    log.info(f"{'='*70}")

    if results:
        times = [r.total_elapsed for r in results]
        avg_time = sum(times) / len(times)
        min_time = min(times)
        max_time = max(times)

        log.info(f"Iterations: {len(results)}/{args.iterations}")
        log.info(f"Series per run: {results[0].series_count}")
        log.info(f"Average time: {avg_time:.2f}s")
        log.info(f"Min time: {min_time:.2f}s")
        log.info(f"Max time: {max_time:.2f}s")

        if len(times) > 1:
            stddev = (sum((t - avg_time) ** 2 for t in times) / len(times)) ** 0.5
            log.info(f"Std dev: {stddev:.2f}s")

        # Estimate scaling
        per_series = avg_time / results[0].series_count if results[0].series_count > 0 else 0
        log.info(f"\nScaling: ~{per_series:.3f}s per series (based on average)")

        if args.output:
            output_path = Path(args.output)
            output_path.write_text(
                json.dumps(
                    {
                        "summary": {
                            "iterations": len(results),
                            "series_per_run": results[0].series_count,
                            "average_sec": round(avg_time, 2),
                            "min_sec": round(min_time, 2),
                            "max_sec": round(max_time, 2),
                            "stddev_sec": round(stddev, 2) if len(times) > 1 else None,
                            "per_series_sec": round(per_series, 3),
                        },
                        "runs": [r.to_dict() for r in results],
                    },
                    indent=2,
                )
            )
            log.info(f"\nResults saved to {output_path}")

        return 0
    else:
        log.error("No successful runs")
        return 1


if __name__ == "__main__":
    sys.exit(main())
