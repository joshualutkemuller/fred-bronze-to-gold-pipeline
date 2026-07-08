# Databricks entrypoint for the FRED pipeline.
#
# Runs as a `spark_python_task` (or can be imported as a notebook). It resolves
# the FRED API key from a Databricks secret scope, builds a SparkSession, and
# invokes the orchestrator. Two stages are supported so the job can validate
# manifests before ingesting:
#
#   --stage=validate   validate manifests only (fast, no writes)
#   --stage=run        full Bronze -> Silver -> DQ -> Gold + audit
#
# Example (local dry run, no Spark writes):
#   FRED_API_KEY=... PYTHONPATH=src python notebooks/run_pipeline.py \
#       --stage=run --env=dev --manifests=manifests --dry-run

from __future__ import annotations

import argparse
import json
import sys

from fred_pipeline.config import Environment, PipelineConfig
from fred_pipeline.manifest import all_series, load_manifests
from fred_pipeline.pipeline import FredPipeline


def _get_dbutils():
    try:  # available inside Databricks
        return dbutils  # type: ignore  # noqa: F821
    except NameError:
        return None


def _get_spark(dry_run: bool):
    if dry_run:
        return None
    from fred_pipeline.spark_io import get_spark

    return get_spark()


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--stage", default="run", choices=["validate", "run"])
    parser.add_argument("--env", default="dev", choices=[e.value for e in Environment])
    parser.add_argument("--manifests", default="manifests")
    parser.add_argument("--dry-run", action="store_true")
    args, _unknown = parser.parse_known_args(argv)

    if args.stage == "validate":
        manifests = load_manifests(args.manifests)
        specs = all_series(manifests, active_only=False)
        print(f"Manifest validation OK: {len(specs)} series across "
              f"{len(manifests)} file(s).")
        return 0

    dbutils = _get_dbutils()
    config = PipelineConfig.resolve(environment=Environment(args.env), dbutils=dbutils)
    if not config.fred_api_key:
        print("ERROR: FRED API key not resolved from secret scope or env.",
              file=sys.stderr)
        return 2

    spark = _get_spark(args.dry_run)
    pipeline = FredPipeline(config, spark=spark, persist_audit=not args.dry_run)
    run = pipeline.run_from_manifest(
        args.manifests,
        triggered_by="databricks_job",
        build_gold_layer=not args.dry_run,
    )
    print(json.dumps(run.to_row(), default=str, indent=2))
    return 0 if run.status.value in ("succeeded", "partial") else 1


if __name__ == "__main__":
    raise SystemExit(main())
