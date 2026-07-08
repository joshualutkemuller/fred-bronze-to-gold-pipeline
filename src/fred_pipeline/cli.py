"""Command-line interface for local runs, validation, and dry runs.

Examples
--------
Validate all manifests (no network, no Spark)::

    python -m fred_pipeline validate --manifests manifests

Dry-run the pipeline against the real FRED API but without Spark writes,
printing the audit summary as JSON::

    FRED_API_KEY=... python -m fred_pipeline run --env dev --dry-run
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import asdict
from typing import Optional

from fred_pipeline.config import Environment, PipelineConfig
from fred_pipeline.manifest import all_series, load_manifests
from fred_pipeline.pipeline import FredPipeline


def _cmd_validate(args: argparse.Namespace) -> int:
    manifests = load_manifests(args.manifests)
    specs = all_series(manifests, active_only=False)
    active = [s for s in specs if s.active]
    print(
        f"OK: {len(manifests)} manifest file(s), {len(specs)} series "
        f"({len(active)} active)."
    )
    for man in manifests:
        print(f"  - {man.name}: {len(man.series)} series ({man.source_path})")
    return 0


def _cmd_run(args: argparse.Namespace) -> int:
    config = PipelineConfig.resolve(environment=Environment(args.env))
    if not config.fred_api_key and not args.validate_only:
        print(
            "ERROR: no FRED API key found. Set FRED_API_KEY or configure a "
            "Databricks secret scope.",
            file=sys.stderr,
        )
        return 2

    spark = None
    if not args.dry_run:
        from fred_pipeline.spark_io import get_spark

        spark = get_spark()

    pipeline = FredPipeline(config, spark=spark, persist_audit=not args.dry_run)
    run = pipeline.run_from_manifest(
        args.manifests,
        triggered_by="cli",
        build_gold_layer=not args.dry_run,
    )
    print(json.dumps(run.to_row(), default=str, indent=2))
    return 0 if run.status.value in ("succeeded", "partial") else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="fred_pipeline", description=__doc__)
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="enable debug logging"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    v = sub.add_parser("validate", help="validate manifests only")
    v.add_argument("--manifests", default="manifests")
    v.set_defaults(func=_cmd_validate)

    r = sub.add_parser("run", help="run the pipeline")
    r.add_argument("--env", default="dev", choices=[e.value for e in Environment])
    r.add_argument("--manifests", default="manifests")
    r.add_argument(
        "--dry-run",
        action="store_true",
        help="extract + validate without Spark writes",
    )
    r.add_argument("--validate-only", action="store_true", help=argparse.SUPPRESS)
    r.set_defaults(func=_cmd_run)
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    return args.func(args)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
