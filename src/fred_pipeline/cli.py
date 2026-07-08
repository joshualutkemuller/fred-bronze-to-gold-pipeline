"""Command-line interface for local runs, validation, and dry runs.

Examples
--------
Validate all manifests (no network, no Spark)::

    python -m fred_pipeline validate --manifests manifests

Dry-run the pipeline against the real FRED API but without Spark writes,
printing the audit summary as JSON::

    FRED_API_KEY=... python -m fred_pipeline run --env dev --dry-run

Run fully locally, persisting to a SQLite file::

    FRED_API_KEY=... python -m fred_pipeline run --local --db-path fred_local.db

Generate a new manifest from a FRED category / release / search::

    FRED_API_KEY=... python -m fred_pipeline discover --name rates_extra \\
        --category-id 22 --frequencies d --out manifests/rates_extra.yml

Reconcile manifests against live FRED metadata (drift + lifecycle)::

    FRED_API_KEY=... python -m fred_pipeline reconcile --local --fail-on-drift
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


def _cmd_discover(args: argparse.Namespace) -> int:
    import os

    from fred_pipeline.discovery import (
        build_manifest_dict,
        discover_specs,
        manifest_to_yaml,
    )
    from fred_pipeline.fred_client import FredClient

    sources = [bool(args.category_id), bool(args.release_id), bool(args.search)]
    if sum(sources) != 1:
        print("ERROR: pass exactly one of --category-id / --release-id / --search",
              file=sys.stderr)
        return 2

    config = PipelineConfig.resolve(
        environment=Environment(args.env), config_file=args.config
    )
    if not config.fred_api_key:
        print("ERROR: no FRED API key found (config file / FRED_API_KEY / secret).",
              file=sys.stderr)
        return 2

    client = FredClient(
        api_key=config.fred_api_key,
        base_url=config.fred_base_url,
        timeout=config.request_timeout_seconds,
        max_retries=config.max_retries,
        rate_limit_per_minute=config.rate_limit_per_minute,
    )

    if args.category_id:
        metas = client.get_category_series(args.category_id, max_results=args.max)
        default_desc = f"Series discovered from FRED category {args.category_id}."
    elif args.release_id:
        metas = client.get_release_series(args.release_id, max_results=args.max)
        default_desc = f"Series discovered from FRED release {args.release_id}."
    else:
        metas = client.search_series(args.search, max_results=args.max)
        default_desc = f"Series discovered from FRED search {args.search!r}."

    exclude_ids: set[str] = set()
    if not args.include_existing and os.path.isdir(args.manifests):
        try:
            existing = load_manifests(args.manifests)
            exclude_ids = {s.series_id for s in all_series(existing, active_only=False)}
        except Exception:
            exclude_ids = set()

    frequencies = (
        [f.strip() for f in args.frequencies.split(",")] if args.frequencies else None
    )
    specs, skipped = discover_specs(
        metas,
        category=args.name,
        frequencies=frequencies,
        exclude_discontinued=not args.include_discontinued,
        min_popularity=args.min_popularity,
        exclude_ids=exclude_ids,
    )

    manifest = build_manifest_dict(
        args.name, specs, description=args.description or default_desc
    )
    yaml_text = manifest_to_yaml(manifest)

    print(
        f"Discovered {len(metas)} series; kept {len(specs)}, skipped {len(skipped)} "
        f"(dupes/discontinued/filtered/existing)."
    )
    if args.dry_run or not args.out:
        print("\n--- manifest (dry run, not written) ---\n")
        print(yaml_text)
    else:
        with open(args.out, "w", encoding="utf-8") as fh:
            fh.write(yaml_text)
        print(f"Wrote {len(specs)} series to {args.out}")
        # Prove the written file loads cleanly alongside the rest.
        load_manifests(args.out)
        print("Validated: generated manifest loads successfully.")
    return 0


def _cmd_reconcile(args: argparse.Namespace) -> int:
    from fred_pipeline.fred_client import FredClient
    from fred_pipeline.reconcile import persist_report, reconcile

    config = PipelineConfig.resolve(
        environment=Environment(args.env), config_file=args.config
    )
    if not config.fred_api_key:
        print("ERROR: no FRED API key found (config file / FRED_API_KEY / secret).",
              file=sys.stderr)
        return 2

    client = FredClient(
        api_key=config.fred_api_key,
        base_url=config.fred_base_url,
        timeout=config.request_timeout_seconds,
        max_retries=config.max_retries,
        rate_limit_per_minute=config.rate_limit_per_minute,
    )
    manifests = load_manifests(args.manifests)
    report = reconcile(manifests, client)

    print("Reconciliation summary:", json.dumps(report.summary()))
    for sev in ("error", "warning", "info"):
        for d in report.by_severity(sev):
            print(f"  [{sev:7s}] {d.series_id:12s} {d.kind}: "
                  f"{d.manifest_value!r} (manifest) vs {d.fred_value!r} (FRED)")
    if report.stale:
        print(f"  [stale] {len(report.stale)} series past expected update: "
              f"{', '.join(report.stale)}")

    warehouse = None
    if args.local:
        from fred_pipeline.local_store import LocalWarehouse

        warehouse = LocalWarehouse(config, db_path=args.db_path)
    elif not args.no_persist:
        from fred_pipeline.spark_io import get_spark
        from fred_pipeline.warehouse import SparkWarehouse

        try:
            warehouse = SparkWarehouse(config, get_spark())
        except Exception:
            print("(no Spark available; skipping persistence)", file=sys.stderr)

    if warehouse is not None:
        try:
            counts = persist_report(config, report, warehouse)
            print(f"Persisted {counts['lifecycle_rows']} lifecycle + "
                  f"{counts['drift_rows']} drift rows.")
        finally:
            warehouse.close()

    if args.fail_on_drift and report.has_errors:
        print("FAIL: error-level drift detected.", file=sys.stderr)
        return 1
    return 0


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
    config = PipelineConfig.resolve(
        environment=Environment(args.env), config_file=args.config
    )
    if not config.fred_api_key:
        print(
            "ERROR: no FRED API key found. Set FRED_API_KEY or configure a "
            "Databricks secret scope.",
            file=sys.stderr,
        )
        return 2

    spark = None
    warehouse = None
    persist = not args.dry_run

    if args.dry_run:
        pass  # in-memory only: extract + DQ, no writes
    elif args.local:
        from fred_pipeline.local_store import LocalWarehouse

        warehouse = LocalWarehouse(config, db_path=args.db_path)
        print(f"Using local SQLite backend: {args.db_path}")
    else:
        from fred_pipeline.spark_io import get_spark

        spark = get_spark()

    pipeline = FredPipeline(
        config, spark=spark, warehouse=warehouse, persist_audit=persist
    )
    try:
        run = pipeline.run_from_manifest(
            args.manifests,
            triggered_by="cli-local" if args.local else "cli",
            build_gold_layer=not args.dry_run,
        )
    finally:
        if warehouse is not None:
            warehouse.close()

    print(json.dumps(run.to_row(), default=str, indent=2))
    if args.local and not args.dry_run:
        print(
            f"\nSaved to {args.db_path}. Inspect with e.g.:\n"
            f"  sqlite3 {args.db_path} "
            f"'SELECT series_id, observation_date, value "
            f"FROM gold_fred_latest_observation LIMIT 10;'"
        )
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
        "--config",
        default=None,
        help="path to a YAML config file "
        "(default: $FRED_CONFIG_FILE or config/config.yaml)",
    )
    r.add_argument(
        "--dry-run",
        action="store_true",
        help="extract + validate in memory, no writes (no Spark, no local db)",
    )
    r.add_argument(
        "--local",
        action="store_true",
        help="persist to a local SQLite file instead of Databricks/Delta",
    )
    r.add_argument(
        "--db-path",
        default="fred_local.db",
        help="SQLite file path for --local runs (default: fred_local.db)",
    )
    r.set_defaults(func=_cmd_run)

    d = sub.add_parser(
        "discover",
        help="generate a manifest from a FRED category / release / search",
    )
    d.add_argument("--name", required=True,
                   help="manifest name + category label for the generated series")
    src = d.add_mutually_exclusive_group(required=True)
    src.add_argument("--category-id", type=int, help="FRED category id")
    src.add_argument("--release-id", type=int, help="FRED release id")
    src.add_argument("--search", help="full-text search string")
    d.add_argument("--out", default=None,
                   help="output YAML path (omit or use --dry-run to print instead)")
    d.add_argument("--env", default="dev", choices=[e.value for e in Environment])
    d.add_argument("--config", default=None, help="YAML config path for the API key")
    d.add_argument("--manifests", default="manifests",
                   help="existing manifests dir to dedupe against (default: manifests)")
    d.add_argument("--frequencies", default=None,
                   help="comma-separated frequency filter, e.g. 'd,m,q'")
    d.add_argument("--max", type=int, default=100,
                   help="max series to keep (default: 100)")
    d.add_argument("--min-popularity", type=float, default=0.0,
                   help="drop series below this FRED popularity (0-100)")
    d.add_argument("--include-discontinued", action="store_true",
                   help="keep series whose title is marked DISCONTINUED")
    d.add_argument("--include-existing", action="store_true",
                   help="do not dedupe against series already in --manifests")
    d.add_argument("--description", default=None, help="manifest description")
    d.add_argument("--dry-run", action="store_true", help="print instead of writing")
    d.set_defaults(func=_cmd_discover)

    rc = sub.add_parser(
        "reconcile",
        help="diff manifests against live FRED metadata (drift + lifecycle)",
    )
    rc.add_argument("--manifests", default="manifests")
    rc.add_argument("--env", default="dev", choices=[e.value for e in Environment])
    rc.add_argument("--config", default=None, help="YAML config path for the API key")
    rc.add_argument("--local", action="store_true",
                    help="persist lifecycle/drift to a local SQLite file")
    rc.add_argument("--db-path", default="fred_local.db")
    rc.add_argument("--no-persist", action="store_true",
                    help="report only; do not write to any backend")
    rc.add_argument("--fail-on-drift", action="store_true",
                    help="exit non-zero if any error-level drift is found (for CI)")
    rc.set_defaults(func=_cmd_reconcile)
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
