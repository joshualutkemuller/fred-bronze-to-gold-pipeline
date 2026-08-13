#!/usr/bin/env python3
"""Refresh the FOMC meeting dates in ``config/fomc.yml`` from the Fed's calendar.

Spec: ``docs/handoffs/fomc_calendar_scraper.md``.

Run this when ``test_fomc_meeting_dates_have_runway`` starts failing — it fires
120 days before ``config/fomc.yml`` expires. An expired list makes
``gold.fomc_probability`` / ``gold.fomc_meeting_path`` emit nothing and the
Power BI Fed Policy Watch report render blank, with no error anywhere.

    # what the Fed currently lists
    python scripts/scrape_fomc_calendar.py

    # only what the config is missing, paste-ready
    python scripts/scrape_fomc_calendar.py --missing-only

    # CI / cron: non-zero exit when config and page disagree
    python scripts/scrape_fomc_calendar.py --check

    # no browser, no network -- parse a saved page
    python scripts/scrape_fomc_calendar.py --html-file tests/fixtures/fomccalendars.html

Exit codes: 0 = success / in sync, 1 = drift found in --check, 2 = scrape or
parse failure.

This tool never edits ``config/fomc.yml``. That file carries hand-written
provenance comments, and a config driving a rate-path model deserves a human
diff -- so this prints and a person pastes.
"""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

import yaml  # noqa: E402

from fred_pipeline.catalogs.fomc_calendar import (  # noqa: E402
    FOMC_CALENDAR_URL,
    FOMCScrapeError,
    diff_against_config,
    fetch_calendar_html,
    format_yaml_block,
    parse_fomc_calendar,
)

DEFAULT_CONFIG = REPO_ROOT / "config" / "fomc.yml"


def _configured_dates(path: Path) -> list[date]:
    if not path.is_file():
        return []
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    out = []
    for raw in data.get("meeting_dates") or []:
        out.append(raw if isinstance(raw, date) else date.fromisoformat(str(raw)))
    return sorted(out)


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Scrape scheduled FOMC meeting dates from federalreserve.gov.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--url", default=FOMC_CALENDAR_URL, help="calendar URL")
    p.add_argument(
        "--html-file",
        type=Path,
        help="parse this saved HTML instead of fetching (no browser, no network)",
    )
    p.add_argument(
        "--save-html",
        type=Path,
        help="write the fetched page here (use it to refresh the test fixture)",
    )
    p.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG,
        help=f"config to compare against (default: {DEFAULT_CONFIG})",
    )
    p.add_argument(
        "--check",
        action="store_true",
        help="exit non-zero if the config and the page disagree (CI/cron mode)",
    )
    p.add_argument(
        "--missing-only",
        action="store_true",
        help="print only the meetings the config lacks",
    )
    p.add_argument(
        "--from-year",
        type=int,
        help="ignore meetings before this calendar year",
    )
    p.add_argument(
        "--no-headless",
        action="store_true",
        help="show the browser (debugging a parse failure)",
    )
    p.add_argument("--chromedriver", help="path to chromedriver")
    p.add_argument("--chrome-binary", help="path to the Chrome/Chromium binary")
    p.add_argument(
        "--timeout", type=int, default=30, help="page-load timeout in seconds"
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)

    try:
        if args.html_file:
            html = args.html_file.read_text(encoding="utf-8")
            print(f"parsing {args.html_file} (no browser)", file=sys.stderr)
        else:
            print(f"fetching {args.url} with Selenium…", file=sys.stderr)
            html = fetch_calendar_html(
                args.url,
                headless=not args.no_headless,
                chromedriver_path=args.chromedriver,
                chrome_binary=args.chrome_binary,
                timeout_seconds=args.timeout,
            )
            if args.save_html:
                args.save_html.write_text(html, encoding="utf-8")
                print(f"saved page to {args.save_html}", file=sys.stderr)

        meetings = parse_fomc_calendar(html)
    except FileNotFoundError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    except FOMCScrapeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    if args.from_year:
        meetings = [m for m in meetings if m.decision_date.year >= args.from_year]

    print(
        f"parsed {len(meetings)} scheduled meetings "
        f"({meetings[0].decision_date} … {meetings[-1].decision_date})",
        file=sys.stderr,
    )

    configured = _configured_dates(args.config)
    diff = diff_against_config(meetings, configured)

    if args.check:
        if diff.in_sync:
            print(f"in sync with {args.config} (future meetings only)")
            return 0
        if diff.missing_from_config:
            print("Meetings on the Fed calendar but NOT in the config:")
            print(
                format_yaml_block(
                    m for m in meetings if m.decision_date in diff.missing_from_config
                )
            )
        if diff.absent_upstream:
            print(
                "\nDates in the config the Fed no longer lists "
                "(a meeting may have moved — verify before removing):"
            )
            for d in diff.absent_upstream:
                print(f"  - {d.isoformat()}")
        return 1

    selected = meetings
    if args.missing_only:
        selected = [m for m in meetings if m.decision_date in diff.missing_from_config]
        if not selected:
            print("# config/fomc.yml already has every future meeting listed")
            return 0
        print("# add to meeting_dates in config/fomc.yml:")

    print(format_yaml_block(selected))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
