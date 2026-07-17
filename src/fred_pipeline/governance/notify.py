"""Run notifications / alerts (handoff Workflow step 10: "Notify").

Posts a run summary to a Slack-compatible webhook. The transport is injectable
so tests never touch the network, and formatting is a pure function. Failures to
notify never fail the pipeline run.
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Optional

from fred_pipeline.audit import EtlRun, RunStatus

log = logging.getLogger("fred_pipeline.notify")

Transport = Callable[[str, dict[str, Any]], None]

_STATUS_EMOJI = {
    RunStatus.SUCCEEDED: "✅",
    RunStatus.PARTIAL: "⚠️",
    RunStatus.FAILED: "❌",
    RunStatus.RUNNING: "⏳",
}

MAX_FAILURES_SHOWN = 20


def should_notify(run: EtlRun, notify_on: str) -> bool:
    """Decide whether a run warrants a notification given the policy."""
    policy = (notify_on or "failure").lower()
    if policy == "never":
        return False
    if policy == "always":
        return True
    # default: only non-clean runs
    return run.status in (RunStatus.FAILED, RunStatus.PARTIAL)


def format_run_summary(run: EtlRun, environment: str = "") -> str:
    """Human-readable one-block summary of a run (used as the message body)."""
    emoji = _STATUS_EMOJI.get(run.status, "")
    env = f"[{environment}] " if environment else ""
    dur = f"{run.duration_seconds:.1f}s" if run.duration_seconds is not None else "n/a"
    lines = [
        f"{emoji} FRED pipeline {env}run {run.run_id[:8]}: "
        f"*{run.status.value.upper()}*",
        f"series: {run.series_succeeded}/{run.series_total} ok, "
        f"{run.series_failed} failed  |  duration: {dur}",
    ]
    failures = [s for s in run.series_runs if s.status == RunStatus.FAILED]
    if failures:
        lines.append("failures:")
        for s in failures[:MAX_FAILURES_SHOWN]:
            lines.append(f"  • {s.series_id}: {s.error_message or 'unknown error'}")
        if len(failures) > MAX_FAILURES_SHOWN:
            lines.append(f"  … and {len(failures) - MAX_FAILURES_SHOWN} more")
    return "\n".join(lines)


def slack_payload(run: EtlRun, environment: str = "") -> dict[str, Any]:
    return {"text": format_run_summary(run, environment)}


def _http_post(url: str, payload: dict[str, Any]) -> None:  # pragma: no cover - network
    import requests

    resp = requests.post(url, json=payload, timeout=10)
    if getattr(resp, "status_code", 200) >= 300:
        raise RuntimeError(f"webhook returned HTTP {resp.status_code}")


def send_notification(
    run: EtlRun,
    *,
    webhook_url: str = "",
    notify_on: str = "failure",
    environment: str = "",
    transport: Optional[Transport] = None,
) -> bool:
    """Send a run notification if the policy calls for it.

    Returns True iff a message was actually dispatched to a webhook. When the
    policy triggers but no webhook is configured, the summary is logged instead.
    """
    if not should_notify(run, notify_on):
        return False
    summary = format_run_summary(run, environment)
    if not webhook_url:
        log.info("notify (no webhook configured):\n%s", summary)
        return False
    post = transport or _http_post
    try:
        post(webhook_url, slack_payload(run, environment))
        return True
    except Exception:
        log.exception("Failed to send run notification")
        return False
