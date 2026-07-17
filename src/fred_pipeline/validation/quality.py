"""Data-quality rules for silver observation rows.

Rules are pure functions over the normalized row dicts produced by
:mod:`fred_pipeline.transform`, so they run in unit tests and (identically) on
the driver before a Delta write. Each rule returns a :class:`DQResult`; the
:func:`run_quality_checks` orchestrator applies a series' validation profile to
decide whether failures are fatal.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from typing import Any, Callable, Optional

from fred_pipeline.manifest import FREQUENCY_MAX_AGE_DAYS, ValidationProfile


class Severity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


@dataclass
class DQResult:
    check: str
    passed: bool
    severity: Severity
    series_id: str
    message: str = ""
    metric_value: Optional[float] = None
    details: dict[str, Any] = field(default_factory=dict)


Rows = list[dict[str, Any]]


# ---- individual checks --------------------------------------------------

def check_non_empty(series_id: str, rows: Rows) -> DQResult:
    n = len(rows)
    return DQResult(
        check="non_empty",
        passed=n > 0,
        severity=Severity.ERROR,
        series_id=series_id,
        message=f"{n} observation(s) returned",
        metric_value=float(n),
    )


def check_no_duplicate_keys(series_id: str, rows: Rows) -> DQResult:
    seen: set[tuple[str, str, str]] = set()
    dupes = 0
    for r in rows:
        key = (r["series_id"], r["observation_date"], r.get("realtime_start", ""))
        if key in seen:
            dupes += 1
        seen.add(key)
    return DQResult(
        check="no_duplicate_keys",
        passed=dupes == 0,
        severity=Severity.ERROR,
        series_id=series_id,
        message=f"{dupes} duplicate (date, realtime_start) key(s)",
        metric_value=float(dupes),
    )


def check_missing_ratio(series_id: str, rows: Rows, threshold: float = 0.5) -> DQResult:
    if not rows:
        ratio = 0.0
    else:
        ratio = sum(1 for r in rows if r.get("is_missing")) / len(rows)
    return DQResult(
        check="missing_ratio",
        passed=ratio <= threshold,
        severity=Severity.WARNING,
        series_id=series_id,
        message=f"{ratio:.1%} of observations missing (threshold {threshold:.0%})",
        metric_value=ratio,
        details={"threshold": threshold},
    )


def check_dates_parseable(series_id: str, rows: Rows) -> DQResult:
    bad = 0
    for r in rows:
        try:
            datetime.strptime(r["observation_date"], "%Y-%m-%d")
        except (ValueError, TypeError):
            bad += 1
    return DQResult(
        check="dates_parseable",
        passed=bad == 0,
        severity=Severity.ERROR,
        series_id=series_id,
        message=f"{bad} unparseable observation_date value(s)",
        metric_value=float(bad),
    )


def check_values_numeric(series_id: str, rows: Rows) -> DQResult:
    """Non-missing values should be genuine floats (transform guarantees this)."""
    bad = sum(
        1
        for r in rows
        if not r.get("is_missing") and not isinstance(r.get("value"), (int, float))
    )
    return DQResult(
        check="values_numeric",
        passed=bad == 0,
        severity=Severity.ERROR,
        series_id=series_id,
        message=f"{bad} non-missing value(s) failed numeric parsing",
        metric_value=float(bad),
    )


def check_no_future_dates(series_id: str, rows: Rows, today: Optional[date] = None) -> DQResult:
    today = today or date.today()
    future = 0
    for r in rows:
        try:
            d = datetime.strptime(r["observation_date"], "%Y-%m-%d").date()
            if d > today:
                future += 1
        except (ValueError, TypeError):
            continue
    return DQResult(
        check="no_future_dates",
        passed=future == 0,
        severity=Severity.WARNING,
        series_id=series_id,
        message=f"{future} observation(s) dated in the future",
        metric_value=float(future),
    )


def check_value_bounds(
    series_id: str,
    rows: Rows,
    min_value: Optional[float] = None,
    max_value: Optional[float] = None,
) -> DQResult:
    """Non-missing values must fall within the (inclusive) manifest bounds."""
    out = 0
    for r in rows:
        if r.get("is_missing"):
            continue
        v = r.get("value")
        if not isinstance(v, (int, float)):
            continue
        if (min_value is not None and v < min_value) or (
            max_value is not None and v > max_value
        ):
            out += 1
    return DQResult(
        check="value_bounds",
        passed=out == 0,
        severity=Severity.ERROR,
        series_id=series_id,
        message=f"{out} value(s) outside [{min_value}, {max_value}]",
        metric_value=float(out),
        details={"min_value": min_value, "max_value": max_value},
    )


def check_freshness(
    series_id: str,
    rows: Rows,
    frequency: str,
    today: Optional[date] = None,
) -> DQResult:
    """Warn when the latest observation is older than the frequency allows."""
    today = today or date.today()
    threshold = FREQUENCY_MAX_AGE_DAYS.get((frequency or "").lower())
    latest: Optional[date] = None
    for r in rows:
        try:
            d = datetime.strptime(r["observation_date"], "%Y-%m-%d").date()
        except (ValueError, TypeError, KeyError):
            continue
        if latest is None or d > latest:
            latest = d
    if latest is None or threshold is None:
        return DQResult(
            check="freshness", passed=True, severity=Severity.WARNING,
            series_id=series_id, message="freshness not evaluated",
        )
    age = (today - latest).days
    return DQResult(
        check="freshness",
        passed=age <= threshold,
        severity=Severity.WARNING,
        series_id=series_id,
        message=f"latest observation is {age}d old (max {threshold}d for {frequency})",
        metric_value=float(age),
        details={"threshold_days": threshold, "latest": latest.isoformat()},
    )


# Default rule set. (series_id, rows) -> DQResult
DEFAULT_CHECKS: tuple[Callable[[str, Rows], DQResult], ...] = (
    check_non_empty,
    check_no_duplicate_keys,
    check_dates_parseable,
    check_values_numeric,
    check_missing_ratio,
    check_no_future_dates,
)

# Under the LENIENT profile we only run the cheapest structural checks.
LENIENT_CHECKS = (check_non_empty, check_dates_parseable)


@dataclass
class QualityReport:
    series_id: str
    profile: ValidationProfile
    results: list[DQResult]

    @property
    def passed(self) -> bool:
        """Whether the series should be considered a *successful* load.

        STRICT: any failing check fails the series.
        STANDARD: only ERROR-severity failures fail the series.
        LENIENT: never fails on quality (results are advisory only).
        """
        if self.profile == ValidationProfile.LENIENT:
            return True
        if self.profile == ValidationProfile.STRICT:
            return all(r.passed for r in self.results)
        # STANDARD
        return all(r.passed for r in self.results if r.severity == Severity.ERROR)

    @property
    def failures(self) -> list[DQResult]:
        return [r for r in self.results if not r.passed]


def run_quality_checks(
    series_id: str,
    rows: Rows,
    profile: ValidationProfile = ValidationProfile.STANDARD,
    *,
    frequency: Optional[str] = None,
    min_value: Optional[float] = None,
    max_value: Optional[float] = None,
    today: Optional[date] = None,
) -> QualityReport:
    lenient = profile == ValidationProfile.LENIENT
    checks = LENIENT_CHECKS if lenient else DEFAULT_CHECKS
    results = [check(series_id, rows) for check in checks]

    # Parametrized checks run only when the relevant metadata is available.
    if not lenient:
        if min_value is not None or max_value is not None:
            results.append(check_value_bounds(series_id, rows, min_value, max_value))
        if frequency:
            results.append(check_freshness(series_id, rows, frequency, today))

    return QualityReport(series_id=series_id, profile=profile, results=results)
