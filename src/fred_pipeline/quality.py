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

from fred_pipeline.manifest import ValidationProfile


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
) -> QualityReport:
    checks = LENIENT_CHECKS if profile == ValidationProfile.LENIENT else DEFAULT_CHECKS
    results = [check(series_id, rows) for check in checks]
    return QualityReport(series_id=series_id, profile=profile, results=results)
