from datetime import date

from fred_pipeline.manifest import ValidationProfile
from fred_pipeline.quality import (
    Severity,
    check_freshness,
    check_missing_ratio,
    check_no_duplicate_keys,
    check_no_future_dates,
    check_value_bounds,
    run_quality_checks,
)
from fred_pipeline.transform import assign_revision_numbers, normalize_observations


def _rows(payload):
    return assign_revision_numbers(normalize_observations("X", payload, run_id="r"))


def test_all_pass_standard(observations_payload):
    rows = _rows(observations_payload)
    report = run_quality_checks("X", rows, ValidationProfile.STANDARD)
    assert report.passed is True
    assert report.failures == []


def test_empty_fails_error_check():
    report = run_quality_checks("X", [], ValidationProfile.STANDARD)
    assert report.passed is False
    non_empty = [r for r in report.results if r.check == "non_empty"][0]
    assert non_empty.passed is False
    assert non_empty.severity == Severity.ERROR


def test_missing_ratio_is_warning_not_fatal_under_standard():
    payload = {"observations": [
        {"date": "2024-01-01", "value": ".", "realtime_start": "2024-01-02",
         "realtime_end": "9999-12-31"},
        {"date": "2024-01-02", "value": ".", "realtime_start": "2024-01-03",
         "realtime_end": "9999-12-31"},
    ]}
    rows = _rows(payload)
    result = check_missing_ratio("X", rows, threshold=0.5)
    assert result.passed is False
    assert result.severity == Severity.WARNING
    # Under STANDARD, a warning does not fail the series.
    report = run_quality_checks("X", rows, ValidationProfile.STANDARD)
    assert report.passed is True
    # Under STRICT, it does.
    strict = run_quality_checks("X", rows, ValidationProfile.STRICT)
    assert strict.passed is False


def test_lenient_never_fails():
    report = run_quality_checks("X", [], ValidationProfile.LENIENT)
    # non_empty would fail, but LENIENT is advisory-only.
    assert report.passed is True


def test_duplicate_keys_detected():
    rows = [
        {"series_id": "X", "observation_date": "2024-01-01", "realtime_start": "2024-01-02"},
        {"series_id": "X", "observation_date": "2024-01-01", "realtime_start": "2024-01-02"},
    ]
    result = check_no_duplicate_keys("X", rows)
    assert result.passed is False
    assert result.metric_value == 1.0


def test_value_bounds_flags_out_of_range():
    rows = [
        {"series_id": "X", "observation_date": "2024-01-01", "value": 3.0,
         "is_missing": False},
        {"series_id": "X", "observation_date": "2024-01-02", "value": 130.0,
         "is_missing": False},   # out of range
        {"series_id": "X", "observation_date": "2024-01-03", "value": None,
         "is_missing": True},    # missing ignored
    ]
    result = check_value_bounds("X", rows, min_value=0, max_value=100)
    assert result.passed is False
    assert result.severity == Severity.ERROR
    assert result.metric_value == 1.0


def test_value_bounds_pass_within_range():
    rows = [{"series_id": "X", "observation_date": "2024-01-01", "value": 4.2,
             "is_missing": False}]
    assert check_value_bounds("X", rows, min_value=-5, max_value=25).passed is True


def test_freshness_flags_stale_series():
    rows = [{"series_id": "X", "observation_date": "2024-01-01", "value": 1.0,
             "is_missing": False}]
    # monthly threshold 75d; asking as of mid-2024 -> stale
    result = check_freshness("X", rows, "m", today=date(2024, 6, 1))
    assert result.passed is False
    assert result.severity == Severity.WARNING


def test_freshness_ok_when_recent():
    rows = [{"series_id": "X", "observation_date": "2024-05-25", "value": 1.0,
             "is_missing": False}]
    assert check_freshness("X", rows, "m", today=date(2024, 6, 1)).passed is True


def test_run_quality_checks_includes_bounds_and_freshness():
    rows = [{"series_id": "X", "observation_date": "2024-05-25", "value": 999.0,
             "is_missing": False}]
    report = run_quality_checks(
        "X", rows, ValidationProfile.STANDARD,
        frequency="m", min_value=0, max_value=100, today=date(2024, 6, 1),
    )
    checks = {r.check for r in report.results}
    assert {"value_bounds", "freshness"} <= checks
    assert report.passed is False  # bounds error under STANDARD


def test_future_dates_flagged():
    rows = [
        {"series_id": "X", "observation_date": "2999-01-01", "realtime_start": "",
         "is_missing": False, "value": 1.0},
    ]
    result = check_no_future_dates("X", rows, today=date(2024, 1, 1))
    assert result.passed is False
    assert result.severity == Severity.WARNING
