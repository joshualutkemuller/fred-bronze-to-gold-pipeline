import pytest

from fred_pipeline.transform import (
    SILVER_COLUMNS,
    assign_revision_numbers,
    latest_by_observation,
    normalize_observations,
    parse_value,
    payload_summary,
)


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("4.25", 4.25),
        (".", None),
        ("", None),
        (None, None),
        ("1,234.5", 1234.5),
        ("not-a-number", None),
        (7, 7.0),
    ],
)
def test_parse_value(raw, expected):
    assert parse_value(raw) == expected


def test_normalize_observations_schema_and_missing(observations_payload):
    rows = normalize_observations("DGS10", observations_payload, run_id="r1")
    assert len(rows) == 4
    for r in rows:
        assert set(r.keys()) == set(SILVER_COLUMNS)
        assert r["series_id"] == "DGS10"
        assert r["run_id"] == "r1"
    missing = [r for r in rows if r["is_missing"]]
    assert len(missing) == 1
    assert missing[0]["observation_date"] == "2024-01-03"
    assert missing[0]["value"] is None
    assert missing[0]["raw_value"] == "."


def test_non_vintage_blanks_realtime_for_stable_key():
    # FRED stamps realtime_start=today when no realtime params are sent.
    day1 = {"observations": [
        {"date": "2024-01-01", "value": "4.25",
         "realtime_start": "2026-07-08", "realtime_end": "2026-07-08"},
    ]}
    day2 = {"observations": [
        {"date": "2024-01-01", "value": "4.25",
         "realtime_start": "2026-07-09", "realtime_end": "2026-07-09"},
    ]}
    r1 = normalize_observations("DGS10", day1, run_id="r1", track_vintage=False)
    r2 = normalize_observations("DGS10", day2, run_id="r2", track_vintage=False)
    # realtime is blanked, so the natural key is identical across runs
    assert r1[0]["realtime_start"] == "" and r1[0]["realtime_end"] == ""
    key = ("series_id", "observation_date", "realtime_start")
    assert tuple(r1[0][k] for k in key) == tuple(r2[0][k] for k in key)
    # unchanged value -> identical row_hash (no spurious "revision")
    assert r1[0]["row_hash"] == r2[0]["row_hash"]


def test_normalize_missing_observations_key_raises():
    with pytest.raises(ValueError):
        normalize_observations("X", {"count": 0})


def test_row_hash_changes_with_value(observations_payload):
    rows_a = normalize_observations("DGS10", observations_payload, run_id="r1")
    payload_b = {"observations": [dict(observations_payload["observations"][0], value="9.99")]}
    rows_b = normalize_observations("DGS10", payload_b, run_id="r1")
    assert rows_a[0]["row_hash"] != rows_b[0]["row_hash"]


def test_assign_revision_numbers(vintage_payload):
    rows = normalize_observations("PAYEMS", vintage_payload, run_id="r1")
    rows = assign_revision_numbers(rows)
    jan = sorted(
        [r for r in rows if r["observation_date"] == "2024-01-01"],
        key=lambda r: r["revision_number"],
    )
    assert [r["revision_number"] for r in jan] == [1, 2]
    assert jan[0]["value"] == 100.0
    assert jan[1]["value"] == 101.5
    feb = [r for r in rows if r["observation_date"] == "2024-02-01"]
    assert feb[0]["revision_number"] == 1


def test_latest_by_observation(vintage_payload):
    rows = normalize_observations("PAYEMS", vintage_payload, run_id="r1")
    latest = latest_by_observation(rows)
    jan = [r for r in latest if r["observation_date"] == "2024-01-01"]
    assert len(jan) == 1
    assert jan[0]["value"] == 101.5  # latest revision wins


def test_payload_summary(observations_payload):
    s = payload_summary(observations_payload)
    assert s["observation_count"] == 4
    assert s["payload_bytes"] > 0
