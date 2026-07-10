"""Tests for frequency-aware, N-leg cross-series Gold features."""

import textwrap

import pytest

from fred_pipeline.config import Environment, PipelineConfig
from fred_pipeline.cross_series_config import (
    CrossSeriesConfigError,
    CrossSeriesDef,
    load_cross_series_defs,
)
from fred_pipeline.features import compute_cross_series_features
from fred_pipeline.local_store import LocalWarehouse


def _row(series_id, date, value, missing=False):
    return {"series_id": series_id, "observation_date": date, "value": value,
            "is_missing": missing}


def _def(name, op, freq, legs):
    return CrossSeriesDef(name=name, op=op, frequency=freq, legs=tuple(legs))


# ---- config loader -------------------------------------------------------

def _write(tmp_path, text):
    p = tmp_path / "cross.yml"
    p.write_text(textwrap.dedent(text))
    return str(p)


def test_loader_parses_ops_and_weights(tmp_path):
    path = _write(tmp_path, """
        features:
          - name: ry
            op: spread
            frequency: d
            legs: [DGS10, T10YIE]
          - name: dgdp
            op: ratio
            frequency: q
            legs:
              - series_id: DEBT
              - series_id: GDP
          - name: idx
            op: composite
            frequency: m
            legs:
              - {series_id: A, weight: 0.5}
              - {series_id: B, weight: 0.25}
              - C
    """)
    defs = {d.name: d for d in load_cross_series_defs(path)}
    assert defs["ry"].legs == (("DGS10", 1.0), ("T10YIE", 1.0))
    assert defs["dgdp"].op == "ratio" and defs["dgdp"].frequency == "q"
    assert defs["idx"].legs == (("A", 0.5), ("B", 0.25), ("C", 1.0))


def test_loader_missing_file_is_empty():
    assert load_cross_series_defs("does-not-exist.yml") == []


@pytest.mark.parametrize("legs, op", [(["A", "B", "C"], "spread"),
                                      (["A", "B", "C"], "ratio")])
def test_spread_ratio_require_two_legs(legs, op):
    with pytest.raises(CrossSeriesConfigError):
        _def("x", op, "d", [(s, 1.0) for s in legs])


def test_invalid_op_and_frequency():
    with pytest.raises(CrossSeriesConfigError):
        _def("x", "bogus", "d", [("A", 1.0), ("B", 1.0)])
    with pytest.raises(CrossSeriesConfigError):
        _def("x", "spread", "zzz", [("A", 1.0), ("B", 1.0)])


def test_loader_rejects_duplicate_and_unknown(tmp_path):
    dup = _write(tmp_path, """
        features:
          - {name: x, op: spread, frequency: d, legs: [A, B]}
          - {name: x, op: spread, frequency: d, legs: [A, B]}
    """)
    with pytest.raises(CrossSeriesConfigError):
        load_cross_series_defs(dup)


# ---- engine --------------------------------------------------------------

def test_same_frequency_spread():
    rows = [_row("A", "2024-01-01", 10), _row("A", "2024-01-02", 12),
            _row("B", "2024-01-01", 3), _row("B", "2024-01-02", 4)]
    out = compute_cross_series_features(rows, [_def("s", "spread", "d",
                                                    [("A", 1.0), ("B", 1.0)])])
    assert [(r["observation_date"], r["value"]) for r in out] == [
        ("2024-01-01", 7.0), ("2024-01-02", 8.0)]
    assert all(r["op"] == "spread" and r["feature_name"] == "s" for r in out)


def test_ratio_zero_guard():
    rows = [_row("A", "2024-01-01", 10), _row("A", "2024-01-02", 12),
            _row("B", "2024-01-01", 0), _row("B", "2024-01-02", 4)]
    out = compute_cross_series_features(rows, [_def("r", "ratio", "d",
                                                    [("A", 1.0), ("B", 1.0)])])
    # 2024-01-01 dropped (zero denominator); 2024-01-02 -> 3.0
    assert [(r["observation_date"], r["value"]) for r in out] == [("2024-01-02", 3.0)]


def test_composite_weighted_sum():
    rows = [_row("A", "2024-03-31", 100), _row("B", "2024-03-31", 200)]
    out = compute_cross_series_features(rows, [_def("c", "composite", "m",
                                                    [("A", 0.5), ("B", 0.25)])])
    assert out == [{"feature_name": "c", "op": "composite",
                    "observation_date": "2024-03-01", "value": 100.0}]  # 0.5*100+0.25*200


def test_cross_frequency_asof_downsample():
    # daily A downsampled to quarterly = last obs in each quarter
    rows = [
        _row("A", "2024-01-15", 100), _row("A", "2024-02-20", 110),
        _row("A", "2024-03-31", 120),                      # Q1 last -> 120
        _row("A", "2024-04-10", 130),                      # Q2 last -> 130
        _row("B", "2024-01-01", 1000), _row("B", "2024-04-01", 1100),  # quarterly
    ]
    out = compute_cross_series_features(rows, [_def("dg", "ratio", "q",
                                                    [("A", 1.0), ("B", 1.0)])])
    by_date = {r["observation_date"]: r["value"] for r in out}
    assert by_date["2024-01-01"] == pytest.approx(120 / 1000)   # Q1: last-in-quarter
    assert by_date["2024-04-01"] == pytest.approx(130 / 1100)   # Q2


def test_missing_leg_yields_no_rows():
    rows = [_row("A", "2024-01-01", 10)]  # B absent
    out = compute_cross_series_features(rows, [_def("s", "spread", "d",
                                                    [("A", 1.0), ("B", 1.0)])])
    assert out == []


def test_missing_values_are_skipped():
    rows = [_row("A", "2024-01-01", None, missing=True), _row("A", "2024-01-02", 12),
            _row("B", "2024-01-02", 4)]
    out = compute_cross_series_features(rows, [_def("s", "spread", "d",
                                                    [("A", 1.0), ("B", 1.0)])])
    assert [(r["observation_date"], r["value"]) for r in out] == [("2024-01-02", 8.0)]


# ---- polars parity (skipped if polars absent) ----------------------------

def test_polars_parity():
    pytest.importorskip("polars")
    from fred_pipeline.gold_polars import compute_cross_series_features_pl

    rows = [_row("A", "2024-01-01", 10), _row("A", "2024-01-02", 12),
            _row("B", "2024-01-01", 3), _row("B", "2024-01-02", 4)]
    defs = [_def("s", "spread", "d", [("A", 1.0), ("B", 1.0)])]
    assert compute_cross_series_features_pl(rows, defs) == \
        compute_cross_series_features(rows, defs)


# ---- local backend end-to-end -------------------------------------------

def test_local_build_gold_populates_cross_series(tmp_path, monkeypatch):
    cfg_path = _write(tmp_path, """
        features:
          - name: a_minus_b
            op: spread
            frequency: d
            legs: [A, B]
    """)
    monkeypatch.setenv("FRED_CROSS_SERIES_FILE", cfg_path)

    cfg = PipelineConfig(environment=Environment.DEV, fred_api_key="k")
    wh = LocalWarehouse(cfg, db_path=str(tmp_path / "f.db"))
    wh.merge_silver([
        {"source": "fred", "series_id": "A", "observation_date": "2024-01-01",
         "realtime_start": "", "realtime_end": "", "value": 10.0, "raw_value": "10",
         "is_missing": False, "row_hash": "h1", "revision_number": 1,
         "ingested_at": "t", "run_id": "r"},
        {"source": "fred", "series_id": "B", "observation_date": "2024-01-01",
         "realtime_start": "", "realtime_end": "", "value": 3.0, "raw_value": "3",
         "is_missing": False, "row_hash": "h2", "revision_number": 1,
         "ingested_at": "t", "run_id": "r"},
    ])
    wh.build_gold()
    got = wh.query("SELECT feature_name, op, observation_date, value "
                   "FROM gold_fred_cross_series_feature")
    assert got == [{"feature_name": "a_minus_b", "op": "spread",
                    "observation_date": "2024-01-01", "value": 7.0}]
    wh.close()
