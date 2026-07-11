"""Tests for the two governance Gold objects:
gold.fred_source_reconciliation (config-driven table) and
gold.v_source_coverage (multi-source freshness view)."""

import textwrap

import pytest

from fred_pipeline.config import Environment, PipelineConfig
from fred_pipeline.features import compute_source_reconciliation
from fred_pipeline.local_store import LocalWarehouse
from fred_pipeline.manifest import Manifest, SeriesSpec
from fred_pipeline.reconciliation_config import (
    ReconciliationConfigError,
    ReconciliationDef,
    load_reconciliation_defs,
)


def _row(series_id, date, value, source="fred", missing=False):
    return {"source": source, "series_id": series_id, "observation_date": date,
            "value": value, "is_missing": missing}


def _config():
    return PipelineConfig(environment=Environment.DEV, fred_api_key="k")


def _silver(source, series_id, date, value):
    return {"source": source, "series_id": series_id, "observation_date": date,
            "realtime_start": "", "realtime_end": "", "value": value,
            "raw_value": str(value), "is_missing": False, "row_hash": f"{series_id}{date}",
            "revision_number": 1, "ingested_at": "t", "run_id": "r"}


# ---- reconciliation config -----------------------------------------------

def test_reconciliation_loader_and_validation(tmp_path):
    p = tmp_path / "rec.yml"
    p.write_text(textwrap.dedent("""
        reconciliations:
          - name: cpi
            frequency: m
            series_a: A
            series_b: B
            tolerance_pct: 0.5
    """))
    defs = load_reconciliation_defs(str(p))
    assert defs[0].series_a == "A" and defs[0].tolerance_pct == 0.5

    assert load_reconciliation_defs("nope.yml") == []
    with pytest.raises(ReconciliationConfigError):
        ReconciliationDef(name="x", frequency="zzz", series_a="A", series_b="B")
    with pytest.raises(ReconciliationConfigError):
        ReconciliationDef(name="x", frequency="m", series_a="A", series_b="B",
                          tolerance_pct=-1)


# ---- reconciliation engine -----------------------------------------------

def test_reconciliation_flags_divergence():
    rows = [
        _row("A", "2024-01-31", 4.0, source="fred"),
        _row("B", "2024-01-31", 4.02, source="bls"),   # ~0.5% apart
        _row("A", "2024-02-29", 4.0, source="fred"),
        _row("B", "2024-02-29", 5.0, source="bls"),     # 25% apart -> diverged
    ]
    defs = [ReconciliationDef(name="ab", frequency="m", series_a="A", series_b="B",
                              tolerance_pct=1.0)]
    out = compute_source_reconciliation(rows, defs)
    by_date = {r["observation_date"]: r for r in out}
    assert by_date["2024-01-01"]["diverged"] is False   # 0.5% < 1% tolerance
    assert by_date["2024-02-01"]["diverged"] is True
    assert by_date["2024-02-01"]["value_a"] == 4.0
    assert by_date["2024-02-01"]["value_b"] == 5.0


def test_reconciliation_absent_series_is_skipped():
    rows = [_row("A", "2024-01-31", 4.0)]  # B missing
    defs = [ReconciliationDef(name="ab", frequency="m", series_a="A", series_b="B")]
    assert compute_source_reconciliation(rows, defs) == []


def test_reconciliation_zero_denominator_pct_none():
    rows = [_row("A", "2024-01-31", 4.0), _row("B", "2024-01-31", 0.0)]
    defs = [ReconciliationDef(name="ab", frequency="m", series_a="A", series_b="B")]
    out = compute_source_reconciliation(rows, defs)
    assert out[0]["pct_diff"] is None and out[0]["diverged"] is False


# ---- local backend: reconciliation table + coverage view -----------------

def test_local_build_populates_reconciliation(tmp_path, monkeypatch):
    p = tmp_path / "rec.yml"
    p.write_text(textwrap.dedent("""
        reconciliations:
          - name: ab
            frequency: m
            series_a: A
            series_b: B
            tolerance_pct: 1.0
    """))
    monkeypatch.setenv("FRED_RECONCILIATIONS_FILE", str(p))

    wh = LocalWarehouse(_config(), db_path=str(tmp_path / "f.db"))
    wh.merge_silver([_silver("fred", "A", "2024-01-31", 4.0),
                     _silver("bls", "B", "2024-01-31", 5.0)])
    wh.build_gold()
    got = wh.query("SELECT name, value_a, value_b, diverged "
                   "FROM gold_fred_source_reconciliation")
    assert got == [{"name": "ab", "value_a": 4.0, "value_b": 5.0, "diverged": 1}]
    wh.close()


def test_source_coverage_view(tmp_path):
    cfg = _config()
    man = Manifest.from_dict({"name": "t", "series": [
        SeriesSpec(series_id="DGS10", title="10Y", frequency="d",
                   category="rates").to_dict(),
    ]})
    wh = LocalWarehouse(cfg, db_path=str(tmp_path / "f.db"))
    wh.sync_meta([man])
    # an old daily observation -> stale; plus a second date for the count
    wh.merge_silver([_silver("fred", "DGS10", "2020-01-01", 1.9),
                     _silver("fred", "DGS10", "2020-01-02", 2.0)])

    cov = wh.query("SELECT * FROM gold_v_source_coverage")
    assert len(cov) == 1
    r = cov[0]
    assert r["source"] == "fred" and r["series_id"] == "DGS10"
    assert r["category"] == "rates" and r["frequency"] == "d"
    assert r["latest_observation_date"] == "2020-01-02"
    assert r["observation_count"] == 2
    assert r["is_stale"] == 1          # a daily series last seen in 2020 is stale
    wh.close()
