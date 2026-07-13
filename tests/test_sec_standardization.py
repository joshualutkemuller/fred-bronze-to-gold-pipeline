"""Tests for the SEC standardization layer (fundamentals + ratios + ranks)."""

import textwrap

import pytest

from fred_pipeline.config import Environment, PipelineConfig
from fred_pipeline.local_store import LocalWarehouse
from fred_pipeline.sec_standardization import (
    ConceptDef,
    RatioDef,
    SECStandardizationConfigError,
    compute_sec_ratios,
    load_concept_defs,
    load_ratio_defs,
    standardize_sec_statements,
)


def _sec_row(cik, tag, date, value, rt="2024-02-01", unit="USD", missing=False):
    return {
        "source": "sec",
        "series_id": f"{cik}:us-gaap/{tag}:{unit}",
        "observation_date": date, "realtime_start": rt, "value": value,
        "is_missing": missing,
    }


CONCEPTS = [
    ConceptDef(name="liabilities", tags=("Liabilities",), statement="balance_sheet"),
    ConceptDef(name="stockholders_equity",
               tags=("StockholdersEquity",
                     "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"),
               statement="balance_sheet"),
    ConceptDef(name="current_assets", tags=("AssetsCurrent",)),
    ConceptDef(name="current_liabilities", tags=("LiabilitiesCurrent",)),
]
RATIOS = [
    RatioDef(name="debt_to_equity", numerator="liabilities",
             denominator="stockholders_equity"),
    RatioDef(name="current_ratio", numerator="current_assets",
             denominator="current_liabilities"),
]

CIK_A = "CIK0000320193"
CIK_B = "CIK0000789019"


# ---- config loaders ------------------------------------------------------

def test_shipped_configs_load():
    concepts = load_concept_defs("config/sec_concepts.yml")
    names = {c.name for c in concepts}
    assert {"assets", "liabilities", "stockholders_equity"} <= names
    ratios = {r.name for r in load_ratio_defs("config/sec_ratios.yml")}
    assert {"debt_to_equity", "current_ratio"} <= ratios


def test_config_missing_is_empty_and_malformed_raises(tmp_path):
    assert load_concept_defs("nope.yml") == []
    bad = tmp_path / "c.yml"
    bad.write_text("concepts:\n  - name: x\n")  # no tags
    with pytest.raises(SECStandardizationConfigError):
        load_concept_defs(str(bad))


# ---- standardization -----------------------------------------------------

def test_standardize_maps_tags_to_concepts():
    rows = [
        _sec_row(CIK_A, "Liabilities", "2023-09-30", 100.0),
        _sec_row(CIK_A, "StockholdersEquity", "2023-09-30", 50.0),
        _sec_row(CIK_A, "SomeUnmappedTag", "2023-09-30", 999.0),  # dropped
    ]
    out = standardize_sec_statements(rows, CONCEPTS)
    got = {(r["concept"], r["value"]) for r in out}
    assert got == {("liabilities", 100.0), ("stockholders_equity", 50.0)}
    assert all(r["cik"] == CIK_A for r in out)


def test_standardize_tag_priority_and_latest_filing():
    # higher-priority tag wins; within a tag, the later filing wins
    rows = [
        _sec_row(CIK_A, "StockholdersEquity", "2023-09-30", 50.0, rt="2023-11-01"),
        _sec_row(CIK_A, "StockholdersEquity", "2023-09-30", 52.0, rt="2024-02-01"),  # latest
        _sec_row(CIK_A, "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
                 "2023-09-30", 999.0, rt="2024-05-01"),  # lower priority -> ignored
    ]
    out = standardize_sec_statements(rows, CONCEPTS)
    eq = [r for r in out if r["concept"] == "stockholders_equity"]
    assert len(eq) == 1 and eq[0]["value"] == 52.0


def test_standardize_ignores_non_sec_rows():
    rows = [{"source": "fred", "series_id": "DGS10", "observation_date": "2023-09-30",
             "value": 4.0, "is_missing": False}]
    assert standardize_sec_statements(rows, CONCEPTS) == []


# ---- ratios --------------------------------------------------------------

def test_compute_ratios():
    std = [
        {"cik": CIK_A, "concept": "liabilities", "observation_date": "2023-09-30",
         "value": 100.0},
        {"cik": CIK_A, "concept": "stockholders_equity",
         "observation_date": "2023-09-30", "value": 50.0},
        {"cik": CIK_A, "concept": "current_assets", "observation_date": "2023-09-30",
         "value": 30.0},
        {"cik": CIK_A, "concept": "current_liabilities",
         "observation_date": "2023-09-30", "value": 60.0},
    ]
    out = {r["ratio_name"]: r["value"] for r in compute_sec_ratios(std, RATIOS)}
    assert out == {"debt_to_equity": 2.0, "current_ratio": 0.5}


def test_ratio_zero_denominator_skipped():
    std = [
        {"cik": CIK_A, "concept": "liabilities", "observation_date": "2023-09-30",
         "value": 100.0},
        {"cik": CIK_A, "concept": "stockholders_equity",
         "observation_date": "2023-09-30", "value": 0.0},
    ]
    assert compute_sec_ratios(std, RATIOS) == []


# ---- local backend end-to-end (fundamentals, ratios, ranks view) ---------

def _sv(cik, tag, date, value, rt):
    return {"source": "sec", "series_id": f"{cik}:us-gaap/{tag}:USD",
            "observation_date": date, "realtime_start": rt, "realtime_end": "",
            "value": value, "raw_value": str(value), "is_missing": False,
            "row_hash": f"{cik}{tag}{rt}", "revision_number": 1,
            "ingested_at": "t", "run_id": "r"}


def test_local_build_company_financials_and_ranks(tmp_path):
    cfg = PipelineConfig(environment=Environment.DEV, fred_api_key="k")
    wh = LocalWarehouse(cfg, db_path=str(tmp_path / "f.db"))
    # two companies, same period; debt/equity = 2.0 (A) and 1.0 (B)
    wh.merge_silver([
        _sv(CIK_A, "Liabilities", "2023-09-30", 100.0, "2023-11-01"),
        _sv(CIK_A, "StockholdersEquity", "2023-09-30", 50.0, "2023-11-01"),
        _sv(CIK_B, "Liabilities", "2023-09-30", 80.0, "2023-11-01"),
        _sv(CIK_B, "StockholdersEquity", "2023-09-30", 80.0, "2023-11-01"),
    ])
    wh.build_gold()

    fund = wh.query("SELECT count(*) c FROM gold_fred_company_fundamentals")[0]["c"]
    assert fund == 4
    de = {r["cik"]: r["value"] for r in wh.query(
        "SELECT cik, value FROM gold_fred_company_ratios WHERE ratio_name='debt_to_equity'")}
    assert de == {CIK_A: 2.0, CIK_B: 1.0}

    # cross-company ranks view: B (1.0) is lowest leverage -> pct_rank 0
    ranks = {r["cik"]: r["pct_rank"] for r in wh.query(
        "SELECT cik, pct_rank FROM gold_v_company_ratio_ranks "
        "WHERE ratio_name='debt_to_equity'")}
    assert ranks[CIK_B] == 0.0 and ranks[CIK_A] == 1.0
    wh.close()
