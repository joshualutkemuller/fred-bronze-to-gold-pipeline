# Power BI Quick Start — Wave 0 + Report 14

**Built:** fnGold function, Wave 0 kernel specification, Report 14 full build guide  
**Next Step:** Build in Power BI Desktop (no pipeline run required yet)  
**Timeline:** Wave 0 kernel ~2 hours, Report 14 ~4–6 hours

---

## Files Created

| File | Purpose |
|---|---|
| `sql/fnGold.sql` | Databricks stored procedure; run this first on your warehouse |
| `docs/handoffs/powerbi_wave_0_kernel.md` | Build guide for dimensions + measures (common to all reports) |
| `docs/handoffs/powerbi_report_14_health.md` | Complete Report 14 specification (acceptance test for pipeline) |

---

## Prerequisites

- **Databricks SQL warehouse** with FRED Gold layer (from `build_gold()`)
- **Power BI Desktop** (2023.10+)
- **Network access** from Power BI to Databricks

---

## Build Sequence (Before First Pipeline Run)

### Step 1: fnGold Function (5 min)

On your Databricks workspace, execute:

```sql
-- From sql/fnGold.sql
CREATE OR REPLACE FUNCTION fnGold(schema STRING, table_name STRING)
RETURNS STRING
LANGUAGE SQL
IMMUTABLE
AS
  SELECT CONCAT(current_catalog(), '.', schema, '.', table_name)
```

This is the abstraction layer every report query uses. It returns the fully-qualified table name for the current catalog context.

### Step 2: Wave 0 Kernel (2 hours)

Follow `docs/handoffs/powerbi_wave_0_kernel.md` §6 (Build Checklist):

1. Create Power BI Project (.pbip) in `powerbi/` directory
2. Connect to Databricks
3. Load `qDimDate` (Power Query code in the doc)
4. Load `qDimSeries` (filtered to national, Power Query code in the doc)
5. Mark `qDimDate` as date table
6. Create two relationships (qDimDate → facts, qDimSeries → facts)
7. Hide `*_sort` and metadata columns
8. Apply sort-by rules to date columns
9. Define core measures (4 time helpers, 4 format helpers, 2 validation helpers)
10. Apply theme
11. Test: create a card on `[Row Count]` → should show 2+ million

**Save as:** `powerbi/FRED_Gold_Wave_0.pbip`

### Step 3: Report 14 (4–6 hours)

Follow `docs/handoffs/powerbi_report_14_health.md`:

1. Add 8 data tables to the Wave 0 model (qEtlRun, qSeriesRun, qDataQuality, qStaleness, qDrift, qLifecycle, qSeriesMeta, qCatalog)
2. Each has Power Query M code in §2.1 (copy/paste)
3. Create 7 relationships (§2.2)
4. Convert string date columns to datetime in Power Query
5. Define 24 DAX measures (§4, organized by category)
6. Build 8 pages in sequence (§5):
   - Page 1: Overview (executive cards + run history)
   - Page 2: Run History (trends + root cause)
   - Page 3: Series Failures (drill into why series failed)
   - Page 4: Data Quality (check outcomes heatmap)
   - Page 5: Staleness & Freshness (identify stale sources)
   - Page 6: Metadata Drift (FRED vs manifest mismatches)
   - Page 7: Series Lifecycle (discontinuations, age)
   - Page 8: Gold Catalog (what's in the warehouse, documentation)

**Save as:** `powerbi/FRED_Report_14_Health.pbip`

---

## What Happens During First Pipeline Run

The pipeline will populate these tables Report 14 reads:

- `audit.etl_run` — one row per run
- `audit.etl_series_run` — failures, row counts, load details
- `audit.data_quality_result` — check outcomes
- `meta.series_staleness` — days since last observation per series
- `meta.fred_series_drift` — metadata mismatches
- `meta.fred_series_lifecycle` — FRED series metadata snapshots
- `meta.fred_series` — manifest intent (series ownership, priority, etc.)
- `gold.powerbi_catalog` — self-documenting audit of Gold layer contents

When these tables are populated, Report 14 immediately shows:
- ✅ Run completion status (not silently failing Gold layer anymore)
- ✅ Which series failed and why (error_message)
- ✅ Data quality pass rate
- ✅ Freshness per source
- ✅ Known metadata issues

---

## Validation Checklist (After First Pipeline Run)

Once the pipeline runs and you refresh Report 14 in Power BI:

- [ ] All tables load (no blank pages)
- [ ] Overview page cards populate: run status, success rate, failed count, DQ rate
- [ ] `[Total Series]` = 2820 (or audit why it differs)
- [ ] `BGCRRATE` appears in Run History (confirms G-A funding stress fix)
- [ ] Run status is SUCCEEDED or PARTIAL (not FAILURE)
- [ ] `[DQ Pass Rate]` > 95%
- [ ] No ancient drift entries (Drift page mostly empty)
- [ ] Staleness page shows realistic observation dates per source

**If all pass:** Proceed to Wave 2 (Reports 1 & 3).  
**If any fail:** Debug using Report 14 pages; check `audit.etl_series_run` error_message column.

---

## Why Report 14 First (Before Any Other Report)

1. **It is the acceptance test for the pipeline.** Every readiness verdict in `powerbi_report_build_plan.md` §1 is static analysis of configs against manifests. Report 14 is how you find out whether the run *actually succeeded*, which series failed, what the DQ pass rate is, and how stale each source is.

2. **It de-risks the Gold layer.** After the first run, Report 14 shows whether:
   - `BGCRRATE` resolved (the G-A funding stress defect)
   - Any series is failing silently
   - The audit trail is honest (no run falsely reporting success when Gold rebuild failed)

3. **Other reports inherit the same trust.** If Report 14 looks wrong, every downstream report would have inherited the problem invisibly. Building this first prevents that.

---

## Next: Waves 2–7

Once Report 14 is validated, build remaining reports in this order:

| Wave | Reports | Build Time |
|---|---|---|
| 2 | 1 Macro Cockpit, 3 Curve Lab | 4–6 hrs each |
| 3 | 4 Spreads, 7 Credit, 6 Fed Policy | 3–4 hrs each |
| 4 | 2 Inflation, 8 Regime, 12 Regional Map | 3–5 hrs each |
| 5 | 9 Statistical Lab, 13 PIT Lab | 4–6 hrs each |
| 6 | 11 Equity (skip recon), 10 Global (non-commercial) | 4–6 hrs each |
| 7 | 5 Funding & Liquidity | 3–4 hrs |

All use Wave 0 kernel (qDimDate, qDimSeries) + Report 14 as model reference.

---

## File Organization

```
powerbi/
├── FRED_Gold_Wave_0.pbip           (Wave 0 kernel)
├── FRED_Report_14_Health.pbip      (Report 14 validation)
├── theme.json                       (color palette, applied to both)
└── (future reports use same structure)

docs/handoffs/
├── powerbi_report_build_plan.md     (overall strategy & readiness verdicts)
├── powerbi_report_suite.md          (conventions, DAX patterns, page layouts)
├── powerbi_wave_0_kernel.md         (this wave's spec)
├── powerbi_report_14_health.md      (this report's complete spec)
└── powerbi_quick_start.md           (you are here)

sql/
└── fnGold.sql                       (Databricks function)
```

---

## Troubleshooting

**Problem:** Power Query cannot connect to Databricks  
**Solution:** Ensure Databricks connector is installed (Power BI Desktop → Get Data → Databricks). May require workspace URL and HTTP path from Databricks SQL warehouse settings.

**Problem:** Tables load but `[Row Count]` on Wave 0 test card is blank  
**Solution:** Refresh the data model. If still blank, check that qDimDate has data by clicking into the table in Power Query editor.

**Problem:** Report 14 loads but all cards show blank  
**Solution:** Refresh data model, then refresh page. If qSeriesRun is empty, the pipeline run hasn't populated audit tables yet — proceed to run the pipeline.

**Problem:** String dates in qStaleness and qDrift columns won't convert  
**Solution:** Ensure Power Query M code converts text to datetime type before loading. Sample code provided in §2.1 of Report 14 spec.

---

## Questions?

Refer to:
- `powerbi_report_build_plan.md` for readiness status and strategy
- `powerbi_report_suite.md` for DAX patterns and page layout conventions
- `powerbi_wave_0_kernel.md` for Wave 0 build details
- `powerbi_report_14_health.md` for Report 14 complete specification
