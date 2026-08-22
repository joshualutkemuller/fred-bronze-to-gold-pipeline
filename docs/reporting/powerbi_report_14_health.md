# Report 14 — Pipeline Health & Governance

**Status:** Ready to build
**Wave:** 1 (build alone, before other reports)
**Pages:** 8 (Overview, Run History, Series Failures, Data Quality, Staleness, Drift, Lifecycle, Catalog)
**Prerequisite:** Wave 0 kernel (qDimDate, qDimSeries), fnGold function

---

## 1. Purpose & Scope

Report 14 answers four questions every operator needs answered on every run:

1. **Did the run succeed?** — Run status, duration, series counts.
2. **Which series failed and why?** — Failure root cause and per-series audit trail.
3. **Is the data clean?** — Data quality pass rates per check and per series.
4. **Are sources stale?** — Days since last observation per source; manual update flags.

**It is the acceptance test for the pipeline.** Before any other report in this suite is built or trusted, Report 14 must show:
- ✅ Correct run completion status (not SUCCEEDED when gold layer failed)
- ✅ `BGCRRATE` series present in `qSeriesRun` (the defect G-A fixed)
- ✅ Expected series counts matching active manifests
- ✅ DQ pass rates in normal range (>95% passing is typical)

---

## 2. Data Model — Tables & Relationships

### 2.1 Source Tables (8 total)

All are Import mode, partitioned on run date for incremental refresh.

#### qEtlRun
**Grain:** 1 row per pipeline invocation
**Refresh:** One-time per run (no incremental needed)

```sql
SELECT run_id, environment, triggered_by, status, started_at, ended_at,
       duration_seconds, series_total, series_succeeded, series_failed,
       error_message
FROM ${p_Catalog}.audit.etl_run
```

**Power Query M:**
```powerquery
let
    Source = Databricks.Contents(...),
    Query = Source{[Name = "audit.etl_run"]}[Data],
    #"Converted" = Table.TransformColumnTypes(Query, {
        {"run_id", type text},
        {"environment", type text},
        {"triggered_by", type text},
        {"status", type text},
        {"started_at", type datetimezone},
        {"ended_at", type datetimezone},
        {"duration_seconds", Int64.Type},
        {"series_total", Int32.Type},
        {"series_succeeded", Int32.Type},
        {"series_failed", Int32.Type},
        {"error_message", type text}
    }),
    #"Sorted" = Table.Sort(#"Converted", {{"started_at", Order.Descending}})
in
    #"Sorted"
```

#### qSeriesRun
**Grain:** 1 row per (run_id, series_id)
**Refresh:** Incremental on run date

```sql
SELECT run_id, series_id, status, load_type, started_at, duration_seconds,
       observations_extracted, rows_written_bronze, rows_merged_silver,
       dq_passed, error_message
FROM ${p_Catalog}.audit.etl_series_run
```

**Power Query M:**
```powerquery
let
    Source = Databricks.Contents(...),
    Query = Source{[Name = "audit.etl_series_run"]}[Data],
    #"Converted" = Table.TransformColumnTypes(Query, {
        {"run_id", type text},
        {"series_id", type text},
        {"status", type text},
        {"load_type", type text},
        {"started_at", type datetimezone},
        {"duration_seconds", Int64.Type},
        {"observations_extracted", Int32.Type},
        {"rows_written_bronze", Int32.Type},
        {"rows_merged_silver", Int32.Type},
        {"dq_passed", type logical},
        {"error_message", type text}
    })
in
    #"Converted"
```

#### qDataQuality
**Grain:** 1 row per (run_id, series_id, check_name)
**Refresh:** Incremental on run date

```sql
SELECT run_id, series_id, check_name, passed, severity, message, metric_value
FROM ${p_Catalog}.audit.data_quality_result
```

**Power Query M:**
```powerquery
let
    Source = Databricks.Contents(...),
    Query = Source{[Name = "audit.data_quality_result"]}[Data],
    #"Converted" = Table.TransformColumnTypes(Query, {
        {"run_id", type text},
        {"series_id", type text},
        {"check_name", type text},
        {"passed", type logical},
        {"severity", type text},
        {"message", type text},
        {"metric_value", type number}
    })
in
    #"Converted"
```

#### qStaleness
**Grain:** 1 row per (source, series_id) per check
**Refresh:** Latest per key only (deduplicate in PQ)

```sql
SELECT source, series_id, frequency, latest_observation_date,
       days_since_last_observation, is_stale, has_data, checked_at
FROM ${p_Catalog}.meta.series_staleness
```

**Power Query M:**
```powerquery
let
    Source = Databricks.Contents(...),
    Query = Source{[Name = "meta.series_staleness"]}[Data],
    #"String Dates" = Table.TransformColumnTypes(Query, {
        {"checked_at", type text}
    }),
    #"Parsed Dates" = Table.AddColumn(#"String Dates", "checked_at_parsed",
        each DateTime.FromText([checked_at]), type datetimezone),
    #"Latest Per Key" = Table.Group(#"Parsed Dates",
        {"source", "series_id"},
        {{"All", each Table.Sort(_, {{"checked_at_parsed", Order.Descending}})[0]}},
        GroupKind.Local),
    #"Expanded" = Table.ExpandRecordColumn(#"Latest Per Key", "All",
        {"frequency", "latest_observation_date", "days_since_last_observation",
         "is_stale", "has_data", "checked_at", "checked_at_parsed"})
in
    #"Expanded"
```

#### qDrift
**Grain:** 1 row per (series_id, field, kind)
**Refresh:** One-time

```sql
SELECT series_id, field, manifest_value, fred_value, kind, severity, detected_at
FROM ${p_Catalog}.meta.fred_series_drift
```

**Power Query M:**
```powerquery
let
    Source = Databricks.Contents(...),
    Query = Source{[Name = "meta.fred_series_drift"]}[Data],
    #"String Dates" = Table.TransformColumnTypes(Query, {
        {"detected_at", type text}
    }),
    #"Parsed" = Table.AddColumn(#"String Dates", "detected_at_parsed",
        each DateTime.FromText([detected_at]), type datetimezone),
    #"Removed" = Table.RemoveColumns(#"Parsed", {"detected_at"}),
    #"Renamed" = Table.RenameColumns(#"Removed", {{"detected_at_parsed", "detected_at"}})
in
    #"Renamed"
```

#### qLifecycle
**Grain:** 1 row per series_id
**Refresh:** One-time

```sql
SELECT series_id, fred_title, fred_frequency, observation_start, observation_end,
       last_updated, popularity, discontinued, days_since_last_observation,
       is_stale, checked_at
FROM ${p_Catalog}.meta.fred_series_lifecycle
```

**Power Query M:**
```powerquery
let
    Source = Databricks.Contents(...),
    Query = Source{[Name = "meta.fred_series_lifecycle"]}[Data],
    #"String Dates" = Table.TransformColumnTypes(Query, {
        {"observation_start", type text},
        {"observation_end", type text},
        {"last_updated", type text},
        {"checked_at", type text}
    }),
    #"Parsed" = Table.AddColumn(Table.AddColumn(Table.AddColumn(
        Table.AddColumn(#"String Dates", "obs_start",
            each DateTime.FromText([observation_start]), type datetimezone),
        "obs_end", each DateTime.FromText([observation_end]), type datetimezone),
        "updated", each DateTime.FromText([last_updated]), type datetimezone),
        "checked", each DateTime.FromText([checked_at]), type datetimezone)
in
    #"Parsed"
```

#### qSeriesMeta
**Grain:** 1 row per series_id
**Refresh:** One-time

```sql
SELECT series_id, title, category, frequency, units, active, load_type,
       vintage_enabled, validation_profile, business_owner, technical_owner,
       priority
FROM ${p_Catalog}.meta.fred_series
```

**Power Query M:**
```powerquery
let
    Source = Databricks.Contents(...),
    Query = Source{[Name = "meta.fred_series"]}[Data],
    #"Converted" = Table.TransformColumnTypes(Query, {
        {"series_id", type text},
        {"title", type text},
        {"category", type text},
        {"frequency", type text},
        {"units", type text},
        {"active", type logical},
        {"load_type", type text},
        {"vintage_enabled", type logical},
        {"validation_profile", type text},
        {"business_owner", type text},
        {"technical_owner", type text},
        {"priority", Int32.Type}
    })
in
    #"Converted"
```

#### qCatalog
**Grain:** 1 row per Gold table object
**Refresh:** One-time (documentation)

```sql
SELECT object_name, object_type, module, grain, intended_visual, description
FROM ${p_Catalog}.gold.powerbi_catalog
```

**Power Query M:**
```powerquery
let
    Source = Databricks.Contents(...),
    Query = Source{[Name = "gold.powerbi_catalog"]}[Data]
in
    Query
```

---

### 2.2 Relationships

| From | To | Cardinality | Direction | Active |
|---|---|---|---|---|
| `qEtlRun[run_id]` | `qSeriesRun[run_id]` | 1:* | single | ✅ |
| `qEtlRun[run_id]` | `qDataQuality[run_id]` | 1:* | single | ✅ |
| `qSeriesMeta[series_id]` | `qSeriesRun[series_id]` | 1:* | single | ✅ |
| `qSeriesMeta[series_id]` | `qDataQuality[series_id]` | 1:* | single | ✅ |
| `qSeriesMeta[series_id]` | `qStaleness[series_id]` | 1:* | single | ✅ |
| `qSeriesMeta[series_id]` | `qDrift[series_id]` | 1:* | single | ✅ |
| `qSeriesMeta[series_id]` | `qLifecycle[series_id]` | 1:* | single | ✅ |
| `qDimDate[date]` | *derive `run_date` from `qEtlRun[started_at]`* | 1:* | single | ✅ |

**Important:** `qCatalog` is **standalone** — related to nothing. It is documentation only.

---

## 3. Model Settings & Columns

### Column Visibility (Hide from report view)

Hide these to keep the field list clean:

```
qEtlRun: (show all)
qSeriesRun: (show all)
qDataQuality: (show all)
qStaleness: checked_at, checked_at_parsed
qDrift: detected_at, detected_at_parsed
qLifecycle: obs_start, obs_end, updated, checked
qSeriesMeta: (show all)
qCatalog: (show all)
```

### Column Formatting

| Column | Format |
|---|---|
| `days_since_last_observation` | Whole number |
| `duration_seconds` | Whole number |
| `observations_extracted` | Whole number |
| `popularity` | Whole number |
| `checked_at`, `started_at`, `detected_at`, `last_updated` | Date/Time |
| `series_total`, `series_succeeded`, `series_failed` | Whole number |

### Incremental Refresh Setup

In Power BI:
- **qSeriesRun**: Partition on `started_at` (date), refresh last 14 days
- **qDataQuality**: Partition on `started_at`, refresh last 14 days
- **Others**: One-time import (no incremental)

---

## 4. DAX Measures

Define all measures in a single **Measures** table (not on qEtlRun) for organization.

### Run Status & Counts

```dax
-- Latest run (most recent by started_at)
[Latest Run ID] =
    VAR latest = MAX('qEtlRun'[started_at])
    RETURN
        CALCULATE(
            MAX('qEtlRun'[run_id]),
            FILTER('qEtlRun', 'qEtlRun'[started_at] = latest)
        )

-- Overall run status badge
[Run Status] = MAX('qEtlRun'[status])

-- Series success rate (%)
[Success Rate] =
    IF(MAX('qEtlRun'[series_total]) = 0, BLANK(),
        DIVIDE(
            MAX('qEtlRun'[series_succeeded]),
            MAX('qEtlRun'[series_total])
        ) * 100
    )

-- Count of failed series
[Failed Series Count] = MAX('qEtlRun'[series_failed])

-- Total series (for context)
[Total Series] = MAX('qEtlRun'[series_total])

-- Run duration in minutes
[Duration Minutes] =
    IF(MAX('qEtlRun'[duration_seconds]) = BLANK(), BLANK(),
        DIVIDE(MAX('qEtlRun'[duration_seconds]), 60)
    )
```

### Data Quality

```dax
-- DQ pass rate across all checks
[DQ Pass Rate] =
    VAR total = COUNTA('qDataQuality'[passed])
    VAR passed = CALCULATE(
        COUNTA('qDataQuality'[passed]),
        FILTER('qDataQuality', 'qDataQuality'[passed] = TRUE)
    )
    RETURN
        IF(total = 0, BLANK(), DIVIDE(passed, total) * 100)

-- Failed check count per series
[Failed Checks] =
    CALCULATE(
        COUNTA('qDataQuality'[check_name]),
        FILTER('qDataQuality', 'qDataQuality'[passed] = FALSE)
    )

-- Warning-severity DQ issues
[Warning Count] =
    CALCULATE(
        COUNTA('qDataQuality'[check_name]),
        FILTER('qDataQuality', 'qDataQuality'[severity] = "warning" && 'qDataQuality'[passed] = FALSE)
    )
```

### Staleness & Freshness

```dax
-- Count of stale series (days_since_last_observation > frequency-dependent threshold)
[Stale Count] =
    CALCULATE(
        COUNTA('qStaleness'[series_id]),
        FILTER('qStaleness', 'qStaleness'[is_stale] = TRUE)
    )

-- Average days since last observation
[Avg Days Behind] = AVERAGE('qStaleness'[days_since_last_observation])

-- Sources that have not updated today
[Sources Behind Schedule] =
    VAR today = TODAY()
    RETURN
        CALCULATE(
            COUNTDISTINCT('qStaleness'[source]),
            FILTER('qStaleness', 'qStaleness'[days_since_last_observation] > 1)
        )
```

### Drift & Lifecycle Issues

```dax
-- FRED metadata drift detected (manifest vs. live mismatch)
[Drift Count] = COUNTA('qDrift'[series_id])

-- Discontinued series still in manifests (indicator flag)
[Discontinued Count] =
    CALCULATE(
        COUNTA('qLifecycle'[series_id]),
        FILTER('qLifecycle', 'qLifecycle'[discontinued] = TRUE)
    )

-- Very stale FRED series (no update in 2+ years, not expected)
[Ancient Count] =
    CALCULATE(
        COUNTA('qLifecycle'[series_id]),
        FILTER('qLifecycle', 'qLifecycle'[days_since_last_observation] > 730)
    )
```

---

## 5. Page Specifications

Build these eight pages in sequence. Each validates a layer of the pipeline.

### Page 1: Overview (Executive Summary)

**Visuals:**

| Position | Type | Measure | Filter |
|---|---|---|---|
| Top-left | Card | `[Latest Run ID]` | (none) |
| Top-center | Card | `[Run Status]` | (none); conditional fill: green/yellow/red by status |
| Top-right | Card | `[Duration Minutes]` | (none); suffix " min" |
| Middle-left | Card | `[Success Rate]` | (none); format as %; red if <95% |
| Middle-center | Card | `[Failed Series Count]` | (none); red if > 0 |
| Middle-right | Card | `[DQ Pass Rate]` | (none); format as %; warn if <95% |
| Bottom-left | Line chart | X: `qEtlRun[started_at]` (date), Y: `[Success Rate]` | Last 30 runs |
| Bottom-right | Table | Columns: `run_id`, `[Run Status]`, `[Duration Minutes]`, `[Total Series]`, `[Failed Series Count]` | Sorted descending by started_at; top 10 |

**Slicers:**

- `qEtlRun[environment]` (dropdown, single-select)

**Narrative:** "This run completed in [Duration Minutes] with [Success Rate]% of series succeeding. [Failed Series Count] series failed. Data quality is at [DQ Pass Rate]%."

---

### Page 2: Run History (Trend & Root Cause)

**Visuals:**

| Position | Type | Detail |
|---|---|---|
| Top-full | Combo chart | X: started_at (date), Y1: series_succeeded (bar), Y2: series_failed (bar, red) |
| Middle-left | Table | run_id, status, started_at, series_total, series_failed, error_message |
| Middle-right | Gauge | Series success rate %; target = 100%; zones: <95% red, 95–99% yellow, ≥99% green |

**Interactions:**

- Chart click on bar → filter table below to that run
- Table row click → drillthrough to Page 3 (Series Failures)

**Data Validation KPI:**

Add a measure: `[Expected Series Count]` = 2820 (from manifest). Compare to `[Total Series]`. If they diverge, surface it as a warning card.

---

### Page 3: Series Failures

**Purpose:** Find which series failed and why.

**Visuals:**

| Position | Type | Detail |
|---|---|---|
| Top-left | Slicer | `qSeriesMeta[series_id]` → filter table below |
| Top-right | Card | `[Failed Series Count]` (context-filtered) |
| Bottom-full | Table | Columns: series_id, title, status, error_message, duration_seconds; filters: status = "failed" |

**Drill Detail:**

- Click a failed series_id → open a card showing:
  - `qSeriesMeta[title]`, `[business_owner]`, `[technical_owner]`
  - `qDataQuality` checks for that series (list of failed checks)
  - Last 3 runs' status for that series (trend)

---

### Page 4: Data Quality Dashboard

**Purpose:** Monitor DQ check outcomes across all series.

**Visuals:**

| Position | Type | Detail |
|---|---|---|
| Top-left | Card | `[DQ Pass Rate]` |
| Top-right | Card | Count of unique checks that failed in latest run |
| Middle-left | Heatmap | Rows: `check_name`, Columns: `series_id`, Values: passed (T/F color), filtered to top 20 failing series |
| Middle-right | Clustered bar | X: check_name, Y: count of failures (top 10) |
| Bottom-full | Table | run_id, series_id, check_name, passed, severity, message; sort by severity DESC |

**Drill:** Click severity = "error" → filter table to errors only.

---

### Page 5: Staleness & Freshness

**Purpose:** Identify sources and series that have not updated.

**Visuals:**

| Position | Type | Detail |
|---|---|---|
| Top-left | Card | `[Stale Count]` (series with days_since_last_observation > threshold) |
| Top-right | Card | `[Avg Days Behind]` |
| Middle-left | Slicer | `qStaleness[source]` (multi-select) |
| Middle-right | Gauge | Source freshness: min(days_since_last_observation) across selected source; green if <1, yellow <7, red ≥7 |
| Bottom-full | Table | source, series_id, frequency, latest_observation_date, days_since_last_observation, is_stale, checked_at; sort by days_since_last_observation DESC |

**Alert:** Highlight rows where `is_stale = TRUE` in red.

---

### Page 6: Metadata Drift (FRED Only)

**Purpose:** Flag manifest vs. FRED live-data mismatches.

**Visuals:**

| Position | Type | Detail |
|---|---|---|
| Top-full | Card | `[Drift Count]` (red if > 0) |
| Middle-full | Table | series_id, field, manifest_value, fred_value, kind (field type), severity, detected_at; sort by severity DESC |

**Narrative:** "FRED metadata drift indicates that the manifest definition (`manifest_value`) does not match the live FRED API response (`fred_value`). Reconcile and update the manifest if intentional; escalate if not."

**Note:** This page is FRED-only by design. If other sources are added later, they won't populate here.

---

### Page 7: Series Lifecycle (Discontinuations & Age)

**Purpose:** Catch dead or dormant series.

**Visuals:**

| Position | Type | Detail |
|---|---|---|
| Top-left | Card | `[Discontinued Count]` (red if > 0) |
| Top-right | Card | `[Ancient Count]` (series not updated in 2 years) |
| Bottom-full | Table | series_id, fred_title, observation_end (if discontinued), last_updated, days_since_last_observation, discontinued, is_stale, popularity; sort by days_since_last_observation DESC |

**Alert:** Highlight `discontinued = TRUE` rows in red; `days_since_last_observation > 730` in yellow.

---

### Page 8: Gold Catalog (Documentation)

**Purpose:** Self-documenting audit of what Gold layer contains.

**Visual:**

| Position | Type | Detail |
|---|---|---|
| Full | Table | object_name, object_type, module, grain, intended_visual, description; sort by module, object_type |

**Slicer:** `object_type` (filter to "table" / "view" / "function")

**Note:** This table comes from `qCatalog` and is disconnected. It documents the exact Gold layer structure the pipeline built.

---

## 6. Validation After First Pipeline Run

Once the pipeline runs and populates the Gold layer, verify Report 14 on these points before declaring it complete:

| Check | Expected | Action if Wrong |
|---|---|---|
| `[Total Series]` count | 2820 (active + inactive) | Audit `meta.fred_series` active flag |
| `BGCRRATE` in `qSeriesRun` | Should exist (G-A fixed) | Check `audit.etl_series_run` for `BGCRRATE` entry |
| DQ pass rate | >95% typical | Investigate failed check messages on Page 4 |
| Run status | SUCCEEDED or PARTIAL | If FAILURE, check `qEtlRun[error_message]` |
| `[Stale Count]` | 0 on first run | Staleness will accumulate on subsequent runs |
| Drift count | Typically 0–5 | Known drifts = documentation needed in manifest |

**First-run acceptance test:** Report 14 loads, all tables populate, no blank cards, run status is SUCCEEDED or PARTIAL (not FAILURE), and `BGCRRATE` appears in the failure table (it should not fail).

---

## 7. Build Checklist

- [ ] Wave 0 kernel complete (qDimDate, qDimSeries, fnGold)
- [ ] All 8 qXxx tables added to model via Power Query
- [ ] All relationships created (7 total)
- [ ] String date columns converted to datetime in Power Query
- [ ] `run_date` computed column added to `qEtlRun` (from `started_at`, type date)
- [ ] qDimDate joined to `run_date`
- [ ] Column visibility configured (hide checked_at, etc.)
- [ ] Incremental refresh configured on qSeriesRun and qDataQuality
- [ ] All measures defined (24 total across run, DQ, staleness, drift)
- [ ] 8 pages built in order (Overview → History → Failures → DQ → Staleness → Drift → Lifecycle → Catalog)
- [ ] Theme applied
- [ ] Slicers configured (environment on Overview, source on Staleness, check_name on DQ, etc.)
- [ ] Drill-throughs configured (Run History → Failures, top charts → tables)
- [ ] Conditional formatting applied (red/yellow/green status, alert highlighting)
- [ ] Saved as `powerbi/FRED_Report_14_Health.pbip`
- [ ] Pipeline run complete and Gold tables populated
- [ ] Validation checks passed (see §6)

---

## 8. What's Next

Once Report 14 is validated:
1. Archive this report as the baseline (no further changes)
2. Proceed to Wave 2: Reports 1 (Macro Cockpit) & 3 (Curve Lab)
3. Each subsequent report builds on Wave 0 kernel + Report 14 model as reference

See `docs/handoffs/powerbi_report_build_plan.md` §5 for the complete build order.
