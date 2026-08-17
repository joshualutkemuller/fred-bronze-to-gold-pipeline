# Power BI Wave 0 Kernel — Build Guide

**Status:** Specification complete; ready to build in Power BI Desktop  
**Contents:** Core data model, reusable measures, theme  
**Prerequisite:** Databricks access, Power BI Desktop  
**Dependency:** fnGold function created (`sql/fnGold.sql`)

---

## 1. Data Model Foundation

Two dimension tables form the kernel for all fourteen reports. Both are Import mode, uncompressed (optimized for relationships over refresh speed).

### 1.1 qDimDate — Calendar Dimension

**Source Query (Databricks)**

```sql
SELECT date, date_key, year, quarter, quarter_label, year_quarter,
       year_quarter_sort, month, month_name, month_short_name, year_month,
       year_month_sort, week_of_year, year_week, day_name, day_short_name,
       is_weekday, is_month_end, is_quarter_end, is_year_end,
       fiscal_year, fiscal_year_label, fiscal_quarter, fiscal_quarter_label,
       is_recession
FROM ${p_Catalog}.gold.dim_date
```

**Power Query M Code**

```powerquery
let
    Source = Databricks.Contents("workspace_url", "http_path"),
    Query = Source{[Name = "gold.dim_date"]}[Data],
    #"Changed Type" = Table.TransformColumnTypes(Query, {
        {"date", type date},
        {"date_key", Int32.Type},
        {"year", Int32.Type},
        {"quarter", Int32.Type},
        {"quarter_label", type text},
        {"year_quarter", type text},
        {"year_quarter_sort", Int32.Type},
        {"month", Int32.Type},
        {"month_name", type text},
        {"month_short_name", type text},
        {"year_month", type text},
        {"year_month_sort", Int32.Type},
        {"week_of_year", Int32.Type},
        {"year_week", type text},
        {"day_name", type text},
        {"day_short_name", type text},
        {"is_weekday", type logical},
        {"is_month_end", type logical},
        {"is_quarter_end", type logical},
        {"is_year_end", type logical},
        {"fiscal_year", Int32.Type},
        {"fiscal_year_label", type text},
        {"fiscal_quarter", type text},
        {"fiscal_quarter_label", type text},
        {"is_recession", type logical}
    })
in
    #"Changed Type"
```

**Model Settings**

- **Mark as Date Table** on `[date]` column.
- **Hide from report view:** all `*_sort` columns.
- **Sort-by rules:**
  - `month_short_name` sorted by `month`
  - `quarter_label` sorted by `quarter`
  - `year_month` sorted by `year_month_sort`
  - `day_short_name` sorted by `week_of_year`

---

### 1.2 qDimSeries — National Catalog Dimension

**Source Query (Databricks)**

```sql
SELECT series_id, title, source, frequency, units, econ_category,
       polarity, default_transform, scale, decimals, notes
FROM ${p_Catalog}.gold.dim_series
WHERE econ_category <> 'REGIONAL'
```

**Power Query M Code**

```powerquery
let
    Source = Databricks.Contents("workspace_url", "http_path"),
    Query = Source{[Name = "gold.dim_series"]}[Data],
    #"Filtered Rows" = Table.SelectRows(Query, each [econ_category] <> "REGIONAL"),
    #"Changed Type" = Table.TransformColumnTypes(#"Filtered Rows", {
        {"series_id", type text},
        {"title", type text},
        {"source", type text},
        {"frequency", type text},
        {"units", type text},
        {"econ_category", type text},
        {"polarity", type text},
        {"default_transform", type text},
        {"scale", type number},
        {"decimals", Int32.Type},
        {"notes", type text}
    })
in
    #"Changed Type"
```

**Model Settings**

- **Hide from report view:** `polarity`, `default_transform`.
- **Sort-by:** None required (single dimension on series_id).

---

## 2. Core Measures (Reusable across all reports)

Define these in the Report 1 model and copy to other reports as needed. All are simple DAX with no context — they are building blocks for report-specific measures.

### Time Aggregation Helpers

```dax
// DAX measures for Power BI Desktop

-- Returns the latest date in the selected filter context
[Max Date] = MAX('qDimDate'[date])

-- Returns the earliest date in the selected filter context  
[Min Date] = MIN('qDimDate'[date])

-- Returns the calendar year of the Max Date
[Current Year] = YEAR([Max Date])

-- Returns the fiscal year label for the Max Date
[Current Fiscal Year] = MAX('qDimDate'[fiscal_year_label])
```

### Formatting Helpers

```dax
-- Format a decimal as percentage (e.g., 0.003 = 0.3%)
[Format Pct] = 
    VAR val = [Value]
    RETURN
        IF(ISBLANK(val), BLANK(), val * 100 & "%")

-- Format a value as basis points (e.g., 0.0325 = 3.25 bps)
[Format Bps] =
    VAR val = [Value]
    RETURN
        IF(ISBLANK(val), BLANK(), val * 10000 & " bps")

-- Format a z-score to 2 decimal places
[Format Zscore] =
    VAR z = [Zscore]
    RETURN
        IF(ISBLANK(z), BLANK(), ROUND(z, 2))
```

### Validation Measures

```dax
-- Surface row count for debugging (hide in production)
[Row Count] = COUNTROWS(SUMMARIZE(VALUES('qDimDate'[date]), 'qDimDate'[date]))

-- Flag null values (highlight when a required column is blank)
[Has Nulls] = COUNTBLANK([Value])
```

---

## 3. Theme & Color Palette

Power BI themes are JSON. This palette is **brand-neutral placeholder** — swap the hex codes for your brand colors. The formula ensures consistency across reports.

**Place in** `powerbi/theme.json`:

```json
{
  "name": "FRED Pipeline Default",
  "dataColors": [
    "#1f77b4",
    "#ff7f0e", 
    "#2ca02c",
    "#d62728",
    "#9467bd",
    "#8c564b",
    "#e377c2",
    "#7f7f7f",
    "#bcbd22",
    "#17becf"
  ],
  "background": "#ffffff",
  "foreground": "#333333",
  "tableAccent": "#1f77b4",
  "good": "#107c10",
  "warning": "#ffb81c",
  "bad": "#d83b01"
}
```

**How to apply:** In Power BI Desktop → **View** → **Themes** → **Browse for themes** → select `theme.json`.

---

## 4. Model Scaffold: PBIP Structure

Power BI Project files (.pbip) are directory-based. Create this structure in `powerbi/`:

```
powerbi/
├── .gitignore
├── report.json                 # Project metadata
├── theme.json                  # Color palette
├── StaticResources/
│   └── SharedResources/
│       └── BaseThemes/
│           └── FRED.json       # Theme reference
├── Report/
│   ├── definition.pbir         # Report definition
│   └── StaticResources/
│       └── RegisteredResources/ (empty initially)
└── SemanticModel/
    ├── definition.bim          # Data model (generated on first save)
    └── Tables/                 (generated on first save)
```

**report.json** (minimal scaffold):

```json
{
  "version": "1.0",
  "configurationFile": ".pbix.json",
  "securityBindingReference": null,
  "themeReference": {
    "path": "theme.json"
  }
}
```

In Power BI Desktop:
1. File → Save As → **Power BI Project (.pbip)** → select `powerbi/` directory
2. Apply theme: **View** → **Themes** → select `FRED.json`
3. Close and reopen to verify structure

---

## 5. Relationships & Model Settings (Wave 0 baseline)

These relationships form the spine for all fourteen reports. Define them in the first model; reference them when building others.

| From | To | Cardinality | Direction |
|---|---|---|---|
| `qDimDate[date]` | *any fact's date column* | 1:* | Single |
| `qDimSeries[series_id]` | *any national fact's series_id* | 1:* | Single |

**Important:** Do NOT create relationships that fan out. A `qDimDate` to multiple facts is fine; each fact joins once. Avoid triangles (A → C ← B) unless C is explicitly disconnected.

---

## 6. Build Checklist

- [ ] Create Databricks connection in Power BI Desktop
- [ ] Execute `sql/fnGold.sql` on Databricks
- [ ] Create new Power BI Project (.pbip)
- [ ] Load `qDimDate` with Power Query M code above
- [ ] Load `qDimSeries` (national filter applied)
- [ ] Mark `qDimDate` as date table
- [ ] Create both relationships above
- [ ] Apply sort-by rules to date columns
- [ ] Hide `*_sort` and `polarity` columns
- [ ] Apply theme
- [ ] Create a blank page to test: create a card visual on `[Row Count]`, should show 2+ million
- [ ] Save as `powerbi/FRED_Gold_Wave_0.pbip`

---

## 7. Next: Report 14

Once Wave 0 is built and you confirm row counts are reasonable:
1. Add Report 14 queries (eight tables)
2. Add relationships and measures
3. Run pipeline and test against real data
4. Confirm `BGCRRATE` series exists in `qSeriesRun`
5. Proceed to Wave 2

See `docs/handoffs/powerbi_report_14_health.md` for the complete Report 14 specification.
