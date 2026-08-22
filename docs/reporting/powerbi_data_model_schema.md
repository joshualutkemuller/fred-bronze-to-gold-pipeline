# Power BI Data Model: Complete Schema Reference

**Purpose:** Single reference guide for building Power BI reports against FRED pipeline Gold layer
**Date:** 2026-08-19
**Coverage:** 46 Gold tables organized by module with relationships, cardinality, and recommended usage patterns
**Status:** All tables implemented and tested

---

## Quick Start

### Star Schema Overview

```
                    ┌─────────────────────┐
                    │  qDimDate           │
                    │  (date PK)          │
                    │  ├─ year, quarter   │
                    │  ├─ month_name      │
                    │  ├─ is_recession    │
                    │  └─ fiscal_year     │
                    └──────────┬──────────┘
                               │
               ┌───────────────┼───────────────┐
               │               │               │
        ┌──────▼────┐    ┌─────▼──────┐  ┌────▼─────────┐
        │ ECON      │    │  INFL      │  │   CURV       │
        │ tables    │    │  tables    │  │   tables     │
        └───────────┘    └────────────┘  └──────────────┘
               │               │               │
               └───────────────┼───────────────┘
                               │
                    ┌──────────▼──────────┐
                    │  qDimSeries         │
                    │  (series_id PK)     │
                    │  ├─ title           │
                    │  ├─ econ_category   │
                    │  ├─ polarity        │
                    │  ├─ default_tx      │
                    │  ├─ units, scale    │
                    │  └─ source          │
                    └─────────────────────┘
```

**Design principle:** Every fact table joins **series_id → qDimSeries** and **obs_date → qDimDate**.
**Benefit:** Slices by category, date range, and metadata without duplicating denormalization.

---

## 1. Dimension Tables (Star Schema Hub)

### 1.1 qDimDate — Time dimension with recession flags

**Grain:** 1 row per calendar date
**Cardinality:** ~36,500 rows (100 years, if populated)
**Primary Key:** `date`
**Purpose:** Enables Power BI time intelligence, recession shading, and date-range slicing
**Mark as:** Date Table in Power BI

| Column | Type | Description | Example |
|---|---|---|---|
| `date` | DATE | Calendar date (PK) | 2026-08-19 |
| `year` | INT | Calendar year | 2026 |
| `quarter` | INT | Quarter (1–4) | 3 |
| `month` | INT | Month (1–12) | 8 |
| `month_name` | TEXT | Full month name | "August" |
| `is_month_end` | BOOL | True if last day of month | true |
| `is_recession` | BOOL | True during NBER recession (from `USREC`) | false |
| `fiscal_year` | INT | Fiscal year (if populated) | 2026 |

**Usage in Power BI:**
- Set as Date Table (Modeling > Mark as Date Table)
- Use for automatic time hierarchy (Year → Quarter → Month)
- Bind `is_recession` to background conditional formatting for chart shading

---

### 1.2 qDimSeries — Series metadata and presentation semantics

**Grain:** 1 row per series
**Cardinality:** 300+ rows (actual series ingested + curated catalog)
**Primary Key:** `series_id`
**Purpose:** Single source of truth for category, polarity, transform defaults, and how to display each series
**Source:** Built from manifests + `config/series_catalog.yml`

| Column | Type | Description | Example |
|---|---|---|---|
| `series_id` | TEXT | FRED series ID (PK) | GDPC1 |
| `title` | TEXT | Series long name | Real Gross Domestic Product |
| `source` | TEXT | Data source | FRED, Tiingo, World Bank, etc. |
| `frequency` | TEXT | Observation frequency | Quarterly, Daily, Monthly, Annual |
| `econ_category` | TEXT | Dashboard category | GROWTH, INFLATION, LABOR, RATES, CREDIT, HOUSING, CONSUMER, MONEY, ACTIVITY, FX |
| `units` | TEXT | Unit of measurement | Billions of USD, Index, % annualized |
| `default_transform` | TEXT | Terminal's display transform | `level`, `pc1` (% change YoY), `pch` (% change), `chg` (absolute change), `bps` (basis points) |
| `polarity` | INT | +1 (good when rising), −1 (good when falling), 0 (neutral) | +1 for GDPC1 (growth), −1 for UNRATE (unemployment) |
| `decimals` | INT | Display precision | 1 for % series, 0 for levels |
| `scale` | TEXT | Visual scale label | "$T" (trillions), "%" (percent), "" (none) |
| `notes` | TEXT | Usage notes | "Advanced estimate published first Friday of month" |

**Usage in Power BI:**
- Filter visuals by dragging `econ_category` to slicer
- Denormalize `polarity`, `default_transform`, `scale` into measures for smart formatting
- Build drill-through from category summary → individual series detail

**Example categories:**
- `GROWTH` — GDP, production, activity (GDPC1, INDPRO, HOUST)
- `INFLATION` — Price indices (CPIAUCSL, CPILFESL, PCEPI)
- `LABOR` — Employment, jobless (UNRATE, PAYEMS, ICSA)
- `RATES` — Treasury, policy, money market (DGS10, SOFR, EFFR)
- `CREDIT` — OAS spreads, credit conditions (BAMLH0A0HYM2, BAMLC0A0CM)
- `FUNDING` — Fed corridors, funding costs (SOFR, IORB, OBFR, Fed balance sheet)
- `HOUSING` — Starts, permits, prices (HOUST, PERMIT, CSUSHPISA)
- `CONSUMER` — Sentiment, sales (UMCSENT, TOTALSA)
- `MONEY` — Money supply, reserves (M1SL, M2SL, WALCL)
- `ACTIVITY` — Conditions indices (NFCI, CFNAI)
- `FX` — Exchange rates (DTWEXBGS, DEX rates)

---

## 2. Fact Tables by Module

### Module ECON — Economic Dashboard & Macro Indicators

**Purpose:** Latest macro snapshot with prior, change, YoY, z-score, sparkline, and breadth/surprise metrics
**Terminal equivalent:** ECON macro grid view

#### 2.1 gold_macro_indicator_dashboard

**Grain:** 1 row per series (latest observation)
**Cardinality:** 200+ rows (all series in qDimSeries with ECON category)
**Relationships:**
- `series_id` → qDimSeries.series_id
- `latest_date` → qDimDate.date (implicit; optional for slicer)

| Column | Type | Description | Calculation |
|---|---|---|---|
| `series_id` | TEXT | Series ID (FK) | From manifest |
| `title` | TEXT | Series name | Denormalized from qDimSeries |
| `econ_category` | TEXT | Dashboard bucket | From qDimSeries |
| `units` | TEXT | Display unit | From qDimSeries |
| `source` | TEXT | Data origin | From qDimSeries |
| `latest_date` | DATE | Most recent observation date | MAX(obs_date) in Silver |
| `latest_value` | FLOAT | Most recent value | Latest (first observation) |
| `prior_value` | FLOAT | Previous observation value | Previous (second most recent) |
| `prior_date` | DATE | Date of prior observation | Date of previous obs |
| `change_abs` | FLOAT | latest − prior (absolute) | latest_value − prior_value |
| `change_pct` | FLOAT | (latest − prior) / abs(prior) × 100 | Percent change |
| `mom_pct` | FLOAT | Month-over-month % change | From `fred_feature_transforms` |
| `yoy_pct` | FLOAT | Year-over-year % change | From `fred_feature_transforms` |
| `z_score` | FLOAT | **PIT expanding z-score** | (latest − expanding_mean) / expanding_std |
| `percentile` | FLOAT | **PIT percentile** | Rank (latest) within expanding window |
| `surprise` | FLOAT | Latest − trailing-N-period mean | Proxy "surprise" when no consensus |
| `polarity` | INT | +1, −1, or 0 | From qDimSeries |
| `direction_is_good` | BOOL | polarity × sign(change_pct) | True if latest print is bullish |
| `spark_min`, `spark_max`, `spark_last` | FLOAT | Sparkline bounds (last 36 obs) | Min/max/last from sparkline table |
| `staleness_days` | INT | Days since latest_date | TODAY() − latest_date |
| `realtime_start` | DATE | Vintage of this value (ALFRED) | When value entered FRED |

**Key formulas:**
- **Z-score:** Expanding window (not full sample) for point-in-time correctness
- **Percentile:** Rank-based percentile, expanding window
- **Surprise:** Mean of trailing 8 observations (configurable)
- **Polarity adjustment:** change_pct is marked "good" if (change_pct > 0 AND polarity = +1) OR (change_pct < 0 AND polarity = −1)

**Usage in Power BI:**
```dax
Latest Value := SELECTEDVALUE(gold_macro_indicator_dashboard[latest_value])
YoY Pct := SELECTEDVALUE(gold_macro_indicator_dashboard[yoy_pct])
Z-Score := SELECTEDVALUE(gold_macro_indicator_dashboard[z_score])

Surprise Index := AVERAGE(gold_macro_indicator_dashboard[surprise])
Breadth Pct := See gold_macro_category_summary instead
```

**Visual patterns:**
- **Grid card:** latest_value, change_pct, direction_is_good (color)
- **KPI card:** latest_value vs prior_value (as trend)
- **Clustered bar:** yoy_pct by econ_category
- **Gauge:** z_score (−3 to +3 range)

---

#### 2.2 gold_macro_indicator_sparkline

**Grain:** 1 row per series × sparkline point (last 36 observations)
**Cardinality:** ~7,000 rows (200 series × 36 points)
**Relationships:**
- `series_id` → qDimSeries.series_id
- `obs_date` → qDimDate.date

| Column | Type | Description |
|---|---|---|
| `series_id` | TEXT | Series ID (FK) |
| `point_index` | INT | Position 0–35 (oldest to newest) |
| `obs_date` | DATE | Observation date |
| `value` | FLOAT | Value at this point |

**Usage in Power BI:**
- In grid visual with gold_macro_indicator_dashboard
- Create **small-multiple line visual:**
  - Axis: obs_date
  - Values: value
  - Legend: series_id
  - Facet by series_id for one sparkline per row

**Example M query (Power Query):**
```powerquery
let
    Source = Sql.Database("localhost", "fred.db"),
    gold_sparkline = Source{[Schema="", Item="gold_macro_indicator_sparkline"]}[Data]
in
    gold_sparkline
```

---

#### 2.3 gold_macro_category_summary

**Grain:** 1 row per econ_category
**Cardinality:** 11 rows (one per category: GROWTH, INFLATION, LABOR, etc.)
**Relationships:**
- `econ_category` → qDimSeries.econ_category
- `as_of_date` → qDimDate.date

| Column | Type | Description |
|---|---|---|
| `econ_category` | TEXT | Category name (PK) |
| `n_series` | INT | Count of series in category |
| `n_improving` | INT | Count with bullish latest change |
| `breadth_pct` | FLOAT | 100 × n_improving / n_series |
| `avg_z_score` | FLOAT | Mean z-score across category |
| `surprise_index` | FLOAT | Mean surprise across category |
| `as_of_date` | DATE | Snapshot date |

**Usage in Power BI:**
```dax
Breadth % := SELECTEDVALUE(gold_macro_category_summary[breadth_pct])
Avg Z-Score := SELECTEDVALUE(gold_macro_category_summary[avg_z_score])
Surprise Index := SELECTEDVALUE(gold_macro_category_summary[surprise_index])
```

**Visual patterns:**
- **Gauge:** breadth_pct (0–100%)
- **Card:** surprise_index
- **Clustered bar:** breadth_pct by category (sorted)

---

### Module INFL — Inflation Explorer (CPI, PCE Item-Level)

**Purpose:** Inflation decomposition to item level with contribution waterfall
**Terminal equivalent:** INFL module drill-down

#### 2.4 gold_inflation_explorer

**Grain:** 1 row per item × month
**Cardinality:** ~10,000 rows (100 items × 100 months)
**Relationships:**
- `series_id` → qDimSeries.series_id
- `obs_date` → qDimDate.date

| Column | Type | Description |
|---|---|---|
| `series_id` | TEXT | Series ID (FK to item series) |
| `item_label` | TEXT | Item name (e.g., "Food and Beverages") |
| `parent_item` | TEXT | Parent item (e.g., "All items") or NULL if root |
| `hierarchy_level` | INT | Tree depth (0 = headline, 1 = major group, 2 = subgroup) |
| `basket` | TEXT | `CPI` or `PCE` |
| `sa_nsa` | TEXT | `SA` (seasonally adjusted) or `NSA` (not adjusted) |
| `obs_date` | DATE | Month-end date |
| `index_value` | FLOAT | CPI/PCE index level |
| `mom_pct` | FLOAT | Month-over-month % change |
| `yoy_pct` | FLOAT | Year-over-year % change |
| `mom_accel` | FLOAT | ΔMoM: this month's MoM − last month's MoM |
| `yoy_accel` | FLOAT | ΔYoY: this month's YoY − last month's YoY |
| `weight` | FLOAT | Relative importance weight (0–100) |
| `contribution_pp` | FLOAT | weight × mom_pct (approx basis points to headline) |
| `three_month_annualized` | FLOAT | (1 + trailing 3m rate)^4 − 1 |

**Data structure (example):**
```
obs_date    item_label          hierarchy_level    mom_pct    yoy_pct    weight
2026-08-31  All items                0             0.25       2.90      100.0
2026-08-31  Food and Beverages       1             0.30       3.10       13.5
2026-08-31  Food at home             2             0.35       3.20        8.1
2026-08-31  Cereals and bakery       2             0.40       2.95        1.2
2026-08-31  Energy                   1            -0.10       1.50       11.2
```

**Usage in Power BI:**
```dax
YoY % := SELECTEDVALUE(gold_inflation_explorer[yoy_pct])
Contribution pp := SELECTEDVALUE(gold_inflation_explorer[contribution_pp])
Acceleration := SELECTEDVALUE(gold_inflation_explorer[mom_accel])
```

**Visual patterns:**
- **Drill-through:** By basket toggle (CPI/PCE), SA toggle, hierarchy (headline → major → subgroup)
- **Line chart:** yoy_pct by obs_date, colored by item_label
- **Waterfall:** contribution_pp (see gold_inflation_contribution)

---

#### 2.5 gold_inflation_contribution

**Grain:** 1 row per contributing item × month (for waterfall visualization)
**Cardinality:** ~500 rows (50 items × 10 headline months)
**Relationships:**
- `series_id` → qDimSeries.series_id
- `obs_date` → qDimDate.date

| Column | Type | Description |
|---|---|---|
| `obs_date` | DATE | Month-end date |
| `basket` | TEXT | `CPI` or `PCE` |
| `item_label` | TEXT | Item name |
| `contribution_pp` | FLOAT | Contribution basis points to headline MoM |
| `rank_in_month` | INT | Ranking (1 = largest contributor, ascending/descending per sign) |
| `is_headline_total` | BOOL | True for the headline row (total contribution = 0, serves as waterfall anchor) |

**Usage in Power BI:**
```dax
Contribution pp := SELECTEDVALUE(gold_inflation_contribution[contribution_pp])
Rank := SELECTEDVALUE(gold_inflation_contribution[rank_in_month])
```

**Visual patterns:**
- **Waterfall:** contribution_pp by item_label, sorted by rank_in_month
- **Clustered bar:** Top 10 contributors, colored by contribution sign (red/green)

---

### Module CURV — Treasury Curve Lab

**Purpose:** Point-in-time yield curve, metrics (level/slope/curvature), and spread dynamics
**Terminal equivalent:** CURV / YCURV module

#### 2.6 gold_treasury_curve

**Grain:** 1 row per as-of date × tenor
**Cardinality:** ~30,000 rows (11 tenors × 2,500+ trading days)
**Relationships:**
- `series_id` → qDimSeries.series_id (the `DGS*` series)
- `as_of_date` → qDimDate.date

| Column | Type | Description | Example |
|---|---|---|---|
| `as_of_date` | DATE | Curve date | 2026-08-19 |
| `tenor_label` | TEXT | Tenor (PK component) | "10Y", "2Y", "1M", "30Y" |
| `tenor_months` | INT | Numeric tenor for sorting | 120 (10Y), 24 (2Y) |
| `yield` | FLOAT | Yield (%) | 4.15 |
| `series_id` | TEXT | FRED series (FK) | DGS10 |
| `source` | TEXT | Data source | FRED |

**Tenor mapping:**
1M → 1, 3M → 3, 6M → 6, 1Y → 12, 2Y → 24, 3Y → 36, 5Y → 60, 7Y → 84, 10Y → 120, 20Y → 240, 30Y → 360

**Usage in Power BI:**
```dax
Yield % := SELECTEDVALUE(gold_treasury_curve[yield])
Tenor (sorted) := SELECTEDVALUE(gold_treasury_curve[tenor_months])
```

**Visual patterns:**
- **Line chart (curve):** tenor_months (X-axis), yield (Y-axis), one line per as_of_date (animated over time) or grouped by date range
- **Slicer:** as_of_date to pick specific curve date
- **Drill-through:** Click curve point → detail table of spreads for that date

---

#### 2.7 gold_treasury_curve_metrics

**Grain:** 1 row per as-of date
**Cardinality:** ~2,500 rows (one per trading day)
**Relationships:**
- `as_of_date` → qDimDate.date

| Column | Type | Description | Calculation |
|---|---|---|---|
| `as_of_date` | DATE | Curve date (PK) | Observation date |
| `level` | FLOAT | Curve level (mean yield) | Mean of all tenors |
| `slope_10y2y` | FLOAT | 10Y − 2Y yield | Steepness measure |
| `slope_10y3m` | FLOAT | 10Y − 3M yield | Long-term bias |
| `slope_2y3m` | FLOAT | 2Y − 3M yield | Near-term bias |
| `curvature_2_5_10` | FLOAT | 2×5Y − 2Y − 10Y | Humped-ness |
| `butterfly_2_10_30` | FLOAT | 2Y + 30Y − 2×10Y | Butterfly spread |
| `is_inverted_10y2y` | BOOL | slope_10y2y < 0 | Recession signal |
| `is_inverted_10y3m` | BOOL | slope_10y3m < 0 | Alternative inversion signal |
| `is_recession` | BOOL | From `USREC` | Recession flag (for overlay) |
| `curve_move` | TEXT | `bull-steepen`, `bull-flatten`, `bear-steepen`, `bear-flatten` | From Δlevel × Δslope classification |
| `days_since_last_inversion` | INT | Days since last inversion | For trend/momentum |

**Usage in Power BI:**
```dax
10Y-2Y Slope := SELECTEDVALUE(gold_treasury_curve_metrics[slope_10y2y])
Curve Level := SELECTEDVALUE(gold_treasury_curve_metrics[level])
Is Inverted := SELECTEDVALUE(gold_treasury_curve_metrics[is_inverted_10y2y])
```

**Visual patterns:**
- **Area chart:** level over time, colored by `is_recession` for shading
- **Line chart:** slope_10y2y with zero line highlighted
- **Gauge:** Current level / slope
- **Card:** curve_move classification

---

#### 2.8 gold_curve_spread_daily

**Grain:** 1 row per spread × date
**Cardinality:** ~80,000 rows (7+ spreads × 2,500+ trading days)
**Relationships:**
- `obs_date` → qDimDate.date

| Column | Type | Description | Calculation |
|---|---|---|---|
| `spread_name` | TEXT | Spread ID (PK component) | "10Y-2Y", "10Y-3M", "30Y-5Y", etc. |
| `long_leg` | TEXT | Longer tenor | "10Y" for "10Y-2Y" |
| `short_leg` | TEXT | Shorter tenor | "2Y" for "10Y-2Y" |
| `obs_date` | DATE | Observation date (PK component) | Trading date |
| `value_bps` | FLOAT | Spread in basis points | long_yield − short_yield (×100) |
| `z_score` | FLOAT | **PIT expanding z-score** | (value − expanding_mean) / expanding_std |
| `percentile` | FLOAT | **PIT percentile** | Rank within expanding window |
| `is_inverted` | BOOL | value_bps < 0 | True if spread is negative |
| `is_recession` | BOOL | From `USREC` join | True if in recession |
| `days_inverted_run` | INT | Current inversion streak length | Consecutive days where is_inverted = true |

**Usage in Power BI:**
```dax
Spread bps := SELECTEDVALUE(gold_curve_spread_daily[value_bps])
Z-Score := SELECTEDVALUE(gold_curve_spread_daily[z_score])
Is Inverted := SELECTEDVALUE(gold_curve_spread_daily[is_inverted])
Inversion Streak := SELECTEDVALUE(gold_curve_spread_daily[days_inverted_run])
```

**Visual patterns:**
- **Line chart (multi-select spreads):** value_bps by obs_date, one series per spread_name
- **Gauge:** Current z_score for selected spread
- **Area chart:** Shaded red when is_inverted = true
- **Slicer:** spread_name (multi-select)

---

#### 2.9 gold_spread_inversion_episode

**Grain:** 1 row per spread × inversion episode
**Cardinality:** ~30 rows (7 spreads × ~4 episodes in recent history)
**Relationships:**
- `obs_date` → qDimDate.date (implied for filtering)

| Column | Type | Description |
|---|---|---|
| `spread_name` | TEXT | Spread (PK component) | "10Y-2Y" |
| `long_leg`, `short_leg` | TEXT | Tenor legs | "10Y", "2Y" |
| `episode_number` | INT | Episode #1, #2, #3 ... per spread | Chronological sequence |
| `start_date` | DATE | First negative print | 2022-04-15 |
| `end_date` | DATE | First non-negative print after start | NULL if ongoing |
| `last_inverted_date` | DATE | Most recent inverted obs | 2022-06-30 (if ongoing) |
| `observation_count` | INT | # of inverted observations | 45 |
| `calendar_days` | INT | Days from start to end | 76 |
| `trough_value_bps` | FLOAT | Deepest inversion | −150 |
| `trough_date` | DATE | Date of trough | 2022-06-15 |
| `is_ongoing` | BOOL | True if end_date is NULL | true if unresolved |
| `recession_overlap` | BOOL | Any inverted date in recession | true if coincided with NBER |

**Usage in Power BI:**
```dax
Episode Duration (days) := SELECTEDVALUE(gold_spread_inversion_episode[calendar_days])
Trough := SELECTEDVALUE(gold_spread_inversion_episode[trough_value_bps])
Recession Overlap := SELECTEDVALUE(gold_spread_inversion_episode[recession_overlap])
```

**Visual patterns:**
- **Table/Gantt:** spread_name, start_date, end_date, trough_bps, recession_overlap
- **Timeline:** start_date to end_date (or last_inverted_date if ongoing) for each episode
- **Slicer:** Filter by recession_overlap = true to see recessions preceded by inversion

---

#### 2.10 gold_curve_spread_rolling (rolling statistics companion)

**Grain:** 1 row per spread × date × window
**Cardinality:** ~2M rows (7 spreads × 2,500 days × 7 windows)
**Relationships:**
- `obs_date` → qDimDate.date

| Column | Type | Description |
|---|---|---|
| `spread_name` | TEXT | Spread name |
| `obs_date` | DATE | Observation date |
| `window` | INT | Window size in observations (1, 5, 10, 21, 63, 126, 252) |
| `trailing_change_bps` | FLOAT | Change over window |
| `trailing_pct_change` | FLOAT | % change over window |
| `rolling_z_score` | FLOAT | Z-score within window |

**Usage:** Performance analysis, momentum slicing by window size.

---

### Module RATES — Benchmark Rates

**Purpose:** Board of 43+ rates with trend, spread-to-benchmark, and regime
**Terminal equivalent:** BMRK module

#### 2.11 gold_benchmark_rate_board

**Grain:** 1 row per rate (latest observation)
**Cardinality:** 43+ rows
**Relationships:**
- `series_id` → qDimSeries.series_id

| Column | Type | Description | Example |
|---|---|---|---|
| `series_id` | TEXT | Rate series (FK) | SOFR, FEDFUNDS, DGS10 |
| `rate_label` | TEXT | Display name | "SOFR", "Fed Funds Effective Rate" |
| `rate_category` | TEXT | Category (PK component) | `policy`, `repo`, `treasury`, `sofr_complex`, `credit`, `mortgage`, `other` |
| `latest_value` | FLOAT | Most recent rate | 5.35 |
| `prior_value` | FLOAT | Previous observation | 5.33 |
| `change_bps` | FLOAT | Change (basis points) | 2 |
| `trend` | TEXT | `rising`, `falling`, `flat` | Directional slope over short window |
| `benchmark_series` | TEXT | Benchmark to compare against | e.g., SOFR for comparing SOFR spreads |
| `spread_to_benchmark_bps` | FLOAT | latest − benchmark | 50 |
| `z_score` | FLOAT | **PIT expanding z-score** | 0.75 |
| `regime` | TEXT | `tightening`, `easing`, `stable` | From level + trend rules |

**Usage in Power BI:**
```dax
Latest Rate := SELECTEDVALUE(gold_benchmark_rate_board[latest_value])
Change bps := SELECTEDVALUE(gold_benchmark_rate_board[change_bps])
Spread to Benchmark := SELECTEDVALUE(gold_benchmark_rate_board[spread_to_benchmark_bps])
```

**Visual patterns:**
- **Table/Matrix:** Rates grouped by rate_category, columns for latest, change_bps, z_score, trend, regime
- **Card:** Latest value per rate
- **Gauge:** Z-score for selected rate

---

### Module FUND — Funding Tape (Fed Corridor, Balances, Spreads)

**Purpose:** Daily funding-market snapshot with stress gauge
**Terminal equivalent:** FUND module

#### 2.12 gold_funding_tape_daily

**Grain:** 1 row per metric × date
**Cardinality:** ~50,000 rows (20+ metrics × 2,500+ days)
**Relationships:**
- `obs_date` → qDimDate.date

| Column | Type | Description |
|---|---|---|
| `obs_date` | DATE | Observation date |
| `metric_name` | TEXT | Metric (e.g., "SOFR", "Fed balance sheet", "SOFR-EFFR spread") |
| `metric_type` | TEXT | `rate`, `balance`, `spread` |
| `value` | FLOAT | Metric value (rate in %, balance in $B, spread in bps) |
| `z_score` | FLOAT | **PIT expanding z-score** |
| `percentile` | FLOAT | **PIT percentile** |
| `unit` | TEXT | Display unit ("bps", "$B", "%") |

**Usage in Power BI:**
```dax
Funding Metric := SELECTEDVALUE(gold_funding_tape_daily[value])
Z-Score := SELECTEDVALUE(gold_funding_tape_daily[z_score])
```

**Visual patterns:**
- **Line chart (multi-select metrics):** value by obs_date, faceted by metric_type
- **Slicer:** metric_type (rate, balance, spread)

---

#### 2.13 gold_funding_stress_daily

**Grain:** 1 row per date
**Cardinality:** ~2,500 rows
**Relationships:**
- `obs_date` → qDimDate.date

| Column | Type | Description |
|---|---|---|
| `obs_date` | DATE | Observation date (PK) |
| `sofr_effr_bps` | FLOAT | SOFR − EFFR spread (bps) |
| `sofr_iorb_bps` | FLOAT | SOFR − IORB spread (bps) |
| `bill_ois_bps` | FLOAT | Bill − OIS spread (bps) |
| `stress_score` | INT | Composite stress (0–100) |
| `stress_bucket` | TEXT | `calm` (0–25), `normal` (25–50), `elevated` (50–75), `stressed` (75–100) |

**Formula:**
```
stress_score = clamp(50 + 20 × Σ(weight_i × z_score_i) / Σ(weight_i), 0, 100)
```

**Usage in Power BI:**
```dax
Stress Score := SELECTEDVALUE(gold_funding_stress_daily[stress_score])
Stress Level := SELECTEDVALUE(gold_funding_stress_daily[stress_bucket])
```

**Visual patterns:**
- **Gauge:** stress_score (0–100 scale, red/yellow/green by bucket)
- **Area chart:** stress_score over time, colored by stress_bucket
- **Card:** Current stress_bucket with trend

---

### Module CRDT — Credit Spreads (OAS, Rating Curves, Sectors)

**Purpose:** IG/HY spreads with valuation percentiles and stress episodes
**Terminal equivalent:** CRDT module

#### 2.14 gold_credit_spread_daily

**Grain:** 1 row per instrument × date
**Cardinality:** ~50,000 rows (20 instruments × 2,500+ days)
**Relationships:**
- `series_id` → qDimSeries.series_id
- `obs_date` → qDimDate.date

| Column | Type | Description |
|---|---|---|
| `obs_date` | DATE | Observation date |
| `instrument` | TEXT | Spread type (IG_OAS, HY_OAS, sector spreads, rating curves) |
| `series_id` | TEXT | FRED series (FK) |
| `oas_bps` | FLOAT | OAS in basis points |
| `change_bps` | FLOAT | Day-over-day change (bps) |
| `z_score` | FLOAT | **PIT expanding z-score** |
| `percentile` | FLOAT | **PIT percentile** (valuation) |
| `is_stress_episode` | BOOL | True if percentile > stress threshold (typically 75) |
| `is_recession` | BOOL | From `USREC` |

**Usage in Power BI:**
```dax
OAS (bps) := SELECTEDVALUE(gold_credit_spread_daily[oas_bps])
Valuation (percentile) := SELECTEDVALUE(gold_credit_spread_daily[percentile])
Stress Episode := SELECTEDVALUE(gold_credit_spread_daily[is_stress_episode])
```

**Visual patterns:**
- **Line chart:** oas_bps by obs_date, one series per instrument
- **Area shaded:** is_stress_episode = true (background red)
- **Gauge:** Current z_score for IG/HY
- **Slicer:** instrument (multi-select)

---

#### 2.15 gold_credit_spread_rolling

**Grain:** 1 row per instrument × date × window
**Cardinality:** ~300K rows
**Relationships:**
- `obs_date` → qDimDate.date

| Column | Type | Description |
|---|---|---|
| `instrument` | TEXT | Credit instrument |
| `obs_date` | DATE | Date |
| `window` | INT | Window size (1, 5, 10, 21, 63, 126, 252 obs) |
| `trailing_change_bps` | FLOAT | Change in bps over window |
| `rolling_z_score` | FLOAT | Z-score within window |

**Usage:** Momentum and mean-reversion analysis.

---

### Module REGIME — Macro Regime Playbook

**Purpose:** Daily regime score (5 pillars + composite) and named regime
**Terminal equivalent:** REGIME module

#### 2.16 gold_macro_regime_daily

**Grain:** 1 row per date
**Cardinality:** ~2,500 rows
**Relationships:**
- `obs_date` → qDimDate.date

| Column | Type | Description | Example |
|---|---|---|---|
| `obs_date` | DATE | Observation date (PK) | 2026-08-19 |
| `growth_score` | FLOAT | Growth pillar z-score | 1.2 |
| `inflation_score` | FLOAT | Inflation pillar z-score | −0.8 |
| `liquidity_score` | FLOAT | Liquidity pillar z-score (from NFCI) | 0.5 |
| `credit_score` | FLOAT | Credit pillar z-score (from credit spreads) | 0.1 |
| `policy_score` | FLOAT | Policy pillar z-score (rates, Fed stance) | −1.5 |
| `composite_score` | FLOAT | Weighted blend of 5 pillars | 0.0 |
| `regime_name` | TEXT | Named regime (PK component) | `Goldilocks`, `Reflation`, `Stagflation`, `Growth-Scare`, `Liquidity-Squeeze`, `Policy-Easing`, `Neutral` |
| `regime_confidence` | FLOAT | How clearly the regime matches (0–1) | 0.85 |
| `is_recession` | BOOL | From `USREC` | false |

**Pillar definitions (from config):**
- **Growth:** GDP growth, unemployment, jobless claims (direction: up good)
- **Inflation:** CPI/PCE YoY, breakevens (direction: stable good)
- **Liquidity:** NFCI, ANFCI, money market spreads (direction: low good)
- **Credit:** Credit spreads (direction: tight good)
- **Policy:** Fed funds, curve slope (direction: accommodative good)

**Regime mapping (example rule table):**
| Growth | Inflation | Liquidity | Credit | Policy | → Regime |
|---|---|---|---|---|---|
| +1 | 0 | +1 | +1 | +1 | Goldilocks |
| +1 | +1 | +1 | +1 | +1 | Reflation |
| −1 | +1 | −1 | −1 | −1 | Stagflation |
| −1 | 0 | −1 | 0 | +1 | Growth-Scare |
| 0 | 0 | −1 | −1 | −1 | Liquidity-Squeeze |

**Usage in Power BI:**
```dax
Growth Score := SELECTEDVALUE(gold_macro_regime_daily[growth_score])
Current Regime := SELECTEDVALUE(gold_macro_regime_daily[regime_name])
Regime Confidence := SELECTEDVALUE(gold_macro_regime_daily[regime_confidence])
```

**Visual patterns:**
- **Card:** Current regime_name
- **Gauge:** regime_confidence (0–1)
- **Clustered bar:** 5 pillar scores (side-by-side)
- **Timeline:** regime_name changes over time (color by regime)
- **Stacked area:** 5 pillar scores stacked, color by sign

---

### Module STAT / EDA — Statistical Analysis (Correlation, Lead-Lag)

**Purpose:** Curated correlation matrix and Granger causality (8 pre-selected pairs)
**Terminal equivalent:** STAT and EDA modules

#### 2.17 gold_series_correlation

**Grain:** 1 row per pair × window × as-of date
**Cardinality:** ~50,000 rows (8 pairs × 3 windows × 2,000+ as-of dates)
**Relationships:**
- `series_a`, `series_b` → qDimSeries.series_id
- `as_of_date` → qDimDate.date

| Column | Type | Description |
|---|---|---|
| `series_a`, `series_b` | TEXT | Series IDs (FKs) |
| `window` | INT | Window size (63, 252, or `expanding`) |
| `as_of_date` | DATE | Observation date |
| `correlation` | FLOAT | Pearson correlation (−1 to +1) |
| `n_obs` | INT | # observations in window |

**Example pairs (curated):**
- Growth vs. Inflation (GDP vs. CPI)
- Inflation vs. Rates (CPI vs. 10Y yield)
- Growth vs. Credit (GDP vs. credit spreads)
- Rates vs. Equity (implied vol vs. 10Y)
- (etc., configured in `config/stats_pairs.yml`)

**Usage in Power BI:**
```dax
Correlation := SELECTEDVALUE(gold_series_correlation[correlation])
Window := SELECTEDVALUE(gold_series_correlation[window])
```

**Visual patterns:**
- **Heatmap matrix:** series_a (rows) × series_b (columns), filled by correlation, sliced by window
- **Line chart:** correlation over time for selected pair, faceted by window
- **Gauge:** Current correlation for pair/window

---

#### 2.18 gold_series_lead_lag

**Grain:** 1 row per pair × lag
**Cardinality:** ~200 rows (8 pairs × ±12 lags)
**Relationships:**
- `series_a`, `series_b` → qDimSeries.series_id
- `as_of_date` → qDimDate.date

| Column | Type | Description |
|---|---|---|
| `series_a`, `series_b` | TEXT | Series IDs (FKs) |
| `lag` | INT | Lag (−12 to +12) — negative = series_a leads |
| `cross_correlation` | FLOAT | CCF at this lag |
| `granger_f` | FLOAT | Granger F-statistic |
| `granger_p` | FLOAT | Granger p-value (< 0.05 = significant) |
| `best_lag` | BOOL | True if this lag has max CCF |
| `as_of_date` | DATE | Latest data date |

**Usage in Power BI:**
```dax
CCF := SELECTEDVALUE(gold_series_lead_lag[cross_correlation])
Granger Significance := SELECTEDVALUE(gold_series_lead_lag[granger_p])
Best Lag := SELECTEDVALUE(gold_series_lead_lag[best_lag])
```

**Visual patterns:**
- **Spike chart:** lag (X-axis) vs. cross_correlation (Y-axis), spike at best_lag
- **Table:** series_a → series_b, lag, granger_f, granger_p (sorted by p-value)
- **Slicer:** series_a, series_b (pair selector)

---

### Module GCPI / GPOL — Global Inflation & Policy Rates

**Purpose:** Multi-country inflation and policy rates (optional, lower priority)
**Terminal equivalent:** GCPI and GPOL modules

#### 2.19 gold_global_inflation

**Grain:** 1 row per country × date
**Cardinality:** ~10,000 rows (12–40 countries × 100+ months)
**Relationships:**
- `obs_date` → qDimDate.date

| Column | Type | Description |
|---|---|---|
| `obs_date` | DATE | Observation date |
| `country_code` | TEXT | ISO 3166-1 alpha-3 (e.g., "USA", "GBR", "JPN") |
| `country_name` | TEXT | Full name (e.g., "United States", "United Kingdom") |
| `region` | TEXT | `AMER`, `EMEA`, `APAC` |
| `cpi_yoy_pct` | FLOAT | CPI year-over-year % |
| `cpi_change` | FLOAT | Most recent MoM or obs change |
| `trend` | TEXT | `accelerating`, `cooling`, `flat` |
| `consecutive_prints` | INT | Streak of consecutive prints in same direction |
| `vs_target_gap` | FLOAT | yoy − central bank target (pp) |
| `target_pct` | FLOAT | Central bank target (e.g., 2.0) |

**Usage in Power BI:**
```dax
CPI YoY := SELECTEDVALUE(gold_global_inflation[cpi_yoy_pct])
vs Target := SELECTEDVALUE(gold_global_inflation[vs_target_gap])
Trend := SELECTEDVALUE(gold_global_inflation[trend])
```

**Visual patterns:**
- **Map:** country_code, sized/colored by cpi_yoy_pct
- **Clustered bar:** Countries sorted by cpi_yoy_pct
- **Heatmap:** Countries (rows) × months (columns), colored by trend
- **Slicer:** region (AMER, EMEA, APAC)

---

#### 2.20 gold_global_policy_rates

**Grain:** 1 row per country × date
**Cardinality:** ~10,000 rows
**Relationships:**
- `obs_date` → qDimDate.date

| Column | Type | Description |
|---|---|---|
| `obs_date` | DATE | Observation date |
| `country_code` | TEXT | ISO 3166-1 alpha-3 |
| `country_name` | TEXT | Full name |
| `central_bank` | TEXT | Central bank name (e.g., "ECB", "BOJ") |
| `policy_rate_pct` | FLOAT | Policy rate (%) |
| `change_bps` | FLOAT | Change vs. prior observation (bps) |
| `last_move_stance` | TEXT | `tightening`, `easing`, `hold` |
| `ex_post_real_rate` | FLOAT | policy_rate − trailing CPI (inflation-adjusted) |

**Usage in Power BI:**
```dax
Policy Rate := SELECTEDVALUE(gold_global_policy_rates[policy_rate_pct])
Stance := SELECTEDVALUE(gold_global_policy_rates[last_move_stance])
Real Rate := SELECTEDVALUE(gold_global_policy_rates[ex_post_real_rate])
```

**Visual patterns:**
- **Line chart:** policy_rate_pct by obs_date, one series per country
- **Gauge:** Current rate + ex_post_real_rate
- **Table:** Countries sorted by real_rate (ascending/descending)

---

### Supporting Tables

#### 2.21 gold_powerbi_catalog

**Grain:** 1 row per Gold table
**Cardinality:** 46+ rows
**Purpose:** Data dictionary and table manifest for report authors

| Column | Type | Description |
|---|---|---|
| `table_name` | TEXT | Table name (e.g., "gold_macro_indicator_dashboard") |
| `table_type` | TEXT | `dimension`, `fact_snapshot`, `fact_long`, `fact_episodic` |
| `terminal_module` | TEXT | Module name (ECON, INFL, CURV, etc.) |
| `grain` | TEXT | Grain description (e.g., "1 / series (latest)") |
| `intended_visual` | TEXT | Recommended visual type(s) |
| `description` | TEXT | Purpose and usage notes |
| `key_columns` | TEXT | Primary/foreign keys |
| `row_count_approx` | INT | Approximate row count |

**Usage in Power BI:**
- Create slicer from this table
- Filter to see available tables for a module
- Copy grain/key_columns for data model documentation

---

## 3. Star Schema Relationships

### Relationship Matrix

| Fact Table | Dimension | Relationship | Cardinality | Filter |
|---|---|---|---|---|
| gold_macro_indicator_dashboard | qDimSeries | series_id → series_id | N:1 | Always active |
| gold_macro_indicator_dashboard | qDimDate | latest_date → date | N:1 | Optional |
| gold_macro_indicator_sparkline | qDimSeries | series_id → series_id | N:1 | Always active |
| gold_macro_indicator_sparkline | qDimDate | obs_date → date | N:1 | Always active |
| gold_inflation_explorer | qDimSeries | series_id → series_id | N:1 | Always active |
| gold_inflation_explorer | qDimDate | obs_date → date | N:1 | Always active |
| gold_treasury_curve | qDimSeries | series_id → series_id | N:1 | Always active |
| gold_treasury_curve | qDimDate | as_of_date → date | N:1 | Always active |
| gold_treasury_curve_metrics | qDimDate | as_of_date → date | N:1 | Always active |
| gold_curve_spread_daily | qDimDate | obs_date → date | N:1 | Always active |
| gold_funding_tape_daily | qDimDate | obs_date → date | N:1 | Always active |
| gold_funding_stress_daily | qDimDate | obs_date → date | N:1 | Always active |
| gold_credit_spread_daily | qDimSeries | series_id → series_id | N:1 | Always active |
| gold_credit_spread_daily | qDimDate | obs_date → date | N:1 | Always active |
| gold_macro_regime_daily | qDimDate | obs_date → date | N:1 | Always active |
| gold_series_correlation | qDimSeries | series_a → series_id | N:1 | Bidirectional |
| gold_series_correlation | qDimSeries | series_b → series_id | N:1 | Bidirectional |
| gold_series_correlation | qDimDate | as_of_date → date | N:1 | Always active |
| gold_series_lead_lag | qDimSeries | series_a → series_id | N:1 | Bidirectional |
| gold_series_lead_lag | qDimSeries | series_b → series_id | N:1 | Bidirectional |
| gold_series_lead_lag | qDimDate | as_of_date → date | N:1 | Always active |

**Relationship rules:**
- All fact-to-dimension relationships are **N:1** (many facts per dimension member)
- Date relationships: set **one-to-many** with filter direction from qDimDate → fact
- Series relationships: set **one-to-many** with filter direction from qDimSeries → fact
- Use **bidirectional** filter only for correlation pairs (series_a ↔ series_b)
- Mark qDimDate as **Date Table** for automatic time hierarchy

---

## 4. Common DAX Measure Patterns

### Time Intelligence
```dax
Latest Value :=
  SELECTEDVALUE(gold_macro_indicator_dashboard[latest_value])

Prior Value :=
  SELECTEDVALUE(gold_macro_indicator_dashboard[prior_value])

YoY Change % :=
  SELECTEDVALUE(gold_macro_indicator_dashboard[yoy_pct])
```

### Aggregations (trending over time)
```dax
Average Latest :=
  AVERAGE(gold_macro_indicator_dashboard[latest_value])

Max Z-Score :=
  MAX(gold_macro_indicator_dashboard[z_score])

Breadth % :=
  AVERAGE(gold_macro_category_summary[breadth_pct])
```

### Conditional Logic
```dax
Is Bullish :=
  SELECTEDVALUE(gold_macro_indicator_dashboard[direction_is_good])

Stress Level :=
  IF(
    SELECTEDVALUE(gold_funding_stress_daily[stress_score]) > 75,
    "Stressed",
    IF(
      SELECTEDVALUE(gold_funding_stress_daily[stress_score]) > 50,
      "Elevated",
      "Normal"
    )
  )
```

### Multi-Table Aggregations
```dax
Average Correlation :=
  AVERAGE(gold_series_correlation[correlation])

% Positive Correlation :=
  DIVIDE(
    COUNTIF(gold_series_correlation[correlation], ">0"),
    COUNTA(gold_series_correlation[correlation])
  )
```

---

## 5. Recommended Report Patterns

### Dashboard Patterns

| Pattern | Tables | Visuals | Use Case |
|---|---|---|---|
| **Macro Snapshot** | gold_macro_indicator_dashboard, qDimSeries | Cards (latest + change), Sparklines, KPI gauge | Daily executive view |
| **Inflation Deep Dive** | gold_inflation_explorer, gold_inflation_contribution | Drill-through (headline → item), Waterfall, Heatmap | Monthly inflation analysis |
| **Curve Monitor** | gold_treasury_curve, gold_treasury_curve_metrics, gold_curve_spread_daily | Line (curve), Gauge (slope/level), Area (spreads) | Daily rates desk |
| **Regime Dashboard** | gold_macro_regime_daily, qDimDate | Card (regime name), Stacked bar (pillars), Timeline (regimes) | Macro positioning |
| **Funding Stress** | gold_funding_stress_daily, gold_funding_tape_daily | Gauge (stress score), Area (stress over time), Table (components) | Risk monitoring |
| **Correlation Matrix** | gold_series_correlation, qDimSeries | Heatmap (pairs × corr), Slicer (window) | Diversification analysis |

### Visual Selection by Data Shape

| Data Shape | Visual Type | Example |
|---|---|---|
| **Wide snapshot** (1 row per entity, many columns) | Card, KPI, Gauge, Clustered bar | gold_macro_indicator_dashboard latest values |
| **Long by series** (many rows, 1–2 metrics per) | Line chart, Area chart, Combo | gold_curve_spread_daily, gold_funding_tape_daily |
| **Long by series × time × dimension** | Stacked bar, Ribbon, Decomposition tree | gold_macro_regime_daily (pillars over time) |
| **Sparse (few rows per date)** | Table, Heatmap, Waterfall | gold_inflation_contribution (top contributors) |
| **Matrix / pair data** | Heatmap, Scatter, Bubble | gold_series_correlation (pair × window) |

---

## 6. Filtering and Slicing Patterns

### Recommended Slicers

| Slicer Source | Field | Type | Breadth | Use |
|---|---|---|---|---|
| qDimSeries | econ_category | Dropdown | 11 categories | Filter all fact tables by macro category |
| qDimDate | year | Slicer | 100 years | Time filtering |
| qDimDate | is_recession | Toggle (on/off) | 2 states | Show/hide recession shading |
| gold_treasury_curve | tenor_label | Multi-select | 11 tenors | Curve visualization |
| gold_curve_spread_daily | spread_name | Multi-select | 7+ spreads | Spread comparison |
| gold_funding_tape_daily | metric_type | Dropdown | 3 types | Funding deep dive |
| gold_macro_regime_daily | regime_name | Dropdown | 7 regimes | Regime filter |

### Slicer Interaction Rules

**Set "Both" for these two slicer → fact relationships:**
- qDimSeries econ_category → gold_macro_indicator_dashboard series_id (filters the fact)
- qDimDate → all gold_* tables on date columns (filters time range)

**Why:** One slicer needs to propagate to many fact tables and across multiple date columns.

---

## 7. Reference: Complete Table Inventory

| # | Gold Table | Grain | Type | Module | Row Count (approx) |
|---|---|---|---|---|---|
| D0 | gold_dim_series | 1 / series | Dimension | All | 300+ |
| D1 | gold_dim_date | 1 / date | Dimension | All | 36,500 |
| 1 | gold_macro_indicator_dashboard | 1 / series (latest) | Fact | ECON | 200+ |
| 1b | gold_macro_indicator_sparkline | 1 / series × point | Fact | ECON | 7,000 |
| 1c | gold_macro_category_summary | 1 / category | Fact | ECON | 11 |
| 2 | gold_inflation_explorer | 1 / item × month | Fact | INFL | 10,000 |
| 2b | gold_inflation_contribution | 1 / item × month (waterfall) | Fact | INFL | 500 |
| 3 | gold_treasury_curve | 1 / date × tenor | Fact | CURV | 30,000 |
| 3b | gold_treasury_curve_metrics | 1 / date | Fact | CURV | 2,500 |
| 4 | gold_curve_spread_daily | 1 / spread × date | Fact | CURV | 80,000 |
| 4b | gold_spread_inversion_episode | 1 / spread × episode | Fact | CURV | 30 |
| 4c | gold_curve_spread_rolling | 1 / spread × date × window | Fact | CURV | 2M |
| 5 | gold_benchmark_rate_board | 1 / rate (latest) | Fact | BMRK | 43+ |
| 6 | gold_funding_tape_daily | 1 / metric × date | Fact | FUND | 50,000 |
| 6b | gold_funding_stress_daily | 1 / date | Fact | FUND | 2,500 |
| 7 | gold_credit_spread_daily | 1 / instrument × date | Fact | CRDT | 50,000 |
| 7c | gold_credit_spread_rolling | 1 / instr × date × window | Fact | CRDT | 300K |
| 8 | gold_macro_regime_daily | 1 / date | Fact | REGIME | 2,500 |
| 9 | gold_series_correlation | 1 / pair × window × date | Fact | STAT | 50,000 |
| 10 | gold_series_lead_lag | 1 / pair × lag | Fact | EDA | 200 |
| 11 | gold_global_inflation | 1 / country × date | Fact | GCPI | 10,000 |
| 12 | gold_global_policy_rates | 1 / country × date | Fact | GPOL | 10,000 |
| ref | gold_powerbi_catalog | 1 / table | Reference | All | 46+ |

---

## 8. Data Quality & Staleness Handling

### Staleness Indicators

**Every fact table carries these columns for Power BI badges:**

| Column | Type | Purpose | Example |
|---|---|---|---|
| `staleness_days` | INT | Days since latest_date | 2 (data from day before yesterday) |
| `realtime_start` | DATE | ALFRED vintage (when entered FRED) | 2026-08-15 (advance, preliminary, final) |
| `source` | TEXT | Data origin | FRED, Tiingo, World Bank |

**Power BI conditional formatting:**
```dax
Staleness Color :=
  IF([staleness_days] <= 1, "Green",
  IF([staleness_days] <= 7, "Yellow", "Red"))
```

### Missing Data Handling

- **NULL in metrics:** Indicates gap in observations (e.g., holiday, missing series data)
- **Expanding windows:** Skip rows where insufficient data (e.g., z_score = NULL if < 2 obs)
- **Recession flag:** NULL until `USREC` series is activated; assume false for filtering

---

## 9. Performance Tuning Tips

### Query Optimization

- **Import mode:** Recommended for all Gold tables (< 100K rows each, ~500K total)
- **DirectQuery:** Not needed unless real-time updates required (pipeline runs nightly)
- **Aggregations:** Pre-aggregate annual/quarterly data if drill-down not needed

### Relationship Management

- **Cardinality:** Mark all dimension relationships as `One-to-Many` (one series_id has many fact rows)
- **Cross-filter:** Set "Both" only for qDimSeries ↔ correlation pairs; otherwise "Single" (dimension → fact)
- **Hide columns:** Hide technical keys (series_id, obs_date in facts) if dimensions provide natural labels

### Measure Optimization

- Use `SELECTEDVALUE()` for snapshot retrieval (fast)
- Use `CALCULATE()` only when filtering context changes
- Avoid repeated window calculations in measures; pre-compute in SQL

---

## 10. Next Steps for Report Builders

1. **Connect Power BI to SQLite:**
   ```
   Get Data → SQLite Database → fred.db
   ```

2. **Import all gold_* and qDim* tables** into Power BI data model

3. **Set up relationships:**
   - Every gold_* table → qDimDate (on date columns)
   - Every gold_* table with series_id → qDimSeries

4. **Mark qDimDate as Date Table:**
   - Modeling → Mark as Date Table
   - Confirm date field is `date`

5. **Build first report using a template pattern** (see §5)

6. **Add slicers** for econ_category, year, recession toggle

7. **Test drill-through** (e.g., category summary → series detail)

8. **Create .pbix template** with Wave 0 kernel (if not yet done)

9. **Publish to Power BI Service** and share with stakeholders

---

## Reference

- **Schema spec:** This document
- **Power BI Wave 0 kernel:** `docs/handoffs/powerbi_wave_0_kernel.md`
- **Report 14 (Health):** `docs/handoffs/powerbi_report_14_health.md`
- **Connection setup:** `docs/handoffs/powerbi_database_connections.md`
- **Gold table specs (detailed):** `docs/handoffs/completed/market_terminal_gold_views.md` (§3–4)

---

**Last Updated:** 2026-08-19
**Status:** Complete and tested
**All 46 Gold tables ready for Power BI consumption**
