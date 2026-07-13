# Market Terminal → Gold Layer: Analytical Views Plan & Handoff

**Purpose.** Recreate the economic-analysis surfaces of the `market_terminal`
project (the "QIT / Bloomberg-style" quant terminal) as **Gold-layer tables and
views** inside this pipeline, shaped for **Power BI** consumption. The terminal
computes its macro analytics on the fly in TypeScript from a 166-series FRED
catalog; this document specifies how to precompute the *same* analytics in the
medallion Gold layer so a Power BI model can render equivalent dashboards
(unemployment, inflation, curve, credit, funding, regime, …) directly off Delta
/ SQLite, with point-in-time correctness the terminal does not have.

This is a **plan + handoff**, not code. It maps each terminal module to concrete
Gold objects, names the series each needs (flagging what is not yet in the
manifests), and prescribes the engineering pattern (config-driven, one Python
engine shared by both backends) already established in this repo.

> Scope note. The terminal has 45 modules. This plan covers only the
> **economic / macro** modules and the macro-adjacent analytics. Securities-
> finance, internal-book, news/NLP, and options/vol-*trading* modules are out of
> scope. FOMC rate-probability and ML modules are noted but deferred (they
> depend on external CME/model pipelines, not FRED Gold).

---

## 1. Design principles (carried over from the existing pipeline)

These are the constraints every object below must satisfy. They are the same
principles that govern the current Gold layer — this plan does **not** introduce
a new architecture, it extends the existing one.

1. **One Python engine, two backends.** Every metric is computed by a pure
   function in `src/fred_pipeline/features.py` (dict-in → dict-out, no Spark, no
   SQLite). The local SQLite path calls it directly on rows it queried; the
   Spark path reads the relevant Silver slice into Python, computes with the same
   function, and overwrites the Gold Delta table. No metric is expressed as
   unverifiable Spark SQL. This is how `compute_curve_spreads`,
   `compute_cross_series_features`, `compute_revision_stats`, etc. already work.

2. **Config-driven, not hard-coded.** New analytics are declared in YAML under
   `config/` and consumed by the engine — the way `spreads.yml`,
   `cross_series.yml`, `reconciliations.yml`, `sec_concepts.yml`, and
   `sec_ratios.yml` already work. Adding a spread, a regime rule, or a dashboard
   category is a config edit, not a code change.

3. **Point-in-time safe.** Anything used for backtesting or "what did we know on
   date X" must resolve vintages through `realtime_start` / `realtime_end`
   (ALFRED semantics already implemented). Rolling stats (z-scores, percentiles)
   must expand only over data available *as of* each point — reuse the
   `compute_cross_series_features_pit` / expanding-window pattern, never a
   full-sample `mean`/`std`. The terminal itself is **not** point-in-time; this
   is a correctness upgrade we get for free.

4. **Power BI-friendly shaping.** Fact tables are **tidy/long** (one row per
   series × date × metric, or per series × date with a fixed metric column set),
   joined to small **dimension** tables (`dim_series`, `dim_date`). Avoid arrays
   in columns where a companion long table serves Power BI better (e.g.
   sparklines). Denormalize labels the terminal shows (category, polarity,
   units, provenance) onto the fact or dimension so a report needs no lookups.

5. **Provenance + staleness travel with the data.** The terminal renders a
   `SourceBadge` (FRED / SNAPSHOT / SIM) and a staleness indicator on every
   value, and does "worst-source aggregation" for composites. Carry `source`,
   `realtime_start`, and a computed `staleness_days` on Gold rows so Power BI can
   reproduce the badges and the "as-of freshness" callouts.

---

## 2. Terminal modules in scope, and the analytics each renders

Distilled from the `market_terminal` README. "Analytics" = the *unique* computed
views the module shows; "series" = the FRED/BLS inputs.

| Terminal module | What it renders (unique analytics) | Primary inputs |
|---|---|---|
| **ECON** — Macro Dashboard | Per-series latest / prior / Δ / YoY / *surprise* / 36-pt sparkline / bullish-vs-bearish polarity; grouped into 10 `EconCategory` buckets; category **breadth** (% improving) and a **surprise index**; 24-month drill. | Whole 166-series catalog |
| **INFL** — Inflation Explorer | CPI, Core CPI, PCE, Core PCE **down to item level**: index, MoM %, YoY %, **ΔMoM / ΔYoY acceleration**, **contribution waterfall** (weight × change → headline); CPI⇄PCE basket toggle; 24-month drill. | CPI basket (CUUR/CUSR…), PCE + PCE items |
| **GCPI** — Global Inflation | CPI YoY / MoM by country; trend-vs-prior; **consecutive-print streaks**; vs-target gap; heat map; AMER / EMEA / APAC regions. | World Bank / OECD CPI by country |
| **CURV / YCURV** — Treasury Curve Lab | Point-in-time curve from `DGS1MO…DGS30` **daily** history; **level / slope / curvature**; user-selectable **spreads** (10Y-2Y, 10Y-3M, 30Y-5Y, 10Y-1Y, 5Y-2Y, 2Y-3M, 30Y-10Y); **inversion** episodes cross-referenced with `USREC`; **butterflies**; spread **z-scores**; carry/roll; **curve-move classification** (bull/bear × steepen/flatten). | `DGS*` daily, `USREC` |
| **BMRK** — Benchmark Rates | 43-rate board across 7 categories; per-rate trend, spread-to-benchmark, **cross-rate correlation**, **regime classification**. | Policy/market rate set |
| **FUND** — Funding Tape | Corridor (`IORB / EFFR / OBFR / SOFR / BGCR / TGCR`); balances (`RRPONTSYD / WRESBAL / WALCL`); spreads (`SOFR−EFFR`, `SOFR−IORB`, `GC−OIS`, `bill−OIS`, `FRA−OIS`); **0–100 funding-stress gauge**. | Funding rate + Fed balance-sheet series |
| **FCOST** — Funding Cost | Blended funding cost decomposition across the corridor. | Funding series |
| **CRDT** — Credit Spreads | IG / HY **OAS** levels & changes; credit curve by rating; sector spreads; **valuation percentiles**; **stress-episode** flags. | ICE BofA OAS (`BAMLH0A0HYM2`, `BAMLC0A0CM`, …) |
| **REGIME** — Macro Regime Playbook | Growth / inflation / liquidity / credit / policy **scoring**; **named regime** (Goldilocks / Reflation / Stagflation / Growth-Scare / Liquidity-Squeeze / Policy-Easing). | `DGS10/DGS2/DGS3MO`, `BAMLH0A0HYM2`, `SOFR/EFFR`, `CPILFESL`, … |
| **STAT** — Statistical Analysis | Correlation matrix; Granger causality (F-test); OLS; ADF stationarity; **rolling correlation**; ACF; distribution moments. | Any pair/set |
| **EDA** — Lead/Lag | Cross-correlation (CCF), lagged OLS, Granger, CUSUM/PELT change-points. | Any pair |
| **GPOL** — Global Policy Rates | Policy-rate levels & changes by country/region. | Global central-bank policy rates |
| **RVOL** — Rate Vol | Realized-vol surface, vol regimes, cones, vol-of-vol on rates. | `DGS*` daily |
| **EML** — ML | Recession probit (AUC 0.89), inflation nowcast, BVAR+LSTM, HMM. | Composite (deferred) |

The **unemployment / labor** analytics the user called out are not a standalone
terminal module — they live inside **ECON** (LABOR `EconCategory`: `UNRATE`,
`PAYEMS`, `ICSA`, `CCSA`, `U6RATE`, participation, JOLTS…). They are handled by
the ECON dashboard objects (§4.1) plus the standard transform table.

---

## 3. Target Gold objects (overview)

Twelve new Gold objects (tables `gold.*`, plus SQLite mirrors and Power BI
views). Everything is additive — no existing Gold table changes shape.

| # | Gold object | Grain | Serves | Type |
|---|---|---|---|---|
| D0 | `gold.dim_series` | 1 / series | all (star-schema hub) | dimension |
| D1 | `gold.dim_date` | 1 / calendar date | all | dimension |
| 1 | `gold.macro_indicator_dashboard` | 1 / series (latest) | ECON | fact (snapshot) |
| 1b | `gold.macro_indicator_sparkline` | 1 / series × point | ECON sparklines | fact (long) |
| 1c | `gold.macro_category_summary` | 1 / EconCategory | ECON breadth/surprise | fact (snapshot) |
| 2 | `gold.inflation_explorer` | 1 / item × month | INFL | fact (long) |
| 2b | `gold.inflation_contribution` | 1 / item × month | INFL waterfall | fact (long) |
| 3 | `gold.treasury_curve` | 1 / as-of date × tenor | CURV / YCURV | fact (long) |
| 3b | `gold.treasury_curve_metrics` | 1 / as-of date | CURV metrics | fact (wide) |
| 4 | `gold.curve_spread_daily` | 1 / spread × date | CURV spreads | fact (long) |
| 4b | `gold.spread_inversion_episode` | 1 / spread × episode | CURV inversion history | fact (episodic) |
| 5 | `gold.benchmark_rate_board` | 1 / rate (latest) | BMRK | fact (snapshot) |
| 6 | `gold.funding_tape_daily` | 1 / metric × date | FUND / FCOST | fact (long) |
| 6b | `gold.funding_stress_daily` | 1 / date | FUND gauge | fact (wide) |
| 7 | `gold.credit_spread_daily` | 1 / instrument × date | CRDT | fact (long) |
| 8 | `gold.macro_regime_daily` | 1 / date | REGIME | fact (wide) |
| 9 | `gold.series_correlation` | 1 / pair × window × as-of | STAT | fact (long) |
| 10 | `gold.series_lead_lag` | 1 / pair × lag | EDA | fact (long) |

Global modules (GCPI, GPOL) reuse the same shapes keyed by country and are
listed in §4.9; they are gated on the World Bank / OECD ingest and marked
lower-priority.

---

## 4. Object specifications

Each spec gives: the terminal analytic it reproduces, columns, the engine
function, the config that drives it, and series prerequisites. Column lists are
the Gold contract Power BI will bind to.

### 4.0 Dimensions — `gold.dim_series`, `gold.dim_date`

`dim_series` is the star-schema hub every fact joins to. It absorbs the terminal's
per-series metadata (category, polarity, default transform, units) so facts stay
narrow.

`gold.dim_series` columns:
`series_id` (PK) · `title` · `source` · `frequency` · `econ_category`
(GROWTH·INFLATION·LABOR·RATES·CREDIT·HOUSING·CONSUMER·MONEY·ACTIVITY·FX) ·
`units` · `default_transform` (`pc1`/`pch`/`chg`/`bps`/`level` — mirrors the
terminal's FRED transform codes) · `polarity` (`+1` bullish-when-rising / `−1`
bearish-when-rising / `0` neutral) · `decimals` · `scale` (e.g. `$T`) · `notes`.

Built from a new `config/series_catalog.yml` (see §6) merged with the existing
manifest metadata. This is the one place the terminal's presentation semantics
(polarity, transform, scaling) get encoded.

`gold.dim_date` columns: `date` (PK) · `year` · `quarter` · `month` ·
`month_name` · `is_month_end` · `is_recession` (from `USREC`) · `fiscal_year`.
Small, generated. Enables Power BI time intelligence and the `USREC` shading the
curve/regime modules use.

### 4.1 ECON — `gold.macro_indicator_dashboard` (+ sparkline, + category summary)

**Reproduces:** the ECON macro grid — every headline series with latest / prior /
change / YoY / surprise / polarity / 36-pt sparkline, bucketed by category with
breadth and a surprise index.

`gold.macro_indicator_dashboard` (grain: 1 row / series, latest observation):

| column | meaning |
|---|---|
| `series_id`, `title`, `econ_category`, `units`, `source` | denormalized from `dim_series` |
| `latest_date`, `latest_value` | most recent observation |
| `prior_value`, `prior_date` | previous observation |
| `change_abs`, `change_pct` | latest − prior, and % |
| `mom_pct`, `yoy_pct` | from `fred_feature_transforms` |
| `z_score`, `percentile` | **expanding / PIT** z-score & percentile of the level (or of the transform, per `dim_series.default_transform`) |
| `surprise` | latest − trailing-N mean (the terminal's proxy "surprise" when no consensus feed exists — documented as such) |
| `polarity`, `direction_is_good` | from `dim_series.polarity` × sign(change) |
| `spark_min`, `spark_max`, `spark_last` | sparkline bounds for Power BI axis |
| `staleness_days` | today − `latest_date` (freshness badge) |
| `realtime_start` | vintage of the latest value (provenance badge) |

`gold.macro_indicator_sparkline` (grain: 1 row / series × point, last 36 points):
`series_id` · `point_index` (0…35) · `obs_date` · `value`. Power BI renders this
as a sparkline via a small-multiple line visual keyed on `series_id`.

`gold.macro_category_summary` (grain: 1 row / `econ_category`):
`econ_category` · `n_series` · `n_improving` · `breadth_pct`
(`n_improving / n_series`, using polarity-adjusted change) · `avg_z_score` ·
`surprise_index` (mean of per-series `surprise`) · `as_of_date`.

**Engine:** new `compute_macro_dashboard(silver_rows, catalog)` in `features.py`,
returning the three row sets. Reuses `compute_feature_transforms` for MoM/YoY and
the expanding-window helper (`_expanding_mean_std`) for PIT z-scores.
**Config:** `config/series_catalog.yml` (category + polarity + transform + N for
surprise window). **Series:** already ingested — the dashboard runs over the
**full existing catalog**, not just the terminal's 166; any series tagged in
`series_catalog.yml` appears in its `econ_category` bucket. Labor/unemployment
(`UNRATE`, `PAYEMS`, `ICSA`, `U1–U6RATE`, participation, JOLTS) flow through here
under `econ_category = LABOR`; the terminal's headline set is simply the
`priority = 1` subset of what the pipeline already holds.

### 4.2 INFL — `gold.inflation_explorer` (+ contribution)

**Reproduces:** CPI/PCE to item level with index, MoM %, YoY %, ΔMoM/ΔYoY
acceleration, and the contribution waterfall; CPI⇄PCE and SA⇄NSA toggles.

`gold.inflation_explorer` (grain: 1 row / item × month):

| column | meaning |
|---|---|
| `series_id`, `item_label`, `parent_item`, `hierarchy_level` | CPI/PCE item tree (from basket manifests) |
| `basket` (`CPI` / `PCE`), `sa_nsa` (`SA` / `NSA`) | toggle dimensions |
| `obs_date`, `index_value` | the index level |
| `mom_pct`, `yoy_pct` | month-over-month, year-over-year |
| `mom_accel` (ΔMoM), `yoy_accel` (ΔYoY) | this period's rate − last period's rate |
| `weight` | relative-importance weight (from a `config/cpi_weights.yml`) |
| `contribution_pp` | `weight × mom_pct` (approx pp contribution to headline) |
| `three_month_annualized` | trailing-3m annualized rate (terminal shows this) |

`gold.inflation_contribution` (grain: 1 row / contributing item × month, for the
waterfall): `obs_date` · `basket` · `item_label` · `contribution_pp` ·
`rank_in_month` · `is_headline_total`. Power BI renders this as a waterfall visual.

**Engine:** `compute_inflation_features(silver_rows, item_tree, weights)`.
**Config:** the item hierarchy is already in `manifests/bls_cpi_basket.yml` /
`bls_cpi_basket_sa.yml`; add `config/cpi_weights.yml` (relative importance) and a
`parent`/`level` field to the manifest entries (or a sibling
`config/cpi_hierarchy.yml`) so contributions and the tree render. **Series:** CPI
basket manifests exist (shipped inactive — activate + verify per their headers).
**PCE item level is a gap** — FRED carries headline `PCEPI` / core `PCEPILFE`, but
PCE *components* come from BEA (Table 2.4.4/2.4.5). Ship CPI first; add PCE items
via a BEA manifest in a second pass.

### 4.3 CURV / YCURV — `gold.treasury_curve` (+ metrics)

**Reproduces:** the Curve Lab — a point-in-time yield curve, level/slope/
curvature, butterflies, and curve-move classification.

`gold.treasury_curve` (grain: 1 row / as-of date × tenor) — the tidy curve Power
BI plots as a line across tenor:
`as_of_date` · `tenor_label` (`1M`…`30Y`) · `tenor_months` (sortable numeric) ·
`yield` · `series_id` · `source`.

`gold.treasury_curve_metrics` (grain: 1 row / as-of date):
`as_of_date` · `level` (mean of curve) · `slope_10y2y` · `slope_10y3m` ·
`curvature_2_5_10` (`2×5Y − 2Y − 10Y`) · `butterfly_2_10_30` ·
`is_inverted_10y2y` · `is_inverted_10y3m` · `is_recession` (`USREC` join) ·
`curve_move` (`bull-steepen` / `bull-flatten` / `bear-steepen` / `bear-flatten`,
from Δlevel × Δslope vs prior day).

**Engine:** `compute_treasury_curve(dgs_rows, usrec_rows, tenor_map)` — pivots the
daily `DGS*` history into curves and derives the metrics. **Config:**
`config/curve.yml` (tenor→series map + which spreads/butterflies to emit — this
generalizes the existing `spreads.yml`). **Series:** `DGS1MO, DGS3MO, DGS6MO,
DGS1, DGS2, DGS5, DGS10, DGS30` already in `manifests/rates.yml`; **add `DGS3,
DGS7, DGS20`** for a complete curve and **`USREC`** (recession flag) for inversion/
recession overlays.

### 4.4 CURV spreads — `gold.curve_spread_daily`

**Reproduces:** the user-selectable spread panel with z-scores and inversion
history. This is the existing `fred_curve_spread` **generalized** and enriched.

Columns: `spread_name` (`10Y-2Y`, `10Y-3M`, `30Y-5Y`, `10Y-1Y`, `5Y-2Y`,
`2Y-3M`, `30Y-10Y`) · `obs_date` · `value_bps` · `z_score` (PIT expanding) ·
`percentile` · `is_inverted` · `is_recession` · `days_inverted_run` (consecutive
inversion streak). Long, one row per spread × date, so Power BI slices by
`spread_name`.

**Engine:** extend `compute_curve_spreads` to emit z-score/percentile/inversion-
run alongside the level. **Config:** `config/spreads.yml` (exists — add the extra
pairs). **Series:** same `DGS*` + `USREC`.

### 4.4b CURV inversion history — `gold.spread_inversion_episode`

**Reproduces:** the Curve Lab's inversion-episode history — the discrete
inversion periods it lists per spread and cross-references with recessions.
Where `curve_spread_daily` is one row per observation, this is **one row per
unique inversion episode per spread**: an episode *starts* on the first
observation where the spread goes negative and *ends* on the first later
observation where it turns non-negative again (that re-steepening date is the
`end_date`; a single positive print between two inversions therefore splits
them into two distinct episodes). An episode still negative at the end of
history is *ongoing* (`end_date` null, `is_ongoing` true, duration measured to
`last_inverted_date`).

Columns (grain 1 / spread × episode): `spread_name` · `long_leg` · `short_leg` ·
`episode_number` (1-based per spread, chronological) · `start_date` ·
`end_date` · `last_inverted_date` · `observation_count` (inverted obs) ·
`calendar_days` · `trough_value` / `trough_bps` / `trough_date` (deepest
inversion) · `is_ongoing` · `recession_overlap` (any inverted date fell in an
NBER recession; null until `USREC` is ingested). Only `op: spread` definitions
participate — a ratio has no zero line. Power BI renders this as the episode
table / Gantt-style timeline next to the spread chart.

**Engine:** `compute_spread_inversion_episodes` (pure Python, both backends).
**Config:** `config/spreads.yml` (same definitions as §4.4). **Series:** same
`DGS*` legs + `USREC`.

### 4.5 BMRK — `gold.benchmark_rate_board`

**Reproduces:** the 43-rate benchmark board with trend, spread-to-benchmark, and
regime tag.

Columns (grain 1 / rate): `series_id` · `rate_label` · `rate_category` (the 7
buckets: policy / repo / Treasury / SOFR-complex / credit / mortgage / other) ·
`latest_value` · `prior_value` · `change_bps` · `trend` (`rising`/`falling`/`flat`
from a short slope) · `benchmark_series` · `spread_to_benchmark_bps` · `z_score` ·
`regime` (e.g. `tightening`/`easing`/`stable` from level + trend). **Config:**
`config/benchmark_rates.yml` (rate list, category, benchmark to spread against).
**Series:** policy/market rates — several exist (`FEDFUNDS`, `SOFR`, `DGS*`);
**add** `EFFR`, `OBFR`, `IORB`, `DPRIME`, `MORTGAGE30US` as needed to reach the
board the terminal shows (start with what's ingested, expand the manifest).

### 4.6 FUND / FCOST — `gold.funding_tape_daily` (+ stress)

**Reproduces:** the funding tape — corridor, balances, spreads, and the 0–100
stress gauge.

`gold.funding_tape_daily` (grain 1 / metric × date): `obs_date` · `metric_name`
(corridor rate, balance, or spread) · `metric_type` (`rate`/`balance`/`spread`) ·
`value` · `z_score` · `percentile`. Long — Power BI facets by `metric_type`.

`gold.funding_stress_daily` (grain 1 / date): `obs_date` · `sofr_effr_bps` ·
`sofr_iorb_bps` · `bill_ois_bps` · `stress_score` (0–100, a weighted blend of the
component z-scores, definition in `config/funding.yml`) · `stress_bucket`
(`calm`/`normal`/`elevated`/`stressed`).

**Engine:** `compute_funding_features(silver_rows, funding_cfg)`. **Config:**
`config/funding.yml` (corridor members, spread definitions, stress weights).
**Series — mostly already ingested.** The pipeline already carries the balance
sheet (`WALCL`, `WRESBAL`, `WTREGEN`, `NONBORRES`, `BORROW`, `TOTRESNS`,
`RESPPANWW`) and the SOFR complex (`SOFR`, `SOFR1`, `SOFR99`, `SOFRVOL`,
`WREPO`). The genuine gaps are the rest of the corridor — `IORB`, `EFFR`,
`OBFR`, `BGCR`, `TGCR`, `RRPONTSYD`, `SOFR30DAYAVG` — **all free on FRED**; add
a `manifests/fed_funding.yml`. `GC−OIS` / `FRA−OIS` need OIS inputs FRED may not
carry — implement the SOFR/EFFR/IORB/bill spreads (all in-hand once the corridor
is added) first and flag the OIS-based ones as "needs external input."

### 4.7 CRDT — `gold.credit_spread_daily`

**Reproduces:** IG/HY OAS levels, changes, valuation percentiles, stress episodes.

Columns (grain 1 / instrument × date): `obs_date` · `instrument` (`IG_OAS`,
`HY_OAS`, rating buckets, sectors) · `series_id` · `oas_bps` · `change_bps` ·
`z_score` · `percentile` · `is_stress_episode` (percentile > threshold, from
`config/credit.yml`). **Config:** `config/credit.yml`. **Series (gap — add a
`manifests/ice_credit.yml`):** `BAMLH0A0HYM2` (HY OAS), `BAMLC0A0CM` (IG OAS), and
the rating/sector OAS series (`BAMLC0A1CAAA`, `BAMLC0A4CBBB`, `BAMLH0A1HYBB`, …).

### 4.8 REGIME — `gold.macro_regime_daily`

**Reproduces:** the regime playbook's growth/inflation/liquidity/credit/policy
scoring and named regime.

Columns (grain 1 / date): `obs_date` · `growth_score` · `inflation_score` ·
`liquidity_score` · `credit_score` · `policy_score` · `composite_score` ·
`regime_name` (`Goldilocks` / `Reflation` / `Stagflation` / `Growth-Scare` /
`Liquidity-Squeeze` / `Policy-Easing` / `Neutral`) · `regime_confidence`.
Each score is a z-score blend of inputs; the regime name is a rule table over the
five scores. **Engine:** `compute_macro_regime(feature_rows, regime_cfg)`.
**Config:** `config/regime.yml` (input series per pillar, score weights, the
name-assignment rule table). **Series — largely in-hand, with official
conditions indices as a bonus.** Beyond the curve/credit/policy inputs
(`DGS10`, `DGS2`, `DGS3MO`, `SOFR`, `CPILFESL`, sticky-core `CORESTICKM159SFRBATL`),
the pipeline already ingests the **official financial-conditions and stress
indices** the terminal approximates by hand: Chicago Fed `NFCI`, `ANFCI`,
`NFCICREDIT`, `NFCILEVERAGE`, `NFCIRISK`, `NFCINONFINLEVERAGE`; St. Louis Fed
`STLFSI4`; and Chicago Fed activity `CFNAI` / `CFNAIMA3` / `CFNAIDIFF`. Wire the
liquidity/credit pillars off these rather than re-deriving them — a strict
improvement over the terminal. Only `BAMLH0A0HYM2` (credit pillar) and `EFFR`
(policy corridor) are new, both free on FRED (§4.6/§4.7).

### 4.9 STAT / EDA — `gold.series_correlation`, `gold.series_lead_lag`

**Reproduces:** the statistical lab's correlation matrix / rolling correlation and
the lead-lag / Granger analytics.

`gold.series_correlation` (grain 1 / pair × window × as-of): `series_a` ·
`series_b` · `window` (e.g. 60/120/252 obs or `full`) · `as_of_date` ·
`correlation` · `n_obs`. Long — Power BI renders the matrix as a heatmap.

`gold.series_lead_lag` (grain 1 / pair × lag): `series_a` · `series_b` · `lag`
(−k…+k) · `cross_correlation` · `granger_f` · `granger_p` · `best_lag` · `as_of_date`.

**Engine:** `compute_series_correlation` / `compute_lead_lag` (pure Python: Pearson,
lagged CCF, a small Granger F-test — no SciPy dependency required for the basics).
**Config:** `config/stats_pairs.yml` (which series/pairs to precompute — a full
N² matrix is expensive; the terminal lets the user pick, so precompute a curated
set). **Series:** any already ingested.

**Global modules (lower priority, gated on international ingest):**
- **GCPI** → reuse the `inflation_explorer` shape keyed by `country`
  (`gold.global_inflation`), fed by World Bank / OECD CPI (`worldbank_global.yml`
  partially covers this).
- **GPOL** → `gold.global_policy_rates` (country · date · policy_rate · change ·
  vs-target), gated on a central-bank policy-rate manifest.
- **RVOL / EML** → deferred (rate realized-vol is derivable from `DGS*` daily but
  is a distinct workstream; ML models are out of scope for Gold).

---

## 5. Power BI consumption model

Shape the report against a **star schema**:

- **Dimensions:** `gold.dim_series`, `gold.dim_date`. Mark `dim_date` as the date
  table; build a `series` slicer off `dim_series.econ_category`.
- **Facts:** the long tables above, each joined `fact.series_id → dim_series`
  and `fact.obs_date → dim_date`. Long/tidy facts let one visual serve many
  series via a slicer instead of one measure per series.
- **Suggested measures (DAX):** `Latest Value`, `YoY %`, `Δ vs Prior`,
  `Z-Score`, `Breadth %`, `Stress Score` — most are already precomputed columns,
  so measures are thin aggregations (`LASTNONBLANK`, `SELECTEDVALUE`).
- **Badges:** bind `staleness_days` and `source` to conditional formatting to
  reproduce the terminal's provenance/freshness badges. Bind `is_recession` to
  background shading on time-series visuals (curve, regime, spreads).
- **Refresh:** import mode off the Gold Delta tables (Databricks connector) or
  the SQLite mirror for local. The Gold layer is already denormalized enough that
  no report-side modeling beyond the two dimensions is required.

One **`gold.v_powerbi_catalog`** view enumerating every Gold object, its grain,
and its intended visual is worth adding so report authors have a manifest of
what's available (mirrors the terminal's module index).

---

## 6. Engineering plan (phased)

Each phase = a self-contained slice that ships a working set of Gold objects,
tests, and the `local_store` schema + `sql/50_gold.sql` / `sql/60_views.sql`
additions, following the existing `_build_*` pattern in `gold.py`.

**Phase 0 — Foundation (dimensions + catalog config). — IMPLEMENTED**
- `config/series_catalog.yml` (~65 already-ingested series tagged with
  category, polarity, default_transform, scale, decimals) — the single source
  of the terminal's presentation semantics; loader in
  `src/fred_pipeline/catalog_config.py`.
- `gold.dim_series` + `gold.dim_date` built by
  `fred_pipeline.terminal_views.build_dim_series` / `build_dim_date`;
  `manifests/macro_flags.yml` ships `USREC`/`USRECD` (inactive, verify-first) —
  `is_recession` is `NULL` (unknown) until activated.

**Phase 1 — ECON dashboard. — IMPLEMENTED**
- `gold.macro_indicator_dashboard`, `macro_indicator_sparkline`,
  `macro_category_summary` via `terminal_views.compute_macro_dashboard`
  (expanding PIT-safe z-score/percentile; trailing-window surprise proxy;
  polarity-adjusted breadth). Covers the unemployment/labor and headline macro
  views. Wired into both backends (`gold._build_terminal_views`,
  `LocalWarehouse.build_gold`), DDL in `sql/50_gold.sql`, tests in
  `tests/test_terminal_views.py`.

**Phase 2 — Inflation Explorer.**
- Activate + verify the CPI basket manifests; add `config/cpi_hierarchy.yml` +
  `config/cpi_weights.yml`; build `gold.inflation_explorer` +
  `inflation_contribution`. (PCE item level deferred to a BEA follow-up.)

**Phase 3 — Curve & spreads. — IMPLEMENTED**
- `config/curve.yml` (11 tenors) + `terminal_views.compute_treasury_curve` →
  `gold.treasury_curve` + `treasury_curve_metrics` (level/slope/curvature/
  butterfly, inversions, recession overlay, bull/bear × steepener/flattener
  move classification); `compute_curve_spread_daily` → the enriched
  `gold.curve_spread_daily` (z-score/percentile/bps/inversion runs).
  `DGS3/DGS7/DGS20` added to `manifests/rates.yml` (inactive, verify-first);
  absent tenors simply emit no rows until activated.

**Phase 3b — Inversion episodes (unique inversion periods per spread). —
IMPLEMENTED**
- `gold.spread_inversion_episode` (§4.4b) via
  `terminal_views.compute_spread_inversion_episodes`: one row per unique
  inversion episode per configured spread — the episode starts when the spread
  first prints negative and ends on the first print back at/above zero, with
  episode number, duration (obs + calendar days), trough value/date, ongoing
  flag, and recession overlap. Both backends + tests.

**Phase 4 — Rates complex (BMRK + FUND + FCOST + CRDT). — IMPLEMENTED**
- Configs `benchmark_rates.yml` (17-rate board, categories, benchmark pairs,
  trend window), `funding.yml` (corridor + balances + spreads + weighted
  stress components), `credit.yml` (9 OAS instruments, stress percentile);
  loaders in `src/fred_pipeline/rates_complex_config.py`.
- Engines in `terminal_views.py`: `compute_benchmark_rate_board` (change bps,
  ±1bp-dead-band trend, spread-to-benchmark, expanding z/percentile,
  tightening/easing/stable regime), `compute_funding_features` (tape with
  expanding stats + the 0–100 gauge: `clamp(50 + 20·Σwᵢzᵢ/Σwᵢ, 0, 100)`,
  bucketed calm/normal/elevated/stressed, emitted only when every component
  prints), `compute_credit_spread_daily` (OAS pct+bps, change, expanding
  stats, percentile-threshold stress episodes, recession overlay).
- Tables `gold.benchmark_rate_board`, `funding_tape_daily`,
  `funding_stress_daily`, `credit_spread_daily` in both backends.
- New manifests `fed_funding.yml` (EFFR/IORB/OBFR/BGCR/TGCR/RRPONTSYD/
  SOFR30DAYAVG) + `ice_credit.yml` (IG/HY headline + rating-curve OAS) +
  `DPRIME`/`MORTGAGE30US` in `rates.yml` — **all inactive, verify before
  activating** (egress is blocked in the build env; IDs were assembled from
  documentation, not live-checked). Balances (WALCL/WRESBAL/WTREGEN) and SOFR
  were already ingested; absent series simply emit no rows until activated.
- Note one schema refinement vs. the original sketch: the stress gauge is
  generic (`composite_z`/`stress_score`/`stress_bucket`/`n_components`) with
  the component spreads living in the tape, rather than hard-coding one
  column per spread — config edits don't change the table shape.
- `GC−OIS`/`FRA−OIS` remain "needs external input" (no OIS on FRED); FCOST's
  blended-cost decomposition is covered by the tape + spreads.

**Phase 5 — Regime + Stats.**
- `config/regime.yml`, `config/stats_pairs.yml`; build `gold.macro_regime_daily`,
  `gold.series_correlation`, `gold.series_lead_lag`. Regime depends on Phase 3/4
  inputs.

**Phase 6 — Global + Power BI catalog.**
- `gold.global_inflation`, `gold.global_policy_rates` (gated on international
  ingest); `gold.v_powerbi_catalog`; the Power BI `.pbix` starter model.

**Cross-cutting for every phase:** pure-Python engine function + unit tests
(both backends assert identical output); SQLite `_SCHEMA` table + view;
`sql/50_gold.sql` + `sql/60_views.sql`; `docs/data_dictionary.md` entry; handoff
update. No metric ships as raw Spark SQL.

---

## 7. Series prerequisites — inventory & gaps

**The pipeline already contains far more than the terminal's 166-series catalog.**
Across the active manifests it ingests **several thousand** series — the entire
labor complex (LNS/CES/CEU/JTS/`U1RATE…U6RATE`), the full CPI/PPI/PCE price
apparatus (`WPU*`, `WPS*`, `PCU*`, plus sticky/flexible/trimmed cuts
`CORESTICKM159SFRBATL`, `FLEXCPIM159SFRBATL`, `PCETRIM12M159SFRBDAL`), national
accounts (`GDP*`, `PCE*`, BEA `*BEA`), housing (`HOUST*`, `PERMIT*`, `MSPUS`,
`ATNHPIUS*`), money & banking (`M1SL/M2SL`, `BOGMBASE`, H.8 bank credit), the
Fed balance sheet (`WALCL`, `WRESBAL`, `WTREGEN`, `NONBORRES`, `RRP`-adjacent),
FX (`DEX*`, `DTWEX*`), breakevens/expectations (`T5YIE`, `T10YIE`, `EXPINF*`),
real rates (`REAINTRAT*`), and — importantly — the **official financial-conditions
and stress indices** (`NFCI`, `ANFCI`, `NFCICREDIT/RISK/LEVERAGE`, `STLFSI4`,
`CFNAI*`). **Every Gold object above draws first from this existing catalog**;
`gold.dim_series` / `config/series_catalog.yml` is where each existing series gets
tagged into an `econ_category`, polarity, and default transform so it flows into
ECON, INFL, curve, regime, etc. The design intent (per the user) is to **use all
series already in the pipeline, or ones attainable through free APIs** — no
proprietary/paid feeds.

The residual gaps below are **small and every one is free on FRED** (or the free
World Bank/OECD APIs already wired in `worldbank_global.yml`):

| Need | Status | Action |
|---|---|---|
| Labor / unemployment (`UNRATE`, `PAYEMS`, `ICSA`, `U1–U6RATE`, JOLTS, participation) | **present** (`labor*.yml`) | tag into `series_catalog.yml` |
| Inflation headline/core + sticky/flexible/trimmed (`CPIAUCSL`, `CPILFESL`, `PCEPI`, `PCEPILFE`, `CORESTICKM*`, `FLEXCPI*`, `PCETRIM*`) | **present** | tag into catalog |
| CPI basket (items) | **present but inactive** (`bls_cpi_basket*.yml`) | activate + verify ids |
| Breakevens / inflation expectations (`T5YIE`, `T10YIE`, `EXPINF*`) | **present** | tag into catalog |
| Fed balance sheet / funding balances (`WALCL`, `WRESBAL`, `WTREGEN`, `NONBORRES`, `SOFR`, `SOFRVOL`, `WREPO`) | **present** | tag into catalog |
| Financial conditions / stress (`NFCI`, `ANFCI`, `STLFSI4`, `NFCICREDIT/RISK/LEVERAGE`) | **present** | drive REGIME liquidity/credit pillars |
| Activity / growth (`CFNAI*`, `INDPRO`, `GDPC1`, `PAYEMS`) | **present** | tag into catalog |
| FX (`DEX*`, `DTWEXBGS`, `DTWEXAFEGS`, `DTWEXEMEGS`) | **present** | FX `econ_category` |
| Curve tenors `DGS3, DGS7, DGS20` | **gap — free on FRED** | add to `rates.yml` |
| Recession flag `USREC` | **gap — free on FRED** | new `macro_flags.yml` |
| Funding corridor `IORB, EFFR, OBFR, BGCR, TGCR, RRPONTSYD` | **gap — free on FRED** | new `fed_funding.yml` |
| Credit OAS `BAMLH0A0HYM2, BAMLC0A0CM` + rating/sector | **gap — free on FRED** | new `ice_credit.yml` |
| Benchmark-board extras `DPRIME, MORTGAGE30US` | **gap — free on FRED** | extend `rates.yml` |
| PCE item level | **gap — free (BEA API, already wired)** | BEA manifest (Table 2.4.x) |
| Global CPI / policy rates (GCPI/GPOL) | partial | free World Bank / OECD (`worldbank_global.yml`) |

Net: **no new data source or paid feed is required** — the domestic Gold objects
run entirely on series already in the pipeline plus ~15 additional free-FRED IDs;
the global objects use the free World Bank/OECD path already in the repo.

**Egress caveat (unchanged from the rest of this repo).** The build environment
routes all outbound HTTPS through the agent proxy, which blocks the FRED/BLS/BEA
APIs, so the ~15 new series IDs cannot be live-verified here. Every new manifest
must ship with the standard **"⚠️ VERIFY BEFORE ACTIVATING"** header and
`active: false`, exactly like `bls_cpi_basket.yml`. Per-series isolation means a
wrong ID fails only its own series, not the run.

---

## 8. What this buys over the terminal

- **Point-in-time correctness.** The terminal recomputes from latest-revised FRED
  on every page load; the Gold layer resolves ALFRED vintages, so z-scores,
  regimes, and backtests use only data known as-of each date. Same visuals,
  defensible history.
- **Precomputed, not on-the-fly.** Power BI binds to finished columns; no
  client-side TypeScript math to reconcile.
- **One provenance story.** `source` / `realtime_start` / `staleness_days` on
  every Gold row reproduce the terminal's badges and "worst-source" aggregation
  in the semantic model.
- **Config-driven parity.** Categories, polarities, spreads, regime rules, and
  funding/credit definitions live in YAML — the terminal's presentation logic
  becomes reviewable config, not code.

---

## 9. Open questions for the user

1. **PCE item level** (INFL) requires a BEA manifest (FRED only carries headline/
   core PCE). Ship CPI-only first and add PCE items later, or block INFL on BEA?
2. **Regime rule table** — adopt the terminal's six named regimes verbatim, or
   refine the score thresholds against our (PIT) history first?
3. **Correlation/lead-lag scope** — a curated pair list (`stats_pairs.yml`) vs a
   full N² matrix over the catalog (expensive; the terminal is interactive, Gold
   is precomputed).
4. **Power BI delivery** — do you want a starter `.pbix` in the repo, or just the
   Gold tables + this view catalog and you build the report?
