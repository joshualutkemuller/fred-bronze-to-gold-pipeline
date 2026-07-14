# Data Dictionary

Fully-qualified names are `macro_{env}.{schema}.{table}` where `env` is one of
`dev` / `test` / `prod`.

## meta

### `meta.fred_series`
One row per series in the universe, synced from manifests.

| Column | Type | Notes |
|---|---|---|
| series_id | STRING | FRED series id (PK) |
| title | STRING | Human-readable title |
| category | STRING | Manifest category (rates/inflation/labor/growth) |
| frequency | STRING | FRED frequency code (d/w/m/q/…) |
| units | STRING | Reported units |
| active | BOOLEAN | Whether the series is ingested |
| load_type | STRING | `full` or `incremental` |
| expected_update_frequency | STRING | e.g. `monthly`, `business_daily` |
| vintage_enabled | BOOLEAN | Whether full real-time history is pulled |
| validation_profile | STRING | `strict` / `standard` / `lenient` |
| business_owner / technical_owner | STRING | Ownership |
| downstream_use_case | STRING | Primary consumer |
| priority | INT | 1 (highest) … 5 |
| restate_records | INT | Per-series override of `restate_last_n` (NULL = use config default) |
| tags | ARRAY<STRING> | Grouping tags |
| updated_at | TIMESTAMP | Last sync time |

### `meta.fred_manifest`
Registry of manifest files loaded (name, version, source_path, series_count).

### `meta.fred_series_manifest_map`
Membership of `series_id` → `manifest_name`.

### `meta.fred_series_lifecycle`
Append-only snapshots of FRED-reported series health (written by `reconcile`),
so a series' trajectory can be tracked over time.

| Column | Type | Notes |
|---|---|---|
| series_id | STRING | Series |
| fred_title / fred_frequency / fred_units | STRING | As currently reported by FRED |
| seasonal_adjustment | STRING | e.g. SA / NSA |
| observation_start / observation_end | STRING | FRED coverage range |
| last_updated | STRING | FRED last-updated timestamp |
| popularity | INT | FRED popularity (0–100) |
| discontinued | BOOLEAN | Title marked DISCONTINUED |
| days_since_last_observation | INT | today − observation_end |
| is_stale | BOOLEAN | Past the expected update window for its frequency |
| checked_at | STRING | When this snapshot was taken |

### `meta.fred_series_drift`
Drift between manifest intent and live FRED metadata (written by `reconcile`).

| Column | Type | Notes |
|---|---|---|
| series_id | STRING | Series |
| field | STRING | Field that drifted (frequency/title/units/series_id) |
| manifest_value / fred_value | STRING | Declared vs actual |
| kind | STRING | frequency_mismatch \| discontinued \| units_changed \| not_found |
| severity | STRING | info \| warning \| error |
| detected_at | STRING | When detected |

## audit

### `audit.etl_run`
One row per pipeline invocation.

| Column | Type | Notes |
|---|---|---|
| run_id | STRING | UUID hex, unique per run |
| environment | STRING | dev/test/prod |
| manifest_path | STRING | Manifest source used |
| triggered_by | STRING | cli / databricks_job / … |
| status | STRING | running/succeeded/failed/partial |
| started_at / ended_at | TIMESTAMP | Run window |
| duration_seconds | DOUBLE | Wall-clock |
| series_total / series_succeeded / series_failed | INT | Counts |
| error_message | STRING | Run-level error, if any |

### `audit.etl_series_run`
One row per `(run_id, series_id)` with extraction/write counts, `dq_passed`,
timing, and per-series error. `load_type` records the *effective* strategy used
for that run: `full` (first load or `load_type: full`) or `restate_last_<n>`.

### `audit.data_quality_result`
One row per `(run_id, series_id, check_name)` with `passed`, `severity`
(info/warning/error), `message`, and `metric_value`.

## bronze

### `bronze.fred_api_response`
Verbatim upstream payloads (system of record, multi-source), partitioned by
`series_id`.

| Column | Type | Notes |
|---|---|---|
| run_id | STRING | Owning run |
| source | STRING | Upstream API the payload came from (`fred`, `bls`, `eia`, …) |
| series_id | STRING | Series |
| endpoint | STRING | The source endpoint actually called (e.g. `series/observations`, `timeseries/data/{id}`, `seriesid/{id}`) |
| request_params | STRING | JSON of request params (**api_key never stored**) |
| response_payload | STRING | Verbatim upstream JSON |
| observation_count | INT | Rows normalized from the payload (accurate across sources) |
| payload_bytes | INT | Payload size |
| ingested_at | TIMESTAMP | Ingestion time |

## silver

### `silver.fred_observation`
Normalized observations (multi-source). **Natural key / MERGE key:**
`(source, series_id, observation_date, realtime_start)`.

| Column | Type | Notes |
|---|---|---|
| source | STRING | Upstream API the row came from (`fred`, `bls`, …). Leading key component so the same `series_id` could be sourced from more than one API without colliding |
| series_id | STRING | Series |
| observation_date | DATE | The date the value describes |
| realtime_start | DATE | Vintage window start (when value became known). **NULL/blank for `vintage_enabled: false` series** — vintage is not tracked, so the key is `(series_id, observation_date)` and re-runs update in place |
| realtime_end | DATE | Vintage window end (`9999-12-31` = still current → NULL); also blank for non-vintage series |
| value | DOUBLE | Parsed numeric value (NULL if missing) |
| raw_value | STRING | Original upstream string (FRED `.` preserved as-is) |
| is_missing | BOOLEAN | True when the value could not be parsed (e.g. FRED `.`) |
| row_hash | STRING | sha256 change-detection hash |
| revision_number | INT | 1…N per (series_id, observation_date) |
| ingested_at | TIMESTAMP | Ingestion time |
| run_id | STRING | Owning run |

## gold

### `gold.fred_point_in_time`
Full vintage history (every revision). Query with a real-time filter for
as-of-date reconstruction.

### `gold.fred_latest_observation`
Latest revision per `(series_id, observation_date)` — "as revised today".

### `gold.fred_macro_feature_daily`
Daily calendar × series grid, forward-filled from latest observations. Columns:
`as_of_date`, `series_id`, `raw_value` (only on native release dates), `value`
(forward-filled). Suitable for optimizer inputs and ML feature matrices.

### `gold.fred_feature_transforms`
Per-series quant transforms from latest observations: `mom` (period-over-period
% change), `diff` (first difference), `yoy` (year-over-year % change), `zscore`
(expanding, point-in-time safe — mean/std computed only from observations
at-or-before each row's date, never later ones). Keyed `(series_id,
observation_date)`.

### `gold.fred_curve_spread`
Cross-series spreads (`long_leg − short_leg`) and ratios (`long_leg /
short_leg`), **defined in `config/spreads.yml`** (see
`fred_pipeline.spread_config.load_spread_defs`) rather than hardcoded —
review and add pairs there without touching Python. Ships with the original
4 Treasury curve spreads (`T10Y2Y`, `T10Y3M`, `T2Y3M`, `T30Y10Y`). A date is
only emitted when both legs have a non-missing value (and, for a ratio, the
short leg is nonzero). Columns: `spread_name`, `observation_date`,
`long_leg`, `short_leg`, `value`.

### `gold.fred_cross_series_feature`
**Frequency-aware, N-leg** cross-series features, **defined in
`config/cross_series.yml`** (see `fred_pipeline.cross_series_config`). Unlike
`fred_curve_spread` (same-frequency, 2-leg), each leg is aligned **as-of** to the
feature's target `frequency` (the last observation within each period), so legs
of different cadence and different **sources** can be combined — e.g. daily
Treasury debt ÷ quarterly BEA GDP. Ops: `spread` (a−b), `ratio` (a/b, guarded),
`composite` (Σ weightᵢ·legᵢ). A period is emitted only when every leg has an
aligned value. Both backends compute it via the one shared Python engine
(`fred_pipeline.features.compute_cross_series_features`). Columns: `feature_name`,
`op`, `observation_date`, `value`.

### `gold.fred_cross_series_feature_pit`
**Point-in-time (`realtime_start`-aligned)** version of the above: each leg
contributes the value that was **actually known** (as-first-reported — the
earliest vintage per observation) rather than latest-revised, so the feature
series is **leak-free for backtests**. Reads raw Silver (all vintages) via
`fred_pipeline.features.compute_cross_series_features_pit`; the function also
accepts an `as_of` date to reconstruct the series as it stood on any past date.
For non-vintage series this equals the latest-revised feature. Columns:
`feature_name`, `op`, `observation_date`, `value`, `basis` (`first_report` or the
as-of date).

### `gold.fred_source_reconciliation`
Cross-source data-lineage QA: same-concept series from **different sources**
(e.g. FRED `UNRATE` vs BLS `LNS14000000`; FRED `GDP` vs a BEA NIPA line) compared
after as-of alignment, **defined in `config/reconciliations.yml`** (see
`fred_pipeline.reconciliation_config`). Series ids differ by source, so the
pairing is declared, not inferred. Both backends compute it via
`fred_pipeline.features.compute_source_reconciliation`. Columns: `name`,
`observation_date`, `series_a`, `value_a`, `series_b`, `value_b`, `abs_diff`,
`pct_diff`, `diverged` (`|pct_diff| > tolerance_pct`).

### `gold.fred_company_fundamentals`
SEC company financials, **standardized**: raw XBRL tags mapped to canonical line
items (`config/sec_concepts.yml`, priority-ordered candidate tags) so companies
using different tags for the same concept line up. Per `(cik, concept, period)`
the value comes from the highest-priority tag reported, latest-filed vintage.
Built by `fred_pipeline.sec_standardization.standardize_sec_statements`. Columns:
`cik`, `concept`, `statement`, `observation_date`, `value`. (Restatement history
for these is already in `gold.fred_revision_stats`, since SEC filings carry
vintages.)

### `gold.fred_company_ratios`
Derived company ratios (`config/sec_ratios.yml`): `numerator / denominator`
concept per `(cik, period)` — e.g. `debt_to_equity`, `current_ratio`,
`return_on_equity`. Columns: `cik`, `ratio_name`, `observation_date`, `value`.
Income-statement concepts are duration-disambiguated at ingestion (`SEC_PERIOD`,
default `quarterly`), so income and balance-sheet concepts combine on a
consistent basis. Quarterly mode synthesizes Q4 by de-cumulation
(`Q4 = FY − 9-month YTD`, dated at the FY end), since a 10-K reports only the FY
figure — giving a complete quarterly series.

### `gold.fred_revision_stats`
How much each observation moved between its first print and today. Reads raw
Silver (every vintage), not latest-revision rows — it exists to measure
revision behavior itself. Columns: `series_id`, `observation_date`,
`revision_count`, `first_value`, `first_realtime_start`, `latest_value`,
`latest_realtime_start`, `revision_delta` (latest − first), `revision_pct`.
Non-vintage series (`vintage_enabled: false`) always have `revision_count = 1`
— no vintage history is tracked for them, so there's nothing to compare; that's
a legitimate "not revised" signal, not a data gap. Useful for judging how much
to trust a series' initial print (e.g. GDP/payrolls are heavily revised;
market/price series usually are not).

### Market-terminal analytical views
Gold objects recreating the `market_terminal` project's economic-analysis
surfaces for Power BI (plan + full column semantics in
`docs/market_terminal_gold_views.md`). All computed by the shared pure-Python
engines in `fred_pipeline.terminal_views`; the ECON dashboard covers only
series cataloged in `config/series_catalog.yml`.

#### `gold.dim_series`
Star-schema hub: one row per cataloged series — presentation semantics from
`config/series_catalog.yml` (`econ_category`, `polarity` [+1 rise-is-bullish /
−1 / 0], `default_transform` [`pc1|pch|chg|bps|level`], `scale`, `decimals`,
`notes`) merged with `title`/`frequency`/`units` from `meta.fred_series`.

#### `gold.dim_date`
One row per calendar day over the observed range: `year`, `quarter`, `month`,
`month_name`, `is_month_end`, `fiscal_year` (US federal, October start), and
`is_recession` (NBER `USREC`; `NULL` = unknown until
`manifests/macro_flags.yml` is activated, never `false`).

#### `gold.macro_indicator_dashboard`
The ECON macro grid: one row per cataloged series at its latest observation —
`latest`/`prior` value+date, `change_abs`/`change_pct`, `yoy_pct`, expanding
(PIT-safe) `zscore` and `percentile`, `surprise` (no-consensus proxy: latest −
trailing `surprise_window` mean; `surprise_z` divides by that window's std),
polarity-adjusted `direction_is_good`, sparkline bounds, `staleness_days`
(vs. the run's `as_of_date`), `realtime_start` provenance.

#### `gold.macro_indicator_sparkline`
Last 36 observations per cataloged series (`point_index` 0 = oldest) for
Power BI sparkline visuals keyed on `series_id`.

#### `gold.macro_category_summary`
Per-`econ_category` rollup: `n_series`, `n_improving`/`n_deteriorating`
(polarity-adjusted), `breadth_pct` (of directional series), `avg_zscore`,
`surprise_index` (mean `surprise_z`).

#### `gold.treasury_curve`
Curve Lab, tidy: one row per `as_of_date` × tenor with data (`tenor_label`,
sortable `tenor_months`, `series_id`, `yield_pct`). Tenor→series map in
`config/curve.yml`; tenors without ingested data emit no rows.

#### `gold.treasury_curve_metrics`
Per-date curve metrics: `level` (mean of available tenors), `slope_10y2y`,
`slope_10y3m`, `curvature_2_5_10` (2×5Y − 2Y − 10Y), `butterfly_2_10_30`
(2×10Y − 2Y − 30Y), inversion flags, `is_recession`, and `curve_move` — the
bull/bear × steepener/flattener classification vs. the prior curve date
(`parallel-*` / `twist-*` when only one of level/slope moved).

#### `gold.curve_spread_daily`
The configured spreads/ratios (`config/spreads.yml`) enriched with expanding
(PIT-safe) `zscore`/`percentile`, `value_bps`, `is_inverted` +
`inversion_run` (consecutive inverted observations; spreads only — a ratio has
no zero line), and `is_recession`. Supersets `gold.fred_curve_spread` (kept
for compatibility).

#### `gold.spread_inversion_episode`
One row per **unique inversion episode** per configured spread (`op: spread`
only): the episode opens on the first negative observation and closes on the
first later non-negative one (`end_date` = that re-steepening date; a single
positive print between two inversions splits them into two episodes). Columns:
`spread_name`, `long_leg`, `short_leg`, `episode_number` (1-based,
chronological per spread), `start_date`, `end_date` (`NULL` while ongoing),
`last_inverted_date`, `observation_count`, `calendar_days` (to `end_date`, or
to `last_inverted_date` while ongoing), `trough_value`/`trough_bps`/
`trough_date` (deepest inversion), `is_ongoing`, `recession_overlap` (any
inverted date fell in an NBER recession; `NULL` until `USREC` is ingested).

#### `gold.inflation_explorer`
The INFL item trees (`config/inflation_items.yml`: CPI/SA rooted at
`CPIAUCSL`, CPI/NSA rooted at `CUUR0000SA0`, PCE/SA rooted at `PCEPI`): one
row per item × month with `index_value`, `mom_pct`/`yoy_pct` (fractions),
`mom_accel`/`yoy_accel` (this month's rate − last month's), 
`three_month_annualized` (`(I_t/I_{t−3})⁴ − 1`), the item's
relative-importance `weight` (percent of the headline basket), and
`contribution_pp` (`weight × mom_pct`, in headline percentage points), plus
`item_label`/`parent_item`/`hierarchy_level`/`basket`/`sa_nsa` for the
drill-down tree. Month arithmetic is calendar-based — a publication gap
yields NULLs, never a wrong-month comparison.

#### `gold.inflation_contribution`
The contribution waterfall: per tree (basket × sa_nsa) and month where the
headline printed, one row per `waterfall: true` item (the 8 CPI major
groups), ranked by `contribution_pp` (`rank_in_month` 1 = largest), plus an
`is_headline_total` row carrying the headline's own MoM in percentage points
— the bar the item contributions stack against.

#### `gold.benchmark_rate_board`
One row per rate configured in `config/benchmark_rates.yml` at its latest
observation: `latest_value`/`prior_value`, `change_bps`, `trend`
(rising/falling/flat — latest vs. `trend_window` observations ago with a ±1bp
dead-band; `NULL` when history is shorter), `spread_to_benchmark_bps` (vs. the
configured `benchmark_series`' last value on-or-before the rate's date),
expanding `zscore`/`percentile`, `regime` (rising→`tightening`,
falling→`easing`, flat→`stable`), `staleness_days` vs. the board's
`as_of_date`. Rates whose series aren't ingested emit no row.

#### `gold.funding_tape_daily`
The FUND tape (`config/funding.yml`): one row per metric × date —
`metric_type` `rate` (corridor), `balance` (Fed balance-sheet lines), or
`spread` (long − short, emitted on dates both legs print) — with expanding
(PIT-safe) `zscore`/`percentile`.

#### `gold.funding_stress_daily`
The 0–100 funding stress gauge: `composite_z` = weighted mean of the
configured component spreads' expanding z-scores;
`stress_score = clamp(50 + 20 × composite_z, 0, 100)`; `stress_bucket`
calm (<40) / normal (<60) / elevated (<80) / stressed (≥80). Rows appear only
on dates where **every** component spread has a value (`n_components` records
how many were blended). An early observation whose expanding std is still 0
contributes a neutral 0, not a gap.

#### `gold.credit_spread_daily`
OAS history per instrument in `config/credit.yml` (ICE BofA indices; FRED
publishes percent — `oas_pct`, with `oas_bps` = ×100): `change_bps` vs. the
prior print, expanding `zscore`/`percentile`, `is_stress_episode` (expanding
percentile ≥ `stress_percentile`, default 0.90; `NULL` on the first
observation), `is_recession` (NBER overlay, `NULL` until `USREC` is ingested).

#### `gold.curve_spread_rolling` / `gold.credit_spread_rolling` / `gold.treasury_curve_rolling`
Rolling-window stats companions to `curve_spread_daily`,
`credit_spread_daily`, and `treasury_curve`: one row per entity × date ×
`window`, with trailing windows of **1/5/10/21/63/126/252 observations**
(~trading-day horizons: day, week, 2 weeks, month, quarter, half-year, year).
Columns per row: the level (`value` in the spread's native units / `oas_bps`
for credit / `yield_pct` for tenors), `change` (`v_t − v_{t−w}`, native
units; bps for credit), `pct_change` (vs. `v_{t−w}`; `NULL` at a zero base —
and of limited meaning for spreads that cross zero), and `zscore` (vs. the
trailing-w rolling mean/std including the current value; `NULL` when the
window std is 0 — always for window 1). A row appears only once its window
is **fully populated** (no partial-window stats), and all stats are
trailing-only, so they are point-in-time safe. In Power BI, put `window` on
a slicer and one visual serves all horizons.

#### `gold.macro_regime_daily`
The REGIME playbook (`config/regime.yml`): one row per emission date (the
union of the pillar inputs' post-transform dates, once every pillar is live).
The five pillar scores are weighted means of direction-adjusted **expanding**
z-scores of their input series, carried as-of within `max_staleness_days`
(an input staler than the cap drops out; a pillar with no live input
suppresses the row). Score semantics: growth high = strong, inflation high =
hot, liquidity high = loose, credit high = stressed, policy high =
tightening. `composite_score` is the `composite_weight`-signed mean (higher
= friendlier macro mix). `regime_name` comes from the ordered rule table
(first match wins, else the default); `regime_confidence` is the smallest
z-margin by which the matched rule's conditions clear (`NULL` for the
default regime).

#### `gold.series_correlation`
The STAT lab (`config/stats_pairs.yml`): Pearson correlation per pair ×
`window` × date over transformed (default first-differenced), date-aligned
series. `window` 0 = expanding full sample to date (from the 3rd common
observation); rolling windows emit only once fully populated. `correlation`
is `NULL` when either window is constant.

#### `gold.series_lead_lag`
The EDA lead-lag lab: full-sample cross-correlation at lags
−`max_lag`..+`max_lag` per pair (**positive lag = `series_a` leads
`series_b`**), with `best_lag` (largest |CCF|) and both Granger F-test
directions (`granger_f_ab`/`granger_p_ab` = "does a help predict b?", lag
order `granger_lags`) denormalized onto every row; `as_of_date` is the last
common observation. p-values are exact F-distribution tails computed in pure
Python (regularized incomplete beta — no SciPy).

#### `gold.global_inflation`
GCPI (`config/global_series.yml`): one row per country × print — CPI YoY in
percent (`level` entries are already YoY rates, e.g. World Bank
`FP.CPI.TOTL.ZG`; `yoy_from_index` computes it from a CPI index),
`change_pp` vs. the prior print, `trend` (accelerating/cooling/flat, ±0.05pp
dead-band), `streak` (signed consecutive prints: +n accelerating, −n
cooling, flat resets), and `target_pct`/`vs_target_pp` (central-bank target
gap). Slice by `region` (AMER/EMEA/APAC).

#### `gold.global_policy_rates`
GPOL: one row per country × print — `policy_rate_pct`, `change_bps` vs. the
prior print, `last_move_bps` (most recent move >1bp, carried), `stance`
(hiking/cutting from that move's sign; on-hold before any move), and
`real_rate_pct` (policy − the country's latest CPI YoY print on-or-before
the date, when an inflation entry for the same `iso3` is configured and at
most ~400 days old).

#### `gold.powerbi_catalog`
The report author's manifest: one row per Gold object with `object_type`
(dimension/fact/reference), the terminal `module` it serves, `grain`,
`intended_visual`, and a description. Single source of truth is
`fred_pipeline.global_views.POWERBI_CATALOG`; a test fails if a `gold_*`
table is added without a catalog row.

#### `gold.equity_return_daily`
Equity price return (`source: stooq`; scalar-explode `<ticker>:close` Silver
series): one row per ticker × date — the split-adjusted `close`,
`price_change` and `price_return` (simple, day-over-day), and
`price_return_index` (cumulative, =100 at each ticker's first observation).
Dividends excluded — total return is the planned Tiingo slice
(`gold.equity_total_return_index`).

#### `gold.index_constituents`
ETF membership (`source: ishares`; `<ETF>:<constituent>` weight series
exploded from the daily holdings CSV): one row per ETF × constituent ×
snapshot date — `weight_pct`, `weight_rank` within the snapshot, and
`is_latest_snapshot` (filter to that for current membership). Snapshot
history accumulates through the normal incremental path.

#### `gold.equity_total_return_index`
True total return (`source: tiingo`; scalar-explode `<ticker>:close`,
`<ticker>:divCash`, `<ticker>:splitFactor`): one row per ticker × date with
the raw `close`, `dividend`, `split_factor`; the daily `price_return` and
`total_return` (split-adjusted, dividends reinvested —
`(close_t + div_t)/close_{t−1} × split_t − 1`); the cumulative
`price_return_index` and `total_return_index` (=100 at each ticker's first
date; their gap is reinvested income); and `trailing_12m_dividend` /
`dividend_yield_pct`. Reconstructed from the **raw** inputs rather than
Tiingo's `adjClose`, so it can be rebuilt and diffed when a dividend is
restated.

### Point-in-time feature snapshot
`gold.point_in_time_features_sql(as_of)` (Spark) / `LocalWarehouse.
point_in_time_features(as_of)` return each series' value **as it was known** on
`as_of` — a leakage-free feature vector for backtests.

### Views (`gold.v_*` on Databricks; `gold_v_*` in the local SQLite backend)
Defined in `sql/60_views.sql` (Delta) and mirrored by hand in
`local_store.py`'s schema for the local backend (SQLite has no `CREATE OR
REPLACE VIEW`, so these must be kept in sync manually between the two).
* `v_latest_revised` — latest revision per date (backs the Gold table).
* `v_point_in_time` — every vintage row; filter by real-time window.
* `v_series_latest_value` — most recent non-missing value per series.
* `v_series_revision_summary` — per-series rollup of `fred_revision_stats`
  (avg/max revision count, avg/max absolute revision %).
* `v_source_coverage` — multi-source coverage & freshness dashboard: per
  `(source, series_id)` the latest observation date, observation count, days
  since last, and an `is_stale` verdict from the manifest cadence
  (`meta.fred_series.frequency` vs. `FREQUENCY_MAX_AGE_DAYS`).
* `v_company_ratio_ranks` — cross-company ranks/percentiles of each SEC-derived
  ratio within each period (`pct_rank` 0..1 ascending; `rank_desc` = 1 is the
  largest), over `fred_company_ratios`.
