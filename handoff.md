# fred-bronze-to-gold-pipeline
Full ETL Fred Pipeline

# FRED ETL Pipeline --- Databricks Handoff + LLM Build Spec

## Goal

Build a production-grade FRED API ingestion pipeline that will
eventually live in **Databricks** and can be handed either to:

1.  An engineering/database team
2.  An LLM/code agent for implementation

The system should be manifest-driven, scalable, auditable,
metadata-rich, and suitable for quant research, dashboards, optimizer
inputs, and point-in-time macro features.

------------------------------------------------------------------------

# Implementation Status (current)

The original FRED-only spec below is fully realized and the pipeline has been
generalized well past it. Current state:

-   **Multi-source (8):** FRED, BLS, EIA, US Treasury, World Bank, BEA, Census,
    SEC — a pluggable `SourceClient` layer (adding a source = one client module +
    one registry entry). See **Multi-Source Ingestion**.
-   **~2,380 series** across the FRED domain manifests + inactive demo manifests
    for the seven other sources (incl. the NSA + SA **CPI-U baskets**).
-   **Medallion with lineage:** `source` is part of the Silver natural key
    `(source, series_id, observation_date, realtime_start)`; Bronze records
    source + endpoint; replay is source-aware.
-   **Loading:** full-on-first-run then restate-last-N incremental;
    replay-from-Bronze; point-in-time (ALFRED) vintages.
-   **Gold layer complete:** latest / point-in-time / daily feature matrix;
    feature transforms; config-driven curve spreads; **frequency-aware N-leg
    cross-series features** + a **leak-free point-in-time variant**; **governance**
    (coverage/freshness view + cross-source reconciliation); **SEC company
    financials** (standardized statements, ratios, cross-company ranks, with
    quarterly/annual duration disambiguation incl. Q4 de-cumulation). All seven
    Gold-roadmap items are implemented (see the roadmap section).
-   **Engineering:** pure-core + lazy-Spark shell; Databricks/Delta **and** local
    SQLite backends in parity; layered config (file/env/args/secret scope);
    audit + data-quality profiles; Unity Catalog DDL; Asset Bundle (main +
    per-source jobs); **~289 unit tests + a Spark/Delta integration job in CI**.
-   **Docs:** this handoff, `README.md`, `docs/architecture.md`,
    `data_dictionary.md`, `validation.md`, `adding_a_source.md`,
    `deployment_runbook.md`, `incremental_loading.md`, `etl_build_spec.md`.

**Open (non-blocking, documented):** go-live provisioning + live series-ID
verification (`docs/deployment_runbook.md`); per-source metadata reconciliation
for non-FRED sources (`reconcile`/`discover` are FRED-only); optional new sources
(markets/OHLCV, the SDMX bundle) and a full SEC manifest generator.

------------------------------------------------------------------------

# Target Platform

Use Databricks as the long-term home:

-   Unity Catalog for governance, access control, lineage, and discovery
-   Delta Lake (Bronze / Silver / Gold)
-   Databricks Workflows / Jobs
-   Databricks Secrets
-   Lakeflow Spark Declarative Pipelines (where appropriate)
-   Databricks Asset Bundles for CI/CD and deployment

------------------------------------------------------------------------

# High-Level Architecture

``` text
Source APIs (FRED · BLS · EIA · Treasury · World Bank · BEA · Census · SEC)
    ↓
Python Ingestion Package (pluggable source clients)
    ↓
Unity Catalog Volume / Raw Archive
    ↓
Bronze Delta Tables
    ↓
Silver Normalized Tables
    ↓
Gold Feature Store
    ↓
Dashboards / SQL Warehouse / Optimizers / ML
```

Recommended Unity Catalog layout:

``` text
macro_dev
macro_test
macro_prod

Schemas:
- meta
- audit
- bronze
- silver
- gold
- sandbox
```

------------------------------------------------------------------------

# Repository Structure

``` text
fred-databricks-etl/
├── manifests/
├── resources/
├── sql/
├── src/
├── notebooks/
├── tests/
└── docs/
```

------------------------------------------------------------------------

# Core Design Principles

-   Manifest-driven ingestion
-   Pluggable multi-source ingestion (FRED, BLS, EIA, ...)
-   Metadata-driven orchestration
-   Delta MERGE for idempotent loads
-   Raw payload retention
-   Point-in-time (vintage) support
-   Complete auditability
-   Reusable feature store
-   Infrastructure-as-code
-   Environment promotion (Dev → Test → Prod)

------------------------------------------------------------------------

# Core Delta Tables

## Meta

-   meta.fred_series
-   meta.fred_manifest
-   meta.fred_series_manifest_map
-   meta.fred_series_lifecycle   (FRED metadata reconciliation)
-   meta.fred_series_drift       (manifest-vs-source drift)

## Audit

-   audit.etl_run
-   audit.etl_series_run
-   audit.data_quality_result

## Bronze

-   bronze.fred_api_response   (multi-source: carries a `source` column + the endpoint actually called)

## Silver

-   silver.fred_observation   (natural / MERGE key: source, series_id, observation_date, realtime_start)

## Gold

-   gold.fred_latest_observation
-   gold.fred_point_in_time
-   gold.fred_macro_feature_daily
-   gold.fred_feature_transforms
-   gold.fred_curve_spread
-   gold.fred_cross_series_feature
-   gold.fred_cross_series_feature_pit
-   gold.fred_source_reconciliation
-   gold.fred_company_fundamentals
-   gold.fred_company_ratios
-   gold.fred_revision_stats

------------------------------------------------------------------------

# Databricks Workflow

1.  Validate Manifest
2.  Initialize Run
3.  Read Manifest
4.  Extract Source Data (per series `source`: FRED / BLS / EIA / Treasury /
    World Bank / BEA / Census / SEC)
5.  Write Bronze
6.  Transform to Silver
7.  Run Data Quality
8.  Build Gold Features
9.  Close Audit
10. Notify

------------------------------------------------------------------------

# Manifest Requirements

Each manifest should define:

-   series_id
-   title
-   category
-   frequency
-   units
-   active
-   source            (upstream API: fred (default) / bls / eia)
-   load_type
-   expected_update_frequency
-   vintage_enabled
-   validation_profile
-   business_owner
-   technical_owner
-   downstream_use_case
-   priority

------------------------------------------------------------------------

# Point-in-Time Support

Persisted per Silver observation (see `silver.fred_observation` /
`transform.SILVER_COLUMNS`):

-   source
-   series_id
-   observation_date
-   realtime_start
-   realtime_end
-   value
-   revision_number   (1..N per (series_id, observation_date), by realtime_start)
-   is_missing
-   ingested_at

Vintage history is captured as the set of `realtime_start` rows per
observation_date rather than separate `first_seen_at` / `latest_seen_at`
columns: the earliest `realtime_start` is the first print and the latest is the
current revision. `ingested_at` records when each row was loaded.

Maintain two analytical views (implemented as `gold.v_latest_revised` /
`gold.v_point_in_time`):

-   latest_revised
-   point_in_time

------------------------------------------------------------------------

# Security

-   Store API keys in Databricks Secret Scopes (fred_api_key, plus optional
    bls_api_key / eia_api_key in the same scope)
-   Never hardcode credentials
-   Parameterize catalog/schema names
-   Support Dev/Test/Prod deployments

------------------------------------------------------------------------

# LLM Build Specification

The implementation should:

1.  Read YAML manifests.
2.  Validate manifests.
3.  Retrieve FRED observations.
4.  Store raw payloads in Bronze.
5.  Transform to Silver.
6.  Perform Delta MERGE.
7.  Record audit metadata.
8.  Execute validation rules.
9.  Build Gold feature tables.
10. Refresh analytical views.
11. Use Databricks Secrets.
12. Include unit tests.
13. Include SQL DDL.
14. Include Databricks Asset Bundle configuration.

------------------------------------------------------------------------

# Suggested Initial Series

> **Historical seed set.** These 27 FRED series were the original hand-picked
> universe. The live pipeline has since grown to **~2,380 series across 8
> sources** — the FRED domain manifests (rates, inflation, labor, growth,
> money/banking, prices, production/housing, international) plus the BLS/EIA/
> Treasury/World Bank/BEA/Census/SEC demo manifests (see **Multi-Source
> Ingestion** below and `manifests/`). This section is kept for provenance; it is
> not the current universe.

## Rates

-   DGS1MO
-   DGS3MO
-   DGS6MO
-   DGS1
-   DGS2
-   DGS5
-   DGS10
-   DGS30
-   FEDFUNDS
-   SOFR

## Inflation

-   CPIAUCSL
-   CPILFESL
-   PCEPI
-   PCEPILFE
-   T5YIE
-   T10YIE

## Labor

-   UNRATE
-   PAYEMS
-   ICSA
-   CIVPART
-   JTSJOL

## Growth

-   GDP
-   GDPC1
-   INDPRO
-   RSAFS
-   HOUST
-   PERMIT

## BLS CPI Basket (`source: bls`, `manifests/bls_cpi_basket.yml`)

The full CPI-U item hierarchy from BLS (FRED mirrors only a subset). CPI-U, NSA,
U.S. city average — series id `CUUR0000<item>`. A seasonally-adjusted companion
(`manifests/bls_cpi_basket_sa.yml`, `CUSR0000<item>`) mirrors the SA-published
aggregates/majors/common sub-items. Inactive by default; **item codes to be
verified against the live BLS series directory before activating.**

- **Headline / special aggregates:** SA0 (All items), SA0L1E (Core), SA0E
  (Energy), SAC (Commodities), SAS (Services), SACL1E (Core goods), SASLE (Core
  services)
- **8 major groups:** SAF (Food & beverages), SAH (Housing), SAA (Apparel), SAT
  (Transportation), SAM (Medical care), SAR (Recreation), SAE (Education &
  communication), SAG (Other goods & services)
- **Food:** SAF1 (Food), SAF11 (Food at home), SEFV (Food away from home), SAF116
  (Alcoholic beverages)
- **Housing:** SAH1 (Shelter), SEHA (Rent of primary residence), SEHC (Owners'
  equivalent rent), SAH2 (Fuels & utilities), SEHF01 (Electricity), SEHF02
  (Utility/piped gas)
- **Transportation:** SETA01 (New vehicles), SETA02 (Used cars & trucks), SETB01
  (Gasoline, all types)
- **Medical:** SAM1 (Medical care commodities), SAM2 (Medical care services)

SA (seasonally adjusted) variants use the `CUSR0000<item>` prefix and are shipped
as `manifests/bls_cpi_basket_sa.yml` (29 series — SA is published for the
aggregates/majors/common sub-items, not the full stratum set).

------------------------------------------------------------------------

# Gold Layer Feature Engineering Roadmap

Identified once the series universe grew beyond the initial seed set (the 27
FRED "Suggested Initial Series" → **~2,380 series across 8 sources**: FRED
[rates, inflation, labor, growth, money/banking, prices, production/housing,
international], plus BLS, EIA, US Treasury, World Bank, BEA, Census, and SEC). All
seven roadmap items below are now **implemented** (see each item's status); this
section is retained as the design record of how they came to be.

## 1. Point-in-time-safe (rolling) z-score --- correctness fix

**Status: implemented** (`features.py::compute_feature_transforms` /
`gold_polars.py::compute_feature_transforms_frame`, expanding z-score).

`gold.fred_feature_transforms.zscore` was previously computed from the
**full-sample** mean/std of each series (past *and* future observations
relative to any given row's date). That leaks future information into
historical rows, which conflicts with the pipeline's stated leak-free,
point-in-time design goal. Replace with a rolling or expanding-as-of-date
z-score (mean/std computed only from observations at-or-before each row's
date) so every row only reflects what was knowable as of that date.

## 2. Revision-magnitude / vintage-volatility Gold table

**Status: implemented** (`gold.fred_revision_stats` table +
`gold.v_series_revision_summary` view; `features.py::compute_revision_stats`
/ `gold_polars.py::compute_revision_stats_frame` / `gold.py::
_revision_stats_sql`).

The pipeline already retains full point-in-time vintage history
(`gold.fred_point_in_time`) but doesn't surface *how much* a series
typically gets revised. A new Gold table --- e.g. `gold.fred_revision_stats`
--- could capture, per series (and optionally per observation_date):

-   first-release value vs. latest-revised value (revision delta / % change)
-   number of revisions per observation_date
-   average / max revision magnitude over a trailing window

This is uniquely cheap to produce here (the vintage data is already
captured) and is valuable for quant researchers assessing how much to trust
a series' initial print (e.g. GDP/payrolls are heavily revised; market
series are not).

## 3. Config-driven spreads/ratios (generalize `curve_spread`)

**Status: implemented** (`config/spreads.yml` +
`fred_pipeline.spread_config.load_spread_defs`; wired into
`features.py::compute_curve_spreads`, `gold_polars.py::
compute_curve_spreads_frame`, and `gold.py::_curve_spread_sql` / the
static `sql/50_gold.sql` mirror).

`gold.fred_curve_spread` used to hardcode 4 Treasury curve pairs
(`DEFAULT_CURVE_SPREADS` in `features.py`). It's now driven by a reviewable
YAML file, `config/spreads.yml`, with each entry a `(name, long_leg,
short_leg, op)` definition — `op: spread` (long − short) or `op: ratio`
(long / short, guarded against a zero short leg). Ships with the same 4
Treasury pairs as before (no output change until new entries are added).
Now that the series universe spans rates, prices, labor, production/housing,
international, and national accounts, new cross-series features (e.g. real
yields = nominal minus breakeven inflation, credit spreads = corporate yield
minus Treasury, PCE vs. CPI divergence) can be added by editing that YAML —
no Python or SQL changes needed for the Python-driven backends (local SQLite
and Databricks/Spark). **Still needs quant sign-off on which new pairs to
add** — the mechanism is built, but no new pairs beyond the original 4 have
been added, since that's a domain judgment call, not an engineering one.

## 4. Frequency-aware, N-leg cross-series features

**Status: implemented** (`config/cross_series.yml` +
`fred_pipeline.cross_series_config`; engine
`fred_pipeline.features.compute_cross_series_features`, reused by both backends;
new table `gold.fred_cross_series_feature`).

`fred_curve_spread` (item 3) only combines **same-frequency, 2-leg** series on a
shared date grid. With the multi-source universe (daily Treasury, quarterly BEA,
annual World Bank, monthly Census…), useful features cross both **frequency** and
**source**. This adds an as-of alignment step — each leg is downsampled to a
target `frequency` (the last observation within each period) — plus **N-leg**
combinations: `spread` (a−b), `ratio` (a/b), and `composite` (Σ weightᵢ·legᵢ).
Ships three illustrative examples (real 10y yield; debt-to-GDP across
Treasury÷BEA; a monthly activity composite). **Same governance note as item 3**:
the mechanism is built; which features to compute (and their weights) is a quant
sign-off.

## 5. Governance Gold objects (coverage + cross-source reconciliation)

**Status: implemented.** Two governance views/tables over the multi-source data:

-   **`gold.v_source_coverage`** (pure view; `sql/60_views.sql` +
    `local_store.py`) — per `(source, series_id)`: latest observation, count,
    days-since, and an `is_stale` verdict from the manifest cadence. Operational
    visibility across the 8 sources.
-   **`gold.fred_source_reconciliation`** (config-driven table;
    `config/reconciliations.yml` + `fred_pipeline.reconciliation_config`; engine
    `fred_pipeline.features.compute_source_reconciliation`, reused by both
    backends) — same-concept series from different sources (FRED `UNRATE` vs BLS
    `LNS14000000`; FRED vs BEA GDP) aligned and compared with a `diverged` flag.
    A pure view can't do this (series ids differ by source with no join key), so
    the concept pairs are declared in YAML.

## 6. Point-in-time cross-series features (leak-free)

**Status: implemented** (`fred_pipeline.features.compute_cross_series_features_pit`;
new table `gold.fred_cross_series_feature_pit`).

The cross-series features (item 4) use *latest-revised* values — fine for
dashboards, but they inject later revisions into historical points (look-ahead
bias). This adds a **`realtime_start`-aligned** variant: each leg contributes the
value that was actually known (as-first-reported by default, or as-of any date),
so the feature series is leak-free for backtests. Reuses the same alignment/
combine logic (both backends share the one Python engine), reads raw Silver for
vintages. Identical to latest-revised for non-vintage series.

## 7. SEC statement standardization + company financials

**Status: implemented** (`config/sec_concepts.yml` + `config/sec_ratios.yml` +
`fred_pipeline.sec_standardization`; tables `gold.fred_company_fundamentals` /
`gold.fred_company_ratios`; view `gold.v_company_ratio_ranks`).

The SEC source lands one raw XBRL concept per series; companies use different
tags for the same line item. This standardizes raw tags → canonical concepts via
a priority-ordered mapping, then computes derived ratios, then ranks companies
cross-sectionally. **Restatement analytics come free** from
`gold.fred_revision_stats` (SEC filings carry `filed`-date vintages, which already
flow through it). Both backends share the one Python engine (SQL can't do the
priority tag-coalescing cleanly).

**Duration disambiguation: implemented.** Income-statement facts carry a
duration; a single 10-Q reports both the quarterly (~3-month) and YTD (~9-month)
figure for the same period end, which collided on the natural key. The SEC
normalizer now keeps only facts matching a target duration — `SEC_PERIOD`
(default `quarterly`, or `annual`) — so income concepts land as a consistent
series and ratios like net margin use matching durations. Instant balance-sheet
facts are always kept; Bronze replay resolves `SEC_PERIOD` identically. A 10-K
reports the FY (12-month) figure, not Q4, so **Q4 is synthesized by
de-cumulation** (`Q4 = FY − 9-month YTD`, dated at the FY end, known as of the
10-K filing) — giving a complete quarterly series. This is the last item on the
Gold roadmap; the remaining open work is non-Gold (per-source metadata
reconciliation for non-FRED sources; optional new sources).

------------------------------------------------------------------------

# Market Terminal Analytical Views (Power BI Gold plan)

**Status: ALL PHASES (0–6) implemented** (branch
`EconGoldTerminalViews`; 3c = rolling-window stats companions
`gold.curve_spread_rolling` / `credit_spread_rolling` /
`treasury_curve_rolling`, windows 1/5/10/21/63/126/252 obs; 5 = the regime
playbook `gold.macro_regime_daily` [five pillar scores from
`config/regime.yml`, ordered rule table, official conditions indices driving
liquidity/credit] plus the statistical lab `gold.series_correlation` /
`gold.series_lead_lag` [curated pairs in `config/stats_pairs.yml`,
rolling/expanding Pearson, ±12-lag CCF, two-direction Granger F with
pure-Python p-values]; 6 = the global tables `gold.global_inflation` /
`gold.global_policy_rates` [GCPI/GPOL, `config/global_series.yml`, US rows
live off active series, World Bank/ECB entries inactive verify-first] plus
`gold.powerbi_catalog`, the report author's manifest of every Gold object,
kept current by a test) —
full spec + per-phase status in `docs/market_terminal_gold_views.md`; column
semantics in `docs/data_dictionary.md`.

Implemented: `gold.dim_series` + `gold.dim_date` (star-schema dimensions;
`config/series_catalog.yml` tags ~65 already-ingested series with category/
polarity/transform), the ECON macro dashboard
(`gold.macro_indicator_dashboard` / `macro_indicator_sparkline` /
`macro_category_summary`), and the Treasury Curve Lab (`gold.treasury_curve` /
`treasury_curve_metrics` / `curve_spread_daily`, plus
`gold.spread_inversion_episode` — one row per unique inversion period per
spread, opening on the first negative print and closing when the spread turns
non-negative), and the Phase-4 rates complex — the BMRK benchmark board
(`gold.benchmark_rate_board`: change/trend/spread-to-benchmark/regime per
configured rate), the FUND funding tape + 0–100 stress gauge
(`gold.funding_tape_daily` / `funding_stress_daily`), and CRDT credit spreads
(`gold.credit_spread_daily`: ICE BofA OAS with percentile-threshold stress
episodes) — all via the shared pure-Python engines in
`src/fred_pipeline/terminal_views.py` wired into both backends, configured by
`config/benchmark_rates.yml` / `funding.yml` / `credit.yml`. New manifests
ship inactive pending live-FRED verification: `macro_flags.yml`
(`USREC`/`USRECD` — recession overlays are NULL until activated),
`fed_funding.yml` (EFFR/IORB/OBFR/BGCR/TGCR/RRPONTSYD/SOFR30DAYAVG),
`ice_credit.yml` (9 OAS indices), and `DGS3/DGS7/DGS20` + `DPRIME`/
`MORTGAGE30US` in `rates.yml`; absent series emit no Gold rows until
activated, so the tables populate progressively. Phase 2's Inflation
Explorer (`gold.inflation_explorer` / `inflation_contribution`,
`config/inflation_items.yml`) ships three item trees — the SA tree is rooted
at already-active `CPIAUCSL`/`PCEPI` so headline rows appear immediately; the
CUUR/CUSR item drill-down fills in when the CPI basket manifests are
activated, and the shipped group weights are approximate (refresh from the
BLS relative-importance table). The build plan is complete; what remains is
operational — verify + activate the inactive manifests (USREC, funding
corridor, OAS, CPI baskets, DGS3/7/20, World Bank CPI, ECB rates), refresh
the CPI weights, tune the regime thresholds once real history is loaded —
plus two deferred code items: PCE item-level via BEA, and the optional
starter `.pbix` (plan doc open question #4).

A separate project, `market_terminal` (a Bloomberg-style quant terminal),
renders its macro analytics (ECON macro dashboard, INFL inflation explorer, CURV
treasury-curve lab, BMRK benchmark rates, FUND funding tape, CRDT credit spreads,
REGIME regime playbook, STAT/EDA stats) on the fly in TypeScript from a
166-series FRED subset. The plan recreates those surfaces as **precomputed
Gold-layer tables/views for Power BI**, gaining point-in-time correctness the
terminal lacks.

Key facts for whoever picks this up:
- **Uses the existing catalog first.** The pipeline already ingests ~2,380
  series — a superset of the terminal's 166 — including the official
  financial-conditions/stress indices (`NFCI`, `ANFCI`, `STLFSI4`), sticky/
  flexible/trimmed CPI, breakevens, the Fed balance sheet, and the SOFR complex.
  Every planned Gold object draws from these; a new `config/series_catalog.yml`
  (+ `gold.dim_series`) tags each into an `econ_category`, polarity, and default
  transform.
- **~15 free-FRED gaps only:** `DGS3/DGS7/DGS20`, `USREC`, the funding corridor
  (`IORB/EFFR/OBFR/BGCR/TGCR/RRPONTSYD`), ICE BofA OAS (`BAMLH0A0HYM2`,
  `BAMLC0A0CM`, rating/sector), `DPRIME/MORTGAGE30US`. PCE item level via the
  already-wired BEA API; global CPI/policy via the free World Bank/OECD path. **No
  paid feeds.**
- **Same engineering pattern:** one pure-Python engine per metric shared by both
  backends, YAML-driven config, PIT-safe rolling stats, provenance/staleness on
  every row. New Gold objects are all additive (see the doc's §3 table).
- Delivered in 6 phases (dimensions → ECON → INFL → curve → rates complex →
  regime/stats → global + Power BI catalog). Four open questions for the user are
  listed at the end of the doc (PCE scope, regime rule table, correlation scope,
  `.pbix` delivery).

------------------------------------------------------------------------

# Multi-Source Ingestion

**Status: implemented** (`src/fred_pipeline/sources/`,
`pipeline.SOURCE_FACTORIES`; full guide in `docs/adding_a_source.md`).

The pipeline began FRED-only, but the FRED-specific surface is small and
isolated, so it was generalized to ingest additional public APIs through the
*same* Bronze → Silver → DQ → Gold → audit path. A manifest series declares its
upstream API with a `source:` field (default `fred`); everything downstream is
source-agnostic.

## Implemented sources

-   **FRED** — refactored onto a shared HTTP transport; behavior unchanged.
-   **BLS** (Bureau of Labor Statistics) — `source: bls`. Key optional (keyless
    works at a lower quota). Demo manifests (inactive): `manifests/bls_labor.yml`
    (unemployment); `manifests/bls_cpi_basket.yml` — the full **CPI-U item
    hierarchy** (30 series: headline + special aggregates, the 8 major groups,
    and key sub-strata), **NSA** (`CUUR0000<item>`); and
    `manifests/bls_cpi_basket_sa.yml` — the **seasonally-adjusted** companion (29
    series, `CUSR0000<item>`), covering the SA-published aggregates/majors/common
    sub-items (SA is narrower than NSA). FRED mirrors only a subset of BLS's CPI
    series, so the whole consumer basket lives here. **Item codes were assembled
    from the documented CPI structure and must be verified against the live BLS
    series directory before activating.**
-   **EIA** (Energy Information Administration) — `source: eia`. Key required.
    Demo manifest `manifests/eia_energy.yml` (inactive).
-   **US Treasury** (Fiscal Data) — `source: treasury`. **Keyless.** series_id
    encodes `<dataset_path>:<field>`. Demo `manifests/treasury_fiscal.yml`.
-   **World Bank** (Indicators) — `source: worldbank`. **Keyless.** series_id
    encodes `<country>:<indicator>`. Demo `manifests/worldbank_global.yml`.
-   **BEA** (National accounts) — `source: bea`. Key required. series_id encodes
    `<dataset>:<table>:<line>:<freq>`. Demo `manifests/bea_national_accounts.yml`.
-   **Census** (economic time series) — `source: census`. **Keyless** (key
    optional). series_id encodes `<dataset_path>:<predicate=value,...>`. Demo
    `manifests/census_indicators.yml`.
-   **SEC** (company financials) — `source: sec`. **Keyless** (needs a
    descriptive User-Agent). series_id encodes `<CIK>:<taxonomy>/<tag>:<unit>`;
    filings are captured as point-in-time vintages. Demo
    `manifests/sec_financials.yml`; generate at scale via
    `sources.sec.build_sec_manifest`.

## How it works

-   `sources/base.py::HTTPSource` holds the shared rate limiter, retry/backoff,
    and request engine. The `SourceClient` protocol (`get_observations` +
    `normalize`) is the entire surface the orchestrator depends on, so a new
    source is **one client module plus one `SOURCE_FACTORIES` entry** — nothing
    in Bronze/Silver/Gold moves.
-   **`source` is part of the Silver natural key**:
    `(source, series_id, observation_date, realtime_start)`. Bronze also records
    `source` and the endpoint actually called, and the Bronze→Silver replay
    re-derives each payload with the correct per-source normalizer.
-   **Keys/secrets**: `bls_api_key` / `eia_api_key` config settings, `BLS_API_KEY`
    / `EIA_API_KEY` env vars, or the same Databricks secret scope as FRED
    (`secrets/<scope>/bls_api_key`, `.../eia_api_key`). The CLI and notebook
    entrypoints require only the keys the *active* sources need.
-   **Deployment**: run everything in one job, or one job per source
    (`resources/source_jobs.yml` — paused `bls_ingestion` / `eia_ingestion`
    templates scoped to their manifest files).

## Still requires (not engineering)

-   Quant sign-off on which non-FRED series to activate.
-   A live check of the demo series IDs once API keys are configured (blocked in
    the build environment by egress policy; IDs are structurally verified).

------------------------------------------------------------------------

# Equity Price & Total Return — Two-Source Sub-Plan (Stooq + Tiingo)

**Status: planned** (not yet implemented). Adds an *equities* asset class —
stock and broad-ETF price return and true total return — as two new
`source:` clients plus a constituent-list ingester, reusing the existing
Bronze → Silver → DQ → Gold → audit path unchanged.

> ⚠️ **Verify before building.** Free-tier terms in market data shift often
> and could not be checked live from this environment. Confirm current quotas
> **and licensing** on each provider's site first. In particular, **Tiingo's
> free tier is personal / non-commercial** — commercial use needs a paid
> plan; that is a licensing decision, not an engineering one.

## Why two sources

No single free API gives both breadth *and* the dividend data needed for
total return. Split the job:

-   **Stooq — breadth (price return).** Keyless, no account, effectively
    unlimited; publishes a **bulk EOD ZIP of the entire US market** plus
    per-ticker daily CSV. **Split-adjusted close only — no dividends**, so it
    yields *price* return for thousands of tickers at near-zero API cost. This
    is the wide net.
-   **Tiingo — depth (total return).** Free with a (free) account + key;
    metered (~1,000 req/day, ~50/hr, and the binding limit **~500 unique
    symbols/month**). Its daily endpoint returns, per ticker, `close`,
    **`adjClose`, `divCash`, and `splitFactor` in one call** — the only free
    tier that hands you dividend cash amounts, so Gold can compute true total
    return. Reserve it for the core list: S&P 500 constituents + the broad
    ETFs (SPY/VTI/QQQ/IWM/EFA/EEM/AGG/TLT/HYG/GLD…). ~530 symbols brushes the
    500/month cap — trim to top weights + ETFs, or let Yahoo (fragile,
    unofficial) cover overflow.

**Design bias — store inputs, derive returns.** Prefer the raw
`close`/`divCash`/`splitFactor` over a pre-adjusted number: computing total
return in Gold from raw inputs (below) is reproducible and survives dividend
restatements, exactly the Bronze-verbatim → Gold-derived principle the rest
of the pipeline follows. Pre-adjusted feeds (Yahoo's `Adj Close`) can't be
rebuilt when a dividend is corrected.

## Constituent list — daily ETF-holdings CSV (the hard part, solved freely)

Index *membership* is licensed data with no good free API. The free,
defensible workaround: **iShares and State Street publish full ETF holdings
as daily CSV/XLSX** (e.g. iShares **IVV** "Detailed Holdings and Analytics"
CSV; State Street **SPY** holdings file). Ingest that file to get, per day,
the constituent tickers **and their weights**. It serves two roles:

1.  It *is* the symbol universe — `build_equity_manifest(holdings_csv)`
    generates the ticker manifest Stooq/Tiingo pull (mirrors
    `sources.sec.build_sec_manifest`), so the pull list tracks index changes
    automatically instead of being hand-maintained.
2.  It feeds `gold.index_constituents` (ticker × weight × as-of × index),
    the weight source for any index-reconstruction or attribution Gold table.

This is a scraper/CSV-puller, not a JSON API, but it still fits `HTTPSource`
(fetch bytes → normalize); the daily holdings file is captured verbatim in
Bronze and exploded in Silver.

## How it fits the existing abstraction

-   **New `source:` values `stooq`, `tiingo`, `ishares` (holdings).** Each is
    **one client module + one `SOURCE_FACTORIES` entry** (`sources/stooq.py`
    ~ the size of `census.py` since it's CSV-over-HTTP; `sources/tiingo.py` ~
    `eia.py`; `sources/ishares.py` for holdings). Nothing in Bronze/Silver/
    Gold moves. `source` is already in the Silver natural key.
-   **Scalar-explode to fit the existing single-`value` Silver, recommended.**
    Silver is a scalar `(source, series_id, observation_date, value, …)`
    model; a stock bar is multi-field. Encode `series_id = <ticker>:<field>`
    (e.g. `AAPL:close`, `AAPL:adjClose`, `AAPL:divCash`, `AAPL:splitFactor`,
    `AAPL:volume`) — the same composite-id convention Treasury
    (`<dataset>:<field>`) and World Bank (`<country>:<indicator>`) already
    use. This keeps **every** downstream mechanism (DQ, replay, PIT, the Gold
    transforms) working with zero schema change; the trade-off is more series
    rows. *Alternative:* a dedicated wide `silver_equity_bar` table — more
    natural for OHLCV but breaks the one-path principle and needs its own
    Gold; only adopt it if true bar semantics are needed downstream.
-   **Not revised → `vintage_enabled: false`, blank `realtime_start`** (same
    as `rates.yml` market data). Incremental restate-last-N still applies:
    pull the last ~5 observations each run to catch late split/dividend
    corrections.
-   **Keys**: `tiingo_api_key` config setting / `TIINGO_API_KEY` env var /
    Databricks secret scope, exactly like `eia_api_key`; Stooq and the
    holdings CSV are keyless. The CLI only demands the key when a `tiingo`
    series is active.

## New Gold objects

Computed by the shared-Python-engine pattern (one function, both backends),
like every other Gold table:

-   **`gold.equity_total_return_index`** — the payoff of this whole plan.
    `TR_t = TR_{t−1} × (P_t + D_t) / P_{t−1}` from raw `close` + `divCash`
    (dividends reinvested), indexed to 100 at each series' start. One row per
    ticker × date. Rebuildable when a dividend is restated.
-   **`gold.equity_return_daily`** — price return from `adjClose` (already
    split-adjusted) and total return day-over-day, per ticker × date; MoM/YoY/
    trailing-window horizons reuse the existing transform + rolling-window
    engines directly.
-   **`gold.index_constituents`** — ticker × weight × shares × as-of × index,
    exploded from the holdings CSV.
-   **Free reuse:** point Stooq's and Tiingo's `close` for the same ticker at
    the existing **cross-source reconciliation** engine
    (`config/reconciliations.yml`) to flag price disagreements — no new code.

## Open decisions (for the user)

1.  **Which repo owns equities.** The companion `market_terminal` project
    already has a `market_data_pipeline` (FRED + Yahoo, DuckDB/Parquet/Polars)
    that owns the Yahoo/equity lane. Deliberately choose: fold equities into
    *this* medallion pipeline (gains PIT/DQ/audit/total-return-from-inputs), or
    keep them there and let this pipeline stay macro-only.
2.  **Scalar-explode vs. a wide `silver_equity_bar`** (recommendation:
    scalar-explode — see above).
3.  **Tiingo symbol budget** — trim the core list under the ~500/month cap, or
    accept Yahoo overflow for the tail (and its fragility/ToS risk).
4.  **Commercial use** — if this is commercial, Tiingo's free tier doesn't
    cover it; decide paid Tiingo vs. Stooq-only (price return only).

------------------------------------------------------------------------

# Engineering Handoff Checklist

> Actionable, checkbox form with commands and per-item owners:
> **`docs/deployment_runbook.md`** (workspace provisioning + quant/ops decisions).

## Quant

-   Define series universe
-   Approve validation rules
-   Identify vintage-sensitive series
-   Validate outputs

## Engineering

-   Create Unity Catalog objects
-   Configure Delta tables
-   Configure workflows
-   Configure Asset Bundles (incl. per-source jobs in resources/source_jobs.yml)
-   Configure secret scopes (fred_api_key + optional bls_api_key / eia_api_key)

## Platform

-   Configure permissions
-   Configure monitoring
-   Configure alerts
-   Configure cost controls

------------------------------------------------------------------------

# First Sprint

Deliver:

-   Databricks project
-   YAML manifests
-   Python package
-   Bronze/Silver/Gold tables
-   Audit framework
-   Data quality checks
-   Initial Databricks Workflow
-   Documentation

The objective is a production-shaped MVP that can scale to hundreds of
FRED series while remaining governed, auditable, and reusable for quant
research and optimization workflows.
