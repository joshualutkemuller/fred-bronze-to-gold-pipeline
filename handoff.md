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
FRED API
    ↓
Python Ingestion Package
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

## Audit

-   audit.etl_run
-   audit.etl_series_run
-   audit.data_quality_result

## Bronze

-   bronze.fred_api_response

## Silver

-   silver.fred_observation

## Gold

-   gold.fred_latest_observation
-   gold.fred_point_in_time
-   gold.fred_macro_feature_daily

------------------------------------------------------------------------

# Databricks Workflow

1.  Validate Manifest
2.  Initialize Run
3.  Read Manifest
4.  Extract FRED Data
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

Persist:

-   observation_date
-   realtime_start
-   realtime_end
-   value
-   first_seen_at
-   latest_seen_at
-   revision_number

Maintain two analytical views:

-   latest_revised
-   point_in_time

------------------------------------------------------------------------

# Security

-   Store API keys in Databricks Secret Scopes
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

------------------------------------------------------------------------

# Gold Layer Feature Engineering Roadmap

Identified once the series universe grew beyond the initial seed set (27 →
2,300+ series across rates, inflation, labor, growth, money/banking, prices,
production/housing, and international domains). Not yet implemented.

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

`gold.fred_curve_spread` currently hardcodes 4 Treasury curve pairs
(`DEFAULT_CURVE_SPREADS` in `features.py`). Now that the series universe
spans rates, prices, labor, production/housing, international, and national
accounts, a manifest-style YAML list of `(name, long_leg, short_leg, op)`
definitions --- spreads *and* ratios --- would let new cross-series features
(e.g. real yields = nominal minus breakeven inflation, credit spreads =
corporate yield minus Treasury, PCE vs. CPI divergence) be added by
reviewers without touching Python, the same way series are added today.

**Prioritization:** (1) is a correctness fix to existing tested code and
should land first; (2) is net-new and additive; (3) is a design/config
decision that needs quant sign-off on which pairs matter before
implementation.

------------------------------------------------------------------------

# Engineering Handoff Checklist

## Quant

-   Define series universe
-   Approve validation rules
-   Identify vintage-sensitive series
-   Validate outputs

## Engineering

-   Create Unity Catalog objects
-   Configure Delta tables
-   Configure workflows
-   Configure Asset Bundles
-   Configure secret scopes

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
