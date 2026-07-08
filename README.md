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
