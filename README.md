# fred-bronze-to-gold-pipeline

A production-grade, **manifest-driven** FRED (Federal Reserve Economic Data)
ingestion pipeline that lands data in a **Bronze → Silver → Gold** medallion
architecture on **Databricks + Delta Lake**. Built for quant research,
dashboards, optimizer inputs, and **point-in-time** macro features.

> 📄 The original product spec / engineering handoff lives in
> [`handoff.md`](./handoff.md). This README is the practical guide to the
> implementation. Architecture rationale is in
> [`docs/architecture.md`](./docs/architecture.md); every table/column is in
> [`docs/data_dictionary.md`](./docs/data_dictionary.md).

## What it does

```
manifests/*.yml → FRED API → Bronze (raw JSON) → Silver (normalized, MERGE)
     → Data Quality → Gold (latest / point-in-time / daily feature matrix)
     → full audit trail (runs, series, DQ results)
```

* **Manifest-driven** — the series universe and per-series policy (load type,
  validation profile, vintage tracking, ownership) live in reviewable YAML.
* **Idempotent** — Silver is a Delta `MERGE` on
  `(series_id, observation_date, realtime_start)`; re-runs never duplicate.
* **Point-in-time** — vintage-enabled series retain every revision, enabling
  leak-free backtests ("what was known on date X").
* **Auditable** — every run, series-run, and data-quality check is persisted.
* **Testable** — the business logic is pure Python (no Spark/network), covered
  by a fast unit-test suite; PySpark is imported lazily only for I/O.
* **Promotable** — one codebase targets `macro_dev` / `macro_test` /
  `macro_prod` via config + a Databricks Asset Bundle.

## Repository layout

```
fred-bronze-to-gold-pipeline/
├── manifests/            # YAML series universe + JSON schema
├── src/fred_pipeline/    # the Python package (pure core + Spark I/O)
├── sql/                  # Unity Catalog DDL (00..60, parameterized by {{catalog}})
├── resources/            # Databricks Asset Bundle job definition
├── databricks.yml        # Asset Bundle (dev/test/prod targets)
├── notebooks/            # Databricks job entrypoint
├── tests/                # pytest suite (no Spark required)
└── docs/                 # architecture + data dictionary
```

## Quickstart (local)

The pipeline has three storage backends, chosen at run time:

| Mode | Flag | Writes to | Needs Spark? |
|---|---|---|---|
| Dry run | `--dry-run` | nothing (extract + DQ only) | no |
| **Local** | `--local` | a **SQLite `.db` file** | **no** |
| Databricks | *(default)* | Delta / Unity Catalog | yes |

```bash
# 1. Install dev dependencies
pip install -r requirements-dev.txt

# 2. Validate the manifests (no network, no Spark)
PYTHONPATH=src python -m fred_pipeline validate --manifests manifests

# 3. Run the fast unit-test suite
python -m pytest

# 4. Dry-run against the real FRED API (extract + DQ, no writes)
export FRED_API_KEY=your_key_here
PYTHONPATH=src python -m fred_pipeline run --env dev --dry-run
```

Get a free FRED API key at <https://fredaccount.stlouisfed.org/apikeys>.

### Run fully locally and save to a database file

No Databricks, no Spark — the whole Bronze → Silver → Gold → audit flow runs on
your machine and persists to a single SQLite file you can open with any SQLite
tool, `pandas.read_sql`, DBeaver, DuckDB, etc.

```bash
export FRED_API_KEY=your_key_here
PYTHONPATH=src python -m fred_pipeline run --local --db-path fred_local.db
```

This creates `fred_local.db` with all layers as `{schema}_{table}` tables:

```
meta_fred_series              gold_fred_latest_observation
bronze_fred_api_response      gold_fred_point_in_time
silver_fred_observation       gold_fred_macro_feature_daily
audit_etl_run / _series_run   audit_data_quality_result
```

Inspect it, e.g.:

```bash
python - <<'PY'
import sqlite3, pandas as pd
con = sqlite3.connect("fred_local.db")
print(pd.read_sql("SELECT series_id, observation_date, value "
                  "FROM gold_fred_latest_observation "
                  "ORDER BY observation_date DESC LIMIT 10", con))
PY
```

Re-running is idempotent (Silver upserts on the same natural key the Delta MERGE
uses), so you can run it repeatedly against the same file without duplicates.
The same code path, pointed at a `SparkWarehouse` instead, is what runs on
Databricks — so local results match production semantics.

## Deploy to Databricks

```bash
# One-time per environment: create catalog/schemas/tables and the secret scope
#   (run each sql/*.sql file with {{catalog}} replaced by macro_dev/test/prod)
databricks secrets create-scope fred
databricks secrets put-secret   fred api_key

# Deploy + run the job via Asset Bundles
databricks bundle validate -t dev
databricks bundle deploy   -t dev
databricks bundle run fred_ingestion -t dev
```

The job resolves the API key from the `fred/api_key` secret scope, syncs
manifests into the `meta` schema, ingests each series, runs data quality, and
rebuilds the Gold layer — recording a complete audit trail.

## Configuration

| Setting | Source |
|---|---|
| FRED API key | `FRED_API_KEY` env var **or** Databricks secret scope `fred/api_key` |
| Target catalog | `--env {dev,test,prod}` → `macro_{env}` |
| Series universe | `manifests/*.yml` |
| HTTP tuning (retries, rate limit, timeout) | `PipelineConfig` |

## Adding a series

Add an entry to the appropriate manifest under `manifests/` (fields validated
against `manifests/manifest.schema.json`), open a PR, and the next run picks it
up — including syncing its metadata into `meta.fred_series`.

```yaml
- series_id: DGS10
  title: 10-Year Treasury Constant Maturity Rate
  category: rates
  frequency: d
  units: Percent
  vintage_enabled: false
  validation_profile: standard
  downstream_use_case: yield_curve
  priority: 1
  tags: [rates, curve, treasury]
```

## Status

MVP implemented and unit-tested (53 tests). Ships the four seed manifests from
the handoff (rates, inflation, labor, growth — 27 series), the full Bronze→Gold
Python package, a pluggable storage backend (**Databricks/Delta or local
SQLite**), Unity Catalog DDL, the audit + data-quality framework, and the
Databricks Asset Bundle. Designed to scale to hundreds of series while staying
governed, auditable, and reusable.
