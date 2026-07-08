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

```bash
# 1. Install dev dependencies
pip install -r requirements-dev.txt

# 2. Validate the manifests (no network, no Spark)
PYTHONPATH=src python -m fred_pipeline validate --manifests manifests

# 3. Run the fast unit-test suite
python -m pytest

# 4. Dry-run against the real FRED API (extract + DQ, no Spark writes)
export FRED_API_KEY=your_key_here
PYTHONPATH=src python -m fred_pipeline run --env dev --dry-run
```

Get a free FRED API key at <https://fredaccount.stlouisfed.org/apikeys>.

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

MVP implemented and unit-tested (49 tests). Ships the four seed manifests from
the handoff (rates, inflation, labor, growth — 27 series), the full Bronze→Gold
Python package, Unity Catalog DDL, the audit + data-quality framework, and the
Databricks Asset Bundle. Designed to scale to hundreds of series while staying
governed, auditable, and reusable.
