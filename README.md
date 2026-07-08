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
├── config/               # config.example.yaml template (real config.yaml git-ignored)
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

Settings can come from a **YAML config file**, environment variables, CLI
arguments, or a Databricks secret scope. Precedence (highest wins):

```
explicit CLI/arg  >  environment variable  >  config file  >  built-in default
```

The FRED API key additionally falls back to a Databricks secret scope when not
set by any of the above.

### Config file

Copy the template and edit it (the real file is git-ignored so your key never
gets committed):

```bash
cp config/config.example.yaml config/config.yaml
# edit config/config.yaml — set fred_api_key and any HTTP knobs
PYTHONPATH=src python -m fred_pipeline run --local   # auto-reads config/config.yaml
```

`config/config.yaml` is picked up automatically. Point elsewhere with
`--config path/to/file.yaml` or `FRED_CONFIG_FILE=...`. It supports a flat
mapping or a `default:` block with per-environment overrides:

```yaml
default:
  fred_api_key: ""            # prefer env var / secret scope for real keys
  rate_limit_per_minute: 120
  max_retries: 5
  secret_scope: fred          # Databricks secret scope name
  secret_key: api_key
environments:
  prod:
    rate_limit_per_minute: 60 # merged on top of default for --env prod
```

### Where each setting can come from

| Setting | Config key | Env var | CLI |
|---|---|---|---|
| FRED API key | `fred_api_key` | `FRED_API_KEY` | `--fred-api-key`* / secret scope |
| API base URL | `fred_base_url` | `FRED_BASE_URL` | |
| Request timeout | `request_timeout_seconds` | `FRED_REQUEST_TIMEOUT_SECONDS` | |
| Max retries | `max_retries` | `FRED_MAX_RETRIES` | |
| Rate limit / min | `rate_limit_per_minute` | `FRED_RATE_LIMIT_PER_MINUTE` | |
| Secret scope / key | `secret_scope` / `secret_key` | `FRED_SECRET_SCOPE` / `FRED_SECRET_KEY` | |
| Raw archive volume | `raw_volume_path` | `FRED_RAW_VOLUME_PATH` | |
| Target catalog | — | — | `--env {dev,test,prod}` → `macro_{env}` |
| Series universe | — | — | `--manifests DIR` (`manifests/*.yml`) |
| Config file path | — | `FRED_CONFIG_FILE` | `--config FILE` |

*The API key is passed programmatically (`PipelineConfig.resolve(fred_api_key=…)`);
on the CLI, use the config file, `FRED_API_KEY`, or a Databricks secret scope.

## The series universe

The four shipped manifests wire the **27 "Suggested Initial Series"** from the
handoff (rates, inflation, labor, growth). This is a deliberate, reviewed seed
set — not all of FRED (which has ~800k series). Grow it two ways:

### 1. Add a series by hand

Add an entry to the appropriate manifest under `manifests/` (fields validated
against `manifests/manifest.schema.json`), open a PR, and the next run picks it
up — including syncing its metadata into `meta.fred_series`.

```yaml
- series_id: DGS10
  title: 10-Year Treasury Constant Maturity Rate
  category: rates
  frequency: d
  units: Percent
  # vintage_enabled defaults to true (point-in-time safe). Set false only for
  # provably non-revised market/price series if you want a leaner pull.
  validation_profile: standard
  downstream_use_case: yield_curve
  priority: 1
  tags: [rates, curve, treasury]
```

**Revision-sensitivity is on by default.** `vintage_enabled` defaults to `true`,
so every series captures its full point-in-time (ALFRED) history unless you
opt out. This is the leakage-safe default for backtests: you can always collapse
vintages to "latest revised" (the `gold.v_latest_revised` view), but you cannot
recover vintages a run never captured. For never-revised market series (yields,
SOFR, breakevens) it's a cheap no-op — one vintage per date.

### 2. Discover series from the FRED API

Generate a whole manifest from a FRED **category**, **release**, or **search**
instead of hand-listing ids. The generator maps FRED metadata → validated specs
(popularity → priority), drops `DISCONTINUED` series, dedupes against your
existing manifests, and writes YAML that's guaranteed to load.

```bash
# Preview series in FRED category 22 (Treasury constant maturities), no write:
PYTHONPATH=src python -m fred_pipeline discover --name rates_extra \
    --category-id 22 --frequencies d --dry-run

# Write a manifest from a release, keeping only popular monthly/quarterly series:
PYTHONPATH=src python -m fred_pipeline discover --name jolts \
    --release-id 192 --frequencies m,q --min-popularity 20 \
    --out manifests/jolts.yml

# Or from a search:
PYTHONPATH=src python -m fred_pipeline discover --name inflation_breakevens \
    --search "breakeven inflation" --max 25 --out manifests/breakevens.yml
```

Useful flags: `--max N`, `--min-popularity 0-100`, `--frequencies d,w,m,q`,
`--include-discontinued`, `--include-existing` (skip dedupe), `--dry-run`.
Find category/release ids on the FRED website (the id is in the page URL) or via
the API. Review the generated YAML, set `vintage_enabled` / `validation_profile`
where it matters, and commit it like any other manifest.

## Status

MVP implemented and unit-tested (80 tests). Ships the four seed manifests from
the handoff (rates, inflation, labor, growth — 27 series) plus an **API-driven
discovery** command to generate more from FRED categories/releases/search, the
full Bronze→Gold Python package, a pluggable storage backend (**Databricks/Delta
or local SQLite**), layered configuration (**YAML file / env vars / args /
secret scope**), Unity Catalog DDL, the audit + data-quality framework, and the
Databricks Asset Bundle. Designed to scale to hundreds of series while staying
governed, auditable, and reusable.
