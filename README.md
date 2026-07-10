# fred-bronze-to-gold-pipeline

A production-grade, **manifest-driven**, **multi-source** economic-data
ingestion pipeline that lands data in a **Bronze → Silver → Gold** medallion
architecture on **Databricks + Delta Lake**. Built for quant research,
dashboards, optimizer inputs, and **point-in-time** macro features.

Started FRED-only; the source layer is now pluggable, so a series declares its
upstream API (`source:` in the manifest) and flows through the same path.
**Eight sources are wired**: FRED, BLS, EIA, US Treasury, World Bank, BEA,
Census, and SEC (company financials) — see [Data sources](#data-sources).

> 📄 The original product spec / engineering handoff lives in
> [`handoff.md`](./handoff.md). This README is the practical guide to the
> implementation. Architecture rationale is in
> [`docs/architecture.md`](./docs/architecture.md); every table/column is in
> [`docs/data_dictionary.md`](./docs/data_dictionary.md); the data-quality
> rules and where to review/change them are in
> [`docs/validation.md`](./docs/validation.md). A dataset-agnostic, reusable
> build spec (usable as a prompt for standing up a *new* ETL like this one) is in
> [`docs/etl_build_spec.md`](./docs/etl_build_spec.md).

## What it does

```
manifests/*.yml → source API (FRED / BLS / EIA / Treasury / World Bank /
                              BEA / Census / SEC) → Bronze (raw JSON)
     → Silver (normalized, MERGE) → Data Quality
     → Gold (latest / point-in-time / daily feature matrix)
     → full audit trail (runs, series, DQ results)
```

* **Manifest-driven** — the series universe and per-series policy (source, load
  type, validation profile, vintage tracking, ownership) live in reviewable YAML.
* **Multi-source** — a pluggable `SourceClient` layer; adding a source is one
  client module + one registry entry. Bronze/Silver/Gold are source-agnostic.
* **Idempotent** — Silver is a Delta `MERGE` on
  `(source, series_id, observation_date, realtime_start)`; re-runs never
  duplicate, and each row is tagged with its origin.
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
├── manifests/            # YAML series universe (per-domain + per-source) + JSON schema
├── src/fred_pipeline/    # the Python package (pure core + Spark I/O)
│   └── sources/          # pluggable source clients (base + fred/bls/eia/…/sec)
├── sql/                  # Unity Catalog DDL (00..60, parameterized by {{catalog}})
├── resources/            # Databricks Asset Bundle jobs (main + per-source templates)
├── databricks.yml        # Asset Bundle (dev/test/prod targets)
├── notebooks/            # Databricks job entrypoint
├── .github/workflows/    # CI: unit matrix + Spark/Delta integration job
├── tests/                # pytest suite (Spark tests auto-skip if PySpark absent)
└── docs/                 # architecture, data dictionary, validation,
                          #   adding_a_source, deployment_runbook, incremental_loading
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

# 3. Run the fast unit-test suite (no Spark needed; Spark tests auto-skip)
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

> Full go-live checklist (provisioning steps + the quant/ops decisions, with
> owners): [`docs/deployment_runbook.md`](docs/deployment_runbook.md).

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

API keys additionally fall back to a Databricks secret scope when not set by any
of the above — `fred_api_key` from `secrets/<scope>/api_key`, and the other
source keys from `secrets/<scope>/<field>` (e.g. `secrets/fred/eia_api_key`).

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
| BLS API key (optional) | `bls_api_key` | `BLS_API_KEY` | keyless works at a lower quota |
| EIA API key | `eia_api_key` | `EIA_API_KEY` | required to activate `source: eia` series |
| BEA API key | `bea_api_key` | `BEA_API_KEY` | required to activate `source: bea` series |
| Census API key (optional) | `census_api_key` | `CENSUS_API_KEY` | keyless works at a lower quota |
| SEC User-Agent | `sec_user_agent` | `SEC_USER_AGENT` | set to your contact; SEC 403s without a descriptive UA |
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

## Data sources

A series' `source:` selects its upstream API and its client; every source lands
in the same tables, tagged by `source` in the natural key. FRED is active by
default; the other seven ship as **inactive demo manifests** (`active: false`)
so a default run doesn't hit them until you opt in.

| Source | `source:` | API key | Status | Demo manifest |
|---|---|---|---|---|
| FRED | `fred` | required | **active** (~2,300 series) | the domain manifests |
| BLS | `bls` | optional (keyless) | demo | `bls_labor.yml` |
| EIA | `eia` | **required** | demo | `eia_energy.yml` |
| US Treasury | `treasury` | none | demo | `treasury_fiscal.yml` |
| World Bank | `worldbank` | none | demo | `worldbank_global.yml` |
| BEA | `bea` | **required** | demo | `bea_national_accounts.yml` |
| Census | `census` | optional (keyless) | demo | `census_indicators.yml` |
| SEC (company financials) | `sec` | none (User-Agent) | demo | `sec_financials.yml` |

SEC is the one that exercises the point-in-time machinery — each filing's `filed`
date becomes a vintage. To add a source, see
[`docs/adding_a_source.md`](docs/adding_a_source.md); to take the demos to
production (keys, egress, activation), see the decision register in
[`docs/deployment_runbook.md`](docs/deployment_runbook.md).

## The series universe

The FRED universe spans **~2,300 series** across the domain manifests (rates,
inflation, labor, growth, money/banking, prices, production/housing,
international, national accounts). It grew from the handoff's 27-series seed via
**API-driven discovery** (below). It is a deliberate, reviewed set — not all of
FRED (~800k series). Grow it three ways:

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

### 2. Add a series from another source

Series aren't limited to FRED. A manifest entry can set `source:` to `bls`,
`eia`, `treasury`, `worldbank`, `bea`, `census`, or `sec` and it flows through
the same Bronze/Silver/Gold path — each row is tagged with its `source` in the
natural key. Treasury, World Bank, Census, and SEC are keyless (SEC needs a
descriptive User-Agent); EIA and BEA require a key. **SEC** brings company
financials (fundamentals from EDGAR XBRL) in as point-in-time series. See the
inactive demos under `manifests/` (`*_labor.yml`, `eia_energy.yml`,
`treasury_fiscal.yml`, `worldbank_global.yml`, `bea_national_accounts.yml`,
`census_indicators.yml`, `sec_financials.yml`), and
[`docs/adding_a_source.md`](docs/adding_a_source.md) for how to add a new source
(one client module + one registry entry).

## Metadata governance (drift + lifecycle)

Manifests declare *intent*; FRED is the source of *truth*, and they drift over
time. `reconcile` fetches FRED's `/series` metadata for each series and reports:

- **drift** — `frequency_mismatch` (error), `discontinued` (warning),
  `units_changed` (info), `not_found` (error);
- **lifecycle snapshots** — observation range, `last_updated`, popularity, and a
  **staleness** verdict (did the expected release actually land?), appended to
  `meta.fred_series_lifecycle` so each series' health is tracked over time.

```bash
# report only
PYTHONPATH=src python -m fred_pipeline reconcile --no-persist

# persist lifecycle + drift to a local SQLite file
PYTHONPATH=src python -m fred_pipeline reconcile --local --db-path fred_local.db

# gate CI: exit non-zero on any error-level drift
PYTHONPATH=src python -m fred_pipeline reconcile --fail-on-drift
```

Findings land in `meta.fred_series_drift` and `meta.fred_series_lifecycle`
(Delta or SQLite). Use `--fail-on-drift` in CI to catch a series changing
frequency or being discontinued before it silently corrupts downstream features.

## Incremental loads (full-on-first-run, then restate last N)

Each run decides a load window per series against whatever backend it's writing
to (Delta or local SQLite):

- **Series has no data yet → full history.** The first ever load pulls the
  complete series (and, for `vintage_enabled`, its full vintage history).
- **Series already loaded → restate the last N observations.** Subsequent runs
  re-pull only the most recent `N` observation dates (`observation_start` set to
  the N-th most recent) and `MERGE` them, so **revisions to recent points are
  restated and new points are inserted** — idempotently, no duplicates.
- **`load_type: full`** on a series forces a full re-pull every run.

`N` is `restate_last_n` (default **90**, set via config/env/CLI), overridable
per series in a manifest with `restate_records:` — tune it higher for series
with deep benchmark revisions (GDP, payrolls) and lower for high-frequency
series. The effective strategy per series is recorded in
`audit.etl_series_run.load_type` (`full` or `restate_last_<n>`).

```yaml
# manifest entry: restate the last 24 observations for a heavily-revised series
- series_id: GDPC1
  title: Real Gross Domestic Product
  category: growth
  frequency: q
  load_type: incremental
  restate_records: 24        # ~6 years of quarters, to catch annual revisions
```

> Because "restate last N" only re-pulls recent observations, revisions to
> points *older* than the window won't be re-captured until a `full` run. Size
> `restate_records` to each series' revision behavior, or schedule a periodic
> full refresh for the deeply-revised ones.

### 3. Discover series from the FRED API

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

## Open decisions (before non-FRED go-live)

The code is complete; what's left is provisioning + domain calls, tracked as a
checkboxed decision register in
[`docs/deployment_runbook.md`](docs/deployment_runbook.md). In short:

- **Which sources/series to activate** — the seven non-FRED demos are inactive;
  turn on what you want (and, for SEC at scale, generate the manifest with
  `fred_pipeline.sources.sec.build_sec_manifest`).
- **Keys & secrets** — provision EIA/BEA keys and (optional) BLS/Census keys in
  the secret scope; set `SEC_USER_AGENT`.
- **Egress** — allow the source hosts the active sources need.
- **Per-series data policy** — `vintage_enabled`, `validation_profile`, value
  bounds, `restate_records`; plus any new `config/spreads.yml` pairs.
- **Verify demo IDs live** — the demo series IDs (and Census predicate codes)
  were built to the documented API shapes but not verified against the live APIs.
- **Known follow-ons** — SEC statement standardization (canonical tags + duration
  disambiguation); per-source metadata reconciliation (reconcile is FRED-only).

## Status

Implemented and tested (**246 unit tests + a Spark/Delta integration suite in
CI**, green on the latest commit). Highlights: **eight pluggable sources**
(FRED active; BLS/EIA/Treasury/World Bank/BEA/Census/SEC as inactive demos) with
`source` in the natural key and source-aware Bronze lineage + replay;
**API-driven FRED discovery**; **metadata governance** (drift + lifecycle vs.
live FRED); **incremental loads** (full-on-first-run, then restate last N);
**replay-from-Bronze** rebuild; **run notifications**; richer **data quality**
(freshness + value bounds); **quant Gold features** (MoM/YoY/diff/z-score, curve
spreads, as-of-date point-in-time snapshots); a pluggable storage backend
(**Databricks/Delta or local SQLite**); layered configuration (**YAML file / env
vars / args / secret scope**); Unity Catalog DDL; the audit framework; a
**GitHub Actions CI**; and the Databricks Asset Bundle (main + per-source jobs).
