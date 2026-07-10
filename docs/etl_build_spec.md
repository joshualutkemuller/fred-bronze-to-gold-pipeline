# Reusable ETL + Database Build Specification

A dataset-agnostic build spec, distilled from this pipeline, for standing up a
new **manifest-driven, medallion (Bronze → Silver → Gold) ETL** with an
**idempotent database** and a **local-first** development story. Hand it to an
engineer or a coding agent; fill in the placeholders in §0; keep, drop, or defer
the modules marked **[OPTIONAL]**.

> This is intentionally not tied to any one data type. Time-series economic data
> (with revisions/vintages) is one instance; the same skeleton fits prices,
> events, telemetry, catalog data, etc. Where a feature only makes sense for
> revised time series (point-in-time/vintages), it is called out as optional.

---

## §0. Fill in these parameters first

Replace every `{{PLACEHOLDER}}` before building.

| Parameter | Meaning | Example |
|---|---|---|
| `{{PROJECT}}` | package / repo name | `weather-bronze-to-gold` |
| `{{DATASET}}` | what you're ingesting | station weather observations |
| `{{SOURCE(S)}}` | upstream API(s) + base URL + auth + rate limit | NOAA CDO, key header, 5 req/s |
| `{{ENTITY}}` | the thing a row describes (the series/instrument/entity) | a station |
| `{{ENTITY_ID}}` | stable id for an entity | `station_id` |
| `{{GRAIN}}` | one observation = one … | (entity, date) |
| `{{VALUE_FIELDS}}` | measured columns | `tmax, tmin, prcp` |
| `{{CADENCE}}` | how often it updates | daily |
| `{{NATURAL_KEY}}` | what makes a Silver row unique (see §6) | `(source, entity_id, obs_date)` |
| `{{REVISED?}}` | do past values get restated? → enables §7 | no |
| `{{BACKENDS}}` | target warehouse(s) | Delta/Databricks + local SQLite |

---

## §1. Goal & principles

Build a production-grade ETL that is:

- **Manifest-driven** — *what* to ingest and *how* to treat each entity lives in
  reviewable config (YAML), never hardcoded in code.
- **Medallion** — **Bronze** (raw, verbatim, system of record) → **Silver**
  (normalized, deduplicated, queryable) → **Gold** (analytics-ready
  features/aggregates/views).
- **Idempotent** — loads are an upsert/MERGE on a **natural key**; re-runs never
  duplicate and never require a manual clean-up.
- **Pure core, thin I/O shell** — all business logic (config, manifest parsing,
  HTTP clients, normalization, quality, audit) is plain, unit-testable code with
  **no engine/network imports**; the warehouse engine (Spark/Delta, a DB driver)
  lives only in a thin adapter and is imported lazily.
- **Auditable** — every run, every entity-run, and every data-quality outcome is
  persisted.
- **Local-first parity** — the entire flow runs on a laptop against a local
  store (e.g. SQLite) with the **same code path and semantics** as production.
- **Promotable** — one codebase targets dev/test/prod via config, not edits.
- **Pluggable sources** — a small `SourceClient` contract lets you add a new
  upstream API as **one module + one registry entry**, with nothing in
  Bronze/Silver/Gold changing.

---

## §2. Architecture

```
{{SOURCE(S)}} API
      ↓   (pluggable source client: auth, rate-limit, retry)
Ingestion package (pure Python)
      ↓
Raw archive / Bronze (verbatim payloads = system of record)
      ↓   (normalize → canonical rows)
Silver (deduplicated, MERGE on natural key)
      ↓   (features / aggregates / views)
Gold (analytics-ready)
      ↓
Consumers (SQL / BI / ML / optimizers)
      +  Audit (runs, entity-runs, DQ results)   — written throughout
```

---

## §3. Repository structure

```
{{PROJECT}}/
├── config/            # config template (real config git-ignored)
├── manifests/         # YAML entity universe + JSON schema
├── src/{{PROJECT}}/
│   ├── config.py      # layered config resolution
│   ├── manifest.py    # manifest load + validation
│   ├── sources/       # base.py (shared transport + SourceClient) + one file per source
│   ├── transform.py   # payload → canonical rows (+ optional revision logic)
│   ├── quality.py     # data-quality rules + profiles
│   ├── audit.py       # run / entity-run / DQ records
│   ├── bronze.py / silver.py / gold.py   # layer writers
│   ├── warehouse.py   # Warehouse protocol + engine adapter (lazy import)
│   ├── local_store.py # local backend (e.g. SQLite) implementing Warehouse
│   ├── pipeline.py    # orchestration
│   └── cli.py         # command-line entrypoints
├── sql/               # DDL (parameterized per environment)
├── tests/             # fast unit suite (no network); engine tests auto-skip
├── notebooks/ or jobs # platform entrypoint
├── resources/ + bundle/IaC   # deployment definitions
└── docs/              # architecture, data dictionary, this spec, runbook
```

---

## §4. Components to build (the contract)

Build these; each bullet is a module with a stable, testable interface.

### 4.1 Configuration (`config.py`)
- A single immutable `Config` object. Resolve settings with precedence:
  **explicit arg > environment variable > config file > built-in default**.
- Secrets (API keys) never hardcoded, never logged (keep out of `repr`); support
  a secret-store fallback (e.g. a secret scope) keyed by field name.
- Include HTTP knobs (timeout, max_retries, rate limit), the target
  environment/catalog, and per-source keys.

### 4.2 Manifest (`manifest.py` + `manifests/*.yml` + JSON schema)
- One entity spec per manifest row. Required: `{{ENTITY_ID}}`, a title, the
  cadence/frequency. Sensible defaults for everything else so a minimal manifest
  stays readable.
- Standard per-entity fields: `source`, `active`, `load_type`
  (`full`/`incremental`), `validation_profile`, value bounds, ownership,
  priority, tags, and any source-specific addressing. **[OPTIONAL]**
  `vintage_enabled` if `{{REVISED?}}`.
- Validate strictly: unknown fields rejected, enums normalized, cross-file
  duplicate `{{ENTITY_ID}}`s rejected. Ship a JSON schema and a `validate` CLI.

### 4.3 Source clients (`sources/`)
- **`base.py`** holds everything source-agnostic:
  - a thread-safe **rate limiter** (min interval; shared across workers),
  - **retry with exponential backoff + jitter** on transient/5xx/429,
  - one `_request()` engine with per-source hooks for **auth params**, **error
    body shape**, and **headers**,
  - a small **`SourceClient` protocol** — the entire surface the orchestrator
    depends on:
    - `get_observations(entity_id, **window) -> raw_payload` (returned verbatim
      for Bronze),
    - `normalize(entity_id, payload, ...) -> list[canonical_row]`.
- **One file per source** subclasses the base and implements only what differs
  (endpoint, auth, response shape). Register it in a `SOURCE_FACTORIES`
  `{name -> factory}` map. **Adding a source = one file + one map entry.**
- Inject the HTTP session so tests use a fake with no network.

### 4.4 Canonical row schema + normalization (`transform.py`)
- Define **one canonical row schema** every source normalizes into, so DQ /
  Silver / Gold are source-agnostic. Suggested columns:
  `source, {{ENTITY_ID}}, observation_date, {{value fields or single value}},
  raw_value, is_missing, row_hash, ingested_at, run_id`
  (+ **[OPTIONAL]** `realtime_start, realtime_end, revision_number` for §7).
- `row_hash` = deterministic hash of the identifying fields + value, for change
  detection. Parse messy values defensively (missing sentinels → null, never
  raise on one bad cell).

### 4.5 Layer writers (`bronze.py`, `silver.py`, `gold.py`)
- **Bronze**: append the **verbatim** payload + minimal ingest metadata
  (`source`, endpoint called, entity, run_id, byte size, observation count from
  the normalized rows, ingested_at). Bronze is the system of record — Silver/Gold
  are always re-derivable from it.
- **Silver**: `MERGE`/upsert canonical rows on the **natural key** (§6). Cast
  types at the boundary; empty strings → NULLs.
- **Gold**: derived features / aggregates / “latest” + point-in-time views. Keep
  a pure-Python reference implementation so the local backend matches production.

### 4.6 Data quality (`quality.py`)
- Rule set with **profiles**: `strict` (any failure fails the entity),
  `standard` (record warnings, continue), `lenient` (minimal). Typical checks:
  not-all-missing, value bounds, freshness/staleness vs. expected cadence,
  monotonic dates, duplicate keys. Persist every check outcome.

### 4.7 Audit (`audit.py`)
- Three record types: **run** (id, env, status, timings, counts), **entity-run**
  (per entity: status, rows written/merged, extracted count, error), and
  **DQ result** (per check). Status model: `succeeded` / `partial` / `failed`.

### 4.8 Warehouse abstraction (`warehouse.py` + `local_store.py`)
- A `Warehouse` **protocol** with the operations the pipeline needs
  (`write_bronze`, `merge_silver`, `build_gold`, `persist_run`, `persist_dq`,
  watermark lookup, meta sync…).
- Two implementations behind it: the **production engine** (Spark/Delta or a DB)
  in a lazily-imported adapter, and a **local store** (SQLite) that mirrors the
  same tables as `{schema}_{table}` and the same upsert-on-natural-key semantics.
  Local and prod must produce equivalent results.

### 4.9 Orchestrator (`pipeline.py`)
- One `run()` that, per entity: **plan the load window → extract (via the entity's
  source client) → normalize → data-quality → write Bronze/Silver → record
  audit**, then build Gold once, then persist run + notify.
- **Per-entity isolation**: one entity failing (bad id, DQ failure, network
  exhaustion) is recorded and the run continues as `partial`.
- **Concurrency**: network-bound extraction on a thread pool that shares the one
  rate limiter; DB writes serialized.
- Resolve the client per entity from `spec.source` via `SOURCE_FACTORIES`; require
  only the keys the *active* sources need.

### 4.10 Incremental loads
- First load of an entity → **full history**. Subsequent loads → **re-pull the
  last N** observations (a watermark = N-th most recent date) and MERGE, so
  recent values are restated and new ones inserted. `load_type: full` forces a
  full pull. `N` is a global default, overridable per entity.

### 4.11 Replay from Bronze
- A command that rebuilds Silver (and Gold) from archived Bronze payloads with no
  network — routing each payload to the right source normalizer. Essential after
  a transform fix or a dropped table.

### 4.12 [OPTIONAL] Discovery / manifest generation
- If the source has a catalog, generate manifests programmatically (map source
  metadata → validated specs) instead of hand-authoring at scale.

### 4.13 [OPTIONAL] Notifications
- Post a run summary to a webhook on failure/always; never let notification
  errors fail a run.

---

## §5. Natural key & idempotency

- Pick the **`{{NATURAL_KEY}}`** that makes a Silver row unique. Lead with
  `source` when multi-source so the same `{{ENTITY_ID}}` can’t collide across
  APIs. Base case: `(source, {{ENTITY_ID}}, observation_date)`.
- All loads MERGE/upsert on it. Re-running any step is safe and non-duplicating.

## §6. [OPTIONAL] Point-in-time / revisions

Include **only if `{{REVISED?}}`** (values get restated after first publish):
- Add `realtime_start` / `realtime_end` to the row and the natural key, and a
  `revision_number` per `(entity, observation_date)`.
- Capture each “as-of” version (e.g. an as-reported/filed date → `realtime_start`)
  so you can reconstruct *what was known on date X* (leak-free backtests).
- Provide two views: **latest-revised** and **point-in-time**.
- If not revised, omit all of this; the natural key stays
  `(source, {{ENTITY_ID}}, observation_date)` and re-runs update in place.

## §7. Configuration & secrets

- Layered resolution (§4.1). Ship a `config.example.yaml`; git-ignore the real
  one. Support per-environment override blocks. Keys via env var or secret store;
  never in the repo. Entrypoints require only the keys the active sources need.

## §8. Testing requirements (non-negotiable)

- **Pure-core unit tests, no network, no engine.** Inject fake HTTP
  sessions/clients that replay canned payloads; assert on parsing, error
  handling, normalization → canonical schema, DQ outcomes.
- **Local end-to-end**: run the orchestrator against the local store and assert
  Bronze/Silver/Gold/audit contents; assert **idempotency** (run twice → same
  counts) and **per-entity isolation** (one bad entity → `partial`).
- Engine/integration tests (Spark/DB) live separately and **auto-skip** when the
  engine isn’t installed.
- **CI**: run the unit suite on a version matrix; run integration in its own job.

## §9. [OPTIONAL] Deployment

- Infrastructure-as-code for the target platform (e.g. Databricks Asset Bundle):
  create catalog/schemas/tables from `sql/` per environment; a scheduled job (or
  one per source); secrets provisioned in the secret store; egress allowed to the
  source hosts. Ship a **decision register / runbook** separating provisioning
  steps (owned by engineering/platform) from domain decisions (owned by the data
  owners).

## §10. Deliverables checklist

- [ ] Manifests + JSON schema + `validate` command
- [ ] `sources/base.py` (transport + `SourceClient`) and ≥1 source client
- [ ] Canonical row schema + normalization
- [ ] Bronze / Silver (MERGE) / Gold writers + views
- [ ] Data-quality rules + profiles
- [ ] Audit records (run / entity-run / DQ) persisted
- [ ] `Warehouse` protocol + production adapter + **local backend**
- [ ] Orchestrator with per-entity isolation + concurrency
- [ ] Incremental loads; replay-from-Bronze
- [ ] Layered config + secret handling
- [ ] Unit suite (no network) + local e2e + idempotency tests; CI
- [ ] SQL DDL; deployment/runbook **[OPTIONAL]**
- [ ] Docs: architecture, data dictionary, this spec

## §11. Conventions & guardrails

- Never hardcode credentials; keep them out of logs and `repr`.
- Bronze stores payloads **verbatim** — never lose the raw response.
- Every write is idempotent on the natural key.
- Business logic imports no engine and no network (so it stays fast to test).
- One canonical row schema; sources differ only in their `normalize()`.
- Parameterize environment/catalog names; promote by config, not code edits.
- Fail one entity, not the run. Record why.

---

### How to use this as a prompt

> Build `{{PROJECT}}`, a manifest-driven Bronze→Silver→Gold ETL for `{{DATASET}}`
> from `{{SOURCE(S)}}`, following the specification in this document. The grain is
> `{{GRAIN}}`, the natural key is `{{NATURAL_KEY}}`, and revisions are
> `{{REVISED?}}` (include §6 only if yes). Target `{{BACKENDS}}`. Implement every
> item in §4 and satisfy the §10 checklist and §8 testing requirements. Keep the
> core pure and engine-free; make the local backend match production semantics.
