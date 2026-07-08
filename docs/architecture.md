# Architecture

This document describes how the FRED Bronze-to-Gold pipeline is put together
and *why*. The authoritative product spec is [`handoff.md`](../handoff.md);
this file explains the implementation that realizes it.

## Design tenets

1. **Manifest-driven.** The series universe and every per-series policy
   (load type, validation profile, vintage tracking, ownership) live in
   reviewable YAML under [`manifests/`](../manifests). Nothing about *what* to
   ingest is hardcoded in Python.
2. **Pure core, thin Spark shell.** All business logic — manifest parsing,
   FRED payload normalization, revision/point-in-time derivation, and data
   quality — is plain Python with no Spark or network imports, so it is fast to
   test and portable. Spark/Delta live only in
   [`spark_io.py`](../src/fred_pipeline/spark_io.py) and the layer writers, and
   PySpark is imported lazily.
3. **Bronze is the system of record.** Raw FRED JSON is archived verbatim, so
   Silver and Gold can always be re-derived.
4. **Idempotent by construction.** Silver loads are a Delta `MERGE` on the
   natural key `(series_id, observation_date, realtime_start)`; re-runs never
   duplicate and revisions land as new real-time rows.
5. **Auditable end-to-end.** Every run and every series-run is recorded, along
   with each data-quality check outcome.
6. **Environment promotion.** One codebase targets `macro_dev`, `macro_test`,
   and `macro_prod` via `PipelineConfig` / the Databricks Asset Bundle targets.

## Component map

| Concern | Module | Spark? |
|---|---|---|
| Config / catalog naming / secret resolution | `config.py` | no |
| Manifest load + validation | `manifest.py` | no |
| FRED REST client (retry, rate limit) | `fred_client.py` | no |
| Payload → normalized rows, revisions, PIT | `transform.py` | no |
| Data-quality rules + profiles | `quality.py` | no |
| Run / series / DQ audit records | `audit.py` | no |
| Spark session + Delta MERGE/append helpers | `spark_io.py` | yes (lazy) |
| Bronze / Silver / Gold writers | `bronze.py` / `silver.py` / `gold.py` | yes (lazy) |
| Meta-layer sync | `meta.py` | yes (lazy) |
| Orchestration | `pipeline.py` | optional |
| CLI + Databricks entrypoint | `cli.py`, `notebooks/run_pipeline.py` | optional |

## Data flow

```
manifests/*.yml
      │  load + validate (manifest.py)
      ▼
meta.fred_series / meta.fred_manifest        ← sync_meta (meta.py)
      │
      ▼  per series
FRED API (fred_client.py) ──raw JSON──► bronze.fred_api_response
      │                                        (verbatim retention)
      │  normalize_observations + revisions (transform.py)
      ▼
run_quality_checks (quality.py) ──► audit.data_quality_result
      │  (profile decides fatal vs advisory)
      ▼  MERGE on natural key
silver.fred_observation
      │  pure-SQL rebuild (gold.py / sql/50_gold.sql)
      ▼
gold.fred_point_in_time      (full vintage history)
gold.fred_latest_observation (latest revision per date)
gold.fred_macro_feature_daily(daily forward-filled matrix)
      │
      ▼
audit.etl_run / audit.etl_series_run
```

## Point-in-time (vintage) handling

For series with `vintage_enabled: true`, the pipeline requests FRED's full
real-time window (`realtime_start=1776-07-04`, `realtime_end=9999-12-31`),
which returns every vintage of every observation.
`transform.assign_revision_numbers` orders vintages by `realtime_start` to
produce a `revision_number`, giving two lenses in Gold:

* **`v_latest_revised`** — "as revised today" (latest vintage per date).
* **`v_point_in_time`** — "what was known on date X" (filter by
  `realtime_start <= X < realtime_end`).

This is what makes the data safe for backtests: features can be reconstructed
using only information available at each historical decision point.

## Failure isolation

`pipeline._process_series` wraps each series in try/except. A bad series id,
network exhaustion, or a strict-profile DQ failure marks *that* series
`FAILED` and the run continues, finishing `PARTIAL`. A run is `SUCCEEDED` only
when every series succeeds, and `FAILED` only when all fail.

## Testing strategy

The Spark-free core is covered by fast unit tests (`tests/`), including the
retry/rate-limit client (via a fake session), the transform/revision logic, the
DQ profiles, and the orchestrator (via a fake client, no Spark). Spark writers
are exercised in Databricks; their logic is thin because the hard parts are in
the tested pure functions.
