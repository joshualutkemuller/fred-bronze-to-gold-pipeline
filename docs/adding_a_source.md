# Adding a Data Source

This pipeline was built around FRED, but the FRED-specific surface is small and
isolated. Bronze (verbatim payload), the Silver `MERGE`, data-quality profiles,
audit, and Gold are all **source-agnostic** — they operate on the canonical
silver row schema and never inspect where a row came from. Adding a new source
(BLS, EIA, ECB, ...) means implementing one small contract, not forking the
pipeline.

FRED has been refactored onto the shared transport, and BLS is wired through the
orchestrator end-to-end as a second, differently-shaped source. See
`src/fred_pipeline/sources/` and the tests `tests/test_sources_base.py` /
`tests/test_bls_client.py` (the latter runs a mixed FRED+BLS batch through
`FredPipeline` and asserts each series is dispatched to its own client).

## The layout

```
src/fred_pipeline/sources/
  base.py   # HTTPSource (rate limit + retry/backoff + _request) and the
            # SourceClient protocol — shared by every source
  fred.py   # FredClient: auth, vintage-cap recovery, vintage batching, discovery
  bls.py    # BLSClient: a second source proving the contract generalizes
```

`fred_pipeline.fred_client` remains as a thin back-compat shim re-exporting
`FredClient` / `FredAPIError` / `RateLimiter`, so existing imports keep working.

## The contract

A source implements `SourceClient` (`sources/base.py`):

```python
class SourceClient(Protocol):
    source_name: str
    def get_observations(self, series_id: str, **kwargs) -> dict: ...     # raw payload
    def normalize(self, series_id, payload, *, run_id=None,
                  track_vintage=True) -> list[dict]: ...                  # silver rows
```

* **`get_observations`** returns the *raw* upstream payload, archived verbatim
  in Bronze so Silver/Gold can always be re-derived.
* **`normalize`** maps that payload into the canonical silver schema
  (`transform.SILVER_COLUMNS`): `series_id, observation_date, realtime_start,
  realtime_end, value, raw_value, is_missing, row_hash, ingested_at, run_id`.
  Downstream code is identical for every source.

## What you inherit vs. what you write

`HTTPSource` gives you, for free:

* the shared `RateLimiter` (one aggregate ceiling, thread-safe),
* retry with exponential backoff + jitter on transient/5xx/429,
* the `_request` engine and error raising.

Per source you override only what genuinely differs:

| Hook | FRED | BLS |
|---|---|---|
| `_default_query()` (auth) | `api_key` + `file_type=json` | `registrationkey` (optional) |
| `_error_detail()` (error body) | `error_message` field | `message[]` list |
| `get_observations()` | GET, `series/observations`, vintage window | GET `timeseries/data/{id}`, checks `status` on HTTP 200 |
| `normalize()` | delegates to `transform.normalize_observations` | maps `year`+`period` → date, blanks realtime |

BLS is a good stress test because it breaks three FRED assumptions at once:
different auth, HTTP 200 on logical failure (`status != "REQUEST_SUCCEEDED"`),
and a nested `year`/`period` response shape — yet its normalized rows pass the
same DQ checks and MERGE into the warehouse idempotently
(`test_bls_rows_pass_dq_and_merge_into_warehouse`).

## How a source is wired through the orchestrator

The following are implemented — a series declaring `source: bls` flows through
`FredPipeline` with no other changes:

1. **Manifest dimension.** `SeriesSpec` has an optional `source: str = "fred"`
   field (also in `manifests/manifest.schema.json`). The default keeps every
   existing manifest valid; a BLS series sets `source: bls`. See the shipped
   demo `manifests/bls_labor.yml` (inactive by default).
2. **Client selection.** `pipeline.SOURCE_FACTORIES` maps `source` → a client
   factory (`{"fred": …, "bls": …}`). `FredPipeline._client_for(spec)` resolves
   and caches the right client per series; unknown sources fail that one series
   (per-series isolation) rather than the run. Clients can also be injected via
   the `clients=` constructor arg (used in tests).
3. **Normalize via the client.** `_finish_series` calls
   `FredPipeline._normalize`, which delegates to the source client's
   `normalize` (falling back to the FRED normalizer for lightweight test
   doubles) and then applies `assign_revision_numbers` uniformly, so revision
   numbering stays source-agnostic.

### Adding another source (e.g. EIA)

1. Add `sources/eia.py` with an `EIAClient(HTTPSource)` implementing
   `get_observations` + `normalize` (override `_default_query` / `_error_detail`
   as needed).
2. Register it: one entry in `SOURCE_FACTORIES`.
3. Author a manifest with `source: eia`.

That's the whole change — nothing in Bronze/Silver/Gold moves.

### Still optional (not needed for correctness)

* **Per-source lineage column.** To physically tag rows by origin, add a
  `source` column to Bronze/Silver and include it in the natural key
  (`(source, series_id, observation_date, realtime_start)`). Today sources share
  the observation tables, keyed by `series_id`; distinct series IDs across
  sources keep rows unambiguous.
* **Deploy.** Add one Databricks job per source (or per manifest group) in
  `databricks.yml`, each on its own schedule, all sharing the same wheel.
