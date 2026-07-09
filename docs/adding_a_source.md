# Adding a Data Source

This pipeline was built around FRED, but the FRED-specific surface is small and
isolated. Bronze (verbatim payload), the Silver `MERGE`, data-quality profiles,
audit, and Gold are all **source-agnostic** — they operate on the canonical
silver row schema and never inspect where a row came from. Adding a new source
(BLS, EIA, ECB, ...) means implementing one small contract, not forking the
pipeline.

This is a proof-of-concept: FRED has been refactored onto the shared transport,
and BLS is included as a second, differently-shaped source to demonstrate the
abstraction holds. See `src/fred_pipeline/sources/` and the tests
`tests/test_sources_base.py` / `tests/test_bls_client.py`.

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

## Next steps to wire a source end-to-end

The POC stops at the client boundary. To run a source through the orchestrator:

1. **Manifest dimension.** Add an optional `source: str = "fred"` field to
   `SeriesSpec` (and the JSON schema), then namespace manifests per source
   (`manifests/bls/…`). Default `"fred"` keeps every existing manifest valid.
2. **Client selection.** Map `spec.source` → the right client in
   `FredPipeline` (a small registry: `{"fred": FredClient, "bls": BLSClient}`).
3. **Normalize via the client.** Have `_finish_series` call
   `client.normalize(...)` instead of `build_silver_rows` directly, so each
   source's response shape is handled by its own normalizer.
4. **Table dimension (optional).** If you want per-source lineage, add a
   `source` column to Bronze/Silver and include it in the natural key
   (`(source, series_id, observation_date, realtime_start)`).
5. **Deploy.** Add one Databricks job per source (or per manifest group) in
   `databricks.yml`, each on its own schedule, all sharing the same wheel.

None of these touch Bronze/Silver/Gold internals — they're additive wiring.
