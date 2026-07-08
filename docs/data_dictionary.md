# Data Dictionary

Fully-qualified names are `macro_{env}.{schema}.{table}` where `env` is one of
`dev` / `test` / `prod`.

## meta

### `meta.fred_series`
One row per series in the universe, synced from manifests.

| Column | Type | Notes |
|---|---|---|
| series_id | STRING | FRED series id (PK) |
| title | STRING | Human-readable title |
| category | STRING | Manifest category (rates/inflation/labor/growth) |
| frequency | STRING | FRED frequency code (d/w/m/q/…) |
| units | STRING | Reported units |
| active | BOOLEAN | Whether the series is ingested |
| load_type | STRING | `full` or `incremental` |
| expected_update_frequency | STRING | e.g. `monthly`, `business_daily` |
| vintage_enabled | BOOLEAN | Whether full real-time history is pulled |
| validation_profile | STRING | `strict` / `standard` / `lenient` |
| business_owner / technical_owner | STRING | Ownership |
| downstream_use_case | STRING | Primary consumer |
| priority | INT | 1 (highest) … 5 |
| restate_records | INT | Per-series override of `restate_last_n` (NULL = use config default) |
| tags | ARRAY<STRING> | Grouping tags |
| updated_at | TIMESTAMP | Last sync time |

### `meta.fred_manifest`
Registry of manifest files loaded (name, version, source_path, series_count).

### `meta.fred_series_manifest_map`
Membership of `series_id` → `manifest_name`.

### `meta.fred_series_lifecycle`
Append-only snapshots of FRED-reported series health (written by `reconcile`),
so a series' trajectory can be tracked over time.

| Column | Type | Notes |
|---|---|---|
| series_id | STRING | Series |
| fred_title / fred_frequency / fred_units | STRING | As currently reported by FRED |
| seasonal_adjustment | STRING | e.g. SA / NSA |
| observation_start / observation_end | STRING | FRED coverage range |
| last_updated | STRING | FRED last-updated timestamp |
| popularity | INT | FRED popularity (0–100) |
| discontinued | BOOLEAN | Title marked DISCONTINUED |
| days_since_last_observation | INT | today − observation_end |
| is_stale | BOOLEAN | Past the expected update window for its frequency |
| checked_at | STRING | When this snapshot was taken |

### `meta.fred_series_drift`
Drift between manifest intent and live FRED metadata (written by `reconcile`).

| Column | Type | Notes |
|---|---|---|
| series_id | STRING | Series |
| field | STRING | Field that drifted (frequency/title/units/series_id) |
| manifest_value / fred_value | STRING | Declared vs actual |
| kind | STRING | frequency_mismatch \| discontinued \| units_changed \| not_found |
| severity | STRING | info \| warning \| error |
| detected_at | STRING | When detected |

## audit

### `audit.etl_run`
One row per pipeline invocation.

| Column | Type | Notes |
|---|---|---|
| run_id | STRING | UUID hex, unique per run |
| environment | STRING | dev/test/prod |
| manifest_path | STRING | Manifest source used |
| triggered_by | STRING | cli / databricks_job / … |
| status | STRING | running/succeeded/failed/partial |
| started_at / ended_at | TIMESTAMP | Run window |
| duration_seconds | DOUBLE | Wall-clock |
| series_total / series_succeeded / series_failed | INT | Counts |
| error_message | STRING | Run-level error, if any |

### `audit.etl_series_run`
One row per `(run_id, series_id)` with extraction/write counts, `dq_passed`,
timing, and per-series error. `load_type` records the *effective* strategy used
for that run: `full` (first load or `load_type: full`) or `restate_last_<n>`.

### `audit.data_quality_result`
One row per `(run_id, series_id, check_name)` with `passed`, `severity`
(info/warning/error), `message`, and `metric_value`.

## bronze

### `bronze.fred_api_response`
Verbatim FRED payloads (system of record), partitioned by `series_id`.

| Column | Type | Notes |
|---|---|---|
| run_id | STRING | Owning run |
| series_id | STRING | Series |
| endpoint | STRING | e.g. `series/observations` |
| request_params | STRING | JSON of request params (**api_key never stored**) |
| response_payload | STRING | Verbatim FRED JSON |
| observation_count | INT | Count in payload |
| payload_bytes | INT | Payload size |
| ingested_at | TIMESTAMP | Ingestion time |

## silver

### `silver.fred_observation`
Normalized observations. **Natural key / MERGE key:**
`(series_id, observation_date, realtime_start)`.

| Column | Type | Notes |
|---|---|---|
| series_id | STRING | Series |
| observation_date | DATE | The date the value describes |
| realtime_start | DATE | Vintage window start (when value became known). **NULL/blank for `vintage_enabled: false` series** — vintage is not tracked, so the key is `(series_id, observation_date)` and re-runs update in place |
| realtime_end | DATE | Vintage window end (`9999-12-31` = still current → NULL); also blank for non-vintage series |
| value | DOUBLE | Parsed numeric value (NULL if missing) |
| raw_value | STRING | Original FRED string (`.` preserved) |
| is_missing | BOOLEAN | True when FRED returned `.` |
| row_hash | STRING | sha256 change-detection hash |
| revision_number | INT | 1…N per (series_id, observation_date) |
| ingested_at | TIMESTAMP | Ingestion time |
| run_id | STRING | Owning run |

## gold

### `gold.fred_point_in_time`
Full vintage history (every revision). Query with a real-time filter for
as-of-date reconstruction.

### `gold.fred_latest_observation`
Latest revision per `(series_id, observation_date)` — "as revised today".

### `gold.fred_macro_feature_daily`
Daily calendar × series grid, forward-filled from latest observations. Columns:
`as_of_date`, `series_id`, `raw_value` (only on native release dates), `value`
(forward-filled). Suitable for optimizer inputs and ML feature matrices.

### Views (`gold.v_*`)
* `v_latest_revised` — latest revision per date (backs the Gold table).
* `v_point_in_time` — every vintage row; filter by real-time window.
* `v_series_latest_value` — most recent non-missing value per series.
