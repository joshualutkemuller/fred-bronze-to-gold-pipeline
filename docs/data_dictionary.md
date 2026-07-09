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
Verbatim upstream payloads (system of record, multi-source), partitioned by
`series_id`.

| Column | Type | Notes |
|---|---|---|
| run_id | STRING | Owning run |
| source | STRING | Upstream API the payload came from (`fred`, `bls`, `eia`, …) |
| series_id | STRING | Series |
| endpoint | STRING | The source endpoint actually called (e.g. `series/observations`, `timeseries/data/{id}`, `seriesid/{id}`) |
| request_params | STRING | JSON of request params (**api_key never stored**) |
| response_payload | STRING | Verbatim upstream JSON |
| observation_count | INT | Rows normalized from the payload (accurate across sources) |
| payload_bytes | INT | Payload size |
| ingested_at | TIMESTAMP | Ingestion time |

## silver

### `silver.fred_observation`
Normalized observations (multi-source). **Natural key / MERGE key:**
`(source, series_id, observation_date, realtime_start)`.

| Column | Type | Notes |
|---|---|---|
| source | STRING | Upstream API the row came from (`fred`, `bls`, …). Leading key component so the same `series_id` could be sourced from more than one API without colliding |
| series_id | STRING | Series |
| observation_date | DATE | The date the value describes |
| realtime_start | DATE | Vintage window start (when value became known). **NULL/blank for `vintage_enabled: false` series** — vintage is not tracked, so the key is `(series_id, observation_date)` and re-runs update in place |
| realtime_end | DATE | Vintage window end (`9999-12-31` = still current → NULL); also blank for non-vintage series |
| value | DOUBLE | Parsed numeric value (NULL if missing) |
| raw_value | STRING | Original upstream string (FRED `.` preserved as-is) |
| is_missing | BOOLEAN | True when the value could not be parsed (e.g. FRED `.`) |
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

### `gold.fred_feature_transforms`
Per-series quant transforms from latest observations: `mom` (period-over-period
% change), `diff` (first difference), `yoy` (year-over-year % change), `zscore`
(expanding, point-in-time safe — mean/std computed only from observations
at-or-before each row's date, never later ones). Keyed `(series_id,
observation_date)`.

### `gold.fred_curve_spread`
Cross-series spreads (`long_leg − short_leg`) and ratios (`long_leg /
short_leg`), **defined in `config/spreads.yml`** (see
`fred_pipeline.spread_config.load_spread_defs`) rather than hardcoded —
review and add pairs there without touching Python. Ships with the original
4 Treasury curve spreads (`T10Y2Y`, `T10Y3M`, `T2Y3M`, `T30Y10Y`). A date is
only emitted when both legs have a non-missing value (and, for a ratio, the
short leg is nonzero). Columns: `spread_name`, `observation_date`,
`long_leg`, `short_leg`, `value`.

### `gold.fred_revision_stats`
How much each observation moved between its first print and today. Reads raw
Silver (every vintage), not latest-revision rows — it exists to measure
revision behavior itself. Columns: `series_id`, `observation_date`,
`revision_count`, `first_value`, `first_realtime_start`, `latest_value`,
`latest_realtime_start`, `revision_delta` (latest − first), `revision_pct`.
Non-vintage series (`vintage_enabled: false`) always have `revision_count = 1`
— no vintage history is tracked for them, so there's nothing to compare; that's
a legitimate "not revised" signal, not a data gap. Useful for judging how much
to trust a series' initial print (e.g. GDP/payrolls are heavily revised;
market/price series usually are not).

### Point-in-time feature snapshot
`gold.point_in_time_features_sql(as_of)` (Spark) / `LocalWarehouse.
point_in_time_features(as_of)` return each series' value **as it was known** on
`as_of` — a leakage-free feature vector for backtests.

### Views (`gold.v_*` on Databricks; `gold_v_*` in the local SQLite backend)
Defined in `sql/60_views.sql` (Delta) and mirrored by hand in
`local_store.py`'s schema for the local backend (SQLite has no `CREATE OR
REPLACE VIEW`, so these must be kept in sync manually between the two).
* `v_latest_revised` — latest revision per date (backs the Gold table).
* `v_point_in_time` — every vintage row; filter by real-time window.
* `v_series_latest_value` — most recent non-missing value per series.
* `v_series_revision_summary` — per-series rollup of `fred_revision_stats`
  (avg/max revision count, avg/max absolute revision %).
