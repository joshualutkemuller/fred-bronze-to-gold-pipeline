-- Meta layer: series reference + manifest registry.
-- Parameterize {{catalog}}.

USE CATALOG {{catalog}};

-- One row per FRED series in the universe (mirrors manifest SeriesSpec).
CREATE TABLE IF NOT EXISTS meta.fred_series (
    series_id                 STRING  NOT NULL,
    title                     STRING,
    category                  STRING,
    frequency                 STRING,
    units                     STRING,
    active                    BOOLEAN,
    load_type                 STRING,
    expected_update_frequency STRING,
    vintage_enabled           BOOLEAN,
    validation_profile        STRING,
    business_owner            STRING,
    technical_owner           STRING,
    downstream_use_case       STRING,
    priority                  INT,
    restate_records           INT,
    min_value                 DOUBLE,
    max_value                 DOUBLE,
    tags                      ARRAY<STRING>,
    updated_at                TIMESTAMP
)
USING DELTA
COMMENT 'Series reference metadata sourced from manifests'
TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- Registry of manifest files loaded (audit of the universe over time).
CREATE TABLE IF NOT EXISTS meta.fred_manifest (
    manifest_name STRING NOT NULL,
    description   STRING,
    version       INT,
    source_path   STRING,
    series_count  INT,
    loaded_at     TIMESTAMP
)
USING DELTA
COMMENT 'Manifest files registered with the pipeline';

-- Mapping of series -> manifest (a series belongs to exactly one manifest).
CREATE TABLE IF NOT EXISTS meta.fred_series_manifest_map (
    series_id     STRING NOT NULL,
    manifest_name STRING NOT NULL,
    updated_at    TIMESTAMP
)
USING DELTA
COMMENT 'Series-to-manifest membership';

-- Governance: FRED-reported lifecycle snapshots (append-only, tracked over time).
CREATE TABLE IF NOT EXISTS meta.fred_series_lifecycle (
    series_id                   STRING NOT NULL,
    fred_title                  STRING,
    fred_frequency              STRING,
    fred_units                  STRING,
    seasonal_adjustment         STRING,
    observation_start           STRING,
    observation_end             STRING,
    last_updated                STRING,
    popularity                  INT,
    discontinued                BOOLEAN,
    days_since_last_observation INT,
    is_stale                    BOOLEAN,
    checked_at                  STRING
)
USING DELTA
COMMENT 'Point-in-time snapshots of FRED-reported series health';

-- Governance: drift between manifest intent and live FRED metadata.
CREATE TABLE IF NOT EXISTS meta.fred_series_drift (
    series_id      STRING NOT NULL,
    field          STRING,
    manifest_value STRING,
    fred_value     STRING,
    kind           STRING,   -- frequency_mismatch | discontinued | units_changed | not_found
    severity       STRING,   -- info | warning | error
    detected_at    STRING
)
USING DELTA
COMMENT 'Manifest-vs-FRED metadata drift findings';
