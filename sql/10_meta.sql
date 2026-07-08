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
