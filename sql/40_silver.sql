-- Silver layer: normalized, deduplicated observations.
-- Parameterize {{catalog}}. Column names mirror fred_pipeline.transform.SILVER_COLUMNS
-- plus revision_number added by transform.assign_revision_numbers.
-- Natural key (MERGE key): (series_id, observation_date, realtime_start).

USE CATALOG {{catalog}};

CREATE TABLE IF NOT EXISTS silver.fred_observation (
    series_id        STRING  NOT NULL,
    observation_date DATE    NOT NULL,
    realtime_start   DATE,
    realtime_end     DATE,
    value            DOUBLE,
    raw_value        STRING,   -- original FRED string ('.' preserved as-is)
    is_missing       BOOLEAN,
    row_hash         STRING,   -- sha256 change-detection hash
    revision_number  INT,      -- 1..N per (series_id, observation_date)
    ingested_at      TIMESTAMP,
    run_id           STRING
)
USING DELTA
PARTITIONED BY (series_id)
COMMENT 'Normalized FRED observations, idempotent via MERGE on natural key'
TBLPROPERTIES (
    delta.autoOptimize.optimizeWrite = true,
    delta.autoOptimize.autoCompact = true,
    delta.enableChangeDataFeed = true
);
