-- Bronze layer: raw API payload retention (multi-source).
-- Parameterize {{catalog}}. Column names mirror fred_pipeline.bronze.

USE CATALOG {{catalog}};

CREATE TABLE IF NOT EXISTS bronze.fred_api_response (
    run_id            STRING NOT NULL,
    source            STRING NOT NULL,   -- upstream API: 'fred', 'bls', 'eia', ...
    series_id         STRING NOT NULL,
    endpoint          STRING,            -- the source endpoint actually called
    request_params    STRING,   -- JSON string of request params (api_key redacted)
    response_payload  STRING,   -- verbatim upstream JSON response
    observation_count INT,
    payload_bytes     INT,
    ingested_at       TIMESTAMP
)
USING DELTA
PARTITIONED BY (series_id)
COMMENT 'Raw, verbatim API responses, multi-source (system of record)'
TBLPROPERTIES (
    delta.autoOptimize.optimizeWrite = true,
    delta.autoOptimize.autoCompact = true
);
