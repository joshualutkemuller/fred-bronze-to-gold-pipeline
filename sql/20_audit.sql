-- Audit layer: run-level + series-level lineage and data-quality results.
-- Parameterize {{catalog}}. Column names mirror fred_pipeline.audit.

USE CATALOG {{catalog}};

CREATE TABLE IF NOT EXISTS audit.etl_run (
    run_id            STRING NOT NULL,
    environment       STRING,
    manifest_path     STRING,
    triggered_by      STRING,
    status            STRING,   -- running | succeeded | failed | partial
    started_at        TIMESTAMP,
    ended_at          TIMESTAMP,
    duration_seconds  DOUBLE,
    series_total      INT,
    series_succeeded  INT,
    series_failed     INT,
    error_message     STRING
)
USING DELTA
COMMENT 'One row per pipeline invocation';

CREATE TABLE IF NOT EXISTS audit.etl_series_run (
    run_id                STRING NOT NULL,
    series_id             STRING NOT NULL,
    status                STRING,
    load_type             STRING,
    started_at            TIMESTAMP,
    ended_at              TIMESTAMP,
    duration_seconds      DOUBLE,
    observations_extracted INT,
    rows_written_bronze   INT,
    rows_merged_silver    INT,
    dq_passed             BOOLEAN,
    error_message         STRING
)
USING DELTA
COMMENT 'One row per (run, series)';

CREATE TABLE IF NOT EXISTS audit.data_quality_result (
    run_id       STRING NOT NULL,
    series_id    STRING NOT NULL,
    check_name   STRING NOT NULL,
    passed       BOOLEAN,
    severity     STRING,   -- info | warning | error
    message      STRING,
    metric_value DOUBLE
)
USING DELTA
COMMENT 'Data-quality check outcomes per (run, series, check)';
