-- Unity Catalog: catalog + schema bootstrap.
-- Parameterize {{catalog}} per environment (macro_dev / macro_test / macro_prod).
-- Run once per environment; safe to re-run.

CREATE CATALOG IF NOT EXISTS {{catalog}}
  COMMENT 'FRED macro data pipeline';

USE CATALOG {{catalog}};

CREATE SCHEMA IF NOT EXISTS meta    COMMENT 'Manifest + series reference metadata';
CREATE SCHEMA IF NOT EXISTS audit   COMMENT 'ETL run + data-quality lineage';
CREATE SCHEMA IF NOT EXISTS bronze  COMMENT 'Raw, verbatim FRED API payloads';
CREATE SCHEMA IF NOT EXISTS silver  COMMENT 'Normalized, deduplicated observations';
CREATE SCHEMA IF NOT EXISTS gold    COMMENT 'Analytics-ready features + PIT views';
CREATE SCHEMA IF NOT EXISTS sandbox COMMENT 'Ad-hoc quant research scratch space';

-- Raw archive volume for JSON payload retention (referenced by config.raw_archive_path).
CREATE VOLUME IF NOT EXISTS bronze.raw
  COMMENT 'Archived raw FRED JSON payloads';
