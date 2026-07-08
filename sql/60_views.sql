-- Analytical views over Silver: the two canonical lenses from the handoff doc.
-- Parameterize {{catalog}}.

USE CATALOG {{catalog}};

-- latest_revised: current best estimate of each observation (values as revised).
CREATE OR REPLACE VIEW gold.v_latest_revised AS
WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY series_id, observation_date
            ORDER BY realtime_start DESC
        ) AS rn
    FROM silver.fred_observation
)
SELECT series_id, observation_date, value, realtime_start, realtime_end,
       is_missing, revision_number, ingested_at
FROM ranked
WHERE rn = 1;

-- point_in_time: every vintage row. Query with a real-time filter, e.g.:
--   SELECT * FROM gold.v_point_in_time
--   WHERE realtime_start <= DATE '2020-06-30'
--     AND (realtime_end   >= DATE '2020-06-30' OR realtime_end IS NULL);
CREATE OR REPLACE VIEW gold.v_point_in_time AS
SELECT series_id, observation_date, realtime_start, realtime_end, value,
       revision_number, is_missing, ingested_at
FROM silver.fred_observation;

-- Convenience: latest value per series (for dashboards / spot checks).
CREATE OR REPLACE VIEW gold.v_series_latest_value AS
WITH latest AS (
    SELECT
        series_id,
        observation_date,
        value,
        ROW_NUMBER() OVER (
            PARTITION BY series_id ORDER BY observation_date DESC
        ) AS rn
    FROM gold.v_latest_revised
    WHERE is_missing = false
)
SELECT series_id, observation_date, value
FROM latest
WHERE rn = 1;
