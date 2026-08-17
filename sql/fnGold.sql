-- fnGold: Databricks → Power BI abstraction layer
-- Resolves ${p_Catalog} in queries to the current catalog
-- Usage: SELECT * FROM fnGold('gold', 'macro_indicator_dashboard')
-- Returns: fully-qualified table name for the catalog context
--
-- For SQLite mirror, substitute inline: fnGold('gold', 'table') → gold_table

CREATE OR REPLACE FUNCTION fnGold(schema STRING, table_name STRING)
RETURNS STRING
LANGUAGE SQL
IMMUTABLE
AS
  SELECT CONCAT(current_catalog(), '.', schema, '.', table_name)
