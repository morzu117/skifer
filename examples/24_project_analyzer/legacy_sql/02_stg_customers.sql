-- Staging: customers.
-- UPPER(TRIM(country)) -- occurrence 1/3 of a recurring expression (04, 05 also
-- derive it) -> shared business-rule candidate (Plan 43 43.4).
-- Apostrophe literal in a CASE WHEN. Written with a backslash escape, not
-- the ANSI '' convention: Spark/Databricks SQL supports both, but sqlglot's
-- databricks dialect only parses \' as an escaped quote -- '' is read as two
-- adjacent string literals (implicit concatenation), silently losing the
-- apostrophe. A real-world script using '' would need this called out as a
-- residue before mapping it (Plan 43 43.2).
CREATE OR REPLACE TABLE silver.stg_customers AS
SELECT
    customer_id,
    name,
    UPPER(TRIM(country)) AS country,
    region,
    CASE WHEN name = 'O\'Brien' THEN 1 ELSE 0 END AS is_vip
FROM bronze.customers_raw;
