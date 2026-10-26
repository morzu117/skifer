-- Silver: customer geography split by hemisphere.
-- Re-derives UPPER(TRIM(country)) independently of 02_stg_customers.sql,
-- straight from the bronze source -- occurrence 3/3 of the recurring
-- expression, this time duplicated across two SILVER scripts rather than
-- reused from one (Plan 43 43.4).
-- The 'cleaned_customers' CTE is referenced by BOTH branches of the UNION
-- ALL below -> a partial candidate (Plan 43 43.3), not two copies of the
-- same cleaning logic.
WITH cleaned_customers AS (
    SELECT
        customer_id,
        UPPER(TRIM(country)) AS country,
        region
    FROM bronze.customers_raw
)
SELECT customer_id, country, region, 'NORTH' AS hemisphere
FROM cleaned_customers
WHERE region IN ('EU', 'US', 'CA')
UNION ALL
SELECT customer_id, country, region, 'SOUTH' AS hemisphere
FROM cleaned_customers
WHERE region IN ('BR', 'AR', 'AU');
