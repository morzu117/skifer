-- Silver: orders enriched with refund status and customer geography.
-- LEFT JOIN stg_refunds followed by a WHERE on the RIGHT side (r.status):
-- collapses the left join to inner-join semantics on that predicate, so it
-- is NOT a push-down candidate -- it maps to a post-join 'where:' (Plan 43
-- 43.1/43.2), contrasted with 01's base-table WHERE.
-- amount * 1.2 -- occurrence 1/2 of a recurring expression with a literal
-- variant (08 uses 1.21 for the same VAT-style markup) -> flagged as a
-- divergence, not silently unified (Plan 43 43.4).
-- UPPER(TRIM(country)) and COALESCE(discount, 0): occurrences 2/3, reused
-- from the already-cleaned upstream staging tables.
CREATE OR REPLACE TABLE silver.orders_enriched AS
SELECT
    o.order_id,
    o.customer_id,
    o.product_id,
    o.amount,
    o.amount * 1.2 AS amount_with_vat,
    COALESCE(r.discount, 0) AS discount,
    UPPER(TRIM(c.country)) AS country
FROM silver.stg_orders o
LEFT JOIN silver.stg_refunds r ON o.order_id = r.order_id
LEFT JOIN silver.stg_customers c ON o.customer_id = c.customer_id
WHERE r.status = 'APPROVED';
