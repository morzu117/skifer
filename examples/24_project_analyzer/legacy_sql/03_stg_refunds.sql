-- Staging: refunds.
-- COALESCE(discount, 0) -- occurrence 1/3 of a recurring expression (04, 08
-- also derive it) -> shared business-rule candidate (Plan 43 43.4).
-- NOT IN on a nullable column: a NULL 'reason' makes the whole predicate
-- UNKNOWN, silently dropping the row -- a residue the mapper must flag, not
-- translate as a plain 'not_in' filter (Plan 43 43.2).
CREATE OR REPLACE TABLE silver.stg_refunds AS
SELECT
    refund_id,
    order_id,
    status,
    reason,
    COALESCE(discount, 0) AS discount
FROM bronze.refunds_raw
WHERE reason NOT IN ('DUPLICATE', 'FRAUD');
