-- Staging: raw orders.
-- WHERE on the base table only, no join -> trivial push-down to a table-level
-- 'filter:' (Plan 43 43.2 baseline case, contrasted with 04's where:).
-- 3-branch CASE WHEN on order_status.
CREATE OR REPLACE TABLE silver.stg_orders AS
SELECT
    order_id,
    customer_id,
    product_id,
    order_status,
    amount,
    order_date,
    CASE
        WHEN order_status = 'NEW' THEN 'Pending'
        WHEN order_status = 'SHIPPED' THEN 'In transit'
        ELSE 'Closed'
    END AS status_label
FROM bronze.orders_raw
WHERE order_status != 'CANCELLED';
