-- Gold: net margin, method 2 -- amount times (1 - margin rate). Same business
-- concept as 06_gold_margin_v1.sql, computed differently (Plan 43 43.4).
-- The scalar subquery below is byte-for-byte identical to the one in
-- 08_kpi_monthly.sql -> an intermediate-table candidate (Plan 43 43.3):
-- both scripts should read one materialized total, not repeat the query.
CREATE OR REPLACE TABLE gold.margin_v2 AS
SELECT
    o.order_id,
    o.amount,
    o.amount * (1 - p.margin_rate) AS net_margin,
    (SELECT SUM(amount) FROM silver.orders_enriched) AS total_orders_amount
FROM silver.orders_enriched o
JOIN bronze.products_raw p ON o.product_id = p.product_id;
