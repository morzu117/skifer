-- Gold: net margin, method 1 -- amount minus the product's absolute cost.
-- Computes the SAME business concept as 07_gold_margin_v2.sql from a
-- DIFFERENT formula -- a business divergence the analyzer must surface, not
-- silently unify into one shared rule (Plan 43 43.4).
CREATE OR REPLACE TABLE gold.margin_v1 AS
SELECT
    o.order_id,
    o.amount,
    o.amount - p.cost AS net_margin
FROM silver.orders_enriched o
JOIN bronze.products_raw p ON o.product_id = p.product_id;
