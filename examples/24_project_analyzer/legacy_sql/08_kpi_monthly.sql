-- Gold: monthly KPIs.
-- ROW_NUMBER() OVER (...) + WHERE rn = 1 dedups to the latest snapshot per
-- order -> maps to 'preprocess.qualify' (Plan 43 43.2).
-- LAG(...) OVER (...) has no equivalent in the declarative grammar for an
-- arbitrary window function over a result set -> a residue, surfaced rather
-- than silently dropped (Plan 43 43.2).
-- amount * 1.21 -- occurrence 2/2 of the VAT-style markup (04 uses 1.2 for
-- the same concept) -> literal-variant divergence (Plan 43 43.4).
-- COALESCE(discount, 0): occurrence 3/3 of the recurring expression.
-- The scalar subquery is byte-for-byte identical to 07's -> intermediate
-- table candidate (Plan 43 43.3); UNION ALL for the YTD row, GROUP BY +
-- HAVING on the monthly aggregate.
WITH latest_snapshot AS (
    SELECT
        order_id,
        amount,
        order_month,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY snapshot_date DESC) AS rn
    FROM silver.order_snapshots
),
monthly AS (
    SELECT
        order_month,
        SUM(amount) AS total_amount,
        SUM(COALESCE(discount, 0)) AS total_discount
    FROM latest_snapshot
    JOIN silver.stg_refunds USING (order_id)
    WHERE rn = 1
    GROUP BY order_month
    HAVING SUM(amount) > 1000
)
SELECT
    order_month,
    total_amount,
    total_amount * 1.21 AS total_amount_with_vat,
    total_discount,
    LAG(total_amount) OVER (ORDER BY order_month) AS prev_month_amount,
    (SELECT SUM(amount) FROM silver.orders_enriched) AS total_orders_amount
FROM monthly
UNION ALL
SELECT
    'YTD' AS order_month,
    SUM(total_amount) AS total_amount,
    SUM(total_amount) * 1.21 AS total_amount_with_vat,
    SUM(total_discount) AS total_discount,
    NULL AS prev_month_amount,
    (SELECT SUM(amount) FROM silver.orders_enriched) AS total_orders_amount
FROM monthly;
