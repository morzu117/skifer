# 26 — Portable SQL on DuckDB

**What it shows:** one declarative pipeline compiles to Databricks SQL and DuckDB SQL.
DuckDB reads a local Parquet file, filters completed orders, drops null amounts, uppercases
country codes and aggregates revenue and customer counts. The capability report exposes the
portability status of every operation used. Raw `expr:` SQL is explicitly refused on DuckDB.
No Spark session or `SkiferEngine` is needed.

## Run it

Install DuckDB and the dialect renderer if needed:

```bash
pip install 'skifer[dialects]' duckdb
python examples/26_portable_sql/run.py
```

## What you should see

```text
Databricks SQL:
WITH `ord` AS (
  SELECT * FROM `silver`.`orders`
  WHERE `status` = 'DONE' AND `amount` IS NOT NULL
)
SELECT `country_code`, SUM(`amount`) AS `total_amount`, COUNT(`order_id`) AS `nb_orders`, COUNT(DISTINCT `customer_id`) AS `nb_customers`
FROM (
  SELECT *, UPPER(`country`) AS `country_code`
  FROM `ord`
) AS `_skifer_src`
GROUP BY `country_code`
HAVING `nb_orders` > '1'

DuckDB SQL:
WITH "ord" AS (SELECT * FROM "silver"."orders" WHERE "status" = 'DONE' AND NOT "amount" IS NULL) SELECT "country_code", SUM("amount") AS "total_amount", COUNT("order_id") AS "nb_orders", COUNT(DISTINCT "customer_id") AS "nb_customers" FROM (SELECT *, UPPER("country") AS "country_code" FROM "ord") AS "_skifer_src" GROUP BY "country_code" HAVING "nb_orders" > '1'

DuckDB result:
('DE', Decimal('50.00'), 2, 2)
('FR', Decimal('150.00'), 2, 1)
('IT', Decimal('25.00'), 2, 2)

Refused for duckdb: Capabilities refused for 'duckdb': 'op:expr': ...
```

The report also lists seven `supported` capabilities. The ES group has only one order and
is removed by `having`; the pending FR order and the null amount are removed before aggregation.
Repeated customers count once. The temporary Parquet directory is removed in a `finally` block.

## Remember

The same YAML is compiled once into relational SQL, then rendered for each target.
DuckDB support is backed by executed three-way value parity. A `conditional` capability only
has parity under its stated condition; this does not guarantee identical SQL types.
The Databricks SQL is displayed here; only DuckDB SQL is executed in this example.

## Next

[01 — First pipeline](../01_first_pipeline/) returns to the first example.
