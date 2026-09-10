# 08 — Materialized views and SQL compilation

**What it shows:** a `materialization: materialized_view` schema becomes one compiled
`SELECT` and one `CREATE MATERIALIZED VIEW` statement. The compiler and definition hash are
pure Python, so this example deliberately never creates a `SkiferEngine` or starts Spark.

## Run it

```bash
python examples/08_materialized_view/run.py
```

## What you should see

```text
Compiled SELECT:
WITH `orders` AS (
  SELECT * FROM `silver`.`orders`
  WHERE `status` = 'COMPLETED'
)
SELECT `country`, SUM(`amount`) AS `total_revenue`, COUNT(`order_id`) AS `order_count`
FROM (
  SELECT *
  FROM `orders`
) AS `_skifer_src`
GROUP BY `country`
Definition hashes:
same schema:      2ec0910bae8d... == 2ec0910bae8d...
changed schedule: a5c09a07032f...
schedule changed hash: True
CREATE MATERIALIZED VIEW DDL:
CREATE MATERIALIZED VIEW `gold`.`revenue_by_country`
  CLUSTER BY (`country`)
  COMMENT 'Completed revenue by country'
  TBLPROPERTIES ('skifer.definition_hash' = '2ec0910bae8dbc71c6fec83cd0fe53fd14506c17ae57e384dda71bf746f41d4c')
  SCHEDULE EVERY 6 HOURS
AS
WITH `orders` AS (
  SELECT * FROM `silver`.`orders`
  WHERE `status` = 'COMPLETED'
)
SELECT `country`, SUM(`amount`) AS `total_revenue`, COUNT(`order_id`) AS `order_count`
FROM (
  SELECT *
  FROM `orders`
) AS `_skifer_src`
GROUP BY `country`
Refused schema: Schema validation failed with 1 error(s):
  [materialized_view] 'business_rules' cannot run in a materialized view — Python rules are not expressible in SQL. Materialize the rule output in an upstream (silver) table, then join or aggregate that table here.
```

The hash covers both the compiled query and definition-bearing options such as `schedule`,
`comment`, and clustering. Recompiling the same schema yields the same hash; changing only the
schedule yields another hash and would make the engine use `CREATE OR REPLACE`.

The refused schema names a Python `business_rule`. Python can shape an upstream table, but it
cannot become part of a persisted SQL definition, so the loader refuses it before compilation.

## Remember

A materialized view is compiled SQL, not a DataFrame that Spark writes.

## Next

[09 — Streaming tables](../09_streaming_table/) keeps state in a checkpoint and incrementally
upserts only newly arrived rows.
