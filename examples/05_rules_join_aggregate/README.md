# 05 — Python rules, joins, and aggregates

**What it shows:** the half of the framework the other examples never reach. The YAML says
**what** the pipeline does; a registered Python rule says **how** one business term is computed.
Two CSV sources are joined, the rule labels each order, and a declarative `aggregate` folds the
result with a `having` filter.

## Run it

```bash
python examples/05_rules_join_aggregate/run.py
```

## The two halves, side by side

The YAML names the rule. It does not contain it:

```yaml
business_rules:
  - classify_order
```

`run.py` defines it, and nothing else about the pipeline:

```python
@RuleRegistry.register_rule()
def classify_order(df):
    return {
        "order_class": F.when(F.col("amount") >= 500, F.lit("priority")).otherwise(
            F.lit("standard")
        )
    }
```

A projection rule returns a mapping of column name to expression. It never selects, never joins,
never writes. Consecutive projection rules are fused into a single `select()`, so adding a tenth
rule does not add a tenth pass over the data.

## What you should see

```
Joined rows after Python rule `classify_order`:
+--------+-------------+------+------+-----------+
|order_id|customer_name|region|amount|order_class|
+--------+-------------+------+------+-----------+
|1001    |Alice        |EMEA  |1200  |priority   |
|1002    |Alice        |EMEA  |300   |standard   |
|1003    |Bob          |APAC  |700   |priority   |
|1004    |Carla        |LATAM |100   |standard   |
+--------+-------------+------+------+-----------+
Aggregated rows after `having total_amount > 500`:
+------+------------+------------------+
|region|total_amount|distinct_customers|
+------+------------+------------------+
|APAC  |700         |1                 |
|EMEA  |1500        |1                 |
+------+------------+------------------+
Refused schema: [aggregate] 'aggregate' and 'select_final' are mutually exclusive.
```

LATAM is missing from the second table: its total is 100, and `having` drops it after grouping.
That is the difference between `having` and `filter` — one refuses rows before the fold, the other
refuses groups after it.

## Two files, because `aggregate` is terminal

[`joined_orders.yaml`](joined_orders.yaml) stops at `select_final`, so you can see the rows the rule
produced. [`orders_by_region.yaml`](orders_by_region.yaml) aggregates them. Declaring the earlier
stage is how you inspect it — not by editing the aggregating schema at runtime.

## The refusal

[`refused_aggregate_and_select.yaml`](refused_aggregate_and_select.yaml) declares both `aggregate`
and `select_final`, and is refused when the schema loads, before Spark is touched. Aggregation
already defines the final shape; letting a second block define it again would leave the output
depending on evaluation order.

## Remember

The YAML says what, the Python says how, and the boundary between them is a name.

## Next

[06 — Nested partials](../06_nested_partials/) keeps a rule-produced join key in memory instead of
writing an intermediate table.
