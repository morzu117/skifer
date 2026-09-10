# 11 — Column lineage and the data dictionary

**What it shows:** where every output column came from, derived by reading the YAML and the Python
rules as source. No Spark session is created, no data is read, and the pipeline is never run.

## Run it

```bash
python examples/11_lineage_and_dictionary/run.py
```

It analyses [example 05](../05_rules_join_aggregate/), which joins two tables and adds a column
with a Python rule.

## What you should see

```text
Static analysis only: no Spark session was created.

A column read straight from a source table:
amount:
  from raw_orders.amount [select]

A column no source table contains, made by a Python rule:
order_class:
  from <rule>.order_class [select]
  from raw_orders.amount [rule] via rule:classify_order

Dictionary entry:
  field: gold.joined_orders.order_class
  sources: <rule>.order_class, raw_orders.amount
```

## `<rule>` is the point of this example

`order_class` does not exist in `raw_orders`. A Python rule computes it from `amount`. Lineage
therefore reports two things about it: the rule edge naming its real input and the rule that made
it, and a select edge whose origin is `<rule>` rather than a source table.

Naming a source table there would be worse than saying nothing. A reader would go looking for
`raw_orders.order_class`, find no such column, and have no way to tell whether the lineage or the
table was wrong. That is exactly what this framework did until Plan 34: the analyzer read output
columns only from `withColumn`, so projection rules — the documented default kind — contributed
nothing, and the select edge claimed the source table by default.

## What static analysis cannot see

```text
  opaque_rule declares outputs: <none detected>
```

A rule that builds its column name at runtime is invisible to an AST reader. The tracker omits it
rather than guessing. Lineage from static analysis is a lower bound on what a pipeline does, never
an upper one, and a rule written to be unreadable will not appear.

## Remember

Lineage that names a column no table contains is worse than lineage that admits it came from code.

## Next

[12 — Tracing](../12_tracing/) records what a run did, with the values redacted.
