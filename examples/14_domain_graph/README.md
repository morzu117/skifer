# 14 — Semantic domain graph and calendars

**What it shows:** three semantic models declare entities and relationships. From names alone,
`SemanticPlanner` chooses a join path and `QueryResolver` compiles its declared keys into SQL.
The same pre-SQL planning boundary refuses fanout, many-to-many aggregation, and ambiguous dates.

## Run it

```bash
python examples/14_domain_graph/run.py
```

No Spark session is created. The `gold.*` tables need not exist because this example stops after
planning and compilation.

## What you should see

```text
🧠 [SemanticEngine] Catalogue chargé — 3 modèles indexés.
Names supplied: metric=revenue, dimension=customers.segment
Resolved join path:
  orders --orders_customers (many_to_one)--> customers
Planner supplied the aliases and join condition; no LLM supplied SQL.
Compiled SQL:
SELECT
    m1.`segment` AS segment,
    SUM(m0.`amount`) AS revenue
FROM `gold`.`orders` AS m0
LEFT JOIN `gold`.`customers` AS m1
  ON m0.`customer_id` = m1.`customer_id`
GROUP BY m1.`segment`
Versioned calendar:
  FY2025_H1 -> 2025-02-01 through 2025-07-31 (retail_445 v2025.1)
Refused unsafe fanout: Unsafe fanout: metric grain 'order' crosses one_to_many relationship 'order_lines'.
Refused many-to-many path: Relationship 'lines_customers_unbridged' is many_to_many, which is not queryable for a metric because it fans the metric out. Route the query through a declared bridge model, or restate the relationship at a grain where it is many_to_one.
Refused ambiguous dates: Query cannot combine period with date_from or date_to; choose one unambiguous date constraint.
```

The caller supplies `revenue` and `customers.segment`. Positional aliases `m0` and `m1`, the
`customer_id` equality, join type, and physical SQL columns all come from the YAML model graph.
The LLM emits no SQL, join condition, or alias.

`FY2025_H1` is also just a name. Its dates come from the versioned
`calendars/retail_445.yaml` definition. Supplying both that name and a free date is refused instead
of choosing one silently.

## Remember

Semantic names are planned into declared paths and dates before any SQL exists.

## Next

[15 — GenBI agent](../15_genbi_agent/) lets a stub LLM choose those names, then gives them to the
deterministic resolver.
