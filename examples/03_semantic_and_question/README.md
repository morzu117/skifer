# 03 — Semantic query and evidence

**What it shows:** a business question answered from the certified `gold.fact_orders` table through
a hand-written semantic model, with the evidence that accompanies the answer. No LLM, API key, or
network call is involved.

## Run it

```bash
python examples/03_semantic_and_question/run.py
```

The script publishes the example 02 pipeline in-process first, so it also runs on a clean checkout.
It then asks for January revenue and order count by region using an explicit `SemanticQuery`.

## What you should see

```text
+------+-------------+-----------+
|region|total_revenue|order_count|
+------+-------------+-----------+
|APAC  |2188.75      |2          |
|EMEA  |1680.75      |2          |
+------+-------------+-----------+
```

## The LLM never writes the SQL

This is the boundary to remember:

```text
natural-language question
        │
        ▼
GenBIAgent proposes declared names only
  model: sales.orders
  metrics: total_revenue, order_count
  dimension: region
        │
        ▼
QueryResolver validates those names and builds SQL deterministically
        │
        ▼
SemanticEngine executes the SQL and records evidence
```

The example starts at the explicit `SemanticQuery`, so it exercises the deterministic half without
calling a `GenBIAgent`. An agent would add name selection from natural language; it would not add SQL
generation. If it proposed `total_revenues`, which does not exist, the resolver refuses the query and
suggests `total_revenue`. It does not invent a column or send invalid SQL to Spark.

## Why the evidence matters

`query_with_evidence()` returns the DataFrame and a standalone evidence record from the same
execution. The script prints three useful parts:

```text
SQL hash : sha256:v1:e198eea...
datasets : ['gold.fact_orders']
certification snapshot:
  gold.fact_orders: CERTIFIED (contract 1.0.0)
```

The hash identifies the SQL actually executed without disclosing it. The dataset list says what the
answer read, and the frozen certification snapshot says what the access decision knew at query time.
Here the environment uses `semantic_certification_policy: enforce`, so an absent or failed
certification would stop the query before execution.

## Next

[04 — Governed capability](../04_governed_capability/) crosses the other governance boundary: from
reading governed data to proposing an action against an external system.
