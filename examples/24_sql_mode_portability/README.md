# 24 — SQL mode portability

**What it shows:** one YAML pipeline runs through two execution engines. The input,
projection, and output stay fixed; only the named implementation of `order_class`
changes. The script compares the rows itself and raises if Spark and DuckDB disagree.

The example keeps its CSV under `data/` instead of borrowing example 01's input so it
is self-contained: copying or moving another example cannot silently break this one.

## Run it

Install both optional runtimes, then run the script from the repository root:

```bash
pip install -e ".[spark,sql]"
python examples/24_sql_mode_portability/run.py
```

`LOCAL_SQL` is deliberately absent from `priority_check`; SQL mode is entered only
through `SkiferEngine(force_env="LOCAL_SQL")`. The script creates that engine before
the Spark engine.

## One YAML, two rule implementations

[`pipeline.yaml`](pipeline.yaml) names its business rule through one parameter:

```yaml
business_rules:
  - "{{ business_rule }}"
```

`run.py` registers `portable_order_class_spark` as the default PySpark projection
kind and `portable_order_class_sql` as `kind="sql"`. Each returns the same named
column. Selecting a rule implementation changes the **how**, while the YAML's
sources, filters, and final shape remain the **what**.

## What you should see

```text
Executed pipeline.yaml on DuckDB SQL with rule `portable_order_class_sql`: 3 rows
Executed pipeline.yaml on Spark with rule `portable_order_class_spark`: 3 rows
Equality check: PASS — 3 identical rows from one YAML.
  1001 | 1200 | priority
  1002 | 300 | standard
  1003 | 700 | priority
```

The `PASS` line is reached only after exact ordered dictionaries from both engines
compare equal. Two tables printed for visual inspection would not establish parity.

## Current SQL-mode boundary

- Example 02 (`02_quality_and_contract`) declares `data_product:`, so it uses
  certified publication. That path belongs to phase 39.5 and is not ported yet.
- Example 07 (`07_sources_and_shaping`) declares `source_type: loader`: a Python
  function constructs a DataFrame. No SQL equivalent has been designed; the adapter
  refuses this capability by name, which is the intended behavior. A portable loader
  remains an open design decision.
- Streaming, materialized views, and JDBC sinks remain Spark/Databricks-only.

## Remember

Portability is an executed equality claim: one declarative transformation, two rule
languages, and one programmatic comparison of the resulting rows.

## Next

[05 — Rules, join, and aggregate](../05_rules_join_aggregate/) develops the PySpark
rule side in more detail; the SQL limitations above define the current boundary.
