# 01 — Your first pipeline

**What it shows:** a Bronze → Silver pipeline written entirely in YAML. No PySpark, no SQL, no
cluster.

## Run it

```bash
pip install -e ".[spark]"
python examples/01_first_pipeline/run.py
```

Spark starts in `local[*]` mode with Delta Lake. Nothing is sent anywhere.

## What you should see

The input CSV has **7 rows**. The table you get back has **4**:

```
+--------+-----------+------+----------+----------+-------------+
|order_id|customer_id|region|amount_eur|order_date|source_system|
+--------+-----------+------+----------+----------+-------------+
|1001    |C-1        |EMEA  |1250.5    |2026-01-14|erp          |
|1002    |C-2        |APAC  |88.0      |2026-01-15|erp          |
|1003    |C-1        |EMEA  |430.25    |2026-01-16|erp          |
|1005    |C-2        |APAC  |2100.75   |2026-01-18|erp          |
+--------+-----------+------+----------+----------+-------------+
```

Three rows were dropped, each by a line you can point at in
[`silver_orders.yaml`](silver_orders.yaml):

| Row | Why it is gone |
|---|---|
| `1004` — CANCELLED | `filter: "status:not_equals:CANCELLED"` |
| `1006` — no amount | `filter: "amount:is_not_null"` |
| `1003` — appears twice | `quality_checks: drop_duplicates_on: [order_id]` |

## Three things worth noticing

**The YAML says *what*, never *how*.** There is no `withColumn`, no `join`, no `select`. The filter
reads `status:not_equals:CANCELLED` — an analyst can review that line without knowing Spark.

**The pipeline writes a table; it does not return a DataFrame.** `run_from_yaml` returns nothing.
The product of a pipeline is the table, and reading it back is a separate step — the same one a
downstream job or a dashboard performs.

**It wrote to `silver_<yourname>`, not to `silver`.** In interactive mode the engine resolves your
personal sandbox schema automatically, so you cannot overwrite a shared table by running an example.
In a job or in production the suffix disappears and the same YAML writes to plain `silver`.

## Next

[02 — Quality checks and a data contract](../02_quality_and_contract/) turns this table into a
**certified** one, and shows what happens when the data does not honour its contract.
