# 02 — Quality checks and a data contract

**What it shows:** the same table as example 01, now **published under a contract**. This is where
governance starts, and it is opt-in: a pipeline without a `data_product` block keeps the plain write
path of example 01, unchanged.

## Run it

```bash
python examples/02_quality_and_contract/run.py
```

## What you should see

```
Certification for this dataset:
  status           : CERTIFIED
  checks passed    : True
  contract version : 1.0.0
  certified at     : 2026-...
```

## What actually happened

Declaring `data_product` in the YAML routed the write through a different path:

```
build the DataFrame
      │
      ▼
stage it in _skifer_staging     ← the target table is untouched so far
      │
      ▼
run the contract checks against the staged data
      │
      ├── all critical checks pass ──▶ promote to gold.fact_orders, record CERTIFIED
      │
      └── a critical check fails ────▶ quarantine the staged snapshot, tag the offending
                                        rows, and LEAVE THE PREVIOUS TABLE IN PLACE
```

The point of staging first is that a bad run never reaches consumers. The previous good version of
the table stays exactly where it was.

## Why the engine refuses to run without a monitor

```python
engine.monitor = DataMonitor(engine.backend)
engine.certification_store = store
```

A pipeline that declares a contract but is given nothing to check it with, or nowhere to record the
verdict, **fails fast**. It does not publish an uncertified table and hope someone notices. This is
the general posture of the framework: an unanswerable question is refused, never answered anyway.

## What the certification is for

The record you printed is not a report for humans to file away. It is what the **semantic layer and
any agent consult before answering a question** about this table. With
`semantic_certification_policy: enforce` on an environment, a dataset that is not certified — or
whose certification is older than the configured maximum age — is simply not queryable.

That is the single gate mentioned on the front page: the same certification decides whether a
dashboard may read the table and whether an agent may.

## Next

[03 — Ask a question in natural language](../03_semantic_and_question/) turns this certified table
into a semantic model and queries it in plain language.
