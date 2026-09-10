# 10 — Monitor a batch and quarantine failures

**What it shows:** a good batch is promoted under a data contract, then a second batch fails a
critical `NullCheck`. The failed batch is preserved in quarantine with row-level violation tags,
while consumers continue to read the previous good table.

## Run it

```bash
python examples/10_monitor_and_quarantine/run.py
```

The local certification registry is temporary. The good publish overwrites this example's target
before the failing publish, so rerunning the command is deterministic even when the local Delta
warehouse already contains an earlier run.

## What you should see

```text
Good batch promoted to the consumer table:
  order_id=2 | amount=80
  order_id=1 | amount=120
Framework refusal: DataQualityError
Failed check: NullCheck — Column 'order_id' has 1 NULL value(s).
Consumer table unchanged after refusal: True
  order_id=2 | amount=80
  order_id=1 | amount=120
Recorded publication state: QUARANTINED
Quarantine run id: ...
Quarantined snapshot (the complete rejected batch):
  order_id=4 | amount=40 | _violations=<none>
  order_id=None | amount=999 | _violations=NullCheck:0
```

The `NullCheck` is derived from `quality_checks.drop_nulls_in`. The example rule introduces the
null after source cleanup so the invalid row reaches the staged table: checks run against staging,
not against the consumer table. Both rows from the rejected batch remain in its snapshot; only the
offending row carries `NullCheck:0` in `_violations`.

## Remember

Promotion is all-or-nothing: a bad batch is preserved for diagnosis while the last good table stays live.

## Next

[11 — Lineage and data dictionary](../11_lineage_and_dictionary/) derives column provenance without
starting Spark.
