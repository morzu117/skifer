# 09 — Incremental streaming table with upsert

**What it shows:** one local Delta source is read with `streaming: true`, drained by an
`available_now` trigger, and written with CDC type 1 `upsert`. A second run reuses the same
automatic checkpoint after two source records are appended.

## Run it

```bash
python examples/09_streaming_table/run.py
```

The script calls `engine.full_refresh()` at startup. That explicitly clears this example's
target table and its `checkpoint: auto` directory under `.spark-warehouse/_checkpoints/`, so
repeating the command is deterministic even when artifacts from an earlier run remain.

## What you should see

```text
Startup reset: target table and automatic checkpoint cleared.
Run 1 added 2 target rows:
order_id=1 | status=pending
order_id=2 | status=pending
Checkpoint commits after run 1: 1
Run 2 added 1 target row, from 4 source rows:
order_id=1 | status=shipped
order_id=2 | status=pending
order_id=3 | status=pending
Checkpoint commits after run 2: 2
Order 1 was updated in place; it was not duplicated.
Refused schema: Schema validation failed with 1 error(s):
  [streaming] table 'orders': 'dev_limit' is incompatible with 'streaming: true' — limit() is unsupported on streaming DataFrames.
```

Run 1 backfills two rows. Before run 2, the script appends two Delta records: order 1 changes
to `shipped`, and order 3 arrives. The merge key updates order 1 and inserts order 3, so four
source rows leave three in the target.

**The table cannot prove the second run was incremental**, which is why the script prints the
checkpoint instead. Replaying the whole source with an upsert would produce exactly these rows.
The checkpoint records one committed micro-batch per run, and it is what makes the second run
read only the appended records rather than the original snapshot again.

The refused schema places `dev_limit` on the streaming source. Spark cannot apply an unbounded
streaming `limit()`, so Skifer refuses it while loading the YAML.

## Remember

The checkpoint decides what is new; the upsert key decides whether each new record updates or inserts.

## Next

[10 — Monitor and quarantine](../10_monitor_and_quarantine/) shows the failed half of certified
publication and proves that the last good consumer table stays in place.
