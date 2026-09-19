# 25 — Incremental merge and SCD2 snapshot

**What it shows:** the same two days of orders, written twice through two write
strategies, and what separates them. `incremental merge` keeps one row per key
carrying the latest values. `snapshot` keeps the history: a changed row is closed
and a new version opened, so the table still answers what an order looked like
yesterday.

The point of both is the **second** run. A single run leaves the same rows whichever
strategy you pick — including a plain overwrite, which is neither of them. That is
why this example runs each pipeline twice before asserting anything.

## Run it

```bash
pip install -e ".[sql]"
python examples/25_incremental_snapshot/run.py
```

It runs on DuckDB (`force_env="LOCAL_SQL"`), so it needs no Spark session and no
warehouse. Both strategies are compiled to SQL and are equally available on the
Spark path.

## The two days

`data/orders_day1.csv` and `data/orders_day2.csv` differ in four deliberate ways,
one per behaviour worth proving:

| Order | Day 1 | Day 2 | What it exercises |
|---|---|---|---|
| 1 | `pending` | `shipped` | a change: merge overwrites, SCD2 versions |
| 2 | `shipped` | `shipped`, identical | **no** change: neither may create a version |
| 3 | present | absent | a disappeared row, governed by `on_missing` |
| 4 | absent | present | a new key |

Order 2 is the one that catches a wrong implementation. An SCD2 write that closes
and reinserts every key produces the correct answer for orders 1 and 4, and a
spurious second version here.

## What you should see

```text
incremental merge — 4 row(s), one per order:
  1 | Ada        | shipped  | 100
  2 | Grace      | shipped  | 250
  3 | Alan       | pending  | 80
  4 | Katherine  | pending  | 410

snapshot SCD2 — 5 row(s), versions included:
  1 | Ada        | pending  | 100 | closed ...
  1 | Ada        | shipped  | 100 | open
  2 | Grace      | shipped  | 250 | open
  3 | Alan       | pending  |  80 | open
  4 | Katherine  | pending  | 410 | open
```

The closing time is the run's own clock, elided above; every other value is fixed
by the two CSV files.

Order 3 survives day 2 in both tables, for different reasons: merge never touches a
key absent from the source, and the snapshot was told to read absence as a partial
extract.

## The decision the framework refuses to make for you

[`snapshot.yaml`](snapshot.yaml) must declare `on_missing`; there is no default.

```yaml
materialization:
  type: snapshot
  strategy: check
  unique_key: [order_id]
  check_columns: [status, amount]
  on_missing: ignore    # or: close
```

A row that stops appearing in the source means one of two incompatible things, and
only the person who knows the extract can say which. `close` reads it as a deletion
and ends the row's validity. `ignore` reads it as a partial or late extract and
leaves the row open. Guessing either one corrupts history quietly: a truncated
export would close every customer, and a genuine deletion would stay open forever.

`strategy: check` compares the columns you list. Use `strategy: timestamp` with
`updated_at:` instead when the source carries a reliable modification time — it is
cheaper, because it compares one column rather than several.
