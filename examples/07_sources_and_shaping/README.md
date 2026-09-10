# 07 — Sources and shaping

**What it shows:** a checked-in JSON source, an OR-of-ANDs `filter_groups`, a Python-registered
loader, a local `dev_limit`, and an `add_columns` value used as an aggregate group key.

## Run it

```bash
python examples/07_sources_and_shaping/run.py
```

## What you should see

```
Registered loader supplied segment labels from Python.
Aggregated after grouped filters and dev_limit=3:
2026-01 | Business | total=80 | events=1
2026-01 | Retail | total=120 | events=1
2026-02 | Business | total=300 | events=1
Refused filter: Schema validation failed with 1 error(s):
  [table 'events' filter] column 'region': unknown filter operator 'equalz'.  Did you mean: ...
```

The grouped predicate keeps `(EMEA AND web) OR (APAC AND store)`. A flat list would AND every
condition and keep nothing. The development limit is applied after those filters; then the loader's
lookup is joined and `event_month` is built before the aggregation groups by it.

The misspelled operator is refused while loading the schema, with the framework's own suggestion.

## Remember

Source shaping happens in a deliberate order: filter groups, development limit, join, added columns,
then aggregation.

## Next

[08 — Materialized views](../08_materialized_view/) turns a declarative pipeline into a pure
SQL definition without starting Spark.
