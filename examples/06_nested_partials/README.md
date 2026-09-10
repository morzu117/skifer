# 06 — Nested partials

**What it shows:** a child YAML whose Python rule creates the key used by its parent's join, first
kept inline and then exposed as a temporary view for debugging.

## Run it

```bash
python examples/06_nested_partials/run.py
```

## What you should see

```
Mode: inline
2001 | C-1 | Alice | 900
2003 | C-1 | Alice | 75
Intermediate temp view exists: False
Mode: temp_view (set in run params, absent from YAML)
Intermediate temp view exists: True
Refused partial: [partials] Partial alias 'customers' collides with a 'tables' alias/name. Aliases must be unique across 'partials' and 'tables'.
```

Both modes write the same parent result. Inline adds no artifact; `temp_view` adds an inspectable,
session-scoped view. The mode is passed to `run_from_yaml` because materialization is a run-global
debug choice, not part of the declarative schema.

The refusal is real load-time validation: a partial and a table cannot claim the same alias.

## Remember

A partial lets a parent consume a rule-produced column directly; materialize it only when debugging
needs an inspectable artifact.

## Next

[07 — Sources and shaping](../07_sources_and_shaping/) combines grouped filters, a registered
loader, a development limit, and pre-aggregation columns.
