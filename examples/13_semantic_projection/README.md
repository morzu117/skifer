# 13 — Semantic projection and synchronization

**What it shows:** a pipeline declares its `data_product`, output `contract`, and semantic seed
in one file. `SemanticDraftBuilder` deterministically projects the managed semantic model from
that source, while `semantic sync --check` exposes stable exit codes for CI.

## Run it

```bash
python examples/13_semantic_projection/run.py
```

No Spark session, API key, or network access is needed. The script creates a temporary working
directory for the managed draft, curated model, catalog, and edited pipeline candidates, then
removes the whole directory before exiting.

## What you should see

```text
Managed draft projected from pipeline:
  model: orders_summary
  fields: country, orders
  generated marker: semantic_draft
Draft generated twice: bytes identical = True
semantic sync --check (current): exit 0
semantic sync --check (added total_amount): exit 2
semantic sync --check (changed grain): exit 3
Refused unmanaged draft: Refusing to overwrite curated model without --promote.
Temporary workspace removed: True
```

The first check is current. Adding a contract output and matching aggregate measure is safe
drift. Changing the declared grain is a conflict requiring a human decision. Those are the CLI's
CI contract: `0` current, `2` drift, and `3` conflict.

The refusal is raised by `SemanticDraftBuilder` itself. A YAML file without the managed
`_generated_by` marker is human-owned, so draft generation will not overwrite it.

## Remember

The semantic model is derived from the pipeline in the same change as the table, never written by hand afterwards.

## Next

[14 — Semantic domain graph](../14_domain_graph/) resolves declared relationships and calendar
periods before any SQL can exist.
