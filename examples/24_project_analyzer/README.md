# 24 — Project analyzer: existing SQL to a Skifer pipeline draft

**What it shows:** `skifer analyze` turns a corpus of plain SQL scripts into Skifer artifacts
without an LLM — deterministic parsing, mapping, pattern detection and emission. `legacy_sql/`
is a synthetic 8-script corpus (bronze staging, a CTE-split silver enrichment, two divergent gold
margin formulas, a bare-SELECT KPI query) built to exercise every one of those paths at once.

## Run it

```bash
python examples/24_project_analyzer/run.py
```

This calls `run_analyze_command` directly — the same function `skifer analyze` dispatches to —
so the exit codes and report shapes are exactly what CI sees. The CLI's own per-file/JSON noise
is captured rather than printed verbatim; the equivalent real commands are below.

## What you should see

```text
=== --dry-run (map + round-trip-verify, nothing written) ===
statements: 8, verified clean: 2

=== --dry-run --split (adds the CTE/subquery dependency graph) ===
split units: 4, kinds present: ['intermediate_table', 'partial']

=== --emit (writes validated drafts + rule stubs) ===
exit code: 2 (2 = emitted, with residue reported — expected on this corpus)
artifacts written:
  rules/_generated/country.py
  schemas/.drafts/gold/margin_v1.yaml
  schemas/.drafts/gold/margin_v2.yaml
  schemas/.drafts/silver/orders_enriched.yaml
  schemas/.drafts/silver/orders_enriched_agg_sum_amount.yaml
  schemas/.drafts/silver/stg_customers.yaml
  schemas/.drafts/silver/stg_orders.yaml
  schemas/.drafts/silver/stg_refunds.yaml
skipped (partials + non-materialized SELECTs, by design — see emitter.py):
  examples/24_project_analyzer/legacy_sql/05_silver_customers_geo.sql
  examples/24_project_analyzer/legacy_sql/08_kpi_monthly.sql
  cleaned_customers_branch1
  cleaned_customers_branch2
  monthly

schemas/.drafts/silver/stg_customers.yaml business_rules wiring: ['business_rules:', '- country']
```

## The CLI directly

```bash
skifer analyze "examples/24_project_analyzer/legacy_sql/*.sql" --dry-run --format md
skifer analyze "examples/24_project_analyzer/legacy_sql/*.sql" --dry-run --split --format json
skifer analyze "examples/24_project_analyzer/legacy_sql/*.sql" --emit --out /some/project/root
```

`--dry-run` maps every statement to a draft schema and round-trip-verifies it against the source
SQL (`skifer/analyzer/report.py`); 6 of the 8 statements in this corpus verify cleanly, the other
2 are reported `needs_curation` (a `UNION` and a `ROW_NUMBER() OVER (...)` — both named, neither
silently dropped). `--split` additionally classifies every script-level CTE/subquery into a
partial, a proposed intermediate table, or an inlined fragment, and appends a Mermaid dependency
graph.

`--emit` finds expressions recurring across the corpus (`UPPER(TRIM(country))`, found 3 times
across 2+ scripts) and promotes them to a shared rule — `rules/_generated/country.py` — wired
into every draft that used to inline it via `business_rules:`, never left duplicated in
`select_final:`. It never arbitrates a genuine conflict: `06_gold_margin_v1.sql` and
`07_gold_margin_v2.sql` compute `net_margin` with two different formulas, so neither is promoted —
both stay a `DivergenceFinding` in the report, named side by side, for a human to resolve. Every
draft and stub carries a `_generated_by: project_analyzer` marker and a source hash: re-running
`--emit` against an unchanged corpus is a byte-for-byte no-op (`unchanged`), a hand-edited file
without the marker is never overwritten (`refused_exists`, exit 4), and `--strict` refuses to
write anything at all the moment any residue is detected in a dry preflight — no partial output.

## Remember

The analyzer is deterministic-first: every promotion, divergence and skip cites the rule or
threshold behind it, never a guess. A draft under `schemas/.drafts/` is a starting point for a
human to curate and promote, not a finished pipeline.

## Next

This is the last example in the walkthrough — see [the examples index](../README.md) for the
full list, [the full walkthrough page](../../docs/project_analyzer.md) for the report shape, exit
codes and the "LLM never decides structure" rule in detail, or
[the Plan 43 roadmap](../../docs/roadmap/43_project_analyzer_plan.md) for what is still ahead
(only 43.8's own documentation sync, by the time this line was last updated).
