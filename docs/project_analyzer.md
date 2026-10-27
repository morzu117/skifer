# Project analyzer: from existing SQL to a Skifer pipeline draft

`skifer analyze` turns SQL you already have — scripts, Databricks/Jupyter notebooks, or
`system.query.history` — into a first draft of Skifer pipeline YAML, shared `RuleRegistry` rule
stubs, and a report that says exactly what it could convert and what it left for a human. No LLM
is involved in any of that: every structural decision comes from parsing the statement with
`sqlglot` against the same closed vocabulary `core/op_catalog.py` already uses, and every mapped
draft is proven by recompiling it back to SQL (`compile_select`) and comparing it, structurally,
to the source. An LLM only ever gets to *name* things, as a later, optional step — see
[§7](#7-optional-llm-enrichment-names-and-descriptions-only).

```text
.sql / notebooks / query history ──▶ front-ends ──▶ sqlglot AST (dialect databricks)
                                                            │
                                 ┌──────────────────────────┤
                                 ▼                          ▼
                     mapper: statement → schema       DAG of statements → split
                     (closed op catalogue)             (partials, intermediate tables)
                                 │                          │
                                 ▼                          ▼
                     round-trip oracle                 cross-script fingerprints
                     compile_select ⇄ source           → shared rules, divergences
                                 │                          │
                                 └──────────┬───────────────┘
                                            ▼
                     emitter: managed drafts + rule stubs + report
                                            │ (optional) parity run on a disposable table
                                            │ (optional) LLM enrichment: names, descriptions
                                            ▼
                     schemas/.drafts/<target>.yaml   rules/_generated/<name>.py   report
```

This page walks the whole thing against `examples/24_project_analyzer/` — a synthetic 8-script
corpus (bronze staging, a CTE-split silver enrichment, two gold tables computing the same business
concept two different ways, and a KPI query with a window function the grammar cannot express) built
to exercise every path at once. Run it yourself:

```bash
python examples/24_project_analyzer/run.py
```

## 1. `--inventory`: what is actually in the corpus

Before mapping anything, `--inventory` just counts raw `sqlglot` AST node types — no attempt at the
Skifer grammar yet:

```bash
skifer analyze "examples/24_project_analyzer/legacy_sql/*.sql" --inventory
```

Useful as a first look at an unfamiliar corpus: a high count of `Window` nodes, say, is a signal
that `--dry-run` will report several `needs_curation` statements before you even run it.

## 2. `--dry-run`: map, verify, write nothing

```bash
skifer analyze "examples/24_project_analyzer/legacy_sql/*.sql" --dry-run --format json
```

Each statement is mapped to a draft schema dict and the draft is round-trip-verified: recompiled to
SQL via `compile_select` and compared, structurally, to the original. The report
(`skifer/analyzer/report.py`) is a flat list, one entry per statement:

```json
{
  "statement_count": 8,
  "clean_count": 2,
  "statements": [
    {
      "source_path": "examples/24_project_analyzer/legacy_sql/01_stg_orders.sql",
      "line": 5,
      "target": "silver.stg_orders",
      "mapped": true,
      "roundtrip_status": "verified",
      "roundtrip_detail": null,
      "residues": []
    }
  ]
}
```

On this corpus, 6 of the 8 statements map and verify cleanly. The other two are reported
`needs_curation`, never silently dropped or guessed at:

- `08_kpi_monthly.sql`'s `LAG(total_amount) OVER (ORDER BY order_month)` has no equivalent in the
  declarative grammar for an arbitrary window function over a result set — a named residue, not a
  best-effort translation.
- the same script's trailing `UNION ALL` (the YTD row) is reported the same way.

A residue always names the construct (`code`, `message`, whether it is `blocking`) — it never
contains a table name, a filter value, or anything else specific to the data, so a report is safe to
paste into a ticket or a PR description as-is.

`--split` adds a second pass that classifies every script-level CTE or subquery into a partial, a
proposed intermediate table, or an inlined fragment, plus a Mermaid dependency graph:

```bash
skifer analyze "examples/24_project_analyzer/legacy_sql/*.sql" --dry-run --split --format json
```

`08_kpi_monthly.sql`'s two CTEs (`latest_snapshot`, `monthly`) become split units this way, and the
scalar subquery it shares, byte-for-byte, with `07_gold_margin_v2.sql` is flagged as an
intermediate-table candidate — both scripts should read one materialized total instead of repeating
the same query.

## 3. `--emit`: write validated drafts and rule stubs

```bash
skifer analyze "examples/24_project_analyzer/legacy_sql/*.sql" --emit --out /some/project/root
```

`--emit` finds row-level expressions that recur across the corpus — `UPPER(TRIM(country))`, found 3
times across 2 scripts in this corpus — fingerprints them the same `sha256:v1:` way
`adaptive/models.py` already does for usage patterns, and promotes anything clearing
`--min-occurrences`/`--min-scripts` (default: 3 occurrences across 2+ scripts) to a shared
`RuleRegistry` rule stub under `rules/_generated/`. Every draft that used to inline the expression
is rewired to call the rule via `business_rules:` instead of repeating it in `select_final:`.

It never arbitrates a genuine business disagreement: `06_gold_margin_v1.sql` computes `net_margin`
as `amount - cost`, `07_gold_margin_v2.sql` as `amount * (1 - margin_rate)` — the same output name,
two different formulas. Neither is promoted; both are reported, named side by side, for a human to
resolve. Unifying them automatically would be guessing at which one is correct.

Every draft (`schemas/.drafts/...`) and rule stub carries a `_generated_by: project_analyzer` marker
and a hash of the source it was generated from — the same managed-file pattern
`SemanticDraftBuilder` already uses for a semantic model draft:

- re-running `--emit` against an **unchanged** corpus is a byte-for-byte no-op (`status: "unchanged"`
  in the report) — safe to run in CI on every commit.
- a file that exists on disk **without** the marker (something a human wrote) is never overwritten
  (`status: "refused_exists"`); the run still exits non-zero so the collision is visible.
- `--strict` runs a dry preflight first and refuses to write **anything at all** the moment it finds
  any residue — no partial output to reconcile by hand.

## 4. Reading the report and exit codes

`skifer analyze`'s exit code is the contract a CI step checks, not the stdout text:

| Code | Meaning |
|---|---|
| `0` | Clean: every statement mapped and verified (`--dry-run`), or every artifact written/unchanged with no residue (`--emit`). |
| `1` | A technical error (unreadable file, missing optional dependency, a mapping crash). |
| `2` | A residue was reported — a statement needed curation, or `--emit` skipped something (or, under `--strict`, refused to write anything at all). |
| `4` | `--emit` found an existing file without the `project_analyzer` marker and left it untouched. |
| `5` | `analyze parity` found a measured discrepancy against the legacy table (§6). |

Running `examples/24_project_analyzer/run.py` end to end prints exactly this: `--emit` on the sample
corpus exits `2` (two residues — the LAG window function and the divergent margin formulas — are
expected on this corpus, not a bug), while the artifacts that *could* be derived deterministically
are still written.

## 5. Notebook and query-history front-ends

The same three modes also accept, in place of a `.sql` path:

- a **Databricks `.py` notebook export** or a **Jupyter `.ipynb` notebook** — a `%sql`/`%%sql` magic
  cell is extracted the same way as a `.sql` file's statements, and a plain code cell is scanned for
  a `spark.sql(...)` call via Python's own `ast` module. Only a single positional literal string
  argument is accepted: an f-string, a variable, a concatenation, or a keyword argument is reported
  as a named residue, never interpolated or guessed at.
- **`system.query.history`** itself, via `skifer analyze history`:

  ```bash
  skifer analyze history --warehouse-id <id> --days 7 [--schema cat.schema] [--profile name] --dry-run
  ```

  fetched through the Databricks SDK's Statement Execution API, deduplicated by the same content
  fingerprint the pattern-detection step uses. It never selects an author or user column — there is
  no identity to omit from a report, because it was never read in the first place — and it never
  reads a query's result data, only its text. Needs the optional `[databricks]` extra.

## 6. `analyze parity`: proof on data, not just on SQL

The round-trip oracle in §2 proves a draft compiles to the *same SQL text*, structurally, as the
source. It does not prove the draft produces the *same rows* once it actually runs on Spark — a
`LEFT JOIN` with a three-valued `NULL` comparison, for instance, can compile identically and still
behave differently at execution time. `analyze parity` closes that gap before a cutover:

```bash
skifer analyze parity schemas/.drafts/silver/orders_enriched.yaml \
  --against cat.silver.orders_enriched_legacy --target-layer silver [--key order_id]
```

It runs the draft through the real engine into a **disposable** table — never the draft's own
eventual target, never the legacy table itself — then compares row counts, columns, a row hash, and
(with `--key`) a per-key mismatch count, before dropping the disposable table. The report never
carries a data value, only counts and column names: it is safe to paste into a PR the same way a
`--dry-run` report is. Exit `0` means identical, `5` means a measured discrepancy, `1` a technical
error. This is the only `analyze` mode that needs a real Spark session — every other mode in this
page is Spark-free.

## 7. Optional LLM enrichment: names and descriptions only

```bash
skifer analyze "examples/24_project_analyzer/legacy_sql/*.sql" --emit --out . --enrich
```

`--enrich` asks a configured `LLMProvider` for an advisory rule name and a one-sentence docstring per
shared-rule candidate, a one-sentence description per draft, and a grouping suggestion across
candidates — added under an `"enrichment"` key on the printed report, **never written to a draft or
stub on disk**. With no configured provider, `--enrich` is a silent no-op: same report, no
`"enrichment"` key, exit code unaffected.

This is the one place in the whole pipeline where an LLM is involved at all, and the rule is
absolute: **the LLM never decides structure.** Concretely:

- every call sends the LLM only what is already proven safe — a candidate's canonical,
  qualifier-stripped expression shape, or a draft's already-declared output column names — never a
  table name, a join condition, or a filter value;
- the response is parsed against a **closed allowlist of keys**; any key outside it rejects the
  whole response, never a partially-accepted one;
- a proposed rule name is purely advisory: it is validated as a Python identifier, but it is **never
  used** to rename the generated function or its `business_rules:` wiring — doing so would cross
  from naming into structure.

This mirrors `SemanticBuilder.build_from_projection()`'s own discipline for enriching a projected
semantic model: the LLM's output is always treated as untrusted input, never as a decision.

## Limits, by design

- A statement the mapper cannot translate is `needs_curation` — named, never guessed at. Two
  documented, permanent examples: an arbitrary window function with no declarative equivalent
  (`LAG`, `08_kpi_monthly.sql`), and `sqlglot`'s `databricks` dialect reading an ANSI `''`-escaped
  apostrophe as two adjacent concatenated string literals rather than one escaped quote (Spark
  itself accepts both forms; `sqlglot` only the backslash form) — a real script using `''` needs
  this flagged as a residue, not silently mistranslated.
- A genuine business divergence (two formulas computing the same output name) is reported side by
  side, never unified by guessing which one is right.
- `analyze parity` needs a real Spark session; everything else in this page does not.
- `--enrich` only ever proposes a name or a description; it cannot add a dimension, rename a managed
  field, or otherwise change what a draft computes.

## See also

- [Core Engine → Project analyzer](core.md#project-analyzer-skifer-analyze-plan-43) — the quick
  command reference.
- `examples/24_project_analyzer/` — the runnable example this page walks through (`run.py`, the
  8-script corpus, expected output).
- the Plan 43 roadmap (`docs/roadmap/43_project_analyzer_plan.md`) — the full design history and
  the risks table (sqlglot's own escaping gap, DNF explosion bounds, column-collision detection, …);
  excluded from the published site's nav, like every other roadmap document.
