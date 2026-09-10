# Getting Started

This path starts with one local YAML pipeline and ends at governed agent actions. Every example is
executed by `tests/test_examples.py`, uses local files only, and needs no API key, network access,
Databricks account, or cluster.

## 1. Install

From a checkout, install the package with its local Spark runtime:

```bash
pip install -e ".[spark]"
```

The full extras matrix—including Databricks, LLM providers, serving, tracing, and MCP—is in the
installation section of the project `README.md`.

## 2. Configure local mode

Skifer discovers `config.yaml` by walking upward from the current directory. The minimum
local configuration has `default_env` set to `LOCAL`, a `priority_check` containing `LOCAL`, and one
`LOCAL` environment with `catalog: null` and `is_production: false`.

`catalog: null` is the switch that selects local PySpark, Delta Lake, and the embedded Derby
metastore. Interactive runs also receive a personal schema suffix, so the first two examples cannot
overwrite a shared table. The repository's existing `config.yaml` already contains this local
configuration.

## 3. Run the first pipeline

```bash
python examples/01_first_pipeline/run.py
```

The script reads seven CSV rows, removes a cancelled order and a missing amount, and deduplicates the
business key. It shapes the remaining four rows and writes a Delta table in your local sandbox
schema. `run_from_yaml` returns nothing: the table is the pipeline product, and the script reads it
back explicitly to print it.

See `examples/01_first_pipeline/` for the input rows, YAML, expected table, and an
explanation of each removed row.

## 4. Add a contract and certification

```bash
python examples/02_quality_and_contract/run.py
```

Example 02 adds `data_product` and `contract` blocks to the pipeline you just ran. That opt-in routes
the write through staged publication: checks run before promotion, a passing snapshot becomes the
consumer table, and a failing one is quarantined while the previous good table remains in place. The
run prints the resulting `CERTIFIED` record from a local SQLite certification store.

Certification is operational state, not a report to file away. With semantic certification policy
set to `enforce`, the semantic layer refuses data whose certification is missing, failed, or too old.
See `examples/02_quality_and_contract/` for the complete publication flow.

## 5. Query semantic names and inspect evidence

```bash
python examples/03_semantic_and_question/run.py
```

Example 03 republishes example 02's certified table so it works from a clean checkout, then executes
an explicit `SemanticQuery` for revenue and order count by region. It prints the DataFrame and the
evidence from that same execution: the SQL hash, datasets read, and frozen certification snapshot.
No LLM is called.

The **LLM never writes the SQL**. A `GenBIAgent` adds natural-language selection of names already
declared in the semantic model; `QueryResolver` validates those names and builds SQL
deterministically. If a proposed name does not exist, the request is refused with nearest-name
suggestions rather than compiling an invented column.

See `examples/03_semantic_and_question/` for the model YAML, exact result, evidence,
and the visible unknown-name refusal.

## 6. Govern an action

```bash
python examples/04_governed_capability/run.py
```

Reading governed data and changing an external system are different boundaries. Example 04 loads a
closed capability definition and registers a fake in-process executor, but default `SHADOW` autonomy
wins over the document's more permissive `guarded` ceiling. The result is `PROPOSED`, with zero
executor calls and no credential broker.

The script also submits an undeclared `priority` argument and prints the refusal before showing that
the executor call count is still zero. There is no rollback because nothing ran. See
`examples/04_governed_capability/` for the capability catalog and complete output.

## 7. Where to go next

| You want to… | Read | Run |
|---|---|---|
| Define rules, joins, aggregates, partials, source shaping, streaming tables, or materialized views | [Core Engine](core.md) and [YAML reference](yaml_spec.md) | `examples/05_rules_join_aggregate/` through `examples/09_streaming_table/` |
| Understand governance from pipeline contract through agent action | [Governance](governance.md) | `examples/10_monitor_and_quarantine/` and `examples/18_capability_execution/` continue the introductory path |
| Define checks, contracts, certified publication, or runtime tracing | [Data Observability](observability.md) | `examples/10_monitor_and_quarantine/` and `examples/12_tracing/` |
| Inspect static column provenance and its limits | [Lineage & Data Dictionary](lineage.md) | `examples/11_lineage_and_dictionary/` |
| Build, synchronize, and plan semantic models | [Semantic Layer](semantic.md) | `examples/13_semantic_projection/` and `examples/14_domain_graph/` |
| Ask natural-language questions while keeping SQL deterministic | [Agentic Layer](agentic.md#genbiagent) | `examples/15_genbi_agent/` |
| Expose governed reads to external agents through MCP | [Read-only MCP](mcp.md) | `examples/16_mcp_readonly/` |
| Turn usage patterns into human-reviewed optimization proposals | [Supervised adaptive Gold](adaptive.md) | `examples/17_adaptive_gold/` |
| Govern external write-back approvals, idempotency, and compensation | [Governed capabilities](capabilities.md) | `examples/18_capability_execution/` |
| Look up exact Python classes, methods, and return types | [API Reference](api_reference.md) | — |
| Install on Databricks instead of running locally | [Databricks installation](install_databricks.md) | — |
