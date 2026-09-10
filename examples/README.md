# Examples

Every example here **runs**, locally, with no Databricks account and no cluster:

```bash
pip install -e ".[spark]"
python examples/01_first_pipeline/run.py
```

They are executed by the test suite (`tests/test_examples.py`), so an example that stops working
breaks the build like any other regression. What you read here is what the code does today.

| # | Example | What it shows |
|---|---|---|
| 01 | [First pipeline](01_first_pipeline/) | A Bronze → Silver pipeline in YAML: filters, deduplication, column shaping |
| 02 | [Quality and contract](02_quality_and_contract/) | Contract checks, staged publication, and a certification record |
| 03 | [Semantic query and evidence](03_semantic_and_question/) | Deterministic semantic SQL, an offline question, and its evidence |
| 04 | [Governed capability](04_governed_capability/) | A shadow proposal, zero external execution, and a visible refusal |
| 05 | [Rules, join, and aggregate](05_rules_join_aggregate/) | Python business rules, a join, and declarative aggregation with `having` |
| 06 | [Nested partials](06_nested_partials/) | A rule-produced join key kept inline or exposed as a temporary view |
| 07 | [Sources and shaping](07_sources_and_shaping/) | JSON, grouped filters, a registered loader, development limits, and pre-aggregation columns |
| 08 | [Materialized view](08_materialized_view/) | Pure-Python SQL compilation, definition hashes, and an uncompilable-rule refusal |
| 09 | [Streaming table](09_streaming_table/) | Finite incremental runs, automatic checkpoints, and CDC type 1 upsert |
| 10 | [Monitor and quarantine](10_monitor_and_quarantine/) | A failed contract check, unchanged consumer data, and a row-tagged quarantine snapshot |
| 11 | [Lineage and data dictionary](11_lineage_and_dictionary/) | Static column provenance, Mermaid rendering, and the honest boundary of opaque rules |
| 12 | [Runtime tracing](12_tracing/) | Nested in-memory spans, attribute redaction, identity omission, and exporter isolation |
| 13 | [Semantic projection](13_semantic_projection/) | Deterministic managed drafts, three-way sync, and CI exit codes |
| 14 | [Semantic domain graph](14_domain_graph/) | Declared join paths, fanout safety, and versioned calendar periods |
| 15 | [GenBI agent](15_genbi_agent/) | Stubbed natural language interpretation and deterministic names-to-SQL resolution |
| 16 | [Read-only MCP boundary](16_mcp_readonly/) | Scope-filtered discovery, closed query schema, and hard service budgets |
| 17 | [Supervised adaptive Gold](17_adaptive_gold/) | Privacy-safe usage aggregation, explainable proposals, and human review refusals |
| 18 | [Capability execution](18_capability_execution/) | Exact approvals, idempotent external effects, redacted credentials, and audited compensation |
| 19 | [Rule optimizer](19_rule_optimizer/) | Spark-free static rule profiles, redundancy warnings, and the engine report path |
| 20 | [Builder and orchestration](20_builder_and_orchestration/) | Offline LLM drafting, schema-validation refusal, and disposable orchestration export |
| 21 | [Hub routing](21_hub_routing/) | Spark-free routing to static lineage and dictionary agents, with honest missing-agent refusals |
| 22 | [Quality agent](22_quality_agent/) | Live checks, explicit execution errors, response-level critical status, and temporary history |

Read them in order for a guided tour, or jump directly to the feature you need; every example is self-contained about its local inputs.
