# Architecture Principles

## Current Principles

1. Keep YAML schemas ("what") strictly decoupled from Python business rules
   ("how").
2. Stabilize Spark/Databricks before expanding runtime targets.
3. Prefer explicit, fail-fast validation over silent fallbacks.
4. Preserve local PySpark/Delta development parity where possible.
5. Keep architecture decisions traceable through ADRs, roadmap plans, and the
   decision log.
6. Avoid adding dependencies or broad abstractions unless they remove real
   complexity in the current Spark-first product path.

## Repository Boundaries

- `src/skifer/core/` owns schema loading, execution context,
  interpretation, registry, sandboxing, and planning/execution helpers.
- `src/skifer/backends/spark.py` is the supported runtime backend.
- `semantic/` and `agentic/` must preserve the zero-LLM-SQL boundary:
  `QueryResolver` builds SQL deterministically from semantic YAML names.
- `observability/` and `lineage/` should reuse YAML-as-contract data instead of
  duplicating configuration.

