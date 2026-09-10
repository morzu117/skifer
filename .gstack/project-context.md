# Project Context

## Status

- Workflow mode: Multi-Agent Codex V1.
- Product focus: Spark/Databricks-first Skifer stabilization.
- Runtime target: Databricks Lakehouse in production; Databricks Connect or
  local PySpark/Delta in development.

## Product Context

Skifer is a declarative data engineering framework that turns
Bronze/Silver/Gold Spark pipelines into YAML contracts plus isolated Python
business rules.

The current product strategy is to stabilize one runtime path: Spark and
Databricks. Legacy non-Spark backend code may remain for compatibility, but new
workflow decisions should not expand that surface unless the human explicitly
reopens it.

## Source Of Truth

- `init.md` defines the multi-agent workflow contract.
- `AGENTS.md` and `CLAUDE.md` define repo-level engineering rules.
- `.gstack/` stores stable shared knowledge.
- `features/<FEATURE-ID>/` stores feature-specific working memory.
- `adr/` stores long-lived architecture decisions.
- `docs/roadmap/` stores roadmap plans.
- `CHANGELOG.md` records release-facing changes.

