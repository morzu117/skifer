# Decision Log

## DEC-0001 - Adopt Multi-Agent Codex Workflow V1

- Date: 2026-07-02
- Status: accepted
- Owner: Human/Codex
- Context: Skifer needs a disciplined gstack/gbrain-style
  development workflow.
- Decision: initialize `.agents/`, `.gstack/`, `features/_template/`, `adr/`,
  and `init.md` for this repository, adapted to the Spark/Databricks-first
  product strategy.
- Consequences:
  - feature work should now use shaping, scoring, sequential development, and
    structured review artifacts;
  - stable knowledge belongs in `.gstack/`;
  - feature-specific working memory belongs in `features/<FEATURE-ID>/`;
  - ADRs capture long-lived architecture decisions.

