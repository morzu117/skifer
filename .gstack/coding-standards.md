# Coding Standards

## General Rules

- Follow existing repository conventions and local helper APIs.
- Make the smallest change that satisfies the task.
- Keep functions, modules, and diffs reviewable.
- Avoid unrelated refactors.
- Add succinct comments only where they clarify non-obvious logic.

## Skifer Rules

- Every `src/skifer/` change needs a corresponding test update and a
  `CHANGELOG.md` entry under `## [Unreleased]`.
- Never bump `pyproject.toml` version; the user manages releases.
- Prefer Spark/Databricks behavior when product direction is ambiguous.
- The product is Spark/Databricks-only (Plan 26); do not reintroduce non-Spark
  backends without explicit user direction.
- Preserve the semantic-layer rule: the LLM never writes raw SQL.

## Commit Rules

- Keep commits atomic and scoped to one workflow step.
- For roadmap implementation, one plan point maps to one separate commit.
- Record tests run and unresolved limitations in the feature summary.

