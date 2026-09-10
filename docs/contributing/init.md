# init.md — Skifer Multi-Agent Workflow Bootstrap

## Purpose

This repository uses a controlled multi-agent workflow for Codex-assisted
development.

The goal is a disciplined delivery chain for a Spark/Databricks-first Python
library:

- explicit architecture shaping before implementation;
- scoped implementation tasks;
- task scoring and model routing;
- structured review and bounded fix loops;
- atomic commits per workflow step;
- shared stable memory in `.gstack/` and feature memory in `features/`;
- clear escalation rules.

When starting a new feature, Codex must read and follow this file before
modifying application code.

## Operating Principles

Agents do not negotiate freely with each other. They exchange structured
artifacts only: feature brief, architecture notes, task breakdown, scoring,
implementation summary, review findings, revision summary, decision log.

The human user owns product decisions, roadmap priority, business trade-offs,
irreversible architecture decisions, and acceptance of known debt.

The Architecture Agent owns design, trade-offs, task decomposition, scoring,
and escalation analysis. It must not implement application code unless
explicitly instructed.

The Developer Agent implements one scoped sub-task at a time. It must not
expand scope, rewrite architecture, add dependencies, or perform unrelated
refactors without escalation.

The Review Agent verifies scoped changes against the original task and
architecture notes. It produces structured findings and targeted re-reviews.

## Repository Workflow Structure

```text
.agents/
  architect.md
  developer.md
  reviewer.md
  orchestrator.md
  scoring.md
  escalation-policy.md

.gstack/
  project-context.md
  architecture-principles.md
  coding-standards.md
  testing-policy.md
  decision-log.md

features/
  _template/
    brief.md
    architecture-notes.md
    task-breakdown.md
    scoring.json
    implementation-plan.md
    review-report.md
    review-loop.md
    final-summary.md

adr/
```

`.gstack/` stores stable, validated, reusable project knowledge. Feature-specific
working memory belongs under `features/<FEATURE-ID>/`.

## Workflow States

```text
DRAFT
  -> SHAPED
  -> SCORED
  -> PLANNED
  -> IN_PROGRESS
  -> DEV_DONE
  -> REVIEWED
  -> FIXED
  -> INTEGRATED
  -> ACCEPTED
```

The Orchestrator must not advance state unless required artifacts and exit
conditions are satisfied.

## Feature Bootstrap

For every new feature, create:

```text
features/<FEATURE-ID>/
  brief.md
  architecture-notes.md
  task-breakdown.md
  scoring.json
  implementation-plan.md
  review-report.md
  review-loop.md
  final-summary.md
```

Use identifiers such as `FEAT-001`, `BUG-001`, `TECH-001`, or a plan-aligned ID
such as `PLAN26-stabilize-spark-runtime`.

## Architecture Contract

Architecture output must include:

- problem statement;
- functional scope;
- non-functional constraints;
- architecture impact;
- likely impacted files or modules;
- assumptions and open questions;
- risks and escalations;
- sub-tasks with acceptance criteria;
- verification strategy;
- scoring and model recommendation.

After shaping, create a documentation-only commit:

```bash
git add features/<FEATURE-ID>/ .gstack/ adr/
git commit -m "docs(<FEATURE-ID>): shape implementation plan"
```

## Developer Contract

The Developer Agent receives only the assigned sub-task, acceptance criteria,
relevant architecture notes, relevant `.gstack/` standards, expected tests, likely
impacted files, and current branch state.

For `src/skifer/` changes, the mandatory repository workflow still
applies:

1. Modify source in `src/skifer/`.
2. Add or update tests in `tests/`.
3. Add an entry to `CHANGELOG.md` under `## [Unreleased]`.
4. Do not bump `pyproject.toml` version.

When implementing a roadmap plan, one plan point maps to one separate commit.

## Review Contract

Findings use these severity levels:

- `blocker`
- `major`
- `minor`
- `nit`

Findings use these types:

- `correctness`
- `test_failure`
- `missing_test`
- `security`
- `performance`
- `maintainability`
- `architecture_drift`
- `unclear_requirement`
- `style`

Review verdicts:

- `APPROVED`
- `APPROVED_WITH_COMMENTS`
- `CHANGES_REQUESTED`
- `ESCALATE_TO_ARCHITECT`
- `ESCALATE_TO_HUMAN`

## Review Loop Policy

V1 is sequential and conservative:

- one Architecture Agent;
- one Orchestrator Agent;
- one Developer Agent;
- one Review Agent;
- one sub-task at a time;
- maximum two review cycles;
- maximum one developer fix cycle;
- maximum one targeted re-review.

If a finding remains unresolved after one developer fix cycle, escalate.

## Shared Brain Policy

Canonical memory:

- `.gstack/` for stable project knowledge;
- `features/<FEATURE-ID>/` for feature-specific working memory;
- `adr/` for long-lived architecture decisions;
- `CHANGELOG.md` for release-facing change history.

Local retrieval:

- Prefer the `gbrain` CLI for semantic lookups when available.
- Keep runtime indices such as `.brain-runtime/` and `.pip-cache/` out of git.
- Retrieval results are advisory and must not override repository files or
  explicit human decisions.

## Verification Defaults

For documentation-only workflow changes:

```bash
python3 -c "import tomllib; tomllib.load(open('pyproject.toml','rb'))"
```

For source changes:

```bash
.venv/bin/python -m pytest tests/ -x --tb=short
.venv/bin/python -m ruff check src/
```

Spark tests may require an unsandboxed local run because Py4J binds to localhost.

