# Plan 06 — Rule Optimizer

## Context

Business rules are free-form Python functions (`df → df`). When multiple rules read or compute the same expressions (e.g. `amount >= 1000`), the framework has no visibility into it — each rule is a black box. The goal is not automatic correction (too risky for user behavior) but **making redundancies visible** via a static analysis report. The user stays in control; the framework informs.

Analogy: a linter, not a compiler optimizer.

## Scope

- Best-effort AST static analysis of rule functions
- Detection of written columns (`withColumn` / `with_column`) and read columns (`F.col`, `col`, `df["col"]`)
- Detection of redundant patterns across rules (OVERWRITE, SHARED_READ, DUPLICATE_EXPR)
- Human-readable report via `engine.explain_rules(schema_dict)`
- No change to the existing execution behavior

## Files Created / Modified

| File | Action | Role |
|---|---|---|
| `src/skifer/core/rule_analyzer.py` | Created | `RuleAnalyzer`, `RuleProfile`, `RuleWarning` |
| `src/skifer/core/core.py` | Modified | `explain_rules()` method on `SkiferEngine` |
| `src/skifer/__init__.py` | Modified | Export `RuleAnalyzer` |
| `tests/test_rule_analyzer.py` | Created | 27 unit tests (pure Python, no Spark) |
| `tests/test_engine_fake_backend.py` | Modified | 4 integration tests for `explain_rules` |
| `CHANGELOG.md` | Modified | `[Unreleased]` entry |

## Architecture

### `RuleProfile`
Dataclass capturing per-rule analysis: `name`, `output_columns`, `input_columns`, `raw_expressions`, `source_available`.

### `RuleWarning`
Dataclass for a detected redundancy: `level` (warning/info), `code` (OVERWRITE/SHARED_READ/DUPLICATE_EXPR), `message`, `rules`, `column`.

### `RuleAnalyzer`
- `analyze_rule(func, name)` — parses AST via `inspect.getsource()` + `ast.parse()`, detects `withColumn`/`with_column` (outputs) and `F.col`/`col`/`df["col"]` (inputs)
- `analyze_rules(rule_names)` — delegates to `RuleRegistry.get_rule()` for each name
- `detect_warnings(profiles, shared_read_threshold=2)` — produces OVERWRITE, SHARED_READ, DUPLICATE_EXPR warnings
- `print_report(profiles, warnings)` — human-readable stdout report

### `engine.explain_rules(schema_dict, shared_read_threshold=2)`
Dry-run method on `SkiferEngine`. Returns `(profiles, warnings)` for programmatic use.

## Known Limitations

- Dynamically constructed column names (e.g. `df.withColumn(var, ...)`) are not detected.
- Rules defined in a REPL or compiled notebook may not expose source code (`source_available=False`, silently skipped).
- Complex closures or class-based rules may produce incomplete profiles.

## Feature 7 — Runtime Resource Optimization (future)

The current implementation is a **logical optimizer** (linter). It detects redundancies but does not change execution behavior.

A future `engine.run_optimized(schema_dict)` mode would move from visibility to actual resource savings:

- **Shared column materialization**: columns identified as SHARED_READ are computed once by an auto-injected rule, then consumed by downstream rules — eliminating redundant expression evaluation.
- **Strategic caching**: `df.cache()` injected at the right point in the rule chain so that shared intermediate results are not recomputed across rules.
- **Platform-aware**: meaningful on Snowpark and BigQuery where there is no Catalyst CSE; on Spark, complements rather than duplicates Catalyst's own optimization.
- **Transparent and opt-in**: standard `run_process_to_table()` is unchanged; `run_optimized()` is an explicit choice with a dry-run preview of what was rewritten.

Prerequisite: the `RuleAnalyzer` profiles produced in this plan are the input contract for Feature 7.

## Verification

```bash
pytest tests/test_rule_analyzer.py -v      # 27 unit tests
pytest tests/ -x --tb=short               # 542 tests, full suite
```
