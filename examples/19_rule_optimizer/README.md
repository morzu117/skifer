# 19 — Static rule optimizer report

**What it shows:** `RuleAnalyzer` reads registered Python rules without running them and reports
redundant work. The rules deliberately rewrite `is_priority` and repeatedly read `amount`, earning
real `OVERWRITE` and `SHARED_READ` warnings.

## Run it

```bash
python examples/19_rule_optimizer/run.py
```

There are two paths because they serve different callers. Direct `RuleAnalyzer` use needs no Spark
and is the sensible CI check. `engine.explain_rules(schema)` is the documented convenience API, but
`SkiferEngine(force_env="LOCAL")` starts a local Spark session in its constructor even though the
analysis is static. That startup cost is a wart, not a feature; neither path reads data.

## What you should see

```text
Direct RuleAnalyzer path (Spark-free; suitable for CI):
  make_priority: source_available=True, reads=['amount'], writes=['is_priority']
  OVERWRITE: rules=['make_priority', 'rewrite_priority'], column=is_priority
  SHARED_READ: rules=['make_priority', 'rewrite_priority', 'make_discount'], column=amount
  source_unavailable: source_available=False; skipped, not guessed

Engine path (documented API; constructor starts local Spark):
   [SparkFactory] Local PySpark + Delta Lake session initialized.
   [SparkFactory] Warehouse: ...

=======================================================
 Rule Analysis Report
=======================================================

 make_priority
   Writes : is_priority
   Reads  : amount

 rewrite_priority
   Writes : is_priority
   Reads  : amount

 make_discount
   Writes : discount
   Reads  : amount

 source_unavailable
   (source not available — skipped)

 ----------------------------------------
 Warnings
 ----------------------------------------

  [OVERWRITE] Column 'is_priority' is written by 2 rules: make_priority, rewrite_priority. Each rule overwrites the previous result.

  [SHARED_READ] Column 'amount' is read by 3 rules: make_priority, rewrite_priority, make_discount. If the condition on 'amount' is identical, consider extracting it to a shared rule computed once.

=======================================================

Engine returned: profiles=4, warnings=2
  OVERWRITE: rules=['make_priority', 'rewrite_priority'], column=is_priority
  SHARED_READ: rules=['make_priority', 'rewrite_priority', 'make_discount'], column=amount
```

The unavailable rule is a registered builtin whose Python source cannot be inspected. It remains in
both reports and is explicitly skipped: static analysis gives a lower bound and never invents a
profile.

## Remember

Use `RuleAnalyzer` directly for Spark-free CI; static warnings expose likely redundancy but never guess through unavailable source.

## Next

[20 — Builder and orchestration](../20_builder_and_orchestration/) validates an offline LLM draft
before exporting disposable orchestration artifacts.
