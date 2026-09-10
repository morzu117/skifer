# 22 — Inspect live data with the quality agent

**What it shows:** `QualityAgent` runs a critical null check and uniqueness check against a real
local Delta table, preserves an unevaluable check as `ERROR`, renders the latest report, and stores
both runs in temporary SQLite history.

## Run it

```bash
python examples/22_quality_agent/run.py
```

The script overwrites its table in the engine's sandboxed `gold` schema on every run, so it is
idempotent. The SQLite store lives in a `TemporaryDirectory`, is closed explicitly, and is removed
before the script exits.

Always construct these contract dataclasses with `column=` or `columns=`. Their second positional
field is `severity`; passing a column there silently leaves the column name empty.

## What you should see

```text
[SparkFactory] Local PySpark + Delta Lake session initialized.
[SparkFactory] Warehouse: ...
Two critical checks on the real table:
  NullCheck: FAIL — Column 'amount' has 1 NULL value(s).
  UniqueCheck: FAIL — Columns ['order_id'] have 1 duplicate(s) (3 rows, 2 distinct).
  QualityResponse.passed: False

A check that could not execute:
  NullCheck status: ERROR
  Framework refusal: Check execution error: ...
  QualityResponse.passed: True

Latest text report: Status    : WARN

History after two runs:
  mode: history
  reports recorded: 2
  checks per run (newest first): 1, 2
Temporary history directory removed: True
```

`QualityResponse.passed` means **no critical failure**, not “every check passed.” The first response
is `False` because both failures are critical. The second is `True` even though its one warning-level
check is `ERROR`; the individual result is the source of truth when execution itself matters. A
check that could not run must never be indistinguishable from one that passed.

The latest text report is `WARN`, and `get_history()` returns two reports newest-first: one result
from the missing-column run and two results from the critical-check run.

## Remember

Read individual check statuses: response-level `passed` only means “no critical failure.”

## Next

Return to the [examples index](../) to choose a feature or revisit the sequence.
