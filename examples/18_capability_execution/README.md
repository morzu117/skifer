# 18 — Governed capability execution

**What it shows:** example 04's shadow proposal now crosses a fake external-system boundary under a
human approval. The real governed executor binds that approval to one request, replays duplicates
without a second side effect, and records compensation as a separate audited action.

## Run it

```bash
python examples/18_capability_execution/run.py
```

No Spark, API key, network, or real ticket system is used. Two explicitly registered in-process
executors append to call lists, making external side effects countable independently of the
framework's journal. History lives in a temporary SQLite database that is removed afterwards.

## What you should see

```text
Exact request approval applies: True
One-character change refusal: Approval request_hash does not match the current request.
Expired-approval refusal: Approval has expired.
First execution result: {'ticket_id': 'ticket-1'}
Same request submitted twice; external create calls: 1
Second result replayed from history: True
Original audit before compensation: EXECUTING -> EXECUTED
Successful compensation result: succeeded
Original audit after compensation: EXECUTING -> EXECUTED -> COMPENSATING -> COMPENSATED
Compensation's own audit: EXECUTING -> EXECUTED
Compensation has a separate request hash: True
Failed compensation result: failed (TimeoutError)
Failed original stays: COMPENSATING
Failed compensation's own audit: EXECUTING -> FAILED
External compensation calls: 2
Compensation is an audited action, not a rollback.
Credential lease repr: CredentialLease(subject=<redacted>, audience=<redacted>, scopes=<redacted>, expires_at=<redacted>, secret=<redacted>)
Secret present in repr: False
Only reveal() returns the secret: True
Temporary history removed: True
```

The same approval passes for `Network issue` and fails for `Network issues`: one character changes
the canonical request hash. Expiry is an independent real refusal, with no clock tolerance.

Submitting the approved request twice leaves the fake external create adapter at one call. The
second result is reconstructed from the terminal journal event. It is the external call list—not
the framework's belief about itself—that proves idempotence.

Compensation does not erase or rewrite the original events. It adds `COMPENSATING`, runs the declared
compensation capability under a different request hash, then adds `COMPENSATED` only on success. A
failed compensation has its own `FAILED` audit while the original remains `COMPENSATING`: recovery
still needs attention and is never presented as a rollback.

The credential lease has no serializable secret-bearing representation. `repr()` is fully redacted;
the single explicit `reveal()` boundary is the only method that returns the secret.

## Remember

Approval authorizes one exact request; the journal prevents duplicate effects, and compensation is a new governed action.

## Next

[19 — Rule optimizer](../19_rule_optimizer/) statically reports redundant business rules and the
honest boundary of unavailable Python source.
