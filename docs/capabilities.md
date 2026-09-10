# Governed capabilities (write-back)

This page documents governed external-system write-back for platform engineers and agent builders who need approvals, preconditions, credentials, and idempotent execution; for the conversational analytics layer, return to the [Agentic Layer](agentic.md).

A capability lets an agent trigger an action on an **external** system. It is the
only non-read-only surface in the framework, and every ambiguity in it is resolved
by refusing.

```
capability catalog YAML (lazy) ─▶ CapabilityRegistry ─▶ CapabilityDefinition
                                                              │
   agent picks an id + fills a closed input schema            │
                                                              ▼
                                        PreconditionEvaluator (ALLOW/DENY/UNKNOWN)
                                                              │ ALLOW
                                                              ▼
                                        AutonomyStateMachine (shadow/supervised/guarded)
                                                              │ approved
                                                              ▼
                                        CredentialBroker (just-in-time, injected provider)
                                                              │
                                                              ▼
                                        GovernedExecutor ─▶ append-only history
```

The LLM selects a `capability_id` and fills an input schema. It never decides the
policy, the preconditions, the approval, the credentials or the idempotency.

### The document cannot name Python

`executor` and every precondition `rule` are plain lowercase names resolved against
an explicit in-process registry. Anything containing a dot, a colon or a slash is
refused, and the modules contain no `importlib`, `__import__`, `eval` or dynamic
attribute lookup — asserted at source level. A capability YAML can select code that
was registered; it can never name code to import.

### The input schema is closed and bounded

`additionalProperties: false` at every level, mandatory `maxLength` / `maxItems` /
`minimum` / `maximum`, and outright refusal of `$ref`, `allOf`, `anyOf`, `oneOf`,
`not` and `patternProperties`. Indirect or unbounded schema constructs make the
accepted input set impossible to reason about, and that set is the attack surface.
Nesting is bounded too — depth is a size, and an unbounded one crashes the validator
rather than answering.

### Decisions fail closed

A precondition rule returns a typed `PreconditionOutcome` whose `reason_code` is a
closed lowercase identifier: a sentence, a retrieved document or a model's answer
cannot become a decision, it becomes `UNKNOWN`. A rule that raises becomes `UNKNOWN`
carrying only the exception class name. **`UNKNOWN` never becomes `ALLOW`** — a single
`DENY` refuses, and a single `UNKNOWN` escalates.

The state hash a rule observes is computed by the evaluator, never by the rule: the
attestation cannot be forged by what it attests. Preconditions are re-evaluated
immediately before execution, and any movement refuses.

### Autonomy resolves to the stricter side

The effective mode is the **more restrictive** of the capability's declared `approval`
and the environment's `capability_autonomy`, never the more permissive; absent
configuration means `shadow`. An `irreversible` capability can never reach `guarded`.

```yaml
environments:
  prod:
    capability_autonomy: supervised   # shadow (default) | supervised
```

In `shadow` nothing executes: the proposal is recorded, no credential is taken, and an
executor runs only if it declared dry-run support at registration — never by being
called to find out.

### An approval binds to one exact request

`ApprovalRecord` carries the actor, the reason, a bounded validity window (4 hours
maximum, enforced) and two hashes: the request and the precondition report. Changing
one character of one argument invalidates it. Expiry is checked separately, in UTC,
with no tolerance — which is why the precondition hash covers the rules, their verdicts
and the observed state, but never wall-clock time.

### Credentials are just-in-time and never stored

The provider is **injected**: Skifer never mints, signs or renews a
credential and is not an authorization server. The broker asks for exactly the
capability's declared scopes, refuses a TTL above the ceiling rather than clamping it,
and verifies the provider's answer — a lease coming back with extra scopes, a different
subject or a longer life is refused. The secret is reachable only through `reveal()`;
`repr`, `str`, logging and `json` render it redacted, and pickling raises rather than
serialising it.

### Execution is idempotent, and compensation is an action

The request identity is a hash of the capability version, the normalised input and the
subject. `GovernedExecutor` consults the append-only journal first, writes the pre-call
`EXECUTING` event **before** the external call — the only thing that makes a lost
response recoverable — and replays the recorded outcome for a duplicate instead of
calling again. A lost response is never retried blindly: either the executor declared a
reconciliation lookup, or a human reconciles.

Compensation is a first-class audited run with its own identity and its own events, not
a rollback: it never rewrites the original run, and a failed compensation stays
`COMPENSATING` rather than being recorded as compensated.

`examples/18_capability_execution/` crosses a fake external boundary and prints
one-character and expired-approval refusals, one external call for a duplicated
request, and the separate audit histories for successful and failed
compensation. It also shows that credential representations remain redacted.

A live credential echoed back by an executor is redacted out of the stored outcome and
out of the returned result identically, so a replay stays byte-for-byte identical to the
first call. The journal is append-only and contains no `UPDATE` or `DELETE`.

### Over MCP, an agent cannot approve itself

Discovery is filtered by scope and autonomy mode: a capability the caller cannot invoke
does not appear, and an invisible tool produces exactly the same error as an unknown
one. `approval`, `subject`, `mode`, `scopes` and `lease` are **absent from the schema**,
not merely rejected — the subject, the mode and any approval come from the server. There
is no tool that grants an approval. MCP read-only annotations are advisory; enforcement
is server-side.

Every call returns a versioned envelope whose status is one of `proposed`, `pending`,
`executed` or `refused`, with reason **codes** only — never prose, never an exception
message, never credential material.

### The harness measures composition

`CapabilityHarness` replays an adversarial dataset — allow, deny, unknown, stale
approval, state changed, duplicate request, timeout, lost response, compensation
failure, prompt injection — against the real components, and reports decision precision,
escalation rate and duplicate side effects. Duplicates are counted from the external
system's own call log, never from what the framework believes it did.
