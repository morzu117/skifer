# 04 — Governed capability in shadow mode

**What it shows:** a write capability selected and validated locally, ending as a `PROPOSED` action
without touching any external system.

## Run it

```bash
python examples/04_governed_capability/run.py
```

No Spark session, API key, network, or Databricks account is needed. The capability catalog and full
definition are YAML files beside the script; the named executor is a fake Python function registered
explicitly in the process.

## What you should see

```text
Document asks for : guarded
Resolved autonomy : SHADOW
Recorded state    : PROPOSED
Executor calls    : 0
Credential issued : no credential broker was configured

Refused request: Invalid capability arguments: Field 'arguments.priority' is not allowed.
Executor calls after refusal: 0
```

The document says `approval: guarded`, but that is a ceiling, not permission to run. Effective
autonomy is the stricter of the document and environment configuration. With no environment override,
the default is **shadow**, so the fake executor remains at zero calls.

## The refusal is part of the example

The input schema is closed with `additionalProperties: false`. Adding an undeclared `priority`
argument is therefore refused before execution. The second zero call count is the observable proof:
bad input did not slip through to the adapter.

## What shadow guarantees here

Shadow is the default. Nothing executed, and no credential was issued—the example does not even
configure a credential broker. The result is a proposal for review, not a side effect.

There is no rollback in this layer because there is nothing to roll back. Compensation and recovery
matter only after a more permissive, explicitly authorised mode has crossed the external write
boundary.

## Next

[05 — Python rules, joins, and aggregates](../05_rules_join_aggregate/) shows the Python half of the
framework for the first time: YAML names a business rule, and registered code defines it.
