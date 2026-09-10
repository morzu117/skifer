# Governance from contract to action

Governance in Skifer is not a fifth layer added after pipelines, semantics, observability,
and agents. It is the set of guarantees that follows data through all four: from the contract that
defines a dataset, through publication and query execution, to the boundary where an agent may act
on another system.

## 1. The pipeline declares the contract

A pipeline opts into the governed path by declaring `data_product` and `contract`. The declaration
identifies the product and version, defines its output and grain, and supplies the stable definition
that later certification decisions refer to. The same pipeline can project a managed semantic draft,
so the table contract and the business names used to query it do not evolve independently.

The detailed authoring surfaces are documented in [Core Engine](core.md) and
[Projected semantic models](semantic.md#projected-semantic-models-plan-29).

## 2. Publication certifies before promotion

An opted-in batch run does not write straight to the consumer table. It stages the candidate,
executes the declared checks, persists their results under the run identity, and then takes one of
two paths: passing data is promoted, while a critical failure is quarantined and the previous good
table remains in place. Certification is therefore the recorded outcome of the publication path,
not a label applied later.

See [Contract identity and certified publication](observability.md#contract-identity-and-odcs-export)
for the store, checks, promotion, quarantine, and recovery mechanics.

Start that path with `examples/02_quality_and_contract/`: it declares the
contract, stages and promotes a passing batch, then prints the certification
record that downstream policy will consume.

## 3. The certification gate controls consumption

At query time, the environment policy decides whether each dataset is usable. Depending on that
policy, missing, failed, expired, or explicitly overridden certification can allow with a warning,
deny, or require a human. The semantic engine checks before compilation when it can and checks again
immediately before execution. For a multi-model query, the second check covers every dataset in the
resolved join plan, not only the root model.

This is the same boundary whether the consumer is a dashboard, a person asking a question, or an
external agent. The conversational path and its migration modes are detailed under
[Semantic certification migration](agentic.md#semantic-certification-migration).

## 4. Every executed answer carries evidence

A permitted query returns more than a DataFrame: its standalone evidence records the hash of the SQL
actually executed, metric definition hashes, selected-column lineage, a frozen certification
snapshot, the policy decision, and execution status and duration. SQL and filter values are withheld
by default, and asking for withheld material fails explicitly rather than returning an ambiguous
empty field.

The evidence is produced by the same execution path as the answer, so the attestation cannot drift
from what ran. See [Semantic evidence](agentic.md#semantic-evidence-plan-29-feature-4) for disclosure,
failure, provenance, and serving behavior.

## 5. Tracing observes without changing the business result

Runtime tracing can correlate the pipeline, semantic, and agentic spans, but it is off by default and
is not an authorization mechanism. Exporters are optional and loaded lazily. Unless a deployment
explicitly marks tracing as required, an exporter failure cannot change a return value, persisted
data, or exception. A strict attribute allowlist keeps questions, prompts, SQL text, filter values,
and raw identities out of exported spans.

Configuration, redaction, identity, and exporter behavior are covered in
[Runtime tracing](observability.md#runtime-tracing-plan-29-feature-5).

## 6. What an agent may read and do

Reading and acting are deliberately separate boundaries. The [Read-only MCP surface](mcp.md) exposes
only governed models, contracts, certification, lineage, and structured semantic queries. Scopes,
budgets, certification, and evidence are enforced server-side; MCP never accepts arbitrary SQL and
never passes a client scope through as a certification override.

Changing an external system goes through a [governed capability](capabilities.md). The agent may
choose a declared capability and fill its closed input schema, but it cannot decide policy,
preconditions, autonomy, approval, credentials, or idempotency. Shadow mode is the default, approvals
bind one exact request, credentials are obtained just in time, and execution is journaled before the
external call.

`examples/04_governed_capability/` prints the first boundary: a validated action
stays `PROPOSED` in shadow mode with zero executor calls. Continue with
`examples/18_capability_execution/`, which prints exact approval binding,
idempotent replay, and compensation as a separately audited external action.

Usage may also inform [supervised adaptive Gold](adaptive.md), which turns recurring query patterns
into validated, reviewable proposals. It never edits a production schema, deploys an asset, or rolls
one back. In every direction, a model may propose or explain; it never grants itself authority.
