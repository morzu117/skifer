# 16 — Read-only MCP boundary

**What it shows:** the transport-independent `AgentReadyDataService` security boundary, plus the
dependency-free MCP resource and tool descriptors built over it. Discovery hides resources the
caller cannot use, query budgets are hard refusals, and client scopes cannot become semantic
certification overrides.

## Run it

```bash
python examples/16_mcp_readonly/run.py
```

No Spark session, network, API key, or MCP SDK is needed. `mcp.resources` and `mcp.tools` are the
dependency-free handlers; the optional SDK is imported only by the transport adapter, which this
example deliberately does not start.

## What you should see

```text
Discovery without scopes: resources=[], templates=[]
Discovery with models:read: resources=['skifer://semantic/catalog'], templates=['skifer://semantic/models/{key}']
Hidden endpoints prevent reconnaissance: callers learn only what they may use.
Tool: query_semantic_model
Closed request object: True
Closed filter object: True
Tool fields: ['date_from', 'date_to', 'filters', 'group_by', 'limit', 'metrics', 'model', 'period']
SQL, mode, and view_name exposed: False
Advertised limits: rows=2, filters=1
Missing-scope refusal: Scope 'models:read' is required.
Row-limit refusal: query limit must be an integer in 1..2; got 3.
Filter-limit refusal: A query may contain at most 1 filters.
Caller requested scopes: ['certification_override', 'query:execute']
ConsumerContext scopes after boundary: []
certification_override crossed the boundary: False
```

The two discovery calls differ only by `models:read`. Returning an empty list is intentional:
advertising an inaccessible endpoint and the scope that opens it would give an untrusted caller a
map of the governed surface.

The tool schema is closed at both object levels and contains semantic names only—no SQL, arbitrary
table, execution mode, or view creation field. Its advertised limits come from this service
instance, so the tool never promises more rows or filters than the boundary accepts.

The final call asks for `certification_override` alongside `query:execute`. The query runs, but the
allowlisted `ConsumerContext.scopes` is empty. Client authority therefore cannot manufacture a
break-glass semantic read.

## Remember

Discovery, arguments, budgets, and scopes are all enforced at the data-service boundary, not trusted to the MCP client.

## Next

[17 — Supervised adaptive Gold](../17_adaptive_gold/) turns privacy-safe usage into proposals that
still require a human decision.
