# Read-only MCP surface

This page documents Skifer's read-only MCP boundary for platform engineers and agent integrators who need scoped access to governed semantic data; for the conversational analytics layer, return to the [Agentic Layer](agentic.md).

The optional MCP server lets any MCP-compatible agent discover and query the
same governed semantic layer. It does not expose raw SQL, arbitrary tables,
files, Python expressions, write tools, or prompts that can perform actions.
Every handler delegates to `AgentReadyDataService`, so certification policy,
evidence redaction, tracing, scope checks, and result budgets remain identical
to the Python boundary.

Install the supported official Python SDK range with:

```bash
pip install "skifer[mcp]"
```

| Component | Supported versions |
|---|---|
| Skifer MCP adapter | MCP protocol surface v1 (read-only) |
| Official Python `mcp` SDK | `mcp>=2.1,<3` |

The server publishes these resources:

| URI | Required scope |
|---|---|
| `skifer://semantic/catalog` | `models:read` |
| `skifer://semantic/models/{key}` | `models:read` |
| `skifer://contracts/{id}/{version}` | `contracts:read` |
| `skifer://certification/{dataset}` | `contracts:read` |
| `skifer://lineage/{dataset}/{column}` | `lineage:read` |

The sole tool, `query_semantic_model`, requires `query:execute`. Its closed
input schema accepts semantic model, metric and dimension names, structured
filters, ISO dates or a declared period, and a row limit. It never accepts SQL.
Resources are filtered by the caller's scopes rather than merely failing when
read.

Service budgets are configurable only below their hard ceilings: 100 resources
per page, 1,000 query rows, 50 filters, and 1,024 characters per filter value.
The defaults equal those ceilings; a deployment can lower each value under
`service.limits`. Query responses carry bounded JSON-native rows, redacted
evidence, and a `truncated` flag.

`examples/16_mcp_readonly/` prints discovery with and without `models:read` to
show hidden endpoints, inspects the closed tool schema, and demonstrates hard
row/filter-limit and missing-scope refusals; it also proves a caller's
`certification_override` scope cannot cross into the semantic context.

### Server configuration and launch

An MCP configuration is a closed YAML document. Unknown fields, unsupported
transports or scopes, malformed YAML, mismatched CLI/config transports, and
limits above the hard ceilings all stop startup. The `service.factory` target
uses `module:callable` syntax and is called as `factory(limits=ServiceLimits)`;
it must return an `AgentReadyDataService`. This keeps Spark, catalog, lineage,
and certification-store construction deployment-specific without weakening
the transport boundary.

Local stdio example:

```yaml
transport: stdio
bind: {host: 127.0.0.1, port: 8000}
service:
  factory: my_deployment.mcp:create_service
  limits:
    max_page_size: 50
    max_query_rows: 100
    max_filters: 20
    max_filter_value_length: 512
stdio:
  subject: local-agent
  consumer_class: mcp
  scopes: [models:read, contracts:read, lineage:read, query:execute]
```

```bash
skifer mcp serve --transport stdio --config mcp-stdio.yaml
```

Stdio authority is immutable startup configuration; it has no HTTP OAuth flow.
Its bind must be loopback. Although stdio does not listen on that address, the
explicit loopback-only check prevents a deployment configuration from being
silently reused as an unauthenticated public server.

Streamable HTTP example:

```yaml
transport: http
bind: {host: 127.0.0.1, port: 8080}
service:
  factory: my_deployment.mcp:create_service
  limits: {max_query_rows: 100}
http:
  auth:
    verifier_factory: my_deployment.auth:create_verifier
    audience: https://agents.example
    issuer: https://identity.example
    resource: skifer:mcp
    required_scopes: [models:read, query:execute]
```

```bash
skifer mcp serve --transport http --config mcp-http.yaml
```

The verifier factory takes no argument and returns the cryptographic bearer
token verifier defined by the authentication boundary. It should obtain keys
and secrets from the deployment environment, not from this YAML. HTTP always
requires the full verifier, audience, issuer, resource, and required-scope
configuration, including on loopback. A public bind is accepted only for this
fully authenticated HTTP form; public stdio is always rejected at startup.
Bearer credentials are resolved independently on every request and are never
stored in server singleton or connection state.

Streamable HTTP is served at `/mcp`. `GET /health` returns exactly
`{"status":"ok"}`: it deliberately includes no model, dataset, table, contract,
version, or object count.

### Generic agent integration

For an MCP-compatible agent—including Claude-, ChatGPT-, or Codex-based
agents—configure one of the two standard transport shapes supported by its MCP
host:

- For stdio, set the server command to `skifer` and pass
  `mcp serve --transport stdio --config /absolute/path/mcp-stdio.yaml` as its
  arguments. The host starts the subprocess and exchanges MCP messages over its
  standard input and output. Keep the configuration and subprocess local.
- For HTTP, point the host at `https://your-service.example/mcp` and arrange for
  it to send a bearer token minted for the configured issuer, audience,
  resource, and scopes. Terminate TLS in the deployment platform and keep the
  Skifer verifier responsible for signature and claim validation.

After connection, the client initializes the MCP session, lists the visible
resources and templates, reads only those useful to the task, and invokes
`query_semantic_model` with structured arguments. Client-side read-only hints
are descriptive; server scopes and `AgentReadyDataService` remain the security
boundary.
