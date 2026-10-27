"""The read-only MCP boundary: scope-filtered discovery, a closed tool schema, hard limits.

    python examples/16_mcp_readonly/run.py
"""

from datetime import datetime, timezone

from skifer.agentic.data_service import (
    AgentReadyDataService,
    LimitExceeded,
    RequestContext,
    ScopeDenied,
    ServiceLimits,
)
from skifer.agentic.resolver import SemanticQuery
from skifer.mcp.resources import MCPResources
from skifer.mcp.tools import MCPTools
from skifer.observability.tracing import TraceContext
from skifer.semantic.access_policy import CertificationDecision, PolicyEvaluation
from skifer.semantic.evidence import SemanticEvidence, SemanticResult

NOW = datetime(2026, 9, 9, 12, tzinfo=timezone.utc)
TRACE_ID = "1234567890abcdef1234567890abcdef"


class Rows(list):
    def limit(self, n):
        return Rows(self[:n])

    def collect(self):
        return list(self)


class SemanticEngine:
    """Stands in for SemanticEngine; records the context the service forwards."""

    forwarded_context = None

    def get_model_summary(self, key):
        return {"key": key, "description": "Governed orders", "layer": "gold", "table": "gold.orders"}

    def query_with_evidence(self, query, **kwargs):
        self.forwarded_context = kwargs["consumer_context"]
        evidence = SemanticEvidence(
            evidence_id="example-evidence",
            trace_id=TRACE_ID,
            model_keys=(query.model_name,),
            metrics=(),
            dimensions=(),
            normalized_filters=(),
            sources=(),
            policy=PolicyEvaluation(CertificationDecision.ALLOW, (), NOW),
            sql_hash="sha256:v1:example",
            sql_text=None,
            statement_id=None,
            compiled_at=NOW,
            executed_at=NOW,
            execution_status="succeeded",
        )
        return SemanticResult(Rows([{"order_count": 2}]), evidence)


def context(*scopes):
    return RequestContext(
        subject="local-agent",
        scopes=frozenset(scopes),
        consumer_class="mcp",
        trace_context=TraceContext(trace_id=TRACE_ID),
    )


engine = SemanticEngine()
service = AgentReadyDataService(engine, limits=ServiceLimits(max_query_rows=2, max_filters=1))
resources = MCPResources(service)

# Discovery is filtered by scope: listing an endpoint you may not use would be reconnaissance.
for label, ctx in [("without scopes", context()), ("with models:read", context("models:read"))]:
    uris = [item.uri for item in resources.list_resources(ctx).items]
    templates = [item.uri_template for item in resources.list_resource_templates(ctx).items]
    print(f"Discovery {label}: resources={uris}, templates={templates}")
print("Hidden endpoints prevent reconnaissance: callers learn only what they may use.")

tool = MCPTools(service).list_tools(context("query:execute"))[0]
schema = tool.input_schema
fields = sorted(schema["properties"])
print(f"Tool: {tool.name}")
print(f"Closed request object: {schema['additionalProperties'] is False}")
print(f"Closed filter object: {schema['properties']['filters']['items']['additionalProperties'] is False}")
print(f"Tool fields: {fields}")
print(f"SQL, mode, and view_name exposed: {bool({'sql', 'mode', 'view_name'} & set(fields))}")
print(f"Advertised limits: rows={schema['properties']['limit']['maximum']}, filters={schema['properties']['filters']['maxItems']}")

try:
    service.list_models(context())
except ScopeDenied as exc:
    print(f"Missing-scope refusal: {exc}")
try:
    service.query(context("query:execute"), SemanticQuery("orders"), limit=3)
except LimitExceeded as exc:
    print(f"Row-limit refusal: {exc}")
try:
    filters = [
        {"column": "region", "operator": "eq", "value": "EMEA"},
        {"column": "channel", "operator": "eq", "value": "web"},
    ]
    service.query(context("query:execute"), SemanticQuery("orders", filters=filters), limit=1)
except LimitExceeded as exc:
    print(f"Filter-limit refusal: {exc}")

# No client scope enters ConsumerContext: an agent cannot grant itself an override.
caller = context("query:execute", "certification_override")
service.query(caller, SemanticQuery("orders"), limit=1)
forwarded = engine.forwarded_context.scopes
print(f"Caller requested scopes: {sorted(caller.scopes)}")
print(f"ConsumerContext scopes after boundary: {sorted(forwarded)}")
print(f"certification_override crossed the boundary: {'certification_override' in forwarded}")
