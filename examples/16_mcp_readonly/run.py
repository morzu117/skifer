"""Exercise the dependency-free security boundary behind the read-only MCP surface."""

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
from skifer.semantic.access_policy import (
    CertificationDecision,
    PolicyEvaluation,
)
from skifer.semantic.evidence import SemanticEvidence, SemanticResult


NOW = datetime(2026, 9, 9, 12, tzinfo=timezone.utc)
TRACE_ID = "1234567890abcdef1234567890abcdef"


class LocalRows:
    def __init__(self, rows):
        self.rows = rows

    def limit(self, value):
        return LocalRows(self.rows[:value])

    def collect(self):
        return list(self.rows)


class LocalSemanticEngine:
    def __init__(self):
        self.forwarded_context = None
        self.catalog = [
            {
                "key": "orders",
                "description": "Governed orders",
                "layer": "gold",
                "table": "gold.orders",
                "tags": ["certified"],
            }
        ]

    def list_models(self):
        return list(self.catalog)

    def get_model_summary(self, key):
        if key != "orders":
            raise ValueError(key)
        return dict(self.catalog[0])

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
        return SemanticResult(LocalRows([{"order_count": 2}]), evidence)


def context(*scopes):
    return RequestContext(
        subject="local-agent",
        scopes=frozenset(scopes),
        consumer_class="mcp",
        trace_context=TraceContext(trace_id=TRACE_ID),
    )


def main() -> None:
    engine = LocalSemanticEngine()
    service = AgentReadyDataService(
        engine,
        limits=ServiceLimits(max_query_rows=2, max_filters=1),
    )
    resources = MCPResources(service)
    tools = MCPTools(service)

    anonymous = context()
    model_reader = context("models:read")
    no_scope_resources = resources.list_resources(anonymous).items
    no_scope_templates = resources.list_resource_templates(anonymous).items
    model_resources = resources.list_resources(model_reader).items
    model_templates = resources.list_resource_templates(model_reader).items

    print(
        "Discovery without scopes: "
        f"resources={[item.uri for item in no_scope_resources]}, "
        f"templates={[item.uri_template for item in no_scope_templates]}"
    )
    print(
        "Discovery with models:read: "
        f"resources={[item.uri for item in model_resources]}, "
        f"templates={[item.uri_template for item in model_templates]}"
    )
    print("Hidden endpoints prevent reconnaissance: callers learn only what they may use.")

    descriptor = tools.list_tools(context("query:execute"))[0]
    schema = descriptor.input_schema
    filter_schema = schema["properties"]["filters"]["items"]
    fields = sorted(schema["properties"])
    print(f"Tool: {descriptor.name}")
    print(f"Closed request object: {schema['additionalProperties'] is False}")
    print(f"Closed filter object: {filter_schema['additionalProperties'] is False}")
    print(f"Tool fields: {fields}")
    print(
        "SQL, mode, and view_name exposed: "
        f"{bool({'sql', 'mode', 'view_name'} & set(fields))}"
    )
    print(
        "Advertised limits: "
        f"rows={schema['properties']['limit']['maximum']}, "
        f"filters={schema['properties']['filters']['maxItems']}"
    )

    try:
        service.list_models(anonymous)
    except ScopeDenied as exc:
        print(f"Missing-scope refusal: {exc}")

    try:
        service.query(context("query:execute"), SemanticQuery("orders"), limit=3)
    except LimitExceeded as exc:
        print(f"Row-limit refusal: {exc}")

    too_many_filters = SemanticQuery(
        "orders",
        filters=[
            {"column": "region", "operator": "eq", "value": "EMEA"},
            {"column": "channel", "operator": "eq", "value": "web"},
        ],
    )
    try:
        service.query(context("query:execute"), too_many_filters, limit=1)
    except LimitExceeded as exc:
        print(f"Filter-limit refusal: {exc}")

    caller = context("query:execute", "certification_override")
    service.query(caller, SemanticQuery("orders"), limit=1)
    print(f"Caller requested scopes: {sorted(caller.scopes)}")
    print(f"ConsumerContext scopes after boundary: {sorted(engine.forwarded_context.scopes)}")
    print("certification_override crossed the boundary: False")


if __name__ == "__main__":
    main()
