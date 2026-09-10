"""Plan 29 slice 7.3 closed, read-only MCP Tool contract."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import json
import math
from types import ModuleType, SimpleNamespace
import sys
from unittest.mock import Mock, call

import pytest

from skifer.agentic.data_service import (
    AgentReadyDataService,
    HARD_MAX_FILTERS,
    ServiceLimits,
    HARD_MAX_FILTER_VALUE_LENGTH,
    HARD_MAX_QUERY_ROWS,
    LimitExceeded,
    QueryEnvelope,
    RequestContext,
    SerializationError,
)
from skifer.mcp.resources import (
    INTERNAL_ERROR,
    INVALID_PARAMS,
    MCPResourceError,
    RESOURCE_UNAVAILABLE,
    SEMANTIC_ACCESS_DENIED,
)
from skifer.mcp.tools import (
    MCPTools,
    QUERY_SEMANTIC_MODEL_INPUT_SCHEMA,
    QUERY_TIMEOUT,
)
from skifer.observability.tracing import TraceContext
from skifer.semantic.access_policy import (
    CertificationDecision,
    SemanticAccessDenied,
)


TRACE_ID = "1234567890abcdef1234567890abcdef"


def _ctx(*scopes):
    return RequestContext(
        subject="external-agent",
        scopes=frozenset(scopes),
        consumer_class="mcp",
        trace_context=TraceContext(trace_id=TRACE_ID),
    )


def _envelope(*, truncated=False, evidence=None):
    return QueryEnvelope(
        rows=[{"region": "EMEA", "revenue": 42}],
        evidence=evidence
        or {
            "execution_status": "succeeded",
            "policy_decision": {"decision": "ALLOW", "reasons": []},
        },
        truncated=truncated,
    )


class _ExplodingCollaborator:
    def __getattr__(self, name):
        raise AssertionError(f"Tool bypassed data service through {name}.")


def _service(envelope=None):
    service = object.__new__(AgentReadyDataService)
    # __init__ is bypassed here, so the budget the tool reads must be set
    # explicitly; MCPTools deliberately has no fallback for a missing one.
    service.limits = ServiceLimits()
    service.query = Mock(return_value=envelope or _envelope())
    service.semantic_engine = _ExplodingCollaborator()
    service.lineage_graph = _ExplodingCollaborator()
    for method_name in (
        "list_models",
        "get_model",
        "get_contract",
        "get_certification",
        "get_lineage",
    ):
        setattr(
            service,
            method_name,
            Mock(side_effect=AssertionError(f"Unexpected call to {method_name}.")),
        )
    return service


def _arguments(**overrides):
    arguments = {
        "model": "orders",
        "metrics": ["revenue"],
        "group_by": ["region"],
        "filters": [{"column": "region", "operator": "eq", "value": "EMEA"}],
        "date_from": "2026-01-01",
        "date_to": "2026-12-31",
        "limit": 100,
    }
    arguments.update(overrides)
    return arguments


def _call(service, arguments=None):
    return MCPTools(service).call(
        _ctx("query:execute"),
        "query_semantic_model",
        _arguments() if arguments is None else arguments,
    )


def _assert_invalid(service, arguments, *, error_type="invalid_request"):
    with pytest.raises(MCPResourceError) as raised:
        _call(service, arguments)

    assert raised.value.code == INVALID_PARAMS
    assert raised.value.data == {"error_type": error_type}
    service.query.assert_not_called()


def test_input_schema_closes_every_object_level():
    object_schemas = []

    def visit(node):
        if isinstance(node, dict):
            if node.get("type") == "object":
                object_schemas.append(node)
            for value in node.values():
                visit(value)
        elif isinstance(node, list):
            for value in node:
                visit(value)

    visit(QUERY_SEMANTIC_MODEL_INPUT_SCHEMA)

    assert len(object_schemas) == 2
    assert all(schema.get("additionalProperties") is False for schema in object_schemas)


def test_input_schema_exposes_only_safe_semantic_fields():
    properties = QUERY_SEMANTIC_MODEL_INPUT_SCHEMA["properties"]

    assert set(properties) == {
        "model",
        "metrics",
        "group_by",
        "filters",
        "date_from",
        "date_to",
        "period",
        "limit",
    }
    assert not {
        "sql",
        "query",
        "table",
        "fqn",
        "path",
        "expr",
        "python_expression",
        "mode",
        "view_name",
    } & set(properties)


def test_input_schema_has_the_exact_semantic_filter_operator_enum():
    operator_schema = QUERY_SEMANTIC_MODEL_INPUT_SCHEMA["properties"]["filters"][
        "items"
    ]["properties"]["operator"]

    assert operator_schema["enum"] == [
        "eq",
        "neq",
        "gt",
        "lt",
        "gte",
        "lte",
        "in",
        "like",
        "is_null",
        "is_not_null",
    ]


def test_schema_bounds_track_data_service_hard_limits():
    """Changing a service ceiling alone must make this contract test fail."""
    properties = QUERY_SEMANTIC_MODEL_INPUT_SCHEMA["properties"]
    filter_value = properties["filters"]["items"]["properties"]["value"]
    scalar_value = filter_value["oneOf"][0]
    in_values = filter_value["oneOf"][1]

    assert properties["metrics"]["maxItems"] == HARD_MAX_FILTERS
    assert properties["group_by"]["maxItems"] == HARD_MAX_FILTERS
    assert properties["filters"]["maxItems"] == HARD_MAX_FILTERS
    assert in_values["maxItems"] == HARD_MAX_FILTERS
    assert scalar_value["oneOf"][0]["maxLength"] == HARD_MAX_FILTER_VALUE_LENGTH
    assert in_values["items"]["oneOf"][0]["maxLength"] == (
        HARD_MAX_FILTER_VALUE_LENGTH
    )
    assert properties["limit"]["minimum"] == 1
    assert properties["limit"]["maximum"] == HARD_MAX_QUERY_ROWS
    assert properties["date_from"]["format"] == "date"
    assert properties["date_to"]["format"] == "date"


@pytest.mark.parametrize(
    "field",
    [
        "sql",
        "query",
        "table",
        "fqn",
        "path",
        "expr",
        "python_expression",
        "mode",
        "view_name",
    ],
)
def test_dangerous_and_write_capable_top_level_fields_are_rejected(field):
    service = _service()

    _assert_invalid(service, _arguments(**{field: "SELECT * FROM private.table"}))


def test_unknown_nested_filter_field_is_rejected():
    service = _service()
    filters = [
        {
            "column": "region",
            "operator": "eq",
            "value": "EMEA",
            "expr": "__import__('os').system('id')",
        }
    ]

    _assert_invalid(service, _arguments(filters=filters))


def test_filter_operator_outside_closed_enum_is_rejected():
    service = _service()

    _assert_invalid(
        service,
        _arguments(
            filters=[{"column": "region", "operator": "sql", "value": "1=1"}]
        ),
    )


@pytest.mark.parametrize("field", ["metrics", "group_by", "filters"])
def test_oversized_top_level_lists_are_rejected(field):
    service = _service()
    if field == "filters":
        value = [
            {"column": "region", "operator": "eq", "value": index}
            for index in range(HARD_MAX_FILTERS + 1)
        ]
    else:
        value = [f"member_{index}" for index in range(HARD_MAX_FILTERS + 1)]

    _assert_invalid(
        service,
        _arguments(**{field: value}),
        error_type="limit_exceeded",
    )


def test_oversized_in_value_list_is_rejected():
    service = _service()
    filters = [
        {
            "column": "region",
            "operator": "in",
            "value": list(range(HARD_MAX_FILTERS + 1)),
        }
    ]

    _assert_invalid(
        service,
        _arguments(filters=filters),
        error_type="limit_exceeded",
    )


def test_oversized_filter_value_is_rejected():
    service = _service()
    sentinel = "x" * (HARD_MAX_FILTER_VALUE_LENGTH + 1)

    _assert_invalid(
        service,
        _arguments(
            filters=[{"column": "region", "operator": "eq", "value": sentinel}]
        ),
        error_type="limit_exceeded",
    )


@pytest.mark.parametrize("limit", [0, HARD_MAX_QUERY_ROWS + 1, True])
def test_limit_outside_closed_integer_bounds_is_rejected(limit):
    service = _service()

    _assert_invalid(
        service,
        _arguments(limit=limit),
        error_type="limit_exceeded",
    )


@pytest.mark.parametrize("field,value", [("date_from", "2026/01/01"), ("date_to", "2026-02-30")])
def test_invalid_dates_are_rejected(field, value):
    service = _service()

    _assert_invalid(service, _arguments(**{field: value}))


def test_success_delegates_only_to_query_and_preserves_truncation_warning_and_literals():
    injection_canary = "x' OR 1=1 -- FILTER_CANARY"
    evidence = {
        "execution_status": "succeeded",
        "policy_decision": {"decision": "WARN", "reasons": ["EXPIRED"]},
        "lineage_status": "complete",
    }
    service = _service(_envelope(truncated=True, evidence=evidence))
    ctx = _ctx("query:execute")
    arguments = _arguments(
        filters=[
            {"column": "region", "operator": "eq", "value": injection_canary}
        ]
    )

    payload = MCPTools(service).call(ctx, "query_semantic_model", arguments)

    service.query.assert_called_once()
    called_ctx, called_query, called_limit = service.query.call_args.args
    assert called_ctx is ctx
    assert called_query.filters == [
        {"column": "region", "operator": "eq", "value": injection_canary}
    ]
    assert called_query.mode == "query"
    assert called_query.view_name is None
    assert called_limit == 100
    assert payload["truncated"] is True
    assert payload["evidence"]["policy_decision"]["decision"] == "WARN"
    serialized = json.dumps(payload, allow_nan=False)
    assert injection_canary not in serialized
    assert "sql" not in payload["evidence"]
    assert json.loads(serialized) == payload
    assert all(not hasattr(row, "collect") for row in payload["rows"])
    assert not any(
        getattr(service, method).called
        for method in (
            "list_models",
            "get_model",
            "get_contract",
            "get_certification",
            "get_lineage",
        )
    )


def test_certification_deny_is_an_explicit_mcp_error_not_an_empty_result():
    service = _service()
    service.query.side_effect = SemanticAccessDenied(
        model_key="orders",
        datasets=("catalog.private.secret_table",),
        decision=CertificationDecision.DENY,
        reasons=("MISSING",),
        evaluated_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        recommended_action="Inspect /private/path and SELECT secret.",
    )

    with pytest.raises(MCPResourceError) as raised:
        _call(service)

    assert raised.value.code == SEMANTIC_ACCESS_DENIED
    assert raised.value.data == {"error_type": "semantic_access_denied"}
    assert raised.value.message == "Semantic certification denied access."


def test_service_row_limit_error_is_structured_and_returns_no_rows():
    service = _service()
    service.query.side_effect = LimitExceeded("1001 secret rows")

    with pytest.raises(MCPResourceError) as raised:
        _call(service)

    assert raised.value.code == INVALID_PARAMS
    assert raised.value.data == {"error_type": "limit_exceeded"}
    assert "secret" not in raised.value.message


def test_spark_value_serialization_error_returns_no_non_json_object():
    service = _service()
    service.query.side_effect = SerializationError(
        "rows[0].amount has unsupported type 'DenseVector' with secret data"
    )

    with pytest.raises(MCPResourceError) as raised:
        _call(service)

    assert raised.value.code == RESOURCE_UNAVAILABLE
    assert raised.value.data == {"error_type": "resource_unavailable"}
    assert raised.value.message == "Governed query result is unavailable."


def test_non_json_query_envelope_is_never_returned():
    service = _service(QueryEnvelope(rows=[{"bad": math.nan}], evidence={}, truncated=False))

    with pytest.raises(ValueError):
        _call(service)


def test_timeout_is_a_sanitized_explicit_mcp_error():
    service = _service()
    service.query.side_effect = TimeoutError("warehouse secret timed out")

    with pytest.raises(MCPResourceError) as raised:
        _call(service)

    assert raised.value.code == QUERY_TIMEOUT
    assert raised.value.data == {"error_type": "query_timeout"}
    assert raised.value.message == "Semantic query timed out."
    assert "secret" not in raised.value.message


def _install_fake_mcp(monkeypatch):
    class FakeMCPError(Exception):
        def __init__(self, *, code, message, data):
            self.code = code
            self.message = message
            self.data = data
            super().__init__(message)

    class FakeServer:
        def __init__(self, name, **handlers):
            self.name = name
            self.handlers = handlers

    def model_type(name):
        return type(
            name,
            (),
            {"__init__": lambda self, **kwargs: self.__dict__.update(kwargs)},
        )

    mcp = ModuleType("mcp")
    mcp.MCPError = FakeMCPError
    server = ModuleType("mcp.server")
    server.Server = FakeServer
    types = ModuleType("mcp.types")
    for name in (
        "ListToolsResult",
        "Tool",
        "ToolAnnotations",
        "CallToolResult",
        "TextContent",
    ):
        setattr(types, name, model_type(name))
    monkeypatch.setitem(sys.modules, "mcp", mcp)
    monkeypatch.setitem(sys.modules, "mcp.server", server)
    monkeypatch.setitem(sys.modules, "mcp.types", types)
    return FakeMCPError


def test_server_wires_tool_context_output_and_read_only_annotation(monkeypatch):
    _install_fake_mcp(monkeypatch)
    from skifer.mcp.server import create_server

    service = _service(_envelope(truncated=True))
    ctx = _ctx("query:execute")
    provider = Mock(return_value=ctx)
    server = create_server(service, provider)
    request_context = object()

    listed = asyncio.run(server.handlers["on_list_tools"](request_context, None))
    result = asyncio.run(
        server.handlers["on_call_tool"](
            request_context,
            SimpleNamespace(name="query_semantic_model", arguments=_arguments()),
        )
    )

    assert [tool.name for tool in listed.tools] == ["query_semantic_model"]
    assert listed.tools[0].annotations.readOnlyHint is True
    assert listed.tools[0].annotations.destructiveHint is False
    assert result.is_error is False
    assert result.structured_content["truncated"] is True
    assert json.loads(result.content[0].text) == result.structured_content
    assert provider.call_args_list == [call(request_context)] * 2


def test_server_uses_unexpected_translation_for_non_json_service_output(monkeypatch):
    sdk_error = _install_fake_mcp(monkeypatch)
    from skifer.mcp.server import create_server

    service = _service(
        QueryEnvelope(rows=[{"secret": object()}], evidence={}, truncated=False)
    )
    server = create_server(service, lambda request: _ctx("query:execute"))

    with pytest.raises(sdk_error) as raised:
        asyncio.run(
            server.handlers["on_call_tool"](
                object(),
                SimpleNamespace(name="query_semantic_model", arguments=_arguments()),
            )
        )

    assert raised.value.code == INTERNAL_ERROR
    assert raised.value.message == "Unexpected service error: TypeError."
    assert raised.value.data == {"error_type": "internal_error"}


def test_tool_discovery_is_scope_filtered_without_calling_service():
    service = _service()
    tools = MCPTools(service)

    assert tools.list_tools(_ctx()) == ()
    assert [item.name for item in tools.list_tools(_ctx("query:execute"))] == [
        "query_semantic_model"
    ]
    service.query.assert_not_called()


# ---------------------------------------------------------------------------
# Plan 29 — the advertised schema must match the limits actually enforced
# ---------------------------------------------------------------------------

def _tightened_service(**limit_overrides):
    service = _service()
    service.limits = ServiceLimits(**limit_overrides)
    return service


def test_advertised_schema_follows_a_tightened_service_budget():
    """A schema built from the hard ceilings promises what the service refuses.

    An operator lowering max_query_rows to 100 left the tool advertising 1000:
    every such request was refused after the fact. Fail-closed, but the agent
    cannot plan against a schema that overstates what it may ask for.
    """
    tools = MCPTools(_tightened_service(max_query_rows=100, max_filters=5))

    served = tools.list_tools(_ctx("query:execute"))[0]
    properties = served.input_schema["properties"]

    assert properties["limit"]["maximum"] == 100
    assert properties["filters"]["maxItems"] == 5
    assert properties["metrics"]["maxItems"] == 5


def test_tightened_budget_is_enforced_by_the_tool_not_only_the_service():
    """The tool refuses at the effective bound, not at the hard ceiling."""
    tools = MCPTools(_tightened_service(max_query_rows=100))

    with pytest.raises(MCPResourceError) as raised:
        tools.call(_ctx("query:execute"), "query_semantic_model", {"model": "orders", "limit": 500})

    assert raised.value.data == {"error_type": "limit_exceeded"}


def test_advertised_filter_value_length_follows_the_service():
    tools = MCPTools(_tightened_service(max_filter_value_length=32))

    served = tools.list_tools(_ctx("query:execute"))[0]
    scalar = served.input_schema["properties"]["filters"]["items"]["properties"]["value"]
    assert scalar["oneOf"][0]["oneOf"][0]["maxLength"] == 32


def test_default_descriptor_still_sits_at_the_hard_ceilings():
    """A service left at defaults must advertise the documented maxima."""
    tools = MCPTools(_service())

    served = tools.list_tools(_ctx("query:execute"))[0]
    assert served.input_schema["properties"]["limit"]["maximum"] == HARD_MAX_QUERY_ROWS
