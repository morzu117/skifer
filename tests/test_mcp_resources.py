"""Plan 29 slice 7.2 read-only MCP resource contract."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock, call

import pytest

from skifer.agentic.data_service import (
    ServiceLimits,
    AgentReadyDataService,
    InvalidCursor,
    InvalidRequest,
    LimitExceeded,
    RequestContext,
    ResourceNotFound,
    ResourceUnavailable,
    ScopeDenied,
)
from skifer.mcp.resources import (
    _parse_uri,
    CATALOG_RESOURCE,
    ETAG_META_KEY,
    INTERNAL_ERROR,
    INVALID_PARAMS,
    MCPResourceError,
    MCPResources,
    RESOURCE_TEMPLATES,
    RESOURCE_UNAVAILABLE,
    SCOPE_DENIED,
    SEMANTIC_ACCESS_DENIED,
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


def _view(payload):
    return SimpleNamespace(to_dict=lambda: payload)


def _service():
    service = object.__new__(AgentReadyDataService)
    # create_server now builds MCPTools, which reads the budget in force.
    service.limits = ServiceLimits()
    service.list_models = Mock(
        return_value=_view(
            {"items": [{"key": "orders"}], "next_cursor": "next", "total": 2}
        )
    )
    service.get_model = Mock(return_value=_view({"key": "orders"}))
    service.get_contract = Mock(
        return_value=_view({"contract_id": "orders", "contract_version": "1.2.3"})
    )
    service.get_certification = Mock(
        return_value=_view({"dataset": "orders", "status": "CERTIFIED"})
    )
    service.get_lineage = Mock(
        return_value=_view({"dataset": "orders", "column": "order_id"})
    )
    return service


def test_mcp_modules_import_without_loading_optional_sdk():
    project_root = Path(__file__).resolve().parents[1]
    script = """
import sys
assert 'mcp' not in sys.modules
import skifer
import skifer.mcp
import skifer.mcp.resources
import skifer.mcp.server
assert 'mcp' not in sys.modules
assert not any(name.startswith('mcp.') for name in sys.modules)
"""
    completed = subprocess.run(
        [sys.executable, "-c", script],
        cwd=project_root,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr


def test_discovery_lists_exact_v1_surface_without_touching_service():
    service = _service()
    resources = MCPResources(service)

    ctx = _ctx("models:read", "contracts:read", "lineage:read")
    fixed = resources.list_resources(ctx)
    templates = resources.list_resource_templates(ctx)

    assert fixed.items == (CATALOG_RESOURCE,)
    assert [item.uri_template for item in templates.items] == [
        "skifer://semantic/models/{key}",
        "skifer://contracts/{id}/{version}",
        "skifer://certification/{dataset}",
        "skifer://lineage/{dataset}/{column}",
    ]
    assert [item.scope for item in RESOURCE_TEMPLATES] == [
        "models:read",
        "contracts:read",
        "contracts:read",
        "lineage:read",
    ]
    assert not any(
        method.called
        for method in (
            service.list_models,
            service.get_model,
            service.get_contract,
            service.get_certification,
            service.get_lineage,
        )
    )


def test_discovery_rejects_cursor_after_complete_single_page():
    resources = MCPResources(_service())

    with pytest.raises(MCPResourceError) as raised:
        resources.list_resource_templates(_ctx("models:read"), cursor="stale")

    assert raised.value.code == INVALID_PARAMS
    assert raised.value.data == {"error_type": "invalid_cursor"}


@pytest.mark.parametrize(
    ("uri", "method", "args", "scope"),
    [
        (
            "skifer://semantic/models/orders",
            "get_model",
            ("orders",),
            "models:read",
        ),
        (
            "skifer://contracts/orders/1.2.3",
            "get_contract",
            ("orders", "1.2.3"),
            "contracts:read",
        ),
        (
            "skifer://certification/orders",
            "get_certification",
            ("orders",),
            "contracts:read",
        ),
        (
            "skifer://lineage/orders/order_id",
            "get_lineage",
            ("orders", "order_id"),
            "lineage:read",
        ),
    ],
)
def test_template_reads_delegate_only_to_data_service(uri, method, args, scope):
    service = _service()
    resources = MCPResources(service)
    ctx = _ctx(scope)

    content = resources.read(ctx, uri)

    getattr(service, method).assert_called_once_with(ctx, *args)
    called = [
        name
        for name in (
            "list_models",
            "get_model",
            "get_contract",
            "get_certification",
            "get_lineage",
        )
        if getattr(service, name).called
    ]
    assert called == [method]
    assert content.mime_type == "application/json"
    assert content.metadata[ETAG_META_KEY].startswith("sha256:")
    assert json.loads(content.text) == getattr(service, method).return_value.to_dict()


def test_catalog_read_preserves_service_pagination_and_has_stable_etag():
    service = _service()
    resources = MCPResources(service)
    ctx = _ctx("models:read")
    uri = "skifer://semantic/catalog?cursor=opaque%3D%3D&limit=7"

    first = resources.read(ctx, uri)
    second = resources.read(ctx, uri)

    service.list_models.assert_called_with(ctx, cursor="opaque==", limit=7)
    assert json.loads(first.text)["next_cursor"] == "next"
    assert first.metadata == second.metadata


@pytest.mark.parametrize(
    ("service_error", "code", "error_type"),
    [
        (ScopeDenied("secret scope"), SCOPE_DENIED, "scope_denied"),
        (ResourceNotFound("secret path"), INVALID_PARAMS, "resource_not_found"),
        (
            ResourceUnavailable("backend table secret"),
            RESOURCE_UNAVAILABLE,
            "resource_unavailable",
        ),
        (InvalidRequest("secret input"), INVALID_PARAMS, "invalid_request"),
        (InvalidCursor("secret cursor"), INVALID_PARAMS, "invalid_cursor"),
        (LimitExceeded("secret limit"), INVALID_PARAMS, "limit_exceeded"),
    ],
)
def test_expected_service_errors_are_structured_and_redacted(
    service_error, code, error_type
):
    service = _service()
    service.get_model.side_effect = service_error

    with pytest.raises(MCPResourceError) as raised:
        MCPResources(service).read(
            _ctx("models:read"), "skifer://semantic/models/orders"
        )

    assert raised.value.code == code
    assert raised.value.data == {"error_type": error_type}
    assert "secret" not in raised.value.message
    assert "secret" not in json.dumps(raised.value.data)


def test_certification_deny_is_an_explicit_error_not_partial_content():
    service = _service()
    service.get_model.side_effect = SemanticAccessDenied(
        model_key="orders",
        datasets=("catalog.private.secret_table",),
        decision=CertificationDecision.DENY,
        reasons=("MISSING",),
        evaluated_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        recommended_action="Inspect /private/path and SELECT secret.",
    )

    with pytest.raises(MCPResourceError) as raised:
        MCPResources(service).read(
            _ctx("models:read"), "skifer://semantic/models/orders"
        )

    assert raised.value.code == SEMANTIC_ACCESS_DENIED
    assert raised.value.data == {"error_type": "semantic_access_denied"}
    assert raised.value.message == "Semantic certification denied access."


def test_unexpected_error_exposes_only_exception_class_not_backend_message():
    class BackendCredentialFailure(Exception):
        pass

    service = _service()
    service.get_model.side_effect = BackendCredentialFailure(
        "token=TOP_SECRET sql=SELECT * FROM private.table /tmp/secret"
    )

    with pytest.raises(MCPResourceError) as raised:
        MCPResources(service).read(
            _ctx("models:read"), "skifer://semantic/models/orders"
        )

    assert raised.value.code == INTERNAL_ERROR
    assert raised.value.message == "Unexpected service error: BackendCredentialFailure."
    serialized = raised.value.message + json.dumps(raised.value.data)
    assert "TOP_SECRET" not in serialized
    assert "SELECT" not in serialized
    assert "/tmp" not in serialized


@pytest.mark.parametrize(
    "uri",
    [
        "https://semantic/catalog",
        "skifer://semantic/catalog/",
        "skifer://semantic/models/%2Fprivate",
        "skifer://lineage/orders/%ZZ",
        "skifer://contracts/orders/1.2.3?sql=SELECT",
        "skifer://semantic/catalog?limit=1&limit=2",
    ],
)
def test_uri_parser_fails_closed_without_calling_service(uri):
    service = _service()

    with pytest.raises(MCPResourceError) as raised:
        MCPResources(service).read(_ctx("models:read"), uri)

    assert raised.value.code == INVALID_PARAMS
    assert not any(
        getattr(service, name).called
        for name in (
            "list_models",
            "get_model",
            "get_contract",
            "get_certification",
            "get_lineage",
        )
    )


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
            {
                "__init__": lambda self, **kwargs: self.__dict__.update(kwargs),
            },
        )

    mcp = ModuleType("mcp")
    mcp.MCPError = FakeMCPError
    server = ModuleType("mcp.server")
    server.Server = FakeServer
    types = ModuleType("mcp.types")
    for name in (
        "ListResourcesResult",
        "Resource",
        "ListResourceTemplatesResult",
        "ResourceTemplate",
        "ReadResourceResult",
        "TextResourceContents",
    ):
        setattr(types, name, model_type(name))
    monkeypatch.setitem(sys.modules, "mcp", mcp)
    monkeypatch.setitem(sys.modules, "mcp.server", server)
    monkeypatch.setitem(sys.modules, "mcp.types", types)
    return FakeMCPError


def test_server_factory_wires_sdk_types_etag_and_request_context(monkeypatch):
    _install_fake_mcp(monkeypatch)
    from skifer.mcp.server import create_server

    service = _service()
    ctx = _ctx("models:read", "contracts:read", "lineage:read")
    provider = Mock(return_value=ctx)
    server = create_server(service, provider)
    request_context = object()

    listed = asyncio.run(server.handlers["on_list_resources"](request_context, None))
    templates = asyncio.run(
        server.handlers["on_list_resource_templates"](request_context, None)
    )
    read = asyncio.run(
        server.handlers["on_read_resource"](
            request_context,
            SimpleNamespace(uri="skifer://semantic/models/orders"),
        )
    )

    assert [item.uri for item in listed.resources] == [
        "skifer://semantic/catalog"
    ]
    assert len(templates.resource_templates) == 4
    assert read.cache_scope == "private"
    assert read.contents[0]._meta[ETAG_META_KEY].startswith("sha256:")
    # Every handler resolves the caller, discovery included: a list handler that
    # skipped the provider would answer before anyone had been identified.
    assert provider.call_args_list == [call(request_context)] * 3
    service.get_model.assert_called_once_with(ctx, "orders")


def test_server_converts_sanitized_resource_error_to_sdk_error(monkeypatch):
    sdk_error = _install_fake_mcp(monkeypatch)
    from skifer.mcp.server import create_server

    service = _service()
    service.get_model.side_effect = ScopeDenied("scope plus secret token")
    server = create_server(service, lambda request: _ctx())

    with pytest.raises(sdk_error) as raised:
        asyncio.run(
            server.handlers["on_read_resource"](
                object(),
                SimpleNamespace(uri="skifer://semantic/models/orders"),
            )
        )

    assert raised.value.code == SCOPE_DENIED
    assert raised.value.message == "Required scope is missing."
    assert raised.value.data == {"error_type": "scope_denied"}


def test_server_sanitizes_context_provider_failure(monkeypatch):
    sdk_error = _install_fake_mcp(monkeypatch)
    from skifer.mcp.server import create_server

    def failed_context(request):
        raise RuntimeError("Bearer TOP_SECRET from /private/token")

    server = create_server(_service(), failed_context)

    with pytest.raises(sdk_error) as raised:
        asyncio.run(
            server.handlers["on_read_resource"](
                object(),
                SimpleNamespace(uri="skifer://semantic/models/orders"),
            )
        )

    assert raised.value.code == INTERNAL_ERROR
    assert raised.value.message == "Unexpected service error: RuntimeError."
    assert "TOP_SECRET" not in str(raised.value.data)


# ---------------------------------------------------------------------------
# Plan 29 — discovery is part of the protected surface
# ---------------------------------------------------------------------------

def test_discovery_hides_entries_the_caller_holds_no_scope_for():
    """Advertising an endpoint and the scope that opens it is reconnaissance.

    The plan's definition of done requires resources to be filtered by scope;
    listing them unconditionally handed an agent with no grant at all the full
    map of the governed surface.
    """
    resources = MCPResources(_service())

    templates = resources.list_resource_templates(_ctx("lineage:read"))

    assert [item.uri_template for item in templates.items] == [
        "skifer://lineage/{dataset}/{column}"
    ]
    assert resources.list_resources(_ctx("lineage:read")).items == ()


def test_discovery_shows_nothing_to_a_caller_with_no_scopes():
    resources = MCPResources(_service())

    assert resources.list_resources(_ctx()).items == ()
    assert resources.list_resource_templates(_ctx()).items == ()


def test_discovery_shows_both_contract_templates_to_one_contracts_scope():
    """Two templates share contracts:read; neither may be dropped."""
    resources = MCPResources(_service())

    templates = resources.list_resource_templates(_ctx("contracts:read"))

    assert [item.uri_template for item in templates.items] == [
        "skifer://contracts/{id}/{version}",
        "skifer://certification/{dataset}",
    ]


@pytest.mark.parametrize("bad_ctx", [None, object(), "models:read", {"models:read"}])
def test_discovery_refuses_anything_that_is_not_a_request_context(bad_ctx):
    """Fail closed: no context means no listing, never an unfiltered one."""
    resources = MCPResources(_service())

    with pytest.raises(InvalidRequest):
        resources.list_resources(bad_ctx)
    with pytest.raises(InvalidRequest):
        resources.list_resource_templates(bad_ctx)


# ---------------------------------------------------------------------------
# URI query parsing across Python versions
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "uri, expected_query",
    [
        ("skifer://semantic/catalog", {}),
        ("skifer://semantic/models/orders", {}),
        ("skifer://semantic/catalog?limit=10", {"limit": ["10"]}),
        ("skifer://semantic/catalog?limit=", {"limit": [""]}),
    ],
)
def test_a_uri_without_a_query_is_not_a_malformed_uri(uri, expected_query):
    """An absent query must parse as an empty one, on every supported Python.

    Before 3.11, ``parse_qs`` raised on the empty string under ``strict_parsing``. Calling it
    unconditionally therefore rejected every URI carrying no parameters — which is nearly all of
    them — and the whole MCP surface answered ``invalid_request`` on 3.10. The suite only ever ran
    on 3.12, so nothing noticed until CI covered the declared minimum.
    """
    _, _, query = _parse_uri(uri)
    assert query == expected_query


@pytest.mark.parametrize("uri", ["skifer://semantic/catalog?%%", "skifer://semantic/catalog?a&b&c"])
def test_a_present_query_is_still_parsed_strictly(uri):
    """Guarding the empty case must not loosen parsing of a query that does exist."""
    with pytest.raises(InvalidRequest):
        _parse_uri(uri)
