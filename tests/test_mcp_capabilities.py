"""Plan 29 slice 9.7 governed MCP capability tools."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from itertools import count
import json
from pathlib import Path
import subprocess
import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock

import pytest
import yaml

from skifer.agentic.data_service import (
    AgentReadyDataService,
    RequestContext,
    ServiceLimits,
)
from skifer.capabilities import (
    ApprovalRecord,
    AutonomyMode,
    CapabilityExecutorRegistry,
    CapabilityRegistry,
    GovernedExecutor,
    PreconditionDecision,
    PreconditionEvaluator,
    PreconditionOutcome,
    PreconditionRegistry,
    PreconditionReport,
    SqliteCapabilityHistoryStore,
    precondition_hash,
    request_hash,
)
from skifer.mcp.capability_tools import MCPCapabilityTools
from skifer.observability.tracing import TraceContext


NOW = datetime(2026, 9, 8, 12, tzinfo=timezone.utc)
SUBJECT = "external-agent"
ARGUMENTS = {"request_id": "request-1", "description": "Network issue"}


def _ctx(*scopes: str) -> RequestContext:
    return RequestContext(
        subject=SUBJECT,
        scopes=frozenset(scopes),
        consumer_class="mcp",
        trace_context=TraceContext(trace_id="1" * 32),
    )


def _definition(
    executor: str,
    *,
    approval: str = "guarded",
    scopes: tuple[str, ...] = ("tickets:create", "support:write"),
    preconditions: tuple[str, ...] = (),
) -> dict:
    return {
        "id": "support.create_ticket",
        "version": "1.0.0",
        "owner": "support-platform",
        "description": "Create a support ticket through the governed executor",
        "mode": "write",
        "executor": executor,
        "acting_as": "delegated_user",
        "required_scopes": list(scopes),
        "input_schema": {
            "type": "object",
            "additionalProperties": False,
            "required": ["request_id", "description"],
            "properties": {
                "request_id": {"type": "string", "maxLength": 128},
                "description": {"type": "string", "maxLength": 2000},
            },
        },
        "preconditions": [{"rule": rule} for rule in preconditions],
        "reversibility": "reversible",
        "approval": approval,
        "idempotency_key": "request_id",
        "provenance": {
            "policy_uri": "policies/support.md",
            "policy_hash": "sha256:" + "a" * 64,
        },
    }


def _registry(tmp_path: Path, definition: dict) -> CapabilityRegistry:
    tmp_path.mkdir(parents=True, exist_ok=True)
    definition_path = tmp_path / "support.yaml"
    definition_path.write_text(
        yaml.safe_dump(definition, sort_keys=False), encoding="utf-8"
    )
    summary = {
        key: definition[key]
        for key in (
            "id",
            "version",
            "owner",
            "description",
            "mode",
            "approval",
            "reversibility",
        )
    }
    summary["path"] = definition_path.name
    (tmp_path / "capability_catalog.yaml").write_text(
        yaml.safe_dump({"capabilities": [summary]}, sort_keys=False),
        encoding="utf-8",
    )
    return CapabilityRegistry(tmp_path)


def _surface(
    tmp_path: Path,
    executor_name: str,
    *,
    mode: AutonomyMode = AutonomyMode.SHADOW,
    approval: str = "guarded",
    scopes: tuple[str, ...] = ("tickets:create", "support:write"),
    preconditions: tuple[str, ...] = (),
    approval_provider=None,
    clock=None,
    state_reader=None,
) -> tuple[MCPCapabilityTools, CapabilityRegistry]:
    registry = _registry(
        tmp_path,
        _definition(
            executor_name,
            approval=approval,
            scopes=scopes,
            preconditions=preconditions,
        ),
    )
    active_clock = clock or (lambda: NOW)
    evaluator = (
        PreconditionEvaluator(clock=active_clock) if preconditions else None
    )
    governed = GovernedExecutor(
        registry,
        SqliteCapabilityHistoryStore(str(tmp_path / "history.db")),
        clock=active_clock,
        scopes=scopes,
        precondition_evaluator=evaluator,
        state_reader=state_reader,
    )
    return (
        MCPCapabilityTools(
            registry,
            governed_executor=governed,
            autonomy_mode=mode,
            approval_provider=approval_provider,
        ),
        registry,
    )


def _approval(definition, *, expires_at=NOW + timedelta(hours=1)) -> ApprovalRecord:
    report = PreconditionReport(
        capability_id=definition.id,
        capability_version=definition.version,
        decision=PreconditionDecision.ALLOW,
        outcomes=(),
        evaluated_at=NOW,
        requires_escalation=False,
    )
    return ApprovalRecord(
        capability_id=definition.id,
        capability_version=definition.version,
        actor="independent-reviewer",
        decision="approved",
        reason="Reviewed in the server-side approval system",
        granted_at=NOW,
        expires_at=expires_at,
        request_hash=request_hash(
            capability_id=definition.id,
            capability_version=definition.version,
            subject=SUBJECT,
            arguments=ARGUMENTS,
        ),
        precondition_hash=precondition_hash(report),
    )


def test_zero_or_partial_scope_callers_discover_nothing(tmp_path):
    surface, _ = _surface(tmp_path, "test_mcp_visibility")

    assert surface.list_tools(_ctx()) == ()
    assert surface.list_tools(_ctx("tickets:create")) == ()
    assert [
        item.name
        for item in surface.list_tools(_ctx("tickets:create", "support:write"))
    ] == ["capability_support_create_ticket"]


def test_descriptor_uses_definition_limits_and_guarded_is_not_an_mcp_mode(tmp_path):
    shadow, _ = _surface(tmp_path, "test_mcp_definition_limits")
    ctx = _ctx("tickets:create", "support:write")
    descriptor = shadow.list_tools(ctx)[0]

    assert descriptor.input_schema["properties"]["request_id"]["maxLength"] == 128
    assert descriptor.input_schema["properties"]["description"]["maxLength"] == 2000

    guarded, _ = _surface(
        tmp_path / "guarded",
        "test_mcp_guarded_hidden",
        mode=AutonomyMode.GUARDED,
    )
    assert guarded.list_tools(ctx) == ()


@pytest.mark.parametrize(
    "field",
    ["approval", "approved", "actor", "subject", "mode", "autonomy_mode", "scopes", "lease"],
)
def test_agent_cannot_supply_server_authority_fields(tmp_path, field):
    calls = []
    name = f"test_mcp_forbidden_{field}"
    CapabilityExecutorRegistry.register(name)(
        lambda arguments: calls.append(arguments) or {"ticket_id": "ticket-1"}
    )
    surface, _ = _surface(tmp_path, name, mode=AutonomyMode.SUPERVISED)

    result = surface.call(
        _ctx("tickets:create", "support:write"),
        "capability_support_create_ticket",
        {**ARGUMENTS, field: "caller-controlled"},
    )

    assert result["status"] == "refused"
    assert result["reason_codes"] == ["invalid_arguments"]
    assert calls == []


def test_server_authority_fields_are_absent_from_advertised_schema(tmp_path):
    surface, _ = _surface(tmp_path, "test_mcp_schema")
    descriptor = surface.list_tools(
        _ctx("tickets:create", "support:write")
    )[0]
    serialized = json.dumps(descriptor.input_schema, sort_keys=True)

    for forbidden in (
        "approval",
        "approved",
        "actor",
        "subject",
        "mode",
        "autonomy_mode",
        "scopes",
        "lease",
    ):
        assert f'"{forbidden}"' not in serialized


def test_shadow_returns_proposed_and_never_calls_executor(tmp_path):
    calls = []
    name = "test_mcp_shadow"
    CapabilityExecutorRegistry.register(name)(
        lambda arguments: calls.append(arguments) or {"changed": True}
    )
    surface, _ = _surface(tmp_path, name, mode=AutonomyMode.SHADOW)

    result = surface.call(
        _ctx("tickets:create", "support:write"),
        "capability_support_create_ticket",
        ARGUMENTS,
    )

    assert result["status"] == "proposed"
    assert calls == []


def test_supervised_without_approval_returns_pending_without_call(tmp_path):
    calls = []
    name = "test_mcp_pending"
    CapabilityExecutorRegistry.register(name)(
        lambda arguments: calls.append(arguments) or {"ticket_id": "ticket-1"}
    )
    surface, _ = _surface(tmp_path, name, mode=AutonomyMode.SUPERVISED)

    result = surface.call(
        _ctx("tickets:create", "support:write"),
        "capability_support_create_ticket",
        ARGUMENTS,
    )

    assert result["status"] == "pending"
    assert result["reason_codes"] == ["approval_required"]
    assert calls == []


def test_server_side_valid_approval_executes_and_duplicate_replays(tmp_path):
    calls = []
    name = "test_mcp_approved_duplicate"
    CapabilityExecutorRegistry.register(name)(
        lambda arguments: calls.append(dict(arguments)) or {"ticket_id": "ticket-1"}
    )
    holder = {}
    approval_lookups = count()
    ticks = count()

    def approval_provider(definition, identity):
        if next(approval_lookups) == 0:
            return holder["approval"]
        raise OSError("approval backend unavailable during replay")

    surface, registry = _surface(
        tmp_path,
        name,
        mode=AutonomyMode.SUPERVISED,
        approval_provider=approval_provider,
        clock=lambda: NOW + timedelta(seconds=next(ticks)),
    )
    holder["approval"] = _approval(registry.get("support.create_ticket"))
    ctx = _ctx("tickets:create", "support:write")

    first = surface.call(ctx, "capability_support_create_ticket", ARGUMENTS)
    replay = surface.call(ctx, "capability_support_create_ticket", ARGUMENTS)

    assert first["status"] == replay["status"] == "executed"
    assert first["outcome"] == replay["outcome"]
    assert calls == [ARGUMENTS]


def test_expired_approval_never_executes_with_an_advancing_clock(tmp_path):
    calls = []
    name = "test_mcp_expired_approval"
    CapabilityExecutorRegistry.register(name)(
        lambda arguments: calls.append(arguments) or {"ticket_id": "ticket-1"}
    )
    ticks = count()

    def moving_clock():
        return NOW + timedelta(seconds=next(ticks))

    holder = {}
    surface, registry = _surface(
        tmp_path,
        name,
        mode=AutonomyMode.SUPERVISED,
        approval_provider=lambda definition, identity: holder["approval"],
        clock=moving_clock,
    )
    holder["approval"] = _approval(
        registry.get("support.create_ticket"),
        expires_at=NOW + timedelta(seconds=1),
    )

    result = surface.call(
        _ctx("tickets:create", "support:write"),
        "capability_support_create_ticket",
        ARGUMENTS,
    )

    assert result["status"] in {"pending", "refused"}
    assert result["status"] != "executed"
    assert calls == []


@pytest.mark.parametrize(
    ("decision", "reason_code"),
    [
        (PreconditionDecision.DENY, "duplicate_open"),
        (PreconditionDecision.UNKNOWN, "state_unavailable"),
    ],
)
def test_non_allow_precondition_returns_codes_only(
    tmp_path, decision, reason_code
):
    rule_name = f"test_mcp_precondition_{decision.value.lower()}"

    @PreconditionRegistry.register(rule_name)
    def rule(arguments, state):
        return PreconditionOutcome(
            rule=rule_name,
            rule_version="1.0.0",
            decision=decision,
            reason_code=reason_code,
            observed_state_hash="sha256:v1:" + "0" * 64,
            observed_at=NOW,
        )

    calls = []
    executor_name = f"test_mcp_executor_{decision.value.lower()}"
    CapabilityExecutorRegistry.register(executor_name)(
        lambda arguments: calls.append(arguments) or {}
    )
    surface, _ = _surface(
        tmp_path,
        executor_name,
        mode=AutonomyMode.SUPERVISED,
        preconditions=(rule_name,),
        state_reader=lambda: {"private_detail": "must-not-cross"},
    )

    result = surface.call(
        _ctx("tickets:create", "support:write"),
        "capability_support_create_ticket",
        ARGUMENTS,
    )

    assert result["status"] == "refused"
    assert result["reason_codes"] == ["precondition_refused"]
    serialized = json.dumps(result)
    assert reason_code not in serialized
    assert "private_detail" not in serialized
    assert calls == []


def test_envelope_omits_free_text_exception_and_authority_material(tmp_path):
    sentinel = "credential=TOP_SECRET lease at /private/path"
    name = "test_mcp_failed_output"

    @CapabilityExecutorRegistry.register(name)
    def executor(arguments):
        raise RuntimeError(sentinel)

    holder = {}
    ticks = count()
    surface, registry = _surface(
        tmp_path,
        name,
        mode=AutonomyMode.SUPERVISED,
        approval_provider=lambda definition, identity: holder["approval"],
        clock=lambda: NOW + timedelta(seconds=next(ticks)),
    )
    definition = registry.get("support.create_ticket")
    holder["approval"] = _approval(definition)

    result = surface.call(
        _ctx("tickets:create", "support:write", "certification_override"),
        "capability_support_create_ticket",
        ARGUMENTS,
    )

    serialized = json.dumps(result, sort_keys=True)
    assert result["status"] == "executed"
    assert result["outcome"] == {
        "status": "failed",
        "output": {},
        "error_type": "RuntimeError",
    }
    for forbidden in (
        sentinel,
        "TOP_SECRET",
        "certification_override",
        SUBJECT,
        definition.description,
        definition.policy_uri,
        "independent-reviewer",
        "Reviewed in the server-side approval system",
    ):
        assert forbidden not in serialized


def _service() -> AgentReadyDataService:
    service = object.__new__(AgentReadyDataService)
    service.limits = ServiceLimits()
    service.query = Mock()
    return service


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


def test_server_merges_tools_and_advisory_read_only_hint_cannot_bypass_governance(
    tmp_path, monkeypatch
):
    _install_fake_mcp(monkeypatch)
    from skifer.mcp.server import create_server

    calls = []
    name = "test_mcp_annotation_not_boundary"
    CapabilityExecutorRegistry.register(name)(
        lambda arguments: calls.append(arguments) or {"ticket_id": "ticket-1"}
    )
    holder = {}
    ticks = count()
    surface, registry = _surface(
        tmp_path,
        name,
        mode=AutonomyMode.SUPERVISED,
        approval_provider=lambda definition, identity: holder["approval"],
        clock=lambda: NOW + timedelta(seconds=next(ticks)),
    )
    holder["approval"] = _approval(registry.get("support.create_ticket"))
    original_list = surface.list_tools

    def misleading_list(ctx):
        descriptors = original_list(ctx)
        object.__setattr__(descriptors[0], "read_only_hint", True)
        return descriptors

    monkeypatch.setattr(surface, "list_tools", misleading_list)
    ctx = _ctx("query:execute", "tickets:create", "support:write")
    server = create_server(_service(), lambda request: ctx, capability_tools=surface)

    listed = asyncio.run(server.handlers["on_list_tools"](object(), None))
    result = asyncio.run(
        server.handlers["on_call_tool"](
            object(),
            SimpleNamespace(
                name="capability_support_create_ticket",
                arguments=ARGUMENTS,
            ),
        )
    )

    assert [tool.name for tool in listed.tools] == [
        "query_semantic_model",
        "capability_support_create_ticket",
    ]
    assert listed.tools[1].annotations.readOnlyHint is True
    assert result.structured_content["status"] == "executed"
    assert calls == [ARGUMENTS]


def test_unknown_and_caller_invisible_tools_have_identical_server_error(
    tmp_path, monkeypatch
):
    sdk_error = _install_fake_mcp(monkeypatch)
    from skifer.mcp.server import create_server

    surface, _ = _surface(tmp_path, "test_mcp_invisible")
    server = create_server(_service(), lambda request: _ctx(), capability_tools=surface)

    errors = []
    for name in ("capability_does_not_exist", "capability_support_create_ticket"):
        with pytest.raises(sdk_error) as raised:
            asyncio.run(
                server.handlers["on_call_tool"](
                    object(), SimpleNamespace(name=name, arguments=ARGUMENTS)
                )
            )
        errors.append((raised.value.code, raised.value.message, raised.value.data))

    assert errors[0] == errors[1]


def test_capability_mcp_modules_import_without_optional_sdk():
    project_root = Path(__file__).resolve().parents[1]
    script = """
import importlib.abc
import sys
class BlockMCP(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == 'mcp' or fullname.startswith('mcp.'):
            raise AssertionError('optional MCP SDK imported')
        return None
sys.meta_path.insert(0, BlockMCP())
import skifer
import skifer.mcp.capability_tools
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
