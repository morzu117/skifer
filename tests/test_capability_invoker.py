"""Selection, input and result contracts for read-only capability invocation."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import gc
from pathlib import Path
from typing import Any
import weakref

import pytest
import yaml

from skifer.capabilities import (
    CapabilityExecutorError,
    CapabilityExecutorRegistry,
    CapabilityInvoker,
    CapabilityRegistry,
    CapabilityRegistryError,
    AutonomyMode,
    CredentialBroker,
    CredentialError,
    CredentialLease,
    PreconditionDecision,
    PreconditionError,
    PreconditionEvaluator,
    PreconditionOutcome,
    PreconditionRegistry,
    validate_arguments,
)


EXECUTOR_CALLS: list[dict[str, Any]] = []
FAILING_MESSAGE = "private external value must never cross the boundary"


@CapabilityExecutorRegistry.register("test_inventory_lookup")
def fake_read_executor(arguments):
    EXECUTOR_CALLS.append(arguments)
    arguments["record_id"] = "executor-mutated-copy"
    return {"found": True, "labels": ["safe"], "count": 1}


@CapabilityExecutorRegistry.register("test_inventory_failure")
def fake_failing_executor(arguments):
    raise RuntimeError(f"{FAILING_MESSAGE}: {arguments['record_id']}")


@CapabilityExecutorRegistry.register("test_inventory_unsafe_output")
def fake_unsafe_output_executor(arguments):
    return {"unsafe": {arguments["record_id"]}}


CREDENTIAL_EXECUTOR_CALLS: list[tuple[dict[str, Any], str]] = []
CREDENTIAL_LEASE_REFS: list[weakref.ReferenceType] = []


@CapabilityExecutorRegistry.register(
    "test_inventory_credential", needs_credential=True
)
def fake_credential_executor(arguments, lease):
    CREDENTIAL_EXECUTOR_CALLS.append((arguments, lease.reveal()))
    CREDENTIAL_LEASE_REFS.append(weakref.ref(lease))
    return {"found": True}


class InvokerCredentialProvider:
    def __init__(self, *, expires_at=None):
        self.calls = []
        self.expires_at = expires_at or datetime(2026, 9, 8, 10, 5, tzinfo=timezone.utc)

    def issue(self, **kwargs):
        self.calls.append(kwargs)
        return CredentialLease(
            subject=kwargs["subject"],
            audience=kwargs["audience"],
            scopes=kwargs["scopes"],
            expires_at=self.expires_at,
            secret="executor-only-secret",
        )


def _read_capability(executor: str = "test_inventory_lookup") -> dict[str, Any]:
    return {
        "id": "inventory.lookup_record",
        "version": "1.2.0",
        "owner": "inventory-platform",
        "description": "Look up a fake inventory record",
        "mode": "read",
        "executor": executor,
        "acting_as": "service",
        "required_scopes": ["inventory:read"],
        "input_schema": {
            "type": "object",
            "additionalProperties": False,
            "required": ["record_id", "limit", "active"],
            "properties": {
                "record_id": {
                    "type": "string",
                    "maxLength": 12,
                    "enum": ["item-1", "item-2"],
                },
                "limit": {"type": "integer", "minimum": 1, "maximum": 5},
                "active": {"type": "boolean", "enum": [True]},
                "tags": {
                    "type": "array",
                    "maxItems": 2,
                    "items": {"type": "string", "maxLength": 8},
                },
            },
        },
        "preconditions": [],
        "reversibility": "reversible",
        "approval": "guarded",
        "provenance": {
            "policy_uri": "policies/inventory-read-v1.md",
            "policy_hash": "sha256:" + "b" * 64,
        },
    }


def _write_catalog(root: Path, definition: dict[str, Any]) -> CapabilityRegistry:
    definition_path = root / "lookup.yaml"
    definition_path.write_text(yaml.safe_dump(definition, sort_keys=False), encoding="utf-8")
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
    (root / "capability_catalog.yaml").write_text(
        yaml.safe_dump({"capabilities": [summary]}, sort_keys=False),
        encoding="utf-8",
    )
    return CapabilityRegistry(root)


@pytest.fixture
def registry(tmp_path: Path) -> CapabilityRegistry:
    return _write_catalog(tmp_path, _read_capability())


def test_invoke_selects_by_id_validates_and_returns_safe_result(
    registry: CapabilityRegistry,
) -> None:
    EXECUTOR_CALLS.clear()
    arguments = {"record_id": "item-1", "limit": 2, "active": True, "tags": ["blue"]}
    result = CapabilityInvoker(registry, scopes=["inventory:read"]).invoke(
        "inventory.lookup_record", arguments
    )

    assert arguments["record_id"] == "item-1"
    assert len(EXECUTOR_CALLS) == 1
    assert result.to_dict() == {
        "capability_id": "inventory.lookup_record",
        "capability_version": "1.2.0",
        "status": "succeeded",
        "output": {"found": True, "labels": ["safe"], "count": 1},
        "error_type": None,
    }


def test_unknown_id_is_refused_before_argument_or_executor_work(
    registry: CapabilityRegistry,
) -> None:
    EXECUTOR_CALLS.clear()
    with pytest.raises(CapabilityRegistryError, match="inventory.missing"):
        CapabilityInvoker(registry, scopes=["inventory:read"]).invoke(
            "inventory.missing", {"unknown": object()}
        )
    assert EXECUTOR_CALLS == []


def test_write_capability_is_loudly_refused_before_scope_check(tmp_path: Path) -> None:
    definition = _read_capability()
    definition.update(
        mode="write",
        idempotency_key="record_id",
        required_scopes=["inventory:write"],
        approval="supervised",
    )
    registry = _write_catalog(tmp_path, definition)

    EXECUTOR_CALLS.clear()
    with pytest.raises(CapabilityExecutorError, match="requires GovernedExecutor"):
        CapabilityInvoker(registry).invoke("inventory.lookup_record", {})
    assert EXECUTOR_CALLS == []


def test_required_scopes_come_only_from_invoker(registry: CapabilityRegistry) -> None:
    arguments = {
        "record_id": "item-1",
        "limit": 2,
        "active": True,
        "scopes": ["inventory:read"],
    }
    with pytest.raises(CapabilityExecutorError, match="inventory:read"):
        CapabilityInvoker(registry).invoke("inventory.lookup_record", arguments)


@pytest.mark.parametrize(
    "arguments,expected",
    [
        ({"record_id": "item-1", "limit": 1, "active": True, "extra": 1}, "extra"),
        ({"record_id": "item-1", "limit": 1}, "active"),
        ({"record_id": "item-1", "limit": True, "active": True}, "integer"),
        ({"record_id": "too-long-for-schema", "limit": 1, "active": True}, "characters"),
        ({"record_id": "other", "limit": 1, "active": True}, "enum"),
        ({"record_id": "item-1", "limit": 0, "active": True}, "at least"),
        ({"record_id": "item-1", "limit": 6, "active": True}, "at most"),
        (
            {"record_id": "item-1", "limit": 1, "active": True, "tags": ["a", "b", "c"]},
            "items",
        ),
    ],
)
def test_argument_validation_refuses_each_closed_schema_violation(
    registry: CapabilityRegistry,
    arguments: dict[str, Any],
    expected: str,
) -> None:
    with pytest.raises(CapabilityExecutorError, match=expected):
        CapabilityInvoker(registry, scopes=["inventory:read"]).invoke(
            "inventory.lookup_record", arguments
        )


def test_validate_arguments_accepts_declared_values(registry: CapabilityRegistry) -> None:
    schema = registry.get("inventory.lookup_record").input_schema
    assert validate_arguments(
        schema,
        {"record_id": "item-2", "limit": 5, "active": True, "tags": []},
    ) == []


def test_executor_failure_returns_only_exception_class(tmp_path: Path) -> None:
    registry = _write_catalog(tmp_path, _read_capability("test_inventory_failure"))
    result = CapabilityInvoker(registry, scopes=["inventory:read"]).invoke(
        "inventory.lookup_record",
        {"record_id": "item-1", "limit": 1, "active": True},
    )

    serialized = result.to_dict()
    assert serialized["status"] == "failed"
    assert serialized["output"] == {}
    assert serialized["error_type"] == "RuntimeError"
    assert FAILING_MESSAGE not in str(serialized)
    assert "item-1" not in str(serialized)


def test_non_json_safe_executor_output_becomes_failed_result(tmp_path: Path) -> None:
    registry = _write_catalog(tmp_path, _read_capability("test_inventory_unsafe_output"))
    result = CapabilityInvoker(registry, scopes=["inventory:read"]).invoke(
        "inventory.lookup_record",
        {"record_id": "item-1", "limit": 1, "active": True},
    )
    assert result.status == "failed"
    assert result.output == {}
    assert result.error_type == "TypeError"


_BOUNDED_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["tags"],
    "properties": {
        "tags": {
            "type": "array",
            "maxItems": 10,
            "items": {"type": "string", "maxLength": 8},
        }
    },
}


def test_oversized_array_is_refused_without_walking_it():
    """A declared bound must stop the walk, not merely be reported.

    Reporting `maxItems` and then descending into every element buys the caller
    work proportional to what they sent: 200k surplus items produced 200k error
    messages and an 8 MB exception string, from a capability that declared 10.
    """
    from skifer.capabilities.invoker import validate_arguments

    # Each item also breaches maxLength, so a walk that does not stop reports one
    # error per element on top of the maxItems breach.
    errors = validate_arguments(_BOUNDED_SCHEMA, {"tags": ["x" * 50] * 200_000})

    assert errors == ["Field 'arguments.tags' must contain at most 10 items."]


def test_argument_errors_are_capped_and_say_so():
    from skifer.capabilities.invoker import (
        _MAX_ARGUMENT_ERRORS,
        validate_arguments,
    )

    unknown = {f"unexpected_{index}": 1 for index in range(5_000)}
    errors = validate_arguments(_BOUNDED_SCHEMA, unknown)

    assert len(errors) == _MAX_ARGUMENT_ERRORS + 1
    assert errors[-1] == (
        f"Argument validation stopped after {_MAX_ARGUMENT_ERRORS} errors."
    )
    assert len("; ".join(errors)) < 4096


def test_valid_bounded_arguments_still_pass():
    from skifer.capabilities.invoker import validate_arguments

    assert validate_arguments(_BOUNDED_SCHEMA, {"tags": ["a", "b"]}) == []


def _with_precondition(rule: str) -> dict[str, Any]:
    definition = _read_capability()
    definition["preconditions"] = [{"rule": rule}]
    return definition


def test_declared_precondition_without_evaluator_is_refused_before_executor(
    tmp_path: Path,
) -> None:
    registry = _write_catalog(
        tmp_path, _with_precondition("test_invoker_missing_evaluator")
    )
    EXECUTOR_CALLS.clear()

    with pytest.raises(PreconditionError, match="no precondition evaluator"):
        CapabilityInvoker(registry, scopes=["inventory:read"]).invoke(
            "inventory.lookup_record",
            {"record_id": "item-1", "limit": 1, "active": True},
        )
    assert EXECUTOR_CALLS == []


def test_all_allow_preconditions_are_rechecked_and_executor_runs_once(
    tmp_path: Path,
) -> None:
    name = "test_invoker_all_allow"
    rule_calls = []

    @PreconditionRegistry.register(name)
    def rule(arguments, state):
        rule_calls.append(state["revision"])
        return PreconditionOutcome(
            rule=name,
            rule_version="1.0.0",
            decision=PreconditionDecision.ALLOW,
            reason_code="record_open",
            observed_state_hash="sha256:v1:" + "0" * 64,
            observed_at=datetime(2026, 9, 8, tzinfo=timezone.utc),
        )

    registry = _write_catalog(tmp_path, _with_precondition(name))
    EXECUTOR_CALLS.clear()
    state_reads = []

    def state_reader():
        state_reads.append(True)
        return {"revision": 1}

    result = CapabilityInvoker(
        registry,
        scopes=["inventory:read"],
        precondition_evaluator=PreconditionEvaluator(
            clock=lambda: datetime(2026, 9, 8, tzinfo=timezone.utc)
        ),
        state_reader=state_reader,
    ).invoke(
        "inventory.lookup_record",
        {"record_id": "item-1", "limit": 1, "active": True},
    )

    assert result.status == "succeeded"
    assert rule_calls == [1, 1]
    assert len(state_reads) == 2
    assert len(EXECUTOR_CALLS) == 1


def test_non_allow_precondition_refuses_with_codes_only(tmp_path: Path) -> None:
    name = "test_invoker_denied"
    secret = "customer 123 must not leak"

    @PreconditionRegistry.register(name)
    def rule(arguments, state):
        raise TimeoutError(secret)

    registry = _write_catalog(tmp_path, _with_precondition(name))
    EXECUTOR_CALLS.clear()
    invoker = CapabilityInvoker(
        registry,
        scopes=["inventory:read"],
        precondition_evaluator=PreconditionEvaluator(
            clock=lambda: datetime(2026, 9, 8, tzinfo=timezone.utc)
        ),
        state_reader=lambda: {},
    )

    with pytest.raises(PreconditionError, match="UNKNOWN.*rule_error_timeouterror") as exc:
        invoker.invoke(
            "inventory.lookup_record",
            {"record_id": "item-1", "limit": 1, "active": True},
        )
    assert secret not in str(exc.value)
    assert EXECUTOR_CALLS == []


def test_invoker_recheck_refuses_changed_state_before_executor(tmp_path: Path) -> None:
    name = "test_invoker_changed_state"

    @PreconditionRegistry.register(name)
    def rule(arguments, state):
        return PreconditionOutcome(
            rule=name,
            rule_version="1.0.0",
            decision=PreconditionDecision.ALLOW,
            reason_code="allowed",
            observed_state_hash="sha256:v1:" + "0" * 64,
            observed_at=datetime(2026, 9, 8, tzinfo=timezone.utc),
        )

    states = iter(({"revision": 1}, {"revision": 2}))
    registry = _write_catalog(tmp_path, _with_precondition(name))
    EXECUTOR_CALLS.clear()
    invoker = CapabilityInvoker(
        registry,
        scopes=["inventory:read"],
        precondition_evaluator=PreconditionEvaluator(
            clock=lambda: datetime(2026, 9, 8, tzinfo=timezone.utc)
        ),
        state_reader=lambda: next(states),
    )

    with pytest.raises(PreconditionError, match="state changed"):
        invoker.invoke(
            "inventory.lookup_record",
            {"record_id": "item-1", "limit": 1, "active": True},
        )
    assert EXECUTOR_CALLS == []


def test_credential_lease_is_second_argument_and_not_retained(tmp_path: Path) -> None:
    registry = _write_catalog(tmp_path, _read_capability("test_inventory_credential"))
    provider = InvokerCredentialProvider()
    broker = CredentialBroker(
        provider, clock=lambda: datetime(2026, 9, 8, 10, tzinfo=timezone.utc)
    )
    CREDENTIAL_EXECUTOR_CALLS.clear()
    CREDENTIAL_LEASE_REFS.clear()

    result = CapabilityInvoker(
        registry,
        scopes=["inventory:read"],
        credential_broker=broker,
        credential_subject="user-1",
        credential_audience="inventory-api",
        autonomy_mode=AutonomyMode.SUPERVISED,
    ).invoke(
        "inventory.lookup_record",
        {"record_id": "item-1", "limit": 1, "active": True},
    )

    assert result.status == "succeeded"
    assert CREDENTIAL_EXECUTOR_CALLS == [
        ({"record_id": "item-1", "limit": 1, "active": True}, "executor-only-secret")
    ]
    assert "lease" not in CREDENTIAL_EXECUTOR_CALLS[0][0]
    gc.collect()
    assert CREDENTIAL_LEASE_REFS[0]() is None
    assert not any(
        isinstance(value, CredentialLease)
        for value in vars(
            CapabilityInvoker(
                registry,
                scopes=["inventory:read"],
                credential_broker=broker,
                credential_subject="user-1",
                credential_audience="inventory-api",
            )
        ).values()
    )


def test_unmarked_read_executor_never_acquires_credential(registry) -> None:
    provider = InvokerCredentialProvider()
    result = CapabilityInvoker(
        registry,
        scopes=["inventory:read"],
        credential_broker=CredentialBroker(
            provider, clock=lambda: datetime(2026, 9, 8, 10, tzinfo=timezone.utc)
        ),
        credential_subject="user-1",
        credential_audience="inventory-api",
    ).invoke(
        "inventory.lookup_record",
        {"record_id": "item-1", "limit": 1, "active": True},
    )
    assert result.status == "succeeded"
    assert provider.calls == []


def test_shadow_credential_read_refuses_without_calling_provider(tmp_path: Path) -> None:
    registry = _write_catalog(tmp_path, _read_capability("test_inventory_credential"))
    provider = InvokerCredentialProvider()
    invoker = CapabilityInvoker(
        registry,
        scopes=["inventory:read"],
        credential_broker=CredentialBroker(
            provider, clock=lambda: datetime(2026, 9, 8, 10, tzinfo=timezone.utc)
        ),
        credential_subject="user-1",
        credential_audience="inventory-api",
        autonomy_mode=AutonomyMode.SHADOW,
    )
    with pytest.raises(CredentialError, match="SHADOW"):
        invoker.invoke(
            "inventory.lookup_record",
            {"record_id": "item-1", "limit": 1, "active": True},
        )
    assert provider.calls == []


def test_lease_expiring_between_issuance_and_use_is_refused(tmp_path: Path) -> None:
    base = datetime(2026, 9, 8, 10, tzinfo=timezone.utc)
    clock_values = iter((base, base + timedelta(seconds=2)))
    provider = InvokerCredentialProvider(expires_at=base + timedelta(seconds=1))
    registry = _write_catalog(tmp_path, _read_capability("test_inventory_credential"))
    CREDENTIAL_EXECUTOR_CALLS.clear()

    invoker = CapabilityInvoker(
        registry,
        scopes=["inventory:read"],
        credential_broker=CredentialBroker(provider, clock=lambda: next(clock_values)),
        credential_subject="user-1",
        credential_audience="inventory-api",
        autonomy_mode=AutonomyMode.SUPERVISED,
    )
    with pytest.raises(CredentialError, match="expires_at"):
        invoker.invoke(
            "inventory.lookup_record",
            {"record_id": "item-1", "limit": 1, "active": True},
        )
    assert CREDENTIAL_EXECUTOR_CALLS == []
