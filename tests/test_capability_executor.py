"""Idempotent governed execution and audited compensation contracts."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from itertools import count
from pathlib import Path
from typing import Any

import pytest
import yaml

from skifer.capabilities import (
    ApprovalError,
    ApprovalRecord,
    AutonomyMode,
    CapabilityExecutorRegistry,
    CapabilityHistoryError,
    CapabilityRegistry,
    CapabilityStateEvent,
    CredentialBroker,
    CredentialLease,
    ExecutionState,
    GovernedExecutor,
    LostResponseError,
    PreconditionDecision,
    PreconditionReport,
    SqliteCapabilityHistoryStore,
    precondition_hash,
    request_hash,
)


NOW = datetime(2026, 9, 8, 12, tzinfo=timezone.utc)
SUBJECT = "agent@example.com"
ARGUMENTS = {"request_id": "request-1", "description": "Network issue"}


def _definition(
    executor: str,
    *,
    capability_id: str = "support.create_ticket",
    approval: str = "guarded",
    reversibility: str = "compensatable",
    compensation: str | None = "support.close_ticket",
    scopes=None,
) -> dict[str, Any]:
    payload = {
        "id": capability_id,
        "version": "1.0.0",
        "owner": "support-platform",
        "description": "Change a support ticket",
        "mode": "write",
        "executor": executor,
        "acting_as": "delegated_user",
        "required_scopes": scopes or ["tickets:create"],
        "input_schema": {
            "type": "object",
            "additionalProperties": False,
            "required": ["request_id", "description"],
            "properties": {
                "request_id": {"type": "string", "maxLength": 128},
                "description": {"type": "string", "maxLength": 2000},
            },
        },
        "preconditions": [],
        "reversibility": reversibility,
        "approval": approval,
        "idempotency_key": "request_id",
        "provenance": {
            "policy_uri": "policies/support.md",
            "policy_hash": "sha256:" + "a" * 64,
        },
    }
    if compensation is not None:
        payload["compensation"] = compensation
    return payload


def _compensation_definition(executor: str) -> dict[str, Any]:
    payload = _definition(
        executor,
        capability_id="support.close_ticket",
        reversibility="reversible",
        compensation=None,
        scopes=["tickets:close"],
    )
    payload["input_schema"] = {
        "type": "object",
        "additionalProperties": False,
        "required": ["original_request_hash", "reason"],
        "properties": {
            "original_request_hash": {"type": "string", "maxLength": 80},
            "reason": {"type": "string", "maxLength": 256},
        },
    }
    payload["idempotency_key"] = "original_request_hash"
    return payload


def _registry(tmp_path: Path, *definitions: dict[str, Any]) -> CapabilityRegistry:
    entries = []
    for index, definition in enumerate(definitions):
        path = tmp_path / f"capability-{index}.yaml"
        path.write_text(yaml.safe_dump(definition, sort_keys=False), encoding="utf-8")
        entry = {
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
        entry["path"] = path.name
        entries.append(entry)
    (tmp_path / "capability_catalog.yaml").write_text(
        yaml.safe_dump({"capabilities": entries}, sort_keys=False), encoding="utf-8"
    )
    return CapabilityRegistry(tmp_path)


def _report(definition, at=NOW) -> PreconditionReport:
    return PreconditionReport(
        capability_id=definition.id,
        capability_version=definition.version,
        decision=PreconditionDecision.ALLOW,
        outcomes=(),
        evaluated_at=at,
        requires_escalation=False,
    )


def _approval(definition, arguments=ARGUMENTS, *, expires_at=None) -> ApprovalRecord:
    report = _report(definition)
    return ApprovalRecord(
        capability_id=definition.id,
        capability_version=definition.version,
        actor="reviewer@example.com",
        decision="approved",
        reason="Reviewed exact request and state",
        granted_at=NOW,
        expires_at=expires_at or NOW + timedelta(hours=1),
        request_hash=request_hash(
            capability_id=definition.id,
            capability_version=definition.version,
            subject=SUBJECT,
            arguments=arguments,
        ),
        precondition_hash=precondition_hash(report),
    )


def _governed(registry, store, **kwargs) -> GovernedExecutor:
    return GovernedExecutor(
        registry,
        store,
        clock=kwargs.pop("clock", lambda: NOW),
        scopes=kwargs.pop("scopes", ["tickets:create", "tickets:close"]),
        **kwargs,
    )


def test_pre_call_event_is_durable_before_external_executor_runs(tmp_path: Path) -> None:
    store = SqliteCapabilityHistoryStore(str(tmp_path / "history.db"))
    seen = []
    name = "test_governed_precall"
    definition_payload = _definition(name)
    registry = _registry(tmp_path, definition_payload)
    definition = registry.get(definition_payload["id"])
    digest = request_hash(
        capability_id=definition.id,
        capability_version=definition.version,
        subject=SUBJECT,
        arguments=ARGUMENTS,
    )

    @CapabilityExecutorRegistry.register(name)
    def executor(arguments):
        seen.append(store.latest_state(digest).state)
        return {"ticket_id": "ticket-1"}

    result = _governed(registry, store).execute(
        definition,
        subject=SUBJECT,
        arguments=ARGUMENTS,
        mode=AutonomyMode.GUARDED,
    )
    assert result.status == "succeeded"
    assert seen == [ExecutionState.EXECUTING]
    assert [event.state for event in store.events_for(digest)] == [
        ExecutionState.EXECUTING,
        ExecutionState.EXECUTED,
    ]


def test_duplicate_request_replays_recorded_outcome_and_calls_external_once(
    tmp_path: Path,
) -> None:
    calls = []
    name = "test_governed_duplicate"

    @CapabilityExecutorRegistry.register(name)
    def executor(arguments):
        calls.append(dict(arguments))
        return {"ticket_id": "ticket-1"}

    registry = _registry(tmp_path, _definition(name))
    definition = registry.get("support.create_ticket")
    store = SqliteCapabilityHistoryStore(str(tmp_path / "history.db"))
    governed = _governed(registry, store)

    first = governed.execute(
        definition, subject=SUBJECT, arguments=ARGUMENTS, mode=AutonomyMode.GUARDED
    )
    second = governed.execute(
        definition, subject=SUBJECT, arguments=ARGUMENTS, mode=AutonomyMode.GUARDED
    )

    assert second.to_dict() == first.to_dict()
    assert calls == [ARGUMENTS]


def test_lost_response_without_lookup_never_calls_external_again(
    tmp_path: Path,
) -> None:
    calls = []
    name = "test_lost_without_lookup"

    @CapabilityExecutorRegistry.register(name)
    def executor(arguments):
        calls.append(arguments)
        return {"unsafe": True}

    registry = _registry(tmp_path, _definition(name))
    definition = registry.get("support.create_ticket")
    store = SqliteCapabilityHistoryStore(str(tmp_path / "history.db"))
    digest = _seed_executing(store, definition)

    with pytest.raises(LostResponseError, match="no declared idempotency lookup"):
        _governed(registry, store).execute(
            definition,
            subject=SUBJECT,
            arguments=ARGUMENTS,
            mode=AutonomyMode.GUARDED,
        )
    assert calls == []
    assert store.latest_state(digest).state is ExecutionState.EXECUTING


def _seed_executing(store, definition, *, detail=None) -> str:
    digest = request_hash(
        capability_id=definition.id,
        capability_version=definition.version,
        subject=SUBJECT,
        arguments=ARGUMENTS,
    )
    store.append(
        CapabilityStateEvent(
            event_id=f"seed-{len(store.events_for(digest))}",
            request_hash=digest,
            capability_id=definition.id,
            capability_version=definition.version,
            subject=SUBJECT,
            state=ExecutionState.EXECUTING,
            occurred_at=NOW,
            detail=detail or {"kind": "external_call"},
        )
    )
    return digest


def test_lost_response_with_lookup_reconciles_without_recalling_and_clock_moves(
    tmp_path: Path,
) -> None:
    external_calls = []
    lookup_calls = []

    def lookup(arguments):
        lookup_calls.append(dict(arguments))
        return {"ticket_id": "ticket-existing"}

    name = "test_lost_with_lookup"

    @CapabilityExecutorRegistry.register(name, lookup=lookup)
    def executor(arguments):
        external_calls.append(arguments)
        return {"ticket_id": "ticket-new"}

    registry = _registry(tmp_path, _definition(name))
    definition = registry.get("support.create_ticket")
    store = SqliteCapabilityHistoryStore(str(tmp_path / "history.db"))
    digest = _seed_executing(store, definition)
    instants = iter((NOW + timedelta(seconds=1), NOW + timedelta(seconds=2)))

    result = _governed(registry, store, clock=lambda: next(instants)).execute(
        definition,
        subject=SUBJECT,
        arguments=ARGUMENTS,
        mode=AutonomyMode.GUARDED,
    )

    assert result.output == {"ticket_id": "ticket-existing"}
    assert external_calls == []
    assert lookup_calls == [ARGUMENTS]
    events = store.events_for(digest)
    assert [event.state for event in events] == [
        ExecutionState.EXECUTING,
        ExecutionState.EXECUTING,
        ExecutionState.EXECUTED,
    ]
    assert events[1].occurred_at < events[2].occurred_at


def test_reconciliation_retry_count_is_an_enforced_refusal(tmp_path: Path) -> None:
    lookup_calls = []

    def lookup(arguments):
        lookup_calls.append(arguments)
        return {"ticket_id": "ticket-existing"}

    name = "test_lost_retry_bound"
    CapabilityExecutorRegistry.register(name, lookup=lookup)(lambda arguments: {})
    registry = _registry(tmp_path, _definition(name))
    definition = registry.get("support.create_ticket")
    store = SqliteCapabilityHistoryStore(str(tmp_path / "history.db"))
    digest = _seed_executing(store, definition)
    store.append(
        CapabilityStateEvent(
            event_id="attempt-1",
            request_hash=digest,
            capability_id=definition.id,
            capability_version=definition.version,
            subject=SUBJECT,
            state=ExecutionState.EXECUTING,
            occurred_at=NOW + timedelta(seconds=1),
            detail={"kind": "reconciliation_attempt", "attempt": 1},
        )
    )

    with pytest.raises(LostResponseError, match="retry limit of 1"):
        _governed(registry, store, max_retries=1).execute(
            definition,
            subject=SUBJECT,
            arguments=ARGUMENTS,
            mode=AutonomyMode.GUARDED,
        )
    assert lookup_calls == []


def test_approval_for_different_arguments_is_refused_before_call(tmp_path: Path) -> None:
    calls = []
    name = "test_approval_wrong_arguments"
    CapabilityExecutorRegistry.register(name)(lambda arguments: calls.append(arguments) or {})
    registry = _registry(tmp_path, _definition(name, approval="supervised"))
    definition = registry.get("support.create_ticket")
    store = SqliteCapabilityHistoryStore(str(tmp_path / "history.db"))

    with pytest.raises(ApprovalError, match="request_hash"):
        _governed(registry, store).execute(
            definition,
            subject=SUBJECT,
            arguments=ARGUMENTS,
            mode=AutonomyMode.SUPERVISED,
            approval=_approval(
                definition,
                {**ARGUMENTS, "description": "A different request"},
            ),
        )
    assert calls == []
    assert store.events_for(
        request_hash(
            capability_id=definition.id,
            capability_version=definition.version,
            subject=SUBJECT,
            arguments=ARGUMENTS,
        )
    ) == []


def test_approval_expiry_uses_a_clock_that_advances_before_execution(
    tmp_path: Path,
) -> None:
    calls = []
    name = "test_approval_moving_clock"
    CapabilityExecutorRegistry.register(name)(lambda arguments: calls.append(arguments) or {})
    registry = _registry(tmp_path, _definition(name, approval="supervised"))
    definition = registry.get("support.create_ticket")
    store = SqliteCapabilityHistoryStore(str(tmp_path / "history.db"))
    instants = iter((NOW, NOW + timedelta(seconds=2)))

    with pytest.raises(ApprovalError, match="expired"):
        _governed(registry, store, clock=lambda: next(instants)).execute(
            definition,
            subject=SUBJECT,
            arguments=ARGUMENTS,
            mode=AutonomyMode.SUPERVISED,
            approval=_approval(definition, expires_at=NOW + timedelta(seconds=1)),
        )
    assert calls == []


def test_shadow_never_calls_executor_takes_lease_or_records_executing(
    tmp_path: Path,
) -> None:
    calls = []
    provider_calls = []
    name = "test_shadow_write"

    @CapabilityExecutorRegistry.register(name, needs_credential=True)
    def executor(arguments, lease):
        calls.append((arguments, lease))
        return {}

    class Provider:
        def issue(self, **kwargs):
            provider_calls.append(kwargs)
            raise AssertionError("must not issue")

    registry = _registry(tmp_path, _definition(name))
    definition = registry.get("support.create_ticket")
    store = SqliteCapabilityHistoryStore(str(tmp_path / "history.db"))
    governed = _governed(
        registry,
        store,
        credential_broker=CredentialBroker(Provider(), clock=lambda: NOW),
        credential_audience="support-api",
    )

    result = governed.execute(
        definition, subject=SUBJECT, arguments=ARGUMENTS, mode=AutonomyMode.SHADOW
    )
    assert result.output == {"state": "PROPOSED"}
    assert calls == []
    assert provider_calls == []
    assert store._conn.execute("SELECT COUNT(*) FROM capability_state_events").fetchone() == (0,)


def test_post_call_store_failure_is_surfaced_not_reported_as_success(
    tmp_path: Path,
) -> None:
    calls = []
    name = "test_post_call_append_failure"
    CapabilityExecutorRegistry.register(name)(
        lambda arguments: calls.append(arguments) or {"ticket_id": "ticket-1"}
    )
    registry = _registry(tmp_path, _definition(name))
    definition = registry.get("support.create_ticket")

    class FailingHistory:
        def __init__(self):
            self.events = []

        def latest_state(self, request_hash):
            return self.events[-1] if self.events else None

        def events_for(self, request_hash, *, limit=100):
            return list(self.events[:limit])

        def append(self, event):
            if self.events:
                raise OSError("history unavailable")
            self.events.append(event)

    history = FailingHistory()
    with pytest.raises(OSError, match="history unavailable"):
        _governed(registry, history).execute(
            definition,
            subject=SUBJECT,
            arguments=ARGUMENTS,
            mode=AutonomyMode.GUARDED,
        )
    assert len(calls) == 1
    assert history.events[0].state is ExecutionState.EXECUTING


def test_credential_secret_never_reaches_raw_history_rows(tmp_path: Path) -> None:
    secret = "credential-material-that-must-not-leak"
    name = "test_history_no_credential"

    @CapabilityExecutorRegistry.register(name, needs_credential=True)
    def executor(arguments, lease):
        assert lease.reveal() == secret
        return {"ticket_id": "ticket-1"}

    class Provider:
        def issue(self, **kwargs):
            return CredentialLease(
                subject=kwargs["subject"],
                audience=kwargs["audience"],
                scopes=kwargs["scopes"],
                expires_at=NOW + timedelta(minutes=5),
                secret=secret,
            )

    registry = _registry(tmp_path, _definition(name))
    definition = registry.get("support.create_ticket")
    store = SqliteCapabilityHistoryStore(str(tmp_path / "history.db"))
    # One advancing clock per consumer. A single shared finite iterator ran out
    # mid-execution, so this test raised StopIteration and its leak assertions --
    # the whole point of the test -- never ran.
    ticks = count()

    def advancing_clock():
        return NOW + timedelta(seconds=next(ticks))

    broker = CredentialBroker(Provider(), clock=advancing_clock)

    result = _governed(
        registry,
        store,
        clock=advancing_clock,
        credential_broker=broker,
        credential_audience="support-api",
    ).execute(
        definition, subject=SUBJECT, arguments=ARGUMENTS, mode=AutonomyMode.GUARDED
    )
    assert result.status == "succeeded"
    raw = store._conn.execute("SELECT * FROM capability_state_events").fetchall()
    assert secret not in repr(raw)
    assert "CredentialLease" not in repr(raw)


def test_executor_detail_bound_refuses_oversized_outcome_after_call(tmp_path: Path) -> None:
    calls = []
    name = "test_executor_detail_bound"
    CapabilityExecutorRegistry.register(name)(
        lambda arguments: calls.append(arguments) or {"large": "x" * 256}
    )
    registry = _registry(tmp_path, _definition(name))
    definition = registry.get("support.create_ticket")
    store = SqliteCapabilityHistoryStore(str(tmp_path / "history.db"))

    with pytest.raises(CapabilityHistoryError, match="128-byte limit"):
        _governed(registry, store, max_detail_bytes=128).execute(
            definition,
            subject=SUBJECT,
            arguments=ARGUMENTS,
            mode=AutonomyMode.GUARDED,
        )
    assert len(calls) == 1
    assert store.latest_state(
        request_hash(
            capability_id=definition.id,
            capability_version=definition.version,
            subject=SUBJECT,
            arguments=ARGUMENTS,
        )
    ).state is ExecutionState.EXECUTING


def test_compensation_is_separate_run_and_preserves_original_events(tmp_path: Path) -> None:
    create_name = "test_compensation_create"
    close_name = "test_compensation_close"
    CapabilityExecutorRegistry.register(create_name)(lambda arguments: {"ticket_id": "ticket-1"})
    CapabilityExecutorRegistry.register(close_name)(
        lambda arguments: {"closed": arguments["original_request_hash"]}
    )
    registry = _registry(
        tmp_path, _definition(create_name), _compensation_definition(close_name)
    )
    original = registry.get("support.create_ticket")
    store = SqliteCapabilityHistoryStore(str(tmp_path / "history.db"))
    governed = _governed(registry, store)
    governed.execute(
        original, subject=SUBJECT, arguments=ARGUMENTS, mode=AutonomyMode.GUARDED
    )
    original_hash = request_hash(
        capability_id=original.id,
        capability_version=original.version,
        subject=SUBJECT,
        arguments=ARGUMENTS,
    )
    before = store.events_for(original_hash)

    result = governed.compensate(
        original,
        subject=SUBJECT,
        original_request_hash=original_hash,
        reason="Duplicate ticket",
    )

    after = store.events_for(original_hash)
    assert result.status == "succeeded"
    assert after[: len(before)] == before
    assert [event.state for event in after] == [
        ExecutionState.EXECUTING,
        ExecutionState.EXECUTED,
        ExecutionState.COMPENSATING,
        ExecutionState.COMPENSATED,
    ]
    compensation_hash = after[-1].detail["compensation_request_hash"]
    assert compensation_hash != original_hash
    assert [event.state for event in store.events_for(compensation_hash)] == [
        ExecutionState.EXECUTING,
        ExecutionState.EXECUTED,
    ]


def test_failed_compensation_leaves_original_compensating_never_compensated(
    tmp_path: Path,
) -> None:
    create_name = "test_failed_compensation_create"
    close_name = "test_failed_compensation_close"
    CapabilityExecutorRegistry.register(create_name)(lambda arguments: {"ticket_id": "ticket-1"})

    @CapabilityExecutorRegistry.register(close_name)
    def failing_compensation(arguments):
        raise TimeoutError("external private message")

    registry = _registry(
        tmp_path, _definition(create_name), _compensation_definition(close_name)
    )
    original = registry.get("support.create_ticket")
    store = SqliteCapabilityHistoryStore(str(tmp_path / "history.db"))
    governed = _governed(registry, store)
    governed.execute(
        original, subject=SUBJECT, arguments=ARGUMENTS, mode=AutonomyMode.GUARDED
    )
    original_hash = request_hash(
        capability_id=original.id,
        capability_version=original.version,
        subject=SUBJECT,
        arguments=ARGUMENTS,
    )

    result = governed.compensate(
        original,
        subject=SUBJECT,
        original_request_hash=original_hash,
        reason="Operator requested compensation",
    )

    assert result.status == "failed"
    original_events = store.events_for(original_hash)
    assert original_events[-1].state is ExecutionState.COMPENSATING
    assert all(event.state is not ExecutionState.COMPENSATED for event in original_events)
    compensation_hash = original_events[-1].detail["compensation_request_hash"]
    assert [event.state for event in store.events_for(compensation_hash)] == [
        ExecutionState.EXECUTING,
        ExecutionState.FAILED,
    ]


def test_credential_echoed_by_an_executor_is_redacted_not_stored(tmp_path: Path) -> None:
    """An executor that echoes its lease must not put it in the history.

    The history is append-only, so a secret written there cannot be scrubbed
    afterwards. Rejecting a `CredentialLease` instance and refusing secret-looking
    key names is not enough: the secret VALUE under an innocuous key sailed
    straight into the raw database file. Redaction beats failure here — the
    external side effect has already happened, and discarding the record of it
    would invite exactly the duplicate this slice exists to prevent.
    """
    secret = "credential-echoed-by-a-careless-executor"
    name = "test_executor_echoes_lease"

    @CapabilityExecutorRegistry.register(name, needs_credential=True)
    def executor(arguments, lease):
        return {"ticket_id": "ticket-1", "trace": lease.reveal()}

    class Provider:
        def issue(self, **kwargs):
            return CredentialLease(
                subject=kwargs["subject"],
                audience=kwargs["audience"],
                scopes=kwargs["scopes"],
                expires_at=NOW + timedelta(minutes=5),
                secret=secret,
            )

    registry = _registry(tmp_path, _definition(name))
    definition = registry.get("support.create_ticket")
    database = tmp_path / "history.db"
    store = SqliteCapabilityHistoryStore(str(database))
    ticks = count()

    def advancing_clock():
        return NOW + timedelta(seconds=next(ticks))

    broker = CredentialBroker(Provider(), clock=advancing_clock)
    governed = _governed(
        registry,
        store,
        clock=advancing_clock,
        credential_broker=broker,
        credential_audience="support-api",
    )

    result = governed.execute(
        definition, subject=SUBJECT, arguments=ARGUMENTS, mode=AutonomyMode.GUARDED
    )

    # The action succeeded and is recorded as such.
    assert result.status == "succeeded"
    assert store.latest_state(
        request_hash(
            capability_id=definition.id,
            capability_version=definition.version,
            subject=SUBJECT,
            arguments=ARGUMENTS,
        )
    ).state is ExecutionState.EXECUTED

    # The secret is nowhere on disk, not merely absent from the parsed rows.
    store._conn.commit()
    assert secret.encode() not in database.read_bytes()
    assert result.output["trace"] == "<redacted-credential>"
    assert result.output["ticket_id"] == "ticket-1"


def test_redacted_replay_is_identical_to_the_first_call(tmp_path: Path) -> None:
    """Replay must be indistinguishable from the original, redaction included."""
    secret = "credential-echoed-then-replayed"
    name = "test_executor_echo_replay"
    calls: list[int] = []

    @CapabilityExecutorRegistry.register(name, needs_credential=True)
    def executor(arguments, lease):
        calls.append(1)
        return {"ticket_id": "ticket-1", "trace": lease.reveal()}

    class Provider:
        def issue(self, **kwargs):
            return CredentialLease(
                subject=kwargs["subject"],
                audience=kwargs["audience"],
                scopes=kwargs["scopes"],
                expires_at=NOW + timedelta(minutes=5),
                secret=secret,
            )

    registry = _registry(tmp_path, _definition(name))
    definition = registry.get("support.create_ticket")
    store = SqliteCapabilityHistoryStore(str(tmp_path / "history.db"))

    def build():
        ticks = count()

        def advancing_clock():
            return NOW + timedelta(seconds=next(ticks))

        return _governed(
            registry,
            store,
            clock=advancing_clock,
            credential_broker=CredentialBroker(Provider(), clock=advancing_clock),
            credential_audience="support-api",
        )

    first = build().execute(
        definition, subject=SUBJECT, arguments=ARGUMENTS, mode=AutonomyMode.GUARDED
    )
    replay = build().execute(
        definition, subject=SUBJECT, arguments=ARGUMENTS, mode=AutonomyMode.GUARDED
    )

    assert replay.to_dict() == first.to_dict()
    assert calls == [1]
