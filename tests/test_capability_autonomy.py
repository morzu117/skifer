"""Autonomy policy, bounded approvals, and explicit execution-state tests."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import os
import subprocess
import sys
import textwrap
from typing import Any

import pytest

from skifer.capabilities import (
    ActingAs,
    ApprovalError,
    ApprovalMode,
    ApprovalRecord,
    AutonomyError,
    AutonomyMode,
    AutonomyStateMachine,
    CapabilityDefinition,
    CapabilityExecutorRegistry,
    CapabilityMode,
    ExecutionState,
    PreconditionDecision,
    PreconditionOutcome,
    PreconditionReport,
    Reversibility,
    TransitionError,
    precondition_hash,
    request_hash,
    resolve_autonomy_mode,
    validate_approval,
)
from skifer.capabilities.autonomy import (
    MAX_APPROVAL_ACTOR_LENGTH,
    MAX_APPROVAL_REASON_LENGTH,
    MAX_APPROVAL_WINDOW,
    MAX_STORED_APPROVALS,
    TRANSITIONS,
)
from skifer.capabilities.preconditions import _MAX_STATE_BYTES


NOW = datetime(2026, 9, 8, 12, 0, tzinfo=timezone.utc)
ARGUMENTS = {"ticket_id": "ticket-1", "note": "checked"}


def _definition(
    *,
    approval: ApprovalMode = ApprovalMode.SUPERVISED,
    reversibility: Reversibility = Reversibility.COMPENSATABLE,
    compensation: str | None = "support.close_ticket",
    executor: str = "test_autonomy_executor",
) -> CapabilityDefinition:
    return CapabilityDefinition(
        id="support.update_ticket",
        version="1.0.0",
        owner="support-platform",
        description="Update a support ticket",
        mode=CapabilityMode.WRITE,
        executor=executor,
        acting_as=ActingAs.DELEGATED_USER,
        required_scopes=("support:write",),
        input_schema={
            "type": "object",
            "additionalProperties": False,
            "required": ["ticket_id", "note"],
            "properties": {
                "ticket_id": {"type": "string", "maxLength": 64},
                "note": {"type": "string", "maxLength": 2000},
            },
        },
        preconditions=("ticket_open",),
        reversibility=reversibility,
        compensation=compensation,
        approval=approval,
        idempotency_key="ticket_id",
        policy_uri="policies/support-v1.md",
        policy_hash="sha256:" + "a" * 64,
    )


def _report(*, observed_hash: str = "sha256:v1:" + "1" * 64) -> PreconditionReport:
    return PreconditionReport(
        capability_id="support.update_ticket",
        capability_version="1.0.0",
        decision=PreconditionDecision.ALLOW,
        outcomes=(
            PreconditionOutcome(
                rule="ticket_open",
                rule_version="1.0.0",
                decision=PreconditionDecision.ALLOW,
                reason_code="ticket_open",
                observed_state_hash=observed_hash,
                observed_at=NOW,
            ),
        ),
        evaluated_at=NOW,
        requires_escalation=False,
    )


def _approval(
    definition: CapabilityDefinition,
    report: PreconditionReport,
    **overrides: Any,
) -> ApprovalRecord:
    values = {
        "capability_id": definition.id,
        "capability_version": definition.version,
        "actor": "reviewer@example.com",
        "decision": "approved",
        "reason": "Request and current ticket state reviewed",
        "granted_at": NOW,
        "expires_at": NOW + timedelta(hours=1),
        "request_hash": request_hash(
            capability_id=definition.id,
            capability_version=definition.version,
            subject="agent@example.com",
            arguments=ARGUMENTS,
        ),
        "precondition_hash": precondition_hash(report),
    }
    values.update(overrides)
    return ApprovalRecord(**values)


def test_full_supervised_legal_path() -> None:
    definition = _definition()
    report = _report()
    approval = _approval(definition, report)
    machine = AutonomyStateMachine(clock=lambda: NOW + timedelta(minutes=1))

    machine.preconditions_passed(report)
    assert machine.state is ExecutionState.PRECONDITIONS_PASSED
    machine.request_approval(
        definition=definition, subject="agent@example.com", arguments=ARGUMENTS
    )
    assert machine.state is ExecutionState.PENDING_APPROVAL
    machine.record_approval(approval)
    assert machine.state is ExecutionState.APPROVED
    machine.begin_execution(
        definition=definition,
        subject="agent@example.com",
        arguments=ARGUMENTS,
        report=report,
    )
    assert machine.state is ExecutionState.EXECUTING
    machine.record_success()
    assert machine.state is ExecutionState.EXECUTED


def test_transition_table_exposes_the_complete_graph() -> None:
    assert set(TRANSITIONS) == set(ExecutionState)
    assert ExecutionState.EXECUTED not in TRANSITIONS[ExecutionState.FAILED]
    assert ExecutionState.COMPENSATED not in TRANSITIONS[ExecutionState.EXECUTED]


def test_proposed_cannot_execute_without_allow_preconditions() -> None:
    with pytest.raises(TransitionError, match="ALLOW precondition"):
        AutonomyStateMachine(clock=lambda: NOW).begin_execution(
            definition=_definition(),
            subject="agent@example.com",
            arguments=ARGUMENTS,
        )

    with pytest.raises(TransitionError, match="PROPOSED -> EXECUTING"):
        AutonomyStateMachine(clock=lambda: NOW).begin_execution(
            definition=_definition(),
            subject="agent@example.com",
            arguments=ARGUMENTS,
            report=_report(),
        )


def test_non_allow_report_cannot_leave_proposed() -> None:
    denied = _report()
    object.__setattr__(denied, "decision", PreconditionDecision.DENY)
    machine = AutonomyStateMachine(clock=lambda: NOW)

    with pytest.raises(TransitionError, match="requires an ALLOW"):
        machine.preconditions_passed(denied)
    assert machine.state is ExecutionState.PROPOSED


def test_pending_approval_cannot_execute_without_valid_approval() -> None:
    definition = _definition()
    report = _report()
    machine = AutonomyStateMachine(clock=lambda: NOW)
    machine.preconditions_passed(report)
    machine.request_approval(
        definition=definition, subject="agent@example.com", arguments=ARGUMENTS
    )

    with pytest.raises(TransitionError, match="without a valid approval"):
        machine.begin_execution(
            definition=definition,
            subject="agent@example.com",
            arguments=ARGUMENTS,
            report=report,
        )


def test_failed_cannot_become_executed_without_reconciliation() -> None:
    definition = _definition(approval=ApprovalMode.GUARDED)
    report = _report()
    machine = AutonomyStateMachine(clock=lambda: NOW)
    machine.preconditions_passed(report)
    machine.begin_execution(
        definition=definition,
        subject="agent@example.com",
        arguments=ARGUMENTS,
        report=report,
    )
    machine.record_failure()

    with pytest.raises(TransitionError, match="FAILED -> EXECUTED"):
        machine.record_success()


def test_compensation_requires_intermediate_state_and_declaration() -> None:
    definition = _definition(approval=ApprovalMode.GUARDED)
    report = _report()
    machine = AutonomyStateMachine(clock=lambda: NOW)
    machine.preconditions_passed(report)
    machine.begin_execution(
        definition=definition,
        subject="agent@example.com",
        arguments=ARGUMENTS,
        report=report,
    )
    machine.record_success()

    with pytest.raises(TransitionError, match="EXECUTED -> COMPENSATED"):
        machine.record_compensated()
    with pytest.raises(TransitionError, match="declared compensation"):
        machine.begin_compensation(
            definition=_definition(
                approval=ApprovalMode.GUARDED,
                reversibility=Reversibility.REVERSIBLE,
                compensation=None,
            )
        )
    machine.begin_compensation(definition=definition)
    machine.record_compensated()
    assert machine.state is ExecutionState.COMPENSATED


def test_resolve_mode_always_selects_more_restrictive_side() -> None:
    assert (
        resolve_autonomy_mode(
            _definition(approval=ApprovalMode.SHADOW), configured=AutonomyMode.SUPERVISED
        )
        is AutonomyMode.SHADOW
    )
    assert (
        resolve_autonomy_mode(
            _definition(approval=ApprovalMode.GUARDED), configured=AutonomyMode.SUPERVISED
        )
        is AutonomyMode.SUPERVISED
    )
    assert resolve_autonomy_mode(_definition(), configured=None) is AutonomyMode.SHADOW


def test_irreversible_capability_can_never_resolve_guarded() -> None:
    definition = _definition()
    object.__setattr__(definition, "reversibility", Reversibility.IRREVERSIBLE)
    object.__setattr__(definition, "approval", ApprovalMode.GUARDED)

    with pytest.raises(AutonomyError, match="never resolve to GUARDED"):
        resolve_autonomy_mode(definition, configured=AutonomyMode.GUARDED)


def test_approval_to_dict_is_field_by_field_allowlisted() -> None:
    approval = _approval(_definition(), _report())
    object.__setattr__(approval, "credential", "must-not-leak")

    assert set(approval.to_dict()) == {
        "capability_id",
        "capability_version",
        "actor",
        "decision",
        "reason",
        "granted_at",
        "expires_at",
        "request_hash",
        "precondition_hash",
    }
    assert "must-not-leak" not in str(approval.to_dict())


def test_request_hash_reorders_arguments_but_detects_value_change() -> None:
    common = {
        "capability_id": "support.update_ticket",
        "capability_version": "1.0.0",
        "subject": "agent@example.com",
    }
    first = request_hash(arguments={"b": 2, "a": 1}, **common)
    reordered = request_hash(arguments={"a": 1, "b": 2}, **common)
    changed = request_hash(arguments={"a": 1, "b": 3}, **common)
    assert first == reordered
    assert first != changed


def test_request_hash_is_stable_across_python_hash_seeds() -> None:
    script = textwrap.dedent(
        """
        from skifer.capabilities import request_hash
        arguments = {key: key for key in {"gamma", "alpha", "beta"}}
        print(request_hash(capability_id="support.update_ticket",
                           capability_version="1.0.0", subject="agent", arguments=arguments))
        """
    )
    outputs = set()
    for seed in ("1", "17", "999"):
        environment = os.environ.copy()
        environment["PYTHONHASHSEED"] = seed
        outputs.add(
            subprocess.check_output(
                [sys.executable, "-c", script], text=True, env=environment
            ).strip()
        )
    assert len(outputs) == 1


def test_request_hash_reuses_enforced_state_snapshot_bound() -> None:
    with pytest.raises(ApprovalError, match="too large"):
        request_hash(
            capability_id="support.update_ticket",
            capability_version="1.0.0",
            subject="agent",
            arguments={"note": "x" * (_MAX_STATE_BYTES + 1)},
        )


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ({"decision": "denied"}, "not approved"),
        ({"expires_at": NOW}, "expired"),
        ({"capability_id": "support.other"}, "capability id"),
        ({"capability_version": "2.0.0"}, "capability version"),
    ],
)
def test_validate_approval_distinct_refusals(mutation, message) -> None:
    definition = _definition()
    report = _report()
    approval = _approval(definition, report)
    for key, value in mutation.items():
        object.__setattr__(approval, key, value)

    with pytest.raises(ApprovalError, match=message):
        validate_approval(
            approval,
            definition=definition,
            subject="agent@example.com",
            arguments=ARGUMENTS,
            report=report,
            now=NOW,
        )


@pytest.mark.parametrize("field", ["granted_at", "expires_at"])
def test_validate_approval_refuses_naive_record_datetime(field: str) -> None:
    definition = _definition()
    report = _report()
    approval = _approval(definition, report)
    object.__setattr__(approval, field, NOW.replace(tzinfo=None))

    with pytest.raises(ApprovalError, match=f"{field}.*timezone-aware"):
        validate_approval(
            approval,
            definition=definition,
            subject="agent@example.com",
            arguments=ARGUMENTS,
            report=report,
            now=NOW,
        )


def test_validate_approval_refuses_naive_now() -> None:
    definition = _definition()
    report = _report()
    with pytest.raises(ApprovalError, match="now.*timezone-aware"):
        validate_approval(
            _approval(definition, report),
            definition=definition,
            subject="agent@example.com",
            arguments=ARGUMENTS,
            report=report,
            now=NOW.replace(tzinfo=None),
        )


def test_approval_ttl_ceiling_is_an_enforced_refusal() -> None:
    with pytest.raises(ApprovalError, match="enforced 4-hour maximum"):
        _approval(
            _definition(),
            _report(),
            expires_at=NOW + MAX_APPROVAL_WINDOW + timedelta(microseconds=1),
        )


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("actor", "", "actor"),
        ("actor", "a" * (MAX_APPROVAL_ACTOR_LENGTH + 1), "actor"),
        ("reason", "r" * (MAX_APPROVAL_REASON_LENGTH + 1), "reason"),
        ("reason", "unsafe\nreason", "control characters"),
    ],
)
def test_approval_text_bounds_are_enforced(field, value, message) -> None:
    with pytest.raises(ApprovalError, match=message):
        _approval(_definition(), _report(), **{field: value})


def test_stored_approval_limit_is_an_enforced_refusal() -> None:
    definition = _definition()
    report = _report()
    approval = _approval(definition, report)
    machine = AutonomyStateMachine(clock=lambda: NOW)
    machine.preconditions_passed(report)
    machine.request_approval(
        definition=definition, subject="agent@example.com", arguments=ARGUMENTS
    )
    machine._approvals.extend([approval] * MAX_STORED_APPROVALS)

    with pytest.raises(ApprovalError, match="Stored approval limit"):
        machine.record_approval(approval)
    assert machine.state is ExecutionState.PENDING_APPROVAL


def test_one_character_argument_change_invalidates_approval() -> None:
    definition = _definition()
    report = _report()
    approval = _approval(definition, report)

    with pytest.raises(ApprovalError, match="request_hash"):
        validate_approval(
            approval,
            definition=definition,
            subject="agent@example.com",
            arguments={**ARGUMENTS, "note": "checkee"},
            report=report,
            now=NOW,
        )


def test_changed_precondition_hash_invalidates_approval() -> None:
    definition = _definition()
    report = _report()
    approval = _approval(definition, report)

    with pytest.raises(ApprovalError, match="precondition_hash"):
        validate_approval(
            approval,
            definition=definition,
            subject="agent@example.com",
            arguments=ARGUMENTS,
            report=_report(observed_hash="sha256:v1:" + "2" * 64),
            now=NOW,
        )


def test_approved_state_revalidates_hashes_before_execution() -> None:
    definition = _definition()
    report = _report()
    machine = AutonomyStateMachine(clock=lambda: NOW)
    machine.preconditions_passed(report)
    machine.request_approval(
        definition=definition, subject="agent@example.com", arguments=ARGUMENTS
    )
    machine.record_approval(_approval(definition, report))

    with pytest.raises(ApprovalError, match="request_hash"):
        machine.begin_execution(
            definition=definition,
            subject="agent@example.com",
            arguments={**ARGUMENTS, "note": "checkee"},
            report=report,
        )
    assert machine.state is ExecutionState.APPROVED


def test_shadow_without_dry_run_records_proposal_and_never_calls_executor() -> None:
    name = "test_shadow_no_dry_run"
    calls = []

    @CapabilityExecutorRegistry.register(name)
    def executor(arguments):
        calls.append(arguments)
        return {"changed": True}

    machine = AutonomyStateMachine(clock=lambda: NOW)
    assert machine.record_shadow(
        definition=_definition(executor=name),
        subject="agent@example.com",
        arguments=ARGUMENTS,
    ) is None
    assert calls == []
    assert machine.proposal is not None
    assert machine.state is ExecutionState.PROPOSED


def test_shadow_dry_run_is_explicit_simulation_and_never_executes() -> None:
    name = "test_shadow_with_dry_run"
    contexts = []

    @CapabilityExecutorRegistry.register(name, supports_dry_run=True)
    def executor(arguments, *, dry_run):
        contexts.append(dry_run)
        assert dry_run is True
        return {"would_change": arguments["ticket_id"]}

    machine = AutonomyStateMachine(clock=lambda: NOW)
    result = machine.record_shadow(
        definition=_definition(executor=name),
        subject="agent@example.com",
        arguments=ARGUMENTS,
    )

    assert getattr(executor, "supports_dry_run") is True
    assert contexts == [True]
    assert result == {
        "status": "SIMULATED",
        "output": {"would_change": "ticket-1"},
    }
    assert machine.state is ExecutionState.PROPOSED


def _report_at(moment: datetime, *, observed_hash: str = "sha256:v1:" + "1" * 64):
    """The same verdicts over the same state, evaluated at a different moment."""
    base = _report(observed_hash=observed_hash)
    return PreconditionReport(
        capability_id=base.capability_id,
        capability_version=base.capability_version,
        decision=base.decision,
        outcomes=tuple(
            PreconditionOutcome(
                rule=outcome.rule,
                rule_version=outcome.rule_version,
                decision=outcome.decision,
                reason_code=outcome.reason_code,
                observed_state_hash=outcome.observed_state_hash,
                observed_at=moment,
            )
            for outcome in base.outcomes
        ),
        evaluated_at=moment,
        requires_escalation=base.requires_escalation,
    )


def test_precondition_hash_ignores_wall_clock_time():
    """The report is re-evaluated before execution; time must not change its identity.

    Hashing `evaluated_at` made the hash differ on every recheck even when the state
    and every verdict were identical, so an approval could never validate against a
    fresh report and supervised mode was unusable. A fixed test clock hid this
    entirely.
    """
    later = NOW + timedelta(seconds=30)

    assert precondition_hash(_report()) == precondition_hash(_report_at(later))


def test_precondition_hash_still_tracks_observed_state():
    moved = "sha256:v1:" + "2" * 64

    assert precondition_hash(_report()) != precondition_hash(_report(observed_hash=moved))


def test_approval_validates_against_a_freshly_evaluated_report():
    definition = _definition()
    approval = _approval(definition, _report())

    validate_approval(
        approval,
        definition=definition,
        subject="agent@example.com",
        arguments=ARGUMENTS,
        report=_report_at(NOW + timedelta(seconds=30)),
        now=NOW + timedelta(seconds=30),
    )
