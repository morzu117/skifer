"""Exact approvals, idempotent execution, audited compensation, and a redacted credential.

    python examples/18_capability_execution/run.py
"""

import atexit
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path
import tempfile

from skifer.capabilities import (
    ApprovalError,
    ApprovalRecord,
    AutonomyMode,
    CapabilityExecutorRegistry,
    CapabilityRegistry,
    CredentialLease,
    GovernedExecutor,
    PreconditionDecision,
    PreconditionReport,
    SqliteCapabilityHistoryStore,
    precondition_hash,
    request_hash,
    validate_approval,
)

NOW = datetime(2026, 9, 9, 12, tzinfo=timezone.utc)
SUBJECT = "local-reader"
create_calls, compensation_calls = [], []


@CapabilityExecutorRegistry.register("example_execute_ticket")
def execute_ticket(arguments):
    create_calls.append(arguments)
    return {"ticket_id": f"ticket-{len(create_calls)}"}


@CapabilityExecutorRegistry.register("example_compensate_ticket")
def compensate_ticket(arguments):
    compensation_calls.append(arguments)
    if arguments["reason"] == "demonstrate failure":
        raise TimeoutError("private external-system detail")
    return {"closed": True}


registry = CapabilityRegistry(Path(__file__).parent)
definition = registry.get("support.create_ticket")
report = PreconditionReport(
    capability_id=definition.id,
    capability_version=definition.version,
    decision=PreconditionDecision.ALLOW,
    outcomes=(),
    evaluated_at=NOW,
    requires_escalation=False,
)


def digest(arguments):
    return request_hash(
        capability_id=definition.id,
        capability_version=definition.version,
        subject=SUBJECT,
        arguments=arguments,
    )


def approve(arguments, granted_at=NOW):
    # An approval binds one exact request (its hash) and one precondition report.
    return ApprovalRecord(
        capability_id=definition.id,
        capability_version=definition.version,
        actor="human-reviewer",
        decision="approved",
        reason="Reviewed this exact request",
        granted_at=granted_at,
        expires_at=granted_at + timedelta(hours=1),
        request_hash=digest(arguments),
        precondition_hash=precondition_hash(report),
    )


def audit(store, request):
    return " -> ".join(event.state.value for event in store.events_for(request))


arguments = {"request_id": "request-1", "description": "Network issue"}
changed = {**arguments, "description": "Network issues"}
approval = approve(arguments)

validate_approval(approval, definition=definition, subject=SUBJECT, arguments=arguments, report=report, now=NOW)
print("Exact request approval applies: True")
try:
    validate_approval(approval, definition=definition, subject=SUBJECT, arguments=changed, report=report, now=NOW)
except ApprovalError as exc:
    print(f"One-character change refusal: {exc}")
try:
    expired = approve(arguments, granted_at=NOW - timedelta(hours=2))
    validate_approval(expired, definition=definition, subject=SUBJECT, arguments=arguments, report=report, now=NOW)
except ApprovalError as exc:
    print(f"Expired-approval refusal: {exc}")

workdir = Path(tempfile.mkdtemp())
atexit.register(shutil.rmtree, workdir, ignore_errors=True)

store = SqliteCapabilityHistoryStore(str(workdir / "history.db"))
governed = GovernedExecutor(registry, store, clock=lambda: NOW, scopes=("tickets:create", "tickets:close"))

first = governed.execute(definition, subject=SUBJECT, arguments=arguments, mode=AutonomyMode.SUPERVISED, approval=approval)
second = governed.execute(definition, subject=SUBJECT, arguments=arguments, mode=AutonomyMode.SUPERVISED, approval=approval)
print(f"First execution result: {first.output}")
print(f"Same request submitted twice; external create calls: {len(create_calls)}")
print(f"Second result replayed from history: {second.to_dict() == first.to_dict()}")
print(f"Original audit before compensation: {audit(store, digest(arguments))}")

# Compensation is its own audited request, never a rollback of the original.
compensated = governed.compensate(
    definition, subject=SUBJECT, original_request_hash=digest(arguments), reason="duplicate ticket"
)
compensation = store.events_for(digest(arguments))[-1].detail["compensation_request_hash"]
print(f"Successful compensation result: {compensated.status}")
print(f"Original audit after compensation: {audit(store, digest(arguments))}")
print(f"Compensation's own audit: {audit(store, compensation)}")
print(f"Compensation has a separate request hash: {compensation != digest(arguments)}")

governed.execute(definition, subject=SUBJECT, arguments=changed, mode=AutonomyMode.SUPERVISED, approval=approve(changed))
failed = governed.compensate(
    definition, subject=SUBJECT, original_request_hash=digest(changed), reason="demonstrate failure"
)
failed_compensation = store.events_for(digest(changed))[-1].detail["compensation_request_hash"]
print(f"Failed compensation result: {failed.status} ({failed.error_type})")
print(f"Failed original stays: {store.latest_state(digest(changed)).state.value}")
print(f"Failed compensation's own audit: {audit(store, failed_compensation)}")
print(f"External compensation calls: {len(compensation_calls)}")
print("Compensation is an audited action, not a rollback.")

secret = "local-example-secret"
lease = CredentialLease(
    subject=SUBJECT,
    audience="support-api",
    scopes=frozenset({"tickets:create"}),
    expires_at=NOW + timedelta(minutes=5),
    secret=secret,
)
print(f"Credential lease repr: {lease!r}")
print(f"Secret present in repr: {secret in repr(lease)}")
print(f"Only reveal() returns the secret: {lease.reveal() == secret}")
