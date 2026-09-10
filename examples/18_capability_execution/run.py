"""Execute, replay, and compensate a fake external capability locally."""

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


EXAMPLE_DIR = Path(__file__).parent
NOW = datetime(2026, 9, 9, 12, tzinfo=timezone.utc)
SUBJECT = "local-reader"
CREATE_CALLS: list[dict] = []
COMPENSATION_CALLS: list[dict] = []


@CapabilityExecutorRegistry.register("example_execute_ticket")
def execute_ticket(arguments):
    CREATE_CALLS.append(dict(arguments))
    return {"ticket_id": f"ticket-{len(CREATE_CALLS)}"}


@CapabilityExecutorRegistry.register("example_compensate_ticket")
def compensate_ticket(arguments):
    COMPENSATION_CALLS.append(dict(arguments))
    if arguments["reason"] == "demonstrate failure":
        raise TimeoutError("private external-system detail")
    return {"closed": True}


def report(definition) -> PreconditionReport:
    return PreconditionReport(
        capability_id=definition.id,
        capability_version=definition.version,
        decision=PreconditionDecision.ALLOW,
        outcomes=(),
        evaluated_at=NOW,
        requires_escalation=False,
    )


def approval(definition, arguments, *, granted_at=NOW, expires_at=None):
    current_report = report(definition)
    return ApprovalRecord(
        capability_id=definition.id,
        capability_version=definition.version,
        actor="human-reviewer",
        decision="approved",
        reason="Reviewed this exact request",
        granted_at=granted_at,
        expires_at=expires_at or granted_at + timedelta(hours=1),
        request_hash=request_hash(
            capability_id=definition.id,
            capability_version=definition.version,
            subject=SUBJECT,
            arguments=arguments,
        ),
        precondition_hash=precondition_hash(current_report),
    )


def state_path(store, digest):
    return " -> ".join(event.state.value for event in store.events_for(digest))


def main() -> None:
    registry = CapabilityRegistry(EXAMPLE_DIR)
    definition = registry.get("support.create_ticket")
    arguments = {
        "request_id": "request-1",
        "description": "Network issue",
    }
    exact_approval = approval(definition, arguments)

    validate_approval(
        exact_approval,
        definition=definition,
        subject=SUBJECT,
        arguments=arguments,
        report=report(definition),
        now=NOW,
    )
    print("Exact request approval applies: True")
    changed = {**arguments, "description": "Network issues"}
    try:
        validate_approval(
            exact_approval,
            definition=definition,
            subject=SUBJECT,
            arguments=changed,
            report=report(definition),
            now=NOW,
        )
    except ApprovalError as exc:
        print(f"One-character change refusal: {exc}")

    expired = approval(
        definition,
        arguments,
        granted_at=NOW - timedelta(hours=2),
        expires_at=NOW - timedelta(hours=1),
    )
    try:
        validate_approval(
            expired,
            definition=definition,
            subject=SUBJECT,
            arguments=arguments,
            report=report(definition),
            now=NOW,
        )
    except ApprovalError as exc:
        print(f"Expired-approval refusal: {exc}")

    temporary_path = None
    with tempfile.TemporaryDirectory() as temporary:
        temporary_path = Path(temporary)
        store = SqliteCapabilityHistoryStore(str(temporary_path / "history.db"))
        governed = GovernedExecutor(
            registry,
            store,
            clock=lambda: NOW,
            scopes=("tickets:create", "tickets:close"),
        )

        first = governed.execute(
            definition,
            subject=SUBJECT,
            arguments=arguments,
            mode=AutonomyMode.SUPERVISED,
            approval=exact_approval,
        )
        second = governed.execute(
            definition,
            subject=SUBJECT,
            arguments=arguments,
            mode=AutonomyMode.SUPERVISED,
            approval=exact_approval,
        )
        original_hash = request_hash(
            capability_id=definition.id,
            capability_version=definition.version,
            subject=SUBJECT,
            arguments=arguments,
        )
        print(f"First execution result: {first.output}")
        print(f"Same request submitted twice; external create calls: {len(CREATE_CALLS)}")
        print(f"Second result replayed from history: {second.to_dict() == first.to_dict()}")
        print(f"Original audit before compensation: {state_path(store, original_hash)}")

        compensated = governed.compensate(
            definition,
            subject=SUBJECT,
            original_request_hash=original_hash,
            reason="duplicate ticket",
        )
        original_events = store.events_for(original_hash)
        compensation_hash = original_events[-1].detail["compensation_request_hash"]
        print(f"Successful compensation result: {compensated.status}")
        print(f"Original audit after compensation: {state_path(store, original_hash)}")
        print(f"Compensation's own audit: {state_path(store, compensation_hash)}")
        print(f"Compensation has a separate request hash: {compensation_hash != original_hash}")

        changed_approval = approval(definition, changed)
        governed.execute(
            definition,
            subject=SUBJECT,
            arguments=changed,
            mode=AutonomyMode.SUPERVISED,
            approval=changed_approval,
        )
        changed_hash = request_hash(
            capability_id=definition.id,
            capability_version=definition.version,
            subject=SUBJECT,
            arguments=changed,
        )
        failed = governed.compensate(
            definition,
            subject=SUBJECT,
            original_request_hash=changed_hash,
            reason="demonstrate failure",
        )
        changed_events = store.events_for(changed_hash)
        failed_compensation_hash = changed_events[-1].detail[
            "compensation_request_hash"
        ]
        print(f"Failed compensation result: {failed.status} ({failed.error_type})")
        print(f"Failed original stays: {store.latest_state(changed_hash).state.value}")
        print(
            "Failed compensation's own audit: "
            f"{state_path(store, failed_compensation_hash)}"
        )
        print(f"External compensation calls: {len(COMPENSATION_CALLS)}")
        print("Compensation is an audited action, not a rollback.")
        store.close()

    secret = "local-example-secret"
    lease = CredentialLease(
        subject=SUBJECT,
        audience="support-api",
        scopes=frozenset({"tickets:create"}),
        expires_at=NOW + timedelta(minutes=5),
        secret=secret,
    )
    rendered = repr(lease)
    print(f"Credential lease repr: {rendered}")
    print(f"Secret present in repr: {secret in rendered}")
    print(f"Only reveal() returns the secret: {lease.reveal() == secret}")
    print(f"Temporary history removed: {not temporary_path.exists()}")


if __name__ == "__main__":
    main()
