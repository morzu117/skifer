"""Record a governed capability proposal without executing an action."""

from datetime import datetime, timezone
from pathlib import Path
import tempfile

from skifer.capabilities import (
    AutonomyMode,
    CapabilityExecutorRegistry,
    CapabilityRegistry,
    GovernedExecutionError,
    GovernedExecutor,
    SqliteCapabilityHistoryStore,
    resolve_autonomy_mode,
)


EXAMPLE_DIR = Path(__file__).parent
EXECUTOR_CALLS: list[dict] = []


@CapabilityExecutorRegistry.register("example_create_ticket")
def create_ticket(arguments):
    EXECUTOR_CALLS.append(dict(arguments))
    return {"ticket_id": "would-have-been-created"}


def main() -> None:
    registry = CapabilityRegistry(EXAMPLE_DIR)
    definition = registry.get("support.create_ticket")

    # No environment override is supplied. The configured default is therefore
    # SHADOW, even though the capability document permits GUARDED execution.
    effective_mode = resolve_autonomy_mode(definition, configured=None)
    history_path = Path(tempfile.mkdtemp()) / "capability-history.db"
    governed = GovernedExecutor(
        registry,
        SqliteCapabilityHistoryStore(str(history_path)),
        clock=lambda: datetime.now(timezone.utc),
        scopes=["tickets:create"],
    )

    arguments = {
        "request_id": "onboarding-1",
        "description": "The example order export needs review",
    }
    result = governed.execute(
        definition,
        subject="onboarding-reader",
        arguments=arguments,
        mode=effective_mode,
    )

    print(f"Document asks for : {definition.approval.value}")
    print(f"Resolved autonomy : {effective_mode.value}")
    print(f"Recorded state    : {result.output['state']}")
    print(f"Executor calls    : {len(EXECUTOR_CALLS)}")
    print("Credential issued : no credential broker was configured")

    invalid_arguments = {**arguments, "priority": "urgent"}
    try:
        governed.execute(
            definition,
            subject="onboarding-reader",
            arguments=invalid_arguments,
            mode=AutonomyMode.SHADOW,
        )
    except GovernedExecutionError as exc:
        print(f"\nRefused request: {exc}")

    print(f"Executor calls after refusal: {len(EXECUTOR_CALLS)}")


if __name__ == "__main__":
    main()
