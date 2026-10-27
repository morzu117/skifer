"""A governed capability in shadow mode: a proposal is recorded, nothing is executed.

    python examples/04_governed_capability/run.py
"""

from datetime import datetime, timezone
from pathlib import Path
import tempfile

from skifer.capabilities import (
    CapabilityExecutorRegistry,
    CapabilityRegistry,
    GovernedExecutionError,
    GovernedExecutor,
    SqliteCapabilityHistoryStore,
    resolve_autonomy_mode,
)

calls = []


@CapabilityExecutorRegistry.register("example_create_ticket")
def create_ticket(arguments):
    calls.append(arguments)
    return {"ticket_id": "would-have-been-created"}


registry = CapabilityRegistry(Path(__file__).parent)
definition = registry.get("support.create_ticket")
# Effective mode = the stricter of the document and the environment (default: shadow).
mode = resolve_autonomy_mode(definition, configured=None)

governed = GovernedExecutor(
    registry,
    SqliteCapabilityHistoryStore(str(Path(tempfile.mkdtemp()) / "history.db")),
    clock=lambda: datetime.now(timezone.utc),
    scopes=["tickets:create"],
)
arguments = {"request_id": "onboarding-1", "description": "The example order export needs review"}
result = governed.execute(definition, subject="onboarding-reader", arguments=arguments, mode=mode)

print(f"Document asks for : {definition.approval.value}")
print(f"Resolved autonomy : {mode.value}")
print(f"Recorded state    : {result.output['state']}")
print(f"Executor calls    : {len(calls)}")
print("Credential issued : no credential broker was configured")

# The input schema is closed: an undeclared argument is refused before execution.
try:
    governed.execute(
        definition,
        subject="onboarding-reader",
        arguments={**arguments, "priority": "urgent"},
        mode=mode,
    )
except GovernedExecutionError as exc:
    print(f"\nRefused request: {exc}")
print(f"Executor calls after refusal: {len(calls)}")
