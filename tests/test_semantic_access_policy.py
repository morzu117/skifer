from datetime import datetime, timezone

from skifer.semantic.access_policy import ConsumerContext, CertificationDecision, evaluate
from skifer.semantic.dependencies import SemanticDependency, resolve_dependencies


def test_policy_modes_are_pure():
    now = datetime.now(timezone.utc)
    context = ConsumerContext("a", "agent_read")

    assert evaluate([None],context,"off",now).decision is CertificationDecision.ALLOW
    assert evaluate([None],context,"enforce",now).decision is CertificationDecision.DENY


def test_resolve_dependencies_returns_single_table_dependency():
    assert resolve_dependencies({"table": "gold.orders"}) == (
        SemanticDependency("gold.orders", None, None),
    )


def test_resolve_dependencies_returns_empty_tuple_without_table():
    assert resolve_dependencies({}) == ()


def test_resolve_dependencies_is_stable_for_the_same_model():
    model = {"table": "gold.orders"}

    assert resolve_dependencies(model) == resolve_dependencies(model)
