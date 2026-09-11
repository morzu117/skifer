from datetime import datetime, timezone

from skifer.semantic.access_policy import (
    CertificationDecision,
    ConsumerContext,
    LifecycleReason,
    evaluate,
    evaluate_lifecycle,
)
from skifer.semantic.dependencies import SemanticDependency, resolve_dependencies


def test_policy_modes_are_pure():
    now = datetime.now(timezone.utc)
    context = ConsumerContext("a", "agent_read")

    assert evaluate([None],context,"off",now).decision is CertificationDecision.ALLOW
    assert evaluate([None],context,"enforce",now).decision is CertificationDecision.DENY


def test_gate_warns_on_deprecated():
    result = evaluate_lifecycle(
        status="deprecated",
        effective_from=None,
        effective_until=None,
        now=datetime(2026, 6, 1, tzinfo=timezone.utc),
    )

    assert result.decision is CertificationDecision.WARN
    assert LifecycleReason.DEPRECATED.value in result.reasons


def test_gate_denies_before_effective_from():
    result = evaluate_lifecycle(
        status="active",
        effective_from="2026-07-01",
        effective_until=None,
        now=datetime(2026, 6, 1, tzinfo=timezone.utc),
    )

    assert result.decision is CertificationDecision.DENY
    assert result.reasons == (LifecycleReason.NOT_YET_EFFECTIVE.value,)


def test_gate_denies_after_effective_until():
    result = evaluate_lifecycle(
        status="active",
        effective_from=None,
        effective_until="2026-05-31",
        now=datetime(2026, 6, 1, tzinfo=timezone.utc),
    )

    assert result.decision is CertificationDecision.DENY
    assert result.reasons == (LifecycleReason.EXPIRED_WINDOW.value,)


def test_gate_allows_active_within_window():
    result = evaluate_lifecycle(
        status="active",
        effective_from="2026-01-01",
        effective_until="2026-12-31",
        now=datetime(2026, 6, 1, tzinfo=timezone.utc),
    )

    assert result.decision is CertificationDecision.ALLOW
    assert result.reasons == ()


def test_resolve_dependencies_returns_single_table_dependency():
    assert resolve_dependencies({"table": "gold.orders"}) == (
        SemanticDependency("gold.orders", None, None),
    )


def test_resolve_dependencies_returns_empty_tuple_without_table():
    assert resolve_dependencies({}) == ()


def test_resolve_dependencies_is_stable_for_the_same_model():
    model = {"table": "gold.orders"}

    assert resolve_dependencies(model) == resolve_dependencies(model)
