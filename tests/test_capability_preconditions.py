"""Deterministic and bounded governed-capability preconditions."""

from __future__ import annotations

from datetime import datetime, timezone
import os
from pathlib import Path
import subprocess
import sys
import textwrap
from typing import Any

import pytest

from skifer.capabilities import (
    ActingAs,
    ApprovalMode,
    CapabilityDefinition,
    CapabilityMode,
    PreconditionDecision,
    PreconditionError,
    PreconditionEvaluator,
    PreconditionOutcome,
    PreconditionRegistry,
    PreconditionReport,
    Reversibility,
)
from skifer.capabilities.preconditions import (
    _MAX_RULE_NAME_LENGTH,
    _MAX_STATE_BYTES,
    _MAX_STATE_DEPTH,
    _MAX_STATE_NODES,
)


NOW = datetime(2026, 9, 8, 12, 0, tzinfo=timezone.utc)
PLACEHOLDER_HASH = "sha256:v1:" + "0" * 64


def _definition(*rules: str) -> CapabilityDefinition:
    return CapabilityDefinition(
        id="inventory.lookup_record",
        version="1.2.0",
        owner="inventory-platform",
        description="Look up a fake inventory record",
        mode=CapabilityMode.READ,
        executor="test_inventory_lookup",
        acting_as=ActingAs.SERVICE,
        required_scopes=("inventory:read",),
        input_schema={
            "type": "object",
            "additionalProperties": False,
            "required": [],
            "properties": {},
        },
        preconditions=rules,
        reversibility=Reversibility.REVERSIBLE,
        compensation=None,
        approval=ApprovalMode.GUARDED,
        idempotency_key=None,
        policy_uri="policies/inventory-read-v1.md",
        policy_hash="sha256:" + "b" * 64,
    )


def _outcome(
    rule: str,
    decision: PreconditionDecision = PreconditionDecision.ALLOW,
    reason_code: str = "allowed",
) -> PreconditionOutcome:
    return PreconditionOutcome(
        rule=rule,
        rule_version="1.0.0",
        decision=decision,
        reason_code=reason_code,
        observed_state_hash=PLACEHOLDER_HASH,
        observed_at=NOW,
    )


def test_registry_registers_lists_and_resolves_plain_rule() -> None:
    name = "test_precondition_registry_list"

    @PreconditionRegistry.register(name)
    def rule(arguments, state):
        return _outcome(name)

    assert PreconditionRegistry.get(name) is rule
    assert name in PreconditionRegistry.list_rules()


@pytest.mark.parametrize(
    "name",
    [
        "Uppercase",
        "pkg.rule",
        "module:rule",
        "with-dash",
        "_private",
        "1first",
        "a" * (_MAX_RULE_NAME_LENGTH + 1),
    ],
)
def test_registry_refuses_non_plain_rule_names(name: str) -> None:
    with pytest.raises(PreconditionError, match="must match"):
        PreconditionRegistry.register(name)


def test_registry_refuses_duplicate_and_non_callable_rules() -> None:
    name = "test_precondition_duplicate"

    @PreconditionRegistry.register(name)
    def first(arguments, state):
        return _outcome(name)

    with pytest.raises(PreconditionError, match="already registered"):
        PreconditionRegistry.register(name)(lambda arguments, state: _outcome(name))
    with pytest.raises(PreconditionError, match="callable"):
        PreconditionRegistry.register("test_precondition_not_callable")(None)


def test_precondition_module_has_no_dynamic_resolution_escape_hatch() -> None:
    source = (
        Path(__file__).parents[1]
        / "src"
        / "skifer"
        / "capabilities"
        / "preconditions.py"
    ).read_text(encoding="utf-8")
    forbidden = ("importlib", "__import__", "eval(", "exec(", "getattr(__")
    assert all(fragment not in source for fragment in forbidden)


@pytest.mark.parametrize(
    "returned",
    ["ALLOW", {"decision": "ALLOW"}, "The model says this should be allowed."],
)
def test_untyped_rule_returns_become_unknown(returned: Any) -> None:
    name = f"test_invalid_return_{len(PreconditionRegistry.list_rules())}"
    PreconditionRegistry.register(name)(lambda arguments, state: returned)

    report = PreconditionEvaluator(clock=lambda: NOW).evaluate(
        _definition(name), {}, {"record": "open"}
    )

    assert report.decision is PreconditionDecision.UNKNOWN
    assert report.requires_escalation is True
    assert report.outcomes[0].reason_code == "invalid_rule_return"


def test_oversized_rule_return_is_stopped_and_becomes_unknown() -> None:
    name = "test_oversized_rule_return"
    outcome = _outcome(name)
    object.__setattr__(outcome, "rule_version", "x" * 100_000)
    PreconditionRegistry.register(name)(lambda arguments, state: outcome)

    report = PreconditionEvaluator(clock=lambda: NOW).evaluate(_definition(name), {}, {})

    assert report.decision is PreconditionDecision.UNKNOWN
    assert report.outcomes[0].reason_code == "invalid_rule_return"
    assert len(str(report.to_dict())) < 1024


def test_rule_exception_becomes_unknown_without_its_message() -> None:
    name = "test_precondition_exception"
    secret = "offending customer value"

    @PreconditionRegistry.register(name)
    def rule(arguments, state):
        raise TimeoutError(secret)

    report = PreconditionEvaluator(clock=lambda: NOW).evaluate(_definition(name), {}, {})

    assert report.outcomes[0].reason_code == "rule_error_timeouterror"
    assert secret not in str(report.to_dict())


def test_aggregation_deny_unknown_allow_and_empty_contract() -> None:
    decisions = {
        "test_aggregate_allow": PreconditionDecision.ALLOW,
        "test_aggregate_unknown": PreconditionDecision.UNKNOWN,
        "test_aggregate_deny": PreconditionDecision.DENY,
    }
    for name, decision in decisions.items():
        PreconditionRegistry.register(name)(
            lambda arguments, state, name=name, decision=decision: _outcome(
                name, decision, decision.value.lower()
            )
        )
    evaluator = PreconditionEvaluator(clock=lambda: NOW)

    denied = evaluator.evaluate(_definition(*decisions), {}, {})
    allowed = evaluator.evaluate(_definition("test_aggregate_allow"), {}, {})
    unknown = evaluator.evaluate(
        _definition("test_aggregate_allow", "test_aggregate_unknown"), {}, {}
    )
    empty = evaluator.evaluate(_definition(), {}, {})

    assert denied.decision is PreconditionDecision.DENY
    assert denied.requires_escalation is False
    assert allowed.decision is PreconditionDecision.ALLOW
    assert allowed.requires_escalation is False
    assert unknown.decision is PreconditionDecision.UNKNOWN
    assert unknown.requires_escalation is True
    assert empty.decision is PreconditionDecision.ALLOW
    assert empty.outcomes == ()
    assert empty.requires_escalation is False


def test_identical_state_has_identical_canonical_hash() -> None:
    name = "test_identical_state_hash"
    PreconditionRegistry.register(name)(lambda arguments, state: _outcome(name))
    evaluator = PreconditionEvaluator(clock=lambda: NOW)

    first = evaluator.evaluate(_definition(name), {}, {"b": 2, "a": [True, None]})
    second = evaluator.evaluate(_definition(name), {}, {"a": [True, None], "b": 2})

    assert first.outcomes[0].observed_state_hash == second.outcomes[0].observed_state_hash
    assert first.outcomes[0].observed_state_hash.startswith("sha256:v1:")


def test_state_hash_is_stable_across_python_hash_seeds() -> None:
    script = textwrap.dedent(
        """
        from datetime import datetime, timezone
        from skifer.capabilities import (
            ActingAs, ApprovalMode, CapabilityDefinition, CapabilityMode,
            PreconditionDecision, PreconditionEvaluator, PreconditionOutcome,
            PreconditionRegistry, Reversibility,
        )
        now = datetime(2026, 9, 8, tzinfo=timezone.utc)
        name = "subprocess_state_rule"
        @PreconditionRegistry.register(name)
        def rule(arguments, state):
            return PreconditionOutcome(
                name, "1", PreconditionDecision.ALLOW, "allowed",
                "sha256:v1:" + "0" * 64, now,
            )
        definition = CapabilityDefinition(
            id="inventory.lookup_record", version="1.0.0", owner="owner",
            description="description", mode=CapabilityMode.READ, executor="executor",
            acting_as=ActingAs.SERVICE, required_scopes=(),
            input_schema={"type": "object", "additionalProperties": False,
                          "required": [], "properties": {}},
            preconditions=(name,), reversibility=Reversibility.REVERSIBLE,
            compensation=None, approval=ApprovalMode.GUARDED, idempotency_key=None,
            policy_uri="policy.md", policy_hash="sha256:" + "a" * 64,
        )
        state = {key: key for key in {"gamma", "alpha", "beta"}}
        report = PreconditionEvaluator(clock=lambda: now).evaluate(definition, {}, state)
        print(report.outcomes[0].observed_state_hash)
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


def test_oversized_state_stops_before_rule_execution() -> None:
    name = "test_oversized_state"
    calls = []

    @PreconditionRegistry.register(name)
    def rule(arguments, state):
        calls.append(True)
        return _outcome(name)

    report = PreconditionEvaluator(clock=lambda: NOW).evaluate(
        _definition(name), {}, {"payload": "x" * (_MAX_STATE_BYTES + 1)}
    )

    assert calls == []
    assert report.decision is PreconditionDecision.UNKNOWN
    assert report.outcomes[0].reason_code == "state_snapshot_too_large"


def _too_deep_state() -> dict[str, Any]:
    nested: Any = 1
    for _ in range(_MAX_STATE_DEPTH + 1):
        nested = [nested]
    return {"nested": nested}


@pytest.mark.parametrize(
    "state",
    [
        {f"key_{index}": index for index in range(_MAX_STATE_NODES + 1)},
        _too_deep_state(),
    ],
)
def test_state_structural_bounds_stop_before_rule_execution(state: dict[str, Any]) -> None:
    name = f"test_structural_state_bound_{len(PreconditionRegistry.list_rules())}"
    calls = []
    PreconditionRegistry.register(name)(
        lambda arguments, observed: calls.append(True) or _outcome(name)
    )

    report = PreconditionEvaluator(clock=lambda: NOW).evaluate(
        _definition(name), {}, state
    )

    assert calls == []
    assert report.decision is PreconditionDecision.UNKNOWN
    assert report.outcomes[0].reason_code == "state_snapshot_too_large"


def test_assert_unchanged_refuses_state_change() -> None:
    name = "test_state_recheck"
    PreconditionRegistry.register(name)(lambda arguments, state: _outcome(name))
    evaluator = PreconditionEvaluator(clock=lambda: NOW)
    definition = _definition(name)
    report = evaluator.evaluate(definition, {}, {"revision": 1})

    with pytest.raises(PreconditionError, match="state changed"):
        evaluator.assert_unchanged(definition, {}, {"revision": 2}, report)


def test_assert_unchanged_refuses_when_rule_is_no_longer_allow() -> None:
    name = "test_recheck_no_longer_allow"

    @PreconditionRegistry.register(name)
    def rule(arguments, state):
        decision = (
            PreconditionDecision.ALLOW
            if state["available"]
            else PreconditionDecision.DENY
        )
        return _outcome(name, decision, "available" if state["available"] else "unavailable")

    evaluator = PreconditionEvaluator(clock=lambda: NOW)
    definition = _definition(name)
    report = evaluator.evaluate(definition, {}, {"available": True})

    with pytest.raises(PreconditionError, match="no longer ALLOW: DENY"):
        evaluator.assert_unchanged(definition, {}, {"available": False}, report)


def test_assert_unchanged_refuses_a_missing_rule_outcome() -> None:
    first = "test_missing_rule_first"
    second = "test_missing_rule_second"
    for name in (first, second):
        PreconditionRegistry.register(name)(
            lambda arguments, state, name=name: _outcome(name)
        )
    evaluator = PreconditionEvaluator(clock=lambda: NOW)
    definition = _definition(first, second)
    complete = evaluator.evaluate(definition, {}, {})
    incomplete = PreconditionReport(
        capability_id=complete.capability_id,
        capability_version=complete.capability_version,
        decision=PreconditionDecision.ALLOW,
        outcomes=complete.outcomes[:1],
        evaluated_at=complete.evaluated_at,
        requires_escalation=False,
    )

    with pytest.raises(PreconditionError, match="state changed"):
        evaluator.assert_unchanged(definition, {}, {}, incomplete)


def test_max_rules_is_an_enforced_stop_before_any_rule_runs() -> None:
    calls = []
    names = ("test_rule_limit_one", "test_rule_limit_two")
    for name in names:
        PreconditionRegistry.register(name)(
            lambda arguments, state, name=name: calls.append(name) or _outcome(name)
        )

    with pytest.raises(PreconditionError, match="enforced maximum is 1"):
        PreconditionEvaluator(clock=lambda: NOW, max_rules=1).evaluate(
            _definition(*names), {}, {}
        )
    assert calls == []


def test_outcome_and_report_serialization_are_field_allowlists() -> None:
    outcome = _outcome("test_allowlist_rule")
    report = PreconditionReport(
        capability_id="inventory.lookup_record",
        capability_version="1.2.0",
        decision=PreconditionDecision.ALLOW,
        outcomes=(outcome,),
        evaluated_at=NOW,
        requires_escalation=False,
    )
    object.__setattr__(outcome, "future_private_field", "must not leak")
    object.__setattr__(report, "future_private_field", "must not leak")

    assert "future_private_field" not in outcome.to_dict()
    assert "future_private_field" not in report.to_dict()
    assert "future_private_field" not in report.to_dict()["outcomes"][0]


def test_outcome_refuses_unbounded_or_open_grammar_fields() -> None:
    with pytest.raises(ValueError, match="reason_code"):
        _outcome("test_bad_reason", reason_code="natural language is not a code")
    with pytest.raises(ValueError, match="rule_version"):
        PreconditionOutcome(
            rule="test_bad_version",
            rule_version="x" * 65,
            decision=PreconditionDecision.ALLOW,
            reason_code="allowed",
            observed_state_hash=PLACEHOLDER_HASH,
            observed_at=NOW,
        )
    with pytest.raises(ValueError, match="reason_code"):
        _outcome("test_long_reason", reason_code="a" * 65)
