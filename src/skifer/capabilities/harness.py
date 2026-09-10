"""Deterministic evaluation harness for composed governed capabilities.

The harness deliberately knows nothing about capability policy.  Scenario runners
drive the real registry, preconditions, autonomy machine, credentials, and governed
executor, then return the small public observation those components produced.  The
only independent measurement here is the fake external system's append-only call
log: it is the authority for call counts and duplicate side effects.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime
import re
from typing import Any


# "error" is a harness outcome, never a governance one: a scenario whose own code
# raised has not observed the system refuse anything. Labelling that "refused" lets
# a broken scenario wear the costume of a governance decision, and a future
# scenario that legitimately expects "refused" could then pass on a crash.
_DECISIONS = frozenset({"executed", "refused", "pending", "proposed", "error"})
_REASON_CODE = re.compile(r"^[a-z][a-z0-9_]*$")


class ScenarioClockError(RuntimeError):
    """A scenario clock was exhausted or failed to advance."""


class ScenarioClockExhausted(ScenarioClockError):
    """A finite scenario clock ran out before the real flow completed."""


@dataclass(frozen=True)
class ScenarioExpectation:
    """The exact safe outcome expected from one adversarial scenario."""

    decision: str
    reason_codes: tuple[str, ...]
    external_calls: int

    def __post_init__(self) -> None:
        _validate_decision(self.decision)
        _validate_reason_codes(self.reason_codes)
        if (
            not isinstance(self.external_calls, int)
            or isinstance(self.external_calls, bool)
            or self.external_calls < 0
        ):
            raise ValueError("Scenario external_calls must be a non-negative integer.")


@dataclass(frozen=True)
class ScenarioObservation:
    """Public decision returned by a scenario's real governed execution path."""

    decision: str
    reason_codes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _validate_decision(self.decision)
        _validate_reason_codes(self.reason_codes)


@dataclass(frozen=True)
class CapabilityScenario:
    """A runnable scenario plus an independent external-system call-log probe.

    ``runner`` receives a fresh guarded clock made by ``CapabilityHarness``.
    ``external_call_log`` must expose stable side-effect identity strings from the
    fake external system itself; repeated identities are counted as duplicates.
    """

    name: str
    expected: ScenarioExpectation
    runner: Callable[[Callable[[], datetime]], ScenarioObservation]
    external_call_log: Callable[[], Sequence[str]]

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or _REASON_CODE.fullmatch(self.name) is None:
            raise ValueError("Scenario name must match ^[a-z][a-z0-9_]*$.")
        if not isinstance(self.expected, ScenarioExpectation):
            raise TypeError("Scenario expected must be a ScenarioExpectation.")
        if not callable(self.runner):
            raise TypeError("Scenario runner must be callable.")
        if not callable(self.external_call_log):
            raise TypeError("Scenario external_call_log must be callable.")


@dataclass(frozen=True)
class ScenarioResult:
    name: str
    expected: ScenarioExpectation
    observed_decision: str
    observed_reason_codes: tuple[str, ...]
    observed_external_calls: int
    passed: bool

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or _REASON_CODE.fullmatch(self.name) is None:
            raise ValueError("Scenario result name must match ^[a-z][a-z0-9_]*$.")
        if not isinstance(self.expected, ScenarioExpectation):
            raise TypeError("Scenario result expected must be a ScenarioExpectation.")
        _validate_decision(self.observed_decision)
        _validate_reason_codes(self.observed_reason_codes)
        if (
            not isinstance(self.observed_external_calls, int)
            or isinstance(self.observed_external_calls, bool)
            or self.observed_external_calls < 0
        ):
            raise ValueError("Observed external calls must be a non-negative integer.")
        if not isinstance(self.passed, bool):
            raise TypeError("Scenario result passed must be boolean.")

    def to_dict(self) -> dict[str, Any]:
        """Serialize the fixed public metric fields, never instance attributes."""
        return {
            "name": self.name,
            "expected": {
                "decision": self.expected.decision,
                "reason_codes": list(self.expected.reason_codes),
                "external_calls": self.expected.external_calls,
            },
            "observed_decision": self.observed_decision,
            "observed_reason_codes": list(self.observed_reason_codes),
            "observed_external_calls": self.observed_external_calls,
            "passed": self.passed,
        }


@dataclass(frozen=True)
class HarnessReport:
    results: tuple[ScenarioResult, ...]
    decision_precision: float
    escalation_rate: float
    duplicate_side_effects: int

    def __post_init__(self) -> None:
        if not isinstance(self.results, tuple) or not all(
            isinstance(result, ScenarioResult) for result in self.results
        ):
            raise TypeError("Harness results must be a tuple of ScenarioResult values.")
        for field_name in ("decision_precision", "escalation_rate"):
            value = getattr(self, field_name)
            if not isinstance(value, float) or not 0.0 <= value <= 1.0:
                raise ValueError(f"Harness {field_name} must be a float between 0 and 1.")
        if (
            not isinstance(self.duplicate_side_effects, int)
            or isinstance(self.duplicate_side_effects, bool)
            or self.duplicate_side_effects < 0
        ):
            raise ValueError("Harness duplicate_side_effects must be non-negative.")

    def to_dict(self) -> dict[str, Any]:
        """Serialize only deliberately published aggregate and result fields."""
        return {
            "results": [result.to_dict() for result in self.results],
            "decision_precision": self.decision_precision,
            "escalation_rate": self.escalation_rate,
            "duplicate_side_effects": self.duplicate_side_effects,
        }


class _GuardedClock:
    """Remember clock misuse even if a scenario catches the raised exception."""

    def __init__(self, clock: Callable[[], datetime]) -> None:
        if not callable(clock):
            raise TypeError("Scenario clock factory must return a callable.")
        self._clock = clock
        self._last: datetime | None = None
        self.error: ScenarioClockError | None = None

    def __call__(self) -> datetime:
        try:
            current = self._clock()
        except (StopIteration, IndexError):
            self.error = ScenarioClockExhausted(
                "Scenario clock exhausted before execution completed."
            )
            raise self.error from None
        if not isinstance(current, datetime):
            self.error = ScenarioClockError("Scenario clock must return datetime values.")
            raise self.error
        if self._last is not None and current <= self._last:
            self.error = ScenarioClockError(
                "Scenario clock must advance strictly on every reading."
            )
            raise self.error
        self._last = current
        return current


class CapabilityHarness:
    """Run isolated scenarios and compute safety metrics deterministically."""

    def __init__(
        self,
        *,
        clock_factory: Callable[[str], Callable[[], datetime]],
    ) -> None:
        if not callable(clock_factory):
            raise TypeError("Capability harness clock_factory must be callable.")
        self._clock_factory = clock_factory

    def run(self, scenarios: Sequence[CapabilityScenario]) -> HarnessReport:
        if not isinstance(scenarios, Sequence) or isinstance(scenarios, (str, bytes)):
            raise TypeError("Harness scenarios must be a sequence.")
        if not all(isinstance(scenario, CapabilityScenario) for scenario in scenarios):
            raise TypeError("Harness scenarios must contain CapabilityScenario values.")
        names = [scenario.name for scenario in scenarios]
        if len(names) != len(set(names)):
            raise ValueError("Harness scenario names must be unique.")

        results: list[ScenarioResult] = []
        duplicate_side_effects = 0
        for scenario in scenarios:
            before = _read_call_log(scenario.external_call_log)
            clock = _GuardedClock(self._clock_factory(scenario.name))
            try:
                observation = scenario.runner(clock)
                if not isinstance(observation, ScenarioObservation):
                    raise TypeError("Scenario runner must return ScenarioObservation.")
            except ScenarioClockError:
                raise
            except Exception:
                observation = ScenarioObservation(
                    decision="error", reason_codes=("scenario_error",)
                )
            if clock.error is not None:
                raise clock.error

            after = _read_call_log(scenario.external_call_log)
            if len(after) < len(before) or after[: len(before)] != before:
                raise ValueError("External-system call log must be append-only.")
            scenario_calls = after[len(before) :]
            counts = Counter(scenario_calls)
            duplicate_side_effects += sum(count - 1 for count in counts.values())
            observed_external_calls = len(scenario_calls)
            passed = (
                observation.decision == scenario.expected.decision
                and observation.reason_codes == scenario.expected.reason_codes
                and observed_external_calls == scenario.expected.external_calls
            )
            results.append(
                ScenarioResult(
                    name=scenario.name,
                    expected=scenario.expected,
                    observed_decision=observation.decision,
                    observed_reason_codes=observation.reason_codes,
                    observed_external_calls=observed_external_calls,
                    passed=passed,
                )
            )

        total = len(results)
        decision_precision = (
            sum(
                result.observed_decision == result.expected.decision
                for result in results
            )
            / total
            if total
            else 0.0
        )
        escalation_rate = (
            sum(result.observed_decision == "pending" for result in results) / total
            if total
            else 0.0
        )
        return HarnessReport(
            results=tuple(results),
            decision_precision=float(decision_precision),
            escalation_rate=float(escalation_rate),
            duplicate_side_effects=duplicate_side_effects,
        )


def _read_call_log(probe: Callable[[], Sequence[str]]) -> tuple[str, ...]:
    log = probe()
    if not isinstance(log, Sequence) or isinstance(log, (str, bytes)):
        raise TypeError("External-system call log must be a sequence of identity strings.")
    if not all(isinstance(identity, str) and identity for identity in log):
        raise ValueError("External-system call identities must be non-empty strings.")
    return tuple(log)


def _validate_decision(decision: str) -> None:
    if decision not in _DECISIONS:
        raise ValueError(f"Scenario decision must be one of {sorted(_DECISIONS)}.")


def _validate_reason_codes(reason_codes: tuple[str, ...]) -> None:
    if not isinstance(reason_codes, tuple):
        raise TypeError("Scenario reason_codes must be a tuple.")
    if not all(
        isinstance(code, str) and _REASON_CODE.fullmatch(code) is not None
        for code in reason_codes
    ):
        raise ValueError("Scenario reason codes must match ^[a-z][a-z0-9_]*$.")
