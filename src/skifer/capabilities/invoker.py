"""Fail-closed invocation boundary for read-only capabilities."""

from __future__ import annotations

import math
from typing import Any, Callable, Iterable, Mapping

from .autonomy import AutonomyMode
from .credentials import CredentialBroker, CredentialError
from .executors import (
    CapabilityExecutorError,
    CapabilityExecutorRegistry,
    CapabilityResult,
    _copy_json_safe,
)
from .models import CapabilityMode
from .preconditions import (
    PreconditionDecision,
    PreconditionError,
    PreconditionEvaluator,
)
from .registry import CapabilityRegistry


# Arguments arrive from the caller, so the amount of work they can buy has to be
# bounded too. Reporting a breached bound while still walking every element turns a
# declared limit into unbounded work, and turns the error list itself into a payload:
# 200k surplus items produced 200k messages and an 8 MB exception string.
_MAX_ARGUMENT_ERRORS = 32


def validate_arguments(schema: Mapping[str, Any], arguments: Mapping[str, Any]) -> list[str]:
    """Validate JSON-shaped arguments against the already-validated closed schema."""
    if not isinstance(arguments, Mapping):
        return ["Arguments must be a mapping."]
    errors: list[str] = []
    _validate_value(schema, arguments, "arguments", errors)
    if len(errors) >= _MAX_ARGUMENT_ERRORS:
        errors.append(
            f"Argument validation stopped after {_MAX_ARGUMENT_ERRORS} errors."
        )
    return errors


def _validate_value(
    schema: Mapping[str, Any],
    value: Any,
    path: str,
    errors: list[str],
) -> None:
    if len(errors) >= _MAX_ARGUMENT_ERRORS:
        return
    schema_type = schema["type"]
    if not _matches_type(value, schema_type):
        errors.append(f"Field '{path}' must have type '{schema_type}'.")
        return

    if "enum" in schema and not any(
        type(value) is type(candidate) and value == candidate for candidate in schema["enum"]
    ):
        errors.append(f"Field '{path}' must be one of the declared enum values.")

    if schema_type == "object":
        properties = schema["properties"]
        for key in sorted(set(value) - set(properties), key=str):
            if len(errors) >= _MAX_ARGUMENT_ERRORS:
                return
            errors.append(f"Field '{path}.{key}' is not allowed.")
        for name in schema["required"]:
            if len(errors) >= _MAX_ARGUMENT_ERRORS:
                return
            if name not in value:
                errors.append(f"Field '{path}.{name}' is required.")
        for name, child_schema in properties.items():
            if name in value:
                _validate_value(child_schema, value[name], f"{path}.{name}", errors)
    elif schema_type == "array":
        if len(value) > schema["maxItems"]:
            # Stop here. The array is already refused, so descending into it buys
            # the caller work proportional to what they sent rather than to the
            # bound the capability declared.
            errors.append(
                f"Field '{path}' must contain at most {schema['maxItems']} items."
            )
            return
        for index, item in enumerate(value):
            _validate_value(schema["items"], item, f"{path}[{index}]", errors)
    elif schema_type == "string" and len(value) > schema["maxLength"]:
        errors.append(
            f"Field '{path}' must contain at most {schema['maxLength']} characters."
        )
    elif schema_type in {"integer", "number"}:
        if value < schema["minimum"]:
            errors.append(f"Field '{path}' must be at least {schema['minimum']}.")
        if value > schema["maximum"]:
            errors.append(f"Field '{path}' must be at most {schema['maximum']}.")


def _matches_type(value: Any, schema_type: str) -> bool:
    if schema_type == "object":
        return isinstance(value, Mapping)
    if schema_type == "array":
        return isinstance(value, list)
    if schema_type == "string":
        return isinstance(value, str)
    if schema_type == "boolean":
        return isinstance(value, bool)
    if schema_type == "integer":
        return type(value) is int
    if schema_type == "number":
        return (
            not isinstance(value, bool)
            and isinstance(value, (int, float))
            and math.isfinite(value)
        )
    return False


class CapabilityInvoker:
    """Resolve, authorize, validate and execute one read-only capability."""

    def __init__(
        self,
        registry: CapabilityRegistry,
        *,
        scopes: Iterable[str] = (),
        precondition_evaluator: PreconditionEvaluator | None = None,
        state_reader: Callable[[], Mapping[str, Any]] | None = None,
        credential_broker: CredentialBroker | None = None,
        credential_audience: str | None = None,
        autonomy_mode: AutonomyMode | None = None,
        credential_subject: str | None = None,
    ) -> None:
        self._registry = registry
        self._scopes = frozenset(scopes)
        self._precondition_evaluator = precondition_evaluator
        self._state_reader = state_reader
        self._credential_broker = credential_broker
        self._credential_audience = credential_audience
        self._autonomy_mode = autonomy_mode
        self._credential_subject = credential_subject

    def invoke(
        self,
        capability_id: str,
        arguments: Mapping[str, Any],
    ) -> CapabilityResult:
        """Invoke a selected capability through the ordered safety checks."""
        definition = self._registry.get(capability_id)

        if definition.mode is CapabilityMode.WRITE:
            raise CapabilityExecutorError(
                "Write capability execution requires GovernedExecutor, an autonomy mode, "
                "and an append-only history store."
            )

        missing_scopes = sorted(set(definition.required_scopes) - self._scopes)
        if missing_scopes:
            raise CapabilityExecutorError(
                f"Capability '{capability_id}' requires missing scopes: "
                f"{', '.join(missing_scopes)}."
            )

        argument_errors = validate_arguments(definition.input_schema, arguments)
        if argument_errors:
            raise CapabilityExecutorError(
                "Invalid capability arguments: " + "; ".join(argument_errors)
            )
        copied_arguments = _copy_json_safe(dict(arguments), path="arguments")

        precondition_report = None
        if definition.preconditions:
            if self._precondition_evaluator is None or self._state_reader is None:
                raise PreconditionError(
                    f"Capability '{capability_id}' declares preconditions but no "
                    "precondition evaluator and state reader are configured."
                )
            initial_state = self._read_state()
            precondition_report = self._precondition_evaluator.evaluate(
                definition,
                _copy_json_safe(copied_arguments, path="arguments"),
                initial_state,
            )
            if precondition_report.decision is not PreconditionDecision.ALLOW:
                reason_codes = sorted(
                    {
                        outcome.reason_code
                        for outcome in precondition_report.outcomes
                        if outcome.decision is not PreconditionDecision.ALLOW
                    }
                )
                raise PreconditionError(
                    "Capability preconditions refused with decision "
                    f"{precondition_report.decision.value}: {', '.join(reason_codes)}."
                )

        try:
            executor = CapabilityExecutorRegistry.get(definition.executor)
        except Exception as exc:
            return CapabilityResult(
                capability_id=definition.id,
                capability_version=definition.version,
                status="failed",
                output={},
                error_type=type(exc).__name__,
            )

        if precondition_report is not None:
            assert self._precondition_evaluator is not None
            self._precondition_evaluator.assert_unchanged(
                definition,
                _copy_json_safe(copied_arguments, path="arguments"),
                self._read_state(),
                precondition_report,
            )

        lease = None
        needs_credential = CapabilityExecutorRegistry.needs_credential(definition.executor)
        if needs_credential:
            if not definition.required_scopes:
                raise CredentialError(
                    "Credential executor requires declared capability scopes."
                )
            if self._credential_broker is None:
                raise CredentialError("Credential executor requires a configured broker.")
            if not self._credential_subject:
                raise CredentialError("Credential executor requires a configured subject.")
            if not self._credential_audience:
                raise CredentialError("Credential executor requires a configured audience.")
            lease = self._credential_broker.issue_for(
                definition,
                subject=self._credential_subject,
                audience=self._credential_audience,
                ttl_seconds=self._credential_broker.max_ttl_seconds,
                mode=self._autonomy_mode,
            )
            self._credential_broker.assert_valid(lease)

        try:
            if needs_credential:
                raw_output = executor(copied_arguments, lease)
            else:
                raw_output = executor(copied_arguments)
            if not isinstance(raw_output, Mapping):
                raise TypeError("Capability executor output must be a mapping.")
            output = _copy_json_safe(raw_output)
            return CapabilityResult(
                capability_id=definition.id,
                capability_version=definition.version,
                status="succeeded",
                output=output,
                error_type=None,
            )
        except Exception as exc:
            return CapabilityResult(
                capability_id=definition.id,
                capability_version=definition.version,
                status="failed",
                output={},
                error_type=type(exc).__name__,
            )
        finally:
            lease = None

    def _read_state(self) -> Mapping[str, Any]:
        """Read injected live state without exposing provider exception messages."""
        assert self._state_reader is not None
        try:
            state = self._state_reader()
        except Exception as exc:
            raise PreconditionError(
                f"Capability state reader failed with {type(exc).__name__}."
            ) from exc
        if not isinstance(state, Mapping):
            raise PreconditionError("Capability state reader must return a mapping.")
        return state
