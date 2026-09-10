"""Fail-closed structural validation for governed capability declarations."""

from __future__ import annotations

from dataclasses import dataclass, field
import math
import re
from typing import Any, Mapping
import unicodedata

from .models import (
    ActingAs,
    ApprovalMode,
    CapabilityDefinition,
    CapabilityMode,
    Reversibility,
)


_CAPABILITY_ID = re.compile(r"^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+$")
_SEMVER = re.compile(r"^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$")
_PLAIN_NAME = re.compile(r"^[a-z][a-z0-9_]*$")
_SCOPE = re.compile(r"^[a-z][a-z0-9_]*(:[a-z][a-z0-9_]*)+$")
_POLICY_HASH = re.compile(r"^sha256:[0-9a-f]{64}$")
_SECRET_KEY_PARTS = (
    "token",
    "secret",
    "password",
    "passwd",
    "credential",
    "api_key",
    "apikey",
    "private_key",
)
_TOP_LEVEL_KEYS = {
    "id",
    "version",
    "owner",
    "description",
    "mode",
    "executor",
    "acting_as",
    "required_scopes",
    "input_schema",
    "preconditions",
    "reversibility",
    "compensation",
    "approval",
    "idempotency_key",
    "provenance",
}
_REQUIRED_TOP_LEVEL_KEYS = _TOP_LEVEL_KEYS - {"compensation", "idempotency_key"}
_SUMMARY_KEYS = {
    "id",
    "version",
    "owner",
    "description",
    "mode",
    "approval",
    "reversibility",
}
_SCHEMA_TYPES = {"string", "integer", "number", "boolean", "object", "array"}
_SCHEMA_KEYWORDS = {
    "type",
    "additionalProperties",
    "required",
    "properties",
    "maxLength",
    "maxItems",
    "items",
    "minimum",
    "maximum",
    "enum",
}
_SCHEMA_KEYWORDS_BY_TYPE = {
    "string": {"type", "maxLength", "enum"},
    "integer": {"type", "minimum", "maximum", "enum"},
    "number": {"type", "minimum", "maximum", "enum"},
    "boolean": {"type", "enum"},
    "object": {"type", "additionalProperties", "required", "properties"},
    "array": {"type", "maxItems", "items"},
}
# Nesting is a size, and section 8 of the plan requires sizes to be bounded. Without
# an explicit ceiling the two recursive walks below blow the interpreter stack at a
# nesting depth of a few hundred — a handful of kilobytes of YAML — and raise
# RecursionError instead of returning a verdict. A document that crashes the
# validator is a document that was never validated, so fail-closed requires the
# refusal to be an answer rather than a crash.
_MAX_SCHEMA_DEPTH = 8
_MAX_DOCUMENT_DEPTH = 16

_FORBIDDEN_SCHEMA_KEYWORDS = {
    "$ref",
    "allOf",
    "anyOf",
    "oneOf",
    "not",
    "patternProperties",
    "dependencies",
    "unevaluatedProperties",
}


@dataclass
class ValidationResult:
    """Collected capability validation errors without raising for bad content."""

    ok: bool = True
    errors: list[str] = field(default_factory=list)

    def add_error(self, message: str) -> None:
        self.errors.append(message)
        self.ok = False


class CapabilityValidationError(ValueError):
    """A capability document failed closed validation."""

    def __init__(self, errors: list[str] | tuple[str, ...]) -> None:
        self.errors = tuple(errors)
        super().__init__("Invalid capability definition: " + "; ".join(self.errors))


class CapabilityValidator:
    """Validate one complete capability mapping without interpreting policy text.

    The plan example uses a shortened provenance placeholder. Runtime definitions
    deliberately require ``sha256:<64 lowercase hex>`` so provenance can later
    invalidate a review deterministically.
    """

    def validate(self, payload: Mapping[str, Any]) -> ValidationResult:
        """Return all structural errors; only a wrong argument type raises."""
        if not isinstance(payload, Mapping):
            raise TypeError("capability payload must be a mapping.")

        result = ValidationResult()
        self._check_secret_keys(payload, "capability", result, set())
        self._check_top_level(payload, result)
        self._check_identity(payload, result)
        self._check_enums(payload, result)
        self._check_executor(payload, result)
        self._check_scopes(payload, result)
        self._check_preconditions(payload, result)
        self._check_input_schema(payload, result)
        self._check_idempotency(payload, result)
        self._check_compensation(payload, result)
        self._check_irreversible_approval(payload, result)
        self._check_provenance(payload, result)
        return result

    def validate_summary(self, payload: Mapping[str, Any]) -> ValidationResult:
        """Validate the closed catalog summary shape without loading a definition."""
        if not isinstance(payload, Mapping):
            raise TypeError("capability summary payload must be a mapping.")
        result = ValidationResult()
        self._check_secret_keys(payload, "capability_summary", result, set())
        for key in sorted(set(payload) - _SUMMARY_KEYS, key=str):
            result.add_error(f"Field '{key}' is not allowed in a capability summary.")
        for key in sorted(_SUMMARY_KEYS - set(payload)):
            result.add_error(f"Field '{key}' is required in a capability summary.")
        self._check_identity(payload, result)
        choices = {
            "mode": {item.value for item in CapabilityMode},
            "reversibility": {item.value for item in Reversibility},
            "approval": {item.value for item in ApprovalMode},
        }
        for field_name, allowed in choices.items():
            value = payload.get(field_name)
            if not isinstance(value, str) or value not in allowed:
                result.add_error(
                    f"Field '{field_name}' must be exactly one of {sorted(allowed)}."
                )
        return result

    @staticmethod
    def _check_top_level(payload: Mapping[str, Any], result: ValidationResult) -> None:
        for key in sorted(set(payload) - _TOP_LEVEL_KEYS, key=str):
            result.add_error(f"Field '{key}' is not allowed at capability top level.")
        for key in sorted(_REQUIRED_TOP_LEVEL_KEYS - set(payload)):
            result.add_error(f"Field '{key}' is required.")

    @staticmethod
    def _check_identity(payload: Mapping[str, Any], result: ValidationResult) -> None:
        capability_id = payload.get("id")
        if (
            not isinstance(capability_id, str)
            or len(capability_id) > 128
            or _CAPABILITY_ID.fullmatch(capability_id) is None
        ):
            result.add_error(
                "Field 'id' must be a lowercase namespaced capability id of at most 128 characters."
            )
        version = payload.get("version")
        if not isinstance(version, str) or _SEMVER.fullmatch(version) is None:
            result.add_error("Field 'version' must be strict numeric semver X.Y.Z.")
        _check_text(payload.get("owner"), "owner", 128, result)
        _check_text(payload.get("description"), "description", 2000, result)

    @staticmethod
    def _check_enums(payload: Mapping[str, Any], result: ValidationResult) -> None:
        choices = {
            "mode": {item.value for item in CapabilityMode},
            "acting_as": {item.value for item in ActingAs},
            "reversibility": {item.value for item in Reversibility},
            "approval": {item.value for item in ApprovalMode},
        }
        for field_name, allowed in choices.items():
            value = payload.get(field_name)
            if not isinstance(value, str) or value not in allowed:
                result.add_error(
                    f"Field '{field_name}' must be exactly one of {sorted(allowed)}."
                )

    @staticmethod
    def _check_executor(payload: Mapping[str, Any], result: ValidationResult) -> None:
        executor = payload.get("executor")
        if not isinstance(executor, str) or _PLAIN_NAME.fullmatch(executor) is None:
            result.add_error("Field 'executor' must be a plain lowercase registry name.")

    @staticmethod
    def _check_scopes(payload: Mapping[str, Any], result: ValidationResult) -> None:
        scopes = payload.get("required_scopes")
        if not isinstance(scopes, (list, tuple)):
            result.add_error("Field 'required_scopes' must be a list of scope names.")
            return
        seen: set[str] = set()
        for index, scope in enumerate(scopes):
            if not isinstance(scope, str) or _SCOPE.fullmatch(scope) is None:
                result.add_error(
                    f"Field 'required_scopes[{index}]' must be a namespaced scope name."
                )
            elif scope in seen:
                result.add_error(f"Field 'required_scopes' contains duplicate '{scope}'.")
            else:
                seen.add(scope)
        if payload.get("mode") == CapabilityMode.WRITE.value and not scopes:
            result.add_error("Field 'required_scopes' must be non-empty when mode is 'write'.")

    @staticmethod
    def _check_preconditions(payload: Mapping[str, Any], result: ValidationResult) -> None:
        preconditions = payload.get("preconditions")
        if not isinstance(preconditions, (list, tuple)):
            result.add_error("Field 'preconditions' must be a list of rule mappings.")
            return
        for index, entry in enumerate(preconditions):
            field_name = f"preconditions[{index}]"
            if not isinstance(entry, Mapping):
                result.add_error(f"Field '{field_name}' must be a mapping with only 'rule'.")
                continue
            if set(entry) != {"rule"}:
                result.add_error(f"Field '{field_name}' must contain only the 'rule' key.")
                continue
            rule = entry.get("rule")
            if not isinstance(rule, str) or _PLAIN_NAME.fullmatch(rule) is None:
                result.add_error(f"Field '{field_name}.rule' must be a plain lowercase rule name.")

    @staticmethod
    def _check_input_schema(payload: Mapping[str, Any], result: ValidationResult) -> None:
        schema = payload.get("input_schema")
        if not isinstance(schema, Mapping):
            result.add_error("Field 'input_schema' must be a JSON Schema mapping.")
            return
        _validate_schema_node(schema, "input_schema", result, top_level=True)

    @staticmethod
    def _check_idempotency(payload: Mapping[str, Any], result: ValidationResult) -> None:
        key = payload.get("idempotency_key")
        if payload.get("mode") == CapabilityMode.WRITE.value and key is None:
            result.add_error("Field 'idempotency_key' is required when mode is 'write'.")
            return
        if key is None:
            return
        if not isinstance(key, str) or not key:
            result.add_error("Field 'idempotency_key' must be a non-empty property name.")
            return
        schema = payload.get("input_schema")
        if not isinstance(schema, Mapping):
            return
        properties = schema.get("properties")
        required = schema.get("required")
        if not isinstance(properties, Mapping) or key not in properties:
            result.add_error(
                f"Field 'idempotency_key' names '{key}', which is absent from input_schema.properties."
            )
        if not isinstance(required, (list, tuple)) or key not in required:
            result.add_error(
                f"Field 'idempotency_key' names '{key}', which is not listed in input_schema.required."
            )

    @staticmethod
    def _check_compensation(payload: Mapping[str, Any], result: ValidationResult) -> None:
        reversibility = payload.get("reversibility")
        compensation = payload.get("compensation")
        if reversibility == Reversibility.COMPENSATABLE.value:
            if (
                not isinstance(compensation, str)
                or len(compensation) > 128
                or _CAPABILITY_ID.fullmatch(compensation) is None
            ):
                result.add_error(
                    "Field 'compensation' must be a valid capability id when reversibility is 'compensatable'."
                )
            elif compensation == payload.get("id"):
                result.add_error("Field 'compensation' must not equal field 'id'.")
        elif "compensation" in payload:
            result.add_error(
                "Field 'compensation' must be absent for reversible or irreversible capabilities."
            )

    @staticmethod
    def _check_irreversible_approval(
        payload: Mapping[str, Any], result: ValidationResult
    ) -> None:
        if (
            payload.get("reversibility") == Reversibility.IRREVERSIBLE.value
            and payload.get("approval") != ApprovalMode.SUPERVISED.value
        ):
            result.add_error(
                "Field 'approval' must be 'supervised' when reversibility is 'irreversible'."
            )

    @staticmethod
    def _check_provenance(payload: Mapping[str, Any], result: ValidationResult) -> None:
        provenance = payload.get("provenance")
        if not isinstance(provenance, Mapping):
            result.add_error("Field 'provenance' must be a mapping.")
            return
        if set(provenance) != {"policy_uri", "policy_hash"}:
            result.add_error(
                "Field 'provenance' must contain only 'policy_uri' and 'policy_hash'."
            )
        _check_text(provenance.get("policy_uri"), "provenance.policy_uri", 2048, result)
        policy_hash = provenance.get("policy_hash")
        if not isinstance(policy_hash, str) or _POLICY_HASH.fullmatch(policy_hash) is None:
            result.add_error(
                "Field 'provenance.policy_hash' must match sha256:<64 lowercase hex characters>."
            )

    @classmethod
    def _check_secret_keys(
        cls,
        value: Any,
        path: str,
        result: ValidationResult,
        seen: set[int],
        depth: int = 0,
    ) -> None:
        if depth > _MAX_DOCUMENT_DEPTH:
            result.add_error(
                f"Field '{path}' exceeds the maximum document nesting depth of "
                f"{_MAX_DOCUMENT_DEPTH}."
            )
            return
        if isinstance(value, Mapping):
            if id(value) in seen:
                return
            seen.add(id(value))
            for key, item in value.items():
                if isinstance(key, str) and _is_secret_key(key):
                    result.add_error(f"Field '{path}.{key}' is forbidden because secret keys are not allowed.")
                cls._check_secret_keys(item, f"{path}.{key}", result, seen, depth + 1)
        elif isinstance(value, (list, tuple)):
            if id(value) in seen:
                return
            seen.add(id(value))
            for index, item in enumerate(value):
                cls._check_secret_keys(item, f"{path}[{index}]", result, seen, depth + 1)


def _check_text(value: Any, field_name: str, maximum: int, result: ValidationResult) -> None:
    if not isinstance(value, str) or not value.strip() or len(value) > maximum:
        result.add_error(
            f"Field '{field_name}' must be non-empty text of at most {maximum} characters."
        )
        return
    if any(char not in {"\n", "\t"} and unicodedata.category(char) == "Cc" for char in value):
        result.add_error(f"Field '{field_name}' contains a forbidden control character.")


def _is_secret_key(key: str) -> bool:
    lowered = key.lower()
    compact = re.sub(r"[^a-z0-9]", "", lowered)
    return any(
        part in lowered or part.replace("_", "") in compact
        for part in _SECRET_KEY_PARTS
    )


def _validate_schema_node(
    schema: Mapping[str, Any],
    path: str,
    result: ValidationResult,
    *,
    top_level: bool = False,
    ancestors: frozenset[int] = frozenset(),
    depth: int = 0,
) -> None:
    if depth > _MAX_SCHEMA_DEPTH:
        result.add_error(
            f"Field '{path}' exceeds the maximum input schema nesting depth of "
            f"{_MAX_SCHEMA_DEPTH}."
        )
        return
    if id(schema) in ancestors:
        result.add_error(f"Field '{path}' contains a recursive JSON Schema structure.")
        return
    ancestors = ancestors | {id(schema)}
    for keyword in sorted(set(schema) & _FORBIDDEN_SCHEMA_KEYWORDS):
        result.add_error(f"Field '{path}.{keyword}' uses a forbidden JSON Schema keyword.")
    for keyword in sorted(set(schema) - _SCHEMA_KEYWORDS, key=str):
        result.add_error(f"Field '{path}.{keyword}' is not an allowed JSON Schema keyword.")

    schema_type = schema.get("type")
    if not isinstance(schema_type, str) or schema_type not in _SCHEMA_TYPES:
        result.add_error(f"Field '{path}.type' must be one of {sorted(_SCHEMA_TYPES)}.")
        return
    for keyword in sorted(set(schema) - _SCHEMA_KEYWORDS_BY_TYPE[schema_type], key=str):
        if keyword in _SCHEMA_KEYWORDS:
            result.add_error(
                f"Field '{path}.{keyword}' is not allowed for JSON Schema type '{schema_type}'."
            )
    if top_level and schema_type != "object":
        result.add_error("Field 'input_schema.type' must be 'object'.")

    _validate_enum(schema, schema_type, path, result)
    if schema_type == "object":
        _validate_object_schema(schema, path, result, ancestors, depth)
    elif schema_type == "array":
        _validate_array_schema(schema, path, result, ancestors, depth)
    elif schema_type == "string":
        _validate_string_schema(schema, path, result)
    elif schema_type in {"integer", "number"}:
        _validate_numeric_schema(schema, schema_type, path, result)


def _validate_object_schema(
    schema: Mapping[str, Any],
    path: str,
    result: ValidationResult,
    ancestors: frozenset[int],
    depth: int = 0,
) -> None:
    if schema.get("additionalProperties") is not False:
        result.add_error(f"Field '{path}.additionalProperties' must be false.")
    properties = schema.get("properties")
    if not isinstance(properties, Mapping):
        result.add_error(f"Field '{path}.properties' must be a mapping.")
        return
    required = schema.get("required")
    required_is_valid = isinstance(required, (list, tuple)) and all(
        isinstance(name, str) for name in required
    )
    if not required_is_valid:
        result.add_error(f"Field '{path}.required' must be a list of property names.")
        required = []
    elif len(set(required)) != len(required):
        result.add_error(f"Field '{path}.required' must not contain duplicates.")
    for name in required:
        if name not in properties:
            result.add_error(
                f"Field '{path}.required' names unknown property '{name}'."
            )
    for name, child in properties.items():
        child_path = f"{path}.properties.{name}"
        if not isinstance(name, str) or not name:
            result.add_error(f"Field '{path}.properties' contains an invalid property name.")
        if not isinstance(child, Mapping):
            result.add_error(f"Field '{child_path}' must be a JSON Schema mapping.")
            continue
        _validate_schema_node(child, child_path, result, ancestors=ancestors, depth=depth + 1)


def _validate_array_schema(
    schema: Mapping[str, Any],
    path: str,
    result: ValidationResult,
    ancestors: frozenset[int],
    depth: int = 0,
) -> None:
    maximum = schema.get("maxItems")
    if type(maximum) is not int or not 0 <= maximum <= 100:
        result.add_error(f"Field '{path}.maxItems' must be an integer from 0 to 100.")
    items = schema.get("items")
    if not isinstance(items, Mapping):
        result.add_error(f"Field '{path}.items' must be a JSON Schema mapping.")
    else:
        _validate_schema_node(items, f"{path}.items", result, ancestors=ancestors, depth=depth + 1)


def _validate_string_schema(
    schema: Mapping[str, Any], path: str, result: ValidationResult
) -> None:
    maximum = schema.get("maxLength")
    if type(maximum) is not int or not 0 <= maximum <= 4096:
        result.add_error(f"Field '{path}.maxLength' must be an integer from 0 to 4096.")


def _validate_numeric_schema(
    schema: Mapping[str, Any], schema_type: str, path: str, result: ValidationResult
) -> None:
    minimum = schema.get("minimum")
    maximum = schema.get("maximum")
    if not _is_number(minimum):
        result.add_error(f"Field '{path}.minimum' must be a finite number.")
    if not _is_number(maximum):
        result.add_error(f"Field '{path}.maximum' must be a finite number.")
    if _is_number(minimum) and _is_number(maximum) and minimum > maximum:
        result.add_error(f"Fields '{path}.minimum' and '{path}.maximum' are inconsistent.")
    if schema_type == "integer":
        if _is_number(minimum) and type(minimum) is not int:
            result.add_error(f"Field '{path}.minimum' must be an integer.")
        if _is_number(maximum) and type(maximum) is not int:
            result.add_error(f"Field '{path}.maximum' must be an integer.")


def _validate_enum(
    schema: Mapping[str, Any], schema_type: str, path: str, result: ValidationResult
) -> None:
    if "enum" not in schema:
        return
    values = schema["enum"]
    if not isinstance(values, (list, tuple)) or not 1 <= len(values) <= 64:
        result.add_error(f"Field '{path}.enum' must contain from 1 to 64 scalar values.")
        return
    for index, value in enumerate(values):
        if not _enum_value_matches(value, schema_type):
            result.add_error(
                f"Field '{path}.enum[{index}]' must be a scalar of declared type '{schema_type}'."
            )


def _enum_value_matches(value: Any, schema_type: str) -> bool:
    if schema_type == "string":
        return isinstance(value, str)
    if schema_type == "boolean":
        return isinstance(value, bool)
    if schema_type == "integer":
        return type(value) is int
    if schema_type == "number":
        return _is_number(value)
    return False


def _is_number(value: Any) -> bool:
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return True
    return isinstance(value, float) and math.isfinite(value)


def parse_capability(payload: Mapping[str, Any]) -> CapabilityDefinition:
    """Validate and construct one immutable capability definition."""
    result = CapabilityValidator().validate(payload)
    if not result.ok:
        raise CapabilityValidationError(result.errors)
    provenance = payload["provenance"]
    return CapabilityDefinition(
        id=payload["id"],
        version=payload["version"],
        owner=payload["owner"],
        description=payload["description"],
        mode=CapabilityMode(payload["mode"]),
        executor=payload["executor"],
        acting_as=ActingAs(payload["acting_as"]),
        required_scopes=tuple(payload["required_scopes"]),
        input_schema=payload["input_schema"],
        preconditions=tuple(item["rule"] for item in payload["preconditions"]),
        reversibility=Reversibility(payload["reversibility"]),
        compensation=payload.get("compensation"),
        approval=ApprovalMode(payload["approval"]),
        idempotency_key=payload.get("idempotency_key"),
        policy_uri=provenance["policy_uri"],
        policy_hash=provenance["policy_hash"],
    )
