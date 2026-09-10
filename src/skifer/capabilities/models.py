"""Immutable declarations for governed external-system capabilities."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from types import MappingProxyType
from typing import Any, Mapping


class CapabilityMode(str, Enum):
    """Whether a capability observes or changes an external system."""

    READ = "read"
    WRITE = "write"


class ActingAs(str, Enum):
    """Identity posture required by a capability definition."""

    SERVICE = "service"
    DELEGATED_USER = "delegated_user"


class Reversibility(str, Enum):
    """Declared recovery posture for an external side effect."""

    REVERSIBLE = "reversible"
    COMPENSATABLE = "compensatable"
    IRREVERSIBLE = "irreversible"


class ApprovalMode(str, Enum):
    """Maximum autonomy posture declared by the capability."""

    SHADOW = "shadow"
    SUPERVISED = "supervised"
    GUARDED = "guarded"


def _freeze(value: Any) -> Any:
    """Deep-copy JSON-shaped data into immutable containers."""
    if isinstance(value, Mapping):
        return MappingProxyType({key: _freeze(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(item) for item in value)
    return value


def _thaw(value: Any) -> Any:
    """Return fresh JSON-native containers for a public serialization boundary."""
    if isinstance(value, Mapping):
        return {key: _thaw(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw(item) for item in value]
    return value


@dataclass(frozen=True)
class CapabilitySummary:
    """The small allowlisted capability surface exposed by the startup catalog."""

    id: str
    version: str
    owner: str
    description: str
    mode: CapabilityMode
    approval: ApprovalMode
    reversibility: Reversibility

    def to_dict(self) -> dict[str, Any]:
        """Serialize only catalog fields, field by field."""
        return {
            "id": self.id,
            "version": self.version,
            "owner": self.owner,
            "description": self.description,
            "mode": self.mode.value,
            "approval": self.approval.value,
            "reversibility": self.reversibility.value,
        }


@dataclass(frozen=True)
class CapabilityDefinition:
    """A fully validated and immutable governed capability definition."""

    id: str
    version: str
    owner: str
    description: str
    mode: CapabilityMode
    executor: str
    acting_as: ActingAs
    required_scopes: tuple[str, ...]
    input_schema: Mapping[str, Any]
    preconditions: tuple[str, ...]
    reversibility: Reversibility
    compensation: str | None
    approval: ApprovalMode
    idempotency_key: str | None
    policy_uri: str
    policy_hash: str

    def __post_init__(self) -> None:
        """Revalidate direct construction, then freeze nested input data."""
        from .validator import CapabilityValidationError, CapabilityValidator

        construction_errors = []
        for field_name, enum_type in (
            ("mode", CapabilityMode),
            ("acting_as", ActingAs),
            ("reversibility", Reversibility),
            ("approval", ApprovalMode),
        ):
            if not isinstance(getattr(self, field_name), enum_type):
                construction_errors.append(
                    f"Field '{field_name}' must be a {enum_type.__name__} value."
                )
        if not isinstance(self.required_scopes, tuple):
            construction_errors.append("Field 'required_scopes' must be a tuple.")
        if not isinstance(self.preconditions, tuple):
            construction_errors.append("Field 'preconditions' must be a tuple.")
        if not isinstance(self.input_schema, Mapping):
            construction_errors.append("Field 'input_schema' must be a mapping.")
        if construction_errors:
            raise CapabilityValidationError(construction_errors)

        # A direct constructor call must provide the same guarantees as the YAML
        # parser. Importing locally avoids a module cycle while keeping the
        # validator the single source of truth for the authorization surface.
        validation_payload = {
            "id": self.id,
            "version": self.version,
            "owner": self.owner,
            "description": self.description,
            "mode": self.mode.value,
            "executor": self.executor,
            "acting_as": self.acting_as.value,
            "required_scopes": list(self.required_scopes),
            "input_schema": self.input_schema,
            "preconditions": [{"rule": rule} for rule in self.preconditions],
            "reversibility": self.reversibility.value,
            "approval": self.approval.value,
            "provenance": {
                "policy_uri": self.policy_uri,
                "policy_hash": self.policy_hash,
            },
        }
        if self.compensation is not None:
            validation_payload["compensation"] = self.compensation
        if self.idempotency_key is not None:
            validation_payload["idempotency_key"] = self.idempotency_key
        result = CapabilityValidator().validate(validation_payload)
        if not result.ok:
            raise CapabilityValidationError(result.errors)
        object.__setattr__(self, "input_schema", _freeze(self.input_schema))

    def summary(self) -> CapabilitySummary:
        """Project the definition onto the catalog-safe summary contract."""
        return CapabilitySummary(
            id=self.id,
            version=self.version,
            owner=self.owner,
            description=self.description,
            mode=self.mode,
            approval=self.approval,
            reversibility=self.reversibility,
        )

    def to_dict(self) -> dict[str, Any]:
        """Serialize the public definition through an explicit allowlist."""
        payload = {
            "id": self.id,
            "version": self.version,
            "owner": self.owner,
            "description": self.description,
            "mode": self.mode.value if isinstance(self.mode, CapabilityMode) else self.mode,
            "executor": self.executor,
            "acting_as": (
                self.acting_as.value if isinstance(self.acting_as, ActingAs) else self.acting_as
            ),
            "required_scopes": list(self.required_scopes),
            "input_schema": _thaw(self.input_schema),
            "preconditions": [{"rule": rule} for rule in self.preconditions],
            "reversibility": (
                self.reversibility.value
                if isinstance(self.reversibility, Reversibility)
                else self.reversibility
            ),
            "approval": (
                self.approval.value if isinstance(self.approval, ApprovalMode) else self.approval
            ),
            "provenance": {
                "policy_uri": self.policy_uri,
                "policy_hash": self.policy_hash,
            },
        }
        if self.compensation is not None:
            payload["compensation"] = self.compensation
        if self.idempotency_key is not None:
            payload["idempotency_key"] = self.idempotency_key
        return payload
