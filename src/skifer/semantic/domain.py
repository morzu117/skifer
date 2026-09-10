"""Typed semantic-domain definitions for optional model relationships."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal


ENTITY_ROLES = frozenset({"primary", "foreign", "unique"})
METRIC_ADDITIVITY = frozenset({"additive", "semi_additive", "non_additive"})
RELATIONSHIP_CARDINALITIES = frozenset(
    {"one_to_one", "many_to_one", "one_to_many", "many_to_many", "unknown"}
)
RELATIONSHIP_JOIN_TYPES = frozenset({"inner", "left"})


@dataclass(frozen=True)
class EntityDef:
    name: str
    model_key: str
    key_columns: tuple[str, ...]
    role: Literal["primary", "foreign", "unique"]


@dataclass(frozen=True)
class EntityRef:
    model_key: str
    entity_name: str


@dataclass(frozen=True)
class RelationshipDef:
    name: str
    from_entity: EntityRef
    to_entity: EntityRef
    cardinality: str
    join_type: str
    verified_by_contract: bool


def parse_grain(model: dict[str, Any]) -> tuple[str, ...]:
    """Return the declared semantic grain as an ordered tuple of entity names."""
    grain = model.get("grain") or []
    if isinstance(grain, str):
        return (grain,)
    if isinstance(grain, list):
        return tuple(str(item) for item in grain)
    return ()


def parse_entities(model: dict[str, Any]) -> tuple[EntityDef, ...]:
    """Parse ``entities`` from one semantic model payload."""
    model_key = str(model.get("key") or model.get("name") or "")
    entities = model.get("entities") or []
    if not isinstance(entities, list):
        return ()

    parsed: list[EntityDef] = []
    for entity in entities:
        if not isinstance(entity, dict):
            continue
        parsed.append(
            EntityDef(
                name=str(entity.get("name") or ""),
                model_key=model_key,
                key_columns=_parse_key_columns(entity.get("key")),
                role=str(entity.get("type") or ""),
            )
        )
    return tuple(parsed)


def parse_relationships(model: dict[str, Any]) -> tuple[RelationshipDef, ...]:
    """Parse ``relationships`` from one semantic model payload."""
    model_key = str(model.get("key") or model.get("name") or "")
    relationships = model.get("relationships") or []
    if not isinstance(relationships, list):
        return ()

    parsed: list[RelationshipDef] = []
    for relationship in relationships:
        if not isinstance(relationship, dict):
            continue
        parsed.append(
            RelationshipDef(
                name=str(relationship.get("name") or ""),
                from_entity=EntityRef(
                    model_key=model_key,
                    entity_name=str(relationship.get("from_entity") or ""),
                ),
                to_entity=EntityRef(
                    model_key=str(relationship.get("to_model") or ""),
                    entity_name=str(relationship.get("to_entity") or ""),
                ),
                cardinality=str(relationship.get("cardinality") or ""),
                join_type=str(relationship.get("join_type") or ""),
                verified_by_contract=bool(relationship.get("verified_by_contract", False)),
            )
        )
    return tuple(parsed)


def _parse_key_columns(raw_key: Any) -> tuple[str, ...]:
    if isinstance(raw_key, str):
        return (raw_key,)
    if isinstance(raw_key, list):
        return tuple(str(item) for item in raw_key)
    return ()
