"""JSON Schema for semantic YAML; SemanticValidator remains the complete verdict.

Unknown fields and warning-only metadata are intentionally open. This module is
Spark-free and uses only the validator/domain vocabulary, never a packaged JSON file.
"""
from __future__ import annotations

from .domain import (
    ENTITY_ROLES,
    METRIC_ADDITIVITY,
    RELATIONSHIP_CARDINALITIES,
    RELATIONSHIP_JOIN_TYPES,
)
from .validator import _SAFE_IDENTIFIER, _VALID_AGG_TYPES, _VALID_DIM_TYPES


def generate_semantic_json_schema() -> dict:
    """Return a fresh draft-07 schema for the complete ``models: [...]`` document.

    Cross-member references, uniqueness of names and SQL expression checks still
    require ``SemanticValidator.validate_yaml``. Acceptance here is necessary,
    not sufficient. Case-insensitive aggregate types mirror ``str.lower()``.
    """
    identifier = {"type": "string", "pattern": _SAFE_IDENTIFIER.pattern}
    text = {"type": "string", "minLength": 1}
    aggregate_pattern = "^(?:" + "|".join(
        "".join(f"[{c.lower()}{c.upper()}]" if c.isalpha() else c for c in value)
        for value in sorted(_VALID_AGG_TYPES)
    ) + ")$"
    entity = {
        "type": "object",
        "required": ["name", "type", "key"],
        "properties": {
            "name": identifier,
            "type": {"enum": sorted(ENTITY_ROLES)},
            "key": {"oneOf": [identifier, {
                "type": "array", "items": identifier, "minItems": 1, "uniqueItems": True,
            }]},
        },
    }
    relationship = {
        "type": "object",
        "required": ["name", "from_entity", "to_model", "to_entity", "cardinality", "join_type"],
        "properties": {
            **{key: identifier for key in ("name", "from_entity", "to_model", "to_entity")},
            "cardinality": {
                "enum": sorted(RELATIONSHIP_CARDINALITIES - {"unknown"}),
                "description": "The validator explicitly refuses unknown cardinality.",
            },
            "join_type": {"enum": sorted(RELATIONSHIP_JOIN_TYPES)},
            "verified_by_contract": {"description": "Legacy validator uses truthiness, without a type restriction."},
        },
    }
    dimension = {
        "type": "object", "required": ["name", "sql"],
        "properties": {
            "name": identifier, "sql": text,
            "type": {
                "type": "string",
                "description": "Non-standard values only warn. Standard types: " + ", ".join(sorted(_VALID_DIM_TYPES)),
            },
            "description": {}, "synonyms": {},
        },
    }
    metric = {
        "type": "object", "required": ["name", "sql", "type"],
        "properties": {
            "name": identifier, "sql": text,
            "type": {"type": "string", "pattern": aggregate_pattern},
            "additivity": {"enum": sorted(METRIC_ADDITIVITY)},
            "non_additive_dimensions": {},
            "filters": {"description": "Malformed inline filters only warn; retain validator acceptance."},
            "description": {}, "synonyms": {},
        },
        "allOf": [
            {
                "if": {"properties": {"additivity": {"const": "semi_additive"}}, "required": ["additivity"]},
                "then": {
                    "required": ["non_additive_dimensions"],
                    "properties": {"non_additive_dimensions": {
                        "type": "array", "minItems": 1, "items": identifier,
                    }},
                },
            },
            {
                "if": {"properties": {"additivity": {"const": "additive"}}},
                "then": {"not": {"required": ["non_additive_dimensions"]}},
            },
        ],
    }
    model = {
        "type": "object", "required": ["name", "table", "dimensions", "metrics"],
        "properties": {
            # Model display names and keys are not identifier-pattern checked by the validator.
            "name": text, "key": {"type": "string"}, "table": text,
            "dimensions": {"type": "array", "minItems": 1, "items": {"$ref": "#/$defs/Dimension"}},
            "metrics": {"type": "array", "minItems": 1, "items": {"$ref": "#/$defs/Metric"}},
            "entities": {"type": ["array", "null"], "items": {"$ref": "#/$defs/Entity"}},
            "relationships": {"type": ["array", "null"], "items": {"$ref": "#/$defs/Relationship"}},
            "grain": {
                "description": "Legacy parser ignores non-string/non-array grain values and empty strings.",
                "allOf": [
                    {"if": {"type": "string", "minLength": 1}, "then": identifier},
                    {"if": {"type": "array"}, "then": {"items": identifier, "uniqueItems": True}},
                ],
            },
            "calendar": {"anyOf": [identifier, {"type": "null"}]},
            "row_policy": {
                "type": "object", "required": ["column", "attribute"],
                "additionalProperties": False,
                "properties": {"column": identifier, "attribute": identifier},
            },
            "description": {}, "synonyms": {}, "layer": {}, "base_filter": {},
            "default_time_dimension": {}, "metadata": {},
        },
    }
    return {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "$id": "https://skifer.io/schemas/semantic-model/v1",
        "title": "Skifer semantic model document",
        "description": "Necessary structural validation. Run SemanticValidator for the complete verdict.",
        "type": "object", "required": ["models"],
        "properties": {"models": {"type": "array", "minItems": 1, "items": {"$ref": "#/$defs/Model"}}},
        "$defs": {"Model": model, "Dimension": dimension, "Metric": metric, "Entity": entity, "Relationship": relationship},
    }
