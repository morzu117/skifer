"""
JSON Schema generator for Skifer pipeline YAML files.

Derives the schema from the operator catalog (op_catalog.py) so that the
generated schema automatically reflects any new operators added there.

Usage::

    from skifer.core.json_schema import generate_json_schema
    import json
    print(json.dumps(generate_json_schema(), indent=2))

    # Regenerate the committed schema file:
    python -c "from skifer.core.json_schema import generate_json_schema; \\
               import json; print(json.dumps(generate_json_schema(), indent=2))" \\
           > schemas/skifer-pipeline.schema.json
"""
from __future__ import annotations

from skifer.core.constants import (
    CLASSIFICATION_LEVELS,
    VALID_MATERIALIZATION_TYPES,
    VALID_MV_REFRESH_MODES,
    VALID_SOURCE_TYPES,
)
from skifer.core.op_catalog import AGGREGATE_FUNCTIONS, FILTER_OPERATORS, COLUMN_OPS


def generate_json_schema() -> dict:
    """
    Build a JSON Schema (draft-07) describing a Skifer pipeline YAML.

    The schema is derived from the operator catalog — filter operator enums and
    column op descriptions come directly from FILTER_OPERATORS / COLUMN_OPS.

    Returns:
        dict: A JSON Schema dict ready to serialize with ``json.dumps``.
    """
    filter_operator_enum = sorted(FILTER_OPERATORS.keys())
    column_op_enum = sorted(COLUMN_OPS.keys())

    # Build the schema
    schema: dict = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "$id": "https://skifer.io/schemas/pipeline/v1",
        "title": "Skifer Pipeline Schema",
        "description": (
            "Declarative data-engineering pipeline schema for Skifer. "
            "Describes sources, joins, business rules, and output column selection."
        ),
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "tables": {
                "description": "Source tables and their per-table transformations.",
                "type": "array",
                "items": {"$ref": "#/$defs/TableDef"},
            },
            "join": {
                "description": "JOIN specifications applied after all sources are loaded.",
                "type": "array",
                "items": {"$ref": "#/$defs/JoinDef"},
            },
            "business_rules": {
                "description": "Ordered list of registered rule names to apply.",
                "type": "array",
                "items": {"type": "string"},
            },
            "select_final": {
                "description": "Output column selection — mutually exclusive with keep_all_columns.",
                "type": "array",
                "items": {"$ref": "#/$defs/SelectEntry"},
            },
            "add_columns": {
                "description": "Extra columns added when keep_all_columns is true.",
                "type": "array",
                "items": {"$ref": "#/$defs/SelectEntry"},
            },
            "keep_all_columns": {
                "description": "Pass all source columns through unchanged. Mutually exclusive with select_final.",
                "type": "boolean",
            },
            "aggregate": {
                "description": (
                    "Declarative GROUP BY (Plan 28) — mutually exclusive with "
                    "select_final and keep_all_columns."
                ),
                "$ref": "#/$defs/AggregateDef",
            },
            "dev_limit": {
                "description": "Row cap applied in interactive/non-prod mode (ignored in job/prod).",
                "type": "integer",
                "minimum": 1,
            },
            "sink": {
                "description": "Optional output sink override (default: Delta table write).",
                "$ref": "#/$defs/SinkDef",
            },
            "materialization": {
                "description": (
                    "Target materialization: batch table (default), streaming_table "
                    "(Plan 27) or materialized_view (Plan 28). String shorthand or dict "
                    "form; the allowed keys depend on the type."
                ),
                "$ref": "#/$defs/MaterializationDef",
            },
            "partials": {
                "description": (
                    "Nested YAML sub-transformations (Plan 25) exposed as joinable aliases. "
                    "Child schemas never write final tables; batch only."
                ),
                "type": "array",
                "items": {"$ref": "#/$defs/PartialDef"},
            },
            "data_product": {
                "description": "Versioned owner metadata for this pipeline output (Plan 29).",
                "$ref": "#/$defs/DataProductDef",
            },
            "contract": {
                "description": "Explicit final-output contract used by semantic projection (Plan 29).",
                "$ref": "#/$defs/ContractDef",
            },
            "semantic": {
                "description": "Seed for a semantic model generated alongside this pipeline (Plan 29).",
                "$ref": "#/$defs/SemanticSeedDef",
            },
        },
        "$defs": {
            # ------------------------------------------------------------------
            # Agent-ready product / output contract / semantic seed (Plan 29)
            # ------------------------------------------------------------------
            "DataProductDef": {
                "type": "object",
                "required": ["id", "version"],
                "additionalProperties": False,
                "properties": {
                    "id": {
                        "type": "string",
                        "pattern": "^[A-Za-z0-9_.-]+$",
                        "description": "Stable data-product identifier.",
                    },
                    "version": {
                        "type": "string",
                        "pattern": "^\\d+\\.\\d+\\.\\d+(?:-[0-9A-Za-z.-]+)?(?:\\+[0-9A-Za-z.-]+)?$",
                        "description": "Semantic version of the product contract.",
                    },
                    "owner": {
                        "oneOf": [
                            {"type": "string", "minLength": 1},
                            {
                                "type": "object",
                                "additionalProperties": False,
                                "properties": {
                                    "team": {"type": "string", "minLength": 1},
                                    "steward": {"type": "string", "minLength": 1},
                                    "domain": {"type": "string", "minLength": 1},
                                    "contact": {"type": "string", "minLength": 1},
                                },
                            },
                        ]
                    },
                    "description": {"type": "string", "minLength": 1},
                    "domain": {"type": "string", "minLength": 1},
                },
            },
            "OutputFieldDef": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "logical_type": {"type": "string", "minLength": 1},
                    "required": {"type": "boolean"},
                    "unique": {"type": "boolean"},
                    "classification": {
                        "type": "string",
                        "enum": list(CLASSIFICATION_LEVELS),
                    },
                    "entity": {"type": "string", "minLength": 1},
                    "description": {"type": "string", "minLength": 1},
                },
            },
            "ContractDef": {
                "type": "object",
                "required": ["output"],
                "additionalProperties": False,
                "properties": {
                    "grain": {
                        "type": "array",
                        "uniqueItems": True,
                        "items": {"type": "string", "pattern": "^[A-Za-z0-9_.-]+$"},
                    },
                    "output": {
                        "type": "object",
                        "minProperties": 1,
                        "propertyNames": {"pattern": "^[A-Za-z0-9_.-]+$"},
                        "additionalProperties": {"$ref": "#/$defs/OutputFieldDef"},
                    },
                },
            },
            "SemanticSeedDef": {
                "type": "object",
                "required": ["model_key"],
                "additionalProperties": False,
                "properties": {
                    "model_key": {"type": "string", "pattern": "^[A-Za-z0-9_.-]+$"},
                    "entity": {"type": "string", "pattern": "^[A-Za-z0-9_.-]+$"},
                    "default_time_dimension": {
                        "type": "string",
                        "pattern": "^[A-Za-z0-9_.-]+$",
                    },
                    "dimensions": {
                        "type": "array",
                        "uniqueItems": True,
                        "items": {"type": "string", "pattern": "^[A-Za-z0-9_.-]+$"},
                    },
                },
            },
            # ------------------------------------------------------------------
            # Filter
            # ------------------------------------------------------------------
            "FilterOperator": {
                "description": "Canonical filter operator name.",
                "type": "string",
                "enum": filter_operator_enum,
            },
            "FilterDictEntry": {
                "description": "Filter predicate in normalized dict form.",
                "type": "object",
                "required": ["column", "operator"],
                "additionalProperties": False,
                "properties": {
                    "column": {"type": "string", "description": "Column name to filter on."},
                    "operator": {
                        "type": "string",
                        "description": (
                            "Filter comparison operator (canonical or alias). "
                            f"Canonical names: {filter_operator_enum}."
                        ),
                    },
                    "value": {
                        "description": "Comparison value (omit for nullary operators: is_null, is_not_null).",
                        "oneOf": [
                            {"type": "string"},
                            {"type": "number"},
                            {"type": "array", "items": {"type": "string"}},
                        ],
                    },
                },
            },
            "FilterStringEntry": {
                "description": "Filter predicate in compact string form 'column:operator[:value]'.",
                "type": "string",
                "pattern": "^[^:]+:[^:]+",
            },
            "FilterEntry": {
                "description": "A single filter predicate — compact string OR dict form.",
                "oneOf": [
                    {"$ref": "#/$defs/FilterStringEntry"},
                    {"$ref": "#/$defs/FilterDictEntry"},
                ],
            },
            "FilterList": {
                "description": "List form: an ordered AND-combination of filter predicates.",
                "type": "array",
                "items": {"$ref": "#/$defs/FilterEntry"},
            },
            "FilterMapping": {
                "description": (
                    "Mapping form: {column: value} for equals, "
                    "{column: {operator: value}} for other operators, "
                    "{column: is_not_null} for no-arg operators."
                ),
                "type": "object",
                "additionalProperties": {
                    "oneOf": [
                        {"type": "string"},
                        {"type": "number"},
                        {"type": "null"},
                        {
                            "type": "object",
                            "minProperties": 1,
                            "maxProperties": 1,
                            "additionalProperties": {
                                "oneOf": [
                                    {"type": "string"},
                                    {"type": "number"},
                                    {"type": "null"},
                                    {"type": "array", "items": {"type": "string"}},
                                ]
                            },
                        },
                    ]
                },
            },
            # ------------------------------------------------------------------
            # Column operation
            # ------------------------------------------------------------------
            "ColumnOpString": {
                "description": (
                    "Column operation string — e.g. 'cast:double', 'round:2', 'upper'. "
                    f"Known op names: {column_op_enum}."
                ),
                "type": "string",
            },
            "ColumnOpDict": {
                "description": (
                    "Single-key dict op form — e.g. {cast: double}, {round: 2}. "
                    "Multi-key dicts are used for when/then/else conditional branches."
                ),
                "type": "object",
            },
            "ColumnOp": {
                "description": "A column operation — string form or dict form.",
                "oneOf": [
                    {"$ref": "#/$defs/ColumnOpString"},
                    {"$ref": "#/$defs/ColumnOpDict"},
                ],
            },
            # ------------------------------------------------------------------
            # SelectEntry — three valid forms
            # ------------------------------------------------------------------
            "SelectEntryList": {
                "description": "Compact list form: [source, target] or [source, target, [ops…]].",
                "type": "array",
                "minItems": 2,
                "maxItems": 3,
            },
            "SelectEntrySourceTarget": {
                "description": "Dict form with source/target keys.",
                "type": "object",
                "required": ["target"],
                "additionalProperties": False,
                "properties": {
                    "source": {"type": ["string", "null"]},
                    "target": {"type": "string"},
                    "ops": {"type": "array", "items": {"$ref": "#/$defs/ColumnOp"}},
                },
            },
            "SelectEntryFromAs": {
                "description": "New mapping form with from/as keys.",
                "type": "object",
                "required": ["as"],
                "additionalProperties": False,
                "properties": {
                    "from": {"type": ["string", "null"]},
                    "as": {"type": "string"},
                    "ops": {"type": "array", "items": {"$ref": "#/$defs/ColumnOp"}},
                },
            },
            "SelectEntry": {
                "description": "One output column spec — list, source/target dict, or from/as dict.",
                "oneOf": [
                    {"$ref": "#/$defs/SelectEntryList"},
                    {"$ref": "#/$defs/SelectEntrySourceTarget"},
                    {"$ref": "#/$defs/SelectEntryFromAs"},
                ],
            },
            # ------------------------------------------------------------------
            # Aggregate block (Plan 28)
            # ------------------------------------------------------------------
            "MeasureEntry": {
                "description": "One aggregate measure — [source, target, func] or a mapping.",
                "oneOf": [
                    {
                        "type": "array",
                        "minItems": 3,
                        "maxItems": 3,
                        "items": {"type": "string"},
                        "description": "Compact form: [source, target, func].",
                    },
                    {
                        "type": "object",
                        "required": ["source", "target", "func"],
                        "additionalProperties": False,
                        "properties": {
                            "source": {
                                "type": "string",
                                "description": "Aggregated column, or '*' for count.",
                            },
                            "target": {"type": "string", "description": "Output column name."},
                            "func": {
                                "type": "string",
                                "enum": sorted(AGGREGATE_FUNCTIONS.keys()),
                                "description": "Aggregate function (aliases also accepted).",
                            },
                        },
                    },
                ],
            },
            "AggregateDef": {
                "description": "Declarative GROUP BY producing the final projection.",
                "type": "object",
                "required": ["group_by", "measures"],
                "additionalProperties": False,
                "properties": {
                    "group_by": {
                        "type": "array",
                        "minItems": 1,
                        "items": {"type": "string"},
                        "description": "Grouping key columns.",
                    },
                    "measures": {
                        "type": "array",
                        "minItems": 1,
                        "items": {"$ref": "#/$defs/MeasureEntry"},
                        "description": "Aggregate measures producing the remaining columns.",
                    },
                    "having": {
                        "type": "array",
                        "items": {"$ref": "#/$defs/FilterEntry"},
                        "description": "Post-aggregation predicates on group keys or measure targets.",
                    },
                },
            },
            # ------------------------------------------------------------------
            # Source block
            # ------------------------------------------------------------------
            "SourceDef": {
                "description": "External file source declaration.",
                "type": "object",
                "required": ["type", "path"],
                "additionalProperties": False,
                "properties": {
                    "type": {
                        "type": "string",
                        "enum": sorted(VALID_SOURCE_TYPES),
                    },
                    "path": {"type": "string", "description": "File path or glob (ADLS, S3, GCS, DBFS, local)."},
                    "options": {
                        "type": "object",
                        "additionalProperties": {"type": "string"},
                        "description": "spark.read options (e.g. {header: 'true'}).",
                    },
                },
            },
            # ------------------------------------------------------------------
            # Table
            # ------------------------------------------------------------------
            "QualityChecks": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "drop_nulls_in": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Drop rows where any of these columns is null.",
                    },
                    "drop_duplicates_on": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Drop duplicate rows based on these columns.",
                    },
                },
            },
            "TableDef": {
                "description": "A source table specification.",
                "type": "object",
                "required": ["name"],
                "additionalProperties": True,
                "properties": {
                    "name": {"type": "string", "description": "Fully-qualified table name (catalog.schema.table or schema.table)."},
                    "alias": {"type": "string", "description": "Short alias used in join references."},
                    "source": {
                        "$ref": "#/$defs/SourceDef",
                        "description": "External file source (mutually exclusive with catalog table load).",
                    },
                    "filter": {
                        "description": "Row filter applied at load time.",
                        "oneOf": [
                            {"$ref": "#/$defs/FilterList"},
                            {"$ref": "#/$defs/FilterMapping"},
                        ],
                    },
                    "filter_groups": {
                        "description": "OR-of-AND groups of filter predicates.",
                        "type": "array",
                        "items": {"$ref": "#/$defs/FilterList"},
                    },
                    "quality_checks": {"$ref": "#/$defs/QualityChecks"},
                    "fields": {
                        "description": "Column selection applied at source load time.",
                        "type": "array",
                        "items": {"$ref": "#/$defs/SelectEntry"},
                    },
                    "dev_limit": {
                        "type": "integer",
                        "minimum": 1,
                        "description": "Per-table row cap in dev mode (overrides schema-level dev_limit).",
                    },
                    "streaming": {
                        "type": "boolean",
                        "description": (
                            "Read this table via readStream (Plan 27). Requires a top-level "
                            "'materialization: streaming_table' block; incompatible with "
                            "dev_limit, preprocess.qualify and drop_duplicates_on."
                        ),
                    },
                },
            },
            # ------------------------------------------------------------------
            # Join
            # ------------------------------------------------------------------
            "JoinDef": {
                "description": "A JOIN specification.",
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "table_from": {
                        "oneOf": [
                            {"type": "string"},
                            {"type": "array", "minItems": 2, "maxItems": 2, "items": {"type": "string"}},
                        ],
                        "description": "Left table alias (or [alias, key] compact form).",
                    },
                    "table_to": {
                        "oneOf": [
                            {"type": "string"},
                            {"type": "array", "minItems": 2, "maxItems": 2, "items": {"type": "string"}},
                        ],
                        "description": "Right table alias (or [alias, key] compact form).",
                    },
                    "on_from": {"type": "string", "description": "Left join key column."},
                    "on_to": {"type": "string", "description": "Right join key column."},
                    "type": {
                        "type": "string",
                        "enum": ["left", "inner", "right", "outer", "cross", "full"],
                        "default": "left",
                    },
                },
            },
            # ------------------------------------------------------------------
            # Sink
            # ------------------------------------------------------------------
            "SinkDef": {
                "description": "Output sink override (default is Delta table write).",
                "type": "object",
                "required": ["type"],
                "additionalProperties": False,
                "properties": {
                    "type": {
                        "type": "string",
                        "enum": ["delta", "postgres", "jdbc"],
                        "description": "Sink backend.",
                    },
                    "schema": {"type": "string", "description": "Target schema override."},
                    "table": {"type": "string", "description": "Target table override."},
                },
            },
            # ------------------------------------------------------------------
            # Materialization (Plan 27)
            # ------------------------------------------------------------------
            "MaterializationDef": {
                "description": (
                    "Target materialization: batch table (default), incremental streaming "
                    "table with checkpoint, or Databricks materialized view. Keys are "
                    "type-specific — the loader rejects options that do not apply to the "
                    "declared type."
                ),
                "oneOf": [
                    {
                        "type": "string",
                        "enum": sorted(VALID_MATERIALIZATION_TYPES),
                        "description": "Shorthand form — defaults applied for streaming_table.",
                    },
                    {
                        "type": "object",
                        "required": ["type"],
                        "additionalProperties": False,
                        "properties": {
                            "type": {
                                "type": "string",
                                "enum": sorted(VALID_MATERIALIZATION_TYPES),
                            },
                            "trigger": {
                                "type": "string",
                                "description": (
                                    "'available_now' (default — incremental batch) or "
                                    "'interval:<duration>' (e.g. 'interval:30 seconds')."
                                ),
                            },
                            "checkpoint": {
                                "type": "string",
                                "description": (
                                    "'auto' (default — resolved per environment/sandbox) "
                                    "or an explicit checkpoint path."
                                ),
                            },
                            "write_mode": {
                                "type": "string",
                                "enum": ["append", "upsert"],
                                "description": (
                                    "append (default) or upsert — CDC Type 1 via "
                                    "foreachBatch + MERGE INTO on 'keys'."
                                ),
                            },
                            "keys": {
                                "type": "array",
                                "minItems": 1,
                                "items": {"type": "string"},
                                "description": "Merge-key columns — required with write_mode: upsert.",
                            },
                            "schedule": {
                                "type": "string",
                                "description": (
                                    "materialized_view only — refresh schedule, e.g. "
                                    "'EVERY 6 HOURS' or \"CRON '0 0 6 * * ?' AT TIME ZONE 'UTC'\"."
                                ),
                            },
                            "comment": {
                                "type": "string",
                                "description": "materialized_view only — COMMENT stored on the view.",
                            },
                            "cluster_by": {
                                "type": "array",
                                "minItems": 1,
                                "items": {"type": "string"},
                                "description": (
                                    "materialized_view only — CLUSTER BY columns "
                                    "(mutually exclusive with partition_by)."
                                ),
                            },
                            "partition_by": {
                                "type": "array",
                                "minItems": 1,
                                "items": {"type": "string"},
                                "description": (
                                    "materialized_view only — PARTITIONED BY columns "
                                    "(mutually exclusive with cluster_by)."
                                ),
                            },
                            "refresh": {
                                "type": "string",
                                "enum": sorted(VALID_MV_REFRESH_MODES),
                                "description": (
                                    "materialized_view only — 'auto' (default) refreshes on "
                                    "each run; 'never' leaves it to the declared schedule."
                                ),
                            },
                        },
                    },
                ],
            },
            # ------------------------------------------------------------------
            # Partials (Plan 25)
            # ------------------------------------------------------------------
            "PartialDef": {
                "description": "A nested YAML sub-transformation exposed under an alias.",
                "type": "object",
                "required": ["alias", "path"],
                "additionalProperties": False,
                "properties": {
                    "alias": {
                        "type": "string",
                        "description": "Name under which the child output is joinable.",
                    },
                    "path": {
                        "type": "string",
                        "description": "Child YAML path, relative to this YAML's directory.",
                    },
                },
            },
        },
    }

    return schema
