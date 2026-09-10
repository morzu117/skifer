"""Deterministic semantic-draft generation from a projected pipeline schema.

This module is intentionally pure and LLM-free: the emitted draft is a
deterministic function of ``ProjectedSchema`` plus the parsed pipeline contract
metadata.

Type policy:
- Dimension types are mapped only when a logical or physical type can be
  translated safely to a validator-supported semantic type.
- If a dimension type cannot be mapped safely, the builder omits the ``type``
  key and marks the dimension with ``needs_curation: [type]`` instead of
  silently coercing it to ``string``.
- Metrics are emitted from declarative aggregate measures only. The pipeline
  supports more aggregates than the semantic layer (``stddev``, ``variance``,
  ``sum_distinct``, ``approx_count_distinct``, ``first``, ``last``); such a
  measure is listed under ``metadata.unmapped_measures`` for curation rather
  than failing the whole draft, since the pipeline itself is perfectly valid.
- When a pipeline yields no mappable measure at all, the builder adds the
  fallback metric ``row_count`` as ``COUNT(*)`` — a fact about the table rather
  than a business metric — flagged ``needs_curation`` so a human replaces it.
"""
from __future__ import annotations

import os
from pathlib import Path
import re
from typing import Any

import yaml

from skifer.core.ir import ParsedSchema

from .output_projection import ProjectedField, ProjectedJoin, ProjectedSchema
from .persistence import write_yaml_atomic


_DIMENSION_TYPE_MAP = {
    "bool": "boolean",
    "boolean": "boolean",
    "category": "string",
    "date": "date",
    "datetime": "datetime",
    "decimal": "float",
    "double": "float",
    "float": "float",
    "identifier": "string",
    "int": "integer",
    "integer": "integer",
    "long": "integer",
    "number": "float",
    "numeric": "float",
    "string": "string",
    "text": "string",
    "timestamp": "timestamp",
}
_VALID_METRIC_TYPES = {"sum", "count_distinct", "count", "avg", "min", "max"}
_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_MANAGED_MARKER = {
    "tool": "skifer.semantic.draft_builder",
    "kind": "semantic_draft",
    "version": 1,
}


class SemanticDraftBuilder:
    """Build and atomically persist managed semantic-model drafts."""

    def __init__(self, output_dir: str = "semantic_models"):
        self.output_dir = os.path.abspath(output_dir) if not os.path.isabs(output_dir) else output_dir

    def build_draft(self, projected: ProjectedSchema, schema: ParsedSchema) -> dict[str, Any]:
        """Return the deterministic YAML payload for one managed semantic draft."""
        model_key = self._resolve_model_key(schema, projected)
        table = self._resolve_table(projected)
        projected_fields = {field.name: field for field in projected.fields}
        contract_fields = {field.name: field for field in schema.contract_output}

        explicit_dimensions = list(schema.semantic.dimensions) if schema.semantic else []
        default_time_dimension = schema.semantic.default_time_dimension if schema.semantic else None
        validated_dimensions = list(explicit_dimensions)
        if default_time_dimension and default_time_dimension not in validated_dimensions:
            validated_dimensions.append(default_time_dimension)
        self._validate_explicit_dimensions(validated_dimensions, projected_fields)

        dimension_names = self._collect_dimension_names(
            schema=schema,
            projected=projected,
            projected_fields=projected_fields,
            explicit_dimensions=explicit_dimensions,
            default_time_dimension=default_time_dimension,
        )
        dimensions = [
            self._build_dimension(projected_fields[name], contract_fields.get(name))
            for name in dimension_names
        ]
        metrics, unmapped_measures = self._build_metrics(
            projected=projected,
            contract_fields=contract_fields,
        )

        model: dict[str, Any] = {
            "name": model_key,
            "key": model_key,
            "description": schema.data_product.description if schema.data_product else "",
            "layer": self._infer_layer(table),
            "table": table,
            "dimensions": dimensions,
            "metrics": metrics,
            "metadata": {
                "source_contract_id": schema.data_product.id if schema.data_product else projected.data_product_id,
                "source_contract_version": schema.data_product.version if schema.data_product else None,
                "source_definition_hash": projected.definition_hash,
                "generated_fields": [field.name for field in projected.fields],
                # Persisted so sync (slice 0.4) can detect a grain change directly
                # instead of guessing it from the generated dimension names.
                "grain": list(projected.grain),
            },
        }
        if unmapped_measures:
            model["metadata"]["unmapped_measures"] = unmapped_measures
        relationship_candidates, relationship_candidate_rejections = self._build_relationship_candidates(
            model_key=model_key,
            projected=projected,
            schema=schema,
        )
        if relationship_candidates:
            model["metadata"]["relationship_candidates"] = relationship_candidates
        if relationship_candidate_rejections:
            model["metadata"]["relationship_candidate_rejections"] = relationship_candidate_rejections
        if schema.semantic and schema.semantic.entity:
            model["entity"] = schema.semantic.entity
        if default_time_dimension:
            model["default_time_dimension"] = default_time_dimension

        return {
            "_generated_by": dict(_MANAGED_MARKER),
            "models": [model],
        }

    def write_draft(self, projected: ProjectedSchema, schema: ParsedSchema) -> str:
        """Write the managed draft atomically under ``<output_dir>/.drafts``."""
        payload = self.build_draft(projected, schema)
        model_key = payload["models"][0]["key"]
        drafts_dir = Path(self.output_dir) / ".drafts"
        drafts_dir.mkdir(parents=True, exist_ok=True)
        target_path = drafts_dir / f"{model_key}.yaml"

        if target_path.exists():
            self._assert_managed_draft(target_path)

        return write_yaml_atomic(target_path, payload)

    @staticmethod
    def _resolve_model_key(schema: ParsedSchema, projected: ProjectedSchema) -> str:
        if schema.semantic and schema.semantic.model_key:
            return schema.semantic.model_key
        if projected.data_product_id:
            return projected.data_product_id
        if projected.target_hint:
            return projected.target_hint.replace("/", ".")
        raise ValueError(
            "[semantic.model_key] semantic draft key is missing: no semantic seed, data product "
            "or target hint is available. Declare semantic.model_key."
        )

    @staticmethod
    def _resolve_table(projected: ProjectedSchema) -> str:
        if projected.target_hint:
            return projected.target_hint
        raise ValueError(
            "[semantic.table] semantic draft target is missing: the pipeline has no deterministic "
            "sink hint. Declare sink.schema and sink.table."
        )

    @staticmethod
    def _infer_layer(table: str) -> str | None:
        parts = table.split(".")
        if len(parts) >= 2:
            return parts[-2]
        return None

    def _validate_explicit_dimensions(
        self,
        explicit_dimensions: list[str],
        projected_fields: dict[str, ProjectedField],
    ) -> None:
        for name in explicit_dimensions:
            if name not in projected_fields:
                raise ValueError(f"[semantic.dimensions] '{name}' is not a projected output.")

    def _collect_dimension_names(
        self,
        *,
        schema: ParsedSchema,
        projected: ProjectedSchema,
        projected_fields: dict[str, ProjectedField],
        explicit_dimensions: list[str],
        default_time_dimension: str | None,
    ) -> list[str]:
        dimension_names: list[str] = []
        seen: set[str] = set()

        def add(name: str) -> None:
            if name not in seen:
                seen.add(name)
                dimension_names.append(name)

        structural_candidates: list[str] = []
        if schema.aggregate is not None:
            structural_candidates.extend(schema.aggregate.group_by)
        structural_candidates.extend(projected.grain)

        declared_outputs = {field.name for field in schema.contract_output}
        for name in structural_candidates:
            field = projected_fields.get(name)
            if field is None:
                continue
            if name in declared_outputs:
                add(name)
                continue
            if default_time_dimension == name:
                add(name)
                continue
            if field.inference_status == "unknown":
                continue
            if self._map_dimension_type(field) is not None:
                add(name)

        for name in explicit_dimensions:
            add(name)
        if default_time_dimension:
            add(default_time_dimension)

        return dimension_names

    def _build_dimension(self, field: ProjectedField, contract_field) -> dict[str, Any]:
        dimension: dict[str, Any] = {
            "name": field.name,
            "sql": field.name,
        }
        semantic_type = self._map_dimension_type(field)
        if semantic_type is None:
            dimension["needs_curation"] = ["type"]
        else:
            dimension["type"] = semantic_type
        if contract_field and contract_field.description:
            dimension["description"] = contract_field.description
        if contract_field and contract_field.entity:
            dimension["entity"] = contract_field.entity
        return dimension

    def _build_metrics(
        self,
        *,
        projected: ProjectedSchema,
        contract_fields: dict[str, Any],
    ) -> tuple[list[dict[str, Any]], list[str]]:
        """Return the emitted metrics plus the names of measures left for curation."""
        metrics: list[dict[str, Any]] = []
        unmapped: list[str] = []
        for field in projected.fields:
            aggregate_func = self._aggregate_func(field)
            if aggregate_func is None:
                continue
            if aggregate_func not in _VALID_METRIC_TYPES:
                # The pipeline supports more aggregates than the semantic layer
                # (stddev, variance, sum_distinct, …). Dropping the whole draft
                # over one of them would punish a perfectly valid pipeline, so
                # the measure is recorded for curation instead.
                unmapped.append(field.name)
                continue
            metric: dict[str, Any] = {
                "name": field.name,
                "sql": field.source_fields[0] if field.source_fields else "*",
                "type": aggregate_func,
            }
            contract_field = contract_fields.get(field.name)
            if contract_field and contract_field.description:
                metric["description"] = contract_field.description
            metrics.append(metric)

        if metrics:
            return metrics, unmapped

        # SemanticValidator requires at least one metric. COUNT(*) is a fact
        # about the table rather than a business metric, so it is a safe
        # placeholder — flagged so a human replaces it before promotion.
        return [
            {
                "name": "row_count",
                "sql": "*",
                "type": "count",
                "description": "Deterministic fallback metric counting output rows.",
                "needs_curation": ["definition"],
            }
        ], unmapped

    def _build_relationship_candidates(
        self,
        *,
        model_key: str,
        projected: ProjectedSchema,
        schema: ParsedSchema,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        if not projected.joins:
            return [], []

        entity_by_source = self._index_entities_by_source(projected.fields)
        grain_sources = self._projected_grain_sources(projected)
        table_by_alias = {table.alias: table for table in schema.tables}
        model_slug = self._model_slug(model_key)

        candidates: list[dict[str, Any]] = []
        rejections: list[dict[str, Any]] = []
        for join in projected.joins:
            if join.join_type not in {"inner", "left"}:
                rejections.append(
                    self._relationship_rejection(
                        join,
                        f"join_type '{join.join_type}' is not eligible for automatic semantic proposals "
                        "(v1 supports only inner/left).",
                    )
                )
                continue

            from_entity = self._resolve_entity_name(entity_by_source, join.keys_left)
            if from_entity is None:
                rejections.append(
                    self._relationship_rejection(
                        join,
                        "left join key has no single explicit entity annotation in projected outputs.",
                    )
                )
                continue

            # `to_entity` names an entity of the TARGET model, which this pipeline
            # cannot see. It is deliberately left for curation: deriving it from
            # this model's projected outputs matched join keys by bare column
            # name, so a common name like `id` silently attributed the target
            # side to a local entity — confidently wrong.
            to_model = self._proposed_model_key(join)
            if to_model is None:
                rejections.append(
                    self._relationship_rejection(
                        join,
                        "target model key cannot be derived as a safe identifier from the joined table.",
                    )
                )
                continue

            relationship_name = f"{model_slug}_{from_entity}"
            unsafe_identifiers = [
                value
                for value in (model_slug, from_entity, to_model, relationship_name, *join.keys_left, *join.keys_right)
                if not _SAFE_IDENTIFIER.match(value)
            ]
            if unsafe_identifiers:
                rejections.append(
                    self._relationship_rejection(
                        join,
                        "unsafe identifier(s) prevent a valid proposal: "
                        + ", ".join(unsafe_identifiers),
                    )
                )
                continue

            left_evidence = self._uniqueness_evidence(
                alias=join.alias_left,
                keys=join.keys_left,
                table_by_alias=table_by_alias,
                grain_sources=grain_sources,
            )
            right_evidence = self._uniqueness_evidence(
                alias=join.alias_right,
                keys=join.keys_right,
                table_by_alias=table_by_alias,
                grain_sources=grain_sources,
            )
            cardinality = self._cardinality(left_evidence, right_evidence)

            candidate: dict[str, Any] = {
                "name": relationship_name,
                "from_entity": from_entity,
                # Suggested from the joined table's basename — the physical table
                # is recorded alongside so a human can check the guess instead of
                # having to reverse-engineer it.
                "to_model": to_model,
                "to_table": join.right_table or join.alias_right,
                "to_entity": None,
                "cardinality": cardinality,
                "join_type": join.join_type,
                "status": "proposed",
                "requires_curation": ["to_model", "to_entity"],
                "verified_by_contract": bool(left_evidence or right_evidence),
                "join_keys": {
                    "from": list(join.keys_left),
                    "to": list(join.keys_right),
                },
            }
            if left_evidence or right_evidence:
                evidence: dict[str, str] = {}
                if left_evidence:
                    evidence["from"] = left_evidence
                if right_evidence:
                    evidence["to"] = right_evidence
                candidate["evidence"] = evidence
            candidates.append(candidate)

        return candidates, rejections

    @staticmethod
    def _aggregate_func(field: ProjectedField) -> str | None:
        for transformation in field.transformations:
            if transformation.startswith("aggregate:"):
                return transformation.split(":", 1)[1]
        return None

    @staticmethod
    def _map_dimension_type(field: ProjectedField) -> str | None:
        for raw_type in (field.logical_type, field.physical_type):
            if raw_type is None:
                continue
            mapped = _DIMENSION_TYPE_MAP.get(raw_type.strip().lower())
            if mapped is not None:
                return mapped
        return None

    @staticmethod
    def _index_entities_by_source(fields: tuple[ProjectedField, ...]) -> dict[str, set[str]]:
        entity_by_source: dict[str, set[str]] = {}
        for field in fields:
            if not field.entity:
                continue
            for source_field in field.source_fields:
                entity_by_source.setdefault(source_field, set()).add(field.entity)
        return entity_by_source

    @staticmethod
    def _resolve_entity_name(
        entity_by_source: dict[str, set[str]],
        keys: tuple[str, ...],
    ) -> str | None:
        resolved: list[str] = []
        for key in keys:
            entity_names = entity_by_source.get(key)
            if not entity_names or len(entity_names) != 1:
                return None
            resolved.append(next(iter(entity_names)))
        if not resolved or len(set(resolved)) != 1:
            return None
        return resolved[0]

    @staticmethod
    def _proposed_model_key(join: ProjectedJoin) -> str | None:
        raw = (join.right_table or join.alias_right).split(".")[-1]
        if not raw or not _SAFE_IDENTIFIER.match(raw):
            return None
        return raw

    @staticmethod
    def _projected_grain_sources(projected: ProjectedSchema) -> tuple[str, ...] | None:
        if not projected.grain:
            return None
        source_fields: list[str] = []
        fields_by_name = {field.name: field for field in projected.fields}
        for grain_name in projected.grain:
            field = fields_by_name.get(grain_name)
            if field is None or len(field.source_fields) != 1:
                return None
            source_fields.append(field.source_fields[0])
        return tuple(source_fields)

    @staticmethod
    def _uniqueness_evidence(
        *,
        alias: str,
        keys: tuple[str, ...],
        table_by_alias: dict[str, Any],
        grain_sources: tuple[str, ...] | None,
    ) -> str | None:
        table = table_by_alias.get(alias)
        if table is not None and tuple(table.drop_duplicates_on) == tuple(keys):
            return "unique_check"
        if grain_sources == tuple(keys):
            return "contract_grain"
        return None

    @staticmethod
    def _cardinality(left_evidence: str | None, right_evidence: str | None) -> str:
        if left_evidence and right_evidence:
            return "one_to_one"
        if right_evidence:
            return "many_to_one"
        if left_evidence:
            return "one_to_many"
        return "unknown"

    @staticmethod
    def _relationship_rejection(join: ProjectedJoin, reason: str) -> dict[str, Any]:
        return {
            "alias_left": join.alias_left,
            "keys_left": list(join.keys_left),
            "alias_right": join.alias_right,
            "keys_right": list(join.keys_right),
            "join_type": join.join_type,
            "reason": reason,
        }

    @staticmethod
    def _model_slug(model_key: str) -> str:
        return model_key.split(".")[-1]

    @staticmethod
    def _assert_managed_draft(target_path: Path) -> None:
        try:
            with target_path.open(encoding="utf-8") as handle:
                existing = yaml.safe_load(handle) or {}
        except yaml.YAMLError as exc:
            raise ValueError(
                f"[_generated_by] '{target_path}' is not a managed semantic draft: invalid YAML "
                f"({exc}). Refusing to overwrite curated model without --promote."
            ) from exc

        marker = existing.get("_generated_by") if isinstance(existing, dict) else None
        if marker != _MANAGED_MARKER:
            raise ValueError(
                f"[_generated_by] '{target_path}' is not a managed semantic draft: marker missing "
                "or unrecognized. Refusing to overwrite curated model without --promote."
            )
