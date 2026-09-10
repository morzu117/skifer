"""Static, Spark-free projection of a pipeline's final output (Plan 29)."""
from __future__ import annotations

from dataclasses import asdict, dataclass
from hashlib import sha256
import json
from typing import Literal

from skifer.core.ir import ParsedColumnSpec, ParsedOutputField, ParsedSchema


InferenceStatus = Literal["known", "declared", "unknown"]


@dataclass(frozen=True)
class ProjectedField:
    """One final field inferred from the declarative pipeline, never from Spark."""

    name: str
    physical_type: str | None
    logical_type: str | None
    source_fields: tuple[str, ...]
    transformations: tuple[str, ...]
    entity: str | None
    inference_status: InferenceStatus
    inference_reason: str | None

    def __post_init__(self):
        object.__setattr__(self, "source_fields", tuple(self.source_fields))
        object.__setattr__(self, "transformations", tuple(self.transformations))


@dataclass(frozen=True)
class ProjectedJoin:
    """One physical join carried forward for semantic relationship proposals."""

    alias_left: str
    keys_left: tuple[str, ...]
    alias_right: str
    keys_right: tuple[str, ...]
    join_type: str
    left_table: str | None
    right_table: str | None

    def __post_init__(self):
        object.__setattr__(self, "keys_left", tuple(self.keys_left))
        object.__setattr__(self, "keys_right", tuple(self.keys_right))


@dataclass(frozen=True)
class ProjectedSchema:
    """Deterministic, potentially partial description of a pipeline output."""

    data_product_id: str | None
    target_hint: str | None
    fields: tuple[ProjectedField, ...]
    joins: tuple[ProjectedJoin, ...]
    grain: tuple[str, ...]
    definition_hash: str
    is_complete: bool
    incomplete_reason: str | None = None

    def __post_init__(self):
        object.__setattr__(self, "fields", tuple(self.fields))
        object.__setattr__(self, "joins", tuple(self.joins))
        object.__setattr__(self, "grain", tuple(self.grain))


_CAST_TYPES = {
    "double": "double",
    "float": "double",
    "decimal": "decimal",
    "int": "int",
    "integer": "int",
    "long": "long",
    "bigint": "long",
    "string": "string",
    "date": "date",
    "timestamp": "timestamp",
    "boolean": "boolean",
}
_AGGREGATE_TYPES = {
    "count": "long",
    "count_distinct": "long",
    "approx_count_distinct": "long",
    "avg": "double",
    "stddev": "double",
    "variance": "double",
}


class OutputProjector:
    """Project an immutable :class:`ParsedSchema` without opening a Spark session."""

    def project(self, schema: ParsedSchema) -> ProjectedSchema:
        """Return final fields, provenance diagnostics and a canonical definition hash."""
        declarations = {field.name: field for field in schema.contract_output}
        joins = self._project_joins(schema)

        if schema.aggregate is not None:
            fields = self._project_aggregate(schema, declarations)
            complete = True
            incomplete_reason = None
        elif schema.select_final:
            fields = self._project_select_final(schema, declarations)
            complete = True
            incomplete_reason = None
        elif schema.keep_all_columns:
            fields = [self._project_column(spec, declarations.get(spec.target)) for spec in schema.add_columns]
            complete = False
            incomplete_reason = (
                "keep_all_columns forwards source fields that cannot be enumerated without a catalog."
            )
        else:
            fields = []
            complete = False
            incomplete_reason = "The pipeline has no statically declared final projection."

        if schema.business_rules:
            rule_names = ", ".join(schema.business_rules)
            fields = [
                ProjectedField(
                    name=field.name,
                    physical_type=None,
                    logical_type=field.logical_type,
                    source_fields=field.source_fields,
                    transformations=field.transformations,
                    entity=field.entity,
                    inference_status="unknown",
                    inference_reason=(
                        f"Python business rule(s) ({rule_names}) are not statically introspectable."
                    ),
                )
                for field in fields
            ]

        target_hint = self._target_hint(schema)
        grain = tuple(schema.contract_grain)
        payload = {
            "data_product_id": schema.data_product.id if schema.data_product else None,
            "target_hint": target_hint,
            "fields": [asdict(field) for field in fields],
            "joins": [asdict(join) for join in joins],
            "grain": grain,
            "is_complete": complete,
            "incomplete_reason": incomplete_reason,
        }
        definition_hash = sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":"), default=list).encode("utf-8")
        ).hexdigest()
        return ProjectedSchema(
            data_product_id=payload["data_product_id"],
            target_hint=target_hint,
            fields=tuple(fields),
            joins=tuple(joins),
            grain=grain,
            definition_hash=definition_hash,
            is_complete=complete,
            incomplete_reason=incomplete_reason,
        )

    @staticmethod
    def _target_hint(schema: ParsedSchema) -> str | None:
        sink = schema.sink or {}
        if sink.get("schema") and sink.get("table"):
            return f"{sink['schema']}.{sink['table']}"
        return sink.get("table")

    def _project_aggregate(
        self, schema: ParsedSchema, declarations: dict[str, ParsedOutputField]
    ) -> list[ProjectedField]:
        assert schema.aggregate is not None
        fields = [
                self._apply_declaration(
                    name=name,
                    physical_type=None,
                    source_fields=(name,),
                    transformations=("group_by",),
                    entity=declarations.get(name).entity if declarations.get(name) else None,
                    status="unknown",
                    reason="Group-key type requires source catalog metadata.",
                    declaration=declarations.get(name),
                )
            for name in schema.aggregate.group_by
        ]
        for measure in schema.aggregate.measures:
            physical_type = _AGGREGATE_TYPES.get(measure.func)
            fields.append(
                self._apply_declaration(
                    name=measure.target,
                    physical_type=physical_type,
                    source_fields=() if measure.source == "*" else (measure.source,),
                    transformations=(f"aggregate:{measure.func}",),
                    entity=declarations.get(measure.target).entity if declarations.get(measure.target) else None,
                    status="known" if physical_type else "unknown",
                    reason=None if physical_type else (
                        f"Aggregate '{measure.func}' preserves an unknown source type."
                    ),
                    declaration=declarations.get(measure.target),
                )
            )
        return fields

    def _project_select_final(
        self, schema: ParsedSchema, declarations: dict[str, ParsedOutputField]
    ) -> list[ProjectedField]:
        derived = {spec.target: spec for spec in schema.add_columns}
        fields: list[ProjectedField] = []
        for spec in schema.select_final:
            field = self._project_column(spec, declarations.get(spec.target))
            source_spec = derived.get(spec.source or "")
            if source_spec is not None:
                upstream = self._project_column(source_spec, None)
                field = ProjectedField(
                    name=field.name,
                    physical_type=field.physical_type or upstream.physical_type,
                    logical_type=field.logical_type,
                    source_fields=upstream.source_fields,
                    transformations=upstream.transformations + field.transformations,
                    inference_status=field.inference_status,
                    inference_reason=field.inference_reason or upstream.inference_reason,
                )
            fields.append(field)
        return fields

    def _project_column(
        self, spec: ParsedColumnSpec, declaration: ParsedOutputField | None
    ) -> ProjectedField:
        transformations = tuple(self._op_label(op.name, op.args) for op in spec.ops)
        source_fields = (spec.source,) if spec.source else ()
        physical_type: str | None = None
        status: InferenceStatus = "unknown"
        reason: str | None = "Source-column type requires catalog metadata."

        if spec.is_conditional:
            transformations = tuple(
                self._op_label(item.condition.name, item.condition.args)
                + " → "
                + self._op_label(item.then.name, item.then.args)
                for item in spec.when_chain
            )
            if spec.otherwise:
                transformations += (self._op_label(spec.otherwise.name, spec.otherwise.args),)
            reason = "Conditional branches may have heterogeneous result types."
        else:
            for op in spec.ops:
                if op.name == "expr":
                    physical_type = None
                    status = "unknown"
                    reason = "expr: is raw SQL and cannot be safely inferred."
                    break
                if op.name == "cast" and op.args:
                    physical_type = _CAST_TYPES.get(op.args[0].lower())
                    if physical_type:
                        status, reason = "known", None
                elif op.name in {"length", "ceil"}:
                    physical_type, status, reason = "long", "known", None
                elif op.name in {"upper", "lower", "trim", "split", "substring"}:
                    physical_type, status, reason = "string", "known", None
                elif op.name == "to_date":
                    physical_type, status, reason = "date", "known", None
                elif op.name == "lit":
                    physical_type, status, reason = "string", "known", None
                elif op.name in {"coalesce", "nvl"}:
                    physical_type, status = None, "unknown"
                    reason = "coalesce/nvl may combine heterogeneous source and literal types."

        return self._apply_declaration(
            name=spec.target,
            physical_type=physical_type,
            source_fields=source_fields,
            transformations=transformations,
            entity=declaration.entity if declaration else None,
            status=status,
            reason=reason,
            declaration=declaration,
        )

    @staticmethod
    def _project_joins(schema: ParsedSchema) -> list[ProjectedJoin]:
        alias_to_table = {table.alias: table.name for table in schema.tables}
        return [
            ProjectedJoin(
                alias_left=join.alias_left,
                keys_left=tuple(join.keys_left),
                alias_right=join.alias_right,
                keys_right=tuple(join.keys_right),
                join_type=join.join_type,
                left_table=alias_to_table.get(join.alias_left),
                right_table=alias_to_table.get(join.alias_right),
            )
            for join in schema.joins
        ]

    @staticmethod
    def _op_label(name: str, args: tuple[str, ...]) -> str:
        return f"{name}:{','.join(args)}" if args else name

    @staticmethod
    def _apply_declaration(
        *,
        name: str,
        physical_type: str | None,
        source_fields: tuple[str, ...],
        transformations: tuple[str, ...],
        entity: str | None,
        status: InferenceStatus,
        reason: str | None,
        declaration: ParsedOutputField | None,
    ) -> ProjectedField:
        if declaration and declaration.logical_type:
            if status == "unknown":
                status = "declared"
            reason = reason or "Logical type is declared by contract."
        return ProjectedField(
            name=name,
            physical_type=physical_type,
            logical_type=declaration.logical_type if declaration else None,
            source_fields=source_fields,
            transformations=transformations,
            entity=entity,
            inference_status=status,
            inference_reason=reason,
        )
