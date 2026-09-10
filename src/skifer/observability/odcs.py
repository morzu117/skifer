"""Small, loss-aware ODCS 3.1 export for Skifer contracts (Plan 29)."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from skifer.core.ir import ParsedSchema
from skifer.observability.certification import ContractDefinition


@dataclass(frozen=True)
class OdcsExport:
    """ODCS document plus explicit notices for source metadata with no mapping."""

    document: dict[str, Any]
    warnings: tuple[str, ...]

    def __post_init__(self):
        object.__setattr__(self, "warnings", tuple(self.warnings))


def export_odcs_31(schema: ParsedSchema, definition: ContractDefinition) -> OdcsExport:
    """Export the supported contract surface without silently discarding metadata."""
    warnings: list[str] = []
    fields = []
    quality = []
    for field in schema.contract_output:
        fields.append(
            {
                "name": field.name,
                "logicalType": field.logical_type or "unknown",
                "description": field.description,
                "classification": field.classification,
            }
        )
        if field.required:
            quality.append({"type": "required", "field": field.name})
        if field.unique:
            quality.append({"type": "unique", "field": field.name})
        if field.entity:
            warnings.append(
                f"contract.output.{field.name}.entity has no ODCS 3.1 mapping and was not exported."
            )

    if not schema.contract_grain:
        warnings.append("contract.grain is absent; no primary-key quality rule was exported.")
    elif len(schema.contract_grain) == 1:
        quality.append({"type": "unique", "field": schema.contract_grain[0]})
    else:
        warnings.append("Composite contract.grain has no portable ODCS 3.1 quality mapping.")

    if schema.semantic is not None:
        warnings.append("semantic seed metadata has no ODCS 3.1 mapping and was not exported.")

    product = schema.data_product
    document = {
        "apiVersion": "v3.1.0",
        "kind": "DataContract",
        "id": definition.contract_id,
        "version": definition.contract_version,
        "status": definition.status.lower(),
        "description": product.description if product else None,
        "schema": {"properties": fields},
        "quality": quality,
        "team": [{"name": definition.owner}] if definition.owner else [],
        "roles": [{"role": "reader", "access": "read"}],
        "slaProperties": [],
        "customProperties": {
            "skifer.definition_hash": definition.definition_hash,
            "skifer.hash_algorithm": definition.hash_algorithm,
            "skifer.canonicalization_version": definition.canonicalization_version,
        },
    }
    return OdcsExport(document=document, warnings=tuple(warnings))
