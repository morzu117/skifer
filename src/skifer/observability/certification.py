"""Canonical, versioned identities for declarative data contracts (Plan 29)."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
import json
import re

from skifer.core.ir import ParsedSchema


CANONICALIZATION_VERSION = 1
HASH_ALGORITHM = "sha256"
_SEMVER_RE = re.compile(r"^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?$")


@dataclass(frozen=True)
class ContractDefinition:
    """Append-only contract definition suitable for a certification store."""

    contract_id: str
    contract_version: str
    definition_hash: str
    canonical_json: str
    data_product_id: str
    owner: str | None
    hash_algorithm: str = HASH_ALGORITHM
    canonicalization_version: int = CANONICALIZATION_VERSION
    status: str = "DRAFT"
    created_at: datetime | None = None


def canonicalize_contract(schema: ParsedSchema) -> ContractDefinition:
    """Build a stable definition from a parsed pipeline contract.

    Field ordering is retained because it can be meaningful to downstream model
    generators. Mapping keys are sorted in the emitted JSON. Ownership and human
    descriptions are persisted alongside the definition but deliberately do not
    alter its hash: documentation-only updates must not invalidate a certification.
    """
    if schema.data_product is None:
        raise ValueError("A certification contract requires a 'data_product' block.")
    if not _SEMVER_RE.fullmatch(schema.data_product.version):
        raise ValueError(
            f"Contract version '{schema.data_product.version}' is not a valid semantic version."
        )
    if not schema.contract_output:
        raise ValueError("A certification contract requires a non-empty 'contract.output' block.")

    payload = {
        "canonicalization_version": CANONICALIZATION_VERSION,
        "data_product": {
            "id": schema.data_product.id,
            "version": schema.data_product.version,
        },
        "contract": {
            "grain": list(schema.contract_grain),
            "output": [
                {
                    "name": field.name,
                    "logical_type": field.logical_type,
                    "required": field.required,
                    "unique": field.unique,
                    "classification": field.classification,
                    "entity": field.entity,
                }
                for field in schema.contract_output
            ],
        },
        "semantic": (
            {
                "model_key": schema.semantic.model_key,
                "entity": schema.semantic.entity,
                "default_time_dimension": schema.semantic.default_time_dimension,
                "dimensions": list(schema.semantic.dimensions),
            }
            if schema.semantic
            else None
        ),
    }
    canonical_json = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    definition_hash = sha256(canonical_json.encode("utf-8")).hexdigest()
    return ContractDefinition(
        contract_id=schema.data_product.id,
        contract_version=schema.data_product.version,
        definition_hash=definition_hash,
        canonical_json=canonical_json,
        data_product_id=schema.data_product.id,
        owner=schema.data_product.owner_label,
        created_at=datetime.now(timezone.utc),
    )
