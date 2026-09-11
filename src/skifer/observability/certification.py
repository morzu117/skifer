"""Canonical, versioned identities for declarative data contracts (Plan 29)."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
import json
import re

from skifer.core.constants import CLASSIFICATION_RANK
from skifer.core.ir import ParsedSchema
from skifer.observability.checks import FreshnessCheck


CANONICALIZATION_VERSION = 2
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


@dataclass(frozen=True)
class ContractDiff:
    """Governance-facing delta between two parsed contract definitions."""

    added: tuple[str, ...] = ()
    removed: tuple[str, ...] = ()
    retyped: tuple[tuple[str, str, str], ...] = ()
    required_changed: tuple[tuple[str, bool, bool], ...] = ()
    classification_changed: tuple[tuple[str, str, str], ...] = ()
    sla_changed: bool = False
    breaking: bool = False


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
            "sla": (
                {
                    "refresh_frequency": schema.contract_sla.refresh_frequency,
                    "max_latency": schema.contract_sla.max_latency,
                }
                if schema.contract_sla
                else None
            ),
            "security": (
                {
                    "level": schema.contract_security.level,
                    "access_policy": schema.contract_security.access_policy,
                }
                if schema.contract_security
                else None
            ),
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


def diff_contracts(a: ParsedSchema, b: ParsedSchema) -> ContractDiff:
    """Compare two contracts at ``contract.output`` and SLA level.

    ``a`` is the old contract and ``b`` is the new one. Breaking changes are
    removals, retypes, required hardening, classification downgrades, SLA
    relaxation, and any changed SLA that cannot be compared safely.
    """
    fields_a = {field.name: field for field in a.contract_output}
    fields_b = {field.name: field for field in b.contract_output}

    added = tuple(field.name for field in b.contract_output if field.name not in fields_a)
    removed = tuple(field.name for field in a.contract_output if field.name not in fields_b)

    retyped: list[tuple[str, str, str]] = []
    required_changed: list[tuple[str, bool, bool]] = []
    classification_changed: list[tuple[str, str, str]] = []
    required_hardened = False
    classification_downgraded = False

    for field in a.contract_output:
        next_field = fields_b.get(field.name)
        if next_field is None:
            continue

        type_a = field.logical_type or ""
        type_b = next_field.logical_type or ""
        if type_a != type_b:
            retyped.append((field.name, type_a, type_b))

        required_a = bool(field.required)
        required_b = bool(next_field.required)
        if required_a != required_b:
            required_changed.append((field.name, required_a, required_b))
            required_hardened = required_hardened or (not required_a and required_b)

        class_a = field.classification or "public"
        class_b = next_field.classification or "public"
        if class_a != class_b:
            classification_changed.append((field.name, class_a, class_b))
            classification_downgraded = classification_downgraded or (
                CLASSIFICATION_RANK.get(class_b, 0) < CLASSIFICATION_RANK.get(class_a, 0)
            )

    sla_changed, sla_breaking = _diff_sla_breaking(a, b)
    breaking = bool(
        removed
        or retyped
        or required_hardened
        or classification_downgraded
        or sla_breaking
    )
    return ContractDiff(
        added=added,
        removed=removed,
        retyped=tuple(retyped),
        required_changed=tuple(required_changed),
        classification_changed=tuple(classification_changed),
        sla_changed=sla_changed,
        breaking=breaking,
    )


def _diff_sla_breaking(a: ParsedSchema, b: ParsedSchema) -> tuple[bool, bool]:
    old_sla = _sla_tuple(a)
    new_sla = _sla_tuple(b)
    if old_sla == new_sla:
        return False, False

    old_refresh, old_latency = old_sla
    new_refresh, new_latency = new_sla
    if old_refresh != new_refresh:
        return True, True
    if old_latency is None or new_latency is None:
        return True, True
    try:
        old_delay = FreshnessCheck._parse_delay(old_latency)
        new_delay = FreshnessCheck._parse_delay(new_latency)
    except (TypeError, ValueError):
        return True, True
    return True, new_delay > old_delay


def _sla_tuple(schema: ParsedSchema) -> tuple[str | None, str | None]:
    if schema.contract_sla is None:
        return None, None
    return schema.contract_sla.refresh_frequency, schema.contract_sla.max_latency
