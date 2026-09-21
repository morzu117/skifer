"""Canonical, versioned identities for declarative data contracts (Plan 29)."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
import json
import re

from skifer.core.constants import CLASSIFICATION_RANK
from skifer.core.ir import ParsedSchema
from skifer.core.ir import ParsedOutputField, ParsedSecurity, ParsedSla
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
    unique_changed: tuple[tuple[str, bool, bool], ...] = ()
    entity_changed: tuple[tuple[str, str, str], ...] = ()
    grain_changed: tuple[tuple[str, ...], tuple[str, ...]] | None = None
    sla_changed: bool = False
    security_changed: bool = False
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


def schema_from_definition(definition: ContractDefinition) -> ParsedSchema:
    """Rebuild the contract fields needed for governance comparisons."""
    try:
        payload = json.loads(definition.canonical_json)
    except (TypeError, json.JSONDecodeError) as exc:
        raise ValueError("Contract definition canonical JSON must be valid JSON.") from exc
    if not isinstance(payload, dict):
        raise ValueError("Contract definition payload must be an object.")
    contract = payload.get("contract")
    if not isinstance(contract, dict) or not isinstance(contract.get("output"), list):
        raise ValueError("Contract definition must contain a contract.output list.")

    output = []
    for index, field in enumerate(contract["output"]):
        if not isinstance(field, dict):
            raise ValueError(f"Contract output field at index {index} must be an object.")
        name = field.get("name")
        if not isinstance(name, str) or not name:
            raise ValueError(
                f"Contract output field at index {index} must have a non-empty string name."
            )
        output.append(
            ParsedOutputField(
                name=name,
                logical_type=field.get("logical_type"),
                required=field.get("required"),
                unique=field.get("unique"),
                classification=field.get("classification"),
                entity=field.get("entity"),
            )
        )

    raw_sla = contract.get("sla")
    if raw_sla is not None and not isinstance(raw_sla, dict):
        raise ValueError("Contract definition contract.sla must be an object or null.")
    sla = (
        ParsedSla(
            refresh_frequency=raw_sla.get("refresh_frequency"),
            max_latency=raw_sla.get("max_latency"),
        )
        if raw_sla is not None
        else None
    )

    # `grain` and `security` are in the hashed payload, so two definitions that
    # differ only there are correctly two identities. Dropping them here made
    # them invisible to every comparison built on this rebuild — including the
    # breaking-change alert at publication, which reconstructs both sides this
    # way and so never saw a downgraded security level or a redefined grain.
    raw_grain = contract.get("grain", [])
    if not isinstance(raw_grain, list) or not all(isinstance(k, str) for k in raw_grain):
        raise ValueError("Contract definition contract.grain must be a list of strings.")

    raw_security = contract.get("security")
    if raw_security is not None and not isinstance(raw_security, dict):
        raise ValueError("Contract definition contract.security must be an object or null.")
    security = (
        ParsedSecurity(
            level=raw_security.get("level"),
            access_policy=raw_security.get("access_policy"),
        )
        if raw_security is not None
        else None
    )

    return ParsedSchema(
        contract_output=output,
        contract_grain=list(raw_grain),
        contract_sla=sla,
        contract_security=security,
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
    unique_changed: list[tuple[str, bool, bool]] = []
    entity_changed: list[tuple[str, str, str]] = []
    required_hardened = False
    unique_hardened = False
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

        # A withdrawn or newly imposed uniqueness guarantee is a contractual
        # change: consumers use it as a merge or join grain. Tightening it is
        # what breaks a producer, exactly as for `required`.
        unique_a = bool(field.unique)
        unique_b = bool(next_field.unique)
        if unique_a != unique_b:
            unique_changed.append((field.name, unique_a, unique_b))
            unique_hardened = unique_hardened or (not unique_a and unique_b)

        entity_a = field.entity or ""
        entity_b = next_field.entity or ""
        if entity_a != entity_b:
            entity_changed.append((field.name, entity_a, entity_b))

        class_a = field.classification or "public"
        class_b = next_field.classification or "public"
        if class_a != class_b:
            classification_changed.append((field.name, class_a, class_b))
            classification_downgraded = classification_downgraded or (
                CLASSIFICATION_RANK.get(class_b, 0) < CLASSIFICATION_RANK.get(class_a, 0)
            )

    sla_changed, sla_breaking = _diff_sla_breaking(a, b)
    security_changed, security_breaking = _diff_security_breaking(a, b)
    # The grain is what one row means. Changing it changes every row count and
    # every join downstream, so there is no non-breaking direction to it.
    grain_a, grain_b = tuple(a.contract_grain), tuple(b.contract_grain)
    grain_changed = (grain_a, grain_b) if grain_a != grain_b else None
    breaking = bool(
        removed
        or retyped
        or required_hardened
        or unique_hardened
        or classification_downgraded
        or grain_changed
        or sla_breaking
        or security_breaking
    )
    return ContractDiff(
        added=added,
        removed=removed,
        retyped=tuple(retyped),
        required_changed=tuple(required_changed),
        classification_changed=tuple(classification_changed),
        unique_changed=tuple(unique_changed),
        entity_changed=tuple(entity_changed),
        grain_changed=grain_changed,
        sla_changed=sla_changed,
        security_changed=security_changed,
        breaking=breaking,
    )


def _diff_security_breaking(a: ParsedSchema, b: ParsedSchema) -> tuple[bool, bool]:
    """Report a security change, and whether it can be shown to be safe.

    `security.level` is a free string — only the block's keys are validated — so
    two levels are comparable only when both are known taxonomy levels. Anything
    else, including every `access_policy` edit, cannot be compared: a row filter
    is an expression, not a rank. Treating what cannot be compared as breaking is
    the rule this module already applies to an SLA, and the safe direction here:
    the alternative is a silent relaxation of access.
    """
    old = _security_tuple(a)
    new = _security_tuple(b)
    if old == new:
        return False, False

    old_level, old_policy = old
    new_level, new_policy = new
    if old_policy != new_policy:
        return True, True
    if old_level in CLASSIFICATION_RANK and new_level in CLASSIFICATION_RANK:
        return True, CLASSIFICATION_RANK[new_level] < CLASSIFICATION_RANK[old_level]
    return True, True


def _security_tuple(schema: ParsedSchema) -> tuple[str | None, str | None]:
    security = schema.contract_security
    if security is None:
        return None, None
    return security.level, security.access_policy


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
