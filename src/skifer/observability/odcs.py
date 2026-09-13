"""Small, loss-aware ODCS 3.1 export for Skifer contracts (Plan 29)."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from skifer.core.ir import ParsedOwner, ParsedSchema
from skifer.observability.certification import ContractDefinition


@dataclass(frozen=True)
class OdcsExport:
    """ODCS document plus explicit notices for source metadata with no mapping."""

    document: dict[str, Any]
    warnings: tuple[str, ...]

    def __post_init__(self):
        object.__setattr__(self, "warnings", tuple(self.warnings))


@dataclass(frozen=True)
class OdcsImport:
    """Skifer YAML metadata reconstructed from an ODCS document."""

    data_product: dict[str, Any]
    contract: dict[str, Any]
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
    team = []
    if product and product.owner is not None:
        if isinstance(product.owner, str):
            team = [{"name": product.owner}]
        elif isinstance(product.owner, ParsedOwner):
            member_name = product.owner.team or product.owner.steward or product.owner.contact
            if member_name:
                member = {"name": member_name}
                if product.owner.steward:
                    member["role"] = "steward"
                team = [member]

    custom_properties = {
        "skifer.definition_hash": definition.definition_hash,
        "skifer.hash_algorithm": definition.hash_algorithm,
        "skifer.canonicalization_version": definition.canonicalization_version,
    }
    if product and product.owner_domain:
        custom_properties["skifer.domain"] = product.owner_domain

    sla_properties = []
    if schema.contract_sla:
        if schema.contract_sla.refresh_frequency:
            sla_properties.append(
                {
                    "property": "refreshFrequency",
                    "value": schema.contract_sla.refresh_frequency,
                }
            )
        if schema.contract_sla.max_latency:
            sla_properties.append(
                {"property": "latency", "value": schema.contract_sla.max_latency}
            )

    document = {
        "apiVersion": "v3.1.0",
        "kind": "DataContract",
        "id": definition.contract_id,
        "version": definition.contract_version,
        "status": schema.contract_status or definition.status.lower(),
        "description": product.description if product else None,
        "schema": {"properties": fields},
        "quality": quality,
        "team": team,
        "roles": [{"role": "reader", "access": "read"}],
        "slaProperties": sla_properties,
        "customProperties": custom_properties,
    }
    return OdcsExport(document=document, warnings=tuple(warnings))


def import_odcs_31(doc: dict[str, Any]) -> OdcsImport:
    """Reconstruct Skifer YAML metadata blocks from an ODCS 3.1 DataContract."""
    if not isinstance(doc, dict):
        raise ValueError("ODCS document must be a mapping.")
    api_version = doc.get("apiVersion")
    if not isinstance(api_version, str) or not api_version.startswith("v3."):
        raise ValueError("ODCS apiVersion must start with 'v3.'.")
    if doc.get("kind") != "DataContract":
        raise ValueError("ODCS kind must be 'DataContract'.")

    contract_id = _required_string(doc.get("id"), "id")
    version = _required_string(doc.get("version"), "version")
    warnings: list[str] = []

    data_product: dict[str, Any] = {"id": contract_id, "version": version}
    owner = _import_team(doc.get("team"), warnings)
    if owner is not None:
        data_product["owner"] = owner

    description = doc.get("description")
    if description is not None:
        if isinstance(description, str) and description.strip():
            data_product["description"] = description.strip()
        else:
            warnings.append("description is not a non-empty string and was not imported.")

    custom_properties = doc.get("customProperties")
    if custom_properties is not None:
        _import_custom_properties(custom_properties, data_product, warnings)

    contract: dict[str, Any] = {"output": _import_properties(doc.get("schema"), warnings)}
    grain = _import_quality(doc.get("quality"), contract["output"], warnings)
    if grain:
        contract["grain"] = grain

    status = doc.get("status")
    if status is not None:
        if isinstance(status, str) and status.strip():
            contract["status"] = status.strip().lower()
        else:
            warnings.append("status is not a non-empty string and was not imported.")

    sla = _import_sla(doc.get("slaProperties"), warnings)
    if sla:
        contract["sla"] = sla

    security = _import_security(doc, warnings)
    if security:
        contract["security"] = security

    _warn_unmapped_top_level(doc, warnings)
    return OdcsImport(data_product=data_product, contract=contract, warnings=tuple(warnings))


def _required_string(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"ODCS '{field}' is required and must be a non-empty string.")
    return value.strip()


def _import_properties(schema: object, warnings: list[str]) -> dict[str, dict[str, Any]]:
    if not isinstance(schema, dict):
        raise ValueError("ODCS schema must be a mapping with a properties list.")
    properties = schema.get("properties")
    if not isinstance(properties, list) or not properties:
        raise ValueError("ODCS schema.properties must be a non-empty list.")
    for key in sorted(set(schema) - {"properties"}):
        warnings.append(f"schema.{key} has no Skifer YAML mapping and was not imported.")

    output: dict[str, dict[str, Any]] = {}
    for index, prop in enumerate(properties):
        if not isinstance(prop, dict):
            warnings.append(f"schema.properties[{index}] is not a mapping and was not imported.")
            continue
        name = prop.get("name")
        if not isinstance(name, str) or not name.strip():
            warnings.append(f"schema.properties[{index}].name is missing; field was not imported.")
            continue
        field_name = name.strip()
        field: dict[str, Any] = {}
        _copy_string_property(prop, "logicalType", field, "logical_type", warnings, field_name)
        _copy_string_property(prop, "description", field, "description", warnings, field_name)
        _copy_string_property(prop, "classification", field, "classification", warnings, field_name)
        for key in sorted(set(prop) - {"name", "logicalType", "description", "classification"}):
            warnings.append(
                f"schema.properties.{field_name}.{key} has no Skifer YAML mapping and was not imported."
            )
        output[field_name] = field
    if not output:
        raise ValueError("ODCS schema.properties did not contain any importable fields.")
    return output


def _copy_string_property(
    source: dict[str, Any],
    source_key: str,
    target: dict[str, Any],
    target_key: str,
    warnings: list[str],
    field_name: str,
) -> None:
    if source_key not in source or source[source_key] is None:
        return
    value = source[source_key]
    if isinstance(value, str) and value.strip():
        target[target_key] = value.strip()
        return
    warnings.append(
        f"schema.properties.{field_name}.{source_key} is not a non-empty string and was not imported."
    )


def _import_quality(
    quality: object,
    output: dict[str, dict[str, Any]],
    warnings: list[str],
) -> list[str]:
    if quality is None:
        return []
    if not isinstance(quality, list):
        warnings.append("quality is not a list and was not imported.")
        return []

    unique_fields: list[str] = []
    for index, rule in enumerate(quality):
        if not isinstance(rule, dict):
            warnings.append(f"quality[{index}] is not a mapping and was not imported.")
            continue
        rule_type = rule.get("type")
        field = rule.get("field")
        if not isinstance(rule_type, str) or not isinstance(field, str) or not field.strip():
            warnings.append(f"quality[{index}] is not a supported single-field rule and was not imported.")
            continue
        field_name = field.strip()
        if field_name not in output:
            warnings.append(
                f"quality[{index}] references unknown field '{field_name}' and was not imported."
            )
            continue
        normalized_type = rule_type.strip().lower()
        if normalized_type == "required":
            output[field_name]["required"] = True
        elif normalized_type == "unique":
            output[field_name]["unique"] = True
            unique_fields.append(field_name)
        else:
            warnings.append(
                f"quality[{index}] type '{rule_type}' has no Skifer YAML mapping and was not imported."
            )
            continue
        for key in sorted(set(rule) - {"type", "field"}):
            warnings.append(f"quality[{index}].{key} has no Skifer YAML mapping and was not imported.")

    distinct_unique = sorted(set(unique_fields), key=unique_fields.index)
    if len(distinct_unique) == 1:
        return distinct_unique
    if len(distinct_unique) > 1:
        warnings.append(
            "Multiple unique quality fields cannot be imported as a single-field contract.grain."
        )
    return []


def _import_team(team: object, warnings: list[str]) -> str | dict[str, str] | None:
    if team is None:
        return None
    if not isinstance(team, list):
        warnings.append("team is not a list and was not imported.")
        return None
    members = [member for member in team if isinstance(member, dict)]
    for index, member in enumerate(team):
        if not isinstance(member, dict):
            warnings.append(f"team[{index}] is not a mapping and was not imported.")
    if not members:
        return None

    if len(members) == 1:
        name = members[0].get("name")
        if isinstance(name, str) and name.strip() and set(members[0]) <= {"name"}:
            return name.strip()

    owner: dict[str, str] = {}
    for index, member in enumerate(members):
        name = member.get("name")
        if not isinstance(name, str) or not name.strip():
            warnings.append(f"team[{index}].name is missing and was not imported.")
            continue
        role = member.get("role")
        normalized_role = role.strip().lower() if isinstance(role, str) else ""
        key = "team"
        if normalized_role == "steward":
            key = "steward"
        elif normalized_role == "contact":
            key = "contact"
        elif normalized_role in {"", "owner", "team"}:
            key = "team"
        else:
            warnings.append(f"team[{index}].role '{role}' has no Skifer owner mapping.")
            if "team" in owner:
                continue
        owner.setdefault(key, name.strip())
        for extra in sorted(set(member) - {"name", "role"}):
            warnings.append(f"team[{index}].{extra} has no Skifer YAML mapping and was not imported.")
    return owner or None


def _import_sla(sla_properties: object, warnings: list[str]) -> dict[str, str]:
    if sla_properties is None:
        return {}
    if not isinstance(sla_properties, list):
        warnings.append("slaProperties is not a list and was not imported.")
        return {}

    sla: dict[str, str] = {}
    mapping = {
        "refreshFrequency": "refresh_frequency",
        "refresh_frequency": "refresh_frequency",
        "latency": "max_latency",
        "maxLatency": "max_latency",
        "max_latency": "max_latency",
    }
    for index, prop in enumerate(sla_properties):
        if not isinstance(prop, dict):
            warnings.append(f"slaProperties[{index}] is not a mapping and was not imported.")
            continue
        name = prop.get("property")
        value = prop.get("value")
        if not isinstance(name, str) or not isinstance(value, str) or not value.strip():
            warnings.append(f"slaProperties[{index}] is not a supported property/value pair.")
            continue
        target = mapping.get(name.strip())
        if target is None:
            warnings.append(
                f"slaProperties[{index}].property '{name}' has no Skifer YAML mapping and was not imported."
            )
            continue
        sla[target] = value.strip()
        for key in sorted(set(prop) - {"property", "value"}):
            warnings.append(f"slaProperties[{index}].{key} has no Skifer YAML mapping and was not imported.")
    return sla


def _import_custom_properties(
    custom_properties: object,
    data_product: dict[str, Any],
    warnings: list[str],
) -> None:
    if not isinstance(custom_properties, dict):
        warnings.append("customProperties is not a mapping and was not imported.")
        return
    for key, value in custom_properties.items():
        if key == "skifer.domain":
            if isinstance(value, str) and value.strip():
                data_product["domain"] = value.strip()
            else:
                warnings.append("customProperties.skifer.domain is not a non-empty string.")
        else:
            warnings.append(f"customProperties.{key} has no Skifer YAML mapping and was not imported.")


def _import_security(doc: dict[str, Any], warnings: list[str]) -> dict[str, str]:
    security: dict[str, str] = {}
    raw_security = doc.get("security")
    if raw_security is not None:
        if isinstance(raw_security, dict):
            _copy_security_mapping(raw_security, security, warnings, "security")
        else:
            warnings.append("security is not a mapping and was not imported.")

    raw_properties = doc.get("securityProperties")
    if raw_properties is not None:
        if isinstance(raw_properties, list):
            for index, prop in enumerate(raw_properties):
                if not isinstance(prop, dict):
                    warnings.append(f"securityProperties[{index}] is not a mapping and was not imported.")
                    continue
                name = prop.get("property")
                value = prop.get("value")
                if not isinstance(name, str) or not isinstance(value, str) or not value.strip():
                    warnings.append(f"securityProperties[{index}] is not a supported property/value pair.")
                    continue
                target = _security_target_key(name)
                if target is None:
                    warnings.append(
                        f"securityProperties[{index}].property '{name}' has no Skifer YAML mapping."
                    )
                    continue
                security[target] = value.strip()
                for key in sorted(set(prop) - {"property", "value"}):
                    warnings.append(
                        f"securityProperties[{index}].{key} has no Skifer YAML mapping and was not imported."
                    )
        else:
            warnings.append("securityProperties is not a list and was not imported.")
    return security


def _copy_security_mapping(
    source: dict[str, Any],
    target: dict[str, str],
    warnings: list[str],
    prefix: str,
) -> None:
    for key, value in source.items():
        target_key = _security_target_key(key)
        if target_key is None:
            warnings.append(f"{prefix}.{key} has no Skifer YAML mapping and was not imported.")
            continue
        if isinstance(value, str) and value.strip():
            target[target_key] = value.strip()
        else:
            warnings.append(f"{prefix}.{key} is not a non-empty string and was not imported.")


def _security_target_key(key: str) -> str | None:
    return {
        "level": "level",
        "dataClassification": "level",
        "classification": "level",
        "accessPolicy": "access_policy",
        "access_policy": "access_policy",
        "rowFilter": "access_policy",
        "rowFilters": "access_policy",
    }.get(key)


def _warn_unmapped_top_level(doc: dict[str, Any], warnings: list[str]) -> None:
    mapped = {
        "apiVersion",
        "kind",
        "id",
        "version",
        "status",
        "description",
        "schema",
        "quality",
        "team",
        "slaProperties",
        "security",
        "securityProperties",
        "customProperties",
    }
    for key in sorted(set(doc) - mapped):
        warnings.append(f"{key} has no Skifer YAML mapping and was not imported.")
