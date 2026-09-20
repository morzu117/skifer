"""Plan 29 contract identity tests."""
from datetime import datetime, timezone
from hashlib import sha256
import json

import pytest

from skifer.core.ir import parse_to_ir
from skifer.core.schema_loader import parse_schema
from skifer.observability.certification import ContractDefinition, canonicalize_contract
from skifer.observability.certification_store import SqliteCertificationStore


def _contract(yaml_text: str):
    return canonicalize_contract(parse_to_ir(parse_schema(yaml_text)))


def _v1_canonical_json(yaml_text: str) -> str:
    schema = parse_to_ir(parse_schema(yaml_text))
    payload = {
        "canonicalization_version": 1,
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
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


BASE_YAML = """
data_product:
  id: sales.orders
  version: 1.2.3
  owner: sales-data
  description: Orders for reporting
contract:
  grain: [order_id]
  output:
    order_id: {logical_type: identifier, required: true, unique: true}
    net_revenue: {logical_type: currency, classification: internal}
semantic:
  model_key: orders
  entity: order
  dimensions: [order_id]
tables: [{name: silver.orders}]
select_final: [[id, order_id], [amount, net_revenue]]
"""


def test_contract_identity_is_canonical_and_stable():
    first = _contract(BASE_YAML)
    second = _contract(BASE_YAML)

    assert first.definition_hash == second.definition_hash
    assert first.hash_algorithm == "sha256"
    assert first.canonicalization_version == 2
    assert json.loads(first.canonical_json)["contract"]["grain"] == ["order_id"]


def test_classification_slice_does_not_change_existing_fixture_hash():
    contract = _contract(BASE_YAML)

    assert contract.definition_hash == (
        "36287249d14bec0b9b783b5e29f86c6599b14db58f45fbf12f91063e3f0b097f"
    )
    assert contract.canonicalization_version == 2


def test_canonicalization_version_is_2():
    contract = _contract(BASE_YAML)

    assert contract.canonicalization_version == 2
    assert json.loads(contract.canonical_json)["canonicalization_version"] == 2


@pytest.mark.parametrize(
    "replacement",
    [
        ("logical_type: currency", "logical_type: decimal"),
        ("grain: [order_id]", "grain: [order_id, net_revenue]"),
        ("dimensions: [order_id]", "dimensions: [net_revenue]"),
    ],
)
def test_contract_business_definition_changes_hash(replacement):
    baseline = _contract(BASE_YAML)
    changed = _contract(BASE_YAML.replace(*replacement))

    assert changed.definition_hash != baseline.definition_hash


def test_documentation_and_owner_do_not_change_definition_hash():
    baseline = _contract(BASE_YAML)
    changed = _contract(
        BASE_YAML.replace("owner: sales-data", "owner: platform-team").replace(
            "description: Orders for reporting", "description: Clarified human documentation"
        )
    )

    assert changed.definition_hash == baseline.definition_hash
    assert changed.owner == "platform-team"


def test_status_change_does_not_change_hash():
    active = _contract(BASE_YAML.replace("contract:\n", "contract:\n  status: active\n"))
    deprecated = _contract(BASE_YAML.replace("contract:\n", "contract:\n  status: deprecated\n"))

    assert deprecated.definition_hash == active.definition_hash
    assert "status" not in json.loads(active.canonical_json)["contract"]


def test_reviewers_and_dates_out_of_hash():
    baseline = _contract(BASE_YAML)
    lifecycle = _contract(
        BASE_YAML.replace(
            "contract:\n",
            "contract:\n"
            "  reviewers: [alice@example.com, bob@example.com]\n"
            "  effective_from: \"2026-01-01\"\n"
            "  effective_until: \"2026-12-31\"\n",
        )
    )

    assert lifecycle.definition_hash == baseline.definition_hash
    payload = json.loads(lifecycle.canonical_json)["contract"]
    assert "reviewers" not in payload
    assert "effective_from" not in payload
    assert "effective_until" not in payload


def test_sla_change_produces_new_hash():
    baseline = _contract(
        BASE_YAML.replace(
            "contract:\n",
            "contract:\n  sla: {refresh_frequency: 1h, max_latency: 24h}\n",
        )
    )
    changed = _contract(
        BASE_YAML.replace(
            "contract:\n",
            "contract:\n  sla: {refresh_frequency: 1h, max_latency: 12h}\n",
        )
    )

    assert changed.definition_hash != baseline.definition_hash
    assert json.loads(changed.canonical_json)["contract"]["sla"]["max_latency"] == "12h"


def test_security_change_produces_new_hash():
    baseline = _contract(
        BASE_YAML.replace(
            "contract:\n",
            "contract:\n  security: {level: internal, access_policy: \"row_filter:region\"}\n",
        )
    )
    changed = _contract(
        BASE_YAML.replace(
            "contract:\n",
            "contract:\n  security: {level: restricted, access_policy: \"row_filter:region\"}\n",
        )
    )

    assert changed.definition_hash != baseline.definition_hash
    assert json.loads(changed.canonical_json)["contract"]["security"]["level"] == "restricted"


def test_owner_string_and_mapping_same_definition_hash():
    string_owner = _contract(BASE_YAML)
    mapping_owner = _contract(
        BASE_YAML.replace(
            "owner: sales-data",
            "owner:\n    team: sales-data\n    steward: jane@example.com\n    domain: commerce",
        )
    )

    assert mapping_owner.definition_hash == string_owner.definition_hash
    assert mapping_owner.canonical_json == string_owner.canonical_json
    assert mapping_owner.owner == string_owner.owner == "sales-data"

    payload = json.loads(mapping_owner.canonical_json)
    assert "owner" not in payload["data_product"]
    assert "domain" not in payload["data_product"]


def test_legacy_v1_hashes_remain_readable():
    v1_json = _v1_canonical_json(BASE_YAML)
    v1_hash = sha256(v1_json.encode("utf-8")).hexdigest()
    assert v1_hash == "efa969f1983f633dd62c31656e7953556aae0f8b5f41f6a189f1622c7e4e68cb"

    definition = ContractDefinition(
        contract_id="sales.orders",
        contract_version="1.2.3",
        definition_hash=v1_hash,
        canonical_json=v1_json,
        data_product_id="sales.orders",
        owner="sales-data",
        canonicalization_version=1,
        created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    store = SqliteCertificationStore(":memory:")
    store.register_contract(definition)

    restored = store.get_contract("sales.orders", "1.2.3")

    assert restored == definition


def test_contract_identity_requires_product_and_output_contract():
    with pytest.raises(ValueError, match="data_product"):
        _contract("tables: [{name: silver.orders}]")
    with pytest.raises(ValueError, match="contract.output"):
        _contract("data_product: {id: sales.orders, version: 1.0.0}\ntables: [{name: silver.orders}]")


# ---------------------------------------------------------------------------
# Drift guards on contract identity
#
# `canonicalize_contract` builds its payload from a hand-written list of field
# names. Adding a field to any contract dataclass therefore leaves it out of the
# hash silently, and two materially different contracts then share one
# certification identity — the guarantee the hash exists to provide. These tests
# fail when a field belongs to neither set, so the choice has to be made.
# ---------------------------------------------------------------------------

#: Field name → the value to flip it to. Every one must change the hash.
_OUTPUT_PARTICIPATES = {
    "name": "order_key",
    "logical_type": "string",
    "required": False,
    "unique": False,
    "classification": "pii",
    "entity": "customer",
}
#: Field name → why it is deliberately out of the identity.
_OUTPUT_EXCLUDED = {
    "description": "documentation-only edits must not invalidate a certification",
}

FULL_FIELD_YAML = """
data_product:
  id: sales.orders
  version: 1.2.3
contract:
  grain: [order_id]
  sla: {refresh_frequency: 1h, max_latency: 24h}
  security: {level: internal, access_policy: "row_filter:region"}
  output:
    order_id:
      logical_type: identifier
      required: true
      unique: true
      classification: internal
      entity: order
      description: The order identifier
    order_date:
      logical_type: date
semantic:
  model_key: orders
  entity: order
  default_time_dimension: order_date
  dimensions: [order_id]
tables: [{name: silver.orders}]
select_final: [[id, order_id], [dt, order_date]]
"""


def _hash_with_output_field(**changes) -> str:
    import dataclasses

    schema = parse_to_ir(parse_schema(FULL_FIELD_YAML))
    first, *rest = schema.contract_output
    return canonicalize_contract(
        dataclasses.replace(
            schema,
            contract_output=[dataclasses.replace(first, **changes), *rest],
        )
    ).definition_hash


def test_every_output_field_attribute_is_classified_for_the_identity_hash():
    import dataclasses

    from skifer.core.ir import ParsedOutputField

    declared = {f.name for f in dataclasses.fields(ParsedOutputField)}

    assert declared == set(_OUTPUT_PARTICIPATES) | set(_OUTPUT_EXCLUDED), (
        "A contract output field attribute is in neither set. Decide whether it "
        "is part of the contract's identity (add it to _OUTPUT_PARTICIPATES) or "
        "documentation only (add it to _OUTPUT_EXCLUDED with the reason)."
    )


@pytest.mark.parametrize("attribute,replacement", sorted(_OUTPUT_PARTICIPATES.items()))
def test_changing_a_contractual_output_attribute_changes_the_hash(attribute, replacement):
    assert _hash_with_output_field(**{attribute: replacement}) != _hash_with_output_field()


@pytest.mark.parametrize("attribute", sorted(_OUTPUT_EXCLUDED))
def test_changing_a_documentation_only_attribute_keeps_the_hash(attribute):
    assert _hash_with_output_field(**{attribute: "rewritten"}) == _hash_with_output_field()


#: The other three contract blocks put every attribute they own into the hash.
#: Value to flip each one to, so the test proves it rather than asserting it.
_BLOCK_PARTICIPATES = {
    "contract_sla": {"refresh_frequency": "6h", "max_latency": "48h"},
    "contract_security": {"level": "restricted", "access_policy": "row_filter:country"},
    "semantic": {
        "model_key": "orders_v2",
        "entity": "customer",
        "default_time_dimension": None,
        "dimensions": ("order_date",),
    },
}


def _hash_with_block(block: str, **changes) -> str:
    import dataclasses

    schema = parse_to_ir(parse_schema(FULL_FIELD_YAML))
    current = getattr(schema, block)
    return canonicalize_contract(
        dataclasses.replace(schema, **{block: dataclasses.replace(current, **changes)})
    ).definition_hash


@pytest.mark.parametrize("block", sorted(_BLOCK_PARTICIPATES))
def test_every_contract_block_attribute_is_in_the_identity_hash(block):
    import dataclasses

    schema = parse_to_ir(parse_schema(FULL_FIELD_YAML))
    current = getattr(schema, block)
    assert current is not None, f"the fixture must populate '{block}' for this guard to mean anything"

    declared = {f.name for f in dataclasses.fields(type(current))}
    assert declared == set(_BLOCK_PARTICIPATES[block]), (
        f"'{type(current).__name__}' gained or lost an attribute. Every one of them "
        "is part of the contract's identity today; decide explicitly before "
        "shipping one that is not."
    )


@pytest.mark.parametrize(
    "block,attribute",
    sorted(
        (block, attribute)
        for block, changes in _BLOCK_PARTICIPATES.items()
        for attribute in changes
    ),
)
def test_changing_a_contract_block_attribute_changes_the_hash(block, attribute):
    replacement = _BLOCK_PARTICIPATES[block][attribute]
    assert _hash_with_block(block, **{attribute: replacement}) != _hash_with_block(block)
