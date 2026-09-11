"""Plan 29 contract identity tests."""
import json

import pytest

from skifer.core.ir import parse_to_ir
from skifer.core.schema_loader import parse_schema
from skifer.observability.certification import canonicalize_contract


def _contract(yaml_text: str):
    return canonicalize_contract(parse_to_ir(parse_schema(yaml_text)))


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
    assert first.canonicalization_version == 1
    assert json.loads(first.canonical_json)["contract"]["grain"] == ["order_id"]


def test_classification_slice_does_not_change_existing_fixture_hash():
    contract = _contract(BASE_YAML)

    assert contract.definition_hash == (
        "efa969f1983f633dd62c31656e7953556aae0f8b5f41f6a189f1622c7e4e68cb"
    )
    assert contract.canonicalization_version == 1


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


def test_contract_identity_requires_product_and_output_contract():
    with pytest.raises(ValueError, match="data_product"):
        _contract("tables: [{name: silver.orders}]")
    with pytest.raises(ValueError, match="contract.output"):
        _contract("data_product: {id: sales.orders, version: 1.0.0}\ntables: [{name: silver.orders}]")
