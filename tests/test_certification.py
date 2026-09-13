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
