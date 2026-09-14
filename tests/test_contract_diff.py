"""Spark-free contract diff tests (Plan 31 slice 3.5)."""

from __future__ import annotations

import yaml
import json
import pytest

from skifer.core.ir import parse_to_ir
from skifer.core.schema_loader import parse_schema
from skifer.observability.certification import diff_contracts
from skifer.observability.certification import ContractDefinition
from skifer.observability.certification import canonicalize_contract, schema_from_definition


def _schema(fields: dict, *, sla: dict | None = None):
    contract = {"output": fields}
    if sla is not None:
        contract["sla"] = sla
    payload = {
        "data_product": {"id": "sales.orders", "version": "1.0.0"},
        "contract": contract,
        "tables": [{"name": "silver.orders"}],
        "select_final": [[name, name] for name in fields],
    }
    return parse_to_ir(parse_schema(yaml.safe_dump(payload, sort_keys=False)))


def test_diff_added_removed_retyped():
    old = _schema(
        {
            "id": {"logical_type": "identifier"},
            "amount": {"logical_type": "currency"},
            "legacy_code": {"logical_type": "string"},
        }
    )
    new = _schema(
        {
            "id": {"logical_type": "identifier"},
            "amount": {"logical_type": "decimal"},
            "customer_id": {"logical_type": "identifier"},
        }
    )

    diff = diff_contracts(old, new)

    assert diff.added == ("customer_id",)
    assert diff.removed == ("legacy_code",)
    assert diff.retyped == (("amount", "currency", "decimal"),)


def test_diff_required_changed():
    old = _schema(
        {
            "customer_id": {"required": False},
            "comment": {"required": True},
        }
    )
    new = _schema(
        {
            "customer_id": {"required": True},
            "comment": {"required": False},
        }
    )

    diff = diff_contracts(old, new)

    assert diff.required_changed == (
        ("customer_id", False, True),
        ("comment", True, False),
    )
    assert diff.breaking is True


def test_diff_added_column_not_breaking():
    old = _schema({"id": {}})
    new = _schema({"id": {}, "amount": {}})

    diff = diff_contracts(old, new)

    assert diff.added == ("amount",)
    assert diff.breaking is False


def test_diff_required_softening_not_breaking():
    old = _schema({"comment": {"required": True}})
    new = _schema({"comment": {"required": False}})

    diff = diff_contracts(old, new)

    assert diff.required_changed == (("comment", True, False),)
    assert diff.breaking is False


def test_diff_classification_changed_downgrade_is_breaking():
    old = _schema({"ssn": {"classification": "restricted"}})
    new = _schema({"ssn": {"classification": "internal"}})

    diff = diff_contracts(old, new)

    assert diff.classification_changed == (("ssn", "restricted", "internal"),)
    assert diff.breaking is True


def test_diff_classification_upgrade_not_breaking():
    old = _schema({"ssn": {"classification": "internal"}})
    new = _schema({"ssn": {"classification": "restricted"}})

    diff = diff_contracts(old, new)

    assert diff.classification_changed == (("ssn", "internal", "restricted"),)
    assert diff.breaking is False


def test_diff_breaking_on_removal():
    old = _schema({"id": {}, "amount": {}})
    new = _schema({"id": {}})

    diff = diff_contracts(old, new)

    assert diff.removed == ("amount",)
    assert diff.breaking is True


def test_diff_breaking_on_retype():
    old = _schema({"amount": {"logical_type": "currency"}})
    new = _schema({"amount": {"logical_type": "decimal"}})

    diff = diff_contracts(old, new)

    assert diff.retyped == (("amount", "currency", "decimal"),)
    assert diff.breaking is True


def test_diff_sla_relaxation_is_breaking():
    old = _schema({"id": {}}, sla={"max_latency": "12h"})
    new = _schema({"id": {}}, sla={"max_latency": "24h"})

    diff = diff_contracts(old, new)

    assert diff.sla_changed is True
    assert diff.breaking is True


def test_diff_sla_tightening_not_breaking():
    old = _schema({"id": {}}, sla={"max_latency": "24h"})
    new = _schema({"id": {}}, sla={"max_latency": "12h"})

    diff = diff_contracts(old, new)

    assert diff.sla_changed is True
    assert diff.breaking is False


def test_diff_sla_non_comparable_is_breaking():
    old = _schema({"id": {}}, sla={"max_latency": "daily"})
    new = _schema({"id": {}}, sla={"max_latency": "weekly"})

    diff = diff_contracts(old, new)

    assert diff.sla_changed is True
    assert diff.breaking is True


def test_contract_definition_round_trip_preserves_diff_fields_and_sla():
    schema = _schema(
        {
            "customer_id": {
                "logical_type": "identifier",
                "required": True,
                "unique": True,
                "classification": "confidential",
                "entity": "customer",
            },
            "amount": {
                "logical_type": "currency",
                "required": False,
                "unique": False,
                "classification": "internal",
            },
        },
        sla={"refresh_frequency": "daily", "max_latency": "12h"},
    )

    restored = schema_from_definition(canonicalize_contract(schema))
    diff = diff_contracts(schema, restored)

    assert restored.contract_output == schema.contract_output
    assert restored.contract_sla == schema.contract_sla
    assert diff.added == ()
    assert diff.removed == ()
    assert diff.retyped == ()
    assert diff.required_changed == ()
    assert diff.classification_changed == ()
    assert diff.sla_changed is False
    assert diff.breaking is False


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        ([], "payload must be an object"),
        ({}, "contract.output list"),
        ({"contract": {}}, "contract.output list"),
        ({"contract": {"output": [{}]}}, "non-empty string name"),
        ({"contract": {"output": [{"name": ""}]}}, "non-empty string name"),
    ],
)
def test_schema_from_definition_refuses_malformed_payloads(payload, message):
    definition = ContractDefinition(
        "sales.orders",
        "1.0.0",
        "hash",
        json.dumps(payload),
        "sales.orders",
        None,
    )

    with pytest.raises(ValueError, match=message):
        schema_from_definition(definition)
