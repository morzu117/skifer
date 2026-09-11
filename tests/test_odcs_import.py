"""Plan 31 ODCS 3.1 import tests."""

import pytest
import yaml

from skifer.core.ir import parse_to_ir
from skifer.core.schema_loader import parse_schema
from skifer.observability.certification import canonicalize_contract
from skifer.observability.odcs import export_odcs_31, import_odcs_31


def _schema_from_block(block: dict) -> object:
    output_names = list(block["contract"]["output"])
    pipeline = {
        **block,
        "tables": [{"name": "silver.orders"}],
        "select_final": [[name, name] for name in output_names],
    }
    return parse_to_ir(parse_schema(yaml.safe_dump(pipeline, sort_keys=False)))


def _mapped_export_view(document: dict) -> dict:
    return {
        "id": document["id"],
        "version": document["version"],
        "status": document["status"],
        "schema": document["schema"],
        "quality": document["quality"],
        "slaProperties": document["slaProperties"],
    }


def test_export_import_export_idempotent_on_mapped_fields():
    schema = parse_to_ir(
        parse_schema(
            """
data_product:
  id: sales.orders
  version: 1.0.0
  owner:
    steward: sales-data
  description: Orders
contract:
  status: deprecated
  grain: [order_id]
  sla:
    refresh_frequency: 1h
    max_latency: 12h
  output:
    order_id:
      logical_type: identifier
      required: true
      unique: true
      classification: internal
      description: Order id
    net_revenue:
      logical_type: currency
      classification: confidential
      description: Net revenue
tables: [{name: silver.orders}]
select_final:
  - [order_id, order_id]
  - [net_revenue, net_revenue]
"""
        )
    )
    first = export_odcs_31(schema, canonicalize_contract(schema))
    imported = import_odcs_31(first.document)
    imported_schema = _schema_from_block(
        {"data_product": imported.data_product, "contract": imported.contract}
    )
    second = export_odcs_31(imported_schema, canonicalize_contract(imported_schema))

    assert _mapped_export_view(second.document) == _mapped_export_view(first.document)


def test_import_reports_unmapped_fields():
    result = import_odcs_31(
        {
            "apiVersion": "v3.1.0",
            "kind": "DataContract",
            "id": "sales.orders",
            "version": "1.0.0",
            "schema": {"properties": [{"name": "order_id"}]},
            "roles": [{"role": "reader", "access": "read"}],
            "servers": [{"server": "warehouse"}],
        }
    )

    assert any("roles" in warning for warning in result.warnings)
    assert any("servers" in warning for warning in result.warnings)


def test_import_rejects_non_datacontract():
    with pytest.raises(ValueError, match="DataContract"):
        import_odcs_31(
            {
                "apiVersion": "v3.1.0",
                "kind": "DataProduct",
                "id": "sales.orders",
                "version": "1.0.0",
                "schema": {"properties": [{"name": "order_id"}]},
            }
        )


def test_import_team_single_name_becomes_string_owner():
    result = import_odcs_31(
        {
            "apiVersion": "v3.1.0",
            "kind": "DataContract",
            "id": "sales.orders",
            "version": "1.0.0",
            "schema": {"properties": [{"name": "order_id"}]},
            "team": [{"name": "sales-data"}],
        }
    )

    assert result.data_product["owner"] == "sales-data"


def test_import_team_multi_becomes_mapping():
    result = import_odcs_31(
        {
            "apiVersion": "v3.1.0",
            "kind": "DataContract",
            "id": "sales.orders",
            "version": "1.0.0",
            "schema": {"properties": [{"name": "order_id"}]},
            "team": [
                {"name": "sales-data", "role": "team"},
                {"name": "jane@example.com", "role": "steward"},
                {"name": "data@example.com", "role": "contact"},
            ],
        }
    )

    assert result.data_product["owner"] == {
        "team": "sales-data",
        "steward": "jane@example.com",
        "contact": "data@example.com",
    }


def test_import_sla_roundtrip():
    result = import_odcs_31(
        {
            "apiVersion": "v3.1.0",
            "kind": "DataContract",
            "id": "sales.orders",
            "version": "1.0.0",
            "schema": {"properties": [{"name": "order_id"}]},
            "slaProperties": [
                {"property": "refreshFrequency", "value": "1h"},
                {"property": "latency", "value": "12h"},
            ],
        }
    )

    assert result.contract["sla"] == {
        "refresh_frequency": "1h",
        "max_latency": "12h",
    }


def test_import_security_roundtrip():
    result = import_odcs_31(
        {
            "apiVersion": "v3.1.0",
            "kind": "DataContract",
            "id": "sales.orders",
            "version": "1.0.0",
            "schema": {"properties": [{"name": "order_id"}]},
            "security": {"level": "restricted", "accessPolicy": "row_filter:region"},
        }
    )

    assert result.contract["security"] == {
        "level": "restricted",
        "access_policy": "row_filter:region",
    }
