"""Plan 29 ODCS 3.1 export tests."""
from skifer.core.ir import parse_to_ir
from skifer.core.schema_loader import parse_schema
from skifer.observability.certification import canonicalize_contract
from skifer.observability.odcs import export_odcs_31


def test_odcs_export_maps_contract_and_reports_unmapped_fields():
    schema = parse_to_ir(parse_schema("""
data_product:
  id: sales.orders
  version: 1.0.0
  owner: sales-data
  description: Orders
contract:
  grain: [order_id]
  output:
    order_id: {logical_type: identifier, required: true, unique: true, entity: order}
tables: [{name: silver.orders}]
select_final: [[id, order_id]]
"""))

    exported = export_odcs_31(schema, canonicalize_contract(schema))

    assert exported.document["apiVersion"] == "v3.1.0"
    assert exported.document["id"] == "sales.orders"
    assert exported.document["schema"]["properties"][0]["name"] == "order_id"
    assert {entry["type"] for entry in exported.document["quality"]} == {"required", "unique"}
    assert exported.document["roles"] == [{"role": "reader", "access": "read"}]
    assert any("entity" in warning for warning in exported.warnings)


def test_odcs_team_backcompat_string_owner():
    schema = parse_to_ir(parse_schema("""
data_product:
  id: sales.orders
  version: 1.0.0
  owner: sales-data
contract:
  output:
    order_id: {}
tables: [{name: silver.orders}]
select_final: [[id, order_id]]
"""))

    exported = export_odcs_31(schema, canonicalize_contract(schema))

    assert exported.document["team"] == [{"name": "sales-data"}]


def test_odcs_team_filled_from_mapping():
    schema = parse_to_ir(parse_schema("""
data_product:
  id: sales.orders
  version: 1.0.0
  owner:
    team: sales-data
    steward: jane@example.com
    domain: commerce
contract:
  output:
    order_id: {}
tables: [{name: silver.orders}]
select_final: [[id, order_id]]
"""))

    exported = export_odcs_31(schema, canonicalize_contract(schema))

    assert exported.document["team"] == [{"name": "sales-data", "role": "steward"}]
    assert exported.document["customProperties"]["skifer.domain"] == "commerce"


def test_odcs_exports_field_classification():
    schema = parse_to_ir(parse_schema("""
data_product: {id: customers, version: 1.0.0}
contract:
  output:
    ssn: {logical_type: string, classification: pii}
tables: [{name: silver.customers}]
select_final: [[ssn, ssn]]
"""))

    exported = export_odcs_31(schema, canonicalize_contract(schema))

    assert exported.document["schema"]["properties"][0]["classification"] == "pii"
