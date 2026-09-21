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


def test_odcs_sla_properties_filled():
    schema = parse_to_ir(parse_schema("""
data_product: {id: sales.orders, version: 1.0.0}
contract:
  status: deprecated
  sla:
    refresh_frequency: 1h
    max_latency: 12h
  output:
    order_id: {}
tables: [{name: silver.orders}]
select_final: [[id, order_id]]
"""))

    exported = export_odcs_31(schema, canonicalize_contract(schema))

    assert exported.document["status"] == "deprecated"
    assert exported.document["slaProperties"] == [
        {"property": "refreshFrequency", "value": "1h"},
        {"property": "latency", "value": "12h"},
    ]


# ---------------------------------------------------------------------------
# "Export the supported contract surface without silently discarding metadata"
#
# That promise is the function's docstring, and it is testable: for each
# attribute, either the emitted document changes when the attribute is present,
# or a warning names it. `contract.security` satisfied neither — it left with no
# entry and no warning — while `import_odcs_31` had read a top-level `security`
# mapping all along.
# ---------------------------------------------------------------------------

FULL_CONTRACT = """
data_product:
  id: sales.orders
  version: 1.0.0
  owner: sales-data
  description: Orders
contract:
  grain: [order_id]
  sla: {refresh_frequency: 1h, max_latency: 24h}
  security: {level: internal, access_policy: "row_filter:region"}
  output:
    order_id: {logical_type: identifier, required: true}
    amount:
      logical_type: currency
      required: true
      unique: true
      classification: restricted
      entity: order
      description: Net amount
semantic:
  model_key: orders
  dimensions: [order_id]
tables: [{name: silver.orders}]
select_final: [[id, order_id], [amt, amount]]
"""

#: attribute → (how to strip it, a token a warning must contain if the document
#: does not change). `amount` carries the per-field attributes rather than
#: `order_id`, because the grain already implies uniqueness on the grain column:
#: dropping `unique` there changes nothing and loses nothing.
_STRIPPERS = {
    "output.logical_type": (lambda s, r: _drop_field(s, r, logical_type=None), "logical_type"),
    "output.required": (lambda s, r: _drop_field(s, r, required=None), "required"),
    "output.unique": (lambda s, r: _drop_field(s, r, unique=None), "unique"),
    "output.classification": (lambda s, r: _drop_field(s, r, classification=None), "classification"),
    "output.description": (lambda s, r: _drop_field(s, r, description=None), "description"),
    "output.entity": (lambda s, r: _drop_field(s, r, entity=None), "entity"),
    "contract.grain": (lambda s, r: r(s, contract_grain=[]), "grain"),
    "contract.sla": (lambda s, r: r(s, contract_sla=None), "sla"),
    "contract.security": (lambda s, r: r(s, contract_security=None), "security"),
    "semantic": (lambda s, r: r(s, semantic=None), "semantic"),
}


def _drop_field(schema, replace, **changes):
    return replace(
        schema,
        contract_output=[
            replace(f, **changes) if f.name == "amount" else f
            for f in schema.contract_output
        ],
    )


import pytest  # noqa: E402


@pytest.mark.parametrize("attribute", sorted(_STRIPPERS))
def test_no_contract_attribute_leaves_without_an_entry_or_a_warning(attribute):
    import dataclasses

    schema = parse_to_ir(parse_schema(FULL_CONTRACT))
    strip, token = _STRIPPERS[attribute]
    stripped = strip(schema, dataclasses.replace)

    # The SAME definition on both sides on purpose: `customProperties` carries
    # the definition hash, which changes for any schema edit at all. Letting it
    # vary made every comparison differ and the assertion vacuous — a mutation
    # removing the `entity` warning left this test green.
    definition = canonicalize_contract(schema)
    present = export_odcs_31(schema, definition)
    absent = export_odcs_31(stripped, definition)

    changed_document = present.document != absent.document
    named_in_warning = any(token in warning for warning in present.warnings)

    assert changed_document or named_in_warning, (
        f"'{attribute}' leaves this export with neither a document entry nor a "
        "warning. Either map it, or warn that it has no ODCS 3.1 mapping — the "
        "one thing the docstring promises is that it is not dropped in silence."
    )


def test_contract_security_survives_the_export_import_round_trip():
    from skifer.observability.odcs import import_odcs_31

    schema = parse_to_ir(parse_schema(FULL_CONTRACT))
    document = export_odcs_31(schema, canonicalize_contract(schema)).document

    assert document["security"] == {
        "level": "internal",
        "accessPolicy": "row_filter:region",
    }
    assert import_odcs_31(document).contract["security"] == {
        "level": "internal",
        "access_policy": "row_filter:region",
    }


def test_a_grain_column_marked_unique_yields_one_rule_not_two():
    """Asserted as a count, not a set membership: a set hid the duplicate.

    The grain implies uniqueness and `unique: true` states it, so this contract
    reaches the two code paths that each append the rule.
    """
    schema = parse_to_ir(parse_schema("""
data_product: {id: sales.orders, version: 1.0.0}
contract:
  grain: [order_id]
  output:
    order_id: {logical_type: identifier, required: true, unique: true}
tables: [{name: silver.orders}]
select_final: [[id, order_id]]
"""))

    quality = export_odcs_31(schema, canonicalize_contract(schema)).document["quality"]

    assert quality.count({"type": "unique", "field": "order_id"}) == 1
