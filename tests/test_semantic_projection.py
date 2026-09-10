"""Tests for the Spark-free Plan 29 output projector."""
from skifer.core.ir import parse_to_ir
from skifer.core.schema_loader import parse_schema
from skifer.semantic.output_projection import OutputProjector


def _project(yaml_text: str):
    return OutputProjector().project(parse_to_ir(parse_schema(yaml_text)))


def test_projects_select_final_with_static_types_and_contract():
    projected = _project(
        """
data_product: {id: sales.orders, version: 1.0.0}
contract:
  grain: [order_id]
  output:
    order_id: {logical_type: identifier}
    order_day: {logical_type: date}
    source_name: {}
tables: [{name: silver.orders}]
select_final:
  - [id, order_id]
  - [order_timestamp, order_day, [cast:date]]
  - [literal:erp, source_name]
"""
    )

    assert projected.data_product_id == "sales.orders"
    assert projected.grain == ("order_id",)
    assert projected.is_complete is True
    fields = {field.name: field for field in projected.fields}
    assert fields["order_id"].inference_status == "declared"
    assert fields["order_day"].physical_type == "date"
    assert fields["order_day"].source_fields == ("order_timestamp",)
    assert fields["source_name"].physical_type == "string"


def test_projects_aggregate_types_without_starting_spark():
    projected = _project(
        """
data_product: {id: sales.orders, version: 1.0.0}
tables: [{name: silver.orders}]
aggregate:
  group_by: [country]
  measures:
    - [order_id, orders, count_distinct]
    - [amount, average_amount, avg]
    - [amount, total_amount, sum]
"""
    )

    fields = {field.name: field for field in projected.fields}
    assert fields["country"].inference_status == "unknown"
    assert fields["orders"].physical_type == "long"
    assert fields["average_amount"].physical_type == "double"
    assert fields["total_amount"].inference_status == "unknown"


def test_keep_all_is_explicitly_partial_but_projects_added_columns():
    projected = _project(
        """
data_product: {id: sales.orders, version: 1.0.0}
tables: [{name: silver.orders}]
keep_all_columns: true
add_columns:
  - [name, name_length, [length]]
"""
    )

    assert projected.is_complete is False
    assert "cannot be enumerated" in projected.incomplete_reason
    assert projected.fields[0].name == "name_length"
    assert projected.fields[0].physical_type == "long"


def test_projector_reports_raw_sql_and_coalesce_uncertainty():
    projected = _project(
        """
data_product: {id: sales.orders, version: 1.0.0}
tables: [{name: silver.orders}]
select_final:
  - [amount, expression_amount, [expr:amount * tax_rate]]
  - [amount, normalized_amount, [coalesce:0]]
"""
    )

    fields = {field.name: field for field in projected.fields}
    assert "raw SQL" in fields["expression_amount"].inference_reason
    assert fields["expression_amount"].physical_type is None
    assert "heterogeneous" in fields["normalized_amount"].inference_reason


def test_projector_does_not_assume_types_after_python_business_rules():
    projected = _project(
        """
data_product: {id: sales.orders, version: 1.0.0}
tables: [{name: silver.orders}]
business_rules: [uninspectable_rule]
select_final: [[amount, normalized_amount, [cast:double]]]
"""
    )

    field = projected.fields[0]
    assert field.physical_type is None
    assert field.inference_status == "unknown"
    assert "Python business rule" in field.inference_reason


def test_definition_hash_is_stable_and_ignores_source_paths():
    first = _project(
        """
data_product: {id: sales.orders, version: 1.0.0}
tables:
  - name: ignored
    source: {type: csv, path: /tmp/a/orders.csv}
select_final: [[id, order_id, [cast:long]]]
"""
    )
    second = _project(
        """
data_product: {id: sales.orders, version: 1.0.0}
tables:
  - name: ignored
    source: {type: csv, path: /another-machine/orders.csv}
select_final: [[id, order_id, [cast:long]]]
"""
    )

    assert first.definition_hash == second.definition_hash
    assert len(first.definition_hash) == 64
