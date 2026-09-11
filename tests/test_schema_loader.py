"""
Tests for schema_loader: load_schema, parse_schema, normalization functions.
"""
import pytest

from skifer.core.schema_loader import (
    parse_schema,
    load_schema,
    _normalize_filter_string,
    _normalize_filters,
    _normalize_join,
    _normalize_select_final,
    _normalize_filter_mapping,
    _normalize_select_entry_mapping,
)
from skifer.core.constants import CLASSIFICATION_LEVELS, VALID_SOURCE_TYPES
from skifer import load_schema as top_level_load_schema, parse_schema as top_level_parse_schema


# ==============================================================================
# parse_schema
# ==============================================================================

def test_parse_schema_valid_yaml():
    """parse_schema with valid YAML returns a dict."""
    yaml_str = """
tables:
  - name: silver.orders
    alias: orders
select_final:
  - [order_id, id, []]
"""
    result = parse_schema(yaml_str)
    assert isinstance(result, dict)
    assert "tables" in result
    assert result["tables"][0]["name"] == "silver.orders"


def test_parse_schema_with_params():
    """parse_schema injects params into {{ key }} placeholders."""
    yaml_str = """
tables:
  - name: "{{ catalog }}.silver.orders"
    alias: orders
"""
    result = parse_schema(yaml_str, params={"catalog": "my_catalog"})
    assert result["tables"][0]["name"] == "my_catalog.silver.orders"


def test_parse_schema_missing_param_raises():
    """parse_schema raises ValueError when a placeholder is not resolved."""
    yaml_str = """
tables:
  - name: "{{ catalog }}.silver.orders"
"""
    with pytest.raises(ValueError, match="Missing template parameters"):
        parse_schema(yaml_str, params={})


def test_parse_schema_none_param_becomes_empty_string():
    """parse_schema silently converts a None param value to an empty string."""
    yaml_str = "tables:\n  - name: '{{ catalog }}.silver.orders'"
    schema = parse_schema(yaml_str, params={"catalog": None})
    assert schema["tables"][0]["name"] == ".silver.orders"


def test_parse_schema_malformed_yaml_raises():
    """parse_schema raises ValueError on malformed YAML."""
    yaml_str = "tables: [\n  - broken: [missing close"
    with pytest.raises(ValueError, match="Malformed YAML"):
        parse_schema(yaml_str)


def test_parse_schema_non_dict_raises():
    """parse_schema raises ValueError if top level is not a dict."""
    with pytest.raises(ValueError, match="YAML mapping"):
        parse_schema("- item1\n- item2")


# ==============================================================================
# Agent-ready product / contract / semantic seed (Plan 29, slice 0.1)
# ==============================================================================

def test_agent_ready_metadata_is_optional_for_existing_pipeline_yaml():
    schema = parse_schema(
        """
tables:
  - name: silver.orders
select_final:
  - [order_id, order_id]
"""
    )

    assert "data_product" not in schema
    assert "contract" not in schema
    assert "semantic" not in schema


def _classification_schema(classification_line: str = "") -> str:
    return f"""
data_product: {{id: sales.orders, version: 1.0.0}}
contract:
  output:
    order_id: {{{classification_line}}}
tables: [{{name: silver.orders}}]
select_final: [[order_id, order_id]]
"""


def test_classification_unknown_value_refused():
    with pytest.raises(ValueError, match=r"classification 'secret'.*Allowed:"):
        parse_schema(_classification_schema("classification: secret"))


@pytest.mark.parametrize("classification", CLASSIFICATION_LEVELS)
def test_classification_all_valid_levels_accepted(classification):
    schema = parse_schema(
        _classification_schema(f"classification: {classification}")
    )

    assert schema["contract"]["output"]["order_id"]["classification"] == classification


def test_classification_absent_still_loads():
    schema = parse_schema(_classification_schema())

    assert "classification" not in schema["contract"]["output"]["order_id"]


def test_parse_schema_normalizes_agent_ready_metadata():
    schema = parse_schema(
        """
data_product:
  id: sales.orders
  version: 1.2.3
  owner: sales-data
  description: Certified orders
contract:
  grain: [order_id]
  output:
    order_id:
      logical_type: string
      required: true
      unique: true
    net_revenue:
      logical_type: number
      description: Net amount
semantic:
  model_key: orders
  entity: order
  default_time_dimension: order_id
  dimensions: [order_id]
tables:
  - name: silver.orders
select_final:
  - [order_id, order_id]
  - [amount, net_revenue, [cast:double]]
"""
    )

    assert schema["data_product"] == {
        "id": "sales.orders",
        "version": "1.2.3",
        "owner": "sales-data",
        "description": "Certified orders",
    }
    assert schema["contract"]["grain"] == ["order_id"]
    assert schema["contract"]["output"]["order_id"]["required"] is True
    assert schema["semantic"] == {
        "model_key": "orders",
        "entity": "order",
        "default_time_dimension": "order_id",
        "dimensions": ["order_id"],
    }


def test_agent_ready_metadata_supports_injected_owner_and_description():
    schema = parse_schema(
        """
data_product:
  id: sales.orders
  version: 1.0.0
  owner: "{{ team }}"
  description: "{{ description }}"
contract:
  output:
    order_id: {}
semantic:
  model_key: orders
tables:
  - name: silver.orders
select_final:
  - [order_id, order_id]
""",
        params={"team": "sales-data", "description": "Orders for EMEA"},
    )

    assert schema["data_product"]["owner"] == "sales-data"
    assert schema["data_product"]["description"] == "Orders for EMEA"


def test_owner_mapping_is_normalized_in_place():
    schema = parse_schema(
        """
data_product:
  id: sales.orders
  version: 1.0.0
  owner:
    team: " sales-data "
    steward: " jane@example.com "
    domain: " commerce "
    contact: " #sales-data "
contract:
  output:
    order_id: {}
tables:
  - name: silver.orders
select_final:
  - [order_id, order_id]
"""
    )

    assert schema["data_product"]["owner"] == {
        "team": "sales-data",
        "steward": "jane@example.com",
        "domain": "commerce",
        "contact": "#sales-data",
    }


def test_owner_unknown_key_refused():
    with pytest.raises(ValueError, match=r"data_product.owner.*unknown keys.*teem"):
        parse_schema(
            """
data_product:
  id: sales.orders
  version: 1.0.0
  owner:
    teem: sales-data
tables:
  - name: silver.orders
"""
        )


def test_data_product_domain_is_normalized():
    schema = parse_schema(
        """
data_product:
  id: sales.orders
  version: 1.0.0
  domain: " commerce "
contract:
  output:
    order_id: {}
tables:
  - name: silver.orders
select_final:
  - [order_id, order_id]
"""
    )

    assert schema["data_product"]["domain"] == "commerce"


@pytest.mark.parametrize(
    ("fragment", "match"),
    [
        ("version: bad", "data_product.version"),
        ("version: 1.0", "data_product.version"),
    ],
)
def test_agent_ready_metadata_rejects_invalid_product_version(fragment, match):
    with pytest.raises(ValueError, match=match):
        parse_schema(
            "data_product:\n"
            "  id: sales.orders\n"
            f"  {fragment}\n"
            "tables:\n"
            "  - name: silver.orders\n"
        )


def test_agent_ready_contract_output_must_be_mapping():
    with pytest.raises(ValueError, match="contract.output.*non-empty mapping"):
        parse_schema(
            """
contract:
  output: [order_id]
tables:
  - name: silver.orders
"""
        )


def test_agent_ready_metadata_aggregates_unknown_keys():
    with pytest.raises(ValueError) as exc_info:
        parse_schema(
            """
data_product:
  id: sales.orders
  version: 1.0.0
  unexpected: no
contract:
  output:
    order_id:
      invalid: yes
  unexpected: no
semantic:
  model_key: orders
  sql: SELECT nope
tables:
  - name: silver.orders
select_final:
  - [order_id, order_id]
"""
        )

    message = str(exc_info.value)
    assert "[data_product] unknown keys" in message
    assert "[contract] unknown keys" in message
    assert "[contract.output] field 'order_id' unknown keys" in message
    assert "[semantic] unknown keys" in message


def test_agent_ready_contract_grain_must_reference_declared_output():
    with pytest.raises(ValueError, match="contract.grain.*not declared in contract.output"):
        parse_schema(
            """
contract:
  grain: [missing]
  output:
    order_id: {}
tables:
  - name: silver.orders
select_final:
  - [order_id, order_id]
"""
        )


def test_agent_ready_contract_output_must_be_explicit_pipeline_output_when_known():
    with pytest.raises(ValueError, match="contract.output.*not produced by the pipeline"):
        parse_schema(
            """
contract:
  output:
    customer_id: {}
tables:
  - name: silver.orders
select_final:
  - [order_id, order_id]
"""
        )


def test_agent_ready_semantic_references_must_be_declared_outputs():
    with pytest.raises(ValueError, match="semantic.dimensions.*not a declared output field"):
        parse_schema(
            """
contract:
  output:
    order_id: {}
semantic:
  model_key: orders
  dimensions: [missing]
tables:
  - name: silver.orders
select_final:
  - [order_id, order_id]
"""
        )


def test_agent_ready_time_dimension_must_be_declared_output():
    with pytest.raises(ValueError, match="semantic.dimensions.*not a declared output field"):
        parse_schema(
            """
contract:
  output:
    order_id: {}
semantic:
  model_key: orders
  default_time_dimension: order_date
tables:
  - name: silver.orders
select_final:
  - [order_id, order_id]
"""
        )


# ==============================================================================
# Filter string normalization
# ==============================================================================

def test_normalize_filter_string_with_value():
    """'region:equals:EMEA' -> {column, operator, value}"""
    result = _normalize_filter_string("region:equals:EMEA")
    assert result == {"column": "region", "operator": "equals", "value": "EMEA"}


def test_normalize_filter_string_no_value():
    """'customer_id:is_not_null' -> {column, operator} without value key (or empty)"""
    result = _normalize_filter_string("customer_id:is_not_null")
    assert result["column"] == "customer_id"
    assert result["operator"] == "is_not_null"
    assert "value" not in result


def test_normalize_filter_string_in_comma():
    """'status:in:A,B' -> value is a list ['A', 'B']"""
    result = _normalize_filter_string("status:in:A,B")
    assert result["operator"] == "in"
    assert result["value"] == ["A", "B"]


def test_normalize_filter_string_in_semicolon():
    """'status:in:A;B' -> value is a list ['A', 'B'] (semicolon supported)"""
    result = _normalize_filter_string("status:in:A;B")
    assert result["operator"] == "in"
    assert result["value"] == ["A", "B"]


def test_normalize_filter_string_invalid_raises():
    """Invalid filter string (no colon) raises ValueError."""
    with pytest.raises(ValueError, match="Invalid filter string"):
        _normalize_filter_string("nocolon")


def test_normalize_filters_mixed_list():
    """_normalize_filters handles mix of strings and dicts."""
    filters = [
        "region:equals:EMEA",
        {"column": "status", "operator": "is_not_null"},
    ]
    result = _normalize_filters(filters)
    assert result[0] == {"column": "region", "operator": "equals", "value": "EMEA"}
    assert result[1] == {"column": "status", "operator": "is_not_null"}


# ==============================================================================
# Join normalization
# ==============================================================================

def test_normalize_join_compact_table_from():
    """[alias, key] compact form -> separate table_from and on_from fields."""
    join = {"table_from": ["orders", "customer_id"], "table_to": "customers", "on_to": "id"}
    result = _normalize_join(join)
    assert result["table_from"] == "orders"
    assert result["on_from"] == "customer_id"
    assert result["table_to"] == "customers"


def test_normalize_join_compact_both():
    """Compact [alias, key] for both table_from and table_to."""
    join = {
        "table_from": ["orders", "customer_id"],
        "table_to": ["customers", "id"],
        "type": "left"
    }
    result = _normalize_join(join)
    assert result["table_from"] == "orders"
    assert result["on_from"] == "customer_id"
    assert result["table_to"] == "customers"
    assert result["on_to"] == "id"


def test_normalize_join_already_expanded():
    """Normal dict join is not modified."""
    join = {"table_from": "orders", "on_from": "id", "table_to": "customers", "on_to": "id"}
    result = _normalize_join(join)
    assert result == join


# ==============================================================================
# select_final normalization
# ==============================================================================

def test_normalize_select_final_two_element():
    """[src, tgt] 2-element row -> [src, tgt, []]"""
    result = _normalize_select_final([["order_id", "id"]])
    assert result == [["order_id", "id", []]]


def test_normalize_select_final_three_element_unchanged():
    """[src, tgt, ops] 3-element row is unchanged."""
    row = ["amount", "amount_eur", ["cast:double"]]
    result = _normalize_select_final([row])
    assert result == [row]


def test_normalize_select_final_literal_shorthand():
    """[literal:ERP, source_system] -> [None, source_system, [lit:ERP]]"""
    result = _normalize_select_final([["literal:ERP", "source_system"]])
    assert result[0][0] is None
    assert result[0][1] == "source_system"
    assert "lit:ERP" in result[0][2]


# ==============================================================================
# load_schema with temp file
# ==============================================================================

def test_load_schema_from_temp_file(tmp_path):
    """load_schema reads and parses a YAML file correctly."""
    schema_content = """
tables:
  - name: silver.orders
    alias: orders
select_final:
  - [order_id, id]
  - [amount, amount_eur, [cast:double]]
"""
    schema_file = tmp_path / "test_schema.yaml"
    schema_file.write_text(schema_content)

    result = load_schema(str(schema_file))
    assert isinstance(result, dict)
    assert result["tables"][0]["alias"] == "orders"
    # 2-element row should be normalized
    assert result["select_final"][0] == ["order_id", "id", []]


def test_load_schema_with_params(tmp_path):
    """load_schema injects params from file."""
    schema_content = """
tables:
  - name: "{{ catalog }}.silver.orders"
    alias: orders
"""
    schema_file = tmp_path / "parameterized.yaml"
    schema_file.write_text(schema_content)

    result = load_schema(str(schema_file), params={"catalog": "my_cat"})
    assert result["tables"][0]["name"] == "my_cat.silver.orders"


def test_load_schema_file_not_found():
    """load_schema raises FileNotFoundError for non-existent file."""
    with pytest.raises(FileNotFoundError, match="not found"):
        load_schema("/nonexistent/path/to/schema.yaml")


# ==============================================================================
# Top-level exports
# ==============================================================================

def test_top_level_exports():
    """load_schema and parse_schema are importable from the top-level package."""
    assert callable(top_level_load_schema)
    assert callable(top_level_parse_schema)


# ==============================================================================
# Full round-trip: parse_schema normalization
# ==============================================================================

def test_parse_schema_normalizes_filter_strings():
    """parse_schema normalizes filter strings in tables."""
    yaml_str = """
tables:
  - name: silver.orders
    alias: orders
    filter:
      - "region:equals:EMEA"
      - "customer_id:is_not_null"
"""
    result = parse_schema(yaml_str)
    filters = result["tables"][0]["filter"]
    assert filters[0] == {"column": "region", "operator": "equals", "value": "EMEA"}
    assert filters[1] == {"column": "customer_id", "operator": "is_not_null"}


def test_parse_schema_keep_all_columns_and_select_final_raises():
    """parse_schema raises ValueError when keep_all_columns and select_final coexist."""
    yaml_str = """
tables:
  - name: silver.orders
    alias: orders
keep_all_columns: true
select_final:
  - [order_id, id]
"""
    with pytest.raises(ValueError, match="mutually exclusive"):
        parse_schema(yaml_str)


def test_normalize_select_final_literal_shorthand_with_ops():
    """[literal:ERP, source_system, [cast:string]] -> [None, source_system, [lit:ERP, cast:string]]"""
    result = _normalize_select_final([["literal:ERP", "source_system", ["cast:string"]]])
    assert result[0][0] is None
    assert result[0][1] == "source_system"
    assert "lit:ERP" in result[0][2]
    assert "cast:string" in result[0][2]


def test_parse_schema_normalizes_joins():
    """parse_schema normalizes compact join format."""
    yaml_str = """
tables:
  - name: silver.orders
    alias: orders
  - name: silver.customers
    alias: customers
join:
  - table_from: [orders, customer_id]
    table_to: [customers, id]
    type: left
"""
    result = parse_schema(yaml_str)
    join = result["join"][0]
    assert join["table_from"] == "orders"
    assert join["on_from"] == "customer_id"
    assert join["table_to"] == "customers"
    assert join["on_to"] == "id"


# ------------------------------------------------------------------
# Phase 14 — source: block normalization tests
# ------------------------------------------------------------------

def test_parse_schema_source_csv_valid():
    """parse_schema normalizes a valid source: csv block."""
    yaml_str = """
tables:
  - name: raw_orders
    alias: orders
    source:
      type: csv
      path: /data/orders/*.csv
      options:
        header: "true"
        inferSchema: "true"
"""
    result = parse_schema(yaml_str)
    src = result["tables"][0]["source"]
    assert src["type"] == "csv"
    assert src["path"] == "/data/orders/*.csv"
    assert src["options"] == {"header": "true", "inferSchema": "true"}


def test_parse_schema_source_options_default_empty():
    """When options is absent, it should default to {}."""
    yaml_str = """
tables:
  - name: raw_events
    source:
      type: parquet
      path: /data/events/
"""
    result = parse_schema(yaml_str)
    src = result["tables"][0]["source"]
    assert src["options"] == {}


def test_parse_schema_source_options_null_normalized():
    """When options is explicitly null, it should be normalized to {}."""
    yaml_str = """
tables:
  - name: raw_events
    source:
      type: json
      path: /data/events.json
      options: ~
"""
    result = parse_schema(yaml_str)
    src = result["tables"][0]["source"]
    assert src["options"] == {}


def test_parse_schema_source_param_injection_in_path():
    """{{ param }} placeholders in source.path are resolved before YAML parse."""
    yaml_str = """
tables:
  - name: raw_orders
    source:
      type: csv
      path: "{{ base_path }}/orders/*.csv"
"""
    result = parse_schema(yaml_str, params={"base_path": "abfss://container@account.dfs.core.windows.net"})
    src = result["tables"][0]["source"]
    assert src["path"] == "abfss://container@account.dfs.core.windows.net/orders/*.csv"


def test_parse_schema_source_missing_type_raises():
    """source: without type should raise ValueError."""
    yaml_str = """
tables:
  - name: raw_orders
    source:
      path: /data/orders/*.csv
"""
    with pytest.raises(ValueError, match="missing required field 'type'"):
        parse_schema(yaml_str)


def test_parse_schema_source_missing_path_raises():
    """source: without path should raise ValueError."""
    yaml_str = """
tables:
  - name: raw_orders
    source:
      type: csv
"""
    with pytest.raises(ValueError, match="missing required field 'path'"):
        parse_schema(yaml_str)


def test_parse_schema_source_invalid_type_raises():
    """Unknown source type should raise ValueError."""
    yaml_str = """
tables:
  - name: raw_orders
    source:
      type: xlsx
      path: /data/file.xlsx
"""
    with pytest.raises(ValueError, match="unknown source type 'xlsx'"):
        parse_schema(yaml_str)


def test_parse_schema_source_and_loader_mutually_exclusive():
    """source: and source_type: loader are mutually exclusive."""
    yaml_str = """
tables:
  - name: raw_orders
    source_type: loader
    function_name: my_loader
    source:
      type: csv
      path: /data/orders/*.csv
"""
    with pytest.raises(ValueError, match="mutually exclusive"):
        parse_schema(yaml_str)


def test_parse_schema_source_all_valid_types():
    """All declared valid source types should parse without error."""
    for t in sorted(VALID_SOURCE_TYPES):
        yaml_str = f"""
tables:
  - name: raw_data
    source:
      type: {t}
      path: /data/file
"""
        result = parse_schema(yaml_str)
        assert result["tables"][0]["source"]["type"] == t


def test_parse_schema_source_coexists_with_filter_and_quality_checks():
    """source: should coexist with filter and quality_checks without error."""
    yaml_str = """
tables:
  - name: raw_orders
    alias: orders
    source:
      type: csv
      path: /data/orders/*.csv
      options:
        header: "true"
    filter:
      - status:is_not_null
    quality_checks:
      drop_nulls_in: [order_id]
"""
    result = parse_schema(yaml_str)
    t = result["tables"][0]
    assert t["source"]["type"] == "csv"
    assert t["filter"][0]["operator"] == "is_not_null"
    assert t["quality_checks"]["drop_nulls_in"] == ["order_id"]


# ==============================================================================
# Fail-fast operator validation (Plan 17-1.2)
# ==============================================================================

class TestFailFastOperatorValidation:
    """Schema validation raises ValueError on unknown operators at load time."""

    def test_unknown_filter_operator_raises(self):
        yaml_str = """
tables:
  - name: silver.orders
    alias: orders
    filter:
      - "region:equalz:EMEA"
"""
        with pytest.raises(ValueError, match="equalz"):
            parse_schema(yaml_str)

    def test_known_filter_operator_passes(self):
        yaml_str = """
tables:
  - name: silver.orders
    alias: orders
    filter:
      - "region:equals:EMEA"
      - "status:in:ACTIVE,PENDING"
      - "customer_id:is_not_null"
"""
        result = parse_schema(yaml_str)
        assert len(result["tables"][0]["filter"]) == 3

    def test_unknown_column_op_raises(self):
        yaml_str = """
tables:
  - name: silver.orders
    alias: orders
select_final:
  - [amount, amount_eur, [castt:double]]
"""
        with pytest.raises(ValueError, match="castt"):
            parse_schema(yaml_str)

    def test_known_column_ops_pass(self):
        yaml_str = """
tables:
  - name: silver.orders
    alias: orders
select_final:
  - [amount, amount_eur, [cast:double, round:2]]
  - [name, name_upper, [upper]]
"""
        result = parse_schema(yaml_str)
        assert len(result["select_final"]) == 2

    def test_unknown_when_condition_raises(self):
        yaml_str = """
tables:
  - name: silver.orders
    alias: orders
select_final:
  - [status, status_label, [when:equalz:DONE, then:lit:Paid, else:lit:Unknown]]
"""
        with pytest.raises(ValueError, match="equalz"):
            parse_schema(yaml_str)

    def test_known_when_condition_passes(self):
        yaml_str = """
tables:
  - name: silver.orders
    alias: orders
select_final:
  - [status, status_label, [when:equals:DONE, then:lit:Paid, else:lit:Unknown]]
"""
        result = parse_schema(yaml_str)
        assert result["select_final"]

    def test_unknown_op_in_add_columns_raises(self):
        yaml_str = """
tables:
  - name: silver.orders
    alias: orders
keep_all_columns: true
add_columns:
  - [amount, amount_rounded, [roound:2]]
"""
        with pytest.raises(ValueError, match="roound"):
            parse_schema(yaml_str)

    def test_multiple_errors_aggregated(self):
        """All errors are collected and raised in a single ValueError."""
        yaml_str = """
tables:
  - name: silver.orders
    alias: orders
    filter:
      - "region:equalz:EMEA"
      - "status:notin:ACTIVE"
select_final:
  - [amount, amount_eur, [castt:double]]
"""
        with pytest.raises(ValueError) as exc_info:
            parse_schema(yaml_str)
        msg = str(exc_info.value)
        assert "equalz" in msg
        assert "notin" in msg
        assert "castt" in msg

    def test_filter_alias_passes_validation(self):
        """Aliases like 'eq' and 'gt' are valid and must not raise."""
        yaml_str = """
tables:
  - name: silver.orders
    alias: orders
    filter:
      - "amount:gt:100"
      - "region:eq:EMEA"
"""
        result = parse_schema(yaml_str)
        assert len(result["tables"][0]["filter"]) == 2

    def test_unknown_op_in_filter_groups_raises(self):
        yaml_str = """
tables:
  - name: silver.orders
    alias: orders
    filter_groups:
      - ["region:equalz:EMEA"]
"""
        with pytest.raises(ValueError, match="equalz"):
            parse_schema(yaml_str)

    def test_dict_form_when_condition_validation(self):
        """Dict-form when: conditions are also validated."""
        yaml_str = """
tables:
  - name: silver.orders
    alias: orders
select_final:
  - source: status
    target: status_label
    ops:
      - when: "equalz:DONE"
        then: "lit:Paid"
      - else: "lit:Unknown"
"""
        with pytest.raises(ValueError, match="equalz"):
            parse_schema(yaml_str)

    def test_dict_form_valid_passes(self):
        yaml_str = """
tables:
  - name: silver.orders
    alias: orders
select_final:
  - source: status
    target: status_label
    ops:
      - when: "equals:DONE"
        then: "lit:Paid"
      - else: "lit:Unknown"
"""
        result = parse_schema(yaml_str)
        assert result["select_final"]


# ==============================================================================
# Join alias validation (Plan 17-1.4)
# ==============================================================================

class TestJoinAliasValidation:
    """Schema validation raises ValueError when join references an unknown alias."""

    def test_valid_join_aliases_pass(self):
        yaml_str = """
tables:
  - name: silver.orders
    alias: ord
  - name: silver.customers
    alias: cust
join:
  - table_from: [ord, customer_id]
    table_to: [cust, id]
    type: left
"""
        result = parse_schema(yaml_str)
        assert len(result["join"]) == 1

    def test_unknown_table_from_raises(self):
        yaml_str = """
tables:
  - name: silver.orders
    alias: ord
  - name: silver.customers
    alias: cust
join:
  - table_from: [unknown_alias, customer_id]
    table_to: [cust, id]
    type: left
"""
        with pytest.raises(ValueError, match="unknown_alias"):
            parse_schema(yaml_str)

    def test_unknown_table_to_raises(self):
        yaml_str = """
tables:
  - name: silver.orders
    alias: ord
  - name: silver.customers
    alias: cust
join:
  - table_from: [ord, customer_id]
    table_to: [ghost, id]
    type: left
"""
        with pytest.raises(ValueError, match="ghost"):
            parse_schema(yaml_str)

    def test_table_name_without_alias_accepted(self):
        """Join can reference the table name (or its last segment) if no alias is set."""
        yaml_str = """
tables:
  - name: silver.orders
  - name: silver.customers
join:
  - table_from: [orders, customer_id]
    table_to: [customers, id]
    type: inner
"""
        result = parse_schema(yaml_str)
        assert result["join"]

    def test_error_message_contains_suggestion(self):
        """Error message includes a Did you mean suggestion when a close match exists."""
        yaml_str = """
tables:
  - name: silver.orders
    alias: orders
  - name: silver.customers
    alias: customers
join:
  - table_from: [orderz, customer_id]
    table_to: [customers, id]
    type: left
"""
        with pytest.raises(ValueError, match="Did you mean"):
            parse_schema(yaml_str)


class TestCompactWhenThenElseValidation:
    """B.2 — Structural validation of compact when/then/else chains."""

    def test_valid_chain_passes(self):
        yaml_str = """
tables:
  - name: silver.orders
    alias: orders
select_final:
  - [status, status_label, [when:equals:DONE, then:lit:Paid, else:lit:Unknown]]
"""
        result = parse_schema(yaml_str)
        assert result["select_final"]

    def test_two_elements_raises(self):
        """Missing else raises at load time."""
        yaml_str = """
tables:
  - name: silver.orders
    alias: orders
select_final:
  - [status, status_label, [when:equals:DONE, then:lit:Paid]]
"""
        with pytest.raises(ValueError, match="exactly 3 elements"):
            parse_schema(yaml_str)

    def test_four_elements_raises(self):
        """Extra element after else raises at load time."""
        yaml_str = """
tables:
  - name: silver.orders
    alias: orders
select_final:
  - [status, status_label, [when:equals:DONE, then:lit:Paid, else:lit:Unknown, upper]]
"""
        with pytest.raises(ValueError, match="exactly 3 elements"):
            parse_schema(yaml_str)

    def test_wrong_prefix_at_position_1_raises(self):
        """Second element must start with 'then:'."""
        yaml_str = """
tables:
  - name: silver.orders
    alias: orders
select_final:
  - [status, status_label, [when:equals:DONE, lit:Paid, else:lit:Unknown]]
"""
        with pytest.raises(ValueError, match="then:"):
            parse_schema(yaml_str)

    def test_wrong_prefix_at_position_2_raises(self):
        """Third element must start with 'else:'."""
        yaml_str = """
tables:
  - name: silver.orders
    alias: orders
select_final:
  - [status, status_label, [when:equals:DONE, then:lit:Paid, lit:Unknown]]
"""
        with pytest.raises(ValueError, match="else:"):
            parse_schema(yaml_str)


# ==============================================================================
# Plan 18-3.5 — YAML mapping forms for filter and select_final
# ==============================================================================

class TestFilterMappingForm:
    def test_equals_implicit(self):
        """Bare string value normalizes to equals."""
        result = _normalize_filter_mapping({"region": "EMEA"})
        assert result == [{"column": "region", "operator": "equals", "value": "EMEA"}]

    def test_no_arg_operator_string(self):
        """No-arg operator name as bare string value."""
        result = _normalize_filter_mapping({"customer_id": "is_not_null"})
        assert result == [{"column": "customer_id", "operator": "is_not_null"}]

    def test_in_operator_list(self):
        """Dict form with list value."""
        result = _normalize_filter_mapping({"status": {"in": ["ACTIVE", "PENDING"]}})
        assert result == [{"column": "status", "operator": "in", "value": ["ACTIVE", "PENDING"]}]

    def test_comparison_operator(self):
        """Dict form with scalar value."""
        result = _normalize_filter_mapping({"amount": {"greater_than": "100"}})
        assert result == [{"column": "amount", "operator": "greater_than", "value": "100"}]

    def test_null_value(self):
        """Null value normalizes to is_null."""
        result = _normalize_filter_mapping({"deleted_at": None})
        assert result == [{"column": "deleted_at", "operator": "is_null"}]

    def test_multiple_columns(self):
        """Multiple columns produce one filter dict each."""
        result = _normalize_filter_mapping({"region": "EMEA", "status": "is_not_null"})
        assert len(result) == 2

    def test_alias_operator_resolved(self):
        """Operator alias in dict form is resolved to canonical."""
        result = _normalize_filter_mapping({"amount": {"gt": "0"}})
        assert result[0]["operator"] == "greater_than"

    def test_unknown_operator_in_dict_raises(self):
        """Unknown operator in dict form raises ValueError."""
        with pytest.raises(ValueError, match="unknown filter operator"):
            _normalize_filter_mapping({"col": {"badop": "val"}})

    def test_numeric_scalar_equals(self):
        """Numeric value normalizes to equals string."""
        result = _normalize_filter_mapping({"score": 42})
        assert result == [{"column": "score", "operator": "equals", "value": "42"}]

    def test_parse_schema_dict_filter(self):
        """parse_schema accepts filter in mapping form end-to-end."""
        yaml_str = """
tables:
  - name: silver.orders
    alias: ord
    filter:
      region: EMEA
      status: {in: [ACTIVE, PENDING]}
"""
        schema = parse_schema(yaml_str)
        filters = schema["tables"][0]["filter"]
        assert len(filters) == 2
        assert filters[0] == {"column": "region", "operator": "equals", "value": "EMEA"}
        assert filters[1]["operator"] == "in"


class TestSelectFinalMappingForm:
    def test_from_as_basic(self):
        """Entry with from/as keys normalizes to source/target."""
        result = _normalize_select_entry_mapping({"from": "amount", "as": "amount_eur", "ops": []})
        assert result["source"] == "amount"
        assert result["target"] == "amount_eur"
        assert result["ops"] == []

    def test_single_key_dict_op_to_string(self):
        """{cast: double} op dict normalizes to 'cast:double' string."""
        result = _normalize_select_entry_mapping({
            "from": "amount", "as": "amount_eur", "ops": [{"cast": "double"}, {"round": "2"}]
        })
        assert result["ops"] == ["cast:double", "round:2"]

    def test_none_arg_op(self):
        """{upper: None} normalizes to 'upper'."""
        result = _normalize_select_entry_mapping({"from": "name", "as": "name_upper", "ops": [{"upper": None}]})
        assert result["ops"] == ["upper"]

    def test_string_ops_passthrough(self):
        """Existing string ops pass through unchanged."""
        result = _normalize_select_entry_mapping({"from": "amount", "as": "x", "ops": ["cast:double"]})
        assert result["ops"] == ["cast:double"]

    def test_multi_key_dict_op_kept_as_is(self):
        """Multi-key dict ops (when/then/else) are kept as-is."""
        when_op = {"when": "equals:DONE", "then": "lit:Paid"}
        result = _normalize_select_entry_mapping({"from": "status", "as": "label", "ops": [when_op]})
        assert result["ops"] == [when_op]

    def test_normalize_select_final_mixes_forms(self):
        """_normalize_select_final handles both {from,as} and list forms in one list."""
        rows = [
            {"from": "amount", "as": "amount_eur", "ops": [{"cast": "double"}]},
            ["status", "status_out", []],
        ]
        result = _normalize_select_final(rows)
        assert result[0] == {"source": "amount", "target": "amount_eur", "ops": ["cast:double"]}
        assert result[1] == ["status", "status_out", []]

    def test_parse_schema_from_as_form(self):
        """parse_schema accepts select_final in {from, as, ops} form end-to-end."""
        yaml_str = """
tables:
  - name: silver.orders
    alias: ord
select_final:
  - {from: amount, as: amount_eur, ops: [{cast: double}]}
  - {from: status, as: status_out, ops: [upper, trim]}
"""
        schema = parse_schema(yaml_str)
        sf = schema["select_final"]
        assert sf[0] == {"source": "amount", "target": "amount_eur", "ops": ["cast:double"]}
        assert sf[1] == {"source": "status", "target": "status_out", "ops": ["upper", "trim"]}


# ==============================================================================
# Streaming tables — materialization: + streaming: (Plan 27)
# ==============================================================================

class TestStreamingValidation:
    """Load-time validation of the streaming surface (Plan 27)."""

    # ---- nominal cases -------------------------------------------------

    def test_streaming_table_nominal_shorthand(self):
        schema = parse_schema("""
materialization: streaming_table
tables:
  - name: bronze.events
    alias: ev
    streaming: true
keep_all_columns: true
""")
        mat = schema["materialization"]
        assert mat == {
            "type": "streaming_table",
            "trigger": "available_now",
            "checkpoint": "auto",
            "write_mode": "append",
        }

    def test_streaming_table_nominal_dict_form(self):
        schema = parse_schema("""
materialization:
  type: streaming_table
  trigger: "interval:30 seconds"
  checkpoint: /tmp/ckpt/events
tables:
  - name: bronze.events
    alias: ev
    streaming: true
keep_all_columns: true
""")
        mat = schema["materialization"]
        assert mat["trigger"] == "interval:30 seconds"
        assert mat["checkpoint"] == "/tmp/ckpt/events"
        assert mat["write_mode"] == "append"

    def test_streaming_table_upsert_with_keys(self):
        schema = parse_schema("""
materialization:
  type: streaming_table
  write_mode: upsert
  keys: [event_id, source_system]
tables:
  - name: bronze.events
    alias: ev
    streaming: true
keep_all_columns: true
""")
        mat = schema["materialization"]
        assert mat["write_mode"] == "upsert"
        assert mat["keys"] == ["event_id", "source_system"]

    def test_materialization_table_shorthand_is_noop_marker(self):
        schema = parse_schema("""
materialization: table
tables:
  - name: silver.orders
    alias: ord
keep_all_columns: true
""")
        assert schema["materialization"] == {"type": "table"}

    def test_checkpoint_supports_param_injection(self):
        schema = parse_schema("""
materialization:
  type: streaming_table
  checkpoint: "{{ checkpoint_base }}/events"
tables:
  - name: bronze.events
    alias: ev
    streaming: true
keep_all_columns: true
""", params={"checkpoint_base": "/Volumes/prod/_ckpt"})
        assert schema["materialization"]["checkpoint"] == "/Volumes/prod/_ckpt/events"

    def test_stream_static_join_inner_left_ok(self):
        schema = parse_schema("""
materialization: streaming_table
tables:
  - name: bronze.events
    alias: ev
    streaming: true
  - name: silver.dim_country
    alias: dim
join:
  - table_from: [ev, country_id]
    table_to: [dim, id]
    type: left
keep_all_columns: true
""")
        assert schema["tables"][0]["streaming"] is True

    def test_batch_schema_untouched(self):
        """A batch schema without materialization keeps its exact shape."""
        schema = parse_schema("""
tables:
  - name: silver.orders
    alias: ord
keep_all_columns: true
""")
        assert "materialization" not in schema

    # ---- materialization block rejections ------------------------------

    def test_materialized_view_accepted(self):
        """Plan 28 lifted the Plan 27 rejection — see TestMaterializedViewValidation."""
        schema = parse_schema("materialization: materialized_view\ntables:\n  - name: t\n")
        assert schema["materialization"] == {"type": "materialized_view", "refresh": "auto"}

    def test_unknown_materialization_type_rejected(self):
        with pytest.raises(ValueError, match="Unknown type 'bogus'"):
            parse_schema("materialization: bogus\ntables:\n  - name: t\n")

    def test_unknown_materialization_key_rejected(self):
        with pytest.raises(ValueError, match="Unknown keys"):
            parse_schema("""
materialization:
  type: streaming_table
  bogus_key: 1
tables:
  - name: t
    streaming: true
""")

    def test_invalid_trigger_rejected(self):
        with pytest.raises(ValueError, match="Invalid trigger"):
            parse_schema("""
materialization:
  type: streaming_table
  trigger: every_5_minutes
tables:
  - name: t
    streaming: true
""")

    def test_upsert_without_keys_rejected(self):
        with pytest.raises(ValueError, match="requires 'keys'"):
            parse_schema("""
materialization:
  type: streaming_table
  write_mode: upsert
tables:
  - name: t
    streaming: true
""")

    def test_keys_without_upsert_rejected(self):
        with pytest.raises(ValueError, match="only applies to 'write_mode: upsert'"):
            parse_schema("""
materialization:
  type: streaming_table
  keys: [id]
tables:
  - name: t
    streaming: true
""")

    def test_invalid_write_mode_rejected(self):
        with pytest.raises(ValueError, match="Invalid write_mode"):
            parse_schema("""
materialization:
  type: streaming_table
  write_mode: merge
tables:
  - name: t
    streaming: true
""")

    def test_trigger_on_type_table_rejected(self):
        """Per-type allowlist: streaming options never leak onto another type."""
        with pytest.raises(ValueError, match=r"Unknown keys for type 'table': \['trigger'\]"):
            parse_schema("""
materialization:
  type: table
  trigger: available_now
tables:
  - name: t
""")

    # ---- streaming flag / cross-validation rejections ------------------

    def test_streaming_non_bool_rejected(self):
        with pytest.raises(ValueError, match="must be a\\s+boolean"):
            parse_schema("""
tables:
  - name: t
    streaming: "yes"
""")

    def test_streaming_without_materialization_rejected(self):
        """Strict bijection (decision #17)."""
        with pytest.raises(ValueError, match="require a top-level"):
            parse_schema("""
tables:
  - name: bronze.events
    streaming: true
keep_all_columns: true
""")

    def test_materialization_without_streaming_table_rejected(self):
        with pytest.raises(ValueError, match="at least one table"):
            parse_schema("""
materialization: streaming_table
tables:
  - name: silver.orders
keep_all_columns: true
""")

    def test_two_streaming_tables_rejected(self):
        with pytest.raises(ValueError, match="only one table"):
            parse_schema("""
materialization: streaming_table
tables:
  - name: bronze.a
    streaming: true
  - name: bronze.b
    streaming: true
keep_all_columns: true
""")

    def test_dev_limit_on_streaming_table_rejected(self):
        with pytest.raises(ValueError, match="dev_limit"):
            parse_schema("""
materialization: streaming_table
tables:
  - name: bronze.events
    streaming: true
    dev_limit: 100
keep_all_columns: true
""")

    def test_schema_level_dev_limit_rejected(self):
        with pytest.raises(ValueError, match="schema-level 'dev_limit'"):
            parse_schema("""
materialization: streaming_table
dev_limit: 100
tables:
  - name: bronze.events
    streaming: true
keep_all_columns: true
""")

    def test_qualify_on_streaming_table_rejected(self):
        with pytest.raises(ValueError, match="preprocess.qualify"):
            parse_schema("""
materialization: streaming_table
tables:
  - name: bronze.events
    streaming: true
    preprocess:
      qualify:
        partition_by: [id]
        order_by: [ts]
keep_all_columns: true
""")

    def test_drop_duplicates_on_streaming_guides_to_upsert(self):
        with pytest.raises(ValueError, match="write_mode: upsert"):
            parse_schema("""
materialization: streaming_table
tables:
  - name: bronze.events
    streaming: true
    quality_checks:
      drop_duplicates_on: [id]
keep_all_columns: true
""")

    def test_streaming_csv_source_rejected(self):
        with pytest.raises(ValueError, match="cannot\\s+be read as a stream"):
            parse_schema("""
materialization: streaming_table
tables:
  - name: raw_events
    streaming: true
    source:
      type: csv
      path: /data/events/*.csv
keep_all_columns: true
""")

    def test_streaming_delta_source_ok(self):
        schema = parse_schema("""
materialization: streaming_table
tables:
  - name: raw_events
    streaming: true
    source:
      type: delta
      path: /data/events
keep_all_columns: true
""")
        assert schema["tables"][0]["source"]["type"] == "delta"

    def test_streaming_loader_rejected(self):
        with pytest.raises(ValueError, match="loaders return batch"):
            parse_schema("""
materialization: streaming_table
tables:
  - name: t
    streaming: true
    source_type: loader
    loader: my_loader
keep_all_columns: true
""")

    def test_streaming_with_jdbc_sink_rejected(self):
        with pytest.raises(ValueError, match="streaming writes target Delta only"):
            parse_schema("""
materialization: streaming_table
tables:
  - name: bronze.events
    streaming: true
sink:
  type: postgres
keep_all_columns: true
""")

    def test_stream_as_table_to_rejected(self):
        with pytest.raises(ValueError, match="cannot appear as\\s+'table_to'"):
            parse_schema("""
materialization: streaming_table
tables:
  - name: silver.dim
    alias: dim
  - name: bronze.events
    alias: ev
    streaming: true
join:
  - table_from: [dim, id]
    table_to: [ev, country_id]
    type: left
keep_all_columns: true
""")

    def test_stream_join_right_rejected(self):
        with pytest.raises(ValueError, match="not supported\\s+with a streaming base"):
            parse_schema("""
materialization: streaming_table
tables:
  - name: bronze.events
    alias: ev
    streaming: true
  - name: silver.dim
    alias: dim
join:
  - table_from: [ev, country_id]
    table_to: [dim, id]
    type: full
keep_all_columns: true
""")

    def test_errors_are_aggregated(self):
        """Multiple streaming violations surface in one aggregated error."""
        with pytest.raises(ValueError, match="error\\(s\\)"):
            parse_schema("""
materialization: streaming_table
dev_limit: 100
tables:
  - name: bronze.events
    streaming: true
    dev_limit: 50
    quality_checks:
      drop_duplicates_on: [id]
keep_all_columns: true
""")


# ==============================================================================
# Materialized views — materialization: materialized_view (Plan 28)
# ==============================================================================

class TestMaterializedViewValidation:
    """Load-time validation of the materialized-view surface (Plan 28)."""

    # ---- nominal cases -------------------------------------------------

    def test_shorthand_applies_defaults(self):
        schema = parse_schema("""
materialization: materialized_view
tables:
  - name: silver.orders
    alias: ord
aggregate:
  group_by: [country]
  measures:
    - [amount, total, sum]
""")
        assert schema["materialization"] == {"type": "materialized_view", "refresh": "auto"}

    def test_full_dict_form(self):
        schema = parse_schema("""
materialization:
  type: materialized_view
  schedule: "EVERY 6 HOURS"
  comment: "CA par pays"
  cluster_by: [country]
  refresh: never
tables:
  - name: silver.orders
    alias: ord
select_final:
  - [country, country]
""")
        assert schema["materialization"] == {
            "type": "materialized_view",
            "schedule": "EVERY 6 HOURS",
            "comment": "CA par pays",
            "cluster_by": ["country"],
            "refresh": "never",
        }

    def test_cron_schedule_accepted(self):
        schema = parse_schema("""
materialization:
  type: materialized_view
  schedule: "CRON '0 0 6 * * ?' AT TIME ZONE 'Europe/Paris'"
tables:
  - name: t
""")
        assert schema["materialization"]["schedule"].startswith("CRON ")

    def test_keep_all_columns_allowed_on_single_table(self):
        schema = parse_schema("""
materialization: materialized_view
tables:
  - name: silver.orders
    alias: ord
keep_all_columns: true
""")
        assert schema["materialization"]["type"] == "materialized_view"

    def test_partition_by_accepted_alone(self):
        schema = parse_schema("""
materialization:
  type: materialized_view
  partition_by: [country]
tables:
  - name: t
""")
        assert schema["materialization"]["partition_by"] == ["country"]

    # ---- materialization block rejections ------------------------------

    def test_streaming_key_on_materialized_view_rejected(self):
        with pytest.raises(ValueError, match=r"Unknown keys for type 'materialized_view'"):
            parse_schema("""
materialization:
  type: materialized_view
  checkpoint: auto
tables:
  - name: t
""")

    def test_bad_schedule_rejected(self):
        with pytest.raises(ValueError, match="Invalid schedule"):
            parse_schema("""
materialization:
  type: materialized_view
  schedule: "daily"
tables:
  - name: t
""")

    def test_cluster_by_and_partition_by_rejected_together(self):
        with pytest.raises(ValueError, match="mutually exclusive"):
            parse_schema("""
materialization:
  type: materialized_view
  cluster_by: [country]
  partition_by: [region]
tables:
  - name: t
""")

    def test_empty_cluster_by_rejected(self):
        with pytest.raises(ValueError, match="'cluster_by' must be a non-empty list"):
            parse_schema("""
materialization:
  type: materialized_view
  cluster_by: []
tables:
  - name: t
""")

    def test_bad_refresh_rejected(self):
        with pytest.raises(ValueError, match="Invalid refresh"):
            parse_schema("""
materialization:
  type: materialized_view
  refresh: sometimes
tables:
  - name: t
""")

    def test_blank_comment_rejected(self):
        with pytest.raises(ValueError, match="'comment' must be a non-empty string"):
            parse_schema("""
materialization:
  type: materialized_view
  comment: "  "
tables:
  - name: t
""")

    # ---- cross-validation rejections -----------------------------------

    def test_business_rules_rejected(self):
        with pytest.raises(ValueError, match="Python rules are not expressible in SQL"):
            parse_schema("""
materialization: materialized_view
tables:
  - name: silver.orders
    alias: ord
business_rules:
  - flag_high_value
keep_all_columns: true
""")

    def test_file_source_rejected(self):
        with pytest.raises(ValueError, match="file sources"):
            parse_schema("""
materialization: materialized_view
tables:
  - name: raw_orders
    alias: r
    source:
      type: csv
      path: /tmp/orders.csv
keep_all_columns: true
""")

    def test_loader_rejected(self):
        with pytest.raises(ValueError, match="Python loaders are not expressible"):
            parse_schema("""
materialization: materialized_view
tables:
  - name: raw
    alias: r
    source_type: loader
    loader: my_loader
keep_all_columns: true
""")

    def test_dev_limit_rejected(self):
        with pytest.raises(ValueError, match=r"\[materialized_view\].*dev_limit"):
            parse_schema("""
materialization: materialized_view
dev_limit: 100
tables:
  - name: t
""")

    def test_table_dev_limit_rejected(self):
        with pytest.raises(ValueError, match=r"\[materialized_view\].*dev_limit"):
            parse_schema("""
materialization: materialized_view
tables:
  - name: t
    alias: t
    dev_limit: 50
""")

    def test_jdbc_sink_rejected(self):
        with pytest.raises(ValueError, match=r"\[materialized_view\].*sink"):
            parse_schema("""
materialization: materialized_view
sink:
  type: postgres
tables:
  - name: t
""")

    def test_streaming_table_flag_rejected(self):
        with pytest.raises(ValueError, match="error\\(s\\)"):
            parse_schema("""
materialization: materialized_view
tables:
  - name: bronze.events
    alias: ev
    streaming: true
keep_all_columns: true
""")

    def test_drop_duplicates_on_rejected(self):
        with pytest.raises(ValueError, match="drop_duplicates_on"):
            parse_schema("""
materialization: materialized_view
tables:
  - name: t
    alias: t
    quality_checks:
      drop_duplicates_on: [id]
keep_all_columns: true
""")

    def test_qualify_rejected(self):
        with pytest.raises(ValueError, match="preprocess.qualify"):
            parse_schema("""
materialization: materialized_view
tables:
  - name: t
    alias: t
    preprocess:
      qualify:
        partition_by: [id]
        order_by:
          - column: updated_at
            direction: desc
keep_all_columns: true
""")

    def test_keep_all_columns_with_join_rejected(self):
        with pytest.raises(ValueError, match="only supported on a single table"):
            parse_schema("""
materialization: materialized_view
tables:
  - name: silver.orders
    alias: ord
  - name: silver.customers
    alias: cust
join:
  - table_from: [ord, customer_id]
    table_to: [cust, id]
    type: left
keep_all_columns: true
""")

    def test_errors_are_aggregated(self):
        """Multiple materialized-view violations surface in one aggregated error."""
        with pytest.raises(ValueError, match="3 error\\(s\\)"):
            parse_schema("""
materialization: materialized_view
dev_limit: 100
tables:
  - name: t
    alias: t
    dev_limit: 50
business_rules:
  - some_rule
keep_all_columns: true
""")
