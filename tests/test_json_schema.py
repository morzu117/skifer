"""
Tests for core/json_schema.py — pipeline JSON Schema generator.

Covers:
  - Meta-validation: generated schema is itself a valid JSON Schema
  - Non-drift: committed schemas/skifer-pipeline.schema.json matches generate_json_schema()
  - Positive validation: valid pipeline YAML passes
  - Negative validation: invalid structures fail (operator in wrong place, unknown values)
"""
from __future__ import annotations

import json
import os
import pathlib

import pytest

from skifer.core.json_schema import generate_json_schema
from skifer.core.op_catalog import FILTER_OPERATORS, COLUMN_OPS


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SCHEMA_FILE = pathlib.Path(__file__).parents[1] / "schemas" / "skifer-pipeline.schema.json"


def _load_committed_schema() -> dict:
    with open(SCHEMA_FILE, encoding="utf-8") as f:
        return json.load(f)


try:
    import jsonschema
    _HAS_JSONSCHEMA = True
except ImportError:
    _HAS_JSONSCHEMA = False


# ---------------------------------------------------------------------------
# Structure checks (no jsonschema dep needed)
# ---------------------------------------------------------------------------

class TestGenerateJsonSchema:
    def test_returns_dict(self):
        assert isinstance(generate_json_schema(), dict)

    def test_top_level_keys(self):
        s = generate_json_schema()
        assert s.get("$schema") == "http://json-schema.org/draft-07/schema#"
        assert "title" in s
        assert "properties" in s
        assert "$defs" in s

    def test_filter_operator_enum_reflects_catalog(self):
        s = generate_json_schema()
        enum = s["$defs"]["FilterOperator"]["enum"]
        assert set(enum) == set(FILTER_OPERATORS.keys())

    def test_all_catalog_ops_mentioned_in_column_op_description(self):
        s = generate_json_schema()
        desc = s["$defs"]["ColumnOpString"].get("description", "")
        for op in COLUMN_OPS:
            assert op in desc, f"Column op '{op}' missing from ColumnOpString description"

    def test_required_defs_present(self):
        s = generate_json_schema()
        for def_name in (
            "TableDef", "JoinDef", "SinkDef", "SelectEntry", "FilterEntry",
            "FilterMapping", "FilterList", "SourceDef",
            "MaterializationDef", "PartialDef", "DataProductDef", "ContractDef",
            "OutputFieldDef", "SemanticSeedDef",
        ):
            assert def_name in s["$defs"], f"Missing $defs entry: {def_name}"

    def test_top_level_properties_present(self):
        s = generate_json_schema()
        for prop in ("tables", "join", "business_rules", "select_final", "add_columns",
                     "keep_all_columns", "dev_limit", "sink", "materialization", "partials",
                     "data_product", "contract", "semantic"):
            assert prop in s["properties"], f"Missing top-level property: {prop}"

    def test_agent_ready_defs_are_closed_and_require_identity(self):
        s = generate_json_schema()
        product = s["$defs"]["DataProductDef"]
        contract = s["$defs"]["ContractDef"]
        semantic = s["$defs"]["SemanticSeedDef"]
        assert product["required"] == ["id", "version"]
        assert product["additionalProperties"] is False
        assert contract["required"] == ["output"]
        assert semantic["required"] == ["model_key"]
        assert semantic["additionalProperties"] is False

    def test_table_def_has_streaming_flag(self):
        s = generate_json_schema()
        assert s["$defs"]["TableDef"]["properties"]["streaming"]["type"] == "boolean"

    def test_materialization_def_shapes(self):
        """MaterializationDef accepts the string shorthand and the dict form."""
        s = generate_json_schema()
        mat = s["$defs"]["MaterializationDef"]
        shorthand, dict_form = mat["oneOf"]
        assert "streaming_table" in shorthand["enum"]
        assert "materialized_view" in shorthand["enum"]
        assert set(dict_form["properties"]) == {
            "type", "trigger", "checkpoint", "write_mode", "keys",
            "schedule", "comment", "cluster_by", "partition_by", "refresh",
        }
        assert dict_form["properties"]["write_mode"]["enum"] == ["append", "upsert"]
        assert dict_form["properties"]["refresh"]["enum"] == ["auto", "never"]

    def test_aggregate_def_shape(self):
        s = generate_json_schema()
        agg = s["$defs"]["AggregateDef"]
        assert agg["required"] == ["group_by", "measures"]
        assert set(agg["properties"]) == {"group_by", "measures", "having"}
        measure_dict = s["$defs"]["MeasureEntry"]["oneOf"][1]
        assert "count_distinct" in measure_dict["properties"]["func"]["enum"]

    def test_sink_type_enum(self):
        s = generate_json_schema()
        sink_type = s["$defs"]["SinkDef"]["properties"]["type"]["enum"]
        assert "delta" in sink_type
        assert "postgres" in sink_type
        assert "jdbc" in sink_type

    def test_source_type_enum(self):
        s = generate_json_schema()
        source_type = s["$defs"]["SourceDef"]["properties"]["type"]["enum"]
        for fmt in ("csv", "parquet", "json", "delta"):
            assert fmt in source_type

    def test_deterministic_output(self):
        """generate_json_schema() returns the same result on repeated calls."""
        s1 = generate_json_schema()
        s2 = generate_json_schema()
        assert json.dumps(s1, sort_keys=True) == json.dumps(s2, sort_keys=True)


# ---------------------------------------------------------------------------
# Non-drift test
# ---------------------------------------------------------------------------

class TestNonDrift:
    def test_committed_schema_matches_generated(self):
        """
        The committed schemas/skifer-pipeline.schema.json must match what
        generate_json_schema() produces.  If this fails, regenerate the file:

            python -c "from skifer.core.json_schema import generate_json_schema; \\
                       import json; print(json.dumps(generate_json_schema(), indent=2))" \\
                   > schemas/skifer-pipeline.schema.json
        """
        assert SCHEMA_FILE.exists(), f"Committed schema not found: {SCHEMA_FILE}"
        committed = _load_committed_schema()
        generated = generate_json_schema()
        assert committed == generated, (
            "schemas/skifer-pipeline.schema.json is stale. "
            "Regenerate it by running generate_json_schema() and writing the output."
        )


# ---------------------------------------------------------------------------
# Validation tests (require jsonschema)
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not _HAS_JSONSCHEMA, reason="jsonschema not installed")
class TestValidation:
    @pytest.fixture(autouse=True)
    def _schema(self):
        self.schema = generate_json_schema()

    def _validate(self, instance):
        jsonschema.validate(instance=instance, schema=self.schema)

    def _expect_fail(self, instance):
        with pytest.raises(jsonschema.ValidationError):
            self._validate(instance)

    def test_minimal_valid_pipeline(self):
        """Minimal pipeline with only tables passes."""
        self._validate({
            "tables": [{"name": "silver.orders", "alias": "ord"}],
        })

    def test_select_final_list_form_valid(self):
        self._validate({
            "tables": [{"name": "silver.orders"}],
            "select_final": [["amount", "amount_eur", ["cast:double"]]],
        })

    def test_select_final_source_target_form_valid(self):
        self._validate({
            "tables": [{"name": "silver.orders"}],
            "select_final": [{"source": "amount", "target": "amount_eur", "ops": ["cast:double"]}],
        })

    def test_select_final_from_as_form_valid(self):
        self._validate({
            "tables": [{"name": "silver.orders"}],
            "select_final": [{"from": "amount", "as": "amount_eur", "ops": [{"cast": "double"}]}],
        })

    def test_filter_list_form_valid(self):
        self._validate({
            "tables": [{"name": "silver.orders", "filter": ["region:equals:EMEA"]}],
        })

    def test_filter_dict_form_valid(self):
        self._validate({
            "tables": [{
                "name": "silver.orders",
                "filter": [{"column": "region", "operator": "equals", "value": "EMEA"}],
            }],
        })

    def test_filter_mapping_form_valid(self):
        self._validate({
            "tables": [{
                "name": "silver.orders",
                "filter": {"region": "EMEA"},
            }],
        })

    def test_sink_valid(self):
        self._validate({
            "tables": [{"name": "silver.orders"}],
            "sink": {"type": "postgres"},
        })

    def test_keep_all_columns_valid(self):
        self._validate({
            "tables": [{"name": "silver.orders"}],
            "keep_all_columns": True,
        })

    def test_dev_limit_valid(self):
        self._validate({
            "tables": [{"name": "silver.orders"}],
            "dev_limit": 1000,
        })

    def test_dev_limit_must_be_positive(self):
        self._expect_fail({
            "tables": [{"name": "silver.orders"}],
            "dev_limit": 0,
        })

    def test_agent_ready_metadata_valid(self):
        self._validate({
            "tables": [{"name": "silver.orders"}],
            "select_final": [["order_id", "order_id"]],
            "data_product": {"id": "sales.orders", "version": "1.0.0"},
            "contract": {"output": {"order_id": {"required": True}}},
            "semantic": {"model_key": "orders", "dimensions": ["order_id"]},
        })

    def test_agent_ready_metadata_invalid_shapes_fail(self):
        self._expect_fail({
            "tables": [{"name": "silver.orders"}],
            "data_product": {"id": "sales orders", "version": "1.0"},
            "contract": {"output": ["order_id"]},
            "semantic": {"model_key": "orders", "sql": "SELECT 1"},
        })

    def test_sink_unknown_type_fails(self):
        self._expect_fail({
            "tables": [{"name": "silver.orders"}],
            "sink": {"type": "kafka"},
        })

    def test_sink_missing_type_fails(self):
        self._expect_fail({
            "tables": [{"name": "silver.orders"}],
            "sink": {"schema": "analytics"},
        })
