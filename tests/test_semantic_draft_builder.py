"""Tests for deterministic semantic draft generation (Plan 29 slice 0.3)."""
from pathlib import Path

import pytest
import yaml

from skifer.core.ir import parse_to_ir
from skifer.core.schema_loader import parse_schema
from skifer.semantic.draft_builder import SemanticDraftBuilder
from skifer.semantic.output_projection import OutputProjector
from skifer.semantic.validator import SemanticValidator


def _parse_ir(yaml_text: str):
    return parse_to_ir(parse_schema(yaml_text))


def _write_draft(yaml_text: str, tmp_path: Path):
    schema = _parse_ir(yaml_text)
    projected = OutputProjector().project(schema)
    builder = SemanticDraftBuilder(output_dir=str(tmp_path / "semantic_models"))
    path = Path(builder.write_draft(projected, schema))
    raw = path.read_text(encoding="utf-8")
    parsed = yaml.safe_load(raw)
    return path, raw, parsed, schema, projected


def test_builds_nominal_aggregate_draft_and_passes_validator(tmp_path):
    path, raw, parsed, _, projected = _write_draft(
        """
data_product:
  id: sales.orders
  version: 1.0.0
  description: Orders semantic draft
contract:
  grain: [country, order_day]
  output:
    country: {logical_type: string, description: Country}
    order_day: {logical_type: date, description: Order day}
    orders: {description: Distinct orders}
    total_amount: {description: Total amount}
semantic:
  model_key: orders_summary
  entity: order
  default_time_dimension: order_day
tables: [{name: silver.orders}]
aggregate:
  group_by: [country, order_day]
  measures:
    - [order_id, orders, count_distinct]
    - [amount, total_amount, sum]
sink: {type: delta, schema: gold, table: fact_orders}
"""
        ,
        tmp_path,
    )

    assert path == tmp_path / "semantic_models" / ".drafts" / "orders_summary.yaml"
    assert raw == yaml.safe_dump(parsed, sort_keys=False, allow_unicode=True, default_flow_style=False)
    assert parsed == {
        "_generated_by": {
            "tool": "skifer.semantic.draft_builder",
            "kind": "semantic_draft",
            "version": 1,
        },
        "models": [
            {
                "name": "orders_summary",
                "key": "orders_summary",
                "description": "Orders semantic draft",
                "layer": "gold",
                "table": "gold.fact_orders",
                "dimensions": [
                    {
                        "name": "country",
                        "sql": "country",
                        "type": "string",
                        "description": "Country",
                    },
                    {
                        "name": "order_day",
                        "sql": "order_day",
                        "type": "date",
                        "description": "Order day",
                    },
                ],
                "metrics": [
                    {
                        "name": "orders",
                        "sql": "order_id",
                        "type": "count_distinct",
                        "description": "Distinct orders",
                    },
                    {
                        "name": "total_amount",
                        "sql": "amount",
                        "type": "sum",
                        "description": "Total amount",
                    },
                ],
                "metadata": {
                    "source_contract_id": "sales.orders",
                    "source_contract_version": "1.0.0",
                    "source_definition_hash": projected.definition_hash,
                    "generated_fields": ["country", "order_day", "orders", "total_amount"],
                    "grain": ["country", "order_day"],
                },
                "entity": "order",
                "default_time_dimension": "order_day",
            }
        ],
    }

    validator = SemanticValidator()
    result = validator.validate_yaml(parsed)
    assert result.ok


def test_builds_select_final_draft_with_safe_fallback_metric(tmp_path):
    _, _, parsed, _, _ = _write_draft(
        """
data_product:
  id: sales.orders_detail
  version: 1.0.0
contract:
  grain: [order_id]
  output:
    order_id: {logical_type: identifier, description: Order id}
    order_day: {logical_type: date, description: Order day}
    source_name: {description: Source name}
semantic:
  model_key: orders_detail
  dimensions: [order_id, order_day]
  default_time_dimension: order_day
tables: [{name: silver.orders}]
select_final:
  - [id, order_id]
  - [order_timestamp, order_day, [cast:date]]
  - [literal:erp, source_name]
sink: {type: delta, schema: gold, table: fact_orders_detail}
"""
        ,
        tmp_path,
    )

    model = parsed["models"][0]
    assert model["dimensions"] == [
        {
            "name": "order_id",
            "sql": "order_id",
            "type": "string",
            "description": "Order id",
        },
        {
            "name": "order_day",
            "sql": "order_day",
            "type": "date",
            "description": "Order day",
        },
    ]
    assert model["metrics"] == [
        {
            "name": "row_count",
            "sql": "*",
            "type": "count",
            "description": "Deterministic fallback metric counting output rows.",
            "needs_curation": ["definition"],
        }
    ]

    result = SemanticValidator().validate_yaml(parsed)
    assert result.ok


def test_draft_generation_is_byte_for_byte_reproducible(tmp_path):
    yaml_text = """
data_product: {id: sales.orders, version: 1.0.0, description: Orders semantic draft}
contract:
  grain: [order_id]
  output:
    order_id: {logical_type: identifier}
semantic:
  model_key: orders_detail
  dimensions: [order_id]
tables: [{name: silver.orders}]
select_final:
  - [id, order_id]
sink: {type: delta, schema: gold, table: fact_orders_detail}
"""
    first_path, first_raw, _, _, _ = _write_draft(yaml_text, tmp_path)
    second_path, second_raw, _, _, _ = _write_draft(yaml_text, tmp_path)

    assert first_path == second_path
    assert first_raw == second_raw


def test_unknown_inference_field_is_not_silently_promoted_to_dimension(tmp_path):
    _, _, parsed, _, _ = _write_draft(
        """
data_product: {id: sales.orders, version: 1.0.0}
contract:
  grain: [order_id]
  output:
    order_id: {logical_type: identifier}
semantic:
  model_key: orders_unknown
  dimensions: [order_id]
tables: [{name: silver.orders}]
select_final:
  - [id, order_id]
  - [customer_name, customer_name]
sink: {type: delta, schema: gold, table: fact_orders_unknown}
"""
        ,
        tmp_path,
    )

    dimension_names = [item["name"] for item in parsed["models"][0]["dimensions"]]
    assert dimension_names == ["order_id"]
    assert "customer_name" not in dimension_names


def test_missing_seed_dimension_in_projection_raises_documented_error(tmp_path):
    schema = _parse_ir(
        """
data_product: {id: sales.orders, version: 1.0.0}
contract:
  output:
    missing: {description: Missing field}
semantic:
  model_key: orders_missing_dim
  dimensions: [missing]
tables: [{name: silver.orders}]
keep_all_columns: true
add_columns:
  - [id, order_id]
sink: {type: delta, schema: gold, table: fact_orders_missing_dim}
"""
    )
    projected = OutputProjector().project(schema)
    builder = SemanticDraftBuilder(output_dir=str(tmp_path / "semantic_models"))

    with pytest.raises(ValueError, match=r"\[semantic\.dimensions\] 'missing' is not a projected output\."):
        builder.build_draft(projected, schema)


def test_unmappable_declared_dimension_omits_type_and_marks_curation(tmp_path):
    _, _, parsed, _, _ = _write_draft(
        """
data_product: {id: sales.orders, version: 1.0.0}
contract:
  output:
    country: {description: Country}
semantic:
  model_key: orders_grouped
tables: [{name: silver.orders}]
aggregate:
  group_by: [country]
  measures:
    - [order_id, orders, count_distinct]
sink: {type: delta, schema: gold, table: fact_orders_grouped}
"""
        ,
        tmp_path,
    )

    dimension = parsed["models"][0]["dimensions"][0]
    assert dimension == {
        "name": "country",
        "sql": "country",
        "needs_curation": ["type"],
        "description": "Country",
    }


def test_refuses_to_overwrite_non_managed_draft(tmp_path):
    drafts_dir = tmp_path / "semantic_models" / ".drafts"
    drafts_dir.mkdir(parents=True)
    target_path = drafts_dir / "orders_summary.yaml"
    target_path.write_text("models:\n  - name: curated\n", encoding="utf-8")

    schema = _parse_ir(
        """
data_product: {id: sales.orders, version: 1.0.0}
contract:
  output:
    country: {logical_type: string}
semantic:
  model_key: orders_summary
tables: [{name: silver.orders}]
aggregate:
  group_by: [country]
  measures:
    - [order_id, orders, count_distinct]
sink: {type: delta, schema: gold, table: fact_orders}
"""
    )
    projected = OutputProjector().project(schema)
    builder = SemanticDraftBuilder(output_dir=str(tmp_path / "semantic_models"))

    with pytest.raises(
        ValueError,
        match=r"Refusing to overwrite curated model without --promote\.",
    ):
        builder.write_draft(projected, schema)


def test_overwrites_managed_draft(tmp_path):
    first_yaml = """
data_product: {id: sales.orders, version: 1.0.0, description: First}
contract:
  output:
    country: {logical_type: string}
semantic:
  model_key: orders_summary
tables: [{name: silver.orders}]
aggregate:
  group_by: [country]
  measures:
    - [order_id, orders, count_distinct]
sink: {type: delta, schema: gold, table: fact_orders}
"""
    second_yaml = """
data_product: {id: sales.orders, version: 1.0.0, description: Second}
contract:
  output:
    country: {logical_type: string}
semantic:
  model_key: orders_summary
tables: [{name: silver.orders}]
aggregate:
  group_by: [country]
  measures:
    - [order_id, orders, count_distinct]
    - [amount, total_amount, sum]
sink: {type: delta, schema: gold, table: fact_orders}
"""
    first_path, first_raw, _, _, _ = _write_draft(first_yaml, tmp_path)
    second_path, second_raw, parsed, _, _ = _write_draft(second_yaml, tmp_path)

    assert first_path == second_path
    assert first_raw != second_raw
    assert parsed["models"][0]["description"] == "Second"
    assert [metric["name"] for metric in parsed["models"][0]["metrics"]] == [
        "orders",
        "total_amount",
    ]


def test_semantic_only_aggregate_is_curated_instead_of_failing_the_draft(tmp_path):
    # The pipeline supports stddev/variance/sum_distinct/… but the semantic
    # layer does not. A valid pipeline must still produce a draft: the
    # unmappable measures are recorded for curation, not raised on.
    _, _, parsed, _, _ = _write_draft(
        """
data_product: {id: sales.orders, version: 1.0.0}
contract:
  output:
    country: {logical_type: string}
semantic:
  model_key: orders_stats
  dimensions: [country]
tables: [{name: silver.orders}]
aggregate:
  group_by: [country]
  measures:
    - [amount, total_amount, sum]
    - [amount, amount_stddev, stddev]
    - [amount, amount_variance, variance]
sink: {type: delta, schema: gold, table: fact_orders_stats}
"""
        ,
        tmp_path,
    )

    model = parsed["models"][0]
    assert [metric["name"] for metric in model["metrics"]] == ["total_amount"]
    assert model["metadata"]["unmapped_measures"] == ["amount_stddev", "amount_variance"]
    assert SemanticValidator().validate_yaml(parsed).ok


def test_draft_without_any_mappable_measure_still_validates(tmp_path):
    _, _, parsed, _, _ = _write_draft(
        """
data_product: {id: sales.orders, version: 1.0.0}
contract:
  output:
    country: {logical_type: string}
semantic:
  model_key: orders_spread
  dimensions: [country]
tables: [{name: silver.orders}]
aggregate:
  group_by: [country]
  measures:
    - [amount, amount_stddev, stddev]
sink: {type: delta, schema: gold, table: fact_orders_spread}
"""
        ,
        tmp_path,
    )

    model = parsed["models"][0]
    assert [metric["name"] for metric in model["metrics"]] == ["row_count"]
    assert model["metrics"][0]["needs_curation"] == ["definition"]
    assert model["metadata"]["unmapped_measures"] == ["amount_stddev"]
    assert SemanticValidator().validate_yaml(parsed).ok


def test_unmapped_measures_key_is_absent_when_everything_maps(tmp_path):
    _, _, parsed, _, _ = _write_draft(
        """
data_product: {id: sales.orders, version: 1.0.0}
contract:
  output:
    country: {logical_type: string}
semantic:
  model_key: orders_clean
  dimensions: [country]
tables: [{name: silver.orders}]
aggregate:
  group_by: [country]
  measures:
    - [amount, total_amount, sum]
sink: {type: delta, schema: gold, table: fact_orders_clean}
"""
        ,
        tmp_path,
    )

    assert "unmapped_measures" not in parsed["models"][0]["metadata"]


def test_join_becomes_proposed_relationship_candidate_with_unique_check_evidence(tmp_path):
    _, _, parsed, _, _ = _write_draft(
        """
data_product: {id: sales.orders, version: 1.0.0}
contract:
  grain: [order_id]
  output:
    order_id: {logical_type: identifier, entity: order}
    customer_fk: {logical_type: identifier, entity: customer}
    customer_pk: {logical_type: identifier, entity: customer}
semantic:
  model_key: orders
tables:
  - name: silver.orders
    alias: orders_src
  - name: silver.customers
    alias: customers_src
    quality_checks:
      drop_duplicates_on: [customer_pk]
join:
  - table_from: orders_src
    on_from: [customer_fk]
    table_to: customers_src
    on_to: [customer_pk]
    type: left
select_final:
  - [order_id, order_id]
  - [customer_fk, customer_fk]
  - [customer_pk, customer_pk]
sink: {type: delta, schema: gold, table: fact_orders}
"""
        ,
        tmp_path,
    )

    model = parsed["models"][0]
    assert "relationships" not in model
    assert model["metadata"]["relationship_candidates"] == [
        {
            "name": "orders_customer",
            "from_entity": "customer",
            # to_entity names an entity of the TARGET model, which this pipeline
            # cannot see — it is left for curation rather than guessed, and the
            # physical table is recorded so the to_model guess can be checked.
            "to_model": "customers",
            "to_table": "silver.customers",
            "to_entity": None,
            "cardinality": "many_to_one",
            "join_type": "left",
            "status": "proposed",
            "requires_curation": ["to_model", "to_entity"],
            "verified_by_contract": True,
            "join_keys": {
                "from": ["customer_fk"],
                "to": ["customer_pk"],
            },
            "evidence": {"to": "unique_check"},
        }
    ]
    assert SemanticValidator().validate_yaml(parsed).ok


def test_composite_join_candidate_preserves_declared_key_order(tmp_path):
    _, _, parsed, _, _ = _write_draft(
        """
data_product: {id: sales.order_lines, version: 1.0.0}
contract:
  output:
    order_id: {logical_type: identifier, entity: order_line}
    line_id: {logical_type: identifier, entity: order_line}
    parent_order_id: {logical_type: identifier, entity: order}
    parent_line_id: {logical_type: identifier, entity: order}
semantic:
  model_key: order_lines
tables:
  - name: silver.order_lines
    alias: lines_src
  - name: silver.orders
    alias: orders_src
join:
  - table_from: lines_src
    on_from: [parent_order_id, parent_line_id]
    table_to: orders_src
    on_to: [order_id, line_id]
    type: inner
select_final:
  - [parent_order_id, parent_order_id]
  - [parent_line_id, parent_line_id]
  - [order_id, order_id]
  - [line_id, line_id]
sink: {type: delta, schema: gold, table: fact_order_lines}
"""
        ,
        tmp_path,
    )

    candidate = parsed["models"][0]["metadata"]["relationship_candidates"][0]
    assert candidate["join_keys"] == {
        "from": ["parent_order_id", "parent_line_id"],
        "to": ["order_id", "line_id"],
    }


def test_join_without_uniqueness_evidence_stays_unknown_and_non_queryable(tmp_path):
    _, _, parsed, _, _ = _write_draft(
        """
data_product: {id: sales.orders, version: 1.0.0}
contract:
  output:
    customer_fk: {logical_type: identifier, entity: customer}
    customer_pk: {logical_type: identifier, entity: customer}
semantic:
  model_key: orders
tables:
  - name: silver.orders
    alias: orders_src
  - name: silver.customers
    alias: customers_src
join:
  - table_from: orders_src
    on_from: [customer_fk]
    table_to: customers_src
    on_to: [customer_pk]
    type: left
select_final:
  - [customer_fk, customer_fk]
  - [customer_pk, customer_pk]
sink: {type: delta, schema: gold, table: fact_orders}
"""
        ,
        tmp_path,
    )

    model = parsed["models"][0]
    candidate = model["metadata"]["relationship_candidates"][0]
    assert candidate["cardinality"] == "unknown"
    assert candidate["status"] == "proposed"
    assert candidate["verified_by_contract"] is False
    assert "evidence" not in candidate
    assert "relationships" not in model


def test_grain_can_justify_one_side_of_a_proposed_relationship(tmp_path):
    _, _, parsed, _, _ = _write_draft(
        """
data_product: {id: sales.customer_snapshot, version: 1.0.0}
contract:
  grain: [customer_id]
  output:
    customer_id: {logical_type: identifier, entity: customer}
    account_customer_id: {logical_type: identifier, entity: customer}
semantic:
  model_key: customer_snapshot
tables:
  - name: silver.customers
    alias: customers_src
  - name: silver.accounts
    alias: accounts_src
join:
  - table_from: customers_src
    on_from: [customer_id]
    table_to: accounts_src
    on_to: [account_customer_id]
    type: left
select_final:
  - [customer_id, customer_id]
  - [account_customer_id, account_customer_id]
sink: {type: delta, schema: gold, table: fact_customer_snapshot}
"""
        ,
        tmp_path,
    )

    candidate = parsed["models"][0]["metadata"]["relationship_candidates"][0]
    assert candidate["cardinality"] == "one_to_many"
    assert candidate["evidence"] == {"from": "contract_grain"}


def test_cross_and_anti_joins_are_rejected_with_recorded_reason(tmp_path):
    _, _, parsed, _, _ = _write_draft(
        """
data_product: {id: sales.orders, version: 1.0.0}
contract:
  output:
    customer_fk: {logical_type: identifier, entity: customer}
    customer_pk: {logical_type: identifier, entity: customer}
semantic:
  model_key: orders
tables:
  - name: silver.orders
    alias: orders_src
  - name: silver.customers
    alias: customers_src
join:
  - table_from: orders_src
    on_from: [customer_fk]
    table_to: customers_src
    on_to: [customer_pk]
    type: cross
  - table_from: orders_src
    on_from: [customer_fk]
    table_to: customers_src
    on_to: [customer_pk]
    type: left_anti
select_final:
  - [customer_fk, customer_fk]
  - [customer_pk, customer_pk]
sink: {type: delta, schema: gold, table: fact_orders}
"""
        ,
        tmp_path,
    )

    metadata = parsed["models"][0]["metadata"]
    assert "relationship_candidates" not in metadata
    assert metadata["relationship_candidate_rejections"] == [
        {
            "alias_left": "orders_src",
            "keys_left": ["customer_fk"],
            "alias_right": "customers_src",
            "keys_right": ["customer_pk"],
            "join_type": "cross",
            "reason": "join_type 'cross' is not eligible for automatic semantic proposals (v1 supports only inner/left).",
        },
        {
            "alias_left": "orders_src",
            "keys_left": ["customer_fk"],
            "alias_right": "customers_src",
            "keys_right": ["customer_pk"],
            "join_type": "left_anti",
            "reason": "join_type 'left_anti' is not eligible for automatic semantic proposals (v1 supports only inner/left).",
        },
    ]


def test_join_candidates_are_byte_for_byte_reproducible(tmp_path):
    yaml_text = """
data_product: {id: sales.orders, version: 1.0.0}
contract:
  output:
    customer_fk: {logical_type: identifier, entity: customer}
    customer_pk: {logical_type: identifier, entity: customer}
semantic:
  model_key: orders
tables:
  - name: silver.orders
    alias: orders_src
  - name: silver.customers
    alias: customers_src
    quality_checks:
      drop_duplicates_on: [customer_pk]
join:
  - table_from: orders_src
    on_from: [customer_fk]
    table_to: customers_src
    on_to: [customer_pk]
    type: left
select_final:
  - [customer_fk, customer_fk]
  - [customer_pk, customer_pk]
sink: {type: delta, schema: gold, table: fact_orders}
"""
    first_path, first_raw, _, _, _ = _write_draft(yaml_text, tmp_path)
    second_path, second_raw, _, _, _ = _write_draft(yaml_text, tmp_path)

    assert first_path == second_path
    assert first_raw == second_raw


def test_pipeline_without_joins_does_not_add_candidate_metadata(tmp_path):
    _, _, parsed, _, _ = _write_draft(
        """
data_product: {id: sales.orders, version: 1.0.0}
contract:
  output:
    order_id: {logical_type: identifier, entity: order}
semantic:
  model_key: orders
tables: [{name: silver.orders}]
select_final:
  - [id, order_id]
sink: {type: delta, schema: gold, table: fact_orders}
"""
        ,
        tmp_path,
    )

    metadata = parsed["models"][0]["metadata"]
    assert "relationship_candidates" not in metadata
    assert "relationship_candidate_rejections" not in metadata


def test_candidate_is_not_affected_by_a_colliding_join_key_name(tmp_path):
    # The target-side entity used to be resolved from THIS model's projected
    # outputs, matched by bare column name. A join onto a column named `id` —
    # about the most common name there is — silently picked up the local `order`
    # entity and emitted a confidently wrong candidate (orders_order, customer
    # → order). Without a collision the join was rejected outright, so the
    # derivation was either wrong or useless, never right.
    def draft_for(order_pk: str):
        _, _, parsed, _, _ = _write_draft(
            f"""
data_product: {{id: sales.orders, version: 1.0.0}}
contract:
  grain: [order_id]
  output:
    order_id:    {{logical_type: identifier, entity: order}}
    customer_id: {{logical_type: identifier, entity: customer}}
semantic:
  model_key: orders
tables:
  - {{name: silver.orders, alias: ord}}
  - name: gold.dim_customers
    alias: cust
    quality_checks:
      drop_duplicates_on: [id]
join:
  - table_from: [ord, customer_id]
    table_to: [cust, id]
    type: left
select_final:
  - [{order_pk}, order_id]
  - [customer_id, customer_id]
sink: {{type: delta, schema: gold, table: fact_orders}}
"""
            ,
            tmp_path / order_pk,
        )
        return parsed["models"][0]["metadata"]["relationship_candidates"]

    colliding = draft_for("id")
    non_colliding = draft_for("order_ref")

    # identical either way: the candidate no longer depends on a name collision
    assert colliding == non_colliding
    assert len(colliding) == 1
    candidate = colliding[0]
    assert candidate["name"] == "orders_customer"
    assert candidate["from_entity"] == "customer"
    assert candidate["to_entity"] is None
    assert candidate["to_table"] == "gold.dim_customers"
    assert candidate["requires_curation"] == ["to_model", "to_entity"]
