"""Tests for semantic draft synchronization (Plan 29 slice 0.4)."""
from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import yaml

from skifer.core.ir import parse_to_ir
from skifer.core.schema_loader import parse_schema
from skifer.semantic.draft_builder import SemanticDraftBuilder
from skifer.semantic.output_projection import OutputProjector
from skifer.semantic.sync import SemanticSynchronizer


def _parse_ir(yaml_text: str):
    return parse_to_ir(parse_schema(yaml_text))


def _build_payload(yaml_text: str, tmp_path: Path):
    schema = _parse_ir(yaml_text)
    projected = OutputProjector().project(schema)
    builder = SemanticDraftBuilder(output_dir=str(tmp_path / "semantic_models"))
    payload = builder.build_draft(projected, schema)
    return payload, schema, projected


def _write_yaml(path: Path, payload: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(payload, sort_keys=False, allow_unicode=True, default_flow_style=False),
        encoding="utf-8",
    )


def _seed_base_and_curated(tmp_path: Path, yaml_text: str, *, curated_mutation=None):
    payload, _, _ = _build_payload(yaml_text, tmp_path)
    model_key = payload["models"][0]["key"]
    output_dir = tmp_path / "semantic_models"
    draft_path = output_dir / ".drafts" / f"{model_key}.yaml"
    curated_path = output_dir / f"{model_key}.yaml"
    _write_yaml(draft_path, payload)
    curated_payload = deepcopy(payload)
    if curated_mutation is not None:
        curated_mutation(curated_payload)
    _write_yaml(curated_path, curated_payload)
    return payload, draft_path, curated_path


def _sync(tmp_path: Path, yaml_text: str, *, write: bool = False):
    schema = _parse_ir(yaml_text)
    projected = OutputProjector().project(schema)
    synchronizer = SemanticSynchronizer(output_dir=str(tmp_path / "semantic_models"))
    return synchronizer.sync(projected, schema, write=write)


def test_sync_reports_compatible_added_metric_and_writes_updated_draft(tmp_path):
    base_yaml = """
data_product: {id: sales.orders, version: 1.0.0}
contract:
  grain: [country]
  output:
    country: {logical_type: string, description: Country}
    orders: {description: Orders}
semantic:
  model_key: orders_sync
tables: [{name: silver.orders}]
aggregate:
  group_by: [country]
  measures:
    - [order_id, orders, count_distinct]
sink: {type: delta, schema: gold, table: fact_orders_sync}
"""
    next_yaml = """
data_product: {id: sales.orders, version: 1.0.0}
contract:
  grain: [country]
  output:
    country: {logical_type: string, description: Country}
    orders: {description: Orders}
    total_amount: {description: Total amount}
semantic:
  model_key: orders_sync
tables: [{name: silver.orders}]
aggregate:
  group_by: [country]
  measures:
    - [order_id, orders, count_distinct]
    - [amount, total_amount, sum]
sink: {type: delta, schema: gold, table: fact_orders_sync}
"""
    _seed_base_and_curated(tmp_path, base_yaml)

    report = _sync(tmp_path, next_yaml, write=True)

    assert report.safe_to_apply is True
    assert report.conflicts == ()
    assert report.suggestions == ()
    assert report.wrote is True
    assert report.changes == (
        type(report.changes[0])(
            kind="add_field",
            target="metrics.total_amount",
            before=None,
            after="total_amount",
            details={"collection": "metrics"},
        ),
    )

    synced = yaml.safe_load((tmp_path / "semantic_models" / ".drafts" / "orders_sync.yaml").read_text())
    assert [metric["name"] for metric in synced["models"][0]["metrics"]] == ["orders", "total_amount"]


def test_sync_reports_unreferenced_generated_metric_removal(tmp_path):
    base_yaml = """
data_product: {id: sales.orders, version: 1.0.0}
contract:
  grain: [country]
  output:
    country: {logical_type: string}
    orders: {}
    total_amount: {}
semantic:
  model_key: orders_sync
tables: [{name: silver.orders}]
aggregate:
  group_by: [country]
  measures:
    - [order_id, orders, count_distinct]
    - [amount, total_amount, sum]
sink: {type: delta, schema: gold, table: fact_orders_sync}
"""
    next_yaml = """
data_product: {id: sales.orders, version: 1.0.0}
contract:
  grain: [country]
  output:
    country: {logical_type: string}
    orders: {}
semantic:
  model_key: orders_sync
tables: [{name: silver.orders}]
aggregate:
  group_by: [country]
  measures:
    - [order_id, orders, count_distinct]
sink: {type: delta, schema: gold, table: fact_orders_sync}
"""
    _seed_base_and_curated(tmp_path, base_yaml)

    report = _sync(tmp_path, next_yaml)

    assert report.safe_to_apply is True
    assert report.wrote is False
    assert report.conflicts == ()
    assert report.suggestions == ()
    assert report.changes == (
        type(report.changes[0])(
            kind="remove_field",
            target="metrics.total_amount",
            before="total_amount",
            after=None,
            details={"collection": "metrics"},
        ),
    )


def test_sync_reports_removed_generated_column_still_used_by_curated_metric(tmp_path):
    base_yaml = """
data_product: {id: sales.orders, version: 1.0.0}
contract:
  grain: [order_id]
  output:
    order_id: {logical_type: identifier}
    net_revenue: {logical_type: integer}
semantic:
  model_key: orders_sync
  dimensions: [order_id, net_revenue]
tables: [{name: silver.orders}]
select_final:
  - [id, order_id]
  - [amount, net_revenue]
sink: {type: delta, schema: gold, table: fact_orders_sync}
"""
    next_yaml = """
data_product: {id: sales.orders, version: 1.0.0}
contract:
  grain: [order_id]
  output:
    order_id: {logical_type: identifier}
semantic:
  model_key: orders_sync
  dimensions: [order_id]
tables: [{name: silver.orders}]
select_final:
  - [id, order_id]
sink: {type: delta, schema: gold, table: fact_orders_sync}
"""

    def add_curated_metric(payload: dict):
        payload["models"][0]["metrics"].append(
            {
                "name": "m",
                "sql": "net_revenue",
                "type": "sum",
                "description": "Curated revenue metric",
            }
        )

    _seed_base_and_curated(tmp_path, base_yaml, curated_mutation=add_curated_metric)

    report = _sync(tmp_path, next_yaml)

    assert report.safe_to_apply is False
    assert report.wrote is False
    assert report.changes == ()
    assert len(report.conflicts) == 1
    conflict = report.conflicts[0]
    assert conflict.kind == "removed_dependency"
    assert conflict.message == "Semantic sync conflict: metric 'm' depends on removed column 'net_revenue'."
    assert conflict.target == "dimensions.net_revenue"
    assert conflict.details == {"metric": "m", "column": "net_revenue"}


def test_sync_applies_type_widening_but_blocks_narrowing(tmp_path):
    base_yaml = """
data_product: {id: sales.orders, version: 1.0.0}
contract:
  grain: [order_id]
  output:
    order_id: {logical_type: integer}
semantic:
  model_key: orders_sync
  dimensions: [order_id]
tables: [{name: silver.orders}]
select_final:
  - [id, order_id]
sink: {type: delta, schema: gold, table: fact_orders_sync}
"""
    widened_yaml = """
data_product: {id: sales.orders, version: 1.0.0}
contract:
  grain: [order_id]
  output:
    order_id: {logical_type: float}
semantic:
  model_key: orders_sync
  dimensions: [order_id]
tables: [{name: silver.orders}]
select_final:
  - [id, order_id]
sink: {type: delta, schema: gold, table: fact_orders_sync}
"""
    narrowed_yaml = base_yaml
    _seed_base_and_curated(tmp_path, base_yaml)

    widened = _sync(tmp_path, widened_yaml)

    assert widened.safe_to_apply is True
    assert widened.conflicts == ()
    assert widened.changes == (
        type(widened.changes[0])(
            kind="widen_type",
            target="dimensions.order_id.type",
            before="integer",
            after="float",
            details={},
        ),
    )

    _seed_base_and_curated(tmp_path, widened_yaml)
    narrowed = _sync(tmp_path, narrowed_yaml)

    assert narrowed.safe_to_apply is False
    assert narrowed.wrote is False
    assert narrowed.changes == ()
    assert len(narrowed.conflicts) == 1
    conflict = narrowed.conflicts[0]
    assert conflict.kind == "narrow_type"
    assert conflict.target == "dimensions.order_id.type"
    assert conflict.details == {
        "before": "float",
        "current": "float",
        "candidate": "integer",
    }


def test_sync_reports_grain_change_as_conflict(tmp_path):
    base_yaml = """
data_product: {id: sales.orders, version: 1.0.0}
contract:
  grain: [country]
  output:
    country: {logical_type: string}
    orders: {}
semantic:
  model_key: orders_sync
tables: [{name: silver.orders}]
aggregate:
  group_by: [country]
  measures:
    - [order_id, orders, count_distinct]
sink: {type: delta, schema: gold, table: fact_orders_sync}
"""
    next_yaml = """
data_product: {id: sales.orders, version: 1.0.0}
contract:
  grain: [country, order_day]
  output:
    country: {logical_type: string}
    order_day: {logical_type: date}
    orders: {}
semantic:
  model_key: orders_sync
tables: [{name: silver.orders}]
aggregate:
  group_by: [country, order_day]
  measures:
    - [order_id, orders, count_distinct]
sink: {type: delta, schema: gold, table: fact_orders_sync}
"""
    _seed_base_and_curated(tmp_path, base_yaml)

    report = _sync(tmp_path, next_yaml)

    assert report.safe_to_apply is False
    assert report.wrote is False
    assert len(report.conflicts) == 1
    conflict = report.conflicts[0]
    assert conflict.kind == "grain_changed"
    assert conflict.target == "grain"
    assert conflict.details == {"before": ("country",), "after": ("country", "order_day")}


def test_sync_suggests_metric_rename_without_applying_it(tmp_path):
    base_yaml = """
data_product: {id: sales.orders, version: 1.0.0}
contract:
  grain: [country]
  output:
    country: {logical_type: string}
    customer_name: {}
semantic:
  model_key: orders_sync
tables: [{name: silver.orders}]
aggregate:
  group_by: [country]
  measures:
    - [customer_name, customer_name, count_distinct]
sink: {type: delta, schema: gold, table: fact_orders_sync}
"""
    next_yaml = """
data_product: {id: sales.orders, version: 1.0.0}
contract:
  grain: [country]
  output:
    country: {logical_type: string}
    customer_names: {}
semantic:
  model_key: orders_sync
tables: [{name: silver.orders}]
aggregate:
  group_by: [country]
  measures:
    - [customer_name, customer_names, count_distinct]
sink: {type: delta, schema: gold, table: fact_orders_sync}
"""
    _seed_base_and_curated(tmp_path, base_yaml)

    report = _sync(tmp_path, next_yaml, write=True)

    assert report.safe_to_apply is False
    assert report.wrote is False
    assert report.conflicts == ()
    assert report.changes == ()
    assert report.suggestions == (
        type(report.suggestions[0])(
            kind="rename_suggestion",
            target="metrics.customer_name",
            before="customer_name",
            after="customer_names",
            details={"collection": "metrics"},
        ),
    )


def test_sync_preserves_curated_description_when_regenerating_and_stays_idempotent_on_rerun(tmp_path):
    base_yaml = """
data_product: {id: sales.orders, version: 1.0.0}
contract:
  grain: [country]
  output:
    country: {logical_type: string, description: Country}
    orders: {description: Orders}
semantic:
  model_key: orders_sync
tables: [{name: silver.orders}]
aggregate:
  group_by: [country]
  measures:
    - [order_id, orders, count_distinct]
sink: {type: delta, schema: gold, table: fact_orders_sync}
"""
    next_yaml = """
data_product: {id: sales.orders, version: 1.0.0}
contract:
  grain: [country]
  output:
    country: {logical_type: string, description: Country}
    orders: {description: Orders}
    total_amount: {description: Total amount}
semantic:
  model_key: orders_sync
tables: [{name: silver.orders}]
aggregate:
  group_by: [country]
  measures:
    - [order_id, orders, count_distinct]
    - [amount, total_amount, sum]
sink: {type: delta, schema: gold, table: fact_orders_sync}
"""

    def curate_description(payload: dict):
        payload["models"][0]["dimensions"][0]["description"] = "Country label curated by a human"

    _seed_base_and_curated(tmp_path, base_yaml, curated_mutation=curate_description)

    first = _sync(tmp_path, next_yaml, write=True)

    assert first.safe_to_apply is True
    assert first.wrote is True
    synced = yaml.safe_load((tmp_path / "semantic_models" / ".drafts" / "orders_sync.yaml").read_text())
    assert synced["models"][0]["dimensions"][0]["description"] == "Country label curated by a human"

    second = _sync(tmp_path, next_yaml, write=True)

    assert second.safe_to_apply is True
    assert second.wrote is False
    assert second.changes == ()
    assert second.conflicts == ()
    assert second.suggestions == ()


def test_sync_reports_grain_change_on_a_select_final_pipeline(tmp_path):
    # Grain used to be inferred from generated dimension names and gated on
    # aggregate metrics being present, so a select_final pipeline could change
    # its declared contract.grain completely undetected. The draft now persists
    # metadata.grain and sync compares it directly.
    base_yaml = """
data_product: {id: sales.orders, version: 1.0.0}
contract:
  grain: [order_id]
  output:
    order_id: {logical_type: identifier}
    line_id: {logical_type: identifier}
semantic:
  model_key: orders_lines
  dimensions: [order_id, line_id]
tables: [{name: silver.orders}]
select_final:
  - [id, order_id]
  - [line, line_id]
sink: {type: delta, schema: gold, table: fact_order_lines}
"""
    next_yaml = """
data_product: {id: sales.orders, version: 1.0.0}
contract:
  grain: [order_id, line_id]
  output:
    order_id: {logical_type: identifier}
    line_id: {logical_type: identifier}
semantic:
  model_key: orders_lines
  dimensions: [order_id, line_id]
tables: [{name: silver.orders}]
select_final:
  - [id, order_id]
  - [line, line_id]
sink: {type: delta, schema: gold, table: fact_order_lines}
"""
    _seed_base_and_curated(tmp_path, base_yaml)

    report = _sync(tmp_path, next_yaml)

    assert report.safe_to_apply is False
    assert report.wrote is False
    grain_conflicts = [c for c in report.conflicts if c.kind == "grain_changed"]
    assert len(grain_conflicts) == 1
    assert grain_conflicts[0].details == {
        "before": ("order_id",),
        "after": ("order_id", "line_id"),
    }


def test_sync_preserves_curated_model_level_keys_outside_any_allow_list(tmp_path):
    # A fixed model-level allow-list silently dropped every key it did not know
    # about — notably `synonyms`, which build_from_projection() is explicitly
    # allowed to write, permanently blocking --promote after any pipeline drift.
    base_yaml = """
data_product: {id: sales.orders, version: 1.0.0}
contract:
  grain: [country]
  output:
    country: {logical_type: string}
    orders: {}
semantic:
  model_key: orders_sync
tables: [{name: silver.orders}]
aggregate:
  group_by: [country]
  measures:
    - [order_id, orders, count_distinct]
sink: {type: delta, schema: gold, table: fact_orders_sync}
"""
    next_yaml = base_yaml.replace(
        "    - [order_id, orders, count_distinct]",
        "    - [order_id, orders, count_distinct]\n    - [amount, total_amount, sum]",
    )
    _seed_base_and_curated(tmp_path, base_yaml)

    curated_path = tmp_path / "semantic_models" / "orders_sync.yaml"
    curated = yaml.safe_load(curated_path.read_text(encoding="utf-8"))
    curated["models"][0]["synonyms"] = ["commandes", "ventes"]
    curated["models"][0]["tags"] = ["revenue"]
    curated_path.write_text(
        yaml.safe_dump(curated, sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )

    report = _sync(tmp_path, next_yaml)

    assert report.conflicts == ()
    merged_model = report.payload["models"][0]
    assert merged_model["synonyms"] == ["commandes", "ventes"]
    assert merged_model["tags"] == ["revenue"]
    assert [metric["name"] for metric in merged_model["metrics"]] == ["orders", "total_amount"]
