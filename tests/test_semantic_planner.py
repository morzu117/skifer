"""Tests for multi-model semantic planning (Plan 29 slice 6.4)."""

import pytest

from skifer.agentic.resolver import SemanticQuery, SemanticQueryError
from skifer.semantic.calendar import parse_calendar
from skifer.semantic.planner import SemanticPlanner


class FakeSemanticEngine:
    def __init__(self, models, calendars=None):
        self.models = models
        self.calendars = calendars or {}
        self.loaded: list[str] = []
        self._catalog = {
            key: {
                "key": key,
                "dimensions": [item["name"] for item in model.get("dimensions", [])],
                "metrics": [item["name"] for item in model.get("metrics", [])],
                "entities": [item["name"] for item in model.get("entities", [])],
                "related_models": sorted(
                    {
                        item["to_model"]
                        for item in model.get("relationships", [])
                    }
                ),
            }
            for key, model in models.items()
        }

    def _get_model(self, model_key):
        self.loaded.append(model_key)
        return self.models[model_key]

    def get_model_summary(self, model_key):
        if model_key not in self._catalog:
            raise ValueError(f"Unknown model '{model_key}'.")
        return self._catalog[model_key]

    def _get_calendar(self, calendar_key):
        return self.calendars[calendar_key]


def _relationship(
    name,
    from_entity,
    to_model,
    to_entity,
    *,
    cardinality="many_to_one",
):
    return {
        "name": name,
        "from_entity": from_entity,
        "to_model": to_model,
        "to_entity": to_entity,
        "cardinality": cardinality,
        "join_type": "left",
        "verified_by_contract": True,
    }


def _models():
    return {
        "orders": {
            "key": "orders",
            "table": "gold.orders",
            "grain": ["order"],
            "dimensions": [
                {"name": "order_date", "sql": "order_date", "type": "date"},
                {"name": "status", "sql": "status", "type": "string"},
            ],
            "metrics": [{"name": "revenue", "sql": "amount", "type": "sum"}],
            "entities": [
                {"name": "order", "type": "primary", "key": "order_id"},
                {"name": "customer", "type": "foreign", "key": "customer_id"},
                {"name": "store", "type": "foreign", "key": "store_id"},
            ],
            "relationships": [
                _relationship("orders_customer", "customer", "customers", "customer"),
            ],
        },
        "customers": {
            "key": "customers",
            "table": "gold.customers",
            "dimensions": [
                {"name": "segment", "sql": "segment", "type": "string"},
                {"name": "label", "sql": "label", "type": "string"},
            ],
            "metrics": [{"name": "customer_count", "sql": "customer_id", "type": "count"}],
            "entities": [
                {"name": "customer", "type": "primary", "key": "customer_id"},
                {"name": "region", "type": "foreign", "key": "region_id"},
            ],
            "relationships": [],
        },
        "stores": {
            "key": "stores",
            "table": "gold.stores",
            "dimensions": [{"name": "label", "sql": "label", "type": "string"}],
            "metrics": [{"name": "store_count", "sql": "store_id", "type": "count"}],
            "entities": [
                {"name": "store", "type": "primary", "key": "store_id"},
                {"name": "region", "type": "foreign", "key": "region_id"},
            ],
            "relationships": [],
        },
        "regions": {
            "key": "regions",
            "table": "gold.regions",
            "dimensions": [{"name": "region_name", "sql": "region_name", "type": "string"}],
            "metrics": [{"name": "region_count", "sql": "region_id", "type": "count"}],
            "entities": [{"name": "region", "type": "primary", "key": "region_id"}],
            "relationships": [],
        },
        "suppliers": {
            "key": "suppliers",
            "table": "gold.suppliers",
            "dimensions": [{"name": "supplier_name", "sql": "supplier_name", "type": "string"}],
            "metrics": [{"name": "supplier_count", "sql": "supplier_id", "type": "count"}],
            "entities": [{"name": "supplier", "type": "primary", "key": "supplier_id"}],
            "relationships": [],
        },
    }


def test_unqualified_root_name_resolves_without_a_join():
    semantic = FakeSemanticEngine(_models())
    plan = SemanticPlanner(semantic).plan(
        SemanticQuery(model_name="orders", metrics=["revenue"], group_by=["status"])
    )

    assert plan.required_models == ("orders",)
    assert plan.metrics[0].model_key == "orders"
    assert plan.dimensions[0].model_key == "orders"
    assert plan.joins == ()


def test_two_model_plan_resolves_owner_and_ordered_join_keys():
    semantic = FakeSemanticEngine(_models())
    plan = SemanticPlanner(semantic).plan(
        SemanticQuery(model_name="orders", metrics=["revenue"], group_by=["segment"])
    )

    assert plan.required_models == ("orders", "customers")
    assert plan.dimensions[0].model_key == "customers"
    assert plan.joins[0].source_key_columns == ("customer_id",)
    assert plan.joins[0].target_key_columns == ("customer_id",)
    assert plan.grain == ("order",)


def test_ambiguous_minimal_paths_raise_with_both_paths_named():
    models = _models()
    models["orders"]["relationships"].append(
        _relationship("orders_store", "store", "stores", "store")
    )
    models["customers"]["relationships"].append(
        _relationship("customers_region", "region", "regions", "region")
    )
    models["stores"]["relationships"].append(
        _relationship("stores_region", "region", "regions", "region")
    )

    with pytest.raises(SemanticQueryError) as error:
        SemanticPlanner(FakeSemanticEngine(models)).plan(
            SemanticQuery(
                model_name="orders",
                metrics=["revenue"],
                group_by=["regions.region_name"],
            )
        )

    assert (
        "Ambiguous semantic join path: orders→customers→regions or "
        "orders→stores→regions"
    ) in str(error.value)


def test_no_path_raises_actionable_metric_dimension_error():
    with pytest.raises(
        SemanticQueryError,
        match="No declared relationship connects metric 'revenue' to dimension 'supplier_name'",
    ):
        SemanticPlanner(FakeSemanticEngine(_models())).plan(
            SemanticQuery(
                model_name="orders",
                metrics=["revenue"],
                group_by=["suppliers.supplier_name"],
            )
        )


def test_unknown_relationship_cardinality_is_refused_for_metric_query():
    models = _models()
    models["orders"]["relationships"][0]["cardinality"] = "unknown"

    with pytest.raises(
        SemanticQueryError,
        match="cardinality is unknown; declare or certify uniqueness before querying",
    ):
        SemanticPlanner(FakeSemanticEngine(models)).plan(
            SemanticQuery(model_name="orders", metrics=["revenue"], group_by=["segment"])
        )


def test_many_to_many_is_refused_with_its_own_cause_not_as_unknown():
    # A declared many_to_many is not an *unknown* cardinality. Reporting it as
    # unknown told the author to declare something they already had declared.
    models = _models()
    models["orders"]["relationships"][0]["cardinality"] = "many_to_many"

    with pytest.raises(SemanticQueryError) as exc_info:
        SemanticPlanner(FakeSemanticEngine(models)).plan(
            SemanticQuery(model_name="orders", metrics=["revenue"], group_by=["segment"])
        )

    message = str(exc_info.value)
    assert "many_to_many" in message
    assert "fans the metric out" in message
    assert "unknown" not in message


def test_composite_join_keys_are_paired_in_declared_order():
    models = _models()
    models["orders"]["entities"][1]["key"] = ["customer_id", "customer_tenant"]
    models["customers"]["entities"][0]["key"] = ["id", "tenant"]

    plan = SemanticPlanner(FakeSemanticEngine(models)).plan(
        SemanticQuery(model_name="orders", metrics=["revenue"], group_by=["segment"])
    )

    assert plan.joins[0].source_key_columns == ("customer_id", "customer_tenant")
    assert plan.joins[0].target_key_columns == ("id", "tenant")


def test_join_identifiers_are_revalidated_at_planning_boundary():
    models = _models()
    models["orders"]["entities"][1]["key"] = "customer_id` OR 1=1 --"

    with pytest.raises(SemanticQueryError, match="Unsafe relationship key column"):
        SemanticPlanner(FakeSemanticEngine(models)).plan(
            SemanticQuery(model_name="orders", metrics=["revenue"], group_by=["segment"])
        )


def test_duplicate_non_root_name_requires_qualification():
    with pytest.raises(SemanticQueryError, match="Qualify it as one of") as error:
        SemanticPlanner(FakeSemanticEngine(_models())).plan(
            SemanticQuery(model_name="orders", metrics=["revenue"], group_by=["label"])
        )

    assert "customers.label" in str(error.value)
    assert "stores.label" in str(error.value)


def test_root_name_wins_when_same_name_exists_in_another_model():
    models = _models()
    models["customers"]["dimensions"].append(
        {"name": "status", "sql": "status", "type": "string"}
    )
    semantic = FakeSemanticEngine(models)

    plan = SemanticPlanner(semantic).plan(
        SemanticQuery(model_name="orders", metrics=["revenue"], group_by=["status"])
    )

    assert plan.dimensions[0].model_key == "orders"


def test_planner_does_not_load_unrelated_owner_candidates():
    semantic = FakeSemanticEngine(_models())
    SemanticPlanner(semantic).plan(
        SemanticQuery(model_name="orders", metrics=["revenue"], group_by=["segment"])
    )

    assert "suppliers" not in semantic.loaded
    assert "regions" not in semantic.loaded


def test_one_to_many_fanout_is_refused_with_mandatory_message():
    models = _models()
    relationship = models["orders"]["relationships"][0]
    relationship["name"] = "order_lines"
    relationship["cardinality"] = "one_to_many"

    with pytest.raises(SemanticQueryError) as exc_info:
        SemanticPlanner(FakeSemanticEngine(models)).plan(
            SemanticQuery(model_name="orders", metrics=["revenue"], group_by=["segment"])
        )

    assert str(exc_info.value) == (
        "Unsafe fanout: metric grain 'order' crosses one_to_many relationship "
        "'order_lines'."
    )


def test_one_to_many_is_allowed_for_dimension_only_query():
    models = _models()
    models["orders"]["relationships"][0]["cardinality"] = "one_to_many"

    plan = SemanticPlanner(FakeSemanticEngine(models)).plan(
        SemanticQuery(model_name="orders", group_by=["segment"])
    )

    assert plan.joins[0].cardinality == "one_to_many"


def _semi_additive_models():
    models = _models()
    models["orders"]["dimensions"].append(
        {"name": "snapshot_date", "sql": "snapshot_date", "type": "date"}
    )
    models["orders"]["metrics"][0].update(
        additivity="semi_additive",
        non_additive_dimensions=["snapshot_date"],
    )
    return models


def test_semi_additive_metric_is_pinned_by_group_by():
    plan = SemanticPlanner(FakeSemanticEngine(_semi_additive_models())).plan(
        SemanticQuery(
            model_name="orders", metrics=["revenue"], group_by=["snapshot_date"]
        )
    )

    assert plan.metrics[0].name == "revenue"


def test_semi_additive_metric_is_pinned_by_equality_filter():
    plan = SemanticPlanner(FakeSemanticEngine(_semi_additive_models())).plan(
        SemanticQuery(
            model_name="orders",
            metrics=["revenue"],
            filters=[
                {"column": "snapshot_date", "operator": "eq", "value": "2024-10-01"}
            ],
        )
    )

    assert plan.metrics[0].name == "revenue"


def test_semi_additive_metric_without_pin_is_refused():
    models = _semi_additive_models()
    models["orders"]["metrics"][0]["name"] = "balance"

    with pytest.raises(SemanticQueryError) as exc_info:
        SemanticPlanner(FakeSemanticEngine(models)).plan(
            SemanticQuery(model_name="orders", metrics=["balance"])
        )

    assert str(exc_info.value) == (
        "Metric 'balance' is semi_additive and cannot be aggregated across "
        "dimension 'snapshot_date'; add it to group_by or pin it with an equality filter."
    )


def test_non_additive_metric_is_refused_across_join():
    models = _models()
    models["orders"]["metrics"][0].update(
        name="margin_rate", additivity="non_additive"
    )

    with pytest.raises(SemanticQueryError) as exc_info:
        SemanticPlanner(FakeSemanticEngine(models)).plan(
            SemanticQuery(
                model_name="orders", metrics=["margin_rate"], group_by=["segment"]
            )
        )

    assert str(exc_info.value) == (
        "Metric 'margin_rate' is non_additive and cannot be computed across a join; "
        "query it from model 'orders' alone, or materialise it at the target grain."
    )


def test_non_additive_metric_is_allowed_without_join():
    models = _models()
    models["orders"]["metrics"][0]["additivity"] = "non_additive"

    plan = SemanticPlanner(FakeSemanticEngine(models)).plan(
        SemanticQuery(model_name="orders", metrics=["revenue"], group_by=["status"])
    )

    assert plan.joins == ()


def _fiscal_calendar():
    return parse_calendar(
        {
            "key": "fiscal_fr",
            "version": "2024.1",
            "periods": [
                {"name": "FY2024_Q3", "start": "2024-10-01", "end": "2024-12-31"}
            ],
        }
    )


def test_fiscal_period_resolves_to_versioned_calendar_bounds():
    models = _models()
    models["orders"]["calendar"] = "fiscal_fr"

    plan = SemanticPlanner(
        FakeSemanticEngine(models, calendars={"fiscal_fr": _fiscal_calendar()})
    ).plan(
        SemanticQuery(model_name="orders", metrics=["revenue"], period="FY2024_Q3")
    )

    assert plan.date_from == "2024-10-01"
    assert plan.date_to == "2024-12-31"
    assert plan.period_name == "FY2024_Q3"
    assert plan.calendar_key == "fiscal_fr"
    assert plan.calendar_version == "2024.1"


def test_period_and_explicit_date_are_rejected_as_ambiguous():
    with pytest.raises(SemanticQueryError, match="cannot combine period with date_from"):
        SemanticPlanner(FakeSemanticEngine(_models())).plan(
            SemanticQuery(
                model_name="orders", period="FY2024_Q3", date_from="2024-10-01"
            )
        )


def test_period_without_root_calendar_is_rejected():
    with pytest.raises(SemanticQueryError, match="does not declare a calendar"):
        SemanticPlanner(FakeSemanticEngine(_models())).plan(
            SemanticQuery(model_name="orders", period="FY2024_Q3")
        )


def test_unknown_period_lists_available_periods_and_suggestion():
    models = _models()
    models["orders"]["calendar"] = "fiscal_fr"

    with pytest.raises(SemanticQueryError) as exc_info:
        SemanticPlanner(
            FakeSemanticEngine(models, calendars={"fiscal_fr": _fiscal_calendar()})
        ).plan(SemanticQuery(model_name="orders", period="FY2024_Q4"))

    message = str(exc_info.value)
    assert "FY2024_Q3" in message
    assert "Did you mean" in message


def _fanout_models():
    """orders -1:N-> lines -N:1-> products, so every side of a fanout is testable."""
    models = _models()
    models["orders"]["relationships"] = [
        _relationship("order_lines", "order", "lines", "order", cardinality="one_to_many")
    ]
    models["orders"]["entities"] = [{"name": "order", "type": "primary", "key": "order_id"}]
    models["lines"] = {
        "key": "lines",
        "table": "gold.lines",
        "grain": ["line"],
        "dimensions": [{"name": "line_status", "sql": "line_status", "type": "string"}],
        "metrics": [{"name": "line_amount", "sql": "line_amount", "type": "sum"}],
        "entities": [
            {"name": "order", "type": "primary", "key": "order_id"},
            {"name": "product", "type": "foreign", "key": "product_id"},
        ],
        "relationships": [
            _relationship("lines_product", "product", "products", "product")
        ],
    }
    models["products"] = {
        "key": "products",
        "table": "gold.products",
        "grain": ["product"],
        "dimensions": [{"name": "product_name", "sql": "product_name", "type": "string"}],
        "metrics": [{"name": "catalog_value", "sql": "list_price", "type": "sum"}],
        "entities": [{"name": "product", "type": "primary", "key": "product_id"}],
        "relationships": [],
    }
    return models


def test_metric_on_the_many_side_of_a_fanout_stays_queryable():
    # SUM over the many-side is computed at its own grain, so refusing it would
    # be a false positive on a perfectly ordinary line-level question.
    plan = SemanticPlanner(FakeSemanticEngine(_fanout_models())).plan(
        SemanticQuery(model_name="orders", metrics=["line_amount"], group_by=["status"])
    )

    assert plan.required_models == ("orders", "lines")


def test_metric_reached_beyond_a_fanout_is_refused():
    # products is joined *after* the one_to_many, so it is never "already
    # joined" when the fanout edge is walked — a scan that only looks upstream
    # of the fanout lets SUM(list_price) through at line grain.
    with pytest.raises(SemanticQueryError) as error:
        SemanticPlanner(FakeSemanticEngine(_fanout_models())).plan(
            SemanticQuery(
                model_name="orders",
                metrics=["catalog_value"],
                group_by=["status"],
            )
        )

    message = str(error.value)
    assert "Unsafe fanout" in message
    assert "metric grain 'product'" in message
    assert "lines_product" in message


def test_metric_on_the_one_side_of_many_to_one_is_refused():
    # The mirror image of one_to_many: one customer row repeats once per order,
    # so summing a customer measure across the join double-counts it.
    with pytest.raises(SemanticQueryError, match="sits on the one-side of"):
        SemanticPlanner(FakeSemanticEngine(_models())).plan(
            SemanticQuery(
                model_name="orders",
                metrics=["customers.customer_count"],
                group_by=["status"],
            )
        )
