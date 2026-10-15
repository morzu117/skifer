"""Tests unitaires pour semantic/metric_views.py — traduction pure (Plan 42)."""

import pytest

from skifer.agentic.resolver import QueryResolver
from skifer.semantic.metric_views import (
    MetricViewTranslationError,
    metric_view_ddl,
    metric_view_definition,
    source_table,
    view_name,
)


@pytest.fixture
def model():
    return {
        "key": "sales.orders",
        "table": "gold.kpi_orders",
        "description": "Commandes",
        "base_filter": None,
        "dimensions": [
            {"name": "region", "sql": "region", "description": "Région de vente"},
        ],
        "metrics": [
            {"name": "total_revenue", "sql": "revenue", "type": "sum", "description": "CA"},
        ],
    }


# ---------------------------------------------------------------------------
# Traduction — cas normal
# ---------------------------------------------------------------------------

def test_view_name_is_the_last_segment_of_the_model_key(model):
    assert view_name(model) == "orders"


def test_source_table_qualifies_a_two_part_table_with_the_catalog(model):
    assert source_table(model, "sts_demo_catalog") == "sts_demo_catalog.gold.kpi_orders"


def test_source_table_accepts_an_already_qualified_table():
    assert source_table({"key": "k", "table": "c.gold.kpi_orders"}, "ignored") == "c.gold.kpi_orders"


def test_definition_carries_comment_source_dimensions_and_measures(model):
    definition = metric_view_definition(model, "sts_demo_catalog")

    assert definition["version"] == "1.1"
    assert definition["comment"] == "Commandes"
    assert definition["source"] == "sts_demo_catalog.gold.kpi_orders"
    assert definition["dimensions"] == [
        {"name": "region", "expr": "region", "comment": "Région de vente"}
    ]
    assert definition["measures"] == [{"name": "total_revenue", "expr": "SUM(revenue)", "comment": "CA"}]
    assert "filter" not in definition


def test_base_filter_becomes_the_view_filter(model):
    model["base_filter"] = "region <> 'TEST'"

    assert metric_view_definition(model, "c")["filter"] == "region <> 'TEST'"


def test_ddl_wraps_the_definition_in_a_metric_view_statement(model):
    ddl = metric_view_ddl(model, "sts_demo_catalog", "semantic")

    assert ddl.startswith(
        "CREATE OR REPLACE VIEW `sts_demo_catalog`.`semantic`.`orders`\nWITH METRICS\nLANGUAGE YAML\nAS $$\n"
    )
    assert ddl.endswith("$$")


# ---------------------------------------------------------------------------
# L'expression d'une mesure est celle du resolver, pas une réimplémentation
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "metric",
    [
        {"name": "m", "sql": "amount", "type": "sum"},
        {"name": "m", "sql": "amount", "type": "count"},
        {"name": "m", "sql": "amount", "type": "count_distinct"},
        {"name": "m", "sql": "amount", "type": "avg"},
        {"name": "m", "sql": "amount", "type": "min"},
        {"name": "m", "sql": "amount", "type": "max"},
        {"name": "m", "sql": "amount", "type": "sum", "filters": [{"sql": "channel = 'online'"}]},
    ],
)
def test_measure_expression_matches_the_resolvers_own_build(model, metric):
    model["metrics"] = [metric]
    resolver = QueryResolver()

    definition = metric_view_definition(model, "c")

    # _build_metric_expr() ajoute " AS <name>" : on retire l'alias pour comparer
    # à l'expression nue que produit la vue.
    expected = resolver._build_metric_expr(metric).removesuffix(f" AS {metric['name']}")
    assert definition["measures"][0]["expr"] == expected


# ---------------------------------------------------------------------------
# Refus explicites (Plan 42, décision D4, et garde-fous d'identifiants)
# ---------------------------------------------------------------------------

def test_refuses_an_unsupported_aggregate(model):
    model["metrics"][0]["type"] = "median"

    with pytest.raises(MetricViewTranslationError, match="median"):
        metric_view_definition(model, "c")


def test_refuses_relationships_in_v1(model):
    model["relationships"] = [{"name": "customer", "to": {}}]

    with pytest.raises(MetricViewTranslationError, match="relationships"):
        metric_view_definition(model, "c")


def test_refuses_a_semi_additive_metric(model):
    model["metrics"][0]["additivity"] = "semi_additive"

    with pytest.raises(MetricViewTranslationError, match="semi_additive"):
        metric_view_definition(model, "c")


def test_refuses_an_unsafe_dimension_name(model):
    model["dimensions"][0]["name"] = "region; DROP TABLE x"

    with pytest.raises(MetricViewTranslationError, match="invalide"):
        metric_view_definition(model, "c")


def test_refuses_an_unsafe_catalog_or_schema(model):
    with pytest.raises(MetricViewTranslationError, match="invalide"):
        metric_view_ddl(model, "c; DROP TABLE x", "semantic")


def test_refuses_a_malformed_table_reference():
    with pytest.raises(MetricViewTranslationError, match="invalide"):
        source_table({"key": "k", "table": "kpi_orders"}, "c")


def test_refuses_dollar_dollar_in_a_value(model):
    model["description"] = "contient $$ littéralement"

    with pytest.raises(MetricViewTranslationError, match=r"\$\$"):
        metric_view_ddl(model, "c", "semantic")
