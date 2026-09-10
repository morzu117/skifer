"""
Tests unitaires pour QueryResolver — résolution déterministe SemanticQuery → SQL.
"""
import os
import subprocess
import sys
import textwrap

import pytest

from skifer.agentic.resolver import (
    QueryResolver,
    SemanticQuery,
    SemanticQueryError,
    ResolvedQuery,
)
from skifer.semantic.planner import SemanticPlanner


# ---------------------------------------------------------------------------
# Fixtures — modèle YAML simplifié
# ---------------------------------------------------------------------------

@pytest.fixture
def model():
    """Modèle YAML simplifié pour les tests."""
    return {
        "name": "kpi_orders",
        "key": "kpi_orders.erp",
        "table": "gold.fact_orders",
        "layer": "gold",
        "description": "Métriques commandes ERP",
        "base_filter": "source_system = 'ERP'",
        "dimensions": [
            {"name": "region", "sql": "region", "type": "string"},
            {"name": "order_date", "sql": "order_date", "type": "date"},
            {"name": "product_category", "sql": "product_cat", "type": "string"},
        ],
        "metrics": [
            {
                "name": "gross_revenue",
                "sql": "amount_ttc",
                "type": "sum",
                "description": "CA brut TTC",
                "filters": [{"sql": "status != 'Cancelled'"}],
            },
            {
                "name": "nb_orders",
                "sql": "order_id",
                "type": "count_distinct",
                "description": "Nombre de commandes",
            },
            {
                "name": "avg_basket",
                "sql": "amount_ttc",
                "type": "avg",
                "description": "Panier moyen",
            },
        ],
    }


@pytest.fixture
def resolver():
    return QueryResolver()


# ---------------------------------------------------------------------------
# SemanticQuery.from_dict
# ---------------------------------------------------------------------------

def test_semantic_query_from_dict():
    data = {
        "model_name": "kpi_orders.erp",
        "metrics": ["gross_revenue"],
        "group_by": ["region"],
        "mode": "query",
        "explanation": "CA par région",
        "period": "FY2024_Q3",
    }
    sq = SemanticQuery.from_dict(data)
    assert sq.model_name == "kpi_orders.erp"
    assert sq.metrics == ["gross_revenue"]
    assert sq.group_by == ["region"]
    assert sq.mode == "query"
    assert sq.explanation == "CA par région"
    assert sq.period == "FY2024_Q3"


def test_semantic_query_defaults():
    sq = SemanticQuery(model_name="test.model")
    assert sq.metrics == []
    assert sq.group_by == []
    assert sq.filters == []
    assert sq.mode == "query"
    assert sq.date_from is None


# ---------------------------------------------------------------------------
# QueryResolver._validate — validation des noms
# ---------------------------------------------------------------------------

def test_validate_unknown_metric(resolver, model):
    sq = SemanticQuery(model_name="kpi_orders.erp", metrics=["unknown_metric"])
    with pytest.raises(SemanticQueryError, match="unknown_metric"):
        resolver._validate(sq, model)


def test_validate_unknown_dimension(resolver, model):
    sq = SemanticQuery(
        model_name="kpi_orders.erp",
        metrics=["gross_revenue"],
        group_by=["country"],
    )
    with pytest.raises(SemanticQueryError, match="country"):
        resolver._validate(sq, model)


def test_validate_unknown_filter_column(resolver, model):
    sq = SemanticQuery(
        model_name="kpi_orders.erp",
        metrics=["gross_revenue"],
        filters=[{"column": "unknown_col", "operator": "eq", "value": "X"}],
    )
    with pytest.raises(SemanticQueryError, match="unknown_col"):
        resolver._validate(sq, model)


def test_validate_unknown_operator(resolver, model):
    sq = SemanticQuery(
        model_name="kpi_orders.erp",
        metrics=["gross_revenue"],
        filters=[{"column": "region", "operator": "not_an_op", "value": "X"}],
    )
    with pytest.raises(SemanticQueryError, match="not_an_op"):
        resolver._validate(sq, model)


def test_validate_valid_query_passes(resolver, model):
    sq = SemanticQuery(
        model_name="kpi_orders.erp",
        metrics=["gross_revenue", "nb_orders"],
        group_by=["region"],
        filters=[{"column": "region", "operator": "eq", "value": "EMEA"}],
    )
    # Ne doit pas lever d'exception
    resolver._validate(sq, model)


# ---------------------------------------------------------------------------
# QueryResolver.resolve — construction SQL
# ---------------------------------------------------------------------------

def test_resolve_basic_query(resolver, model):
    sq = SemanticQuery(
        model_name="kpi_orders.erp",
        metrics=["gross_revenue"],
        group_by=["region"],
    )
    result = resolver.resolve(sq, model, "my_catalog")

    assert isinstance(result, ResolvedQuery)
    assert "my_catalog" in result.from_fqn
    assert "fact_orders" in result.from_fqn
    # base_filter présent
    assert "source_system = 'ERP'" in result.where_clauses
    # GROUP BY
    assert "region" in result.group_by_exprs
    # SQL complet valide
    assert "SELECT" in result.full_sql
    assert "FROM" in result.full_sql
    assert "GROUP BY" in result.full_sql


def test_resolve_metric_with_filter(resolver, model):
    """La métrique gross_revenue a un filtre inline — doit générer un CASE WHEN."""
    sq = SemanticQuery(
        model_name="kpi_orders.erp",
        metrics=["gross_revenue"],
    )
    result = resolver.resolve(sq, model, "catalog")
    # Le filtre inline doit apparaître dans le SELECT
    assert "CASE WHEN" in result.full_sql
    assert "status != 'Cancelled'" in result.full_sql


def test_resolve_count_distinct(resolver, model):
    sq = SemanticQuery(model_name="kpi_orders.erp", metrics=["nb_orders"])
    result = resolver.resolve(sq, model, "catalog")
    assert "COUNT(DISTINCT" in result.full_sql


def test_resolve_avg_metric(resolver, model):
    sq = SemanticQuery(model_name="kpi_orders.erp", metrics=["avg_basket"])
    result = resolver.resolve(sq, model, "catalog")
    assert "AVG(" in result.full_sql


def test_resolve_filter_eq(resolver, model):
    sq = SemanticQuery(
        model_name="kpi_orders.erp",
        metrics=["gross_revenue"],
        filters=[{"column": "region", "operator": "eq", "value": "EMEA"}],
    )
    result = resolver.resolve(sq, model, "catalog")
    assert "region = 'EMEA'" in result.where_clauses


def test_resolve_filter_in(resolver, model):
    sq = SemanticQuery(
        model_name="kpi_orders.erp",
        metrics=["gross_revenue"],
        filters=[{"column": "region", "operator": "in", "value": ["EMEA", "NA"]}],
    )
    result = resolver.resolve(sq, model, "catalog")
    assert any("IN" in clause for clause in result.where_clauses)


def test_resolve_date_filter_applied_to_date_dimension(resolver, model):
    sq = SemanticQuery(
        model_name="kpi_orders.erp",
        metrics=["gross_revenue"],
        date_from="2024-01-01",
        date_to="2024-12-31",
    )
    result = resolver.resolve(sq, model, "catalog")
    clauses = " ".join(result.where_clauses)
    assert "2024-01-01" in clauses
    assert "2024-12-31" in clauses


def test_resolve_no_group_by_no_group_clause(resolver, model):
    """Sans group_by, pas de GROUP BY dans le SQL."""
    sq = SemanticQuery(
        model_name="kpi_orders.erp",
        metrics=["gross_revenue"],
    )
    result = resolver.resolve(sq, model, "catalog")
    assert "GROUP BY" not in result.full_sql


def test_resolve_table_fqn_format(resolver, model):
    sq = SemanticQuery(model_name="kpi_orders.erp", metrics=["gross_revenue"])
    result = resolver.resolve(sq, model, "my_catalog")
    assert result.from_fqn == "`my_catalog`.`gold`.`fact_orders`"


def test_resolve_table_fqn_omits_catalog_in_local_mode(resolver, model):
    sq = SemanticQuery(model_name="kpi_orders.erp", metrics=["gross_revenue"])
    result = resolver.resolve(sq, model, None)
    assert result.from_fqn == "`gold`.`fact_orders`"


# ---------------------------------------------------------------------------
# _suggest_closest
# ---------------------------------------------------------------------------

def test_suggest_closest_finds_near_match(resolver):
    candidates = {"region", "order_date", "product_category"}
    suggestions = resolver._suggest_closest("regions", candidates)
    assert "region" in suggestions


def test_suggest_closest_returns_empty_for_no_match(resolver):
    candidates = {"region", "order_date"}
    suggestions = resolver._suggest_closest("xyz_totally_unrelated", candidates)
    assert isinstance(suggestions, list)


class _PlannerSemantic:
    def __init__(self, models):
        self.models = models
        self._catalog = {
            key: {
                "key": key,
                "dimensions": [item["name"] for item in value.get("dimensions", [])],
                "metrics": [item["name"] for item in value.get("metrics", [])],
                "entities": [item["name"] for item in value.get("entities", [])],
                "related_models": sorted(
                    {item["to_model"] for item in value.get("relationships", [])}
                ),
            }
            for key, value in models.items()
        }

    def _get_model(self, model_key):
        return self.models[model_key]

    def get_model_summary(self, model_key):
        return self._catalog[model_key]


def _multi_models():
    return {
        "orders": {
            "key": "orders",
            "table": "gold.orders",
            "grain": ["order"],
            "dimensions": [{"name": "status", "sql": "status", "type": "string"}],
            "metrics": [{"name": "revenue", "sql": "amount", "type": "sum"}],
            "entities": [
                {"name": "order", "type": "primary", "key": "order_id"},
                {
                    "name": "customer",
                    "type": "foreign",
                    "key": ["customer_id", "customer_tenant"],
                },
            ],
            "relationships": [
                {
                    "name": "orders_customer",
                    "from_entity": "customer",
                    "to_model": "customers",
                    "to_entity": "customer",
                    "cardinality": "many_to_one",
                    "join_type": "left",
                    "verified_by_contract": True,
                }
            ],
        },
        "customers": {
            "key": "customers",
            "table": "gold.customers",
            "dimensions": [{"name": "segment", "sql": "segment", "type": "string"}],
            "metrics": [{"name": "customer_count", "sql": "customer_id", "type": "count"}],
            "entities": [
                {
                    "name": "customer",
                    "type": "primary",
                    "key": ["id", "tenant"],
                }
            ],
            "relationships": [],
        },
    }


def test_single_table_sql_is_byte_identical_with_planner(model):
    semantic = _PlannerSemantic({"kpi_orders.erp": model})
    resolver = QueryResolver(SemanticPlanner(semantic))
    query = SemanticQuery(
        model_name="kpi_orders.erp",
        metrics=["gross_revenue"],
        group_by=["region"],
        filters=[{"column": "region", "operator": "eq", "value": "EMEA"}],
        date_from="2024-01-01",
        date_to="2024-12-31",
    )

    assert resolver.resolve(query, model, "my_catalog").full_sql == (
        "SELECT\n"
        "    region AS region,\n"
        "    SUM(CASE WHEN status != 'Cancelled' THEN amount_ttc END) AS gross_revenue\n"
        "FROM `my_catalog`.`gold`.`fact_orders`\n"
        "WHERE source_system = 'ERP'\n"
        "  AND region = 'EMEA'\n"
        "  AND order_date >= '2024-01-01'\n"
        "  AND order_date <= '2024-12-31'\n"
        "GROUP BY region"
    )


def test_two_model_plan_compiles_deterministic_qualified_sql():
    models = _multi_models()
    semantic = _PlannerSemantic(models)
    resolver = QueryResolver(SemanticPlanner(semantic))
    query = SemanticQuery(
        model_name="orders",
        metrics=["revenue"],
        group_by=["customers.segment"],
    )

    result = resolver.resolve(query, models["orders"], "main")

    assert result.full_sql == (
        "SELECT\n"
        "    m1.`segment` AS segment,\n"
        "    SUM(m0.`amount`) AS revenue\n"
        "FROM `main`.`gold`.`orders` AS m0\n"
        "LEFT JOIN `main`.`gold`.`customers` AS m1\n"
        "  ON m0.`customer_id` = m1.`id`\n"
        " AND m0.`customer_tenant` = m1.`tenant`\n"
        "GROUP BY m1.`segment`"
    )


def test_multi_model_aliases_are_stable_across_hash_seeds():
    script = textwrap.dedent(
        """
        from skifer.agentic.resolver import QueryResolver, SemanticQuery
        from skifer.semantic.planner import SemanticPlanner

        class Semantic:
            def __init__(self):
                entries = {
                    ('orders', 'segment'),
                    ('customers', 'segment'),
                }
                self.models = {
                    'orders': {
                        'key': 'orders', 'table': 'gold.orders',
                        'dimensions': [{'name': 'status', 'sql': 'status', 'type': 'string'}],
                        'metrics': [{'name': 'revenue', 'sql': 'amount', 'type': 'sum'}],
                        'entities': [{'name': 'customer', 'type': 'foreign', 'key': 'customer_id'}],
                        'relationships': [{'name': 'orders_customer', 'from_entity': 'customer',
                            'to_model': 'customers', 'to_entity': 'customer',
                            'cardinality': 'many_to_one', 'join_type': 'left',
                            'verified_by_contract': True}],
                    },
                    'customers': {
                        'key': 'customers', 'table': 'gold.customers',
                        'dimensions': [{'name': 'segment', 'sql': 'segment', 'type': 'string'}],
                        'metrics': [{'name': 'customer_count', 'sql': 'customer_id', 'type': 'count'}],
                        'entities': [{'name': 'customer', 'type': 'primary', 'key': 'customer_id'}],
                    },
                }
                self._catalog = {}
                for key, unused in entries:
                    model = self.models[key]
                    self._catalog[key] = {
                        'key': key,
                        'dimensions': [item['name'] for item in model.get('dimensions', [])],
                        'metrics': [item['name'] for item in model.get('metrics', [])],
                        'entities': [item['name'] for item in model.get('entities', [])],
                        'related_models': [item['to_model'] for item in model.get('relationships', [])],
                    }
            def _get_model(self, key): return self.models[key]
            def get_model_summary(self, key): return self._catalog[key]

        semantic = Semantic()
        query = SemanticQuery(model_name='orders', metrics=['revenue'], group_by=['customers.segment'])
        print(QueryResolver(SemanticPlanner(semantic)).resolve(query, semantic.models['orders'], 'main').full_sql)
        """
    )
    outputs = []
    for seed in ("1", "7", "101"):
        env = os.environ.copy()
        env["PYTHONHASHSEED"] = seed
        outputs.append(
            subprocess.check_output(
                [sys.executable, "-c", script],
                text=True,
                env=env,
            )
        )

    assert len(set(outputs)) == 1


@pytest.mark.parametrize(
    "filter_def",
    [
        {"column": "region", "operator": "in", "value": ["A','B') OR 1=1 --"]},
        {"column": "region", "operator": "in", "value": "X') OR 1=1 --"},
        {"column": "region", "operator": "eq", "value": {"a": "' OR 1=1 --"}},
        {"column": "region", "operator": "gt", "value": ["' OR 1=1 --"]},
    ],
    ids=["in_list", "in_scalar", "eq_mapping", "gt_sequence"],
)
def test_filter_values_are_never_interpolated_unchecked(filter_def):
    # `in` members and non-string values used to reach the SQL through a bare
    # str(), carrying their own quotes: IN ('A','B') OR 1=1 --') .
    model = {
        "key": "orders",
        "table": "gold.orders",
        "dimensions": [{"name": "region", "sql": "region", "type": "string"}],
        "metrics": [{"name": "revenue", "sql": "amount", "type": "sum"}],
    }
    query = SemanticQuery(
        model_name="orders", metrics=["revenue"], filters=[filter_def]
    )

    with pytest.raises(SemanticQueryError):
        QueryResolver().resolve(query, model, "`main`")


@pytest.mark.parametrize(
    ("value", "operator", "expected"),
    [
        (["EMEA", "APAC"], "in", "region IN ('EMEA', 'APAC')"),
        ([1, 2], "in", "region IN (1, 2)"),
        ("EMEA", "eq", "region = 'EMEA'"),
        (10, "gte", "region >= 10"),
        (True, "eq", "region = True"),
    ],
    ids=["in_strings", "in_numbers", "eq_string", "gte_number", "eq_bool"],
)
def test_wellformed_filter_values_keep_their_previous_rendering(
    value, operator, expected
):
    model = {
        "key": "orders",
        "table": "gold.orders",
        "dimensions": [{"name": "region", "sql": "region", "type": "string"}],
        "metrics": [{"name": "revenue", "sql": "amount", "type": "sum"}],
    }
    query = SemanticQuery(
        model_name="orders",
        metrics=["revenue"],
        filters=[{"column": "region", "operator": operator, "value": value}],
    )

    resolved = QueryResolver().resolve(query, model, "`main`")

    assert resolved.where_clauses == [expected]
