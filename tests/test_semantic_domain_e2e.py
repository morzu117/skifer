"""Adversarial and local-Spark proof tests for Plan 29, slice 6.6."""
from __future__ import annotations

import builtins
from datetime import date
import os
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from skifer.agentic.resolver import (
    QueryResolver,
    SemanticQuery,
    SemanticQueryError,
)
from skifer.semantic.planner import SemanticPlanner
from skifer.semantic.semantic import SemanticEngine


SCHEMA = "semantic_domain_e2e"


def _relationship(name, from_entity, to_model, to_entity, cardinality="many_to_one"):
    return {
        "name": name,
        "from_entity": from_entity,
        "to_model": to_model,
        "to_entity": to_entity,
        "cardinality": cardinality,
        "join_type": "left",
        "verified_by_contract": True,
    }


def _models() -> dict[str, dict]:
    """A curated orders domain with an alternate route to regions and a fanout."""
    return {
        "orders": {
            "name": "orders",
            "key": "orders",
            "table": f"{SCHEMA}.orders",
            "grain": ["order"],
            "calendar": "fiscal_fr",
            "dimensions": [
                {"name": "order_date", "sql": "order_date", "type": "date"},
                {"name": "status", "sql": "status", "type": "string"},
            ],
            "metrics": [{"name": "revenue", "sql": "amount", "type": "sum"}],
            "entities": [
                {"name": "order", "type": "primary", "key": "order_id"},
                {
                    "name": "customer",
                    "type": "foreign",
                    "key": ["customer_id", "tenant_id"],
                },
                {"name": "store", "type": "foreign", "key": "store_id"},
            ],
            "relationships": [
                _relationship("orders_customers", "customer", "customers", "customer"),
                _relationship("orders_stores", "store", "stores", "store"),
                _relationship(
                    "order_lines", "order", "order_lines", "order", "one_to_many"
                ),
            ],
            "metadata": {
                "relationship_candidates": [
                    {"name": "orders_suppliers", "to_model": "suppliers", "status": "proposed"}
                ]
            },
        },
        "customers": {
            "name": "customers",
            "key": "customers",
            "table": f"{SCHEMA}.customers",
            "dimensions": [{"name": "segment", "sql": "segment", "type": "string"}],
            "metrics": [{"name": "customer_count", "sql": "customer_id", "type": "count"}],
            "entities": [
                {"name": "customer", "type": "primary", "key": ["id", "tenant_id"]},
                {"name": "region", "type": "foreign", "key": "region_id"},
            ],
            "relationships": [_relationship("customers_regions", "region", "regions", "region")],
        },
        "stores": {
            "name": "stores",
            "key": "stores",
            "table": f"{SCHEMA}.stores",
            "dimensions": [{"name": "store_name", "sql": "store_name", "type": "string"}],
            "metrics": [{"name": "store_count", "sql": "store_id", "type": "count"}],
            "entities": [
                {"name": "store", "type": "primary", "key": "store_id"},
                {"name": "region", "type": "foreign", "key": "region_id"},
            ],
            "relationships": [_relationship("stores_regions", "region", "regions", "region")],
        },
        "order_lines": {
            "name": "order_lines",
            "key": "order_lines",
            "table": f"{SCHEMA}.order_lines",
            "grain": ["line"],
            "dimensions": [{"name": "line_sku", "sql": "sku", "type": "string"}],
            "metrics": [{"name": "line_revenue", "sql": "line_amount", "type": "sum"}],
            "entities": [
                {"name": "line", "type": "primary", "key": "line_id"},
                {"name": "order", "type": "foreign", "key": "order_id"},
                {"name": "product", "type": "foreign", "key": "product_id"},
            ],
            "relationships": [_relationship("lines_products", "product", "products", "product")],
        },
        "products": {
            "name": "products",
            "key": "products",
            "table": f"{SCHEMA}.products",
            "dimensions": [{"name": "product_name", "sql": "product_name", "type": "string"}],
            "metrics": [{"name": "product_count", "sql": "product_id", "type": "count"}],
            "entities": [{"name": "product", "type": "primary", "key": "product_id"}],
        },
        "regions": {
            "name": "regions",
            "key": "regions",
            "table": f"{SCHEMA}.regions",
            "dimensions": [{"name": "region_name", "sql": "region_name", "type": "string"}],
            "metrics": [{"name": "region_count", "sql": "region_id", "type": "count"}],
            "entities": [{"name": "region", "type": "primary", "key": "region_id"}],
        },
        "suppliers": {
            "name": "suppliers",
            "key": "suppliers",
            "table": f"{SCHEMA}.suppliers",
            "dimensions": [{"name": "supplier_name", "sql": "supplier_name", "type": "string"}],
            "metrics": [{"name": "supplier_count", "sql": "supplier_id", "type": "count"}],
            "entities": [{"name": "supplier", "type": "primary", "key": "supplier_id"}],
        },
    }


def _write_domain(models_dir: Path, models: dict[str, dict]) -> None:
    catalog = {
        "models": [
            {
                "key": key,
                "file": f"{key}.yaml",
                "dimensions": [item["name"] for item in model["dimensions"]],
                "metrics": [item["name"] for item in model["metrics"]],
                "entities": [item["name"] for item in model.get("entities", [])],
                "related_models": sorted(
                    relation["to_model"] for relation in model.get("relationships", [])
                ),
            }
            for key, model in models.items()
        ]
    }
    (models_dir / "calendars").mkdir(parents=True)
    (models_dir / "semantic_catalog.yaml").write_text(
        yaml.safe_dump(catalog, sort_keys=False), encoding="utf-8"
    )
    for key, model in models.items():
        (models_dir / f"{key}.yaml").write_text(
            yaml.safe_dump({"models": [model]}, sort_keys=False), encoding="utf-8"
        )
    (models_dir / "calendars" / "fiscal_fr.yaml").write_text(
        yaml.safe_dump(
            {
                "key": "fiscal_fr",
                "version": "2024.1",
                "periods": [
                    {"name": "FY2024_Q3", "start": "2024-10-01", "end": "2024-12-31"}
                ],
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )


@pytest.fixture
def domain(tmp_path):
    models_dir = tmp_path / "semantic_models"
    _write_domain(models_dir, _models())
    engine = SemanticEngine(SimpleNamespace(), models_dir=str(models_dir))
    return engine, QueryResolver(SemanticPlanner(engine)), models_dir


def _resolve(domain, query):
    engine, resolver, _ = domain
    return resolver.resolve(query, engine._get_model(query.model_name), "spark_catalog")


def test_domain_adversarial_errors_are_specific_and_fail_closed(domain):
    engine, resolver, _ = domain
    cases = [
        (
            SemanticQuery(model_name="orders", metrics=["missing_metric"]),
            "Metric 'missing_metric' is not declared in root model 'orders' or any catalog model. Available:",
        ),
        (
            SemanticQuery(model_name="orders", metrics=["revenue"], group_by=["suppliers.supplier_name"]),
            "No declared relationship connects metric 'revenue' to dimension 'supplier_name'.",
        ),
        (
            SemanticQuery(model_name="orders", metrics=["revenue"], group_by=["regions.region_name"]),
            "Ambiguous semantic join path: orders→customers→regions or orders→stores→regions.",
        ),
    ]
    for query, expected in cases:
        with pytest.raises(SemanticQueryError) as error:
            resolver.resolve(query, engine._get_model("orders"), "spark_catalog")
        assert expected in str(error.value)
        if query.metrics == ["missing_metric"]:
            assert "revenue" in str(error.value)


def test_unknown_semantic_name_includes_nearest_suggestion(domain):
    engine, resolver, _ = domain

    with pytest.raises(SemanticQueryError, match="Did you mean.*revenue"):
        resolver.resolve(
            SemanticQuery(model_name="orders", metrics=["revenues"]),
            engine._get_model("orders"),
            "spark_catalog",
        )


def test_multi_model_query_omits_catalog_in_local_mode(domain):
    engine, resolver, _ = domain

    resolved = resolver.resolve(
        SemanticQuery(
            model_name="orders",
            metrics=["revenue"],
            group_by=["customers.segment"],
        ),
        engine._get_model("orders"),
        None,
    )

    assert "`semantic_domain_e2e`.`orders` AS m0" in resolved.full_sql
    assert "`semantic_domain_e2e`.`customers` AS m1" in resolved.full_sql


@pytest.mark.parametrize(
    ("cardinality", "expected"),
    [
        ("many_to_many", "is many_to_many, which is not queryable for a metric because it fans the metric out"),
        ("unknown", "Relationship cardinality is unknown; declare or certify uniqueness before querying."),
        ("unknown", "Offending relationship: 'orders_customers'."),
    ],
)
def test_unsafe_cardinalities_have_their_own_cause(domain, cardinality, expected):
    engine, resolver, _ = domain
    engine._get_model("orders")["relationships"][0]["cardinality"] = cardinality

    with pytest.raises(SemanticQueryError) as error:
        resolver.resolve(
            SemanticQuery(model_name="orders", metrics=["revenue"], group_by=["customers.segment"]),
            engine._get_model("orders"),
            "spark_catalog",
        )

    assert expected in str(error.value)
    if cardinality == "many_to_many":
        assert "unknown" not in str(error.value)


def test_metric_upstream_of_a_fanout_is_refused_whatever_the_dimension(domain):
    engine, resolver, _ = domain
    # Both queries carry the same metric, owned by orders, so both are refused
    # for the same reason: crossing order_lines duplicates every order row.
    one_hop = SemanticQuery(model_name="orders", metrics=["revenue"], group_by=["order_lines.line_sku"])
    two_hops = SemanticQuery(model_name="orders", metrics=["revenue"], group_by=["products.product_name"])
    for query in (one_hop, two_hops):
        with pytest.raises(SemanticQueryError) as error:
            resolver.resolve(query, engine._get_model("orders"), "spark_catalog")
        assert str(error.value) == (
            "Unsafe fanout: metric grain 'order' crosses one_to_many relationship 'order_lines'."
        )


def test_metric_owned_beyond_the_fanout_is_refused_on_its_own_side(domain):
    # products is joined *after* the one_to_many, so it is never upstream of it.
    # A scan looking only for models already joined before the fanout edge lets
    # SUM(products.*) through at line grain.
    engine, resolver, _ = domain

    with pytest.raises(SemanticQueryError) as error:
        resolver.resolve(
            SemanticQuery(model_name="orders", metrics=["products.product_count"], group_by=["status"]),
            engine._get_model("orders"),
            "spark_catalog",
        )

    message = str(error.value)
    assert "Unsafe fanout" in message
    assert "sits on the one-side of many_to_one relationship 'lines_products'" in message


def test_metric_on_the_direct_many_side_stays_queryable(domain):
    # Refusing this would be a false positive: the sum is computed at the grain
    # of the model that owns it.
    allowed = _resolve(
        domain,
        SemanticQuery(model_name="orders", metrics=["order_lines.line_revenue"]),
    )

    assert "SUM(m1.`line_amount`) AS line_revenue" in allowed.full_sql


def test_semi_additive_metric_requires_its_dimension_to_be_pinned(domain):
    engine, resolver, _ = domain
    orders = engine._get_model("orders")
    orders["dimensions"].append({"name": "snapshot_date", "sql": "snapshot_date", "type": "date"})
    orders["metrics"][0].update(additivity="semi_additive", non_additive_dimensions=["snapshot_date"])

    with pytest.raises(SemanticQueryError) as error:
        resolver.resolve(SemanticQuery(model_name="orders", metrics=["revenue"]), orders, "spark_catalog")

    assert "semi_additive and cannot be aggregated across dimension 'snapshot_date'" in str(error.value)


def test_composite_join_sql_preserves_declared_key_order(domain):
    resolved = _resolve(
        domain,
        SemanticQuery(model_name="orders", metrics=["revenue"], group_by=["customers.segment"]),
    )

    assert "m0.`customer_id` = m1.`id`\n AND m0.`tenant_id` = m1.`tenant_id`" in resolved.full_sql


def test_period_and_all_injection_vectors_are_rejected_before_sql(domain):
    engine, resolver, _ = domain
    unsafe_value = SemanticQuery(
        model_name="orders", metrics=["revenue"], filters=[{"column": "status", "operator": "eq", "value": "paid' OR 1=1 --"}]
    )
    with pytest.raises(SemanticQueryError, match="Filter value.*unsafe characters"):
        resolver.resolve(unsafe_value, engine._get_model("orders"), "spark_catalog")

    with pytest.raises(SemanticQueryError, match="Period 'FY2024_Q3; DROP' is not declared"):
        resolver.resolve(
            SemanticQuery(model_name="orders", metrics=["revenue"], period="FY2024_Q3; DROP"),
            engine._get_model("orders"),
            "spark_catalog",
        )

    engine._get_model("orders")["calendar"] = "fiscal_fr;DROP"
    with pytest.raises(SemanticQueryError, match="Unsafe calendar key 'fiscal_fr;DROP'"):
        resolver.resolve(
            SemanticQuery(model_name="orders", metrics=["revenue"], period="FY2024_Q3"),
            engine._get_model("orders"),
            "spark_catalog",
        )


def test_real_file_access_is_lazy_and_calendar_is_cached(tmp_path, monkeypatch):
    models_dir = tmp_path / "semantic_models"
    _write_domain(models_dir, _models())
    real_open = builtins.open
    opened: list[str] = []

    def tracking_open(path, *args, **kwargs):
        opened.append(os.path.relpath(os.fspath(path), models_dir))
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", tracking_open)
    engine = SemanticEngine(SimpleNamespace(), models_dir=str(models_dir))
    assert opened == ["semantic_catalog.yaml"]

    resolver = QueryResolver(SemanticPlanner(engine))
    resolver.resolve(SemanticQuery(model_name="orders", metrics=["revenue"]), engine._get_model("orders"), "spark_catalog")
    assert "suppliers.yaml" not in opened
    assert "calendars/fiscal_fr.yaml" not in opened

    period_query = SemanticQuery(model_name="orders", metrics=["revenue"], period="FY2024_Q3")
    resolver.resolve(period_query, engine._get_model("orders"), "spark_catalog")
    resolver.resolve(period_query, engine._get_model("orders"), "spark_catalog")
    assert opened.count("calendars/fiscal_fr.yaml") == 1
    assert "suppliers.yaml" not in opened


def test_proposed_draft_relationship_is_not_queryable(domain):
    engine, resolver, _ = domain
    # The pipeline proposed orders -> suppliers, and the candidate really is
    # carried in the model; asserting the refusal without asserting the
    # candidate's presence would pass just as well on a domain that never
    # proposed anything.
    candidates = engine._get_model("orders")["metadata"]["relationship_candidates"]
    assert [c["to_model"] for c in candidates if c["status"] == "proposed"] == ["suppliers"]

    with pytest.raises(SemanticQueryError) as error:
        resolver.resolve(
            SemanticQuery(model_name="orders", metrics=["revenue"], group_by=["suppliers.supplier_name"]),
            engine._get_model("orders"),
            "spark_catalog",
        )
    assert "No declared relationship connects" in str(error.value)


@pytest.fixture
def populated_domain(domain, spark, tmp_path):
    # Anchor the database in tmp_path rather than the shared spark-warehouse.
    # A run interrupted mid-test leaves the table directory behind while the
    # metastore forgets the table, and the next saveAsTable then fails with
    # LOCATION_ALREADY_EXISTS — which a DROP TABLE teardown cannot repair.
    spark.sql(f"DROP DATABASE IF EXISTS {SCHEMA} CASCADE")
    spark.sql(f"CREATE DATABASE {SCHEMA} LOCATION '{tmp_path / 'warehouse'}'")
    tables = {
        "orders": [
            ("o1", "c1", "t1", "s1", date(2024, 10, 2), "paid", 100.0),
            ("o2", "c1", "t1", "s1", date(2024, 9, 30), "paid", 50.0),
            ("o3", "c2", "t1", "s1", date(2024, 10, 20), "new", 40.0),
        ],
        "customers": [("c1", "t1", "enterprise", "r1"), ("c2", "t1", "small", "r2")],
        "order_lines": [("l1", "o1", "p1", "SKU1", 60.0), ("l2", "o1", "p2", "SKU2", 40.0), ("l3", "o2", "p1", "SKU1", 50.0), ("l4", "o3", "p2", "SKU2", 40.0)],
        "products": [("p1", "Widget"), ("p2", "Gadget")],
    }
    schemas = {
        "orders": "order_id string, customer_id string, tenant_id string, store_id string, order_date date, status string, amount double",
        "customers": "id string, tenant_id string, segment string, region_id string",
        "order_lines": "line_id string, order_id string, product_id string, sku string, line_amount double",
        "products": "product_id string, product_name string",
    }
    for name, rows in tables.items():
        spark.createDataFrame(rows, schemas[name]).write.mode("overwrite").saveAsTable(f"{SCHEMA}.{name}")
    yield domain
    spark.sql(f"DROP DATABASE IF EXISTS {SCHEMA} CASCADE")


def _rows(spark, sql):
    return sorted(tuple(row) for row in spark.sql(sql).collect())


def test_local_spark_nominal_single_and_two_model_rows(populated_domain, spark):
    single = _resolve(populated_domain, SemanticQuery(model_name="orders", metrics=["revenue"], group_by=["status"]))
    joined = _resolve(populated_domain, SemanticQuery(model_name="orders", metrics=["revenue"], group_by=["customers.segment"]))

    assert _rows(spark, single.full_sql) == [("new", 40.0), ("paid", 150.0)]
    assert _rows(spark, joined.full_sql) == [("enterprise", 150.0), ("small", 40.0)]


def test_local_spark_fiscal_period_and_fanout_protection(populated_domain, spark):
    fiscal = _resolve(populated_domain, SemanticQuery(model_name="orders", metrics=["revenue"], period="FY2024_Q3"))
    assert "m0.`order_date` >= '2024-10-01'" in fiscal.full_sql
    assert "m0.`order_date` <= '2024-12-31'" in fiscal.full_sql
    assert _rows(spark, fiscal.full_sql) == [(140.0,)]

    safe_total = _rows(spark, f"SELECT SUM(amount) FROM {SCHEMA}.orders")
    fanout_total = _rows(
        spark,
        f"SELECT SUM(o.amount) FROM {SCHEMA}.orders o JOIN {SCHEMA}.order_lines l ON o.order_id = l.order_id",
    )
    assert safe_total == [(190.0,)]
    assert fanout_total == [(290.0,)]
    assert fanout_total != safe_total


def test_single_model_sql_remains_byte_for_byte_legacy_compatible(domain):
    resolved = _resolve(domain, SemanticQuery(model_name="orders", metrics=["revenue"], group_by=["status"]))
    assert resolved.full_sql == (
        "SELECT\n"
        "    status AS status,\n"
        "    SUM(amount) AS revenue\n"
        f"FROM `spark_catalog`.`{SCHEMA}`.`orders`\n"
        "GROUP BY status"
    )
