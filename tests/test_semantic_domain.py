"""Tests for semantic domain parsing helpers (Plan 29 slice 6.1)."""

import builtins
import os
from unittest.mock import MagicMock

import pytest

from skifer.semantic.domain import (
    EntityDef,
    EntityRef,
    RelationshipDef,
    parse_entities,
    parse_grain,
    parse_relationships,
)
from skifer.semantic.domain_graph import DomainGraph
from skifer.semantic.semantic import SemanticEngine


@pytest.fixture
def mock_core():
    core = MagicMock()
    core.spark = MagicMock()
    core.db = "my_catalog"
    core.env = "dev"
    core.config = {}
    core.schema_suffix = ""
    return core


def test_parse_domain_blocks_preserves_composite_key_order():
    model = {
        "key": "orders",
        "grain": ["order", "customer"],
        "entities": [
            {"name": "order", "type": "primary", "key": ["order_id", "order_line_id"]},
            {"name": "customer", "type": "foreign", "key": "customer_id"},
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
    }

    assert parse_grain(model) == ("order", "customer")
    assert parse_entities(model) == (
        EntityDef(
            name="order",
            model_key="orders",
            key_columns=("order_id", "order_line_id"),
            role="primary",
        ),
        EntityDef(
            name="customer",
            model_key="orders",
            key_columns=("customer_id",),
            role="foreign",
        ),
    )
    assert parse_relationships(model) == (
        RelationshipDef(
            name="orders_customer",
            from_entity=EntityRef(model_key="orders", entity_name="customer"),
            to_entity=EntityRef(model_key="customers", entity_name="customer"),
            cardinality="many_to_one",
            join_type="left",
            verified_by_contract=True,
        ),
    )


def test_parse_domain_blocks_are_optional():
    model = {"key": "orders", "dimensions": [], "metrics": []}

    assert parse_grain(model) == ()
    assert parse_entities(model) == ()
    assert parse_relationships(model) == ()


def test_domain_graph_finds_all_minimal_paths_in_deterministic_order_and_stays_lazy(
    mock_core,
    tmp_path,
    monkeypatch,
):
    import yaml

    models_dir = tmp_path / "semantic_models"
    models_dir.mkdir()

    catalog = {
        "models": [
            {
                "key": "orders",
                "file": "orders.yaml",
                "description": "Orders",
                "dimensions": ["order_id"],
                "metrics": ["revenue"],
                "entities": ["order", "customer", "store"],
                "related_models": ["stores", "customers"],
            },
            {
                "key": "customers",
                "file": "customers.yaml",
                "description": "Customers",
                "dimensions": ["customer_id"],
                "metrics": ["customer_count"],
                "entities": ["customer", "region"],
                "related_models": ["regions"],
            },
            {
                "key": "stores",
                "file": "stores.yaml",
                "description": "Stores",
                "dimensions": ["store_id"],
                "metrics": ["store_count"],
                "entities": ["store", "region"],
                "related_models": ["regions"],
            },
            {
                "key": "regions",
                "file": "regions.yaml",
                "description": "Regions",
                "dimensions": ["region_id"],
                "metrics": ["region_count"],
                "entities": ["region"],
                "related_models": [],
            },
            {
                "key": "suppliers",
                "file": "suppliers.yaml",
                "description": "Suppliers",
                "dimensions": ["supplier_id"],
                "metrics": ["supplier_count"],
                "entities": ["supplier"],
                "related_models": [],
            },
        ]
    }
    (models_dir / "semantic_catalog.yaml").write_text(
        yaml.safe_dump(catalog, sort_keys=False),
        encoding="utf-8",
    )

    models = {
        "orders.yaml": {
            "models": [
                {
                    "name": "orders",
                    "key": "orders",
                    "table": "gold.orders",
                    "dimensions": [{"name": "order_id", "sql": "order_id", "type": "string"}],
                    "metrics": [{"name": "revenue", "sql": "amount", "type": "sum"}],
                    "entities": [
                        {"name": "order", "type": "primary", "key": "order_id"},
                        {"name": "customer", "type": "foreign", "key": "customer_id"},
                        {"name": "store", "type": "foreign", "key": "store_id"},
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
                        },
                        {
                            "name": "orders_store",
                            "from_entity": "store",
                            "to_model": "stores",
                            "to_entity": "store",
                            "cardinality": "many_to_one",
                            "join_type": "left",
                            "verified_by_contract": True,
                        },
                    ],
                }
            ]
        },
        "customers.yaml": {
            "models": [
                {
                    "name": "customers",
                    "key": "customers",
                    "table": "gold.customers",
                    "dimensions": [{"name": "customer_id", "sql": "customer_id", "type": "string"}],
                    "metrics": [{"name": "customer_count", "sql": "*", "type": "count"}],
                    "entities": [
                        {"name": "customer", "type": "primary", "key": "customer_id"},
                        {"name": "region", "type": "foreign", "key": "region_id"},
                    ],
                    "relationships": [
                        {
                            "name": "customers_region",
                            "from_entity": "region",
                            "to_model": "regions",
                            "to_entity": "region",
                            "cardinality": "many_to_one",
                            "join_type": "left",
                            "verified_by_contract": True,
                        }
                    ],
                }
            ]
        },
        "stores.yaml": {
            "models": [
                {
                    "name": "stores",
                    "key": "stores",
                    "table": "gold.stores",
                    "dimensions": [{"name": "store_id", "sql": "store_id", "type": "string"}],
                    "metrics": [{"name": "store_count", "sql": "*", "type": "count"}],
                    "entities": [
                        {"name": "store", "type": "primary", "key": "store_id"},
                        {"name": "region", "type": "foreign", "key": "region_id"},
                    ],
                    "relationships": [
                        {
                            "name": "stores_region",
                            "from_entity": "region",
                            "to_model": "regions",
                            "to_entity": "region",
                            "cardinality": "many_to_one",
                            "join_type": "left",
                            "verified_by_contract": True,
                        }
                    ],
                }
            ]
        },
        "regions.yaml": {
            "models": [
                {
                    "name": "regions",
                    "key": "regions",
                    "table": "gold.regions",
                    "dimensions": [{"name": "region_id", "sql": "region_id", "type": "string"}],
                    "metrics": [{"name": "region_count", "sql": "*", "type": "count"}],
                    "entities": [{"name": "region", "type": "primary", "key": "region_id"}],
                }
            ]
        },
        "suppliers.yaml": {
            "models": [
                {
                    "name": "suppliers",
                    "key": "suppliers",
                    "table": "gold.suppliers",
                    "dimensions": [{"name": "supplier_id", "sql": "supplier_id", "type": "string"}],
                    "metrics": [{"name": "supplier_count", "sql": "*", "type": "count"}],
                    "entities": [{"name": "supplier", "type": "primary", "key": "supplier_id"}],
                }
            ]
        },
    }
    for filename, payload in models.items():
        (models_dir / filename).write_text(
            yaml.safe_dump(payload, sort_keys=False),
            encoding="utf-8",
        )

    real_open = builtins.open
    opened_models: list[str] = []

    def tracking_open(path, *args, **kwargs):
        file_name = os.path.basename(os.fspath(path))
        if file_name.endswith(".yaml") and file_name != "semantic_catalog.yaml":
            opened_models.append(file_name)
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", tracking_open)

    engine = SemanticEngine(mock_core, models_dir=str(models_dir))
    graph = DomainGraph(engine)

    assert graph.related_models("orders") == ("customers", "stores")
    assert opened_models == []

    paths = graph.find_paths("orders", "regions")

    assert [
        tuple(edge.source_model for edge in path) + (path[-1].target_model,)
        for path in paths
    ] == [
        ("orders", "customers", "regions"),
        ("orders", "stores", "regions"),
    ]
    assert sorted(opened_models) == ["customers.yaml", "orders.yaml", "stores.yaml"]
    assert "suppliers.yaml" not in opened_models
    assert "regions.yaml" not in opened_models


def test_domain_graph_handles_cycles_without_looping(mock_core, tmp_path):
    import yaml

    models_dir = tmp_path / "semantic_models"
    models_dir.mkdir()
    (models_dir / "semantic_catalog.yaml").write_text(
        yaml.safe_dump(
            {
                "models": [
                    {"key": "a", "file": "a.yaml", "entities": ["a"], "related_models": ["b"]},
                    {"key": "b", "file": "b.yaml", "entities": ["b"], "related_models": ["c"]},
                    {"key": "c", "file": "c.yaml", "entities": ["c"], "related_models": ["a"]},
                ]
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    for key, target in (("a", "b"), ("b", "c"), ("c", "a")):
        (models_dir / f"{key}.yaml").write_text(
            yaml.safe_dump(
                {
                    "models": [
                        {
                            "name": key,
                            "key": key,
                            "table": f"gold.{key}",
                            "dimensions": [{"name": key, "sql": key, "type": "string"}],
                            "metrics": [{"name": f"{key}_count", "sql": "*", "type": "count"}],
                            "entities": [{"name": key, "type": "primary", "key": f"{key}_id"}],
                            "relationships": [
                                {
                                    "name": f"{key}_{target}",
                                    "from_entity": key,
                                    "to_model": target,
                                    "to_entity": target,
                                    "cardinality": "many_to_one",
                                    "join_type": "left",
                                    "verified_by_contract": True,
                                }
                            ],
                        }
                    ]
                },
                sort_keys=False,
            ),
            encoding="utf-8",
        )

    engine = SemanticEngine(mock_core, models_dir=str(models_dir))
    graph = DomainGraph(engine, max_depth=4)

    paths = graph.find_paths("a", "c")

    assert len(paths) == 1
    assert [edge.relationship.name for edge in paths[0]] == ["a_b", "b_c"]


def test_domain_graph_raises_explicitly_when_depth_limit_is_exceeded(mock_core, tmp_path):
    import yaml

    models_dir = tmp_path / "semantic_models"
    models_dir.mkdir()
    (models_dir / "semantic_catalog.yaml").write_text(
        yaml.safe_dump(
            {
                "models": [
                    {"key": "orders", "file": "orders.yaml", "entities": ["order"], "related_models": ["customers"]},
                    {"key": "customers", "file": "customers.yaml", "entities": ["customer"], "related_models": ["regions"]},
                    {"key": "regions", "file": "regions.yaml", "entities": ["region"], "related_models": []},
                ]
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    for key, entity, target in (
        ("orders", "order", "customers"),
        ("customers", "customer", "regions"),
        ("regions", "region", None),
    ):
        relationships = []
        if target is not None:
            relationships.append(
                {
                    "name": f"{key}_{target}",
                    "from_entity": entity,
                    "to_model": target,
                    "to_entity": target[:-1] if target.endswith("s") else target,
                    "cardinality": "one_to_one",
                    "join_type": "inner",
                    "verified_by_contract": True,
                }
            )
        (models_dir / f"{key}.yaml").write_text(
            yaml.safe_dump(
                {
                    "models": [
                        {
                            "name": key,
                            "key": key,
                            "table": f"gold.{key}",
                            "dimensions": [{"name": entity, "sql": entity, "type": "string"}],
                            "metrics": [{"name": f"{entity}_count", "sql": "*", "type": "count"}],
                            "entities": [{"name": entity, "type": "primary", "key": f"{entity}_id"}],
                            "relationships": relationships,
                        }
                    ]
                },
                sort_keys=False,
            ),
            encoding="utf-8",
        )

    engine = SemanticEngine(mock_core, models_dir=str(models_dir))
    graph = DomainGraph(engine, max_depth=1)

    try:
        graph.find_paths("orders", "regions")
    except ValueError as exc:
        assert "depth limit exceeded" in str(exc)
    else:
        raise AssertionError("Expected an explicit depth-limit error.")


def test_domain_graph_ignores_proposed_relationship_candidates(mock_core, tmp_path):
    import yaml

    models_dir = tmp_path / "semantic_models"
    models_dir.mkdir()
    (models_dir / "semantic_catalog.yaml").write_text(
        yaml.safe_dump(
            {
                "models": [
                    {"key": "orders", "file": "orders.yaml", "entities": ["order", "customer"], "related_models": []},
                    {"key": "customers", "file": "customers.yaml", "entities": ["customer"], "related_models": []},
                ]
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    (models_dir / "orders.yaml").write_text(
        yaml.safe_dump(
            {
                "models": [
                    {
                        "name": "orders",
                        "key": "orders",
                        "table": "gold.orders",
                        "dimensions": [{"name": "order_id", "sql": "order_id", "type": "string"}],
                        "metrics": [{"name": "revenue", "sql": "amount", "type": "sum"}],
                        "entities": [
                            {"name": "order", "type": "primary", "key": "order_id"},
                            {"name": "customer", "type": "foreign", "key": "customer_id"},
                        ],
                        "metadata": {
                            "relationship_candidates": [
                                {
                                    "status": "proposed",
                                    "name": "orders_customer_candidate",
                                    "from_entity": "customer",
                                    "to_model": "customers",
                                }
                            ]
                        },
                    }
                ]
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    (models_dir / "customers.yaml").write_text(
        yaml.safe_dump(
            {
                "models": [
                    {
                        "name": "customers",
                        "key": "customers",
                        "table": "gold.customers",
                        "dimensions": [{"name": "customer_id", "sql": "customer_id", "type": "string"}],
                        "metrics": [{"name": "customer_count", "sql": "*", "type": "count"}],
                        "entities": [{"name": "customer", "type": "primary", "key": "customer_id"}],
                    }
                ]
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    engine = SemanticEngine(mock_core, models_dir=str(models_dir))
    graph = DomainGraph(engine)

    assert graph.neighbors("orders") == ()
    assert graph.find_paths("orders", "customers") == ()


def test_domain_graph_keeps_model_without_relationships_unchanged(mock_core, tmp_path):
    import yaml

    models_dir = tmp_path / "semantic_models"
    models_dir.mkdir()
    (models_dir / "semantic_catalog.yaml").write_text(
        yaml.safe_dump(
            {
                "models": [
                    {
                        "key": "orders",
                        "file": "orders.yaml",
                        "description": "Orders",
                        "entities": ["order"],
                        "related_models": [],
                    }
                ]
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    (models_dir / "orders.yaml").write_text(
        yaml.safe_dump(
            {
                "models": [
                    {
                        "name": "orders",
                        "key": "orders",
                        "table": "gold.orders",
                        "dimensions": [{"name": "order_id", "sql": "order_id", "type": "string"}],
                        "metrics": [{"name": "revenue", "sql": "amount", "type": "sum"}],
                        "entities": [{"name": "order", "type": "primary", "key": "order_id"}],
                    }
                ]
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    engine = SemanticEngine(mock_core, models_dir=str(models_dir))
    graph = DomainGraph(engine)

    assert graph.related_models("orders") == ()
    assert graph.neighbors("orders") == ()
    assert graph.find_paths("orders", "orders") == ((),)


def test_domain_graph_allows_only_explicitly_safe_reverse_traversal(mock_core, tmp_path):
    import yaml

    models_dir = tmp_path / "semantic_models"
    models_dir.mkdir()
    (models_dir / "semantic_catalog.yaml").write_text(
        yaml.safe_dump(
            {
                "models": [
                    {"key": "profiles", "file": "profiles.yaml", "entities": ["profile"], "related_models": ["users"]},
                    {"key": "users", "file": "users.yaml", "entities": ["user"], "related_models": []},
                    {"key": "orders", "file": "orders.yaml", "entities": ["order", "customer"], "related_models": ["customers"]},
                    {"key": "customers", "file": "customers.yaml", "entities": ["customer"], "related_models": []},
                ]
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    payloads = {
        "profiles.yaml": {
            "models": [
                {
                    "name": "profiles",
                    "key": "profiles",
                    "table": "gold.profiles",
                    "dimensions": [{"name": "profile_id", "sql": "profile_id", "type": "string"}],
                    "metrics": [{"name": "profile_count", "sql": "*", "type": "count"}],
                    "entities": [{"name": "profile", "type": "primary", "key": "profile_id"}],
                    "relationships": [
                        {
                            "name": "profiles_users",
                            "from_entity": "profile",
                            "to_model": "users",
                            "to_entity": "user",
                            "cardinality": "one_to_one",
                            "join_type": "inner",
                            "verified_by_contract": True,
                        }
                    ],
                }
            ]
        },
        "users.yaml": {
            "models": [
                {
                    "name": "users",
                    "key": "users",
                    "table": "gold.users",
                    "dimensions": [{"name": "user_id", "sql": "user_id", "type": "string"}],
                    "metrics": [{"name": "user_count", "sql": "*", "type": "count"}],
                    "entities": [{"name": "user", "type": "primary", "key": "user_id"}],
                }
            ]
        },
        "orders.yaml": {
            "models": [
                {
                    "name": "orders",
                    "key": "orders",
                    "table": "gold.orders",
                    "dimensions": [{"name": "order_id", "sql": "order_id", "type": "string"}],
                    "metrics": [{"name": "order_count", "sql": "*", "type": "count"}],
                    "entities": [
                        {"name": "order", "type": "primary", "key": "order_id"},
                        {"name": "customer", "type": "foreign", "key": "customer_id"},
                    ],
                    "relationships": [
                        {
                            "name": "orders_customers",
                            "from_entity": "customer",
                            "to_model": "customers",
                            "to_entity": "customer",
                            "cardinality": "many_to_one",
                            "join_type": "left",
                            "verified_by_contract": True,
                        }
                    ],
                }
            ]
        },
        "customers.yaml": {
            "models": [
                {
                    "name": "customers",
                    "key": "customers",
                    "table": "gold.customers",
                    "dimensions": [{"name": "customer_id", "sql": "customer_id", "type": "string"}],
                    "metrics": [{"name": "customer_count", "sql": "*", "type": "count"}],
                    "entities": [{"name": "customer", "type": "primary", "key": "customer_id"}],
                }
            ]
        },
    }
    for filename, payload in payloads.items():
        (models_dir / filename).write_text(
            yaml.safe_dump(payload, sort_keys=False),
            encoding="utf-8",
        )

    engine = SemanticEngine(mock_core, models_dir=str(models_dir))
    graph = DomainGraph(engine)

    user_edges = graph.neighbors("users")
    customer_edges = graph.neighbors("customers")

    assert [(edge.target_model, edge.reverse) for edge in user_edges] == [("profiles", True)]
    assert customer_edges == ()


def test_domain_graph_invalidates_stale_edges_after_catalog_reload(mock_core, tmp_path):
    import yaml

    models_dir = tmp_path / "semantic_models"
    models_dir.mkdir()

    def write_state(related_models):
        (models_dir / "semantic_catalog.yaml").write_text(
            yaml.safe_dump(
                {
                    "models": [
                        {
                            "key": "orders",
                            "file": "orders.yaml",
                            "entities": ["order", "customer", "store"],
                            "related_models": related_models,
                        },
                        {"key": "customers", "file": "customers.yaml", "entities": ["customer"], "related_models": []},
                        {"key": "stores", "file": "stores.yaml", "entities": ["store"], "related_models": []},
                    ]
                },
                sort_keys=False,
            ),
            encoding="utf-8",
        )
        (models_dir / "orders.yaml").write_text(
            yaml.safe_dump(
                {
                    "models": [
                        {
                            "name": "orders",
                            "key": "orders",
                            "table": "gold.orders",
                            "dimensions": [{"name": "order_id", "sql": "order_id", "type": "string"}],
                            "metrics": [{"name": "revenue", "sql": "amount", "type": "sum"}],
                            "entities": [
                                {"name": "order", "type": "primary", "key": "order_id"},
                                {"name": "customer", "type": "foreign", "key": "customer_id"},
                                {"name": "store", "type": "foreign", "key": "store_id"},
                            ],
                            "metadata": {"source_definition_hash": ",".join(related_models)},
                            "relationships": [
                                {
                                    "name": "orders_customers",
                                    "from_entity": "customer",
                                    "to_model": "customers",
                                    "to_entity": "customer",
                                    "cardinality": "many_to_one",
                                    "join_type": "left",
                                    "verified_by_contract": True,
                                }
                            ]
                            + (
                                [
                                    {
                                        "name": "orders_stores",
                                        "from_entity": "store",
                                        "to_model": "stores",
                                        "to_entity": "store",
                                        "cardinality": "many_to_one",
                                        "join_type": "left",
                                        "verified_by_contract": True,
                                    }
                                ]
                                if "stores" in related_models
                                else []
                            ),
                        }
                    ]
                },
                sort_keys=False,
            ),
            encoding="utf-8",
        )
        for key in ("customers", "stores"):
            (models_dir / f"{key}.yaml").write_text(
                yaml.safe_dump(
                    {
                        "models": [
                            {
                                "name": key,
                                "key": key,
                                "table": f"gold.{key}",
                                "dimensions": [{"name": f"{key[:-1]}_id", "sql": f"{key[:-1]}_id", "type": "string"}],
                                "metrics": [{"name": f"{key[:-1]}_count", "sql": "*", "type": "count"}],
                                "entities": [{"name": key[:-1], "type": "primary", "key": f"{key[:-1]}_id"}],
                            }
                        ]
                    },
                    sort_keys=False,
                ),
                encoding="utf-8",
            )

    write_state(["customers"])

    engine = SemanticEngine(mock_core, models_dir=str(models_dir))
    graph = DomainGraph(engine)

    assert [edge.target_model for edge in graph.neighbors("orders")] == ["customers"]

    write_state(["customers", "stores"])
    engine.reload_catalog()

    assert [edge.target_model for edge in graph.neighbors("orders")] == ["customers", "stores"]


def test_domain_graph_invalidates_reverse_edges_on_definition_hash_change(mock_core, tmp_path):
    import yaml

    models_dir = tmp_path / "semantic_models"
    models_dir.mkdir()
    (models_dir / "semantic_catalog.yaml").write_text(
        yaml.safe_dump(
            {
                "models": [
                    {"key": "profiles", "file": "profiles.yaml", "entities": ["profile"], "related_models": ["users"]},
                    {"key": "users", "file": "users.yaml", "entities": ["user"], "related_models": []},
                ]
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    (models_dir / "profiles.yaml").write_text(
        yaml.safe_dump(
            {
                "models": [
                    {
                        "name": "profiles",
                        "key": "profiles",
                        "table": "gold.profiles",
                        "dimensions": [{"name": "profile_id", "sql": "profile_id", "type": "string"}],
                        "metrics": [{"name": "profile_count", "sql": "*", "type": "count"}],
                        "entities": [{"name": "profile", "type": "primary", "key": "profile_id"}],
                        "metadata": {"source_definition_hash": "v1"},
                        "relationships": [
                            {
                                "name": "profiles_users",
                                "from_entity": "profile",
                                "to_model": "users",
                                "to_entity": "user",
                                "cardinality": "one_to_one",
                                "join_type": "inner",
                                "verified_by_contract": True,
                            }
                        ],
                    }
                ]
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    (models_dir / "users.yaml").write_text(
        yaml.safe_dump(
            {
                "models": [
                    {
                        "name": "users",
                        "key": "users",
                        "table": "gold.users",
                        "dimensions": [{"name": "user_id", "sql": "user_id", "type": "string"}],
                        "metrics": [{"name": "user_count", "sql": "*", "type": "count"}],
                        "entities": [{"name": "user", "type": "primary", "key": "user_id"}],
                    }
                ]
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    engine = SemanticEngine(mock_core, models_dir=str(models_dir))
    graph = DomainGraph(engine)

    assert [(edge.target_model, edge.reverse) for edge in graph.neighbors("users")] == [
        ("profiles", True)
    ]

    engine._cache["profiles"] = {
        "name": "profiles",
        "key": "profiles",
        "table": "gold.profiles",
        "dimensions": [{"name": "profile_id", "sql": "profile_id", "type": "string"}],
        "metrics": [{"name": "profile_count", "sql": "*", "type": "count"}],
        "entities": [{"name": "profile", "type": "primary", "key": "profile_id"}],
        "metadata": {"source_definition_hash": "v2"},
    }

    assert graph.neighbors("users") == ()
