import pytest

from skifer.core.capabilities_matrix import (
    ALL_CAPABILITIES,
    DATABRICKS_CAPABILITIES,
    CAP_DEV_LIMIT,
    CAP_DROP_DUPLICATES,
    CAP_FILE_SOURCES,
    CAP_JDBC_SINK,
    CAP_INCREMENTAL,
    CAP_LOADERS,
    CAP_MATERIALIZED_VIEW,
    CAP_PREPROCESS_QUALIFY,
    CAP_PYTHON_RULES,
    CAP_STREAMING,
    CAP_SNAPSHOT,
    CAP_VIEW,
    UnsupportedCapabilityError,
    assert_supported,
    required_capabilities,
)
from skifer.core.ir import parse_to_ir
from skifer.core.registry import RuleRegistry


@pytest.mark.parametrize(
    ("schema", "expected"),
    [
        (
            {"tables": [{"name": "orders", "source": {"type": "csv"}}]},
            CAP_FILE_SOURCES,
        ),
        (
            {
                "tables": [
                    {
                        "name": "orders",
                        "source_type": "loader",
                        "function_name": "load_orders",
                    }
                ]
            },
            CAP_LOADERS,
        ),
        ({"tables": [{"name": "orders", "streaming": True}]}, CAP_STREAMING),
        (
            {"materialization": {"type": "materialized_view"}},
            CAP_MATERIALIZED_VIEW,
        ),
        ({"materialization": {"type": "view"}}, CAP_VIEW),
        (
            {"materialization": {"type": "incremental", "strategy": "append"}},
            CAP_INCREMENTAL,
        ),
        (
            {
                "materialization": {
                    "type": "snapshot",
                    "strategy": "timestamp",
                    "unique_key": ["id"],
                    "updated_at": "modified_at",
                }
            },
            CAP_SNAPSHOT,
        ),
        ({"sink": {"type": "jdbc"}}, CAP_JDBC_SINK),
        ({"dev_limit": 10}, CAP_DEV_LIMIT),
        (
            {
                "tables": [
                    {
                        "name": "orders",
                        "quality_checks": {"drop_duplicates_on": ["id"]},
                    }
                ]
            },
            CAP_DROP_DUPLICATES,
        ),
        (
            {
                "tables": [
                    {"name": "orders", "preprocess": {"qualify": {"limit": 1}}}
                ]
            },
            CAP_PREPROCESS_QUALIFY,
        ),
    ],
)
def test_required_capabilities_detects_each_yaml_construction(schema, expected):
    assert required_capabilities(parse_to_ir(schema)) == frozenset({expected})


@pytest.mark.parametrize(
    ("kind", "expected"),
    [
        ("projection", frozenset({CAP_PYTHON_RULES})),
        ("aggregation", frozenset({CAP_PYTHON_RULES})),
        ("transform", frozenset({CAP_PYTHON_RULES})),
        ("sql", frozenset()),
    ],
)
def test_business_rule_capability_depends_on_registered_kind(kind, expected):
    name = f"_capability_{kind}"

    if kind == "sql":
        @RuleRegistry.register_rule(name=name, kind=kind)
        def rule():
            return {"derived": "amount * 2"}
    else:
        @RuleRegistry.register_rule(name=name, kind=kind)
        def rule(df):
            return {}

    try:
        parsed = parse_to_ir({"business_rules": [name]})
        assert required_capabilities(parsed) == expected
    finally:
        RuleRegistry._rules.pop(name, None)


def test_ordinary_schema_requires_no_capability():
    parsed = parse_to_ir({"tables": [{"name": "silver.orders"}]})

    assert required_capabilities(parsed) == frozenset()


def test_partials_require_their_child_capabilities_recursively():
    parsed = parse_to_ir(
        {
            "partials": [
                {
                    "alias": "outer",
                    "schema": {
                        "partials": [
                            {
                                "alias": "inner",
                                "schema": {"business_rules": ["enrich"]},
                            }
                        ]
                    },
                }
            ]
        }
    )

    assert required_capabilities(parsed) == frozenset({CAP_PYTHON_RULES})


def test_assert_supported_accepts_all_capabilities():
    parsed = parse_to_ir(
        {
            "business_rules": ["enrich"],
            "tables": [{"name": "orders", "streaming": True}],
        }
    )

    assert_supported(
        parsed, adapter_name="databricks", supported=ALL_CAPABILITIES
    )


_CAPABILITY_ERROR_SCHEMAS = (
    {
        "business_rules": ["enrich"],
        "dev_limit": 10,
        "tables": [
            {
                "name": "file_orders",
                "source": {"type": "csv", "path": "/tmp/orders.csv"},
                "quality_checks": {"drop_duplicates_on": ["id"]},
                "preprocess": {"qualify": {"limit": 1}},
            },
            {
                "name": "loaded_orders",
                "source_type": "loader",
                "function_name": "load_orders",
            },
        ],
        "materialization": {"type": "table"},
        "sink": {"type": "jdbc"},
    },
    {
        "tables": [{"name": "events", "streaming": True}],
        "materialization": {"type": "streaming_table"},
    },
    {
        "tables": [{"name": "silver.orders"}],
        "materialization": {"type": "materialized_view"},
    },
    {
        "tables": [{"name": "silver.orders"}],
        "materialization": {"type": "view"},
    },
    {
        "tables": [{"name": "silver.orders"}],
        "materialization": {"type": "incremental", "strategy": "append"},
    },
    {
        "tables": [{"name": "silver.orders"}],
        "materialization": {
            "type": "snapshot",
            "strategy": "timestamp",
            "unique_key": ["id"],
            "updated_at": "modified_at",
        },
    },
)


@pytest.mark.parametrize(
    "schema",
    _CAPABILITY_ERROR_SCHEMAS,
    ids=lambda schema: schema["materialization"]["type"],
)
def test_assert_supported_names_every_missing_capability_deterministically(schema):
    """Parameterize by materialization because root materializations are exclusive.

    Keeping each materialization at the root proves the diagnostic on product-valid
    shapes; putting mutually exclusive writes in partials would test ignored output
    semantics that a partial cannot validly declare.
    """
    parsed = parse_to_ir(schema)
    required = required_capabilities(parsed)

    messages = []
    for _ in range(2):
        with pytest.raises(UnsupportedCapabilityError) as exc_info:
            assert_supported(
                parsed, adapter_name="tiny", supported=frozenset()
            )
        messages.append(str(exc_info.value))

    assert messages[0] == messages[1]
    assert "tiny" in messages[0]
    for capability in sorted(required):
        assert capability in messages[0]
    positions = [
        messages[0].index(f"{capability} (required")
        for capability in sorted(required)
    ]
    assert positions == sorted(positions)
    assert "YAML" in messages[0]


def test_capability_error_schemas_cover_all_capabilities_exactly():
    covered = frozenset().union(
        *(
            required_capabilities(parse_to_ir(schema))
            for schema in _CAPABILITY_ERROR_SCHEMAS
        )
    )

    assert covered == ALL_CAPABILITIES


def test_databricks_capabilities_exclude_unimplemented_write_strategies():
    assert DATABRICKS_CAPABILITIES.isdisjoint({CAP_SNAPSHOT})
    assert CAP_VIEW in DATABRICKS_CAPABILITIES
    assert CAP_INCREMENTAL in DATABRICKS_CAPABILITIES


def test_incremental_capability_does_not_enable_merge_strategy():
    parsed = parse_to_ir(
        {
            "tables": [{"name": "orders"}],
            "materialization": {
                "type": "incremental",
                "strategy": "merge",
                "unique_key": ["id"],
            },
        }
    )

    with pytest.raises(UnsupportedCapabilityError, match="merge.*append"):
        assert_supported(
            parsed, adapter_name="databricks", supported=DATABRICKS_CAPABILITIES
        )
