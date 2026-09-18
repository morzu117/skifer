import pytest

from skifer.core.capabilities_matrix import (
    ALL_CAPABILITIES,
    CAP_DEV_LIMIT,
    CAP_DROP_DUPLICATES,
    CAP_FILE_SOURCES,
    CAP_JDBC_SINK,
    CAP_LOADERS,
    CAP_MATERIALIZED_VIEW,
    CAP_PREPROCESS_QUALIFY,
    CAP_PYTHON_RULES,
    CAP_STREAMING,
    UnsupportedCapabilityError,
    assert_supported,
    required_capabilities,
)
from skifer.core.ir import parse_to_ir


@pytest.mark.parametrize(
    ("schema", "expected"),
    [
        ({"business_rules": ["enrich"]}, CAP_PYTHON_RULES),
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


def test_assert_supported_names_every_missing_capability_deterministically():
    parsed = parse_to_ir(
        {
            "business_rules": ["enrich"],
            "partials": [{"alias": "nested"}],
            "dev_limit": 10,
            "tables": [
                {
                    "name": "file_orders",
                    "source": {"type": "csv"},
                    "streaming": True,
                    "quality_checks": {"drop_duplicates_on": ["id"]},
                    "preprocess": {"qualify": {"limit": 1}},
                },
                {
                    "name": "loaded_orders",
                    "source_type": "loader",
                    "function_name": "load_orders",
                },
            ],
            "materialization": {"type": "materialized_view"},
            "sink": {"type": "jdbc"},
        }
    )

    messages = []
    for _ in range(2):
        with pytest.raises(UnsupportedCapabilityError) as exc_info:
            assert_supported(
                parsed, adapter_name="tiny", supported=frozenset()
            )
        messages.append(str(exc_info.value))

    assert messages[0] == messages[1]
    assert "tiny" in messages[0]
    for capability in sorted(ALL_CAPABILITIES):
        assert capability in messages[0]
    positions = [messages[0].index(capability) for capability in sorted(ALL_CAPABILITIES)]
    assert positions == sorted(positions)
    assert "YAML" in messages[0]
