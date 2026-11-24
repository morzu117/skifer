"""Matrix completeness is independent of the explicit implementation rows."""

from dataclasses import FrozenInstanceError

import pytest

from skifer.core.capability_matrix import MATRIX, Conditional, Refused, Supported, capability
from skifer.core.dialect import TARGETS
from skifer.core.ir import VALID_JOIN_TYPES, _JOIN_TYPE_ALIASES
from skifer.core.op_catalog import AGGREGATE_FUNCTIONS, COLUMN_OPS, FILTER_OPERATORS

ALL_KEYS = (
    {f"filter:{key}" for key in FILTER_OPERATORS}
    | {f"op:{key}" for key in COLUMN_OPS}
    | {f"agg:{key}" for key in AGGREGATE_FUNCTIONS}
    | {f"join:{key}" for key in VALID_JOIN_TYPES | set(_JOIN_TYPE_ALIASES.values())}
)


@pytest.mark.parametrize("target", TARGETS)
def test_catalog_drift_guard(target):
    assert set(MATRIX) == set(TARGETS)
    assert set(MATRIX[target]) == ALL_KEYS


def test_first_is_conditional_on_duckdb_and_refused_on_snowflake():
    assert capability("duckdb", "agg:first").kind == "conditional"
    assert capability("snowflake", "agg:first").kind == "refused"


@pytest.mark.parametrize("target,key", [("oracle", "agg:sum"), ("duckdb", "sum")])
def test_unknown_target_or_key_is_explicit(target, key):
    with pytest.raises(KeyError, match="Unknown capability"):
        capability(target, key)


def test_statuses_have_reasons_and_databricks_is_supported():
    for entries in MATRIX.values():
        for status in entries.values():
            assert type(status) in {Supported, Conditional, Refused}
            if isinstance(status, (Conditional, Refused)):
                assert status.reason
    assert all(isinstance(s, Supported) and s.reason == "" for s in MATRIX["databricks"].values())


@pytest.mark.parametrize("status", [Supported(), Conditional("pending"), Refused("unsafe")])
def test_status_is_frozen(status):
    with pytest.raises(FrozenInstanceError):
        status.kind = "unknown"


@pytest.mark.parametrize("target", ["duckdb", "snowflake"])
def test_explicit_conditional_reasons(target):
    reasons = {
        "filter:sql": "raw SQL is passed through unrendered; the author owns the target dialect",
        "op:expr": "raw SQL is passed through unrendered; the author owns the target dialect",
        "op:split": "Spark split() takes a regex, the target splits on a literal separator",
        "op:to_date": "date format strings differ between Spark and the target",
        "op:cast": "type names are rendered by sqlglot; verify each type in parity",
    }
    for key, reason in reasons.items():
        assert capability(target, key) == Conditional(reason)
