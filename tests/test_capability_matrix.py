"""Matrix completeness is independent of the explicit implementation rows."""

from dataclasses import FrozenInstanceError

import pytest

from skifer.core.capability_matrix import MATRIX, Conditional, Refused, Supported, capability
from skifer.core.capability_matrix import _DUCKDB_PROVEN, _KEYS
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
    assert set(MATRIX[target]) == ALL_KEYS | VALUE_KEYS


def test_duckdb_supported_keys_are_explicitly_proven():
    assert _DUCKDB_PROVEN <= _KEYS
    assert {
        key for key, status in MATRIX["duckdb"].items() if isinstance(status, Supported)
    } == _DUCKDB_PROVEN


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
        "op:split": "Spark split() takes a regex, the target splits on a literal separator",
        "op:to_date": "date format strings differ between Spark and the target",
        "op:cast": (
            "type names are rendered by sqlglot; cast failure semantics are unverified until parity (Plan 46.6)"
        ),
    }
    if target == "duckdb":
        reasons = {
            "op:cast": (
                'TRY_CAST on DuckDB: a double->int cast truncates on Spark but rounds on DuckDB, and '
                'an unconvertible value raises on Spark (ANSI) but yields NULL on DuckDB; parity '
                'proven for casts only (numeric text->double, integer->string, '
                'decimal->double)'
            ),
            "op:split": (
                'both engines split on a regex (Spark split, DuckDB str_split_regex) and sqlglot '
                'shifts the 0-based index; parity proven for a literal separator only, Java and RE2 '
                'regex syntax diverge beyond that'
            ),
            "op:to_date": (
                'sqlglot translates Java date tokens to strptime; parity proven for yyyy-MM-dd and '
                'dd/MM/yyyy only'
            ),
            "agg:avg": (
                'AVG over DECIMAL yields DECIMAL(p+4,s+4) on Spark and DOUBLE on DuckDB; parity proven '
                'over DOUBLE inputs only'
            ),
            "agg:first": (
                'order-dependent without ORDER BY; parity proven only when every group holds a single '
                'non-null distinct value'
            ),
            "agg:last": (
                'order-dependent without ORDER BY; parity proven only when every group holds a single '
                'non-null distinct value'
            ),
            "agg:approx_count_distinct": (
                'approximate algorithms differ by engine; parity proven exact on the canonical dataset '
                '(at most 2 distinct values per group on the canonical dataset)'
            ),
        }
    for key, reason in reasons.items():
        assert capability(target, key) == Conditional(reason)


@pytest.mark.parametrize("target", ["duckdb", "snowflake"])
@pytest.mark.parametrize("key", ["filter:sql", "op:expr"])
def test_raw_sql_is_refused(target, key):
    assert capability(target, key) == Refused(
        "raw SQL fragments are authored in Spark SQL and are not rendered for another target"
    )


def test_matrix_targets_are_read_only():
    with pytest.raises(TypeError):
        MATRIX["duckdb"] = {}


@pytest.mark.parametrize("target", TARGETS)
def test_matrix_entries_are_read_only(target):
    with pytest.raises(TypeError):
        MATRIX[target]["op:cast"] = Supported()


def test_snowflake_has_no_executed_parity_even_with_a_connector():
    assert all(isinstance(status, (Conditional, Refused)) for status in MATRIX["snowflake"].values())
    special = {"op:split", "op:to_date", "op:cast", "literal:decimal_text"}
    for key, status in MATRIX["snowflake"].items():
        if isinstance(status, Conditional) and key not in special:
            assert status.reason == "parity not executed against a Snowflake account"


# --- Plan 47.5: value-conditioned keys --------------------------------------------------

#: Declared by hand, like the matrix itself: these keys come from a filter *value*,
#: not from op_catalog, so the drift guard must name them explicitly.
VALUE_KEYS = {"literal:decimal_text", "literal:like_backslash"}


def test_value_keys_are_declared_in_the_literal_space():
    assert VALUE_KEYS <= _KEYS
    assert {key for key in _KEYS if key.startswith("literal:")} == VALUE_KEYS
    assert VALUE_KEYS.isdisjoint(ALL_KEYS)
    assert VALUE_KEYS.isdisjoint(_DUCKDB_PROVEN)


@pytest.mark.parametrize("target", ["duckdb", "snowflake"])
def test_value_keys_statuses(target):
    assert capability("databricks", "literal:decimal_text") == Supported()
    assert capability("databricks", "literal:like_backslash") == Supported()
    assert capability(target, "literal:decimal_text") == Conditional(
        "compared to an integer column, Spark (ANSI) raises and the target rounds"
    )
    assert capability(target, "literal:like_backslash") == Refused(
        "'\\' is a LIKE escape on Spark and a literal character on the target"
    )
