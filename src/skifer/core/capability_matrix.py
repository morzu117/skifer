"""Explicit SQL capability declarations, pending executed parity in Plan 46.3."""

from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Literal


@dataclass(frozen=True)
class Supported:
    kind: Literal["supported"] = field(default="supported", init=False)
    reason: str = field(default="", init=False)


@dataclass(frozen=True)
class Conditional:
    reason: str
    kind: Literal["conditional"] = field(default="conditional", init=False)


@dataclass(frozen=True)
class Refused:
    reason: str
    kind: Literal["refused"] = field(default="refused", init=False)


Status = Supported | Conditional | Refused

# Deliberately enumerated, not generated from op_catalog: adding a language
# operation must fail the drift guard until its capability is declared here.
_KEYS = frozenset({
    "filter:is_null", "filter:is_not_null", "filter:equals", "filter:not_equals",
    "filter:greater_than", "filter:less_than", "filter:greater_than_equal",
    "filter:less_than_equal", "filter:in", "filter:not_in", "filter:between",
    "filter:not_between", "filter:contains", "filter:not_contains",
    "filter:starts_with", "filter:ends_with", "filter:like", "filter:not_like", "filter:sql",
    "op:cast", "op:upper", "op:lower", "op:trim", "op:length", "op:split",
    "op:substring", "op:round", "op:abs", "op:ceil", "op:to_date", "op:nvl",
    "op:coalesce", "op:lit", "op:expr", "op:col", "op:when", "op:then", "op:else",
    "agg:sum", "agg:avg", "agg:min", "agg:max", "agg:count", "agg:count_distinct",
    "agg:sum_distinct", "agg:approx_count_distinct", "agg:stddev", "agg:variance",
    "agg:first", "agg:last",
    "join:inner", "join:left", "join:right", "join:full", "join:cross",
    "join:left_semi", "join:left_anti",
})

_matrix: dict[str, dict[str, Status]] = {
    "databricks": {key: Supported() for key in _KEYS},
    "duckdb": {key: Conditional("parity not executed yet (Plan 46.3)") for key in _KEYS},
    "snowflake": {key: Conditional("parity not executed yet (Plan 46.3)") for key in _KEYS},
}
for _target in ("duckdb", "snowflake"):
    for _key in ("filter:sql", "op:expr"):
        _matrix[_target][_key] = Refused(
            "raw SQL fragments are authored in Spark SQL and are not rendered for another target"
        )
    _matrix[_target]["op:split"] = Conditional(
        "Spark split() takes a regex, the target splits on a literal separator"
    )
    _matrix[_target]["op:to_date"] = Conditional(
        "date format strings differ between Spark and the target"
    )
    _matrix[_target]["op:cast"] = Conditional(
        "type names are rendered by sqlglot and Spark casts return NULL on failure "
        "where the target raises; verify in parity"
    )
for _key in ("agg:first", "agg:last"):
    _matrix["snowflake"][_key] = Refused(
        "non-deterministic without ORDER BY; no portable equivalent"
    )
for _key in ("join:left_semi", "join:left_anti"):
    _matrix["snowflake"][_key] = Refused("no SEMI/ANTI JOIN syntax on this target")

_matrix["duckdb"]["op:split"] = Conditional(
    "Spark split() is a regex split; sqlglot maps it to str_split_regex, "
    "verify the separator semantics in parity"
)
MATRIX = MappingProxyType({
    target: MappingProxyType(entries) for target, entries in _matrix.items()
})
del _matrix


def capability(target: str, key: str) -> Status:
    """Look up a declared capability; unknown targets and keys never default."""
    if target not in MATRIX:
        raise KeyError(f"Unknown capability target: {target!r}")
    if key not in MATRIX[target]:
        raise KeyError(f"Unknown capability key {key!r} for target {target!r}")
    return MATRIX[target][key]
