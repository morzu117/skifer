"""
Operator catalog — single source of truth for all filter operators and column operations.

Pure Python module with no Spark/Snowpark dependency. Used by:
  - schema_loader.py    (fail-fast validation at load time)
  - operations.py       (runtime safety net)
  - core/spark_backend.py (Lot 3 IR dispatch tables)
  - json_schema.py      (Lot 4 JSON Schema generation)
"""

from __future__ import annotations

import difflib
from dataclasses import dataclass, field


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class OperatorSpec:
    """Descriptor for a single filter operator."""

    canonical: str
    """Canonical name, e.g. ``"equals"``."""

    aliases: frozenset = field(default_factory=frozenset)
    """Alternative spellings accepted in YAML, e.g. ``frozenset({"eq", "="})``.``"""

    arity: str = "single"
    """
    ``"none"``    — operator takes no value (``is_null``, ``is_not_null``)
    ``"single"``  — operator takes one scalar value
    ``"list"``    — operator takes a comma-separated / YAML-list value (``in``, ``not_in``, ``between``, ``not_between``)
    ``"sql"``     — operator takes a raw SQL expression (``sql``)
    """

    description: str = ""
    requires_raw_sql: bool = False


@dataclass(frozen=True)
class OpSpec:
    """Descriptor for a single column operation used in ``select_final`` / ``add_columns``."""

    name: str
    """Canonical op name (prefix before ``:``)."""

    arity: str = "none"
    """
    ``"none"``        — no argument  (``upper``, ``lower``, ``trim``, ``abs``, ``length``, ``ceil``)
    ``"single"``      — one argument after ``:``  (``cast:``, ``lit:``, ``round:``, …)
    ``"two"``         — two comma-separated arguments (``split:sep,idx``, ``substring:start,len``)
    ``"expression"``  — raw SQL expression (``expr:``)
    ``"condition"``   — filter-style sub-expression (``when:``)
    ``"passthrough"`` — no-op / structural keyword (``then:``, ``else:``, ``col``)
    """

    description: str = ""
    requires_raw_sql: bool = False


@dataclass(frozen=True)
class AggregateSpec:
    """Descriptor for a single aggregate function used in the ``aggregate:`` block."""

    canonical: str
    """Canonical function name, e.g. ``"count_distinct"``."""

    sql_func: str
    """SQL function name emitted by the compiler, e.g. ``"COUNT"``."""

    aliases: frozenset = field(default_factory=frozenset)
    """Alternative spellings accepted in YAML, e.g. ``frozenset({"mean"})``."""

    distinct: bool = False
    """When True the compiler emits ``FUNC(DISTINCT col)``."""

    allows_star: bool = False
    """When True the measure may use ``source: "*"`` (only ``count``)."""

    description: str = ""


# ---------------------------------------------------------------------------
# Filter operator catalog
# ---------------------------------------------------------------------------

def _fs(*args) -> frozenset:
    return frozenset(args)


FILTER_OPERATORS: dict[str, OperatorSpec] = {
    # ----- existence checks -----
    "is_null": OperatorSpec(
        canonical="is_null",
        arity="none",
        description="Passes rows where the column is NULL.",
    ),
    "is_not_null": OperatorSpec(
        canonical="is_not_null",
        arity="none",
        description="Passes rows where the column is not NULL.",
    ),
    # ----- equality -----
    "equals": OperatorSpec(
        canonical="equals",
        aliases=_fs("eq", "="),
        arity="single",
        description="Passes rows where column == value.",
    ),
    "not_equals": OperatorSpec(
        canonical="not_equals",
        aliases=_fs("ne", "!="),
        arity="single",
        description="Passes rows where column != value.",
    ),
    # ----- comparison -----
    "greater_than": OperatorSpec(
        canonical="greater_than",
        aliases=_fs("gt", ">"),
        arity="single",
        description="Passes rows where column > value.",
    ),
    "less_than": OperatorSpec(
        canonical="less_than",
        aliases=_fs("lt", "<"),
        arity="single",
        description="Passes rows where column < value.",
    ),
    "greater_than_equal": OperatorSpec(
        canonical="greater_than_equal",
        aliases=_fs("gte", ">="),
        arity="single",
        description="Passes rows where column >= value.",
    ),
    "less_than_equal": OperatorSpec(
        canonical="less_than_equal",
        aliases=_fs("lte", "<="),
        arity="single",
        description="Passes rows where column <= value.",
    ),
    # ----- set membership -----
    "in": OperatorSpec(
        canonical="in",
        arity="list",
        description="Passes rows where column value is in the provided list.",
    ),
    "between": OperatorSpec(
        canonical="between",
        arity="list",
        description="Passes rows where lo <= column <= hi (inclusive). Two comma-separated values.",
    ),
    "not_between": OperatorSpec(
        canonical="not_between",
        arity="list",
        description="Passes rows where column is strictly outside the provided [lo, hi] interval.",
    ),
    "not_in": OperatorSpec(
        canonical="not_in",
        arity="list",
        description="Passes rows where column value is NOT in the provided list.",
    ),
    # ----- string pattern -----
    "contains": OperatorSpec(
        canonical="contains",
        arity="single",
        description="Passes rows where column contains the substring.",
    ),
    "not_contains": OperatorSpec(
        canonical="not_contains",
        aliases=_fs("notcontains"),
        arity="single",
        description="Passes rows where column does NOT contain the substring.",
    ),
    "starts_with": OperatorSpec(
        canonical="starts_with",
        arity="single",
        description="Passes rows where column starts with the prefix.",
    ),
    "ends_with": OperatorSpec(
        canonical="ends_with",
        arity="single",
        description="Passes rows where column ends with the suffix.",
    ),
    "like": OperatorSpec(
        canonical="like",
        arity="single",
        description="Passes rows where column matches the SQL LIKE pattern.",
    ),
    "not_like": OperatorSpec(
        canonical="not_like",
        aliases=_fs("notlike"),
        arity="single",
        description="Passes rows where column does NOT match the SQL LIKE pattern.",
    ),
    # ----- raw SQL escape hatch -----
    "sql": OperatorSpec(
        canonical="sql",
        arity="sql",
        description="Passes rows matching a raw SQL expression (governed by allow_raw_sql).",
        requires_raw_sql=True,
    ),
}

# ---------------------------------------------------------------------------
# Column operation catalog
# ---------------------------------------------------------------------------

COLUMN_OPS: dict[str, OpSpec] = {
    # ---- type conversion ----
    "cast": OpSpec("cast", arity="single", description="Cast column to the given Spark/SQL type."),
    # ---- string ----
    "upper": OpSpec("upper", arity="none", description="Convert string to upper case."),
    "lower": OpSpec("lower", arity="none", description="Convert string to lower case."),
    "trim": OpSpec("trim", arity="none", description="Strip leading and trailing whitespace."),
    "length": OpSpec("length", arity="none", description="Return string/binary length."),
    "split": OpSpec("split", arity="two", description="Split string on separator and return element at index (sep,idx)."),
    "substring": OpSpec("substring", arity="two", description="Extract substring (start,len)."),
    # ---- numeric ----
    "round": OpSpec("round", arity="single", description="Round to N decimal places."),
    "abs": OpSpec("abs", arity="none", description="Absolute value."),
    "ceil": OpSpec("ceil", arity="none", description="Round up to nearest integer."),
    # ---- date ----
    "to_date": OpSpec("to_date", arity="single", description="Parse string to date using the given format."),
    # ---- null handling ----
    "nvl": OpSpec("nvl", arity="single", description="Replace NULL with the given literal value (alias: coalesce:)."),
    "coalesce": OpSpec("coalesce", arity="single", description="Replace NULL with the given literal value (alias: nvl:)."),
    # ---- literal / expression ----
    "lit": OpSpec("lit", arity="single", description="Replace column with a literal constant."),
    "expr": OpSpec(
        "expr",
        arity="expression",
        description="Apply a raw Spark SQL expression (governed by allow_raw_sql).",
        requires_raw_sql=True,
    ),
    # ---- column reference ----
    "col": OpSpec("col", arity="passthrough", description="Reference another column by name, or keep the current column unchanged."),
    # ---- conditional ----
    "when": OpSpec("when", arity="condition", description="Conditional test; must be followed by then:/else:."),
    "then": OpSpec("then", arity="passthrough", description="Value/op applied when the preceding when: condition is true."),
    "else": OpSpec("else", arity="passthrough", description="Fallback value/op when no when: condition matched."),
}

# ---------------------------------------------------------------------------
# Aggregate function catalog (Plan 28)
# ---------------------------------------------------------------------------

AGGREGATE_FUNCTIONS: dict[str, AggregateSpec] = {
    "sum": AggregateSpec("sum", sql_func="SUM", description="Sum of the column values."),
    "avg": AggregateSpec("avg", sql_func="AVG", aliases=_fs("mean", "average"), description="Arithmetic mean."),
    "min": AggregateSpec("min", sql_func="MIN", description="Smallest value."),
    "max": AggregateSpec("max", sql_func="MAX", description="Largest value."),
    "count": AggregateSpec(
        "count",
        sql_func="COUNT",
        allows_star=True,
        description="Row count; use source '*' to count every row including NULLs.",
    ),
    "count_distinct": AggregateSpec(
        "count_distinct",
        sql_func="COUNT",
        aliases=_fs("countdistinct", "distinct_count"),
        distinct=True,
        description="Number of distinct non-NULL values.",
    ),
    "sum_distinct": AggregateSpec(
        "sum_distinct",
        sql_func="SUM",
        aliases=_fs("sumdistinct"),
        distinct=True,
        description="Sum of the distinct values.",
    ),
    "approx_count_distinct": AggregateSpec(
        "approx_count_distinct",
        sql_func="APPROX_COUNT_DISTINCT",
        aliases=_fs("approx_distinct_count"),
        description="HyperLogLog estimate of the distinct count (cheaper on large data).",
    ),
    "stddev": AggregateSpec("stddev", sql_func="STDDEV", aliases=_fs("std"), description="Sample standard deviation."),
    "variance": AggregateSpec("variance", sql_func="VARIANCE", aliases=_fs("var"), description="Sample variance."),
    "first": AggregateSpec("first", sql_func="FIRST", description="First value in the group."),
    "last": AggregateSpec("last", sql_func="LAST", description="Last value in the group."),
}


# ---------------------------------------------------------------------------
# Static type inference maps (used by infer_output_schema — no Spark needed)
# ---------------------------------------------------------------------------

# Maps column op name → inferred SQL output type
OP_INFERRED_TYPE: dict[str, str] = {
    "to_date": "date",
    "round": "double",
}

# Maps cast target (op.args[0]) → inferred SQL output type
CAST_INFERRED_TYPE: dict[str, str] = {
    "double": "double",
    "float": "double",
    "int": "int",
    "integer": "int",
    "long": "long",
    "bigint": "long",
    "string": "string",
    "date": "date",
    "timestamp": "timestamp",
    "boolean": "boolean",
}

# ---------------------------------------------------------------------------
# Alias → canonical resolution helpers
# ---------------------------------------------------------------------------

# Build flat alias→canonical map for O(1) lookup.
_FILTER_ALIAS_MAP: dict[str, str] = {}
for _spec in FILTER_OPERATORS.values():
    _FILTER_ALIAS_MAP[_spec.canonical] = _spec.canonical
    for _alias in _spec.aliases:
        _FILTER_ALIAS_MAP[_alias] = _spec.canonical

_OP_ALIAS_MAP: dict[str, str] = {name: name for name in COLUMN_OPS}

_AGG_ALIAS_MAP: dict[str, str] = {}
for _agg in AGGREGATE_FUNCTIONS.values():
    _AGG_ALIAS_MAP[_agg.canonical] = _agg.canonical
    for _alias in _agg.aliases:
        _AGG_ALIAS_MAP[_alias] = _agg.canonical


def resolve_filter_operator(name: str) -> str | None:
    """
    Resolve a filter operator name (canonical or alias) to its canonical form.

    Returns:
        The canonical name, or ``None`` if *name* is not in the catalog.
    """
    return _FILTER_ALIAS_MAP.get(name.lower() if name else "")


def resolve_column_op(name: str) -> str | None:
    """
    Resolve a column op name to its canonical form.

    Returns:
        The canonical name, or ``None`` if *name* is not in the catalog.
    """
    return _OP_ALIAS_MAP.get(name.lower() if name else "")


def resolve_aggregate_function(name: str) -> str | None:
    """
    Resolve an aggregate function name (canonical or alias) to its canonical form.

    Returns:
        The canonical name, or ``None`` if *name* is not in the catalog.
    """
    return _AGG_ALIAS_MAP.get(name.lower() if name else "")


def suggest(name: str, catalog: dict) -> list[str]:
    """
    Return close-match suggestions for *name* within *catalog* keys (and aliases).

    Uses ``difflib.get_close_matches`` with a cutoff of 0.6.

    Args:
        name:    The unknown / misspelled name.
        catalog: :data:`FILTER_OPERATORS`, :data:`COLUMN_OPS` or :data:`AGGREGATE_FUNCTIONS`.

    Returns:
        A list of suggested canonical names (may be empty).
    """
    if catalog is FILTER_OPERATORS:
        universe = list(_FILTER_ALIAS_MAP.keys())
        resolve = resolve_filter_operator
    elif catalog is AGGREGATE_FUNCTIONS:
        universe = list(_AGG_ALIAS_MAP.keys())
        resolve = resolve_aggregate_function
    else:
        universe = list(_OP_ALIAS_MAP.keys())
        resolve = resolve_column_op
    matches = difflib.get_close_matches(name, universe, n=3, cutoff=0.6)
    # Resolve aliases → canonical so suggestions are always canonical.
    seen: set[str] = set()
    result: list[str] = []
    for m in matches:
        canonical = resolve(m)
        if canonical and canonical not in seen:
            seen.add(canonical)
            result.append(canonical)
    return result
