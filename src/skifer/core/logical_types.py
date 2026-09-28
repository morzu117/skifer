"""Closed set of verifiable ``contract.output`` logical types (Plan 40).

A contract declares a *logical* type — ``string``, ``decimal`` — while an engine
reports a *physical* one. Checking one against the other needs a mapping, and
the mapping is the whole risk of the feature: a logical type with no physical
equivalent has no honest verdict.

``docs/yaml_spec.md`` documents ``logical_type: identifier``, which is exactly
that case — an identifier may be a ``string`` or a ``bigint``. A permissive
mapping would either compare it against something arbitrary or skip it, and a
check that cannot fail is worse than no check: it reports coverage it does not
provide. So the set below is **closed**, and anything outside it is refused at
configuration time rather than silently skipped.

The physical names were measured, not assumed — on a local Spark 4 session and
on DuckDB, by describing a table with one column per logical type. Adding a
dialect means measuring it the same way; ``COVERED_DIALECTS`` exists so that an
unmeasured engine raises instead of guessing.
"""

from __future__ import annotations


class UnverifiableLogicalType(ValueError):
    """Raised for a logical type, or a dialect, with no established mapping."""


#: Dialects whose physical type names have actually been measured.
#:
#: ``skifer.core.dialect.SUPPORTED_DIALECTS`` is larger: Snowflake and BigQuery
#: can be transpiled to, but their introspection output has never been observed
#: here (plan 39.7 / 39.8 need real accounts). Listing them anyway would put an
#: unverified mapping behind a check whose only job is to be trustworthy.
COVERED_DIALECTS: frozenset[str] = frozenset({"databricks", "duckdb"})


#: logical type -> dialect -> accepted physical type names, lowercased.
_PHYSICAL_TYPES: dict[str, dict[str, tuple[str, ...]]] = {
    "string": {"databricks": ("string",), "duckdb": ("varchar",)},
    "integer": {"databricks": ("int",), "duckdb": ("integer",)},
    "long": {"databricks": ("bigint",), "duckdb": ("bigint",)},
    "double": {"databricks": ("double",), "duckdb": ("double",)},
    "decimal": {"databricks": ("decimal",), "duckdb": ("decimal",)},
    "boolean": {"databricks": ("boolean",), "duckdb": ("boolean",)},
    "date": {"databricks": ("date",), "duckdb": ("date",)},
    "timestamp": {"databricks": ("timestamp",), "duckdb": ("timestamp",)},
}

#: The logical types a contract may declare.
LOGICAL_TYPES: frozenset[str] = frozenset(_PHYSICAL_TYPES)


def is_verifiable(logical_type: str) -> bool:
    """Return whether ``logical_type`` has an established physical mapping."""
    return logical_type.strip().lower() in LOGICAL_TYPES


def physical_types_for(logical_type: str, dialect: str) -> tuple[str, ...]:
    """Return the physical type names accepted for ``logical_type`` on ``dialect``.

    Raises :class:`UnverifiableLogicalType` rather than returning an empty tuple:
    an empty tuple would flow into the comparison below and quietly fail every
    column, turning a configuration mistake into a data incident.
    """
    normalized = logical_type.strip().lower()
    if normalized not in _PHYSICAL_TYPES:
        allowed = ", ".join(sorted(LOGICAL_TYPES))
        raise UnverifiableLogicalType(
            f"Logical type {logical_type!r} has no physical equivalent, so a contract "
            f"declaring it cannot be verified against the produced table. "
            f"Verifiable logical types: {allowed}."
        )

    by_dialect = _PHYSICAL_TYPES[normalized]
    if dialect not in by_dialect:
        covered = ", ".join(sorted(COVERED_DIALECTS))
        raise UnverifiableLogicalType(
            f"Logical type {logical_type!r} has no measured physical mapping on "
            f"dialect {dialect!r}; contract types can be verified on: {covered}."
        )
    return by_dialect[dialect]


def matches(actual_physical_type: str, accepted: tuple[str, ...]) -> bool:
    """Return whether an observed physical type is one of the accepted names.

    A parameterised type matches its base name — ``decimal(10,2)`` satisfies
    ``decimal`` — but only on a parenthesis boundary. A bare ``startswith`` would
    let ``int`` match ``interval``, which is how a type check ends up agreeing
    with a column it never looked at properly.
    """
    observed = (actual_physical_type or "").strip().lower()
    return any(
        observed == candidate or observed.startswith(f"{candidate}(")
        for candidate in accepted
    )
