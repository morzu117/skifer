"""
SQL compiler — turns a parsed pipeline schema into a single SELECT statement (Plan 28).

Materialized views are defined by a SQL query, not by a DataFrame, so the
declarative part of a schema (tables, filters, joins, projections, aggregations)
is compiled here into SQL. Pure Python: no Spark import, no catalog access.

Anything that cannot be expressed faithfully in SQL raises
:class:`SqlCompilationError` rather than emitting approximate SQL — Python
``business_rules`` above all, but also the operations whose DataFrame semantics
have no portable SQL equivalent (see ``_reject_uncompilable``).

The dispatch tables below mirror ``spark_backend._SPARK_FILTER_DISPATCH`` and
``_SPARK_OP_DISPATCH`` one-for-one; a drift-guard test keeps the key sets equal.
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
from typing import Any, Callable

from skifer.core.capability_matrix import Refused, Status, capability
from skifer.core.dialect import TARGETS, DialectError, render
from skifer.core.ir import ParsedColumnSpec, ParsedOp, ParsedSchema, _parse_op
from skifer.core.op_catalog import AGGREGATE_FUNCTIONS, resolve_filter_operator

logger = logging.getLogger(__name__)


class SqlCompilationError(ValueError):
    """Raised when a schema cannot be compiled to SQL faithfully."""


#: Canonical join type → SQL keyword.
_SQL_JOIN_TYPES: dict[str, str] = {
    "inner": "INNER JOIN",
    "left": "LEFT JOIN",
    "right": "RIGHT JOIN",
    "full": "FULL OUTER JOIN",
    "cross": "CROSS JOIN",
    "left_anti": "LEFT ANTI JOIN",
    "left_semi": "LEFT SEMI JOIN",
}

#: Sub-select alias for the pre-aggregation projection.
_AGG_SOURCE_ALIAS = "_skifer_src"

#: Materialization keys that are part of the *definition* of a materialized view.
#: A change to any of them must trigger CREATE OR REPLACE, so they are hashed
#: alongside the compiled SELECT. ``refresh`` is deliberately excluded: it drives
#: what the engine does at run time, not what the view is.
_MV_DEFINITION_KEYS: tuple[str, ...] = ("schedule", "comment", "cluster_by", "partition_by")


# ---------------------------------------------------------------------------
# Quoting / literals
# ---------------------------------------------------------------------------

def quote_ident(name: str) -> str:
    """Backtick-quote an identifier, escaping embedded backticks."""
    return "`" + str(name).replace("`", "``") + "`"


#: An already-quoted name is accepted only whole (Plan 47.2): one to three back-quoted
#: parts separated by dots, no backtick or NUL inside a part — what core/sandbox.py emits.
_QUOTED_FQN_RE = re.compile(r"`[^`\x00]+`(?:\.`[^`\x00]+`){0,2}")


def quote_fqn(fqn: str) -> str:
    """Quote a possibly dotted table name, leaving already-quoted names alone."""
    if "`" in fqn:
        if _QUOTED_FQN_RE.fullmatch(fqn) is None:
            raise SqlCompilationError(
                f"Malformed table name {fqn!r}: a back-quoted name must be one to three "
                "`part`s separated by dots, with no backtick or NUL inside a part."
            )
        return fqn
    if "" in fqn.split("."):
        raise SqlCompilationError(f"Malformed table name {fqn!r}: empty name part.")
    return ".".join(quote_ident(part) for part in fqn.split("."))


def escape_sql_string(value: str) -> str:
    """Escape a string for embedding inside single quotes in Spark SQL.

    Doubling the quote alone is NOT enough: with Spark's default
    ``spark.sql.parser.escapedStringLiterals=false`` a backslash escapes the
    next character, so a value ending in ``\\`` turns the closing quote into an
    escaped quote and lets everything after it be parsed as SQL. The backslash
    must be doubled first, or doubling the quotes would itself be escaped.
    """
    return str(value).replace("\\", "\\\\").replace("'", "''")


def sql_literal(value: Any) -> str:
    """Render a Python value as a SQL literal (strings are quoted, never interpolated raw)."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return repr(value)
    return "'" + escape_sql_string(value) + "'"


def smart_literal(val_str: str) -> str:
    """Twin of ``spark_backend._smart_lit_spark``: numeric-looking strings stay numeric."""
    try:
        if "." in str(val_str):
            return repr(float(val_str))
        return repr(int(val_str))
    except (ValueError, TypeError):
        return sql_literal(val_str)


def _value_list(value: Any) -> list:
    """Twin of ``spark_backend._isin_list``."""
    if isinstance(value, list):
        return value
    return [v.strip() for v in str(value).replace(";", ",").split(",")]


def _in_list_sql(value: Any) -> str:
    return ", ".join(sql_literal(v) for v in _value_list(value))


def _between_sql(c: str, f: Any, negate: bool) -> str:
    vals = list(f.value) if isinstance(f.value, (list, tuple)) else _value_list(f.value)
    if len(vals) != 2:
        op_name = "not_between" if negate else "between"
        raise SqlCompilationError(
            f"Filter on column '{f.column}': {op_name} operator expects exactly 2 values "
            f"(lo,hi); got {len(vals)}."
        )
    lo, hi = sql_literal(vals[0]), sql_literal(vals[1])
    return f"({c} < {lo} OR {c} > {hi})" if negate else f"({c} >= {lo} AND {c} <= {hi})"


# ---------------------------------------------------------------------------
# Cast types — closed grammar (Plan 47.1)
# ---------------------------------------------------------------------------

#: Scalar type names a ``cast:`` may name, matched case-insensitively.
CAST_SCALAR_TYPES: tuple[str, ...] = (
    "string", "varchar", "char", "int", "integer", "bigint", "long", "smallint", "short",
    "tinyint", "byte", "double", "float", "real", "decimal", "numeric", "dec", "boolean",
    "bool", "date", "timestamp", "timestamp_ntz", "datetime", "binary",
)
#: The only scalars that take a ``(p)`` or ``(p,s)`` precision.
_CAST_PRECISION_TYPES = frozenset({"decimal", "numeric", "dec", "varchar", "char"})
#: Type levels a cast may nest (``array<array<array<int>>>`` is 4).
_CAST_MAX_DEPTH = 4
_CAST_FIELD_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_CAST_PUNCTUATION = frozenset("<>(),:")
_CAST_ALLOWED = (
    f"scalar {', '.join(CAST_SCALAR_TYPES)} (case-insensitive; precision (p) or (p,s) only "
    "on decimal, numeric, dec, varchar, char), or array<T>, map<K,V>, struct<name:T,...> "
    f"nested at most {_CAST_MAX_DEPTH} levels"
)


class _CastTypeParser:
    """Recursive descent over the cast grammar; renders from tokens, never from the input."""

    def __init__(self, text: str):
        self.text = text
        self.tokens = self._tokenize(text)
        self.pos = 0

    def error(self, detail: str) -> SqlCompilationError:
        return SqlCompilationError(
            f"cast: type {self.text!r} is not an allowed cast type ({detail}). "
            f"Allowed: {_CAST_ALLOWED}."
        )

    def _tokenize(self, text: str) -> list[str]:
        tokens: list[str] = []
        i = 0
        while i < len(text):
            ch = text[i]
            if ch in (" ", "\t"):
                i += 1
            elif ch in _CAST_PUNCTUATION:
                tokens.append(ch)
                i += 1
            elif ch.isascii() and (ch.isalnum() or ch == "_"):
                j = i + 1
                while j < len(text) and text[j].isascii() and (text[j].isalnum() or text[j] == "_"):
                    j += 1
                tokens.append(text[i:j])
                i = j
            else:
                raise self.error(f"unexpected character {ch!r}")
        return tokens

    def peek(self) -> str | None:
        return self.tokens[self.pos] if self.pos < len(self.tokens) else None

    def take(self, expected: str | None = None) -> str:
        token = self.peek()
        if token is None:
            raise self.error("unexpected end" + (f", expected {expected!r}" if expected else ""))
        if expected is not None and token != expected:
            raise self.error(f"expected {expected!r}, got {token!r}")
        self.pos += 1
        return token

    def integer(self) -> int:
        token = self.take()
        if not token.isdigit():
            raise self.error(f"precision must be an integer, got {token!r}")
        return int(token)

    def parse(self) -> str:
        rendered = self.type_(1)
        if self.peek() is not None:
            raise self.error(f"unexpected {self.peek()!r} after the type")
        return rendered

    def type_(self, depth: int) -> str:
        if depth > _CAST_MAX_DEPTH:
            raise self.error(f"nested deeper than {_CAST_MAX_DEPTH} levels")
        name = self.take()
        lower = name.lower()
        if not _CAST_FIELD_NAME_RE.match(name):
            raise self.error(f"{name!r} is not a type name")
        if lower == "array":
            self.take("<")
            inner = self.type_(depth + 1)
            self.take(">")
            return f"{name}<{inner}>"
        if lower == "map":
            self.take("<")
            key = self.type_(depth + 1)
            self.take(",")
            value = self.type_(depth + 1)
            self.take(">")
            return f"{name}<{key},{value}>"
        if lower == "struct":
            self.take("<")
            fields = []
            while True:
                field_name = self.take()
                if not _CAST_FIELD_NAME_RE.match(field_name):
                    raise self.error(f"struct field name {field_name!r} is not an identifier")
                self.take(":")
                fields.append(f"{field_name}:{self.type_(depth + 1)}")
                if self.peek() != ",":
                    break
                self.take(",")
            self.take(">")
            return f"{name}<{','.join(fields)}>"
        if lower not in CAST_SCALAR_TYPES:
            raise self.error(f"unknown type name {name!r}")
        # Twin of the historical mapping: Spark has no ``datetime`` type.
        rendered = "timestamp" if lower in ("datetime", "timestamp") else name
        if self.peek() == "(":
            if lower not in _CAST_PRECISION_TYPES:
                raise self.error(f"{name!r} takes no precision")
            self.take("(")
            precision = [self.integer()]
            if self.peek() == ",":
                self.take(",")
                precision.append(self.integer())
            self.take(")")
            rendered += "(" + ",".join(str(p) for p in precision) + ")"
        return rendered


def validate_cast_type(text: Any) -> str:
    """Return the SQL spelling of a ``cast:`` type, or raise ``SqlCompilationError``.

    Accepts exactly a closed list of scalar names (with an integer precision on the
    decimal / character family) and ``array<T>`` / ``map<K,V>`` / ``struct<name:T,...>``
    nested at most four levels. The output is rebuilt from the parsed tokens, so nothing
    from the input reaches the SQL unless the grammar accepted it.
    """
    return _CastTypeParser(str(text)).parse()


# ---------------------------------------------------------------------------
# Dispatch tables — mirrors of the Spark ones
# ---------------------------------------------------------------------------

# Filter dispatch: (c: str, f: ParsedFilter) -> str (boolean SQL)
_SQL_FILTER_DISPATCH: dict = {
    "is_not_null": lambda c, f: f"{c} IS NOT NULL",
    "is_null":     lambda c, f: f"{c} IS NULL",
    "equals":      lambda c, f: f"{c} = {sql_literal(f.value)}",
    "not_equals":  lambda c, f: f"{c} != {sql_literal(f.value)}",
    "greater_than": lambda c, f: f"{c} > {sql_literal(f.value)}",
    "less_than":    lambda c, f: f"{c} < {sql_literal(f.value)}",
    "greater_than_equal": lambda c, f: f"{c} >= {sql_literal(f.value)}",
    "less_than_equal":    lambda c, f: f"{c} <= {sql_literal(f.value)}",
    "contains":    lambda c, f: f"{c} LIKE {sql_literal('%' + str(f.value) + '%')}",
    "not_contains": lambda c, f: f"NOT ({c} LIKE {sql_literal('%' + str(f.value) + '%')})",
    "starts_with": lambda c, f: f"{c} LIKE {sql_literal(str(f.value) + '%')}",
    "ends_with":   lambda c, f: f"{c} LIKE {sql_literal('%' + str(f.value))}",
    "like":        lambda c, f: f"{c} LIKE {sql_literal(f.value)}",
    "not_like":    lambda c, f: f"NOT ({c} LIKE {sql_literal(f.value)})",
    "in":          lambda c, f: f"{c} IN ({_in_list_sql(f.value)})",
    "not_in":      lambda c, f: f"{c} NOT IN ({_in_list_sql(f.value)})",
    "between":     lambda c, f: _between_sql(c, f, negate=False),
    "not_between": lambda c, f: _between_sql(c, f, negate=True),
}

# Op dispatch: (c: str, op: ParsedOp) -> str (value SQL)
_SQL_OP_DISPATCH: dict = {
    "cast":      lambda c, op: f"CAST({c} AS {validate_cast_type(','.join(map(str, op.args)))})",
    "upper":     lambda c, op: f"UPPER({c})",
    "lower":     lambda c, op: f"LOWER({c})",
    "trim":      lambda c, op: f"TRIM({c})",
    "round":     lambda c, op: f"ROUND({c}, {int(op.args[0])})",
    "abs":       lambda c, op: f"ABS({c})",
    "ceil":      lambda c, op: f"CEIL({c})",
    "length":    lambda c, op: f"LENGTH({c})",
    "to_date":   lambda c, op: f"TO_DATE({c}, {sql_literal(op.args[0])})",
    "split":     lambda c, op: f"SPLIT({c}, {sql_literal(op.args[0])})[{int(op.args[1])}]",
    "substring": lambda c, op: f"SUBSTRING({c}, {int(op.args[0])}, {int(op.args[1])})",
    "coalesce":  lambda c, op: f"COALESCE({c}, {smart_literal(','.join(map(str, op.args)))})",
    "nvl":       lambda c, op: f"COALESCE({c}, {smart_literal(','.join(map(str, op.args)))})",
    "col":       lambda c, op: quote_ident(op.args[0]) if op.args else c,
    "lit":       lambda c, op: smart_literal(",".join(map(str, op.args))),
}


# ---------------------------------------------------------------------------
# Expression compilation
# ---------------------------------------------------------------------------

def compile_filter(f: Any, allow_raw_sql: bool = True) -> str:
    """Compile a ParsedFilter to a boolean SQL expression."""
    canonical = resolve_filter_operator(f.operator) or f.operator
    c = quote_ident(f.column)

    if canonical == "sql":
        if not allow_raw_sql:
            raise SqlCompilationError(
                "[Governance] sql: filter operator is disabled in the current environment "
                "(allow_raw_sql: false)."
            )
        return f"({f.value})"

    handler = _SQL_FILTER_DISPATCH.get(canonical)
    if handler is None:
        raise SqlCompilationError(
            f"Filter operator '{f.operator}' cannot be compiled to SQL. "
            f"Supported operators: {sorted(_SQL_FILTER_DISPATCH)} + 'sql'."
        )
    return handler(c, f)


def compile_op(c: str | None, op: ParsedOp, allow_raw_sql: bool = True) -> str:
    """Apply one ParsedOp to a SQL expression string (twin of ``SparkBackend.apply_op``)."""
    from skifer.core.ir import ParsedFilter

    name = op.name
    raw_arg = ",".join(str(a) for a in op.args)

    if name == "when":
        inner_str = raw_arg if op.args else "is_not_null"
        parts = inner_str.split(":", 1)
        inner_op = resolve_filter_operator(parts[0]) or parts[0]
        inner_val = parts[1] if len(parts) > 1 else None
        handler = _SQL_FILTER_DISPATCH.get(inner_op)
        if handler is None:
            raise SqlCompilationError(f"Unknown when: condition '{parts[0]}'.")
        return handler(c, ParsedFilter(column="__", operator=inner_op, value=inner_val))

    if name in ("then", "else"):
        inner = _parse_op(raw_arg) if op.args else ParsedOp("col", ())
        return compile_op(c, inner, allow_raw_sql)

    if name == "expr":
        if not allow_raw_sql:
            raise SqlCompilationError(
                "[Governance] expr: operation is disabled in the current environment "
                "(allow_raw_sql: false)."
            )
        return f"({raw_arg})"

    handler = _SQL_OP_DISPATCH.get(name)
    if handler is None:
        raise SqlCompilationError(
            f"Column operation '{name}' cannot be compiled to SQL. "
            f"Supported ops: {sorted(_SQL_OP_DISPATCH)} + 'expr'/'when'/'then'/'else'."
        )
    return handler(c, op)


def compile_column_spec(spec: ParsedColumnSpec, allow_raw_sql: bool = True) -> str:
    """Compile one select_final / add_columns / fields entry to ``<expr> AS `target```."""
    c = quote_ident(spec.source) if spec.source else None

    if spec.is_conditional:
        branches = [
            f"WHEN {compile_op(c, clause.condition, allow_raw_sql)} "
            f"THEN {compile_op(c, clause.then, allow_raw_sql)}"
            for clause in spec.when_chain
        ]
        otherwise = compile_op(c, spec.otherwise, allow_raw_sql) if spec.otherwise else "NULL"
        expr = "CASE " + " ".join(branches) + f" ELSE {otherwise} END"
    else:
        expr = c
        for op in spec.ops:
            expr = compile_op(expr, op, allow_raw_sql)
        if expr is None:
            raise SqlCompilationError(
                f"Column '{spec.target}' has neither a source column nor any operation."
            )
    return f"{expr} AS {quote_ident(spec.target)}"


def compile_measure(measure: Any) -> str:
    """Compile one ParsedMeasure to ``FUNC(col) AS `target```."""
    spec = AGGREGATE_FUNCTIONS.get(measure.func)
    if spec is None:
        raise SqlCompilationError(f"Unknown aggregate function '{measure.func}'.")
    if measure.source == "*":
        inner = "*"
    else:
        inner = ("DISTINCT " if spec.distinct else "") + quote_ident(measure.source)
    return f"{spec.sql_func}({inner}) AS {quote_ident(measure.target)}"


# ---------------------------------------------------------------------------
# Guards — never emit approximate SQL
# ---------------------------------------------------------------------------

def _reject_uncompilable(parsed: ParsedSchema) -> None:
    """Raise SqlCompilationError for every construct with no faithful SQL form."""
    if parsed.business_rules:
        raise SqlCompilationError(
            f"Python business_rules {list(parsed.business_rules)} cannot be compiled to SQL. "
            "Materialize them upstream in a silver table, then join/aggregate that table here."
        )
    if parsed.partials:
        raise SqlCompilationError(
            "'partials:' sub-transformations cannot be compiled to SQL (they run Python). "
            "Materialize the partial as its own table and reference it in 'tables:'."
        )
    if parsed.dev_limit:
        raise SqlCompilationError(
            "schema-level 'dev_limit' cannot be compiled to SQL — a frozen LIMIT in a "
            "persisted definition silently truncates the result."
        )

    for t in parsed.tables:
        label = t.alias or t.name
        if t.is_loader:
            raise SqlCompilationError(
                f"table '{label}': Python loaders cannot be compiled to SQL. "
                "Ingest into a catalog table first, then reference that table."
            )
        if t.source_type:
            raise SqlCompilationError(
                f"table '{label}': file sources ('source.type: {t.source_type}') cannot be "
                "compiled to SQL — reference a Unity Catalog table instead (ingest in bronze first)."
            )
        if t.streaming:
            raise SqlCompilationError(
                f"table '{label}': streaming reads have no SQL equivalent here — "
                "use 'materialization: streaming_table' for incremental append."
            )
        if t.dev_limit:
            raise SqlCompilationError(
                f"table '{label}': 'dev_limit' cannot be compiled to SQL — a frozen LIMIT "
                "in a persisted definition silently truncates the result."
            )
        if t.drop_duplicates_on:
            raise SqlCompilationError(
                f"table '{label}': 'quality_checks.drop_duplicates_on' cannot be compiled to "
                "SQL faithfully (it needs a windowed row_number with a deterministic ORDER BY). "
                "Deduplicate upstream, or express it declaratively with an 'aggregate:' block "
                f"grouping on {t.drop_duplicates_on}."
            )

    # ``preprocess`` is not carried by the IR — inspect the raw schema for it.
    for t in parsed.raw.get("tables", []):
        if "qualify" in (t.get("preprocess") or {}):
            label = t.get("alias") or t.get("name", "?")
            raise SqlCompilationError(
                f"table '{label}': 'preprocess.qualify' cannot be compiled to SQL faithfully "
                "(row_number windows require dropping the helper column, which needs the full "
                "column list). Materialize the qualified result upstream instead."
            )


# ---------------------------------------------------------------------------
# Compilation
# ---------------------------------------------------------------------------

def _compile_table_cte(table: Any, resolve_table: Callable[[str], str], allow_raw_sql: bool) -> str:
    """Compile one source table (projection + all its predicates) to a CTE body."""
    if table.fields:
        projection = ", ".join(compile_column_spec(f, allow_raw_sql) for f in table.fields)
    else:
        projection = "*"

    predicates = [compile_filter(f, allow_raw_sql) for f in table.filters]
    predicates += [f"{quote_ident(col)} IS NOT NULL" for col in table.drop_nulls_in]

    group_predicates = []
    for group in table.filter_groups:
        conds = [compile_filter(f, allow_raw_sql) for f in group]
        if conds:
            group_predicates.append("(" + " AND ".join(conds) + ")")
    if group_predicates:
        predicates.append("(" + " OR ".join(group_predicates) + ")")

    sql = f"SELECT {projection} FROM {quote_fqn(resolve_table(table.name))}"
    if predicates:
        sql += "\n  WHERE " + " AND ".join(predicates)
    return sql


def _compile_where_clause(parsed: ParsedSchema, allow_raw_sql: bool) -> str:
    """Compile the post-join 'where:'/'where_groups:' predicates (Plan 43).

    Mirrors ``_compile_table_cte``'s predicate handling, but against the
    joined row instead of a single table — these predicates are never pushed
    down into a table's CTE (see ``ParsedSchema.where`` docstring).
    """
    predicates = [compile_filter(f, allow_raw_sql) for f in parsed.where]

    group_predicates = []
    for group in parsed.where_groups:
        conds = [compile_filter(f, allow_raw_sql) for f in group]
        if conds:
            group_predicates.append("(" + " AND ".join(conds) + ")")
    if group_predicates:
        predicates.append("(" + " OR ".join(group_predicates) + ")")

    return ("\n  WHERE " + " AND ".join(predicates)) if predicates else ""


def _compile_join_tree(parsed: ParsedSchema) -> str:
    """Compile the FROM clause: base alias followed by each join in declaration order."""
    aliases = [t.alias for t in parsed.tables]
    if not aliases:
        raise SqlCompilationError("No tables declared — nothing to compile.")

    base_alias = parsed.joins[0].alias_left if parsed.joins else aliases[0]
    if base_alias not in aliases:
        base_alias = aliases[0]

    # Plan 47.3: a declared table no join references would be silently ignored.
    joined = {j.alias_left for j in parsed.joins} | {j.alias_right for j in parsed.joins}
    unjoined = [a for a in aliases if a != base_alias and a not in joined]
    if len(aliases) > 1 and unjoined:
        raise SqlCompilationError(
            f"Table(s) {', '.join(repr(a) for a in unjoined)} declared in 'tables:' but never "
            "joined — the compiled SQL would silently ignore them. Join each one in 'join:' "
            "(table_from / table_to) or remove it from 'tables:'."
        )

    sql = quote_ident(base_alias)
    for j in parsed.joins:
        keyword = _SQL_JOIN_TYPES.get(j.join_type)
        if keyword is None:
            raise SqlCompilationError(
                f"Join type '{j.join_type}' cannot be compiled to SQL. "
                f"Supported: {sorted(_SQL_JOIN_TYPES)}."
            )
        right = quote_ident(j.alias_right)
        if j.join_type == "cross":
            sql += f"\n  {keyword} {right}"
            continue
        if j.keys_left == j.keys_right:
            using = ", ".join(quote_ident(k) for k in j.keys_left)
            sql += f"\n  {keyword} {right} USING ({using})"
        else:
            conds = " AND ".join(
                f"{quote_ident(j.alias_left)}.{quote_ident(kl)} = {right}.{quote_ident(kr)}"
                for kl, kr in zip(j.keys_left, j.keys_right)
            )
            sql += f"\n  {keyword} {right} ON {conds}"
    return sql


#: Plan 47.5 — filters whose text value is compared to the column, and filters rendered
#: to ``LIKE``. A value of one of these shapes behaves differently across engines.
_COMPARISON_FILTERS = frozenset({
    "equals", "not_equals", "greater_than", "less_than", "greater_than_equal",
    "less_than_equal", "between", "not_between", "in", "not_in",
})
_LIST_FILTERS = frozenset({"between", "not_between", "in", "not_in"})
_LIKE_FILTERS = frozenset({
    "like", "not_like", "contains", "not_contains", "starts_with", "ends_with",
})
_DECIMAL_TEXT_RE = re.compile(r"-?\d+\.\d+")


def _value_capabilities(operator: str, value: Any) -> set[str]:
    """Return the ``literal:`` keys a filter value triggers (Plan 47.5)."""
    if value is None:
        return set()
    if operator in _COMPARISON_FILTERS:
        if isinstance(value, (list, tuple)):
            values = list(value)
        elif operator in _LIST_FILTERS:
            values = _value_list(value)
        else:
            values = [value]
        if any(isinstance(v, str) and _DECIMAL_TEXT_RE.fullmatch(v) for v in values):
            return {"literal:decimal_text"}
    elif operator in _LIKE_FILTERS and "\\" in str(value):
        return {"literal:like_backslash"}
    return set()


def used_capabilities(parsed: ParsedSchema) -> set[str]:
    """Return canonical operation keys used by a pipeline, including nested ops."""
    keys: set[str] = set()

    def filters(items):
        for item in items:
            canonical = resolve_filter_operator(item.operator) or item.operator
            keys.add(f"filter:{canonical}")
            keys.update(_value_capabilities(canonical, item.value))

    def operation(op):
        keys.add(f"op:{op.name}")
        raw_arg = ",".join(str(arg) for arg in op.args)
        if op.name == "when":
            inner = raw_arg.split(":", 1)[0] if op.args else "is_not_null"
            keys.add(f"filter:{resolve_filter_operator(inner) or inner}")
            if ":" in raw_arg:
                keys.update(_value_capabilities(
                    resolve_filter_operator(inner) or inner, raw_arg.split(":", 1)[1]
                ))
        elif op.name in ("then", "else"):
            operation(_parse_op(raw_arg) if op.args else ParsedOp("col", ()))

    def columns(items):
        for spec in items:
            if spec.is_conditional:
                for clause in spec.when_chain:
                    operation(clause.condition)
                    keys.add("op:then")
                    operation(clause.then)
                if spec.otherwise:
                    keys.add("op:else")
                    operation(spec.otherwise)
            else:
                for op in spec.ops:
                    operation(op)

    for table in parsed.tables:
        filters(table.filters)
        for group in table.filter_groups:
            filters(group)
        if table.drop_nulls_in:
            keys.add("filter:is_not_null")
        columns(table.fields)
    filters(parsed.where)
    for group in parsed.where_groups:
        filters(group)
    columns(parsed.select_final)
    columns(parsed.add_columns)
    if parsed.aggregate:
        filters(parsed.aggregate.having)
        keys.update(f"agg:{measure.func}" for measure in parsed.aggregate.measures)
    keys.update(f"join:{join.join_type}" for join in parsed.joins)
    return keys


def capability_report(parsed: ParsedSchema, dialect: str) -> dict[str, Status]:
    """Return target statuses for used capabilities, ordered by canonical key."""
    if dialect not in TARGETS:
        raise SqlCompilationError(f"Unknown SQL dialect {dialect!r}; expected one of {TARGETS}.")
    try:
        return {key: capability(dialect, key) for key in sorted(used_capabilities(parsed))}
    except KeyError as exc:
        raise SqlCompilationError(str(exc)) from exc


def compile_select(
    parsed: ParsedSchema,
    resolve_table: Callable[[str], str] | None = None,
    allow_raw_sql: bool = True,
    *,
    dialect: str = "databricks",
) -> str:
    """
    Compile a parsed schema into a single SELECT statement.

    Args:
        parsed:        The schema IR (``parse_to_ir(schema_dict)``).
        resolve_table: Maps a declared table name to its actual FQN (sandbox
                       resolution). Defaults to identity.
        allow_raw_sql: When False, ``expr:`` and the ``sql`` filter operator raise.
        dialect:       Target SQL dialect; refused capabilities raise, conditional
                       capabilities render without implying executed parity.

    Returns:
        A ``WITH … SELECT …`` statement, without trailing semicolon.

    Raises:
        SqlCompilationError: when the schema uses a construct with no faithful
                             SQL equivalent (Python rules, loaders, file sources…).
    """
    _reject_uncompilable(parsed)
    report = capability_report(parsed, dialect)
    refused = [
        f"{key!r}: {status.reason}" for key, status in report.items()
        if isinstance(status, Refused)
    ]
    if refused:
        raise SqlCompilationError(f"Capabilities refused for {dialect!r}: " + "; ".join(refused))
    resolve = resolve_table or (lambda name: name)

    ctes = ",\n".join(
        f"{quote_ident(t.alias)} AS (\n  {_compile_table_cte(t, resolve, allow_raw_sql)}\n)"
        for t in parsed.tables
    )
    join_tree = _compile_join_tree(parsed)
    add_columns = [compile_column_spec(f, allow_raw_sql) for f in parsed.add_columns]
    where_clause = _compile_where_clause(parsed, allow_raw_sql)

    if parsed.aggregate:
        agg = parsed.aggregate
        inner_projection = ", ".join(["*"] + add_columns)
        inner = f"SELECT {inner_projection}\n  FROM {join_tree}{where_clause}"
        outputs = [quote_ident(k) for k in agg.group_by]
        outputs += [compile_measure(m) for m in agg.measures]
        body = (
            f"SELECT {', '.join(outputs)}\nFROM (\n  {inner}\n) AS {quote_ident(_AGG_SOURCE_ALIAS)}"
            f"\nGROUP BY {', '.join(quote_ident(k) for k in agg.group_by)}"
        )
        if agg.having:
            body += "\nHAVING " + " AND ".join(compile_filter(h, allow_raw_sql) for h in agg.having)
    elif parsed.select_final:
        projection = ", ".join(compile_column_spec(f, allow_raw_sql) for f in parsed.select_final)
        body = f"SELECT {projection}\nFROM {join_tree}{where_clause}"
    else:
        # keep_all_columns (or neither) — pass every column through, plus add_columns.
        projection = ", ".join(["*"] + add_columns)
        body = f"SELECT {projection}\nFROM {join_tree}{where_clause}"

    try:
        return render(f"WITH {ctes}\n{body}", dialect)
    except DialectError as exc:
        raise SqlCompilationError(str(exc)) from exc


# ---------------------------------------------------------------------------
# Materialized view DDL
# ---------------------------------------------------------------------------

def definition_hash(select_sql: str, materialization: dict | None = None) -> str:
    """
    Stable SHA-256 of everything that defines a materialized view.

    Covers the compiled SELECT *and* the definition-bearing materialization
    options (schedule, comment, clustering), so editing the YAML schedule is
    detected as a definition change just like editing a filter. Stored in
    ``TBLPROPERTIES`` and compared on the next run (Plan 28, decision #15).
    """
    options = {
        key: (materialization or {}).get(key)
        for key in _MV_DEFINITION_KEYS
        if (materialization or {}).get(key) is not None
    }
    payload = json.dumps(
        {"select": select_sql, "options": options}, sort_keys=True, default=str
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def compile_materialized_view_ddl(
    fqn: str,
    select_sql: str,
    materialization: dict | None = None,
    definition_hash_value: str | None = None,
    or_replace: bool = False,
) -> str:
    """
    Assemble the ``CREATE [OR REPLACE] MATERIALIZED VIEW … AS <select>`` statement.

    Clause order follows the Databricks grammar: clustering, COMMENT,
    TBLPROPERTIES, SCHEDULE, then the query.

    Args:
        fqn:                   Target name (quoted if it is not already).
        select_sql:            The compiled SELECT (from :func:`compile_select`).
        materialization:       The normalized ``materialization:`` block.
        definition_hash_value: Hash to persist in TBLPROPERTIES; omitted when None.
        or_replace:            Emit ``CREATE OR REPLACE`` (definition changed).
    """
    from skifer.core.constants import MV_DEFINITION_HASH_PROPERTY

    mat = materialization or {}
    head = "CREATE OR REPLACE MATERIALIZED VIEW" if or_replace else "CREATE MATERIALIZED VIEW"
    lines = [f"{head} {quote_fqn(fqn)}"]

    cluster_by = mat.get("cluster_by")
    partition_by = mat.get("partition_by")
    if cluster_by:
        lines.append("  CLUSTER BY (" + ", ".join(quote_ident(c) for c in cluster_by) + ")")
    elif partition_by:
        lines.append("  PARTITIONED BY (" + ", ".join(quote_ident(c) for c in partition_by) + ")")

    if mat.get("comment"):
        lines.append(f"  COMMENT {sql_literal(mat['comment'])}")

    if definition_hash_value:
        lines.append(
            f"  TBLPROPERTIES ({sql_literal(MV_DEFINITION_HASH_PROPERTY)} = "
            f"{sql_literal(definition_hash_value)})"
        )

    if mat.get("schedule"):
        lines.append(f"  SCHEDULE {mat['schedule']}")

    lines.append("AS")
    lines.append(select_sql)
    return "\n".join(lines)
