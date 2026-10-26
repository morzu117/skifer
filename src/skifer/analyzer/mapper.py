"""Deterministic mapping from a parsed SQL statement to a Skifer schema draft
(Plan 43 — 43.2).

Structural decisions only — no LLM here. A statement either maps cleanly (with
zero or more non-blocking, informational residues) or is marked
``needs_curation`` and is not emitted at all: CTEs, set operations and
subqueries in SELECT require the DAG/splitting work of 43.3 and are explicitly
out of this module's scope. A construct the catalog cannot express falls back
to an ``expr:`` escape hatch per D7, rather than blocking the whole statement.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from skifer.analyzer.parser import Statement

# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Residue:
    """One thing the mapper could not translate faithfully, or flags for review.

    ``blocking=True`` means the whole statement is not emitted (``needs_curation``).
    A non-blocking residue still produces a draft schema, surfaced in the report
    for a human to check (D7).
    """

    code: str
    message: str
    blocking: bool = False


@dataclass
class MappedStatement:
    """The mapper's verdict for one SQL statement."""

    source_path: str
    line: int
    target: str | None
    schema_dict: dict | None
    residues: list[Residue] = field(default_factory=list)

    @property
    def needs_curation(self) -> bool:
        return self.schema_dict is None

    @property
    def blocking_residues(self) -> list[Residue]:
        return [r for r in self.residues if r.blocking]


# ---------------------------------------------------------------------------
# Comparison-operator and scalar-function catalogs (sqlglot -> Skifer canonical)
# ---------------------------------------------------------------------------

_COMPARISON_OPS: dict[str, str] = {
    "EQ": "equals",
    "NEQ": "not_equals",
    "GT": "greater_than",
    "LT": "less_than",
    "GTE": "greater_than_equal",
    "LTE": "less_than_equal",
}

_SCALAR_NOARG: dict[str, str] = {
    "Upper": "upper",
    "Lower": "lower",
    "Trim": "trim",
    "Abs": "abs",
    "Ceil": "ceil",
    "Length": "length",
}

_MAX_OR_GROUPS = 8


def map_statement(stmt: Statement) -> MappedStatement:
    """Map one parsed SQL statement to a Skifer schema draft, or ``needs_curation``."""

    target, body = _extract_target_and_body(stmt.ast)

    curation_reason = _detect_unsupported_constructs(body)
    if curation_reason is not None:
        return MappedStatement(
            source_path=stmt.source_path,
            line=stmt.line,
            target=target,
            schema_dict=None,
            residues=[curation_reason],
        )

    residues: list[Residue] = []

    def _curation(extra: Residue | None = None):
        return MappedStatement(
            source_path=stmt.source_path,
            line=stmt.line,
            target=target,
            schema_dict=None,
            residues=residues + ([extra] if extra else []),
        )

    tables, joins, nullable_aliases, consumed_where = _map_from_and_joins(body, residues)
    if tables is None:
        return _curation()  # a blocking residue was already appended describing why.

    if consumed_where:
        where_filter, where_groups, table_filters, table_filter_groups = [], [], {}, {}
    else:
        where_filter, where_groups, table_filters, table_filter_groups = _map_where(
            body, tables, nullable_aliases, residues,
        )
        if where_filter is None and where_groups is None:
            return _curation()

    for alias, filters in table_filters.items():
        tables[alias]["filter"] = filters
    for alias, groups in table_filter_groups.items():
        tables[alias]["filter_groups"] = groups

    schema_dict: dict = {"tables": list(tables.values())}
    if joins:
        schema_dict["join"] = joins
    if where_filter:
        schema_dict["where"] = where_filter
    if where_groups:
        schema_dict["where_groups"] = where_groups

    group = body.args.get("group")
    if group is not None:
        if joins:
            return _curation(Residue(
                code="aggregate_with_join_unsupported",
                message="GROUP BY combined with a JOIN is not mapped in 43.2.",
                blocking=True,
            ))
        aggregate = _map_aggregate(body, group, residues)
        if aggregate is None:
            return _curation()
        schema_dict["aggregate"] = aggregate
        return MappedStatement(
            source_path=stmt.source_path, line=stmt.line, target=target,
            schema_dict=schema_dict, residues=residues,
        )

    select_items = _select_items(body)
    if select_items is None:
        return _curation(Residue(
            code="unsupported_select_shape",
            message="SELECT list is not a plain column/expression list this mapper understands.",
            blocking=True,
        ))

    if _is_star_only(select_items):
        if body.args.get("distinct") is not None:
            return _curation(Residue(
                code="distinct_star_unsupported",
                message="SELECT DISTINCT * cannot be pushed down without a known column list.",
                blocking=True,
            ))
        schema_dict["keep_all_columns"] = True
        return MappedStatement(
            source_path=stmt.source_path, line=stmt.line, target=target,
            schema_dict=schema_dict, residues=residues,
        )

    select_final = []
    for item in select_items:
        target_name, mapped = _map_select_item(item, residues)
        if mapped is None:
            return _curation(Residue(
                code="unsupported_select_expression",
                message=f"Could not map SELECT expression for '{target_name}'.",
                blocking=True,
            ))
        select_final.append(mapped)

    if body.args.get("distinct") is not None:
        if joins:
            return _curation(Residue(
                code="distinct_with_join_unsupported",
                message="SELECT DISTINCT combined with a JOIN has no table-level equivalent.",
                blocking=True,
            ))
        output_names = [entry[1] if isinstance(entry, list) else entry["target"] for entry in select_final]
        only_alias = next(iter(tables))
        tables[only_alias]["quality_checks"] = {"drop_duplicates_on": output_names}

    schema_dict["select_final"] = select_final
    return MappedStatement(
        source_path=stmt.source_path,
        line=stmt.line,
        target=target,
        schema_dict=schema_dict,
        residues=residues,
    )


# ---------------------------------------------------------------------------
# Target + body extraction
# ---------------------------------------------------------------------------


def _extract_target_and_body(ast):
    """Return (target_fqn_or_None, body_select_or_union)."""
    from sqlglot import exp

    if isinstance(ast, exp.Create):
        table = ast.this
        name = table.this.name if hasattr(table, "this") else None
        db = table.args.get("db")
        catalog = table.args.get("catalog")
        parts = [p.name for p in (catalog, db) if p is not None] + ([name] if name else [])
        target = ".".join(parts) if parts else None
        return target, ast.expression
    if isinstance(ast, exp.Insert):
        table = ast.this
        name = getattr(table, "name", None) or getattr(table, "this", None)
        target = name.sql() if hasattr(name, "sql") else str(name) if name else None
        return target, ast.expression
    return None, ast


def _detect_unsupported_constructs(body) -> Residue | None:
    from sqlglot import exp

    if body is None:
        return Residue(code="empty_statement", message="Statement has no SELECT body.", blocking=True)

    if isinstance(body, exp.Union):
        return Residue(
            code="union_requires_multi_schema",
            message=(
                "UNION/UNION ALL maps to multiple draft schemas consumed via "
                "run_union_sources_to_table, not one schema_dict — deferred to 43.3/43.4."
            ),
            blocking=True,
        )

    if not isinstance(body, exp.Select):
        return Residue(
            code="unsupported_statement_shape",
            message=f"Unsupported top-level statement shape: {type(body).__name__}.",
            blocking=True,
        )

    if body.args.get("with_"):
        return Residue(
            code="cte",
            message="CTEs require the DAG/splitting work of 43.3 (partials/intermediate tables).",
            blocking=True,
        )

    from_expr = body.args.get("from_")
    from_this = from_expr.this if from_expr is not None else None
    dedup = _detect_qualify_dedup(from_this, body.args.get("where"))
    if dedup is None and isinstance(from_this, exp.Subquery):
        return Residue(
            code="subquery_in_from",
            message="Derived-table subquery in FROM is not a recognized dedup pattern.",
            blocking=True,
        )

    # Any subquery elsewhere (SELECT list, WHERE) is out of scope for 43.2.
    for e in body.expressions:
        for node in e.walk():
            if isinstance(node, exp.Subquery):
                return Residue(
                    code="subquery_in_select",
                    message="Scalar/derived subqueries in SELECT require the intermediate-table work of 43.3.",
                    blocking=True,
                )
    where = body.args.get("where")
    if where is not None and dedup is None:
        for node in where.this.walk():
            if isinstance(node, exp.Subquery):
                return Residue(
                    code="subquery_in_where",
                    message="Subqueries in WHERE require the intermediate-table work of 43.3.",
                    blocking=True,
                )

    if dedup is None:
        for node in body.walk():
            if isinstance(node, exp.Window):
                return Residue(
                    code="window_function_unsupported",
                    message=(
                        f"Window function '{node.this.sql_name() if hasattr(node.this, 'sql_name') else node.this}' "
                        "has no declarative equivalent beyond the ROW_NUMBER()=1 dedup pattern."
                    ),
                    blocking=True,
                )

    return None


# ---------------------------------------------------------------------------
# ROW_NUMBER() = 1 dedup -> preprocess.qualify
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class QualifyDedup:
    base_table: str
    partition_by: list[str]
    order_by: str
    descending: bool


def _detect_qualify_dedup(from_this, where) -> QualifyDedup | None:
    """Recognize ``FROM (SELECT ..., ROW_NUMBER() OVER (...) AS rn FROM t) WHERE rn = 1``."""
    from sqlglot import exp

    if from_this is None or not isinstance(from_this, exp.Subquery):
        return None
    inner = from_this.this
    if not isinstance(inner, exp.Select) or where is None:
        return None

    predicate = where.this
    if not isinstance(predicate, exp.EQ):
        return None
    rn_ref = predicate.this
    rn_value = predicate.expression
    if not (isinstance(rn_ref, exp.Column) and isinstance(rn_value, exp.Literal) and rn_value.name == "1"):
        return None
    rn_alias = rn_ref.name

    window_node = None
    for item in inner.expressions:
        if isinstance(item, exp.Alias) and item.alias == rn_alias and isinstance(item.this, exp.Window):
            window_node = item.this
            break
    if window_node is None:
        return None
    func = window_node.this
    if not (isinstance(func, exp.Anonymous) and func.name.upper() == "ROW_NUMBER") and not (
        type(func).__name__ == "RowNumber"
    ):
        return None

    partition_cols = [c.name for c in window_node.args.get("partition_by", []) if isinstance(c, exp.Column)]
    order_exprs = window_node.args.get("order") or window_node.args.get("orderby")
    order_items = order_exprs.expressions if order_exprs is not None else []
    if len(order_items) != 1:
        return None
    order_item = order_items[0]
    order_col = order_item.this
    if not isinstance(order_col, exp.Column):
        return None

    inner_from = inner.args.get("from_")
    base_table = inner_from.this if inner_from is not None else None
    if not isinstance(base_table, exp.Table):
        return None

    return QualifyDedup(
        base_table=_table_fqn(base_table),
        partition_by=partition_cols,
        order_by=order_col.name,
        descending=bool(order_item.args.get("desc")),
    )


def _table_fqn(table) -> str:
    parts = [p.name for p in (table.args.get("catalog"), table.args.get("db")) if p is not None]
    parts.append(table.this.name)
    return ".".join(parts)


# ---------------------------------------------------------------------------
# FROM / JOIN
# ---------------------------------------------------------------------------

_JOIN_TYPE_BY_SHAPE: dict[tuple[str | None, str | None], str] = {
    (None, None): "inner",
    ("LEFT", None): "left",
    ("RIGHT", None): "right",
    ("FULL", "OUTER"): "full",
    ("FULL", None): "full",
    (None, "CROSS"): "cross",
    (None, "SEMI"): "left_semi",
    ("LEFT", "SEMI"): "left_semi",
    (None, "ANTI"): "left_anti",
    ("LEFT", "ANTI"): "left_anti",
}


def _join_type(join_node) -> str:
    side = join_node.args.get("side")
    kind = join_node.args.get("kind")
    side = side.upper() if side else None
    kind = kind.upper() if kind else None
    return _JOIN_TYPE_BY_SHAPE.get((side, kind), "inner")


def _map_from_and_joins(body, residues: list[Residue]):
    """Return (tables_by_alias, join_list, nullable_aliases, consumed_where).

    Returns (None, None, None, False) on a blocking residue (already appended).
    ``consumed_where`` is True when the WHERE clause was entirely consumed by
    a recognized FROM-level pattern (the ROW_NUMBER()=1 dedup) and must not be
    mapped again as an ordinary filter.
    """
    from sqlglot import exp

    from_expr = body.args.get("from_")
    base = from_expr.this if from_expr is not None else None

    dedup = _detect_qualify_dedup(base, body.args.get("where"))
    tables: dict[str, dict] = {}
    if dedup is not None:
        alias = _default_alias(dedup.base_table, tables)
        table_dict: dict = {"name": dedup.base_table, "alias": alias}
        qualify: dict = {}
        if dedup.partition_by:
            qualify["partition_by"] = dedup.partition_by
        qualify["order_by"] = dedup.order_by
        qualify["descending"] = dedup.descending
        table_dict["preprocess"] = {"qualify": qualify}
        tables[alias] = table_dict
        return tables, [], set(), True

    if not isinstance(base, exp.Table):
        residues.append(Residue(
            code="unsupported_from_shape",
            message=f"Unsupported FROM clause shape: {type(base).__name__ if base is not None else 'None'}.",
            blocking=True,
        ))
        return None, None, None, False

    base_fqn = _table_fqn(base)
    base_alias = base.alias or _default_alias(base_fqn, tables)
    tables[base_alias] = {"name": base_fqn, "alias": base_alias}

    joins: list[dict] = []
    nullable_aliases: set[str] = set()

    for join_node in body.args.get("joins") or []:
        right = join_node.this
        if not isinstance(right, exp.Table):
            residues.append(Residue(
                code="unsupported_join_shape",
                message="A JOIN's right side is not a plain table reference.",
                blocking=True,
            ))
            return None, None, None, False
        right_fqn = _table_fqn(right)
        right_alias = right.alias or _default_alias(right_fqn, tables)
        tables[right_alias] = {"name": right_fqn, "alias": right_alias}

        join_type = _join_type(join_node)
        if join_type == "left":
            nullable_aliases.add(right_alias)
        elif join_type == "right":
            nullable_aliases.add(base_alias)
        elif join_type == "full":
            nullable_aliases.add(base_alias)
            nullable_aliases.add(right_alias)

        using = join_node.args.get("using")
        if using:
            keys = [c.name for c in using]
            joins.append({
                "table_from": [base_alias, keys[0]] if len(keys) == 1 else [base_alias, keys],
                "table_to": [right_alias, keys[0]] if len(keys) == 1 else [right_alias, keys],
                "type": join_type,
            })
            continue

        on = join_node.args.get("on")
        pair = _map_equi_join_condition(on, base_alias, right_alias)
        if pair is None:
            residues.append(Residue(
                code="non_equi_join_unsupported",
                message=(
                    f"JOIN between '{base_alias}' and '{right_alias}' is not a plain "
                    "equality (or AND-of-equalities) join on direct columns."
                ),
                blocking=True,
            ))
            return None, None, None, False
        keys_left, keys_right = pair
        joins.append({
            "table_from": [base_alias, keys_left[0]] if len(keys_left) == 1 else [base_alias, keys_left],
            "table_to": [right_alias, keys_right[0]] if len(keys_right) == 1 else [right_alias, keys_right],
            "type": join_type,
        })

    return tables, joins, nullable_aliases, False


def _default_alias(fqn: str, existing: dict) -> str:
    base = fqn.rsplit(".", 1)[-1]
    alias = base
    i = 0
    while alias in existing:
        i += 1
        alias = f"{base}_{i}"
    return alias


def _map_equi_join_condition(on, left_alias: str, right_alias: str):
    """Return (keys_left, keys_right) for an AND-of-equalities ON clause, or None."""
    from sqlglot import exp

    if on is None:
        return None

    conjuncts = _flatten_and(on)
    keys_left: list[str] = []
    keys_right: list[str] = []
    for conjunct in conjuncts:
        if not isinstance(conjunct, exp.EQ):
            return None
        left_col, right_col = conjunct.this, conjunct.expression
        if not (isinstance(left_col, exp.Column) and isinstance(right_col, exp.Column)):
            return None
        sides = {left_col.table, right_col.table}
        if sides != {left_alias, right_alias}:
            return None
        if left_col.table == left_alias:
            keys_left.append(left_col.name)
            keys_right.append(right_col.name)
        else:
            keys_left.append(right_col.name)
            keys_right.append(left_col.name)
    if not keys_left:
        return None
    return keys_left, keys_right


def _unwrap_paren(node):
    from sqlglot import exp

    while isinstance(node, exp.Paren):
        node = node.this
    return node


def _flatten_and(node) -> list:
    from sqlglot import exp

    node = _unwrap_paren(node)
    if isinstance(node, exp.And):
        return _flatten_and(node.this) + _flatten_and(node.expression)
    return [node]


def _flatten_or(node) -> list:
    from sqlglot import exp

    node = _unwrap_paren(node)
    if isinstance(node, exp.Or):
        return _flatten_or(node.this) + _flatten_or(node.expression)
    return [node]


# ---------------------------------------------------------------------------
# SELECT list
# ---------------------------------------------------------------------------


def _select_items(body):
    return list(body.expressions)


def _is_star_only(items) -> bool:
    from sqlglot import exp

    return len(items) == 1 and isinstance(items[0], exp.Star)


def _map_select_item(item, residues: list[Residue]):
    """Return (target_name, select_final_entry) or (best_effort_name, None)."""
    from sqlglot import exp

    if isinstance(item, exp.Alias):
        target_name = item.alias
        expr = item.this
    elif isinstance(item, exp.Column):
        target_name = item.name
        expr = item
    else:
        return "?", None

    if isinstance(expr, exp.Column):
        return target_name, [expr.name, target_name, []]

    if isinstance(expr, exp.Case):
        mapped = _map_case(expr, target_name)
        if mapped is not None:
            return target_name, mapped
        return _expr_fallback(expr, target_name, residues)

    chain = _map_scalar_chain(expr)
    if chain is not None:
        source_col, ops = chain
        return target_name, [source_col, target_name, ops]

    if isinstance(expr, exp.Literal):
        return target_name, [None, target_name, [f"lit:{_literal_text(expr)}"]]

    return _expr_fallback(expr, target_name, residues)


def _expr_fallback(expr, target_name: str, residues: list[Residue]):
    text = _strip_qualifiers_sql(expr)
    residues.append(Residue(
        code="expr_fallback",
        message=f"'{target_name}' has no catalog equivalent; mapped via expr:{text}.",
        blocking=False,
    ))
    return target_name, [None, target_name, [f"expr:{text}"]]


def _strip_qualifiers_sql(expr) -> str:
    from sqlglot import exp

    copy = expr.copy()
    for col in copy.find_all(exp.Column):
        col.set("table", None)
    return copy.sql(dialect="databricks")


def _map_scalar_chain(expr):
    """Walk a nest of single-argument scalar ops down to a bare column.

    Returns (source_column, ops) in application order, or None if the chain
    hits anything outside the catalog (multi-column, unsupported function).
    """
    from sqlglot import exp

    ops: list[str] = []
    node = expr
    while True:
        if isinstance(node, exp.Column):
            return node.name, list(reversed(ops))

        cls_name = type(node).__name__
        if cls_name in _SCALAR_NOARG:
            ops.append(_SCALAR_NOARG[cls_name])
            node = node.this
            continue

        if isinstance(node, exp.Cast):
            type_name = node.args["to"].this.name.lower() if hasattr(node.args["to"].this, "name") else str(node.args["to"].this).lower()
            ops.append(f"cast:{type_name}")
            node = node.this
            continue

        if isinstance(node, exp.Round):
            decimals = node.args.get("decimals")
            digits = decimals.name if decimals is not None else "0"
            ops.append(f"round:{digits}")
            node = node.this
            continue

        if isinstance(node, exp.Coalesce):
            extra = node.expressions
            if len(extra) != 1:
                return None
            value = _literal_text(extra[0])
            if value is None:
                return None
            ops.append(f"coalesce:{value}")
            node = node.this
            continue

        return None


def _literal_text(node) -> str | None:
    from sqlglot import exp

    if isinstance(node, exp.Literal):
        return str(node.this)
    if isinstance(node, exp.Null):
        return None
    return None


def _map_case(expr, target_name: str):
    """Map a single-source-column CASE WHEN chain to the dict when_chain grammar."""

    ifs = expr.args.get("ifs") or []
    if not ifs:
        return None

    common_source: str | None = None
    branches: list[dict] = []
    for if_ in ifs:
        condition_str = _predicate_condition_string(if_.this)
        if condition_str is None:
            return None
        source_col, op_str = condition_str
        if common_source is None:
            common_source = source_col
        elif common_source != source_col:
            return None
        then_op = _value_to_op(if_.args.get("true"))
        if then_op is None:
            return None
        branches.append({"when": op_str, "then": then_op})

    default = expr.args.get("default")
    if default is not None:
        else_op = _value_to_op(default)
        if else_op is None:
            return None
        branches.append({"else": else_op})

    if common_source is None:
        return None
    return {"source": common_source, "target": target_name, "ops": branches}


def _value_to_op(node) -> str | None:
    from sqlglot import exp

    if node is None:
        return "lit:"
    if isinstance(node, exp.Column):
        return f"col:{node.name}"
    literal = _literal_text(node)
    if literal is not None:
        return f"lit:{literal}"
    if isinstance(node, exp.Null):
        return "lit:"
    return None


# ---------------------------------------------------------------------------
# WHERE / push-down (D6)
# ---------------------------------------------------------------------------


def _predicate_condition_string(predicate):
    """Map a single comparison predicate to (bare_column, 'operator:value'-or-'operator')."""
    from sqlglot import exp

    negate = False
    node = _unwrap_paren(predicate)
    if isinstance(node, exp.Not):
        negate = True
        node = _unwrap_paren(node.this)

    if isinstance(node, exp.Is):
        if not isinstance(node.this, exp.Column):
            return None
        is_null = isinstance(node.expression, exp.Null)
        if not is_null:
            return None
        op = "is_not_null" if negate else "is_null"
        return node.this.name, op

    if isinstance(node, exp.In):
        if not isinstance(node.this, exp.Column):
            return None
        values = [_literal_text(e) for e in node.expressions]
        if any(v is None for v in values):
            return None
        op = "not_in" if negate else "in"
        return node.this.name, f"{op}:{','.join(values)}"

    if isinstance(node, exp.Between):
        if not isinstance(node.this, exp.Column):
            return None
        low = _literal_text(node.args.get("low"))
        high = _literal_text(node.args.get("high"))
        if low is None or high is None:
            return None
        op = "not_between" if negate else "between"
        return node.this.name, f"{op}:{low},{high}"

    if isinstance(node, exp.Like):
        if not isinstance(node.this, exp.Column):
            return None
        pattern = _literal_text(node.expression)
        if pattern is None:
            return None
        op = "not_like" if negate or node.args.get("negate") else "like"
        return node.this.name, f"{op}:{pattern}"

    if type(node).__name__ in _COMPARISON_OPS:
        if negate:
            return None  # NOT EQ/NEQ/... is unusual SQL; out of scope, falls back to residue.
        if not isinstance(node.this, exp.Column):
            return None
        value = _literal_text(node.expression)
        if value is None:
            return None
        op = _COMPARISON_OPS[type(node).__name__]
        return node.this.name, f"{op}:{value}"

    return None


def _predicate_table_and_string(predicate, tables: dict, residues: list[Residue]):
    """Map a conjunct to (alias_or_None_if_ambiguous, 'col:operator[:value]') or (alias, None)."""
    from sqlglot import exp

    columns = list(predicate.find_all(exp.Column))
    aliases = {c.table for c in columns if c.table}
    if not aliases:
        # No table qualification used anywhere in the predicate: unambiguous
        # only when the query has a single source table.
        alias = next(iter(tables)) if len(tables) == 1 else None
    elif len(aliases) == 1:
        alias = next(iter(aliases))
    else:
        alias = None  # references more than one table -> never push-down-able.

    mapped = _predicate_condition_string(predicate)
    if mapped is None:
        return alias, None
    bare_col, op_str = mapped
    return alias, f"{bare_col}:{op_str}"


def _flag_not_in_nullable_pitfall(filt: str, residues: list[Residue]) -> None:
    bare_col, operator = filt.split(":", 2)[:2]
    if operator == "not_in":
        residues.append(Residue(
            code="not_in_nullable_pitfall",
            message=(
                f"'{bare_col} NOT IN (...)' silently drops rows where '{bare_col}' is NULL "
                "(tri-valued SQL logic) — verify this is intentional."
            ),
            blocking=False,
        ))


def _map_where(body, tables, nullable_aliases, residues):
    """Return (where_list, where_groups, table_filters, table_filter_groups).

    Returns (None, None, ..., ...) when a blocking residue was appended.
    """
    from sqlglot import exp

    where = body.args.get("where")
    table_filters: dict[str, list[str]] = {}
    table_filter_groups: dict[str, list[list[str]]] = {}
    where_list: list[str] = []
    where_groups: list[list[str]] = []
    or_group_already_placed = False

    if where is None:
        return where_list, where_groups, table_filters, table_filter_groups

    predicate = where.this
    top = [predicate] if isinstance(predicate, exp.Or) else _flatten_and(predicate)

    for conjunct in top:
        if isinstance(conjunct, exp.Or):
            if or_group_already_placed:
                residues.append(Residue(
                    code="multiple_or_groups_unsupported",
                    message=(
                        "More than one top-level OR group in WHERE: an AND of two "
                        "OR-of-ANDs groups cannot be represented by a single filter_groups/"
                        "where_groups block without changing its meaning."
                    ),
                    blocking=True,
                ))
                return None, None, table_filters, table_filter_groups

            or_branches = _flatten_or(conjunct)
            if len(or_branches) > _MAX_OR_GROUPS:
                residues.append(Residue(
                    code="or_group_limit_exceeded",
                    message=f"OR has more than {_MAX_OR_GROUPS} branches; not mapped structurally.",
                    blocking=True,
                ))
                return None, None, table_filters, table_filter_groups

            mapped_branches: list[list[str]] = []
            aliases_seen: set[str] = set()
            for branch in or_branches:
                branch_strings = []
                for bc in _flatten_and(branch):
                    alias, filt = _predicate_table_and_string(bc, tables, residues)
                    if filt is None:
                        residues.append(Residue(
                            code="unsupported_predicate",
                            message=f"Could not map predicate in OR group: {bc.sql(dialect='databricks')}.",
                            blocking=True,
                        ))
                        return None, None, table_filters, table_filter_groups
                    _flag_not_in_nullable_pitfall(filt, residues)
                    branch_strings.append(filt)
                    if alias is not None:
                        aliases_seen.add(alias)
                mapped_branches.append(branch_strings)

            single_alias = next(iter(aliases_seen)) if len(aliases_seen) == 1 else None
            if single_alias is not None and single_alias in tables and single_alias not in nullable_aliases:
                table_filter_groups.setdefault(single_alias, []).extend(mapped_branches)
            else:
                where_groups.extend(mapped_branches)
            or_group_already_placed = True
            continue

        alias, filt = _predicate_table_and_string(conjunct, tables, residues)
        if filt is None:
            residues.append(Residue(
                code="unsupported_predicate",
                message=f"Could not map WHERE predicate: {conjunct.sql(dialect='databricks')}.",
                blocking=True,
            ))
            return None, None, table_filters, table_filter_groups

        _flag_not_in_nullable_pitfall(filt, residues)

        if alias is not None and alias in tables and alias not in nullable_aliases:
            table_filters.setdefault(alias, []).append(filt)
        else:
            where_list.append(filt)

    return where_list, where_groups, table_filters, table_filter_groups


# ---------------------------------------------------------------------------
# GROUP BY / HAVING -> aggregate: (Plan 28 grammar)
# ---------------------------------------------------------------------------

_AGG_CLASS_TO_FUNC: dict[str, str] = {
    "Sum": "sum",
    "Avg": "avg",
    "Min": "min",
    "Max": "max",
    "Stddev": "stddev",
    "Variance": "variance",
}


def _map_aggregate(body, group, residues: list[Residue]) -> dict | None:
    """Map a GROUP BY/HAVING statement to the declarative ``aggregate:`` block."""
    from sqlglot import exp

    if group.args.get("all") or not group.expressions:
        residues.append(Residue(
            code="group_by_all_unsupported",
            message="GROUP BY ALL/ordinal is not supported; list grouping columns explicitly.",
            blocking=True,
        ))
        return None

    group_cols: list[str] = []
    for g in group.expressions:
        if not isinstance(g, exp.Column):
            residues.append(Residue(
                code="group_by_expression_unsupported",
                message="GROUP BY on a non-column expression is not supported.",
                blocking=True,
            ))
            return None
        group_cols.append(g.name)

    measures: list[dict] = []
    measure_alias_by_text: dict[str, str] = {}
    for item in body.expressions:
        if isinstance(item, exp.Alias):
            target_name = item.alias
            expr = item.this
        elif isinstance(item, exp.Column):
            target_name = item.name
            expr = item
        else:
            residues.append(Residue(
                code="aggregate_select_item_unsupported",
                message="A GROUP BY SELECT item is neither a plain column nor an aggregate.",
                blocking=True,
            ))
            return None

        if isinstance(expr, exp.Column) and expr.name in group_cols:
            continue  # passthrough dimension — already covered by group_by.

        measure = _map_aggregate_measure(expr, target_name)
        if measure is None:
            residues.append(Residue(
                code="aggregate_select_item_unsupported",
                message=f"'{target_name}' is not a supported aggregate function.",
                blocking=True,
            ))
            return None
        measures.append(measure)
        measure_alias_by_text[_strip_qualifiers_sql(expr)] = target_name

    having_list: list[str] = []
    having_node = body.args.get("having")
    if having_node is not None:
        for conjunct in _flatten_and(having_node.this):
            mapped = _map_having_conjunct(conjunct, measure_alias_by_text)
            if mapped is None:
                residues.append(Residue(
                    code="having_predicate_unsupported",
                    message=f"Could not map HAVING predicate: {conjunct.sql(dialect='databricks')}.",
                    blocking=True,
                ))
                return None
            having_list.append(mapped)

    result: dict = {"group_by": group_cols, "measures": measures}
    if having_list:
        result["having"] = having_list
    return result


def _map_aggregate_measure(expr, target_name: str) -> dict | None:
    from sqlglot import exp

    if isinstance(expr, exp.Count):
        inner = expr.this
        if isinstance(inner, exp.Star):
            return {"source": "*", "target": target_name, "func": "count"}
        if isinstance(inner, exp.Distinct):
            cols = inner.expressions
            if len(cols) != 1 or not isinstance(cols[0], exp.Column):
                return None
            return {"source": cols[0].name, "target": target_name, "func": "count_distinct"}
        if isinstance(inner, exp.Column):
            return {"source": inner.name, "target": target_name, "func": "count"}
        return None

    cls_name = type(expr).__name__
    if cls_name in _AGG_CLASS_TO_FUNC and isinstance(expr.this, exp.Column):
        return {"source": expr.this.name, "target": target_name, "func": _AGG_CLASS_TO_FUNC[cls_name]}

    return None


def _map_having_conjunct(conjunct, measure_alias_by_text: dict[str, str]) -> str | None:
    """Map a HAVING predicate's aggregate side to its measure's output alias."""
    from sqlglot import exp

    node = _unwrap_paren(conjunct)
    left = node.this if hasattr(node, "this") else None
    if left is None or not isinstance(left, exp.Expression):
        return None

    alias = measure_alias_by_text.get(_strip_qualifiers_sql(left))
    if alias is None:
        return None

    # Re-run the generic predicate mapper as if `left` were the bare column `alias`.
    rewritten = node.copy()
    rewritten.set("this", exp.column(alias))
    mapped = _predicate_condition_string(rewritten)
    if mapped is None:
        return None
    _, op_str = mapped
    return f"{alias}:{op_str}"
