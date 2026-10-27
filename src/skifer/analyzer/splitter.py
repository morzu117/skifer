"""Splits script-level CTEs and scalar subqueries into independently mappable
units (Plan 43 — 43.3): a ``partials:`` child (read by several branches of one
script), a proposed Silver intermediate table (the same definition read by
several different scripts), or inlined directly into its one consumer.

Deterministic and structural only: classification follows occurrence counts
and content fingerprints (qualifier-stripped canonical SQL text), never an
LLM guess. A shape this module does not recognize — anything beyond a single
FROM-level CTE reference, or a scalar subquery that is not one aggregate over
one un-joined, un-grouped table — is left untouched: the original statement
keeps whatever residue 43.2's mapper already reports for it (D7: a residue
surfaces, it never silently disappears).
"""
from __future__ import annotations

import dataclasses
from dataclasses import dataclass

from skifer.analyzer.parser import Statement

_DEFAULT_INTERMEDIATE_LAYER = "silver"


@dataclass(frozen=True)
class SplitUnit:
    """One independently mappable unit produced by splitting."""

    kind: str  # "main" | "partial" | "intermediate_table"
    name: str | None  # partial alias, or intermediate table FQN; ``None`` for "main"
    statement: Statement
    path: str | None = None  # for "partial": its ``_partials/<alias>.yaml`` path
    schema_dict: dict | None = None  # for "intermediate_table" built outside map_statement


@dataclass(frozen=True)
class InlineNote:
    """A single-use CTE fused directly into its one consumer — informational."""

    cte_name: str
    source_path: str


@dataclass(frozen=True)
class SplitResult:
    units: tuple[SplitUnit, ...]
    inlined: tuple[InlineNote, ...]


# ---------------------------------------------------------------------------
# AST helpers
# ---------------------------------------------------------------------------


def _flatten_union(node) -> list:
    from sqlglot import exp

    if isinstance(node, exp.Union):
        return _flatten_union(node.this) + _flatten_union(node.expression)
    return [node]


def _with_clause(body):
    return body.args.get("with_") if hasattr(body, "args") else None


def _cte_defs(with_) -> list[tuple[str, object]]:
    return [(cte.alias_or_name, cte.this) for cte in with_.expressions]


def _fingerprint(select_ast) -> str:
    from skifer.analyzer.mapper import _strip_qualifiers_sql

    return _strip_qualifiers_sql(select_ast)


def _find_table_refs(node, name: str) -> list:
    from sqlglot import exp

    out = []
    for n in node.walk():
        n = n[0] if isinstance(n, tuple) else n
        if isinstance(n, exp.Table) and n.name == name and n.args.get("db") is None and n.args.get("catalog") is None:
            out.append(n)
    return out


def _enclosing_predicate_sql(ref) -> str | None:
    """WHERE predicate text of the nearest enclosing SELECT around *ref*."""
    from sqlglot import exp
    from skifer.analyzer.mapper import _strip_qualifiers_sql

    node = ref
    while node is not None and not isinstance(node, exp.Select):
        node = node.parent
    if node is None:
        return None
    where = node.args.get("where")
    if where is None:
        return None
    return _strip_qualifiers_sql(where.this)


def _enclosing_select(ref):
    from sqlglot import exp

    node = ref
    while node is not None and not isinstance(node, exp.Select):
        node = node.parent
    return node


def _rename_table_ref(ref, new_name: str, *, db: str | None = None) -> None:
    from sqlglot import exp

    ref.set("this", exp.to_identifier(new_name))
    ref.set("db", exp.to_identifier(db) if db else None)
    ref.set("catalog", None)


def _inline_subquery(ref, cte_body, alias: str) -> None:
    from sqlglot import exp

    ref.replace(exp.Subquery(this=cte_body.copy(), alias=exp.TableAlias(this=exp.to_identifier(alias))))


def _statement_for_body(stmt: Statement, body_ast, suffix: str, *, target_fqn: str | None = None) -> Statement:
    from sqlglot import exp

    ast = body_ast
    if target_fqn is not None and isinstance(stmt.ast, exp.Create):
        wrapped = stmt.ast.copy()
        wrapped.set("expression", body_ast)
        ast = wrapped
    text = ast.sql(dialect="databricks")
    return Statement(source_path=f"{stmt.source_path}::{suffix}", line=stmt.line, text=text, ast=ast)


def _substitute_resolved_refs(body, resolved: dict[str, tuple[str, str | None]]):
    """Rename references to already-resolved (earlier) CTEs inside *body*."""
    copy = body.copy()
    for name, (new_name, db) in resolved.items():
        for ref in _find_table_refs(copy, name):
            if new_name != name or db:
                _rename_table_ref(ref, new_name, db=db)
    return copy


def _intermediate_fqn_for_cte(name: str) -> str:
    return f"{_DEFAULT_INTERMEDIATE_LAYER}.{name}"


# ---------------------------------------------------------------------------
# Pass 1 — WITH-clause CTEs: partial / intermediate_table / inline
# ---------------------------------------------------------------------------


def _split_statement(
    stmt: Statement,
    defs: list[tuple[str, object]],
    fingerprint_sources: dict[str, set[str]],
    created_intermediate: dict[str, SplitUnit],
):
    from sqlglot import exp
    from skifer.analyzer.mapper import _extract_target_and_body

    target, body = _extract_target_and_body(stmt.ast)
    if isinstance(body, exp.Union):
        branches = _flatten_union(body)
    else:
        stripped = body.copy()
        stripped.set("with_", None)
        branches = [stripped]

    resolved: dict[str, tuple[str, str | None]] = {}  # name -> (new_name, db) for chained CTE refs
    units: list[SplitUnit] = []
    inlined: list[InlineNote] = []

    for name, cte_body in defs:
        occurrences: list[tuple[object, object]] = []
        for branch in branches:
            for ref in _find_table_refs(branch, name):
                occurrences.append((branch, ref))
        for other_name, other_body in defs:
            if other_name == name:
                continue
            for ref in _find_table_refs(other_body, name):
                occurrences.append((other_body, ref))

        fp = _fingerprint(cte_body)
        cross_script = bool(fingerprint_sources.get(fp, set()) - {stmt.source_path})
        resolved_cte_body = _substitute_resolved_refs(cte_body, resolved)

        if cross_script:
            fqn = _intermediate_fqn_for_cte(name)
            schema, _, bare = fqn.rpartition(".")
            if fqn not in created_intermediate:
                child_stmt = _statement_for_body(stmt, resolved_cte_body, suffix=f"cte:{name}")
                created_intermediate[fqn] = SplitUnit(kind="intermediate_table", name=fqn, statement=child_stmt)
                units.append(created_intermediate[fqn])
            for _, ref in occurrences:
                _rename_table_ref(ref, bare, db=schema or None)
            resolved[name] = (bare, schema or None)
            continue

        if len(occurrences) <= 1:
            if occurrences:
                _, ref = occurrences[0]
                _inline_subquery(ref, resolved_cte_body, name)
            inlined.append(InlineNote(cte_name=name, source_path=stmt.source_path))
            continue

        predicates = [_enclosing_predicate_sql(ref) for _, ref in occurrences]
        if len(set(predicates)) <= 1:
            path = f"_partials/{name}.yaml"
            child_stmt = _statement_for_body(stmt, resolved_cte_body.copy(), suffix=f"partial:{name}")
            units.append(SplitUnit(kind="partial", name=name, statement=child_stmt, path=path))
            resolved[name] = (name, None)
        else:
            for occ_idx, (consumer, ref) in enumerate(occurrences):
                alias = f"{name}_branch{occ_idx + 1}"
                path = f"_partials/{alias}.yaml"
                child_body = resolved_cte_body.copy()
                predicate = predicates[occ_idx]
                if predicate:
                    child_body = child_body.where(predicate, dialect="databricks")
                child_stmt = _statement_for_body(stmt, child_body, suffix=f"partial:{alias}")
                units.append(SplitUnit(kind="partial", name=alias, statement=child_stmt, path=path))
                _rename_table_ref(ref, alias)
                if predicate:
                    consumer.set("where", None)

    main_statements = [_statement_for_body(stmt, branch, suffix="main", target_fqn=target) for branch in branches]
    return main_statements, units, inlined


# ---------------------------------------------------------------------------
# Pass 2 — scalar subqueries shared across statements -> intermediate table
# ---------------------------------------------------------------------------


def _recognized_scalar_subquery(subquery_ast):
    """Recognize ``(SELECT FUNC(col) FROM table)`` — one aggregate, one
    un-joined/un-grouped/un-filtered source table. Returns
    ``(func, col, table_fqn)`` or ``None`` for any other shape."""
    from sqlglot import exp
    from skifer.analyzer.mapper import _AGG_CLASS_TO_FUNC, _table_fqn

    inner = subquery_ast.this
    if not isinstance(inner, exp.Select):
        return None
    if inner.args.get("with_") or inner.args.get("joins") or inner.args.get("group") or inner.args.get("where"):
        return None
    from_ = inner.args.get("from_")
    if from_ is None or not isinstance(from_.this, exp.Table):
        return None
    if len(inner.expressions) != 1:
        return None
    item = inner.expressions[0]
    expr = item.this if isinstance(item, exp.Alias) else item
    func = _AGG_CLASS_TO_FUNC.get(type(expr).__name__)
    if func is None or not isinstance(expr.this, exp.Column):
        return None
    return func, expr.this.name, _table_fqn(from_.this)


def _find_subquery_items(body):
    from sqlglot import exp

    out = []
    for item in body.expressions:
        target = item.alias if isinstance(item, exp.Alias) else None
        inner_expr = item.this if isinstance(item, exp.Alias) else item
        if isinstance(inner_expr, exp.Subquery) and target is not None:
            out.append((item, target, inner_expr))
    return out


def _grand_total_schema_dict(source_fqn: str, alias: str, func: str, col: str, target_col: str) -> dict:
    return {
        "tables": [{"name": source_fqn, "alias": alias}],
        "add_columns": [["literal:1", "_group"]],
        "aggregate": {"group_by": ["_group"], "measures": [[col, target_col, func]]},
    }


def _split_subqueries(leaves: list[Statement]) -> tuple[list[Statement], list[SplitUnit]]:
    import sqlglot
    from sqlglot import exp
    from skifer.analyzer.mapper import _extract_target_and_body, _strip_qualifiers_sql

    occurrences: dict[str, list[tuple[int, object, object, str]]] = {}
    bodies: list[object] = []
    for idx, stmt in enumerate(leaves):
        _, body = _extract_target_and_body(stmt.ast)
        bodies.append(body)
        if body is None:
            continue
        for item, alias, subquery in _find_subquery_items(body):
            fp = _strip_qualifiers_sql(subquery.this)
            occurrences.setdefault(fp, []).append((idx, item, subquery, alias))

    units: list[SplitUnit] = []
    for fp, occs in occurrences.items():
        if len(occs) < 2:
            continue
        recognized = _recognized_scalar_subquery(occs[0][2])
        if recognized is None:
            continue
        func, col, source_fqn = recognized
        target_col = occs[0][3]
        schema, _, table_bare = source_fqn.rpartition(".")
        alias = table_bare
        fqn = f"{schema or _DEFAULT_INTERMEDIATE_LAYER}.{table_bare}_agg_{func}_{col}"
        schema_dict = _grand_total_schema_dict(source_fqn, alias, func, col, target_col)
        units.append(SplitUnit(kind="intermediate_table", name=fqn, statement=None, schema_dict=schema_dict))

        join_sql = f"SELECT 1 FROM x CROSS JOIN {fqn} AS {alias}"
        join_node = sqlglot.parse_one(join_sql, dialect="databricks").args["joins"][0]

        for idx, item, subquery, consumer_alias in occs:
            new_column = exp.column(consumer_alias if consumer_alias == target_col else target_col)
            item.replace(new_column)
            body = bodies[idx]
            existing_joins = body.args.get("joins") or []
            body.set("joins", [*existing_joins, join_node.copy()])

    rewritten: list[Statement] = []
    for idx, stmt in enumerate(leaves):
        if bodies[idx] is None:
            rewritten.append(stmt)
            continue
        rewritten.append(dataclasses.replace(stmt, text=stmt.ast.sql(dialect="databricks")))

    return rewritten, units


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def split_corpus(statements: list[Statement]) -> SplitResult:
    """Decompose every CTE and recurring scalar subquery in *statements* into
    independently mappable units. A statement with neither passes through
    unchanged as its own single ``main`` unit."""
    from skifer.analyzer.mapper import _extract_target_and_body

    fingerprint_sources: dict[str, set[str]] = {}
    per_statement_ctes: dict[int, list[tuple[str, object]]] = {}
    for idx, stmt in enumerate(statements):
        _, body = _extract_target_and_body(stmt.ast)
        with_ = _with_clause(body) if body is not None else None
        if with_ is None:
            continue
        defs = _cte_defs(with_)
        per_statement_ctes[idx] = defs
        for name, select_ast in defs:
            fingerprint_sources.setdefault(_fingerprint(select_ast), set()).add(stmt.source_path)

    non_main_units: list[SplitUnit] = []
    inlined: list[InlineNote] = []
    leaves: list[Statement] = []
    created_intermediate: dict[str, SplitUnit] = {}

    for idx, stmt in enumerate(statements):
        defs = per_statement_ctes.get(idx)
        if not defs:
            leaves.append(stmt)
            continue
        main_statements, units_here, inlined_here = _split_statement(stmt, defs, fingerprint_sources, created_intermediate)
        leaves.extend(main_statements)
        non_main_units.extend(units_here)
        inlined.extend(inlined_here)

    rewritten_leaves, subquery_units = _split_subqueries(leaves)
    main_units = [SplitUnit(kind="main", name=None, statement=s) for s in rewritten_leaves]

    return SplitResult(units=tuple(non_main_units + subquery_units + main_units), inlined=tuple(inlined))
