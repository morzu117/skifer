"""Round-trip oracle: compile a mapped schema draft back to SQL and compare it
structurally against the original source statement (Plan 43 — 43.2).

Spark-free: this only exercises ``parse_schema`` -> ``parse_to_ir`` ->
``compile_select``, the same pure-Python path the materialized-view compiler
uses. A construct the compiler itself refuses (``business_rules``, ``partials``,
``preprocess.qualify``, …) is reported as ``unverified`` with the compiler's own
reason — never a crash.

The comparison is a documented approximation, not full semantic equivalence:
output column names/positions, qualifier-stripped output expressions, the set
of base tables, the set of (join key pairs, join type) per join, and the WHERE
predicate as a DNF of qualifier-stripped conjuncts (AND/OR are commutative, so
set comparison tolerates a predicate moving between a table-level push-down
and the post-join ``where:``, and tolerates cosmetic ``Paren``/operand-order
differences in an OR-of-ANDs). A quoted numeric literal compares equal to its
bare form, since the filter DSL always SQL-quotes a filter value regardless of
the source's own literal syntax.
"""
from __future__ import annotations

from dataclasses import dataclass

import yaml

from skifer.analyzer.mapper import MappedStatement
from skifer.analyzer.parser import Statement
from skifer.core.ir import parse_to_ir
from skifer.core.schema_loader import parse_schema
from skifer.core.sql_compiler import SqlCompilationError, compile_select

VERIFIED = "verified"
DIVERGENT = "divergent"
UNVERIFIED = "unverified"
SKIPPED = "skipped"


@dataclass(frozen=True)
class RoundTripResult:
    status: str  # one of VERIFIED, DIVERGENT, UNVERIFIED, SKIPPED
    detail: str | None = None
    compiled_sql: str | None = None


def verify(mapped: MappedStatement, source: Statement) -> RoundTripResult:
    """Compile *mapped*'s schema draft and compare it to *source*'s own SQL."""
    if mapped.needs_curation:
        return RoundTripResult(status=SKIPPED, detail="needs_curation — not emitted")

    try:
        yaml_str = yaml.safe_dump(mapped.schema_dict, sort_keys=False)
        normalized = parse_schema(yaml_str, params={"allow_raw_sql": True})
        parsed = parse_to_ir(normalized)
        compiled_sql = compile_select(parsed)
    except SqlCompilationError as exc:
        return RoundTripResult(status=UNVERIFIED, detail=f"SqlCompilationError: {exc}")
    except Exception as exc:  # defensive: the oracle must never crash the analyzer
        return RoundTripResult(status=UNVERIFIED, detail=f"{type(exc).__name__}: {exc}")

    from skifer.analyzer.mapper import _extract_target_and_body

    import sqlglot

    try:
        compiled_ast = sqlglot.parse_one(_normalize_doubled_quotes(compiled_sql), dialect="databricks")
    except Exception as exc:
        return RoundTripResult(
            status=UNVERIFIED, detail=f"compiled SQL failed to re-parse: {exc}", compiled_sql=compiled_sql,
        )

    _, source_body = _extract_target_and_body(source.ast)
    compiled_body, cte_bodies = _unwrap_ctes(compiled_ast)

    if not _structurally_equivalent(source_body, compiled_body, cte_bodies):
        return RoundTripResult(status=DIVERGENT, detail="compiled SQL does not structurally match the source", compiled_sql=compiled_sql)

    return RoundTripResult(status=VERIFIED, compiled_sql=compiled_sql)


def _normalize_doubled_quotes(sql: str) -> str:
    """Rewrite ANSI ``''`` escapes to backslash escapes before re-parsing.

    ``compile_select`` emits standard-SQL doubled-quote escaping, which
    sqlglot's ``databricks`` dialect misreads as implicit string concatenation
    (a documented sqlglot limitation, not a real Spark/Databricks one — see
    docs/roadmap/43_project_analyzer_plan.md). Only the oracle's own re-parse
    of the compiler's output needs this; the compiled SQL returned to the
    caller is untouched and remains valid Databricks SQL.
    """
    out: list[str] = []
    in_string = False
    i = 0
    n = len(sql)
    while i < n:
        ch = sql[i]
        if not in_string:
            if ch == "'":
                in_string = True
            out.append(ch)
            i += 1
            continue
        if ch == "\\" and i + 1 < n:
            out.append(ch)
            out.append(sql[i + 1])
            i += 2
            continue
        if ch == "'":
            if i + 1 < n and sql[i + 1] == "'":
                out.append("\\'")
                i += 2
                continue
            in_string = False
            out.append(ch)
            i += 1
            continue
        out.append(ch)
        i += 1
    return "".join(out)


def _unwrap_ctes(ast):
    """Return (final_select, list_of_cte_selects) for compile_select's ``WITH ... SELECT`` shape."""
    from sqlglot import exp

    ctes = []
    if isinstance(ast, exp.Select) and ast.args.get("with_"):
        for cte in ast.args["with_"].expressions:
            ctes.append(cte.this)
    return ast, ctes


def _strip_qualifiers(expr):
    from sqlglot import exp

    copy = expr.copy()
    while isinstance(copy, exp.Paren):
        copy = copy.this
    for col in copy.find_all(exp.Column):
        col.set("table", None)
    for ident in copy.find_all(exp.Identifier):
        ident.set("quoted", False)
    if isinstance(copy, exp.Identifier):
        copy.set("quoted", False)
    for lit in copy.find_all(exp.Literal):
        if lit.is_string and _looks_numeric(lit.this):
            lit.set("is_string", False)
    return copy


def _looks_numeric(text: str) -> bool:
    """The filter DSL stores every value as a string and always SQL-quotes it,
    so a filter pushed down compiles to a quoted literal even when the source
    SQL wrote it bare (Spark compares them equal via implicit cast either way).
    Treat a quoted numeric-looking literal as equivalent to its bare form so
    this universal filter-DSL trait doesn't read as a divergence.
    """
    try:
        float(text)
    except (TypeError, ValueError):
        return False
    return True


def _output_columns(select_expr):
    from sqlglot import exp

    names = []
    exprs = []
    for item in select_expr.expressions:
        if isinstance(item, exp.Alias):
            names.append(item.alias)
            exprs.append(item.this)
        elif isinstance(item, exp.Column):
            names.append(item.name)
            exprs.append(item)
        else:
            names.append(None)
            exprs.append(item)
    return names, exprs


def _base_tables(bodies: list) -> set[str]:
    from sqlglot import exp

    tables: set[str] = set()
    for body in bodies:
        for t in body.find_all(exp.Table):
            parts = [p.name for p in (t.args.get("catalog"), t.args.get("db")) if p is not None]
            parts.append(t.this.name)
            tables.add(".".join(parts))
    return tables


def _join_signatures(select_expr) -> set[tuple]:
    from sqlglot import exp

    signatures = set()
    for join_node in select_expr.args.get("joins") or []:
        side = str(join_node.args.get("side") or "").upper() or None
        kind = str(join_node.args.get("kind") or "").upper() or None
        if side is None and kind is None:
            kind = "INNER"  # a bare `JOIN` and an explicit `INNER JOIN` are the same join.
        using = join_node.args.get("using")
        if using:
            keys = frozenset(frozenset({c.name}) for c in using)
        else:
            on = join_node.args.get("on")
            keys = frozenset()
            if on is not None:
                pairs = []
                for conjunct in _flatten_and(on):
                    if isinstance(conjunct, exp.EQ) and isinstance(conjunct.this, exp.Column) and isinstance(conjunct.expression, exp.Column):
                        pairs.append(frozenset({conjunct.this.name, conjunct.expression.name}))
                keys = frozenset(pairs)
        signatures.add((side, kind, keys))
    return signatures


def _flatten_and(node):
    from sqlglot import exp

    if isinstance(node, exp.And):
        return _flatten_and(node.this) + _flatten_and(node.expression)
    return [node]


def _predicate_dnf(node) -> frozenset:
    """A predicate's boolean structure as a set of OR-branches, each an
    unordered set of AND'd conjunct texts — a DNF that is insensitive to
    cosmetic ``Paren`` placement and to AND/OR operand order, which is all
    the compiler's own pretty-printing differs on.
    """
    from sqlglot import exp

    while isinstance(node, exp.Paren):
        node = node.this
    if isinstance(node, exp.Or):
        return _predicate_dnf(node.this) | _predicate_dnf(node.expression)
    if isinstance(node, exp.And):
        left = _predicate_dnf(node.this)
        right = _predicate_dnf(node.expression)
        return frozenset(branch | other for branch in left for other in right)
    text = _strip_qualifiers(node).sql(dialect="databricks")
    return frozenset({frozenset({text})})


def _combined_dnf(predicates: list) -> frozenset:
    """AND together several independent predicate trees (e.g. one per
    table-level push-down plus the post-join ``where:``) and return the
    combined DNF.
    """
    dnf: frozenset = frozenset({frozenset()})
    for predicate in predicates:
        dnf = frozenset(branch | other for branch in dnf for other in _predicate_dnf(predicate))
    return dnf


def _where_conjuncts(select_expr, extra_wheres: list) -> frozenset:
    where = select_expr.args.get("where")
    sources = ([where.this] if where is not None else []) + extra_wheres
    return _combined_dnf(sources)


def _structurally_equivalent(source_body, compiled_body, cte_bodies: list) -> bool:
    source_names, source_exprs = _output_columns(source_body)
    compiled_names, compiled_exprs = _output_columns(compiled_body)
    if source_names != compiled_names:
        return False

    source_texts = [_strip_qualifiers(e).sql(dialect="databricks") for e in source_exprs]
    compiled_texts = [_strip_qualifiers(e).sql(dialect="databricks") for e in compiled_exprs]
    if source_texts != compiled_texts:
        return False

    if _base_tables([source_body]) != _base_tables(cte_bodies):
        return False

    if _join_signatures(source_body) != _join_signatures(compiled_body):
        return False

    cte_wheres = [cte.args.get("where").this for cte in cte_bodies if cte.args.get("where") is not None]
    if _where_conjuncts(source_body, []) != _where_conjuncts(compiled_body, cte_wheres):
        return False

    return True
