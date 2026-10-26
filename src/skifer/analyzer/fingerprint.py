"""Deterministic structural fingerprints over SQL expressions (Plan 43 — 43.4).

Every fingerprint is ``sha256:v1:<hex>`` over a *canonicalized* rendering: table
qualifiers stripped off every column, literals optionally replaced by a typed
placeholder (``abstract_literals``), and the operands of a commutative binary
op (``+``/``*``) sorted by their own canonical text. Canonicalization never
touches the caller's AST (it mutates a copy) and never re-parses — only
``sqlglot``'s own ``.sql()`` renderer is used, so the result is insensitive to
``PYTHONHASHSEED`` and to the original operand order.
"""
from __future__ import annotations

import hashlib

_VERSION_PREFIX = "sha256:v1:"


def _hash(text: str) -> str:
    return _VERSION_PREFIX + hashlib.sha256(text.encode("utf-8")).hexdigest()


def _canonicalize(node, *, abstract_literals: bool):
    from sqlglot import exp

    canon = node.copy()
    for n in list(canon.walk()):
        n = n[0] if isinstance(n, tuple) else n
        if isinstance(n, exp.Column):
            n.set("table", None)
            n.set("catalog", None)
        elif abstract_literals and isinstance(n, exp.Literal):
            n.set("this", "STRLIT" if n.args.get("is_string") else "NUMLIT")

    for n in list(canon.walk()):
        n = n[0] if isinstance(n, tuple) else n
        if isinstance(n, (exp.Add, exp.Mul)):
            left, right = n.this, n.expression
            if left is not None and right is not None:
                left_sql = left.sql(dialect="databricks")
                right_sql = right.sql(dialect="databricks")
                if left_sql > right_sql:
                    n.set("this", right)
                    n.set("expression", left)

    return canon


def expression_fingerprint(expr, *, abstract_literals: bool) -> str:
    """Fingerprint one scalar expression's AST, qualifier-stripped.

    ``abstract_literals=True`` collapses every literal value to a typed
    placeholder first — two occurrences of the same shape with different
    literal values (``amount * 1.2`` vs. ``amount * 1.21``) then fingerprint
    identically, which is what groups a *literal-variant* recurrence together
    before parameters are considered. ``abstract_literals=False`` keeps the
    literal values, so it only matches byte-for-byte-equivalent expressions.
    """
    canon = _canonicalize(expr, abstract_literals=abstract_literals)
    return _hash(canon.sql(dialect="databricks"))


def _conjuncts(where_expr) -> list:
    from sqlglot import exp

    if isinstance(where_expr, exp.And):
        return _conjuncts(where_expr.this) + _conjuncts(where_expr.expression)
    return [where_expr]


def subquery_fingerprint(select_ast) -> str:
    """Fingerprint a ``SELECT`` by ``(tables, filters, projection)`` rather
    than its raw text — resilient to clause reordering across occurrences
    that still read the same tables, filter the same way, and project the
    same columns in the same order."""
    from sqlglot import exp
    from skifer.analyzer.mapper import _table_fqn

    tables = sorted({
        _table_fqn(n[0] if isinstance(n, tuple) else n)
        for n in select_ast.walk()
        if isinstance(n[0] if isinstance(n, tuple) else n, exp.Table)
    })

    where = select_ast.args.get("where")
    filters = sorted(
        _canonicalize(c, abstract_literals=False).sql(dialect="databricks")
        for c in (_conjuncts(where.this) if where is not None else [])
    )

    projection = [
        _canonicalize(item, abstract_literals=False).sql(dialect="databricks")
        for item in select_ast.expressions
    ]

    text = "TABLES:" + ",".join(tables) + "|FILTERS:" + ",".join(filters) + "|PROJECTION:" + ",".join(projection)
    return _hash(text)
