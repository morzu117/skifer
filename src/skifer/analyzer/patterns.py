"""Finds recurring row-level expressions across a SQL corpus and classifies
them deterministically (Plan 43 — 43.4): a shape repeated often enough becomes
a ``SharedRuleCandidate`` (parameterized when only its literals vary), the
same output alias computed by genuinely different formulas becomes a
``DivergenceFinding`` (both named, never arbitrated — D7), and anything
recurring but short of the threshold is still reported, never silently
dropped.

Candidate expressions are collected from every top-level ``SELECT`` item in
every ``SELECT`` node of the corpus (the main query and every CTE body, at
every union branch) whose shape is one this module recognizes: ``CASE``,
``COALESCE``, ``UPPER``/``LOWER``/``TRIM``/``CAST``, and an arithmetic
``+``/``-``/``*``/``/`` over columns, literals or a recognized nested shape.
A bare column reference is a passthrough, not a pattern; anything else
(a window function, an aggregate, a subquery) is out of scope here — it is
43.2's/43.3's own residues that already say why, this module does not repeat
that job.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from skifer.analyzer.fingerprint import expression_fingerprint
from skifer.analyzer.parser import Statement

_CANDIDATE_FUNCS = {"Coalesce", "Upper", "Lower", "Trim", "Cast"}
_AGGREGATE_FUNCS = {"Sum", "Avg", "Min", "Max", "Count"}


@dataclass(frozen=True)
class PatternThresholds:
    """Named, explainable thresholds (same discipline as
    ``adaptive/recommender.py``) — every verdict cites the one it applied."""

    min_occurrences: int = 3
    min_scripts: int = 2


@dataclass(frozen=True)
class RuleOccurrence:
    """One site where a candidate expression was found."""

    source_path: str
    line: int
    output_alias: str
    sql_text: str  # qualifier-stripped, literal-exact canonical SQL
    literal_values: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict:
        return {
            "source_path": self.source_path,
            "line": self.line,
            "output_alias": self.output_alias,
            "sql_text": self.sql_text,
            "literal_values": list(self.literal_values),
        }


@dataclass(frozen=True)
class SharedRuleCandidate:
    """A recurring expression that cleared both thresholds."""

    name: str
    kind: str  # "projection" | "aggregation"
    fingerprint: str  # abstract-literal shape family
    occurrences: tuple[RuleOccurrence, ...]

    @property
    def is_parameterized(self) -> bool:
        literal_sets = {o.literal_values for o in self.occurrences}
        return len(literal_sets) > 1

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "kind": self.kind,
            "fingerprint": self.fingerprint,
            "is_parameterized": self.is_parameterized,
            "occurrences": [o.to_dict() for o in self.occurrences],
        }


@dataclass(frozen=True)
class DivergenceFinding:
    """The same output alias computed by >=2 structurally different formulas."""

    output_alias: str
    occurrences: tuple[RuleOccurrence, ...]

    def to_dict(self) -> dict:
        return {
            "output_alias": self.output_alias,
            "occurrences": [o.to_dict() for o in self.occurrences],
        }


@dataclass(frozen=True)
class BelowThresholdCandidate:
    """A recurring shape that did not clear ``thresholds`` — reported, not
    silenced (D7)."""

    fingerprint: str
    occurrence_count: int
    distinct_scripts: int
    min_occurrences: int
    min_scripts: int
    occurrences: tuple[RuleOccurrence, ...]

    def to_dict(self) -> dict:
        return {
            "fingerprint": self.fingerprint,
            "occurrence_count": self.occurrence_count,
            "distinct_scripts": self.distinct_scripts,
            "min_occurrences": self.min_occurrences,
            "min_scripts": self.min_scripts,
            "occurrences": [o.to_dict() for o in self.occurrences],
        }


@dataclass(frozen=True)
class PatternReport:
    shared_rule_candidates: tuple[SharedRuleCandidate, ...] = field(default_factory=tuple)
    divergences: tuple[DivergenceFinding, ...] = field(default_factory=tuple)
    below_threshold: tuple[BelowThresholdCandidate, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict:
        return {
            "shared_rule_candidates": [c.to_dict() for c in self.shared_rule_candidates],
            "divergences": [d.to_dict() for d in self.divergences],
            "below_threshold": [b.to_dict() for b in self.below_threshold],
        }


# ---------------------------------------------------------------------------
# Candidate shape recognition
# ---------------------------------------------------------------------------


def _is_scalar_shape(node) -> bool:
    from sqlglot import exp

    if isinstance(node, (exp.Column, exp.Literal, exp.Null, exp.Boolean)):
        return True
    if isinstance(node, exp.Paren):
        return _is_scalar_shape(node.this)
    if isinstance(node, (exp.Add, exp.Sub, exp.Mul, exp.Div,
                          exp.EQ, exp.NEQ, exp.GT, exp.LT, exp.GTE, exp.LTE)):
        return _is_scalar_shape(node.this) and _is_scalar_shape(node.expression)
    if isinstance(node, exp.Case):
        for if_ in node.args.get("ifs") or []:
            if not _is_scalar_shape(if_.this) or not _is_scalar_shape(if_.args.get("true")):
                return False
        default = node.args.get("default")
        return default is None or _is_scalar_shape(default)
    if isinstance(node, exp.Cast):
        return _is_scalar_shape(node.this)
    if type(node).__name__ in _CANDIDATE_FUNCS:
        flat_args = []
        for key, value in node.args.items():
            if key in ("to", "format", "safe", "action"):
                continue  # type/option metadata, not an operand.
            if isinstance(value, list):
                flat_args.extend(value)
            else:
                flat_args.append(value)
        return all(_is_scalar_shape(arg) for arg in flat_args if hasattr(arg, "walk"))
    return False


def _is_candidate_expr(node) -> bool:
    from sqlglot import exp

    if isinstance(node, (exp.Column, exp.Literal, exp.Null, exp.Boolean)):
        return False  # a bare passthrough or constant tag is not a pattern.
    return _is_scalar_shape(node)


def _item_alias_and_expr(item):
    from sqlglot import exp

    if isinstance(item, exp.Alias):
        return item.alias, item.this
    if isinstance(item, exp.Column):
        return item.name, item
    return None, None


def _all_selects(ast):
    """Every ``SELECT`` node reachable from *ast* — the main query, every CTE
    body, and every ``UNION``/``UNION ALL`` branch."""
    from sqlglot import exp

    out = []
    for node in ast.walk():
        node = node[0] if isinstance(node, tuple) else node
        if isinstance(node, exp.Select):
            out.append(node)
    return out


def _strip_qualifiers_sql(expr) -> str:
    from skifer.analyzer.mapper import _strip_qualifiers_sql as _impl

    return _impl(expr)


def _literal_values(expr) -> tuple[str, ...]:
    from sqlglot import exp

    values = []
    for n in expr.walk():
        n = n[0] if isinstance(n, tuple) else n
        if isinstance(n, exp.Literal):
            values.append(str(n.this))
    return tuple(values)


def _collect_occurrences(statements: list[Statement]) -> list[RuleOccurrence]:
    occurrences: list[RuleOccurrence] = []
    for stmt in statements:
        for select_node in _all_selects(stmt.ast):
            for item in select_node.expressions:
                alias, expr = _item_alias_and_expr(item)
                if alias is None or not _is_candidate_expr(expr):
                    continue
                occurrences.append(RuleOccurrence(
                    source_path=stmt.source_path,
                    line=stmt.line,
                    output_alias=alias,
                    sql_text=_strip_qualifiers_sql(expr),
                    literal_values=_literal_values(expr),
                ))
    return occurrences


def _expr_from_sql_text(sql_text: str):
    import sqlglot

    return sqlglot.parse_one(sql_text, dialect="databricks")


def _infer_kind(sql_text: str) -> str:
    expr = _expr_from_sql_text(sql_text)
    if type(expr).__name__ in _AGGREGATE_FUNCS:
        return "aggregation"
    return "projection"


def _majority_name(occurrences: tuple[RuleOccurrence, ...]) -> str:
    counts: dict[str, int] = {}
    for o in occurrences:
        counts[o.output_alias] = counts.get(o.output_alias, 0) + 1
    best = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))
    return best[0][0]


def find_patterns(
    statements: list[Statement],
    *,
    thresholds: PatternThresholds = PatternThresholds(),
) -> PatternReport:
    """Scan *statements* for recurring/divergent row-level expressions."""
    occurrences = _collect_occurrences(statements)

    by_abstract_fp: dict[str, list[RuleOccurrence]] = {}
    by_alias_exact_fp: dict[str, dict[str, list[RuleOccurrence]]] = {}
    for occ in occurrences:
        expr = _expr_from_sql_text(occ.sql_text)
        abstract_fp = expression_fingerprint(expr, abstract_literals=True)
        exact_fp = expression_fingerprint(expr, abstract_literals=False)
        by_abstract_fp.setdefault(abstract_fp, []).append(occ)
        by_alias_exact_fp.setdefault(occ.output_alias, {}).setdefault(exact_fp, []).append(occ)

    shared: list[SharedRuleCandidate] = []
    below: list[BelowThresholdCandidate] = []
    for fp, occs in sorted(by_abstract_fp.items()):
        scripts = {o.source_path for o in occs}
        if len(occs) >= thresholds.min_occurrences and len(scripts) >= thresholds.min_scripts:
            shared.append(SharedRuleCandidate(
                name=_majority_name(tuple(occs)),
                kind=_infer_kind(occs[0].sql_text),
                fingerprint=fp,
                occurrences=tuple(occs),
            ))
        elif len(occs) >= 2:
            below.append(BelowThresholdCandidate(
                fingerprint=fp,
                occurrence_count=len(occs),
                distinct_scripts=len(scripts),
                min_occurrences=thresholds.min_occurrences,
                min_scripts=thresholds.min_scripts,
                occurrences=tuple(occs),
            ))

    divergences: list[DivergenceFinding] = []
    for alias, by_exact in sorted(by_alias_exact_fp.items()):
        if len(by_exact) >= 2:
            all_occs = tuple(o for occs in by_exact.values() for o in occs)
            divergences.append(DivergenceFinding(output_alias=alias, occurrences=all_occs))

    return PatternReport(
        shared_rule_candidates=tuple(shared),
        divergences=tuple(divergences),
        below_threshold=tuple(below),
    )
