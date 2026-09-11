"""
RuleAnalyzer — best-effort static analysis of Business Rule functions.

Detects output columns (withColumn / with_column calls) and input columns
(F.col / col calls) via AST parsing, then surfaces redundancy warnings.

Limitations:
  - Dynamically constructed column names (e.g. df.withColumn(var, ...)) are not detected.
  - Rules defined in a REPL or compiled notebook may not expose source code.
  - Complex closures or class-based rules may produce incomplete profiles.
"""

from __future__ import annotations

import ast
import copy
import inspect
import textwrap
from dataclasses import dataclass, field
from typing import Callable

from .registry import RuleRegistry


# ---------------------------------------------------------------------------
# AST canonicalization helpers (plan17-0.5)
# ---------------------------------------------------------------------------

#: Well-known PySpark function attributes — used to detect F-alias references.
_SPARK_FUNC_ATTRS: frozenset[str] = frozenset({
    "col", "lit", "when", "sum", "count", "avg", "max", "min",
    "concat", "coalesce", "length", "upper", "lower", "trim",
    "year", "month", "dayofmonth", "hour", "minute", "second",
    "to_date", "to_timestamp", "datediff", "date_add", "date_sub",
    "explode", "array", "struct", "map", "expr", "split",
    "regexp_replace", "regexp_extract", "round", "floor", "ceil",
    "abs", "sqrt", "log", "pow", "rand", "randn",
    "lag", "lead", "row_number", "rank", "dense_rank",
    "window", "Window", "pandas_udf", "udf", "otherwise",
})


class _ASTCanonicalizer(ast.NodeTransformer):
    """Normalizes an expression AST node for cross-rule deduplication.

    Any ``Name`` used as the receiver of a known Spark function attribute
    (e.g. ``F.col``, ``functions.lit``, ``sf.when``) is replaced with the
    canonical placeholder ``_F``.  This lets expressions like
    ``F.col("x") * 2`` and ``sf.col("x") * 2`` be recognized as duplicates.
    """

    def visit_Attribute(self, node: ast.Attribute) -> ast.AST:
        self.generic_visit(node)
        if node.attr in _SPARK_FUNC_ATTRS and isinstance(node.value, ast.Name):
            new_value = ast.copy_location(
                ast.Name(id="_F", ctx=ast.Load()), node.value
            )
            return ast.copy_location(
                ast.Attribute(value=new_value, attr=node.attr, ctx=node.ctx), node
            )
        return node


def _canonicalize_expr(node: ast.AST) -> str:
    """Return a canonical string representation of an expression AST node.

    Deep-copies the node before transformation so the original AST is
    not mutated.
    """
    try:
        canonical = _ASTCanonicalizer().visit(copy.deepcopy(node))
        ast.fix_missing_locations(canonical)
        return ast.unparse(canonical)
    except Exception:
        return ast.unparse(node)


@dataclass
class RuleProfile:
    """Static analysis profile for a single Business Rule function."""
    name: str
    output_columns: list[str] = field(default_factory=list)
    input_columns: list[str] = field(default_factory=list)
    raw_expressions: list[str] = field(default_factory=list)
    source_available: bool = True
    # Performance metadata (populated by analyze_rule)
    has_python_udf: bool = False      # True if @udf / @pandas_udf / F.udf(...) detected
    loc: int = 0                      # Lines of source code (excluding decorator line)
    withcolumn_count: int = 0         # Number of withColumn / with_column calls


@dataclass
class RuleWarning:
    """A warning produced by redundancy detection across multiple rules."""
    level: str          # "warning" | "info"
    code: str           # "OVERWRITE" | "SHARED_READ" | "DUPLICATE_EXPR" | "PHOTON_BREAKING" | "COMPLEXITY_HIGH"
    message: str
    rules: list[str]
    column: str | None = None


class _ColumnVisitor(ast.NodeVisitor):
    """AST visitor that collects column reads and writes from a rule function body."""

    def __init__(self):
        self.outputs: list[str] = []
        self.inputs: list[str] = []
        self.expressions: list[str] = []
        # Projection rules return {name: Column}. The returned dict is tracked
        # rather than every dict literal, so a lookup table inside a rule is not
        # mistaken for a set of output columns.
        self._returned_names: set = set()
        self._assigned_dicts: dict = {}
        self._returned_dicts: list = []

    def _record_dict_outputs(self, node) -> None:
        for key, value in zip(node.keys, node.values):
            if not (isinstance(key, ast.Constant) and isinstance(key.value, str)):
                continue
            if key.value not in self.outputs:
                self.outputs.append(key.value)
            try:
                expr_str = _canonicalize_expr(value)
            except Exception:
                continue
            if expr_str not in self.expressions:
                self.expressions.append(expr_str)

    def resolve_returned_dicts(self) -> None:
        """Fold in dicts returned through a local name, once the body is walked."""
        for node in self._returned_dicts:
            self._record_dict_outputs(node)
        for name in self._returned_names:
            node = self._assigned_dicts.get(name)
            if node is not None:
                self._record_dict_outputs(node)

    def visit_Return(self, node: ast.Return):
        if isinstance(node.value, ast.Dict):
            self._returned_dicts.append(node.value)
        elif isinstance(node.value, ast.Name):
            self._returned_names.add(node.value.id)
        self.generic_visit(node)

    def visit_Assign(self, node: ast.Assign):
        if isinstance(node.value, ast.Dict):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    self._assigned_dicts[target.id] = node.value
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call):
        func = node.func

        # --- Output columns: df.withColumn("name", ...) or df.with_column("name", ...) ---
        if (
            isinstance(func, ast.Attribute)
            and func.attr in ("withColumn", "with_column")
            and node.args
            and isinstance(node.args[0], ast.Constant)
            and isinstance(node.args[0].value, str)
        ):
            col_name = node.args[0].value
            if col_name not in self.outputs:
                self.outputs.append(col_name)

            # Capture the second argument as a canonical expression string (best-effort)
            if len(node.args) > 1:
                try:
                    expr_str = _canonicalize_expr(node.args[1])
                    if expr_str not in self.expressions:
                        self.expressions.append(expr_str)
                except Exception:
                    pass

        # --- Input columns: F.col("name") or col("name") ---
        is_fcol = (
            isinstance(func, ast.Attribute)
            and func.attr == "col"
        )
        is_bare_col = (
            isinstance(func, ast.Name)
            and func.id == "col"
        )
        if (is_fcol or is_bare_col) and node.args and isinstance(node.args[0], ast.Constant):
            col_name = node.args[0].value
            if isinstance(col_name, str) and col_name not in self.inputs:
                self.inputs.append(col_name)

        # --- Subscript access: df["col_name"] ---
        self.generic_visit(node)

    def visit_Subscript(self, node: ast.Subscript):
        if isinstance(node.slice, ast.Constant) and isinstance(node.slice.value, str):
            col_name = node.slice.value
            if col_name not in self.inputs:
                self.inputs.append(col_name)
        self.generic_visit(node)


class _PerformanceVisitor(ast.NodeVisitor):
    """AST visitor that detects performance anti-patterns in a rule function body."""

    # Decorator names that indicate a Python UDF (breaks Photon)
    _UDF_DECORATOR_NAMES = frozenset({"udf", "pandas_udf"})

    def __init__(self):
        self.has_python_udf: bool = False
        self.withcolumn_count: int = 0

    def visit_Call(self, node: ast.Call):
        func = node.func

        # Detect F.udf(...) or F.pandas_udf(...) call-style
        if isinstance(func, ast.Attribute) and func.attr in ("udf", "pandas_udf"):
            self.has_python_udf = True

        # Count withColumn / with_column calls
        if (
            isinstance(func, ast.Attribute)
            and func.attr in ("withColumn", "with_column")
        ):
            self.withcolumn_count += 1

        self.generic_visit(node)

    def visit_FunctionDef(self, node: ast.FunctionDef):
        """Detect @udf / @pandas_udf decorator on a nested function definition."""
        for decorator in node.decorator_list:
            name = None
            if isinstance(decorator, ast.Name):
                name = decorator.id
            elif isinstance(decorator, ast.Attribute):
                name = decorator.attr
            elif isinstance(decorator, ast.Call):
                if isinstance(decorator.func, ast.Name):
                    name = decorator.func.id
                elif isinstance(decorator.func, ast.Attribute):
                    name = decorator.func.attr
            if name in self._UDF_DECORATOR_NAMES:
                self.has_python_udf = True
        self.generic_visit(node)


class RuleAnalyzer:
    """
    Analyzes registered Business Rule functions via AST introspection.

    Usage::

        analyzer = RuleAnalyzer()
        profiles = analyzer.analyze_rules(["flag_high_value", "flag_vip"])
        warnings = analyzer.detect_warnings(profiles)
        analyzer.print_report(profiles, warnings)
    """

    def analyze_rule(self, func: Callable, name: str | None = None) -> RuleProfile:
        """
        Analyze a single rule function.

        Args:
            func:  The rule function to analyze.
            name:  Optional display name (defaults to func.__name__).

        Returns:
            RuleProfile with detected columns and expressions.
        """
        rule_name = name or func.__name__

        try:
            source = inspect.getsource(func)
            source = textwrap.dedent(source)
            tree = ast.parse(source)
        except (OSError, TypeError, IndentationError, SyntaxError):
            return RuleProfile(name=rule_name, source_available=False)

        visitor = _ColumnVisitor()
        visitor.visit(tree)
        visitor.resolve_returned_dicts()

        perf_visitor = _PerformanceVisitor()
        perf_visitor.visit(tree)

        # Count meaningful source lines (skip decorator and blank lines)
        loc = sum(
            1 for line in source.splitlines()
            if (stripped := line.strip()) and not stripped.startswith("@")
        )

        # Remove output columns from inputs to avoid self-references
        inputs = [c for c in visitor.inputs if c not in visitor.outputs]

        return RuleProfile(
            name=rule_name,
            output_columns=visitor.outputs,
            input_columns=inputs,
            raw_expressions=visitor.expressions,
            source_available=True,
            has_python_udf=perf_visitor.has_python_udf,
            loc=loc,
            withcolumn_count=perf_visitor.withcolumn_count,
        )

    def analyze_source(self, source: str, name: str) -> RuleProfile:
        """Analyze raw rule source without requiring a registered callable."""
        try:
            dedented_source = textwrap.dedent(source)
            tree = ast.parse(dedented_source)
        except (SyntaxError, IndentationError):
            return RuleProfile(name=name, source_available=False)

        visitor = _ColumnVisitor()
        visitor.visit(tree)
        visitor.resolve_returned_dicts()

        perf_visitor = _PerformanceVisitor()
        perf_visitor.visit(tree)

        loc = sum(
            1
            for line in dedented_source.splitlines()
            if (stripped := line.strip()) and not stripped.startswith("@")
        )
        inputs = [column for column in visitor.inputs if column not in visitor.outputs]
        return RuleProfile(
            name=name,
            output_columns=visitor.outputs,
            input_columns=inputs,
            raw_expressions=visitor.expressions,
            source_available=True,
            has_python_udf=perf_visitor.has_python_udf,
            loc=loc,
            withcolumn_count=perf_visitor.withcolumn_count,
        )

    def analyze_rules(self, rule_names: list[str]) -> list[RuleProfile]:
        """
        Analyze a list of registered rule names.

        Returns cached profiles stored on each ``RuleSpec`` when available,
        avoiding redundant AST parsing across multiple callers.

        Args:
            rule_names: List of rule names as they appear in the schema.

        Returns:
            List of RuleProfile, one per rule. Rules whose source is unavailable
            are included with source_available=False.
        """
        profiles = []
        for name in rule_names:
            try:
                rule_spec = RuleRegistry.get_rule(name)
            except ValueError:
                profiles.append(RuleProfile(name=name, source_available=False))
                continue
            profiles.append(rule_spec.profile)
        return profiles

    def build_dependency_graph(
        self, profiles: list[RuleProfile]
    ) -> dict[str, list[str]]:
        """
        Build a dependency graph from a list of RuleProfile objects.

        A rule *B* depends on rule *A* if *B* reads a column that *A* writes.
        The graph is represented as ``{rule_name: [dependency_rule_name, ...]}``.

        Rules whose source is unavailable are treated conservatively: their
        position in the original list is preserved (no edges inferred).

        Args:
            profiles: Ordered list of RuleProfile (same order as YAML declaration).

        Returns:
            dict mapping each rule name to the list of rule names it depends on.
        """
        # Map from output column → last rule that writes it (in declaration order)
        col_writer: dict[str, str] = {}
        dep_graph: dict[str, list[str]] = {p.name: [] for p in profiles}
        previous_name: str | None = None

        for profile in profiles:
            if not profile.source_available:
                # Cannot infer deps → keep original order by chaining to the
                # immediately previous rule when possible.
                if previous_name and previous_name not in dep_graph[profile.name]:
                    dep_graph[profile.name].append(previous_name)
                for col in profile.output_columns:
                    col_writer[col] = profile.name
                previous_name = profile.name
                continue

            # Add dependencies: for each input column this rule reads, check
            # if a previous rule wrote it.
            for col in profile.input_columns:
                writer = col_writer.get(col)
                if writer and writer != profile.name:
                    if writer not in dep_graph[profile.name]:
                        dep_graph[profile.name].append(writer)

            # Register this rule as the writer of its output columns
            for col in profile.output_columns:
                col_writer[col] = profile.name
            previous_name = profile.name

        return dep_graph

    def detect_warnings(
        self,
        profiles: list[RuleProfile],
        shared_read_threshold: int = 2,
        loc_threshold: int = 30,
        withcolumn_threshold: int = 5,
    ) -> list[RuleWarning]:
        """
        Detect redundancy and performance patterns across a list of RuleProfile.

        Args:
            profiles:               List of analyzed rule profiles.
            shared_read_threshold:  Minimum number of rules reading the same column
                                    to trigger a SHARED_READ warning (default: 2).
            loc_threshold:          Lines-of-code limit before COMPLEXITY_HIGH fires
                                    (default: 30).
            withcolumn_threshold:   Number of ``withColumn`` calls before
                                    COMPLEXITY_HIGH fires (default: 5).

        Returns:
            List of RuleWarning, ordered by severity (warning before info).
        """
        warnings: list[RuleWarning] = []

        # --- OVERWRITE: same column written by multiple rules ---
        output_to_rules: dict[str, list[str]] = {}
        for p in profiles:
            for col in p.output_columns:
                output_to_rules.setdefault(col, []).append(p.name)

        for col, rule_names in output_to_rules.items():
            if len(rule_names) > 1:
                warnings.append(RuleWarning(
                    level="warning",
                    code="OVERWRITE",
                    message=(
                        f"Column '{col}' is written by {len(rule_names)} rules: "
                        f"{', '.join(rule_names)}. "
                        f"Each rule overwrites the previous result."
                    ),
                    rules=rule_names,
                    column=col,
                ))

        # --- SHARED_READ: same column read by N+ rules ---
        input_to_rules: dict[str, list[str]] = {}
        for p in profiles:
            for col in p.input_columns:
                input_to_rules.setdefault(col, []).append(p.name)

        for col, rule_names in input_to_rules.items():
            if len(rule_names) >= shared_read_threshold:
                warnings.append(RuleWarning(
                    level="info",
                    code="SHARED_READ",
                    message=(
                        f"Column '{col}' is read by {len(rule_names)} rules: "
                        f"{', '.join(rule_names)}. "
                        f"If the condition on '{col}' is identical, consider extracting "
                        f"it to a shared rule computed once."
                    ),
                    rules=rule_names,
                    column=col,
                ))

        # --- DUPLICATE_EXPR: same expression string in multiple rules ---
        expr_to_rules: dict[str, list[str]] = {}
        for p in profiles:
            for expr in p.raw_expressions:
                expr_to_rules.setdefault(expr, []).append(p.name)

        for expr, rule_names in expr_to_rules.items():
            if len(rule_names) > 1:
                short = expr if len(expr) <= 60 else expr[:57] + "..."
                warnings.append(RuleWarning(
                    level="warning",
                    code="DUPLICATE_EXPR",
                    message=(
                        f"Expression `{short}` appears in {len(rule_names)} rules: "
                        f"{', '.join(rule_names)}. "
                        f"This computation is redundant and could be extracted."
                    ),
                    rules=rule_names,
                    column=None,
                ))

        # --- PHOTON_BREAKING: Python UDF detected (disables Photon acceleration) ---
        for p in profiles:
            if p.source_available and p.has_python_udf:
                warnings.append(RuleWarning(
                    level="warning",
                    code="PHOTON_BREAKING",
                    message=(
                        f"Rule '{p.name}' contains a Python UDF (@udf or @pandas_udf). "
                        "Python UDFs disable Photon acceleration on Databricks. "
                        "Consider rewriting using native PySpark Column expressions. "
                        "See: https://docs.databricks.com/en/compute/photon.html"
                    ),
                    rules=[p.name],
                    column=None,
                ))

        # --- COMPLEXITY_HIGH: rule is too large or has too many withColumn calls ---
        for p in profiles:
            if not p.source_available:
                continue
            reasons = []
            if p.loc > loc_threshold:
                reasons.append(f"{p.loc} lines of code (threshold: {loc_threshold})")
            if p.withcolumn_count > withcolumn_threshold:
                reasons.append(
                    f"{p.withcolumn_count} withColumn calls (threshold: {withcolumn_threshold})"
                )
            if reasons:
                warnings.append(RuleWarning(
                    level="info",
                    code="COMPLEXITY_HIGH",
                    message=(
                        f"Rule '{p.name}' is complex: {'; '.join(reasons)}. "
                        "Consider splitting it into smaller projection rules — "
                        "the RuleExecutor will fuse them at no extra cost."
                    ),
                    rules=[p.name],
                    column=None,
                ))

        return warnings

    def print_report(
        self,
        profiles: list[RuleProfile],
        warnings: list[RuleWarning],
    ) -> None:
        """Print a human-readable rule analysis report to stdout."""
        SEP = "=" * 55
        SEP2 = "-" * 40

        print(f"\n{SEP}")
        print(" Rule Analysis Report")
        print(SEP)

        if not profiles:
            print(" No rules to analyze.")
            print(SEP)
            return

        for p in profiles:
            print(f"\n {p.name}")
            if not p.source_available:
                print("   (source not available — skipped)")
                continue
            if p.output_columns:
                print(f"   Writes : {', '.join(p.output_columns)}")
            else:
                print("   Writes : (none detected)")
            if p.input_columns:
                print(f"   Reads  : {', '.join(p.input_columns)}")
            else:
                print("   Reads  : (none detected)")

        if warnings:
            print(f"\n {SEP2}")
            print(" Warnings")
            print(f" {SEP2}")
            for w in warnings:
                tag = f"[{w.code}]"
                print(f"\n  {tag} {w.message}")
        else:
            print(f"\n {SEP2}")
            print(" No redundancies detected.")

        print(f"\n{SEP}\n")
