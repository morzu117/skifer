"""
RuleExecutor — executes a list of :class:`~skifer.core.rule_planner.RuleStage`
objects produced by :class:`~skifer.core.rule_planner.RulePlanner`.

Fusion strategies
-----------------
* **projection** stage: calls every projection or SQL rule function, converts
  SQL strings through the backend's ``expr`` primitive, collects all column
  definitions, and applies them in a **single**
  ``select(*existing_cols, *new_cols)`` call.  This avoids the O(N²) Catalyst
  plan re-traversal caused by chained ``withColumn()`` calls.
* **aggregation** stage with multiple rules sharing the same ``groupBy`` keys:
  merges all ``agg(...)`` expressions into a single ``groupBy(...).agg(...)``
  call.  Requires the rule function to carry an ``agg_keys`` attribute
  (set via the ``kind="aggregation"`` decorator convention).
* **transform** stage: applies each rule function sequentially (legacy
  ``df → df`` contract, no fusion).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from skifer.core.rule_planner import RuleStage

logger = logging.getLogger(__name__)


def _has_statement_separator(expression: str) -> bool:
    """Return whether ``expression`` contains ``;`` outside a string literal."""
    in_string = False
    index = 0
    while index < len(expression):
        char = expression[index]
        if char == "'":
            if in_string and index + 1 < len(expression) and expression[index + 1] == "'":
                index += 2
                continue
            if index == 0 or expression[index - 1] != "\\":
                in_string = not in_string
        elif char == ";" and not in_string:
            return True
        index += 1
    return False


def validate_sql_rule_result(rule_name: str, result: object) -> dict[str, str]:
    """Validate one ``kind='sql'`` result before it reaches an SQL engine.

    ``allow_raw_sql`` intentionally does not govern these expressions. That
    setting protects raw SQL authored in declarative YAML (the "what" layer),
    while a registered rule is reviewed and deployed Python code in the "how"
    layer, just like a projection rule that can already call ``F.expr``.

    Structural validation is dependency-free. When the optional ``sqlglot``
    package is installed, every value is additionally parsed as a Databricks
    expression and statement roots such as SELECT/DDL/DML are rejected.
    """
    if not isinstance(result, dict):
        raise TypeError(
            f"Rule '{rule_name}' is declared as kind='sql' but returned "
            f"{type(result).__name__} instead of dict[str, str]."
        )

    validated: dict[str, str] = {}
    for column, expression in result.items():
        if not isinstance(expression, str):
            raise TypeError(
                f"Rule '{rule_name}' column '{column}' is declared as kind='sql' but "
                f"returned {type(expression).__name__}; expected a SQL expression string."
            )
        if not expression.strip():
            raise ValueError(
                f"Rule '{rule_name}' column '{column}' is declared as kind='sql' but "
                "returned an empty SQL expression."
            )
        if _has_statement_separator(expression):
            raise ValueError(
                f"Rule '{rule_name}' column '{column}' contains a statement separator ';' "
                "outside a string literal."
            )

        try:
            import sqlglot  # noqa: PLC0415
            from sqlglot import exp  # noqa: PLC0415
            from sqlglot.errors import SqlglotError  # noqa: PLC0415
        except ImportError:
            pass
        else:
            try:
                parsed = sqlglot.parse_one(expression, read="databricks")
            except SqlglotError as exc:
                raise ValueError(
                    f"Rule '{rule_name}' column '{column}' is not a valid SQL expression: "
                    f"{type(exc).__name__}."
                ) from exc
            if isinstance(parsed, (exp.Query, exp.DDL, exp.DML, exp.Command)):
                raise ValueError(
                    f"Rule '{rule_name}' column '{column}' must be a SQL expression, "
                    f"not a {type(parsed).__name__} statement."
                )

        validated[column] = expression
    return validated


class RuleExecutor:
    """
    Applies an ordered list of :class:`RuleStage` objects to a DataFrame.

    Usage::

        planner = RulePlanner()
        executor = RuleExecutor()
        stages = planner.plan(rule_names)
        df = executor.execute(df, stages)
    """

    def __init__(self, backend=None) -> None:
        self._backend = backend

    def execute(self, df: "DataFrame", stages: list["RuleStage"]) -> "DataFrame":
        """
        Execute all stages sequentially, applying fusion where possible.

        Args:
            df:     Input DataFrame.
            stages: Ordered list of :class:`RuleStage` produced by
                    :class:`RulePlanner`.

        Returns:
            Transformed DataFrame.
        """
        for stage in stages:
            if stage.kind == "projection":
                df = self._execute_projection_stage(df, stage)
            elif stage.kind == "aggregation":
                df = self._execute_aggregation_stage(df, stage)
            else:
                df = self._execute_transform_stage(df, stage)
        return df

    # ------------------------------------------------------------------
    # Stage executors
    # ------------------------------------------------------------------

    def _execute_projection_stage(self, df: "DataFrame", stage: "RuleStage") -> "DataFrame":
        """
        Fuse all projection rules into a single ``select()`` call.

        Each rule returns ``dict[str, Column]``.  Brand-new columns are appended
        to the existing schema in topological order (already sorted by the
        planner).  Duplicate column names produced by different rules are
        de-duplicated: the last rule's expression wins (consistent with
        sequential semantics).

        A rule may also produce a column whose name already exists in the input
        DataFrame (e.g. ``{"x": F.upper(F.col("x"))}``).  Such columns *replace*
        the input column in place — keeping their original position — exactly
        like a sequential ``withColumn``.  This avoids emitting two homonymous
        columns in the fused ``select()`` (which would raise
        ``AMBIGUOUS_REFERENCE``).
        """
        if self._backend is None:
            from pyspark.sql import functions as F

            col = F.col
            sql_expr = F.expr
        else:
            col = self._backend.col
            sql_expr = self._backend.expr

        rule_names = [s.name for s in stage.rules]
        logger.debug("[Executor] Fusing %d projection rules: %s", len(stage.rules), rule_names)

        # Collect all new column definitions (ordered, last-writer wins for dupes)
        new_cols: dict[str, object] = {}
        for spec in stage.rules:
            result = spec.func() if spec.kind == "sql" else spec.func(df)
            if spec.kind == "sql":
                result = {
                    name: sql_expr(expression)
                    for name, expression in validate_sql_rule_result(spec.name, result).items()
                }
            if not isinstance(result, dict):
                raise TypeError(
                    f"Rule '{spec.name}' is declared as kind='projection' but returned "
                    f"{type(result).__name__} instead of dict[str, Column]. "
                    "Either return a dict or change kind to 'transform'."
                )
            new_cols.update(result)

        if not new_cols:
            return df

        # Build the select expression. Columns rewritten by a rule replace the
        # input column in place (preserving position); brand-new columns are
        # appended. Keeping a rewritten column out of `existing` avoids two
        # homonymous columns in the select (AMBIGUOUS_REFERENCE).
        select_exprs = [
            new_cols[c].alias(c) if c in new_cols else col(c)
            for c in df.columns
        ]
        select_exprs += [
            col_expr.alias(col_name)
            for col_name, col_expr in new_cols.items()
            if col_name not in df.columns
        ]
        if self._backend is not None:
            return self._backend.select(df, select_exprs)
        return df.select(*select_exprs)

    def _execute_aggregation_stage(self, df: "DataFrame", stage: "RuleStage") -> "DataFrame":
        """
        Execute aggregation rules.

        When all rules in the stage share the same ``groupBy`` keys (declared
        via the ``agg_keys`` attribute on the rule function), they are merged
        into a single ``groupBy(...).agg(...)`` call.

        Each aggregation rule must return a tuple
        ``(group_keys: list[str], agg_exprs: dict[str, Column])`` when the
        ``agg_keys`` attribute is set; otherwise it must return a ``DataFrame``
        (legacy transform-style fallback).
        """
        if not stage.rules:
            return df

        # Check if all rules declare agg_keys and share the same keys
        all_keys = [getattr(spec.func, "agg_keys", None) for spec in stage.rules]
        all_exprs_available = all(k is not None for k in all_keys)

        if all_exprs_available and len(set(tuple(sorted(k)) for k in all_keys)) == 1:
            # Fused path: merge all agg exprs
            group_keys = list(all_keys[0])
            logger.debug(
                "[Executor] Fusing %d aggregation rules on keys %s",
                len(stage.rules),
                group_keys,
            )

            merged_agg: dict[str, object] = {}
            for spec in stage.rules:
                _, agg_exprs = spec.func(df)
                merged_agg.update(agg_exprs)

            agg_cols = [col_expr.alias(col_name) for col_name, col_expr in merged_agg.items()]
            return df.groupBy(*group_keys).agg(*agg_cols)
        else:
            # Sequential fallback
            for spec in stage.rules:
                result = spec.func(df)
                if isinstance(result, tuple):
                    group_keys, agg_exprs = result
                    agg_cols = [col_expr.alias(col_name) for col_name, col_expr in agg_exprs.items()]
                    df = df.groupBy(*group_keys).agg(*agg_cols)
                else:
                    df = result
            return df

    def _execute_transform_stage(self, df: "DataFrame", stage: "RuleStage") -> "DataFrame":
        """Execute transform rules sequentially (legacy ``df → df`` contract)."""
        for spec in stage.rules:
            df = spec.func(df)
        return df
