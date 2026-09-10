"""
RulePlanner — groups Business Rules into homogeneous execution stages and
performs topological ordering within ``projection`` stages.

Key concepts
------------
* A **stage** is a maximal consecutive sequence of rules of the same ``kind``.
* Consecutive ``projection`` stages are merged into a single ``select()`` call
  (see ``RuleExecutor``).
* ``aggregation`` stages sharing the same ``groupBy`` keys are merged into a
  single ``groupBy(...).agg(...)`` call.
* ``transform`` stages are executed as-is (legacy ``df → df`` contract).

Topological sort
----------------
For a ``projection`` stage, inter-rule dependencies are detected using
``RuleAnalyzer.build_dependency_graph()``.  If rule *B* reads a column that
rule *A* writes, *B* depends on *A*.  The rules are reordered accordingly and
a log is emitted when the order differs from the YAML declaration order.

Cycle detection
---------------
A dependency cycle within a single ``projection`` stage raises
``RuleCycleError``.  The fix is either to split the stage manually or tag one
of the rules as ``kind="transform"``.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from skifer.core.registry import RuleSpec

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class RuleCycleError(Exception):
    """Raised when a topological sort detects a cycle in rule dependencies."""


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class RuleStage:
    """A homogeneous group of rules to be executed together."""

    kind: str                        # "projection" | "aggregation" | "transform"
    rules: list["RuleSpec"] = field(default_factory=list)

    def __repr__(self) -> str:
        names = [r.name for r in self.rules]
        return f"RuleStage(kind={self.kind!r}, rules={names})"


# ---------------------------------------------------------------------------
# Planner
# ---------------------------------------------------------------------------

class RulePlanner:
    """
    Converts a flat list of rule names into an ordered list of
    :class:`RuleStage` objects ready for :class:`RuleExecutor`.

    Usage::

        planner = RulePlanner()
        stages = planner.plan(["proj_a", "proj_b", "agg_c", "proj_d"])
        # → [RuleStage(projection, [proj_a, proj_b]),
        #    RuleStage(aggregation, [agg_c]),
        #    RuleStage(projection, [proj_d])]
    """

    def plan(self, rule_names: list[str]) -> list[RuleStage]:
        """
        Group rules into homogeneous stages and apply intra-stage optimisations.

        Args:
            rule_names: Ordered list of rule names as declared in the YAML schema.

        Returns:
            Ordered list of :class:`RuleStage` objects.
        """
        from skifer.core.registry import RuleRegistry

        stages: list[RuleStage] = []
        current_stage: RuleStage | None = None

        for name in rule_names:
            spec = RuleRegistry.get_rule(name)
            if current_stage is None or spec.kind != current_stage.kind:
                current_stage = RuleStage(kind=spec.kind)
                stages.append(current_stage)
            current_stage.rules.append(spec)

        # Optimise each stage
        result: list[RuleStage] = []
        for stage in stages:
            if stage.kind == "projection" and len(stage.rules) > 1:
                result.append(self._sort_projection_stage(stage))
            elif stage.kind == "aggregation" and len(stage.rules) > 1:
                result.extend(self._split_aggregation_stages(stage))
            else:
                result.append(stage)

        return result

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _sort_projection_stage(self, stage: RuleStage) -> RuleStage:
        """Topologically sort rules within a projection stage."""
        from skifer.core.rule_analyzer import RuleAnalyzer

        analyzer = RuleAnalyzer()
        profiles = [spec.profile for spec in stage.rules]
        dep_graph = analyzer.build_dependency_graph(profiles)

        original_order = [s.name for s in stage.rules]
        sorted_names = _topological_sort(dep_graph, original_order)

        if sorted_names != original_order:
            logger.info(
                "[Planner] Reordered rules: %s → %s",
                original_order,
                sorted_names,
            )

        name_to_spec = {s.name: s for s in stage.rules}
        sorted_specs = [name_to_spec[n] for n in sorted_names]
        return RuleStage(kind="projection", rules=sorted_specs)

    def _split_aggregation_stages(self, stage: RuleStage) -> list[RuleStage]:
        """
        Keep aggregation rules together only when they share the same
        ``groupBy`` keys (inspected from ``.agg_keys`` attribute on the rule
        function if present — Option A).  Falls back to one stage per rule.
        """
        # Group consecutive rules that declare the same agg_keys
        groups: list[RuleStage] = []
        current: RuleStage | None = None
        current_keys: tuple | None = None

        for spec in stage.rules:
            keys = getattr(spec.func, "agg_keys", None)
            keys_tuple = tuple(sorted(keys)) if keys is not None else None

            if current is None or keys_tuple is None or keys_tuple != current_keys:
                current = RuleStage(kind="aggregation")
                groups.append(current)
                current_keys = keys_tuple
            current.rules.append(spec)

        return groups


# ---------------------------------------------------------------------------
# Topological sort (Kahn's algorithm)
# ---------------------------------------------------------------------------

def _topological_sort(dep_graph: dict[str, list[str]], names: list[str]) -> list[str]:
    """
    Return ``names`` in topological order given ``dep_graph``.

    ``dep_graph[A] = [B, C]`` means *A depends on B and C* (B and C must come
    before A).

    Raises:
        RuleCycleError: If a cycle is detected.
    """
    # Build in-degree table for the subset of names
    name_set = set(names)
    in_degree: dict[str, int] = {n: 0 for n in names}
    dependents: dict[str, list[str]] = {n: [] for n in names}  # n → rules that depend on n

    for node, deps in dep_graph.items():
        if node not in name_set:
            continue
        for dep in deps:
            if dep not in name_set:
                continue
            in_degree[node] += 1
            dependents[dep].append(node)

    queue = [n for n in names if in_degree[n] == 0]
    sorted_names: list[str] = []

    while queue:
        # Stable: prefer original declaration order when multiple nodes are ready
        queue.sort(key=lambda n: names.index(n))
        node = queue.pop(0)
        sorted_names.append(node)
        for dependent in dependents[node]:
            in_degree[dependent] -= 1
            if in_degree[dependent] == 0:
                queue.append(dependent)

    if len(sorted_names) != len(names):
        cycle_nodes = [n for n in names if n not in sorted_names]
        raise RuleCycleError(
            f"Dependency cycle detected among projection rules: {cycle_nodes}. "
            "Split the cycle into separate stages or use kind='transform'."
        )

    return sorted_names
