"""Deterministic planning of named semantic queries across curated models."""
from __future__ import annotations

from dataclasses import dataclass
import difflib
import re
from typing import Any, Literal

from ..agentic.resolver import SemanticQueryError
from .calendar import parse_iso_date, resolve_period
from .domain import (
    METRIC_ADDITIVITY,
    RELATIONSHIP_CARDINALITIES,
    RELATIONSHIP_JOIN_TYPES,
    parse_grain,
)
from .domain_graph import DomainEdge, DomainGraph


_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# A relationship duplicates the rows of whichever side it is crossed *from*
# when that side is the "one" side — which flips with the direction of travel.
_FANS_WHEN_TRAVERSED_FORWARD = frozenset({"one_to_many", "many_to_many"})
_FANS_WHEN_TRAVERSED_BACKWARD = frozenset({"many_to_one", "many_to_many"})


@dataclass(frozen=True)
class MetricRef:
    """A metric name resolved to the semantic model that owns it."""

    model_key: str
    name: str
    reference: str


@dataclass(frozen=True)
class DimensionRef:
    """A dimension name resolved to the semantic model that owns it."""

    model_key: str
    name: str
    reference: str


@dataclass(frozen=True)
class PlannedJoin:
    """One ordered, fully declared join in a semantic plan."""

    relationship_name: str
    source_model: str
    target_model: str
    source_entity: str
    target_entity: str
    source_key_columns: tuple[str, ...]
    target_key_columns: tuple[str, ...]
    cardinality: str
    join_type: Literal["inner", "left"]


@dataclass(frozen=True)
class SemanticPlan:
    """Resolved model ownership and join path for one named query."""

    root_model: str
    required_models: tuple[str, ...]
    joins: tuple[PlannedJoin, ...]
    metrics: tuple[MetricRef, ...]
    dimensions: tuple[DimensionRef, ...]
    grain: tuple[str, ...]
    date_from: str | None = None
    date_to: str | None = None
    period_name: str | None = None
    calendar_key: str | None = None
    calendar_version: str | None = None


@dataclass(frozen=True)
class FanoutRisk:
    """The first directional edge that duplicates a metric's source rows."""

    metric: MetricRef
    join: PlannedJoin
    forward: bool


def find_unsafe_fanout(
    metrics: tuple[MetricRef, ...], joins: tuple[PlannedJoin, ...]
) -> FanoutRisk | None:
    """Return the first unsafe directional traversal using planner semantics."""
    if not metrics or not joins:
        return None

    adjacency: dict[str, list[tuple[str, PlannedJoin, bool]]] = {}
    for join in joins:
        adjacency.setdefault(join.source_model, []).append(
            (join.target_model, join, True)
        )
        adjacency.setdefault(join.target_model, []).append(
            (join.source_model, join, False)
        )

    for metric in metrics:
        visited = {metric.model_key}
        frontier = [metric.model_key]
        while frontier:
            current = frontier.pop(0)
            for neighbour, join, forward in adjacency.get(current, ()):
                if neighbour in visited:
                    continue
                fanning = (
                    _FANS_WHEN_TRAVERSED_FORWARD
                    if forward
                    else _FANS_WHEN_TRAVERSED_BACKWARD
                )
                if join.cardinality in fanning:
                    return FanoutRisk(metric=metric, join=join, forward=forward)
                visited.add(neighbour)
                frontier.append(neighbour)
    return None


class SemanticPlanner:
    """Resolve semantic names and declared paths without accepting SQL input."""

    def __init__(self, semantic_engine, graph: DomainGraph | None = None) -> None:
        self.semantic = semantic_engine
        self.graph = graph or DomainGraph(semantic_engine)

    def plan(self, query: Any) -> SemanticPlan:
        """Build a deterministic plan from a ``SemanticQuery`` of names only."""
        root_model = str(query.model_name)
        self._assert_safe_model_key(root_model)
        root = self.semantic._get_model(root_model)
        date_from, date_to, calendar_key, calendar_version = self._resolve_dates(
            query, root
        )

        metrics = tuple(
            self._resolve_metric(reference, root_model)
            for reference in query.metrics
        )
        dimensions = self._resolve_dimensions(query, root_model)

        joins: list[PlannedJoin] = []
        required_models = [root_model]
        seen_joins: set[tuple[str, str, str]] = set()

        owners = self._unique_in_order(
            [ref.model_key for ref in (*metrics, *dimensions)]
        )
        for owner in owners:
            if owner == root_model:
                continue
            paths = self._find_paths(root_model, owner)
            if not paths:
                raise self._no_path_error(metrics, dimensions, root_model, owner)
            if len(paths) > 1:
                rendered = " or ".join(self._render_path(path) for path in paths)
                raise SemanticQueryError(
                    f"Ambiguous semantic join path: {rendered}. "
                    "Curate a single relationship path before querying."
                )

            for edge in paths[0]:
                join_key = (
                    edge.source_model,
                    edge.target_model,
                    edge.relationship.name,
                )
                if join_key in seen_joins:
                    continue
                planned_join = self._plan_join(edge, bool(metrics))
                joins.append(planned_join)
                seen_joins.add(join_key)
                if edge.source_model not in required_models:
                    required_models.append(edge.source_model)
                if edge.target_model not in required_models:
                    required_models.append(edge.target_model)

        planned_joins = tuple(joins)
        self._check_fanout(metrics, planned_joins, root_model)
        self._check_metric_additivity(query, metrics, dimensions, planned_joins)

        return SemanticPlan(
            root_model=root_model,
            required_models=tuple(required_models),
            joins=planned_joins,
            metrics=metrics,
            dimensions=dimensions,
            grain=parse_grain(root),
            date_from=date_from,
            date_to=date_to,
            period_name=getattr(query, "period", None),
            calendar_key=calendar_key,
            calendar_version=calendar_version,
        )

    def _resolve_dates(
        self,
        query: Any,
        root: dict[str, Any],
    ) -> tuple[str | None, str | None, str | None, str | None]:
        period_name = getattr(query, "period", None)
        date_from = getattr(query, "date_from", None)
        date_to = getattr(query, "date_to", None)
        if period_name is not None and (date_from is not None or date_to is not None):
            raise SemanticQueryError(
                "Query cannot combine period with date_from or date_to; choose one "
                "unambiguous date constraint."
            )

        for value, label in ((date_from, "date_from"), (date_to, "date_to")):
            if value is not None:
                try:
                    parse_iso_date(value, label)
                except ValueError as exc:
                    raise SemanticQueryError(str(exc)) from exc

        if period_name is None:
            return date_from, date_to, None, None
        if not isinstance(period_name, str) or not period_name:
            raise SemanticQueryError("Period must be a non-empty declared name.")

        calendar_key = root.get("calendar")
        if not isinstance(calendar_key, str) or not calendar_key:
            raise SemanticQueryError(
                f"Semantic model '{root.get('key') or root.get('name')}' does not "
                f"declare a calendar; period '{period_name}' cannot be resolved."
            )
        self._assert_safe_identifier(calendar_key, "calendar key")
        try:
            calendar = self.semantic._get_calendar(calendar_key)
        except (AttributeError, ValueError) as exc:
            raise SemanticQueryError(str(exc)) from exc

        try:
            period = resolve_period(calendar, period_name)
        except ValueError as exc:
            raise SemanticQueryError(str(exc)) from exc
        return (
            period.start.isoformat(),
            period.end.isoformat(),
            calendar.key,
            calendar.version,
        )

    def _check_fanout(
        self,
        metrics: tuple[MetricRef, ...],
        joins: tuple[PlannedJoin, ...],
        root_model: str,
    ) -> None:
        """Refuse a metric whose rows the join path would silently duplicate.

        Safety is a property of the *direction of travel*, not of the declared
        cardinality alone. Walking outward from the model that owns the metric,
        an edge duplicates that metric when it is crossed from its "one" side
        towards its "many" side. Scanning only for edges declared
        ``one_to_many`` therefore misses two real fanouts: a metric reached
        beyond a fanout through a ``many_to_one`` hop, and a metric that simply
        sits on the one-side of a ``many_to_one`` — both are repeated once per
        row of the many-side.
        """
        risk = find_unsafe_fanout(metrics, joins)
        if risk is not None:
            raise self._fanout_error(risk.metric, risk.join, risk.forward)

    def _fanout_error(
        self,
        metric: MetricRef,
        join: PlannedJoin,
        forward: bool,
    ) -> SemanticQueryError:
        grain = (
            ", ".join(parse_grain(self.semantic._get_model(metric.model_key)))
            or "unknown"
        )
        if forward:
            # Wording mandated by section 7 of the Feature 6 plan. A forward
            # fanout can only be one_to_many here: many_to_many is already
            # refused upstream for any metric query.
            return SemanticQueryError(
                f"Unsafe fanout: metric grain '{grain}' crosses "
                f"{join.cardinality} relationship '{join.relationship_name}'."
            )
        return SemanticQueryError(
            f"Unsafe fanout: metric grain '{grain}' sits on the one-side of "
            f"{join.cardinality} relationship '{join.relationship_name}', so the "
            "join repeats each of its rows. Aggregate the metric in its own "
            "model, or restate the relationship at the metric's grain."
        )

    def _check_metric_additivity(
        self,
        query: Any,
        metrics: tuple[MetricRef, ...],
        dimensions: tuple[DimensionRef, ...],
        joins: tuple[PlannedJoin, ...],
    ) -> None:
        dimension_refs = {ref.reference: ref for ref in dimensions}
        grouped = {
            (dimension_refs[reference].model_key, dimension_refs[reference].name)
            for reference in query.group_by
        }
        equality_filtered = {
            (
                dimension_refs[filter_def["column"]].model_key,
                dimension_refs[filter_def["column"]].name,
            )
            for filter_def in query.filters
            if filter_def.get("column") in dimension_refs
            and filter_def.get("operator") == "eq"
        }

        for metric_ref in metrics:
            model = self.semantic._get_model(metric_ref.model_key)
            metric = self._member_definition(model, "metrics", metric_ref.name)
            additivity = metric.get("additivity", "additive")
            if additivity not in METRIC_ADDITIVITY:
                raise SemanticQueryError(
                    f"Metric '{metric_ref.name}' has invalid additivity "
                    f"'{additivity}'."
                )
            if additivity == "non_additive" and joins:
                raise SemanticQueryError(
                    f"Metric '{metric_ref.name}' is non_additive and cannot be "
                    "computed across a join; query it from model "
                    f"'{metric_ref.model_key}' alone, or materialise it at the "
                    "target grain."
                )
            if additivity != "semi_additive":
                continue

            restricted = metric.get("non_additive_dimensions")
            if not isinstance(restricted, list) or not restricted:
                raise SemanticQueryError(
                    f"Metric '{metric_ref.name}' is semi_additive and must declare "
                    "non_additive_dimensions."
                )
            declared_dimensions = {
                item.get("name")
                for item in model.get("dimensions", [])
                if isinstance(item, dict)
            }
            for dimension_name in restricted:
                if dimension_name not in declared_dimensions:
                    raise SemanticQueryError(
                        f"Metric '{metric_ref.name}' references undeclared non-additive "
                        f"dimension '{dimension_name}'."
                    )
                pin = (metric_ref.model_key, dimension_name)
                if pin not in grouped and pin not in equality_filtered:
                    raise SemanticQueryError(
                        f"Metric '{metric_ref.name}' is semi_additive and cannot be "
                        f"aggregated across dimension '{dimension_name}'; add it to "
                        "group_by or pin it with an equality filter."
                    )

    @staticmethod
    def _member_definition(model: dict, block: str, name: str) -> dict:
        for definition in model.get(block, []):
            if isinstance(definition, dict) and definition.get("name") == name:
                return definition
        raise SemanticQueryError(
            f"Semantic model '{model.get('key') or model.get('name')}' no longer "
            f"declares {block[:-1]} '{name}'."
        )

    def _resolve_metric(self, reference: str, root_model: str) -> MetricRef:
        owner, name = self._resolve_owner(reference, root_model, "metrics", "Metric")
        return MetricRef(model_key=owner, name=name, reference=reference)

    def _resolve_dimension(self, reference: str, root_model: str) -> DimensionRef:
        owner, name = self._resolve_owner(
            reference, root_model, "dimensions", "Dimension"
        )
        return DimensionRef(model_key=owner, name=name, reference=reference)

    def _resolve_dimensions(
        self,
        query: Any,
        root_model: str,
    ) -> tuple[DimensionRef, ...]:
        references = list(query.group_by)
        references.extend(
            filter_def.get("column", "")
            for filter_def in query.filters
            if filter_def.get("column")
        )
        return tuple(
            self._resolve_dimension(reference, root_model)
            for reference in dict.fromkeys(references)
        )

    def _resolve_owner(
        self,
        reference: str,
        root_model: str,
        member_type: str,
        label: str,
    ) -> tuple[str, str]:
        if not isinstance(reference, str) or not reference:
            raise SemanticQueryError(f"{label} reference must be a non-empty name.")

        qualified = self._split_qualified_reference(reference)
        if qualified is not None:
            owner, name = qualified
            members = self._summary_members(owner, member_type)
            if name not in members:
                raise self._unknown_name(label, reference, members, owner)
            return owner, name

        self._assert_safe_identifier(reference, f"{label.lower()} name")
        root_members = self._summary_members(root_model, member_type)
        if reference in root_members:
            # Root ownership is the explicit backwards-compatibility rule.
            return root_model, reference

        candidates = sorted(
            model_key
            for model_key in self.semantic._catalog
            if reference in self._summary_members(model_key, member_type)
        )
        if len(candidates) > 1:
            choices = ", ".join(f"'{key}.{reference}'" for key in candidates)
            raise SemanticQueryError(
                f"Ambiguous {label.lower()} name '{reference}' exists in models "
                f"{candidates}. Qualify it as one of: {choices}."
            )
        if candidates:
            return candidates[0], reference
        return self._raise_unknown_global(label, reference, member_type, root_model)

    def _split_qualified_reference(self, reference: str) -> tuple[str, str] | None:
        matches = [
            model_key
            for model_key in self.semantic._catalog
            if reference.startswith(f"{model_key}.")
        ]
        if not matches:
            return None
        owner = max(matches, key=len)
        name = reference[len(owner) + 1 :]
        self._assert_safe_model_key(owner)
        self._assert_safe_identifier(name, "qualified semantic name")
        return owner, name

    def _summary_members(self, model_key: str, member_type: str) -> tuple[str, ...]:
        try:
            summary = self.semantic.get_model_summary(model_key)
        except ValueError as exc:
            raise SemanticQueryError(str(exc)) from exc
        raw_members = summary.get(member_type) or []
        members = tuple(
            str(member.get("name") if isinstance(member, dict) else member)
            for member in raw_members
        )
        for member in members:
            self._assert_safe_identifier(member, f"catalog {member_type} name")
        return members

    def _find_paths(
        self,
        root_model: str,
        owner: str,
    ) -> tuple[tuple[DomainEdge, ...], ...]:
        try:
            return self.graph.find_paths(root_model, owner)
        except ValueError as exc:
            raise SemanticQueryError(str(exc)) from exc

    def _plan_join(self, edge: DomainEdge, is_metric_query: bool) -> PlannedJoin:
        relationship = edge.relationship
        for value, label in (
            (relationship.name, "relationship name"),
            (edge.source_entity.entity_name, "source entity name"),
            (edge.target_entity.entity_name, "target entity name"),
        ):
            self._assert_safe_identifier(value, label)
        self._assert_safe_model_key(edge.source_model)
        self._assert_safe_model_key(edge.target_model)

        if relationship.cardinality not in RELATIONSHIP_CARDINALITIES:
            raise SemanticQueryError(
                f"Relationship '{relationship.name}' has unsupported cardinality "
                f"'{relationship.cardinality}'."
            )
        if relationship.join_type not in RELATIONSHIP_JOIN_TYPES:
            raise SemanticQueryError(
                f"Relationship '{relationship.name}' has unsupported join type "
                f"'{relationship.join_type}'; expected 'inner' or 'left'."
            )

        if is_metric_query and relationship.cardinality == "unknown":
            # The plan mandates this wording; the offending name is appended
            # rather than interpolated so the sentence stays verbatim while a
            # multi-hop path still tells the author which relationship to fix.
            raise SemanticQueryError(
                "Relationship cardinality is unknown; declare or certify uniqueness "
                f"before querying. Offending relationship: '{relationship.name}'."
            )
        if is_metric_query and relationship.cardinality == "many_to_many":
            # Distinct from `unknown`: the cardinality IS declared. Telling the
            # author to "declare" it sends them to check something they already
            # did — the actual blocker is that many_to_many fans out.
            raise SemanticQueryError(
                f"Relationship '{relationship.name}' is many_to_many, which is not "
                "queryable for a metric because it fans the metric out. Route the "
                "query through a declared bridge model, or restate the relationship "
                "at a grain where it is many_to_one."
            )

        source = self.graph.get_entity(
            edge.source_model, edge.source_entity.entity_name
        )
        target = self.graph.get_entity(
            edge.target_model, edge.target_entity.entity_name
        )
        if len(source.key_columns) != len(target.key_columns):
            raise SemanticQueryError(
                f"Relationship '{relationship.name}' cannot pair composite keys: "
                f"entity '{source.name}' has {len(source.key_columns)} column(s) but "
                f"entity '{target.name}' has {len(target.key_columns)}."
            )
        for column in (*source.key_columns, *target.key_columns):
            self._assert_safe_identifier(column, "relationship key column")

        return PlannedJoin(
            relationship_name=relationship.name,
            source_model=edge.source_model,
            target_model=edge.target_model,
            source_entity=edge.source_entity.entity_name,
            target_entity=edge.target_entity.entity_name,
            source_key_columns=source.key_columns,
            target_key_columns=target.key_columns,
            cardinality=relationship.cardinality,
            join_type=relationship.join_type,
        )

    def _no_path_error(
        self,
        metrics: tuple[MetricRef, ...],
        dimensions: tuple[DimensionRef, ...],
        root_model: str,
        owner: str,
    ) -> SemanticQueryError:
        metric = metrics[0].name if metrics else None
        dimension = next(
            (ref.name for ref in dimensions if ref.model_key == owner),
            dimensions[0].name if dimensions else None,
        )
        if metric and dimension:
            return SemanticQueryError(
                f"No declared relationship connects metric '{metric}' to dimension "
                f"'{dimension}'. Curate a relationship between their semantic models."
            )
        requested = metric or dimension or owner
        return SemanticQueryError(
            f"No declared relationship connects root model '{root_model}' to "
            f"semantic object '{requested}' in model '{owner}'."
        )

    def _unknown_name(
        self,
        label: str,
        reference: str,
        members: tuple[str, ...],
        owner: str,
    ) -> SemanticQueryError:
        suggestion = self._suggestion(reference.rsplit(".", 1)[-1], members)
        return SemanticQueryError(
            f"{label} '{reference}' is not declared in semantic model '{owner}'. "
            f"{suggestion}Available: {sorted(members)}"
        )

    def _raise_unknown_global(
        self,
        label: str,
        reference: str,
        member_type: str,
        root_model: str,
    ) -> tuple[str, str]:
        available = sorted(
            {
                member
                for model_key in self.semantic._catalog
                for member in self._summary_members(model_key, member_type)
            }
        )
        suggestion = self._suggestion(reference, tuple(available))
        raise SemanticQueryError(
            f"{label} '{reference}' is not declared in root model '{root_model}' or "
            f"any catalog model. {suggestion}Available: {available}"
        )

    @staticmethod
    def _suggestion(reference: str, available: tuple[str, ...]) -> str:
        matches = difflib.get_close_matches(reference, available, n=3, cutoff=0.6)
        return f"Did you mean {matches}? " if matches else ""

    @staticmethod
    def _render_path(path: tuple[DomainEdge, ...]) -> str:
        models = [path[0].source_model, *(edge.target_model for edge in path)]
        rendered = "→".join(models)
        relationship_names = tuple(edge.relationship.name for edge in path)
        if len(set(models)) != len(models) or len(set(relationship_names)) < len(path):
            return f"{rendered} ({'→'.join(relationship_names)})"
        return rendered

    @staticmethod
    def _unique_in_order(values: list[str]) -> tuple[str, ...]:
        return tuple(dict.fromkeys(values))

    @staticmethod
    def _assert_safe_identifier(value: str, label: str) -> None:
        if not _SAFE_IDENTIFIER.fullmatch(value):
            raise SemanticQueryError(
                f"Unsafe {label} '{value}'; expected a SQL-safe identifier."
            )

    @classmethod
    def _assert_safe_model_key(cls, model_key: str) -> None:
        parts = model_key.split(".")
        if not parts or any(not _SAFE_IDENTIFIER.fullmatch(part) for part in parts):
            raise SemanticQueryError(
                f"Unsafe semantic model key '{model_key}'; each component must be a "
                "SQL-safe identifier."
            )
