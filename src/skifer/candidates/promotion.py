"""Level 1 promotion of a validated candidate (Plan 44, S05).

Level 1 is automatic and **uncertified**: a candidate that passes the three
validation criteria is published as a SQL view, and as a single-table metric view
on top of it, in a schema of its own — ``<schema>_candidates`` — tagged
``certification = CANDIDATE``. The certified metric views schema is never written.

A stored candidate carries ``{{ pN }}`` filters taken from *one* question; they
cannot define a published metric. ``metric_definition`` turns each of them into a
dimension instead, and is the single function shared with level 2 (S06), so that
both levels publish the same SQL.
"""

from __future__ import annotations

import copy
from dataclasses import dataclass, replace
from datetime import datetime
import re
from typing import Any, Callable
import uuid

from skifer.core.ir import parse_to_ir
from skifer.core.sql_compiler import compile_select, sql_literal
from skifer.semantic.evidence import hash_sql
from skifer.semantic.metric_views import MetricViewTranslationError, metric_view_ddl

from .models import ALLOWED_TRANSITIONS, CandidateRun, CertificationEvent, MetricCandidate

# S05.1 / risk R1 — verified live on sts_demo_catalog (Pro/Serverless warehouse,
# 2026-10-28): a metric view accepts a view as its `source`, MEASURE() reads it
# back, and SET TAGS applies to it.
LEVEL1_PUBLISHES_METRIC_VIEW: bool = True
CANDIDATES_SCHEMA_SUFFIX = "_candidates"
# Unity Catalog refuses '.' and '=' in a tag key (verified in the same live test),
# so the plan's `skifer.certification` cannot exist; this is the key the Plan 41
# certification tag mirror already uses.
CANDIDATE_TAG_KEY = "certification"
CANDIDATE_TAG_VALUE = "CANDIDATE"
METRIC_VIEW_SUFFIX = "_metrics"
VALIDATOR_DECIDED_BY = "agent:validator_v1"
SHORTCUT_WEIGHT = 0.8
LOCAL_METRIC_VIEW_NOTE = (
    "metric view skipped: metric views only exist in Unity Catalog (catalog: null)"
)

_PLACEHOLDER = re.compile(r"\{\{\s*p\d+\s*\}\}")
# A measure is re-aggregated by the metric view over the already grouped view:
# only these functions give the same number at every rollup.
_ROLLUP = {"sum": "sum", "count": "sum", "min": "min", "max": "max"}


def _has_placeholder(value: Any) -> bool:
    if isinstance(value, str):
        return bool(_PLACEHOLDER.search(value))
    if isinstance(value, (list, tuple)):
        return any(_has_placeholder(item) for item in value)
    if isinstance(value, dict):
        return any(_has_placeholder(item) for item in value.values())
    return False


def _split_container(container: Any) -> tuple[Any, list[str]]:
    """(container without parameterized filters, their columns in order)."""
    columns: list[str] = []
    if isinstance(container, dict) and not ("column" in container and "operator" in container):
        kept: dict = {}
        for column, value in container.items():
            if isinstance(value, dict):
                operators = {op: v for op, v in value.items() if not _has_placeholder(v)}
                if len(operators) != len(value):
                    columns.append(str(column))
                if operators:
                    kept[column] = operators
            elif _has_placeholder(value):
                columns.append(str(column))
            else:
                kept[column] = value
        return kept, columns
    items = container if isinstance(container, list) else [container]
    kept_items = []
    for item in items:
        if isinstance(item, dict) and "column" in item:
            if _has_placeholder(item.get("value")):
                columns.append(str(item["column"]))
                continue
        elif isinstance(item, str) and _has_placeholder(item):
            columns.append(item.split(":", 1)[0].strip())
            continue
        elif isinstance(item, dict):
            sub, sub_columns = _split_container(item)
            columns.extend(sub_columns)
            if sub:
                kept_items.append(sub)
            continue
        kept_items.append(item)
    return kept_items, columns


def metric_definition(schema_dict: dict) -> tuple[dict, tuple[str, ...]]:
    """The publishable definition of a stored candidate, and the columns converted.

    Every parameterized filter is removed and its column is added to
    ``aggregate.group_by``: the value a single question filtered on becomes a
    dimension of the published metric. Filters without a value (``is_null``…)
    stay. A filter group left empty made its OR always true, so the whole groups
    list goes. A ``having`` filter on a measure is removed without becoming a
    dimension (a measure cannot be grouped by). Pure; ``schema_dict`` is not mutated.
    """
    definition = copy.deepcopy(schema_dict)
    aggregate = definition.get("aggregate")
    if not isinstance(aggregate, dict) or not aggregate.get("measures"):
        raise ValueError("A candidate without an aggregate block does not define a metric.")
    measures = {
        str(m.get("target") if isinstance(m, dict) else m[1]) for m in aggregate["measures"]
    }
    converted: list[str] = []

    def take(container: Any) -> Any:
        kept, columns = _split_container(container)
        converted.extend(columns)
        return kept

    def take_groups(groups: list) -> list | None:
        kept_groups = [take(group) for group in groups]
        return None if any(not group for group in kept_groups) else kept_groups

    for table in definition.get("tables") or []:
        if isinstance(table, dict):
            for key, split in (("filter", take), ("filter_groups", take_groups)):
                if table.get(key):
                    table[key] = split(table[key])
                    if not table[key]:
                        table.pop(key)
    for key, split in (("where", take), ("where_groups", take_groups)):
        if definition.get(key):
            definition[key] = split(definition[key])
            if not definition[key]:
                definition.pop(key)
    if aggregate.get("having"):
        before = len(converted)
        aggregate["having"] = take(aggregate["having"])
        converted[before:] = [c for c in converted[before:] if c not in measures]
        if not aggregate["having"]:
            aggregate.pop("having")

    group_by = list(aggregate.get("group_by") or [])
    for column in converted:
        if column not in group_by:
            group_by.append(column)
    aggregate["group_by"] = group_by
    return definition, tuple(sorted(set(converted) - measures))


def level1_sql(candidate: MetricCandidate) -> str:
    """The SELECT published at level 1 — and, by construction, at level 2."""
    definition, _ = metric_definition(candidate.schema_dict)
    return compile_select(parse_to_ir(definition), allow_raw_sql=False)


def candidates_schema(schema: str) -> str:
    return f"{schema}{CANDIDATES_SCHEMA_SUFFIX}"


def _target(catalog: str | None, schema: str, name: str) -> str:
    parts = ([catalog] if catalog else []) + [candidates_schema(schema), name]
    return ".".join(f"`{part}`" for part in parts)


def level1_view_fqn(candidate: MetricCandidate, catalog: str | None, schema: str) -> str:
    return _target(catalog, schema, candidate.metric_name)


def level1_metric_view_fqn(candidate: MetricCandidate, catalog: str, schema: str) -> str:
    return _target(catalog, schema, candidate.metric_name + METRIC_VIEW_SUFFIX)


def level1_view_ddl(candidate: MetricCandidate, catalog: str | None, schema: str) -> str:
    """``CREATE OR REPLACE VIEW`` in ``<schema>_candidates``, provenance in its comment."""
    sql = level1_sql(candidate)
    comment = f"skifer candidate {candidate.candidate_id} sql_hash={hash_sql(sql)}"
    return (
        f"CREATE OR REPLACE VIEW {level1_view_fqn(candidate, catalog, schema)}\n"
        f"COMMENT {sql_literal(comment)}\nAS {sql}"
    )


def level1_model(candidate: MetricCandidate, schema: str) -> dict:
    """Single-table semantic model on the level 1 view: its measures, its group_by as dimensions."""
    definition, _ = metric_definition(candidate.schema_dict)
    aggregate = definition["aggregate"]
    metrics = []
    for measure in aggregate["measures"]:
        source, target, func = (
            (measure["source"], measure["target"], measure["func"])
            if isinstance(measure, dict) else tuple(measure[:3])
        )
        rollup = _ROLLUP.get(str(func).lower())
        if rollup is None:
            raise MetricViewTranslationError(
                f"[metric_view] Candidate '{candidate.metric_name}', mesure '{target}' : "
                f"'{func}' ne se ré-agrège pas sur une vue déjà groupée."
            )
        metrics.append({"name": target, "sql": target, "type": rollup})
    model = {
        "key": f"candidates.{candidate.metric_name}{METRIC_VIEW_SUFFIX}",
        "table": f"{candidates_schema(schema)}.{candidate.metric_name}",
        "dimensions": [{"name": column, "sql": column} for column in aggregate["group_by"]],
        "metrics": metrics,
    }
    if candidate.metric_description:
        model["description"] = candidate.metric_description
    return model


def level1_metric_view_ddl(candidate: MetricCandidate, catalog: str, schema: str) -> str:
    return metric_view_ddl(level1_model(candidate, schema), catalog, candidates_schema(schema))


def tag_ddl(object_fqn: str) -> str:
    return (
        f"ALTER VIEW {object_fqn} SET TAGS "
        f"({sql_literal(CANDIDATE_TAG_KEY)} = {sql_literal(CANDIDATE_TAG_VALUE)})"
    )


def level1_ddl(candidate: MetricCandidate, catalog: str | None, schema: str) -> tuple[str, ...]:
    """Every level 1 statement, in order: schema, view, [metric view], tags.

    All of them are built before any runs, so a ``MetricViewTranslationError``
    leaves nothing half published. Without a catalog there is no Unity Catalog:
    no metric view and no tags, only the Spark SQL view.
    """
    target_schema = ".".join(
        f"`{part}`" for part in ([catalog] if catalog else []) + [candidates_schema(schema)]
    )
    view = level1_view_fqn(candidate, catalog, schema)
    statements = [f"CREATE SCHEMA IF NOT EXISTS {target_schema}", level1_view_ddl(
        candidate, catalog, schema)]
    if not catalog:
        return tuple(statements)
    tagged = [view]
    if LEVEL1_PUBLISHES_METRIC_VIEW:
        statements.append(level1_metric_view_ddl(candidate, catalog, schema))
        tagged.append(level1_metric_view_fqn(candidate, catalog, schema))
    statements.extend(tag_ddl(fqn) for fqn in tagged)
    return tuple(statements)


def promote_level1(
    candidate: MetricCandidate, *, backend: Any, catalog: str | None, schema: str,
    dry_run: bool = False,
) -> tuple[str, ...]:
    """Emit the level 1 DDL through ``backend.execute_sql``; ``dry_run`` only returns it."""
    statements = level1_ddl(candidate, catalog, schema)
    if not dry_run:
        for statement in statements:
            backend.execute_sql(statement)
    return statements


def _unquoted(fqn: str) -> str:
    return fqn.replace("`", "")


def level1_index_entries(candidate: MetricCandidate) -> tuple[Any, list]:
    """The ``metric`` node S03 finds again, and one ``shortcut`` edge per join."""
    from skifer.knowledge.models import Edge, Node

    text = " ".join(
        part for part in (candidate.metric_description, candidate.origin_question) if part
    )
    node = Node.build(kind="metric", fqn=f"cand:{candidate.metric_name}",
                      label=candidate.metric_name, text=text, status="candidate",
                      source="candidate")
    edges = []
    for left, right, keys in candidate.joins_used:
        left_key, _, right_key = keys.partition("=")
        edges.append(Edge.build(kind="shortcut", from_id=f"table:{left}", to_id=f"table:{right}",
                                keys=(left_key, right_key), weight=SHORTCUT_WEIGHT,
                                source="candidate"))
    return node, edges


@dataclass(frozen=True)
class PromotionResult:
    candidate_id: str
    previous_state: str
    new_state: str
    statements: tuple[str, ...] = ()
    notes: tuple[str, ...] = ()
    error_type: str | None = None


def apply_report(
    report: Any,
    *,
    store: Any,
    backend: Any,
    catalog: str | None,
    schema: str,
    clock: Callable[[], datetime],
    knowledge_store: Any = None,
    dry_run: bool = False,
) -> PromotionResult:
    """Turn a ``ValidationReport`` into transitions, level 1 DDL and index entries.

    ``pending`` changes nothing. ``needs_review`` writes one event. ``validated``
    publishes, then writes ``candidate → validated → promoted`` (or only
    ``validated → promoted`` for a candidate validated by an earlier run whose
    DDL failed). A DDL failure is a ``promotion`` run with its error class; the
    candidate keeps its state. ``dry_run`` writes nothing anywhere.
    """
    candidate = store.get_candidate(report.candidate_id)
    if candidate is None:
        raise KeyError(f"Unknown candidate {report.candidate_id!r}.")
    state = candidate.state
    if report.outcome == "pending":
        return PromotionResult(candidate.candidate_id, state, state)
    if report.outcome == "needs_review":
        return _needs_review(candidate, store, clock, report, report.to_dict(), dry_run=dry_run)

    from skifer.semantic.metric_views import MetricViewTranslationError

    try:
        statements = level1_ddl(candidate, catalog, schema)
    except MetricViewTranslationError as exc:
        evidence = {"reason": "metric_view_translation", "message": str(exc),
                    "report": report.to_dict()}
        return _needs_review(candidate, store, clock, report, evidence, dry_run=dry_run)
    notes = () if catalog else (LOCAL_METRIC_VIEW_NOTE,)
    if dry_run:
        return PromotionResult(candidate.candidate_id, state, "promoted", statements, notes)
    try:
        for statement in statements:
            backend.execute_sql(statement)
    except Exception as exc:
        store.append_run(CandidateRun(
            run_id=uuid.uuid4().hex, candidate_id=candidate.candidate_id, executed_at=clock(),
            purpose="promotion", error_type=type(exc).__name__,
        ))
        return PromotionResult(candidate.candidate_id, state, state, statements, notes,
                               error_type=type(exc).__name__)

    now = clock()
    steps = [("validated", {"outcome": "validated"})] if state == "candidate" else []
    steps.append(("promoted", report.to_dict()))
    previous = state
    for new_state, evidence in steps:
        _event(store, candidate, previous, new_state, evidence, report, now)
        previous = new_state
    published_metric_view = (
        _unquoted(level1_metric_view_fqn(candidate, catalog, schema))
        if catalog and LEVEL1_PUBLISHES_METRIC_VIEW else None
    )
    store.upsert_candidate(replace(
        candidate, state="promoted", updated_at=now,
        published_view=_unquoted(level1_view_fqn(candidate, catalog, schema)),
        published_metric_view=published_metric_view,
    ))
    if knowledge_store is not None:
        node, edges = level1_index_entries(candidate)
        knowledge_store.upsert_nodes([node])
        if edges:
            knowledge_store.upsert_edges(edges)
    return PromotionResult(candidate.candidate_id, state, "promoted", statements, notes)


def _needs_review(
    candidate: MetricCandidate, store: Any, clock: Callable[[], datetime], report: Any,
    evidence: dict, *, dry_run: bool,
) -> PromotionResult:
    state = candidate.state
    if "needs_review" not in ALLOWED_TRANSITIONS[state]:
        return PromotionResult(candidate.candidate_id, state, state)
    if not dry_run:
        now = clock()
        _event(store, candidate, state, "needs_review", evidence, report, now)
        store.upsert_candidate(replace(candidate, state="needs_review", updated_at=now))
    return PromotionResult(candidate.candidate_id, state, "needs_review")


def _event(
    store: Any, candidate: MetricCandidate, previous: str, new_state: str, evidence: dict,
    report: Any, now: datetime,
) -> None:
    store.append_event(CertificationEvent(
        event_id=uuid.uuid4().hex, metric_id=candidate.candidate_id,
        metric_name=candidate.metric_name, event_type=new_state, previous_state=previous,
        new_state=new_state, decided_by=VALIDATOR_DECIDED_BY, justification=None,
        evidence=evidence, replaced_by_metric=None, replaces_metric=None,
        semantic_layer_version=report.semantic_layer_version, trace_id=None, event_at=now,
    ))
