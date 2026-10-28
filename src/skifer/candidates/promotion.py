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
import re
from typing import Any

from skifer.core.ir import parse_to_ir
from skifer.core.sql_compiler import compile_select, sql_literal
from skifer.semantic.evidence import hash_sql
from skifer.semantic.metric_views import MetricViewTranslationError, metric_view_ddl

from .models import MetricCandidate

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
