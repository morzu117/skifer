"""Fallback agent for questions the semantic layer cannot answer (Plan 44, S03).

The LLM never writes executed SQL. It proposes a ``schema_dict`` in Skifer's own
YAML grammar — or, failing that, a ``SELECT`` that the Plan 43 mapper translates
into one — and what runs is always ``compile_select`` of the validated schema.
The answer is marked ``CANDIDATE``: a candidate metric is persisted without any
filter value, and a refusal says what is missing instead of guessing.
"""

from __future__ import annotations

import copy
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from enum import Enum
import hashlib
import json
import re
import threading
import time
from typing import Any
import uuid

import yaml

from skifer.candidates.models import (
    ALLOWED_TRANSITIONS,
    CandidateRun,
    CertificationEvent,
    MetricCandidate,
    candidate_id_for,
    fingerprint_candidate,
)
from skifer.candidates.redaction import (
    _shape_items,
    _walk_filter_locations,
    parameterize_filters,
    question_hash,
    requester_pseudonym,
)
from skifer.core.ir import parse_to_ir
from skifer.core.op_catalog import AGGREGATE_FUNCTIONS, COLUMN_OPS, FILTER_OPERATORS
from skifer.core.schema_loader import parse_schema
from skifer.core.sql_compiler import compile_select
from skifer.observability.tracing import NoOpTracer, configured_span_scope, set_span_attribute
from skifer.semantic.access_policy import SemanticAccessDenied
from skifer.semantic.dependencies import SemanticDependency
from skifer.semantic.evidence import MetricEvidence, SemanticEvidence, hash_sql
from skifer.semantic.domain import parse_relationships
from skifer.semantic.planner import MetricRef, PlannedJoin, SemanticPlanner, find_unsafe_fanout

from .models import AgentResponse, FormattedResult, ResponseFormat


CANDIDATE_SPAN_NAME = "skifer.candidate.build"
MAX_CORRECTION_ATTEMPTS = 3
ALLOWED_SCHEMA_KEYS = frozenset({
    "tables", "join", "where", "where_groups", "select_final", "add_columns", "aggregate",
})
# Excluded: partials, business_rules, sink, materialization, data_product, contract,
# semantic, keep_all_columns, and a table-level ``source`` — a candidate only reads
# catalog tables.
ALLOWED_TABLE_KEYS = frozenset({"name", "alias", "filter", "filter_groups"})
RESPONSE_KEYS = frozenset({"metric_name", "metric_description", "schema_dict", "sql"})
DECIDED_BY = "agent:candidate_agent_v1"

SEARCH_K = 20
MAX_CONTEXT_TABLES = 8
MAX_CONTEXT_COLUMNS = 40
_FENCE_RE = re.compile(r"^\s*```(?:json)?\s*|\s*```\s*$", re.IGNORECASE)


class CandidateRefusal(str, Enum):
    NO_TABLE_FOUND = "no_table_found"
    UNKNOWN_TABLE = "unknown_table"
    JOIN_WITHOUT_KEY = "join_without_key"
    UNSAFE_FANOUT = "unsafe_fanout"
    RAW_SQL_REJECTED = "raw_sql_rejected"
    BLOCKING_RESIDUE = "blocking_residue"
    SQL_PATH_UNAVAILABLE = "sql_path_unavailable"
    INVALID_AFTER_RETRIES = "invalid_after_retries"
    EXECUTION_FAILED = "execution_failed"


_RECOMMENDED_ACTIONS = {
    CandidateRefusal.NO_TABLE_FOUND: (
        "Index the tables that hold this data ('skifer knowledge sync') or add the metric "
        "to a semantic model."
    ),
    CandidateRefusal.UNKNOWN_TABLE: (
        "Sync the table into the knowledge index, or ask about a table it already knows."
    ),
    CandidateRefusal.JOIN_WITHOUT_KEY: "Declare the join keys between these tables.",
    CandidateRefusal.UNSAFE_FANOUT: (
        "Aggregate the metric at its own grain first, or declare a safe relationship."
    ),
    CandidateRefusal.RAW_SQL_REJECTED: (
        "Express the logic with catalog operators, or write it as a governed rule."
    ),
    CandidateRefusal.BLOCKING_RESIDUE: (
        "Materialize this construct upstream in a pipeline, then ask again."
    ),
    CandidateRefusal.SQL_PATH_UNAVAILABLE: "Install the optional '[analyzer]' extra.",
    CandidateRefusal.INVALID_AFTER_RETRIES: (
        "Rephrase the question, or add the metric to a semantic model."
    ),
    CandidateRefusal.EXECUTION_FAILED: (
        "Check that the source tables are readable, then ask again."
    ),
}


class ProposalError(ValueError):
    """A rejected proposal; the message is sent back to the LLM when ``retryable``."""

    def __init__(self, code: CandidateRefusal, message: str, *, retryable: bool = True):
        super().__init__(message)
        self.code = code
        self.retryable = retryable


@dataclass(frozen=True)
class ValidatedProposal:
    """A proposal that passed every check; ``schema_dict`` is ``parse_schema``'s output."""

    metric_name: str
    metric_description: str | None
    schema_dict: dict
    origin: str                                   # "schema_dict" | "sql"
    residues: tuple[dict, ...]
    tables_used: tuple[str, ...]
    joins_used: tuple[tuple[str, str, str], ...]
    coherence: dict = field(default_factory=dict)
    attempts: int = 1


class _GrainSource:
    """Lets ``SemanticPlanner._fanout_error`` word its message for a table without a model."""

    def __init__(self, semantic: Any):
        self._semantic = semantic

    def _get_model(self, model_key: str) -> dict:
        try:
            return self._semantic._get_model(model_key)
        except Exception:
            return {}


_NO_DOMAIN_GRAPH = object()   # the planner is only used to word a fanout message


def _system_prompt() -> str:
    operators = sorted(name for name in FILTER_OPERATORS if name != "sql")
    column_ops = sorted(name for name in COLUMN_OPS if name != "expr")
    return (
        "You answer a business question that no certified metric covers, by proposing a "
        "metric in Skifer's declarative pipeline grammar. You never write executed SQL.\n"
        "Reply with ONE JSON object and nothing else, either\n"
        '  {"metric_name": "<identifier>", "metric_description": "<one sentence>", '
        '"schema_dict": {...}}\n'
        "or, only if the grammar cannot express it,\n"
        '  {"metric_name": "<identifier>", "metric_description": "<one sentence>", '
        '"sql": "SELECT ..."}\n'
        f"schema_dict keys allowed: {', '.join(sorted(ALLOWED_SCHEMA_KEYS))}.\n"
        f"Table keys allowed: {', '.join(sorted(ALLOWED_TABLE_KEYS))}; every table must be "
        "one of the tables listed in the context, by its exact name.\n"
        "Filters are 'column:operator[:value]' strings; operators: "
        f"{', '.join(operators)}.\n"
        f"Column operations: {', '.join(column_ops)}.\n"
        f"Aggregate functions: {', '.join(sorted(AGGREGATE_FUNCTIONS))}.\n"
        "Every join names its keys: {\"table_from\": [alias, key], \"table_to\": [alias, key], "
        "\"type\": \"left\"}.\n"
        "Never use 'expr:', the 'sql' filter operator, business_rules, partials, sinks or "
        "sources. metric_name matches ^[A-Za-z_][A-Za-z0-9_]*$."
    )


def parse_proposal(text: str) -> dict[str, Any]:
    """Closed JSON: exactly a metric name, an optional description, and one body."""
    if not isinstance(text, str):
        raise ProposalError(CandidateRefusal.INVALID_AFTER_RETRIES, "The answer is not text.")
    try:
        payload = json.loads(_FENCE_RE.sub("", text.strip()))
    except json.JSONDecodeError as exc:
        raise ProposalError(
            CandidateRefusal.INVALID_AFTER_RETRIES, f"The answer is not valid JSON: {exc.msg}."
        ) from None
    if not isinstance(payload, dict):
        raise ProposalError(CandidateRefusal.INVALID_AFTER_RETRIES, "The answer must be a JSON object.")
    unknown = sorted(set(payload) - RESPONSE_KEYS)
    if unknown:
        raise ProposalError(
            CandidateRefusal.INVALID_AFTER_RETRIES,
            f"Unknown keys {unknown}; allowed: {sorted(RESPONSE_KEYS)}.",
        )
    if ("schema_dict" in payload) == ("sql" in payload):
        raise ProposalError(
            CandidateRefusal.INVALID_AFTER_RETRIES,
            "Give exactly one of 'schema_dict' or 'sql'.",
        )
    name = payload.get("metric_name")
    if not isinstance(name, str) or not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", name):
        raise ProposalError(
            CandidateRefusal.INVALID_AFTER_RETRIES,
            "metric_name must match ^[A-Za-z_][A-Za-z0-9_]*$.",
        )
    description = payload.get("metric_description")
    if description is not None and not isinstance(description, str):
        raise ProposalError(
            CandidateRefusal.INVALID_AFTER_RETRIES, "metric_description must be a string."
        )
    if "schema_dict" in payload and not isinstance(payload["schema_dict"], dict):
        raise ProposalError(CandidateRefusal.INVALID_AFTER_RETRIES, "schema_dict must be an object.")
    if "sql" in payload and (not isinstance(payload["sql"], str) or not payload["sql"].strip()):
        raise ProposalError(CandidateRefusal.INVALID_AFTER_RETRIES, "sql must be a non-empty string.")
    return payload


class MetricCandidateAgent:
    """Answer outside the semantic layer with a governed, persisted candidate metric."""

    def __init__(
        self,
        semantic_engine: Any,
        llm_provider: Any,
        index: Any,
        store: Any,
        *,
        policy: Any = None,
        row_limit: int = 1000,
        timeout_seconds: int = 60,
        tracer: Any = None,
        clock: Any = None,
    ):
        if isinstance(row_limit, bool) or not isinstance(row_limit, int) or row_limit < 1:
            raise ValueError(f"row_limit must be a positive integer; got {row_limit!r}.")
        if (
            isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, (int, float))
            or timeout_seconds <= 0
        ):
            raise ValueError(f"timeout_seconds must be positive; got {timeout_seconds!r}.")
        self.semantic = semantic_engine
        self.llm = llm_provider
        self.index = index
        self.store = store
        self.policy = policy
        self.row_limit = row_limit
        self.timeout_seconds = timeout_seconds
        self._tracer = tracer
        self._clock = clock

    # --- context -----------------------------------------------------------------

    def build_context(self, hits: list) -> dict[str, Any]:
        """Compact context in score order: at most 8 tables and 40 columns per table."""
        tables: list[str] = []

        def add_table(fqn: str) -> None:
            if fqn not in tables and self.index.get(f"table:{fqn}") is not None:
                tables.append(fqn)

        for hit in hits:
            node = hit.node
            if node.kind == "table":
                add_table(node.fqn)
            elif node.kind == "column":
                add_table(node.fqn.rsplit(".", 1)[0])
            elif node.kind == "metric":
                for owner, _ in self.index.neighbors(node.node_id, max_depth=1, kinds=["table"]):
                    add_table(owner.fqn)
        tables = tables[:MAX_CONTEXT_TABLES]

        hit_rank = {hit.node.node_id: rank for rank, hit in enumerate(hits)}
        table_entries = []
        for fqn in tables:
            table = self.index.get(f"table:{fqn}")
            columns = [
                node for node, _ in self.index.neighbors(table.node_id, max_depth=1, kinds=["column"])
                if node.fqn.rsplit(".", 1)[0] == fqn
            ]
            columns.sort(key=lambda node: (hit_rank.get(node.node_id, len(hits)), node.node_id))
            table_entries.append({
                "name": fqn,
                "description": table.text,
                "columns": [
                    {"name": column.label, "about": column.text}
                    for column in columns[:MAX_CONTEXT_COLUMNS]
                ],
            })

        metrics = [hit.node for hit in hits if hit.node.kind == "metric"]
        for fqn in tables:
            for node, _ in self.index.neighbors(f"table:{fqn}", max_depth=1, kinds=["metric"]):
                if node not in metrics:
                    metrics.append(node)
        promoted = [node for node in metrics if node.source == "candidate"]
        certified = [node for node in metrics if node.source != "candidate"]

        table_ids = {f"table:{fqn}" for fqn in tables}
        column_ids = {
            node.node_id
            for fqn in tables
            for node, _ in self.index.neighbors(f"table:{fqn}", max_depth=1, kinds=["column"])
        }
        joins = [
            {
                "from": edge.from_id.split(":", 1)[1],
                "to": edge.to_id.split(":", 1)[1],
                "keys": list(edge.keys),
            }
            for edge in self.index.edges(node_ids=table_ids | column_ids, kinds=["joins", "shortcut"])
        ]
        return {
            "promoted_candidates": [_metric_entry(node) for node in promoted],
            "tables": table_entries,
            "metrics": [_metric_entry(node) for node in certified],
            "rules": [
                {"name": hit.node.label, "description": hit.node.text}
                for hit in hits if hit.node.kind == "rule"
            ],
            "joins": joins,
        }

    @staticmethod
    def build_user_message(question: str, context: dict[str, Any], error: str | None = None) -> str:
        message = (
            f"Question: {question}\n\n"
            "Context (the only tables you may use):\n"
            # Key order is meaning here: promoted candidates come first in the prompt.
            f"{json.dumps(context, ensure_ascii=False, indent=1)}"
        )
        if error:
            message += (
                f"\n\nYour previous answer was rejected: {error}\n"
                "Return a corrected JSON object."
            )
        return message

    # --- validation ------------------------------------------------------------------

    def validate(self, payload: dict[str, Any], *, attempts: int = 1) -> ValidatedProposal:
        """Steps 4.1–4.8 of the spec; raises ``ProposalError`` on the first failure."""
        residues: tuple[dict, ...] = ()
        if "sql" in payload:
            schema, residues = self._schema_from_sql(payload["sql"])
            origin = "sql"
        else:
            schema, origin = copy.deepcopy(payload["schema_dict"]), "schema_dict"
        self._check_keys(schema)
        self._check_tables(schema)
        self._check_raw_sql(schema)
        try:
            parsed = parse_schema(yaml.safe_dump(schema, sort_keys=False, allow_unicode=True))
        except Exception as exc:
            raise ProposalError(
                CandidateRefusal.INVALID_AFTER_RETRIES,
                f"The schema validator rejected the schema_dict: {type(exc).__name__}: {exc}",
            ) from None
        aliases = {
            key: table["name"]
            for table in parsed["tables"]
            for key in (table.get("alias") or table["name"], table["name"])
        }
        joins_used, planned, undeclared = self._check_joins(parsed, aliases)
        self._check_fanout(parsed, aliases, planned)
        coherence = {"undeclared_join": undeclared} if undeclared else {}
        return ValidatedProposal(
            metric_name=payload["metric_name"],
            metric_description=payload.get("metric_description"),
            schema_dict=parsed,
            origin=origin,
            residues=residues,
            tables_used=tuple(sorted({table["name"] for table in parsed["tables"]})),
            joins_used=joins_used,
            coherence=coherence,
            attempts=attempts,
        )

    @staticmethod
    def _schema_from_sql(sql: str) -> tuple[dict, tuple[dict, ...]]:
        """Translate a SELECT with the Plan 43 mapper; the SQL text itself never runs."""
        try:
            import sqlglot
            from skifer.analyzer.frontends import split_sql_text
            from skifer.analyzer.mapper import map_statement
            from skifer.analyzer.parser import DEFAULT_DIALECT, Statement
        except ImportError:
            raise ProposalError(
                CandidateRefusal.SQL_PATH_UNAVAILABLE,
                "The 'sql' answer form needs the optional [analyzer] extra, which is not "
                "installed; answer with a 'schema_dict' instead.",
            ) from None
        statements = split_sql_text(sql, "<candidate>")
        if len(statements) != 1:
            raise ProposalError(
                CandidateRefusal.INVALID_AFTER_RETRIES, "The 'sql' form holds exactly one SELECT."
            )
        try:
            ast = sqlglot.parse_one(statements[0].text, dialect=DEFAULT_DIALECT)
        except Exception as exc:
            raise ProposalError(
                CandidateRefusal.INVALID_AFTER_RETRIES,
                f"The SQL does not parse: {type(exc).__name__}.",
            ) from None
        mapped = map_statement(
            Statement(source_path="<candidate>", line=1, text=statements[0].text, ast=ast)
        )
        blocking = mapped.blocking_residues
        if blocking:
            names = ", ".join(f"'{residue.code}' ({residue.message})" for residue in blocking)
            raise ProposalError(
                CandidateRefusal.BLOCKING_RESIDUE,
                f"The SQL has no declarative equivalent: blocking residue {names}.",
            )
        if mapped.schema_dict is None:
            raise ProposalError(
                CandidateRefusal.INVALID_AFTER_RETRIES, "The SQL could not be translated."
            )
        residues = tuple(
            {"code": residue.code, "message": residue.message, "blocking": False}
            for residue in mapped.residues
        )
        return mapped.schema_dict, residues

    @staticmethod
    def _check_keys(schema: Any) -> None:
        if not isinstance(schema, dict):
            raise ProposalError(CandidateRefusal.INVALID_AFTER_RETRIES, "schema_dict must be an object.")
        forbidden = sorted(set(schema) - ALLOWED_SCHEMA_KEYS)
        if forbidden:
            raise ProposalError(
                CandidateRefusal.INVALID_AFTER_RETRIES,
                f"Keys not allowed in a candidate: {forbidden}; allowed: "
                f"{sorted(ALLOWED_SCHEMA_KEYS)}.",
            )
        tables = schema.get("tables")
        if not isinstance(tables, list) or not tables:
            raise ProposalError(
                CandidateRefusal.INVALID_AFTER_RETRIES, "'tables' must be a non-empty list."
            )
        for table in tables:
            if not isinstance(table, dict):
                raise ProposalError(
                    CandidateRefusal.INVALID_AFTER_RETRIES, "Each table must be an object."
                )
            extra = sorted(set(table) - ALLOWED_TABLE_KEYS)
            if extra:
                raise ProposalError(
                    CandidateRefusal.INVALID_AFTER_RETRIES,
                    f"Table keys not allowed in a candidate: {extra}; allowed: "
                    f"{sorted(ALLOWED_TABLE_KEYS)}.",
                )

    def _check_tables(self, schema: dict) -> None:
        for table in schema["tables"]:
            name = table.get("name")
            if not isinstance(name, str) or self.index.get(f"table:{name}") is None:
                raise ProposalError(
                    CandidateRefusal.UNKNOWN_TABLE,
                    f"Table {name!r} is not in the knowledge index; use only the context tables.",
                )

    @staticmethod
    def _check_raw_sql(schema: dict) -> None:
        """No ``expr:`` anywhere and no ``sql`` filter, whatever ``allow_raw_sql`` says."""
        def walk(value: Any) -> None:
            if isinstance(value, str):
                if value.strip().lower().startswith("expr:"):
                    raise ProposalError(
                        CandidateRefusal.RAW_SQL_REJECTED,
                        "'expr:' is not allowed in a candidate; use catalog operations.",
                    )
            elif isinstance(value, dict):
                if any(str(key).strip().lower() == "expr" for key in value):
                    raise ProposalError(
                        CandidateRefusal.RAW_SQL_REJECTED,
                        "'expr' is not allowed in a candidate; use catalog operations.",
                    )
                for item in value.values():
                    walk(item)
            elif isinstance(value, list):
                for item in value:
                    walk(item)

        walk(schema)
        operators: list[str] = []

        def collect(container: Any) -> Any:
            operators.extend(
                shape["operator"] for shape in _shape_items(container) if "operator" in shape
            )
            return container

        _walk_filter_locations(copy.deepcopy(schema), collect)
        if "sql" in operators:
            raise ProposalError(
                CandidateRefusal.RAW_SQL_REJECTED,
                "The 'sql' filter operator is not allowed in a candidate.",
            )

    # --- joins and fanout ------------------------------------------------------------

    def _check_joins(
        self, parsed: dict, aliases: dict[str, str]
    ) -> tuple[tuple[tuple[str, str, str], ...], tuple[PlannedJoin, ...], list[str]]:
        joins_used, planned, undeclared = [], [], []
        for join in parsed.get("join") or []:
            left = aliases.get(join.get("table_from"))
            right = aliases.get(join.get("table_to"))
            left_key, right_key = join.get("on_from"), join.get("on_to")
            if (
                str(join.get("type") or "left").lower() == "cross"
                or not left_key or not right_key
                or not isinstance(left_key, str) or not isinstance(right_key, str)
            ):
                raise ProposalError(
                    CandidateRefusal.JOIN_WITHOUT_KEY,
                    f"The join between {join.get('table_from')!r} and {join.get('table_to')!r} "
                    "names no key.",
                    retryable=False,
                )
            if left is None or right is None:
                raise ProposalError(
                    CandidateRefusal.INVALID_AFTER_RETRIES,
                    "A join references an alias that is not among the tables.",
                )
            joins_used.append((left, right, f"{left_key}={right_key}"))
            relationship = self._relationship(left, left_key, right, right_key)
            if relationship is not None:
                planned.append(relationship)
                continue
            if not self._edge_declares(left, left_key, right, right_key):
                undeclared.append(f"{left}.{left_key}={right}.{right_key}")
            planned.append(PlannedJoin(
                relationship_name=f"{left}.{left_key}={right}.{right_key}",
                source_model=self._model_node(left), target_model=self._model_node(right),
                source_entity=left_key, target_entity=right_key,
                source_key_columns=(left_key,), target_key_columns=(right_key,),
                cardinality="unknown", join_type="left",
            ))
        return tuple(joins_used), tuple(planned), undeclared

    def _edge_declares(self, left: str, left_key: str, right: str, right_key: str) -> bool:
        ends = {(left, left_key), (right, right_key)}
        for edge in self.index.edges(kinds=["joins", "shortcut"]):
            if edge.kind == "shortcut" and len(edge.keys) == 2:
                pair = {
                    (edge.from_id.split(":", 1)[1], edge.keys[0]),
                    (edge.to_id.split(":", 1)[1], edge.keys[1]),
                }
            elif edge.kind == "joins":
                pair = {
                    tuple(edge.from_id.split(":", 1)[1].rsplit(".", 1)),
                    tuple(edge.to_id.split(":", 1)[1].rsplit(".", 1)),
                }
            else:
                continue
            if pair == ends:
                return True
        return False

    def _semantic_models(self, table: str) -> list[tuple[str, dict]]:
        """Semantic models over ``table``, found through the index — loaded lazily, one by one."""
        if self.semantic is None:
            return []
        keys = sorted({
            node.fqn.rsplit(".", 1)[0]
            for node, _ in self.index.neighbors(
                f"table:{table}", max_depth=1, kinds=["metric", "dimension"]
            )
            if node.source == "semantic"
        })
        models = []
        for key in keys:
            try:
                model = self.semantic._get_model(key)
            except Exception:
                continue
            if model.get("table") == table:
                models.append((key, model))
        return models

    def _model_node(self, table: str) -> str:
        models = self._semantic_models(table)
        return models[0][0] if len(models) == 1 else f"table:{table}"

    def _relationship(
        self, left: str, left_key: str, right: str, right_key: str
    ) -> PlannedJoin | None:
        ends = {(left, left_key), (right, right_key)}
        for table in (left, right):
            for key, model in self._semantic_models(table):
                entity_keys = {
                    entity.get("name"): entity.get("key") for entity in model.get("entities") or []
                }
                for relationship in parse_relationships(model):
                    target_key = relationship.to_entity.model_key
                    try:
                        target = self.semantic._get_model(target_key)
                    except Exception:
                        continue
                    target_entities = {
                        entity.get("name"): entity.get("key")
                        for entity in target.get("entities") or []
                    }
                    from_column = entity_keys.get(relationship.from_entity.entity_name)
                    to_column = target_entities.get(relationship.to_entity.entity_name)
                    if {(table, from_column), (target.get("table"), to_column)} != ends:
                        continue
                    return PlannedJoin(
                        relationship_name=relationship.name,
                        source_model=key, target_model=target_key,
                        source_entity=relationship.from_entity.entity_name,
                        target_entity=relationship.to_entity.entity_name,
                        source_key_columns=(str(from_column),),
                        target_key_columns=(str(to_column),),
                        cardinality=relationship.cardinality,
                        join_type="inner" if relationship.join_type == "inner" else "left",
                    )
        return None

    def _check_fanout(
        self, parsed: dict, aliases: dict[str, str], planned: tuple[PlannedJoin, ...]
    ) -> None:
        aggregate = parsed.get("aggregate")
        if not planned or not isinstance(aggregate, dict):
            return
        tables = sorted({table["name"] for table in parsed["tables"]})
        metrics = []
        for measure in aggregate.get("measures") or []:
            source = measure.get("source") if isinstance(measure, dict) else None
            if not isinstance(source, str) or source == "*":
                continue
            owners = [t for t in tables if self.index.get(f"column:{t}.{source}") is not None]
            if len(owners) != 1:
                continue
            model = self._model_node(owners[0])
            metrics.append(MetricRef(model_key=model, name=str(measure.get("target")),
                                     reference=str(measure.get("target"))))
        risk = find_unsafe_fanout(tuple(metrics), planned)
        if risk is None:
            return
        planner = SemanticPlanner(_GrainSource(self.semantic), graph=_NO_DOMAIN_GRAPH)
        message = str(planner._fanout_error(risk.metric, risk.join, risk.forward))
        raise ProposalError(CandidateRefusal.UNSAFE_FANOUT, message, retryable=False)

    # --- correction loop -------------------------------------------------------------

    def propose(self, question: str, hits: list) -> ValidatedProposal | AgentResponse:
        """At most ``MAX_CORRECTION_ATTEMPTS`` LLM calls; a correctable error is sent back."""
        context = self.build_context(hits)
        system = _system_prompt()
        error: ProposalError | None = None
        for attempt in range(1, MAX_CORRECTION_ATTEMPTS + 1):
            text = self.llm.complete(
                system, self.build_user_message(question, context, None if error is None else str(error)),
                temperature=0.0,
            )
            try:
                return self.validate(parse_proposal(text), attempts=attempt)
            except ProposalError as exc:
                if not exc.retryable:
                    return self._refusal(question, exc.code, str(exc))
                error = exc
        return self._refusal(
            question, error.code,
            f"No valid metric after {MAX_CORRECTION_ATTEMPTS} attempts; last error: {error}",
        )

    # --- execution -------------------------------------------------------------------

    def compile(self, proposal: ValidatedProposal) -> str:
        """The only SQL that ever runs: ``compile_select`` of the validated schema."""
        return compile_select(parse_to_ir(proposal.schema_dict), allow_raw_sql=False)

    def _execute(self, sql: str) -> tuple[Any, list[dict[str, Any]]]:
        """``EXPLAIN`` then the statement, bounded by ``row_limit`` and ``timeout_seconds``.

        On timeout the worker thread is abandoned, not killed: the caller gets its
        answer on time and the backend finishes or cancels the job on its own.
        """
        backend = self.semantic.core._get_backend()
        box: dict[str, Any] = {}

        def run() -> None:
            try:
                backend.execute_sql("EXPLAIN " + sql)
                frame = backend.execute_sql(sql).limit(self.row_limit)
                box["rows"] = [_row_dict(row) for row in frame.collect()]
                box["frame"] = frame
            except BaseException as exc:  # re-raised in the caller's thread
                box["error"] = exc

        worker = threading.Thread(target=run, name="skifer-candidate-execution", daemon=True)
        worker.start()
        worker.join(self.timeout_seconds)
        if worker.is_alive():
            raise TimeoutError()
        if "error" in box:
            raise box["error"]
        return box["frame"], box["rows"]

    # --- persistence -----------------------------------------------------------------

    def _now(self) -> datetime:
        return self._clock() if self._clock is not None else datetime.now(timezone.utc)

    def _columns_used(self, proposal: ValidatedProposal) -> tuple[str, ...]:
        schema = proposal.schema_dict
        names: set[str] = set()

        def collect(container: Any) -> Any:
            names.update(shape["column"] for shape in _shape_items(container) if "column" in shape)
            return container

        _walk_filter_locations(copy.deepcopy(schema), collect)
        for join in schema.get("join") or []:
            names.update(str(join[key]) for key in ("on_from", "on_to") if join.get(key))
        aggregate = schema.get("aggregate") or {}
        names.update(str(column) for column in aggregate.get("group_by") or [])
        for measure in aggregate.get("measures") or []:
            if isinstance(measure, dict) and measure.get("source") not in (None, "*"):
                names.add(str(measure["source"]))
        for key in ("select_final", "add_columns"):
            for item in schema.get(key) or []:
                source = item.get("source") if isinstance(item, dict) else (
                    item[0] if isinstance(item, list) and item else None
                )
                if isinstance(source, str):
                    names.add(source)
        return tuple(sorted(
            f"{table}.{name}"
            for name in names
            for table in proposal.tables_used
            if self.index.get(f"column:{table}.{name}") is not None
        ))

    def _persist(
        self,
        question: str,
        proposal: ValidatedProposal,
        *,
        requester_id: str | None,
        trace_id: str | None,
        now: datetime,
    ) -> tuple[MetricCandidate, bool]:
        """Create the candidate or count one more request; never stores a filter value."""
        schema, values = parameterize_filters(proposal.schema_dict)
        fingerprint = fingerprint_candidate(schema)
        candidate_id = candidate_id_for(fingerprint)
        requester = requester_pseudonym(requester_id, self.policy)
        existing = self.store.find_by_fingerprint(fingerprint)
        if existing is not None:
            requesters = existing.distinct_requesters
            if requester is not None and requester not in requesters:
                requesters = tuple(sorted((*requesters, requester)))
            candidate = replace(
                existing,
                request_count=existing.request_count + 1,
                distinct_requesters=requesters,
                last_requested_at=now,
                updated_at=now,
            )
            self.store.upsert_candidate(candidate)
            return candidate, False
        candidate = MetricCandidate(
            candidate_id=candidate_id,
            metric_name=proposal.metric_name,
            metric_description=proposal.metric_description,
            origin_question=_without_values(question, values),
            origin_question_hash=question_hash(question),
            schema_dict=schema,
            sql_hash=hash_sql(compile_select(parse_to_ir(schema), allow_raw_sql=False)),
            fingerprint=fingerprint,
            tables_used=proposal.tables_used,
            columns_used=self._columns_used(proposal),
            joins_used=proposal.joins_used,
            business_rules_refs=(),
            residues=tuple(
                {**residue, "message": _without_values(residue["message"], values)}
                for residue in proposal.residues
            ),
            state="candidate",
            request_count=1,
            distinct_requesters=() if requester is None else (requester,),
            stability_score=None,
            coherence=proposal.coherence or None,
            trace_id=trace_id,
            reviewer=None,
            published_view=None,
            published_metric_view=None,
            created_at=now,
            updated_at=now,
            last_requested_at=now,
        )
        self.store.upsert_candidate(candidate)
        self.store.append_event(CertificationEvent(
            event_id=uuid.uuid4().hex,
            metric_id=candidate_id,
            metric_name=proposal.metric_name,
            event_type="created",
            previous_state=None,
            new_state="candidate",
            decided_by=DECIDED_BY,
            justification=None,
            evidence={"origin": proposal.origin, "attempts": proposal.attempts},
            replaced_by_metric=None,
            replaces_metric=None,
            semantic_layer_version=None,
            trace_id=trace_id,
            event_at=now,
        ))
        return candidate, True

    def _record_failure(
        self, proposal: ValidatedProposal, exc: BaseException, *, duration_ms: int,
        trace_id: str | None, now: datetime,
    ) -> None:
        """A failed run is recorded by class name; an existing candidate is rejected."""
        schema, _ = parameterize_filters(proposal.schema_dict)
        fingerprint = fingerprint_candidate(schema)
        candidate_id = candidate_id_for(fingerprint)
        self.store.append_run(CandidateRun(
            run_id=uuid.uuid4().hex, candidate_id=candidate_id, executed_at=now,
            purpose="answer", duration_ms=duration_ms, trace_id=trace_id,
            error_type=type(exc).__name__,
        ))
        existing = self.store.find_by_fingerprint(fingerprint)
        if existing is None or "rejected" not in ALLOWED_TRANSITIONS[existing.state]:
            return
        self.store.upsert_candidate(replace(existing, state="rejected", updated_at=now))
        self.store.append_event(CertificationEvent(
            event_id=uuid.uuid4().hex, metric_id=candidate_id, metric_name=existing.metric_name,
            event_type="rejected", previous_state=existing.state, new_state="rejected",
            decided_by=DECIDED_BY, justification=None,
            evidence={"refusal": CandidateRefusal.EXECUTION_FAILED.value,
                      "error_type": type(exc).__name__},
            replaced_by_metric=None, replaces_metric=None, semantic_layer_version=None,
            trace_id=trace_id, event_at=now,
        ))

    def answer(
        self,
        question: str,
        proposal: ValidatedProposal,
        *,
        requester_id: str | None = None,
        trace_id: str | None = None,
        consumer_context: Any = None,
    ) -> AgentResponse:
        """Certification gate, compile, run bounded, persist the candidate and its run."""
        try:
            mode, _, deps, certifications, policy = (
                self.semantic._enforce_certification_gate_with_evaluation(
                    f"candidate.{proposal.metric_name}",
                    {},
                    consumer_context,
                    deps=tuple(SemanticDependency(dataset=t) for t in proposal.tables_used),
                )
            )
        except SemanticAccessDenied as exc:
            return AgentResponse(
                question=question,
                mode="error",
                explanation=proposal.metric_description or "",
                error=str(exc),
                access_denied=True,
                policy_decision=exc.decision.value,
                policy_reasons=exc.reasons,
                recommended_action=exc.recommended_action,
            )
        sql = self.compile(proposal)
        compiled_at = self._now()
        started = time.monotonic()
        try:
            frame, rows = self._execute(sql)
        except Exception as exc:
            duration_ms = int((time.monotonic() - started) * 1000)
            self._record_failure(
                proposal, exc, duration_ms=duration_ms, trace_id=trace_id, now=self._now()
            )
            return self._refusal(
                question, CandidateRefusal.EXECUTION_FAILED,
                f"The candidate metric could not be computed ({type(exc).__name__}).",
            )
        duration_seconds = time.monotonic() - started
        duration_ms = int(duration_seconds * 1000)
        now = self._now()
        candidate, _ = self._persist(
            question, proposal, requester_id=requester_id, trace_id=trace_id, now=now
        )
        self.store.append_run(CandidateRun(
            run_id=uuid.uuid4().hex, candidate_id=candidate.candidate_id, executed_at=now,
            purpose="answer", result_hash=result_hash(rows), row_count=len(rows),
            duration_ms=duration_ms, trace_id=trace_id,
        ))
        from skifer.semantic.semantic import SemanticEngine

        evidence = SemanticEvidence(
            evidence_id=str(uuid.uuid4()),
            trace_id=trace_id,
            model_keys=(),
            metrics=(MetricEvidence(
                name=candidate.metric_name,
                model_key=candidate.candidate_id,
                definition_hash=candidate.fingerprint,
                source_columns=candidate.columns_used,
                lineage_status="resolved" if candidate.columns_used else "unresolved",
                certification="CANDIDATE",
                candidate_id=candidate.candidate_id,
            ),),
            dimensions=tuple(
                str(column) for column in (proposal.schema_dict.get("aggregate") or {}).get(
                    "group_by"
                ) or ()
            ),
            normalized_filters=_redacted_filters(proposal.schema_dict),
            sources=SemanticEngine._source_evidence(
                _Sources(proposal.tables_used), deps, certifications, policy=policy, mode=mode
            ),
            policy=policy,
            sql_hash=hash_sql(sql),
            sql_text=None,
            sql_redacted=True,
            statement_id=None,
            compiled_at=compiled_at,
            executed_at=now,
            execution_status="succeeded",
            execution_duration_seconds=duration_seconds,
        )
        return AgentResponse(
            question=question,
            mode="candidate",
            model_used="",
            explanation=proposal.metric_description or "",
            result=FormattedResult(
                format=ResponseFormat.TABLE, data=frame, title=candidate.metric_name
            ),
            evidence=evidence,
            candidate_id=candidate.candidate_id,
        )

    # --- refusals ------------------------------------------------------------------

    @staticmethod
    def _refusal(question: str, code: CandidateRefusal, explanation: str) -> AgentResponse:
        return AgentResponse(
            question=question,
            mode="candidate_refused",
            error=None,
            explanation=explanation,
            recommended_action=_RECOMMENDED_ACTIONS[code],
            policy_reasons=(code.value,),
        )

    # --- entry point -----------------------------------------------------------------

    def _resolved_tracer(self) -> Any:
        return self._tracer or getattr(self.semantic, "tracer", None) or NoOpTracer()

    def ask(
        self,
        question: str,
        *,
        consumer_context: Any = None,
        requester_id: str | None = None,
    ) -> AgentResponse:
        """One question, one ``skifer.candidate.build`` span; business output never depends on it."""
        with configured_span_scope(self._resolved_tracer(), CANDIDATE_SPAN_NAME) as span:
            trace_id = getattr(span, "trace_id", None)
            response = self._ask(question, consumer_context, requester_id, trace_id)
            set_span_attribute(span, "status", response.mode)
            if response.policy_decision is not None:
                set_span_attribute(span, "decision", response.policy_decision)
            evidence = response.evidence
            if evidence is not None:
                set_span_attribute(span, "sql_hash", evidence.sql_hash)
                set_span_attribute(span, "evidence_id", evidence.evidence_id)
            return response

    def _ask(
        self, question: str, consumer_context: Any, requester_id: str | None, trace_id: str | None
    ) -> AgentResponse:
        hits = self.index.search(question, k=SEARCH_K)
        if not any(hit.node.kind in ("table", "column") for hit in hits):
            return self._refusal(
                question, CandidateRefusal.NO_TABLE_FOUND,
                "No indexed table or column matches this question, so no metric can be built.",
            )
        outcome = self.propose(question, hits)
        if isinstance(outcome, AgentResponse):
            return outcome
        return self.answer(
            question, outcome, requester_id=requester_id, trace_id=trace_id,
            consumer_context=consumer_context,
        )


class _Sources:
    """The ``resolved.sources`` shape ``SemanticEngine._source_evidence`` reads."""

    def __init__(self, sources: tuple[str, ...]):
        self.sources = sources


def _redacted_filters(schema_dict: dict) -> tuple[dict, ...]:
    """Filter shapes for the evidence: column and operator, never the value."""
    shapes: list[dict] = []

    def collect(container: Any) -> Any:
        for shape in _shape_items(container):
            if "column" in shape:
                entry = {"column": shape["column"], "operator": shape["operator"]}
                if "value" in shape:
                    entry["value"] = "<redacted>"
                shapes.append(entry)
        return container

    _walk_filter_locations(copy.deepcopy(schema_dict), collect)
    return tuple(shapes)


def result_hash(rows: list[dict[str, Any]]) -> str:
    """``sha256:v1:`` of the rows, each as canonical JSON, sorted: row order is not meaning."""
    encoded = sorted(
        json.dumps(row, ensure_ascii=False, separators=(",", ":"), sort_keys=True, default=str)
        for row in rows
    )
    digest = hashlib.sha256(("[" + ",".join(encoded) + "]").encode("utf-8")).hexdigest()
    return f"sha256:v1:{digest}"


def _row_dict(row: Any) -> dict[str, Any]:
    if hasattr(row, "asDict"):
        return row.asDict(recursive=True)
    return dict(row)


def _without_values(text: str | None, values: dict[str, Any]) -> str | None:
    """Replace every filter value quoted in ``text`` by ``?`` (longest first)."""
    if not text:
        return text
    for value in sorted({str(v) for v in values.values() if str(v).strip()}, key=len, reverse=True):
        text = re.sub(re.escape(value), "?", text, flags=re.IGNORECASE)
    return text


def _metric_entry(node: Any) -> dict[str, Any]:
    return {
        "name": node.fqn,
        "description": node.text,
        "synonyms": list(node.synonyms),
        "status": node.status,
    }
