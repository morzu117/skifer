"""Fallback agent for questions the semantic layer cannot answer (Plan 44, S03).

The LLM never writes executed SQL. It proposes a ``schema_dict`` in Skifer's own
YAML grammar — or, failing that, a ``SELECT`` that the Plan 43 mapper translates
into one — and what runs is always ``compile_select`` of the validated schema.
The answer is marked ``CANDIDATE``: a candidate metric is persisted without any
filter value, and a refusal says what is missing instead of guessing.
"""

from __future__ import annotations

from enum import Enum
import json
import re
from typing import Any

from skifer.core.op_catalog import AGGREGATE_FUNCTIONS, COLUMN_OPS, FILTER_OPERATORS

from .models import AgentResponse


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
    """A proposal the LLM can correct; the message is sent back for the next attempt."""

    def __init__(self, code: CandidateRefusal, message: str):
        super().__init__(message)
        self.code = code


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
            f"{json.dumps(context, ensure_ascii=False, sort_keys=True, indent=1)}"
        )
        if error:
            message += (
                f"\n\nYour previous answer was rejected: {error}\n"
                "Return a corrected JSON object."
            )
        return message

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

    def ask(
        self,
        question: str,
        *,
        consumer_context: Any = None,
        requester_id: str | None = None,
    ) -> AgentResponse:
        hits = self.index.search(question, k=SEARCH_K)
        if not any(hit.node.kind in ("table", "column") for hit in hits):
            return self._refusal(
                question, CandidateRefusal.NO_TABLE_FOUND,
                "No indexed table or column matches this question, so no metric can be built.",
            )
        raise NotImplementedError("Proposal validation and execution are not implemented yet.")


def _metric_entry(node: Any) -> dict[str, Any]:
    return {
        "name": node.fqn,
        "description": node.text,
        "synonyms": list(node.synonyms),
        "status": node.status,
    }
