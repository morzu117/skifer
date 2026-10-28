"""MetricCandidateAgent — fallback agent outside the semantic layer (Plan 44, S03)."""

from __future__ import annotations

import json

import pytest

from skifer.agentic.candidate_agent import (
    ALLOWED_SCHEMA_KEYS,
    CANDIDATE_SPAN_NAME,
    MAX_CONTEXT_COLUMNS,
    MAX_CONTEXT_TABLES,
    MAX_CORRECTION_ATTEMPTS,
    CandidateRefusal,
    MetricCandidateAgent,
    ProposalError,
    _system_prompt,
    parse_proposal,
)
from skifer.agentic.models import AgentResponse
from skifer.knowledge import Edge, KnowledgeIndex, Node
from tests.fakes.fake_llm import FakeLLM


def _table(fqn, text=""):
    return Node.build(kind="table", fqn=fqn, label=fqn.rsplit(".", 1)[1], text=text,
                      source="pipeline", domain="main.sales")


def _column(table, name, text=""):
    return Node.build(kind="column", fqn=f"{table}.{name}", label=name, text=text,
                      source="pipeline", domain="main.sales")


def _has_column(table, column):
    return Edge.build(kind="has_column", from_id=f"table:{table}", to_id=column.node_id,
                      source="pipeline")


ORDERS = "main.sales.orders"
CUSTOMERS = "main.sales.customers"


def sales_graph():
    nodes, edges = [], []
    orders = _table(ORDERS, "Customer orders with amount and country")
    customers = _table(CUSTOMERS, "Customer master data")
    nodes += [orders, customers]
    for name, text in [("order_id", "order identifier"), ("amount", "order amount in euros"),
                       ("country", "delivery country"), ("customer_id", "customer key")]:
        column = _column(ORDERS, name, text)
        nodes.append(column)
        edges.append(_has_column(ORDERS, column))
    for name, text in [("id", "customer key"), ("segment", "customer segment")]:
        column = _column(CUSTOMERS, name, text)
        nodes.append(column)
        edges.append(_has_column(CUSTOMERS, column))
    edges.append(Edge.build(kind="joins", from_id=f"column:{ORDERS}.customer_id",
                            to_id=f"column:{CUSTOMERS}.id", keys=("customer_id", "id"),
                            source="pipeline"))
    certified = Node.build(kind="metric", fqn="sales.orders.total_revenue", label="total_revenue",
                           text="Revenue of orders", status="certified", source="semantic")
    promoted = Node.build(kind="metric", fqn="cand:revenue_by_segment", label="revenue_by_segment",
                          text="Revenue of orders by customer segment", status="candidate",
                          source="candidate")
    nodes += [certified, promoted]
    edges += [
        Edge.build(kind="belongs_to", from_id=certified.node_id, to_id=f"table:{ORDERS}",
                   source="semantic"),
        Edge.build(kind="belongs_to", from_id=promoted.node_id, to_id=f"table:{ORDERS}",
                   source="candidate"),
    ]
    rule = Node.build(kind="rule", fqn="flag_high_value", label="flag_high_value",
                      text="Flags orders whose amount is high", source="rule")
    nodes.append(rule)
    return KnowledgeIndex(nodes, edges)


def make_agent(llm=None, index=None, **kwargs):
    return MetricCandidateAgent(
        semantic_engine=None, llm_provider=llm or FakeLLM(), index=index or sales_graph(),
        store=None, **kwargs,
    )


# --- contract -----------------------------------------------------------------------

def test_contract_constants():
    assert CANDIDATE_SPAN_NAME == "skifer.candidate.build"
    assert MAX_CORRECTION_ATTEMPTS == 3
    assert ALLOWED_SCHEMA_KEYS == frozenset({
        "tables", "join", "where", "where_groups", "select_final", "add_columns", "aggregate",
    })
    assert [code.value for code in CandidateRefusal] == [
        "no_table_found", "unknown_table", "join_without_key", "unsafe_fanout",
        "raw_sql_rejected", "blocking_residue", "sql_path_unavailable",
        "invalid_after_retries", "execution_failed",
    ]


def test_agent_response_gains_candidate_id_without_touching_candidates():
    response = AgentResponse(question="q", mode="candidate", candidate_id="cand:abc")
    assert response.candidate_id == "cand:abc"
    assert response.candidates == []
    assert AgentResponse(question="q").candidate_id is None


@pytest.mark.parametrize("kwargs", [{"row_limit": 0}, {"row_limit": True},
                                    {"timeout_seconds": 0}, {"timeout_seconds": -1}])
def test_constructor_rejects_invalid_bounds(kwargs):
    with pytest.raises(ValueError):
        make_agent(**kwargs)


# --- S03-R7 ------------------------------------------------------------------------

def test_no_table_refusal_makes_no_llm_call():
    llm = FakeLLM()
    index = KnowledgeIndex([
        Node.build(kind="rule", fqn="flag_weather", label="flag_weather",
                   text="weather forecast", source="rule"),
    ])
    response = make_agent(llm=llm, index=index).ask("quelle est la météo de demain ?")
    assert response.mode == "candidate_refused"
    assert response.error is None
    assert response.policy_reasons == (CandidateRefusal.NO_TABLE_FOUND.value,)
    assert response.explanation and response.recommended_action
    assert response.candidate_id is None
    assert llm.calls == []


# --- context ----------------------------------------------------------------------

def test_context_puts_promoted_candidates_first_and_lists_joins():
    agent = make_agent()
    index = agent.index
    context = agent.build_context(index.search("revenue amount customer segment", k=20))
    assert list(context) == ["promoted_candidates", "tables", "metrics", "rules", "joins"]
    assert [m["name"] for m in context["promoted_candidates"]] == ["cand:revenue_by_segment"]
    assert [m["name"] for m in context["metrics"]] == ["sales.orders.total_revenue"]
    assert {t["name"] for t in context["tables"]} == {ORDERS, CUSTOMERS}
    orders = next(t for t in context["tables"] if t["name"] == ORDERS)
    assert {c["name"] for c in orders["columns"]} == {"order_id", "amount", "country",
                                                       "customer_id"}
    assert context["joins"] == [{"from": f"{ORDERS}.customer_id", "to": f"{CUSTOMERS}.id",
                                 "keys": ["customer_id", "id"]}]


def test_context_is_bounded_in_tables_and_columns():
    nodes, edges = [], []
    for t in range(12):
        fqn = f"main.big.sales_{t:02d}"
        nodes.append(_table(fqn, "sales table"))
        for c in range(60):
            column = _column(fqn, f"col_{c:02d}", "sales measure")
            nodes.append(column)
            edges.append(_has_column(fqn, column))
    agent = make_agent(index=KnowledgeIndex(nodes, edges))
    context = agent.build_context(agent.index.search("sales", k=20))
    assert len(context["tables"]) == MAX_CONTEXT_TABLES == 8
    assert all(len(t["columns"]) == MAX_CONTEXT_COLUMNS == 40 for t in context["tables"])


def test_context_follows_score_order():
    agent = make_agent()
    hits = agent.index.search("segment", k=20)
    context = agent.build_context(hits)
    assert context["tables"][0]["name"] == CUSTOMERS


# --- prompt ----------------------------------------------------------------------

def test_system_prompt_lists_grammar_from_op_catalog_without_raw_sql():
    prompt = _system_prompt()
    for key in ALLOWED_SCHEMA_KEYS:
        assert key in prompt
    for operator in ("equals", "between", "is_not_null"):
        assert operator in prompt
    for function in ("sum", "count_distinct"):
        assert function in prompt
    operators_line = next(line for line in prompt.splitlines() if line.startswith("Filters"))
    assert ", sql" not in operators_line
    ops_line = next(line for line in prompt.splitlines() if line.startswith("Column operations"))
    assert "expr" not in ops_line


def test_user_message_carries_question_context_and_previous_error():
    message = MetricCandidateAgent.build_user_message(
        "CA par segment", {"tables": []}, error="Unknown keys ['sink']."
    )
    assert "CA par segment" in message
    assert '"tables": []' in message
    assert "Unknown keys ['sink']." in message


# --- closed JSON -------------------------------------------------------------------

def test_parse_proposal_accepts_both_bodies_and_fences():
    body = {"metric_name": "revenue", "metric_description": "d", "schema_dict": {"tables": []}}
    assert parse_proposal(json.dumps(body)) == body
    fenced = "```json\n" + json.dumps({"metric_name": "r", "sql": "SELECT 1"}) + "\n```"
    assert parse_proposal(fenced)["sql"] == "SELECT 1"


@pytest.mark.parametrize("text", [
    "not json",
    "[1, 2]",
    json.dumps({"metric_name": "r", "schema_dict": {}, "extra": 1}),
    json.dumps({"metric_name": "r"}),
    json.dumps({"metric_name": "r", "schema_dict": {}, "sql": "SELECT 1"}),
    json.dumps({"metric_name": "1bad", "schema_dict": {}}),
    json.dumps({"metric_name": "r", "metric_description": 3, "schema_dict": {}}),
    json.dumps({"metric_name": "r", "schema_dict": []}),
    json.dumps({"metric_name": "r", "sql": "  "}),
])
def test_parse_proposal_is_closed(text):
    with pytest.raises(ProposalError):
        parse_proposal(text)

