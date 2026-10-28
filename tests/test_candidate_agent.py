"""MetricCandidateAgent — fallback agent outside the semantic layer (Plan 44, S03)."""

from __future__ import annotations

import json
import os

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

    context = {"promoted_candidates": [], "tables": [], "joins": []}
    rendered = MetricCandidateAgent.build_user_message("q", context)
    assert rendered.index('"promoted_candidates"') < rendered.index('"tables"') < rendered.index('"joins"')


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



# --- S03.2: validation and correction loop -------------------------------------------

def revenue_by_country(country="FR", **extra):
    schema = {
        "tables": [{"name": ORDERS, "alias": "o", "filter": [f"country:equals:{country}"]}],
        "aggregate": {"group_by": ["country"], "measures": [["amount", "revenue", "sum"]]},
    }
    schema.update(extra)
    return {"metric_name": "revenue_by_country", "metric_description": "Revenue per country",
            "schema_dict": schema}


def revenue_by_segment():
    return {"metric_name": "revenue_by_segment", "schema_dict": {
        "tables": [{"name": ORDERS, "alias": "o"}, {"name": CUSTOMERS, "alias": "c"}],
        "join": [{"table_from": ["o", "customer_id"], "table_to": ["c", "id"], "type": "left"}],
        "aggregate": {"group_by": ["segment"], "measures": [["amount", "revenue", "sum"]]},
    }}


def _propose(*responses, semantic=None, question="revenue per country"):
    llm = FakeLLM(*responses)
    agent = make_agent(llm=llm)
    agent.semantic = semantic
    return agent.propose(question, agent.index.search(question, k=20)), llm


def test_valid_schema_dict_is_normalized_and_described():
    proposal, llm = _propose(revenue_by_country())
    assert len(llm.calls) == 1
    assert proposal.metric_name == "revenue_by_country"
    assert proposal.origin == "schema_dict"
    assert proposal.tables_used == (ORDERS,)
    assert proposal.schema_dict["tables"][0]["filter"] == [
        {"column": "country", "operator": "equals", "value": "FR"}
    ]
    assert proposal.residues == () and proposal.coherence == {}


def test_correction_loop_recovers_on_second_attempt():
    invalid = revenue_by_country()
    invalid["schema_dict"]["aggregate"]["measures"] = [["amount", "revenue", "median"]]
    proposal, llm = _propose(invalid, revenue_by_country())
    assert proposal.attempts == 2
    assert len(llm.calls) == 2
    assert "median" in llm.calls[1]["user"]
    assert "rejected" in llm.calls[1]["user"]


def test_three_invalid_attempts_refuse():
    invalid = revenue_by_country()
    invalid["schema_dict"]["aggregate"]["measures"] = [["amount", "revenue", "median"]]
    llm = FakeLLM("not json", invalid, {"metric_name": "x"}, revenue_by_country())
    agent = make_agent(llm=llm)
    response = agent.propose("revenue", agent.index.search("revenue", k=20))
    assert response.mode == "candidate_refused"
    assert response.policy_reasons == (CandidateRefusal.INVALID_AFTER_RETRIES.value,)
    assert len(llm.calls) == MAX_CORRECTION_ATTEMPTS == 3


class _EnvSemantic:
    def __init__(self, allow_raw_sql):
        env = {"allow_raw_sql": allow_raw_sql}
        self.core = type("Core", (), {"_context": type("Ctx", (), {"env_config": lambda s: env})()})()

    def _get_model(self, key):
        raise ValueError(key)


FORBIDDEN = {
    "sink": (revenue_by_country(sink={"type": "postgres"}), CandidateRefusal.INVALID_AFTER_RETRIES),
    "business_rules": (revenue_by_country(business_rules=["flag_high_value"]),
                       CandidateRefusal.INVALID_AFTER_RETRIES),
    "table_source": ({"metric_name": "m", "schema_dict": {
        "tables": [{"name": ORDERS, "source": {"type": "csv", "path": "/tmp/x"}}],
        "keep_all_columns": True}}, CandidateRefusal.INVALID_AFTER_RETRIES),
    "expr": ({"metric_name": "m", "schema_dict": {
        "tables": [{"name": ORDERS, "alias": "o"}],
        "select_final": [["amount", "doubled", ["expr:amount * 2"]]]}},
        CandidateRefusal.RAW_SQL_REJECTED),
    "sql_filter": (revenue_by_country(where=["country:sql:1=1"]), CandidateRefusal.RAW_SQL_REJECTED),
}


@pytest.mark.parametrize("allow_raw_sql", [pytest.param(True, id="allow_raw_sql=True"),
                                           pytest.param(False, id="allow_raw_sql=False")])
@pytest.mark.parametrize("case", sorted(FORBIDDEN))
def test_forbidden_keys_and_raw_sql_rejected(case, allow_raw_sql):
    payload, code = FORBIDDEN[case]
    response, llm = _propose(payload, payload, payload, semantic=_EnvSemantic(allow_raw_sql))
    assert response.mode == "candidate_refused"
    assert response.policy_reasons == (code.value,)
    assert len(llm.calls) == 3


def test_unknown_table_refused_before_compile(monkeypatch):
    import skifer.agentic.candidate_agent as module

    compiled = []
    monkeypatch.setattr(module, "parse_schema", lambda *a, **k: compiled.append(a))
    payload = {"metric_name": "m", "schema_dict": {"tables": [{"name": "main.secret.salaries"}]}}
    response, llm = _propose(payload, payload, payload)
    assert response.policy_reasons == (CandidateRefusal.UNKNOWN_TABLE.value,)
    assert "main.secret.salaries" in response.explanation
    assert compiled == []


def test_join_with_declared_edge_is_accepted_and_recorded():
    proposal, _ = _propose(revenue_by_segment())
    assert proposal.joins_used == ((ORDERS, CUSTOMERS, "customer_id=id"),)
    assert proposal.tables_used == (CUSTOMERS, ORDERS)
    assert proposal.coherence == {}


def test_undeclared_join_with_keys_is_kept_in_coherence():
    payload = revenue_by_segment()
    payload["schema_dict"]["join"][0]["table_to"] = ["c", "segment"]
    proposal, _ = _propose(payload)
    assert proposal.coherence == {"undeclared_join": [f"{ORDERS}.customer_id={CUSTOMERS}.segment"]}


@pytest.mark.parametrize("join", [
    {"table_from": "o", "table_to": "c", "type": "cross"},
    {"table_from": "o", "table_to": "c"},
])
def test_join_without_key_is_refused_without_retry(join):
    payload = revenue_by_segment()
    payload["schema_dict"]["join"] = [join]
    response, llm = _propose(payload, revenue_by_segment())
    assert response.policy_reasons == (CandidateRefusal.JOIN_WITHOUT_KEY.value,)
    assert len(llm.calls) == 1


# --- sql path through the Plan 43 mapper ----------------------------------------------


def test_sql_answer_is_mapped_never_executed_as_text():
    pytest.importorskip("sqlglot")
    sql = f"SELECT country, SUM(amount) AS revenue FROM {ORDERS} WHERE country = 'FR' GROUP BY country"
    proposal, _ = _propose({"metric_name": "revenue_by_country", "sql": sql})
    assert proposal.origin == "sql"
    assert proposal.tables_used == (ORDERS,)
    assert proposal.schema_dict["aggregate"]["measures"] == [
        {"source": "amount", "target": "revenue", "func": "sum"}
    ]


def test_lag_is_blocking_residue():
    pytest.importorskip("sqlglot")
    lag = {"metric_name": "prev_amount",
           "sql": f"SELECT order_id, LAG(amount) OVER (ORDER BY order_id) AS prev FROM {ORDERS}"}
    response, llm = _propose(lag, lag, lag)
    assert response.policy_reasons == (CandidateRefusal.BLOCKING_RESIDUE.value,)
    assert "window_function_unsupported" in response.explanation
    assert len(llm.calls) == 3


def test_non_blocking_residue_is_carried_by_the_proposal():
    pytest.importorskip("sqlglot")
    sql = (f"SELECT country, SUM(amount) AS revenue FROM {ORDERS} "
           "WHERE country NOT IN ('FR', 'BE') GROUP BY country")
    proposal, _ = _propose({"metric_name": "revenue_outside", "sql": sql})
    assert [r["code"] for r in proposal.residues] == ["not_in_nullable_pitfall"]
    assert all(r["blocking"] is False for r in proposal.residues)


def test_sql_path_disabled_without_analyzer_extra(monkeypatch):
    import sys

    monkeypatch.setitem(sys.modules, "sqlglot", None)
    sql_answer = {"metric_name": "m", "sql": f"SELECT country FROM {ORDERS}"}
    response, llm = _propose(sql_answer, sql_answer, sql_answer)
    assert response.policy_reasons == (CandidateRefusal.SQL_PATH_UNAVAILABLE.value,)
    proposal, _ = _propose(sql_answer, revenue_by_country())
    assert proposal.origin == "schema_dict" and proposal.attempts == 2


# --- fanout -------------------------------------------------------------------------

LINES = "main.sales.order_lines"


class _ModelSemantic:
    def __init__(self, models):
        self.models = models
        self.loaded = []

    def _get_model(self, key):
        self.loaded.append(key)
        if key not in self.models:
            raise ValueError(key)
        return self.models[key]


def fanout_world():
    nodes, edges = [], []
    for table, columns in [(ORDERS, ["order_id", "amount", "country"]),
                           (LINES, ["line_id", "order_id", "sku"])]:
        nodes.append(_table(table))
        for name in columns:
            column = _column(table, name)
            nodes.append(column)
            edges.append(_has_column(table, column))
    for model, table, metric in [("orders", ORDERS, "revenue"), ("order_lines", LINES, "lines")]:
        node = Node.build(kind="metric", fqn=f"{model}.{metric}", label=metric,
                          status="certified", source="semantic")
        nodes.append(node)
        edges.append(Edge.build(kind="belongs_to", from_id=node.node_id, to_id=f"table:{table}",
                                source="semantic"))
    semantic = _ModelSemantic({
        "orders": {"key": "orders", "table": ORDERS, "grain": ["order"],
                   "entities": [{"name": "order", "type": "primary", "key": "order_id"}],
                   "relationships": [{"name": "order_lines", "from_entity": "order",
                                      "to_model": "order_lines", "to_entity": "order",
                                      "cardinality": "one_to_many", "join_type": "left"}]},
        "order_lines": {"key": "order_lines", "table": LINES, "grain": ["line"],
                        "entities": [{"name": "line", "type": "primary", "key": "line_id"},
                                     {"name": "order", "type": "foreign", "key": "order_id"}]},
        "unrelated": {"key": "unrelated", "table": "main.other.t"},
    })
    return KnowledgeIndex(nodes, edges), semantic


def test_fanout_refused_with_planner_message():
    index, semantic = fanout_world()
    payload = {"metric_name": "revenue_by_sku", "schema_dict": {
        "tables": [{"name": ORDERS, "alias": "o"}, {"name": LINES, "alias": "l"}],
        "join": [{"table_from": ["o", "order_id"], "table_to": ["l", "order_id"]}],
        "aggregate": {"group_by": ["sku"], "measures": [["amount", "revenue", "sum"]]},
    }}
    llm = FakeLLM(payload, revenue_by_country())
    agent = MetricCandidateAgent(semantic, llm, index, None)
    response = agent.propose("revenue per sku", agent.index.search("revenue sku", k=20))
    assert response.policy_reasons == (CandidateRefusal.UNSAFE_FANOUT.value,)
    assert response.explanation == (
        "Unsafe fanout: metric grain 'order' crosses one_to_many relationship 'order_lines'."
    )
    assert len(llm.calls) == 1
    assert "unrelated" not in semantic.loaded


def test_metric_on_the_many_side_is_not_a_fanout():
    index, semantic = fanout_world()
    payload = {"metric_name": "lines_per_country", "schema_dict": {
        "tables": [{"name": ORDERS, "alias": "o"}, {"name": LINES, "alias": "l"}],
        "join": [{"table_from": ["o", "order_id"], "table_to": ["l", "order_id"]}],
        "aggregate": {"group_by": ["country"], "measures": [["line_id", "lines", "count"]]},
    }}
    agent = MetricCandidateAgent(semantic, FakeLLM(payload), index, None)
    proposal = agent.propose("lines", agent.index.search("lines", k=20))
    assert proposal.joins_used == ((ORDERS, LINES, "order_id=order_id"),)
    assert proposal.coherence == {}


# --- S03.3: execution and persistence ---------------------------------------------------

import threading  # noqa: E402

from skifer.candidates import SqliteCandidateStore  # noqa: E402
from skifer.core.ir import parse_to_ir  # noqa: E402
from skifer.core.schema_loader import parse_schema  # noqa: E402
from skifer.core.sql_compiler import compile_select  # noqa: E402
from skifer.observability.tracing import TraceAttributePolicy  # noqa: E402
from tests.fakes.fake_backend import FakeBackend  # noqa: E402

import yaml  # noqa: E402

ROWS = [{"country": "FR", "revenue": 120.5}]


class _Frame:
    def __init__(self, backend, rows):
        self.backend, self.rows = backend, rows

    def limit(self, n):
        self.backend.limits.append(n)
        return _Frame(self.backend, self.rows[:n])

    def collect(self):
        return list(self.rows)


class SpyBackend(FakeBackend):
    """Records every SQL text; fails or blocks on demand."""

    def __init__(self, rows=ROWS, *, error=None, block=None):
        super().__init__()
        self.executed, self.limits = [], []
        self.rows, self.error, self.block = rows, error, block

    def execute_sql(self, sql):
        self.executed.append(sql)
        if self.block is not None and not sql.startswith("EXPLAIN"):
            self.block.wait(5)
        if self.error is not None and not sql.startswith("EXPLAIN"):
            raise self.error
        return _Frame(self, self.rows)


class _Context:
    def __init__(self, env):
        self.env = env

    def env_config(self):
        return self.env


class _Core:
    def __init__(self, backend, env=None):
        self._backend = backend
        self._context = _Context(env or {})

    def _get_backend(self):
        return self._backend


def SpySemantic(backend, env=None, certification_store=None):
    """A real SemanticEngine (real certification gate) without a catalogue on disk."""
    from datetime import datetime, timezone
    import time

    from skifer.semantic.semantic import SemanticEngine

    engine = SemanticEngine.__new__(SemanticEngine)
    engine.core = _Core(backend, env)
    engine.certification_store = certification_store
    engine._utc_now = lambda: datetime(2026, 10, 28, 12, tzinfo=timezone.utc)
    engine._monotonic = time.monotonic
    engine.certification_warning_count = 0
    engine._catalog, engine._cache = {}, {}
    return engine


@pytest.fixture
def store(tmp_path):
    candidate_store = SqliteCandidateStore(str(tmp_path / "candidates.db"))
    yield candidate_store
    candidate_store.close()


def run_agent(store, *responses, backend=None, question="amount per country in FR", **kwargs):
    backend = backend or SpyBackend()
    llm = FakeLLM(*responses)
    requester_id = kwargs.pop("requester_id", None)
    agent = MetricCandidateAgent(SpySemantic(backend), llm, sales_graph(), store, **kwargs)
    return agent.ask(question, requester_id=requester_id), backend, llm


def expected_sql(schema_dict):
    return compile_select(parse_to_ir(parse_schema(yaml.safe_dump(schema_dict))),
                          allow_raw_sql=False)


def test_only_compiled_sql_reaches_backend(store):
    pytest.importorskip("sqlglot")
    llm_sql = (f"SELECT country, SUM(amount) AS revenue FROM {ORDERS} "
               "WHERE country = 'FR' GROUP BY country")
    response, backend, _ = run_agent(store, {"metric_name": "revenue_by_country", "sql": llm_sql})
    assert response.mode == "candidate"
    mapped = {"tables": [{"name": ORDERS, "alias": "orders", "filter": ["country:equals:FR"]}],
              "aggregate": {"group_by": ["country"],
                            "measures": [{"source": "amount", "target": "revenue",
                                          "func": "sum"}]}}
    compiled = expected_sql(mapped)
    assert backend.executed == ["EXPLAIN " + compiled, compiled]
    assert llm_sql not in backend.executed

    response, backend, _ = run_agent(store, revenue_by_country())
    compiled = expected_sql(revenue_by_country()["schema_dict"])
    assert backend.executed == ["EXPLAIN " + compiled, compiled]


def test_answer_is_bounded_and_persisted_as_candidate(store):
    rows = [{"country": c, "revenue": float(i)} for i, c in enumerate("ABCDEFGH")]
    response, backend, _ = run_agent(store, revenue_by_country(), backend=SpyBackend(rows),
                                     row_limit=3)
    assert backend.limits == [3]
    assert response.mode == "candidate" and response.error is None
    assert response.result.title == "revenue_by_country"
    candidate = store.get_candidate(response.candidate_id)
    assert candidate.state == "candidate"
    assert candidate.request_count == 1
    assert candidate.tables_used == (ORDERS,)
    assert candidate.columns_used == (f"{ORDERS}.amount", f"{ORDERS}.country")
    assert candidate.business_rules_refs == ()
    [event] = store.list_events(metric_id=response.candidate_id)
    assert (event.event_type, event.previous_state, event.new_state, event.decided_by) == (
        "created", None, "candidate", "agent:candidate_agent_v1")
    [run] = store.list_runs(response.candidate_id)
    assert run.purpose == "answer" and run.row_count == 3 and run.error_type is None
    assert run.result_hash.startswith("sha256:v1:")


def test_result_hash_ignores_row_order():
    from skifer.agentic.candidate_agent import result_hash

    a, b = {"x": 1, "y": "a"}, {"y": "b", "x": 2}
    assert result_hash([a, b]) == result_hash([b, a])
    assert result_hash([a]) != result_hash([b])


def test_same_structure_different_values_dedupes(store):
    first, _, _ = run_agent(store, revenue_by_country("FR"), question="amount per country in FR")
    second, _, _ = run_agent(store, revenue_by_country("BE"), question="amount per country in BE")
    assert first.candidate_id == second.candidate_id
    [candidate] = store.list_candidates()
    assert candidate.request_count == 2
    assert len(store.list_runs(candidate.candidate_id)) == 2
    assert [e.event_type for e in store.list_events(metric_id=candidate.candidate_id)] == ["created"]


def test_distinct_requesters_are_pseudonymized(store):
    policy = TraceAttributePolicy(user_identity="hmac", hmac_secret="test-secret")
    run_agent(store, revenue_by_country("FR"), policy=policy, requester_id="alice@corp")
    run_agent(store, revenue_by_country("BE"), policy=policy, requester_id="bob@corp")
    run_agent(store, revenue_by_country("DE"), policy=policy, requester_id="alice@corp")
    [candidate] = store.list_candidates()
    assert candidate.request_count == 3
    assert len(candidate.distinct_requesters) == 2
    assert not any("corp" in r for r in candidate.distinct_requesters)
    run_agent(store, revenue_by_country("IT"), requester_id="carol@corp")
    assert len(store.list_candidates()[0].distinct_requesters) == 2


def test_non_blocking_residue_kept_on_candidate(store):
    pytest.importorskip("sqlglot")
    sql = (f"SELECT country, SUM(amount) AS revenue FROM {ORDERS} "
           "WHERE country NOT IN ('ZZCANARY', 'BE') GROUP BY country")
    response, _, _ = run_agent(store, {"metric_name": "revenue_outside", "sql": sql})
    candidate = store.get_candidate(response.candidate_id)
    assert [r["code"] for r in candidate.residues] == ["not_in_nullable_pitfall"]
    assert not candidate.auto_promotable


def test_question_values_are_not_stored(store):
    response, _, _ = run_agent(store, revenue_by_country("ZZCANARY"),
                               question="amount by country for ZZCANARY please")
    candidate = store.get_candidate(response.candidate_id)
    assert candidate.origin_question == "amount by country for ? please"
    assert "ZZCANARY" not in json.dumps(candidate.to_dict())


def test_execution_failure_records_class_name_only(store):
    error = RuntimeError("AnalysisException near 'ZZCANARY': secret detail")
    response, _, _ = run_agent(store, revenue_by_country("ZZCANARY"),
                               backend=SpyBackend(error=error))
    assert response.mode == "candidate_refused"
    assert response.policy_reasons == (CandidateRefusal.EXECUTION_FAILED.value,)
    assert "secret detail" not in response.explanation
    assert "ZZCANARY" not in response.explanation
    assert store.list_candidates() == []
    candidate_id = candidate_id_for_schema(revenue_by_country()["schema_dict"])
    [run] = store.list_runs(candidate_id)
    assert run.error_type == "RuntimeError"
    assert run.result_hash is None and run.row_count is None
    assert store.list_events(metric_id=candidate_id) == []


def test_execution_failure_rejects_an_existing_candidate(store):
    created, _, _ = run_agent(store, revenue_by_country("FR"))
    failed, _, _ = run_agent(store, revenue_by_country("BE"),
                             backend=SpyBackend(error=ValueError("boom")))
    assert failed.policy_reasons == (CandidateRefusal.EXECUTION_FAILED.value,)
    candidate = store.get_candidate(created.candidate_id)
    assert candidate.state == "rejected"
    assert [e.event_type for e in store.list_events(metric_id=created.candidate_id)] == [
        "created", "rejected"]


def test_execution_timeout_is_a_typed_refusal(store):
    gate = threading.Event()
    try:
        response, _, _ = run_agent(store, revenue_by_country(),
                                   backend=SpyBackend(block=gate), timeout_seconds=0.05)
    finally:
        gate.set()
    assert response.policy_reasons == (CandidateRefusal.EXECUTION_FAILED.value,)
    [run] = store.list_runs(candidate_id_for_schema(revenue_by_country()["schema_dict"]))
    assert run.error_type == "TimeoutError"


def candidate_id_for_schema(schema_dict):
    from skifer.candidates import candidate_id_for, fingerprint_candidate, parameterize_filters

    parsed = parse_schema(yaml.safe_dump(schema_dict))
    return candidate_id_for(fingerprint_candidate(parameterize_filters(parsed)[0]))


# --- S03.4: response, evidence, tracing and certification gate ---------------------------

from skifer.observability.tracing import InMemoryTracer, NoOpTracer  # noqa: E402
from skifer.semantic.evidence import hash_sql  # noqa: E402
from skifer.serving._response_serializer import _certification_text  # noqa: E402

CANARY = "ZZCANARY_VALUE_42"


def test_response_evidence_is_candidate_v2(store):
    response, backend, _ = run_agent(store, revenue_by_country())
    evidence = response.evidence
    assert evidence.schema_version == "2"
    [metric] = evidence.metrics
    assert metric.certification == "CANDIDATE"
    assert metric.candidate_id == response.candidate_id
    assert metric.definition_hash == store.get_candidate(response.candidate_id).fingerprint
    assert evidence.sql_hash == hash_sql(backend.executed[-1])
    assert evidence.sql_text is None and evidence.sql_redacted is True
    assert evidence.execution_status == "succeeded"
    assert [s.dataset for s in evidence.sources] == [ORDERS]
    payload = evidence.to_dict()
    assert payload["schema_version"] == "2"
    assert payload["metrics"][0]["certification"] == "CANDIDATE"
    text = _certification_text(payload["sources"], payload["metrics"])
    assert "certifié le" not in text


def test_no_filter_value_leaks_anywhere(tmp_path):
    db = tmp_path / "leak.db"
    store = SqliteCandidateStore(str(db))
    tracer = InMemoryTracer()
    try:
        payload = revenue_by_country(CANARY, where_groups=[[f"country:in:{CANARY},BE"]])
        backend = SpyBackend()
        agent = MetricCandidateAgent(SpySemantic(backend), FakeLLM(payload), sales_graph(), store,
                                     tracer=tracer)
        response = agent.ask(f"amount per country for {CANARY}", requester_id="alice")
        assert response.mode == "candidate"
        assert CANARY in backend.executed[-1]           # the value does run…
    finally:
        store.close()
    import sqlite3

    connection = sqlite3.connect(str(db))
    try:
        tables = [r[0] for r in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")]
        dump = json.dumps([
            list(row) for table in tables for row in connection.execute(f"SELECT * FROM {table}")
        ], default=str)
    finally:
        connection.close()
    assert CANARY not in dump                            # …but is never stored,
    spans = json.dumps([
        {"name": span.name, "attributes": span.attributes, "events": span.events}
        for span in tracer.spans
    ], default=str)
    assert CANARY not in spans                           # traced,
    assert CANARY not in json.dumps(response.evidence.to_dict(), default=str)  # or attested.


def _business_view(store, response):
    candidate = store.get_candidate(response.candidate_id).to_dict()
    for key in ("trace_id", "created_at", "updated_at", "last_requested_at"):
        candidate.pop(key)
    runs = [
        {k: v for k, v in run.to_dict().items()
         if k not in ("run_id", "trace_id", "duration_ms", "executed_at")}
        for run in store.list_runs(response.candidate_id)
    ]
    return response.mode, response.candidate_id, response.policy_reasons, candidate, runs


def test_trace_id_recorded_and_behaviour_tracing_independent(tmp_path):
    views, trace_ids = [], []
    for name, tracer in [("noop", NoOpTracer()), ("memory", InMemoryTracer())]:
        store = SqliteCandidateStore(str(tmp_path / f"{name}.db"))
        try:
            agent = MetricCandidateAgent(SpySemantic(SpyBackend()), FakeLLM(revenue_by_country()),
                                         sales_graph(), store, tracer=tracer)
            response = agent.ask("amount per country in FR")
            views.append(_business_view(store, response))
            candidate = store.get_candidate(response.candidate_id)
            [run] = store.list_runs(response.candidate_id)
            trace_ids.append((candidate.trace_id, run.trace_id, response.evidence.trace_id))
        finally:
            store.close()
        if name == "memory":
            [span] = [s for s in tracer.spans if s.name == CANDIDATE_SPAN_NAME]
            assert trace_ids[-1] == (span.trace_id,) * 3
            assert span.attributes["status"] == "candidate"
    assert trace_ids[0] == (None, None, None)
    assert views[0] == views[1]


def test_tracing_failure_never_changes_the_answer(store):
    class BrokenTracer:
        def start_span(self, name, *, attributes=None):
            raise RuntimeError("exporter down")

    agent = MetricCandidateAgent(SpySemantic(SpyBackend()), FakeLLM(revenue_by_country()),
                                 sales_graph(), store, tracer=BrokenTracer())
    response = agent.ask("amount per country in FR")
    assert response.mode == "candidate"
    assert store.get_candidate(response.candidate_id).trace_id is None


def test_certification_gate_applies_to_candidate_sources(store):
    backend = SpyBackend()
    semantic = SpySemantic(backend, env={"semantic_certification_policy": "enforce"})
    llm = FakeLLM(revenue_by_country())
    response = MetricCandidateAgent(semantic, llm, sales_graph(), store).ask(
        "amount per country in FR")
    assert response.mode == "error"
    assert response.access_denied is True
    assert response.policy_decision == "DENY"
    assert "MISSING" in response.policy_reasons
    assert ORDERS in response.recommended_action
    assert backend.executed == []
    assert store.list_candidates() == []

    warn = SpySemantic(SpyBackend(), env={"semantic_certification_policy": "warn"})
    response = MetricCandidateAgent(warn, FakeLLM(revenue_by_country()), sales_graph(),
                                    store).ask("amount per country in FR")
    assert response.mode == "candidate"
    assert response.evidence.policy.decision.value == "WARN"
    assert warn.certification_warning_count == 1


# --- real Spark chain: compile_select -> EXPLAIN -> execution ----------------------------

EXAMPLE_DATA = os.path.join(os.path.dirname(__file__), "..", "examples",
                            "05_rules_join_aggregate", "data")


def test_end_to_end_candidate_on_example_data(spark, tmp_path):
    from skifer.core.spark_backend import SparkBackend

    spark.sql("CREATE DATABASE IF NOT EXISTS plan44_candidates")
    for name in ("orders", "customers"):
        (spark.read.option("header", "true").option("inferSchema", "true")
         .csv(os.path.join(EXAMPLE_DATA, f"{name}.csv"))
         .write.mode("overwrite").saveAsTable(f"plan44_candidates.{name}"))
    orders, customers = "plan44_candidates.orders", "plan44_candidates.customers"
    nodes, edges = [_table(orders, "orders"), _table(customers, "customers and their region")], []
    for table, columns in [(orders, ["order_id", "customer_id", "amount"]),
                           (customers, ["customer_id", "customer_name", "region"])]:
        for column_name in columns:
            column = _column(table, column_name, column_name.replace("_", " "))
            nodes.append(column)
            edges.append(_has_column(table, column))
    proposal = {"metric_name": "amount_by_region", "metric_description": "Order amount per region",
                "schema_dict": {
                    "tables": [{"name": orders, "alias": "o"},
                               {"name": customers, "alias": "c", "filter": ["region:not_equals:XX"]}],
                    "join": [{"table_from": ["o", "customer_id"], "table_to": ["c", "customer_id"],
                              "type": "inner"}],
                    "aggregate": {"group_by": ["region"],
                                  "measures": [["amount", "total_amount", "sum"]]}}}
    backend = SparkBackend(spark=spark, is_local=True)
    store = SqliteCandidateStore(str(tmp_path / "e2e.db"))
    try:
        agent = MetricCandidateAgent(SpySemantic(backend), FakeLLM(proposal),
                                     KnowledgeIndex(nodes, edges), store)
        response = agent.ask("total amount per region")
        assert response.mode == "candidate", response.explanation
        rows = {r["region"]: r["total_amount"] for r in
                (row.asDict() for row in response.result.data.collect())}
        expected = (
            spark.table(orders).join(spark.table(customers), "customer_id")
            .groupBy("region").sum("amount").collect()
        )
        assert rows == {r["region"]: r["sum(amount)"] for r in expected}
        candidate = store.get_candidate(response.candidate_id)
        assert candidate.coherence == {
            "undeclared_join": [f"{orders}.customer_id={customers}.customer_id"]}
        [run] = store.list_runs(candidate.candidate_id)
        assert run.row_count == len(rows) and run.error_type is None
    finally:
        store.close()
        spark.sql("DROP DATABASE IF EXISTS plan44_candidates CASCADE")
