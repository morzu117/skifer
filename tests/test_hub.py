"""
Tests unitaires pour AgenticHub — Phase A.

Tous les tests sont sans LLM ni Spark : chaque agent est mocké avec MagicMock.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from skifer.agentic.hub import (
    _INTENT_TO_KEYWORDS,
    AgenticHub,
    _detect_intent,
    _normalize,
)
from skifer.agentic.models import (
    AgentResponse,
    BuilderResponse,
    CapabilityRequest,
    CapabilityResponse,
    DictionaryResponse,
    LineageResponse,
    QualityResponse,
)
from skifer.capabilities import CapabilityResult
from skifer.capabilities.registry import CapabilityRegistryError
from skifer.semantic.access_policy import ConsumerContext


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_lineage_response(q: str) -> LineageResponse:
    return LineageResponse(question=q, mode="trace")


def _make_dict_response(q: str) -> DictionaryResponse:
    return DictionaryResponse(question=q, mode="lookup")


def _make_quality_response(q: str) -> QualityResponse:
    return QualityResponse(question=q, mode="check")


def _make_builder_response(q: str) -> BuilderResponse:
    return BuilderResponse(success=True, yaml_content="tables: []")


def _make_genbi_response(q: str, **_kwargs) -> AgentResponse:
    return AgentResponse(question=q, mode="query")


# ---------------------------------------------------------------------------
# _normalize / _detect_intent (unité)
# ---------------------------------------------------------------------------

def test_normalize_strips_accents():
    assert _normalize("D'où") == "d'ou"
    assert _normalize("qualité") == "qualite"


def test_detect_intent_lineage():
    assert _detect_intent("D'où vient le champ amount_eur ?") == "lineage"
    assert _detect_intent("quelle est la provenance de ce champ") == "lineage"
    assert _detect_intent("upstream source de orders") == "lineage"


def test_detect_intent_dictionary():
    assert _detect_intent("que signifie gross_revenue ?") == "dictionary"
    assert _detect_intent("donne-moi la definition de amount_eur") == "dictionary"


def test_detect_intent_quality():
    assert _detect_intent("la table fact_orders est-elle saine ?") == "quality"
    assert _detect_intent("vérifie la qualite de la table") == "quality"
    assert _detect_intent("check les doublons") == "quality"


def test_detect_intent_builder():
    assert _detect_intent("cree un yaml pour orders") == "builder"
    assert _detect_intent("genere un pipeline qui joint orders et customers") == "builder"
    assert _detect_intent("construis le schema silver") == "builder"


def test_detect_intent_fallback_genbi():
    assert _detect_intent("quel est le CA par region ce trimestre ?") == "genbi"
    assert _detect_intent("montre-moi les ventes de juin") == "genbi"


def test_free_text_intents_contain_no_capability_route():
    assert "capability" not in {intent for intent, _keywords in _INTENT_TO_KEYWORDS}


# ---------------------------------------------------------------------------
# Phase A — Routage déterministe
# ---------------------------------------------------------------------------

def _hub_with_all_mocks():
    lineage = MagicMock()
    lineage.ask.side_effect = _make_lineage_response

    dictionary = MagicMock()
    dictionary.ask.side_effect = _make_dict_response

    quality = MagicMock()
    quality.ask.side_effect = _make_quality_response

    builder = MagicMock()
    builder.ask.return_value = _make_builder_response("mock")

    genbi = MagicMock()
    genbi.ask.side_effect = _make_genbi_response

    hub = AgenticHub(
        lineage_agent=lineage,
        quality_agent=quality,
        dictionary_agent=dictionary,
        builder_agent=builder,
        genbi_agent=genbi,
    )
    return hub, lineage, dictionary, quality, builder, genbi


def test_route_lineage_deterministic():
    hub, lineage, *_ = _hub_with_all_mocks()
    resp = hub.ask("D'où vient le champ amount_eur ?")
    assert isinstance(resp, LineageResponse)
    lineage.ask.assert_called_once()


def test_route_dictionary_deterministic():
    hub, _, dictionary, *_ = _hub_with_all_mocks()
    resp = hub.ask("que signifie gross_revenue ?")
    assert isinstance(resp, DictionaryResponse)
    dictionary.ask.assert_called_once()


def test_route_quality_deterministic():
    hub, _, _, quality, *_ = _hub_with_all_mocks()
    resp = hub.ask("la table fact_orders est-elle saine ?")
    assert isinstance(resp, QualityResponse)
    quality.ask.assert_called_once()


def test_route_builder_deterministic():
    hub, _, _, _, builder, _ = _hub_with_all_mocks()
    resp = hub.ask("cree un yaml pour orders et customers")
    assert isinstance(resp, BuilderResponse)
    builder.ask.assert_called_once()


def test_route_fallback_genbi():
    hub, _, _, _, _, genbi = _hub_with_all_mocks()
    resp = hub.ask("quel est le CA par region ce trimestre ?")
    assert isinstance(resp, AgentResponse)
    genbi.ask.assert_called_once()


def test_agent_not_configured():
    """Si l'agent cible est None, retourner AgentResponse mode='error'."""
    hub = AgenticHub()  # tous les agents None
    resp = hub.ask("D'où vient le champ amount_eur ?")
    assert isinstance(resp, AgentResponse)
    assert resp.mode == "error"
    assert "lineage" in resp.error.lower()


def test_agent_not_configured_genbi():
    """Fallback genbi absent → AgentResponse mode='error'."""
    hub = AgenticHub()
    resp = hub.ask("quel est le CA ?")
    assert isinstance(resp, AgentResponse)
    assert resp.mode == "error"
    assert "genbi" in resp.error.lower()


def test_hub_history_accessible():
    hub = AgenticHub(session_title="Test")
    assert hub.history is not None
    assert hub.history.title == "Test"


def test_builder_kwargs_forwarded():
    """output_dir doit être transmis à builder.ask()."""
    hub, _, _, _, builder, _ = _hub_with_all_mocks()
    hub.ask("cree un yaml", output_dir="my_schemas")
    _, call_kwargs = builder.ask.call_args
    assert call_kwargs.get("output_dir") == "my_schemas"


def test_hub_forwards_consumer_context_to_genbi_fallback():
    context = ConsumerContext(
        consumer_id="consumer-1",
        consumer_class="agent_read",
        user_id="user-1",
        trace_id="trace-1",
    )
    hub, _, _, _, _, genbi = _hub_with_all_mocks()

    hub.ask("quel est le CA par region ?", consumer_context=context)

    genbi.ask.assert_called_once_with(
        "quel est le CA par region ?",
        consumer_context=context,
    )


@pytest.mark.parametrize("llm_answer", ["capability", "inventory.lookup_record"])
def test_llm_cannot_authorize_a_capability(llm_answer: str):
    capability_invoker = MagicMock()
    llm = MagicMock()
    llm.complete.return_value = llm_answer
    genbi = MagicMock()
    genbi.ask.side_effect = _make_genbi_response
    hub = AgenticHub(
        llm_provider=llm,
        genbi_agent=genbi,
        capability_invoker=capability_invoker,
    )

    response = hub.ask("show inventory status")

    assert isinstance(response, AgentResponse)
    genbi.ask.assert_called_once()
    capability_invoker.invoke.assert_not_called()


def test_free_text_capability_id_still_routes_to_normal_agent():
    capability_invoker = MagicMock()
    genbi = MagicMock()
    genbi.ask.side_effect = _make_genbi_response
    hub = AgenticHub(genbi_agent=genbi, capability_invoker=capability_invoker)

    response = hub.ask("inventory.lookup_record")

    assert isinstance(response, AgentResponse)
    genbi.ask.assert_called_once_with(
        "inventory.lookup_record",
        consumer_context=None,
    )
    capability_invoker.invoke.assert_not_called()


def test_structured_capability_request_uses_dedicated_entry_point():
    result = CapabilityResult(
        capability_id="inventory.lookup_record",
        capability_version="1.2.0",
        status="succeeded",
        output={"found": True},
        error_type=None,
    )
    capability_invoker = MagicMock()
    capability_invoker.invoke.return_value = result
    hub = AgenticHub(capability_invoker=capability_invoker)
    request = CapabilityRequest(
        capability_id="inventory.lookup_record",
        arguments={"record_id": "item-1"},
    )

    response = hub.invoke_capability(request)

    assert isinstance(response, CapabilityResponse)
    assert response.to_dict()["result"]["output"] == {"found": True}
    capability_invoker.invoke.assert_called_once_with(
        "inventory.lookup_record",
        {"record_id": "item-1"},
    )


def test_structured_unregistered_capability_is_refused():
    capability_invoker = MagicMock()
    capability_invoker.invoke.side_effect = CapabilityRegistryError(
        "Capability id 'inventory.missing' is not present in the catalog."
    )
    hub = AgenticHub(capability_invoker=capability_invoker)

    with pytest.raises(CapabilityRegistryError, match="inventory.missing"):
        hub.invoke_capability(CapabilityRequest("inventory.missing", {}))


# ---------------------------------------------------------------------------
# LLM fallback routed to an unconfigured agent → fall back to GenBI
# (Bug Skifer 3.1.1 — défaut 1)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("llm_answer", ["lineage", "builder", "dictionary"])
def test_llm_routed_intent_to_unconfigured_agent_falls_back_to_genbi(llm_answer: str):
    """Hub avec uniquement GenBI configuré : le fallback LLM ne doit jamais
    pointer vers un agent absent — la question libre doit être traitée par
    GenBI plutôt que de renvoyer une erreur 'non configure'."""
    llm = MagicMock()
    llm.complete.return_value = llm_answer
    genbi = MagicMock()
    genbi.ask.side_effect = _make_genbi_response
    hub = AgenticHub(llm_provider=llm, genbi_agent=genbi)

    response = hub.ask(
        "tu peux me donner l'evolution mensuelle du CA des commandes depuis janvier 2026 ?"
    )

    assert isinstance(response, AgentResponse)
    assert response.mode == "query"
    genbi.ask.assert_called_once()


def test_keyword_detected_unconfigured_agent_still_errors_even_with_llm():
    """Une demande explicite (mot-clé) pour une fonction absente doit garder
    l'erreur 'non configure', même quand un llm_provider est injecté — ce
    n'est pas une ambiguïté que le fallback LLM doit arbitrer."""
    llm = MagicMock()
    llm.complete.return_value = "genbi"
    genbi = MagicMock()
    genbi.ask.side_effect = _make_genbi_response
    hub = AgenticHub(llm_provider=llm, genbi_agent=genbi)

    response = hub.ask("D'où vient le champ amount_eur ?")

    assert isinstance(response, AgentResponse)
    assert response.mode == "error"
    assert "lineage" in response.error.lower()
    llm.complete.assert_not_called()


def test_llm_classify_intent_restricted_to_configured_intents():
    """_llm_classify_intent ne doit jamais renvoyer un intent dont l'agent
    n'est pas configuré dans ce hub, même si le LLM le propose."""
    llm = MagicMock()
    llm.complete.return_value = "lineage"
    genbi = MagicMock()
    hub = AgenticHub(llm_provider=llm, genbi_agent=genbi)

    assert hub._llm_classify_intent("une question quelconque") is None


def test_llm_classify_intent_system_prompt_lists_only_configured_intents():
    """Le prompt système envoyé au LLM ne doit mentionner que les intents
    réellement configurés dans ce hub, pas les 5 intents possibles."""
    llm = MagicMock()
    llm.complete.return_value = "genbi"
    genbi = MagicMock()
    quality = MagicMock()
    hub = AgenticHub(llm_provider=llm, genbi_agent=genbi, quality_agent=quality)

    hub._llm_classify_intent("une question quelconque")

    system_prompt = llm.complete.call_args.kwargs["system_prompt"]
    assert "genbi" in system_prompt
    assert "quality" in system_prompt
    assert "lineage" not in system_prompt
    assert "builder" not in system_prompt
    assert "dictionary" not in system_prompt


# ---------------------------------------------------------------------------
# Plan 44 S04 — candidate fallback routing (off | shadow | on)
# ---------------------------------------------------------------------------

import dataclasses  # noqa: E402
import json  # noqa: E402

from skifer.agentic.hub import SHADOW_CANDIDATE_NOTE, _should_fallback  # noqa: E402
from skifer.serving._response_serializer import hub_response_to_text  # noqa: E402

CANDIDATE_ID = "candidate:v1:" + "ab" * 32


class _EnvSemantic:
    """Only what the hub reads: the active environment's configuration."""

    def __init__(self, **env):
        self.env = env

    def _env_config(self):
        return dict(self.env)


def _candidate_response(question, **_kwargs):
    return AgentResponse(question=question, mode="candidate", candidate_id=CANDIDATE_ID,
                         explanation="Revenue per segment")


def _fallback_hub(genbi_response, mode=None, agent=None):
    genbi = MagicMock()
    genbi.ask.side_effect = lambda q, **kw: dataclasses.replace(genbi_response, question=q)
    env = {} if mode is None else {"candidate_fallback": mode}
    agent = agent if agent is not None else MagicMock()
    hub = AgenticHub(semantic_engine=_EnvSemantic(**env), genbi_agent=genbi,
                     candidate_agent=agent)
    return hub, genbi, agent


GENBI_ANSWERS = {
    "query": AgentResponse(question="", mode="query", explanation="ok"),
    "view": AgentResponse(question="", mode="view", result="cat.sch.v"),
    "ambiguous": AgentResponse(question="", mode="ambiguous", candidates=["a", "b"]),
    "error": AgentResponse(question="", mode="error", error="boom"),
    "needs_clarification_with_suggestion": AgentResponse(
        question="", mode="needs_clarification", suggestions=["revenue"]),
    "access_denied": AgentResponse(question="", mode="error", access_denied=True,
                                   policy_decision="DENY", policy_reasons=("MISSING",)),
    "access_denied_no_model": AgentResponse(question="", mode="no_model", access_denied=True),
    "no_model": AgentResponse(question="", mode="no_model", explanation="nothing"),
    "needs_clarification_empty": AgentResponse(question="", mode="needs_clarification"),
}


@pytest.mark.parametrize(("case", "expected"), [
    ("query", False), ("view", False), ("ambiguous", False), ("error", False),
    ("needs_clarification_with_suggestion", False), ("access_denied", False),
    ("access_denied_no_model", False), ("no_model", True), ("needs_clarification_empty", True),
])
def test_should_fallback_matrix(case, expected):
    assert _should_fallback(GENBI_ANSWERS[case]) is expected
    hub, _, agent = _fallback_hub(GENBI_ANSWERS[case], mode="on")
    agent.ask.side_effect = _candidate_response
    response = hub.ask("chiffre d'affaires par segment client")
    assert agent.ask.called is expected
    assert (response.mode == "candidate") is expected


def test_should_fallback_ignores_non_genbi_responses():
    assert _should_fallback(LineageResponse(question="q", mode="trace")) is False


@pytest.mark.parametrize("case", sorted(GENBI_ANSWERS))
def test_off_mode_is_byte_identical(case):
    baseline_hub, _, _ = _fallback_hub(GENBI_ANSWERS[case])
    baseline_hub._candidate_agent = None
    baseline = baseline_hub.ask("chiffre d'affaires par segment client")
    for hub in (
        _fallback_hub(GENBI_ANSWERS[case])[0],             # agent present, key absent
        _fallback_hub(GENBI_ANSWERS[case], mode="off")[0],  # explicit off
        _fallback_hub(GENBI_ANSWERS[case], mode=False)[0],  # YAML's bare `off`
    ):
        response = hub.ask("chiffre d'affaires par segment client")
        assert dataclasses.asdict(response) == dataclasses.asdict(baseline)
        assert hub_response_to_text(response) == hub_response_to_text(baseline)


def test_off_mode_never_touches_agent_or_store():
    store = MagicMock()
    agent = MagicMock()
    agent.store = store
    for mode in (None, "off"):
        hub, _, _ = _fallback_hub(GENBI_ANSWERS["no_model"], mode=mode, agent=agent)
        hub.ask("chiffre d'affaires par segment client")
    agent.ask.assert_not_called()
    assert store.method_calls == []


def test_shadow_returns_genbi_and_persists_candidate(tmp_path):
    from tests.test_candidate_agent import (
        SpyBackend, SpySemantic, revenue_by_country, sales_graph,
    )
    from tests.fakes.fake_llm import FakeLLM
    from skifer.agentic.candidate_agent import MetricCandidateAgent
    from skifer.candidates import SqliteCandidateStore

    store = SqliteCandidateStore(str(tmp_path / "shadow.db"))
    try:
        agent = MetricCandidateAgent(SpySemantic(SpyBackend()), FakeLLM(revenue_by_country()),
                                     sales_graph(), store)
        genbi_answer = GENBI_ANSWERS["no_model"]
        hub, _, _ = _fallback_hub(genbi_answer, mode="shadow", agent=agent)
        response = hub.ask("amount per country in FR")
        [candidate] = store.list_candidates()
        short_id = candidate.candidate_id.rsplit(":", 1)[-1][:12]
        expected = dataclasses.replace(
            genbi_answer, question="amount per country in FR",
            explanation="nothing " + SHADOW_CANDIDATE_NOTE.format(short_id=short_id),
        )
        assert dataclasses.asdict(response) == dataclasses.asdict(expected)
        assert response.mode == "no_model" and response.candidate_id is None
        assert len(store.list_runs(candidate.candidate_id)) == 1
    finally:
        store.close()


def test_shadow_without_candidate_leaves_explanation_untouched():
    hub, _, agent = _fallback_hub(GENBI_ANSWERS["no_model"], mode="shadow")
    agent.ask.return_value = AgentResponse(question="q", mode="candidate_refused",
                                           policy_reasons=("no_table_found",))
    response = hub.ask("chiffre d'affaires par segment client")
    assert response.explanation == "nothing"


def test_shadow_swallows_agent_failure(caplog):
    hub, _, agent = _fallback_hub(GENBI_ANSWERS["needs_clarification_empty"], mode="shadow")
    agent.ask.side_effect = RuntimeError("secret value FR-123 in the message")
    baseline_hub, _, _ = _fallback_hub(GENBI_ANSWERS["needs_clarification_empty"])
    baseline_hub._candidate_agent = None
    with caplog.at_level("WARNING"):
        response = hub.ask("chiffre d'affaires par segment client")
    baseline = baseline_hub.ask("chiffre d'affaires par segment client")
    assert dataclasses.asdict(response) == dataclasses.asdict(baseline)
    messages = [record.getMessage() for record in caplog.records]
    assert sum("RuntimeError" in message for message in messages) == 1
    assert not any("FR-123" in message for message in messages)


def test_promoted_candidate_is_reused_not_duplicated(tmp_path):
    from tests.test_candidate_agent import (
        ORDERS, SpyBackend, SpySemantic, revenue_by_country, sales_graph,
    )
    from tests.fakes.fake_llm import FakeLLM
    from skifer.agentic.candidate_agent import MetricCandidateAgent
    from skifer.candidates import SqliteCandidateStore
    from skifer.knowledge import Edge, KnowledgeIndex, Node

    store = SqliteCandidateStore(str(tmp_path / "reuse.db"))
    try:
        first = MetricCandidateAgent(SpySemantic(SpyBackend()), FakeLLM(revenue_by_country("FR")),
                                     sales_graph(), store).ask("amount per country in FR")
        created = store.get_candidate(first.candidate_id)
        promoted = dataclasses.replace(created, state="promoted")
        store.upsert_candidate(promoted)

        base = sales_graph()
        node = Node.build(kind="metric", fqn=promoted.candidate_id, label=promoted.metric_name,
                          text="Revenue per country", status="candidate", source="candidate")
        nodes = [base.get(i) for i in sorted(base._nodes)] + [node]
        edges = base.edges() + [Edge.build(kind="belongs_to", from_id=node.node_id,
                                           to_id=f"table:{ORDERS}", source="candidate")]
        llm = FakeLLM(revenue_by_country("BE"))
        agent = MetricCandidateAgent(SpySemantic(SpyBackend()), llm,
                                     KnowledgeIndex(nodes, edges), store)
        hub, _, _ = _fallback_hub(GENBI_ANSWERS["no_model"], mode="on", agent=agent)
        response = hub.ask("amount per country in BE")

        assert response.candidate_id == promoted.candidate_id
        [candidate] = store.list_candidates()
        assert candidate.request_count == 2 and candidate.state == "promoted"
        context = llm.calls[0]["user"].split("Context (the only tables you may use):\n", 1)[1]
        assert list(json.loads(context))[0] == "promoted_candidates"
        assert json.loads(context)["promoted_candidates"][0]["name"] == promoted.candidate_id
    finally:
        store.close()
