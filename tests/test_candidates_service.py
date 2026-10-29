"""Human arbitration through ``CandidateService`` (Plan 44, S06)."""

from __future__ import annotations

from datetime import timedelta
import uuid

import pytest

from skifer.candidates import CertificationEvent, SqliteCandidateStore
from skifer.candidates.service import (
    CandidateService,
    DeciderIdentityError,
    StaleCandidateError,
    StateConflictError,
    UnknownCandidateError,
)
from skifer.candidates.validator import semantic_layer_version
from skifer.observability.tracing import TRACING_HMAC_SECRET_ENV
from tests.test_candidates_validator import NOW, make_candidate


class _Engine:
    def __init__(self, models_dir):
        self.models_dir = models_dir


REPORT = {"candidate_id": "x", "outcome": "needs_review", "semantic_layer_version": None,
          "results": [{"criterion": "coherence", "passed": False, "reasons": [
              {"code": "residue_present", "observed": 1, "threshold": 0, "verdict": "fail"}]}]}


@pytest.fixture
def store(tmp_path):
    candidate_store = SqliteCandidateStore(str(tmp_path / "candidates.db"))
    yield candidate_store
    candidate_store.close()


@pytest.fixture
def models_dir(tmp_path):
    (tmp_path / "semantic_catalog.yaml").write_text("models: []\n")
    return str(tmp_path)


def service(store, models_dir, **kwargs):
    kwargs.setdefault("decider_identity", "plain")
    return CandidateService(store, engine=_Engine(models_dir), clock=lambda: NOW, **kwargs)


def validated_event(candidate, previous, new_state, version, *, at=NOW):
    return CertificationEvent(
        event_id=uuid.uuid4().hex, metric_id=candidate.candidate_id,
        metric_name=candidate.metric_name, event_type=new_state, previous_state=previous,
        new_state=new_state, decided_by="agent:validator_v1", evidence=REPORT,
        semantic_layer_version=version, event_at=at,
    )


def seeded(store, models_dir, state="needs_review", *, version=None, **overrides):
    """A candidate in ``state`` whose last validation is recorded against ``version``."""
    candidate = make_candidate(state=state, **overrides)
    store.upsert_candidate(candidate)
    version = version or semantic_layer_version(models_dir)
    previous = {"needs_review": "candidate", "validated": "candidate", "promoted": "validated",
                "under_review": "promoted"}.get(state)
    if previous is not None:
        store.append_event(validated_event(candidate, previous, state, version))
    return candidate


# --- reading ---------------------------------------------------------------------------


def test_list_and_show_render_the_allowlisted_view(store, models_dir):
    candidate = seeded(store, models_dir, origin_question="Revenue in ? per country")
    seeded(store, models_dir, state="candidate", **_distinct("other"))
    views = service(store, models_dir).list(state="needs_review")
    assert [view.candidate_id for view in views] == [candidate.candidate_id]
    view = service(store, models_dir).show(candidate.candidate_id)
    assert view.to_dict() == {
        "candidate_id": candidate.candidate_id, "metric_name": "revenue_by_country",
        "state": "needs_review", "question": "Revenue in ? per country",
        "tables_used": ["main.sales.orders"], "request_count": 6, "distinct_requesters": 2,
        "stability_score": None, "criteria": REPORT["results"], "published_view": None,
        "published_metric_view": None, "updated_at": NOW.isoformat(),
    }
    assert "schema_dict" not in view.to_dict() and "distinct_requesters" in view.to_dict()


def test_unknown_candidate_and_state_are_named_errors(store, models_dir):
    with pytest.raises(UnknownCandidateError, match="candidates list"):
        service(store, models_dir).show("candidate:v1:" + "0" * 64)
    with pytest.raises(Exception, match="Unknown state"):
        service(store, models_dir).list(state="certified")


def test_history_is_chronological_even_on_equal_timestamps(store, models_dir):
    candidate = make_candidate(state="promoted")
    store.upsert_candidate(candidate)
    version = semantic_layer_version(models_dir)
    # Same timestamp, stored in an order the random event ids decide.
    for previous, new_state in [("validated", "promoted"), ("candidate", "validated")]:
        store.append_event(validated_event(candidate, previous, new_state, version))
    store.append_event(validated_event(candidate, "promoted", "under_review", version,
                                       at=NOW + timedelta(hours=1)))
    history = service(store, models_dir).history(candidate.candidate_id)
    assert [(e["previous_state"], e["new_state"]) for e in history] == [
        ("candidate", "validated"), ("validated", "promoted"), ("promoted", "under_review")]


# --- decisions -------------------------------------------------------------------------


def test_reject_and_review_write_one_event_each_with_the_reviewer(store, models_dir):
    rejected = seeded(store, models_dir)
    view = service(store, models_dir).reject(rejected.candidate_id, decider="alice",
                                             reason="duplicate of total_revenue")
    assert view.state == "rejected"
    stored = store.get_candidate(rejected.candidate_id)
    assert stored.reviewer == "human:alice"
    last = service(store, models_dir).history(rejected.candidate_id)[-1]
    assert (last["previous_state"], last["new_state"], last["decided_by"],
            last["justification"]) == (
        "needs_review", "rejected", "human:alice", "duplicate of total_revenue")

    promoted = seeded(store, models_dir, state="promoted", metric_name="promoted_one",
                      **_distinct("promoted_one"))
    view = service(store, models_dir).review(promoted.candidate_id, decider="bob",
                                             reason="usage dropped", signal="usage_drop")
    assert view.state == "under_review"
    last = service(store, models_dir).history(promoted.candidate_id)[-1]
    assert last["evidence"] == {"signal": "usage_drop"}
    assert last["semantic_layer_version"] == semantic_layer_version(models_dir)


def _distinct(target):
    from skifer.candidates import candidate_id_for, fingerprint_candidate
    from tests.test_candidates_validator import country_schema

    schema = country_schema(aggregate={"group_by": ["country"],
                                       "measures": [["amount", target, "sum"]]})
    fingerprint = fingerprint_candidate(schema)
    return {"schema_dict": schema, "fingerprint": fingerprint,
            "candidate_id": candidate_id_for(fingerprint)}


def test_forbidden_transition_is_a_state_conflict(store, models_dir):
    candidate = seeded(store, models_dir, state="candidate")
    with pytest.raises(StateConflictError, match="is candidate; it cannot become under_review"):
        service(store, models_dir).review(candidate.candidate_id, decider="a", reason="r")
    assert store.list_events(metric_id=candidate.candidate_id) == []


# --- S06-R8 ----------------------------------------------------------------------------


def test_stale_candidate_refused_except_reject(store, models_dir, tmp_path):
    candidate = seeded(store, models_dir, state="promoted")
    (tmp_path / "semantic_catalog.yaml").write_text("models: [orders]\n")
    with pytest.raises(StaleCandidateError, match="skifer candidates validate --candidate"):
        service(store, models_dir).review(candidate.candidate_id, decider="a", reason="r")
    assert len(store.list_events(metric_id=candidate.candidate_id)) == 1

    other = seeded(store, models_dir, version="sha256:v1:" + "f" * 64, **_distinct("other"))
    assert service(store, models_dir).reject(other.candidate_id, decider="a",
                                             reason="stale but refused").state == "rejected"


# --- S06-R10 ---------------------------------------------------------------------------


def test_hmac_without_secret_refuses_and_writes_nothing(store, models_dir, monkeypatch):
    monkeypatch.delenv(TRACING_HMAC_SECRET_ENV, raising=False)
    candidate = seeded(store, models_dir)
    hmac_service = service(store, models_dir, decider_identity="hmac")
    with pytest.raises(DeciderIdentityError, match=TRACING_HMAC_SECRET_ENV) as raised:
        hmac_service.reject(candidate.candidate_id, decider="alice", reason="no")
    assert "candidates_decider_identity: plain" in str(raised.value)
    assert len(store.list_events(metric_id=candidate.candidate_id)) == 1
    assert store.get_candidate(candidate.candidate_id) == candidate

    monkeypatch.setenv(TRACING_HMAC_SECRET_ENV, "s3cret")
    hmac_service.reject(candidate.candidate_id, decider="alice", reason="no")
    decided_by = hmac_service.history(candidate.candidate_id)[-1]["decided_by"]
    assert decided_by.startswith("human:") and "alice" not in decided_by


def test_decider_identity_is_validated():
    with pytest.raises(ValueError, match="decider_identity"):
        CandidateService(None, decider_identity="email")


# --- S06.2: approve, promote, demote ----------------------------------------------------

from pathlib import Path  # noqa: E402

import yaml  # noqa: E402

from skifer.candidates import service as service_module  # noqa: E402
from skifer.candidates.models import ALLOWED_TRANSITIONS, CANDIDATE_STATES  # noqa: E402
from skifer.candidates.promotion import level1_sql  # noqa: E402
from skifer.candidates.service import (  # noqa: E402
    ApprovalRefusedError,
    CandidateUsageError,
    OutputExistsError,
)
from skifer.knowledge.store import SqliteKnowledgeStore  # noqa: E402
from tests.test_candidate_agent import (  # noqa: E402
    CUSTOMERS,
    ORDERS,
    SpyBackend,
    SpySemantic,
    sales_graph,
)
from tests.test_candidates_validator import segment_schema  # noqa: E402

RESIDUE = ({"code": "not_in_nullable_pitfall", "message": "?", "blocking": False},)


@pytest.fixture
def full(tmp_path, monkeypatch, store, models_dir):
    """A service with a real certification gate, a knowledge store and a recording backend."""
    monkeypatch.chdir(tmp_path)
    backend = SpyBackend()
    engine = SpySemantic(backend)
    engine.models_dir = models_dir
    engine.core.db = "main"
    knowledge = SqliteKnowledgeStore(str(tmp_path / "knowledge.db"))
    graph = sales_graph()
    knowledge.upsert_nodes([graph.get(node_id) for node_id in sorted(graph._nodes)])
    knowledge.upsert_edges(graph.edges())
    full_service = CandidateService(store, engine=engine, index_store=knowledge,
                                    decider_identity="plain", clock=lambda: NOW)
    yield full_service, backend, knowledge
    knowledge.close()


METHOD_TARGETS = {"approve": "promoted", "reject": "rejected", "review": "under_review",
                  "demote": "demoted"}


@pytest.mark.parametrize("method", sorted(METHOD_TARGETS))
@pytest.mark.parametrize("state", CANDIDATE_STATES)
def test_transition_matrix_through_service(full, store, models_dir, state, method):
    service_, _, _ = full
    candidate = seeded(store, models_dir, state=state)
    target = METHOD_TARGETS[method]
    call = getattr(service_, method)
    if target in ALLOWED_TRANSITIONS[state]:
        assert call(candidate.candidate_id, decider="alice", reason="r").state == target
        assert store.get_candidate(candidate.candidate_id).state == target
    else:
        events = len(store.list_events(metric_id=candidate.candidate_id))
        with pytest.raises(StateConflictError):
            call(candidate.candidate_id, decider="alice", reason="r")
        assert len(store.list_events(metric_id=candidate.candidate_id)) == events


def test_approve_refuses_non_waivable_reasons(full, store, models_dir):
    service_, backend, _ = full
    gone = seeded(store, models_dir, tables_used=(ORDERS, "main.sales.dropped"))
    with pytest.raises(ApprovalRefusedError, match="missing_table can never be waived"):
        service_.approve(gone.candidate_id, decider="alice", reason="looks fine")
    assert store.get_candidate(gone.candidate_id).state == "needs_review"
    assert backend.executed == []

    service_.engine.core._context.env = {"semantic_certification_policy": "enforce"}
    uncertified = seeded(store, models_dir, **_distinct("uncertified"))
    with pytest.raises(ApprovalRefusedError, match="source_not_certified"):
        service_.approve(uncertified.candidate_id, decider="alice", reason="r")
    assert backend.executed == []


def test_approve_waives_residue_and_undeclared_join(full, store, models_dir, tmp_path):
    from skifer.candidates import candidate_id_for, fingerprint_candidate

    service_, backend, knowledge = full
    knowledge.mark_edges_removed([e.edge_id for e in knowledge.list_edges() if e.kind == "joins"])
    schema = segment_schema()
    candidate = seeded(
        store, models_dir, schema_dict=schema, residues=RESIDUE, metric_name="revenue_by_segment",
        fingerprint=fingerprint_candidate(schema),
        candidate_id=candidate_id_for(fingerprint_candidate(schema)),
        tables_used=(CUSTOMERS, ORDERS),
        joins_used=((ORDERS, CUSTOMERS, "customer_id=id"),),
    )
    view = service_.approve(candidate.candidate_id, decider="alice", reason="residue reviewed")
    assert view.state == "promoted"
    assert view.published_view == "main.semantic_candidates.revenue_by_segment"
    assert any("CREATE OR REPLACE VIEW `main`.`semantic_candidates`" in sql
               for sql in backend.executed)
    last = service_.history(candidate.candidate_id)[-1]
    assert last["decided_by"] == "human:alice"
    assert last["evidence"]["waived"] == ["residue_present", "undeclared_join"]
    shortcut = [e for e in knowledge.list_edges() if e.kind == "shortcut"]
    assert len(shortcut) == 1 and shortcut[0].weight == 0.8


def _promote(service_, store, models_dir, **kwargs):
    candidate = seeded(store, models_dir, state="promoted",
                       origin_question="Revenue in ? per country")
    return candidate, service_.promote(candidate.candidate_id, decider="alice",
                                       reason="ready for certification", **kwargs)


def test_promote_writes_only_under_drafts(full, store, models_dir, tmp_path):
    service_, _, _ = full
    catalog = Path(models_dir) / "semantic_catalog.yaml"
    before = catalog.read_text()
    candidate = seeded(store, models_dir, state="promoted")
    with pytest.raises(CandidateUsageError, match=".drafts"):
        service_.promote(candidate.candidate_id, decider="a", reason="r",
                         pipeline_output="schemas/gold/revenue.yaml")
    assert not (tmp_path / "schemas").exists()

    result = service_.promote(candidate.candidate_id, decider="a", reason="r")
    assert result["pipeline_draft"] == "schemas/.drafts/candidates/revenue_by_country.yaml"
    assert result["semantic_draft"] == str(
        Path(models_dir) / ".drafts" / "candidates.revenue_by_country.yaml")
    for path in (result["pipeline_draft"], result["semantic_draft"]):
        assert ".drafts" in Path(path).parts
    assert catalog.read_text() == before
    assert store.get_candidate(candidate.candidate_id).state == "promoted"  # unchanged
    last = service_.history(candidate.candidate_id)[-1]
    assert (last["event_type"], last["previous_state"], last["new_state"]) == (
        "certification_requested", "promoted", "promoted")
    assert last["evidence"] == result


def test_promote_never_overwrites_even_under_race(full, store, models_dir, tmp_path, monkeypatch):
    service_, _, _ = full
    candidate = seeded(store, models_dir, state="promoted")
    pipeline = tmp_path / "schemas/.drafts/candidates/revenue_by_country.yaml"
    semantic = Path(models_dir) / ".drafts" / "candidates.revenue_by_country.yaml"

    semantic.parent.mkdir(parents=True)
    semantic.write_text("human: true\n")
    with pytest.raises(OutputExistsError):
        service_.promote(candidate.candidate_id, decider="a", reason="r")
    assert not pipeline.exists() and semantic.read_text() == "human: true\n"
    semantic.unlink()

    real_load = service_module.load_schema

    def racing_load(path):
        semantic.write_text("raced in\n")  # appears after the existence check
        return real_load(path)

    monkeypatch.setattr(service_module, "load_schema", racing_load)
    with pytest.raises(OutputExistsError):
        service_.promote(candidate.candidate_id, decider="a", reason="r")
    assert not pipeline.exists()
    assert semantic.read_text() == "raced in\n"
    assert all(e["event_type"] != "certification_requested"
               for e in service_.history(candidate.candidate_id))


def test_pipeline_draft_loads_and_projects(full, store, models_dir):
    from skifer import load_schema
    from skifer.core.ir import parse_to_ir
    from skifer.semantic.output_projection import OutputProjector
    from skifer.semantic.validator import SemanticValidator

    service_, _, _ = full
    candidate, result = _promote(service_, store, models_dir)
    parsed = load_schema(result["pipeline_draft"])
    assert parsed["semantic"]["source_candidate_id"] == candidate.candidate_id
    assert parsed["data_product"] == {"id": "candidates.revenue_by_country", "version": "0.1.0",
                                      "description": "Revenue per country"}
    schema = parse_to_ir(parsed)
    payload = yaml.safe_load(Path(result["semantic_draft"]).read_text())
    assert SemanticValidator().validate_against_projection(
        payload, OutputProjector().project(schema), contract_output=[]).ok
    assert payload["models"][0]["metadata"]["source_candidate_id"] == candidate.candidate_id
    assert yaml.safe_load(Path(result["pipeline_draft"]).read_text())["_generated_by"] == (
        "metric_candidates")


def test_parameterized_filters_become_dimensions(full, store, models_dir):
    service_, _, _ = full
    _, result = _promote(service_, store, models_dir)
    assert result["filters_to_dimensions"] == ["country"]
    text = Path(result["pipeline_draft"]).read_text()
    assert "{{" not in text and "p1" not in text
    assert "# filters_to_dimensions: country\n" in text
    draft = yaml.safe_load(text)
    assert draft["aggregate"]["group_by"] == ["country"]
    assert draft["contract"]["grain"] == ["country"]
    assert draft["semantic"]["dimensions"] == ["country"]
    assert "filter" not in draft["tables"][0]


def test_level1_and_level2_sql_match(full, store, models_dir):
    from skifer import parse_schema
    from skifer.core.ir import parse_to_ir
    from skifer.core.sql_compiler import compile_select

    service_, _, _ = full
    candidate, result = _promote(service_, store, models_dir)
    draft = yaml.safe_load(Path(result["pipeline_draft"]).read_text())
    for block in ("data_product", "contract", "semantic", "_generated_by"):
        draft.pop(block)
    level2 = compile_select(parse_to_ir(parse_schema(yaml.safe_dump(draft))), allow_raw_sql=False)
    assert level2 == level1_sql(candidate)


def test_demote_drops_views_and_history_tells_replacement(full, store, models_dir):
    service_, backend, knowledge = full
    x = seeded(store, models_dir, state="promoted",
               published_view="main.semantic_candidates.revenue_by_country",
               published_metric_view="main.semantic_candidates.revenue_by_country_metrics")
    y = seeded(store, models_dir, state="promoted", metric_name="revenue_v2",
               **_distinct("revenue_v2"))
    from skifer.candidates.promotion import level1_index_entries

    node, _ = level1_index_entries(x)
    knowledge.upsert_nodes([node])

    view = service_.demote(x.candidate_id, decider="alice", reason="superseded",
                           replaced_by=y.candidate_id)
    assert view.state == "demoted"
    assert (view.published_view, view.published_metric_view) == (None, None)
    assert backend.executed == [
        "DROP VIEW IF EXISTS `main`.`semantic_candidates`.`revenue_by_country_metrics`",
        "DROP VIEW IF EXISTS `main`.`semantic_candidates`.`revenue_by_country`",
    ]
    demoted = service_.history(x.candidate_id)[-1]
    assert (demoted["new_state"], demoted["replaced_by_metric"]) == ("demoted", y.candidate_id)
    replaced = service_.history(y.candidate_id)[-1]
    assert (replaced["event_type"], replaced["replaces_metric"]) == ("replaced", x.candidate_id)
    assert store.get_candidate(y.candidate_id).state == "promoted"
    assert node.node_id not in {n.node_id for n in knowledge.list_nodes()}

    z = seeded(store, models_dir, state="promoted", **_distinct("z"))
    with pytest.raises(CandidateUsageError, match="replaced-by"):
        service_.demote(z.candidate_id, decider="a", reason="r", replaced_by="sales.revenue")
    service_.demote(z.candidate_id, decider="a", reason="r",
                    replaced_by="model:sales.orders.total_revenue")
    assert store.list_events(metric_id="model:sales.orders.total_revenue")[0].replaces_metric == (
        z.candidate_id)
