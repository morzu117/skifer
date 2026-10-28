"""Candidate validation: coherence, frequency, stability (Plan 44, S05)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
import yaml

from skifer import parse_schema
from skifer.candidates import (
    CandidateRun,
    MetricCandidate,
    SqliteCandidateStore,
    candidate_id_for,
    fingerprint_candidate,
    parameterize_filters,
    question_hash,
)
from skifer.candidates.validator import (
    CandidateValidator,
    ValidationThresholds,
    semantic_layer_version,
)
from tests.test_candidate_agent import (
    CUSTOMERS,
    ORDERS,
    ROWS,
    SpyBackend,
    SpySemantic,
    sales_graph,
)

NOW = datetime(2026, 10, 28, 12, tzinfo=timezone.utc)
ZERO = ValidationThresholds(min_requests=0, min_distinct_requesters=0, min_days=0,
                            stability_runs=1, min_stability=1.0)


def country_schema(**extra) -> dict:
    raw = {
        "tables": [{"name": ORDERS, "alias": "o", "filter": ["country:equals:FR"]}],
        "aggregate": {"group_by": ["country"], "measures": [["amount", "revenue", "sum"]]},
    }
    raw.update(extra)
    return parameterize_filters(parse_schema(yaml.safe_dump(raw)))[0]


def segment_schema() -> dict:
    raw = {
        "tables": [{"name": ORDERS, "alias": "o"}, {"name": CUSTOMERS, "alias": "c"}],
        "join": [{"table_from": ["o", "customer_id"], "table_to": ["c", "id"], "type": "left"}],
        "aggregate": {"group_by": ["segment"], "measures": [["amount", "revenue", "sum"]]},
    }
    return parameterize_filters(parse_schema(yaml.safe_dump(raw)))[0]


def make_candidate(schema=None, **overrides) -> MetricCandidate:
    schema = schema if schema is not None else country_schema()
    fingerprint = fingerprint_candidate(schema)
    values = dict(
        candidate_id=candidate_id_for(fingerprint), metric_name="revenue_by_country",
        metric_description="Revenue per country", origin_question=None,
        origin_question_hash=question_hash("q"), schema_dict=schema,
        sql_hash="sha256:v1:" + "0" * 64, fingerprint=fingerprint,
        tables_used=tuple(sorted(t["name"] for t in schema["tables"])), columns_used=(),
        joins_used=(), business_rules_refs=(), residues=(), state="candidate",
        request_count=6, distinct_requesters=("h:a", "h:b"), created_at=NOW, updated_at=NOW,
    )
    values.update(overrides)
    return MetricCandidate(**values)


@pytest.fixture
def store(tmp_path):
    candidate_store = SqliteCandidateStore(str(tmp_path / "candidates.db"))
    yield candidate_store
    candidate_store.close()


def answered_on(store, candidate, *days):
    for day in days:
        store.append_run(CandidateRun(
            run_id=f"answer-{day}", candidate_id=candidate.candidate_id,
            executed_at=NOW + timedelta(days=day), purpose="answer", result_hash="sha256:v1:x",
        ))


def validator(store, *, backend=None, env=None, thresholds=ValidationThresholds(),
              index=None, models_dir=None):
    semantic = SpySemantic(backend or SpyBackend(), env=env)
    semantic.models_dir = models_dir
    return CandidateValidator(store, semantic, index or sales_graph(),
                              thresholds=thresholds, clock=lambda: NOW)


def reasons(result):
    return {reason["code"]: reason for reason in result.reasons}


def test_thresholds_are_validated():
    for bad in ({"min_requests": -1}, {"min_days": True}, {"stability_runs": 0},
                {"min_stability": 1.5}, {"min_stability": -0.1}):
        with pytest.raises(ValueError):
            ValidationThresholds(**bad)


# --- S05-R1 ----------------------------------------------------------------------------


def test_residue_never_promoted_even_with_zero_thresholds(store):
    residue = {"code": "not_in_nullable_pitfall", "blocking": False, "message": "NOT IN ?"}
    candidate = make_candidate(residues=(residue,))
    store.upsert_candidate(candidate)
    answered_on(store, candidate, 0, 1)
    backend = SpyBackend()

    report = validator(store, backend=backend, thresholds=ZERO).validate(candidate.candidate_id)

    assert report.outcome == "needs_review"
    assert reasons(report.criterion("coherence"))["residue_present"] == {
        "code": "residue_present", "observed": 1, "threshold": 0, "verdict": "fail"}
    assert report.criterion("frequency").passed is True
    assert backend.executed == []  # not even run for stability


# --- S05-R2 ----------------------------------------------------------------------------


def test_coherence_reasons(store):
    ok = validator(store).coherence(make_candidate())
    assert ok.passed is True
    assert set(reasons(ok)) == {"residue_present", "invalid_schema", "missing_table",
                                "undeclared_join", "unsafe_fanout", "source_not_certified"}
    for reason in ok.reasons:
        assert set(reason) == {"code", "observed", "threshold", "verdict"}

    declared = validator(store).coherence(make_candidate(segment_schema()))
    assert reasons(declared)["undeclared_join"]["verdict"] == "pass"

    bare = sales_graph()
    bare._edges = [edge for edge in bare._edges if edge.kind != "joins"]
    undeclared = validator(store, index=bare).coherence(make_candidate(segment_schema()))
    assert undeclared.passed is False
    assert reasons(undeclared)["undeclared_join"]["observed"] == [
        f"{ORDERS}.customer_id={CUSTOMERS}.id"]

    gone = make_candidate(tables_used=(ORDERS, "main.sales.dropped"))
    missing = validator(store).coherence(gone)
    assert reasons(missing)["missing_table"] == {
        "code": "missing_table", "observed": ["main.sales.dropped"], "threshold": [],
        "verdict": "fail"}


def test_frequency_reasons(store):
    candidate = make_candidate(request_count=4, distinct_requesters=("h:a",))
    answered_on(store, candidate, 0, 0, 3)
    loose = ValidationThresholds(min_requests=4, min_distinct_requesters=1, min_days=2)
    result = validator(store, thresholds=loose).frequency(candidate)
    assert result.passed is True
    assert reasons(result)["min_days"]["observed"] == 2

    strict = ValidationThresholds(min_requests=5, min_distinct_requesters=2, min_days=3)
    result = validator(store, thresholds=strict).frequency(candidate)
    assert result.passed is False
    assert [(r["code"], r["observed"], r["threshold"], r["verdict"]) for r in result.reasons] == [
        ("min_requests", 4, 5, "fail"),
        ("min_distinct_requesters", 1, 2, "fail"),
        ("min_days", 2, 3, "fail"),
    ]


def test_frequency_ignores_failed_answer_runs(store):
    candidate = make_candidate()
    answered_on(store, candidate, 0)
    store.append_run(CandidateRun(run_id="failed", candidate_id=candidate.candidate_id,
                                  executed_at=NOW + timedelta(days=5), purpose="answer",
                                  error_type="RuntimeError"))
    assert reasons(validator(store).frequency(candidate))["min_days"]["observed"] == 1


class _ShiftingBackend(SpyBackend):
    """A source that changes between two runs: a different result on each call."""

    def execute_sql(self, sql):
        frame = super().execute_sql(sql)
        if not sql.startswith("EXPLAIN"):
            frame.rows = [{**ROWS[0], "revenue": len(self.executed)}]
        return frame


def test_stability_reasons(store):
    candidate = make_candidate()
    stable = validator(store, thresholds=ValidationThresholds(stability_runs=3))
    result = stable.stability(candidate)
    assert result.passed is True
    assert result.reasons == ({"code": "min_stability", "observed": 1.0, "threshold": 1.0,
                               "verdict": "pass"},)
    runs = store.list_runs(candidate.candidate_id, purpose="stability")
    assert len(runs) == 3 and len({run.result_hash for run in runs}) == 1

    shifting = validator(store, backend=_ShiftingBackend(),
                         thresholds=ValidationThresholds(stability_runs=3, min_stability=0.5))
    result = shifting.stability(make_candidate(metric_name="other"))
    assert result.passed is False
    assert result.reasons[0]["observed"] == pytest.approx(1 / 3, abs=1e-6)


def test_stability_runs_the_published_definition_not_the_question(store):
    backend = SpyBackend()
    validator(store, backend=backend, thresholds=ZERO).stability(make_candidate())
    executed = [sql for sql in backend.executed if not sql.startswith("EXPLAIN")]
    assert executed and all("{{" not in sql and "GROUP BY `country`" in sql for sql in executed)
    assert all("WHERE" not in sql for sql in executed)


def test_dry_run_stability_reads_recorded_runs_only(store):
    candidate = make_candidate()
    backend = SpyBackend()
    check = validator(store, backend=backend, thresholds=ValidationThresholds(stability_runs=2))
    assert reasons(check.stability(candidate, execute=False)) == {"stability_not_measured": {
        "code": "stability_not_measured", "observed": 0, "threshold": 2, "verdict": "fail"}}
    assert backend.executed == [] and store.list_runs(candidate.candidate_id) == []


# --- S05-R3 ----------------------------------------------------------------------------


def test_frequency_fails_without_pseudonyms(store):
    candidate = make_candidate(distinct_requesters=())
    answered_on(store, candidate, 0, 1)
    result = validator(store, thresholds=ZERO).frequency(candidate)
    assert result.passed is False
    assert reasons(result)["requesters_unavailable"] == {
        "code": "requesters_unavailable", "observed": 0, "threshold": 0, "verdict": "fail"}


# --- S05-R4 ----------------------------------------------------------------------------


def test_pending_frequency_keeps_state_and_writes_no_event(store):
    candidate = make_candidate(request_count=1)
    store.upsert_candidate(candidate)
    backend = SpyBackend()

    report = validator(store, backend=backend).validate(candidate.candidate_id)

    assert report.outcome == "pending"
    assert store.get_candidate(candidate.candidate_id) == candidate
    assert store.list_events(metric_id=candidate.candidate_id) == []
    assert store.list_runs(candidate.candidate_id) == [] and backend.executed == []
    assert report.criterion("stability").reasons[0]["verdict"] == "skipped"


# --- S05-R5 ----------------------------------------------------------------------------


def test_uncertified_source_goes_to_needs_review_under_enforce(store):
    candidate = make_candidate()
    store.upsert_candidate(candidate)
    answered_on(store, candidate, 0, 1)
    report = validator(store, thresholds=ZERO,
                       env={"semantic_certification_policy": "enforce"}).validate(
        candidate.candidate_id)
    assert report.outcome == "needs_review"
    assert reasons(report.criterion("coherence"))["source_not_certified"] == {
        "code": "source_not_certified", "observed": "DENY", "threshold": "ALLOW",
        "verdict": "fail"}

    warn = validator(store, thresholds=ZERO, env={"semantic_certification_policy": "warn"})
    assert warn.validate(candidate.candidate_id).outcome == "validated"


def test_validated_when_all_three_pass(store, tmp_path):
    candidate = make_candidate()
    store.upsert_candidate(candidate)
    answered_on(store, candidate, 0, 1)
    (tmp_path / "semantic_catalog.yaml").write_text("models: []\n")
    report = validator(store, models_dir=str(tmp_path)).validate(candidate.candidate_id)
    assert report.outcome == "validated"
    assert report.semantic_layer_version == semantic_layer_version(str(tmp_path))
    assert report.to_dict()["results"][2]["criterion"] == "stability"
    assert store.get_candidate(candidate.candidate_id) == candidate  # judging never writes state
