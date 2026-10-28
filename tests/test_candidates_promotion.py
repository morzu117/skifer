"""Level 1 promotion of a candidate (Plan 44, S05)."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
import yaml

from skifer import parse_schema
from skifer.candidates import (
    MetricCandidate,
    candidate_id_for,
    fingerprint_candidate,
    parameterize_filters,
    question_hash,
)
from skifer.candidates import promotion
from skifer.candidates.promotion import (
    level1_metric_view_ddl,
    level1_model,
    level1_view_ddl,
    metric_definition,
)
from skifer.core.ir import parse_to_ir
from skifer.core.sql_compiler import compile_select
from skifer.semantic.evidence import hash_sql
from skifer.semantic.metric_views import MetricViewTranslationError

NOW = datetime(2026, 10, 28, 12, tzinfo=timezone.utc)


def stored_schema(**overrides) -> dict:
    """A candidate schema as S03 stores it: parse_schema output, filters parameterized."""
    raw = {
        "tables": [{
            "name": "sales.orders", "alias": "o",
            "filter": ["country:equals:FR", "region:in:A,B", "order_id:is_not_null"],
        }],
        "where": ["status:equals:PAID"],
        "aggregate": {
            "group_by": ["segment"],
            "measures": [["amount", "total_amount", "sum"], ["order_id", "nb_orders", "count"]],
            "having": ["total_amount:greater_than:1000"],
        },
    }
    raw.update(overrides)
    schema, _ = parameterize_filters(parse_schema(yaml.safe_dump(raw)))
    return schema


def make_candidate(schema: dict | None = None, **overrides) -> MetricCandidate:
    schema = schema if schema is not None else stored_schema()
    fingerprint = fingerprint_candidate(schema)
    values = dict(
        candidate_id=candidate_id_for(fingerprint),
        metric_name="revenue_by_segment",
        metric_description="Revenue per customer segment",
        origin_question=None,
        origin_question_hash=question_hash("Revenue per segment in FR?"),
        schema_dict=schema,
        sql_hash="sha256:v1:" + "0" * 64,
        fingerprint=fingerprint,
        tables_used=("sales.orders",),
        columns_used=(),
        joins_used=(),
        business_rules_refs=(),
        residues=(),
        state="validated",
        created_at=NOW,
        updated_at=NOW,
    )
    values.update(overrides)
    return MetricCandidate(**values)


# --- metric_definition ------------------------------------------------------------------


def test_metric_definition_turns_parameterized_filters_into_dimensions():
    schema = stored_schema()
    before = yaml.safe_dump(schema)
    definition, converted = metric_definition(schema)

    assert yaml.safe_dump(schema) == before  # pure
    assert converted == ("country", "region", "status")
    assert definition["aggregate"]["group_by"] == ["segment", "country", "region", "status"]
    assert definition["tables"][0]["filter"] == [{"column": "order_id", "operator": "is_not_null"}]
    assert "where" not in definition and "having" not in definition["aggregate"]
    assert "{{" not in yaml.safe_dump(definition)


def test_metric_definition_drops_groups_left_always_true():
    schema = stored_schema(where_groups=[["country:equals:FR"], ["country:is_null"]])
    definition, converted = metric_definition(schema)
    assert "where_groups" not in definition and "country" in converted

    kept = stored_schema(where_groups=[["country:equals:FR", "segment:is_null"], ["segment:is_null"]])
    definition, _ = metric_definition(kept)
    assert definition["where_groups"] == [
        [{"column": "segment", "operator": "is_null"}], [{"column": "segment", "operator": "is_null"}]
    ]


def test_metric_definition_refuses_a_candidate_without_aggregate():
    with pytest.raises(ValueError, match="aggregate"):
        metric_definition({"tables": [{"name": "sales.orders", "alias": "o"}]})


# --- level 1 DDL --------------------------------------------------------------------------


def test_level1_sql_equals_compile_select():
    candidate = make_candidate()
    expected = compile_select(parse_to_ir(metric_definition(candidate.schema_dict)[0]))
    ddl = level1_view_ddl(candidate, "main", "semantic")

    assert ddl.endswith("AS " + expected)
    assert ddl.startswith("CREATE OR REPLACE VIEW `main`.`semantic_candidates`.`revenue_by_segment`")
    assert f"skifer candidate {candidate.candidate_id} sql_hash={hash_sql(expected)}" in ddl
    for statement in (ddl, level1_metric_view_ddl(candidate, "main", "semantic")):
        assert "{{" not in statement and "p1" not in statement


def test_level1_view_is_two_part_without_catalog():
    ddl = level1_view_ddl(make_candidate(), None, "semantic")
    assert ddl.startswith("CREATE OR REPLACE VIEW `semantic_candidates`.`revenue_by_segment`\n")


def test_level1_model_rolls_up_on_the_view_and_metric_view_targets_candidates_schema():
    candidate = make_candidate()
    model = level1_model(candidate, "semantic")
    assert model["table"] == "semantic_candidates.revenue_by_segment"
    assert [d["name"] for d in model["dimensions"]] == ["segment", "country", "region", "status"]
    assert model["metrics"] == [
        {"name": "total_amount", "sql": "total_amount", "type": "sum"},
        {"name": "nb_orders", "sql": "nb_orders", "type": "sum"},
    ]
    ddl = level1_metric_view_ddl(candidate, "main", "semantic")
    assert ddl.startswith(
        "CREATE OR REPLACE VIEW `main`.`semantic_candidates`.`revenue_by_segment_metrics`\nWITH METRICS"
    )
    assert "source: main.semantic_candidates.revenue_by_segment\n" in ddl
    assert "expr: SUM(nb_orders)" in ddl


def test_level1_model_refuses_a_measure_that_does_not_roll_up():
    schema = stored_schema(aggregate={
        "group_by": ["segment"], "measures": [["amount", "avg_amount", "avg"]],
    })
    with pytest.raises(MetricViewTranslationError, match="avg_amount"):
        level1_model(make_candidate(schema), "semantic")


def test_tag_key_has_no_reserved_character():
    assert promotion.LEVEL1_PUBLISHES_METRIC_VIEW is True
    assert not set(promotion.CANDIDATE_TAG_KEY) & {".", "="}
    assert promotion.tag_ddl("`c`.`s`.`v`") == (
        "ALTER VIEW `c`.`s`.`v` SET TAGS ('certification' = 'CANDIDATE')"
    )


# --- S05.3: promote_level1, transitions, index -----------------------------------------


class RecordingBackend:
    def __init__(self, fail_on: str | None = None):
        self.executed: list[str] = []
        self.fail_on = fail_on

    def execute_sql(self, sql):
        if self.fail_on and self.fail_on in sql:
            raise RuntimeError("PERMISSION_DENIED on `main`.`semantic_candidates`")
        self.executed.append(sql)


def _written(statement: str) -> str:
    """The object a statement writes, as 'catalog.schema' (reads are allowed anywhere)."""
    import re

    match = re.match(
        r"(?:CREATE SCHEMA IF NOT EXISTS|CREATE OR REPLACE VIEW|ALTER VIEW) (\S+)", statement)
    parts = match.group(1).replace("`", "").split(".")
    return ".".join(parts[:2])


def test_ddl_targets_only_candidates_schema():
    backend = RecordingBackend()
    statements = promotion.promote_level1(make_candidate(), backend=backend, catalog="main",
                                          schema="semantic")
    assert backend.executed == list(statements)
    assert statements[0] == "CREATE SCHEMA IF NOT EXISTS `main`.`semantic_candidates`"
    for statement in statements:
        assert _written(statement) == "main.semantic_candidates", statement
        assert "`semantic`." not in statement and "{{" not in statement
    metric_view = next(statement for statement in statements if "WITH METRICS" in statement)
    assert "source: main.semantic_candidates.revenue_by_segment\n" in metric_view
    dry = RecordingBackend()
    assert promotion.promote_level1(make_candidate(), backend=dry, catalog="main",
                                    schema="semantic", dry_run=True) == statements
    assert dry.executed == []


def test_level1_with_and_without_metric_view(monkeypatch):
    candidate = make_candidate()
    view = "`main`.`semantic_candidates`.`revenue_by_segment`"
    metric_view = "`main`.`semantic_candidates`.`revenue_by_segment_metrics`"

    monkeypatch.setattr(promotion, "LEVEL1_PUBLISHES_METRIC_VIEW", True)
    with_mv = promotion.level1_ddl(candidate, "main", "semantic")
    assert with_mv == (
        "CREATE SCHEMA IF NOT EXISTS `main`.`semantic_candidates`",
        level1_view_ddl(candidate, "main", "semantic"),
        level1_metric_view_ddl(candidate, "main", "semantic"),
        promotion.tag_ddl(view),
        promotion.tag_ddl(metric_view),
    )

    monkeypatch.setattr(promotion, "LEVEL1_PUBLISHES_METRIC_VIEW", False)
    without = promotion.level1_ddl(candidate, "main", "semantic")
    assert without == (
        "CREATE SCHEMA IF NOT EXISTS `main`.`semantic_candidates`",
        level1_view_ddl(candidate, "main", "semantic"),
        promotion.tag_ddl(view),
    )
    assert not any("WITH METRICS" in statement for statement in without)


def test_level1_local_has_no_metric_view_and_no_tag():
    statements = promotion.level1_ddl(make_candidate(), None, "semantic")
    assert statements == (
        "CREATE SCHEMA IF NOT EXISTS `semantic_candidates`",
        level1_view_ddl(make_candidate(), None, "semantic"),
    )


class _Report:
    def __init__(self, candidate_id, outcome):
        self.candidate_id, self.outcome = candidate_id, outcome
        self.semantic_layer_version = "sha256:v1:" + "a" * 64

    def to_dict(self):
        return {"candidate_id": self.candidate_id, "outcome": self.outcome}


@pytest.fixture
def store(tmp_path):
    from skifer.candidates import SqliteCandidateStore

    candidate_store = SqliteCandidateStore(str(tmp_path / "candidates.db"))
    yield candidate_store
    candidate_store.close()


@pytest.fixture
def knowledge(tmp_path):
    from skifer.knowledge.store import SqliteKnowledgeStore

    knowledge_store = SqliteKnowledgeStore(str(tmp_path / "knowledge.db"))
    yield knowledge_store
    knowledge_store.close()


def _apply(store, candidate, outcome, *, backend=None, catalog="main", **kwargs):
    store.upsert_candidate(candidate)
    return promotion.apply_report(
        _Report(candidate.candidate_id, outcome), store=store,
        backend=backend or RecordingBackend(), catalog=catalog, schema="semantic",
        clock=lambda: NOW, **kwargs,
    )


def joined_candidate(**overrides):
    return make_candidate(
        state="candidate", joins_used=(("sales.orders", "sales.customers", "customer_id=id"),),
        origin_question="Revenue per segment in ?", **overrides,
    )


def test_validated_report_promotes_with_two_events_and_index_entries(store, knowledge):
    from skifer.knowledge.index import KnowledgeIndex

    candidate = joined_candidate()
    result = _apply(store, candidate, "validated", knowledge_store=knowledge)

    assert (result.previous_state, result.new_state, result.error_type) == (
        "candidate", "promoted", None)
    stored = store.get_candidate(candidate.candidate_id)
    assert stored.state == "promoted"
    assert stored.published_view == "main.semantic_candidates.revenue_by_segment"
    assert stored.published_metric_view == "main.semantic_candidates.revenue_by_segment_metrics"
    events = sorted(store.list_events(metric_id=candidate.candidate_id),
                    key=lambda event: ["validated", "promoted"].index(event.new_state))
    assert [(e.previous_state, e.new_state) for e in events] == [
        ("candidate", "validated"), ("validated", "promoted")]
    assert events[1].decided_by == "agent:validator_v1"
    assert events[1].evidence == {"candidate_id": candidate.candidate_id, "outcome": "validated"}
    assert events[1].semantic_layer_version == "sha256:v1:" + "a" * 64

    index = KnowledgeIndex.from_store(knowledge)
    node = index.get("metric:cand:revenue_by_segment")
    assert node.status == "candidate" and node.source == "candidate"
    assert "Revenue per customer segment" in node.text and "Revenue per segment in ?" in node.text
    (edge,) = [edge for edge in knowledge.list_edges() if edge.kind == "shortcut"]
    assert (edge.from_id, edge.to_id, edge.keys, edge.weight, edge.source) == (
        "table:sales.orders", "table:sales.customers", ("customer_id", "id"), 0.8, "candidate")


def test_validated_by_an_earlier_run_writes_only_the_promotion(store):
    candidate = make_candidate(state="validated")
    assert _apply(store, candidate, "validated").new_state == "promoted"
    (event,) = store.list_events(metric_id=candidate.candidate_id)
    assert (event.previous_state, event.new_state) == ("validated", "promoted")


def test_needs_review_report_writes_one_event_and_no_ddl(store):
    backend = RecordingBackend()
    candidate = joined_candidate()
    result = _apply(store, candidate, "needs_review", backend=backend)
    assert result.new_state == "needs_review" and backend.executed == []
    (event,) = store.list_events(metric_id=candidate.candidate_id)
    assert (event.previous_state, event.new_state) == ("candidate", "needs_review")
    assert store.get_candidate(candidate.candidate_id).state == "needs_review"


def test_pending_report_changes_nothing(store):
    candidate = joined_candidate()
    result = _apply(store, candidate, "pending")
    assert result.new_state == "candidate"
    assert store.list_events(metric_id=candidate.candidate_id) == []
    assert store.get_candidate(candidate.candidate_id) == candidate


def test_metric_view_translation_error_goes_to_needs_review(store):
    schema = stored_schema(aggregate={
        "group_by": ["segment"], "measures": [["amount", "avg_amount", "avg"]]})
    backend = RecordingBackend()
    candidate = make_candidate(schema, state="candidate")
    result = _apply(store, candidate, "validated", backend=backend)
    assert result.new_state == "needs_review" and backend.executed == []
    (event,) = store.list_events(metric_id=candidate.candidate_id)
    assert event.evidence["reason"] == "metric_view_translation"
    assert "avg_amount" in event.evidence["message"]


def test_ddl_failure_keeps_state_and_records_a_promotion_run(store):
    candidate = joined_candidate()
    result = _apply(store, candidate, "validated", backend=RecordingBackend(fail_on="WITH METRICS"))
    assert (result.new_state, result.error_type) == ("candidate", "RuntimeError")
    assert store.get_candidate(candidate.candidate_id) == candidate
    assert store.list_events(metric_id=candidate.candidate_id) == []
    (run,) = store.list_runs(candidate.candidate_id, purpose="promotion")
    assert run.error_type == "RuntimeError" and run.result_hash is None


def test_dry_run_apply_writes_nothing(store, knowledge):
    from skifer.knowledge.index import KnowledgeIndex

    backend = RecordingBackend()
    candidate = joined_candidate()
    result = _apply(store, candidate, "validated", backend=backend, knowledge_store=knowledge,
                    dry_run=True)
    assert result.new_state == "promoted" and result.statements
    assert backend.executed == []
    assert store.get_candidate(candidate.candidate_id) == candidate
    assert store.list_events(metric_id=candidate.candidate_id) == []
    assert len(KnowledgeIndex.from_store(knowledge)) == 0


def test_local_promotion_reports_the_skipped_metric_view(store):
    candidate = joined_candidate()
    result = _apply(store, candidate, "validated", catalog=None)
    assert result.notes == (promotion.LOCAL_METRIC_VIEW_NOTE,)
    stored = store.get_candidate(candidate.candidate_id)
    assert stored.published_view == "semantic_candidates.revenue_by_segment"
    assert stored.published_metric_view is None


def test_local_level1_view_runs_on_spark(spark):
    """The view DDL is valid Spark SQL and reads back the candidate's definition."""
    from skifer.core.spark_backend import SparkBackend

    spark.sql("CREATE DATABASE IF NOT EXISTS plan44_promotion")
    spark.createDataFrame(
        [("FR", "B2B", 10.0, 1), ("FR", "B2B", 5.0, 2), ("BE", "B2C", 7.5, 3)],
        ["country", "segment", "amount", "order_id"],
    ).write.mode("overwrite").saveAsTable("plan44_promotion.orders")
    raw = {
        "tables": [{"name": "plan44_promotion.orders", "alias": "o",
                    "filter": ["country:equals:FR"]}],
        "aggregate": {"group_by": ["segment"], "measures": [["amount", "total_amount", "sum"]]},
    }
    schema, _ = parameterize_filters(parse_schema(yaml.safe_dump(raw)))
    candidate = make_candidate(schema, tables_used=("plan44_promotion.orders",))
    try:
        promotion.promote_level1(candidate, backend=SparkBackend(spark), catalog=None,
                                 schema="plan44")
        rows = spark.sql(
            "SELECT country, segment, total_amount FROM plan44_candidates.revenue_by_segment "
            "ORDER BY country").collect()
        assert [tuple(row) for row in rows] == [("BE", "B2C", 7.5), ("FR", "B2B", 15.0)]
    finally:
        spark.sql("DROP SCHEMA IF EXISTS plan44_candidates CASCADE")
        spark.sql("DROP SCHEMA IF EXISTS plan44_promotion CASCADE")
