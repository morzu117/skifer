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
