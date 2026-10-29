"""Plan 44 S01: candidate fingerprint and value redaction."""

from __future__ import annotations

import json
import os
import subprocess
import sys

import pytest

from skifer.candidates.models import candidate_id_for, fingerprint_candidate
from skifer.candidates.redaction import (
    parameterize_filters,
    question_hash,
    requester_pseudonym,
)
from skifer.observability.tracing import TraceAttributePolicy

CANARY = "CANARY-7f3a"


def _schema(**overrides) -> dict:
    schema = {
        "tables": [
            {
                "name": "gold.orders",
                "alias": "o",
                "filter": ["region:equals:EMEA", {"column": "status", "operator": "in", "value": ["A", "B"]}],
                "filter_groups": [["channel:equals:web"], ["channel:equals:shop"]],
            },
            {"name": "gold.customers", "alias": "c", "filter": ["customer_id:is_not_null"]},
        ],
        "join": [{"table_from": ["o", "customer_id"], "table_to": ["c", "id"], "type": "left"}],
        "where": ["segment:equals:B2B"],
        "where_groups": [["country:equals:FR"], ["country:equals:BE"]],
        "aggregate": {
            "group_by": ["region", "segment"],
            "measures": [["amount", "total_amount", "sum"], ["order_id", "nb_orders", "count"]],
            "having": ["total_amount:greater_than:1000"],
        },
    }
    schema.update(overrides)
    return schema


def _canary_schema() -> dict:
    """A canary value in each of the five filter locations, string and dict forms."""
    return {
        "tables": [
            {
                "name": "gold.orders",
                "alias": "o",
                "filter": [
                    f"region:equals:{CANARY}",
                    {"column": "status", "operator": "equals", "value": CANARY},
                    {"column": "code", "operator": "in", "value": [CANARY, "x"]},
                    f"label:in:{CANARY},y",
                    "customer_id:is_not_null",
                ],
                "filter_groups": [
                    [f"channel:equals:{CANARY}"],
                    [{"column": "channel", "operator": "equals", "value": CANARY}],
                ],
            },
            {"name": "gold.customers", "alias": "c", "filter": {"name": CANARY, "tier": {"in": [CANARY]}}},
        ],
        "where": [f"segment:equals:{CANARY}", {"column": "segment", "operator": "not_equals", "value": CANARY}],
        "where_groups": [[f"country:equals:{CANARY}"], [{"column": "country", "operator": "equals", "value": CANARY}]],
        "aggregate": {
            "group_by": ["region"],
            "measures": [["amount", "total_amount", "sum"]],
            "having": [f"total_amount:greater_than:{CANARY}", {"column": "total_amount", "operator": "less_than", "value": CANARY}],
        },
    }


def test_fingerprint_ignores_order_and_filter_values():
    base = _schema()
    shuffled = _schema(
        tables=[
            {"name": "gold.customers", "alias": "c", "filter": ["customer_id:is_not_null"]},
            {
                "name": "gold.orders",
                "alias": "o",
                # other values, other order, dict form instead of string form
                "filter": [
                    {"column": "status", "operator": "in", "value": ["Z"]},
                    {"column": "region", "operator": "eq", "value": "APAC"},
                ],
                "filter_groups": [["channel:equals:other"], ["channel:equals:web"]],
                "dev_limit": 10,
            },
        ],
        where_groups=[["country:equals:DE"], ["country:equals:IT"]],
        aggregate={
            "group_by": ["segment", "region"],
            "measures": [
                {"source": "order_id", "target": "nb_orders", "func": "count"},
                ["amount", "total_amount", "sum"],
            ],
            "having": ["total_amount:greater_than:5"],
        },
        description="presentation only",
    )

    assert fingerprint_candidate(base) == fingerprint_candidate(shuffled)
    assert fingerprint_candidate(base).startswith("sha256:v1:")
    parameterized, _ = parameterize_filters(base)
    assert fingerprint_candidate(parameterized) == fingerprint_candidate(base)


@pytest.mark.parametrize(
    "change",
    [
        lambda s: s["tables"][1].update(name="gold.clients"),
        lambda s: s["join"][0].update(type="inner"),
        lambda s: s["join"][0].update(table_to=["c", "customer_key"]),
        lambda s: s["tables"][0]["filter"].__setitem__(0, "zone:equals:EMEA"),
        lambda s: s["tables"][0]["filter"].__setitem__(0, "region:not_equals:EMEA"),
        lambda s: s["aggregate"]["measures"].__setitem__(0, ["amount", "total_amount", "avg"]),
        lambda s: s["aggregate"]["group_by"].append("country"),
        lambda s: s.update(where=["segment:is_null"]),
    ],
)
def test_fingerprint_changes_on_structure(change):
    changed = _schema()
    change(changed)
    assert fingerprint_candidate(changed) != fingerprint_candidate(_schema())


def test_fingerprint_stable_across_hash_seeds():
    code = (
        "import json, sys\n"
        "from skifer.candidates.models import fingerprint_candidate\n"
        "print(fingerprint_candidate(json.loads(sys.argv[1])))\n"
    )
    outputs = set()
    for seed in ("0", "12345"):
        env = {**os.environ, "PYTHONHASHSEED": seed}
        result = subprocess.run(
            [sys.executable, "-c", code, json.dumps(_schema())],
            capture_output=True, text=True, env=env, check=True,
        )
        outputs.add(result.stdout.strip())
    assert outputs == {fingerprint_candidate(_schema())}


def test_candidate_id_is_derived_from_the_fingerprint():
    fingerprint = fingerprint_candidate(_schema())
    assert candidate_id_for(fingerprint).startswith("candidate:v1:")
    assert candidate_id_for(fingerprint) == candidate_id_for(fingerprint)
    with pytest.raises(ValueError, match="fingerprint"):
        candidate_id_for("md5:abc")


def test_parameterize_removes_every_value_and_returns_them_separately():
    schema = _canary_schema()
    parameterized, values = parameterize_filters(schema)

    assert CANARY not in json.dumps(parameterized)
    assert CANARY in json.dumps(values)
    assert schema == _canary_schema(), "the input must not be mutated"
    assert parameterized["tables"][0]["filter"][0] == "region:equals:{{ p1 }}"
    assert parameterized["tables"][0]["filter"][4] == "customer_id:is_not_null"
    assert parameterized["tables"][0]["filter"][3] == "label:in:{{ p5 }},{{ p6 }}"
    assert values["p1"] == CANARY
    assert parameterized["tables"][0]["filter"][2]["value"] == ["{{ p3 }}", "{{ p4 }}"]


def test_parameterize_keeps_no_value_operators_intact():
    schema = {"tables": [{"name": "t", "filter": {"a": None, "b": "is_not_null"}}], "where": ["c:is_null"]}
    parameterized, values = parameterize_filters(schema)
    assert parameterized == schema
    assert values == {}


@pytest.mark.parametrize(
    "schema",
    [
        {"tables": [{"name": "t", "filter": ["a:sql:a > 1"]}]},
        {"where": [{"column": "a", "operator": "sql", "value": "a > 1"}]},
        {"tables": [{"name": "t", "filter": {"a": {"sql": "a > 1"}}}]},
    ],
)
def test_parameterize_refuses_raw_sql(schema):
    with pytest.raises(ValueError, match="Raw SQL filters cannot be stored in a candidate."):
        parameterize_filters(schema)


def test_question_hash_ignores_case_and_whitespace():
    assert question_hash("Revenue  per\\nregion?") != question_hash("Revenue per region?")
    assert question_hash("Revenue  per\nregion?") == question_hash("revenue per region?")
    assert question_hash("x").startswith("sha256:v1:")


def test_requester_pseudonym_delegates_to_the_tracing_policy():
    assert requester_pseudonym("alice", TraceAttributePolicy()) is None
    keyed = TraceAttributePolicy(user_identity="hmac", hmac_secret="s3cret")
    pseudonym = requester_pseudonym("alice", keyed)
    assert pseudonym and "alice" not in pseudonym
    assert requester_pseudonym("alice", keyed) == pseudonym
    assert requester_pseudonym("alice", None) is None
