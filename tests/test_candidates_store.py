"""Plan 44 S01: candidate registry stores — structure persisted, values never."""

from __future__ import annotations

import inspect
import json
import re
import sqlite3
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from skifer.candidates import store as store_module
from skifer.candidates.models import (
    CandidateRun,
    CertificationEvent,
    MetricCandidate,
    ReferenceQuestion,
    candidate_id_for,
    fingerprint_candidate,
)
from skifer.candidates.redaction import parameterize_filters, question_hash
from skifer.candidates.store import (
    HISTORY_TABLE,
    TABLE_COLUMNS,
    CandidateStore,
    DeltaCandidateStore,
    SqliteCandidateStore,
    candidate_settings,
    default_candidate_store,
)
from skifer.core import spark_backend as spark_backend_module
from skifer.core.spark_backend import SparkBackend
from tests.fakes.fake_backend import FakeBackend

NOW = datetime(2026, 10, 28, 12, tzinfo=timezone.utc)
CANARY = "CANARY-7f3a"
HASH = "sha256:v1:" + "c" * 64


def _canary_schema() -> dict:
    """A canary in each of the five filter locations, string and dict forms."""
    return {
        "tables": [
            {
                "name": "gold.orders",
                "alias": "o",
                "filter": [
                    f"region:equals:{CANARY}",
                    {"column": "status", "operator": "in", "value": [CANARY]},
                ],
                "filter_groups": [
                    [f"channel:equals:{CANARY}"],
                    [{"column": "channel", "operator": "equals", "value": CANARY}],
                ],
            },
        ],
        "where": [f"segment:equals:{CANARY}", {"column": "s", "operator": "equals", "value": CANARY}],
        "where_groups": [[f"country:equals:{CANARY}"], [{"column": "c", "operator": "equals", "value": CANARY}]],
        "aggregate": {
            "group_by": ["region"],
            "measures": [["amount", "total_amount", "sum"]],
            "having": [
                f"total_amount:greater_than:{CANARY}",
                {"column": "total_amount", "operator": "less_than", "value": CANARY},
            ],
        },
    }


def make_candidate(schema: dict | None = None, **overrides) -> MetricCandidate:
    schema = schema if schema is not None else {"tables": [{"name": "gold.orders", "alias": "o"}]}
    fingerprint = fingerprint_candidate(schema)
    values = dict(
        candidate_id=candidate_id_for(fingerprint),
        metric_name="revenue_by_region",
        metric_description=None,
        origin_question="Revenue per region?",
        origin_question_hash=question_hash("Revenue per region?"),
        schema_dict=schema,
        sql_hash=HASH,
        fingerprint=fingerprint,
        tables_used=("gold.orders",),
        columns_used=(),
        joins_used=(),
        business_rules_refs=(),
        residues=(),
        state="candidate",
        created_at=NOW,
        updated_at=NOW,
    )
    values.update(overrides)
    return MetricCandidate(**values)


def make_run(candidate_id: str, **overrides) -> CandidateRun:
    values = dict(run_id="run-1", candidate_id=candidate_id, executed_at=NOW, purpose="answer")
    values.update(overrides)
    return CandidateRun(**values)


def make_event(metric_id: str, **overrides) -> CertificationEvent:
    values = dict(
        event_id="0" * 32,
        metric_id=metric_id,
        metric_name="revenue_by_region",
        event_type="created",
        previous_state=None,
        new_state="candidate",
        decided_by="agent:candidate_agent_v1",
        evidence={},
        event_at=NOW,
    )
    values.update(overrides)
    return CertificationEvent(**values)


def make_reference(**overrides) -> ReferenceQuestion:
    values = dict(
        question_id="q-1",
        question="Revenue per region?",
        family="B",
        expected={"schema_dict": {"tables": []}, "result_hash": None},
        source="human",
        created_at=NOW,
    )
    values.update(overrides)
    return ReferenceQuestion(**values)


# --- store factories ------------------------------------------------------------------


def _sqlite(tmp_path, **kwargs):
    return SqliteCandidateStore(str(tmp_path / "candidates.db"), **kwargs)


def _sqlite_raw_rows(store) -> list[str]:
    rows = []
    for table in TABLE_COLUMNS:
        for row in store._conn.execute(f"SELECT * FROM {table}").fetchall():
            rows.append(json.dumps(row, default=str))
    return rows


def _delta(tmp_path, **kwargs):
    return DeltaCandidateStore(FakeBackend(), "prod_cat._skifer_candidates", **kwargs)


def _delta_raw_rows(store) -> list[str]:
    return [
        json.dumps(row, default=str)
        for rows in store.backend._candidate_tables.values()
        for row in rows
    ]


STORES = {"sqlite": (_sqlite, _sqlite_raw_rows), "delta": (_delta, _delta_raw_rows)}


@pytest.fixture(params=sorted(STORES))
def store_kind(request):
    return request.param


def _make_store(kind, tmp_path, **kwargs):
    factory, _ = STORES[kind]
    return factory(tmp_path, **kwargs)


def _raw_rows(kind, store) -> list[str]:
    _, reader = STORES[kind]
    return reader(store)


# --- tests ----------------------------------------------------------------------------


def test_stores_satisfy_the_protocol(store_kind, tmp_path):
    assert isinstance(_make_store(store_kind, tmp_path), CandidateStore)


def test_no_filter_value_is_persisted(store_kind, tmp_path):
    store = _make_store(store_kind, tmp_path)
    parameterized, values = parameterize_filters(_canary_schema())
    assert CANARY in json.dumps(values)
    candidate = make_candidate(parameterized, state="candidate")
    store.upsert_candidate(candidate)
    store.append_run(make_run(candidate.candidate_id))
    store.append_event(make_event(candidate.candidate_id, evidence={"request_count": 1}))
    store.append_reference(make_reference(expected={"schema_dict": parameterized, "result_hash": None}))

    rows = _raw_rows(store_kind, store)
    assert rows, "the store wrote nothing"
    assert not any(CANARY in row for row in rows)
    assert store.get_candidate(candidate.candidate_id).schema_dict == parameterized


def test_unparameterized_schema_is_refused(store_kind, tmp_path):
    store = _make_store(store_kind, tmp_path)
    with pytest.raises(ValueError, match="holds a filter value"):
        store.upsert_candidate(make_candidate(_canary_schema()))
    assert not any(CANARY in row for row in _raw_rows(store_kind, store))


def test_redacted_mode_stores_only_question_hash(store_kind, tmp_path):
    store = _make_store(store_kind, tmp_path, store_questions="redacted")
    candidate = make_candidate()
    store.upsert_candidate(candidate)

    stored = store.get_candidate(candidate.candidate_id)
    assert stored.origin_question is None
    assert stored.origin_question_hash == question_hash("Revenue per region?")
    assert not any("Revenue per region" in row for row in _raw_rows(store_kind, store))


def test_round_trip_every_field(store_kind, tmp_path):
    store = _make_store(store_kind, tmp_path)
    later = NOW + timedelta(hours=2, microseconds=5)
    rich = make_candidate(
        {"tables": [{"name": "gold.orders", "alias": "o"}, {"name": "gold.c", "alias": "c"}],
         "join": [{"table_from": ["o", "k"], "table_to": ["c", "k"]}]},
        metric_description="Revenue — par région",
        tables_used=("gold.orders", "gold.c"),
        columns_used=("gold.orders.amount",),
        joins_used=(("o", "c", "k"),),
        business_rules_refs=("flag_high_value",),
        residues=({"code": "expr", "message": "expr: kept", "blocking": True},),
        state="promoted",
        request_count=4,
        distinct_requesters=("p1", "p2"),
        stability_score=0.75,
        coherence={"verdict": False, "reasons": ["undeclared_join"]},
        trace_id="trace-1",
        reviewer="human:abc",
        published_view="sem_candidates.v",
        published_metric_view="sem_candidates.mv",
        updated_at=later,
        last_requested_at=later,
    )
    bare = make_candidate()
    for candidate in (rich, bare):
        store.upsert_candidate(candidate)
        assert store.get_candidate(candidate.candidate_id) == candidate
    assert store.find_by_fingerprint(rich.fingerprint) == rich

    full_run = make_run(rich.candidate_id, run_id="r2", purpose="stability", result_hash=HASH,
                        row_count=0, duration_ms=7, trace_id="t", error_type="TimeoutError",
                        executed_at=later)
    bare_run = make_run(rich.candidate_id)
    reference_run = make_run("reference_set:v3", run_id="r3", purpose="evaluation")
    for run in (full_run, bare_run, reference_run):
        store.append_run(run)
    assert store.list_runs(rich.candidate_id) == [bare_run, full_run]
    assert store.list_runs(rich.candidate_id, purpose="stability") == [full_run]
    assert store.list_runs("reference_set:v3") == [reference_run]

    full_event = make_event(
        rich.candidate_id, event_id="1" * 32, event_type="demoted", previous_state="promoted",
        new_state="demoted", decided_by="human:abc", justification="replaced",
        evidence={"request_count": 4, "score": 0.5, "ok": True, "nested": [1, None]},
        replaced_by_metric="model:sales.orders.revenue", replaces_metric=None,
        semantic_layer_version=HASH, trace_id="t", event_at=later,
    )
    bare_event = make_event(rich.candidate_id)
    for event in (full_event, bare_event):
        store.append_event(event)
    assert store.list_events(metric_id=rich.candidate_id) == [bare_event, full_event]

    references = [make_reference(), make_reference(question_id="q-2", family="C",
                                                   expected={"outcome": "refuse"}, source="rejected",
                                                   semantic_layer_version=HASH)]
    for reference in references:
        store.append_reference(reference)
    assert store.list_references() == references
    assert store.list_references(family="C") == references[1:]


def test_upsert_and_append_are_idempotent(store_kind, tmp_path):
    store = _make_store(store_kind, tmp_path)
    candidate = make_candidate()
    store.upsert_candidate(candidate)
    store.upsert_candidate(candidate)
    bumped = replace(candidate, request_count=2, updated_at=NOW + timedelta(minutes=1))
    store.upsert_candidate(bumped)
    assert store.list_candidates() == [bumped]

    run = make_run(candidate.candidate_id)
    event = make_event(candidate.candidate_id)
    reference = make_reference()
    for _ in range(2):
        store.append_run(run)
        store.append_event(event)
        store.append_reference(reference)
    # a second append with the same id is ignored, even with other content
    store.append_event(replace(event, justification="rewritten"))
    assert store.list_runs(candidate.candidate_id) == [run]
    assert store.list_events() == [event]
    assert store.list_references() == [reference]


def test_list_filters_and_half_open_event_window(store_kind, tmp_path):
    store = _make_store(store_kind, tmp_path)
    first = make_candidate()
    second = make_candidate(
        {"tables": [{"name": "gold.other", "alias": "x"}]}, state="needs_review",
        updated_at=NOW + timedelta(hours=1),
    )
    store.upsert_candidate(first)
    store.upsert_candidate(second)
    assert store.list_candidates(state="needs_review") == [second]
    assert store.list_candidates(limit=1) == [second]

    events = [
        make_event(first.candidate_id, event_id=f"{index:032x}", event_at=NOW + timedelta(hours=index))
        for index in range(3)
    ]
    for event in events:
        store.append_event(event)
    window = store.list_events(since=NOW, until=NOW + timedelta(hours=2))
    assert window == events[:2]
    with pytest.raises(ValueError, match="timezone-aware"):
        store.list_events(since=datetime(2026, 1, 1))
    with pytest.raises(ValueError, match="state"):
        store.list_candidates(state="certified")


def test_history_store_has_no_update_or_delete():
    source = inspect.getsource(store_module)
    for verb in ("update", "delete", "remove", "purge", "rewrite"):
        for name in ("event", "history"):
            assert f"{verb}_{name}" not in source
    assert "DELETE" not in source
    assert not re.search(r"(UPDATE|MERGE)[^\n]*" + HISTORY_TABLE, source)
    assert not re.search(r"(UPDATE|MERGE)[^\n]*HISTORY_TABLE", source)
    # the only upsert path is the candidates table
    for match in re.finditer(r"self\._insert\(([A-Z_]+),[^\n]*upsert=True", source):
        assert match.group(1) == "CANDIDATES_TABLE"
    backend_source = inspect.getsource(spark_backend_module)
    start = backend_source.index("Plan 44: metric candidate registry")
    candidate_section = backend_source[start:backend_source.index("def check_catalog_access", start)]
    assert not re.search(r"(UPDATE|MERGE|DELETE)[^\n]*" + HISTORY_TABLE, candidate_section)
    assert "DELETE" not in candidate_section
    assert re.findall(r"MERGE INTO", candidate_section) == ["MERGE INTO"]
    assert "ON t.candidate_id = s.candidate_id" in candidate_section
    for verb in ("update", "delete", "remove"):
        assert not [name for name in dir(SparkBackend) if name.startswith(verb) and "candidate" in name]
    for cls in (CandidateStore, SqliteCandidateStore, DeltaCandidateStore):
        assert {"append_event", "list_events"} <= set(dir(cls))
        assert not [name for name in dir(cls) if "event" in name and not name.startswith(("append_", "list_"))]


def test_sqlite_history_rows_cannot_be_rewritten_through_the_store(tmp_path):
    store = _sqlite(tmp_path)
    candidate = make_candidate()
    store.append_event(make_event(candidate.candidate_id))
    store.append_event(make_event(candidate.candidate_id, justification="other"))
    raw = sqlite3.connect(str(tmp_path / "candidates.db")).execute(
        f"SELECT justification FROM {HISTORY_TABLE}"
    ).fetchall()
    assert raw == [(None,)]


def test_candidate_settings_defaults_and_validation():
    assert candidate_settings(None) == ("full", 365)
    assert candidate_settings({"candidates_store_questions": "redacted",
                               "candidates_retention_days": 30}) == ("redacted", 30)
    with pytest.raises(ValueError, match="candidates_store_questions"):
        candidate_settings({"candidates_store_questions": "hashed"})
    with pytest.raises(ValueError, match="candidates_retention_days"):
        SqliteCandidateStore(":memory:", retention_days=0)


class _Context:
    def __init__(self, env_config):
        self._env_config = env_config

    def env_config(self):
        return self._env_config


class _Engine:
    def __init__(self, *, is_local, db, backend=None, env_config=None):
        self.is_local = is_local
        self.db = db
        self.backend = backend
        self.context = _Context(env_config or {})


def test_default_store_selection(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    local = default_candidate_store(_Engine(is_local=True, db=None))
    assert isinstance(local, SqliteCandidateStore)
    assert (tmp_path / ".skifer_candidates.db").exists()
    no_catalog = default_candidate_store(_Engine(is_local=False, db=None))
    assert isinstance(no_catalog, SqliteCandidateStore)
    local_with_catalog = default_candidate_store(_Engine(is_local=True, db="dev_cat"))
    assert isinstance(local_with_catalog, SqliteCandidateStore)

    backend = object()
    delta = default_candidate_store(
        _Engine(is_local=False, db="prod_cat", backend=backend,
                env_config={"candidates_store_questions": "redacted",
                            "candidates_retention_days": 30})
    )
    assert isinstance(delta, DeltaCandidateStore)
    assert delta.backend is backend
    assert delta.schema == "prod_cat._skifer_candidates"
    assert (delta.store_questions, delta.retention_days) == ("redacted", 30)


def test_delta_ddl_enables_cdf_without_defaults():
    store = _delta(None)
    candidate = make_candidate()
    store.upsert_candidate(candidate)
    store.append_run(make_run(candidate.candidate_id))
    store.append_event(make_event(candidate.candidate_id))
    store.append_reference(make_reference())

    ddl = store.backend._candidate_sql
    assert len(ddl) == 4
    for statement in ddl:
        assert statement.startswith("CREATE TABLE IF NOT EXISTS `prod_cat`.`_skifer_candidates`.")
        assert "USING DELTA" in statement
        assert "TBLPROPERTIES (delta.enableChangeDataFeed = true)" in statement
        assert "DEFAULT" not in statement.upper()
    assert any("request_count BIGINT" in statement for statement in ddl)


def test_delta_escapes_identifiers():
    from unittest.mock import MagicMock

    spark = MagicMock()
    spark.catalog.tableExists.return_value = True
    spark.sql.return_value.collect.return_value = []
    backend = SparkBackend(spark=spark)
    store = DeltaCandidateStore(backend, "prod_cat._skifer_candidates")

    store.get_candidate("candidate:v1:x\\' OR 1=1 --")
    statement = spark.sql.call_args.args[0]
    assert "candidate:v1:x\\\\'' OR 1=1 --" in statement

    store.list_runs("reference_set:it's")
    assert "reference_set:it''s" in spark.sql.call_args.args[0]

    store.append_run(make_run("reference_set:a\\'b", run_id="r\\'1"))
    lookups = [call.args[0] for call in spark.sql.call_args_list if "SELECT 1" in call.args[0]]
    assert "'r\\\\''1'" in lookups[-1]
    assert "`prod_cat`.`_skifer_candidates`" in lookups[-1]


def test_delta_round_trip_on_local_spark(spark):
    schema = "plan44_candidates_store_test"
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    try:
        store = DeltaCandidateStore(SparkBackend(spark=spark), schema)
        parameterized, _ = parameterize_filters(_canary_schema())
        candidate = make_candidate(parameterized, stability_score=0.5,
                                   coherence={"verdict": True, "reasons": []},
                                   joins_used=(("o", "c", "k"),))
        store.upsert_candidate(candidate)
        bumped = replace(candidate, request_count=3, updated_at=NOW + timedelta(seconds=1))
        store.upsert_candidate(bumped)
        assert store.get_candidate(candidate.candidate_id) == bumped
        assert store.find_by_fingerprint(candidate.fingerprint) == bumped
        assert store.list_candidates(state="candidate") == [bumped]

        run = make_run(candidate.candidate_id, row_count=2, duration_ms=5)
        event = make_event(candidate.candidate_id, evidence={"n": 1})
        reference = make_reference()
        for _ in range(2):
            store.append_run(run)
            store.append_event(event)
            store.append_reference(reference)
        assert store.list_runs(candidate.candidate_id) == [run]
        assert store.list_events(metric_id=candidate.candidate_id,
                                 since=NOW, until=NOW + timedelta(seconds=1)) == [event]
        assert store.list_references() == [reference]

        rows = [json.dumps(row.asDict(), default=str)
                for table in TABLE_COLUMNS
                for row in spark.sql(f"SELECT * FROM {schema}.{table}").collect()]
        assert rows and not any(CANARY in row for row in rows)
        for table in TABLE_COLUMNS:
            properties = {
                row["key"]: row["value"]
                for row in spark.sql(f"SHOW TBLPROPERTIES {schema}.{table}").collect()
            }
            assert properties.get("delta.enableChangeDataFeed") == "true"
    finally:
        spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")


def test_candidates_package_imports_without_spark():
    import subprocess
    import sys

    code = (
        "import sys\n"
        "import skifer.candidates, skifer.candidates.store\n"
        "print(sorted(m for m in ('pyspark', 'delta', 'sqlglot', 'mlflow') if m in sys.modules))\n"
    )
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    assert result.stdout.strip() == "[]"
