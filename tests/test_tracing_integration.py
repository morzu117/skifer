"""Integration coverage for Plan 29 slice 5.2 pipeline/certification tracing."""

from __future__ import annotations

from datetime import datetime, timezone
import logging
from types import SimpleNamespace

import pytest
import yaml

from skifer.agentic.hub import AgenticHub
from skifer.agentic.resolver import SemanticQuery
from skifer.core.config import TracingConfig, parse_tracing_config
from skifer.core.context import ExecutionContext
from skifer.core.core import SkiferEngine
from skifer.core.interpreter import SchemaInterpreter
from skifer.core.patterns import PipelinePatterns
from skifer.observability.certification import ContractDefinition
from skifer.observability.certification_store import (
    Certification,
    SqliteCertificationStore,
)
from skifer.observability.checks import CheckResult, CheckStatus, NullCheck
from skifer.observability.monitor import DataMonitor, MonitorReport
from skifer.observability.publication import PublicationCoordinator
from skifer.observability.publication import (
    stage_dataframe,
    start_publication_run,
)
from skifer.observability.tracing import (
    InMemoryTracer,
    NoOpTracer,
    configured_span_scope,
)
from skifer.observability import tracing
from skifer.semantic.access_policy import ConsumerContext, SemanticAccessDenied
from skifer.semantic.evidence import ExecutionResult, SemanticExecutionError
from skifer.semantic.llm_provider import llm_span_scope
from skifer.semantic.semantic import SemanticEngine
from skifer.serving.chat_model import SkiferChatModel
from tests.fakes.fake_backend import FakeBackend, FakeDataFrame


RUN_ID = "123e4567-e89b-12d3-a456-426614174000"


class RaisingTracer:
    """Exporter double that fails at every protocol call."""

    def start_span(self, name, *, attributes=None):
        raise RuntimeError("export unavailable")

    def current_trace_id(self):
        raise RuntimeError("export unavailable")

    def current_context(self):
        raise RuntimeError("export unavailable")

    def use_context(self, context):
        raise RuntimeError("export unavailable")


class CountForbiddenDataFrame(FakeDataFrame):
    def count(self):
        raise AssertionError("tracing must not trigger count()")


class StaticInterpreter:
    def __init__(self, df):
        self.df = df

    def process_schema(self, schema_dict, dataframes_in=None, intermediate_mode="inline"):
        return self.df


class FailingMonitor(DataMonitor):
    def _check_table(self, fqn, contracts, raise_on_critical):
        contract = NullCheck(table=fqn, column="id", severity="critical")
        return MonitorReport(
            fqn,
            [
                CheckResult(
                    contract,
                    status=CheckStatus.FAIL,
                    actual_value=1,
                    expected_value=0,
                    message="failed",
                    severity="critical",
                )
            ],
        )


class StaticMonitor(DataMonitor):
    def _check_table(self, fqn, contracts, raise_on_critical):
        return MonitorReport(
            fqn,
            [],
            timestamp=datetime(2025, 1, 1, tzinfo=timezone.utc),
        )


def _schema(*, required=False):
    field = {"logical_type": "integer"}
    if required:
        field["required"] = True
    return {
        "data_product": {"id": "sales.orders", "version": "1.0.0"},
        "contract": {"output": {"id": field}},
        "tables": [{"name": "silver.orders", "alias": "orders"}],
    }


def _definition():
    return ContractDefinition(
        "sales.orders",
        "1.0.0",
        "definition-hash",
        "{}",
        "sales.orders",
        None,
    )


def _engine(
    tracer,
    *,
    dataframe=None,
    monitor_class=DataMonitor,
    required=False,
):
    backend = FakeBackend({"silver.orders": [{"id": 1}]})
    engine = object.__new__(SkiferEngine)
    engine._context = ExecutionContext(
        env="TEST",
        db=None,
        config={"environments": {"test": {}}},
        is_local=True,
    )
    engine._backend = backend
    engine._tracing_config = TracingConfig(required=required)
    engine._tracer = NoOpTracer()
    engine.monitor = monitor_class(backend)
    engine.certification_store = SqliteCertificationStore(":memory:")
    engine._interpreter = (
        StaticInterpreter(dataframe)
        if dataframe is not None
        else SchemaInterpreter(backend, engine._context)
    )
    engine._patterns = PipelinePatterns(engine)
    engine._ensure_schema_exists = lambda schema: None
    engine.set_tracer(tracer)
    return engine, backend


def _span_names(tracer):
    return [span.name for span in tracer.spans]


def test_pipeline_publication_span_tree_and_run_id_propagation():
    tracer = InMemoryTracer()
    engine, backend = _engine(tracer)

    returned_run_id = engine.run_process_to_table(_schema(), "gold", "orders")

    assert _span_names(tracer) == [
        "skifer.pipeline.run",
        "skifer.source.resolve",
        "skifer.transform.execute",
        "skifer.publication.stage",
        "skifer.contract.evaluate",
        "skifer.publication.promote",
    ]
    root, source, transform, stage, contract, promote = tracer.spans
    assert source.parent_span_id == root.span_id
    assert transform.parent_span_id == source.span_id
    assert stage.parent_span_id == root.span_id
    assert contract.parent_span_id == root.span_id
    assert promote.parent_span_id == root.span_id
    assert all(span.status == "OK" for span in tracer.spans)

    # One identity from the pipeline down to the publication, so the audit link
    # holds without exported traces. The slice 5.2 guarantee still stands and is
    # asserted where it belongs, in
    # test_persisted_run_id_is_identical_whether_or_not_tracing_is_on: the id is
    # minted on the business path, never derived from the trace context.
    assert {span.trace_id for span in tracer.spans} == {root.trace_id}
    assert stage.attributes["run_id"] == promote.attributes["run_id"]
    assert stage.attributes["run_id"] == root.attributes["run_id"]
    # Slice 5.2: the returned run_id is exactly the one propagated through the trace.
    assert returned_run_id == root.attributes["run_id"]
    assert stage.trace_context.run_id == stage.attributes["run_id"]
    assert backend._written["gold.orders"] == [{"id": 1}]


def test_publication_quarantine_traces_stable_boundaries():
    tracer = InMemoryTracer()
    engine, backend = _engine(tracer, monitor_class=FailingMonitor)

    with pytest.raises(Exception) as caught:
        engine.run_process_to_table(_schema(required=True), "gold", "orders")

    assert type(caught.value).__name__ == "DataQualityError"
    assert _span_names(tracer)[-3:] == [
        "skifer.publication.stage",
        "skifer.contract.evaluate",
        "skifer.publication.promote",
    ]
    promote = tracer.spans[-1]
    assert promote.attributes["decision"] == "QUARANTINED"
    assert "gold.orders" not in backend._written


def test_raising_tracer_preserves_engine_and_publication_results():
    baseline_engine, baseline_backend = _engine(NoOpTracer())
    failing_engine, failing_backend = _engine(RaisingTracer())

    baseline_result = baseline_engine.run_process_to_table(
        {"tables": [{"name": "silver.orders", "alias": "orders"}]},
        "gold",
        "orders",
    )
    failing_result = failing_engine.run_process_to_table(
        {"tables": [{"name": "silver.orders", "alias": "orders"}]},
        "gold",
        "orders",
    )
    # Slice 5.2: both runs now return their minted run_id (distinct per run); the
    # raising tracer must not change the business outcome — proven by identical writes.
    assert failing_result is not None and baseline_result is not None
    assert failing_backend._written == baseline_backend._written

    def publish_with(tracer):
        backend = FakeBackend()
        monitor = StaticMonitor(backend)
        monitor.tracer = tracer
        store = SqliteCertificationStore(":memory:")
        result = PublicationCoordinator(backend, monitor, store).publish(
            FakeDataFrame([{"id": 1}]),
            "gold.orders",
            {},
            _definition(),
            RUN_ID,
        )
        return result, backend._written

    baseline_publication, baseline_writes = publish_with(NoOpTracer())
    failing_publication, failing_writes = publish_with(RaisingTracer())
    assert failing_publication == baseline_publication
    assert failing_writes == baseline_writes


def test_raising_tracer_never_masks_business_exceptions():
    failure = RuntimeError("business failed")
    engine, _ = _engine(RaisingTracer())
    engine._patterns.run_process_to_table = lambda *args, **kwargs: (_ for _ in ()).throw(
        failure
    )

    with pytest.raises(RuntimeError) as caught:
        engine.run_process_to_table({}, "gold", "orders")
    assert caught.value is failure

    class BrokenBackend(FakeBackend):
        def write_staging(self, df, fqn):
            raise failure

    backend = BrokenBackend()
    monitor = DataMonitor(backend)
    monitor.tracer = RaisingTracer()
    coordinator = PublicationCoordinator(
        backend,
        monitor,
        SqliteCertificationStore(":memory:"),
    )
    with pytest.raises(RuntimeError) as publication_caught:
        coordinator.publish(
            FakeDataFrame([{"id": 1}]),
            "gold.orders",
            {},
            _definition(),
            RUN_ID,
        )
    assert publication_caught.value is failure


def test_business_error_marks_span_error_and_reraises_same_instance():
    failure = ValueError("invalid source")
    tracer = InMemoryTracer()
    engine, _ = _engine(tracer)
    engine._patterns.run_process_to_table = lambda *args, **kwargs: (_ for _ in ()).throw(
        failure
    )

    with pytest.raises(ValueError) as caught:
        engine.run_process_to_table({}, "gold", "orders")

    assert caught.value is failure
    assert tracer.spans[0].status == "ERROR"
    assert tracer.spans[0].exceptions[0].type_name == "ValueError"


def test_tracing_never_calls_dataframe_count():
    tracer = InMemoryTracer()
    df = CountForbiddenDataFrame([{"id": 1}])
    engine, backend = _engine(tracer, dataframe=df)

    engine.run_process_to_table(
        {"tables": [{"name": "silver.orders", "alias": "orders"}]},
        "gold",
        "orders",
    )

    assert backend._written["`gold`.`orders`"] == [{"id": 1}]


def test_absent_tracing_config_is_noop_and_unknown_exporter_is_actionable():
    settings = parse_tracing_config({})
    assert settings == TracingConfig()
    engine, _ = _engine(NoOpTracer())
    engine.run_process_to_table(
        {"tables": [{"name": "silver.orders", "alias": "orders"}]},
        "gold",
        "orders",
    )
    assert isinstance(engine.tracer, NoOpTracer)

    with pytest.raises(ValueError, match="Valid values: none, otlp, mlflow, dual"):
        parse_tracing_config(
            {"observability": {"tracing": {"exporter": "zipkin"}}}
        )


def test_configuration_manager_reads_global_tracing_block(tmp_path):
    from skifer.core.config import ConfigurationManager

    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
priority_check: [local]
environments:
  local:
    catalog: null
observability:
  tracing:
    exporter: none
    required: true
    capture_prompts: false
    capture_sql: false
    user_identity: omit
    trace_location: null
"""
    )

    manager = ConfigurationManager(config_path=str(config_path))
    settings = parse_tracing_config(manager.config)
    assert settings.required is True
    assert settings.exporter == "none"


def test_run_from_yaml_records_schema_load_boundary(tmp_path):
    schema_path = tmp_path / "orders.yaml"
    schema_path.write_text(
        "tables:\n  - name: silver.orders\n    alias: orders\n"
    )
    tracer = InMemoryTracer()
    engine, _ = _engine(tracer)

    engine.run_from_yaml(str(schema_path), "gold", "orders")

    assert _span_names(tracer)[:4] == [
        "skifer.pipeline.run",
        "skifer.schema.load",
        "skifer.source.resolve",
        "skifer.transform.execute",
    ]


def test_publication_resume_records_promote_boundary():
    tracer = InMemoryTracer()
    backend = FakeBackend()
    monitor = StaticMonitor(backend)
    monitor.tracer = tracer
    store = SqliteCertificationStore(":memory:")
    run = stage_dataframe(
        backend,
        start_publication_run("gold.orders", _definition(), store, RUN_ID),
        _definition(),
        store,
        FakeDataFrame([{"id": 1}]),
    )

    result = PublicationCoordinator(backend, monitor, store).resume(run, _definition())

    assert result.state == "PROMOTED"
    assert _span_names(tracer) == ["skifer.publication.promote"]
    assert tracer.spans[0].attributes["run_id"] == RUN_ID


def test_direct_publication_spans_share_one_trace():
    tracer = InMemoryTracer()
    backend = FakeBackend()
    monitor = StaticMonitor(backend)
    monitor.tracer = tracer
    coordinator = PublicationCoordinator(
        backend,
        monitor,
        SqliteCertificationStore(":memory:"),
    )

    coordinator.publish(
        FakeDataFrame([{"id": 1}]),
        "gold.orders",
        {},
        _definition(),
        RUN_ID,
    )

    assert _span_names(tracer) == [
        "skifer.publication.stage",
        "skifer.contract.evaluate",
        "skifer.publication.promote",
    ]
    assert len({span.trace_id for span in tracer.spans}) == 1
    assert all(span.trace_context.run_id == RUN_ID for span in tracer.spans)


def test_export_failures_are_logged_only_once(monkeypatch, caplog):
    monkeypatch.setattr(tracing, "_FAILURE_WARNING_EMITTED", False)
    engine, _ = _engine(RaisingTracer())

    with caplog.at_level(logging.WARNING, logger=tracing.__name__):
        engine.run_process_to_table(
            {"tables": [{"name": "silver.orders", "alias": "orders"}]},
            "gold",
            "orders_one",
        )
        engine.run_process_to_table(
            {"tables": [{"name": "silver.orders", "alias": "orders"}]},
            "gold",
            "orders_two",
        )

    messages = [
        record.message for record in caplog.records if "Tracing failed" in record.message
    ]
    assert len(messages) == 1


def test_required_tracing_mode_propagates_export_failure():
    engine, backend = _engine(RaisingTracer(), required=True)

    with pytest.raises(RuntimeError, match="export unavailable"):
        engine.run_process_to_table(
            {"tables": [{"name": "silver.orders", "alias": "orders"}]},
            "gold",
            "orders",
        )
    assert backend._written == {}


def test_traces_contain_no_sql_filter_values_or_pii():
    tracer = InMemoryTracer()
    engine, _ = _engine(tracer)
    schema = _schema()
    schema["tables"][0]["filter"] = [
        {"column": "email", "operator": "equals", "value": "alice@example.com"}
    ]
    engine._backend._tables["silver.orders"] = [
        {"id": 1, "email": "alice@example.com"}
    ]

    engine.run_process_to_table(schema, "gold", "orders")

    serialized = repr(
        [(span.name, span.attributes, span.events) for span in tracer.spans]
    )
    assert "alice@example.com" not in serialized
    assert "SELECT " not in serialized
    assert "filter" not in serialized.lower()
    allowed = {
        "skifer.trace_version",
        "environment",
        "run_id",
        "contract_id",
        "contract_version",
        "decision",
    }
    assert all(set(span.attributes) <= allowed for span in tracer.spans)


# ---------------------------------------------------------------------------
# Tracing must not reach into business identity or behaviour
# ---------------------------------------------------------------------------


def _coordinator_with(tracer):
    from skifer.observability.certification_store import SqliteCertificationStore
    from skifer.observability.publication import PublicationCoordinator
    from tests.test_certified_publication import _Monitor, _pass_result
    from tests.fakes.fake_backend import FakeBackend

    monitor = _Monitor(_pass_result)
    monitor.tracer = tracer
    return PublicationCoordinator(FakeBackend(), monitor, SqliteCertificationStore(":memory:"))


@pytest.mark.parametrize("ambient_run_id", ["11111111-2222-3333-4444-555555555555", "not-a-uuid"])
def test_publication_run_id_never_comes_from_the_ambient_trace_context(ambient_run_id):
    # The traced branch used to derive the business run_id from the trace
    # context: turning tracing on renamed the run persisted in the
    # certification store, and outright raised when that ambient id was not a
    # UUID — a publication that succeeds untraced must not fail traced.
    from skifer.observability.tracing import TraceContext, trace_context_scope
    from tests.test_certified_publication import _definition
    from tests.fakes.fake_backend import FakeDataFrame

    tracer = InMemoryTracer()
    coordinator = _coordinator_with(tracer)
    context = TraceContext(trace_id="a" * 32, run_id=ambient_run_id)

    with trace_context_scope(tracer, context):
        result = coordinator.publish(
            FakeDataFrame([{"id": 1}]), "gold.orders", {}, _definition()
        )

    assert result.run.run_id != ambient_run_id
    assert result.state == "PROMOTED"


def test_publication_result_is_identical_with_and_without_tracing():
    from tests.test_certified_publication import _definition
    from tests.fakes.fake_backend import FakeDataFrame

    traced = _coordinator_with(InMemoryTracer()).publish(
        FakeDataFrame([{"id": 1}]), "gold.orders", {}, _definition()
    )
    untraced = _coordinator_with(NoOpTracer()).publish(
        FakeDataFrame([{"id": 1}]), "gold.orders", {}, _definition()
    )

    assert traced.state == untraced.state
    assert (traced.report is None) == (untraced.report is None)


def test_tracer_exposed_as_a_property_is_not_silently_downgraded():
    # Reading monitor.__dict__ skipped descriptors, so a monitor exposing
    # `tracer` as a property ran with tracing quietly off.
    from skifer.observability.certification_store import SqliteCertificationStore
    from skifer.observability.publication import PublicationCoordinator
    from tests.test_certified_publication import _Monitor, _pass_result
    from tests.fakes.fake_backend import FakeBackend

    class MonitorWithProperty(_Monitor):
        @property
        def tracer(self):
            return InMemoryTracer()

    coordinator = PublicationCoordinator(
        FakeBackend(), MonitorWithProperty(_pass_result), SqliteCertificationStore(":memory:")
    )

    assert isinstance(coordinator.tracer, InMemoryTracer)


# ---------------------------------------------------------------------------
# Slice 5.3 — semantic / agentic / LLM / serving
# ---------------------------------------------------------------------------


TRACE_ID = "a" * 32
OTHER_TRACE_ID = "b" * 32
COMPILED_AT = datetime(2026, 9, 5, 10, 0, tzinfo=timezone.utc)


class _SemanticBackend:
    def __init__(self, *, failure=None, statement_id="statement-42"):
        self.sql = []
        self.failure = failure
        self.statement_id = statement_id
        self.dataframe = FakeDataFrame([{"region": "EMEA", "revenue": 42}])

    def execute_sql_with_metadata(self, sql):
        self.sql.append(sql)
        if self.failure is not None:
            raise self.failure
        return ExecutionResult(
            dataframe=self.dataframe,
            statement_id=self.statement_id,
            metadata={"compute_type": "test"},
        )


class _TraceLLM:
    provider_name = "test-provider"
    model_name = "test-model"

    def __init__(self, responses=(), *, failure=None):
        self.responses = list(responses)
        self.failure = failure

    def complete(self, **kwargs):
        if self.failure is not None:
            raise self.failure
        return self.responses.pop(0)

    def complete_with_history(self, **kwargs):
        return self.complete(**kwargs)


def _semantic_query(filter_value="EMEA"):
    return SemanticQuery(
        model_name="sales.orders",
        metrics=["revenue"],
        group_by=["region"],
        filters=[{"column": "region", "operator": "eq", "value": filter_value}],
        response_format="table",
        explanation="Revenue by region",
    )


def _semantic_engine_53(
    tmp_path,
    tracer,
    *,
    backend=None,
    policy="off",
    store=None,
    monotonic=None,
):
    models_dir = tmp_path / f"models_{type(tracer).__name__}_{id(tracer)}"
    models_dir.mkdir()
    model = {
        "models": [
            {
                "key": "sales.orders",
                "table": "gold.fact_orders",
                "dimensions": [{"name": "region", "sql": "region"}],
                "metrics": [{"name": "revenue", "sql": "amount", "type": "sum"}],
            }
        ]
    }
    catalog = {
        "models": [
            {
                "key": "sales.orders",
                "file": "sales_orders.yaml",
                "description": "Sales orders",
                "dimensions": ["region"],
                "metrics": ["revenue"],
            }
        ]
    }
    (models_dir / "sales_orders.yaml").write_text(
        yaml.safe_dump(model), encoding="utf-8"
    )
    (models_dir / "semantic_catalog.yaml").write_text(
        yaml.safe_dump(catalog), encoding="utf-8"
    )
    backend = backend or _SemanticBackend()
    core = SimpleNamespace(
        db="main",
        env="TEST",
        config={
            "environments": {
                "test": {"semantic_certification_policy": policy}
            }
        },
        _tracer=tracer,
        _tracing_config=TracingConfig(),
        _get_backend=lambda: backend,
    )
    engine = SemanticEngine(
        core,
        models_dir=str(models_dir),
        certification_store=store,
        utc_now=lambda: COMPILED_AT,
        monotonic=monotonic,
    )
    return engine, backend


def _hub_53(tmp_path, tracer):
    engine, backend = _semantic_engine_53(tmp_path, tracer)
    llm = _TraceLLM(
        [
            "genbi",
            '{"selected":"sales.orders","candidates":[],"reason":"ok"}',
            (
                '{"model_name":"sales.orders","metrics":["revenue"],'
                '"group_by":["region"],"filters":[],"mode":"query",'
                '"response_format":"table","explanation":"ok"}'
            ),
        ]
    )
    return AgenticHub(semantic_engine=engine, llm_provider=llm), backend


def test_hub_ask_nominal_span_tree_has_stable_names_order_and_parentage(tmp_path):
    tracer = InMemoryTracer()
    hub, backend = _hub_53(tmp_path, tracer)

    response = hub.ask("question-canary-53")

    assert response.success
    assert len(backend.sql) == 1
    assert _span_names(tracer) == [
        "skifer.agent.route",
        "skifer.llm.complete",
        "skifer.semantic.query",
        "skifer.semantic.model_select",
        "skifer.llm.complete",
        "skifer.llm.complete",
        "skifer.semantic.policy",
        "skifer.semantic.compile",
        "skifer.semantic.policy",
        "skifer.sql.execute",
    ]
    route, classify, semantic, select, select_llm, translate_llm, *semantic_tail = (
        tracer.spans
    )
    assert route.parent_span_id is None
    assert classify.parent_span_id == route.span_id
    assert semantic.parent_span_id == route.span_id
    assert select.parent_span_id == semantic.span_id
    assert select_llm.parent_span_id == select.span_id
    assert translate_llm.parent_span_id == semantic.span_id
    assert all(span.parent_span_id == semantic.span_id for span in semantic_tail)
    assert len({span.trace_id for span in tracer.spans}) == 1


def test_direct_query_with_evidence_has_semantic_root_and_active_trace_id(tmp_path):
    tracer = InMemoryTracer()
    engine, _ = _semantic_engine_53(tmp_path, tracer)

    result = engine.query_with_evidence(_semantic_query())

    assert _span_names(tracer) == [
        "skifer.semantic.query",
        "skifer.semantic.policy",
        "skifer.semantic.compile",
        "skifer.semantic.policy",
        "skifer.sql.execute",
    ]
    root = tracer.spans[0]
    assert root.parent_span_id is None
    assert result.evidence.trace_id == root.trace_id


def test_explicit_consumer_trace_seeds_root_but_active_root_takes_precedence(tmp_path):
    tracer = InMemoryTracer()
    engine, _ = _semantic_engine_53(tmp_path, tracer)
    explicit = ConsumerContext("consumer", "dashboard", trace_id=TRACE_ID)

    seeded = engine.query_with_evidence(_semantic_query(), consumer_context=explicit)
    assert seeded.evidence.trace_id == TRACE_ID
    assert tracer.spans[0].trace_id == TRACE_ID

    with configured_span_scope(tracer, "outer-root") as outer:
        nested = engine.query_with_evidence(
            _semantic_query(),
            consumer_context=ConsumerContext(
                "consumer", "dashboard", trace_id=OTHER_TRACE_ID
            ),
        )
    assert nested.evidence.trace_id == outer.trace_id
    assert nested.evidence.trace_id != OTHER_TRACE_ID


def test_noop_keeps_legacy_explicit_consumer_trace_id(tmp_path):
    engine, _ = _semantic_engine_53(tmp_path, NoOpTracer())
    context = ConsumerContext("consumer", "dashboard", trace_id="legacy-trace-id")

    result = engine.query_with_evidence(_semantic_query(), consumer_context=context)

    assert result.evidence.trace_id == "legacy-trace-id"


def test_sql_span_reuses_evidence_hash_and_never_contains_sql_text(tmp_path):
    tracer = InMemoryTracer()
    engine, backend = _semantic_engine_53(tmp_path, tracer)

    result = engine.query_with_evidence(_semantic_query("secret_filter_value"))

    sql_span = next(span for span in tracer.spans if span.name == "skifer.sql.execute")
    assert sql_span.attributes == {
        "sql_hash": result.evidence.sql_hash,
        "compute_type": "statement_execution",
        "statement_id": "statement-42",
    }
    assert backend.sql[0] not in repr(sql_span.attributes)
    assert "SELECT" not in repr(sql_span.attributes)
    assert "secret_filter_value" not in repr(sql_span.attributes)


def test_llm_span_has_provider_model_latency_but_no_prompt_and_reraises(tmp_path):
    failure = RuntimeError("prompt-canary@example.test")
    provider = _TraceLLM(failure=failure)
    tracer = InMemoryTracer()

    with pytest.raises(RuntimeError) as caught:
        with llm_span_scope(provider, tracer):
            provider.complete(
                system_prompt="system-prompt-canary",
                user_message="user-question-canary",
            )

    assert caught.value is failure
    span = tracer.spans[0]
    assert span.status == "ERROR"
    assert span.exceptions[0].type_name == "RuntimeError"
    assert span.attributes["provider"] == "test-provider"
    assert span.attributes["model"] == "test-model"
    assert span.attributes["latency_seconds"] >= 0
    serialized = repr(span.attributes)
    assert "prompt-canary" not in serialized
    assert "user-question-canary" not in serialized


def test_sql_failure_marks_span_error_preserves_cause_and_omits_message(tmp_path):
    failure = ValueError("backend leaked alice@example.test")
    tracer = InMemoryTracer()
    engine, _ = _semantic_engine_53(
        tmp_path, tracer, backend=_SemanticBackend(failure=failure)
    )

    with pytest.raises(SemanticExecutionError) as caught:
        engine.query_with_evidence(_semantic_query())

    assert caught.value.__cause__ is failure
    sql_span = next(span for span in tracer.spans if span.name == "skifer.sql.execute")
    assert sql_span.status == "ERROR"
    assert sql_span.exceptions[0].type_name == "ValueError"
    assert "alice@example.test" not in repr(sql_span.attributes)


def test_semantic_result_is_identical_with_noop_and_inmemory_tracing(tmp_path):
    shared_dataframe = FakeDataFrame([{"region": "EMEA", "revenue": 42}])

    def run(tracer):
        ticks = iter([10.0, 12.0])
        backend = _SemanticBackend()
        backend.dataframe = shared_dataframe
        engine, backend = _semantic_engine_53(
            tmp_path,
            tracer,
            backend=backend,
            monotonic=lambda: next(ticks),
        )
        result = engine.query_with_evidence(
            _semantic_query(),
            consumer_context=ConsumerContext(
                "consumer", "dashboard", trace_id=TRACE_ID
            ),
            evidence_id="evidence-fixed",
            compiled_at=COMPILED_AT,
        )
        return result, backend.sql

    untraced, untraced_sql = run(NoOpTracer())
    traced, traced_sql = run(InMemoryTracer())

    assert traced == untraced
    assert traced_sql == untraced_sql


class _DenyStore:
    def __init__(self):
        self.calls = 0

    def get_certification(self, dataset, consumer_class):
        self.calls += 1
        return Certification(
            dataset=dataset,
            consumer_class=consumer_class,
            status="UNCERTIFIED",
            contract_version=None,
            definition_hash=None,
            certified_at=None,
        )


def test_deny_gate_evaluation_count_and_zero_sql_are_tracer_independent(
    tmp_path, monkeypatch
):
    from skifer.semantic import semantic as semantic_module

    original_evaluate = semantic_module.evaluate
    counts = {"noop": 0, "memory": 0}
    current = {"name": "noop"}

    def counted_evaluate(*args, **kwargs):
        counts[current["name"]] += 1
        return original_evaluate(*args, **kwargs)

    monkeypatch.setattr(semantic_module, "evaluate", counted_evaluate)

    for name, tracer in (("noop", NoOpTracer()), ("memory", InMemoryTracer())):
        current["name"] = name
        store = _DenyStore()
        engine, backend = _semantic_engine_53(
            tmp_path, tracer, policy="enforce", store=store
        )
        with pytest.raises(SemanticAccessDenied):
            engine.query_with_evidence(_semantic_query())
        assert backend.sql == []
        assert store.calls == 1

    assert counts == {"noop": 1, "memory": 1}


def test_semantic_agent_canaries_never_enter_span_data(tmp_path):
    tracer = InMemoryTracer()
    hub, _ = _hub_53(tmp_path, tracer)
    question = "question-secret alice@example.test"

    hub.ask(question)
    hub._semantic_engine.query_with_evidence(
        _semantic_query("filter_value_canary")
    )

    serialized = repr(
        [
            (span.name, span.attributes, span.events, span.exceptions)
            for span in tracer.spans
        ]
    )
    assert question not in serialized
    assert "alice@example.test" not in serialized
    assert "filter_value_canary" not in serialized
    assert "system_prompt" not in serialized
    assert "user_message" not in serialized


def test_serving_records_response_serialization_boundary(tmp_path):
    tracer = InMemoryTracer()
    hub, _ = _hub_53(tmp_path, tracer)
    model = SkiferChatModel()
    model._hub = hub

    result = model.predict(
        context=None,
        messages=[{"role": "user", "content": "serve this request"}],
    )

    assert result["choices"][0]["finish_reason"] == "stop"
    assert tracer.spans[-1].name == "skifer.response.serialize"
    assert tracer.spans[-1].status == "OK"


# ---------------------------------------------------------------------------
# Plan 29 — end-to-end run identity
# ---------------------------------------------------------------------------

FIXED_RUN_ID = "11111111-2222-3333-4444-555555555555"


def _persisted_run_ids(engine, dataset="gold.orders"):
    return {event.run_id for event in engine.certification_store.list_history(dataset)}


def _run_with_fixed_id(tracer, runner):
    """Run one pipeline with uuid4 pinned, so two runs are comparable."""
    from unittest.mock import patch
    from uuid import UUID

    with patch("skifer.core.core.uuid4", return_value=UUID(FIXED_RUN_ID)):
        engine, backend = _engine(tracer)
        runner(engine)
    return engine, backend


def test_persisted_run_id_is_identical_whether_or_not_tracing_is_on():
    """The certification identity is business data, so tracing must not move it.

    This is the slice 5.2 guarantee restated for end-to-end identity: the id is
    minted on the business path. Deriving it from the trace context is what once
    made turning tracing on change what the registry persisted.
    """
    def run(engine):
        engine.run_process_to_table(_schema(), "gold", "orders")

    untraced, _ = _run_with_fixed_id(NoOpTracer(), run)
    traced, _ = _run_with_fixed_id(InMemoryTracer(), run)

    assert _persisted_run_ids(untraced) == {FIXED_RUN_ID}
    assert _persisted_run_ids(traced) == {FIXED_RUN_ID}


def test_publication_persists_exactly_one_run_id_per_pipeline_run():
    """A second minted id would split one run across two audit identities."""
    engine, _ = _run_with_fixed_id(
        InMemoryTracer(),
        lambda e: e.run_process_to_table(_schema(), "gold", "orders"),
    )

    history = engine.certification_store.list_history("gold.orders")
    assert {event.run_id for event in history} == {FIXED_RUN_ID}
    # The full lifecycle is recorded, all of it under that single identity.
    assert {event.state for event in history} == {
        "STARTED", "STAGING", "STAGED", "PROMOTING", "PROMOTED",
    }


def test_run_from_yaml_threads_the_pipeline_run_id_to_the_registry(tmp_path):
    """The YAML entry point is the common one; it must not mint a second id."""
    path = tmp_path / "orders.yaml"
    path.write_text(yaml.safe_dump(_schema()))

    for tracer in (NoOpTracer(), InMemoryTracer()):
        engine, _ = _run_with_fixed_id(
            tracer, lambda e: e.run_from_yaml(str(path), "gold", "orders")
        )
        assert _persisted_run_ids(engine) == {FIXED_RUN_ID}


def test_run_from_yaml_emits_the_schema_load_span_from_the_single_flow(tmp_path):
    """One implementation, instrumented in place — not a traced copy of it."""
    path = tmp_path / "orders.yaml"
    path.write_text(yaml.safe_dump(_schema()))

    tracer = InMemoryTracer()
    engine, _ = _engine(tracer)
    engine.run_from_yaml(str(path), "gold", "orders")

    assert "skifer.schema.load" in _span_names(tracer)


def test_patterns_still_mint_a_run_id_when_called_without_one():
    """PipelinePatterns stays usable standalone; the engine supplies the id."""
    engine, _ = _engine(NoOpTracer())
    engine._patterns.run_process_to_table(_schema(), "gold", "orders", run_id=None)

    persisted = _persisted_run_ids(engine)
    assert len(persisted) == 1
    assert persisted != {None}
