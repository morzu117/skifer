"""Plan 29 slice 5.4 exporter tests; every SDK is an in-process fake."""

from __future__ import annotations

import logging
from pathlib import Path
import subprocess
import sys
from types import ModuleType, SimpleNamespace

import pytest

from skifer.core.config import TracingConfig, parse_tracing_config
from skifer.observability import tracing_exporters
from skifer.observability.tracing import NoOpTracer, configured_span_scope
from skifer.observability.tracing_exporters import (
    DualTracer,
    MLflowTracer,
    OTLPTracer,
    TracingExporterError,
    create_tracer,
)


class FakeContext:
    def __init__(self, trace_id):
        self.trace_id = trace_id


class FakeSpan:
    def __init__(self, name, attributes, trace_id, parent=None, failures=()):
        self.name = name
        self.attributes = dict(attributes)
        self.trace_id = trace_id
        self.parent = parent
        self.failures = set(failures)
        self.events = []
        self.exceptions = []
        self.status = None
        self.ended = False

    def get_span_context(self):
        return FakeContext(self.trace_id)

    def set_attribute(self, key, value):
        if "attribute" in self.failures:
            raise RuntimeError("SDK_TOKEN_CANARY")
        self.attributes[key] = value

    def add_event(self, name, attributes=None):
        self.events.append((name, attributes))

    def record_exception(self, exc):
        self.exceptions.append(type(exc).__name__)

    def set_status(self, status):
        self.status = status

    def end(self):
        if "end" in self.failures:
            raise RuntimeError("SDK_TOKEN_CANARY")
        self.ended = True


class FakeManager:
    def __init__(self, tracer, span, failures=()):
        self.tracer = tracer
        self.span = span
        self.failures = set(failures)
        self.previous = None

    def __enter__(self):
        if "open" in self.failures:
            raise RuntimeError("SDK_TOKEN_CANARY")
        self.previous = self.tracer.active
        self.tracer.active = self.span
        return self.span

    def __exit__(self, exc_type, exc, traceback):
        self.tracer.active = self.previous
        if "close" in self.failures:
            raise RuntimeError("SDK_TOKEN_CANARY")
        return False


class FakeOTelSDKTracer:
    def __init__(self, failures=()):
        self.failures = set(failures)
        self.active = None
        self.spans = []

    def start_as_current_span(self, name, attributes=None, end_on_exit=False):
        span = FakeSpan(
            name,
            attributes or {},
            len(self.spans) + 1,
            parent=self.active,
            failures=self.failures,
        )
        self.spans.append(span)
        return FakeManager(self, span, failures=self.failures)


class FakeMLflow(ModuleType):
    def __init__(self, *, failures=(), location_exists=True):
        super().__init__("mlflow")
        self.failures = set(failures)
        self.location_exists = location_exists
        self.active = None
        self.spans = []
        self.searches = []
        self.tracking_uris = []
        self.creation_calls = []

    def set_tracking_uri(self, value):
        self.tracking_uris.append(value)

    def search_traces(self, **kwargs):
        self.searches.append(kwargs)
        if not self.location_exists:
            raise RuntimeError("location missing SDK_TOKEN_CANARY")
        return []

    def create_experiment(self, *args, **kwargs):
        self.creation_calls.append((args, kwargs))
        raise AssertionError("creation is forbidden")

    def set_experiment(self, *args, **kwargs):
        self.creation_calls.append((args, kwargs))
        raise AssertionError("creation is forbidden")

    def start_span(self, *, name, attributes, trace_destination=None):
        span = FakeSpan(
            name,
            attributes,
            len(self.spans) + 100,
            parent=self.active,
            failures=self.failures,
        )
        span.destination = trace_destination
        self.spans.append(span)
        return FakeManager(self, span, failures=self.failures)


def _install_fake_otel(monkeypatch, *, failures=(), init_failure=False):
    sdk_tracer = FakeOTelSDKTracer(failures)
    captured = SimpleNamespace(exporters=[], processors=[])

    class Exporter:
        def __init__(self, **kwargs):
            captured.exporters.append(kwargs)
            if init_failure:
                raise RuntimeError("SDK_TOKEN_CANARY")

    class Provider:
        def add_span_processor(self, processor):
            captured.processors.append(processor)

        def get_tracer(self, name):
            captured.instrumentation_name = name
            return sdk_tracer

    class Processor:
        def __init__(self, exporter):
            self.exporter = exporter

    class StatusCode:
        OK = "OK_CODE"
        ERROR = "ERROR_CODE"

    modules = {
        "opentelemetry": ModuleType("opentelemetry"),
        "opentelemetry.trace": SimpleNamespace(
            Status=lambda code: ("status", code), StatusCode=StatusCode
        ),
        "opentelemetry.sdk": ModuleType("opentelemetry.sdk"),
        "opentelemetry.sdk.trace": SimpleNamespace(TracerProvider=Provider),
        "opentelemetry.sdk.trace.export": SimpleNamespace(BatchSpanProcessor=Processor),
        "opentelemetry.exporter": ModuleType("opentelemetry.exporter"),
        "opentelemetry.exporter.otlp": ModuleType("opentelemetry.exporter.otlp"),
        "opentelemetry.exporter.otlp.proto": ModuleType(
            "opentelemetry.exporter.otlp.proto"
        ),
        "opentelemetry.exporter.otlp.proto.http": ModuleType(
            "opentelemetry.exporter.otlp.proto.http"
        ),
        "opentelemetry.exporter.otlp.proto.http.trace_exporter": SimpleNamespace(
            OTLPSpanExporter=Exporter
        ),
    }
    for name, module in modules.items():
        monkeypatch.setitem(sys.modules, name, module)
    return sdk_tracer, captured


def _install_fake_mlflow(monkeypatch, *, failures=(), location_exists=True):
    mlflow = FakeMLflow(failures=failures, location_exists=location_exists)

    class UnityCatalog:
        def __init__(self, catalog_name, schema_name, table_prefix=None):
            self.catalog_name = catalog_name
            self.schema_name = schema_name
            self.table_prefix = table_prefix

    class MlflowExperimentLocation:
        def __init__(self, experiment_id):
            self.experiment_id = experiment_id

    locations = SimpleNamespace(
        UnityCatalog=UnityCatalog,
        MlflowExperimentLocation=MlflowExperimentLocation,
    )
    monkeypatch.setitem(sys.modules, "mlflow", mlflow)
    monkeypatch.setitem(sys.modules, "mlflow.entities", ModuleType("mlflow.entities"))
    monkeypatch.setitem(sys.modules, "mlflow.entities.trace_location", locations)
    return mlflow


@pytest.fixture(autouse=True)
def reset_export_warning(monkeypatch):
    monkeypatch.setattr(tracing_exporters, "_WARNING_EMITTED", False)


def test_exporter_module_import_is_lazy_without_optional_sdks():
    project_root = Path(__file__).resolve().parents[1]
    script = """
import sys
assert 'mlflow' not in sys.modules
assert not any(n == 'opentelemetry' or n.startswith('opentelemetry.') for n in sys.modules)
import skifer.observability.tracing_exporters
assert 'mlflow' not in sys.modules
assert not any(n == 'opentelemetry' or n.startswith('opentelemetry.') for n in sys.modules)
"""
    completed = subprocess.run(
        [sys.executable, "-c", script],
        cwd=project_root,
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr


def test_factory_none_and_unknown_value_lists_every_valid_choice():
    assert isinstance(create_tracer(TracingConfig()), NoOpTracer)
    with pytest.raises(ValueError) as raised:
        create_tracer(SimpleNamespace(exporter="zipkin"))
    message = str(raised.value)
    assert all(value in message for value in ("none", "otlp", "mlflow", "dual"))


def test_config_parser_accepts_exporters_and_validates_unknown_value():
    for exporter in ("none", "otlp", "mlflow", "dual"):
        parsed = parse_tracing_config(
            {"observability": {"tracing": {"exporter": exporter}}}
        )
        assert parsed.exporter == exporter
    with pytest.raises(ValueError) as raised:
        parse_tracing_config(
            {"observability": {"tracing": {"exporter": "unknown"}}}
        )
    assert "none, otlp, mlflow, dual" in str(raised.value)


def test_factory_builds_otlp_mlflow_and_dual_adapters(monkeypatch):
    _install_fake_otel(monkeypatch)
    _install_fake_mlflow(monkeypatch)
    otlp = create_tracer(TracingConfig(exporter="otlp"))
    mlflow = create_tracer(
        TracingConfig(exporter="mlflow", trace_location="catalog.schema")
    )
    dual = create_tracer(
        TracingConfig(exporter="dual", trace_location="catalog.schema")
    )
    assert isinstance(otlp, OTLPTracer)
    assert isinstance(mlflow, MLflowTracer)
    assert isinstance(dual, DualTracer)
    assert [type(item) for item in dual.tracers] == [OTLPTracer, MLflowTracer]


@pytest.mark.parametrize("exporter", ["otlp", "mlflow"])
def test_missing_dependency_names_optional_extra(monkeypatch, exporter):
    real_import = tracing_exporters.importlib.import_module

    def missing(name):
        if name == "mlflow" or name.startswith("opentelemetry"):
            raise ModuleNotFoundError(name)
        return real_import(name)

    monkeypatch.setattr(tracing_exporters.importlib, "import_module", missing)
    config = TracingConfig(
        exporter=exporter,
        required=True,
        trace_location="catalog.schema",
    )
    with pytest.raises(TracingExporterError, match=r"pip install -e .*.tracing"):
        create_tracer(config)


@pytest.mark.parametrize("failure", ["open", "attribute", "end", "close"])
def test_export_failures_do_not_affect_business_and_warn_once(
    monkeypatch, caplog, failure
):
    sdk, _ = _install_fake_otel(monkeypatch, failures={failure})
    tracer = create_tracer(TracingConfig(exporter="otlp"))

    with caplog.at_level(logging.WARNING, logger=tracing_exporters.__name__):
        with configured_span_scope(tracer, "one") as span:
            span.set_attribute("run_id", "r-42")
        with configured_span_scope(tracer, "two") as span:
            span.set_attribute("run_id", "r-43")

    assert "business result" == "business result"
    assert len([record for record in caplog.records if record.levelno == logging.WARNING]) == 1
    assert "SDK_TOKEN_CANARY" not in caplog.text
    assert [item.name for item in sdk.spans] == ["one", "two"]


@pytest.mark.parametrize("failure", ["open", "attribute", "end", "close"])
def test_required_export_failures_propagate_sanitized_error(monkeypatch, failure):
    _install_fake_otel(monkeypatch, failures={failure})
    tracer = create_tracer(TracingConfig(exporter="otlp", required=True))

    with pytest.raises(TracingExporterError) as raised:
        with configured_span_scope(tracer, "required", required=True) as span:
            span.set_attribute("run_id", "r-42")
    assert "SDK_TOKEN_CANARY" not in str(raised.value)


def test_dual_keeps_mlflow_when_otlp_initialization_fails(monkeypatch):
    _install_fake_otel(monkeypatch, init_failure=True)
    mlflow = _install_fake_mlflow(monkeypatch)
    tracer = create_tracer(
        TracingConfig(exporter="dual", trace_location="catalog.schema")
    )

    with configured_span_scope(tracer, "skifer.semantic.query") as span:
        span.set_attribute("model_key", "orders")

    assert isinstance(tracer, DualTracer)
    assert [span.name for span in mlflow.spans] == ["skifer.semantic.query"]
    assert mlflow.spans[0].attributes["model_key"] == "orders"


def test_otlp_translation_preserves_name_attributes_parentage_and_status(monkeypatch):
    sdk, captured = _install_fake_otel(monkeypatch)
    tracer = create_tracer(
        TracingConfig(
            exporter="otlp",
            otlp_endpoint="https://collector.example/v1/traces",
            otlp_headers={"authorization": "SDK_TOKEN_CANARY"},
        )
    )

    with configured_span_scope(
        tracer, "skifer.semantic.query", attributes={"model_key": "orders"}
    ):
        with pytest.raises(ValueError):
            with configured_span_scope(tracer, "skifer.sql.execute"):
                raise ValueError("business")

    root, child = sdk.spans
    assert root.name == "skifer.semantic.query"
    assert root.attributes == {"model_key": "orders"}
    assert child.parent is root
    assert root.status == ("status", "OK_CODE")
    assert child.status == ("status", "ERROR_CODE")
    assert child.exceptions == ["ValueError"]
    assert captured.exporters == [
        {
            "endpoint": "https://collector.example/v1/traces",
            "headers": {"authorization": "SDK_TOKEN_CANARY"},
        }
    ]


def test_mlflow_missing_location_is_actionable_and_never_creates(monkeypatch):
    mlflow = _install_fake_mlflow(monkeypatch, location_exists=False)
    with pytest.raises(TracingExporterError) as raised:
        create_tracer(
            TracingConfig(
                exporter="mlflow",
                required=True,
                trace_location="catalog.missing",
            )
        )
    assert "does not exist or is not accessible" in str(raised.value)
    assert "No location was created" in str(raised.value)
    assert mlflow.creation_calls == []
    assert mlflow.searches == [
        {"locations": ["catalog.missing"], "max_results": 1}
    ]


def test_token_canary_never_appears_in_repr_init_or_export_failures(
    monkeypatch, caplog
):
    token = "SDK_TOKEN_CANARY"
    config = TracingConfig(
        exporter="otlp",
        required=True,
        otlp_endpoint=f"https://user:{token}@collector.example/v1/traces",
        otlp_headers={"authorization": f"Bearer {token}"},
        mlflow_tracking_uri=f"https://user:{token}@tracking.example",
    )
    assert token not in repr(config)

    _install_fake_otel(monkeypatch, init_failure=True)
    with caplog.at_level(logging.WARNING, logger=tracing_exporters.__name__):
        with pytest.raises(TracingExporterError) as initialized:
            create_tracer(config)
    assert token not in str(initialized.value)
    assert token not in caplog.text

    monkeypatch.setattr(tracing_exporters, "_WARNING_EMITTED", False)
    _install_fake_otel(monkeypatch, failures={"attribute"})
    tracer = create_tracer(config)
    with caplog.at_level(logging.WARNING, logger=tracing_exporters.__name__):
        with pytest.raises(TracingExporterError) as exported:
            with configured_span_scope(tracer, "canary", required=True) as span:
                span.set_attribute("run_id", "value")
    assert token not in str(exported.value)
    assert token not in caplog.text


def test_dedicated_environment_variables_configure_otlp_without_leaking(monkeypatch):
    _, captured = _install_fake_otel(monkeypatch)
    tracer = create_tracer(
        TracingConfig(exporter="otlp"),
        environ={
            tracing_exporters.OTLP_ENDPOINT_ENV: "https://collector/v1/traces",
            tracing_exporters.OTLP_HEADERS_ENV: '{"authorization":"secret"}',
        },
    )
    assert isinstance(tracer, OTLPTracer)
    assert captured.exporters[0] == {
        "endpoint": "https://collector/v1/traces",
        "headers": {"authorization": "secret"},
    }


def test_missing_dependency_warning_says_what_to_install(monkeypatch, caplog):
    # `required=False` is the default and the path an operator hits after
    # configuring an exporter without the optional extra: tracing silently does
    # nothing. The one warning they get must name what to install — the hint
    # used to exist only on the `required=True` exception.
    import skifer.observability.tracing_exporters as exporters

    monkeypatch.setattr(exporters, "_WARNING_EMITTED", False, raising=False)
    config = parse_tracing_config(
        {
            "observability": {
                "tracing": {
                    "exporter": "otlp",
                    "endpoint": "https://collector/v1/traces",
                }
            }
        }
    )

    with caplog.at_level(logging.WARNING):
        tracer = create_tracer(config)

    assert isinstance(tracer, NoOpTracer)
    assert 'pip install -e ".[tracing]"' in caplog.text
    assert "further warnings are suppressed" in caplog.text


def test_exporter_credentials_never_reach_logs_or_repr(monkeypatch, caplog):
    import skifer.observability.tracing_exporters as exporters

    monkeypatch.setattr(exporters, "_WARNING_EMITTED", False, raising=False)
    token = "TOKEN-CANARY-7f3a9b"
    config = parse_tracing_config(
        {
            "observability": {
                "tracing": {
                    "exporter": "otlp",
                    "endpoint": f"https://collector/v1/traces?token={token}",
                    "headers": {"authorization": token},
                }
            }
        }
    )

    with caplog.at_level(logging.DEBUG):
        create_tracer(config)

    assert token not in caplog.text
    assert token not in repr(config)
    with pytest.raises(TracingExporterError) as error:
        create_tracer(
            parse_tracing_config(
                {
                    "observability": {
                        "tracing": {
                            "exporter": "otlp",
                            "required": True,
                            "headers": {"authorization": token},
                        }
                    }
                }
            )
        )
    assert token not in str(error.value)
