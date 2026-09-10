"""Lazy, failure-isolated adapters for optional tracing SDKs.

Importing this module never imports OpenTelemetry or MLflow.  SDK imports and
all endpoint configuration are confined to the builder that needs them.
"""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar, Token
import importlib
import json
import logging
import os
import threading
from typing import Any, Iterator, Mapping

from skifer.observability.tracing import (
    TraceAttributePolicy,
    build_attribute_policy,
    Attributes,
    NoOpTracer,
    Scalar,
    TraceContext,
)


OTLP_ENDPOINT_ENV = "SKIFER_TRACING_OTLP_ENDPOINT"
OTLP_HEADERS_ENV = "SKIFER_TRACING_OTLP_HEADERS"
MLFLOW_TRACKING_URI_ENV = "SKIFER_TRACING_MLFLOW_TRACKING_URI"
MLFLOW_TRACE_LOCATION_ENV = "SKIFER_TRACING_MLFLOW_TRACE_LOCATION"
TRACING_EXTRA = 'pip install -e ".[tracing]"'
VALID_EXPORTERS = ("none", "otlp", "mlflow", "dual")

_LOGGER = logging.getLogger(__name__)
_WARNING_LOCK = threading.Lock()
_WARNING_EMITTED = False
_NOOP_SPAN = NoOpTracer().start_span("unused")


class TracingExporterError(RuntimeError):
    """Sanitized, actionable exporter failure safe to display or log."""


def _warning_once(
    exporter: str,
    phase: str,
    *,
    install_hint: bool = False,
    detail: str | None = None,
) -> None:
    """Log the single diagnostic an operator gets when tracing degrades.

    `required=False` is the default, and it is the path someone hits after
    configuring an exporter and forgetting the optional extra: tracing silently
    does nothing. A warning that omits what to install leaves nothing to act on,
    so the actionable text belongs here and not only on the raised exception.
    """
    global _WARNING_EMITTED
    with _WARNING_LOCK:
        if _WARNING_EMITTED:
            return
        _WARNING_EMITTED = True
    if detail:
        _LOGGER.warning(
            "%s Business execution continues and further warnings are suppressed.",
            detail,
        )
        return
    hint = f" Install the optional dependencies with `{TRACING_EXTRA}`." if install_hint else ""
    _LOGGER.warning(
        "Tracing exporter %s failed during %s; business execution continues and "
        "further warnings are suppressed.%s",
        exporter,
        phase,
        hint,
    )


def _failure(exporter: str, phase: str, *, install_hint: bool = False) -> TracingExporterError:
    hint = f" Install the optional dependencies with `{TRACING_EXTRA}`." if install_hint else ""
    return TracingExporterError(
        f"Tracing exporter {exporter!r} failed during {phase}.{hint} "
        "Exporter credentials and endpoint details were withheld."
    )


def _handle_failure(
    exporter: str,
    phase: str,
    *,
    required: bool,
    install_hint: bool = False,
) -> None:
    _warning_once(exporter, phase, install_hint=install_hint)
    if required:
        raise _failure(exporter, phase, install_hint=install_hint) from None


def _safe_attributes(
    attributes: Attributes | None,
    policy: TraceAttributePolicy | None = None,
) -> dict[str, Scalar]:
    """Apply the same allowlist the in-memory tracer applies.

    Filtering by type alone here would make redaction a test-only property: the
    canary assertions run against InMemoryTracer, while these adapters are the
    code that actually sends attributes out of the process. A key dropped in
    tests and exported in production is the worst possible split.
    """
    if not attributes:
        return {}
    policy = policy or TraceAttributePolicy()
    accepted: dict[str, Scalar] = {}
    for key, value in attributes.items():
        if not isinstance(key, str):
            continue
        if value is not None and not isinstance(value, (str, bool, int, float)):
            continue
        if not policy.allows(key):
            continue
        if len(accepted) >= policy.max_attributes:
            break
        accepted[key] = policy.truncate(value)
    return accepted


def _trace_id_from_span(span: Any) -> str | None:
    try:
        context = span.get_span_context()
        trace_id = context.trace_id
        if isinstance(trace_id, int) and trace_id:
            return f"{trace_id:032x}"
        if isinstance(trace_id, str):
            candidate = trace_id.removeprefix("0x").lower()
            if len(candidate) == 32 and int(candidate, 16):
                return candidate
    except Exception:
        return None
    return None


class _SDKSpan:
    """Translate the internal Span protocol and isolate every SDK operation."""

    def __init__(
        self,
        *,
        exporter: str,
        span: Any,
        manager: Any,
        activation_token: Token,
        active: ContextVar[Any],
        required: bool,
        owns_end: bool,
        status_factory: Any = None,
        attribute_policy: TraceAttributePolicy | None = None,
    ) -> None:
        self._attribute_policy = attribute_policy or TraceAttributePolicy()
        self._exporter = exporter
        self._span = span
        self._manager = manager
        self._activation_token = activation_token
        self._active = active
        self._required = required
        self._owns_end = owns_end
        self._status_factory = status_factory
        self._ended = False
        self.trace_id = _trace_id_from_span(span)

    def _call(self, phase: str, operation) -> None:
        try:
            operation()
        except Exception:
            _handle_failure(self._exporter, phase, required=self._required)

    def set_attribute(self, key: str, value: Scalar) -> None:
        # The allowlist has to be enforced here too, not only on the attributes
        # passed at span creation: this is the call that hands a key straight to
        # the SDK, and it is the one a leak would travel through.
        safe = _safe_attributes({key: value}, self._attribute_policy)
        if not safe:
            return
        for allowed_key, allowed_value in safe.items():
            self._call(
                "attribute export",
                lambda k=allowed_key, v=allowed_value: self._span.set_attribute(k, v),
            )

    def add_event(self, name: str, attributes: Attributes | None = None) -> None:
        def operation() -> None:
            method = getattr(self._span, "add_event", None)
            if method is not None:
                method(name, attributes=_safe_attributes(attributes, self._attribute_policy))

        self._call("event export", operation)

    def record_exception(self, exc: Exception) -> None:
        def operation() -> None:
            method = getattr(self._span, "record_exception", None)
            if method is not None:
                method(exc)
            else:
                self._span.set_attribute("exception.type", type(exc).__name__)

        self._call("exception export", operation)

    def end(self, status: str = "OK") -> None:
        if self._ended:
            return
        self._ended = True
        first_failure: TracingExporterError | None = None

        def attempt(phase: str, operation) -> None:
            nonlocal first_failure
            try:
                operation()
            except Exception:
                _warning_once(self._exporter, phase)
                if self._required and first_failure is None:
                    first_failure = _failure(self._exporter, phase)

        set_status = getattr(self._span, "set_status", None)
        if set_status is not None:
            value = self._status_factory(status) if self._status_factory else status
            attempt("status export", lambda: set_status(value))
        if self._owns_end:
            attempt("span close", self._span.end)
        attempt("span context close", lambda: self._manager.__exit__(None, None, None))
        try:
            self._active.reset(self._activation_token)
        except (RuntimeError, ValueError):
            pass
        if first_failure is not None:
            raise first_failure

    def __enter__(self) -> "_SDKSpan":
        return self

    def __exit__(self, exc_type, exc, traceback) -> bool:
        if isinstance(exc, Exception):
            try:
                self.record_exception(exc)
            except Exception:
                pass
            try:
                self.end("ERROR")
            except Exception:
                pass
        else:
            self.end()
        return False


class _SDKTracer:
    def __init__(
        self,
        exporter: str,
        sdk_tracer: Any,
        *,
        required: bool,
        attribute_policy: TraceAttributePolicy | None = None,
    ) -> None:
        self._attribute_policy = attribute_policy or TraceAttributePolicy()
        self.exporter = exporter
        self._sdk_tracer = sdk_tracer
        self.required = required
        self._active: ContextVar[_SDKSpan | None] = ContextVar(
            f"skifer_{exporter}_active_{id(self)}", default=None
        )
        self._context: ContextVar[TraceContext] = ContextVar(
            f"skifer_{exporter}_context_{id(self)}", default=TraceContext()
        )

    def _start_manager(self, name: str, attributes: dict[str, Scalar]):
        return self._sdk_tracer.start_as_current_span(
            name, attributes=attributes, end_on_exit=False
        ), True, None

    def start_span(self, name: str, *, attributes: Attributes | None = None):
        try:
            manager, owns_end, status_factory = self._start_manager(
                name, _safe_attributes(attributes, self._attribute_policy)
            )
            sdk_span = manager.__enter__()
            wrapper = _SDKSpan.__new__(_SDKSpan)
            token = self._active.set(wrapper)
            _SDKSpan.__init__(
                wrapper,
                exporter=self.exporter,
                span=sdk_span,
                manager=manager,
                activation_token=token,
                active=self._active,
                required=self.required,
                owns_end=owns_end,
                status_factory=status_factory,
                attribute_policy=self._attribute_policy,
            )
            return wrapper
        except Exception:
            _handle_failure(self.exporter, "span open", required=self.required)
            return _NOOP_SPAN

    def current_trace_id(self) -> str | None:
        active = self._active.get()
        if active is not None:
            return active.trace_id or self._context.get().trace_id
        return self._context.get().trace_id

    def current_context(self) -> TraceContext:
        ambient = self._context.get()
        return TraceContext(
            trace_id=self.current_trace_id() or ambient.trace_id,
            run_id=ambient.run_id,
            session_id=ambient.session_id,
            evidence_id=ambient.evidence_id,
        )

    @contextmanager
    def use_context(self, context: TraceContext) -> Iterator[TraceContext]:
        token = self._context.set(context)
        try:
            yield context
        finally:
            self._context.reset(token)


class OTLPTracer(_SDKTracer):
    """OpenTelemetry tracer exporting spans over OTLP/HTTP."""


class MLflowTracer(_SDKTracer):
    """MLflow fluent-span adapter targeting an existing trace location."""

    def __init__(self, mlflow_module: Any, destination: Any, *, required: bool) -> None:
        super().__init__("mlflow", mlflow_module, required=required)
        self._destination = destination

    def _start_manager(self, name: str, attributes: dict[str, Scalar]):
        kwargs: dict[str, Any] = {"name": name, "attributes": attributes}
        if self._active.get() is None:
            kwargs["trace_destination"] = self._destination
        return self._sdk_tracer.start_span(**kwargs), False, None


class _DualSpan:
    def __init__(self, spans: list[Any], *, required: bool) -> None:
        self._spans = spans
        self._required = required

    def _each(self, method: str, *args, **kwargs) -> None:
        first: Exception | None = None
        for span in self._spans:
            try:
                getattr(span, method)(*args, **kwargs)
            except Exception as exc:
                first = first or exc
        if self._required and first is not None:
            raise first

    def set_attribute(self, key: str, value: Scalar) -> None:
        self._each("set_attribute", key, value)

    def add_event(self, name: str, attributes: Attributes | None = None) -> None:
        self._each("add_event", name, attributes)

    def record_exception(self, exc: Exception) -> None:
        self._each("record_exception", exc)

    def end(self, status: str = "OK") -> None:
        self._spans.reverse()
        self._each("end", status)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback) -> bool:
        if isinstance(exc, Exception):
            self.record_exception(exc)
            self.end("ERROR")
        else:
            self.end()
        return False


class DualTracer:
    """Fan out every operation; one unavailable destination cannot starve the other."""

    def __init__(self, tracers: list[Any], *, required: bool = False) -> None:
        self.tracers = tuple(tracers)
        self.required = required

    def start_span(self, name: str, *, attributes: Attributes | None = None):
        spans = []
        first: Exception | None = None
        for tracer in self.tracers:
            try:
                spans.append(tracer.start_span(name, attributes=attributes))
            except Exception as exc:
                first = first or exc
        if self.required and first is not None:
            raise first
        return _DualSpan(spans or [_NOOP_SPAN], required=self.required)

    def current_trace_id(self) -> str | None:
        for tracer in self.tracers:
            try:
                value = tracer.current_trace_id()
                if value:
                    return value
            except Exception:
                _warning_once("dual", "trace context read")
        return None

    @contextmanager
    def use_context(self, context: TraceContext) -> Iterator[TraceContext]:
        managers = []
        first: Exception | None = None
        for tracer in self.tracers:
            manager_factory = getattr(tracer, "use_context", None)
            if manager_factory is None:
                continue
            try:
                manager = manager_factory(context)
                manager.__enter__()
                managers.append(manager)
            except Exception as exc:
                first = first or exc
                _warning_once("dual", "trace context attach")
        if self.required and first is not None:
            raise first
        try:
            yield context
        finally:
            for manager in reversed(managers):
                try:
                    manager.__exit__(None, None, None)
                except Exception:
                    _warning_once("dual", "trace context detach")


def _parse_env_headers(raw: str | None) -> dict[str, str]:
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except Exception:
        raise _failure("otlp", f"parsing {OTLP_HEADERS_ENV}") from None
    if not isinstance(parsed, dict) or not all(
        isinstance(key, str) and isinstance(value, str) for key, value in parsed.items()
    ):
        raise _failure("otlp", f"parsing {OTLP_HEADERS_ENV}")
    return parsed


def _build_otlp(config: Any, environ: Mapping[str, str]) -> OTLPTracer:
    try:
        trace = importlib.import_module("opentelemetry.trace")
        sdk_trace = importlib.import_module("opentelemetry.sdk.trace")
        sdk_export = importlib.import_module("opentelemetry.sdk.trace.export")
        otlp = importlib.import_module(
            "opentelemetry.exporter.otlp.proto.http.trace_exporter"
        )
    except (ImportError, ModuleNotFoundError):
        raise _failure("otlp", "initialization", install_hint=True) from None

    endpoint = config.otlp_endpoint or environ.get(OTLP_ENDPOINT_ENV)
    headers = dict(config.otlp_headers) or _parse_env_headers(environ.get(OTLP_HEADERS_ENV))
    try:
        exporter = otlp.OTLPSpanExporter(endpoint=endpoint, headers=headers or None)
        provider = sdk_trace.TracerProvider()
        provider.add_span_processor(sdk_export.BatchSpanProcessor(exporter))
        sdk_tracer = provider.get_tracer("skifer")

        def status_factory(status: str):
            code = trace.StatusCode.ERROR if status == "ERROR" else trace.StatusCode.OK
            return trace.Status(code)

        tracer = OTLPTracer("otlp", sdk_tracer, required=config.required)
        original = tracer._start_manager
        tracer._start_manager = lambda name, attributes: (
            original(name, attributes)[0],
            True,
            status_factory,
        )
        return tracer
    except Exception:
        raise _failure("otlp", "initialization") from None


def _mlflow_destination(mlflow: Any, location: str) -> Any:
    try:
        entities = importlib.import_module("mlflow.entities.trace_location")
        parts = location.split(".")
        if len(parts) in (2, 3) and all(parts):
            prefix = parts[2] if len(parts) == 3 else None
            return entities.UnityCatalog(
                catalog_name=parts[0], schema_name=parts[1], table_prefix=prefix
            )
        return entities.MlflowExperimentLocation(experiment_id=location)
    except Exception:
        raise _failure("mlflow", "trace location parsing") from None


def _build_mlflow(config: Any, environ: Mapping[str, str]) -> MLflowTracer:
    try:
        mlflow = importlib.import_module("mlflow")
    except (ImportError, ModuleNotFoundError):
        raise _failure("mlflow", "initialization", install_hint=True) from None

    location = config.trace_location or environ.get(MLFLOW_TRACE_LOCATION_ENV)
    if not location:
        raise TracingExporterError(
            "MLflow tracing requires an existing `trace_location`. Create the MLflow "
            "experiment or Unity Catalog trace location first, then configure "
            "observability.tracing.trace_location; Skifer will not create it."
        )
    tracking_uri = config.mlflow_tracking_uri or environ.get(MLFLOW_TRACKING_URI_ENV)
    try:
        if tracking_uri:
            mlflow.set_tracking_uri(tracking_uri)
        # Deliberately read-only. In particular, never call set_experiment or
        # set_experiment_trace_location: both may provision storage.
        mlflow.search_traces(locations=[location], max_results=1)
        destination = _mlflow_destination(mlflow, location)
        return MLflowTracer(mlflow, destination, required=config.required)
    except TracingExporterError:
        raise
    except Exception:
        raise TracingExporterError(
            "MLflow trace_location does not exist or is not accessible. Create it in "
            "the configured MLflow/Unity Catalog workspace, grant access, and retry. "
            "No location was created."
        ) from None


def _best_effort_build(builder, config: Any, environ: Mapping[str, str]):
    try:
        return builder(config, environ)
    except Exception as exc:
        exporter = "otlp" if builder is _build_otlp else "mlflow"
        # TracingExporterError is documented as sanitized and actionable, so
        # reuse its message instead of rebuilding a generic one that drops the
        # install hint the builder already worked out.
        detail = str(exc) if isinstance(exc, TracingExporterError) else None
        _warning_once(exporter, "initialization", detail=detail)
        if config.required:
            if isinstance(exc, TracingExporterError):
                raise exc from None
            raise _failure(exporter, "initialization") from None
        return NoOpTracer()


def create_tracer(config: Any, *, environ: Mapping[str, str] | None = None):
    """Build the configured tracer without importing unused optional SDKs."""
    exporter = getattr(config, "exporter", "none")
    if exporter not in VALID_EXPORTERS:
        raise ValueError(
            f"Unsupported tracing exporter {exporter!r}. Valid values: "
            f"{', '.join(VALID_EXPORTERS)}."
        )
    if exporter == "none":
        return NoOpTracer()
    source = os.environ if environ is None else environ
    _ = build_attribute_policy(config, source)  # validated eagerly, applied per tracer
    if exporter == "otlp":
        return _best_effort_build(_build_otlp, config, source)
    if exporter == "mlflow":
        return _best_effort_build(_build_mlflow, config, source)
    return DualTracer(
        [
            _best_effort_build(_build_otlp, config, source),
            _best_effort_build(_build_mlflow, config, source),
        ],
        required=config.required,
    )


__all__ = [
    "DualTracer",
    "MLflowTracer",
    "OTLPTracer",
    "TracingExporterError",
    "create_tracer",
]
