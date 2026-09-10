"""Dependency-free tracing primitives and deterministic in-memory test tracer.

Trace identifiers use the stable W3C representation: 32 lowercase hexadecimal
characters for a trace ID and 16 for a span ID.  The all-zero value is invalid.
This module deliberately has no OpenTelemetry or MLflow import; optional
exporters belong behind a lazy adapter.
"""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar, Token
import hashlib
import hmac
from dataclasses import dataclass
import logging
import math
import re
import secrets
import threading
import time
from typing import Any, Callable, Iterator, Mapping, Optional, Protocol, Union


Scalar = Optional[Union[str, bool, int, float]]
Attributes = Mapping[str, Scalar]

TRACE_ID_HEX_LENGTH = 32
SPAN_ID_HEX_LENGTH = 16
TRACE_FORMAT_VERSION = "w3c-v1"

_TRACE_ID_PATTERN = re.compile(r"^[0-9a-f]{32}$")
_SPAN_ID_PATTERN = re.compile(r"^[0-9a-f]{16}$")
_LOGGER = logging.getLogger(__name__)
_FAILURE_WARNING_LOCK = threading.Lock()
_FAILURE_WARNING_EMITTED = False
_CORRELATION_CONTEXT: ContextVar[TraceContext]


class Span(Protocol):
    """Internal span boundary used by business code."""

    def set_attribute(self, key: str, value: Scalar) -> None: ...

    def add_event(
        self, name: str, attributes: Optional[Attributes] = None
    ) -> None: ...

    def record_exception(self, exc: Exception) -> None: ...

    def end(self, status: str = "OK") -> None: ...


class Tracer(Protocol):
    """Minimal tracing interface; exporters implement this without core coupling."""

    def start_span(
        self, name: str, *, attributes: Optional[Attributes] = None
    ) -> Span: ...

    def current_trace_id(self) -> Optional[str]: ...


@dataclass(frozen=True)
class TraceContext:
    """Correlation IDs propagated independently from any exporter SDK.

    ``trace_id`` follows W3C Trace Context (32 lowercase hexadecimal
    characters, excluding all zeroes).  Run, session and evidence IDs remain
    opaque identifiers owned by their respective Skifer layers.
    """

    trace_id: Optional[str] = None
    run_id: Optional[str] = None
    session_id: Optional[str] = None
    evidence_id: Optional[str] = None

    def __post_init__(self) -> None:
        if self.trace_id is not None and not is_valid_trace_id(self.trace_id):
            raise ValueError(
                "trace_id must be 32 lowercase hexadecimal characters and not all zeroes"
            )


_CORRELATION_CONTEXT = ContextVar(
    "skifer_correlation_context",
    default=TraceContext(),
)


@dataclass(frozen=True)
class SpanEvent:
    """An event captured in deterministic insertion order."""

    name: str
    timestamp: float
    attributes: Mapping[str, Scalar]


@dataclass(frozen=True)
class RecordedException:
    """Safe exception metadata; backend messages are deliberately not retained."""

    type_name: str


@dataclass(frozen=True)
class RejectedAttribute:
    """Audit record for an attribute omitted by scalar validation."""

    span_id: str
    key: str
    value_type: str
    source: str


def is_valid_trace_id(value: str) -> bool:
    """Return whether *value* is a non-zero W3C trace identifier."""
    return bool(_TRACE_ID_PATTERN.fullmatch(value)) and value != "0" * TRACE_ID_HEX_LENGTH


def is_valid_span_id(value: str) -> bool:
    """Return whether *value* is a non-zero W3C span identifier."""
    return bool(_SPAN_ID_PATTERN.fullmatch(value)) and value != "0" * SPAN_ID_HEX_LENGTH


def _generate_w3c_id(hex_length: int) -> str:
    """Generate a lowercase, non-zero W3C-compatible identifier."""
    while True:
        value = secrets.token_hex(hex_length // 2)
        if int(value, 16):
            return value


def new_trace_id() -> str:
    """Create a W3C-compatible trace ID for a new correlation boundary."""
    return _generate_w3c_id(TRACE_ID_HEX_LENGTH)


# Attribute keys the taxonomy admits (section 2 of the Feature 5 plan), plus the
# ones the instrumentation slices actually emit. Anything else is dropped and
# recorded as rejected: an allowlist is the only redaction that stays correct
# when a later slice adds an attribute without thinking about what it contains.
ALLOWED_ATTRIBUTE_KEYS = frozenset({
    "skifer.trace_version",
    "environment",
    "run_id",
    "evidence_id",
    "session_id",
    "contract_id",
    "contract_version",
    "model_key",
    "metric_names",
    "decision",
    "status",
    "route",
    "provider",
    "model",
    "latency_seconds",
    "duration_seconds",
    "row_count",
    "sql_hash",
    "compute_type",
    "statement_id",
    "user_pseudonym",
})

MAX_ATTRIBUTES_PER_SPAN = 32
MAX_EVENTS_PER_SPAN = 64
MAX_ATTRIBUTE_VALUE_LENGTH = 256


@dataclass(frozen=True)
class TraceAttributePolicy:
    """What a span is allowed to carry, and how a user identity is handled."""

    allowed_keys: frozenset[str] = ALLOWED_ATTRIBUTE_KEYS
    max_attributes: int = MAX_ATTRIBUTES_PER_SPAN
    max_events: int = MAX_EVENTS_PER_SPAN
    max_value_length: int = MAX_ATTRIBUTE_VALUE_LENGTH
    user_identity: str = "omit"          # "omit" | "hmac"
    hmac_secret: Optional[str] = None

    def allows(self, key: str) -> bool:
        return key in self.allowed_keys

    def truncate(self, value: Scalar) -> Scalar:
        if isinstance(value, str) and len(value) > self.max_value_length:
            return value[: self.max_value_length]
        return value

    def pseudonymize(self, user_id: Optional[str]) -> Optional[str]:
        """Return a stable pseudonym, or nothing at all.

        With `user_identity="hmac"` but no secret configured, this returns None
        rather than a plain digest. An unkeyed hash of an email or an employee
        id is reversible by anyone who can guess the input — a rainbow table
        over a company directory is small — so omitting the identity is the only
        honest option, and the plan requires it.
        """
        if not user_id or self.user_identity != "hmac":
            return None
        if not self.hmac_secret:
            _warn_missing_hmac_secret_once()
            return None
        digest = hmac.new(
            self.hmac_secret.encode("utf-8"), user_id.encode("utf-8"), hashlib.sha256
        ).hexdigest()
        return digest[:32]


# The secret is read from the environment, never from config.yaml: a committed
# configuration file is the last place a pseudonymisation key should live, and
# putting it there would turn the whole mechanism into decoration.
TRACING_HMAC_SECRET_ENV = "SKIFER_TRACING_HMAC_SECRET"


def build_attribute_policy(config: Any = None, environ: Any = None) -> TraceAttributePolicy:
    """Build the redaction policy from configuration plus the environment."""
    import os as _os

    source = _os.environ if environ is None else environ
    identity = getattr(config, "user_identity", "omit") or "omit"
    return TraceAttributePolicy(
        user_identity=identity,
        hmac_secret=source.get(TRACING_HMAC_SECRET_ENV) or None,
    )


_MISSING_SECRET_WARNED = False


def _warn_missing_hmac_secret_once() -> None:
    global _MISSING_SECRET_WARNED
    if _MISSING_SECRET_WARNED:
        return
    _MISSING_SECRET_WARNED = True
    _LOGGER.warning(
        "Tracing user_identity is 'hmac' but no secret is configured; the user "
        "identity is omitted rather than hashed without a key. Set the secret to "
        "enable pseudonymised identities."
    )


def _is_scalar(value: object) -> bool:
    if value is None or isinstance(value, (str, bool, int)):
        return True
    return isinstance(value, float) and math.isfinite(value)


class _NoOpSpan:
    """Allocation-free shared span used by :class:`NoOpTracer`."""

    __slots__ = ()

    def set_attribute(self, key: str, value: Scalar) -> None:
        return None

    def add_event(
        self, name: str, attributes: Optional[Attributes] = None
    ) -> None:
        return None

    def record_exception(self, exc: Exception) -> None:
        return None

    def end(self, status: str = "OK") -> None:
        return None

    def __enter__(self) -> "_NoOpSpan":
        return self

    def __exit__(self, exc_type, exc, traceback) -> bool:
        return False


_NO_OP_SPAN = _NoOpSpan()


class NoOpTracer:
    """Default tracer with no IDs, state, attribute copies or span allocations."""

    __slots__ = ()

    def start_span(
        self, name: str, *, attributes: Optional[Attributes] = None
    ) -> Span:
        return _NO_OP_SPAN

    def current_trace_id(self) -> Optional[str]:
        return None


class InMemorySpan:
    """Mutable recorded span owned by an :class:`InMemoryTracer`."""

    def __init__(
        self,
        *,
        tracer: "InMemoryTracer",
        name: str,
        trace_context: TraceContext,
        span_id: str,
        parent_span_id: Optional[str],
        started_at: float,
        activation_token: Token,
    ) -> None:
        self.name = name
        self.trace_context = trace_context
        self.trace_id = trace_context.trace_id
        self.span_id = span_id
        self.parent_span_id = parent_span_id
        self.started_at = started_at
        self.ended_at: Optional[float] = None
        self.status = "UNSET"
        self.attributes: dict[str, Scalar] = {}
        self.events: list[SpanEvent] = []
        self.exceptions: list[RecordedException] = []
        self._tracer = tracer
        self._activation_token = activation_token
        self._ended = False

    @property
    def ended(self) -> bool:
        return self._ended

    @property
    def duration_seconds(self) -> Optional[float]:
        if self.ended_at is None:
            return None
        return max(0.0, self.ended_at - self.started_at)

    def set_attribute(self, key: str, value: Scalar) -> None:
        # Routed through the same validation as attributes passed at span
        # creation. Checking only the type here would leave set_attribute as an
        # unguarded side door into the span, which is the call instrumentation
        # code reaches for most.
        accepted = self._tracer._validated_attributes(
            self, {key: value}, source="span", budget=len(self.attributes)
        )
        if not accepted:
            return
        with self._tracer._lock:
            self.attributes.update(accepted)

    def add_event(
        self, name: str, attributes: Optional[Attributes] = None
    ) -> None:
        safe_attributes = self._tracer._validated_attributes(
            self, attributes, source="event"
        )
        event = SpanEvent(
            name=name,
            timestamp=self._tracer._safe_clock(),
            attributes=safe_attributes,
        )
        with self._tracer._lock:
            # A span with unbounded events is a memory leak in-process and an
            # oversized payload at the exporter; drop past the cap rather than
            # grow without limit.
            if len(self.events) >= self._tracer.attribute_policy.max_events:
                return
            self.events.append(event)

    def record_exception(self, exc: Exception) -> None:
        record = RecordedException(type_name=type(exc).__name__)
        with self._tracer._lock:
            self.exceptions.append(record)

    def end(self, status: str = "OK") -> None:
        self._tracer._end_span(self, status)

    def __enter__(self) -> "InMemorySpan":
        return self

    def __exit__(self, exc_type, exc, traceback) -> bool:
        if exc is not None:
            if isinstance(exc, Exception):
                self.record_exception(exc)
            self.end("ERROR")
        else:
            self.end()
        return False


class InMemoryTracer:
    """Thread-safe tracer retaining spans in stable start/event order.

    The ID generator accepts the requested hexadecimal length (32 for a trace,
    16 for a span).  Both it and the monotonic clock are injectable so tests do
    not depend on randomness or wall-clock adjustments.
    """

    def __init__(
        self,
        *,
        context: Optional[TraceContext] = None,
        id_generator: Optional[Callable[[int], str]] = None,
        clock: Optional[Callable[[], float]] = None,
        attribute_policy: Optional[TraceAttributePolicy] = None,
    ) -> None:
        self.spans: list[InMemorySpan] = []
        self.rejected_attributes: list[RejectedAttribute] = []
        self._id_generator = id_generator or _generate_w3c_id
        self._clock = clock or time.monotonic
        self._lock = threading.RLock()
        self._warned_rejections: set[str] = set()
        self._current_span: ContextVar[Optional[InMemorySpan]] = ContextVar(
            f"skifer_current_span_{id(self)}", default=None
        )
        self._span_by_id: dict[str, InMemorySpan] = {}
        self.attribute_policy = attribute_policy or TraceAttributePolicy()
        # Per-instance ContextVars, against the usual "declare at module level"
        # advice: two tracers must not see each other's active span, which a
        # shared variable would allow. The documented cost is that a Context
        # retains them, so tracers are meant to be long-lived (one per process),
        # not created per request.
        self._trace_context: ContextVar[TraceContext] = ContextVar(
            f"skifer_trace_context_{id(self)}",
            default=context or TraceContext(),
        )

    def start_span(
        self, name: str, *, attributes: Optional[Attributes] = None
    ) -> InMemorySpan:
        parent = self._live_ancestor(self._current_span.get())
        # The trace_id always comes from the parent — a trace must stay one
        # trace. The correlation IDs take the innermost context that was
        # explicitly set: inheriting them from the parent alone made a nested
        # `use_context` silently do nothing, which is what pushed the
        # publication code to overwrite its business run_id instead.
        from_parent = parent.trace_context if parent is not None else TraceContext()
        ambient = self._trace_context.get()
        trace_id = (
            from_parent.trace_id or ambient.trace_id or self._new_id(TRACE_ID_HEX_LENGTH)
        )
        trace_context = TraceContext(
            trace_id=trace_id,
            run_id=ambient.run_id or from_parent.run_id,
            session_id=ambient.session_id or from_parent.session_id,
            evidence_id=ambient.evidence_id or from_parent.evidence_id,
        )
        span_id = self._new_id(SPAN_ID_HEX_LENGTH)
        span = InMemorySpan.__new__(InMemorySpan)
        activation_token = self._current_span.set(span)
        InMemorySpan.__init__(
            span,
            tracer=self,
            name=name,
            trace_context=trace_context,
            span_id=span_id,
            parent_span_id=parent.span_id if parent is not None else None,
            started_at=self._safe_clock(),
            activation_token=activation_token,
        )
        with self._lock:
            self.spans.append(span)
            self._span_by_id[span.span_id] = span
        for key, value in self._validated_attributes(
            span, attributes, source="span"
        ).items():
            span.set_attribute(key, value)
        return span

    def current_trace_id(self) -> Optional[str]:
        span = self._current_span.get()
        if span is not None:
            return span.trace_id
        return self._trace_context.get().trace_id

    def current_context(self) -> TraceContext:
        """Return the active correlation context without exposing mutable state."""
        span = self._current_span.get()
        return span.trace_context if span is not None else self._trace_context.get()

    @contextmanager
    def use_context(self, context: TraceContext) -> Iterator[TraceContext]:
        """Attach correlation IDs to spans started within the current context."""
        token = self._trace_context.set(context)
        try:
            yield context
        finally:
            self._trace_context.reset(token)

    @property
    def unfinished_spans(self) -> tuple[InMemorySpan, ...]:
        """Expose spans whose caller forgot to end them, in start order."""
        with self._lock:
            return tuple(span for span in self.spans if not span.ended)

    def _new_id(self, hex_length: int) -> str:
        value = self._id_generator(hex_length)
        valid = is_valid_trace_id(value) if hex_length == TRACE_ID_HEX_LENGTH else is_valid_span_id(value)
        if not valid:
            raise ValueError(
                f"ID generator returned an invalid {hex_length}-character W3C identifier"
            )
        return value

    def _safe_clock(self) -> float:
        return self._clock()

    def _validated_attributes(
        self,
        span: InMemorySpan,
        attributes: Optional[Attributes],
        *,
        source: str,
        budget: int = 0,
    ) -> dict[str, Scalar]:
        if attributes is None:
            return {}
        accepted: dict[str, Scalar] = {}
        try:
            items = attributes.items()
        except Exception:
            self._warn_rejection_once()
            return accepted
        policy = self.attribute_policy
        for key, value in items:
            if not isinstance(key, str) or not _is_scalar(value):
                self._reject_attribute(span, key, value, source)
                continue
            if not policy.allows(key):
                # Not a typo guard: an unknown key is the shape a leak takes —
                # someone attaches `question` or `sql` to a span and nothing
                # else in the system objects.
                self._reject_attribute(span, key, value, source)
                continue
            if budget + len(accepted) >= policy.max_attributes:
                self._reject_attribute(span, key, value, source)
                continue
            accepted[key] = policy.truncate(value)
        return accepted

    def _reject_attribute(
        self,
        span: InMemorySpan,
        key: object,
        value: object,
        source: str,
    ) -> None:
        record = RejectedAttribute(
            span_id=span.span_id,
            key=key if isinstance(key, str) else "<non-string>",
            value_type=type(value).__name__,
            source=source,
        )
        with self._lock:
            self.rejected_attributes.append(record)
        self._warn_rejection_once()

    def _warn_rejection_once(self) -> None:
        with self._lock:
            if "invalid_attribute" in self._warned_rejections:
                return
            self._warned_rejections.add("invalid_attribute")
        _LOGGER.warning(
            "Tracing rejected a non-scalar or non-finite attribute; "
            "further warnings are suppressed."
        )

    def _end_span(self, span: InMemorySpan, status: str) -> None:
        with self._lock:
            if span._ended:
                return
            span.status = status
            span.ended_at = self._safe_clock()
            span._ended = True
        try:
            self._current_span.reset(span._activation_token)
        except (RuntimeError, ValueError):
            # Ending in a copied/different context must never leak a tracing
            # failure into business code. The recorded span is still complete.
            pass
        # Token reset is only correct for LIFO. Ending a parent before its child
        # restores a span that has already closed, and everything started
        # afterwards would hang off a span the backend has seen end — an
        # unrepresentable tree. Fall back to the nearest ancestor still open.
        active = self._current_span.get()
        live = self._live_ancestor(active)
        if live is not active:
            self._current_span.set(live)

    def _live_ancestor(self, span: Optional[InMemorySpan]) -> Optional[InMemorySpan]:
        """Nearest ancestor still open, or None when the whole chain has ended."""
        seen: set[str] = set()
        current = span
        while current is not None and current._ended:
            if current.span_id in seen:  # defensive: never loop on a cycle
                return None
            seen.add(current.span_id)
            current = self._span_by_id.get(current.parent_span_id or "")
        return current


@contextmanager
def span_scope(
    tracer: Tracer,
    name: str,
    *,
    attributes: Optional[Attributes] = None,
) -> Iterator[Span]:
    """Start and reliably end a span without altering business exceptions."""
    with configured_span_scope(tracer, name, attributes=attributes) as span:
        yield span


@contextmanager
def configured_span_scope(
    tracer: Tracer,
    name: str,
    *,
    attributes: Optional[Attributes] = None,
    required: bool = False,
) -> Iterator[Span]:
    """Run a best-effort span, or fail on tracing errors when explicitly required."""
    try:
        span = tracer.start_span(name, attributes=attributes)
    except Exception as exc:
        _warn_tracing_failure_once(exc)
        if required:
            raise
        span = _NO_OP_SPAN
    try:
        yield span
    except Exception as exc:
        try:
            span.record_exception(exc)
        except Exception as tracing_exc:
            _warn_tracing_failure_once(tracing_exc)
        try:
            span.end("ERROR")
        except Exception as tracing_exc:
            _warn_tracing_failure_once(tracing_exc)
        raise
    else:
        try:
            span.end()
        except Exception as exc:
            _warn_tracing_failure_once(exc)
            if required:
                raise


def set_span_attribute(
    span: Span,
    key: str,
    value: Scalar,
    *,
    required: bool = False,
) -> None:
    """Set one attribute without allowing exporter failures to affect business code."""
    try:
        span.set_attribute(key, value)
    except Exception as exc:
        _warn_tracing_failure_once(exc)
        if required:
            raise


@contextmanager
def trace_context_scope(
    tracer: Tracer,
    context: TraceContext,
    *,
    required: bool = False,
) -> Iterator[TraceContext]:
    """Propagate correlation IDs when supported by the concrete tracer.

    Exporter adapters are not required to expose ``use_context``. In that case
    the business operation proceeds without internal correlation state.
    """
    token = _CORRELATION_CONTEXT.set(context)
    try:
        use_context = getattr(tracer, "use_context", None)
        if use_context is None:
            yield context
            return
        try:
            manager = use_context(context)
            manager.__enter__()
        except Exception as exc:
            _warn_tracing_failure_once(exc)
            if required:
                raise
            yield context
            return
        try:
            yield context
        except Exception:
            try:
                manager.__exit__(*__import__("sys").exc_info())
            except Exception as tracing_exc:
                _warn_tracing_failure_once(tracing_exc)
            raise
        else:
            try:
                manager.__exit__(None, None, None)
            except Exception as exc:
                _warn_tracing_failure_once(exc)
                if required:
                    raise
    finally:
        _CORRELATION_CONTEXT.reset(token)


def current_trace_context(tracer: Tracer) -> TraceContext:
    """Read the active correlation context without trusting exporter code."""
    ambient = _CORRELATION_CONTEXT.get()
    getter = getattr(tracer, "current_context", None)
    if getter is not None:
        try:
            context = getter()
            if isinstance(context, TraceContext):
                return TraceContext(
                    trace_id=context.trace_id or ambient.trace_id,
                    run_id=context.run_id or ambient.run_id,
                    session_id=context.session_id or ambient.session_id,
                    evidence_id=context.evidence_id or ambient.evidence_id,
                )
        except Exception as exc:
            _warn_tracing_failure_once(exc)
    try:
        return TraceContext(
            trace_id=tracer.current_trace_id() or ambient.trace_id,
            run_id=ambient.run_id,
            session_id=ambient.session_id,
            evidence_id=ambient.evidence_id,
        )
    except Exception as exc:
        _warn_tracing_failure_once(exc)
        return ambient


def _warn_tracing_failure_once(exc: Exception) -> None:
    """Log the first tracing failure process-wide and suppress repetitions."""
    global _FAILURE_WARNING_EMITTED
    with _FAILURE_WARNING_LOCK:
        if _FAILURE_WARNING_EMITTED:
            return
        _FAILURE_WARNING_EMITTED = True
    _LOGGER.warning(
        "Tracing failed; business execution continues and further warnings are suppressed (%s).",
        type(exc).__name__,
    )


__all__ = [
    "Attributes",
    "InMemorySpan",
    "InMemoryTracer",
    "NoOpTracer",
    "RecordedException",
    "RejectedAttribute",
    "Scalar",
    "Span",
    "SpanEvent",
    "TRACE_FORMAT_VERSION",
    "TraceContext",
    "Tracer",
    "configured_span_scope",
    "current_trace_context",
    "is_valid_span_id",
    "is_valid_trace_id",
    "new_trace_id",
    "set_span_attribute",
    "span_scope",
    "trace_context_scope",
]
