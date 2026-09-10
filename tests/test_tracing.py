import asyncio
from concurrent.futures import ThreadPoolExecutor
import logging
from pathlib import Path
import subprocess
import sys
import threading

import pytest

from skifer.observability import tracing
from skifer.observability.tracing import (
    InMemoryTracer,
    NoOpTracer,
    TraceContext,
    is_valid_span_id,
    is_valid_trace_id,
    span_scope,
)


def test_nested_spans_capture_stable_parentage_and_order():
    tracer = InMemoryTracer()

    with tracer.start_span("skifer.semantic.query") as root:
        with tracer.start_span("skifer.semantic.compile") as child:
            with tracer.start_span("skifer.sql.execute") as grandchild:
                grandchild.add_event("execution.started", {"attempt": 1})

    assert tracer.spans == [root, child, grandchild]
    assert root.parent_span_id is None
    assert child.parent_span_id == root.span_id
    assert grandchild.parent_span_id == child.span_id
    assert {span.trace_id for span in tracer.spans} == {root.trace_id}
    assert [event.name for event in grandchild.events] == ["execution.started"]
    assert root.status == child.status == grandchild.status == "OK"
    assert tracer.current_trace_id() is None


def test_context_manager_records_error_then_reraises_same_exception():
    tracer = InMemoryTracer()
    failure = RuntimeError("business failure")

    with pytest.raises(RuntimeError, match="business failure") as raised:
        with span_scope(tracer, "skifer.pipeline.run"):
            raise failure

    span = tracer.spans[0]
    assert raised.value is failure
    assert span.status == "ERROR"
    assert span.ended
    assert [record.type_name for record in span.exceptions] == ["RuntimeError"]


def test_returned_span_context_manager_reraises_business_exception_unchanged():
    tracer = InMemoryTracer()
    failure = ValueError("unchanged")

    with pytest.raises(ValueError, match="unchanged") as raised:
        with tracer.start_span("skifer.pipeline.run"):
            raise failure

    assert raised.value is failure
    assert tracer.spans[0].status == "ERROR"
    assert [item.type_name for item in tracer.spans[0].exceptions] == ["ValueError"]


def test_span_scope_swallows_tracer_failures_without_masking_business_error():
    class FailingSpan:
        def record_exception(self, exc):
            raise OSError("export record failed")

        def end(self, status="OK"):
            raise OSError("export end failed")

    class FailingTracer:
        def start_span(self, name, *, attributes=None):
            return FailingSpan()

        def current_trace_id(self):
            return None

    failure = RuntimeError("business error wins")

    with pytest.raises(RuntimeError, match="business error wins") as raised:
        with span_scope(FailingTracer(), "skifer.pipeline.run"):
            raise failure

    assert raised.value is failure


def test_two_threads_do_not_cross_parentage():
    tracer = InMemoryTracer()
    barrier = threading.Barrier(2)

    def worker(index):
        with tracer.start_span("skifer.semantic.query") as root:
            barrier.wait(timeout=5)
            with tracer.start_span("skifer.semantic.compile") as child:
                return index, root, child

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(worker, range(2)))

    (_, root_a, child_a), (_, root_b, child_b) = results
    assert root_a.parent_span_id is None
    assert root_b.parent_span_id is None
    assert child_a.parent_span_id == root_a.span_id
    assert child_b.parent_span_id == root_b.span_id
    assert child_a.parent_span_id != root_b.span_id
    assert child_b.parent_span_id != root_a.span_id
    assert root_a.trace_id != root_b.trace_id


def test_two_async_tasks_do_not_cross_parentage():
    tracer = InMemoryTracer()
    both_roots_open = asyncio.Event()
    roots_opened = 0
    lock = asyncio.Lock()

    async def worker(index):
        nonlocal roots_opened
        with tracer.start_span("skifer.semantic.query") as root:
            async with lock:
                roots_opened += 1
                if roots_opened == 2:
                    both_roots_open.set()
            await asyncio.wait_for(both_roots_open.wait(), timeout=5)
            await asyncio.sleep(0)
            with tracer.start_span("skifer.semantic.compile") as child:
                return index, root, child

    async def run_workers():
        return await asyncio.gather(worker(0), worker(1))

    results = asyncio.run(run_workers())
    (_, root_a, child_a), (_, root_b, child_b) = results
    assert root_a.parent_span_id is None
    assert root_b.parent_span_id is None
    assert child_a.parent_span_id == root_a.span_id
    assert child_b.parent_span_id == root_b.span_id
    assert root_a.trace_id != root_b.trace_id


def test_unfinished_span_is_visible_in_start_order():
    tracer = InMemoryTracer()
    forgotten = tracer.start_span("skifer.pipeline.run")

    assert tracer.unfinished_spans == (forgotten,)
    assert forgotten.status == "UNSET"
    assert forgotten.duration_seconds is None

    forgotten.end()
    assert tracer.unfinished_spans == ()


def test_invalid_attributes_are_rejected_retained_and_logged_once(caplog):
    tracer = InMemoryTracer()

    with caplog.at_level(logging.WARNING, logger=tracing.__name__):
        with tracer.start_span(
            "skifer.pipeline.run", attributes={"unsafe": ["secret"]}
        ) as span:
            span.set_attribute("also_unsafe", {"secret": "value"})
            span.add_event("ignored.value", {"unsafe_event": object()})

    assert span.attributes == {}
    assert span.events[0].attributes == {}
    assert [item.key for item in tracer.rejected_attributes] == [
        "unsafe",
        "also_unsafe",
        "unsafe_event",
    ]
    assert [item.value_type for item in tracer.rejected_attributes] == [
        "list",
        "dict",
        "object",
    ]
    warnings = [record for record in caplog.records if record.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "secret" not in warnings[0].getMessage()


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_non_finite_float_attribute_is_rejected_without_raising(value):
    tracer = InMemoryTracer()

    with tracer.start_span("skifer.pipeline.run") as span:
        span.set_attribute("duration", value)

    assert "duration" not in span.attributes
    assert tracer.rejected_attributes[0].value_type == "float"


def test_noop_tracer_reuses_one_span_without_ids_or_recording(monkeypatch):
    def forbidden_id_generation(_length):
        raise AssertionError("NoOpTracer generated an ID")

    monkeypatch.setattr(tracing, "_generate_w3c_id", forbidden_id_generation)
    tracer = NoOpTracer()
    attributes = {"run_id": "run-1"}

    first = tracer.start_span("skifer.pipeline.run", attributes=attributes)
    second = tracer.start_span("skifer.semantic.query")
    first.set_attribute("anything", object())
    first.add_event("anything", attributes)
    first.end()

    assert first is second
    assert not hasattr(tracer, "spans")
    assert not hasattr(first, "attributes")
    assert tracer.current_trace_id() is None


def test_tracing_module_imports_without_optional_tracing_sdks():
    project_root = Path(__file__).resolve().parents[1]
    script = """
import sys
assert 'mlflow' not in sys.modules
assert 'opentelemetry' not in sys.modules
import skifer.observability.tracing
assert 'mlflow' not in sys.modules
assert not any(name == 'opentelemetry' or name.startswith('opentelemetry.') for name in sys.modules)
"""

    completed = subprocess.run(
        [sys.executable, "-c", script],
        cwd=project_root,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr


def test_generated_ids_have_stable_w3c_format():
    tracer = InMemoryTracer()

    with tracer.start_span("skifer.pipeline.run") as span:
        assert is_valid_trace_id(span.trace_id)
        assert is_valid_span_id(span.span_id)
        assert len(span.trace_id) == 32
        assert len(span.span_id) == 16


def test_trace_context_propagates_all_correlation_ids():
    context = TraceContext(
        trace_id="1" * 32,
        run_id="run-123",
        session_id="session-456",
        evidence_id="evidence-789",
    )
    tracer = InMemoryTracer()

    with tracer.use_context(context):
        assert tracer.current_trace_id() == context.trace_id
        with tracer.start_span("skifer.semantic.query") as root:
            with tracer.start_span("skifer.semantic.compile") as child:
                pass

    assert root.trace_context == context
    assert child.trace_context == context
    assert tracer.current_trace_id() is None


def test_clock_and_id_generation_are_injectable_and_duration_is_monotonic():
    ids = iter(["1" * 32, "2" * 16])
    times = iter([10.0, 12.5])
    tracer = InMemoryTracer(
        id_generator=lambda _length: next(ids),
        clock=lambda: next(times),
    )

    with tracer.start_span("skifer.pipeline.run") as span:
        pass

    assert span.trace_id == "1" * 32
    assert span.span_id == "2" * 16
    assert span.duration_seconds == 2.5


# ---------------------------------------------------------------------------
# Parenting must never point at a closed span
# ---------------------------------------------------------------------------


def test_span_started_after_out_of_order_end_is_not_parented_to_a_closed_span():
    # Token reset is only correct for LIFO. Ending a parent before its child
    # restored an already-closed span as the active one, so everything started
    # afterwards hung off a span the backend had seen end.
    tracer = InMemoryTracer()

    parent = tracer.start_span("skifer.pipeline.run")
    child = tracer.start_span("skifer.schema.load")
    parent.end()
    child.end()
    later = tracer.start_span("skifer.sql.execute")
    later.end()

    assert later.parent_span_id is None
    closed_ids = {span.span_id for span in tracer.spans if span.ended}
    assert later.parent_span_id not in closed_ids


def test_nested_end_out_of_order_falls_back_to_the_nearest_open_ancestor():
    tracer = InMemoryTracer()

    root = tracer.start_span("skifer.pipeline.run")
    middle = tracer.start_span("skifer.transform.execute")
    leaf = tracer.start_span("skifer.sql.execute")

    middle.end()  # out of order: leaf is still open
    leaf.end()

    sibling = tracer.start_span("skifer.response.serialize")
    sibling.end()
    root.end()

    # root is the only ancestor still open when `sibling` starts.
    assert sibling.parent_span_id == root.span_id


def test_forgotten_span_stays_the_parent_but_is_reported_as_unfinished():
    # Inheriting from a span that was never ended is the semantically expected
    # result of using start_span() without a context manager; what must not
    # happen is that it goes unnoticed.
    tracer = InMemoryTracer()

    forgotten = tracer.start_span("skifer.transform.execute")
    later = tracer.start_span("skifer.sql.execute")

    assert later.parent_span_id == forgotten.span_id
    assert [span.name for span in tracer.unfinished_spans] == [
        "skifer.transform.execute",
        "skifer.sql.execute",
    ]
