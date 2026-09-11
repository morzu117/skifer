"""Spark-free tests for the transport-neutral execution service."""

from __future__ import annotations

import threading

import pytest

from skifer.observability.tracing import TraceContext
from skifer.services import (
    ExecutionService,
    JobConflict,
    SessionView,
)
from skifer.services.context import (
    InvalidRequest,
    RequestContext,
    ResourceNotFound,
    SCOPE_EXECUTE_RUN,
    ScopeDenied,
)
from skifer.services.execution import NoActiveSession


def _context(*scopes: str) -> RequestContext:
    return RequestContext(
        subject="local-user",
        scopes=frozenset(scopes),
        consumer_class="local",
        trace_context=TraceContext(),
    )


class FakeSpark:
    def __init__(self, engine=None):
        self.engine = engine
        self.stopped = False
        self.sparkContext = FakeSparkContext()

    def stop(self):
        self.stopped = True
        if self.engine is not None:
            self.engine.closed = True


class FakeSparkContext:
    def __init__(self):
        self.groups: list[tuple[str, str, bool | None]] = []
        self.cancelled: list[str] = []

    def setJobGroup(self, job_id, description, interruptOnCancel=None):
        self.groups.append((job_id, description, interruptOnCancel))

    def cancelJobGroup(self, job_id):
        self.cancelled.append(job_id)


class FakeContext:
    def __init__(self, is_production=False):
        self.is_production = is_production


class FakeEngine:
    def __init__(
        self,
        *,
        mode="local",
        env="DEV",
        db="cat",
        current_user="jdoe",
        schema_suffix="_jdoe",
        is_production=False,
        run_release: threading.Event | None = None,
    ):
        self._spark_mode = mode
        self.env = env
        self.db = db
        self.current_user = current_user
        self.schema_suffix = schema_suffix
        self.context = FakeContext(is_production=is_production)
        self.closed = False
        self.spark = FakeSpark(self)
        self.default_params = {}
        self.run_release = run_release
        self.run_started = threading.Event()
        self.run_calls: list[dict] = []

    def run_from_yaml(self, path, target_layer, target_table_name=None, params=None, run_id=None):
        self.run_started.set()
        self.run_calls.append(
            {
                "path": path,
                "target_layer": target_layer,
                "target_table_name": target_table_name,
                "params": params,
                "run_id": run_id,
            }
        )
        if self.run_release is not None:
            self.run_release.wait(timeout=2.0)
        return run_id

    def full_refresh(self, target_layer, target_table_name, run_id=None):
        return run_id

    def get_target_schema(self, layer):
        return f"{layer}{self.schema_suffix}" if self.schema_suffix else layer

    def _build_fqn(self, schema, table):
        return f"{self.db}.{schema}.{table}" if self.db else f"{schema}.{table}"


def _wait_for_state(service, ctx, job_id, expected):
    deadline = threading.Event()
    for _ in range(100):
        status = service.status(ctx, job_id)
        if status.state in expected:
            return status
        deadline.wait(0.01)
    raise AssertionError(f"job {job_id} did not reach {expected}")


def test_connect_requires_execute_scope():
    service = ExecutionService(engine_factory=lambda _config, _env: FakeEngine())

    with pytest.raises(ScopeDenied):
        service.connect(_context())


def test_connect_returns_ready_view():
    service = ExecutionService(engine_factory=lambda _config, _env: FakeEngine())

    view = service.connect(_context(SCOPE_EXECUTE_RUN))

    assert isinstance(view, SessionView)
    assert view.state == "ready"
    assert view.mode == "local"
    assert view.env == "DEV"
    assert view.catalog == "cat"
    assert view.user == "jdoe"
    assert view.sandbox_suffix == "_jdoe"
    assert view.is_production is False
    assert view.cause is None
    assert view.to_dict() == {
        "session_id": view.session_id,
        "mode": "local",
        "env": "DEV",
        "catalog": "cat",
        "user": "jdoe",
        "sandbox_suffix": "_jdoe",
        "is_production": False,
        "state": "ready",
        "cause": None,
        "config_path": None,
        "force_env": None,
    }


def test_connect_failure_is_exposed_not_raised():
    def fail(_config_path, _force_env):
        raise RuntimeError("no catalog")

    service = ExecutionService(engine_factory=fail)

    view = service.connect(_context(SCOPE_EXECUTE_RUN))

    assert view.state == "failed"
    assert view.cause.startswith("RuntimeError: no catalog")
    assert view.env == ""
    assert view.catalog is None
    assert view.user == ""
    assert view.sandbox_suffix == ""
    assert view.is_production is False


def test_session_recreated_on_config_change():
    engines: list[FakeEngine] = []

    def factory(_config_path, _force_env):
        engine = FakeEngine()
        engines.append(engine)
        return engine

    service = ExecutionService(engine_factory=factory)
    ctx = _context(SCOPE_EXECUTE_RUN)

    first = service.connect(ctx, config_path="a")
    second = service.connect(ctx, config_path="b")

    assert second.session_id != first.session_id
    assert engines[0].closed is True
    assert service.session(ctx) == second


def test_same_config_reuses_session():
    calls = 0

    def factory(_config_path, _force_env):
        nonlocal calls
        calls += 1
        return FakeEngine()

    service = ExecutionService(engine_factory=factory)
    ctx = _context(SCOPE_EXECUTE_RUN)

    first = service.connect(ctx, config_path="a", force_env="DEV")
    second = service.connect(ctx, config_path="a", force_env="DEV")

    assert second.session_id == first.session_id
    assert calls == 1


def test_close_is_idempotent_and_stops_local_only():
    local_engine = FakeEngine(mode="local")
    service = ExecutionService(engine_factory=lambda _config, _env: local_engine)
    ctx = _context(SCOPE_EXECUTE_RUN)

    service.connect(ctx)
    service.close(ctx)
    service.close(ctx)

    assert local_engine.spark.stopped is True

    provided_engine = FakeEngine(mode="provided")
    service = ExecutionService(engine_factory=lambda _config, _env: provided_engine)

    service.connect(ctx)
    service.close(ctx)

    assert provided_engine.spark.stopped is False


def test_session_ttl_expiry():
    now = 0.0
    engine = FakeEngine()

    def clock():
        return now

    service = ExecutionService(
        engine_factory=lambda _config, _env: engine,
        clock=clock,
        session_ttl_seconds=10.0,
    )
    ctx = _context(SCOPE_EXECUTE_RUN)

    service.connect(ctx)
    now = 11.0

    assert service.session(ctx) is None
    assert engine.closed is True


def test_submit_requires_active_session():
    service = ExecutionService(engine_factory=lambda _config, _env: FakeEngine())

    with pytest.raises(NoActiveSession):
        service.submit(
            _context(SCOPE_EXECUTE_RUN),
            "run",
            "schema.yaml",
            {"target_layer": "gold", "target_table": "orders"},
        )


def test_submit_requires_execute_scope():
    service = ExecutionService(engine_factory=lambda _config, _env: FakeEngine())

    with pytest.raises(ScopeDenied):
        service.submit(
            _context(),
            "run",
            "schema.yaml",
            {"target_layer": "gold", "target_table": "orders"},
        )


def test_invalid_kind_refused():
    service = ExecutionService(engine_factory=lambda _config, _env: FakeEngine())
    service.connect(_context(SCOPE_EXECUTE_RUN))

    with pytest.raises(InvalidRequest):
        service.submit(_context(SCOPE_EXECUTE_RUN), "delete", "schema.yaml")


def test_one_active_job_refused_cleanly():
    release = threading.Event()
    engine = FakeEngine(run_release=release)
    service = ExecutionService(engine_factory=lambda _config, _env: engine)
    ctx = _context(SCOPE_EXECUTE_RUN)
    service.connect(ctx)

    job_id = service.submit(
        ctx,
        "run",
        "schema.yaml",
        {"target_layer": "gold", "target_table": "orders"},
    )

    with pytest.raises(JobConflict, match=job_id):
        service.submit(
            ctx,
            "run",
            "schema.yaml",
            {"target_layer": "gold", "target_table": "orders2"},
        )

    release.set()
    assert _wait_for_state(service, ctx, job_id, {"succeeded"}).state == "succeeded"


def test_cancellation_flips_state_and_calls_cancel():
    release = threading.Event()
    engine = FakeEngine(run_release=release)
    service = ExecutionService(engine_factory=lambda _config, _env: engine)
    ctx = _context(SCOPE_EXECUTE_RUN)
    service.connect(ctx)

    job_id = service.submit(
        ctx,
        "run",
        "schema.yaml",
        {"target_layer": "gold", "target_table": "orders"},
    )
    assert engine.run_started.wait(timeout=1.0)

    status = service.cancel(ctx, job_id)

    assert status.state == "cancelled"
    assert engine.spark.sparkContext.cancelled == [job_id]
    release.set()
    assert service.status(ctx, job_id).state == "cancelled"


def test_cancel_finished_job_is_noop():
    engine = FakeEngine()
    service = ExecutionService(engine_factory=lambda _config, _env: engine)
    ctx = _context(SCOPE_EXECUTE_RUN)
    service.connect(ctx)
    job_id = service.submit(
        ctx,
        "run",
        "schema.yaml",
        {"target_layer": "gold", "target_table": "orders"},
    )
    assert _wait_for_state(service, ctx, job_id, {"succeeded"}).state == "succeeded"

    assert service.cancel(ctx, job_id).state == "succeeded"


def test_logs_cursor_advances():
    engine = FakeEngine()
    service = ExecutionService(engine_factory=lambda _config, _env: engine)
    ctx = _context(SCOPE_EXECUTE_RUN)
    service.connect(ctx)
    job_id = service.submit(
        ctx,
        "run",
        "schema.yaml",
        {"target_layer": "gold", "target_table": "orders"},
    )
    _wait_for_state(service, ctx, job_id, {"succeeded"})

    first = service.logs(ctx, job_id, after=0)
    second = service.logs(ctx, job_id, after=first.next_offset)

    assert first.next_offset == 3
    assert [entry["message"] for entry in first.entries] == [
        "submitted run job",
        "started",
        "succeeded",
    ]
    assert second.entries == ()
    assert second.next_offset == first.next_offset


def test_status_unknown_job_not_found():
    service = ExecutionService(engine_factory=lambda _config, _env: FakeEngine())

    with pytest.raises(ResourceNotFound):
        service.status(_context(SCOPE_EXECUTE_RUN), "nope")


def test_close_cancels_active_job():
    release = threading.Event()
    engine = FakeEngine(run_release=release)
    service = ExecutionService(engine_factory=lambda _config, _env: engine)
    ctx = _context(SCOPE_EXECUTE_RUN)
    service.connect(ctx)
    job_id = service.submit(
        ctx,
        "run",
        "schema.yaml",
        {"target_layer": "gold", "target_table": "orders"},
    )
    assert engine.run_started.wait(timeout=1.0)

    service.close(ctx)

    assert service.status(ctx, job_id).state == "cancelled"
    assert engine.closed is True
    release.set()


def test_run_job_run_id_equals_pipeline_run_id():
    engine = FakeEngine()
    service = ExecutionService(engine_factory=lambda _config, _env: engine)
    ctx = _context(SCOPE_EXECUTE_RUN)
    service.connect(ctx)

    job_id = service.submit(
        ctx,
        "run",
        "schema.yaml",
        {"target_layer": "gold", "target_table": "orders"},
    )
    status = _wait_for_state(service, ctx, job_id, {"succeeded"})

    assert status.job_id == job_id
    assert status.run_id == job_id
    assert engine.run_calls[0]["run_id"] == job_id
