"""Spark-free tests for the transport-neutral execution service."""

from __future__ import annotations

import pytest

from skifer.observability.tracing import TraceContext
from skifer.services import ExecutionService, SessionView
from skifer.services.context import RequestContext, SCOPE_EXECUTE_RUN, ScopeDenied


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

    def stop(self):
        self.stopped = True
        if self.engine is not None:
            self.engine.closed = True


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
    ):
        self._spark_mode = mode
        self.env = env
        self.db = db
        self.current_user = current_user
        self.schema_suffix = schema_suffix
        self.context = FakeContext(is_production=is_production)
        self.closed = False
        self.spark = FakeSpark(self)


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
