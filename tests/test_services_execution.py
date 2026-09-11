"""Spark-free tests for the transport-neutral execution service."""

from __future__ import annotations

import threading
from datetime import date, datetime, timezone
from decimal import Decimal
import json

import pytest

from skifer.observability.certification_store import RunEvent, StoredCheckResult
from skifer.observability.checks import (
    CheckResult,
    CheckStatus,
    ContractScope,
    DataQualityError,
    NullCheck,
)
from skifer.observability.monitor import MonitorReport
from skifer.observability.tracing import TraceContext
from skifer.services import (
    ExecutionService,
    JobConflict,
    ResultView,
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


class FakeFrame:
    def __init__(self, rows, dtypes=None):
        self.rows = list(rows)
        self.dtypes = dtypes or []

    def count(self):
        return len(self.rows)

    def limit(self, limit):
        return FakeFrame(self.rows[:limit], self.dtypes)

    def collect(self):
        return self.rows


class FakeBackend:
    def __init__(self, frame=None):
        self.frame = frame or FakeFrame([])
        self.queries = []

    def sql(self, query):
        self.queries.append(query)
        return self.frame

    def read_table(self, fqn):
        self.queries.append(f"READ {fqn}")
        return self.frame


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
        frame=None,
        certification_store=None,
        monitor=None,
        run_error=None,
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
        self.preview_frame = frame or FakeFrame([])
        self.backend = FakeBackend(self.preview_frame)
        self.certification_store = certification_store
        self.monitor = monitor
        self.run_error = run_error

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
        if self.run_error is not None:
            raise self.run_error
        return run_id

    def full_refresh(self, target_layer, target_table_name, run_id=None):
        return run_id

    def get_target_schema(self, layer):
        return f"{layer}{self.schema_suffix}" if self.schema_suffix else layer

    def _build_fqn(self, schema, table):
        return f"{self.db}.{schema}.{table}" if self.db else f"{schema}.{table}"

    def process_schema(self, _schema_dict):
        return self.preview_frame


class FakeMonitor:
    def __init__(self, report):
        self.report = report
        self.history = None

    def check_from_schema(self, _fqn, _schema_dict, raise_on_critical=False):
        return self.report


class FakeCertificationStore:
    def __init__(self, state="PROMOTED", quarantine_rows=()):
        self.state = state
        self.quarantine_rows = list(quarantine_rows)
        self.quarantine_limits = []

    def get_run(self, run_id):
        return RunEvent(
            event_id=f"{run_id}:{self.state}",
            run_id=run_id,
            dataset="cat.gold.orders_jdoe",
            state=self.state,
            contract_id="orders",
            contract_version="1.0.0",
            definition_hash="hash",
            occurred_at=datetime(2026, 9, 11, tzinfo=timezone.utc),
            target_fqn="cat.gold_jdoe.orders",
            quarantine_fqn=(
                "cat._skifer_quarantine.orders_snapshot"
                if self.state == "QUARANTINED"
                else None
            ),
        )

    def get_check_results(self, run_id):
        return [
            StoredCheckResult(
                event_id=f"{run_id}:NullCheck:0",
                run_id=run_id,
                check_type="NullCheck",
                scope=ContractScope.ROW,
                severity="critical",
                status=(
                    CheckStatus.FAIL
                    if self.state == "QUARANTINED"
                    else CheckStatus.PASS
                ),
                actual_value="1" if self.state == "QUARANTINED" else "0",
                expected_value="0",
                message="checked",
            )
        ]

    def read_quarantine(self, dataset, limit):
        self.quarantine_limits.append((dataset, limit))
        return FakeFrame(self.quarantine_rows[:limit], [("id", "bigint")])


class FakeCertificationStoreWithoutQuarantine(FakeCertificationStore):
    read_quarantine = None


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


def test_result_unknown_job_not_found():
    service = ExecutionService(engine_factory=lambda _config, _env: FakeEngine())

    with pytest.raises(ResourceNotFound):
        service.result(_context(SCOPE_EXECUTE_RUN), "nope")


def test_result_running_job_refused():
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

    with pytest.raises(InvalidRequest, match="job still running"):
        service.result(ctx, job_id)

    release.set()
    _wait_for_state(service, ctx, job_id, {"succeeded"})


def test_preview_rows_json_native_types(tmp_path):
    schema_path = tmp_path / "preview.yaml"
    schema_path.write_text("tables: []\n")
    timestamp = datetime(2026, 9, 11, 12, 30, tzinfo=timezone.utc)
    frame = FakeFrame(
        [
            {
                "amount": Decimal("12.30"),
                "day": date(2026, 9, 11),
                "created_at": timestamp,
                "missing": None,
            }
        ],
        [
            ("amount", "decimal(10,2)"),
            ("day", "date"),
            ("created_at", "timestamp"),
            ("missing", "string"),
        ],
    )
    service = ExecutionService(
        engine_factory=lambda _config, _env: FakeEngine(frame=frame)
    )
    ctx = _context(SCOPE_EXECUTE_RUN)
    service.connect(ctx)

    job_id = service.submit(ctx, "preview", str(schema_path))
    _wait_for_state(service, ctx, job_id, {"succeeded"})
    result = service.result(ctx, job_id)

    assert isinstance(result, ResultView)
    assert result.rows == (
        {
            "amount": "12.30",
            "day": "2026-09-11",
            "created_at": "2026-09-11T12:30:00+00:00",
            "missing": None,
        },
    )
    assert result.schema[0] == {"name": "amount", "type": "decimal(10,2)"}
    json.dumps(result.to_dict())


def test_preview_total_exact_and_rows_bounded(tmp_path):
    schema_path = tmp_path / "preview.yaml"
    schema_path.write_text("tables: []\n")
    frame = FakeFrame([{"id": index} for index in range(5)], [("id", "bigint")])
    service = ExecutionService(
        engine_factory=lambda _config, _env: FakeEngine(frame=frame)
    )
    ctx = _context(SCOPE_EXECUTE_RUN)
    service.connect(ctx)

    job_id = service.submit(ctx, "preview", str(schema_path), {"limit": 2})
    _wait_for_state(service, ctx, job_id, {"succeeded"})
    result = service.result(ctx, job_id)

    assert result.total == 5
    assert len(result.rows) == 2


def test_preview_rows_capped_at_hard_max(tmp_path):
    schema_path = tmp_path / "preview.yaml"
    schema_path.write_text("tables: []\n")
    frame = FakeFrame([{"id": index} for index in range(1005)], [("id", "bigint")])
    service = ExecutionService(
        engine_factory=lambda _config, _env: FakeEngine(frame=frame)
    )
    ctx = _context(SCOPE_EXECUTE_RUN)
    service.connect(ctx)

    job_id = service.submit(ctx, "preview", str(schema_path), {"limit": 5000})
    _wait_for_state(service, ctx, job_id, {"succeeded"})
    result = service.result(ctx, job_id)

    assert result.total == 1005
    assert len(result.rows) == 1000


def test_check_returns_monitor_report_summary(tmp_path):
    schema_path = tmp_path / "check.yaml"
    schema_path.write_text("tables: []\n")
    report = MonitorReport("cat.gold_jdoe.orders", [])
    engine = FakeEngine(monitor=FakeMonitor(report))
    service = ExecutionService(engine_factory=lambda _config, _env: engine)
    ctx = _context(SCOPE_EXECUTE_RUN)
    service.connect(ctx)

    job_id = service.submit(
        ctx,
        "check",
        str(schema_path),
        {"target_layer": "gold", "target_table": "orders"},
    )
    _wait_for_state(service, ctx, job_id, {"succeeded"})
    result = service.result(ctx, job_id)

    assert result.monitor_report is not None
    assert result.monitor_report["status"] == "PASS"
    assert result.monitor_report["total_checks"] == 0
    assert result.monitor_report["passed"] == 0
    assert result.rows == ()


def test_run_publication_decision_promoted():
    store = FakeCertificationStore(state="PROMOTED")
    engine = FakeEngine(
        frame=FakeFrame([{"id": 1}], [("id", "bigint")]),
        certification_store=store,
    )
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
    result = service.result(ctx, job_id)

    assert result.publication_decision == "PROMOTED"
    assert result.rows == ({"id": 1},)
    assert store.get_run(job_id).run_id == job_id


def test_run_quarantined_bounded_read():
    report = MonitorReport(
        "cat._skifer_staging.orders",
        [
            CheckResult(
                NullCheck(table="cat._skifer_staging.orders", column="id"),
                status=CheckStatus.FAIL,
                actual_value=1,
                expected_value=0,
                message="null id",
                severity="critical",
            )
        ],
    )
    store = FakeCertificationStore(
        state="QUARANTINED",
        quarantine_rows=[{"id": index} for index in range(150)],
    )
    engine = FakeEngine(
        certification_store=store,
        run_error=DataQualityError(report),
    )
    service = ExecutionService(engine_factory=lambda _config, _env: engine)
    ctx = _context(SCOPE_EXECUTE_RUN)
    service.connect(ctx)

    job_id = service.submit(
        ctx,
        "run",
        "schema.yaml",
        {"target_layer": "gold", "target_table": "orders", "limit": 500},
    )
    _wait_for_state(service, ctx, job_id, {"succeeded"})
    result = service.result(ctx, job_id)

    assert result.publication_decision == "QUARANTINED"
    assert result.rows == ()
    assert result.total == 0
    assert result.monitor_report["status"] == "CRITICAL"
    assert result.quarantine is not None
    assert len(result.quarantine["rows"]) == 100
    assert result.quarantine["truncated"] is True
    assert store.quarantine_limits == [("cat.gold_jdoe.orders", 101)]


def test_run_quarantine_uses_backend_when_certification_store_is_metadata_only():
    report = MonitorReport("cat._skifer_staging.orders", [])
    store = FakeCertificationStoreWithoutQuarantine(state="QUARANTINED")
    engine = FakeEngine(
        frame=FakeFrame([{"id": index} for index in range(120)], [("id", "bigint")]),
        certification_store=store,
        run_error=DataQualityError(report),
    )
    service = ExecutionService(engine_factory=lambda _config, _env: engine)
    ctx = _context(SCOPE_EXECUTE_RUN)
    service.connect(ctx)

    job_id = service.submit(
        ctx,
        "run",
        "schema.yaml",
        {"target_layer": "gold", "target_table": "orders", "limit": 500},
    )
    _wait_for_state(service, ctx, job_id, {"succeeded"})
    result = service.result(ctx, job_id)

    assert result.quarantine is not None
    assert len(result.quarantine["rows"]) == 100
    assert result.quarantine["truncated"] is True
    assert engine.backend.queries == [
        "READ cat._skifer_quarantine.orders_snapshot"
    ]


def test_result_requires_execute_scope():
    service = ExecutionService(engine_factory=lambda _config, _env: FakeEngine())

    with pytest.raises(ScopeDenied):
        service.result(_context(), "nope")


def test_failed_and_cancelled_jobs_return_empty_result_views():
    ctx = _context(SCOPE_EXECUTE_RUN)
    failed_service = ExecutionService(
        engine_factory=lambda _config, _env: FakeEngine(
            run_error=RuntimeError("backend unavailable")
        )
    )
    failed_service.connect(ctx)
    failed_id = failed_service.submit(
        ctx,
        "run",
        "schema.yaml",
        {"target_layer": "gold", "target_table": "orders"},
    )
    _wait_for_state(failed_service, ctx, failed_id, {"failed"})

    failed = failed_service.result(ctx, failed_id)
    assert failed.state == "failed"
    assert failed.rows == ()
    assert failed.total == 0
    assert failed.error == "RuntimeError: backend unavailable"

    release = threading.Event()
    cancelled_service = ExecutionService(
        engine_factory=lambda _config, _env: FakeEngine(run_release=release)
    )
    cancelled_service.connect(ctx)
    cancelled_id = cancelled_service.submit(
        ctx,
        "run",
        "schema.yaml",
        {"target_layer": "gold", "target_table": "orders"},
    )
    cancelled_service.cancel(ctx, cancelled_id)

    cancelled = cancelled_service.result(ctx, cancelled_id)
    assert cancelled.state == "cancelled"
    assert cancelled.rows == ()
    assert cancelled.total == 0
    release.set()
