"""Transport-neutral execution session service."""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable
from uuid import uuid4

from skifer.services.context import (
    HARD_MAX_QUERY_ROWS,
    InvalidRequest,
    RequestContext,
    ResourceNotFound,
    ResourceUnavailable,
    SCOPE_EXECUTE_RUN,
    require_scope,
)
from skifer.services.serialization import row_to_json


EngineFactory = Callable[[str | None, str | None], Any]
_VALID_KINDS = ("preview", "run", "full_refresh", "check")


class JobConflict(InvalidRequest):
    """A second job was submitted while the single v1 job slot is active."""


class NoActiveSession(ResourceUnavailable):
    """A job submission requires a ready execution session."""


@dataclass(frozen=True)
class SessionView:
    session_id: str
    mode: str
    env: str
    catalog: str | None
    user: str
    sandbox_suffix: str
    is_production: bool
    state: str
    cause: str | None = None
    config_path: str | None = None
    force_env: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "session_id": self.session_id,
            "mode": self.mode,
            "env": self.env,
            "catalog": self.catalog,
            "user": self.user,
            "sandbox_suffix": self.sandbox_suffix,
            "is_production": self.is_production,
            "state": self.state,
            "cause": self.cause,
            "config_path": self.config_path,
            "force_env": self.force_env,
        }


@dataclass
class _Job:
    job_id: str
    kind: str
    path: str
    params: dict[str, Any]
    state: str
    submitted_at: float
    started_at: float | None = None
    finished_at: float | None = None
    error: str | None = None
    logs: list[tuple[float, str, str]] = field(default_factory=list)
    result: Any | None = None
    cancel_requested: bool = False
    _thread: threading.Thread | None = None


@dataclass(frozen=True)
class JobStatusView:
    job_id: str
    run_id: str
    kind: str
    state: str
    submitted_at: float
    started_at: float | None
    finished_at: float | None
    error: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "run_id": self.run_id,
            "kind": self.kind,
            "state": self.state,
            "submitted_at": self.submitted_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "error": self.error,
        }


@dataclass(frozen=True)
class LogsView:
    job_id: str
    entries: tuple[dict[str, Any], ...]
    next_offset: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "entries": list(self.entries),
            "next_offset": self.next_offset,
        }


def _default_engine_factory(config_path: str | None, force_env: str | None):
    """Build a SkiferEngine wired for certified publication and history."""
    from skifer.core.core import SkiferEngine
    from skifer.observability.certification_store import SqliteCertificationStore
    from skifer.observability.history import SqliteHistoryStore
    from skifer.observability.monitor import DataMonitor

    store = SqliteCertificationStore()
    engine = SkiferEngine(
        config_path=config_path,
        force_env=force_env,
        certification_store=store,
    )
    engine.monitor = DataMonitor(engine.backend, history_store=SqliteHistoryStore())
    return engine


class ExecutionService:
    def __init__(
        self,
        *,
        engine_factory: EngineFactory | None = None,
        clock: Callable[[], float] = time.monotonic,
        session_ttl_seconds: float = 3600.0,
        job_ttl_seconds: float = 3600.0,
    ):
        self._engine_factory = engine_factory or _default_engine_factory
        self._clock = clock
        self._session_ttl = session_ttl_seconds
        self._job_ttl = job_ttl_seconds
        self._lock = threading.RLock()
        self._engine = None
        self._session: SessionView | None = None
        self._session_key: tuple[str | None, str | None] | None = None
        self._last_access: float = 0.0
        self._job = None

    def connect(
        self,
        ctx: RequestContext,
        config_path: str | None = None,
        force_env: str | None = None,
    ) -> SessionView:
        require_scope(ctx, SCOPE_EXECUTE_RUN)
        key = (config_path, force_env)
        session_id = str(uuid4())

        with self._lock:
            self._sweep_ttl_locked()
            if (
                self._session is not None
                and self._session_key == key
                and self._session.state == "ready"
            ):
                self._last_access = self._clock()
                return self._session
            if (
                self._session is not None
                and self._session_key == key
                and self._session.state == "connecting"
            ):
                self._last_access = self._clock()
                return self._session
            if self._session is not None and self._session_key != key:
                self._close_locked()

            connecting = SessionView(
                session_id=session_id,
                mode="unknown",
                env="",
                catalog=None,
                user="",
                sandbox_suffix="",
                is_production=False,
                state="connecting",
                config_path=config_path,
                force_env=force_env,
            )
            self._engine = None
            self._session = connecting
            self._session_key = key
            self._last_access = self._clock()

        try:
            engine = self._engine_factory(config_path, force_env)
        except Exception as exc:
            failed = SessionView(
                session_id=session_id,
                mode="unknown",
                env="",
                catalog=None,
                user="",
                sandbox_suffix="",
                is_production=False,
                state="failed",
                cause=f"{type(exc).__name__}: {exc}"[:200],
                config_path=config_path,
                force_env=force_env,
            )
            with self._lock:
                if self._session is not None and self._session.session_id == session_id:
                    self._engine = None
                    self._session = failed
                    self._session_key = key
                    self._last_access = self._clock()
            return failed

        ready = self._view_from_engine(engine, session_id, config_path, force_env)
        with self._lock:
            if self._session is not None and self._session.session_id == session_id:
                self._engine = engine
                self._session = ready
                self._session_key = key
                self._last_access = self._clock()
            else:
                self._stop_engine_if_local(engine, ready.mode)
        return ready

    def session(self, ctx: RequestContext) -> SessionView | None:
        require_scope(ctx, SCOPE_EXECUTE_RUN)
        with self._lock:
            self._sweep_ttl_locked()
            if self._session is not None:
                self._last_access = self._clock()
            return self._session

    def close(self, ctx: RequestContext) -> None:
        require_scope(ctx, SCOPE_EXECUTE_RUN)
        with self._lock:
            self._close_locked()

    def submit(
        self,
        ctx: RequestContext,
        kind: str,
        path: str,
        params: dict | None = None,
    ) -> str:
        require_scope(ctx, SCOPE_EXECUTE_RUN)
        job_params = self._validate_submit(kind, path, params)
        job_id = str(uuid4())
        job = _Job(
            job_id=job_id,
            kind=kind,
            path=path,
            params=job_params,
            state="running",
            submitted_at=self._clock(),
        )

        with self._lock:
            self._sweep_ttl_locked()
            if (
                self._session is None
                or self._session.state != "ready"
                or self._engine is None
            ):
                raise NoActiveSession("A ready execution session is required to submit a job.")
            if self._job is not None and self._job.state == "running":
                raise JobConflict(
                    f"A job is already running (job_id={self._job.job_id}); "
                    "v1 allows one active job per project."
                )
            self._job = job
            self._append_log_locked(job, "info", f"submitted {kind} job")
            thread = threading.Thread(
                target=self._run_job,
                args=(job_id,),
                name=f"skifer-job-{job_id}",
                daemon=True,
            )
            job._thread = thread
            self._last_access = self._clock()

        thread.start()
        return job_id

    def status(self, ctx: RequestContext, job_id: str) -> JobStatusView:
        require_scope(ctx, SCOPE_EXECUTE_RUN)
        with self._lock:
            self._sweep_ttl_locked()
            job = self._get_job_locked(job_id)
            self._last_access = self._clock() if self._session is not None else self._last_access
            return self._status_view(job)

    def cancel(self, ctx: RequestContext, job_id: str) -> JobStatusView:
        require_scope(ctx, SCOPE_EXECUTE_RUN)
        with self._lock:
            self._sweep_ttl_locked()
            job = self._get_job_locked(job_id)
            if job.state == "running":
                self._cancel_job_locked(job, self._clock(), "cancelled")
            self._last_access = self._clock() if self._session is not None else self._last_access
            return self._status_view(job)

    def logs(self, ctx: RequestContext, job_id: str, after: int = 0) -> LogsView:
        require_scope(ctx, SCOPE_EXECUTE_RUN)
        if type(after) is not int or after < 0:
            raise InvalidRequest("Log cursor 'after' must be a non-negative integer.")
        with self._lock:
            self._sweep_ttl_locked()
            job = self._get_job_locked(job_id)
            entries = tuple(
                {"ts": ts, "level": level, "message": message}
                for ts, level, message in job.logs[after:]
            )
            self._last_access = self._clock() if self._session is not None else self._last_access
            return LogsView(job_id=job.job_id, entries=entries, next_offset=len(job.logs))

    def _view_from_engine(
        self,
        engine,
        session_id,
        config_path,
        force_env,
    ) -> SessionView:
        return SessionView(
            session_id=session_id,
            mode=getattr(engine, "_spark_mode", "unknown"),
            env=engine.env,
            catalog=engine.db,
            user=engine.current_user,
            sandbox_suffix=engine.schema_suffix or "",
            is_production=engine.context.is_production,
            state="ready",
            config_path=config_path,
            force_env=force_env,
        )

    def _close_locked(self) -> None:
        if self._job is not None and self._job.state == "running":
            self._cancel_job_locked(self._job, self._clock(), "cancelled by session close")
        if self._engine is not None:
            mode = self._session.mode if self._session is not None else "unknown"
            self._stop_engine_if_local(self._engine, mode)
        self._engine = None
        self._session = None
        self._session_key = None
        self._last_access = 0.0

    def _sweep_ttl_locked(self) -> None:
        now = self._clock()
        if (
            self._job is not None
            and self._job.state == "running"
            and now - self._job.submitted_at > self._job_ttl
        ):
            self._cancel_job_locked(self._job, now, "cancelled by job TTL")
        if self._session is None:
            return
        if now - self._last_access > self._session_ttl:
            self._close_locked()

    def _stop_engine_if_local(self, engine, mode: str) -> None:
        if mode != "local":
            return
        spark = getattr(engine, "spark", None)
        stop = getattr(spark, "stop", None)
        if callable(stop):
            stop()

    def _validate_submit(
        self,
        kind: str,
        path: str,
        params: dict | None,
    ) -> dict[str, Any]:
        if kind not in _VALID_KINDS:
            raise InvalidRequest(
                f"Invalid job kind {kind!r}; expected one of {', '.join(_VALID_KINDS)}."
            )
        if not isinstance(path, str) or not path:
            raise InvalidRequest("Job path must be non-empty text.")
        if params is None:
            job_params: dict[str, Any] = {}
        elif isinstance(params, dict):
            job_params = dict(params)
        else:
            raise InvalidRequest("Job params must be a mapping.")
        if kind in {"run", "full_refresh", "check"}:
            for key in ("target_layer", "target_table"):
                if not isinstance(job_params.get(key), str) or not job_params[key]:
                    raise InvalidRequest(f"Job kind {kind!r} requires params['{key}'].")
        run_params = job_params.get("run_params")
        if run_params is not None and not isinstance(run_params, dict):
            raise InvalidRequest("params['run_params'] must be a mapping when provided.")
        return job_params

    def _run_job(self, job_id: str) -> None:
        with self._lock:
            if self._job is None or self._job.job_id != job_id or self._job.state != "running":
                return
            job = self._job
            engine = self._engine
            if engine is None:
                job.state = "failed"
                job.finished_at = self._clock()
                job.error = "NoActiveSession: execution session closed before job start"
                self._append_log_locked(job, "error", job.error)
                return
            job.started_at = self._clock()
            self._append_log_locked(job, "info", "started")

        self._set_spark_job_group(engine, job)
        try:
            result = self._execute_job(engine, job)
        except Exception as exc:
            self._finish_job_failed_or_cancelled(job_id, exc)
            return

        with self._lock:
            current = self._get_current_job_locked(job_id)
            if current is None or current.state == "cancelled" or current.cancel_requested:
                return
            current.result = result
            current.state = "succeeded"
            current.finished_at = self._clock()
            self._append_log_locked(current, "info", "succeeded")

    def _execute_job(self, engine, job: _Job) -> Any:
        if job.kind == "run":
            returned = engine.run_from_yaml(
                job.path,
                job.params["target_layer"],
                job.params.get("target_table"),
                job.params.get("run_params"),
                run_id=job.job_id,
            )
            self._assert_returned_run_id(job, returned)
            return {"run_id": returned}
        if job.kind == "full_refresh":
            returned = engine.full_refresh(
                job.params["target_layer"],
                job.params["target_table"],
                run_id=job.job_id,
            )
            self._assert_returned_run_id(job, returned)
            return {"run_id": returned}
        if job.kind == "preview":
            from skifer.core.schema_loader import load_schema

            schema_dict = load_schema(
                job.path,
                params={**engine.default_params, **job.params.get("run_params", {})},
            )
            df = engine.process_schema(schema_dict)
            limit = job.params.get("limit", 100)
            if type(limit) is not int or limit < 1:
                raise InvalidRequest("params['limit'] must be a positive integer.")
            rows = df.limit(min(limit, HARD_MAX_QUERY_ROWS)).collect()
            return {"rows": [row_to_json(row, index) for index, row in enumerate(rows)]}
        if job.kind == "check":
            from skifer.core.schema_loader import load_schema

            schema_dict = load_schema(
                job.path,
                params={**engine.default_params, **job.params.get("run_params", {})},
            )
            monitor = getattr(engine, "monitor", None)
            if monitor is None:
                raise ResourceUnavailable("A monitor is required to run check jobs.")
            target_schema = engine.get_target_schema(job.params["target_layer"])
            fqn = engine._build_fqn(target_schema, job.params["target_table"])
            report = monitor.check_from_schema(fqn, schema_dict, raise_on_critical=False)
            return {"monitor_report": report.summary()}
        raise InvalidRequest(f"Invalid job kind {job.kind!r}.")

    def _assert_returned_run_id(self, job: _Job, returned: Any) -> None:
        if returned != job.job_id:
            raise RuntimeError(
                f"Engine returned run_id {returned!r} for job_id {job.job_id!r}."
            )

    def _finish_job_failed_or_cancelled(self, job_id: str, exc: Exception) -> None:
        with self._lock:
            job = self._get_current_job_locked(job_id)
            if job is None:
                return
            if job.state == "cancelled" or job.cancel_requested:
                job.state = "cancelled"
                if job.finished_at is None:
                    job.finished_at = self._clock()
                return
            try:
                from skifer.observability.checks import DataQualityError
            except Exception:
                DataQualityError = ()  # type: ignore[assignment]
            if isinstance(exc, DataQualityError):
                job.result = {
                    "publication_decision": "QUARANTINED",
                    "monitor_report": exc.report.summary(),
                }
                job.state = "succeeded"
                job.finished_at = self._clock()
                self._append_log_locked(job, "warning", "quarantined")
                return
            job.state = "failed"
            job.finished_at = self._clock()
            job.error = f"{type(exc).__name__}: {exc}"[:500]
            self._append_log_locked(job, "error", job.error)

    def _set_spark_job_group(self, engine, job: _Job) -> None:
        spark = getattr(engine, "spark", None)
        sc = getattr(spark, "sparkContext", None)
        set_job_group = getattr(sc, "setJobGroup", None)
        if callable(set_job_group):
            try:
                set_job_group(job.job_id, job.kind, interruptOnCancel=True)
            except TypeError:
                set_job_group(job.job_id, job.kind)

    def _cancel_job_locked(self, job: _Job, now: float, message: str) -> None:
        job.cancel_requested = True
        job.state = "cancelled"
        job.finished_at = now
        self._append_log_locked(job, "warning", message)
        engine = self._engine
        if engine is None:
            return
        spark = getattr(engine, "spark", None)
        sc = getattr(spark, "sparkContext", None)
        cancel_group = getattr(sc, "cancelJobGroup", None)
        if callable(cancel_group):
            try:
                cancel_group(job.job_id)
            except Exception:
                pass
        interrupt_tag = getattr(spark, "interruptTag", None)
        if callable(interrupt_tag):
            try:
                interrupt_tag(job.job_id)
            except Exception:
                pass

    def _append_log_locked(self, job: _Job, level: str, message: str) -> None:
        job.logs.append((self._clock(), level, message))

    def _get_job_locked(self, job_id: str) -> _Job:
        job = self._get_current_job_locked(job_id)
        if job is None:
            raise ResourceNotFound(f"Unknown job_id {job_id!r}.")
        return job

    def _get_current_job_locked(self, job_id: str) -> _Job | None:
        if self._job is not None and self._job.job_id == job_id:
            return self._job
        return None

    def _status_view(self, job: _Job) -> JobStatusView:
        return JobStatusView(
            job_id=job.job_id,
            run_id=job.job_id,
            kind=job.kind,
            state=job.state,
            submitted_at=job.submitted_at,
            started_at=job.started_at,
            finished_at=job.finished_at,
            error=job.error,
        )


__all__ = [
    "EngineFactory",
    "ExecutionService",
    "JobConflict",
    "JobStatusView",
    "LogsView",
    "NoActiveSession",
    "SessionView",
    "_default_engine_factory",
]
