"""Transport-neutral execution session service."""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Any, Callable
from uuid import uuid4

from skifer.services.context import RequestContext, SCOPE_EXECUTE_RUN, require_scope


EngineFactory = Callable[[str | None, str | None], Any]


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
        self._job = None
        if self._engine is not None:
            mode = self._session.mode if self._session is not None else "unknown"
            self._stop_engine_if_local(self._engine, mode)
        self._engine = None
        self._session = None
        self._session_key = None
        self._last_access = 0.0

    def _sweep_ttl_locked(self) -> None:
        if self._session is None:
            return
        if self._clock() - self._last_access > self._session_ttl:
            self._close_locked()

    def _stop_engine_if_local(self, engine, mode: str) -> None:
        if mode != "local":
            return
        spark = getattr(engine, "spark", None)
        stop = getattr(spark, "stop", None)
        if callable(stop):
            stop()


__all__ = [
    "EngineFactory",
    "ExecutionService",
    "SessionView",
    "_default_engine_factory",
]
