"""Execution-session routes."""

from __future__ import annotations

from skifer.services.container import ServiceContainer


def build(container: ServiceContainer):
    from fastapi import APIRouter, Body, Depends
    from skifer.api.security import Scope
    from skifer.services import RequestContext, SCOPE_EXECUTE_RUN

    router = APIRouter(tags=["session"])
    svc = container.execution

    @router.post("/session/connect")
    def connect(
        config_path: str | None = Body(default=None),
        force_env: str | None = Body(default=None),
        ctx: RequestContext = Depends(Scope(SCOPE_EXECUTE_RUN)),
    ) -> dict:
        return svc.connect(ctx, config_path=config_path, force_env=force_env).to_dict()

    @router.get("/session")
    def session(ctx: RequestContext = Depends(Scope(SCOPE_EXECUTE_RUN))) -> dict | None:
        current = svc.session(ctx)
        return current.to_dict() if current is not None else None

    return router
