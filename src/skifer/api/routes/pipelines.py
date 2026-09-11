"""Pipeline routes."""

from __future__ import annotations

from skifer.services.container import ServiceContainer


def build(container: ServiceContainer):
    from fastapi import APIRouter, Body, Depends
    from skifer.api.security import Scope
    from skifer.services import (
        RequestContext,
        SCOPE_PIPELINES_WRITE,
        SCOPE_PROJECT_READ,
    )

    router = APIRouter(tags=["pipelines"])
    svc = container.project

    @router.get("/pipelines/{path:path}/describe")
    def describe(path: str, ctx: RequestContext = Depends(Scope(SCOPE_PROJECT_READ))) -> dict:
        return svc.describe(ctx, path)

    @router.get("/pipelines/{path:path}/output")
    def output(path: str, ctx: RequestContext = Depends(Scope(SCOPE_PROJECT_READ))) -> dict:
        return svc.project_output(ctx, path)

    @router.get("/pipelines/{path:path}/explain-rules")
    def explain(path: str, ctx: RequestContext = Depends(Scope(SCOPE_PROJECT_READ))) -> dict:
        return svc.explain_rules(ctx, path)

    @router.get("/pipelines/{path:path}")
    def get_pipeline(path: str, ctx: RequestContext = Depends(Scope(SCOPE_PROJECT_READ))) -> dict:
        return svc.get_pipeline(ctx, path).to_dict()

    @router.put("/pipelines/{path:path}")
    def write_pipeline(
        path: str,
        text: str = Body(..., embed=True),
        ctx: RequestContext = Depends(Scope(SCOPE_PIPELINES_WRITE)),
    ) -> dict:
        return {"path": svc.write_pipeline(ctx, path, text)}

    return router
