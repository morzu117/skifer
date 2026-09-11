"""Agent routes."""

from __future__ import annotations

from skifer.services.container import ServiceContainer


def build(container: ServiceContainer):
    from fastapi import APIRouter, Body, Depends
    from skifer.api.security import Scope
    from skifer.services import (
        RequestContext,
        SCOPE_PIPELINES_WRITE,
        SCOPE_QUERY_EXECUTE,
    )

    router = APIRouter(tags=["agents"])
    svc = container.agents

    @router.post("/agents/ask")
    def ask(
        question: str = Body(..., embed=True),
        profile: dict | None = Body(default=None),
        ctx: RequestContext = Depends(Scope(SCOPE_QUERY_EXECUTE)),
    ) -> dict:
        return svc.ask(ctx, question, profile=profile)

    @router.post("/agents/build")
    def build_pipeline(
        description: str = Body(..., embed=True),
        output_dir: str = Body(default="schemas", embed=True),
        ctx: RequestContext = Depends(Scope(SCOPE_PIPELINES_WRITE)),
    ) -> dict:
        return svc.build(ctx, description, output_dir=output_dir)

    return router
