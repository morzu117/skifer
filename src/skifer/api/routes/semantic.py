"""Semantic query and synchronization routes."""

from __future__ import annotations

from skifer.services.container import ServiceContainer


def build(container: ServiceContainer):
    from fastapi import APIRouter, Body, Depends
    from skifer.api.security import Scope
    from skifer.services import (
        RequestContext,
        SCOPE_MODELS_READ,
        SCOPE_PIPELINES_WRITE,
        SCOPE_QUERY_EXECUTE,
    )
    from skifer.services.semantic import semantic_query_from_dict

    router = APIRouter(tags=["semantic"])

    @router.post("/semantic/query")
    def query(
        query_body: dict = Body(...),
        limit: int = 100,
        ctx: RequestContext = Depends(Scope(SCOPE_QUERY_EXECUTE)),
    ) -> dict:
        query_value = semantic_query_from_dict(query_body)
        return container.data_service.query(ctx, query_value, limit=limit).to_dict()

    @router.post("/semantic/check")
    def check(
        pipeline_path: str = Body(..., embed=True),
        ctx: RequestContext = Depends(Scope(SCOPE_MODELS_READ)),
    ) -> dict:
        return container.semantic.check(ctx, pipeline_path).to_dict()

    @router.post("/semantic/write-draft")
    def write_draft(
        pipeline_path: str = Body(..., embed=True),
        ctx: RequestContext = Depends(Scope(SCOPE_PIPELINES_WRITE)),
    ) -> dict:
        return container.semantic.write_draft(ctx, pipeline_path).to_dict()

    @router.post("/semantic/promote")
    def promote(
        pipeline_path: str = Body(..., embed=True),
        ctx: RequestContext = Depends(Scope(SCOPE_PIPELINES_WRITE)),
    ) -> dict:
        return container.semantic.promote(ctx, pipeline_path).to_dict()

    return router
