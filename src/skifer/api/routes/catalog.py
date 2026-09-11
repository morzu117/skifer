"""Semantic catalog routes."""

from __future__ import annotations

from skifer.services.container import ServiceContainer


def build(container: ServiceContainer):
    from fastapi import APIRouter, Depends
    from skifer.api.security import Scope
    from skifer.services import RequestContext, SCOPE_MODELS_READ

    router = APIRouter(tags=["catalog"])
    svc = container.semantic

    @router.get("/catalog")
    def catalog(
        cursor: str | None = None,
        limit: int = 50,
        ctx: RequestContext = Depends(Scope(SCOPE_MODELS_READ)),
    ) -> dict:
        return svc.list_models(ctx, cursor=cursor, limit=limit).to_dict()

    @router.get("/catalog/{key}")
    def model(key: str, ctx: RequestContext = Depends(Scope(SCOPE_MODELS_READ))) -> dict:
        return svc.get_model(ctx, key).to_dict()

    return router
