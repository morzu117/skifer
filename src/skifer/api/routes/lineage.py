"""Lineage routes."""

from __future__ import annotations

from skifer.services.container import ServiceContainer


def build(container: ServiceContainer):
    from fastapi import APIRouter, Depends
    from skifer.api.security import Scope
    from skifer.services import RequestContext, SCOPE_LINEAGE_READ

    router = APIRouter(tags=["lineage"])

    @router.get("/lineage/{dataset}/{column}")
    def lineage(
        dataset: str,
        column: str,
        ctx: RequestContext = Depends(Scope(SCOPE_LINEAGE_READ)),
    ) -> dict:
        return container.data_service.get_lineage(ctx, dataset, column).to_dict()

    return router
