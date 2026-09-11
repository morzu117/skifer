"""Registry-backed dictionary routes."""

from __future__ import annotations

from skifer.services.container import ServiceContainer


def build(container: ServiceContainer):
    from fastapi import APIRouter, Depends
    from skifer.api.security import Scope
    from skifer.services import RequestContext, SCOPE_LINEAGE_READ

    router = APIRouter(tags=["dictionary"])

    @router.get("/dictionary/{dataset}")
    def dictionary(
        dataset: str,
        ctx: RequestContext = Depends(Scope(SCOPE_LINEAGE_READ)),
    ) -> dict:
        return container.governance.dictionary(ctx, dataset)

    return router
