"""Certification routes."""

from __future__ import annotations

from skifer.services.container import ServiceContainer


def build(container: ServiceContainer):
    from fastapi import APIRouter, Depends
    from skifer.api.security import Scope
    from skifer.services import RequestContext, SCOPE_CONTRACTS_READ

    router = APIRouter(tags=["certifications"])
    svc = container.governance

    @router.get("/certifications/{dataset}/history")
    def history(
        dataset: str,
        limit: int = 50,
        ctx: RequestContext = Depends(Scope(SCOPE_CONTRACTS_READ)),
    ) -> list[dict]:
        return svc.list_certification_history(ctx, dataset, limit=limit)

    @router.get("/certifications/{dataset}")
    def certification(
        dataset: str,
        ctx: RequestContext = Depends(Scope(SCOPE_CONTRACTS_READ)),
    ) -> dict:
        return svc.get_certification(ctx, dataset).to_dict()

    return router
