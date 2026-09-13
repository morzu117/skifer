"""Contract routes."""

from __future__ import annotations

from skifer.services.container import ServiceContainer


def build(container: ServiceContainer):
    from fastapi import APIRouter, Depends
    from skifer.api.security import Scope
    from skifer.services import RequestContext, SCOPE_CONTRACTS_READ

    router = APIRouter(tags=["contracts"])
    svc = container.governance

    @router.get("/contracts/{contract_id}/{version}")
    def contract(
        contract_id: str,
        version: str,
        ctx: RequestContext = Depends(Scope(SCOPE_CONTRACTS_READ)),
    ) -> dict:
        return svc.get_contract(ctx, contract_id, version).to_dict()

    @router.get("/contracts/{contract_id}")
    def versions(
        contract_id: str,
        ctx: RequestContext = Depends(Scope(SCOPE_CONTRACTS_READ)),
    ) -> dict:
        return svc.versions(ctx, contract_id).to_dict()

    return router
