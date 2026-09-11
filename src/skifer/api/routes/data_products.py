"""Data-product routes."""

from __future__ import annotations

from skifer.services.container import ServiceContainer


def build(container: ServiceContainer):
    from fastapi import APIRouter, Depends
    from skifer.api.security import Scope
    from skifer.services import RequestContext, SCOPE_CONTRACTS_READ

    router = APIRouter(tags=["data-products"])

    @router.get("/data-products")
    def data_products(
        ctx: RequestContext = Depends(Scope(SCOPE_CONTRACTS_READ)),
    ) -> list[dict]:
        return [
            item.to_dict() for item in container.governance.list_data_products(ctx)
        ]

    @router.get("/data-products/{data_product_id}")
    def data_product(
        data_product_id: str,
        ctx: RequestContext = Depends(Scope(SCOPE_CONTRACTS_READ)),
    ) -> dict:
        return container.governance.get_data_product(ctx, data_product_id).to_dict()

    return router
