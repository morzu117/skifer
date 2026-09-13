"""Quality routes."""

from __future__ import annotations

from skifer.services.container import ServiceContainer


def build(container: ServiceContainer):
    from fastapi import APIRouter, Body, Depends
    from skifer.api.security import Scope
    from skifer.services import RequestContext, SCOPE_CONTRACTS_READ

    router = APIRouter(tags=["quality"])
    svc = container.quality

    @router.post("/quality/checks")
    def checks(
        schema: dict = Body(...),
        ctx: RequestContext = Depends(Scope(SCOPE_CONTRACTS_READ)),
    ) -> list[dict]:
        return [item.to_dict() for item in svc.checks_from_schema(ctx, schema)]

    @router.get("/quality/history/{table}")
    def history(
        table: str,
        limit: int = 20,
        ctx: RequestContext = Depends(Scope(SCOPE_CONTRACTS_READ)),
    ) -> list[dict]:
        return [item.to_dict() for item in svc.history(ctx, table, limit=limit)]

    @router.get("/quality/last/{table}")
    def last(
        table: str,
        ctx: RequestContext = Depends(Scope(SCOPE_CONTRACTS_READ)),
    ) -> dict | None:
        report = svc.last_report(ctx, table)
        return report.to_dict() if report is not None else None

    return router
