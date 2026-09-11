"""Incident routes."""

from __future__ import annotations

from skifer.services.container import ServiceContainer


def build(container: ServiceContainer):
    from fastapi import APIRouter, Body, Depends
    from skifer.api.security import Scope
    from skifer.services import (
        RequestContext,
        SCOPE_CONTRACTS_READ,
        SCOPE_INCIDENTS_WRITE,
    )

    router = APIRouter(tags=["incidents"])
    svc = container.quality

    @router.get("/incidents")
    def incidents(
        status: str | None = None,
        target_fqn: str | None = None,
        limit: int = 50,
        ctx: RequestContext = Depends(Scope(SCOPE_CONTRACTS_READ)),
    ) -> list[dict]:
        return [
            item.to_dict()
            for item in svc.list_incidents(
                ctx, status=status, target_fqn=target_fqn, limit=limit
            )
        ]

    @router.post("/incidents/{incident_id}/ack")
    def acknowledge(
        incident_id: str,
        ctx: RequestContext = Depends(Scope(SCOPE_INCIDENTS_WRITE)),
    ) -> dict:
        return svc.acknowledge_incident(ctx, incident_id).to_dict()

    @router.post("/incidents/{incident_id}/assign")
    def assign(
        incident_id: str,
        assignee: str = Body(..., embed=True),
        ctx: RequestContext = Depends(Scope(SCOPE_INCIDENTS_WRITE)),
    ) -> dict:
        return svc.assign_incident(ctx, incident_id, assignee).to_dict()

    @router.post("/incidents/{incident_id}/resolve")
    def resolve(
        incident_id: str,
        root_cause: str = Body(..., embed=True),
        ctx: RequestContext = Depends(Scope(SCOPE_INCIDENTS_WRITE)),
    ) -> dict:
        return svc.resolve_incident(ctx, incident_id, root_cause).to_dict()

    return router
