"""Current identity route."""

from __future__ import annotations

from skifer.services.container import ServiceContainer


def build(container: ServiceContainer):
    from fastapi import APIRouter, Depends
    from skifer.api.security import Scope
    from skifer.services import RequestContext, SCOPE_PROJECT_READ

    router = APIRouter(tags=["identity"])

    @router.get("/me")
    def me(ctx: RequestContext = Depends(Scope(SCOPE_PROJECT_READ))) -> dict:
        return {
            "subject": ctx.subject,
            "scopes": sorted(ctx.scopes),
            "consumer_class": ctx.consumer_class,
        }

    return router
