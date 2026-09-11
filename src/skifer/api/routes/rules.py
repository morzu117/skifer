"""Rule routes."""

from __future__ import annotations

from skifer.services.container import ServiceContainer


def build(container: ServiceContainer):
    from fastapi import APIRouter, Body, Depends, Query
    from skifer.api.security import Scope
    from skifer.services import (
        RequestContext,
        SCOPE_PROJECT_READ,
        SCOPE_RULES_WRITE,
    )
    from skifer.services.rules import snippet_spec_from_dict

    router = APIRouter(tags=["rules"])
    svc = container.rules

    @router.get("/rules")
    def list_rules(ctx: RequestContext = Depends(Scope(SCOPE_PROJECT_READ))) -> list[dict]:
        return [item.to_dict() for item in svc.list(ctx)]

    @router.get("/rules/graph")
    def graph(
        names: list[str] = Query(default=[]),
        ctx: RequestContext = Depends(Scope(SCOPE_PROJECT_READ)),
    ) -> dict:
        return svc.dependency_graph(ctx, tuple(names))

    @router.post("/rules/snippet")
    def snippet(
        spec: dict = Body(...),
        ctx: RequestContext = Depends(Scope(SCOPE_PROJECT_READ)),
    ) -> dict:
        return {"code": svc.generate_snippet(ctx, snippet_spec_from_dict(spec))}

    @router.put("/rules/{file:path}")
    def write_rule(
        file: str,
        code: str = Body(..., embed=True),
        ctx: RequestContext = Depends(Scope(SCOPE_RULES_WRITE)),
    ) -> dict:
        return {"path": svc.write_rule(ctx, file, code)}

    return router
