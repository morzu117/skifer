"""Project and configuration routes."""

from __future__ import annotations

from skifer.services.container import ServiceContainer


def build(container: ServiceContainer):
    from fastapi import APIRouter, Depends
    from skifer.api.security import Scope
    from skifer.services import RequestContext, SCOPE_PROJECT_READ

    router = APIRouter(tags=["project"])
    svc = container.project

    @router.get("/project")
    def open_project(ctx: RequestContext = Depends(Scope(SCOPE_PROJECT_READ))) -> dict:
        return svc.open(ctx).to_dict()

    @router.get("/config")
    def config(ctx: RequestContext = Depends(Scope(SCOPE_PROJECT_READ))) -> dict:
        return svc.open(ctx).to_dict()["config"]

    @router.get("/project/json-schema")
    def json_schema(ctx: RequestContext = Depends(Scope(SCOPE_PROJECT_READ))) -> dict:
        return svc.json_schema(ctx)

    @router.get("/project/op-catalog")
    def op_catalog(ctx: RequestContext = Depends(Scope(SCOPE_PROJECT_READ))) -> dict:
        return svc.op_catalog(ctx)

    return router
