"""Execution-job routes."""

from __future__ import annotations

from skifer.services.container import ServiceContainer


def build(container: ServiceContainer):
    from fastapi import APIRouter, Body, Depends
    from skifer.api.security import Scope
    from skifer.services import RequestContext, SCOPE_EXECUTE_RUN

    router = APIRouter(tags=["jobs"])
    svc = container.execution

    @router.post("/jobs")
    def submit(
        kind: str = Body(...),
        path: str = Body(...),
        params: dict | None = Body(default=None),
        ctx: RequestContext = Depends(Scope(SCOPE_EXECUTE_RUN)),
    ) -> dict:
        return {"job_id": svc.submit(ctx, kind, path, params)}

    @router.get("/jobs/{job_id}")
    def status(
        job_id: str,
        ctx: RequestContext = Depends(Scope(SCOPE_EXECUTE_RUN)),
    ) -> dict:
        return svc.status(ctx, job_id).to_dict()

    @router.post("/jobs/{job_id}/cancel")
    def cancel(
        job_id: str,
        ctx: RequestContext = Depends(Scope(SCOPE_EXECUTE_RUN)),
    ) -> dict:
        return svc.cancel(ctx, job_id).to_dict()

    @router.get("/jobs/{job_id}/logs")
    def logs(
        job_id: str,
        after: int = 0,
        ctx: RequestContext = Depends(Scope(SCOPE_EXECUTE_RUN)),
    ) -> dict:
        return svc.logs(ctx, job_id, after=after).to_dict()

    @router.get("/jobs/{job_id}/result")
    def result(
        job_id: str,
        ctx: RequestContext = Depends(Scope(SCOPE_EXECUTE_RUN)),
    ) -> dict:
        return svc.result(ctx, job_id).to_dict()

    return router
