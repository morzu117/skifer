"""Uniform HTTP mapping for transport-neutral service errors."""

from __future__ import annotations

from typing import Any

from skifer.services import (
    AgentReadyDataError,
    InvalidCursor,
    InvalidRequest,
    LimitExceeded,
    ResourceNotFound,
    ResourceUnavailable,
    ScopeDenied,
    SerializationError,
)


_ERROR_TABLE: tuple[tuple[type, int, str], ...] = (
    (ScopeDenied, 403, "scope_denied"),
    (LimitExceeded, 400, "limit_exceeded"),
    (InvalidCursor, 400, "invalid_cursor"),
    (InvalidRequest, 400, "invalid_request"),
    (ResourceNotFound, 404, "not_found"),
    (ResourceUnavailable, 503, "unavailable"),
    (SerializationError, 500, "serialization_error"),
    (AgentReadyDataError, 500, "service_error"),
)


def error_payload(exc: Exception, path: str) -> tuple[int, dict[str, str]]:
    """Return a safe status and exact public error shape."""
    for cls, status, code in _ERROR_TABLE:
        if isinstance(exc, cls):
            return status, {"code": code, "message": str(exc), "path": path}
    return 500, {
        "code": "internal_error",
        "message": type(exc).__name__,
        "path": path,
    }


def register_error_handlers(app: Any) -> None:
    """Register one handler covering the complete service hierarchy."""
    from fastapi.responses import JSONResponse

    async def _handle(request: Any, exc: Exception) -> Any:
        status, body = error_payload(exc, request.url.path)
        return JSONResponse(status_code=status, content=body)

    app.add_exception_handler(AgentReadyDataError, _handle)


__all__ = ["error_payload", "register_error_handlers"]
