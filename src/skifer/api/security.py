"""Scope dependency shared by every business route."""

from __future__ import annotations

from fastapi import Request

from skifer.services import NAMED_SCOPES, RequestContext, require_scope


class Scope:
    """Resolve a request context and enforce one known service scope."""

    def __init__(self, scope: str):
        if scope not in NAMED_SCOPES:
            raise ValueError(f"Unknown API scope: {scope!r}")
        self.scope = scope

    def __call__(self, request: Request) -> RequestContext:
        ctx = request.app.state.context_provider(request)
        require_scope(ctx, self.scope)
        return ctx


__all__ = ["Scope"]
