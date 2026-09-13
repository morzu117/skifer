"""Transport-neutral request context, scopes, limits, and service errors."""

from __future__ import annotations

from dataclasses import dataclass

from skifer.observability.tracing import TraceContext


HARD_MAX_PAGE_SIZE = 100
HARD_MAX_QUERY_ROWS = 1_000
HARD_MAX_FILTERS = 50
HARD_MAX_FILTER_VALUE_LENGTH = 1_024
_CONSUMER_SCOPE_ALLOWLIST: frozenset[str] = frozenset()

CERTIFICATION_OVERRIDE_SCOPE = "certification_override"
SCOPE_MODELS_READ = "models:read"
SCOPE_CONTRACTS_READ = "contracts:read"
SCOPE_LINEAGE_READ = "lineage:read"
SCOPE_QUERY_EXECUTE = "query:execute"
SCOPE_PROJECT_READ = "project:read"
SCOPE_PIPELINES_WRITE = "pipelines:write"
SCOPE_RULES_WRITE = "rules:write"
SCOPE_EXECUTE_RUN = "execute:run"
SCOPE_CONTRACTS_WRITE = "contracts:write"
SCOPE_INCIDENTS_WRITE = "incidents:write"

NAMED_SCOPES: frozenset[str] = frozenset(
    {
        SCOPE_MODELS_READ,
        SCOPE_CONTRACTS_READ,
        SCOPE_LINEAGE_READ,
        SCOPE_QUERY_EXECUTE,
        SCOPE_PROJECT_READ,
        SCOPE_PIPELINES_WRITE,
        SCOPE_RULES_WRITE,
        SCOPE_EXECUTE_RUN,
        SCOPE_CONTRACTS_WRITE,
        SCOPE_INCIDENTS_WRITE,
    }
)


class AgentReadyDataError(Exception):
    """Base class for explicit, transport-neutral service refusals."""


class ScopeDenied(AgentReadyDataError):
    """The request context does not carry the exact required scope."""


class InvalidRequest(AgentReadyDataError):
    """The request violates the closed service input contract."""


class LimitExceeded(InvalidRequest):
    """A configured or hard service budget was exceeded."""


class InvalidCursor(InvalidRequest):
    """A pagination cursor is malformed, stale, or outside the result set."""


class ResourceNotFound(AgentReadyDataError):
    """A requested governed resource is not visible to this service."""


class ResourceUnavailable(AgentReadyDataError):
    """A required governance component is not configured."""


class SerializationError(AgentReadyDataError):
    """A query row cannot be represented safely as JSON-native data."""


@dataclass(frozen=True)
class RequestContext:
    subject: str
    scopes: frozenset[str]
    consumer_class: str
    trace_context: TraceContext

    def __post_init__(self) -> None:
        if not isinstance(self.subject, str) or not self.subject.strip():
            raise InvalidRequest("Request subject must be non-empty text.")
        if not isinstance(self.scopes, frozenset) or not all(
            isinstance(scope, str) and scope for scope in self.scopes
        ):
            raise InvalidRequest("Request scopes must be a frozenset of non-empty text.")
        if not isinstance(self.consumer_class, str) or not self.consumer_class.strip():
            raise InvalidRequest("Consumer class must be non-empty text.")
        if not isinstance(self.trace_context, TraceContext):
            raise InvalidRequest("trace_context must be a TraceContext instance.")


def require_scope(ctx: RequestContext, scope: str) -> None:
    """Require one exact scope and deny malformed contexts as well as absences."""
    if not isinstance(ctx, RequestContext) or scope not in ctx.scopes:
        raise ScopeDenied(f"Scope '{scope}' is required.")


@dataclass(frozen=True)
class ServiceLimits:
    max_page_size: int = HARD_MAX_PAGE_SIZE
    max_query_rows: int = HARD_MAX_QUERY_ROWS
    max_filters: int = HARD_MAX_FILTERS
    max_filter_value_length: int = HARD_MAX_FILTER_VALUE_LENGTH

    def __post_init__(self) -> None:
        values = (
            ("max_page_size", self.max_page_size, HARD_MAX_PAGE_SIZE),
            ("max_query_rows", self.max_query_rows, HARD_MAX_QUERY_ROWS),
            ("max_filters", self.max_filters, HARD_MAX_FILTERS),
            (
                "max_filter_value_length",
                self.max_filter_value_length,
                HARD_MAX_FILTER_VALUE_LENGTH,
            ),
        )
        for name, value, hard_maximum in values:
            if type(value) is not int or not 1 <= value <= hard_maximum:
                raise LimitExceeded(
                    f"{name} must be an integer in 1..{hard_maximum}; got {value!r}."
                )


__all__ = [
    "HARD_MAX_PAGE_SIZE",
    "HARD_MAX_QUERY_ROWS",
    "HARD_MAX_FILTERS",
    "HARD_MAX_FILTER_VALUE_LENGTH",
    "_CONSUMER_SCOPE_ALLOWLIST",
    "CERTIFICATION_OVERRIDE_SCOPE",
    "SCOPE_MODELS_READ",
    "SCOPE_CONTRACTS_READ",
    "SCOPE_LINEAGE_READ",
    "SCOPE_QUERY_EXECUTE",
    "SCOPE_PROJECT_READ",
    "SCOPE_PIPELINES_WRITE",
    "SCOPE_RULES_WRITE",
    "SCOPE_EXECUTE_RUN",
    "SCOPE_CONTRACTS_WRITE",
    "SCOPE_INCIDENTS_WRITE",
    "NAMED_SCOPES",
    "AgentReadyDataError",
    "ScopeDenied",
    "InvalidRequest",
    "LimitExceeded",
    "InvalidCursor",
    "ResourceNotFound",
    "ResourceUnavailable",
    "SerializationError",
    "RequestContext",
    "require_scope",
    "ServiceLimits",
]
