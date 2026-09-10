"""Dependency-free MCP resource handlers over ``AgentReadyDataService``.

The optional MCP SDK is deliberately absent from this module.  Transport
objects are created only by :mod:`skifer.mcp.server`; these handlers
own URI parsing, safe error translation, and canonical resource serialization.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import re
from typing import Any, Generic, TypeVar
from urllib.parse import parse_qs, unquote_to_bytes, urlsplit

from skifer.agentic.data_service import (
    AgentReadyDataService,
    InvalidCursor,
    InvalidRequest,
    LimitExceeded,
    RequestContext,
    ResourceNotFound,
    ResourceUnavailable,
    ScopeDenied,
)
from skifer.semantic.access_policy import SemanticAccessDenied


JSON_MIME_TYPE = "application/json"
MAX_DISCOVERY_PAGE_SIZE = 100
ETAG_META_KEY = "io.skifer/etag"
ERROR_TYPE_DATA_KEY = "error_type"

INVALID_PARAMS = -32602
INTERNAL_ERROR = -32603
SCOPE_DENIED = -32001
RESOURCE_UNAVAILABLE = -32002
SEMANTIC_ACCESS_DENIED = -32003

_INVALID_PERCENT_ESCAPE = re.compile(r"%(?![0-9A-Fa-f]{2})")


@dataclass(frozen=True)
class MCPResourceError(Exception):
    """Sanitized protocol error awaiting conversion by the optional SDK."""

    code: int
    message: str
    error_type: str

    @property
    def data(self) -> dict[str, str]:
        return {ERROR_TYPE_DATA_KEY: self.error_type}

    def __str__(self) -> str:
        return self.message


@dataclass(frozen=True)
class ResourceDescriptor:
    uri: str
    name: str
    description: str
    scope: str
    mime_type: str = JSON_MIME_TYPE


@dataclass(frozen=True)
class ResourceTemplateDescriptor:
    uri_template: str
    name: str
    description: str
    scope: str
    mime_type: str = JSON_MIME_TYPE


T = TypeVar("T")


@dataclass(frozen=True)
class ResourcePage(Generic[T]):
    items: tuple[T, ...]
    next_cursor: str | None = None


@dataclass(frozen=True)
class ResourceContent:
    uri: str
    text: str
    mime_type: str
    metadata: dict[str, str]


CATALOG_RESOURCE = ResourceDescriptor(
    uri="skifer://semantic/catalog",
    name="semantic_catalog",
    description="Paginated summaries of governed semantic models.",
    scope="models:read",
)

RESOURCE_TEMPLATES = (
    ResourceTemplateDescriptor(
        uri_template="skifer://semantic/models/{key}",
        name="semantic_model",
        description="A governed semantic model without sensitive SQL or table details.",
        scope="models:read",
    ),
    ResourceTemplateDescriptor(
        uri_template="skifer://contracts/{id}/{version}",
        name="published_contract",
        description="A published governed data contract.",
        scope="contracts:read",
    ),
    ResourceTemplateDescriptor(
        uri_template="skifer://certification/{dataset}",
        name="dataset_certification",
        description="The current governed certification status for a dataset.",
        scope="contracts:read",
    ),
    ResourceTemplateDescriptor(
        uri_template="skifer://lineage/{dataset}/{column}",
        name="column_lineage",
        description="Static upstream and downstream lineage for a governed column.",
        scope="lineage:read",
    ),
)


class MCPResources:
    """Thin read-only resource surface over the application security boundary."""

    def __init__(self, service: AgentReadyDataService):
        if not isinstance(service, AgentReadyDataService):
            raise TypeError("service must be an AgentReadyDataService instance.")
        self._service = service

    def list_resources(
        self, ctx: RequestContext, cursor: str | None = None
    ) -> ResourcePage[ResourceDescriptor]:
        """List the fixed resources this caller may read, touching no catalog."""
        self._reject_discovery_cursor(cursor)
        return ResourcePage(items=_visible(ctx, (CATALOG_RESOURCE,)))

    def list_resource_templates(
        self, ctx: RequestContext, cursor: str | None = None
    ) -> ResourcePage[ResourceTemplateDescriptor]:
        """List the URI templates this caller may read, loading no model YAML."""
        self._reject_discovery_cursor(cursor)
        return ResourcePage(items=_visible(ctx, RESOURCE_TEMPLATES))

    def read(self, ctx: RequestContext, uri: str) -> ResourceContent:
        """Read one governed resource and translate every failure safely."""
        try:
            payload = self._read(ctx, uri)
            text = json.dumps(
                payload,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            etag = "sha256:" + hashlib.sha256(text.encode("utf-8")).hexdigest()
            return ResourceContent(
                uri=uri,
                text=text,
                mime_type=JSON_MIME_TYPE,
                metadata={ETAG_META_KEY: etag},
            )
        except MCPResourceError:
            raise
        except InvalidCursor:
            raise MCPResourceError(
                INVALID_PARAMS, "Pagination cursor is invalid.", "invalid_cursor"
            ) from None
        except LimitExceeded:
            raise MCPResourceError(
                INVALID_PARAMS, "Request limit was exceeded.", "limit_exceeded"
            ) from None
        except InvalidRequest:
            raise MCPResourceError(
                INVALID_PARAMS, "Resource request is invalid.", "invalid_request"
            ) from None
        except ScopeDenied:
            raise MCPResourceError(
                SCOPE_DENIED, "Required scope is missing.", "scope_denied"
            ) from None
        except ResourceNotFound:
            raise MCPResourceError(
                INVALID_PARAMS,
                "Governed resource was not found.",
                "resource_not_found",
            ) from None
        except ResourceUnavailable:
            raise MCPResourceError(
                RESOURCE_UNAVAILABLE,
                "Governed resource is unavailable.",
                "resource_unavailable",
            ) from None
        except SemanticAccessDenied:
            raise MCPResourceError(
                SEMANTIC_ACCESS_DENIED,
                "Semantic certification denied access.",
                "semantic_access_denied",
            ) from None
        except Exception as exc:
            exception_type = type(exc).__name__
            raise MCPResourceError(
                INTERNAL_ERROR,
                f"Unexpected service error: {exception_type}.",
                "internal_error",
            ) from None

    def _read(self, ctx: RequestContext, uri: str) -> dict[str, Any]:
        authority, segments, query = _parse_uri(uri)
        if authority == "semantic" and segments == ("catalog",):
            cursor, limit = _catalog_query(query)
            return self._service.list_models(ctx, cursor=cursor, limit=limit).to_dict()
        if query:
            raise InvalidRequest("Query parameters are only supported by the catalog resource.")
        if authority == "semantic" and len(segments) == 2 and segments[0] == "models":
            return self._service.get_model(ctx, segments[1]).to_dict()
        if authority == "contracts" and len(segments) == 2:
            return self._service.get_contract(ctx, segments[0], segments[1]).to_dict()
        if authority == "certification" and len(segments) == 1:
            return self._service.get_certification(ctx, segments[0]).to_dict()
        if authority == "lineage" and len(segments) == 2:
            return self._service.get_lineage(ctx, segments[0], segments[1]).to_dict()
        raise ResourceNotFound("The governed resource URI was not found.")

    @staticmethod
    def _reject_discovery_cursor(cursor: str | None) -> None:
        if cursor is not None:
            raise MCPResourceError(
                INVALID_PARAMS, "Pagination cursor is invalid.", "invalid_cursor"
            )


def _visible(ctx: RequestContext, descriptors: tuple) -> tuple:
    """Keep only the descriptors the caller holds the scope for.

    Discovery is part of the protected surface, not a public index: telling an
    agent that a contracts endpoint exists, and which scope opens it, is
    reconnaissance it has no claim to. The scope is re-checked by the service on
    every read, so this narrows what is advertised, never what is enforced.
    """
    if not isinstance(ctx, RequestContext):
        raise InvalidRequest("A request context is required to list resources.")
    return tuple(item for item in descriptors if item.scope in ctx.scopes)


def _parse_uri(uri: str) -> tuple[str, tuple[str, ...], dict[str, list[str]]]:
    if not isinstance(uri, str) or not uri:
        raise InvalidRequest("Resource URI must be non-empty text.")
    try:
        parsed = urlsplit(uri)
        port = parsed.port
    except ValueError as exc:
        raise InvalidRequest("Resource URI is malformed.") from exc
    if (
        parsed.scheme != "skifer"
        or not parsed.netloc
        or parsed.username is not None
        or parsed.password is not None
        or port is not None
        or parsed.fragment
    ):
        raise InvalidRequest("Resource URI is malformed.")
    raw_segments = parsed.path.split("/")[1:]
    if not raw_segments or any(not segment for segment in raw_segments):
        raise InvalidRequest("Resource URI path is malformed.")
    segments = tuple(_decode_segment(segment) for segment in raw_segments)
    try:
        query = parse_qs(
            parsed.query,
            keep_blank_values=True,
            strict_parsing=True,
            max_num_fields=2,
        )
    except ValueError as exc:
        raise InvalidRequest("Resource URI query is malformed.") from exc
    return parsed.netloc, segments, query


def _decode_segment(segment: str) -> str:
    if _INVALID_PERCENT_ESCAPE.search(segment):
        raise InvalidRequest("Resource URI contains an invalid escape.")
    try:
        decoded = unquote_to_bytes(segment).decode("utf-8")
    except UnicodeDecodeError as exc:
        raise InvalidRequest("Resource URI path is not valid UTF-8.") from exc
    if not decoded or "/" in decoded or "\\" in decoded or decoded in {".", ".."}:
        raise InvalidRequest("Resource URI path segment is invalid.")
    return decoded


def _catalog_query(query: dict[str, list[str]]) -> tuple[str | None, int]:
    if set(query) - {"cursor", "limit"} or any(len(values) != 1 for values in query.values()):
        raise InvalidRequest("Catalog query parameters are invalid.")
    cursor = query.get("cursor", [None])[0]
    if cursor == "":
        raise InvalidCursor("Catalog cursor must not be empty.")
    raw_limit = query.get("limit", ["50"])[0]
    try:
        limit = int(raw_limit)
    except (TypeError, ValueError) as exc:
        raise InvalidRequest("Catalog limit must be an integer.") from exc
    return cursor, limit
