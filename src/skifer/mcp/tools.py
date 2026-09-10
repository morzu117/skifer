"""Dependency-free MCP tool handlers over ``AgentReadyDataService``.

The optional MCP SDK is deliberately absent from this module.  The sole tool
accepts a closed declarative query shape and delegates execution to the
application security boundary without reaching into any of its collaborators.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import json
import re
from typing import Any

from skifer.agentic.data_service import (
    AgentReadyDataService,
    HARD_MAX_FILTERS,
    HARD_MAX_FILTER_VALUE_LENGTH,
    ServiceLimits,
    InvalidRequest,
    LimitExceeded,
    QueryEnvelope,
    RequestContext,
    ResourceNotFound,
    ResourceUnavailable,
    ScopeDenied,
    SerializationError,
)
from skifer.agentic.resolver import SemanticQuery, SemanticQueryError
from skifer.mcp.resources import (
    INVALID_PARAMS,
    MCPResourceError,
    RESOURCE_UNAVAILABLE,
    SCOPE_DENIED,
    SEMANTIC_ACCESS_DENIED,
)
from skifer.semantic.access_policy import SemanticAccessDenied


QUERY_TOOL_NAME = "query_semantic_model"
QUERY_TIMEOUT = -32004
_DATE_PATTERN = r"^\d{4}-\d{2}-\d{2}$"
_DATE_RE = re.compile(_DATE_PATTERN)
_FILTER_OPERATORS = (
    "eq",
    "neq",
    "gt",
    "lt",
    "gte",
    "lte",
    "in",
    "like",
    "is_null",
    "is_not_null",
)


def _filter_scalar_schema(max_value_length: int) -> dict[str, Any]:
    return {
        "oneOf": [
            {"type": "string", "maxLength": max_value_length},
            {"type": "number"},
            {"type": "boolean"},
            {"type": "null"},
        ]
    }


def build_input_schema(limits: ServiceLimits) -> dict[str, Any]:
    """Advertise exactly the bounds the service will enforce for this instance.

    A schema built from the hard ceilings promised 1000 rows to an agent talking
    to a service configured for 100: every such request was refused after the
    fact. The refusal was fail-closed, but a schema that overstates what may be
    asked is a schema the agent cannot plan against.
    """
    max_items = limits.max_filters
    scalar = _filter_scalar_schema(limits.max_filter_value_length)
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "additionalProperties": False,
        "required": ["model"],
        "properties": {
            "model": {"type": "string", "minLength": 1},
            "metrics": {
                "type": "array",
                "maxItems": max_items,
                "items": {"type": "string", "minLength": 1},
            },
            "group_by": {
                "type": "array",
                "maxItems": max_items,
                "items": {"type": "string", "minLength": 1},
            },
            "filters": {
                "type": "array",
                "maxItems": max_items,
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["column", "operator"],
                    "properties": {
                        "column": {"type": "string", "minLength": 1},
                        "operator": {"type": "string", "enum": list(_FILTER_OPERATORS)},
                        "value": {
                            "oneOf": [
                                scalar,
                                {
                                    "type": "array",
                                    "maxItems": max_items,
                                    "items": scalar,
                                },
                            ]
                        },
                    },
                },
            },
            "date_from": {"type": "string", "format": "date", "pattern": _DATE_PATTERN},
            "date_to": {"type": "string", "format": "date", "pattern": _DATE_PATTERN},
            "period": {"type": "string", "minLength": 1},
            "limit": {"type": "integer", "minimum": 1, "maximum": limits.max_query_rows},
        },
    }


@dataclass(frozen=True)
class ToolDescriptor:
    name: str
    description: str
    input_schema: dict[str, Any]
    scope: str
    read_only_hint: bool = True
    destructive_hint: bool = False
    idempotent_hint: bool = True
    open_world_hint: bool = False


def build_query_tool(limits: ServiceLimits) -> ToolDescriptor:
    return ToolDescriptor(
        name=QUERY_TOOL_NAME,
        description="Run a bounded, governed semantic query and return redacted evidence.",
        input_schema=build_input_schema(limits),
        scope="query:execute",
    )


# Default descriptor at the hard ceilings, for callers without a service.
QUERY_SEMANTIC_MODEL = build_query_tool(ServiceLimits())
QUERY_SEMANTIC_MODEL_INPUT_SCHEMA = QUERY_SEMANTIC_MODEL.input_schema


class MCPTools:
    """The read-only MCP execution surface, delegating only to the data service."""

    def __init__(self, service: AgentReadyDataService):
        if not isinstance(service, AgentReadyDataService):
            raise TypeError("service must be an AgentReadyDataService instance.")
        self._service = service
        # Advertise and validate against the limits this service really applies,
        # not the hard ceilings, so the schema never overstates what may be asked.
        self._limits = service.limits
        self._descriptor = build_query_tool(self._limits)

    def list_tools(self, ctx: RequestContext, cursor: str | None = None) -> tuple[ToolDescriptor, ...]:
        if not isinstance(ctx, RequestContext):
            raise MCPResourceError(
                INVALID_PARAMS, "Tool request is invalid.", "invalid_request"
            )
        if cursor is not None:
            raise MCPResourceError(
                INVALID_PARAMS, "Pagination cursor is invalid.", "invalid_cursor"
            )
        return (self._descriptor,) if self._descriptor.scope in ctx.scopes else ()

    def call(self, ctx: RequestContext, name: str, arguments: Any) -> dict[str, Any]:
        """Validate, execute through the service, and return JSON-native output."""
        try:
            if name != QUERY_TOOL_NAME:
                raise InvalidRequest("Unknown MCP tool.")
            query, limit = _parse_arguments(arguments, self._limits)
            envelope = self._service.query(ctx, query, limit)
            if not isinstance(envelope, QueryEnvelope):
                raise ResourceUnavailable("The data service returned an invalid query envelope.")
            payload = envelope.to_dict()
            # Crossing the protocol boundary must never retain a DataFrame or a
            # Spark-specific value even if a faulty service double returns one.
            return json.loads(
                json.dumps(
                    payload,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                    allow_nan=False,
                )
            )
        except MCPResourceError:
            raise
        except LimitExceeded:
            raise MCPResourceError(
                INVALID_PARAMS, "Request limit was exceeded.", "limit_exceeded"
            ) from None
        except (InvalidRequest, SemanticQueryError):
            raise MCPResourceError(
                INVALID_PARAMS, "Tool request is invalid.", "invalid_request"
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
        except (ResourceUnavailable, SerializationError):
            raise MCPResourceError(
                RESOURCE_UNAVAILABLE,
                "Governed query result is unavailable.",
                "resource_unavailable",
            ) from None
        except SemanticAccessDenied:
            raise MCPResourceError(
                SEMANTIC_ACCESS_DENIED,
                "Semantic certification denied access.",
                "semantic_access_denied",
            ) from None
        except TimeoutError:
            raise MCPResourceError(
                QUERY_TIMEOUT, "Semantic query timed out.", "query_timeout"
            ) from None
        except Exception:
            # The SDK adapter owns unexpected-error conversion through
            # server._unexpected(), keeping that single sanitization convention.
            raise


def _parse_arguments(arguments: Any, limits: ServiceLimits) -> tuple[SemanticQuery, int]:
    if not isinstance(arguments, dict):
        raise InvalidRequest("Tool arguments must be an object.")
    allowed = {
        "model",
        "metrics",
        "group_by",
        "filters",
        "date_from",
        "date_to",
        "period",
        "limit",
    }
    if set(arguments) - allowed or "model" not in arguments:
        raise InvalidRequest("Tool arguments do not match the closed schema.")

    model = _non_empty_text(arguments["model"], "model")
    metrics = _name_list(arguments.get("metrics", []), "metrics", limits)
    group_by = _name_list(arguments.get("group_by", []), "group_by", limits)
    filters = _filters(arguments.get("filters", []), limits)
    date_from = _date_value(arguments.get("date_from"), "date_from")
    date_to = _date_value(arguments.get("date_to"), "date_to")
    period_raw = arguments.get("period")
    period = None if period_raw is None else _non_empty_text(period_raw, "period")
    limit = arguments.get("limit", 100)
    if type(limit) is not int or not 1 <= limit <= limits.max_query_rows:
        raise LimitExceeded("Query limit is outside the hard bound.")
    return (
        SemanticQuery(
            model_name=model,
            metrics=metrics,
            group_by=group_by,
            filters=filters,
            date_from=date_from,
            date_to=date_to,
            period=period,
        ),
        limit,
    )


def _name_list(value: Any, field_name: str, limits: ServiceLimits) -> list[str]:
    if not isinstance(value, list) or len(value) > limits.max_filters:
        raise LimitExceeded(f"{field_name} exceeds the closed schema bound.")
    return [_non_empty_text(item, field_name) for item in value]


def _filters(value: Any, limits: ServiceLimits) -> list[dict[str, Any]]:
    if not isinstance(value, list) or len(value) > limits.max_filters:
        raise LimitExceeded("filters exceeds the closed schema bound.")
    parsed = []
    for item in value:
        if not isinstance(item, dict) or set(item) - {"column", "operator", "value"}:
            raise InvalidRequest("Filter does not match the closed schema.")
        if "column" not in item or "operator" not in item:
            raise InvalidRequest("Filter is missing a required field.")
        column = _non_empty_text(item["column"], "filter column")
        operator = item["operator"]
        if operator not in _FILTER_OPERATORS:
            raise InvalidRequest("Filter operator is unsupported.")
        has_value = "value" in item
        if operator in {"is_null", "is_not_null"}:
            if has_value:
                raise InvalidRequest("Null filter operators do not accept a value.")
            parsed.append({"column": column, "operator": operator})
            continue
        if not has_value:
            raise InvalidRequest("Filter value is required.")
        filter_value = item["value"]
        if operator == "in":
            if not isinstance(filter_value, list) or len(filter_value) > HARD_MAX_FILTERS:
                raise LimitExceeded("Filter list exceeds the closed schema bound.")
            filter_value = [_filter_scalar(entry) for entry in filter_value]
        elif isinstance(filter_value, list):
            raise InvalidRequest("Only the 'in' operator accepts a list value.")
        else:
            filter_value = _filter_scalar(filter_value)
        parsed.append({"column": column, "operator": operator, "value": filter_value})
    return parsed


def _filter_scalar(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        if len(value) > HARD_MAX_FILTER_VALUE_LENGTH:
            raise LimitExceeded("Filter value exceeds the closed schema bound.")
        return value
    raise InvalidRequest("Filter value must be a JSON scalar.")


def _non_empty_text(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value:
        raise InvalidRequest(f"{field_name} must be non-empty text.")
    return value


def _date_value(value: Any, field_name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or _DATE_RE.fullmatch(value) is None:
        raise InvalidRequest(f"{field_name} must use YYYY-MM-DD.")
    try:
        date.fromisoformat(value)
    except ValueError as exc:
        raise InvalidRequest(f"{field_name} must be a valid date.") from exc
    return value
