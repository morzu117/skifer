"""Minimal MCP v2 server wiring with strictly lazy optional SDK imports."""

from __future__ import annotations

import importlib
import json
from typing import Any, Callable

from skifer.services import (
    LOCAL_DEFAULT_SCOPES,
    AgentReadyDataService,
    RequestContext,
)
from skifer.mcp.capability_tools import MCPCapabilityTools
from skifer.mcp.resources import MCPResourceError, MCPResources
from skifer.mcp.tools import MCPTools, QUERY_TOOL_NAME


MCP_EXTRA = 'pip install -e ".[mcp]"'

# Publicly expose the one local authority set used by stdio assembly. The MCP
# layer must not restate this list or derive it from a client request.
__all__ = ["LOCAL_DEFAULT_SCOPES", "MCPDependencyError", "create_server"]


class MCPDependencyError(RuntimeError):
    """The explicitly requested MCP adapter lacks its optional SDK."""


def _unexpected(exc: BaseException) -> MCPResourceError:
    """Only the class name: an exception message routinely quotes the bad value."""
    return MCPResourceError(
        code=-32603,
        message=f"Unexpected service error: {type(exc).__name__}.",
        error_type="internal_error",
    )


def create_server(
    service: AgentReadyDataService,
    context_provider: Callable[[Any], RequestContext],
    *,
    capability_tools: MCPCapabilityTools | None = None,
):
    """Build the SDK server with an optional governed capability surface."""
    if not callable(context_provider):
        raise TypeError("context_provider must be callable.")
    if capability_tools is not None and not isinstance(
        capability_tools, MCPCapabilityTools
    ):
        raise TypeError("capability_tools must be an MCPCapabilityTools instance or None.")
    try:
        mcp_module = importlib.import_module("mcp")
        server_module = importlib.import_module("mcp.server")
        types = importlib.import_module("mcp.types")
    except ImportError:
        raise MCPDependencyError(
            f"MCP server support requires the optional dependencies: `{MCP_EXTRA}`."
        ) from None

    resources = MCPResources(service)
    tools = MCPTools(service)

    def protocol_error(exc: MCPResourceError):
        return mcp_module.MCPError(
            code=exc.code,
            message=exc.message,
            data=exc.data,
        )

    async def list_resources(request_context, params):
        cursor = params.cursor if params is not None else None
        try:
            # Discovery is authenticated too: without the caller's context it
            # would advertise the whole surface to an agent holding no scope.
            ctx = context_provider(request_context)
            page = resources.list_resources(ctx, cursor=cursor)
        except MCPResourceError as exc:
            raise protocol_error(exc) from None
        except Exception as exc:
            raise protocol_error(_unexpected(exc)) from None
        return types.ListResourcesResult(
            resources=[
                types.Resource(
                    uri=item.uri,
                    name=item.name,
                    description=item.description,
                    mime_type=item.mime_type,
                )
                for item in page.items
            ],
            next_cursor=page.next_cursor,
        )

    async def list_resource_templates(request_context, params):
        cursor = params.cursor if params is not None else None
        try:
            ctx = context_provider(request_context)
            page = resources.list_resource_templates(ctx, cursor=cursor)
        except MCPResourceError as exc:
            raise protocol_error(exc) from None
        except Exception as exc:
            raise protocol_error(_unexpected(exc)) from None
        return types.ListResourceTemplatesResult(
            resource_templates=[
                types.ResourceTemplate(
                    uri_template=item.uri_template,
                    name=item.name,
                    description=item.description,
                    mime_type=item.mime_type,
                    _meta={"io.skifer/required-scope": item.scope},
                )
                for item in page.items
            ],
            next_cursor=page.next_cursor,
        )

    async def read_resource(request_context, params):
        try:
            ctx = context_provider(request_context)
            content = resources.read(ctx, str(params.uri))
        except MCPResourceError as exc:
            raise protocol_error(exc) from None
        except Exception as exc:
            raise protocol_error(_unexpected(exc)) from None
        return types.ReadResourceResult(
            contents=[
                types.TextResourceContents(
                    uri=content.uri,
                    text=content.text,
                    mime_type=content.mime_type,
                    _meta=content.metadata,
                )
            ],
            cache_scope="private",
        )

    async def list_tools(request_context, params):
        cursor = params.cursor if params is not None else None
        try:
            ctx = context_provider(request_context)
            descriptors = tools.list_tools(ctx, cursor=cursor)
            if capability_tools is not None:
                descriptors += capability_tools.list_tools(ctx)
        except MCPResourceError as exc:
            raise protocol_error(exc) from None
        except Exception as exc:
            raise protocol_error(_unexpected(exc)) from None
        annotation_type = getattr(types, "ToolAnnotations", None)
        return types.ListToolsResult(
            tools=[
                types.Tool(
                    name=item.name,
                    description=item.description,
                    input_schema=item.input_schema,
                    **(
                        {
                            "annotations": annotation_type(
                                readOnlyHint=item.read_only_hint,
                                destructiveHint=item.destructive_hint,
                                idempotentHint=item.idempotent_hint,
                                openWorldHint=item.open_world_hint,
                            )
                        }
                        if annotation_type is not None
                        else {}
                    ),
                )
                for item in descriptors
            ]
        )

    async def call_tool(request_context, params):
        try:
            ctx = context_provider(request_context)
            if params.name == QUERY_TOOL_NAME or capability_tools is None:
                payload = tools.call(ctx, params.name, params.arguments)
            else:
                payload = capability_tools.call(ctx, params.name, params.arguments)
            text = json.dumps(
                payload,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
        except MCPResourceError as exc:
            raise protocol_error(exc) from None
        except Exception as exc:
            raise protocol_error(_unexpected(exc)) from None
        return types.CallToolResult(
            content=[types.TextContent(type="text", text=text)],
            structured_content=payload,
            is_error=False,
        )

    return server_module.Server(
        "skifer-readonly",
        on_list_resources=list_resources,
        on_list_resource_templates=list_resource_templates,
        on_read_resource=read_resource,
        on_list_tools=list_tools,
        on_call_tool=call_tool,
    )
