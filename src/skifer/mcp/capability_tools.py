"""Dependency-free MCP tools for governed external-system capabilities.

This adapter deliberately contains no capability business policy.  It derives
descriptors from validated definitions and delegates every write to the existing
``GovernedExecutor``.  MCP annotations remain advisory metadata; caller scope,
autonomy, approval, preconditions, credentials, and idempotency are enforced on
the server path independently of those hints.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
import json
from typing import Any

from skifer.services import (
    RequestContext,
    ScopeDenied,
    require_scope,
)
from skifer.capabilities import (
    ApprovalError,
    ApprovalRecord,
    AutonomyError,
    AutonomyMode,
    CapabilityDefinition,
    CapabilityExecutorError,
    CapabilityHistoryError,
    CapabilityMode,
    CapabilityRegistry,
    CapabilityRegistryError,
    CredentialError,
    DuplicateRequestError,
    GovernedExecutionError,
    GovernedExecutor,
    LostResponseError,
    PreconditionError,
    request_hash,
    resolve_autonomy_mode,
    validate_arguments,
)
from skifer.mcp.resources import INVALID_PARAMS, MCPResourceError
from skifer.mcp.tools import ToolDescriptor


CAPABILITY_TOOL_PREFIX = "capability_"
CAPABILITY_ENVELOPE_VERSION = "skifer.mcp.capability.v1"

# These fields carry authority or execution context.  They can only originate
# from server configuration and RequestContext, even if a capability definition
# accidentally declares a property with the same name.
_SERVER_ONLY_ARGUMENTS = frozenset(
    {
        "approval",
        "approved",
        "actor",
        "subject",
        "mode",
        "autonomy_mode",
        "scopes",
        "lease",
    }
)
_MCP_AUTONOMY_MODES = frozenset(
    {AutonomyMode.SHADOW, AutonomyMode.SUPERVISED}
)

ApprovalProvider = Callable[
    [CapabilityDefinition, str], ApprovalRecord | None
]


class MCPCapabilityTools:
    """Expose visible write capabilities through their existing governed path."""

    def __init__(
        self,
        registry: CapabilityRegistry,
        *,
        governed_executor: GovernedExecutor,
        autonomy_mode: AutonomyMode | None = None,
        approval_provider: ApprovalProvider | None = None,
    ) -> None:
        if not isinstance(registry, CapabilityRegistry):
            raise TypeError("registry must be a CapabilityRegistry.")
        if not isinstance(governed_executor, GovernedExecutor):
            raise TypeError("governed_executor must be a GovernedExecutor.")
        if autonomy_mode is not None and not isinstance(autonomy_mode, AutonomyMode):
            raise TypeError("autonomy_mode must be an AutonomyMode or None.")
        if approval_provider is not None and not callable(approval_provider):
            raise TypeError("approval_provider must be callable or None.")
        self._registry = registry
        self._governed_executor = governed_executor
        self._autonomy_mode = autonomy_mode
        self._approval_provider = approval_provider

    def list_tools(self, ctx: RequestContext) -> tuple[ToolDescriptor, ...]:
        """List only capabilities this caller can invoke in the effective mode."""
        if not isinstance(ctx, RequestContext):
            raise _invalid_tool_request()
        descriptors: list[ToolDescriptor] = []
        names: set[str] = set()
        for summary in self._registry.list_capabilities():
            try:
                definition = self._registry.get(summary.id)
            except CapabilityRegistryError:
                # A broken definition is not a capability the caller can invoke
                # and therefore is not part of their discovery surface.
                continue
            if not self._is_visible(ctx, definition):
                continue
            name = _tool_name(definition.id)
            if name in names:
                # Replacing dots with underscores is mandated but is not
                # injective.  Never dispatch an ambiguous name.
                raise MCPResourceError(
                    INVALID_PARAMS,
                    "Capability tool discovery is unavailable.",
                    "ambiguous_tool_name",
                )
            names.add(name)
            schema = definition.to_dict()["input_schema"]
            descriptors.append(
                ToolDescriptor(
                    name=name,
                    description=definition.description,
                    input_schema=schema,
                    scope="",
                    read_only_hint=False,
                    destructive_hint=True,
                    idempotent_hint=True,
                    open_world_hint=False,
                )
            )
        return tuple(descriptors)

    def call(
        self,
        ctx: RequestContext,
        name: str,
        arguments: Any,
    ) -> dict[str, Any]:
        """Invoke one visible capability and return a bounded lifecycle envelope."""
        definition, mode = self._visible_definition(ctx, name)
        if not isinstance(arguments, dict):
            raise _invalid_tool_request()

        # Enforcement is independent of client-side JSON Schema handling.  The
        # outer allowed-set check is intentionally explicit, matching MCPTools.
        properties = definition.input_schema["properties"]
        allowed = set(properties) - _SERVER_ONLY_ARGUMENTS
        if set(arguments) - allowed or _SERVER_ONLY_ARGUMENTS & set(arguments):
            return self._refused(definition, ctx.subject, arguments, "invalid_arguments")
        if validate_arguments(definition.input_schema, arguments):
            # The existing validator caps its work and error count.  Its bounded
            # natural-language messages still do not cross this protocol boundary.
            return self._refused(definition, ctx.subject, arguments, "invalid_arguments")

        identity = request_hash(
            capability_id=definition.id,
            capability_version=definition.version,
            subject=ctx.subject,
            arguments=arguments,
        )
        approval, approval_error = self._approval(definition, identity, mode)

        try:
            result = self._governed_executor.execute(
                definition,
                subject=ctx.subject,
                arguments=arguments,
                mode=mode,
                approval=approval,
            )
        except PreconditionError:
            return _envelope(
                definition,
                identity,
                status="refused",
                reason_codes=("precondition_refused",),
            )
        except ApprovalError:
            return _envelope(
                definition,
                identity,
                status="pending",
                reason_codes=(approval_error or "approval_invalid",),
            )
        except (DuplicateRequestError, LostResponseError):
            return _envelope(
                definition,
                identity,
                status="refused",
                reason_codes=("execution_refused",),
            )
        except GovernedExecutionError:
            status = (
                "pending"
                if mode is AutonomyMode.SUPERVISED and approval is None
                else "refused"
            )
            if status == "pending":
                code = approval_error or "approval_required"
            else:
                code = "governed_refusal"
            return _envelope(
                definition,
                identity,
                status=status,
                reason_codes=(code,),
            )
        except (
            CapabilityExecutorError,
            CapabilityHistoryError,
            CredentialError,
        ):
            return _envelope(
                definition,
                identity,
                status="refused",
                reason_codes=("execution_refused",),
            )

        if mode is AutonomyMode.SHADOW:
            return _envelope(definition, identity, status="proposed")
        return _envelope(
            definition,
            identity,
            status="executed",
            outcome={
                "status": result.status,
                "output": dict(result.output),
                "error_type": result.error_type,
            },
        )

    def _visible_definition(
        self, ctx: RequestContext, name: str
    ) -> tuple[CapabilityDefinition, AutonomyMode]:
        if not isinstance(ctx, RequestContext) or not isinstance(name, str):
            raise _invalid_tool_request()
        matches: list[tuple[CapabilityDefinition, AutonomyMode]] = []
        for summary in self._registry.list_capabilities():
            if _tool_name(summary.id) != name:
                continue
            try:
                definition = self._registry.get(summary.id)
            except CapabilityRegistryError:
                continue
            mode = self._effective_mode(definition)
            if mode is not None and self._has_required_scopes(ctx, definition):
                matches.append((definition, mode))
        if len(matches) != 1:
            # Unknown, malformed, ambiguous, and caller-invisible tools are
            # intentionally indistinguishable at this boundary.
            raise _unknown_tool()
        return matches[0]

    def _is_visible(
        self, ctx: RequestContext, definition: CapabilityDefinition
    ) -> bool:
        return (
            self._effective_mode(definition) is not None
            and self._has_required_scopes(ctx, definition)
        )

    def _effective_mode(
        self, definition: CapabilityDefinition
    ) -> AutonomyMode | None:
        if definition.mode is not CapabilityMode.WRITE:
            return None
        if _SERVER_ONLY_ARGUMENTS & set(definition.input_schema["properties"]):
            return None
        try:
            mode = resolve_autonomy_mode(
                definition,
                configured=self._autonomy_mode,
            )
        except (TypeError, AutonomyError):
            return None
        return mode if mode in _MCP_AUTONOMY_MODES else None

    @staticmethod
    def _has_required_scopes(
        ctx: RequestContext, definition: CapabilityDefinition
    ) -> bool:
        try:
            for scope in definition.required_scopes:
                require_scope(ctx, scope)
        except ScopeDenied:
            return False
        return True

    def _approval(
        self,
        definition: CapabilityDefinition,
        identity: str,
        mode: AutonomyMode,
    ) -> tuple[ApprovalRecord | None, str | None]:
        if mode is not AutonomyMode.SUPERVISED or self._approval_provider is None:
            return None, None
        try:
            approval = self._approval_provider(definition, identity)
        except Exception:
            return None, "approval_unavailable"
        if approval is not None and not isinstance(approval, ApprovalRecord):
            return None, "approval_invalid"
        return approval, None

    @staticmethod
    def _refused(
        definition: CapabilityDefinition,
        subject: str,
        arguments: Mapping[str, Any],
        reason_code: str,
    ) -> dict[str, Any]:
        try:
            identity = request_hash(
                capability_id=definition.id,
                capability_version=definition.version,
                subject=subject,
                arguments=arguments,
            )
        except Exception:
            raise _invalid_tool_request() from None
        return _envelope(
            definition,
            identity,
            status="refused",
            reason_codes=(reason_code,),
        )


def _tool_name(capability_id: str) -> str:
    return CAPABILITY_TOOL_PREFIX + capability_id.replace(".", "_")


def _envelope(
    definition: CapabilityDefinition,
    identity: str,
    *,
    status: str,
    reason_codes: tuple[str, ...] = (),
    outcome: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "format_version": CAPABILITY_ENVELOPE_VERSION,
        "status": status,
        "capability_id": definition.id,
        "capability_version": definition.version,
        "request_id": identity,
    }
    if reason_codes:
        payload["reason_codes"] = list(reason_codes)
    if outcome is not None:
        payload["outcome"] = dict(outcome)
    # No Spark object, lease, mapping proxy, datetime, NaN, or Python-specific
    # value can survive this protocol boundary.
    return json.loads(
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    )


def _invalid_tool_request() -> MCPResourceError:
    return MCPResourceError(
        INVALID_PARAMS,
        "Tool request is invalid.",
        "invalid_request",
    )


def _unknown_tool() -> MCPResourceError:
    return MCPResourceError(
        INVALID_PARAMS,
        "Tool request is invalid.",
        "invalid_request",
    )


__all__ = [
    "CAPABILITY_ENVELOPE_VERSION",
    "CAPABILITY_TOOL_PREFIX",
    "MCPCapabilityTools",
]
