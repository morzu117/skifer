"""Transport-neutral application service surface."""

from importlib import import_module
from typing import Any

from skifer.services.context import (
    HARD_MAX_FILTERS,
    HARD_MAX_FILTER_VALUE_LENGTH,
    HARD_MAX_PAGE_SIZE,
    HARD_MAX_QUERY_ROWS,
    _CONSUMER_SCOPE_ALLOWLIST,
    AgentReadyDataError,
    CERTIFICATION_OVERRIDE_SCOPE,
    InvalidCursor,
    InvalidRequest,
    LimitExceeded,
    NAMED_SCOPES,
    RequestContext,
    ResourceNotFound,
    ResourceUnavailable,
    SCOPE_CONTRACTS_READ,
    SCOPE_CONTRACTS_WRITE,
    SCOPE_EXECUTE_RUN,
    SCOPE_INCIDENTS_WRITE,
    SCOPE_LINEAGE_READ,
    SCOPE_MODELS_READ,
    SCOPE_PIPELINES_WRITE,
    SCOPE_PROJECT_READ,
    SCOPE_QUERY_EXECUTE,
    SCOPE_RULES_WRITE,
    ScopeDenied,
    SerializationError,
    ServiceLimits,
    require_scope,
)
from skifer.services.serialization import row_to_json, to_json_value
from skifer.services.project import (
    LocalizedError,
    PipelineView,
    ProjectService,
    ProjectView,
)
from skifer.services.rules import RuleService, RuleView, ScanReport, SnippetSpec
from skifer.services.governance import (
    ContractVersionView,
    DataProductView,
    GovernanceService,
    ImpactReport,
    QuarantineView,
    RegistryColumnSearchView,
)
from skifer.services.quality import (
    CheckDefinitionView,
    CheckRunView,
    IncidentView,
    QualityReportView,
    QualityService,
)
from skifer.services.semantic import (
    SEMANTIC_CONFLICT,
    SEMANTIC_DRIFT,
    SEMANTIC_OK,
    SemanticService,
    SyncOutcome,
)
from skifer.services.agents import AgentService
from skifer.services.execution import ExecutionService, SessionView
from skifer.services.identity import (
    LOCAL_DEFAULT_SCOPES,
    LocalIdentity,
    local_request_context,
)


_DATA_SERVICE_EXPORTS = frozenset(
    {
        "AgentReadyDataService",
        "CertificationView",
        "ContractFieldView",
        "ContractSemanticView",
        "ContractView",
        "GovernedModelView",
        "LineageEdgeView",
        "LineageView",
        "ModelSummary",
        "Page",
        "QueryEnvelope",
    }
)


def __getattr__(name: str) -> Any:
    """Load legacy data-service views lazily to support either import order."""
    if name not in _DATA_SERVICE_EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module("skifer.agentic.data_service"), name)
    globals()[name] = value
    return value


__all__ = [
    "HARD_MAX_PAGE_SIZE",
    "HARD_MAX_QUERY_ROWS",
    "HARD_MAX_FILTERS",
    "HARD_MAX_FILTER_VALUE_LENGTH",
    "_CONSUMER_SCOPE_ALLOWLIST",
    "NAMED_SCOPES",
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
    "to_json_value",
    "row_to_json",
    "LocalizedError",
    "PipelineView",
    "ProjectService",
    "ProjectView",
    "RuleService",
    "RuleView",
    "ScanReport",
    "SnippetSpec",
    "ContractVersionView",
    "DataProductView",
    "GovernanceService",
    "ImpactReport",
    "QuarantineView",
    "RegistryColumnSearchView",
    "CheckDefinitionView",
    "CheckRunView",
    "IncidentView",
    "QualityReportView",
    "QualityService",
    "SEMANTIC_OK",
    "SEMANTIC_DRIFT",
    "SEMANTIC_CONFLICT",
    "SemanticService",
    "SyncOutcome",
    "AgentService",
    "ExecutionService",
    "SessionView",
    "LOCAL_DEFAULT_SCOPES",
    "LocalIdentity",
    "local_request_context",
    "AgentReadyDataService",
    "ModelSummary",
    "GovernedModelView",
    "ContractFieldView",
    "ContractSemanticView",
    "ContractView",
    "CertificationView",
    "LineageEdgeView",
    "LineageView",
    "Page",
    "QueryEnvelope",
]
