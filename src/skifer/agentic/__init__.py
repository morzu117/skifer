import importlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .agent import GenBIAgent
    from ..semantic.resolver import SemanticQuery
    from ..semantic.resolver import QueryResolver
    from ..semantic.resolver import ResolvedQuery
    from ..semantic.resolver import SemanticQueryError
    from .models import AgentResponse
    from .models import CapabilityRequest
    from .models import CapabilityResponse
    from .models import FormattedResult
    from .models import ResponseFormat
    from .lineage_agent import LineageAgent
    from .models import LineageResponse
    from .quality_agent import QualityAgent
    from .models import QualityResponse
    from .dictionary_agent import DictionaryAgent
    from .models import DictionaryResponse
    from .models import BuilderResponse
    from .history import HistoryEntry
    from .history import SessionHistory
    from .exporter import HistoryExporter
    from .hub import AgenticHub
    from .user_profile import UserProfile

_EXPORTS = {
    "GenBIAgent": ".agent",
    "SemanticQuery": "..semantic.resolver",
    "QueryResolver": "..semantic.resolver",
    "ResolvedQuery": "..semantic.resolver",
    "SemanticQueryError": "..semantic.resolver",
    "AgentResponse": ".models",
    "CapabilityRequest": ".models",
    "CapabilityResponse": ".models",
    "FormattedResult": ".models",
    "ResponseFormat": ".models",
    "LineageResponse": ".models",
    "QualityResponse": ".models",
    "DictionaryResponse": ".models",
    "BuilderResponse": ".models",
    "HistoryEntry": ".history",
    "SessionHistory": ".history",
    "HistoryExporter": ".exporter",
    "LineageAgent": ".lineage_agent",
    "QualityAgent": ".quality_agent",
    "DictionaryAgent": ".dictionary_agent",
    "AgenticHub": ".hub",
    "UserProfile": ".user_profile",
}


def __getattr__(name):
    module = _EXPORTS.get(name)
    if module is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    obj = getattr(importlib.import_module(module, __name__), name)
    globals()[name] = obj
    return obj


def __dir__():
    return sorted(__all__)


__all__ = [
    "GenBIAgent",
    "SemanticQuery",
    "QueryResolver",
    "ResolvedQuery",
    "SemanticQueryError",
    "AgentResponse",
    "CapabilityRequest",
    "CapabilityResponse",
    "FormattedResult",
    "ResponseFormat",
    "LineageAgent",
    "LineageResponse",
    "QualityAgent",
    "QualityResponse",
    "DictionaryAgent",
    "DictionaryResponse",
    "BuilderResponse",
    "HistoryEntry",
    "SessionHistory",
    "HistoryExporter",
    "AgenticHub",
    "UserProfile",
]
