import importlib

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
