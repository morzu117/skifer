import importlib

_EXPORTS = {
    "SemanticEngine": "semantic",
    "SemanticBuilder": "builder",
    "EntityDef": "domain",
    "EntityRef": "domain",
    "RelationshipDef": "domain",
    "SemanticDraftBuilder": "draft_builder",
    "LLMProvider": "llm_provider",
    "get_llm_provider": "llm_provider",
    "SemanticValidator": "validator",
    "ValidationResult": "validator",
    "NotebookExtractor": "extractor",
    "RuleInspector": "extractor",
    "GlossaryReader": "glossary",
    "OutputProjector": "output_projection",
    "ProjectedField": "output_projection",
    "ProjectedSchema": "output_projection",
    "SemanticChange": "sync",
    "SemanticConflict": "sync",
    "SemanticSynchronizer": "sync",
    "SyncReport": "sync",
    "ExecutionResult": "evidence",
    "MetricEvidence": "evidence",
    "SemanticEvidence": "evidence",
    "SemanticExecutionError": "evidence",
    "SemanticResult": "evidence",
    "SourceEvidence": "evidence",
}


def __getattr__(name):
    module = _EXPORTS.get(name)
    if module is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    obj = getattr(importlib.import_module(f"{__name__}.{module}"), name)
    globals()[name] = obj
    return obj


def __dir__():
    return sorted(__all__)


__all__ = [
    "SemanticEngine",
    "SemanticBuilder",
    "EntityDef",
    "EntityRef",
    "RelationshipDef",
    "SemanticDraftBuilder",
    "LLMProvider",
    "get_llm_provider",
    "SemanticValidator",
    "ValidationResult",
    "NotebookExtractor",
    "RuleInspector",
    "GlossaryReader",
    "OutputProjector",
    "ProjectedField",
    "ProjectedSchema",
    "SemanticChange",
    "SemanticConflict",
    "SemanticSynchronizer",
    "SyncReport",
    "SourceEvidence",
    "ExecutionResult",
    "MetricEvidence",
    "SemanticEvidence",
    "SemanticExecutionError",
    "SemanticResult",
]
