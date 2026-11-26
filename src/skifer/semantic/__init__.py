import importlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .semantic import SemanticEngine
    from .builder import SemanticBuilder
    from .domain import EntityDef
    from .domain import EntityRef
    from .domain import RelationshipDef
    from .draft_builder import SemanticDraftBuilder
    from .llm_provider import LLMProvider
    from .llm_provider import get_llm_provider
    from .validator import SemanticValidator
    from .validator import ValidationResult
    from .extractor import NotebookExtractor
    from .extractor import RuleInspector
    from .glossary import GlossaryReader
    from .output_projection import OutputProjector
    from .output_projection import ProjectedField
    from .output_projection import ProjectedSchema
    from .sync import SemanticChange
    from .sync import SemanticConflict
    from .sync import SemanticSynchronizer
    from .sync import SyncReport
    from .evidence import SourceEvidence
    from .evidence import ExecutionResult
    from .evidence import MetricEvidence
    from .evidence import SemanticEvidence
    from .evidence import SemanticExecutionError
    from .evidence import SemanticResult

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
