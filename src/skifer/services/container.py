"""Local service composition root used by optional transports."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from skifer.agentic.data_service import AgentReadyDataService
from skifer.agentic.hub import AgenticHub
from skifer.observability.certification_store import SqliteCertificationStore
from skifer.observability.history import SqliteHistoryStore
from skifer.observability.metadata_store import MetadataRegistryQuery, SqliteMetadataStore
from skifer.semantic.semantic import SemanticEngine
from skifer.services.agents import AgentService
from skifer.services.context import ResourceUnavailable
from skifer.services.execution import ExecutionService
from skifer.services.governance import GovernanceService
from skifer.services.project import ProjectService
from skifer.services.quality import QualityService
from skifer.services.rules import RuleService
from skifer.services.semantic import SemanticService


@dataclass(frozen=True)
class ServiceContainer:
    project: ProjectService
    rules: RuleService
    semantic: SemanticService
    governance: GovernanceService
    quality: QualityService
    agents: AgentService
    execution: ExecutionService
    data_service: AgentReadyDataService


class _CatalogOnlyCore:
    """Core-shaped object that keeps catalog reads Spark-free."""

    def __init__(self, config: dict[str, Any]):
        self.config = config
        self.env = "DEV"

    def _get_backend(self):
        raise ResourceUnavailable("Connect an execution session before querying data.")


def build_services(project_dir: str) -> ServiceContainer:
    """Wire local services without creating a Spark session."""
    root = Path(project_dir).expanduser().resolve()
    config_path = root / "config.yaml"
    config: dict[str, Any] = {}
    if config_path.is_file():
        loaded = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
        if isinstance(loaded, dict):
            config = loaded

    certification_store = SqliteCertificationStore(str(root / ".skifer_certification.db"))
    history_store = SqliteHistoryStore(str(root / ".skifer_observability.db"))
    metadata_store = SqliteMetadataStore(str(root / ".skifer_metadata.db"))
    semantic_engine = SemanticEngine(
        _CatalogOnlyCore(config),
        models_dir=str(root / "semantic_models"),
        certification_store=certification_store,
    )
    registry_query = MetadataRegistryQuery(metadata_store)
    data_service = AgentReadyDataService(
        semantic_engine,
        lineage_graph=registry_query,
    )
    hub = AgenticHub(semantic_engine=semantic_engine)
    return ServiceContainer(
        project=ProjectService(str(root)),
        rules=RuleService(str(root)),
        semantic=SemanticService(data_service, models_dir=str(root / "semantic_models")),
        governance=GovernanceService(
            certification_store,
            metadata_store=metadata_store,
            metadata_registry_query=registry_query,
        ),
        quality=QualityService(
            history_store,
            incident_store=certification_store,
        ),
        agents=AgentService(hub),
        execution=ExecutionService(),
        data_service=data_service,
    )


__all__ = ["ServiceContainer", "build_services"]
