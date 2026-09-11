"""Lazy router registry."""

from __future__ import annotations

from typing import TYPE_CHECKING, Callable

from skifer.services.container import ServiceContainer

if TYPE_CHECKING:
    from fastapi import APIRouter


def all_routers() -> list[Callable[[ServiceContainer], "APIRouter"]]:
    """Return every v0 router builder after FastAPI is available."""
    from skifer.api.routes import (
        agents,
        catalog,
        certifications,
        contracts,
        data_products,
        dictionary,
        identity,
        incidents,
        jobs,
        lineage,
        pipelines,
        project,
        quality,
        rules,
        semantic,
        session,
    )

    return [
        project.build,
        pipelines.build,
        rules.build,
        catalog.build,
        semantic.build,
        lineage.build,
        dictionary.build,
        quality.build,
        incidents.build,
        contracts.build,
        certifications.build,
        data_products.build,
        agents.build,
        identity.build,
        session.build,
        jobs.build,
    ]


__all__ = ["all_routers"]
