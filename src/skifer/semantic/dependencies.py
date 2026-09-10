"""Deterministic semantic-model dependency resolution."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SemanticDependency:
    dataset: str
    contract_version: str | None = None
    definition_hash: str | None = None


def resolve_dependencies(model: dict) -> tuple[SemanticDependency, ...]:
    """Resolve the physical dataset declared by the current semantic model schema."""
    table = model.get("table")
    if not table:
        return ()

    return (SemanticDependency(dataset=table),)
