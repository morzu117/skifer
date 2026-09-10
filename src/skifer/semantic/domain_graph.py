"""Lazy semantic domain graph over curated model relationships."""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass
import hashlib
import json

from .domain import EntityDef, EntityRef, RelationshipDef, parse_entities, parse_relationships


DEFAULT_GRAPH_MAX_DEPTH = 4
DEFAULT_GRAPH_MAX_MODELS = 32


@dataclass(frozen=True)
class DomainEdge:
    """One traversable hop between two semantic models."""

    relationship: RelationshipDef
    source_entity: EntityRef
    target_entity: EntityRef
    reverse: bool = False

    @property
    def source_model(self) -> str:
        return self.source_entity.model_key

    @property
    def target_model(self) -> str:
        return self.target_entity.model_key


class DomainGraph:
    """Navigate curated semantic relationships without preloading all models."""

    def __init__(
        self,
        semantic_engine,
        *,
        max_depth: int = DEFAULT_GRAPH_MAX_DEPTH,
        max_models: int = DEFAULT_GRAPH_MAX_MODELS,
    ) -> None:
        if max_depth < 1:
            raise ValueError("DomainGraph max_depth must be >= 1.")
        if max_models < 1:
            raise ValueError("DomainGraph max_models must be >= 1.")
        self.semantic = semantic_engine
        self.max_depth = max_depth
        self.max_models = max_models
        self._catalog_signature: str | None = None
        self._inbound_candidates: dict[str, tuple[str, ...]] = {}
        self._model_hashes: dict[str, str] = {}
        self._entities: dict[str, tuple[EntityDef, ...]] = {}
        self._entity_index: dict[str, dict[str, EntityDef]] = {}
        self._relationships: dict[str, tuple[RelationshipDef, ...]] = {}
        self._neighbors: dict[str, tuple[DomainEdge, ...]] = {}
        self._neighbor_signatures: dict[str, str] = {}

    def related_models(self, model_key: str) -> tuple[str, ...]:
        """Return catalog-declared related model keys for one model."""
        self._ensure_fresh()
        summary = self.semantic.get_model_summary(model_key)
        related = summary.get("related_models") or []
        return tuple(sorted({str(item) for item in related if item}))

    def get_entity(self, model_key: str, entity_name: str) -> EntityDef:
        """Return one declared entity from a lazily loaded model."""
        self._ensure_fresh()
        self._load_model(model_key)
        entity = self._entity_index.get(model_key, {}).get(entity_name)
        if entity is None:
            available = sorted(self._entity_index.get(model_key, {}))
            raise ValueError(
                f"❌ Entity '{entity_name}' is not declared on semantic model "
                f"'{model_key}'. Available: {available}"
            )
        return entity

    def neighbors(self, model_key: str) -> tuple[DomainEdge, ...]:
        """Return traversable outgoing edges for one model."""
        self._ensure_fresh()
        self._load_model(model_key)
        candidate_keys = self._inbound_candidates.get(model_key, ())
        for candidate_key in candidate_keys:
            self._load_model(candidate_key)

        current_signature = self._neighbor_signature(model_key, candidate_keys)
        cached = self._neighbors.get(model_key)
        if cached is not None and self._neighbor_signatures.get(model_key) == current_signature:
            return cached

        edges = {
            self._forward_edge(relationship)
            for relationship in self._relationships.get(model_key, ())
            if relationship.to_entity.model_key
        }

        for candidate_key in candidate_keys:
            for relationship in self._relationships.get(candidate_key, ()):
                if relationship.to_entity.model_key != model_key:
                    continue
                if not self._can_traverse_reverse(relationship):
                    continue
                edges.add(
                    DomainEdge(
                        relationship=relationship,
                        source_entity=relationship.to_entity,
                        target_entity=relationship.from_entity,
                        reverse=True,
                    )
                )

        ordered = tuple(sorted(edges, key=self._edge_sort_key))
        self._neighbors[model_key] = ordered
        self._neighbor_signatures[model_key] = current_signature
        return ordered

    def find_paths(
        self,
        start_model: str,
        target_model: str,
        *,
        max_depth: int | None = None,
        max_models: int | None = None,
    ) -> tuple[tuple[DomainEdge, ...], ...]:
        """Return every minimal traversable path between two models."""
        self._ensure_fresh()
        self.semantic.get_model_summary(start_model)
        self.semantic.get_model_summary(target_model)

        if start_model == target_model:
            return ((),)

        depth_limit = max_depth if max_depth is not None else self.max_depth
        model_limit = max_models if max_models is not None else self.max_models
        if depth_limit < 1:
            raise ValueError("DomainGraph search max_depth must be >= 1.")
        if model_limit < 1:
            raise ValueError("DomainGraph search max_models must be >= 1.")

        frontier: deque[tuple[str, tuple[DomainEdge, ...]]] = deque([(start_model, ())])
        seen_at_depth: dict[str, int] = {start_model: 0}
        discovered_models = {start_model}
        minimal_paths: list[tuple[DomainEdge, ...]] = []
        hit_depth_limit = False

        while frontier:
            current_model, path = frontier.popleft()
            depth = len(path)
            if minimal_paths and depth >= len(minimal_paths[0]):
                continue

            edges = self.neighbors(current_model)
            if depth >= depth_limit:
                if any(
                    edge.target_model not in self._path_models(path)
                    for edge in edges
                ):
                    hit_depth_limit = True
                continue

            path_models = self._path_models(path, start_model=start_model)
            for edge in edges:
                next_model = edge.target_model
                if next_model in path_models:
                    continue

                next_path = path + (edge,)
                next_depth = depth + 1

                if next_model == target_model:
                    minimal_paths.append(next_path)
                    continue

                previous_depth = seen_at_depth.get(next_model)
                if previous_depth is not None and previous_depth < next_depth:
                    continue

                if next_model not in discovered_models:
                    discovered_models.add(next_model)
                    if len(discovered_models) > model_limit:
                        raise ValueError(
                            "Semantic domain graph model limit exceeded while "
                            f"searching from '{start_model}' to '{target_model}' "
                            f"(limit={model_limit})."
                        )
                seen_at_depth[next_model] = next_depth
                frontier.append((next_model, next_path))

        if minimal_paths:
            return tuple(sorted(minimal_paths, key=self._path_sort_key))
        if hit_depth_limit:
            raise ValueError(
                "Semantic domain graph depth limit exceeded while searching from "
                f"'{start_model}' to '{target_model}' (limit={depth_limit})."
            )
        return ()

    def _ensure_fresh(self) -> None:
        signature = self._catalog_signature_value()
        if signature == self._catalog_signature:
            return

        self._catalog_signature = signature
        self._inbound_candidates = self._build_inbound_candidates()
        self._model_hashes.clear()
        self._entities.clear()
        self._entity_index.clear()
        self._relationships.clear()
        self._neighbors.clear()
        self._neighbor_signatures.clear()

    def _load_model(self, model_key: str) -> None:
        model = self.semantic._get_model(model_key)
        model_hash = self._model_definition_hash(model)
        if self._model_hashes.get(model_key) == model_hash:
            return

        entities = parse_entities(model)
        relationships = parse_relationships(model)
        self._model_hashes[model_key] = model_hash
        self._entities[model_key] = entities
        self._entity_index[model_key] = {entity.name: entity for entity in entities}
        self._relationships[model_key] = relationships
        self._neighbors.pop(model_key, None)
        self._neighbor_signatures.pop(model_key, None)

    def _catalog_signature_value(self) -> str:
        payload = [
            self.semantic._catalog[key]
            for key in sorted(self.semantic._catalog)
        ]
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()

    def _build_inbound_candidates(self) -> dict[str, tuple[str, ...]]:
        inbound: dict[str, set[str]] = {}
        for model_key, entry in self.semantic._catalog.items():
            related = entry.get("related_models") or []
            for related_model in related:
                if not related_model:
                    continue
                inbound.setdefault(str(related_model), set()).add(model_key)
        return {
            model_key: tuple(sorted(candidates))
            for model_key, candidates in inbound.items()
        }

    @staticmethod
    def _model_definition_hash(model: dict) -> str:
        metadata = model.get("metadata") or {}
        if metadata.get("source_definition_hash"):
            return str(metadata["source_definition_hash"])
        encoded = json.dumps(model, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()

    def _neighbor_signature(
        self,
        model_key: str,
        candidate_keys: tuple[str, ...],
    ) -> str:
        payload = {
            "model": self._model_hashes.get(model_key),
            "candidates": {
                candidate_key: self._model_hashes.get(candidate_key)
                for candidate_key in candidate_keys
            },
        }
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()

    @staticmethod
    def _forward_edge(relationship: RelationshipDef) -> DomainEdge:
        return DomainEdge(
            relationship=relationship,
            source_entity=relationship.from_entity,
            target_entity=relationship.to_entity,
            reverse=False,
        )

    @staticmethod
    def _can_traverse_reverse(relationship: RelationshipDef) -> bool:
        return (
            relationship.cardinality == "one_to_one"
            and relationship.verified_by_contract
        )

    @staticmethod
    def _edge_sort_key(edge: DomainEdge) -> tuple[str, str, str, str, bool]:
        return (
            edge.target_model,
            edge.source_entity.entity_name,
            edge.target_entity.entity_name,
            edge.relationship.name,
            edge.reverse,
        )

    @classmethod
    def _path_sort_key(cls, path: tuple[DomainEdge, ...]) -> tuple[tuple[str, ...], tuple[str, ...]]:
        model_path = tuple([path[0].source_model, *(edge.target_model for edge in path)])
        relationship_names = tuple(edge.relationship.name for edge in path)
        return model_path, relationship_names

    @staticmethod
    def _path_models(
        path: tuple[DomainEdge, ...],
        *,
        start_model: str | None = None,
    ) -> set[str]:
        models = {start_model} if start_model is not None else set()
        for edge in path:
            models.add(edge.source_model)
            models.add(edge.target_model)
        return {model for model in models if model}
