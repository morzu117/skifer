"""Catalog-first, lazy loading for governed capability declarations."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import yaml

from .models import (
    ApprovalMode,
    CapabilityDefinition,
    CapabilityMode,
    CapabilitySummary,
    Reversibility,
)
from .validator import CapabilityValidationError, CapabilityValidator, parse_capability


class CapabilityRegistryError(ValueError):
    """The capability catalog or one of its declared definitions is invalid."""


@dataclass(frozen=True)
class _CatalogEntry:
    summary: CapabilitySummary
    path: Path


class CapabilityRegistry:
    """Expose catalog summaries and load full definitions only on explicit access."""

    def __init__(self, capabilities_dir: str | Path = "capabilities") -> None:
        self.capabilities_dir = Path(capabilities_dir).resolve()
        self._catalog: dict[str, _CatalogEntry] = {}
        self._cache: dict[str, CapabilityDefinition] = {}
        self.reload()

    def list_capabilities(self) -> tuple[CapabilitySummary, ...]:
        """Return only startup catalog summaries without reading definition files."""
        return tuple(entry.summary for entry in self._catalog.values())

    def get(self, capability_id: str) -> CapabilityDefinition:
        """Load, validate, cross-check and cache one requested definition."""
        if not isinstance(capability_id, str):
            raise TypeError("capability_id must be a string.")
        cached = self._cache.get(capability_id)
        if cached is not None:
            return cached
        entry = self._catalog.get(capability_id)
        if entry is None:
            raise CapabilityRegistryError(
                f"Capability id '{capability_id}' is not present in the catalog."
            )

        try:
            with entry.path.open(encoding="utf-8") as handle:
                payload = yaml.safe_load(handle)
        except (OSError, yaml.YAMLError) as exc:
            raise CapabilityRegistryError(
                f"Capability id '{capability_id}' definition could not be loaded from '{entry.path.name}'."
            ) from exc
        if not isinstance(payload, Mapping):
            raise CapabilityRegistryError(
                f"Capability id '{capability_id}' definition must be a YAML mapping."
            )
        try:
            definition = parse_capability(payload)
        except CapabilityValidationError as exc:
            raise CapabilityRegistryError(
                f"Capability id '{capability_id}' definition is invalid: {'; '.join(exc.errors)}"
            ) from exc
        if definition.id != capability_id:
            raise CapabilityRegistryError(
                f"Requested capability id '{capability_id}' but definition declares id '{definition.id}'."
            )
        self._check_catalog_agreement(entry.summary, definition)
        self._cache[capability_id] = definition
        return definition

    def reload(self) -> None:
        """Reload catalog summaries and clear every lazily loaded definition."""
        catalog_path = self.capabilities_dir / "capability_catalog.yaml"
        if not catalog_path.exists():
            self._catalog = {}
            self._cache.clear()
            return
        try:
            with catalog_path.open(encoding="utf-8") as handle:
                payload = yaml.safe_load(handle)
        except (OSError, yaml.YAMLError) as exc:
            raise CapabilityRegistryError("Capability catalog is malformed YAML.") from exc
        self._catalog = self._parse_catalog(payload)
        self._cache.clear()

    def _parse_catalog(self, payload: Any) -> dict[str, _CatalogEntry]:
        if not isinstance(payload, Mapping) or set(payload) != {"capabilities"}:
            raise CapabilityRegistryError(
                "Capability catalog must be a closed mapping with only 'capabilities'."
            )
        entries = payload["capabilities"]
        if not isinstance(entries, list):
            raise CapabilityRegistryError("Capability catalog field 'capabilities' must be a list.")

        parsed: dict[str, _CatalogEntry] = {}
        validator = CapabilityValidator()
        for index, raw_entry in enumerate(entries):
            if not isinstance(raw_entry, Mapping):
                raise CapabilityRegistryError(
                    f"Capability catalog entry #{index} must be a mapping."
                )
            if set(raw_entry) != {
                "id",
                "version",
                "owner",
                "description",
                "mode",
                "approval",
                "reversibility",
                "path",
            }:
                raise CapabilityRegistryError(
                    f"Capability catalog entry #{index} must contain only summary fields and 'path'."
                )
            summary_payload = {key: value for key, value in raw_entry.items() if key != "path"}
            result = validator.validate_summary(summary_payload)
            if not result.ok:
                raise CapabilityRegistryError(
                    f"Capability catalog entry #{index} is invalid: {'; '.join(result.errors)}"
                )
            capability_id = summary_payload["id"]
            if capability_id in parsed:
                raise CapabilityRegistryError(
                    f"Capability catalog contains duplicate id '{capability_id}'."
                )
            path = self._resolve_definition_path(raw_entry["path"], capability_id)
            summary = CapabilitySummary(
                id=capability_id,
                version=summary_payload["version"],
                owner=summary_payload["owner"],
                description=summary_payload["description"],
                mode=CapabilityMode(summary_payload["mode"]),
                approval=ApprovalMode(summary_payload["approval"]),
                reversibility=Reversibility(summary_payload["reversibility"]),
            )
            parsed[capability_id] = _CatalogEntry(summary=summary, path=path)
        return parsed

    def _resolve_definition_path(self, raw_path: Any, capability_id: str) -> Path:
        if not isinstance(raw_path, str) or not raw_path:
            raise CapabilityRegistryError(
                f"Capability catalog path for id '{capability_id}' must be non-empty text."
            )
        candidate = Path(raw_path)
        if candidate.is_absolute() or ".." in candidate.parts or "\\" in raw_path:
            raise CapabilityRegistryError(
                f"Capability catalog path for id '{capability_id}' must be relative and contain no '..'."
            )
        resolved = (self.capabilities_dir / candidate).resolve()
        if not resolved.is_relative_to(self.capabilities_dir):
            raise CapabilityRegistryError(
                f"Capability catalog path for id '{capability_id}' escapes the capabilities root."
            )
        return resolved

    @staticmethod
    def _check_catalog_agreement(
        summary: CapabilitySummary,
        definition: CapabilityDefinition,
    ) -> None:
        catalog_values = {
            "id": summary.id,
            "version": summary.version,
            "mode": summary.mode.value,
            "approval": summary.approval.value,
            "reversibility": summary.reversibility.value,
        }
        definition_values = {
            "id": definition.id,
            "version": definition.version,
            "mode": definition.mode.value,
            "approval": definition.approval.value,
            "reversibility": definition.reversibility.value,
        }
        for field_name, catalog_value in catalog_values.items():
            definition_value = definition_values[field_name]
            if catalog_value != definition_value:
                raise CapabilityRegistryError(
                    f"Capability field '{field_name}' disagrees: catalog declares "
                    f"'{catalog_value}' but definition declares '{definition_value}'."
                )
