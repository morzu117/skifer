"""Spark-free project inspection and guarded pipeline persistence."""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import tempfile
from typing import Any

import yaml

from skifer.core.ir import parse_to_ir
from skifer.core.json_schema import generate_json_schema
from skifer.core.op_catalog import AGGREGATE_FUNCTIONS, COLUMN_OPS, FILTER_OPERATORS
from skifer.core.schema_loader import parse_schema_localized
from skifer.lineage.tracker import LineageTracker
from skifer.observability.audit import AuditReport, audit_project
from skifer.semantic.output_projection import OutputProjector
from skifer.services.context import (
    InvalidRequest,
    RequestContext,
    ResourceNotFound,
    SCOPE_PIPELINES_WRITE,
    SCOPE_PROJECT_READ,
    require_scope,
)
from skifer.services.serialization import to_json_value


@dataclass(frozen=True)
class LocalizedError:
    code: str
    message: str
    path: str

    def to_dict(self) -> dict[str, str]:
        return {"code": self.code, "message": self.message, "path": self.path}


@dataclass(frozen=True)
class PipelineView:
    path: str
    raw_yaml: str
    normalized: dict[str, Any] | None
    errors: tuple[LocalizedError, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "raw_yaml": self.raw_yaml,
            "normalized": to_json_value(self.normalized, "normalized"),
            "errors": [error.to_dict() for error in self.errors],
        }


@dataclass(frozen=True)
class ProjectView:
    root: str
    config: dict[str, Any]
    environments: tuple[str, ...]
    pipelines: tuple[str, ...]
    rules: tuple[str, ...]
    models: tuple[str, ...]
    calendars: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "root": self.root,
            "config": to_json_value(self.config, "config"),
            "environments": list(self.environments),
            "pipelines": list(self.pipelines),
            "rules": list(self.rules),
            "models": list(self.models),
            "calendars": list(self.calendars),
        }


_SECRET_KEY_PARTS = frozenset(
    {"api_key", "client_secret", "credential", "password", "secret", "token"}
)


def _config_without_secrets(value: Any) -> Any:
    """Copy config data while omitting conventionally secret-bearing keys."""
    if isinstance(value, dict):
        return {
            key: _config_without_secrets(child)
            for key, child in value.items()
            if isinstance(key, str)
            and not any(part in key.lower() for part in _SECRET_KEY_PARTS)
        }
    if isinstance(value, list):
        return [_config_without_secrets(child) for child in value]
    return value


class ProjectService:
    """Inspect and edit project assets without constructing a Spark session."""

    def __init__(self, project_dir: str):
        self._root = Path(project_dir).expanduser().resolve()

    def open(self, ctx: RequestContext) -> ProjectView:
        require_scope(ctx, SCOPE_PROJECT_READ)
        if not self._root.is_dir():
            raise ResourceNotFound("Project root was not found.")

        config_path = self._root / "config.yaml"
        if not config_path.is_file():
            raise ResourceNotFound("Project config.yaml was not found.")
        config = self._load_mapping(config_path, "Project config.yaml")
        environments = config.get("environments", {})
        environment_names = (
            tuple(sorted(environments)) if isinstance(environments, dict) else ()
        )

        pipelines = self._relative_files(self._root / "schemas", "*.yaml")
        rules = self._relative_files(self._root / "rules", "*.py")
        models = self._model_keys()
        calendars = self._calendar_keys()
        return ProjectView(
            root=str(self._root),
            config=_config_without_secrets(config),
            environments=environment_names,
            pipelines=pipelines,
            rules=rules,
            models=models,
            calendars=calendars,
        )

    def get_pipeline(self, ctx: RequestContext, path: str) -> PipelineView:
        require_scope(ctx, SCOPE_PROJECT_READ)
        target = self._resolve_project_path(path)
        if not target.is_file():
            raise ResourceNotFound("Pipeline was not found.")
        try:
            raw_yaml = target.read_text(encoding="utf-8")
        except OSError as exc:
            raise ResourceNotFound("Pipeline could not be read.") from exc
        normalized, issues = parse_schema_localized(
            raw_yaml, base_dir=str(target.parent)
        )
        errors = tuple(
            LocalizedError(code=issue.code, message=issue.message, path=issue.path)
            for issue in issues
        )
        return PipelineView(
            path=target.relative_to(self._root).as_posix(),
            raw_yaml=raw_yaml,
            normalized=normalized,
            errors=errors,
        )

    def json_schema(self, ctx: RequestContext) -> dict[str, Any]:
        require_scope(ctx, SCOPE_PROJECT_READ)
        return generate_json_schema()

    def op_catalog(self, ctx: RequestContext) -> dict[str, Any]:
        require_scope(ctx, SCOPE_PROJECT_READ)
        return {
            "filters": {
                name: {
                    "canonical": spec.canonical,
                    "aliases": sorted(spec.aliases),
                    "arity": spec.arity,
                    "description": spec.description,
                    "requires_raw_sql": spec.requires_raw_sql,
                }
                for name, spec in FILTER_OPERATORS.items()
            },
            "column_ops": {
                name: {
                    "name": spec.name,
                    "arity": spec.arity,
                    "description": spec.description,
                    "requires_raw_sql": spec.requires_raw_sql,
                }
                for name, spec in COLUMN_OPS.items()
            },
            "aggregate_functions": {
                name: {
                    "canonical": spec.canonical,
                    "sql_func": spec.sql_func,
                    "aliases": sorted(spec.aliases),
                    "distinct": spec.distinct,
                    "allows_star": spec.allows_star,
                    "description": spec.description,
                }
                for name, spec in AGGREGATE_FUNCTIONS.items()
            },
        }

    def describe(self, ctx: RequestContext, path: str) -> dict[str, Any]:
        require_scope(ctx, SCOPE_PROJECT_READ)
        pipeline = self._valid_pipeline(ctx, path)
        schema = pipeline.normalized
        assert schema is not None
        description = {
            "tables": [
                {
                    key: table.get(key)
                    for key in ("name", "alias", "source", "source_type", "streaming")
                    if key in table
                }
                for table in schema.get("tables", [])
            ],
            "joins": [
                {
                    key: join.get(key)
                    for key in ("table_from", "on_from", "table_to", "on_to", "type")
                    if key in join
                }
                for join in schema.get("join", [])
            ],
            "rules": list(schema.get("business_rules", [])),
            "output": {
                "select_final": schema.get("select_final"),
                "add_columns": schema.get("add_columns"),
                "keep_all_columns": bool(schema.get("keep_all_columns", False)),
                "aggregate": schema.get("aggregate"),
                "contract": schema.get("contract"),
            },
        }
        return to_json_value(description, "description")

    def project_output(self, ctx: RequestContext, path: str) -> dict[str, Any]:
        require_scope(ctx, SCOPE_PROJECT_READ)
        pipeline = self._valid_pipeline(ctx, path)
        assert pipeline.normalized is not None
        projected = OutputProjector().project(parse_to_ir(pipeline.normalized))
        return {
            "data_product_id": projected.data_product_id,
            "target_hint": projected.target_hint,
            "fields": [
                {
                    "name": field.name,
                    "physical_type": field.physical_type,
                    "logical_type": field.logical_type,
                    "source_fields": list(field.source_fields),
                    "transformations": list(field.transformations),
                    "entity": field.entity,
                    "inference_status": field.inference_status,
                    "inference_reason": field.inference_reason,
                }
                for field in projected.fields
            ],
            "joins": [
                {
                    "alias_left": join.alias_left,
                    "keys_left": list(join.keys_left),
                    "alias_right": join.alias_right,
                    "keys_right": list(join.keys_right),
                    "join_type": join.join_type,
                    "left_table": join.left_table,
                    "right_table": join.right_table,
                }
                for join in projected.joins
            ],
            "grain": list(projected.grain),
            "definition_hash": projected.definition_hash,
            "is_complete": projected.is_complete,
            "incomplete_reason": projected.incomplete_reason,
        }

    def lineage(self, ctx: RequestContext, path: str) -> dict[str, Any]:
        require_scope(ctx, SCOPE_PROJECT_READ)
        pipeline = self._valid_pipeline(ctx, path)
        assert pipeline.normalized is not None
        return LineageTracker.from_schema(pipeline.normalized).to_dict()

    def explain_rules(self, ctx: RequestContext, path: str) -> dict[str, Any]:
        require_scope(ctx, SCOPE_PROJECT_READ)
        pipeline = self._valid_pipeline(ctx, path)
        assert pipeline.normalized is not None
        from skifer.core.core import SkiferEngine

        engine = object.__new__(SkiferEngine)
        return engine.explain_rules_report(pipeline.normalized)

    def audit(
        self, ctx: RequestContext, *, min_coverage: float | None = None
    ) -> AuditReport:
        """Audit project pipeline governance coverage under project:read."""
        require_scope(ctx, SCOPE_PROJECT_READ)
        view = self.open(ctx)
        abs_paths = [str(self._root / relative) for relative in view.pipelines]
        return audit_project(abs_paths)

    def write_pipeline(self, ctx: RequestContext, path: str, text: str) -> str:
        require_scope(ctx, SCOPE_PIPELINES_WRITE)
        if not isinstance(text, str):
            raise InvalidRequest("Pipeline text must be a string.")
        target = self._resolve_project_path(path)
        normalized, issues = parse_schema_localized(text, base_dir=str(target.parent))
        if normalized is None or issues:
            raise InvalidRequest("Pipeline schema is invalid.")

        target.parent.mkdir(parents=True, exist_ok=True)
        temporary_path = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=target.parent,
                prefix=f".{target.stem}.",
                suffix=".tmp",
                delete=False,
            ) as handle:
                temporary_path = handle.name
                handle.write(text)
            os.replace(temporary_path, target)
        except Exception:
            if temporary_path is not None:
                try:
                    os.unlink(temporary_path)
                except FileNotFoundError:
                    pass
            raise
        return str(target)

    def _valid_pipeline(self, ctx: RequestContext, path: str) -> PipelineView:
        pipeline = self.get_pipeline(ctx, path)
        if pipeline.normalized is None:
            raise InvalidRequest("Pipeline schema is invalid.")
        return pipeline

    def _resolve_project_path(self, path: str) -> Path:
        if not isinstance(path, str) or not path.strip():
            raise InvalidRequest("Pipeline path must be non-empty text.")
        candidate = (self._root / path).resolve()
        if not candidate.is_relative_to(self._root):
            raise InvalidRequest("path escapes the project root")
        return candidate

    def _relative_files(self, directory: Path, pattern: str) -> tuple[str, ...]:
        if not directory.is_dir():
            return ()
        return tuple(
            sorted(
                path.relative_to(self._root).as_posix()
                for path in directory.rglob(pattern)
                if path.is_file()
            )
        )

    def _model_keys(self) -> tuple[str, ...]:
        candidates = (
            self._root / "semantic_catalog.yaml",
            self._root / "semantic_models" / "semantic_catalog.yaml",
        )
        catalog_path = next((path for path in candidates if path.is_file()), None)
        if catalog_path is None:
            return ()
        catalog = self._load_mapping(catalog_path, "Semantic catalog")
        models = catalog.get("models", [])
        if not isinstance(models, list):
            raise InvalidRequest("Semantic catalog models must be a list.")
        return tuple(
            sorted(
                entry["key"]
                for entry in models
                if isinstance(entry, dict) and isinstance(entry.get("key"), str)
            )
        )

    def _calendar_keys(self) -> tuple[str, ...]:
        directories = (
            self._root / "calendars",
            self._root / "semantic_models" / "calendars",
        )
        keys = []
        for directory in directories:
            if not directory.is_dir():
                continue
            for path in directory.glob("*.yaml"):
                payload = self._load_mapping(path, "Calendar")
                key = payload.get("key", path.stem)
                if isinstance(key, str):
                    keys.append(key)
        return tuple(sorted(set(keys)))

    @staticmethod
    def _load_mapping(path: Path, label: str) -> dict[str, Any]:
        try:
            payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        except (OSError, yaml.YAMLError) as exc:
            raise InvalidRequest(f"{label} could not be loaded.") from exc
        if not isinstance(payload, dict):
            raise InvalidRequest(f"{label} must be a YAML mapping.")
        return payload


__all__ = ["LocalizedError", "PipelineView", "ProjectService", "ProjectView"]
