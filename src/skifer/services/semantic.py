"""Transport-neutral semantic synchronization façade."""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import re
from typing import Any

from skifer.core.ir import parse_to_ir
from skifer.core.schema_loader import parse_schema
from skifer.semantic.output_projection import OutputProjector, ProjectedSchema
from skifer.semantic.persistence import build_catalog_entry, write_yaml_atomic
from skifer.semantic.semantic import SemanticEngine
from skifer.semantic.sync import (
    SemanticSynchronizer,
    SyncReport,
    assert_no_curation_loss,
)
from skifer.semantic.validator import SemanticValidator
from skifer.services.context import (
    InvalidRequest,
    RequestContext,
    SCOPE_MODELS_READ,
    SCOPE_PIPELINES_WRITE,
    require_scope,
)
from skifer.services.serialization import to_json_value


SEMANTIC_OK = 0
SEMANTIC_DRIFT = 2
SEMANTIC_CONFLICT = 3

_PLACEHOLDER_RE = re.compile(r"\{\{\s*(\w+)\s*\}\}")


def semantic_query_from_dict(payload: dict[str, Any]) -> Any:
    """Build the service query DTO while keeping transports domain-import free."""
    from skifer.agentic.resolver import SemanticQuery

    if not isinstance(payload, dict):
        raise InvalidRequest("Semantic query body must be an object.")
    try:
        return SemanticQuery.from_dict(payload)
    except (KeyError, TypeError, ValueError) as exc:
        raise InvalidRequest("Semantic query body is invalid.") from exc


@dataclass(frozen=True)
class SyncOutcome:
    code: int
    report: dict[str, Any]
    wrote: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "report": to_json_value(self.report, "report"),
            "wrote": self.wrote,
        }


class SemanticService:
    """Inspect, draft, and promote semantic models without exposing engines."""

    def __init__(self, data_service: Any, *, models_dir: str = "semantic_models"):
        self._data_service = data_service
        self._models_dir = os.path.abspath(models_dir)
        self._synchronizer = SemanticSynchronizer(output_dir=self._models_dir)
        self._validator = SemanticValidator()

    def check(self, ctx: RequestContext, pipeline_path: str) -> SyncOutcome:
        require_scope(ctx, SCOPE_MODELS_READ)
        projected, schema = self._inspect_pipeline(pipeline_path)
        report = self._synchronizer.sync(projected, schema, write=False)
        return self._outcome(report)

    def write_draft(self, ctx: RequestContext, pipeline_path: str) -> SyncOutcome:
        require_scope(ctx, SCOPE_PIPELINES_WRITE)
        projected, schema = self._inspect_pipeline(pipeline_path)
        report = self._synchronizer.sync(projected, schema, write=False)
        if report.conflicts or report.suggestions:
            return self._outcome(report)
        write_report = self._synchronizer.sync(projected, schema, write=True)
        return self._outcome(write_report)

    def promote(self, ctx: RequestContext, pipeline_path: str) -> SyncOutcome:
        require_scope(ctx, SCOPE_PIPELINES_WRITE)
        projected, schema = self._inspect_pipeline(pipeline_path)
        report = self._synchronizer.sync(projected, schema, write=False)
        if report.conflicts or report.suggestions:
            return self._outcome(report)
        if report.payload is None:
            raise InvalidRequest("Semantic sync produced no managed draft payload.")

        validation = self._validator.validate_against_projection(
            report.payload,
            projected,
            contract_output=[field.name for field in schema.contract_output],
        )
        if not validation.ok:
            details = "; ".join(str(error) for error in validation.errors)
            raise InvalidRequest(
                "Semantic model validation failed against the pipeline projection"
                + (f": {details}" if details else ".")
            )

        model_key = self._model_key(report.payload)
        draft_path = Path(self._models_dir) / ".drafts" / f"{model_key}.yaml"
        curated_path = Path(self._models_dir) / f"{model_key}.yaml"
        if curated_path.exists() and not report.has_changes:
            return self._outcome(report, code=SEMANTIC_OK)

        try:
            assert_no_curation_loss(curated_path, report.payload)
        except ValueError as exc:
            return self._outcome(
                report,
                code=SEMANTIC_CONFLICT,
                extra_conflict={
                    "kind": "curation_loss",
                    "message": str(exc),
                    "target": model_key,
                    "details": {},
                },
            )

        write_yaml_atomic(draft_path, report.payload)
        write_yaml_atomic(curated_path, report.payload)
        SemanticEngine.update_catalog(
            self._models_dir, build_catalog_entry(report.payload)
        )
        return self._outcome(report, code=SEMANTIC_OK, wrote=True)

    def list_models(
        self, ctx: RequestContext, cursor: str | None = None, limit: int = 50
    ) -> Any:
        require_scope(ctx, SCOPE_MODELS_READ)
        return self._data_service.list_models(ctx, cursor=cursor, limit=limit)

    def get_model(self, ctx: RequestContext, key: str) -> Any:
        require_scope(ctx, SCOPE_MODELS_READ)
        return self._data_service.get_model(ctx, key)

    @staticmethod
    def _inspect_pipeline(pipeline_path: str) -> tuple[ProjectedSchema, Any]:
        yaml_text = Path(pipeline_path).read_text(encoding="utf-8")
        params = {
            key: f"__sentinel_{key}__"
            for key in _PLACEHOLDER_RE.findall(yaml_text)
        }
        normalized = parse_schema(
            yaml_text,
            params=params,
            base_dir=str(Path(pipeline_path).resolve().parent),
        )
        schema = parse_to_ir(normalized)
        return OutputProjector().project(schema), schema

    @staticmethod
    def _model_key(payload: dict[str, Any]) -> str:
        models = payload.get("models")
        if not isinstance(models, list) or not models or not isinstance(models[0], dict):
            raise InvalidRequest("Semantic sync payload has no model.")
        key = models[0].get("key")
        if not isinstance(key, str) or not key:
            raise InvalidRequest("Semantic sync payload model has no key.")
        return key

    @classmethod
    def _outcome(
        cls,
        report: SyncReport,
        *,
        code: int | None = None,
        wrote: bool | None = None,
        extra_conflict: dict[str, Any] | None = None,
    ) -> SyncOutcome:
        report_view = cls._report_to_dict(report)
        if extra_conflict is not None:
            report_view["conflicts"].append(extra_conflict)
        if code is None:
            if report.conflicts or report.suggestions or extra_conflict is not None:
                code = SEMANTIC_CONFLICT
            elif report.has_changes:
                code = SEMANTIC_DRIFT
            else:
                code = SEMANTIC_OK
        return SyncOutcome(
            code=code,
            report=report_view,
            wrote=report.wrote if wrote is None else wrote,
        )

    @staticmethod
    def _report_to_dict(report: SyncReport) -> dict[str, Any]:
        def change_view(change: Any) -> dict[str, Any]:
            return {
                "kind": change.kind,
                "target": change.target,
                "before": to_json_value(change.before, "change.before"),
                "after": to_json_value(change.after, "change.after"),
                "details": to_json_value(change.details, "change.details"),
            }

        def conflict_view(conflict: Any) -> dict[str, Any]:
            return {
                "kind": conflict.kind,
                "message": conflict.message,
                "target": conflict.target,
                "details": to_json_value(conflict.details, "conflict.details"),
            }

        return {
            "changes": [change_view(change) for change in report.changes],
            "conflicts": [
                conflict_view(conflict) for conflict in report.conflicts
            ],
            "suggestions": [
                change_view(suggestion) for suggestion in report.suggestions
            ],
            "has_changes": report.has_changes,
        }


__all__ = [
    "SEMANTIC_OK",
    "SEMANTIC_DRIFT",
    "SEMANTIC_CONFLICT",
    "SyncOutcome",
    "SemanticService",
    "semantic_query_from_dict",
]
