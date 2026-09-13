"""Spark-free tests for the semantic application service."""

from types import SimpleNamespace

import pytest
import yaml

from skifer.observability.tracing import TraceContext
from skifer.semantic.sync import SemanticChange, SemanticConflict, SyncReport
from skifer.services import (
    RequestContext,
    SCOPE_MODELS_READ,
    SCOPE_PIPELINES_WRITE,
    SEMANTIC_CONFLICT,
    SEMANTIC_DRIFT,
    SEMANTIC_OK,
    SemanticService,
    ScopeDenied,
)


def _context(*scopes: str) -> RequestContext:
    return RequestContext(
        subject="local-user",
        scopes=frozenset(scopes),
        consumer_class="local",
        trace_context=TraceContext(),
    )


class _Synchronizer:
    def __init__(self, report: SyncReport):
        self.report = report
        self.calls: list[bool] = []

    def sync(self, projected, schema, *, write=False):
        self.calls.append(write)
        return self.report


def _service(tmp_path, report: SyncReport) -> SemanticService:
    service = SemanticService(object(), models_dir=str(tmp_path / "semantic_models"))
    service._synchronizer = _Synchronizer(report)
    service._inspect_pipeline = lambda path: (object(), SimpleNamespace(contract_output=[]))
    return service


def test_check_returns_drift_code(tmp_path):
    report = SyncReport(changes=(SemanticChange(kind="added", target="country"),))
    service = _service(tmp_path, report)

    outcome = service.check(_context(SCOPE_MODELS_READ), "pipeline.yaml")

    assert outcome.code == SEMANTIC_DRIFT
    assert outcome.wrote is False


def test_check_returns_conflict_code(tmp_path):
    report = SyncReport(
        conflicts=(
            SemanticConflict(kind="conflict", message="review", target="country"),
        )
    )
    service = _service(tmp_path, report)

    assert service.check(_context(SCOPE_MODELS_READ), "pipeline.yaml").code == SEMANTIC_CONFLICT


def test_check_returns_ok_code(tmp_path):
    service = _service(tmp_path, SyncReport())

    assert service.check(_context(SCOPE_MODELS_READ), "pipeline.yaml").code == SEMANTIC_OK


def test_check_requires_models_read(tmp_path):
    service = _service(tmp_path, SyncReport())

    with pytest.raises(ScopeDenied):
        service.check(_context(), "pipeline.yaml")


def test_promote_refuses_curation_loss(tmp_path):
    models_dir = tmp_path / "semantic_models"
    models_dir.mkdir()
    curated_path = models_dir / "orders.yaml"
    existing = {
        "models": [
            {
                "key": "orders",
                "name": "orders",
                "description": "Written by a human",
                "dimensions": [{"name": "country", "sql": "country"}],
                "metrics": [],
            }
        ]
    }
    curated_path.write_text(yaml.safe_dump(existing), encoding="utf-8")
    payload = {
        "models": [
            {
                "key": "orders",
                "name": "orders",
                "dimensions": [{"name": "country", "sql": "country"}],
                "metrics": [],
            }
        ]
    }
    report = SyncReport(
        changes=(SemanticChange(kind="changed", target="orders"),),
        payload=payload,
    )
    service = _service(tmp_path, report)
    service._validator = SimpleNamespace(
        validate_against_projection=lambda *args, **kwargs: SimpleNamespace(
            ok=True, errors=[]
        )
    )
    before = curated_path.read_text(encoding="utf-8")

    outcome = service.promote(_context(SCOPE_PIPELINES_WRITE), "pipeline.yaml")

    assert outcome.code == SEMANTIC_CONFLICT
    assert outcome.wrote is False
    assert outcome.report["conflicts"][0]["kind"] == "curation_loss"
    assert curated_path.read_text(encoding="utf-8") == before
    assert not (models_dir / ".drafts" / "orders.yaml").exists()
    assert not (models_dir / "semantic_catalog.yaml").exists()


def test_write_draft_requires_pipelines_write(tmp_path):
    service = _service(tmp_path, SyncReport())

    with pytest.raises(ScopeDenied):
        service.write_draft(_context(), "pipeline.yaml")
