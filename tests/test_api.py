from __future__ import annotations

import ast
import builtins
import importlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from skifer.api.app import (
    APIDependencyError,
    API_SCHEMA_VERSION,
    API_TITLE,
    create_app,
    openapi_document,
)
from skifer.api.errors import error_payload
from skifer.observability.tracing import TraceContext
from skifer.services import (
    InvalidRequest,
    NAMED_SCOPES,
    RequestContext,
    ResourceNotFound,
    ResourceUnavailable,
    ScopeDenied,
)
from skifer.services.project import ProjectService


class _View:
    def __init__(self, **payload):
        self.payload = payload or {"ok": True}

    def to_dict(self):
        return dict(self.payload)


class _Project:
    def open(self, _ctx):
        return _View(config={"name": "test"})

    def json_schema(self, _ctx):
        return {"type": "object"}

    def op_catalog(self, _ctx):
        return {"filters": {}}

    def get_pipeline(self, _ctx, path):
        return _View(path=path, normalized={}, errors=[])

    def describe(self, _ctx, path):
        return {"path": path}

    project_output = describe
    explain_rules = describe

    def write_pipeline(self, _ctx, path, _text):
        return path


class _Rules:
    def list(self, _ctx):
        return (_View(name="rule"),)

    def dependency_graph(self, _ctx, names):
        return {"names": list(names)}

    def generate_snippet(self, _ctx, _spec):
        return "code"

    def write_rule(self, _ctx, file, _code):
        return file


class _Semantic:
    def list_models(self, _ctx, **_kwargs):
        return _View(items=[])

    def get_model(self, _ctx, key):
        return _View(key=key)

    def check(self, _ctx, _path):
        return _View(code=0)

    write_draft = check
    promote = check


class _DataService:
    def query(self, _ctx, _query, **_kwargs):
        return _View(rows=[])

    def get_lineage(self, _ctx, dataset, column):
        return _View(dataset=dataset, column=column)


class _Governance:
    def dictionary(self, _ctx, dataset):
        return {"target_fqn": dataset, "columns": []}

    def get_contract(self, _ctx, contract_id, version):
        return _View(contract_id=contract_id, version=version)

    def versions(self, _ctx, contract_id):
        return _View(contract_id=contract_id, versions=[])

    def get_certification(self, _ctx, dataset):
        return _View(dataset=dataset)

    def list_certification_history(self, _ctx, _dataset, **_kwargs):
        return []

    def list_data_products(self, _ctx):
        return []

    def get_data_product(self, _ctx, data_product_id):
        return _View(data_product_id=data_product_id)


class _Quality:
    def checks_from_schema(self, _ctx, _schema):
        return ()

    def history(self, _ctx, _table, **_kwargs):
        return ()

    def last_report(self, _ctx, _table):
        return None

    def list_incidents(self, _ctx, **_kwargs):
        return ()

    def acknowledge_incident(self, _ctx, incident_id):
        return _View(id=incident_id)

    def assign_incident(self, _ctx, incident_id, _assignee):
        return _View(id=incident_id)

    def resolve_incident(self, _ctx, incident_id, _root_cause):
        return _View(id=incident_id)


class _Agents:
    def ask(self, _ctx, question, **_kwargs):
        return {"question": question}

    def build(self, _ctx, description, **_kwargs):
        return {"description": description}


class _Execution:
    def connect(self, _ctx, **_kwargs):
        return _View(state="ready")

    def session(self, _ctx):
        return None

    def submit(self, _ctx, _kind, _path, _params):
        return "job"

    def status(self, _ctx, job_id):
        return _View(job_id=job_id)

    cancel = status

    def logs(self, _ctx, job_id, **_kwargs):
        return _View(job_id=job_id, entries=[])

    result = status


def _services():
    return SimpleNamespace(
        project=_Project(),
        rules=_Rules(),
        semantic=_Semantic(),
        governance=_Governance(),
        quality=_Quality(),
        agents=_Agents(),
        execution=_Execution(),
        data_service=_DataService(),
    )


def _client(tmp_path, **kwargs):
    pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient

    return TestClient(create_app(str(tmp_path), services=_services(), **kwargs))


def test_import_skifer_api_without_fastapi_is_safe():
    assert importlib.import_module("skifer.api").create_app is create_app


def test_create_app_without_extra_raises_dependency_error(monkeypatch, tmp_path):
    original_import = builtins.__import__

    def missing_fastapi(name, *args, **kwargs):
        if name == "fastapi" or name.startswith("fastapi."):
            raise ImportError
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", missing_fastapi)
    with pytest.raises(APIDependencyError, match=r'pip install -e "\.\[api\]"'):
        create_app(str(tmp_path), services=_services())


def test_health_without_spark(monkeypatch, tmp_path):
    pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient

    def fail_if_spark_is_constructed(*_args, **_kwargs):
        raise AssertionError("health must not construct a Spark session")

    monkeypatch.setattr(
        "skifer.spark_factory.get_spark_session",
        fail_if_spark_is_constructed,
    )
    response = TestClient(create_app(str(tmp_path))).get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def _normalized_openapi(document: dict) -> dict:
    methods = {"get", "put", "post", "delete", "options", "head", "patch", "trace"}
    return {
        "info": {
            "title": document["info"]["title"],
            "version": document["info"]["version"],
        },
        "operations": sorted(
            [path, method.upper()]
            for path, item in document["paths"].items()
            for method in item
            if method.lower() in methods
        ),
        "paths": sorted(document["paths"]),
    }


def test_openapi_snapshot_matches(tmp_path):
    pytest.importorskip("fastapi")
    document = openapi_document(str(tmp_path))
    actual = json.dumps(
        _normalized_openapi(document),
        sort_keys=True,
        indent=2,
        ensure_ascii=False,
    ) + "\n"
    snapshot = Path(__file__).parent / "data" / "openapi_snapshot.json"
    assert actual == snapshot.read_text(encoding="utf-8")


def test_openapi_info_uses_fixed_contract_version(tmp_path):
    pytest.importorskip("fastapi")
    document = openapi_document(str(tmp_path))
    assert document["info"] == {
        "title": API_TITLE,
        "version": API_SCHEMA_VERSION,
    }
    assert API_SCHEMA_VERSION == "0"


@pytest.mark.parametrize(
    ("method", "path", "kwargs"),
    [
        ("get", "/health", {}),
        ("get", "/project", {}),
        ("get", "/config", {}),
        ("get", "/project/json-schema", {}),
        ("get", "/project/op-catalog", {}),
        ("get", "/pipelines/a.yaml", {}),
        ("get", "/pipelines/a.yaml/describe", {}),
        ("get", "/pipelines/a.yaml/output", {}),
        ("get", "/pipelines/a.yaml/explain-rules", {}),
        ("put", "/pipelines/a.yaml", {"json": {"text": "x"}}),
        ("get", "/rules", {}),
        ("get", "/rules/graph", {}),
        ("post", "/rules/snippet", {"json": {"kind": "upper", "target": "name"}}),
        ("put", "/rules/a.py", {"json": {"code": "x"}}),
        ("get", "/catalog", {}),
        ("get", "/catalog/model", {}),
        ("post", "/semantic/query", {"json": {"model_name": "model"}}),
        ("post", "/semantic/check", {"json": {"pipeline_path": "a.yaml"}}),
        ("post", "/semantic/write-draft", {"json": {"pipeline_path": "a.yaml"}}),
        ("post", "/semantic/promote", {"json": {"pipeline_path": "a.yaml"}}),
        ("get", "/lineage/table/column", {}),
        ("get", "/dictionary/table", {}),
        ("post", "/quality/checks", {"json": {}}),
        ("get", "/quality/history/table", {}),
        ("get", "/quality/last/table", {}),
        ("get", "/incidents", {}),
        ("post", "/incidents/1/ack", {}),
        ("post", "/incidents/1/assign", {"json": {"assignee": "u"}}),
        ("post", "/incidents/1/resolve", {"json": {"root_cause": "fixed"}}),
        ("get", "/contracts/c/1.0.0", {}),
        ("get", "/contracts/c", {}),
        ("get", "/certifications/table", {}),
        ("get", "/certifications/table/history", {}),
        ("get", "/data-products", {}),
        ("get", "/data-products/p", {}),
        ("post", "/agents/ask", {"json": {"question": "q"}}),
        ("post", "/agents/build", {"json": {"description": "d"}}),
        ("get", "/me", {}),
        ("post", "/session/connect", {"json": {}}),
        ("get", "/session", {}),
        ("post", "/jobs", {"json": {"kind": "run", "path": "a.yaml"}}),
        ("get", "/jobs/j", {}),
        ("post", "/jobs/j/cancel", {}),
        ("get", "/jobs/j/logs", {}),
        ("get", "/jobs/j/result", {}),
    ],
)
def test_every_route_exercised_returns_json(tmp_path, method, path, kwargs):
    response = getattr(_client(tmp_path), method)(path, **kwargs)
    assert response.status_code == 200, response.text
    response.json()


def test_refused_scope_returns_403_with_missing_scope(tmp_path):
    ctx = RequestContext("u", frozenset({"project:read"}), "test", TraceContext())
    response = _client(tmp_path, context_provider=lambda _request: ctx).put(
        "/pipelines/foo.yaml", json={"text": "x"}
    )
    assert response.status_code == 403
    assert response.json() == {
        "code": "scope_denied",
        "message": "Scope 'pipelines:write' is required.",
        "path": "/pipelines/foo.yaml",
    }


def test_pipeline_invalid_yaml_returns_200_with_localized_errors(tmp_path):
    schemas = tmp_path / "schemas"
    schemas.mkdir()
    (tmp_path / "config.yaml").write_text("environments: {}\n", encoding="utf-8")
    (schemas / "bad.yaml").write_text(
        """\
tables:
  - name: bronze.orders
    alias: ord
    filter:
      - region:eqals:EMEA
keep_all_columns: true
""",
        encoding="utf-8",
    )
    services = _services()
    services.project = ProjectService(str(tmp_path))
    pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient

    response = TestClient(create_app(str(tmp_path), services=services)).get(
        "/pipelines/schemas/bad.yaml"
    )
    assert response.status_code == 200
    body = response.json()
    assert body["normalized"] is None
    assert body["errors"]
    assert set(body["errors"][0]) == {"code", "message", "path"}


@pytest.mark.parametrize(
    ("exc", "status", "code"),
    [
        (ScopeDenied("denied"), 403, "scope_denied"),
        (ResourceNotFound("missing"), 404, "not_found"),
        (InvalidRequest("bad"), 400, "invalid_request"),
        (ResourceUnavailable("down"), 503, "unavailable"),
    ],
)
def test_service_error_mapping(exc, status, code):
    actual_status, body = error_payload(exc, "/x")
    assert actual_status == status
    assert body["code"] == code
    assert set(body) == {"code", "message", "path"}


def test_cors_allows_localhost_only(tmp_path):
    client = _client(tmp_path)
    headers = {"Origin": "http://localhost:5173", "Access-Control-Request-Method": "GET"}
    assert client.options("/project", headers=headers).headers[
        "access-control-allow-origin"
    ] == "http://localhost:5173"
    headers["Origin"] = "http://evil.example"
    assert "access-control-allow-origin" not in client.options(
        "/project", headers=headers
    ).headers


def test_no_route_without_scope(tmp_path):
    pytest.importorskip("fastapi")
    from fastapi.routing import APIRoute
    from skifer.api.security import Scope

    excluded = {"/health", "/openapi.json", "/docs", "/redoc", "/docs/oauth2-redirect"}

    def dependencies(node):
        yield from node.dependencies
        for child in node.dependencies:
            yield from dependencies(child)

    app = create_app(str(tmp_path), services=_services())
    for route in app.routes:
        if isinstance(route, APIRoute) and route.path not in excluded:
            scopes = [dep.call for dep in dependencies(route.dependant) if isinstance(dep.call, Scope)]
            assert len(scopes) == 1, route.path
            assert scopes[0].scope in NAMED_SCOPES


def test_api_import_architecture():
    root = Path(__file__).parents[1] / "src" / "skifer" / "api"
    forbidden = (
        "SkiferEngine",
        "SemanticEngine",
        "spark_backend",
        "SparkBackend",
        "from skifer.core",
        "import skifer.core",
        "from skifer.observability",
        "certification_store",
        "history import",
        "import pyspark",
        "spark_factory",
    )
    for path in root.rglob("*.py"):
        source = path.read_text(encoding="utf-8")
        assert not any(pattern in source for pattern in forbidden), path
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module:
                if node.module.startswith("skifer."):
                    assert node.module.startswith(("skifer.api", "skifer.services")), path
