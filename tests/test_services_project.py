"""Spark-free tests for the transport-neutral project service."""

from pathlib import Path

import pytest

from skifer.observability.tracing import TraceContext
from skifer.services import (
    InvalidRequest,
    ProjectService,
    RequestContext,
    SCOPE_PIPELINES_WRITE,
    SCOPE_PROJECT_READ,
    ScopeDenied,
)


VALID_PIPELINE = """\
tables:
  - name: bronze.orders
    alias: ord
select_final:
  - [amount, amount]
"""


def _context(*scopes: str) -> RequestContext:
    return RequestContext(
        subject="local-user",
        scopes=frozenset(scopes),
        consumer_class="local",
        trace_context=TraceContext(),
    )


def _project(tmp_path: Path) -> ProjectService:
    (tmp_path / "config.yaml").write_text(
        "environments:\n  DEV:\n    catalog: dev\napi_token: hidden\n",
        encoding="utf-8",
    )
    return ProjectService(str(tmp_path))


def _write_pipeline(tmp_path: Path, text: str, name: str = "f.yaml") -> str:
    path = tmp_path / "schemas" / "gold" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path.relative_to(tmp_path).as_posix()


def test_open_lists_project_assets(tmp_path):
    service = _project(tmp_path)
    _write_pipeline(tmp_path, VALID_PIPELINE)
    rules = tmp_path / "rules" / "r.py"
    rules.parent.mkdir()
    rules.write_text("def r(df):\n    return df\n", encoding="utf-8")
    calendars = tmp_path / "calendars" / "fy.yaml"
    calendars.parent.mkdir()
    calendars.write_text("key: fiscal\nversion: '1'\nperiods: []\n", encoding="utf-8")
    (tmp_path / "semantic_catalog.yaml").write_text(
        "models:\n  - key: orders\n    file: orders.yaml\n", encoding="utf-8"
    )

    view = service.open(_context(SCOPE_PROJECT_READ))

    assert view.environments == ("DEV",)
    assert view.pipelines == ("schemas/gold/f.yaml",)
    assert view.rules == ("rules/r.py",)
    assert view.models == ("orders",)
    assert view.calendars == ("fiscal",)
    assert view.to_dict()["pipelines"] == ["schemas/gold/f.yaml"]
    assert "api_token" not in view.config


def test_open_requires_project_scope(tmp_path):
    service = _project(tmp_path)
    with pytest.raises(ScopeDenied):
        service.open(_context())


def test_get_pipeline_localized_filter_error(tmp_path):
    service = _project(tmp_path)
    path = _write_pipeline(
        tmp_path,
        """\
tables:
  - name: bronze.orders
    alias: ord
    filter:
      - region:equals:EMEA
      - region:eqals:EMEA
keep_all_columns: true
""",
    )

    view = service.get_pipeline(_context(SCOPE_PROJECT_READ), path)

    assert view.normalized is None
    assert any(
        error.code == "filter.unknown_operator"
        and error.path == "tables[0].filter[1]"
        for error in view.errors
    )


def test_get_pipeline_localized_join_error(tmp_path):
    service = _project(tmp_path)
    path = _write_pipeline(
        tmp_path,
        """\
tables:
  - name: bronze.orders
    alias: ord
  - name: bronze.customers
    alias: cust
join:
  - table_from: [ord, customer_id]
    table_to: [missing, id]
    type: left
keep_all_columns: true
""",
    )

    view = service.get_pipeline(_context(SCOPE_PROJECT_READ), path)

    assert view.normalized is None
    assert any(
        error.code == "join.unknown_alias"
        and error.path == "join[0].table_to"
        for error in view.errors
    )


def test_get_pipeline_localized_contract_error(tmp_path):
    service = _project(tmp_path)
    path = _write_pipeline(
        tmp_path,
        """\
tables:
  - name: bronze.orders
    alias: ord
select_final:
  - [amount, amount]
contract:
  output:
    missing: {}
""",
    )

    view = service.get_pipeline(_context(SCOPE_PROJECT_READ), path)

    assert view.normalized is None
    assert any(
        error.code == "contract.output.unknown_column"
        and error.path == "contract.output.missing"
        for error in view.errors
    )


def test_get_pipeline_valid_returns_normalized(tmp_path):
    service = _project(tmp_path)
    path = _write_pipeline(tmp_path, VALID_PIPELINE)

    view = service.get_pipeline(_context(SCOPE_PROJECT_READ), path)

    assert view.errors == ()
    assert view.normalized is not None
    assert view.raw_yaml == VALID_PIPELINE


def test_path_outside_project_refused(tmp_path):
    service = _project(tmp_path)
    with pytest.raises(InvalidRequest, match="path escapes the project root"):
        service.get_pipeline(_context(SCOPE_PROJECT_READ), "../../etc/passwd")


def test_write_pipeline_atomic(tmp_path):
    service = _project(tmp_path)
    context = _context(SCOPE_PIPELINES_WRITE)
    path = "schemas/gold/f.yaml"

    written = service.write_pipeline(context, path, VALID_PIPELINE)
    assert Path(written).read_text(encoding="utf-8") == VALID_PIPELINE

    with pytest.raises(InvalidRequest):
        service.write_pipeline(
            context,
            path,
            """\
tables:
  - name: bronze.orders
    alias: ord
    filter:
      - region:eqals:EMEA
keep_all_columns: true
""",
        )
    assert Path(written).read_text(encoding="utf-8") == VALID_PIPELINE


def test_write_pipeline_requires_scope(tmp_path):
    service = _project(tmp_path)
    with pytest.raises(ScopeDenied):
        service.write_pipeline(_context(), "schemas/f.yaml", VALID_PIPELINE)


def test_explain_rules_returns_structure_without_stdout(tmp_path, capsys):
    service = _project(tmp_path)
    path = _write_pipeline(
        tmp_path,
        VALID_PIPELINE + "business_rules:\n  - unknown_rule\n",
    )

    report = service.explain_rules(_context(SCOPE_PROJECT_READ), path)

    assert set(report) == {"profiles", "warnings"}
    assert report["profiles"][0]["name"] == "unknown_rule"
    assert capsys.readouterr().out == ""


def test_op_catalog_lists_operators(tmp_path):
    service = _project(tmp_path)
    catalog = service.op_catalog(_context(SCOPE_PROJECT_READ))
    assert catalog["filters"]
    assert catalog["column_ops"]
    assert catalog["aggregate_functions"]


def test_static_inspection_methods_are_json_native(tmp_path):
    service = _project(tmp_path)
    path = _write_pipeline(tmp_path, VALID_PIPELINE)
    context = _context(SCOPE_PROJECT_READ)

    assert service.json_schema(context)["type"] == "object"
    assert service.describe(context, path)["tables"][0]["alias"] == "ord"
    assert service.project_output(context, path)["fields"][0]["name"] == "amount"
    assert isinstance(service.lineage(context, path)["edges"], list)
