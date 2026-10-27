"""
Tests smoke pour la CLI skifer — Phase D.

On teste uniquement les chemins de sortie rapide (pas le REPL interactif).
"""

from __future__ import annotations

import argparse
import builtins
import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

import yaml

from skifer.analyzer.parity import ParityReport
from skifer.cli import (
    ANALYZE_EXIT_ERROR,
    ANALYZE_EXIT_OK,
    ANALYZE_EXIT_PARITY_MISMATCH,
    API_LOOPBACK_HOST,
    AUDIT_EXIT_BELOW_THRESHOLD,
    AUDIT_EXIT_OK,
    INDEX_EXIT_ERROR,
    INDEX_EXIT_OK,
    SEMANTIC_EXIT_CONFLICT,
    SEMANTIC_EXIT_DRIFT,
    SEMANTIC_EXIT_ERROR,
    SEMANTIC_EXIT_OK,
    run_analyze_command,
    run_analyze_history_command,
    run_analyze_parity_command,
    run_contract_command,
    run_audit,
    run_semantic_metric_views,
    run_semantic_sync,
    run_semantic_validate,
    _run_api,
)
from skifer.core.ir import parse_to_ir
from skifer.core.schema_loader import parse_schema
from skifer.semantic.draft_builder import SemanticDraftBuilder
from skifer.semantic.output_projection import OutputProjector

def _run_cli(*args: str) -> subprocess.CompletedProcess:
    """Lance main() via un sous-processus Python avec les arguments donnés."""
    code = (
        f"import sys; sys.argv = {['skifer', *args]!r}; "
        "from skifer.cli import main; main()"
    )
    return subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
    )


def _parse_ir(yaml_text: str):
    return parse_to_ir(parse_schema(yaml_text))


def _build_payload(tmp_path: Path, yaml_text: str) -> dict:
    schema = _parse_ir(yaml_text)
    projected = OutputProjector().project(schema)
    builder = SemanticDraftBuilder(output_dir=str(tmp_path / "semantic_models"))
    return builder.build_draft(projected, schema)


def _write_yaml(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(payload, sort_keys=False, allow_unicode=True, default_flow_style=False),
        encoding="utf-8",
    )


AUDIT_FULLY_COVERED = """\
data_product:
  id: sales.orders
  version: 1.0.0
  owner: sales-data
contract:
  output:
    order_id: {description: Order identifier, classification: internal}
    amount: {description: Order amount, classification: confidential}
tables:
  - name: silver.orders
    alias: ord
select_final:
  - [id, order_id]
  - [amount, amount]
"""


AUDIT_BARE_PIPELINE = """\
tables:
  - name: silver.orders
    alias: ord
select_final:
  - [amount, amount]
"""


def _seed_base_and_curated(
    tmp_path: Path,
    yaml_text: str,
    *,
    draft_mutation=None,
    curated_mutation=None,
) -> tuple[Path, Path]:
    payload = _build_payload(tmp_path, yaml_text)
    model_key = payload["models"][0]["key"]
    draft_path = tmp_path / "semantic_models" / ".drafts" / f"{model_key}.yaml"
    curated_path = tmp_path / "semantic_models" / f"{model_key}.yaml"
    draft_payload = yaml.safe_load(yaml.safe_dump(payload))
    curated_payload = yaml.safe_load(yaml.safe_dump(payload))
    if draft_mutation is not None:
        draft_mutation(draft_payload)
    if curated_mutation is not None:
        curated_mutation(curated_payload)
    _write_yaml(draft_path, draft_payload)
    _write_yaml(curated_path, curated_payload)
    return draft_path, curated_path


def test_hub_help_exits_zero():
    """skifer hub --help doit terminer avec le code 0."""
    result = _run_cli("hub", "--help")
    assert result.returncode == 0
    combined = result.stdout + result.stderr
    assert "config" in combined.lower() or "hub" in combined.lower()


def test_skifer_no_args_exits_zero():
    """skifer sans argument affiche l'aide et retourne un code non nul."""
    result = _run_cli()
    assert result.returncode == 1


def test_hub_no_config_exits_clean(tmp_path):
    """skifer hub avec un config.yaml inexistant → message clair, pas de traceback."""
    result = _run_cli("hub", "--config", str(tmp_path / "nonexistent.yaml"))
    # Ne doit pas crasher avec une traceback Python non gérée
    assert "Traceback" not in result.stdout
    assert "Traceback" not in result.stderr
    # Doit indiquer que la config est introuvable
    combined = result.stdout + result.stderr
    assert any(
        kw in combined.lower()
        for kw in ("introuvable", "not found", "config", "erreur", "error")
    )
    assert result.returncode != 0


def test_cli_main_importable():
    """Le module cli doit être importable sans erreur."""
    result = subprocess.run(
        [sys.executable, "-c", "from skifer.cli import main; print('ok')"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert "ok" in result.stdout


def test_api_serve_help_exits_zero():
    result = _run_cli("api", "serve", "--help")
    assert result.returncode == 0
    assert "--project" in result.stdout
    assert "--port" in result.stdout


def test_api_serve_refuses_non_loopback_host_flag(tmp_path):
    result = _run_cli(
        "api",
        "serve",
        "--project",
        str(tmp_path),
        "--host",
        "0.0.0.0",
    )
    assert result.returncode == 2
    assert "unrecognized arguments: --host 0.0.0.0" in result.stderr


def test_api_serve_uses_fixed_loopback_bind(monkeypatch, tmp_path):
    calls = {}
    fake_app = object()
    monkeypatch.setattr("skifer.api.app.create_app", lambda _project: fake_app)
    monkeypatch.setitem(
        sys.modules,
        "uvicorn",
        SimpleNamespace(run=lambda app, **kwargs: calls.update(app=app, **kwargs)),
    )

    _run_api(
        argparse.Namespace(
            api_command="serve",
            project=str(tmp_path),
            port=8123,
        )
    )

    assert calls == {
        "app": fake_app,
        "host": API_LOOPBACK_HOST,
        "port": 8123,
        "log_config": None,
    }
    assert API_LOOPBACK_HOST == "127.0.0.1"


def test_api_openapi_command_prints_json(tmp_path):
    pytest.importorskip("fastapi")
    result = _run_cli("api", "openapi", "--project", str(tmp_path))
    assert result.returncode == 0, result.stderr
    document = json.loads(result.stdout)
    assert document["openapi"].startswith("3.")
    assert document["info"]["version"] == "0"
    assert list(tmp_path.glob(".skifer_*.db")) == []


def test_api_command_without_extra_reports_dependency(monkeypatch, tmp_path, capsys):
    original_import = builtins.__import__

    def missing_fastapi(name, *args, **kwargs):
        if name == "fastapi" or name.startswith("fastapi."):
            raise ImportError
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", missing_fastapi)
    with pytest.raises(SystemExit) as raised:
        _run_api(
            argparse.Namespace(
                api_command="serve",
                project=str(tmp_path),
                port=8000,
            )
        )
    assert raised.value.code == 1
    assert 'pip install -e ".[api]"' in capsys.readouterr().err


def test_mcp_serve_help_still_works():
    result = _run_cli("mcp", "serve", "--help")
    assert result.returncode == 0
    assert "--transport" in result.stdout
    assert "--config" in result.stdout


def test_cli_contract_import_prints_yaml_block(tmp_path, capsys):
    contract_file = tmp_path / "contract.yaml"
    contract_file.write_text(
        yaml.safe_dump(
            {
                "apiVersion": "v3.1.0",
                "kind": "DataContract",
                "id": "sales.orders",
                "version": "1.0.0",
                "schema": {"properties": [{"name": "order_id", "logicalType": "identifier"}]},
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    exit_code = run_contract_command(
        argparse.Namespace(contract_command="import", file=str(contract_file))
    )

    assert exit_code == 0
    payload = yaml.safe_load(capsys.readouterr().out)
    assert payload["data_product"] == {"id": "sales.orders", "version": "1.0.0"}
    assert payload["contract"]["output"]["order_id"]["logical_type"] == "identifier"


def test_cli_contract_import_missing_file_exit_1(tmp_path, capsys):
    exit_code = run_contract_command(
        argparse.Namespace(contract_command="import", file=str(tmp_path / "missing.yaml"))
    )

    assert exit_code == 1
    assert "Import failed:" in capsys.readouterr().out


def test_cli_contract_import_malformed_yaml_exit_1(tmp_path, capsys):
    contract_file = tmp_path / "malformed.yaml"
    contract_file.write_text("schema: [unterminated\n", encoding="utf-8")

    exit_code = run_contract_command(
        argparse.Namespace(contract_command="import", file=str(contract_file))
    )

    assert exit_code == 1
    assert "Import failed:" in capsys.readouterr().out


def test_cli_contract_missing_subcommand_exit_2(capsys):
    exit_code = run_contract_command(argparse.Namespace(contract_command=None))

    assert exit_code == 2
    assert "Contract command missing" in capsys.readouterr().out


# ==============================================================================
# skifer validate — Plan 18-4.2
def test_validate_valid_schema_exits_zero(tmp_path):
    """skifer validate with a valid schema file exits 0."""
    schema_file = tmp_path / "test.yaml"
    schema_file.write_text("""
tables:
  - name: silver.orders
    alias: ord
select_final:
  - [amount, amount_eur, [cast:double]]
""")
    result = _run_cli("validate", str(schema_file))
    assert result.returncode == 0, result.stdout + result.stderr
    assert "OK" in result.stdout


def test_validate_invalid_schema_exits_one(tmp_path):
    """skifer validate with an invalid schema exits 1 and reports the error."""
    schema_file = tmp_path / "bad.yaml"
    schema_file.write_text("""
tables:
  - name: silver.orders
    alias: ord
    filter:
      - region:badoperator:EMEA
""")
    result = _run_cli("validate", str(schema_file))
    assert result.returncode == 1
    assert "FAIL" in result.stdout
    combined = result.stdout + result.stderr
    assert any(kw in combined for kw in ("unknown", "badoperator", "error", "invalid"))


def test_validate_multiple_files(tmp_path):
    """skifer validate aggregates results across multiple files."""
    good = tmp_path / "good.yaml"
    good.write_text("tables:\n  - name: silver.orders\n    alias: ord\n")
    bad = tmp_path / "bad.yaml"
    bad.write_text("tables:\n  - name: silver.orders\n    filter:\n      - x:noop:y\n")
    result = _run_cli("validate", str(good), str(bad))
    assert result.returncode == 1
    assert "OK" in result.stdout
    assert "FAIL" in result.stdout
    assert "1 passed, 1 failed" in result.stdout


def test_validate_missing_file_exits_one(tmp_path):
    """skifer validate with a non-existent file reports an error."""
    result = _run_cli("validate", str(tmp_path / "nonexistent.yaml"))
    assert result.returncode == 1
    combined = result.stdout + result.stderr
    assert "FAIL" in combined or "error" in combined.lower()


def test_validate_template_params_auto_injected(tmp_path):
    """skifer validate auto-injects sentinel params for {{ key }} placeholders."""
    schema_file = tmp_path / "templated.yaml"
    schema_file.write_text("""
tables:
  - name: "{{ catalog }}.silver.orders"
    alias: ord
select_final:
  - [amount, amount_eur]
""")
    result = _run_cli("validate", str(schema_file))
    assert result.returncode == 0, result.stdout + result.stderr


def test_validate_summary_line(tmp_path):
    """skifer validate prints 'N passed, N failed' summary."""
    good = tmp_path / "a.yaml"
    good.write_text("tables:\n  - name: silver.orders\n")
    result = _run_cli("validate", str(good))
    assert "passed" in result.stdout
    assert "failed" in result.stdout


def test_validate_help_exits_zero():
    """skifer validate --help exits 0."""
    result = _run_cli("validate", "--help")
    assert result.returncode == 0


def test_index_subcommand_indexes_without_spark(tmp_path):
    schema_file = tmp_path / "orders.yaml"
    db_path = tmp_path / "metadata.db"
    schema_file.write_text(
        """
data_product: {id: sales.orders, version: 1.0.0}
tables: [{name: silver.orders, alias: ord}]
select_final:
  - [id, order_id]
""",
        encoding="utf-8",
    )

    first = _run_cli("index", str(schema_file), "--db", str(db_path))
    second = _run_cli("index", str(schema_file), "--db", str(db_path))

    assert first.returncode == INDEX_EXIT_OK, first.stdout + first.stderr
    assert second.returncode == INDEX_EXIT_OK, second.stdout + second.stderr
    assert "updated" in first.stdout
    assert "unchanged" in second.stdout


def test_index_bad_db_path_exits_cleanly(tmp_path):
    result = _run_cli(
        "index",
        str(tmp_path / "schema.yaml"),
        "--db",
        str(tmp_path / "missing" / "metadata.db"),
    )

    assert result.returncode == INDEX_EXIT_ERROR
    assert "Traceback" not in result.stdout + result.stderr
    assert "Failed to open metadata registry" in result.stderr


def test_audit_cli_exit_zero_without_threshold(tmp_path, capsys):
    pipeline = tmp_path / "covered.yaml"
    pipeline.write_text(AUDIT_FULLY_COVERED, encoding="utf-8")

    exit_code = run_audit([str(pipeline)], as_json=False, min_coverage=None)

    assert exit_code == AUDIT_EXIT_OK
    assert "overall" in capsys.readouterr().out


def test_audit_cli_exit_two_below_threshold(tmp_path, capsys):
    pipeline = tmp_path / "bare.yaml"
    pipeline.write_text(AUDIT_BARE_PIPELINE, encoding="utf-8")

    exit_code = run_audit([str(pipeline)], as_json=False, min_coverage=50.0)

    assert exit_code == AUDIT_EXIT_BELOW_THRESHOLD
    assert "0.00%" in capsys.readouterr().out


def test_audit_cli_exit_zero_at_or_above_threshold(tmp_path, capsys):
    pipeline = tmp_path / "covered.yaml"
    pipeline.write_text(AUDIT_FULLY_COVERED, encoding="utf-8")

    exit_code = run_audit([str(pipeline)], as_json=False, min_coverage=80.0)

    assert exit_code == AUDIT_EXIT_OK
    assert "80.00%" in capsys.readouterr().out


def test_audit_cli_json_stable(tmp_path, capsys):
    first = tmp_path / "a.yaml"
    second = tmp_path / "b.yaml"
    first.write_text(AUDIT_FULLY_COVERED, encoding="utf-8")
    second.write_text(AUDIT_BARE_PIPELINE, encoding="utf-8")

    exit_code = run_audit([str(second), str(first)], as_json=True, min_coverage=None)

    assert exit_code == AUDIT_EXIT_OK
    output = capsys.readouterr().out
    payload = json.loads(output)
    assert output.strip() == json.dumps(
        payload,
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
    )
    assert payload["pipelines"][0]["path"] == str(first)


def test_audit_cli_no_files_exits_zero(tmp_path, capsys):
    exit_code = run_audit(
        [str(tmp_path / "none*.yaml")],
        as_json=False,
        min_coverage=90.0,
    )

    assert exit_code == AUDIT_EXIT_OK
    assert "No schema files found." in capsys.readouterr().out


def test_audit_cli_end_to_end(tmp_path):
    pipeline = tmp_path / "covered.yaml"
    pipeline.write_text(AUDIT_FULLY_COVERED, encoding="utf-8")

    result = _run_cli("audit", str(pipeline), "--min-coverage", "100")

    assert result.returncode == AUDIT_EXIT_BELOW_THRESHOLD
    assert "Audited 1 pipeline(s)" in result.stdout
    assert "overall" in result.stdout


BASE_SYNC_YAML = """
data_product: {id: sales.orders, version: 1.0.0}
contract:
  grain: [country]
  output:
    country: {logical_type: string, description: Country}
    orders: {description: Orders}
semantic:
  model_key: orders_sync
tables: [{name: silver.orders}]
aggregate:
  group_by: [country]
  measures:
    - [order_id, orders, count_distinct]
sink: {type: delta, schema: gold, table: fact_orders_sync}
"""


DRIFT_SYNC_YAML = """
data_product: {id: sales.orders, version: 1.0.0}
contract:
  grain: [country]
  output:
    country: {logical_type: string, description: Country}
    orders: {description: Orders}
    total_amount: {description: Total amount}
semantic:
  model_key: orders_sync
tables: [{name: silver.orders}]
aggregate:
  group_by: [country]
  measures:
    - [order_id, orders, count_distinct]
    - [amount, total_amount, sum]
sink: {type: delta, schema: gold, table: fact_orders_sync}
"""


CONFLICT_BASE_YAML = """
data_product: {id: sales.orders, version: 1.0.0}
contract:
  grain: [order_id]
  output:
    order_id: {logical_type: identifier}
    net_revenue: {logical_type: integer}
semantic:
  model_key: orders_sync
  dimensions: [order_id, net_revenue]
tables: [{name: silver.orders}]
select_final:
  - [id, order_id]
  - [amount, net_revenue]
sink: {type: delta, schema: gold, table: fact_orders_sync}
"""


CONFLICT_NEXT_YAML = """
data_product: {id: sales.orders, version: 1.0.0}
contract:
  grain: [order_id]
  output:
    order_id: {logical_type: identifier}
semantic:
  model_key: orders_sync
  dimensions: [order_id]
tables: [{name: silver.orders}]
select_final:
  - [id, order_id]
sink: {type: delta, schema: gold, table: fact_orders_sync}
"""


def test_semantic_sync_check_exit_zero_when_draft_is_current(tmp_path, monkeypatch, capsys):
    pipeline = tmp_path / "pipeline.yaml"
    pipeline.write_text(BASE_SYNC_YAML, encoding="utf-8")
    _seed_base_and_curated(tmp_path, BASE_SYNC_YAML)
    monkeypatch.chdir(tmp_path)

    exit_code = run_semantic_sync(str(pipeline), mode="check")

    assert exit_code == SEMANTIC_EXIT_OK
    assert "0 change(s), 0 conflict(s), 0 suggestion(s)" in capsys.readouterr().out


def test_semantic_sync_check_exit_two_and_writes_nothing(tmp_path, monkeypatch, capsys):
    pipeline = tmp_path / "pipeline.yaml"
    pipeline.write_text(DRIFT_SYNC_YAML, encoding="utf-8")
    before_paths = sorted(str(path.relative_to(tmp_path)) for path in tmp_path.rglob("*"))
    monkeypatch.chdir(tmp_path)

    exit_code = run_semantic_sync(str(pipeline), mode="check")

    after_paths = sorted(str(path.relative_to(tmp_path)) for path in tmp_path.rglob("*"))
    assert exit_code == SEMANTIC_EXIT_DRIFT
    assert before_paths == after_paths
    assert not (tmp_path / "semantic_models").exists()
    assert "CHANGE" in capsys.readouterr().out


def test_semantic_sync_write_draft_writes_when_safe_and_returns_drift(tmp_path, monkeypatch):
    pipeline = tmp_path / "pipeline.yaml"
    pipeline.write_text(DRIFT_SYNC_YAML, encoding="utf-8")
    monkeypatch.chdir(tmp_path)

    exit_code = run_semantic_sync(str(pipeline), mode="write-draft")

    assert exit_code == SEMANTIC_EXIT_DRIFT
    assert (tmp_path / "semantic_models" / ".drafts" / "orders_sync.yaml").exists()
    assert not (tmp_path / "semantic_models" / "orders_sync.yaml").exists()


def test_semantic_sync_exit_three_on_conflict(tmp_path, monkeypatch, capsys):
    pipeline = tmp_path / "pipeline.yaml"
    pipeline.write_text(CONFLICT_NEXT_YAML, encoding="utf-8")

    def add_curated_metric(payload: dict) -> None:
        payload["models"][0]["metrics"].append(
            {
                "name": "m",
                "sql": "net_revenue",
                "type": "sum",
                "description": "Curated revenue metric",
            }
        )

    _seed_base_and_curated(tmp_path, CONFLICT_BASE_YAML, curated_mutation=add_curated_metric)
    monkeypatch.chdir(tmp_path)

    exit_code = run_semantic_sync(str(pipeline), mode="check")

    assert exit_code == SEMANTIC_EXIT_CONFLICT
    assert "Semantic sync conflict: metric 'm' depends on removed column 'net_revenue'." in capsys.readouterr().out


def test_semantic_sync_exit_one_on_technical_failure(tmp_path, monkeypatch, capsys):
    pipeline = tmp_path / "missing.yaml"
    monkeypatch.chdir(tmp_path)

    exit_code = run_semantic_sync(str(pipeline), mode="check")

    assert exit_code == SEMANTIC_EXIT_ERROR
    assert "Failed to inspect pipeline" in capsys.readouterr().out


def test_semantic_sync_promote_refuses_on_conflict_without_writing(tmp_path, monkeypatch, capsys):
    pipeline = tmp_path / "pipeline.yaml"
    pipeline.write_text(CONFLICT_NEXT_YAML, encoding="utf-8")

    def add_curated_metric(payload: dict) -> None:
        payload["models"][0]["metrics"].append(
            {
                "name": "m",
                "sql": "net_revenue",
                "type": "sum",
                "description": "Curated revenue metric",
            }
        )

    draft_path, curated_path = _seed_base_and_curated(
        tmp_path,
        CONFLICT_BASE_YAML,
        curated_mutation=add_curated_metric,
    )
    before_draft = draft_path.read_text(encoding="utf-8")
    before_curated = curated_path.read_text(encoding="utf-8")
    monkeypatch.chdir(tmp_path)

    exit_code = run_semantic_sync(str(pipeline), mode="promote")

    assert exit_code == SEMANTIC_EXIT_CONFLICT
    assert draft_path.read_text(encoding="utf-8") == before_draft
    assert curated_path.read_text(encoding="utf-8") == before_curated
    assert not (tmp_path / "semantic_models" / "semantic_catalog.yaml").exists()
    assert "Resolve the report, then rerun --promote." in capsys.readouterr().out


def test_semantic_sync_promote_refuses_on_validation_failure(tmp_path, monkeypatch, capsys):
    pipeline = tmp_path / "pipeline.yaml"
    pipeline.write_text(BASE_SYNC_YAML, encoding="utf-8")

    def invalidate(payload: dict) -> None:
        payload["models"][0]["metrics"] = []

    draft_path, curated_path = _seed_base_and_curated(
        tmp_path,
        BASE_SYNC_YAML,
        draft_mutation=invalidate,
        curated_mutation=invalidate,
    )
    before_draft = draft_path.read_text(encoding="utf-8")
    before_curated = curated_path.read_text(encoding="utf-8")
    monkeypatch.chdir(tmp_path)

    exit_code = run_semantic_sync(str(pipeline), mode="promote")

    assert exit_code == SEMANTIC_EXIT_ERROR
    assert draft_path.read_text(encoding="utf-8") == before_draft
    assert curated_path.read_text(encoding="utf-8") == before_curated
    assert not (tmp_path / "semantic_models" / "semantic_catalog.yaml").exists()
    assert "Refusing to promote curated model 'orders_sync'" in capsys.readouterr().out


def test_semantic_sync_promote_updates_curated_model_and_catalog(tmp_path, monkeypatch, capsys):
    pipeline = tmp_path / "pipeline.yaml"
    pipeline.write_text(DRIFT_SYNC_YAML, encoding="utf-8")
    monkeypatch.chdir(tmp_path)

    exit_code = run_semantic_sync(str(pipeline), mode="promote")

    assert exit_code == SEMANTIC_EXIT_OK
    draft_path = tmp_path / "semantic_models" / ".drafts" / "orders_sync.yaml"
    curated_path = tmp_path / "semantic_models" / "orders_sync.yaml"
    catalog_path = tmp_path / "semantic_models" / "semantic_catalog.yaml"
    assert draft_path.exists()
    assert curated_path.exists()
    assert catalog_path.exists()

    catalog = yaml.safe_load(catalog_path.read_text(encoding="utf-8"))
    assert catalog["_total_models"] == 1
    assert catalog["models"][0]["key"] == "orders_sync"
    assert catalog["models"][0]["file"] == "orders_sync.yaml"
    assert catalog["models"][0]["dimensions"] == ["country"]
    assert catalog["models"][0]["metrics"] == ["orders", "total_amount"]
    assert "updated semantic_catalog.yaml" in capsys.readouterr().out


def test_semantic_validate_reports_contract_output_error_shape(tmp_path, monkeypatch, capsys):
    pipeline = tmp_path / "pipeline.yaml"
    pipeline.write_text(
        """
data_product: {id: sales.orders, version: 1.0.0}
contract:
  grain: [country]
  output:
    missing_column: {logical_type: string}
semantic:
  model_key: orders_sync
tables: [{name: silver.orders}]
aggregate:
  group_by: [country]
  measures:
    - [order_id, orders, count_distinct]
sink: {type: delta, schema: gold, table: fact_orders_sync}
""",
        encoding="utf-8",
    )
    model = tmp_path / "orders_sync.yaml"
    model.write_text(
        yaml.safe_dump(_build_payload(tmp_path, BASE_SYNC_YAML), sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)

    exit_code = run_semantic_validate(str(pipeline), str(model))

    assert exit_code == SEMANTIC_EXIT_ERROR
    assert "[contract.output] column 'missing_column' is not produced by the pipeline." in capsys.readouterr().out


def test_semantic_validate_reports_semantic_dimension_error_shape(tmp_path, monkeypatch, capsys):
    pipeline = tmp_path / "pipeline.yaml"
    pipeline.write_text(BASE_SYNC_YAML, encoding="utf-8")
    payload = _build_payload(tmp_path, BASE_SYNC_YAML)
    payload["models"][0]["dimensions"].append(
        {
            "name": "missing_dimension",
            "sql": "missing_dimension",
            "type": "string",
        }
    )
    model = tmp_path / "orders_sync.yaml"
    model.write_text(
        yaml.safe_dump(payload, sort_keys=False, allow_unicode=True, default_flow_style=False),
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)

    exit_code = run_semantic_validate(str(pipeline), str(model))

    assert exit_code == SEMANTIC_EXIT_ERROR
    assert "[semantic.dimensions] 'missing_dimension' is not a projected output." in capsys.readouterr().out


def _write_metric_views_catalog(models_dir: Path, models: list[dict]) -> None:
    for model in models:
        (models_dir / f"{model['key']}.yaml").write_text(
            yaml.safe_dump({"models": [model]}), encoding="utf-8"
        )
    catalog = {"models": [{"key": m["key"], "file": f"{m['key']}.yaml"} for m in models]}
    (models_dir / "semantic_catalog.yaml").write_text(yaml.safe_dump(catalog), encoding="utf-8")


def _metric_view_model(key: str, table: str, **extra) -> dict:
    return {
        "key": key,
        "table": table,
        "dimensions": [{"name": "region", "sql": "region"}],
        "metrics": [{"name": "total_revenue", "sql": "revenue", "type": "sum"}],
        **extra,
    }


def test_semantic_metric_views_prints_the_ddl_without_touching_spark(tmp_path, capsys):
    models_dir = tmp_path / "semantic_models"
    models_dir.mkdir()
    _write_metric_views_catalog(models_dir, [_metric_view_model("sales.orders", "gold.kpi_orders")])

    exit_code = run_semantic_metric_views(str(models_dir), "sts_demo_catalog", "semantic")

    assert exit_code == SEMANTIC_EXIT_OK
    out = capsys.readouterr().out
    assert "CREATE OR REPLACE VIEW `sts_demo_catalog`.`semantic`.`orders`" in out
    assert "SUM(revenue)" in out


def test_semantic_metric_views_model_flag_restricts_to_one_model(tmp_path, capsys):
    models_dir = tmp_path / "semantic_models"
    models_dir.mkdir()
    _write_metric_views_catalog(
        models_dir,
        [_metric_view_model("sales.orders", "gold.kpi_orders"), _metric_view_model("sales.customers", "gold.kpi_customers")],
    )

    exit_code = run_semantic_metric_views(
        str(models_dir), "sts_demo_catalog", "semantic", model_key="sales.customers"
    )

    assert exit_code == SEMANTIC_EXIT_OK
    out = capsys.readouterr().out
    assert "`customers`" in out
    assert "`orders`" not in out


def test_semantic_metric_views_unknown_model_key_is_an_error(tmp_path, capsys):
    models_dir = tmp_path / "semantic_models"
    models_dir.mkdir()
    _write_metric_views_catalog(models_dir, [_metric_view_model("sales.orders", "gold.kpi_orders")])

    exit_code = run_semantic_metric_views(
        str(models_dir), "sts_demo_catalog", "semantic", model_key="sales.unknown"
    )

    assert exit_code == SEMANTIC_EXIT_ERROR
    assert "'sales.unknown' not found" in capsys.readouterr().out


def test_semantic_metric_views_exit_one_on_a_translation_refusal(tmp_path, capsys):
    models_dir = tmp_path / "semantic_models"
    models_dir.mkdir()
    _write_metric_views_catalog(
        models_dir,
        [
            _metric_view_model("sales.orders", "gold.kpi_orders", relationships=[{"name": "r", "to": {}}]),
            _metric_view_model("sales.customers", "gold.kpi_customers"),
        ],
    )

    exit_code = run_semantic_metric_views(str(models_dir), "sts_demo_catalog", "semantic")

    out = capsys.readouterr().out
    assert exit_code == SEMANTIC_EXIT_ERROR
    assert "sales.orders" in out and "relationships" in out
    # Un modèle refusé par la traduction ne bloque pas l'affichage des autres.
    assert "`customers`" in out


def test_semantic_metric_views_missing_catalog_is_a_clean_error(tmp_path, capsys):
    exit_code = run_semantic_metric_views(str(tmp_path / "does-not-exist"), "c", "semantic")

    assert exit_code == SEMANTIC_EXIT_ERROR
    assert "Failed to read catalog" in capsys.readouterr().out


def test_semantic_sync_promote_preserves_human_curation_on_rerun(tmp_path, monkeypatch, capsys):
    # Re-promoting an unchanged pipeline used to overwrite the curated model
    # with the un-curated draft, silently destroying every human description.
    pipeline = tmp_path / "pipeline.yaml"
    pipeline.write_text(DRIFT_SYNC_YAML, encoding="utf-8")
    monkeypatch.chdir(tmp_path)

    assert run_semantic_sync(str(pipeline), mode="promote") == SEMANTIC_EXIT_OK

    curated_path = tmp_path / "semantic_models" / "orders_sync.yaml"
    curated = yaml.safe_load(curated_path.read_text(encoding="utf-8"))
    curated["models"][0]["description"] = "Curated by a human"
    curated["models"][0]["dimensions"][0]["description"] = "Curated dimension"
    curated_path.write_text(
        yaml.safe_dump(curated, sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )
    capsys.readouterr()

    exit_code = run_semantic_sync(str(pipeline), mode="promote")

    assert exit_code == SEMANTIC_EXIT_OK
    assert "already current — nothing to promote" in capsys.readouterr().out
    after = yaml.safe_load(curated_path.read_text(encoding="utf-8"))["models"][0]
    assert after["description"] == "Curated by a human"
    assert after["dimensions"][0]["description"] == "Curated dimension"


def test_semantic_sync_promote_refuses_to_drop_curated_content(tmp_path, monkeypatch, capsys):
    # Structural backstop: even if the merge ever regressed, promotion must
    # refuse rather than silently drop curated keys.
    from skifer.cli import _assert_no_curation_loss

    curated_path = tmp_path / "orders_sync.yaml"
    curated_path.write_text(
        yaml.safe_dump(
            {
                "models": [
                    {
                        "name": "orders_sync",
                        "key": "orders_sync",
                        "description": "Curated by a human",
                        "dimensions": [
                            {"name": "country", "sql": "country", "description": "Curated dim"}
                        ],
                        "metrics": [{"name": "orders", "sql": "order_id", "type": "count_distinct"}],
                    }
                ]
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    stripped = {
        "models": [
            {
                "name": "orders_sync",
                "key": "orders_sync",
                "dimensions": [{"name": "country", "sql": "country"}],
                "metrics": [{"name": "orders", "sql": "order_id", "type": "count_distinct"}],
            }
        ]
    }

    with pytest.raises(ValueError) as exc_info:
        _assert_no_curation_loss(curated_path, stripped)

    message = str(exc_info.value)
    assert "description" in message
    assert "dimensions.country.description" in message


def test_analyze_help_exits_zero():
    result = _run_cli("analyze", "--help")

    assert result.returncode == 0
    assert "--inventory" in result.stdout


def test_analyze_without_inventory_flag_is_a_noop(tmp_path, capsys):
    sql_path = tmp_path / "a.sql"
    sql_path.write_text("SELECT 1;\n")
    args = argparse.Namespace(paths=[str(sql_path)], inventory=False, dialect="databricks")

    exit_code = run_analyze_command(args)

    assert exit_code == ANALYZE_EXIT_OK
    assert "Nothing to do" in capsys.readouterr().out


def test_analyze_inventory_prints_join_and_function_counts(tmp_path, capsys):
    sql_path = tmp_path / "a.sql"
    sql_path.write_text(
        "SELECT o.id, UPPER(c.country) AS country\n"
        "FROM silver.orders o\n"
        "LEFT JOIN silver.customers c ON o.customer_id = c.id;\n"
    )
    args = argparse.Namespace(paths=[str(sql_path)], inventory=True, dialect="databricks")

    exit_code = run_analyze_command(args)

    output = capsys.readouterr().out
    assert exit_code == ANALYZE_EXIT_OK
    assert "1 statement(s) analyzed" in output
    assert "left: 1" in output
    assert "upper: 1" in output


def test_analyze_expands_glob_patterns(tmp_path, capsys):
    (tmp_path / "a.sql").write_text("SELECT 1;\n")
    (tmp_path / "b.sql").write_text("SELECT 2;\n")
    args = argparse.Namespace(
        paths=[str(tmp_path / "*.sql")], inventory=True, dialect="databricks"
    )

    exit_code = run_analyze_command(args)

    output = capsys.readouterr().out
    assert exit_code == ANALYZE_EXIT_OK
    assert "2 statement(s) analyzed" in output


def test_analyze_non_matching_glob_falls_back_to_literal_path_and_errors(tmp_path, capsys):
    # A pattern that expands to nothing is treated as a literal path, so a
    # typo'd glob surfaces as a clear "cannot read" error instead of a silent
    # "nothing found" that could mask a mistake.
    pattern = str(tmp_path / "nothing_here_*.sql")
    args = argparse.Namespace(paths=[pattern], inventory=True, dialect="databricks")

    exit_code = run_analyze_command(args)

    err = capsys.readouterr().err
    assert exit_code == ANALYZE_EXIT_ERROR
    assert "Cannot read" in err
    assert pattern in err


def test_analyze_bad_statement_names_file_and_line_and_exits_error(tmp_path, capsys):
    sql_path = tmp_path / "bad.sql"
    sql_path.write_text("SELECT 1;\nSELECT FROM WHERE;\n")
    args = argparse.Namespace(paths=[str(sql_path)], inventory=True, dialect="databricks")

    exit_code = run_analyze_command(args)

    err = capsys.readouterr().err
    assert exit_code == ANALYZE_EXIT_ERROR
    assert str(sql_path) in err
    assert "2" in err


def test_analyze_without_inventory_or_dry_run_is_a_noop(tmp_path, capsys):
    sql_path = tmp_path / "a.sql"
    sql_path.write_text("SELECT 1;\n")
    args = argparse.Namespace(
        paths=[str(sql_path)], inventory=False, dry_run=False, dialect="databricks",
    )

    exit_code = run_analyze_command(args)

    assert exit_code == ANALYZE_EXIT_OK
    assert "Nothing to do" in capsys.readouterr().out


def test_analyze_dry_run_markdown_clean_statement_exits_zero(tmp_path, capsys):
    sql_path = tmp_path / "a.sql"
    sql_path.write_text("SELECT id FROM bronze.orders WHERE status = 'ACTIVE';\n")
    args = argparse.Namespace(
        paths=[str(sql_path)], inventory=False, dry_run=True, format="md", dialect="databricks",
    )

    exit_code = run_analyze_command(args)

    output = capsys.readouterr().out
    assert exit_code == ANALYZE_EXIT_OK
    assert "# Project analyzer report" in output
    assert "verified" in output


def test_analyze_dry_run_needs_curation_exits_two(tmp_path, capsys):
    sql_path = tmp_path / "a.sql"
    sql_path.write_text("SELECT id FROM a UNION SELECT id FROM b;\n")
    args = argparse.Namespace(
        paths=[str(sql_path)], inventory=False, dry_run=True, format="md", dialect="databricks",
    )

    exit_code = run_analyze_command(args)

    output = capsys.readouterr().out
    assert exit_code == 2
    assert "needs_curation" in output
    assert "union_requires_multi_schema" in output


def test_analyze_dry_run_json_format_is_valid_json(tmp_path, capsys):
    sql_path = tmp_path / "a.sql"
    sql_path.write_text("SELECT id FROM bronze.orders WHERE status = 'ACTIVE';\n")
    args = argparse.Namespace(
        paths=[str(sql_path)], inventory=False, dry_run=True, format="json", dialect="databricks",
    )

    exit_code = run_analyze_command(args)

    out = capsys.readouterr().out
    json_text = out[out.index("{"):]
    payload = json.loads(json_text)
    assert exit_code == ANALYZE_EXIT_OK
    assert payload["statement_count"] == 1
    assert payload["clean_count"] == 1


def test_analyze_help_mentions_dry_run_and_format():
    result = _run_cli("analyze", "--help")

    assert result.returncode == 0
    assert "--dry-run" in result.stdout
    assert "--format" in result.stdout
    assert "--split" in result.stdout


def test_analyze_dry_run_split_markdown_appends_decoupage_section(tmp_path, capsys):
    sql_path = tmp_path / "a.sql"
    sql_path.write_text("WITH c AS (SELECT a FROM t) SELECT a FROM c WHERE a > 1 UNION ALL SELECT a FROM c WHERE a > 1;\n")
    args = argparse.Namespace(
        paths=[str(sql_path)], inventory=False, dry_run=True, format="md", dialect="databricks", split=True,
    )

    exit_code = run_analyze_command(args)

    output = capsys.readouterr().out
    # The unsplit statement itself is needs_curation (a bare CTE) — the
    # split report is what shows its CTE resolving into a shared partial.
    assert exit_code == 2
    assert "# Project analyzer report" in output
    assert "## Découpage" in output
    assert "```mermaid" in output


def test_analyze_dry_run_split_json_adds_split_key(tmp_path, capsys):
    sql_path = tmp_path / "a.sql"
    sql_path.write_text("SELECT id FROM bronze.orders WHERE status = 'ACTIVE';\n")
    args = argparse.Namespace(
        paths=[str(sql_path)], inventory=False, dry_run=True, format="json", dialect="databricks", split=True,
    )

    exit_code = run_analyze_command(args)

    out = capsys.readouterr().out
    payload = json.loads(out[out.index("{"):])
    assert exit_code == ANALYZE_EXIT_OK
    assert "split" in payload
    assert "dag_mermaid" in payload["split"]


def test_analyze_dry_run_without_split_flag_omits_decoupage_section(tmp_path, capsys):
    sql_path = tmp_path / "a.sql"
    sql_path.write_text("SELECT id FROM bronze.orders WHERE status = 'ACTIVE';\n")
    args = argparse.Namespace(
        paths=[str(sql_path)], inventory=False, dry_run=True, format="md", dialect="databricks",
    )

    exit_code = run_analyze_command(args)

    output = capsys.readouterr().out
    assert exit_code == ANALYZE_EXIT_OK
    assert "Découpage" not in output


def test_analyze_unreadable_file_exits_error(tmp_path, capsys):
    args = argparse.Namespace(
        paths=[str(tmp_path / "does_not_exist.sql")], inventory=True, dialect="databricks"
    )

    exit_code = run_analyze_command(args)

    assert exit_code == ANALYZE_EXIT_ERROR
    assert "Cannot read" in capsys.readouterr().err


def test_analyze_without_sqlglot_reports_install_hint(tmp_path, capsys):
    sql_path = tmp_path / "a.sql"
    sql_path.write_text("SELECT 1;\n")
    code = (
        f"import sys; sys.argv = {['skifer', 'analyze', str(sql_path), '--inventory']!r}; "
        "import builtins; _real_import = builtins.__import__\n"
        "def _blocking_import(name, *args, **kwargs):\n"
        "    if name == 'sqlglot' or name.startswith('sqlglot.'):\n"
        "        raise ImportError('simulated: sqlglot not installed')\n"
        "    return _real_import(name, *args, **kwargs)\n"
        "builtins.__import__ = _blocking_import\n"
        "from skifer.cli import main; main()"
    )
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)

    assert result.returncode == ANALYZE_EXIT_ERROR
    assert 'pip install -e ".[analyzer]"' in result.stderr
    assert "Traceback" not in result.stderr


def _emit_args(tmp_path, paths, *, out, strict=False, min_occurrences=3, min_scripts=2, enrich=False):
    return argparse.Namespace(
        paths=paths, inventory=False, dry_run=False, emit=True, dialect="databricks",
        out=str(out), strict=strict, min_occurrences=min_occurrences, min_scripts=min_scripts,
        enrich=enrich,
    )


def test_analyze_emit_clean_corpus_exits_zero(tmp_path, capsys):
    sql_path = tmp_path / "a.sql"
    sql_path.write_text("CREATE TABLE silver.t AS SELECT id FROM bronze.t_raw;\n")
    out_dir = tmp_path / "out"

    exit_code = run_analyze_command(_emit_args(tmp_path, [str(sql_path)], out=out_dir))

    assert exit_code == ANALYZE_EXIT_OK
    assert (out_dir / "schemas" / ".drafts" / "silver" / "t.yaml").exists()


def test_analyze_emit_residue_exits_two_and_still_writes_what_it_can(tmp_path, capsys):
    sql_path = tmp_path / "a.sql"
    sql_path.write_text(
        "CREATE TABLE silver.t AS SELECT id FROM bronze.t_raw;\n"
        "SELECT id FROM bronze.other;\n"  # no CREATE/INSERT target -> skipped, a residue.
    )
    out_dir = tmp_path / "out"

    exit_code = run_analyze_command(_emit_args(tmp_path, [str(sql_path)], out=out_dir))

    output = capsys.readouterr().out
    assert exit_code == 2
    assert '"skipped"' in output
    assert (out_dir / "schemas" / ".drafts" / "silver" / "t.yaml").exists()


def test_analyze_emit_strict_refuses_outright_and_writes_nothing(tmp_path, capsys):
    sql_path = tmp_path / "a.sql"
    sql_path.write_text(
        "CREATE TABLE silver.t AS SELECT id FROM bronze.t_raw;\n"
        "SELECT id FROM bronze.other;\n"
    )
    out_dir = tmp_path / "out"

    exit_code = run_analyze_command(_emit_args(tmp_path, [str(sql_path)], out=out_dir, strict=True))

    assert exit_code == 2
    assert "refusing to write anything" in capsys.readouterr().err
    assert not out_dir.exists() or list(out_dir.rglob("*")) == []


def test_analyze_emit_refuses_existing_file_without_marker(tmp_path, capsys):
    sql_path = tmp_path / "a.sql"
    sql_path.write_text("CREATE TABLE silver.t AS SELECT id FROM bronze.t_raw;\n")
    out_dir = tmp_path / "out"
    draft_dir = out_dir / "schemas" / ".drafts" / "silver"
    draft_dir.mkdir(parents=True)
    (draft_dir / "t.yaml").write_text("tables: []\n")

    exit_code = run_analyze_command(_emit_args(tmp_path, [str(sql_path)], out=out_dir))

    assert exit_code == 4
    assert "without the project_analyzer marker" in capsys.readouterr().err
    assert (draft_dir / "t.yaml").read_text() == "tables: []\n"


def test_analyze_emit_is_reproducible_on_a_fresh_parse(tmp_path, capsys):
    sql_path = tmp_path / "a.sql"
    sql_path.write_text("CREATE TABLE silver.t AS SELECT id FROM bronze.t_raw;\n")
    out_dir = tmp_path / "out"

    run_analyze_command(_emit_args(tmp_path, [str(sql_path)], out=out_dir))
    capsys.readouterr()
    exit_code = run_analyze_command(_emit_args(tmp_path, [str(sql_path)], out=out_dir))

    output = capsys.readouterr().out
    assert exit_code == ANALYZE_EXIT_OK
    assert '"status": "unchanged"' in output


def test_analyze_help_mentions_emit_flags():
    result = _run_cli("analyze", "--help")

    assert result.returncode == 0
    assert "--emit" in result.stdout
    assert "--out" in result.stdout
    assert "--min-occurrences" in result.stdout
    assert "--strict" in result.stdout
    assert "--enrich" in result.stdout


def test_analyze_emit_enrich_without_a_configured_provider_is_a_silent_noop(tmp_path, monkeypatch, capsys):
    """No LLM_PROVIDER / API keys in this test's environment -> get_llm_provider()
    raises -> _run_analyze_enrich falls back to provider=None -> empty result,
    same exit code as without --enrich."""
    monkeypatch.delenv("LLM_PROVIDER", raising=False)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)
    monkeypatch.delenv("DATABRICKS_HOST", raising=False)
    monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
    sql_path = tmp_path / "a.sql"
    sql_path.write_text("CREATE TABLE silver.t AS SELECT id FROM bronze.t_raw;\n")
    out_dir = tmp_path / "out"

    exit_code = run_analyze_command(_emit_args(tmp_path, [str(sql_path)], out=out_dir, enrich=True))

    stdout = capsys.readouterr().out
    output = json.loads(stdout[stdout.index("{"):])
    assert exit_code == ANALYZE_EXIT_OK
    assert output["enrichment"] == {"rules": [], "drafts": [], "groupings": []}


def test_analyze_emit_without_enrich_flag_has_no_enrichment_key(tmp_path, capsys):
    sql_path = tmp_path / "a.sql"
    sql_path.write_text("CREATE TABLE silver.t AS SELECT id FROM bronze.t_raw;\n")
    out_dir = tmp_path / "out"

    run_analyze_command(_emit_args(tmp_path, [str(sql_path)], out=out_dir, enrich=False))

    stdout = capsys.readouterr().out
    output = json.loads(stdout[stdout.index("{"):])
    assert "enrichment" not in output


def test_analyze_emit_enrich_with_a_configured_provider_populates_the_report(tmp_path, monkeypatch, capsys):
    sql_path = tmp_path / "a.sql"
    sql_path.write_text("CREATE TABLE silver.t AS SELECT id FROM bronze.t_raw;\n")
    out_dir = tmp_path / "out"

    class _FakeProvider:
        def complete(self, *, system_prompt, user_message, temperature=0.0, **kwargs):
            if "Output columns" in user_message:
                return json.dumps({"description": "Raw ids promoted to silver."})
            return json.dumps({"name": "noop", "docstring": "d"})

    monkeypatch.setattr("skifer.semantic.llm_provider.get_llm_provider", lambda *a, **k: _FakeProvider())

    exit_code = run_analyze_command(_emit_args(tmp_path, [str(sql_path)], out=out_dir, enrich=True))

    stdout = capsys.readouterr().out
    output = json.loads(stdout[stdout.index("{"):])
    assert exit_code == ANALYZE_EXIT_OK
    assert output["enrichment"]["drafts"][0]["description"] == "Raw ids promoted to silver."


def test_analyze_parity_help_shows_required_flags():
    result = _run_cli("analyze", "parity", "--help")

    assert result.returncode == 0
    assert "--against" in result.stdout
    assert "--target-layer" in result.stdout
    assert "--key" in result.stdout


def test_analyze_parity_missing_required_flag_is_a_usage_error(tmp_path):
    draft_path = tmp_path / "draft.yaml"
    draft_path.write_text("tables: []\n")

    result = _run_cli("analyze", "parity", str(draft_path), "--against", "cat.silver.legacy")

    assert result.returncode == 2  # argparse's own usage-error code
    assert "--target-layer" in result.stderr


def test_analyze_plain_usage_is_unaffected_by_the_parity_interception(tmp_path):
    sql_path = tmp_path / "a.sql"
    sql_path.write_text("CREATE TABLE silver.t AS SELECT id FROM bronze.t_raw;\n")

    result = _run_cli("analyze", str(sql_path), "--dry-run")

    assert result.returncode == ANALYZE_EXIT_OK


def _fake_parity_report(*, identical: bool) -> ParityReport:
    same_or_different = ("same", "same") if identical else ("a", "b")
    return ParityReport(
        draft_fqn="cat.silver._skifer_parity_x",
        legacy_fqn="cat.silver.legacy",
        key=("id",),
        draft_row_count=3,
        legacy_row_count=3,
        draft_columns=("id",),
        legacy_columns=("id",),
        draft_row_hash=f"sha256:v1:{same_or_different[0]}",
        legacy_row_hash=f"sha256:v1:{same_or_different[1]}",
        rows_only_in_draft=0,
        rows_only_in_legacy=0,
    )


def _parity_args(draft_path, *, key="id"):
    return argparse.Namespace(
        draft=str(draft_path), against="cat.silver.legacy", target_layer="silver",
        key=key, config=None,
    )


def test_run_analyze_parity_command_exits_ok_when_identical(tmp_path, mocker, capsys):
    draft_path = tmp_path / "draft.yaml"
    draft_path.write_text("tables:\n  - name: bronze.t\n    alias: t\nkeep_all_columns: true\n")
    mock_runner = mocker.patch("skifer.analyzer.parity.ParityRunner")
    mock_runner.return_value.run.return_value = _fake_parity_report(identical=True)

    exit_code = run_analyze_parity_command(_parity_args(draft_path), engine=SimpleNamespace(default_params={}))

    assert exit_code == ANALYZE_EXIT_OK
    assert '"identical": true' in capsys.readouterr().out
    _, kwargs = mock_runner.return_value.run.call_args
    assert kwargs["key"] == ["id"]
    assert kwargs["against"] == "cat.silver.legacy"


def test_run_analyze_parity_command_exits_mismatch_when_not_identical(tmp_path, mocker, capsys):
    draft_path = tmp_path / "draft.yaml"
    draft_path.write_text("tables:\n  - name: bronze.t\n    alias: t\nkeep_all_columns: true\n")
    mocker.patch("skifer.analyzer.parity.ParityRunner").return_value.run.return_value = _fake_parity_report(
        identical=False
    )

    exit_code = run_analyze_parity_command(_parity_args(draft_path), engine=SimpleNamespace(default_params={}))

    assert exit_code == ANALYZE_EXIT_PARITY_MISMATCH
    assert '"identical": false' in capsys.readouterr().out


def test_run_analyze_parity_command_exits_error_on_missing_draft(tmp_path, capsys):
    exit_code = run_analyze_parity_command(
        _parity_args(tmp_path / "missing.yaml"), engine=SimpleNamespace(default_params={}),
    )

    assert exit_code == ANALYZE_EXIT_ERROR
    assert "failed" in capsys.readouterr().err


def test_run_analyze_parity_command_without_key_passes_none(tmp_path, mocker):
    draft_path = tmp_path / "draft.yaml"
    draft_path.write_text("tables:\n  - name: bronze.t\n    alias: t\nkeep_all_columns: true\n")
    mock_runner = mocker.patch("skifer.analyzer.parity.ParityRunner")
    mock_runner.return_value.run.return_value = _fake_parity_report(identical=True)

    run_analyze_parity_command(_parity_args(draft_path, key=None), engine=SimpleNamespace(default_params={}))

    _, kwargs = mock_runner.return_value.run.call_args
    assert kwargs["key"] is None


def test_analyze_history_help_shows_required_flags():
    result = _run_cli("analyze", "history", "--help")

    assert result.returncode == 0
    assert "--warehouse-id" in result.stdout
    assert "--days" in result.stdout
    assert "--schema" in result.stdout


def test_analyze_history_missing_required_flag_is_a_usage_error():
    result = _run_cli("analyze", "history", "--days", "7")

    assert result.returncode == 2  # argparse's own usage-error code
    assert "--warehouse-id" in result.stderr


def test_analyze_plain_usage_is_unaffected_by_the_history_interception(tmp_path):
    sql_path = tmp_path / "a.sql"
    sql_path.write_text("CREATE TABLE silver.t AS SELECT id FROM bronze.t_raw;\n")

    result = _run_cli("analyze", str(sql_path), "--dry-run")

    assert result.returncode == ANALYZE_EXIT_OK


class _FakeStatementExecutionForHistory:
    def __init__(self, rows):
        self.rows = rows

    def execute_statement(self, *, statement, warehouse_id, wait_timeout):
        from types import SimpleNamespace

        return SimpleNamespace(
            statement_id="stmt-1",
            status=SimpleNamespace(state=SimpleNamespace(value="SUCCEEDED"), error=None),
            result=SimpleNamespace(data_array=self.rows),
        )


def _history_args(*, inventory=False, dry_run=False, emit=False, out=".", schema=None, days=7):
    return argparse.Namespace(
        warehouse_id="wh1", days=days, schema=schema, profile=None, dialect="databricks",
        inventory=inventory, dry_run=dry_run, split=False, emit=emit, out=out, format="json",
        min_occurrences=3, min_scripts=2, strict=False, enrich=False,
    )


def test_run_analyze_history_command_nothing_to_do_is_ok(capsys):
    exit_code = run_analyze_history_command(_history_args(), workspace_client=None)

    assert exit_code == ANALYZE_EXIT_OK
    assert "Nothing to do" in capsys.readouterr().out


def test_run_analyze_history_command_dry_run_reports_the_fetched_statement(capsys):
    from types import SimpleNamespace

    client = SimpleNamespace(
        statement_execution=_FakeStatementExecutionForHistory(
            [["id1", "SELECT id FROM silver.orders", "2026-01-01"]]
        )
    )

    exit_code = run_analyze_history_command(_history_args(dry_run=True), workspace_client=client)

    stdout = capsys.readouterr().out
    payload = json.loads(stdout[stdout.index("{"):])
    assert exit_code == ANALYZE_EXIT_OK
    assert payload["statement_count"] == 1


def test_run_analyze_history_command_fetch_failure_is_an_error(capsys):
    class _FailingStatementExecution:
        def execute_statement(self, **kwargs):
            raise RuntimeError("warehouse unreachable")

    from types import SimpleNamespace

    client = SimpleNamespace(statement_execution=_FailingStatementExecution())

    exit_code = run_analyze_history_command(_history_args(dry_run=True), workspace_client=client)

    assert exit_code == ANALYZE_EXIT_ERROR
    assert "fetch failed" in capsys.readouterr().err


def test_run_analyze_history_command_missing_sdk_without_an_injected_client_is_an_error(capsys, monkeypatch):
    def _raise_import_error(*, profile):
        raise ImportError('simulated: `pip install -e ".[databricks]"`')

    monkeypatch.setattr("skifer.analyzer.history.build_workspace_client", _raise_import_error)

    exit_code = run_analyze_history_command(_history_args(dry_run=True))

    assert exit_code == ANALYZE_EXIT_ERROR
    assert "databricks" in capsys.readouterr().err


def test_core_cli_help_does_not_import_optional_sqlglot():
    code = r"""
import importlib.abc
import sys

class BlockSqlglot(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == "sqlglot" or fullname.startswith("sqlglot."):
            raise AssertionError("optional sqlglot dependency imported")
        return None

sys.meta_path.insert(0, BlockSqlglot())
import skifer
from skifer.cli import main
sys.argv = ["skifer", "--help"]
try:
    main()
except SystemExit as exc:
    assert exc.code == 0
assert "sqlglot" not in sys.modules
"""
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)

    assert result.returncode == 0, result.stdout + result.stderr
