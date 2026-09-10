"""
Tests smoke pour la CLI skifer — Phase D.

On teste uniquement les chemins de sortie rapide (pas le REPL interactif).
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

import yaml

from skifer.cli import (
    SEMANTIC_EXIT_CONFLICT,
    SEMANTIC_EXIT_DRIFT,
    SEMANTIC_EXIT_ERROR,
    SEMANTIC_EXIT_OK,
    run_semantic_sync,
    run_semantic_validate,
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
