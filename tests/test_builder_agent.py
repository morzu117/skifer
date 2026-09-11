"""
Tests unitaires pour BuilderAgent (wizard + mode LLM).
"""
from __future__ import annotations

import builtins
import json
import os
import pytest
from unittest.mock import MagicMock

from skifer.agentic.builder_agent import (
    BuilderAgent,
    _build_schema_dict,
    _extract_json,
    _schema_dict_to_yaml,
)
# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_backend(
    exists: bool = True,
    tables: list[str] | None = None,
    columns: list[str] | None = None,
):
    backend = MagicMock()
    backend.build_fqn.side_effect = lambda cat, sch, tbl: (
        f"`{cat}`.`{sch}`.`{tbl}`" if cat else f"`{sch}`.`{tbl}`"
    )
    backend.table_exists.return_value = exists
    backend.list_tables.return_value = tables or ["orders", "customers"]
    backend.list_columns.return_value = columns or ["id", "amount", "region", "status"]
    return backend


def _make_agent(exists=True, tables=None, columns=None, llm=None):
    backend = _make_backend(exists=exists, tables=tables, columns=columns)
    return BuilderAgent(backend=backend, catalog=None, llm_provider=llm)


# ---------------------------------------------------------------------------
# _extract_json
# ---------------------------------------------------------------------------

def test_extract_json_plain():
    raw = '{"tables": [], "joins": []}'
    result = _extract_json(raw)
    assert result["tables"] == []


def test_extract_json_with_markdown():
    raw = "```json\n{\"tables\": []}\n```"
    result = _extract_json(raw)
    assert result["tables"] == []


def test_extract_json_no_json_raises():
    with pytest.raises(ValueError, match="JSON"):
        _extract_json("just text, no braces here")


# ---------------------------------------------------------------------------
# _build_schema_dict
# ---------------------------------------------------------------------------

def test_build_schema_dict_minimal():
    etl = {
        "tables": [{"fqn": "silver.orders", "alias": "ord", "filters": []}],
        "joins": [],
        "business_rules": [],
        "select_final": [],
        "keep_all_columns": True,
    }
    schema = _build_schema_dict(etl)
    assert schema["tables"][0]["name"] == "silver.orders"
    assert schema["tables"][0]["alias"] == "ord"
    assert schema.get("keep_all_columns") is True


def test_build_schema_dict_with_select():
    etl = {
        "tables": [{"fqn": "silver.orders", "alias": "ord", "filters": ["region:equals:EMEA"]}],
        "joins": [],
        "business_rules": ["flag_high_value"],
        "select_final": [{"source": "amount", "target": "amount_eur", "ops": ["cast:double"]}],
        "keep_all_columns": False,
        "dev_limit": 5000,
    }
    schema = _build_schema_dict(etl)
    assert schema["tables"][0]["filter"] == ["region:equals:EMEA"]
    assert schema["business_rules"] == ["flag_high_value"]
    assert schema["select_final"] == [["amount", "amount_eur", ["cast:double"]]]
    assert schema["dev_limit"] == 5000


def test_build_schema_dict_with_joins():
    etl = {
        "tables": [
            {"fqn": "silver.orders", "alias": "ord", "filters": []},
            {"fqn": "silver.customers", "alias": "cust", "filters": []},
        ],
        "joins": [{"from": ["ord", "customer_id"], "to": ["cust", "id"], "type": "left"}],
        "business_rules": [],
        "select_final": [],
        "keep_all_columns": True,
    }
    schema = _build_schema_dict(etl)
    assert schema["join"][0]["table_from"] == ["ord", "customer_id"]
    assert schema["join"][0]["type"] == "left"


# ---------------------------------------------------------------------------
# _schema_dict_to_yaml
# ---------------------------------------------------------------------------

def test_schema_dict_to_yaml_is_valid_yaml():
    import yaml
    schema = {"tables": [{"name": "silver.orders", "alias": "ord"}]}
    content = _schema_dict_to_yaml(schema)
    parsed = yaml.safe_load(content)
    assert parsed["tables"][0]["alias"] == "ord"


# ---------------------------------------------------------------------------
# BuilderAgent.ask() — mode LLM
# ---------------------------------------------------------------------------

def _make_llm_response(etl_struct: dict) -> MagicMock:
    llm = MagicMock()
    llm.complete.return_value = json.dumps(etl_struct)
    return llm


_SIMPLE_ETL = {
    "tables": [{"fqn": "silver.orders", "alias": "ord", "filters": ["region:equals:EMEA"]}],
    "joins": [],
    "business_rules": [],
    "select_final": [{"source": "amount", "target": "amount_eur", "ops": []}],
    "keep_all_columns": False,
    "dev_limit": 10000,
    "output_name": None,
}


def test_ask_success(tmp_path):
    llm = _make_llm_response(_SIMPLE_ETL)
    agent = _make_agent(exists=True, llm=llm)
    result = agent.ask("Crée un pipeline pour les ordres EMEA", output_dir=str(tmp_path))
    assert result.success is True
    assert "silver.orders" in result.yaml_content
    assert result.output_path is not None
    assert os.path.exists(result.output_path)


def test_ask_no_llm_raises():
    agent = _make_agent()
    with pytest.raises(ValueError, match="LLM provider"):
        agent.ask("description")


def test_ask_table_not_found_returns_clarification(tmp_path):
    etl = {**_SIMPLE_ETL, "tables": [{"fqn": "silver.missing_table", "alias": "mt", "filters": []}]}
    llm = _make_llm_response(etl)
    agent = _make_agent(exists=False, tables=["orders", "customers"], llm=llm)
    result = agent.ask("crée un pipeline", output_dir=str(tmp_path))
    assert result.success is False
    assert result.clarification_question is not None
    assert "silver.missing_table" in result.clarification_question


def test_ask_unknown_column_returns_clarification(tmp_path):
    etl = {
        **_SIMPLE_ETL,
        "tables": [{"fqn": "silver.orders", "alias": "ord", "filters": ["unknown_col:equals:X"]}],
    }
    llm = _make_llm_response(etl)
    # table exists but column unknown
    backend = _make_backend(exists=True, columns=["id", "amount", "region"])
    # Override list_columns to return no match for unknown_col
    backend.list_columns.return_value = ["id", "amount", "region"]
    agent = BuilderAgent(backend=backend, catalog=None, llm_provider=llm)
    result = agent.ask("crée un pipeline", output_dir=str(tmp_path))
    assert result.success is False
    assert result.clarification_question is not None


def test_ask_llm_json_error_returns_failure():
    llm = MagicMock()
    llm.complete.return_value = "invalid json response without braces"
    agent = _make_agent(llm=llm)
    result = agent.ask("description")
    assert result.success is False
    assert result.error is not None


def test_ask_llm_raises_returns_failure():
    llm = MagicMock()
    llm.complete.side_effect = RuntimeError("LLM unavailable")
    agent = _make_agent(llm=llm)
    result = agent.ask("description")
    assert result.success is False


def test_wizard_driven_by_answers_without_stdin(tmp_path, monkeypatch):
    agent = _make_agent()
    monkeypatch.setattr(
        builtins,
        "input",
        lambda prompt: (_ for _ in ()).throw(AssertionError("stdin was used")),
    )

    output_path = agent.wizard(
        output_dir=str(tmp_path),
        answers=["silver.orders", "ord", "", "", "", "", "", "", ""],
    )

    assert output_path == str(tmp_path / "pipeline.yaml")
    assert os.path.exists(output_path)


def test_wizard_default_still_uses_input():
    agent = _make_agent()

    assert agent._input is builtins.input


def test_wizard_accepts_prompt_answer_mapping(tmp_path):
    agent = _make_agent()
    output_prompt = f"  Nom du fichier de sortie [{tmp_path / 'pipeline.yaml'}] : "
    answers = {
        "  Nom de la table (FQN, ex: catalog.silver.orders) : ": ["silver.orders"],
        "  Alias : ": ["ord"],
        "  Ajouter une autre table ? (entrée pour passer) : ": [""],
        "  Filtre (ex: region:equals:EMEA) ou entrée pour passer : ": [""],
        "  Règle enregistrée (ex: flag_high_value) ou entrée pour passer : ": [""],
        "  Colonne source (ou entrée pour keep_all_columns) : ": [""],
        "  dev_limit (entrée pour aucune limite) : ": [""],
        output_prompt: [""],
        "Sauvegarder ? [O/n] : ": [""],
    }

    output_path = agent.wizard(output_dir=str(tmp_path), answers=answers)

    assert output_path == str(tmp_path / "pipeline.yaml")


# ---------------------------------------------------------------------------
# BuilderAgent.export_orchestration()
# ---------------------------------------------------------------------------

def test_export_orchestration_script(tmp_path):
    backend = _make_backend()
    agent = BuilderAgent(backend=backend, catalog=None)
    result = agent.export_orchestration(
        ["schemas/silver/orders.yaml"],
        output_dir=str(tmp_path),
        format="script",
    )
    assert result.format == "script"
    assert os.path.exists(result.fallback_script_path)


def test_export_orchestration_airflow(tmp_path):
    backend = _make_backend()
    agent = BuilderAgent(backend=backend, catalog=None)
    result = agent.export_orchestration(
        ["schemas/silver/orders.yaml"],
        output_dir=str(tmp_path),
        format="airflow",
    )
    assert result.format == "airflow"
    assert result.primary_path.endswith(".py")
    assert os.path.exists(result.primary_path)


def test_export_orchestration_databricks(tmp_path):
    backend = _make_backend()
    agent = BuilderAgent(backend=backend, catalog=None)
    result = agent.export_orchestration(
        ["schemas/silver/orders.yaml"],
        output_dir=str(tmp_path),
        format="databricks",
    )
    assert result.format == "databricks"
    assert result.primary_path.endswith(".yml")
    assert os.path.exists(result.primary_path)


# ---------------------------------------------------------------------------
# BuilderAgent._validate_etl_struct
# ---------------------------------------------------------------------------

def test_validate_etl_struct_all_valid():
    agent = _make_agent(exists=True, columns=["id", "region"])
    etl = {"tables": [{"fqn": "silver.orders", "alias": "ord", "filters": ["region:equals:EMEA"]}]}
    result = agent._validate_etl_struct(etl)
    assert result is None


def test_validate_etl_struct_table_missing():
    agent = _make_agent(exists=False, tables=["other_table"])
    etl = {"tables": [{"fqn": "silver.orders", "alias": "ord", "filters": []}]}
    result = agent._validate_etl_struct(etl)
    assert result is not None
    assert "silver.orders" in result
