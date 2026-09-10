"""
Tests unitaires pour OrchestratorExporter.
"""
from __future__ import annotations

import os
import pytest
from unittest.mock import MagicMock

from skifer.agentic.orchestrator import (
    OrchestratorExporter,
    _pipeline_name,
    _detect_backend_type,
    _render_airflow_dag,
    _render_dab_bundle,
    _render_python_script,
)
from skifer.agentic.models import OrchestratorExportResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_backend(class_name: str = "SparkBackend", is_local: bool = False):
    backend = MagicMock()
    backend.__class__.__name__ = class_name
    backend.is_local = is_local
    return backend


# ---------------------------------------------------------------------------
# _pipeline_name
# ---------------------------------------------------------------------------

def test_pipeline_name_simple():
    assert _pipeline_name("schemas/silver/orders.yaml") == "orders"


def test_pipeline_name_dashes():
    assert _pipeline_name("schemas/silver/orders-emea.yaml") == "orders_emea"


def test_pipeline_name_no_extension():
    assert _pipeline_name("orders") == "orders"


# ---------------------------------------------------------------------------
# _detect_backend_type
# ---------------------------------------------------------------------------

def test_detect_spark_remote():
    b = _make_backend("SparkBackend", is_local=False)
    assert _detect_backend_type(b) == "spark"


def test_detect_spark_local():
    b = _make_backend("SparkBackend", is_local=True)
    assert _detect_backend_type(b) == "local"


def test_detect_unknown():
    b = _make_backend("SomeOtherBackend")
    assert _detect_backend_type(b) == "local"


# ---------------------------------------------------------------------------
# _render_airflow_dag
# ---------------------------------------------------------------------------

def test_render_airflow_dag_spark_contains_databricks():
    dag = _render_airflow_dag(["orders", "customers"], "spark")
    assert "DatabricksRunNowOperator" in dag
    assert "orders" in dag
    assert "customers" in dag


def test_render_airflow_dag_local():
    dag = _render_airflow_dag(["orders"], "local")
    assert "PythonOperator" in dag


def test_render_airflow_dag_chain():
    dag = _render_airflow_dag(["step1", "step2", "step3"], "spark")
    assert "step1 >> step2 >> step3" in dag


# ---------------------------------------------------------------------------
# _render_dab_bundle
# ---------------------------------------------------------------------------

def test_render_dab_bundle_contains_task_keys():
    bundle = _render_dab_bundle(["orders", "customers"])
    assert "task_key: orders" in bundle
    assert "task_key: customers" in bundle
    assert "bundle:" in bundle


# ---------------------------------------------------------------------------
# _render_python_script
# ---------------------------------------------------------------------------

def test_render_python_script_contains_imports():
    script = _render_python_script(["schemas/silver/orders.yaml"])
    assert "from skifer import SkiferEngine" in script
    assert "orders" in script
    assert "run_process_to_table" in script


def test_render_python_script_multiple_pipelines():
    script = _render_python_script(["schemas/silver/orders.yaml", "schemas/gold/kpis.yaml"])
    assert "schema_orders" in script
    assert "schema_kpis" in script


# ---------------------------------------------------------------------------
# OrchestratorExporter.export()
# ---------------------------------------------------------------------------

def test_export_script_format(tmp_path):
    exporter = OrchestratorExporter(backend=None)
    result = exporter.export(
        ["schemas/silver/orders.yaml"],
        output_dir=str(tmp_path),
        format="script",
    )
    assert isinstance(result, OrchestratorExportResult)
    assert result.format == "script"
    assert os.path.exists(result.fallback_script_path)
    assert result.pipelines == ["schemas/silver/orders.yaml"]


def test_export_airflow_format(tmp_path):
    exporter = OrchestratorExporter(backend=None)
    result = exporter.export(
        ["schemas/silver/orders.yaml"],
        output_dir=str(tmp_path),
        format="airflow",
    )
    assert result.format == "airflow"
    assert result.primary_path.endswith(".py")
    assert os.path.exists(result.primary_path)
    assert os.path.exists(result.fallback_script_path)


def test_export_databricks_format(tmp_path):
    exporter = OrchestratorExporter(backend=None)
    result = exporter.export(
        ["schemas/silver/orders.yaml"],
        output_dir=str(tmp_path),
        format="databricks",
    )
    assert result.format == "databricks"
    assert result.primary_path.endswith(".yml")
    assert os.path.exists(result.primary_path)
    assert os.path.exists(result.fallback_script_path)


def test_export_auto_spark_resolves_to_databricks(tmp_path):
    b = _make_backend("SparkBackend", is_local=False)
    exporter = OrchestratorExporter(backend=b)
    result = exporter.export(
        ["schemas/silver/orders.yaml"],
        output_dir=str(tmp_path),
        format="auto",
    )
    assert result.format == "databricks"


def test_export_auto_non_spark_resolves_to_script(tmp_path):
    b = _make_backend("SomeOtherBackend")
    exporter = OrchestratorExporter(backend=b)
    result = exporter.export(
        ["schemas/silver/orders.yaml"],
        output_dir=str(tmp_path),
        format="auto",
    )
    assert result.format == "script"


def test_export_auto_local_resolves_to_script(tmp_path):
    b = _make_backend("SparkBackend", is_local=True)
    exporter = OrchestratorExporter(backend=b)
    result = exporter.export(
        ["schemas/silver/orders.yaml"],
        output_dir=str(tmp_path),
        format="auto",
    )
    assert result.format == "script"


def test_export_empty_yaml_paths_raises():
    exporter = OrchestratorExporter(backend=None)
    with pytest.raises(ValueError):
        exporter.export([], output_dir="/tmp")


def test_export_multiple_pipelines(tmp_path):
    exporter = OrchestratorExporter(backend=None)
    result = exporter.export(
        ["schemas/silver/orders.yaml", "schemas/gold/kpis.yaml"],
        output_dir=str(tmp_path),
        format="airflow",
    )
    assert len(result.pipelines) == 2
    content = open(result.primary_path).read()
    assert "orders" in content
    assert "kpis" in content
