"""
Tests for core/patterns.py — PipelinePatterns.

Verifies B.4: absent tables are silently skipped; real errors (permission,
network) propagate out of run_union_sources_to_table.
"""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch


def _make_patterns_engine(table_exists_map: dict | None = None):
    """Return (engine_mock, patterns) with table_exists controlled by map."""
    from skifer.core.patterns import PipelinePatterns

    engine = MagicMock()
    engine._build_fqn.side_effect = lambda schema, tbl: f"`{schema}`.`{tbl}`"
    engine.get_target_schema.side_effect = lambda layer: f"{layer}_schema"
    engine._ensure_schema_exists.return_value = None
    engine._write_dataframe.return_value = None
    engine.certification_store = None
    engine.monitor = None

    backend = MagicMock()
    backend.union_by_name.side_effect = lambda dfs: dfs[0] if len(dfs) == 1 else dfs
    backend.drop_duplicates.side_effect = lambda df: df
    engine._get_backend.return_value = backend

    if table_exists_map is not None:
        def _table_exists(catalog, schema, table):
            return table_exists_map.get(table, False)
        backend.table_exists.side_effect = _table_exists
    else:
        backend.table_exists.return_value = True

    engine.process_schema.return_value = MagicMock()

    patterns = PipelinePatterns(engine=engine)
    return engine, backend, patterns


def _certified_schema(**overrides):
    schema = {
        "data_product": {"id": "sales.orders", "version": "1.0.0"},
        "contract": {
            "output": {
                "id": {"logical_type": "integer", "required": True},
            },
        },
        "tables": [{"name": "silver.orders", "alias": "ord"}],
    }
    schema.update(overrides)
    return schema


# ---------------------------------------------------------------------------
# B.4 — absent table is silently skipped, warning emitted
# ---------------------------------------------------------------------------

def test_absent_table_skipped_with_warning(caplog):
    """Table that does not exist is skipped and logged; present tables proceed."""
    import logging

    present_df = MagicMock()
    engine, backend, patterns = _make_patterns_engine(
        table_exists_map={"src_p1": False, "src_p2": True}
    )
    backend.read_table.return_value = present_df

    with caplog.at_level(logging.WARNING, logger="skifer.core.patterns"):
        patterns.run_union_sources_to_table(
            schema_dict={"tables": []},
            source_partitions=[{"label": "p1"}, {"label": "p2"}],
            source_layer="silver",
            target_layer="gold",
            target_table_name="result",
            source_base_names=["src"],
            source_alias="unioned",
        )

    assert any("Skipped genuinely absent tables" in r.message for r in caplog.records)
    # read_table only called once (for p2)
    assert backend.read_table.call_count == 1


def test_all_absent_raises():
    """When every source table is absent, ValueError is raised (no sources to union)."""
    engine, backend, patterns = _make_patterns_engine(
        table_exists_map={"src_p1": False, "src_p2": False}
    )

    with pytest.raises(ValueError, match="No sources found"):
        patterns.run_union_sources_to_table(
            schema_dict={"tables": []},
            source_partitions=[{"label": "p1"}, {"label": "p2"}],
            source_layer="silver",
            target_layer="gold",
            target_table_name="result",
            source_base_names=["src"],
            source_alias="unioned",
        )


def test_permission_error_propagates():
    """Non-absence backend errors from read_table propagate out (not swallowed)."""
    engine, backend, patterns = _make_patterns_engine(table_exists_map={"src_p1": True})
    backend.read_table.side_effect = PermissionError("access denied")

    with pytest.raises(PermissionError, match="access denied"):
        patterns.run_union_sources_to_table(
            schema_dict={"tables": []},
            source_partitions=[{"label": "p1"}],
            source_layer="silver",
            target_layer="gold",
            target_table_name="result",
            source_base_names=["src"],
            source_alias="unioned",
        )


# ---------------------------------------------------------------------------
# run_from_yaml — config-backed default_params + explicit override
# ---------------------------------------------------------------------------

def test_run_from_yaml_resolves_config_params():
    """Placeholders resolve from engine.default_params with no explicit params."""
    from skifer.core.patterns import PipelinePatterns

    engine = MagicMock()
    engine.default_params = {
        "catalog": "demo_catalog",
        "env": "DEV",
        "inbound_base_path": "/Volumes/demo_catalog/demo_schema/inbound",
    }
    patterns = PipelinePatterns(engine=engine)
    patterns.run_process_to_table = MagicMock()

    with patch("skifer.core.schema_loader.load_schema") as load_schema:
        load_schema.return_value = {"tables": []}
        patterns.run_from_yaml("schemas/bronze/raw_demand.yaml", target_layer="bronze")

    _, kwargs = load_schema.call_args
    assert kwargs["params"]["inbound_base_path"] == "/Volumes/demo_catalog/demo_schema/inbound"
    patterns.run_process_to_table.assert_called_once()


def test_run_from_yaml_explicit_params_override_config():
    """Explicit params passed to run_from_yaml win over config-backed defaults."""
    from skifer.core.patterns import PipelinePatterns

    engine = MagicMock()
    engine.default_params = {
        "catalog": "demo_catalog",
        "env": "DEV",
        "inbound_base_path": "/Volumes/demo_catalog/demo_schema/inbound",
    }
    patterns = PipelinePatterns(engine=engine)
    patterns.run_process_to_table = MagicMock()

    with patch("skifer.core.schema_loader.load_schema") as load_schema:
        load_schema.return_value = {"tables": []}
        patterns.run_from_yaml(
            "schemas/bronze/raw_demand.yaml",
            target_layer="bronze",
            params={"inbound_base_path": "/Volumes/other/other/other"},
        )

    _, kwargs = load_schema.call_args
    assert kwargs["params"]["inbound_base_path"] == "/Volumes/other/other/other"
    # built-ins still present
    assert kwargs["params"]["catalog"] == "demo_catalog"


def test_all_present_no_warning(caplog):
    """When all tables exist, no warning is emitted."""
    import logging

    engine, backend, patterns = _make_patterns_engine(
        table_exists_map={"src_p1": True, "src_p2": True}
    )
    backend.read_table.return_value = MagicMock()

    with caplog.at_level(logging.WARNING, logger="skifer.core.patterns"):
        patterns.run_union_sources_to_table(
            schema_dict={"tables": []},
            source_partitions=[{"label": "p1"}, {"label": "p2"}],
            source_layer="silver",
            target_layer="gold",
            target_table_name="result",
            source_base_names=["src"],
            source_alias="unioned",
        )

    assert not any("Skipped" in r.message for r in caplog.records)
    assert backend.read_table.call_count == 2


def test_run_process_to_table_legacy_schema_keeps_write_and_monitor_flow():
    """A schema without data_product keeps the existing direct write path."""
    engine, _, patterns = _make_patterns_engine()
    report = MagicMock()
    report.summary.return_value = {"status": "PASS", "passed": 1, "total_checks": 1}
    engine.monitor = MagicMock()
    engine.monitor.check_from_schema.return_value = report
    schema = {"tables": [{"name": "silver.orders", "alias": "ord"}]}

    patterns.run_process_to_table(schema, "gold", "fact_orders")

    engine._write_dataframe.assert_called_once()
    engine.monitor.check_from_schema.assert_called_once_with(
        "`gold_schema`.`fact_orders`",
        schema,
        raise_on_critical=True,
    )


def test_run_process_to_table_certified_schema_requires_store_and_monitor_before_processing():
    engine, _, patterns = _make_patterns_engine()
    engine.monitor = MagicMock()

    with pytest.raises(ValueError, match="certification_store and/or monitor"):
        patterns.run_process_to_table(_certified_schema(), "gold", "fact_orders")

    engine.process_schema.assert_not_called()
    engine._write_dataframe.assert_not_called()


@pytest.mark.parametrize(
    "schema",
    [
        _certified_schema(materialization={"type": "streaming_table"}),
        _certified_schema(sink={"type": "jdbc"}),
        _certified_schema(materialization={"type": "materialized_view"}),
    ],
)
def test_run_process_to_table_certified_schema_rejects_unsupported_modes(schema):
    engine, _, patterns = _make_patterns_engine()
    engine.monitor = MagicMock()
    engine.certification_store = MagicMock()

    with pytest.raises(ValueError, match="streaming, JDBC sinks, or materialized views"):
        patterns.run_process_to_table(schema, "gold", "fact_orders")

    engine.process_schema.assert_not_called()
    engine._write_dataframe.assert_not_called()


def test_run_process_to_table_certified_schema_uses_publication_coordinator():
    from skifer.observability.monitor import MonitorReport
    from skifer.observability.publication import PublicationResult, PublicationRun, RunState

    engine, _, patterns = _make_patterns_engine()
    engine.monitor = MagicMock()
    engine.certification_store = MagicMock()
    run = PublicationRun("run-1", "gold_schema.fact_orders", "staging.fact_orders", RunState.PROMOTED)
    result = PublicationResult(run, MonitorReport("staging.fact_orders", []), "PROMOTED")

    with patch("skifer.observability.publication.PublicationCoordinator") as coordinator:
        coordinator.return_value.publish.return_value = result
        patterns.run_process_to_table(_certified_schema(), "gold", "fact_orders")

    coordinator.return_value.publish.assert_called_once()
    engine._write_dataframe.assert_not_called()


def test_run_process_to_table_certified_schema_raises_when_quarantined():
    from skifer.observability.checks import CheckResult, CheckStatus, DataQualityError, NullCheck
    from skifer.observability.monitor import MonitorReport
    from skifer.observability.publication import PublicationResult, PublicationRun, RunState

    engine, _, patterns = _make_patterns_engine()
    engine.monitor = MagicMock()
    engine.certification_store = MagicMock()
    run = PublicationRun("run-1", "gold_schema.fact_orders", "staging.fact_orders", RunState.STAGED)
    report = MonitorReport(
        "staging.fact_orders",
        [
            CheckResult(
                NullCheck(table="staging.fact_orders", column="id", severity="critical"),
                status=CheckStatus.FAIL,
                message="null id",
                severity="critical",
            )
        ],
    )
    result = PublicationResult(run, report, "QUARANTINED")

    with patch("skifer.observability.publication.PublicationCoordinator") as coordinator:
        coordinator.return_value.publish.return_value = result
        with pytest.raises(DataQualityError):
            patterns.run_process_to_table(_certified_schema(), "gold", "fact_orders")

    engine._write_dataframe.assert_not_called()
