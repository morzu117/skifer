"""
Tests for core/patterns.py — PipelinePatterns.

Verifies B.4: absent tables are silently skipped; real errors (permission,
network) propagate out of run_union_sources_to_table.
"""
from __future__ import annotations

from datetime import datetime, timezone
import logging
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
    engine.metadata_store = None

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


def test_run_process_and_split_refuses_data_product_before_engine_work():
    engine, backend, patterns = _make_patterns_engine()

    with pytest.raises(NotImplementedError, match="data_product.*run_process_to_table"):
        patterns.run_process_and_split(
            schema_dict=_certified_schema(),
            split_values=[{"label": "fr", "value": "FR"}],
            target_layer="gold",
            target_base_name="orders",
            split_column="country",
        )

    engine.process_schema.assert_not_called()
    engine._ensure_schema_exists.assert_not_called()
    engine._write_dataframe.assert_not_called()


def test_run_union_sources_to_table_refuses_data_product_before_engine_work():
    engine, backend, patterns = _make_patterns_engine()

    with pytest.raises(NotImplementedError, match="data_product"):
        patterns.run_union_sources_to_table(
            schema_dict=_certified_schema(),
            source_partitions=[{"label": "fr"}],
            source_layer="silver",
            target_layer="gold",
            target_table_name="orders",
            source_base_names=["orders"],
            source_alias="unioned",
        )

    backend.table_exists.assert_not_called()
    backend.read_table.assert_not_called()
    engine._ensure_schema_exists.assert_not_called()
    engine.process_schema.assert_not_called()
    engine._write_dataframe.assert_not_called()


def test_run_process_and_split_without_data_product_reaches_normal_flow():
    engine, backend, patterns = _make_patterns_engine()

    patterns.run_process_and_split(
        schema_dict={"tables": []},
        split_values=[],
        target_layer="gold",
        target_base_name="orders",
        split_column="country",
    )

    engine.process_schema.assert_called_once_with({"tables": []})


def test_run_union_sources_to_table_without_data_product_reaches_normal_flow():
    engine, backend, patterns = _make_patterns_engine(table_exists_map={"orders_fr": False})

    with pytest.raises(ValueError, match="No sources found"):
        patterns.run_union_sources_to_table(
            schema_dict={"tables": []},
            source_partitions=[{"label": "fr"}],
            source_layer="silver",
            target_layer="gold",
            target_table_name="orders",
            source_base_names=["orders"],
            source_alias="unioned",
        )

    backend.table_exists.assert_called_once()


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
    assert coordinator.call_args.kwargs["alert_router"] is None
    assert coordinator.call_args.kwargs["alert_config"] == {}
    engine._write_dataframe.assert_not_called()


@pytest.mark.parametrize(
    "config",
    [
        None,
        "not-a-mapping",
        {},
        {"max_depth": 5, "min_severity": "critical"},
    ],
)
def test_build_alert_router_requires_mapping_with_truthy_channel(config):
    from skifer.core.patterns import _build_alert_router

    engine = MagicMock()
    engine.context.alerts_config.return_value = config

    router, alert_config = _build_alert_router(engine)

    assert router is None
    assert alert_config == {}


def test_build_alert_router_uses_configured_max_depth():
    from skifer.core.patterns import _build_alert_router

    engine = MagicMock()
    engine.metadata_store = None
    config = {
        "slack_webhook": "https://alerts.example.test/hook",
        "max_depth": 7,
        "min_severity": "warning",
    }
    engine.context.alerts_config.return_value = config

    router, alert_config = _build_alert_router(engine)

    assert router is not None
    assert router._max_depth == 7
    assert alert_config == config


def test_build_alert_router_failure_is_non_blocking_and_redacted():
    from skifer.core.patterns import _build_alert_router

    engine = MagicMock()
    engine.context.alerts_config.side_effect = RuntimeError("secret config value")

    with pytest.warns(RuntimeWarning) as caught:
        router, alert_config = _build_alert_router(engine)

    assert router is None
    assert alert_config == {}
    assert [str(item.message) for item in caught] == [
        "[Alerts] failed to build alert router: RuntimeError"
    ]


def test_promoted_publication_indexes_metadata_and_attaches_latest_run_id():
    from skifer.observability.metadata_index import index_schema
    from skifer.observability.metadata_store import SqliteMetadataStore
    from skifer.observability.monitor import MonitorReport
    from skifer.observability.publication import PublicationResult, PublicationRun, RunState

    engine, _, patterns = _make_patterns_engine()
    engine.monitor = MagicMock()
    engine.certification_store = MagicMock()
    store = SqliteMetadataStore(":memory:")
    engine.metadata_store = store
    schema = _certified_schema()
    fqn = "`gold_schema`.`fact_orders`"
    old_record = index_schema(
        schema,
        "sales.orders",
        target_fqn=fqn,
        last_run_id="run-old",
        now=datetime(2026, 9, 11, 12, 0, tzinfo=timezone.utc),
    )
    assert store.upsert(old_record) is True

    run = PublicationRun("run-new", fqn, "staging.fact_orders", RunState.PROMOTED)
    result = PublicationResult(run, MonitorReport("staging.fact_orders", []), "PROMOTED")

    with patch("skifer.observability.publication.PublicationCoordinator") as coordinator:
        coordinator.return_value.publish.return_value = result
        patterns.run_process_to_table(schema, "gold", "fact_orders")

    assert store.get(fqn).last_run_id == "run-new"


def test_promoted_publication_inherits_upstream_pii_classification():
    from skifer.lineage.classification import ClassificationPropagationWarning
    from skifer.observability.metadata_store import (
        ColumnRecord,
        DatasetRecord,
        SqliteMetadataStore,
    )
    from skifer.observability.monitor import MonitorReport
    from skifer.observability.publication import PublicationResult, PublicationRun, RunState

    engine, _, patterns = _make_patterns_engine()
    engine.monitor = MagicMock()
    engine.certification_store = MagicMock()
    store = SqliteMetadataStore(":memory:")
    engine.metadata_store = store
    store.upsert(DatasetRecord(
        target_fqn="silver.orders",
        pipeline_path="upstream.yaml",
        data_product_id="sales.raw_orders",
        contract_version="1.0.0",
        definition_hash="upstream-hash",
        owner=None,
        columns=(ColumnRecord("email", classification="pii"),),
        indexed_at=datetime(2026, 9, 11, 12, 0, tzinfo=timezone.utc),
    ))
    schema = _certified_schema(
        contract={
            "output": {
                "email_hash": {"logical_type": "string", "required": True},
            },
        },
        select_final=[["email", "email_hash", ["upper"]]],
    )
    fqn = "`gold_schema`.`fact_orders`"
    run = PublicationRun("run-new", fqn, "staging.fact_orders", RunState.PROMOTED)
    result = PublicationResult(run, MonitorReport("staging.fact_orders", []), "PROMOTED")

    with patch("skifer.observability.publication.PublicationCoordinator") as coordinator:
        coordinator.return_value.publish.return_value = result
        with pytest.warns(ClassificationPropagationWarning, match="inherits classification 'pii'"):
            patterns.run_process_to_table(schema, "gold", "fact_orders")

    record = store.get(fqn)
    assert record is not None
    assert record.columns[0].name == "email_hash"
    assert record.columns[0].classification == "pii"


def test_strict_classification_rejects_before_processing_and_names_lineage_source():
    from skifer.lineage.classification import ClassificationViolationError
    from skifer.observability.metadata_store import (
        ColumnRecord,
        DatasetRecord,
        SqliteMetadataStore,
    )

    engine, backend, patterns = _make_patterns_engine()
    engine.monitor = MagicMock()
    engine.certification_store = MagicMock()
    engine.context.classification_propagation.return_value = "strict"
    store = SqliteMetadataStore(":memory:")
    engine.metadata_store = store
    store.upsert(DatasetRecord(
        target_fqn="silver.orders",
        pipeline_path="upstream.yaml",
        data_product_id="sales.raw_orders",
        contract_version="1.0.0",
        definition_hash="upstream-hash",
        owner=None,
        columns=(ColumnRecord("email", classification="pii"),),
        indexed_at=datetime(2026, 9, 11, 12, 0, tzinfo=timezone.utc),
    ))
    schema = _certified_schema(
        contract={
            "output": {
                "email_hash": {"logical_type": "string", "required": True},
            },
        },
        select_final=[["email", "email_hash", ["upper"]]],
    )

    with pytest.raises(ClassificationViolationError) as caught:
        patterns.run_process_to_table(schema, "gold", "fact_orders")

    assert "target column 'email_hash'" in str(caught.value)
    assert "silver.orders.email" in str(caught.value)
    engine.process_schema.assert_not_called()
    engine._ensure_schema_exists.assert_not_called()
    engine._write_dataframe.assert_not_called()
    backend.write_table.assert_not_called()


def test_strict_classification_preflight_propagates_technical_index_error():
    class TechnicalIndexError(ValueError):
        pass

    engine, backend, patterns = _make_patterns_engine()
    engine.monitor = MagicMock()
    engine.certification_store = MagicMock()
    engine.context.classification_propagation.return_value = "strict"
    engine.metadata_store = MagicMock()

    with patch(
        "skifer.observability.metadata_index.index_schema",
        side_effect=TechnicalIndexError("invalid dataset record"),
    ):
        with pytest.raises(TechnicalIndexError, match="invalid dataset record"):
            patterns.run_process_to_table(_certified_schema(), "gold", "fact_orders")

    engine.process_schema.assert_not_called()
    engine._ensure_schema_exists.assert_not_called()
    engine._write_dataframe.assert_not_called()
    backend.write_table.assert_not_called()


def test_strict_classification_allows_declared_classification():
    from skifer.observability.metadata_store import SqliteMetadataStore
    from skifer.observability.monitor import MonitorReport
    from skifer.observability.publication import PublicationResult, PublicationRun, RunState

    engine, _, patterns = _make_patterns_engine()
    engine.monitor = MagicMock()
    engine.certification_store = MagicMock()
    engine.context.classification_propagation.return_value = "strict"
    engine.metadata_store = SqliteMetadataStore(":memory:")
    schema = _certified_schema(
        contract={
            "output": {
                "email_hash": {
                    "logical_type": "string",
                    "required": True,
                    "classification": "pii",
                },
            },
        },
        select_final=[["email", "email_hash", ["upper"]]],
    )
    fqn = "`gold_schema`.`fact_orders`"
    run = PublicationRun("run-new", fqn, "staging.fact_orders", RunState.PROMOTED)
    result = PublicationResult(run, MonitorReport("staging.fact_orders", []), "PROMOTED")

    with patch("skifer.observability.publication.PublicationCoordinator") as coordinator:
        coordinator.return_value.publish.return_value = result
        patterns.run_process_to_table(schema, "gold", "fact_orders")

    engine.process_schema.assert_called_once()
    coordinator.return_value.publish.assert_called_once()


def test_strict_classification_requires_metadata_store_before_processing():
    engine, _, patterns = _make_patterns_engine()
    engine.monitor = MagicMock()
    engine.certification_store = MagicMock()
    engine.context.classification_propagation.return_value = "strict"

    with pytest.raises(
        ValueError,
        match="strict requires SkiferEngine\\(metadata_store=\\.\\.\\.\\)",
    ):
        patterns.run_process_to_table(_certified_schema(), "gold", "fact_orders")

    engine.process_schema.assert_not_called()
    engine._ensure_schema_exists.assert_not_called()
    engine._write_dataframe.assert_not_called()


def test_warn_classification_without_metadata_store_keeps_publication_flow():
    from skifer.observability.monitor import MonitorReport
    from skifer.observability.publication import PublicationResult, PublicationRun, RunState

    engine, _, patterns = _make_patterns_engine()
    engine.monitor = MagicMock()
    engine.certification_store = MagicMock()
    engine.context.classification_propagation.return_value = "warn"
    run = PublicationRun(
        "run-1",
        "`gold_schema`.`fact_orders`",
        "staging.fact_orders",
        RunState.PROMOTED,
    )
    result = PublicationResult(run, MonitorReport("staging.fact_orders", []), "PROMOTED")

    with patch("skifer.observability.publication.PublicationCoordinator") as coordinator:
        coordinator.return_value.publish.return_value = result
        patterns.run_process_to_table(_certified_schema(), "gold", "fact_orders")

    engine.process_schema.assert_called_once()
    coordinator.return_value.publish.assert_called_once()


def test_metadata_store_failure_does_not_fail_publication(caplog):
    from skifer.observability.monitor import MonitorReport
    from skifer.observability.publication import PublicationResult, PublicationRun, RunState

    class FailingMetadataStore:
        def upsert(self, record):
            raise RuntimeError("metadata db unavailable")

    engine, _, patterns = _make_patterns_engine()
    engine.monitor = MagicMock()
    engine.certification_store = MagicMock()
    engine.metadata_store = FailingMetadataStore()
    run = PublicationRun(
        "run-1",
        "`gold_schema`.`fact_orders`",
        "staging.fact_orders",
        RunState.PROMOTED,
    )
    result = PublicationResult(run, MonitorReport("staging.fact_orders", []), "PROMOTED")

    with patch("skifer.observability.publication.PublicationCoordinator") as coordinator:
        coordinator.return_value.publish.return_value = result
        with caplog.at_level(logging.WARNING, logger="skifer.core.patterns"):
            patterns.run_process_to_table(_certified_schema(), "gold", "fact_orders")

    assert any("indexing skipped (non-blocking)" in r.message for r in caplog.records)
    engine._write_dataframe.assert_not_called()


def test_publication_without_metadata_store_keeps_certified_publication_flow():
    from skifer.observability.monitor import MonitorReport
    from skifer.observability.publication import PublicationResult, PublicationRun, RunState

    engine, _, patterns = _make_patterns_engine()
    engine.monitor = MagicMock()
    engine.certification_store = MagicMock()
    engine.metadata_store = None
    run = PublicationRun(
        "run-1",
        "`gold_schema`.`fact_orders`",
        "staging.fact_orders",
        RunState.PROMOTED,
    )
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


def test_certified_publication_wires_configured_alert_router():
    from skifer.observability.monitor import MonitorReport
    from skifer.observability.publication import PublicationResult, PublicationRun, RunState

    engine, _, patterns = _make_patterns_engine()
    engine.monitor = MagicMock()
    engine.certification_store = MagicMock()
    config = {
        "slack_webhook": "https://alerts.example.test/hook",
        "max_depth": 2,
    }
    engine.context.alerts_config.return_value = config
    run = PublicationRun(
        "run-1", "gold_schema.fact_orders", "staging.fact_orders", RunState.PROMOTED
    )
    result = PublicationResult(run, MonitorReport("staging.fact_orders", []), "PROMOTED")

    with patch("skifer.observability.publication.PublicationCoordinator") as coordinator:
        coordinator.return_value.publish.return_value = result
        patterns.run_process_to_table(_certified_schema(), "gold", "fact_orders")

    router = coordinator.call_args.kwargs["alert_router"]
    assert router is not None
    assert router._max_depth == 2
    assert coordinator.call_args.kwargs["alert_config"] == config


@pytest.mark.parametrize("max_depth", [None, True, -1])
def test_build_alert_router_invalid_max_depth_defaults_to_three(max_depth):
    from skifer.core.patterns import _build_alert_router

    engine = MagicMock()
    engine.metadata_store = None
    engine.context.alerts_config.return_value = {
        "slack_webhook": "https://alerts.example.test/hook",
        "max_depth": max_depth,
    }

    router, _ = _build_alert_router(engine)

    assert router is not None
    assert router._max_depth == 3


def test_alert_router_without_metadata_store_still_dispatches_to_channel():
    from skifer.core.patterns import _build_alert_router

    engine = MagicMock()
    engine.metadata_store = None
    config = {"slack_webhook": "https://alerts.example.test/hook"}
    engine.context.alerts_config.return_value = config
    dispatcher = MagicMock()
    dispatcher.dispatch_incident.return_value = ["slack"]

    with patch(
        "skifer.observability.alerts.AlertDispatcher", return_value=dispatcher
    ):
        router, alert_config = _build_alert_router(engine)

    incidents = [object()]
    result = router.alert_incident(
        None,
        target_fqn="gold.orders",
        incidents=incidents,
        config=alert_config,
    )

    assert result == ["slack"]
    dispatcher.dispatch_incident.assert_called_once_with(
        target_fqn="gold.orders",
        incidents=incidents,
        recipients=[],
        config=config,
    )
