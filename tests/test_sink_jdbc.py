import pytest
from unittest.mock import MagicMock, patch

from skifer.core.core import SkiferEngine
from skifer.core.schema_loader import parse_schema
from skifer.sinks.jdbc import JDBCSink
from tests.fakes.fake_backend import FakeBackend, FakeDataFrame


def _make_engine(backend=None):
    from skifer.core.context import ExecutionContext
    from skifer.core.interpreter import SchemaInterpreter
    engine = object.__new__(SkiferEngine)
    ctx = ExecutionContext()
    object.__setattr__(engine, "_context", ctx)
    engine.spark = None
    engine.is_local = True
    engine.db = None
    engine.monitor = None
    engine.env = "local"
    engine.config = {"environments": {"local": {}}}
    engine.schema_suffix = ""
    engine.is_job_execution = False
    from skifer.core.patterns import PipelinePatterns
    b = backend or FakeBackend()
    engine._backend = b
    engine._interpreter = SchemaInterpreter(backend=b, context=ctx)
    engine._patterns = PipelinePatterns(engine=engine)
    return engine


def test_jdbc_sink_write_uses_spark_jdbc(monkeypatch):
    monkeypatch.setenv("POSTGRES_HOST", "db.internal")
    monkeypatch.setenv("POSTGRES_PORT", "5432")
    monkeypatch.setenv("POSTGRES_DB", "analytics")
    monkeypatch.setenv("POSTGRES_USER", "etl_user")
    monkeypatch.setenv("POSTGRES_PASSWORD", "secret")

    writer = MagicMock()
    writer.format.return_value = writer
    writer.option.return_value = writer
    writer.mode.return_value = writer
    df = MagicMock()
    df.write = writer

    JDBCSink().write(df, "reporting", "kpi_clients_gold")

    writer.format.assert_called_once_with("jdbc")
    assert writer.option.call_args_list == [
        (("url", "jdbc:postgresql://db.internal:5432/analytics"),),
        (("dbtable", "reporting.kpi_clients_gold"),),
        (("user", "etl_user"),),
        (("password", "secret"),),
        (("driver", "org.postgresql.Driver"),),
    ]
    writer.mode.assert_called_once_with("overwrite")
    writer.save.assert_called_once_with()


def test_jdbc_sink_missing_host_raises(monkeypatch):
    monkeypatch.delenv("POSTGRES_HOST", raising=False)
    monkeypatch.setenv("POSTGRES_DB", "analytics")
    monkeypatch.setenv("POSTGRES_USER", "etl_user")
    monkeypatch.setenv("POSTGRES_PASSWORD", "secret")

    with pytest.raises(EnvironmentError, match="POSTGRES_HOST"):
        JDBCSink()


def test_jdbc_sink_invalid_mode_raises(monkeypatch):
    monkeypatch.setenv("POSTGRES_HOST", "db.internal")
    monkeypatch.setenv("POSTGRES_DB", "analytics")
    monkeypatch.setenv("POSTGRES_USER", "etl_user")
    monkeypatch.setenv("POSTGRES_PASSWORD", "secret")

    with pytest.raises(ValueError, match="Unsupported JDBC write mode"):
        JDBCSink().write(MagicMock(write=MagicMock()), "reporting", "kpi_clients_gold", mode="truncate")


def test_parse_schema_accepts_postgres_sink():
    schema = parse_schema(
        """
tables:
  - name: silver.orders
sink:
  type: postgres
"""
    )
    assert schema["sink"] == {"type": "postgres"}


def test_parse_schema_rejects_unknown_sink_type():
    with pytest.raises(ValueError, match="Unknown sink type 'unknown'"):
        parse_schema(
            """
tables:
  - name: silver.orders
sink:
  type: unknown
"""
        )


def test_parse_schema_rejects_unknown_sink_key():
    with pytest.raises(ValueError, match="Unknown keys"):
        parse_schema(
            """
tables:
  - name: silver.orders
sink:
  type: postgres
  mode: append
"""
        )


def test_run_process_to_table_uses_jdbc_sink_without_delta_writes():
    backend = FakeBackend()
    engine = _make_engine(backend)
    engine.schema_suffix = "_test_user"
    engine.process_schema = MagicMock(return_value=FakeDataFrame([{"id": 1}], name="result"))

    with patch("skifer.sinks.jdbc.JDBCSink") as sink_cls:
        engine.run_process_to_table({"tables": [], "sink": {"type": "postgres"}}, "gold", "fact_orders")

    sink_cls.return_value.write.assert_called_once_with(
        engine.process_schema.return_value,
        "gold",
        "fact_orders",
    )
    assert backend._written == {}
    assert backend._dropped == []
    assert backend._schemas_created == []


def test_run_process_to_table_uses_sink_schema_and_table_overrides():
    backend = FakeBackend()
    engine = _make_engine(backend)
    engine.process_schema = MagicMock(return_value=FakeDataFrame([{"id": 1}], name="result"))

    with patch("skifer.sinks.jdbc.JDBCSink") as sink_cls:
        engine.run_process_to_table(
            {
                "tables": [],
                "sink": {"type": "postgres", "schema": "reporting", "table": "kpi_clients_gold"},
            },
            "gold",
            "fact_orders",
        )

    sink_cls.return_value.write.assert_called_once_with(
        engine.process_schema.return_value,
        "reporting",
        "kpi_clients_gold",
    )


def test_run_process_to_table_without_sink_keeps_delta_flow():
    backend = FakeBackend()
    engine = _make_engine(backend)
    engine.process_schema = MagicMock(return_value=FakeDataFrame([{"id": 1}], name="result"))

    engine.run_process_to_table({"tables": []}, "gold", "fact_orders")

    assert backend._schemas_created == ["gold"]
    # B.1: drop-before-write removed — atomic overwrite; no prior drop expected
    assert backend._dropped == []
    assert backend._written == {"`gold`.`fact_orders`": [{"id": 1}]}


def test_jdbc_sink_port_defaults_to_5432(monkeypatch):
    monkeypatch.setenv("POSTGRES_HOST", "db.internal")
    monkeypatch.delenv("POSTGRES_PORT", raising=False)
    monkeypatch.setenv("POSTGRES_DB", "analytics")
    monkeypatch.setenv("POSTGRES_USER", "etl_user")
    monkeypatch.setenv("POSTGRES_PASSWORD", "secret")

    writer = MagicMock()
    writer.format.return_value = writer
    writer.option.return_value = writer
    writer.mode.return_value = writer
    df = MagicMock()
    df.write = writer

    JDBCSink().write(df, "reporting", "kpi")

    url_call = writer.option.call_args_list[0]
    assert url_call == (("url", "jdbc:postgresql://db.internal:5432/analytics"),)


def test_jdbc_sink_type_alias_jdbc(monkeypatch):
    schema = parse_schema(
        """
tables:
  - name: silver.orders
sink:
  type: jdbc
"""
    )
    assert schema["sink"]["type"] == "jdbc"


def test_parse_schema_sink_delta_explicit():
    schema = parse_schema(
        """
tables:
  - name: silver.orders
sink:
  type: delta
"""
    )
    assert schema["sink"] == {"type": "delta"}


def test_jdbc_sink_schema_none_omits_prefix(monkeypatch):
    monkeypatch.setenv("POSTGRES_HOST", "db.internal")
    monkeypatch.setenv("POSTGRES_DB", "analytics")
    monkeypatch.setenv("POSTGRES_USER", "etl_user")
    monkeypatch.setenv("POSTGRES_PASSWORD", "secret")

    writer = MagicMock()
    writer.format.return_value = writer
    writer.option.return_value = writer
    writer.mode.return_value = writer
    df = MagicMock()
    df.write = writer

    JDBCSink().write(df, None, "kpi_customers")

    dbtable_call = writer.option.call_args_list[1]
    assert dbtable_call == (("dbtable", "kpi_customers"),)


def test_run_process_and_split_raises_for_jdbc_sink():
    engine = _make_engine()
    with pytest.raises(NotImplementedError, match="run_process_and_split"):
        engine.run_process_and_split(
            {"tables": [], "sink": {"type": "postgres"}},
            split_values=[{"value": "A", "label": "a"}],
            target_layer="gold",
            target_base_name="fact_orders",
            split_column="region",
        )


def test_run_union_sources_to_table_raises_for_jdbc_sink():
    engine = _make_engine()
    with pytest.raises(NotImplementedError, match="run_union_sources_to_table"):
        engine.run_union_sources_to_table(
            {"tables": [], "sink": {"type": "jdbc"}},
            source_partitions=[{"label": "2024"}],
            source_layer="bronze",
            target_layer="gold",
            target_table_name="fact_orders",
            source_base_names=["orders"],
            source_alias="ord",
        )
