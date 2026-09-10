"""
Tests for process_schema JOIN logic.
Key invariant: df_to.columns (AnalyzePlan gRPC) must never be called during join planning.
"""
import pytest
from unittest.mock import MagicMock, call, patch
from skifer.core.core import SkiferEngine
from skifer.core.spark_backend import SparkBackend


def _make_engine():
    from skifer.core.context import ExecutionContext
    from skifer.core.interpreter import SchemaInterpreter
    engine = object.__new__(SkiferEngine)
    mock_spark = MagicMock()
    engine.spark = mock_spark
    ctx = ExecutionContext()
    object.__setattr__(engine, "_context", ctx)
    engine.config = {}
    engine.env = "local"
    engine.is_job_execution = True
    engine.schema_suffix = ""
    backend = SparkBackend(spark=mock_spark, is_local=True)
    engine._backend = backend
    engine._interpreter = SchemaInterpreter(backend=backend, context=ctx)
    return engine


def _joined_df():
    """Return a MagicMock DataFrame that also handles chained .drop()."""
    df = MagicMock(name="joined_df")
    df.drop.return_value = df
    return df


class TestProcessSchemaJoin:

    def test_same_keys_uses_list_join(self):
        """on_from == on_to → join(df_to, on=['id'], how='inner'). No .columns call."""
        df_a = MagicMock(name="df_a")
        df_b = MagicMock(name="df_b")
        joined = _joined_df()
        df_a.join.return_value = joined

        schema = {
            "join": [{
                "table_from": "a",
                "table_to": "b",
                "on_from": "id",
                "on_to": "id",
                "type": "inner",
            }]
        }

        engine = _make_engine()
        result = engine.process_schema(schema, dataframes_in={"a": df_a, "b": df_b})

        df_a.join.assert_called_once_with(df_b, on=["id"], how="inner")
        df_b.columns.assert_not_called()  # ← no AnalyzePlan gRPC
        assert result == joined

    def test_same_keys_list_join_default_left(self):
        """Default join type is 'left'."""
        df_a = MagicMock(name="df_a")
        df_b = MagicMock(name="df_b")
        df_a.join.return_value = _joined_df()

        schema = {
            "join": [{"table_from": "a", "table_to": "b", "on_from": "k", "on_to": "k"}]
        }

        engine = _make_engine()
        engine.process_schema(schema, dataframes_in={"a": df_a, "b": df_b})

        df_a.join.assert_called_once_with(df_b, on=["k"], how="left")

    def test_different_keys_uses_expression_join_and_drop(self):
        """on_from != on_to → expression join, then drop right-side keys. No .columns call."""
        df_a = MagicMock(name="df_a")
        df_b = MagicMock(name="df_b")
        joined = _joined_df()
        df_a.join.return_value = joined

        schema = {
            "join": [{
                "table_from": "a",
                "table_to": "b",
                "on_from": "order_id",
                "on_to": "id",
                "type": "left",
            }]
        }

        engine = _make_engine()
        result = engine.process_schema(schema, dataframes_in={"a": df_a, "b": df_b})

        # join must be called with an expression condition (not a list of strings)
        join_args, join_kwargs = df_a.join.call_args
        assert join_args[0] is df_b
        # how can be passed as keyword or 3rd positional depending on backend version
        how_val = join_kwargs.get("how") or (join_args[2] if len(join_args) > 2 else None)
        assert how_val == "left"
        # expression-based join: 'on' is either a kwarg or 2nd positional arg (a condition, not a list)
        on_val = join_kwargs.get("on") or (join_args[1] if len(join_args) > 1 else None)
        assert not isinstance(on_val, list)  # condition object, not list of strings

        # drop must be called on joined df with df_b["id"] reference
        joined.drop.assert_called_once_with(df_b["id"])

        df_b.columns.assert_not_called()  # ← no AnalyzePlan gRPC

    def test_multi_key_same_names(self):
        """Multi-column join with same key names uses list-join."""
        df_a = MagicMock(name="df_a")
        df_b = MagicMock(name="df_b")
        df_a.join.return_value = _joined_df()

        schema = {
            "join": [{
                "table_from": "a",
                "table_to": "b",
                "on_from": ["org", "date"],
                "on_to": ["org", "date"],
                "type": "left",
            }]
        }

        engine = _make_engine()
        engine.process_schema(schema, dataframes_in={"a": df_a, "b": df_b})

        df_a.join.assert_called_once_with(df_b, on=["org", "date"], how="left")
        df_b.columns.assert_not_called()

    def test_multi_key_different_names_drops_all_right_keys(self):
        """Multi-column join with different key names drops all right-side keys."""
        df_a = MagicMock(name="df_a")
        df_b = MagicMock(name="df_b")
        joined = _joined_df()
        df_a.join.return_value = joined

        schema = {
            "join": [{
                "table_from": "a",
                "table_to": "b",
                "on_from": ["org_l", "date_l"],
                "on_to": ["org_r", "date_r"],
                "type": "inner",
            }]
        }

        engine = _make_engine()
        engine.process_schema(schema, dataframes_in={"a": df_a, "b": df_b})

        # drop must receive both right-side key refs
        joined.drop.assert_called_once_with(df_b["org_r"], df_b["date_r"])
        df_b.columns.assert_not_called()

    def test_full_outer_join_same_keys(self):
        """type: full → join(..., how='full') with list-join path."""
        df_a = MagicMock(name="df_a")
        df_b = MagicMock(name="df_b")
        df_a.join.return_value = _joined_df()

        schema = {
            "join": [{"table_from": "a", "table_to": "b", "on_from": "id", "on_to": "id", "type": "full"}]
        }

        engine = _make_engine()
        engine.process_schema(schema, dataframes_in={"a": df_a, "b": df_b})

        df_a.join.assert_called_once_with(df_b, on=["id"], how="full")

    def test_full_outer_join_different_keys(self):
        """type: full outer (alias) → expression join, right key dropped."""
        df_a = MagicMock(name="df_a")
        df_b = MagicMock(name="df_b")
        joined = _joined_df()
        df_a.join.return_value = joined

        schema = {
            "join": [{"table_from": "a", "table_to": "b", "on_from": "order_id", "on_to": "id", "type": "full outer"}]
        }

        engine = _make_engine()
        engine.process_schema(schema, dataframes_in={"a": df_a, "b": df_b})

        _, join_kwargs = df_a.join.call_args
        assert join_kwargs.get("how") == "full"
        joined.drop.assert_called_once_with(df_b["id"])

    @pytest.mark.parametrize(
        ("join_type", "expected"),
        [("left_anti", "left_anti"), ("anti", "left_anti"), ("left_semi", "left_semi"), ("left semi", "left_semi")],
    )
    def test_left_anti_semi_same_keys_passes_canonical_how(self, join_type, expected):
        df_a = MagicMock(name="df_a")
        df_b = MagicMock(name="df_b")
        joined = _joined_df()
        df_a.join.return_value = joined

        schema = {
            "join": [{
                "table_from": "a",
                "table_to": "b",
                "on_from": "id",
                "on_to": "id",
                "type": join_type,
            }]
        }

        engine = _make_engine()
        result = engine.process_schema(schema, dataframes_in={"a": df_a, "b": df_b})

        df_a.join.assert_called_once_with(df_b, on=["id"], how=expected)
        df_b.columns.assert_not_called()
        assert result == joined

    @pytest.mark.parametrize(("join_type", "expected"), [("left_anti", "left_anti"), ("left_semi", "left_semi")])
    def test_left_anti_semi_different_keys_skip_drop(self, join_type, expected):
        df_a = MagicMock(name="df_a")
        df_b = MagicMock(name="df_b")
        joined = _joined_df()
        df_a.join.return_value = joined

        schema = {
            "join": [{
                "table_from": "a",
                "table_to": "b",
                "on_from": "order_id",
                "on_to": "id",
                "type": join_type,
            }]
        }

        engine = _make_engine()
        result = engine.process_schema(schema, dataframes_in={"a": df_a, "b": df_b})

        join_args, join_kwargs = df_a.join.call_args
        assert join_args[0] is df_b
        how_val = join_kwargs.get("how") or (join_args[2] if len(join_args) > 2 else None)
        assert how_val == expected
        on_val = join_kwargs.get("on") or (join_args[1] if len(join_args) > 1 else None)
        assert not isinstance(on_val, list)
        joined.drop.assert_not_called()
        df_b.columns.assert_not_called()
        assert result == joined
