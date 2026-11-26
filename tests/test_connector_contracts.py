"""Pure Python structural contracts for thin SQL connectors."""

from dataclasses import FrozenInstanceError

import pytest

from skifer.core.connector import (
    CatalogContract, ColumnInfo, ConnectorCapabilities, ExecutionContract,
    ExecutionResult, PublicationContract, WRITE_MODES,
)


class MinimalConnector:
    def execute_sql_with_metadata(self, sql):
        return ExecutionResult(dataframe=[], statement_id="test")

    def table_exists(self, fqn):
        return True

    def columns(self, fqn):
        return [ColumnInfo("id", "integer")]

    def resolve_fqn(self, name):
        return f"main.{name}"

    def create_or_replace_view(self, fqn, select_sql):
        pass

    def write_table(self, fqn, select_sql, mode):
        pass


@pytest.mark.parametrize("contract", [ExecutionContract, CatalogContract, PublicationContract])
def test_minimal_connector_satisfies_contract(contract):
    assert isinstance(MinimalConnector(), contract)
    assert not isinstance(object(), contract)


def test_execution_result_compatibility_export():
    from skifer.semantic.evidence import ExecutionResult as SemanticExecutionResult
    from skifer.semantic import ExecutionResult as PublicExecutionResult

    assert SemanticExecutionResult is PublicExecutionResult is ExecutionResult
    assert ExecutionResult([]).metadata == {}
    assert ExecutionResult([]).metadata is not ExecutionResult([]).metadata


def test_capabilities_validate_dialect_and_are_frozen():
    with pytest.raises(ValueError, match="oracle"):
        ConnectorCapabilities("oracle", frozenset())
    capabilities = ConnectorCapabilities("duckdb", frozenset(WRITE_MODES))
    with pytest.raises(FrozenInstanceError):
        capabilities.dialect = "snowflake"
    assert ColumnInfo("id", "integer").nullable is None


def test_capabilities_freeze_caller_write_modes():
    writes = {"append"}
    capabilities = ConnectorCapabilities("duckdb", writes)
    writes.add("overwrite")
    assert isinstance(capabilities.writes, frozenset)
    assert capabilities.writes == frozenset({"append"})


@pytest.mark.parametrize("writes", [{"upsert"}, {"append", "merge"}])
def test_capabilities_refuse_unknown_write_modes(writes):
    with pytest.raises(ValueError, match="Unsupported write modes"):
        ConnectorCapabilities("duckdb", writes)


def test_publication_write_mode_annotation():
    from typing import get_args, get_type_hints
    from skifer.core.connector import WriteMode

    assert get_args(WriteMode) == WRITE_MODES
    assert get_type_hints(PublicationContract.write_table)["mode"] == WriteMode
