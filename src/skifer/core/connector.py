"""Narrow SQL connector contracts; no runtime or DataFrame dependency."""

from dataclasses import dataclass, field
from typing import Any, Literal, Protocol, runtime_checkable

from skifer.core.dialect import TARGETS

WRITE_MODES = ("overwrite", "append")
WriteMode = Literal["overwrite", "append"]


@dataclass(frozen=True)
class ExecutionResult:
    """Optional backend hook result; ``execute_sql`` remains unchanged."""

    dataframe: Any
    statement_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ColumnInfo:
    """A catalog column, with unknown nullability represented by None."""

    name: str
    type: str
    nullable: bool | None = None


@dataclass(frozen=True)
class ConnectorCapabilities:
    """Declared dialect and supported write modes."""

    dialect: str
    writes: frozenset[str]

    def __post_init__(self):
        object.__setattr__(self, "writes", frozenset(self.writes))
        if not self.writes.issubset(WRITE_MODES):
            raise ValueError(f"Unsupported write modes {self.writes!r}; expected modes from {WRITE_MODES}.")
        if self.dialect not in TARGETS:
            raise ValueError(f"Unknown SQL dialect {self.dialect!r}; expected one of {TARGETS}.")


@runtime_checkable
class ExecutionContract(Protocol):
    """Execute SQL and return its result with optional execution metadata."""

    def execute_sql_with_metadata(self, sql: str) -> ExecutionResult: ...


@runtime_checkable
class CatalogContract(Protocol):
    """Inspect tables and resolve logical names to fully qualified names."""

    def table_exists(self, fqn: str) -> bool: ...

    def columns(self, fqn: str) -> list[ColumnInfo]: ...

    def resolve_fqn(self, name: str) -> str: ...


@runtime_checkable
class PublicationContract(Protocol):
    """Publish SQL results; callers must validate mode against WRITE_MODES
    and the connector's declared writes before calling write_table.
    """

    def create_or_replace_view(self, fqn: str, select_sql: str) -> None: ...

    def write_table(self, fqn: str, select_sql: str, mode: WriteMode) -> None: ...
