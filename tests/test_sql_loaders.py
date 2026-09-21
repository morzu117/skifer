"""Portable loaders: `kind="sql"` returns a relation expression, not a DataFrame.

A loader is the last YAML construction that forced a pipeline onto Spark. Declared
`kind="sql"`, it returns a SQL relation expression placed exactly where an adapter
puts a file source's relation — so a portable loader is the user-space counterpart
of `Adapter.resolve_source`, and sqlglot carries it to every dialect.
"""

from __future__ import annotations

from uuid import uuid4

import pytest

from skifer.core.capabilities_matrix import CAP_LOADERS, required_capabilities
from skifer.core.ir import parse_to_ir
from skifer.core.registry import RuleRegistry
from skifer.core.sql_compiler import SqlCompilationError, compile_select


SEGMENTS = "VALUES ('A', 'Retail'), ('B', 'Business') AS t(segment_code, segment_name)"


@pytest.fixture
def registered():
    names: list[str] = []
    yield names
    for name in names:
        RuleRegistry._loaders.pop(name, None)


def _register(names, *, kind, func=None):
    name = f"loader_{kind}_{uuid4().hex}"
    if func is None:
        def func():  # noqa: D401 - the default portable loader
            return SEGMENTS
    RuleRegistry.register_loader(name=name, kind=kind)(func)
    names.append(name)
    return name


def _schema(loader_name, **table_extra):
    return {
        "tables": [
            {
                "name": "segments",
                "alias": "seg",
                "source_type": "loader",
                "function_name": loader_name,
                **table_extra,
            }
        ],
        "select_final": [["segment_code", "segment_code"]],
    }


def test_sql_loader_relation_is_compiled_into_the_table_cte(registered):
    name = _register(registered, kind="sql")

    sql = compile_select(parse_to_ir(_schema(name)), persisted_definition=False)

    assert "VALUES ('A', 'Retail'), ('B', 'Business')" in sql


def test_sql_loader_requires_no_adapter_capability(registered):
    """A portable loader needs no engine, exactly like a kind='sql' rule."""
    sql_loader = _register(registered, kind="sql")
    dataframe_loader = _register(
        registered, kind="dataframe", func=lambda config, *, backend, **kwargs: None
    )

    assert required_capabilities(parse_to_ir(_schema(sql_loader))) == frozenset()
    assert CAP_LOADERS in required_capabilities(
        parse_to_ir(_schema(dataframe_loader))
    )


def test_dataframe_loader_is_still_refused_by_name(registered):
    name = _register(
        registered, kind="dataframe", func=lambda config, *, backend, **kwargs: None
    )

    with pytest.raises(SqlCompilationError, match="kind='sql'"):
        compile_select(parse_to_ir(_schema(name)), persisted_definition=False)


def test_unregistered_loader_is_treated_as_needing_an_engine():
    """An unimported module must not be assumed portable.

    Classifying an unknown loader as SQL would let the pipeline pass every check
    here and fail at the client instead.
    """
    parsed = parse_to_ir(_schema("never_imported"))

    assert CAP_LOADERS in required_capabilities(parsed)
    with pytest.raises(SqlCompilationError):
        compile_select(parsed, persisted_definition=False)


def test_sql_loader_receives_the_yaml_arguments(registered):
    def labels(prefix):
        return f"VALUES ('{prefix}-A', 'Retail') AS t(segment_code, segment_name)"

    name = _register(registered, kind="sql", func=labels)
    schema = _schema(name, arguments={"prefix": "EU"})

    sql = compile_select(parse_to_ir(schema), persisted_definition=False)

    assert "'EU-A'" in sql


def test_sql_loader_called_with_wrong_arguments_names_the_loader(registered):
    def labels(prefix):
        return f"VALUES ('{prefix}') AS t(segment_code)"

    name = _register(registered, kind="sql", func=labels)

    with pytest.raises(SqlCompilationError, match="arguments"):
        compile_select(parse_to_ir(_schema(name)), persisted_definition=False)


@pytest.mark.parametrize("returned", [None, 42, "", "   "])
def test_sql_loader_returning_something_else_is_refused(registered, returned):
    name = _register(registered, kind="sql", func=lambda: returned)

    with pytest.raises(SqlCompilationError, match="relation expression"):
        compile_select(parse_to_ir(_schema(name)), persisted_definition=False)


def test_sql_loader_obeys_allow_raw_sql(registered):
    """A portable loader is raw SQL written by a human.

    Governing `expr:` and kind='sql' rules but not this would leave the same door
    open under another name.
    """
    name = _register(registered, kind="sql")

    with pytest.raises(SqlCompilationError, match="allow_raw_sql"):
        compile_select(
            parse_to_ir(_schema(name)),
            allow_raw_sql=False,
            persisted_definition=False,
        )


def test_sql_loader_cannot_declare_the_engine():
    """Accepting `backend` would let a portable loader reach for Spark.

    It would then compile everywhere and run in one place, and the failure would
    surface at a client rather than at registration.
    """
    with pytest.raises(ValueError, match="backend"):
        RuleRegistry.register_loader(name="peeks_at_engine", kind="sql")(
            lambda backend: SEGMENTS
        )
    assert "peeks_at_engine" not in RuleRegistry._loaders


def test_unknown_loader_kind_is_refused():
    with pytest.raises(ValueError, match="Invalid loader kind"):
        RuleRegistry.register_loader(name="whatever", kind="parquet")


def test_sql_loader_transpiles_to_every_dialect(registered):
    """The loader writes Spark SQL; sqlglot makes it portable.

    Measured: BigQuery has no VALUES table constructor and sqlglot rewrites it as
    `UNNEST([STRUCT(...)])`, which is precisely the work the loader must not have
    to do itself.
    """
    pytest.importorskip("sqlglot")
    from skifer.core.dialect import transpile

    name = _register(registered, kind="sql")
    pivot = compile_select(parse_to_ir(_schema(name)), persisted_definition=False)

    assert transpile(pivot, target="databricks") == pivot
    assert "(VALUES" in transpile(pivot, target="duckdb")
    assert "(VALUES" in transpile(pivot, target="snowflake")
    assert "UNNEST" in transpile(pivot, target="bigquery")


def test_sql_loader_gives_the_same_rows_on_spark_and_duckdb(spark, registered):
    """One YAML, two engines, identical rows — the point of the whole construction.

    The Spark path reads the loader's relation with `SELECT * FROM <relation>`, the
    compiled path embeds the same expression in the table's CTE. Two renderings of
    one declaration is exactly where a divergence would hide.
    """
    duckdb = pytest.importorskip("duckdb")
    from skifer.core.adapters.duckdb import DuckDBAdapter
    from skifer.core.context import ExecutionContext
    from skifer.core.interpreter import SchemaInterpreter
    from skifer.core.spark_backend import SparkBackend
    from skifer.core.sql_runner import run_sql_pipeline
    from tests.test_sql_spark_equivalence import assert_spark_duckdb_equivalent

    name = _register(registered, kind="sql")
    schema = {
        "tables": [
            {
                "name": "segments",
                "alias": "seg",
                "source_type": "loader",
                "function_name": name,
            }
        ],
        "select_final": [
            ["segment_code", "segment_code"],
            ["segment_name", "segment_name", ["upper"]],
        ],
    }
    context = ExecutionContext(
        env="local",
        config={"environments": {"local": {"is_production": False}}},
        is_local=True,
    )

    spark_df = SchemaInterpreter(
        backend=SparkBackend(spark=spark, is_local=True), context=context
    ).process_schema(schema)

    connection = duckdb.connect()
    try:
        adapter = DuckDBAdapter(connection=connection)
        adapter.ensure_schema_exists("gold")
        run_sql_pipeline(adapter, schema, "gold.segments", context=context)
        cursor = adapter.read_table("gold.segments")
        duck_columns = [description[0] for description in cursor.description]
        duck_rows = cursor.fetchall()
    finally:
        connection.close()

    assert len(duck_rows) == 2
    assert_spark_duckdb_equivalent(spark_df, duck_columns, duck_rows)
