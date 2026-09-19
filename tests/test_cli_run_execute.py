"""Real execution of a selection on DuckDB (Plan 39.6.3b)."""

from __future__ import annotations

import argparse
from pathlib import Path

import pytest
import yaml

from skifer.cli import GRAPH_EXIT_ERROR, GRAPH_EXIT_OK, GRAPH_EXIT_USAGE, run_run_command
from skifer.core.core import SkiferEngine
from skifer.observability.metadata_index import index_from_path
from skifer.observability.metadata_store import SqliteMetadataStore
from skifer.observability.pipeline_graph import (
    PipelineEdge,
    PipelineGraph,
    run_selection,
)


def _args(select=None, *, fmt="text", dry_run=False) -> argparse.Namespace:
    return argparse.Namespace(
        db="unused.db",
        format=fmt,
        dry_run=dry_run,
        select=list(select or []),
        config="unused.yaml",
        env=None,
    )


@pytest.fixture
def duckdb_project(tmp_path):
    """A two-stage project on one persistent DuckDB connection.

    `raw.orders -> gold.orders -> mart.kpi`. The second pipeline reads what the
    first writes, so running them out of order fails on a missing table — which
    is what makes this an ordering test rather than a smoke test.
    """
    duckdb = pytest.importorskip("duckdb")
    connection = duckdb.connect(str(tmp_path / "warehouse.duckdb"))
    connection.execute("CREATE SCHEMA raw")
    connection.execute(
        "CREATE TABLE raw.orders AS SELECT * FROM (VALUES (1, 100), (2, 250)) AS t(id, amount)"
    )

    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "priority_check": ["LOCAL_SQL"],
                "default_env": "LOCAL_SQL",
                "environments": {
                    "LOCAL_SQL": {
                        "catalog": None,
                        "engine": "sql",
                        "adapter": "duckdb",
                        "database": str(tmp_path / "warehouse.duckdb"),
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    def factory():
        engine = SkiferEngine(
            config_path=str(config_path), force_env="LOCAL_SQL", connection=connection
        )
        # The Smart Sandbox would redirect writes to a suffixed schema; this test
        # is about ordering, not about sandbox resolution.
        engine.schema_suffix = ""
        return engine

    try:
        yield connection, tmp_path, config_path, factory
    finally:
        connection.close()


def _pipeline(directory: Path, name: str, source: str) -> Path:
    path = directory / name
    path.write_text(
        f"""
tables:
  - name: {source}
    alias: src
select_final:
  - [id, id]
  - [amount, amount]
""",
        encoding="utf-8",
    )
    return path


def _two_stage_store(directory: Path) -> SqliteMetadataStore:
    store = SqliteMetadataStore(":memory:")
    index_from_path(
        str(_pipeline(directory, "a.yaml", "raw.orders")), store, target_fqn="gold.orders"
    )
    index_from_path(
        str(_pipeline(directory, "b.yaml", "gold.orders")), store, target_fqn="mart.kpi"
    )
    return store


def test_a_selection_runs_producer_before_consumer_for_real(duckdb_project, capsys):
    connection, directory, _, factory = duckdb_project
    store = _two_stage_store(directory)

    code = run_run_command(_args(), store=store, engine_factory=factory)

    assert code == GRAPH_EXIT_OK, capsys.readouterr()
    assert connection.execute(
        "SELECT id, amount FROM mart.kpi ORDER BY id"
    ).fetchall() == [(1, 100), (2, 250)]


def test_selecting_only_the_consumer_does_not_run_the_producer(duckdb_project, capsys):
    """`--select mart.kpi` must not quietly rebuild `gold.orders` too."""
    connection, directory, _, factory = duckdb_project
    store = _two_stage_store(directory)

    code = run_run_command(_args(["mart.kpi"]), store=store, engine_factory=factory)

    capsys.readouterr()
    assert code == GRAPH_EXIT_ERROR
    assert connection.execute(
        "SELECT count(*) FROM duckdb_schemas() WHERE schema_name = 'gold'"
    ).fetchone() == (0,)


def test_a_logical_data_product_id_is_refused_before_anything_opens(
    duckdb_project, capsys
):
    """`data_product.id` is not a table address: `sales.orders` is published to
    `gold.orders` routinely, and writing to the id would create a table nobody
    declared, under a name that reads entirely plausible."""
    connection, directory, _, factory = duckdb_project
    path = directory / "product.yaml"
    path.write_text(
        """
data_product: {id: sales.orders, version: 1.0.0}
tables:
  - name: raw.orders
    alias: src
select_final:
  - [id, id]
  - [amount, amount]
""",
        encoding="utf-8",
    )
    store = SqliteMetadataStore(":memory:")
    index_from_path(str(path), store)

    code = run_run_command(_args(), store=store, engine_factory=factory)

    captured = capsys.readouterr()
    assert code == GRAPH_EXIT_USAGE
    assert "no physical target" in captured.err
    assert "data_product" in captured.err
    assert connection.execute(
        "SELECT count(*) FROM duckdb_tables() WHERE schema_name = 'sales'"
    ).fetchone() == (0,)


def test_a_derived_placeholder_target_is_refused(duckdb_project, capsys):
    """Without a sink or a data product the index falls back to
    `<first table>_output` — a name the project never declared."""
    connection, directory, _, factory = duckdb_project
    store = SqliteMetadataStore(":memory:")
    index_from_path(str(_pipeline(directory, "bare.yaml", "raw.orders")), store)

    code = run_run_command(_args(), store=store, engine_factory=factory)

    captured = capsys.readouterr()
    assert code == GRAPH_EXIT_USAGE
    assert "'derived'" in captured.err
    assert connection.execute(
        "SELECT count(*) FROM duckdb_tables() WHERE table_name LIKE '%_output'"
    ).fetchone() == (0,)


def test_a_declared_sink_is_a_physical_target_and_runs(duckdb_project, capsys):
    connection, directory, _, factory = duckdb_project
    path = directory / "sunk.yaml"
    path.write_text(
        """
tables:
  - name: raw.orders
    alias: src
select_final:
  - [id, id]
  - [amount, amount]
sink: {type: delta, schema: gold, table: from_sink}
""",
        encoding="utf-8",
    )
    store = SqliteMetadataStore(":memory:")
    index_from_path(str(path), store)

    code = run_run_command(_args(), store=store, engine_factory=factory)

    assert code == GRAPH_EXIT_OK, capsys.readouterr()
    assert connection.execute("SELECT count(*) FROM gold.from_sink").fetchone() == (2,)


def test_one_refusal_refuses_the_whole_selection(duckdb_project, capsys):
    """Running the runnable subset would make the plan the caller read and the
    work actually done diverge, silently."""
    connection, directory, _, factory = duckdb_project
    store = _two_stage_store(directory)
    index_from_path(str(_pipeline(directory, "bare.yaml", "raw.orders")), store)

    code = run_run_command(_args(), store=store, engine_factory=factory)

    capsys.readouterr()
    assert code == GRAPH_EXIT_USAGE
    assert connection.execute(
        "SELECT count(*) FROM duckdb_schemas() WHERE schema_name IN ('gold', 'mart')"
    ).fetchone() == (0,)


def test_a_failure_blocks_only_what_reads_the_failed_pipeline():
    """An unrelated pipeline must still run, or the skipped list means nothing."""
    graph = PipelineGraph(
        nodes=("broken", "downstream", "unrelated"),
        edges=(PipelineEdge("broken", "downstream"),),
        external_sources=(),
        pipeline_paths=(
            ("broken", "broken.yaml"),
            ("downstream", "downstream.yaml"),
            ("unrelated", "unrelated.yaml"),
        ),
    )
    ran = []

    def run_one(node):
        ran.append(node)
        if node == "broken":
            raise RuntimeError("boom")

    outcomes = run_selection(graph, ("broken", "downstream", "unrelated"), run_one)

    assert ran == ["broken", "unrelated"]
    assert {outcome.node: outcome.state for outcome in outcomes} == {
        "broken": "failed",
        "downstream": "skipped",
        "unrelated": "succeeded",
    }


def test_a_failure_is_reported_with_its_cause_and_exits_one(duckdb_project, capsys):
    connection, directory, _, factory = duckdb_project
    store = SqliteMetadataStore(":memory:")
    index_from_path(
        str(_pipeline(directory, "missing.yaml", "raw.absent")),
        store,
        target_fqn="gold.nope",
    )

    code = run_run_command(_args(), store=store, engine_factory=factory)

    out = capsys.readouterr().out
    assert code == GRAPH_EXIT_ERROR
    assert "failed" in out
    assert "gold.nope" in out
    assert "0 succeeded, 1 failed, 0 skipped." in out


def test_an_in_memory_database_is_refused_because_the_run_would_vanish(tmp_path, capsys):
    """Two engines built from the same config share nothing when the database is
    in memory, and the process boundary loses everything a run wrote."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "priority_check": ["LOCAL_SQL"],
                "default_env": "LOCAL_SQL",
                "environments": {
                    "LOCAL_SQL": {
                        "catalog": None,
                        "engine": "sql",
                        "adapter": "duckdb",
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    store = _two_stage_store(tmp_path)
    args = _args()
    args.config = str(config_path)
    args.env = "LOCAL_SQL"

    code = run_run_command(args, store=store)

    assert code == GRAPH_EXIT_ERROR
    assert "in-memory database" in capsys.readouterr().err


def test_dry_run_still_works_without_a_config(tmp_path, capsys):
    """Planning must not start needing an engine now that running exists."""
    store = _two_stage_store(tmp_path)

    code = run_run_command(_args(dry_run=True), store=store)

    assert code == GRAPH_EXIT_OK
    assert "dry run" in capsys.readouterr().out
