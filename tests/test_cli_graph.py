from __future__ import annotations

import argparse
import builtins
import json
import subprocess
import sys
from pathlib import Path

import pytest

from skifer.cli import GRAPH_EXIT_ERROR, GRAPH_EXIT_OK, run_graph_command
from skifer.observability.metadata_index import index_from_path
from skifer.observability.metadata_store import SqliteMetadataStore
from skifer.observability.pipeline_graph import (
    PipelineGraphCycleError,
    build_pipeline_graph,
)


def _args(fmt: str = "text") -> argparse.Namespace:
    return argparse.Namespace(db="unused.db", format=fmt)


def _run_cli(*args: str) -> subprocess.CompletedProcess:
    code = (
        f"import sys; sys.argv = {['skifer', *args]!r}; "
        "from skifer.cli import main; main()"
    )
    return subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
    )


def _write_pipeline(
    tmp_path: Path,
    name: str,
    *,
    target_fqn: str,
    source_fqn: str,
) -> Path:
    path = tmp_path / name
    path.write_text(
        f"""
data_product: {{id: {target_fqn.replace("`", "")}, version: 1.0.0}}
tables:
  - name: {source_fqn}
    alias: src
select_final:
  - [id, id]
sink: {{type: delta, schema: gold, table: unused}}
""",
        encoding="utf-8",
    )
    return path


def _indexed_store(tmp_path: Path, *pipelines: tuple[str, str, str]) -> SqliteMetadataStore:
    store = SqliteMetadataStore(":memory:")
    for filename, target_fqn, source_fqn in pipelines:
        index_from_path(
            str(
                _write_pipeline(
                    tmp_path,
                    filename,
                    target_fqn=target_fqn,
                    source_fqn=source_fqn,
                )
            ),
            store,
            target_fqn=target_fqn,
        )
    return store


def test_graph_links_indexed_producer_to_consumer_in_the_right_direction(tmp_path):
    store = _indexed_store(
        tmp_path,
        ("orders.yaml", "gold.orders", "raw.orders"),
        ("kpi.yaml", "mart.kpi", "gold.orders"),
    )

    graph = build_pipeline_graph(store)

    assert [edge.to_dict() for edge in graph.edges] == [
        {"producer": "gold.orders", "consumer": "mart.kpi"}
    ]


def test_graph_names_declared_external_sources_without_guessing_edges(tmp_path):
    store = _indexed_store(
        tmp_path,
        ("orders.yaml", "gold.orders", "raw.orders"),
    )

    graph = build_pipeline_graph(store)

    assert graph.edges == ()
    assert [source.to_dict() for source in graph.external_sources] == [
        {"consumer": "gold.orders", "source": "raw.orders", "kind": "table"}
    ]


def test_graph_cycle_is_named_without_recursion_error(tmp_path):
    store = _indexed_store(
        tmp_path,
        ("a.yaml", "gold.a", "gold.b"),
        ("b.yaml", "gold.b", "gold.a"),
    )

    with pytest.raises(PipelineGraphCycleError, match="gold.a -> gold.b -> gold.a"):
        build_pipeline_graph(store)


def test_graph_output_is_deterministic_byte_for_byte(tmp_path, capsys):
    store = _indexed_store(
        tmp_path,
        ("b.yaml", "mart.b", "gold.a"),
        ("a.yaml", "gold.a", "raw.a"),
    )

    assert run_graph_command(_args("json"), store=store) == GRAPH_EXIT_OK
    first = capsys.readouterr().out
    assert run_graph_command(_args("json"), store=store) == GRAPH_EXIT_OK
    second = capsys.readouterr().out

    assert first == second


def test_graph_formats_are_valid_and_json_is_parseable(tmp_path, capsys):
    store = _indexed_store(
        tmp_path,
        ("orders.yaml", "gold.orders", "raw.orders"),
        ("kpi.yaml", "mart.kpi", "gold.orders"),
    )

    assert run_graph_command(_args("text"), store=store) == GRAPH_EXIT_OK
    text = capsys.readouterr().out
    assert "gold.orders -> mart.kpi" in text
    assert "gold.orders <- raw.orders" in text

    assert run_graph_command(_args("json"), store=store) == GRAPH_EXIT_OK
    payload = json.loads(capsys.readouterr().out)
    assert payload["edges"] == [{"consumer": "mart.kpi", "producer": "gold.orders"}]
    assert payload["external_sources"] == [
        {"consumer": "gold.orders", "source": "raw.orders", "kind": "table"}
    ]

    assert run_graph_command(_args("mermaid"), store=store) == GRAPH_EXIT_OK
    mermaid = capsys.readouterr().out
    assert mermaid.startswith("graph LR\n")
    assert "external" in mermaid
    assert "-->" in mermaid


def test_graph_subcommand_reads_sqlite_registry(tmp_path):
    db_path = tmp_path / "metadata.db"
    store = SqliteMetadataStore(str(db_path))
    for filename, target_fqn, source_fqn in (
        ("orders.yaml", "gold.orders", "raw.orders"),
        ("kpi.yaml", "mart.kpi", "gold.orders"),
    ):
        index_from_path(
            str(
                _write_pipeline(
                    tmp_path,
                    filename,
                    target_fqn=target_fqn,
                    source_fqn=source_fqn,
                )
            ),
            store,
            target_fqn=target_fqn,
        )
    store.close()

    result = _run_cli("graph", "--db", str(db_path), "--format", "json")

    assert result.returncode == GRAPH_EXIT_OK, result.stdout + result.stderr
    assert json.loads(result.stdout)["summary"] == {
        "edges": 1,
        "external_sources": 1,
        "nodes": 2,
    }


def test_graph_never_imports_pyspark(tmp_path, capsys, monkeypatch):
    store = _indexed_store(
        tmp_path,
        ("orders.yaml", "gold.orders", "raw.orders"),
    )
    real_import = builtins.__import__

    def guarded(name, *args, **kwargs):
        if name == "pyspark" or name.startswith("pyspark."):
            raise AssertionError(f"'skifer graph' imported {name!r}")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded)

    assert run_graph_command(_args("json"), store=store) == GRAPH_EXIT_OK
    assert json.loads(capsys.readouterr().out)["nodes"] == ["gold.orders"]


def test_empty_registry_renders_empty_graph(capsys):
    store = SqliteMetadataStore(":memory:")

    assert run_graph_command(_args("json"), store=store) == GRAPH_EXIT_OK

    assert json.loads(capsys.readouterr().out) == {
        "edges": [],
        "external_sources": [],
        "nodes": [],
        "summary": {"edges": 0, "external_sources": 0, "nodes": 0},
    }


def test_quoted_fqn_matches_but_two_part_does_not_guess_three_part(tmp_path):
    store = _indexed_store(
        tmp_path,
        ("orders.yaml", "`gold`.`orders`", "raw.orders"),
        ("quoted_consumer.yaml", "mart.quoted", "gold.orders"),
        ("catalog_consumer.yaml", "mart.catalog", "main.gold.orders"),
    )

    graph = build_pipeline_graph(store)

    assert {"producer": "`gold`.`orders`", "consumer": "mart.quoted"} in [
        edge.to_dict() for edge in graph.edges
    ]
    assert {
        "consumer": "mart.catalog",
        "source": "main.gold.orders",
        "kind": "table",
    } in [source.to_dict() for source in graph.external_sources]


def test_graph_cycle_cli_reports_named_error(tmp_path, capsys):
    store = _indexed_store(
        tmp_path,
        ("a.yaml", "gold.a", "gold.b"),
        ("b.yaml", "gold.b", "gold.a"),
    )

    assert run_graph_command(_args("text"), store=store) == GRAPH_EXIT_ERROR
    captured = capsys.readouterr()
    assert "gold.a -> gold.b -> gold.a" in captured.err
    assert "RecursionError" not in captured.err


def test_pipeline_reading_its_own_output_is_reported_as_a_cycle(tmp_path):
    """A self-edge must be kept, not dropped, so the cycle check can name it.

    Silently ignoring `producer == consumer` would make a pipeline that declares
    its own target as a source look like a perfectly acyclic graph — the graph
    would be wrong and say nothing.
    """
    store = _indexed_store(tmp_path, ("self.yaml", "gold.self", "gold.self"))

    with pytest.raises(PipelineGraphCycleError) as exc_info:
        build_pipeline_graph(store)

    assert "gold.self -> gold.self" in str(exc_info.value)


def test_file_source_named_like_another_target_is_not_an_edge(tmp_path):
    """A CSV read under a table's name must not fabricate a dependency.

    A pipeline may declare `name: gold.orders` with `source: {type: csv}` — it
    reads a file, not the `gold.orders` table another pipeline writes. Matching
    the declared name against indexed targets invented an edge here, which in
    `--select` decides execution order: the consumer would wait for a producer
    it never reads, and be skipped when that producer fails.
    """
    producer = tmp_path / "producer.yaml"
    producer.write_text(
        """
tables:
  - name: raw.orders
    alias: src
select_final:
  - [id, id]
""",
        encoding="utf-8",
    )
    consumer = tmp_path / "consumer.yaml"
    consumer.write_text(
        """
tables:
  - name: gold.orders
    alias: src
    source:
      type: csv
      path: /data/orders.csv
select_final:
  - [id, id]
""",
        encoding="utf-8",
    )
    store = SqliteMetadataStore(":memory:")
    index_from_path(str(producer), store, target_fqn="gold.orders")
    index_from_path(str(consumer), store, target_fqn="mart.report")

    graph = build_pipeline_graph(store)

    assert graph.edges == ()
    assert {
        "consumer": "mart.report",
        "source": "gold.orders",
        "kind": "file",
    } in [source.to_dict() for source in graph.external_sources]


def test_loader_input_is_reported_as_a_loader_not_a_missing_table(tmp_path):
    """A loader never becomes an edge, so the graph must not imply it might."""
    path = tmp_path / "loader.yaml"
    path.write_text(
        """
tables:
  - name: gold.orders
    alias: src
    source_type: loader
    function_name: load_orders
select_final:
  - [id, id]
""",
        encoding="utf-8",
    )
    store = SqliteMetadataStore(":memory:")
    index_from_path(str(path), store, target_fqn="mart.loaded")

    graph = build_pipeline_graph(store)

    assert graph.edges == ()
    assert [source.kind for source in graph.external_sources] == ["loader"]
