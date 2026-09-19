"""Selection grammar and execution order for `skifer run --select` (Plan 39.6.3a)."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

import pytest

from skifer.cli import GRAPH_EXIT_ERROR, GRAPH_EXIT_OK, GRAPH_EXIT_USAGE, run_run_command
from skifer.observability.metadata_index import index_from_path
from skifer.observability.metadata_store import SqliteMetadataStore
from skifer.observability.pipeline_graph import (
    PipelineEdge,
    PipelineGraph,
    PipelineGraphCycleError,
    PipelineSelectionError,
    build_pipeline_graph,
    parse_selector,
    select_nodes,
    topological_order,
)


def _args(select=None, *, fmt="text", dry_run=True) -> argparse.Namespace:
    return argparse.Namespace(
        db="unused.db",
        format=fmt,
        dry_run=dry_run,
        select=list(select or []),
    )


def _write(tmp_path: Path, name: str, source_fqn: str) -> Path:
    path = tmp_path / name
    path.write_text(
        f"""
tables:
  - name: {source_fqn}
    alias: src
select_final:
  - [id, id]
""",
        encoding="utf-8",
    )
    return path


def _chain(tmp_path: Path) -> SqliteMetadataStore:
    """raw.orders -> gold.orders -> mart.kpi -> report.final."""
    store = SqliteMetadataStore(":memory:")
    for filename, source, target in (
        ("a.yaml", "raw.orders", "gold.orders"),
        ("b.yaml", "gold.orders", "mart.kpi"),
        ("c.yaml", "mart.kpi", "report.final"),
    ):
        index_from_path(str(_write(tmp_path, filename, source)), store, target_fqn=target)
    return store


def _diamond(tmp_path: Path) -> SqliteMetadataStore:
    """One root read by two independent siblings, both read by one leaf."""
    store = SqliteMetadataStore(":memory:")
    index_from_path(str(_write(tmp_path, "root.yaml", "raw.in")), store, target_fqn="l0.root")
    index_from_path(str(_write(tmp_path, "left.yaml", "l0.root")), store, target_fqn="l1.left")
    index_from_path(str(_write(tmp_path, "right.yaml", "l0.root")), store, target_fqn="l1.right")

    leaf = tmp_path / "leaf.yaml"
    leaf.write_text(
        """
tables:
  - name: l1.left
    alias: a
  - name: l1.right
    alias: b
join:
  - table_from: [a, id]
    table_to: [b, id]
    type: inner
select_final:
  - [id, id]
""",
        encoding="utf-8",
    )
    index_from_path(str(leaf), store, target_fqn="l2.leaf")
    return store


def test_producer_runs_before_every_consumer(tmp_path):
    order = topological_order(build_pipeline_graph(_diamond(tmp_path)))

    assert order.index("l0.root") < order.index("l1.left")
    assert order.index("l0.root") < order.index("l1.right")
    assert order.index("l1.left") < order.index("l2.leaf")
    assert order.index("l1.right") < order.index("l2.leaf")


def test_among_ready_pipelines_the_first_by_name_runs_first(tmp_path):
    """Pin the tie-break rule, which the graph alone does not decide.

    Two roots, one of which has a child: `a.root`, `z.root`, and `a.root -> b.mid`.
    After `a.root` runs, both `z.root` and `b.mid` are ready and neither depends
    on the other, so several topological orders are correct. Without an explicit
    rule the answer falls out of append order, which is stable enough to look
    intentional and changes the day the traversal does. The rule is: among the
    pipelines that are ready, the first by name runs first.
    """
    store = SqliteMetadataStore(":memory:")
    index_from_path(str(_write(tmp_path, "r1.yaml", "raw.in")), store, target_fqn="a.root")
    index_from_path(str(_write(tmp_path, "r2.yaml", "raw.in")), store, target_fqn="z.root")
    index_from_path(str(_write(tmp_path, "m.yaml", "a.root")), store, target_fqn="b.mid")

    order = topological_order(build_pipeline_graph(store))

    assert list(order) == ["a.root", "b.mid", "z.root"]


def test_order_of_independent_siblings_is_stable_across_processes(tmp_path):
    """The same registry must produce the same plan under any PYTHONHASHSEED.

    This guards the traversal against ever becoming dependent on set or dict
    iteration; the tie-break rule itself is pinned by the test above.
    """
    store = _diamond(tmp_path)
    first = topological_order(build_pipeline_graph(store))

    code = (
        "import sys; sys.path.insert(0, %r);"
        "from tests.test_cli_run_select import _diamond;"
        "from skifer.observability.pipeline_graph import build_pipeline_graph, topological_order;"
        "import pathlib;"
        "print(','.join(topological_order(build_pipeline_graph(_diamond(pathlib.Path(%r))))))"
        % (str(Path(__file__).resolve().parent.parent), str(tmp_path))
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        env={"PYTHONHASHSEED": "1", "PATH": "/usr/bin:/bin"},
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip().split(",") == list(first)


@pytest.mark.parametrize(
    "selector, expected",
    [
        ("mart.kpi", ["mart.kpi"]),
        ("mart.kpi+", ["mart.kpi", "report.final"]),
        ("+mart.kpi", ["gold.orders", "mart.kpi"]),
        ("+mart.kpi+", ["gold.orders", "mart.kpi", "report.final"]),
    ],
)
def test_plus_travels_towards_the_side_it_sits_on(tmp_path, selector, expected):
    graph = build_pipeline_graph(_chain(tmp_path))

    assert list(select_nodes(graph, [selector])) == expected


def test_no_selector_selects_every_indexed_pipeline(tmp_path):
    graph = build_pipeline_graph(_chain(tmp_path))

    assert select_nodes(graph, []) == topological_order(graph)


def test_repeated_select_is_a_union_kept_in_dependency_order(tmp_path):
    graph = build_pipeline_graph(_chain(tmp_path))

    selected = select_nodes(graph, ["report.final", "gold.orders"])

    assert list(selected) == ["gold.orders", "report.final"]


def test_a_pipeline_can_be_named_by_its_yaml_path_or_its_target(tmp_path):
    graph = build_pipeline_graph(_chain(tmp_path))

    assert select_nodes(graph, ["b.yaml"]) == select_nodes(graph, ["mart.kpi"])


def test_an_ambiguous_path_is_refused_by_name_never_arbitrated(tmp_path):
    """Two pipelines whose paths end the same way must not be silently picked from."""
    store = SqliteMetadataStore(":memory:")
    for folder, target in (("silver", "silver.orders"), ("gold", "gold.orders")):
        directory = tmp_path / folder
        directory.mkdir()
        index_from_path(
            str(_write(directory, "orders.yaml", "raw.orders")),
            store,
            target_fqn=target,
        )
    graph = build_pipeline_graph(store)

    with pytest.raises(PipelineSelectionError) as exc_info:
        select_nodes(graph, ["orders.yaml"])

    message = str(exc_info.value)
    assert "gold.orders" in message and "silver.orders" in message


def test_unknown_selector_says_how_to_fix_it(tmp_path):
    graph = build_pipeline_graph(_chain(tmp_path))

    with pytest.raises(PipelineSelectionError, match="skifer index"):
        select_nodes(graph, ["mart.kpy"])


def test_a_lone_plus_selects_nothing_and_says_so():
    with pytest.raises(PipelineSelectionError, match="selects nothing"):
        parse_selector("+")


def test_an_empty_selector_is_refused():
    with pytest.raises(PipelineSelectionError, match="cannot be empty"):
        parse_selector("   ")


def test_a_hand_built_cycle_is_refused_rather_than_partially_ordered():
    """A partial order would silently drop the pipelines inside the cycle."""
    graph = PipelineGraph(
        nodes=("a", "b"),
        edges=(PipelineEdge("a", "b"), PipelineEdge("b", "a")),
        external_sources=(),
        pipeline_paths=(("a", "a.yaml"), ("b", "b.yaml")),
    )

    with pytest.raises(PipelineGraphCycleError):
        topological_order(graph)


def test_dry_run_prints_the_plan_and_succeeds(tmp_path, capsys):
    store = _chain(tmp_path)

    assert run_run_command(_args(["+mart.kpi"]), store=store) == GRAPH_EXIT_OK

    out = capsys.readouterr().out
    assert "1. gold.orders" in out
    assert "2. mart.kpi" in out
    assert "report.final" not in out


def test_dry_run_json_carries_order_and_path(tmp_path, capsys):
    store = _chain(tmp_path)

    assert run_run_command(_args(["mart.kpi+"], fmt="json"), store=store) == GRAPH_EXIT_OK

    payload = json.loads(capsys.readouterr().out)
    assert payload["selected"] == ["mart.kpi", "report.final"]
    assert payload["plan"][0] == {
        "order": 0,
        "node": "mart.kpi",
        "pipeline_path": str(tmp_path / "b.yaml"),
    }


def test_without_dry_run_the_command_refuses_instead_of_pretending(tmp_path, capsys):
    """Execution is slice 39.6.3b; until then the flag must not quietly no-op."""
    store = _chain(tmp_path)

    code = run_run_command(_args(["mart.kpi"], dry_run=False), store=store)

    assert code == GRAPH_EXIT_USAGE
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "execution is not" in captured.err


def test_unknown_selector_exits_two_from_the_command(tmp_path, capsys):
    store = _chain(tmp_path)

    assert run_run_command(_args(["nope"]), store=store) == GRAPH_EXIT_USAGE
    assert "nope" in capsys.readouterr().err


def test_cycle_exits_one_not_two(tmp_path, capsys):
    """A cycle is a broken project, not a misuse of the command line."""
    store = SqliteMetadataStore(":memory:")
    index_from_path(
        str(_write(tmp_path, "self.yaml", "gold.loop")), store, target_fqn="gold.loop"
    )

    assert run_run_command(_args(), store=store) == GRAPH_EXIT_ERROR
    assert "cycle" in capsys.readouterr().err.lower()


def test_planning_never_imports_pyspark(tmp_path):
    """The planner reads YAML and SQLite only; importing Spark to order a plan
    would make a command that runs nothing pay for a session."""
    store_path = tmp_path / "reg.db"
    store = SqliteMetadataStore(str(store_path))
    for filename, source, target in (
        ("a.yaml", "raw.orders", "gold.orders"),
        ("b.yaml", "gold.orders", "mart.kpi"),
    ):
        index_from_path(str(_write(tmp_path, filename, source)), store, target_fqn=target)

    code = (
        "import sys;"
        f"sys.argv = ['skifer', 'run', '--dry-run', '--db', {str(store_path)!r}];"
        "from skifer.cli import main;"
        "\ntry:\n    main()\nexcept SystemExit:\n    pass\n"
        "assert 'pyspark' not in sys.modules, 'planning imported pyspark'"
    )
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)

    assert result.returncode == 0, result.stderr
