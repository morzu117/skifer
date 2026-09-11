from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone

import pytest

from skifer.cli import (
    META_EXIT_NOT_FOUND,
    META_EXIT_OK,
    main,
    run_dictionary_command,
    run_lineage_command,
)
from skifer.lineage.tracker import LineageEdge, LineageGraph
from skifer.observability.metadata_store import (
    ColumnRecord,
    DatasetRecord,
    SqliteMetadataStore,
)


NOW = datetime(2026, 9, 11, 12, 0, tzinfo=timezone.utc)


def _record(
    target_fqn: str,
    *,
    columns: tuple[ColumnRecord, ...],
    edges: tuple[LineageEdge, ...] = (),
) -> DatasetRecord:
    graph = LineageGraph()
    for edge in edges:
        graph.add_edge(edge)
    return DatasetRecord(
        target_fqn=target_fqn,
        pipeline_path=f"schemas/{target_fqn.replace('.', '_')}.yaml",
        data_product_id=target_fqn,
        contract_version="1.0.0",
        definition_hash=f"sha256:{target_fqn}",
        owner=None,
        columns=columns,
        indexed_at=NOW,
        lineage=graph.to_dict() if edges else {},
    )


def _store() -> SqliteMetadataStore:
    store = SqliteMetadataStore(":memory:")
    for record in (
        _record(
            "silver.orders",
            columns=(
                ColumnRecord(
                    name="order_id",
                    logical_type="identifier",
                    classification="internal",
                    description="Order identifier",
                    sources=("id",),
                ),
                ColumnRecord(
                    name="amount",
                    logical_type="currency",
                    classification="confidential",
                    description="Order amount",
                    sources=("amount",),
                ),
            ),
            edges=(
                LineageEdge("raw.orders", "id", "silver.orders", "order_id"),
                LineageEdge("raw.orders", "amount", "silver.orders", "amount"),
            ),
        ),
        _record(
            "gold.kpi",
            columns=(
                ColumnRecord(
                    name="order_count",
                    logical_type="number",
                    classification="internal",
                    description="Number of orders",
                    sources=("order_id",),
                ),
                ColumnRecord(
                    name="amount_eur",
                    logical_type="currency",
                    classification="confidential",
                    description="Amount in euros",
                    sources=("amount",),
                ),
            ),
            edges=(
                LineageEdge(
                    "silver.orders",
                    "amount",
                    "gold.kpi",
                    "amount_eur",
                    transformations=["cast:double"],
                ),
                LineageEdge(
                    "silver.orders",
                    "order_id",
                    "gold.kpi",
                    "order_count",
                    transformations=["count"],
                    edge_type="metric",
                ),
            ),
        ),
    ):
        store.upsert(record)
    return store


def _lineage_args(
    target: str,
    *,
    direction: str = "down",
    output_format: str = "mermaid",
) -> argparse.Namespace:
    return argparse.Namespace(
        target=target,
        direction=direction,
        format=output_format,
        db="unused.db",
    )


def _dictionary_args(target: str, *, output_format: str = "text") -> argparse.Namespace:
    return argparse.Namespace(target=target, format=output_format, db="unused.db")


def test_lineage_down_mermaid_exit_0_and_stable(capsys):
    store = _store()
    args = _lineage_args("silver.orders", direction="down", output_format="mermaid")

    first_exit = run_lineage_command(args, store=store)
    first = capsys.readouterr().out
    second_exit = run_lineage_command(args, store=store)
    second = capsys.readouterr().out

    assert first_exit == META_EXIT_OK
    assert second_exit == META_EXIT_OK
    assert first == second
    assert first.startswith("graph LR")
    assert "silver.orders.amount" in first
    assert "gold.kpi.amount_eur" in first


def test_lineage_up_json_stable(capsys):
    store = _store()
    args = _lineage_args("gold.kpi.amount_eur", direction="up", output_format="json")

    first_exit = run_lineage_command(args, store=store)
    first = capsys.readouterr().out
    second_exit = run_lineage_command(args, store=store)
    second = capsys.readouterr().out

    assert first_exit == META_EXIT_OK
    assert second_exit == META_EXIT_OK
    assert first == second
    payload = json.loads(first)
    assert payload["summary"]["total_edges"] == 2
    assert payload["tables"] == ["gold.kpi", "raw.orders", "silver.orders"]


def test_lineage_unknown_fqn_exit_3(capsys):
    exit_code = run_lineage_command(
        _lineage_args("missing.orders.amount", direction="up"),
        store=_store(),
    )

    assert exit_code == META_EXIT_NOT_FOUND
    assert "not found" in capsys.readouterr().err


def test_lineage_column_form_limits_to_selected_column(capsys):
    exit_code = run_lineage_command(
        _lineage_args("gold.kpi.amount_eur", direction="up"),
        store=_store(),
    )

    output = capsys.readouterr().out
    assert exit_code == META_EXIT_OK
    assert "gold.kpi.amount_eur" in output
    assert "raw.orders.amount" in output
    assert "order_count" not in output


def test_dictionary_text_sorted(capsys):
    exit_code = run_dictionary_command(_dictionary_args("gold.kpi"), store=_store())

    lines = capsys.readouterr().out.splitlines()
    assert exit_code == META_EXIT_OK
    assert lines[0].startswith("name")
    assert lines[1].split()[0] == "amount_eur"
    assert lines[2].split()[0] == "order_count"


def test_dictionary_json_contains_classification(capsys):
    exit_code = run_dictionary_command(
        _dictionary_args("gold.kpi", output_format="json"),
        store=_store(),
    )

    payload = json.loads(capsys.readouterr().out)
    amount = payload["columns"][0]
    assert exit_code == META_EXIT_OK
    assert payload["target_fqn"] == "gold.kpi"
    assert amount["name"] == "amount_eur"
    assert amount["classification"] == "confidential"
    assert amount["sources"] == ["amount"]


def test_dictionary_unknown_fqn_exit_3(capsys):
    exit_code = run_dictionary_command(
        _dictionary_args("missing.orders"),
        store=_store(),
    )

    assert exit_code == META_EXIT_NOT_FOUND
    assert "not found" in capsys.readouterr().err


def test_dictionary_bad_format_exit_2(monkeypatch):
    monkeypatch.setattr(
        sys,
        "argv",
        ["skifer", "dictionary", "gold.kpi", "--format", "xml"],
    )

    with pytest.raises(SystemExit) as caught:
        main()

    assert caught.value.code == 2
