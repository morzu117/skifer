"""Classification propagation over static column lineage (Plan 31.3.1)."""
import logging

import pytest

from skifer.lineage.classification import (
    ClassificationPropagationWarning,
    resolve_field_classifications,
)
from skifer.lineage.tracker import LineageEdge, LineageGraph


def _graph(*edges: LineageEdge) -> LineageGraph:
    graph = LineageGraph()
    for edge in edges:
        graph.add_edge(edge)
    return graph


def test_propagation_inherits_highest_source_level_via_rule():
    graph = _graph(
        LineageEdge("orders", "gross", "gold.orders", "net_amount", edge_type="rule"),
        LineageEdge("orders", "discount", "gold.orders", "net_amount", edge_type="rule"),
    )

    with pytest.warns(ClassificationPropagationWarning):
        effective = resolve_field_classifications(
            graph,
            "gold.orders",
            {},
            {"gross": "internal", "discount": "confidential"},
        )

    assert effective == {"net_amount": "confidential"}


def test_propagation_via_join_key():
    graph = _graph(
        LineageEdge(
            "silver.orders",
            "customer_id",
            "silver.customers",
            "customer_id",
            edge_type="join",
        )
    )

    with pytest.warns(ClassificationPropagationWarning):
        effective = resolve_field_classifications(
            graph,
            "silver.customers",
            {},
            {"customer_id": "restricted"},
        )

    assert effective["customer_id"] == "restricted"


def test_explicit_lowering_is_allowed_and_logged(caplog):
    graph = _graph(LineageEdge("customers", "ssn", "gold.customers", "customer_ref"))

    with caplog.at_level(logging.WARNING):
        effective = resolve_field_classifications(
            graph,
            "gold.customers",
            {"customer_ref": "internal"},
            {"ssn": "pii"},
        )

    assert effective["customer_ref"] == "internal"
    assert "customer_ref" in caplog.text
    assert "inferred 'pii', declared 'internal'" in caplog.text


def test_explicit_elevation_is_silent(caplog):
    graph = _graph(LineageEdge("orders", "region", "gold.orders", "region"))

    with caplog.at_level(logging.WARNING):
        effective = resolve_field_classifications(
            graph,
            "gold.orders",
            {"region": "restricted"},
            {"region": "internal"},
        )

    assert effective["region"] == "restricted"
    assert caplog.records == []


def test_strict_mode_raises_on_inferred_elevation():
    graph = _graph(LineageEdge("customers", "email", "gold.customers", "email"))

    with pytest.raises(ValueError, match="email.*confidential"):
        resolve_field_classifications(
            graph,
            "gold.customers",
            {},
            {"email": "confidential"},
            mode="strict",
        )
