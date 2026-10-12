"""Classification propagation over static column lineage (Plan 31.3.1)."""
import logging

import pytest

from skifer.lineage.classification import (
    ClassificationPropagationWarning,
    ClassificationViolationError,
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

    with pytest.raises(ClassificationViolationError, match="email.*confidential") as caught:
        resolve_field_classifications(
            graph,
            "gold.customers",
            {},
            {"email": "confidential"},
            mode="strict",
        )

    assert isinstance(caught.value, ValueError)
    assert caught.value.column == "email"


def _aggregate_graph(measures: str):
    """Lineage for a pipeline whose output is produced by `aggregate:`."""
    from skifer.core.schema_loader import parse_schema
    from skifer.lineage.tracker import LineageTracker

    schema = parse_schema(
        f"""
tables:
  - name: silver.orders
    alias: o
aggregate:
  group_by: [country]
  measures:
{measures}
"""
    )
    return LineageTracker.from_schema(schema, target_name="gold.agg")


def test_classification_propagates_through_an_aggregate():
    """A `pii` column folded by an aggregate must not lose its classification.

    `first` reports the value as-is, so `contacts` is every bit as sensitive as
    the `email` it came from. Before the tracker learned `aggregate:` this
    returned `{}`: no edge existed, so nothing was inferred.
    """
    graph = _aggregate_graph("    - [email, contacts, first]\n")

    with pytest.warns(ClassificationPropagationWarning):
        resolved = resolve_field_classifications(
            graph,
            "gold.agg",
            declared={},
            source_classifications={"email": "pii"},
            mode="warn",
        )

    assert resolved["contacts"] == "pii"


def test_strict_mode_can_now_object_to_an_aggregated_elevation():
    """The fail-open this closes: `strict` rejects undeclared *inferred*
    elevations, so with no edge there was no inference to reject — the strictest
    setting available could not see the problem. A control cannot catch what it
    is never shown."""
    graph = _aggregate_graph("    - [email, contacts, first]\n")

    with pytest.raises(ClassificationViolationError):
        resolve_field_classifications(
            graph,
            "gold.agg",
            declared={},
            source_classifications={"email": "pii"},
            mode="strict",
        )


def test_a_row_count_inherits_nothing_while_staying_visible():
    """`count:*` reads no column, so it inherits nothing — but its output column
    must still exist in the graph, or the dictionary would simply omit it."""
    graph = _aggregate_graph(
        '    - [email, contacts, first]\n    - ["*", nb_rows, count]\n'
    )

    with pytest.warns(ClassificationPropagationWarning):
        resolved = resolve_field_classifications(
            graph,
            "gold.agg",
            declared={},
            source_classifications={"email": "pii"},
            mode="warn",
        )

    assert resolved["contacts"] == "pii"
    assert "nb_rows" not in resolved
    assert any(edge.target_column == "nb_rows" for edge in graph.edges)
