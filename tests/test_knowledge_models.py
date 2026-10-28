"""Plan 44 S02: knowledge graph nodes, edges and fingerprints."""

from __future__ import annotations

import subprocess
import sys
from dataclasses import replace

import pytest

from skifer.knowledge.models import Edge, Node, edge_from_row, node_from_row


def _node(**overrides) -> Node:
    values = dict(kind="column", fqn="main.sales.orders.amount", label="amount",
                  text="Order amount in EUR", synonyms=("revenue",), status="reference",
                  domain="main.sales", source="pipeline")
    values.update(overrides)
    return Node.build(**values)


def test_node_id_and_fingerprint_are_derived_from_content():
    node = _node()
    assert node.node_id == "column:main.sales.orders.amount"
    assert node.fingerprint.startswith("sha256:v1:")
    assert _node().fingerprint == node.fingerprint
    for change in ({"text": "other"}, {"synonyms": ("ca",)}, {"label": "amt"},
                   {"status": "certified"}, {"domain": "main.other"}):
        assert _node(**change).fingerprint != node.fingerprint


@pytest.mark.parametrize(
    ("overrides", "field_name"),
    [
        ({"kind": "view"}, "kind"),
        ({"status": "gold"}, "status"),
        ({"source": "llm"}, "source"),
        ({"label": ""}, "label"),
    ],
)
def test_node_rejects_invalid_fields(overrides, field_name):
    with pytest.raises(ValueError, match=field_name):
        _node(**overrides)


def test_node_rejects_forged_id_or_fingerprint():
    node = _node()
    with pytest.raises(ValueError, match="node_id"):
        replace(node, node_id="table:x")
    with pytest.raises(ValueError, match="fingerprint"):
        replace(node, fingerprint="sha256:v1:" + "0" * 64)


def test_edge_identity_and_weight_bounds():
    edge = Edge.build(kind="shortcut", from_id="table:a", to_id="table:b", keys=("id", "a_id"),
                      source="semantic")
    assert edge.edge_id == Edge.build(kind="shortcut", from_id="table:a", to_id="table:b",
                                      keys=("id", "a_id"), source="pipeline").edge_id
    assert edge.fingerprint != replace(edge, source="pipeline").fingerprint
    for weight in (0, 1.5, float("nan")):
        with pytest.raises(ValueError, match="weight"):
            Edge.build(kind="joins", from_id="a", to_id="b", weight=weight, source="pipeline")
    with pytest.raises(ValueError, match="edge_id"):
        replace(edge, to_id="table:c")


def test_rows_round_trip():
    node = _node()
    assert node_from_row(node.to_dict()) == node
    edge = Edge.build(kind="joins", from_id="column:a.x", to_id="column:b.y", keys=("x", "y"),
                      weight=0.5, source="pipeline")
    assert edge_from_row(edge.to_dict()) == edge


def test_package_imports_without_spark_or_embeddings():
    code = (
        "import sys\n"
        "import skifer.knowledge, skifer.knowledge.models\n"
        "for name in ('skifer.knowledge.store', 'skifer.knowledge.sync', 'skifer.knowledge.index'):\n"
        "    try:\n"
        "        __import__(name)\n"
        "    except ModuleNotFoundError as exc:\n"
        "        if exc.name != name:\n"
        "            raise\n"
        "print(sorted(m for m in ('pyspark', 'sentence_transformers', 'delta', 'torch') if m in sys.modules))\n"
    )
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    assert result.stdout.strip() == "[]"
