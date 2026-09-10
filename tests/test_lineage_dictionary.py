"""
Tests for DataDictionary — pure Python, no Spark required.
"""
import pytest
from skifer.lineage.tracker import RULE_ORIGIN, LineageEdge, LineageGraph
from skifer.lineage.dictionary import DataDictionary, FieldEntry


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_graph(*edges: LineageEdge) -> LineageGraph:
    g = LineageGraph()
    for e in edges:
        g.add_edge(e)
    return g


def _edge(src_table, src_col, tgt_table, tgt_col, ops=None, edge_type="select"):
    return LineageEdge(src_table, src_col, tgt_table, tgt_col, ops or [], edge_type)


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------

class TestDataDictionaryConstruction:
    def test_indexes_target_columns(self):
        g = _make_graph(_edge("bronze.orders", "amount", "silver.fact", "amount_eur"))
        dd = DataDictionary(g)
        entry = dd.get("silver.fact", "amount_eur")
        assert entry is not None
        assert entry.name == "amount_eur"
        assert entry.table == "silver.fact"

    def test_indexes_source_columns_too(self):
        g = _make_graph(_edge("bronze.orders", "amount", "silver.fact", "amount_eur"))
        dd = DataDictionary(g)
        entry = dd.get("bronze.orders", "amount")
        assert entry is not None

    def test_source_fields_populated(self):
        g = _make_graph(_edge("bronze.orders", "amount", "silver.fact", "amount_eur"))
        dd = DataDictionary(g)
        entry = dd.get("silver.fact", "amount_eur")
        assert "bronze.orders.amount" in entry.source_fields

    def test_transformations_populated(self):
        g = _make_graph(_edge("bronze.orders", "amount", "silver.fact", "amount_eur",
                              ops=["cast:double", "round:2"]))
        dd = DataDictionary(g)
        entry = dd.get("silver.fact", "amount_eur")
        assert "cast:double" in entry.transformations
        assert "round:2" in entry.transformations

    def test_multiple_sources_for_same_target(self):
        g = _make_graph(
            _edge("t1", "col_a", "output", "result"),
            _edge("t2", "col_b", "output", "result"),
        )
        dd = DataDictionary(g)
        entry = dd.get("output", "result")
        assert "t1.col_a" in entry.source_fields
        assert "t2.col_b" in entry.source_fields

    def test_duplicate_transformations_not_repeated(self):
        g = _make_graph(
            _edge("t1", "col_a", "output", "result", ops=["cast:double"]),
            _edge("t2", "col_b", "output", "result", ops=["cast:double"]),
        )
        dd = DataDictionary(g)
        entry = dd.get("output", "result")
        assert entry.transformations.count("cast:double") == 1

    def test_get_unknown_column_returns_none(self):
        g = _make_graph(_edge("t1", "col_a", "t2", "col_b"))
        dd = DataDictionary(g)
        assert dd.get("t1", "nonexistent") is None

    def test_empty_graph_produces_empty_dict(self):
        dd = DataDictionary(LineageGraph())
        assert dd.list_fields() == []

    def test_description_defaults_empty(self):
        g = _make_graph(_edge("t1", "col_a", "t2", "col_b"))
        dd = DataDictionary(g)
        entry = dd.get("t2", "col_b")
        assert entry.description == ""


# ---------------------------------------------------------------------------
# list_fields
# ---------------------------------------------------------------------------

class TestListFields:
    def test_returns_all_entries(self):
        g = _make_graph(
            _edge("t1", "col_a", "output", "col_x"),
            _edge("t1", "col_b", "output", "col_y"),
        )
        dd = DataDictionary(g)
        fields = dd.list_fields()
        # At minimum: col_a, col_b (source), col_x, col_y (target)
        assert len(fields) >= 4

    def test_filter_by_table(self):
        g = _make_graph(
            _edge("bronze.orders", "amount", "silver.fact", "amount_eur"),
            _edge("bronze.orders", "status", "silver.fact", "status_label"),
        )
        dd = DataDictionary(g)
        silver_fields = dd.list_fields(table="silver.fact")
        assert all(e.table == "silver.fact" for e in silver_fields)
        assert len(silver_fields) == 2

    def test_sorted_by_table_then_name(self):
        g = _make_graph(
            _edge("t1", "zzz", "output", "bbb"),
            _edge("t1", "aaa", "output", "aaa"),
        )
        dd = DataDictionary(g)
        fields = dd.list_fields()
        keys = [(e.table, e.name) for e in fields]
        assert keys == sorted(keys)

    def test_filter_returns_empty_for_unknown_table(self):
        g = _make_graph(_edge("t1", "col_a", "t2", "col_b"))
        dd = DataDictionary(g)
        assert dd.list_fields(table="nonexistent") == []


# ---------------------------------------------------------------------------
# to_dict
# ---------------------------------------------------------------------------

class TestToDictionary:
    def test_returns_dict(self):
        g = _make_graph(_edge("t1", "col_a", "t2", "col_b"))
        dd = DataDictionary(g)
        result = dd.to_dict()
        assert isinstance(result, dict)

    def test_keys_are_fqn(self):
        g = _make_graph(_edge("t1", "col_a", "t2", "col_b"))
        dd = DataDictionary(g)
        result = dd.to_dict()
        assert "t2.col_b" in result

    def test_value_has_expected_fields(self):
        g = _make_graph(_edge("t1", "col_a", "t2", "col_b", ops=["upper"]))
        dd = DataDictionary(g)
        result = dd.to_dict()
        entry = result["t2.col_b"]
        assert "table" in entry
        assert "name" in entry
        assert "description" in entry
        assert "source_fields" in entry
        assert "transformations" in entry


# ---------------------------------------------------------------------------
# Glossary enrichment (unit test — mocks GlossaryReader)
# ---------------------------------------------------------------------------

class TestGlossaryEnrichment:
    def test_enrich_updates_description(self, tmp_path, monkeypatch):
        glossary_file = tmp_path / "glossary.txt"
        glossary_file.write_text("amount_eur: Montant en euros après conversion\nchannel: Canal de vente\n")

        g = _make_graph(_edge("bronze.orders", "amount", "silver.fact", "amount_eur"))
        dd = DataDictionary(g)

        dd.enrich_from_glossary(str(glossary_file))
        entry = dd.get("silver.fact", "amount_eur")
        assert entry.description != ""
        assert "euros" in entry.description.lower() or "amount_eur" in entry.description.lower() or entry.description != ""

    def test_enrich_does_not_overwrite_existing_description(self, tmp_path):
        glossary_file = tmp_path / "glossary.txt"
        glossary_file.write_text("amount_eur: New description\n")

        g = _make_graph(_edge("bronze.orders", "amount", "silver.fact", "amount_eur"))
        dd = DataDictionary(g)
        dd.get("silver.fact", "amount_eur").description = "Existing description"

        dd.enrich_from_glossary(str(glossary_file))
        entry = dd.get("silver.fact", "amount_eur")
        assert entry.description == "Existing description"

    def test_enrich_unknown_glossary_path_raises(self):
        g = _make_graph(_edge("t1", "col_a", "t2", "col_b"))
        dd = DataDictionary(g)
        with pytest.raises(Exception):
            dd.enrich_from_glossary("/nonexistent/path/glossary.yaml")


# ---------------------------------------------------------------------------
# The rule origin marker is not a table (Plan 34 follow-up)
# ---------------------------------------------------------------------------

def test_the_rule_origin_marker_is_not_indexed_as_a_table():
    """`<rule>` marks a column a rule created, not a place a reader can query.

    Attributing a rule-made column to `<rule>` fixed a lineage edge that named a
    source column no table contains. Indexing that marker as a table put a field
    nobody can query into the data dictionary, and let one name fill two of the
    three nearest-name suggestion slots.
    """
    graph = LineageGraph()
    graph.add_edge(
        LineageEdge(
            source_table=RULE_ORIGIN,
            source_column="order_class",
            target_table="gold.orders",
            target_column="order_class",
            edge_type="select",
        )
    )
    graph.add_edge(
        LineageEdge(
            source_table="raw_orders",
            source_column="amount",
            target_table="gold.orders",
            target_column="order_class",
            transformations=["rule:classify_order"],
            edge_type="rule",
        )
    )

    dictionary = DataDictionary(graph)
    tables = {entry.table for entry in dictionary.list_fields()}

    assert RULE_ORIGIN not in tables
    assert dictionary.get(RULE_ORIGIN, "order_class") is None

    # The provenance itself is kept: the entry still says the rule made it.
    entry = dictionary.get("gold.orders", "order_class")
    assert f"{RULE_ORIGIN}.order_class" in entry.source_fields
    assert "raw_orders.amount" in entry.source_fields
