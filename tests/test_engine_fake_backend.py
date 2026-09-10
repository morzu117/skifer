"""
Tests proving that SkiferEngine works with FakeBackend (no PySpark session
required). FakeBackend is a duck-typed double of the SparkBackend surface.
"""
import inspect

import pytest
from tests.fakes.fake_backend import FakeBackend, FakeDataFrame


# ---------------------------------------------------------------------------
# FakeBackend unit tests
# ---------------------------------------------------------------------------

def test_fake_backend_matches_spark_backend_surface():
    """Drift guard: every public attribute of FakeBackend must exist on
    SparkBackend, so the double cannot silently diverge from the real backend."""
    from skifer.core.spark_backend import SparkBackend
    fake_public = {
        name for name, _ in inspect.getmembers(FakeBackend)
        if not name.startswith("_")
    }
    spark_public = {
        name for name, _ in inspect.getmembers(SparkBackend)
        if not name.startswith("_")
    }
    extra = fake_public - spark_public
    assert not extra, (
        f"FakeBackend exposes members missing on SparkBackend: {sorted(extra)}"
    )


def test_fake_backend_read_write_table():
    b = FakeBackend(tables={"silver.orders": [{"id": 1, "amount": 100}]})
    df = b.read_table("silver.orders")
    assert isinstance(df, FakeDataFrame)
    assert len(df) == 1
    b.write_table(df, "gold.result")
    assert "gold.result" in b._written
    assert b._written["gold.result"] == [{"id": 1, "amount": 100}]


def test_fake_backend_filter():
    from skifer.core.ir import ParsedFilter
    b = FakeBackend()
    df = FakeDataFrame([{"id": 1, "status": "ACTIVE"}, {"id": 2, "status": "CLOSED"}])
    cond = b.build_filter(ParsedFilter("status", "equals", "ACTIVE"))
    result = b.filter(df, cond)
    assert len(result) == 1
    assert result._rows[0]["id"] == 1


def test_fake_backend_drop_nulls():
    b = FakeBackend()
    df = FakeDataFrame([{"id": 1, "amount": 100}, {"id": 2, "amount": None}])
    result = b.drop_nulls(df, ["amount"])
    assert len(result) == 1
    assert result._rows[0]["id"] == 1


def test_fake_backend_drop_duplicates():
    b = FakeBackend()
    df = FakeDataFrame([{"id": 1, "region": "FR"}, {"id": 1, "region": "FR"}, {"id": 2, "region": "DE"}])
    result = b.drop_duplicates(df, ["id"])
    assert len(result) == 2


def test_fake_backend_limit():
    b = FakeBackend()
    df = FakeDataFrame([{"id": i} for i in range(10)])
    result = b.limit(df, 3)
    assert len(result) == 3


def test_fake_backend_union_by_name():
    b = FakeBackend()
    df1 = FakeDataFrame([{"id": 1}])
    df2 = FakeDataFrame([{"id": 2}])
    result = b.union_by_name([df1, df2])
    assert len(result) == 2


def test_fake_backend_join_same_keys():
    b = FakeBackend()
    left = FakeDataFrame([{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}])
    right = FakeDataFrame([{"id": 1, "score": 90}])
    result = b.join(left, right, on=["id"], how="left")
    # Alice matched with score 90; Bob has no match (left join keeps it)
    assert len(result) == 2
    alice = next(r for r in result._rows if r["name"] == "Alice")
    assert alice["score"] == 90


def test_fake_backend_with_column():
    b = FakeBackend()
    df = FakeDataFrame([{"id": 1}])
    col = b.lit("constant_value")
    result = b.with_column(df, "new_col", col)
    assert result._rows[0]["new_col"] == "constant_value"


def test_fake_backend_count_and_is_empty():
    b = FakeBackend()
    df_empty = FakeDataFrame([])
    df_full = FakeDataFrame([{"id": 1}])
    assert b.is_empty(df_empty) is True
    assert b.is_empty(df_full) is False
    assert b.count(df_full) == 1


def test_fake_backend_row_number_over():
    b = FakeBackend()
    df = FakeDataFrame([
        {"region": "FR", "date": "2024-01", "amount": 100},
        {"region": "FR", "date": "2024-02", "amount": 200},
        {"region": "DE", "date": "2024-01", "amount": 50},
    ])
    result = b.row_number_over(df, partition_by=["region"], order_by=[{"field": "date", "order": "asc"}])
    fr_rows = [r for r in result._rows if r["region"] == "FR"]
    assert sorted(r["_rn"] for r in fr_rows) == [1, 2]


def test_fake_backend_build_fqn():
    b = FakeBackend()
    assert b.build_fqn("cat", "schema", "table") == "`cat`.`schema`.`table`"
    assert b.build_fqn(None, "schema", "table") == "`schema`.`table`"


# ---------------------------------------------------------------------------
# SkiferEngine + FakeBackend integration test
# ---------------------------------------------------------------------------

def _make_engine_with_fake(tables=None):
    """Create a SkiferEngine bypassing __init__, injecting FakeBackend."""
    from skifer.core.context import ExecutionContext
    from skifer.core.interpreter import SchemaInterpreter
    from skifer.core.core import SkiferEngine
    engine = object.__new__(SkiferEngine)
    ctx = ExecutionContext()
    object.__setattr__(engine, "_context", ctx)
    engine.spark = None
    engine.is_local = True
    engine.db = None
    engine.env = "local"
    engine.config = {"environments": {"local": {}}}
    engine.schema_suffix = ""
    engine.is_job_execution = False
    from skifer.core.patterns import PipelinePatterns
    b = FakeBackend(tables=tables or {})
    engine._backend = b
    engine._interpreter = SchemaInterpreter(backend=b, context=ctx)
    engine._patterns = PipelinePatterns(engine=engine)
    return engine


def test_engine_process_schema_no_spark():
    """Engine.process_schema works with FakeBackend — no PySpark needed."""
    tables = {
        "silver.orders": [
            {"order_id": "O1", "customer_id": "C1", "amount": 100.0, "status": "ACTIVE"},
            {"order_id": "O2", "customer_id": "C2", "amount": None, "status": "ACTIVE"},
            {"order_id": "O3", "customer_id": "C3", "amount": 50.0, "status": "CLOSED"},
        ]
    }
    engine = _make_engine_with_fake(tables)

    schema = {
        "tables": [
            {
                "name": "silver.orders",
                "alias": "ord",
                "filter": [{"column": "status", "operator": "equals", "value": "ACTIVE"}],
                "quality_checks": {"drop_nulls_in": ["amount"]},
            }
        ]
    }

    result = engine.process_schema(schema)
    assert isinstance(result, FakeDataFrame)
    assert len(result) == 1  # O1 only: O2 dropped (null amount), O3 filtered (CLOSED)
    assert result._rows[0]["order_id"] == "O1"


def test_engine_process_schema_with_dev_limit_no_spark():
    """dev_limit is applied via FakeBackend.limit — no PySpark."""
    tables = {"silver.orders": [{"id": i} for i in range(100)]}
    engine = _make_engine_with_fake(tables)
    schema = {
        "tables": [{"name": "silver.orders", "alias": "ord", "dev_limit": 5}]
    }
    result = engine.process_schema(schema)
    assert len(result) == 5


def test_engine_drop_duplicates_quality_check_no_spark():
    """drop_duplicates_on quality check works via FakeBackend."""
    tables = {
        "silver.orders": [
            {"order_id": "O1", "amount": 100},
            {"order_id": "O1", "amount": 100},  # duplicate
        ]
    }
    engine = _make_engine_with_fake(tables)
    schema = {
        "tables": [
            {
                "name": "silver.orders",
                "alias": "ord",
                "quality_checks": {"drop_duplicates_on": ["order_id"]},
            }
        ]
    }
    result = engine.process_schema(schema)
    assert len(result) == 1


# ---------------------------------------------------------------------------
# explain_rules integration
# ---------------------------------------------------------------------------

def _register_temp_rules():
    from skifer.core.registry import RuleRegistry

    def _rule_amount_flag(df):
        import pyspark.sql.functions as F
        return df.withColumn("amount_flag", F.when(F.col("amount") >= 1000, 1).otherwise(0))

    def _rule_discount(df):
        import pyspark.sql.functions as F
        return df.withColumn("discount", F.when(F.col("amount") >= 1000, 0.05).otherwise(0))

    def _rule_overwrite(df):
        import pyspark.sql.functions as F
        return df.withColumn("amount_flag", F.col("amount") * 2)

    RuleRegistry._rules["_tmp_amount_flag"] = _rule_amount_flag
    RuleRegistry._rules["_tmp_discount"] = _rule_discount
    RuleRegistry._rules["_tmp_overwrite"] = _rule_overwrite
    return ["_tmp_amount_flag", "_tmp_discount", "_tmp_overwrite"]


def _cleanup_temp_rules():
    from skifer.core.registry import RuleRegistry
    for k in ["_tmp_amount_flag", "_tmp_discount", "_tmp_overwrite"]:
        RuleRegistry._rules.pop(k, None)


def test_explain_rules_returns_profiles_and_warnings():
    engine = _make_engine_with_fake()
    rule_names = _register_temp_rules()
    schema = {"business_rules": rule_names}
    try:
        profiles, warnings = engine.explain_rules(schema)
        assert len(profiles) == 3
        assert any(w.code == "OVERWRITE" for w in warnings)
        assert any(w.code == "SHARED_READ" for w in warnings)
    finally:
        _cleanup_temp_rules()


def test_explain_rules_empty_schema_no_crash():
    engine = _make_engine_with_fake()
    profiles, warnings = engine.explain_rules({})
    assert profiles == []
    assert warnings == []


def test_explain_rules_unknown_rule_produces_unavailable_profile():
    engine = _make_engine_with_fake()
    schema = {"business_rules": ["_does_not_exist_at_all"]}
    profiles, warnings = engine.explain_rules(schema)
    assert len(profiles) == 1
    assert profiles[0].source_available is False


def test_explain_rules_top_level_import():
    from skifer import RuleAnalyzer
    assert RuleAnalyzer is not None


# ---------------------------------------------------------------------------
# build_lineage integration tests
# ---------------------------------------------------------------------------

def test_build_lineage_returns_graph():
    from skifer.lineage.tracker import LineageGraph
    engine = _make_engine_with_fake()
    schema = {
        "tables": [{"name": "bronze.raw_orders", "alias": "ord"}],
        "select_final": [
            ["amount", "amount_eur", ["cast:double", "round:2"]],
            ["customer_id", "customer_id", []],
        ],
    }
    graph = engine.build_lineage(schema, target_name="silver.fact_orders")
    assert isinstance(graph, LineageGraph)
    assert len(graph) == 2


def test_build_lineage_edges_have_correct_source():
    engine = _make_engine_with_fake()
    schema = {
        "tables": [{"name": "bronze.raw_orders"}],
        "select_final": [["amount", "amount_eur", ["cast:double"]]],
    }
    graph = engine.build_lineage(schema, target_name="output")
    edges = graph.upstream("output", "amount_eur")
    assert len(edges) == 1
    assert edges[0].source_table == "bronze.raw_orders"
    assert edges[0].source_column == "amount"


def test_build_lineage_with_semantic_model():
    engine = _make_engine_with_fake()
    schema = {
        "tables": [{"name": "silver.fact_orders"}],
        "select_final": [["amount", "amount_eur", []]],
    }
    semantic_model = {
        "key": "kpi_orders",
        "table": "gold.fact_orders",
        "dimensions": [{"name": "channel", "sql": "channel"}],
        "metrics": [],
    }
    graph = engine.build_lineage(schema, semantic_models=[semantic_model], target_name="output")
    # Core edges present
    assert any(e.edge_type == "select" for e in graph.edges)
    # Semantic edges present
    assert any(e.edge_type == "metric" for e in graph.edges)
    assert "kpi_orders" in graph.tables()


def test_build_lineage_empty_schema_returns_empty_graph():
    engine = _make_engine_with_fake()
    graph = engine.build_lineage({})
    assert not graph


def test_lineage_top_level_imports():
    from skifer import LineageTracker, LineageGraph, LineageEdge, DataDictionary, LineageRenderer
    assert LineageTracker is not None
    assert LineageGraph is not None
    assert LineageEdge is not None
    assert DataDictionary is not None
    assert LineageRenderer is not None

