"""
Plan 28.1 — declarative ``aggregate:`` block.

Part 1: load-time normalization and validation (no Spark).
Part 2: execution through the interpreter with a FakeBackend.
Part 3: one end-to-end case on the real local Spark session.
"""
import pytest

from skifer.core.context import ExecutionContext
from skifer.core.core import SkiferEngine
from skifer.core.interpreter import SchemaInterpreter
from skifer.core.ir import parse_to_ir
from skifer.core.op_catalog import AGGREGATE_FUNCTIONS, resolve_aggregate_function
from skifer.core.patterns import PipelinePatterns
from skifer.core.schema_loader import parse_schema
from tests.fakes.fake_backend import FakeBackend


def _make_engine(backend):
    """Minimal engine with an injected backend (model: tests/test_streaming.py)."""
    engine = object.__new__(SkiferEngine)
    ctx = ExecutionContext()
    object.__setattr__(engine, "_context", ctx)
    engine.spark = getattr(backend, "spark", None)
    engine.is_local = backend.is_local
    engine.db = None
    engine.env = "local"
    engine.config = {"environments": {"local": {}}}
    engine.schema_suffix = ""
    engine.is_job_execution = False
    engine._backend = backend
    engine._interpreter = SchemaInterpreter(backend=backend, context=ctx)
    engine._patterns = PipelinePatterns(engine=engine)
    engine.monitor = None
    return engine


ORDERS = [
    {"order_id": 1, "country": "FR", "amount": 100, "status": "DONE"},
    {"order_id": 2, "country": "FR", "amount": 300, "status": "DONE"},
    {"order_id": 3, "country": "DE", "amount": 50, "status": "DONE"},
    {"order_id": 4, "country": "DE", "amount": 50, "status": "PENDING"},
]


# ==============================================================================
# Part 1 — load-time normalization and validation
# ==============================================================================

class TestAggregateNormalization:
    """Normalization of the aggregate: block at load time."""

    def test_nominal_list_form(self):
        schema = parse_schema("""
tables:
  - name: silver.orders
    alias: ord
aggregate:
  group_by: [country]
  measures:
    - [amount, total_amount, sum]
    - [order_id, nb_orders, count_distinct]
""")
        assert schema["aggregate"] == {
            "group_by": ["country"],
            "measures": [
                {"source": "amount", "target": "total_amount", "func": "sum"},
                {"source": "order_id", "target": "nb_orders", "func": "count_distinct"},
            ],
        }

    def test_nominal_mapping_form_and_alias_resolution(self):
        schema = parse_schema("""
tables:
  - name: silver.orders
    alias: ord
aggregate:
  group_by: [country]
  measures:
    - source: amount
      target: panier_moyen
      func: mean
""")
        assert schema["aggregate"]["measures"] == [
            {"source": "amount", "target": "panier_moyen", "func": "avg"}
        ]

    def test_having_is_normalized_to_filter_dicts(self):
        schema = parse_schema("""
tables:
  - name: silver.orders
    alias: ord
aggregate:
  group_by: [country]
  measures:
    - [amount, total_amount, sum]
  having:
    - "total_amount:greater_than:1000"
""")
        assert schema["aggregate"]["having"] == [
            {"column": "total_amount", "operator": "greater_than", "value": "1000"}
        ]

    def test_count_star_allowed(self):
        schema = parse_schema("""
tables:
  - name: silver.orders
    alias: ord
aggregate:
  group_by: [country]
  measures:
    - ["*", nb_rows, count]
""")
        assert schema["aggregate"]["measures"][0]["source"] == "*"

    def test_schema_without_aggregate_untouched(self):
        """Zero churn: schemas that do not aggregate keep their exact shape."""
        schema = parse_schema("""
tables:
  - name: silver.orders
    alias: ord
keep_all_columns: true
""")
        assert "aggregate" not in schema


class TestAggregateRejections:
    """Every aggregate: blocker surfaces at load time with an actionable message."""

    def test_not_a_mapping_rejected(self):
        with pytest.raises(ValueError, match=r"\[aggregate\] must be a mapping"):
            parse_schema("tables:\n  - name: t\naggregate: [country]\n")

    def test_unknown_key_rejected(self):
        with pytest.raises(ValueError, match=r"Unknown keys: \['order_by'\]"):
            parse_schema("""
tables:
  - name: t
aggregate:
  group_by: [country]
  measures:
    - [amount, total, sum]
  order_by: [country]
""")

    def test_missing_group_by_rejected(self):
        with pytest.raises(ValueError, match="'group_by' is required"):
            parse_schema("""
tables:
  - name: t
aggregate:
  measures:
    - [amount, total, sum]
""")

    def test_empty_group_by_rejected(self):
        with pytest.raises(ValueError, match="'group_by' is required"):
            parse_schema("""
tables:
  - name: t
aggregate:
  group_by: []
  measures:
    - [amount, total, sum]
""")

    def test_missing_measures_rejected(self):
        with pytest.raises(ValueError, match="'measures' is required"):
            parse_schema("""
tables:
  - name: t
aggregate:
  group_by: [country]
""")

    def test_bad_measure_arity_rejected(self):
        with pytest.raises(ValueError, match=r"must be \[source, target, func\]"):
            parse_schema("""
tables:
  - name: t
aggregate:
  group_by: [country]
  measures:
    - [amount, total]
""")

    def test_unknown_function_rejected_with_suggestion(self):
        with pytest.raises(ValueError, match="unknown function 'summ'"):
            parse_schema("""
tables:
  - name: t
aggregate:
  group_by: [country]
  measures:
    - [amount, total, summ]
""")

    def test_star_source_rejected_for_non_count(self):
        with pytest.raises(ValueError, match=r"source '\*' is only supported by"):
            parse_schema("""
tables:
  - name: t
aggregate:
  group_by: [country]
  measures:
    - ["*", total, sum]
""")

    def test_duplicate_target_rejected(self):
        with pytest.raises(ValueError, match="duplicate measure target 'total'"):
            parse_schema("""
tables:
  - name: t
aggregate:
  group_by: [country]
  measures:
    - [amount, total, sum]
    - [amount, total, avg]
""")

    def test_target_colliding_with_group_by_rejected(self):
        with pytest.raises(ValueError, match="collides with a 'group_by' column"):
            parse_schema("""
tables:
  - name: t
aggregate:
  group_by: [country]
  measures:
    - [amount, country, sum]
""")

    def test_having_on_unknown_column_rejected(self):
        with pytest.raises(ValueError, match="'having' references unknown column 'revenue'"):
            parse_schema("""
tables:
  - name: t
aggregate:
  group_by: [country]
  measures:
    - [amount, total, sum]
  having:
    - "revenue:greater_than:10"
""")

    def test_having_on_group_key_allowed(self):
        schema = parse_schema("""
tables:
  - name: t
aggregate:
  group_by: [country]
  measures:
    - [amount, total, sum]
  having:
    - "country:not_equals:XX"
""")
        assert schema["aggregate"]["having"][0]["column"] == "country"

    def test_aggregate_with_select_final_rejected(self):
        with pytest.raises(ValueError, match="mutually exclusive"):
            parse_schema("""
tables:
  - name: t
select_final:
  - [country, country]
aggregate:
  group_by: [country]
  measures:
    - [amount, total, sum]
""")

    def test_aggregate_with_keep_all_columns_rejected(self):
        with pytest.raises(ValueError, match="mutually exclusive"):
            parse_schema("""
tables:
  - name: t
keep_all_columns: true
aggregate:
  group_by: [country]
  measures:
    - [amount, total, sum]
""")

    def test_aggregate_with_streaming_rejected(self):
        with pytest.raises(ValueError, match="'aggregate' block is incompatible"):
            parse_schema("""
materialization: streaming_table
tables:
  - name: bronze.events
    alias: ev
    streaming: true
aggregate:
  group_by: [country]
  measures:
    - [amount, total, sum]
""")


class TestAggregateCatalog:
    """The aggregate catalog is the single source of truth."""

    def test_aliases_resolve_to_canonical(self):
        assert resolve_aggregate_function("mean") == "avg"
        assert resolve_aggregate_function("MEAN") == "avg"
        assert resolve_aggregate_function("countdistinct") == "count_distinct"
        assert resolve_aggregate_function("nope") is None

    def test_spark_dispatch_covers_the_whole_catalog(self):
        """Drift guard: catalog and Spark dispatch table must stay in sync."""
        from skifer.core.spark_backend import _SPARK_AGG_DISPATCH
        assert set(_SPARK_AGG_DISPATCH) == set(AGGREGATE_FUNCTIONS)


class TestAggregateIR:
    """The IR carries the aggregate block."""

    def test_parse_to_ir_builds_parsed_aggregate(self):
        schema = parse_schema("""
tables:
  - name: silver.orders
    alias: ord
aggregate:
  group_by: [country]
  measures:
    - [amount, total_amount, sum]
  having:
    - "total_amount:greater_than:100"
""")
        parsed = parse_to_ir(schema)
        assert parsed.aggregate.group_by == ["country"]
        assert parsed.aggregate.measures[0].func == "sum"
        assert parsed.aggregate.measures[0].target == "total_amount"
        assert parsed.aggregate.having[0].operator == "greater_than"

    def test_parse_to_ir_without_aggregate(self):
        parsed = parse_to_ir(parse_schema("tables:\n  - name: t\nkeep_all_columns: true\n"))
        assert parsed.aggregate is None


# ==============================================================================
# Part 2 — execution through the interpreter (FakeBackend)
# ==============================================================================

class TestAggregateExecution:
    """The interpreter turns the aggregate block into groupBy/agg (+ HAVING)."""

    def _run(self, yaml_schema):
        backend = FakeBackend(tables={"silver.orders": ORDERS})
        engine = _make_engine(backend)
        df = engine.process_schema(parse_schema(yaml_schema))
        return sorted(df._rows, key=lambda r: r["country"])

    def test_group_by_with_two_measures(self):
        rows = self._run("""
tables:
  - name: silver.orders
    alias: ord
aggregate:
  group_by: [country]
  measures:
    - [amount, total_amount, sum]
    - [order_id, nb_orders, count]
""")
        assert rows == [
            {"country": "DE", "total_amount": 100, "nb_orders": 2},
            {"country": "FR", "total_amount": 400, "nb_orders": 2},
        ]

    def test_count_star(self):
        rows = self._run("""
tables:
  - name: silver.orders
    alias: ord
aggregate:
  group_by: [country]
  measures:
    - ["*", nb_rows, count]
""")
        assert rows == [{"country": "DE", "nb_rows": 2}, {"country": "FR", "nb_rows": 2}]

    def test_count_distinct(self):
        rows = self._run("""
tables:
  - name: silver.orders
    alias: ord
aggregate:
  group_by: [country]
  measures:
    - [amount, nb_amounts, count_distinct]
""")
        assert rows == [{"country": "DE", "nb_amounts": 1}, {"country": "FR", "nb_amounts": 2}]

    def test_filter_applies_before_aggregation(self):
        rows = self._run("""
tables:
  - name: silver.orders
    alias: ord
    filter:
      - "status:equals:DONE"
aggregate:
  group_by: [country]
  measures:
    - [amount, total_amount, sum]
""")
        assert rows == [
            {"country": "DE", "total_amount": 50},
            {"country": "FR", "total_amount": 400},
        ]

    def test_having_filters_groups(self):
        """HAVING runs after the aggregation (numeric predicates: see the Spark test)."""
        rows = self._run("""
tables:
  - name: silver.orders
    alias: ord
aggregate:
  group_by: [country]
  measures:
    - [amount, total_amount, sum]
  having:
    - "country:equals:FR"
""")
        assert rows == [{"country": "FR", "total_amount": 400}]

    def test_add_columns_run_before_aggregation(self):
        """add_columns feed group_by keys — they must not be dropped."""
        backend = FakeBackend(tables={"silver.orders": ORDERS})
        engine = _make_engine(backend)
        schema = parse_schema("""
tables:
  - name: silver.orders
    alias: ord
add_columns:
  - [country, zone, []]
aggregate:
  group_by: [zone]
  measures:
    - [amount, total_amount, sum]
""")
        rows = sorted(engine.process_schema(schema)._rows, key=lambda r: r["zone"])
        assert rows == [
            {"zone": "DE", "total_amount": 100},
            {"zone": "FR", "total_amount": 400},
        ]


# ==============================================================================
# Part 3 — real local Spark session
# ==============================================================================

class TestAggregateOnSpark:
    """One end-to-end case proving the Spark aggregate primitives work."""

    def test_group_by_agg_on_real_spark(self, spark):
        backend_rows = [
            (1, "FR", 100.0), (2, "FR", 300.0), (3, "DE", 50.0),
        ]
        df = spark.createDataFrame(backend_rows, ["order_id", "country", "amount"])

        from skifer.core.spark_backend import SparkBackend
        backend = SparkBackend(spark=spark, is_local=True)

        exprs = {
            "total_amount": backend.agg_expr("sum", "amount"),
            "nb_orders": backend.agg_expr("count", "*"),
        }
        result = backend.group_by_agg(df, ["country"], exprs)
        rows = {r["country"]: (r["total_amount"], r["nb_orders"]) for r in result.collect()}

        assert rows == {"FR": (400.0, 2), "DE": (50.0, 1)}

    def test_having_on_measure_alias(self, spark):
        """A numeric HAVING threshold filters aggregated groups on real Spark."""
        df = spark.createDataFrame(
            [(1, "FR", 100.0), (2, "FR", 300.0), (3, "DE", 50.0)],
            ["order_id", "country", "amount"],
        )
        from skifer.core.spark_backend import SparkBackend
        backend = SparkBackend(spark=spark, is_local=True)
        interpreter = SchemaInterpreter(backend=backend, context=ExecutionContext())

        aggregate = {
            "group_by": ["country"],
            "measures": [{"source": "amount", "target": "total_amount", "func": "sum"}],
            "having": [{"column": "total_amount", "operator": "greater_than", "value": "100"}],
        }
        result = interpreter._apply_aggregate(df, aggregate, allow_raw_sql=True)

        assert [(r["country"], r["total_amount"]) for r in result.collect()] == [("FR", 400.0)]

    def test_unknown_function_raises(self, spark):
        from skifer.core.spark_backend import SparkBackend
        backend = SparkBackend(spark=spark, is_local=True)
        with pytest.raises(ValueError, match="Unknown aggregate function"):
            backend.agg_expr("median", "amount")
