"""
Plan 28.2 — SQL compiler.

Part 1: text-level assertions per operator/op + drift guards (no Spark).
Part 2: rejections — every construct with no faithful SQL form.
Part 3: PARITY — the compiled SQL and the DataFrame path must return the same
        rows for the same schema, on a real local Spark session. This is the
        primary mitigation for semantic drift between the two execution paths.
"""
import pytest

from skifer.core.context import ExecutionContext
from skifer.core.core import SkiferEngine
from skifer.core.interpreter import SchemaInterpreter
from skifer.core.ir import parse_to_ir
from skifer.core.patterns import PipelinePatterns
from skifer.core.schema_loader import parse_schema
from skifer.core.sql_compiler import (
    SqlCompilationError,
    _SQL_FILTER_DISPATCH,
    _SQL_OP_DISPATCH,
    compile_select,
    quote_ident,
    sql_literal,
)


def _compile(yaml_schema: str, **kwargs) -> str:
    parsed = parse_to_ir(parse_schema(yaml_schema))
    result = compile_select(parsed, **kwargs)
    assert result == compile_select(parsed, dialect="databricks", **kwargs)
    return result


def _single_table(body: str) -> str:
    return f"""
tables:
  - name: silver.orders
    alias: ord
{body}
"""


# ==============================================================================
# Part 1 — compiled text
# ==============================================================================

class TestQuoting:
    def test_identifiers_are_backticked(self):
        assert quote_ident("country") == "`country`"

    def test_embedded_backtick_is_escaped(self):
        assert quote_ident("we`ird") == "`we``ird`"

    def test_string_literals_escape_single_quotes(self):
        assert sql_literal("O'Brien") == "'O''Brien'"

    def test_string_literals_escape_backslashes_before_single_quotes(self):
        assert sql_literal("x\\' OR 1=1 --") == "'x\\\\'' OR 1=1 --'"

    def test_sql_injection_attempt_stays_a_literal(self):
        schema = _single_table("""    filter:
      - "status:equals:x'; DROP TABLE users; --"
keep_all_columns: true
""")
        sql = _compile(schema)
        assert "'x''; DROP TABLE users; --'" in sql
        assert "DROP TABLE users; --'" in sql  # inside the quoted literal, not as a statement


class TestTableCompilation:
    def test_minimal_schema(self):
        sql = _compile(_single_table("keep_all_columns: true\n"))
        assert "WITH `ord` AS (\n  SELECT * FROM `silver`.`orders`\n)" in sql
        assert sql.endswith("SELECT *\nFROM `ord`")

    def test_resolve_table_is_applied(self):
        sql = _compile(
            _single_table("keep_all_columns: true\n"),
            resolve_table=lambda name: f"cat.{name}_1234",
        )
        assert "FROM `cat`.`silver`.`orders_1234`" in sql

    def test_filters_are_anded(self):
        sql = _compile(_single_table("""    filter:
      - "status:equals:DONE"
      - "amount:greater_than:100"
keep_all_columns: true
"""))
        assert "WHERE `status` = 'DONE' AND `amount` > '100'" in sql

    def test_drop_nulls_in_becomes_is_not_null(self):
        sql = _compile(_single_table("""    quality_checks:
      drop_nulls_in: [amount, customer_id]
keep_all_columns: true
"""))
        assert "WHERE `amount` IS NOT NULL AND `customer_id` IS NOT NULL" in sql

    def test_filter_groups_are_or_of_ands(self):
        sql = _compile(_single_table("""    filter_groups:
      - ["region:equals:EMEA", "status:is_not_null"]
      - ["region:equals:APAC"]
keep_all_columns: true
"""))
        assert (
            "WHERE ((`region` = 'EMEA' AND `status` IS NOT NULL) OR (`region` = 'APAC'))" in sql
        )

    def test_fields_become_the_cte_projection(self):
        sql = _compile(_single_table("""    fields:
      - [order_id, order_id]
      - [amount, amount_eur, [round:2]]
"""))
        assert "SELECT `order_id` AS `order_id`, ROUND(`amount`, 2) AS `amount_eur`" in sql


class TestFilterOperators:
    """One assertion per filter operator — the SQL must mirror the Spark dispatch."""

    @pytest.mark.parametrize("expr,expected", [
        ("status:equals:DONE", "`status` = 'DONE'"),
        ("status:not_equals:DONE", "`status` != 'DONE'"),
        ("amount:greater_than:10", "`amount` > '10'"),
        ("amount:less_than:10", "`amount` < '10'"),
        ("amount:greater_than_equal:10", "`amount` >= '10'"),
        ("amount:less_than_equal:10", "`amount` <= '10'"),
        ("amount:is_null", "`amount` IS NULL"),
        ("amount:is_not_null", "`amount` IS NOT NULL"),
        ("status:in:A,B", "`status` IN ('A', 'B')"),
        ("status:not_in:A,B", "`status` NOT IN ('A', 'B')"),
        ("name:contains:foo", "`name` LIKE '%foo%'"),
        ("name:not_contains:foo", "NOT (`name` LIKE '%foo%')"),
        ("name:starts_with:foo", "`name` LIKE 'foo%'"),
        ("name:ends_with:foo", "`name` LIKE '%foo'"),
        ("name:like:%foo%", "`name` LIKE '%foo%'"),
        ("name:not_like:%foo%", "NOT (`name` LIKE '%foo%')"),
        ("amount:between:1,10", "(`amount` >= '1' AND `amount` <= '10')"),
        ("amount:not_between:1,10", "(`amount` < '1' OR `amount` > '10')"),
    ])
    def test_operator(self, expr, expected):
        sql = _compile(_single_table(f'    filter:\n      - "{expr}"\nkeep_all_columns: true\n'))
        assert expected in sql

    def test_raw_sql_operator_governed(self):
        schema = _single_table('    filter:\n      - "x:sql:amount > 0"\nkeep_all_columns: true\n')
        assert "(amount > 0)" in _compile(schema)
        with pytest.raises(SqlCompilationError, match="Governance"):
            _compile(schema, allow_raw_sql=False)


class TestColumnOps:
    @pytest.mark.parametrize("ops,expected", [
        ("[cast:double]", "CAST(`amount` AS double)"),
        ("[cast:datetime]", "CAST(`amount` AS timestamp)"),
        ("[upper]", "UPPER(`amount`)"),
        ("[lower]", "LOWER(`amount`)"),
        ("[trim]", "TRIM(`amount`)"),
        ("[round:2]", "ROUND(`amount`, 2)"),
        ("[abs]", "ABS(`amount`)"),
        ("[ceil]", "CEIL(`amount`)"),
        ("[length]", "LENGTH(`amount`)"),
        ("[to_date:yyyy-MM-dd]", "TO_DATE(`amount`, 'yyyy-MM-dd')"),
        ("[nvl:0]", "COALESCE(`amount`, 0)"),
        ("[coalesce:none]", "COALESCE(`amount`, 'none')"),
        ("[lit:ERP]", "'ERP'"),
        ('["substring:1,3"]', "SUBSTRING(`amount`, 1, 3)"),
        ('["split:-,0"]', "SPLIT(`amount`, '-')[0]"),
    ])
    def test_op(self, ops, expected):
        sql = _compile(_single_table(f"select_final:\n  - [amount, out, {ops}]\n"))
        assert expected in sql

    def test_ops_chain_nests_left_to_right(self):
        sql = _compile(_single_table("select_final:\n  - [amount, out, [cast:double, round:2]]\n"))
        assert "ROUND(CAST(`amount` AS double), 2) AS `out`" in sql

    def test_literal_shorthand(self):
        sql = _compile(_single_table("select_final:\n  - [literal:ERP, source_system]\n"))
        assert "'ERP' AS `source_system`" in sql

    def test_expr_is_governed(self):
        schema = _single_table("select_final:\n  - [amount, out, [\"expr:amount * 2\"]]\n")
        assert "(amount * 2) AS `out`" in _compile(schema)
        with pytest.raises(SqlCompilationError, match="Governance"):
            _compile(schema, allow_raw_sql=False)

    def test_when_then_else_becomes_case_when(self):
        sql = _compile(_single_table("""select_final:
  - source: status
    target: status_label
    ops:
      - when: "equals:DONE"
        then: "lit:Paid"
      - else: "lit:Unknown"
"""))
        assert "CASE WHEN `status` = 'DONE' THEN 'Paid' ELSE 'Unknown' END AS `status_label`" in sql

    def test_when_without_else_falls_back_to_null(self):
        sql = _compile(_single_table("""select_final:
  - source: status
    target: label
    ops:
      - when: "equals:DONE"
        then: "lit:Paid"
"""))
        assert "ELSE NULL END AS `label`" in sql


class TestJoins:
    def _two_tables(self, join_block):
        return f"""
tables:
  - name: silver.orders
    alias: ord
  - name: silver.customers
    alias: cust
join:
{join_block}
keep_all_columns: true
"""

    def test_same_key_names_use_using(self):
        sql = _compile(self._two_tables("  - table_from: [ord, customer_id]\n"
                                        "    table_to: [cust, customer_id]\n"
                                        "    type: left\n"))
        assert "LEFT JOIN `cust` USING (`customer_id`)" in sql

    def test_different_key_names_use_on(self):
        sql = _compile(self._two_tables("  - table_from: [ord, customer_id]\n"
                                        "    table_to: [cust, id]\n"
                                        "    type: inner\n"))
        assert "INNER JOIN `cust` ON `ord`.`customer_id` = `cust`.`id`" in sql

    @pytest.mark.parametrize("declared,keyword", [
        ("left", "LEFT JOIN"),
        ("inner", "INNER JOIN"),
        ("right", "RIGHT JOIN"),
        ("full outer", "FULL OUTER JOIN"),
        ("anti", "LEFT ANTI JOIN"),
        ("semi", "LEFT SEMI JOIN"),
    ])
    def test_join_types(self, declared, keyword):
        sql = _compile(self._two_tables(f"  - table_from: [ord, customer_id]\n"
                                        f"    table_to: [cust, id]\n"
                                        f"    type: {declared}\n"))
        assert keyword in sql

    def test_cross_join_has_no_on_clause(self):
        sql = _compile(self._two_tables("  - table_from: ord\n"
                                        "    table_to: cust\n"
                                        "    type: cross\n"))
        assert "CROSS JOIN `cust`" in sql
        assert " ON " not in sql


class TestAggregateCompilation:
    def test_group_by_and_measures(self):
        sql = _compile(_single_table("""aggregate:
  group_by: [country]
  measures:
    - [amount, total_amount, sum]
    - [order_id, nb_orders, count_distinct]
    - ["*", nb_rows, count]
"""))
        assert "SUM(`amount`) AS `total_amount`" in sql
        assert "COUNT(DISTINCT `order_id`) AS `nb_orders`" in sql
        assert "COUNT(*) AS `nb_rows`" in sql
        assert "GROUP BY `country`" in sql

    def test_having_is_emitted(self):
        sql = _compile(_single_table("""aggregate:
  group_by: [country]
  measures:
    - [amount, total_amount, sum]
  having:
    - "total_amount:greater_than:1000"
"""))
        assert "HAVING `total_amount` > '1000'" in sql

    def test_add_columns_are_projected_before_the_group_by(self):
        sql = _compile(_single_table("""add_columns:
  - [order_date, order_month, [to_date:yyyy-MM]]
aggregate:
  group_by: [order_month]
  measures:
    - [amount, total_amount, sum]
"""))
        assert "SELECT *, TO_DATE(`order_date`, 'yyyy-MM') AS `order_month`" in sql
        assert "GROUP BY `order_month`" in sql


class TestDriftGuards:
    """The SQL dispatch tables must cover exactly the Spark ones."""

    def test_filter_dispatch_matches_spark(self):
        from skifer.core.spark_backend import _SPARK_FILTER_DISPATCH
        assert set(_SQL_FILTER_DISPATCH) == set(_SPARK_FILTER_DISPATCH)

    def test_op_dispatch_matches_spark(self):
        from skifer.core.spark_backend import _SPARK_OP_DISPATCH
        assert set(_SQL_OP_DISPATCH) == set(_SPARK_OP_DISPATCH)


# ==============================================================================
# Part 2 — rejections
# ==============================================================================

class TestRejections:
    """Constructs with no faithful SQL form raise instead of emitting bad SQL."""

    def test_business_rules_rejected(self):
        with pytest.raises(SqlCompilationError, match="business_rules"):
            _compile(_single_table("business_rules:\n  - flag_high_value\nkeep_all_columns: true\n"))

    def test_loader_rejected(self):
        with pytest.raises(SqlCompilationError, match="Python loaders"):
            _compile("""
tables:
  - name: raw
    alias: r
    source_type: loader
    loader: my_loader
keep_all_columns: true
""")

    def test_file_source_rejected(self):
        with pytest.raises(SqlCompilationError, match="file sources"):
            _compile("""
tables:
  - name: raw_orders
    alias: r
    source:
      type: csv
      path: /tmp/orders.csv
keep_all_columns: true
""")

    def test_dev_limit_rejected(self):
        with pytest.raises(SqlCompilationError, match="dev_limit"):
            _compile(_single_table("    dev_limit: 100\nkeep_all_columns: true\n"))

    def test_schema_level_dev_limit_rejected(self):
        with pytest.raises(SqlCompilationError, match="dev_limit"):
            _compile(_single_table("keep_all_columns: true\ndev_limit: 100\n"))

    def test_drop_duplicates_on_rejected(self):
        with pytest.raises(SqlCompilationError, match="drop_duplicates_on"):
            _compile(_single_table("""    quality_checks:
      drop_duplicates_on: [order_id]
keep_all_columns: true
"""))

    def test_qualify_rejected(self):
        with pytest.raises(SqlCompilationError, match="preprocess.qualify"):
            _compile(_single_table("""    preprocess:
      qualify:
        partition_by: [order_id]
        order_by:
          - column: updated_at
            direction: desc
keep_all_columns: true
"""))

    def test_no_tables_rejected(self):
        from skifer.core.ir import ParsedSchema
        with pytest.raises(SqlCompilationError, match="No tables declared"):
            compile_select(ParsedSchema())


# ==============================================================================
# Part 3 — parity: compiled SQL vs DataFrame path
# ==============================================================================

ORDERS = [
    (1, "FR", "DONE", 100.0, "2026-01-15"),
    (2, "FR", "DONE", 300.0, "2026-01-20"),
    (3, "DE", "PENDING", 50.0, "2026-02-01"),
    (4, "DE", "DONE", 70.0, "2026-02-03"),
    (5, "IT", "DONE", None, "2026-03-01"),
]
CUSTOMERS = [(1, "FR", "Alice"), (2, "FR", "Bob"), (3, "DE", "Carla"), (4, "DE", "Dieter")]


def _make_engine(backend):
    engine = object.__new__(SkiferEngine)
    ctx = ExecutionContext()
    object.__setattr__(engine, "_context", ctx)
    engine.spark = backend.spark
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


@pytest.fixture(scope="class")
def parity_views(request):
    """Register the fixture tables as temp views for both execution paths."""
    spark = request.getfixturevalue("spark")
    spark.createDataFrame(
        ORDERS, ["order_id", "country", "status", "amount", "order_date"]
    ).createOrReplaceTempView("orders")
    spark.createDataFrame(
        CUSTOMERS, ["order_id", "cust_country", "name"]
    ).createOrReplaceTempView("customers")
    # Same rows under a differently-named key — exercises the ON join path.
    spark.createDataFrame(
        CUSTOMERS, ["cust_id", "cust_country", "name"]
    ).createOrReplaceTempView("customers_alt")
    return spark


class TestParity:
    """Both paths must produce identical rows — the core anti-drift guarantee."""

    def _assert_parity(self, spark, yaml_schema):
        from skifer.core.spark_backend import SparkBackend

        schema = parse_schema(yaml_schema)
        backend = SparkBackend(spark=spark, is_local=True)
        engine = _make_engine(backend)

        df_path = engine.process_schema(schema)
        sql_text = compile_select(parse_to_ir(schema))
        assert sql_text == compile_select(parse_to_ir(schema), dialect="databricks")
        df_sql = spark.sql(sql_text)

        assert sorted(df_sql.columns) == sorted(df_path.columns), (
            f"column mismatch\nSQL:\n{sql_text}"
        )
        cols = sorted(df_path.columns)
        rows_path = sorted(str(r) for r in df_path.select(*cols).collect())
        rows_sql = sorted(str(r) for r in df_sql.select(*cols).collect())
        assert rows_sql == rows_path, f"row mismatch\nSQL:\n{sql_text}"
        return sql_text

    def test_parity_filters_and_projection(self, parity_views):
        self._assert_parity(parity_views, """
tables:
  - name: orders
    alias: ord
    filter:
      - "status:equals:DONE"
      - "country:in:FR,DE"
select_final:
  - [order_id, order_id]
  - [country, country]
  - [amount, amount_rounded, [round:1]]
  - [literal:ERP, source_system]
""")

    def test_parity_filter_groups_and_drop_nulls(self, parity_views):
        self._assert_parity(parity_views, """
tables:
  - name: orders
    alias: ord
    quality_checks:
      drop_nulls_in: [amount]
    filter_groups:
      - ["country:equals:FR", "status:equals:DONE"]
      - ["country:equals:DE"]
select_final:
  - [order_id, order_id]
  - [amount, amount]
""")

    def test_parity_case_when_chain(self, parity_views):
        self._assert_parity(parity_views, """
tables:
  - name: orders
    alias: ord
select_final:
  - [order_id, order_id]
  - source: status
    target: status_label
    ops:
      - when: "equals:DONE"
        then: "lit:Paid"
      - else: "lit:Unknown"
""")

    def test_parity_ops_chain_and_null_handling(self, parity_views):
        self._assert_parity(parity_views, """
tables:
  - name: orders
    alias: ord
select_final:
  - [order_id, order_id]
  - [amount, amount_filled, [nvl:0]]
  - [country, country_lower, [lower]]
  - [order_date, order_dt, ["to_date:yyyy-MM-dd"]]
""")

    def test_parity_inner_join_same_key(self, parity_views):
        self._assert_parity(parity_views, """
tables:
  - name: orders
    alias: ord
  - name: customers
    alias: cust
join:
  - table_from: [ord, order_id]
    table_to: [cust, order_id]
    type: inner
select_final:
  - [order_id, order_id]
  - [country, country]
  - [name, customer_name]
""")

    def test_parity_left_join_same_key(self, parity_views):
        self._assert_parity(parity_views, """
tables:
  - name: orders
    alias: ord
  - name: customers
    alias: cust
join:
  - table_from: [ord, order_id]
    table_to: [cust, order_id]
    type: left
select_final:
  - [order_id, order_id]
  - [name, customer_name]
""")

    def test_parity_join_with_different_key_names(self, parity_views):
        """ON-form join: the DataFrame path drops the right key, SQL keeps it —
        an explicit select_final makes both paths agree."""
        self._assert_parity(parity_views, """
tables:
  - name: orders
    alias: ord
  - name: customers_alt
    alias: cust
join:
  - table_from: [ord, order_id]
    table_to: [cust, cust_id]
    type: left
select_final:
  - [order_id, order_id]
  - [name, customer_name]
  - [cust_country, cust_country]
""")

    def test_parity_aggregate(self, parity_views):
        self._assert_parity(parity_views, """
tables:
  - name: orders
    alias: ord
    filter:
      - "status:equals:DONE"
aggregate:
  group_by: [country]
  measures:
    - [amount, total_amount, sum]
    - [order_id, nb_orders, count]
    - [amount, max_amount, max]
""")

    def test_parity_aggregate_with_having(self, parity_views):
        self._assert_parity(parity_views, """
tables:
  - name: orders
    alias: ord
aggregate:
  group_by: [country]
  measures:
    - [order_id, nb_orders, count]
  having:
    - "nb_orders:greater_than:1"
""")

    def test_parity_aggregate_over_join(self, parity_views):
        self._assert_parity(parity_views, """
tables:
  - name: orders
    alias: ord
  - name: customers
    alias: cust
join:
  - table_from: [ord, order_id]
    table_to: [cust, order_id]
    type: inner
aggregate:
  group_by: [cust_country]
  measures:
    - [amount, total_amount, sum]
""")

    def test_parity_keep_all_columns_single_table(self, parity_views):
        self._assert_parity(parity_views, """
tables:
  - name: orders
    alias: ord
    filter:
      - "status:equals:DONE"
keep_all_columns: true
""")


class TestDialectRendering:
    """Default identity is also checked by _compile and _assert_parity above,
    so every pre-existing YAML case exercises explicit Databricks rendering.
    """

    def test_default_is_databricks(self):
        parsed = parse_to_ir(parse_schema(_single_table("keep_all_columns: true\n")))
        assert compile_select(parsed) == compile_select(parsed, dialect="databricks")

    def test_used_capabilities_includes_nested_ops(self):
        from skifer.core.sql_compiler import used_capabilities

        parsed = parse_to_ir(parse_schema('''
tables:
  - name: orders
    alias: ord
    filter: ["status:eq:DONE"]
    filter_groups: [["amount:between:1,10"]]
    fields:
      - [country, country, [upper]]
      - [amount, amount, [cast:double]]
      - [id, id]
  - name: customers
    alias: cust
    filter: ["id:is_not_null"]
join:
  - table_from: [ord, id]
    table_to: [cust, id]
    type: left outer
where: ["amount:greater_than:0"]
where_groups: [["country:not_equals:US"]]
add_columns:
  - [amount, rounded, ["when:less_than:5", "then:round:1", "else:nvl:0"]]
  - source: country
    target: label
    ops:
      - when: "in:FR,DE"
        then: "lower"
      - else: "lit:other"
aggregate:
  group_by: [country]
  measures: [[rounded, total, sum]]
  having: ["total:greater_than_equal:1"]
'''))
        assert used_capabilities(parsed) == {
            "filter:equals", "filter:between", "filter:is_not_null",
            "filter:greater_than", "filter:not_equals", "filter:less_than",
            "filter:in", "filter:greater_than_equal", "op:upper", "op:cast",
            "op:when", "op:then", "op:else", "op:round", "op:nvl",
            "op:lower", "op:lit", "agg:sum", "join:left",
        }

    def test_select_final_raw_and_nested_capabilities(self):
        from skifer.core.sql_compiler import used_capabilities

        parsed = parse_to_ir(parse_schema(_single_table('''    filter: ["amount:sql:amount > 0"]
select_final:
  - [amount, out, ["when:is_null", "then:expr:1 + 2", "else:cast:double"]]
''')))
        assert used_capabilities(parsed) == {
            "filter:sql", "filter:is_null", "op:when", "op:then", "op:else",
            "op:expr", "op:cast",
        }

    def test_generated_null_filter_is_collected(self):
        from skifer.core.sql_compiler import used_capabilities

        parsed = parse_to_ir(parse_schema(_single_table('''    quality_checks:
      drop_nulls_in: [amount]
keep_all_columns: true
''')))
        assert used_capabilities(parsed) == {"filter:is_not_null"}

    def test_snowflake_refuses_first_but_duckdb_renders(self):
        parsed = parse_to_ir(parse_schema(_single_table('''aggregate:
  group_by: [country]
  measures: [[amount, first_amount, first]]
''')))
        with pytest.raises(
            SqlCompilationError,
            match="agg:first.*non-deterministic without ORDER BY; no portable equivalent",
        ):
            compile_select(parsed, dialect="snowflake")
        assert 'FIRST(' in compile_select(parsed, dialect="duckdb")

    def test_unknown_dialect(self):
        parsed = parse_to_ir(parse_schema(_single_table("keep_all_columns: true\n")))
        with pytest.raises(SqlCompilationError, match="oracle"):
            compile_select(parsed, dialect="oracle")

    def test_dialect_error_is_chained(self, monkeypatch):
        import sys
        from skifer.core.dialect import DialectError

        parsed = parse_to_ir(parse_schema(_single_table("keep_all_columns: true\n")))
        monkeypatch.setitem(sys.modules, "sqlglot", None)
        with pytest.raises(SqlCompilationError, match="sqlglot") as raised:
            compile_select(parsed, dialect="duckdb")
        assert isinstance(raised.value.__cause__, DialectError)

    @pytest.mark.parametrize("declared,canonical", [("anti", "left_anti"), ("full outer", "full")])
    def test_used_capabilities_canonicalizes_join_and_aggregate_aliases(self, declared, canonical):
        from skifer.core.sql_compiler import used_capabilities

        parsed = parse_to_ir(parse_schema(f"""
tables:
  - name: orders
    alias: ord
  - name: customers
    alias: cust
join:
  - table_from: [ord, customer_id]
    table_to: [cust, id]
    type: {declared}
aggregate:
  group_by: [country]
  measures: [[amount, m, mean]]
"""))
        assert used_capabilities(parsed) == {f"join:{canonical}", "agg:avg"}

    def test_report_and_error_include_all_refused_capabilities(self):
        from skifer.core.capability_matrix import Refused, Supported, capability
        from skifer.core.sql_compiler import capability_report

        parsed = parse_to_ir(parse_schema("""
tables:
  - name: orders
    alias: ord
  - name: customers
    alias: cust
join:
  - table_from: [ord, customer_id]
    table_to: [cust, id]
    type: left_anti
aggregate:
  group_by: [country]
  measures: [[amount, m, first]]
"""))
        report = capability_report(parsed, "snowflake")
        assert list(report) == ["agg:first", "join:left_anti"]
        assert report == {key: capability("snowflake", key) for key in report}
        assert all(isinstance(status, Refused) for status in report.values())
        assert all(isinstance(status, Supported) for status in capability_report(parsed, "databricks").values())
        with pytest.raises(SqlCompilationError) as raised:
            compile_select(parsed, dialect="snowflake")
        assert str(raised.value) == (
            "Capabilities refused for 'snowflake': "
            "'agg:first': non-deterministic without ORDER BY; no portable equivalent; "
            "'join:left_anti': no SEMI/ANTI JOIN syntax on this target"
        )

    @pytest.mark.parametrize("target", ["duckdb", "snowflake"])
    def test_report_exposes_conditionals_without_logging_or_mutation(self, target, caplog):
        from copy import deepcopy
        from skifer.core.capability_matrix import Conditional, Refused, capability
        from skifer.core.sql_compiler import capability_report

        parsed = parse_to_ir(parse_schema(_single_table('''select_final:
  - [amount, out, [cast:double, round:2]]
''')))
        before = deepcopy(parsed)
        report = capability_report(parsed, target)
        assert list(report) == ["op:cast", "op:round"]
        assert report == {key: capability(target, key) for key in report}
        if target == "snowflake":
            assert all(isinstance(status, Conditional) for status in report.values())
        else:
            assert report == {key: capability("duckdb", key) for key in report}
            assert not any(isinstance(status, Refused) for status in report.values())
        compile_select(parsed, dialect=target)
        assert parsed == before
        assert not caplog.records

    @pytest.mark.parametrize("target", ["duckdb", "snowflake"])
    @pytest.mark.parametrize("body,key", [
        ('    filter: ["amount:sql:amount > 0"]\nkeep_all_columns: true\n', "filter:sql"),
        ('select_final:\n  - [amount, out, ["expr:amount * 2"]]\n', "op:expr"),
    ])
    def test_raw_sql_is_refused_before_rendering(self, monkeypatch, target, body, key):
        import skifer.core.sql_compiler as compiler

        def unexpected_render(*args, **kwargs):
            pytest.fail("Refused raw SQL must not reach rendering")

        parsed = parse_to_ir(parse_schema(_single_table(body)))
        monkeypatch.setattr(compiler, "render", unexpected_render)
        with pytest.raises(SqlCompilationError, match=f"{key}.*raw SQL fragments"):
            compile_select(parsed, dialect=target)

    def test_historical_rejections_precede_capability_preflight(self, monkeypatch):
        import skifer.core.sql_compiler as compiler

        parsed = parse_to_ir(parse_schema(_single_table('''business_rules: [flag_high_value]
aggregate:
  group_by: [country]
  measures: [[amount, m, first]]
''')))

        def unexpected_report(*args, **kwargs):
            pytest.fail("Uncompilable schemas must be rejected before capability preflight")

        monkeypatch.setattr(compiler, "capability_report", unexpected_report)
        with pytest.raises(SqlCompilationError) as raised:
            compile_select(parsed, dialect="snowflake")
        assert str(raised.value) == (
            "Python business_rules ['flag_high_value'] cannot be compiled to SQL. "
            "Materialize them upstream in a silver table, then join/aggregate that table here."
        )

    def test_report_rejects_unknown_target_even_without_operations(self):
        from skifer.core.sql_compiler import capability_report

        parsed = parse_to_ir(parse_schema(_single_table("keep_all_columns: true\n")))
        with pytest.raises(SqlCompilationError, match="oracle"):
            capability_report(parsed, "oracle")
