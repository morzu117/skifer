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
    # Plan 47.4: `SELECT *` over differently named keys is refused (see
    # TestStarOverRenamedJoinKeys), so those joins project explicitly.
    _EXPLICIT = "select_final: [[order_id, order_id]]"

    def _two_tables(self, join_block, projection="keep_all_columns: true"):
        return f"""
tables:
  - name: silver.orders
    alias: ord
  - name: silver.customers
    alias: cust
join:
{join_block}
{projection}
"""

    def test_same_key_names_use_using(self):
        sql = _compile(self._two_tables("  - table_from: [ord, customer_id]\n"
                                        "    table_to: [cust, customer_id]\n"
                                        "    type: left\n"))
        assert "LEFT JOIN `cust` USING (`customer_id`)" in sql

    def test_different_key_names_use_on(self):
        sql = _compile(self._two_tables("  - table_from: [ord, customer_id]\n"
                                        "    table_to: [cust, id]\n"
                                        "    type: inner\n", self._EXPLICIT))
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
                                        f"    type: {declared}\n", self._EXPLICIT))
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
        from skifer.core.capability_matrix import Conditional, Supported, capability
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
            assert report == {"op:cast": capability("duckdb", "op:cast"), "op:round": Supported()}
            assert isinstance(report["op:cast"], Conditional)
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


# ==============================================================================
# Plan 47.1 — closed grammar for the cast type (R1)
# ==============================================================================

_CAST_DIALECTS = ("databricks", "duckdb", "snowflake")

_CAST_PAYLOADS = (
    "int) + (SELECT max(secret) FROM main.vault",
    "int) AS a, (SELECT 1",
    "int; DROP TABLE x",
    "int -- x",
    "varchar2",
    "array<array<array<array<int>>>>",
)


def _cast_schema(cast_type: str):
    return parse_to_ir(parse_schema(_single_table(
        f'select_final:\n  - [amount, out, ["cast:{cast_type}"]]\n'
    )))


class TestCastTypeGrammar:
    @pytest.mark.parametrize("cast_type, rendered", [
        ("double", "double"), ("string", "string"), ("date", "date"), ("int", "int"),
        ("long", "long"), ("timestamp", "timestamp"), ("integer", "integer"),
        ("decimal", "decimal"), ("datetime", "timestamp"),
    ])
    def test_types_used_in_the_repository_compile_unchanged(self, cast_type, rendered):
        sql = compile_select(_cast_schema(cast_type))
        assert f"CAST(`amount` AS {rendered}) AS `out`" in sql
        for dialect in _CAST_DIALECTS:
            compile_select(_cast_schema(cast_type), dialect=dialect)

    @pytest.mark.parametrize("cast_type", [
        "decimal(10,2)", "array<string>", "map<string,int>", "struct<a:int,b:array<string>>",
        "DECIMAL(18, 4)", "varchar(20)", "array<array<array<int>>>",
    ])
    def test_parameterised_and_composite_types_compile(self, cast_type):
        from skifer.core.sql_compiler import validate_cast_type

        sql = compile_select(_cast_schema(cast_type))
        assert f"CAST(`amount` AS {validate_cast_type(cast_type)}) AS `out`" in sql
        assert validate_cast_type("decimal(10, 2)") == "decimal(10,2)"
        assert validate_cast_type("map<string, int>") == "map<string,int>"

    @pytest.mark.parametrize("dialect", _CAST_DIALECTS)
    @pytest.mark.parametrize("cast_type", _CAST_PAYLOADS)
    def test_payloads_unknown_types_and_depth_five_are_refused(self, cast_type, dialect):
        with pytest.raises(SqlCompilationError) as raised:
            compile_select(_cast_schema(cast_type), dialect=dialect, allow_raw_sql=False)
        message = str(raised.value)
        assert repr(cast_type) in message
        assert "decimal" in message and "struct<" in message  # the admitted list is cited

    @pytest.mark.parametrize("cast_type", [
        "", "int(10)", "decimal(10,2,3)", "decimal(a)", "struct<>", "struct<1a:int>",
        "struct<a b:int>", "map<string>", "array<string", "array<string>>", "intx",
        "int\x00", "double)", "decimal(-1,2)", "struct<a:int,>",
    ])
    def test_malformed_types_are_refused(self, cast_type):
        from skifer.core.sql_compiler import validate_cast_type

        with pytest.raises(SqlCompilationError, match="cast"):
            validate_cast_type(cast_type)

    def test_scalar_names_are_case_insensitive(self):
        from skifer.core.sql_compiler import validate_cast_type

        assert validate_cast_type("BIGINT") == "BIGINT"
        assert validate_cast_type("TIMESTAMP") == "timestamp"
        assert validate_cast_type("Array<Datetime>") == "Array<timestamp>"


_PROBE = "p`'\\) OR (1=1 --"


class TestNoDispatchEntryInterpolatesRawArguments:
    """Every dispatch entry either renders its argument escaped/validated or raises."""

    @pytest.mark.parametrize("operator", sorted(_SQL_FILTER_DISPATCH))
    def test_filter_dispatch(self, operator):
        from skifer.core.ir import ParsedFilter

        value = [_PROBE, _PROBE] if operator in ("between", "not_between") else _PROBE
        try:
            sql = _SQL_FILTER_DISPATCH[operator](
                "`c`", ParsedFilter(column="c", operator=operator, value=value)
            )
        except (SqlCompilationError, ValueError):
            return
        assert _PROBE not in sql

    @pytest.mark.parametrize("name", sorted(_SQL_OP_DISPATCH))
    def test_op_dispatch(self, name):
        from skifer.core.ir import ParsedOp

        try:
            sql = _SQL_OP_DISPATCH[name]("`c`", ParsedOp(name, (_PROBE, _PROBE)))
        except (SqlCompilationError, ValueError):
            return
        assert _PROBE not in sql

    def test_cast_with_the_probe_is_refused_not_escaped(self):
        from skifer.core.ir import ParsedOp

        with pytest.raises(SqlCompilationError):
            _SQL_OP_DISPATCH["cast"]("`c`", ParsedOp("cast", (_PROBE,)))


# ==============================================================================
# Plan 47.2 — quote_fqn lets nothing through raw (R2)
# ==============================================================================

_R2_PAYLOAD = "`ORDERS` WHERE 1=0 UNION ALL SELECT * FROM `SALARIES`"


class TestQuoteFqn:
    @pytest.mark.parametrize("name, quoted", [
        ("orders", "`orders`"),
        ("silver.orders", "`silver`.`orders`"),
        ("cat.silver.orders", "`cat`.`silver`.`orders`"),
        ("`orders`", "`orders`"),
        ("`silver`.`orders`", "`silver`.`orders`"),
        ("`cat`.`silver`.`orders`", "`cat`.`silver`.`orders`"),
        ("`my cat`.`sil-ver`.`or.ders`", "`my cat`.`sil-ver`.`or.ders`"),
    ])
    def test_one_to_three_parts_quoted_or_not(self, name, quoted):
        from skifer.core.sql_compiler import quote_fqn

        assert quote_fqn(name) == quoted

    @pytest.mark.parametrize("catalog", [None, "dev_catalog"])
    def test_sandbox_output_is_accepted_unchanged(self, catalog):
        # Same shapes as core/sandbox.py's resolve(): fully back-quoted, 2 or 3 parts.
        from skifer.core.sql_compiler import quote_fqn

        sandboxed = "`silver_jdoe`.`orders`" if catalog is None else (
            f"`{catalog}`.`silver_jdoe`.`orders`"
        )
        assert quote_fqn(sandboxed) == sandboxed

    @pytest.mark.parametrize("name", [
        _R2_PAYLOAD,
        "`orders",
        "orders`",
        "`a`b`",
        "cat.`silver`.orders",
        "`a`.`b`.`c`.`d`",
        "``",
        "`a`.``",
        "`a`..`b`",
        "`a\x00b`",
        "`a`.`b\x00`",
        "a..b",
        ".orders",
        "orders.",
        "",
    ])
    def test_malformed_names_are_refused(self, name):
        from skifer.core.sql_compiler import quote_fqn

        with pytest.raises(SqlCompilationError, match="table name"):
            quote_fqn(name)

    @pytest.mark.parametrize("dialect", _CAST_DIALECTS)
    def test_compile_select_refuses_a_forged_resolved_name(self, dialect):
        parsed = parse_to_ir(parse_schema(_single_table("keep_all_columns: true\n")))
        with pytest.raises(SqlCompilationError, match="table name"):
            compile_select(parsed, resolve_table=lambda name: _R2_PAYLOAD, dialect=dialect)


# ==============================================================================
# Plan 47.3 — a table declared but never joined (R6)
# ==============================================================================

class TestUnjoinedTable:
    def test_second_table_without_any_join_is_refused(self):
        with pytest.raises(SqlCompilationError, match="'cust'") as raised:
            _compile("""
tables:
  - {name: silver.orders, alias: ord}
  - {name: silver.customers, alias: cust}
keep_all_columns: true
""")
        assert "join" in str(raised.value) and "remove" in str(raised.value)

    def test_third_table_left_out_of_the_joins_is_refused(self):
        with pytest.raises(SqlCompilationError, match="'ctry'"):
            _compile("""
tables:
  - {name: silver.orders, alias: ord}
  - {name: silver.customers, alias: cust}
  - {name: silver.countries, alias: ctry}
join:
  - {table_from: [ord, customer_id], table_to: [cust, customer_id]}
select_final:
  - [order_id, order_id]
""")

    def test_every_table_joined_compiles(self):
        sql = _compile("""
tables:
  - {name: silver.orders, alias: ord}
  - {name: silver.customers, alias: cust}
  - {name: silver.countries, alias: ctry}
join:
  - {table_from: [ord, customer_id], table_to: [cust, customer_id]}
  - {table_from: [cust, country], table_to: [ctry, country]}
select_final:
  - [order_id, order_id]
""")
        assert "JOIN `cust`" in sql and "JOIN `ctry`" in sql

    def test_single_table_is_unaffected(self):
        assert "FROM `ord`" in _compile(_single_table("keep_all_columns: true\n"))


# ==============================================================================
# Plan 47.5 — value-conditioned capabilities (R5)
# ==============================================================================

_DECIMAL_TEXT = "literal:decimal_text"
_LIKE_BACKSLASH = "literal:like_backslash"


def _used(yaml_text: str) -> set[str]:
    from skifer.core.sql_compiler import used_capabilities

    return used_capabilities(parse_to_ir(parse_schema(yaml_text)))


class TestValueConditionedCapabilities:
    @pytest.mark.parametrize("operator", [
        "equals", "not_equals", "greater_than", "less_than",
        "greater_than_equal", "less_than_equal",
    ])
    def test_decimal_text_in_a_scalar_comparison(self, operator):
        assert _DECIMAL_TEXT in _used(_single_table(f"""    filter: ["qty:{operator}:6.5"]
select_final: [[qty, qty]]
"""))

    @pytest.mark.parametrize("operator,value", [
        ("in", "1,-2.5"), ("not_in", "3.25,4"), ("between", "1,6.5"), ("not_between", "-0.5,2"),
    ])
    def test_decimal_text_in_one_value_of_a_list_comparison(self, operator, value):
        assert _DECIMAL_TEXT in _used(_single_table(f"""    filter: ["qty:{operator}:{value}"]
select_final: [[qty, qty]]
"""))

    @pytest.mark.parametrize("value", ["6", "-6", "6.", ".5", "1e3", "6.5x", "FR"])
    def test_other_text_is_not_decimal_text(self, value):
        assert _DECIMAL_TEXT not in _used(_single_table(f"""    filter: ["qty:equals:{value}"]
select_final: [[qty, qty]]
"""))

    def test_a_numeric_yaml_value_is_not_text(self):
        assert _DECIMAL_TEXT not in _used(_single_table("""    filter:
      - {column: qty, operator: greater_than, value: 6.5}
select_final: [[qty, qty]]
"""))

    def test_decimal_text_on_a_non_comparison_operator_is_not_emitted(self):
        assert _DECIMAL_TEXT not in _used(_single_table("""    filter: ["label:contains:6.5"]
select_final: [[label, label]]
"""))

    @pytest.mark.parametrize("operator", [
        "like", "not_like", "contains", "not_contains", "starts_with", "ends_with",
    ])
    def test_backslash_in_a_like_family_value(self, operator):
        assert _LIKE_BACKSLASH in _used(_single_table(f"""    filter: ['label:{operator}:a\\b']
select_final: [[label, label]]
"""))

    def test_no_backslash_no_like_key(self):
        keys = _used(_single_table("""    filter: ["label:like:100%"]
select_final: [[label, label]]
"""))
        assert _LIKE_BACKSLASH not in keys

    def test_backslash_on_a_comparison_is_not_a_like_key(self):
        assert _LIKE_BACKSLASH not in _used(_single_table("""    filter: ['label:equals:a\\b']
select_final: [[label, label]]
"""))

    def test_every_filter_location_is_walked(self):
        locations = {
            "filter_groups": """    filter_groups: [["qty:equals:6.5"]]
select_final: [[qty, qty]]
""",
            "where": """where: ["qty:equals:6.5"]
select_final: [[qty, qty]]
""",
            "where_groups": """where_groups: [["qty:equals:6.5"]]
select_final: [[qty, qty]]
""",
            "having": """aggregate:
  group_by: [country]
  measures: [[qty, total, sum]]
  having: ["total:greater_than:6.5"]
""",
            "when": """select_final:
  - [qty, band, ["when:greater_than:6.5", "then:lit:high", "else:lit:low"]]
""",
            "when_like": """select_final:
  - source: label
    target: flag
    ops:
      - when: 'like:a\\b'
        then: "lit:yes"
      - else: "lit:no"
""",
        }
        for location, body in locations.items():
            expected = _LIKE_BACKSLASH if location == "when_like" else _DECIMAL_TEXT
            assert expected in _used(_single_table(body)), location

    @pytest.mark.parametrize("target", ["duckdb", "snowflake"])
    def test_like_backslash_is_refused_outside_databricks(self, target):
        parsed = parse_to_ir(parse_schema(_single_table("""    filter: ['label:like:100\\%']
select_final: [[label, label]]
""")))
        with pytest.raises(SqlCompilationError, match="'literal:like_backslash'"):
            compile_select(parsed, dialect=target)
        assert "LIKE '100\\\\%'" in compile_select(parsed)

    @pytest.mark.parametrize("target", ["duckdb", "snowflake"])
    def test_decimal_text_is_reported_conditional_and_still_compiles(self, target):
        from skifer.core.capability_matrix import Conditional, Supported
        from skifer.core.sql_compiler import capability_report

        parsed = parse_to_ir(parse_schema(_single_table("""    filter: ["qty:greater_than:6.5"]
select_final: [[qty, qty]]
""")))
        assert isinstance(capability_report(parsed, target)[_DECIMAL_TEXT], Conditional)
        assert capability_report(parsed, "databricks")[_DECIMAL_TEXT] == Supported()
        assert "SELECT" in compile_select(parsed, dialect=target)


# Plan 47.4 — SELECT * over a join whose keys have different names (R5)
# ==============================================================================

def _two_tables(join: str, projection: str) -> str:
    return f"""
tables:
  - {{name: silver.orders, alias: ord}}
  - {{name: silver.customers, alias: cust}}
join:
  - {join}
{projection}
"""


_RENAMED_KEYS = "{table_from: [ord, customer_id], table_to: [cust, id], type: %s}"
_SAME_KEYS = "{table_from: [ord, customer_id], table_to: [cust, customer_id], type: left}"


class TestStarOverRenamedJoinKeys:
    @pytest.mark.parametrize("join_type", ["left", "inner", "right", "full"])
    @pytest.mark.parametrize("projection", ["keep_all_columns: true", "add_columns:\n  - [amount, a2, [round:2]]"])
    def test_star_projection_is_refused(self, join_type, projection):
        with pytest.raises(SqlCompilationError, match="select_final") as raised:
            _compile(_two_tables(_RENAMED_KEYS % join_type, projection))
        assert "'cust'" in str(raised.value) and "id" in str(raised.value)

    @pytest.mark.parametrize("join, projection", [
        (_SAME_KEYS, "keep_all_columns: true"),
        ("{table_from: ord, table_to: cust, type: cross}", "keep_all_columns: true"),
        (_RENAMED_KEYS % "left_semi", "keep_all_columns: true"),
        (_RENAMED_KEYS % "left_anti", "keep_all_columns: true"),
        (_RENAMED_KEYS % "left", "select_final:\n  - [order_id, order_id]"),
        (_RENAMED_KEYS % "left",
         "aggregate:\n  group_by: [segment]\n  measures: [[amount, total, sum]]"),
    ])
    def test_other_shapes_still_compile(self, join, projection):
        assert _compile(_two_tables(join, projection)).startswith("WITH")
