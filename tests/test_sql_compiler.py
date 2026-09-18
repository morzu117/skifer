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
from skifer.core.registry import RuleRegistry
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
    return compile_select(parse_to_ir(parse_schema(yaml_schema)), **kwargs)


def _single_table(body: str) -> str:
    return f"""
tables:
  - name: silver.orders
    alias: ord
{body}
"""


@pytest.fixture
def register_rule():
    names = []

    def register(name, result, kind="sql"):
        if kind == "sql":
            @RuleRegistry.register_rule(name=name, kind=kind)
            def rule():
                return result
        else:
            @RuleRegistry.register_rule(name=name, kind=kind)
            def rule(df):
                return result

        names.append(name)
        return rule

    yield register

    for name in names:
        RuleRegistry._rules.pop(name, None)


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

    def test_pipeline_without_sql_rule_is_byte_for_byte_unchanged(self):
        assert _compile(_single_table("keep_all_columns: true\n")) == (
            "WITH `ord` AS (\n"
            "  SELECT * FROM `silver`.`orders`\n"
            ")\n"
            "SELECT *\n"
            "FROM `ord`"
        )

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

    def test_drop_duplicates_on_compiles_to_qualify_for_batch_execution(self):
        sql = _compile(
            _single_table("""    quality_checks:
      drop_duplicates_on: [customer_id, order_id]
keep_all_columns: true
"""),
            persisted_definition=False,
        )

        assert (
            "QUALIFY ROW_NUMBER() OVER (PARTITION BY `customer_id`, `order_id` "
            "ORDER BY `customer_id`, `order_id`) = 1" in sql
        )

    def test_table_dev_limit_overrides_schema_dev_limit_for_batch_execution(self):
        sql = _compile(
            _single_table("    dev_limit: 3\nkeep_all_columns: true\ndev_limit: 10\n"),
            persisted_definition=False,
        )

        assert "LIMIT 3" in sql
        assert "LIMIT 10" not in sql

    def test_schema_dev_limit_compiles_for_batch_execution(self):
        sql = _compile(
            _single_table("keep_all_columns: true\ndev_limit: 10\n"),
            persisted_definition=False,
        )

        assert "LIMIT 10" in sql


class TestPartialCompilation:
    def test_simple_partial_is_compiled_before_table_ctes(self):
        child = {
            "tables": [{"name": "silver.orders", "alias": "orders"}],
            "select_final": [["order_id", "order_id"]],
        }
        parsed = parse_to_ir(
            {
                "partials": [
                    {
                        "alias": "daily_orders",
                        "resolved_path": "/schemas/daily_orders.yaml",
                        "schema": child,
                    }
                ],
                "tables": [{"name": "silver.calendar", "alias": "calendar"}],
                "join": [
                    {
                        "table_from": ["daily_orders", "order_id"],
                        "table_to": ["calendar", "order_id"],
                        "type": "inner",
                    }
                ],
                "select_final": [["order_id", "order_id"]],
            }
        )

        sql = compile_select(parsed)

        assert sql.index("`daily_orders` AS") < sql.index("`calendar` AS")
        assert "`daily_orders` AS (\n  WITH `orders` AS" in sql
        assert "FROM `daily_orders`\n  INNER JOIN `calendar` USING (`order_id`)" in sql

    def test_partial_can_contain_another_partial(self):
        leaf = {
            "tables": [{"name": "bronze.events", "alias": "events"}],
            "select_final": [["event_id", "event_id"]],
        }
        middle = {
            "partials": [
                {
                    "alias": "leaf_events",
                    "resolved_path": "/schemas/leaf.yaml",
                    "schema": leaf,
                }
            ],
            "select_final": [["event_id", "event_id"]],
        }
        parsed = parse_to_ir(
            {
                "partials": [
                    {
                        "alias": "middle_events",
                        "resolved_path": "/schemas/middle.yaml",
                        "schema": middle,
                    }
                ],
                "select_final": [["event_id", "event_id"]],
            }
        )

        sql = compile_select(parsed)

        assert "`middle_events` AS (\n  WITH `leaf_events` AS (\n  WITH `events` AS" in sql
        assert sql.endswith("SELECT `event_id` AS `event_id`\nFROM `middle_events`")

    def test_uncompilable_partial_error_names_alias_and_path(self):
        child = {
            "tables": [{"name": "silver.orders", "alias": "orders"}],
            "business_rules": ["enrich_orders"],
            "keep_all_columns": True,
        }
        parsed = parse_to_ir(
            {
                "partials": [
                    {
                        "alias": "bad_orders",
                        "resolved_path": "/schemas/bad_orders.yaml",
                        "schema": child,
                    }
                ],
                "keep_all_columns": True,
            }
        )

        with pytest.raises(SqlCompilationError) as exc_info:
            compile_select(parsed)

        message = str(exc_info.value)
        assert "partial 'bad_orders'" in message
        assert "/schemas/bad_orders.yaml" in message
        assert "business_rules" in message


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
        sql = _compile(self._two_tables("  - table_from: [ord, customer_id]\n"
                                        "    table_to: [cust, id]\n"
                                        "    type: cross\n"))
        assert "CROSS JOIN `cust`" in sql
        assert " ON " not in sql


class TestSqlRuleCompilation:
    def test_simple_rule_is_projected_between_join_and_final_select(self, register_rule):
        register_rule("double_amount", {"doubled": "amount * 2"})

        sql = _compile(
            _single_table("""business_rules:
  - double_amount
keep_all_columns: true
"""),
            resolve_columns=lambda _fqn: ["amount"],
        )

        assert "SELECT *, amount * 2 AS `doubled`\n  FROM `ord`" in sql
        assert "AS `_skifer_rules`" in sql
        assert sql.endswith(
            "SELECT *\nFROM (\n  SELECT *, amount * 2 AS `doubled`\n"
            "  FROM `ord`\n) AS `_skifer_rules`"
        )

    def test_two_rules_keep_declaration_order(self, register_rule):
        register_rule("first_sql", {"first_derived": "amount + 1"})
        register_rule("second_sql", {"second_derived": "amount + 2"})

        sql = _compile(
            _single_table("""business_rules:
  - first_sql
  - second_sql
keep_all_columns: true
"""),
            resolve_columns=lambda _fqn: ["amount"],
        )

        assert sql.index("amount + 1 AS `first_derived`") < sql.index(
            "amount + 2 AS `second_derived`"
        )

    def test_constant_rewrite_proved_by_explicit_projection(self, register_rule):
        register_rule("close_status", {"status": "'CLOSED'"})

        sql = _compile(_single_table("""    fields:
      - [status, status]
business_rules:
  - close_status
keep_all_columns: true
"""))

        assert "SELECT * EXCEPT (`status`), 'CLOSED' AS `status`" in sql

    def test_rewrite_proved_by_column_resolver_uses_resolved_fqn(self, register_rule):
        register_rule("rewrite_amount", {"amount": "amount * 2"})
        resolved = []

        def resolve_columns(fqn):
            resolved.append(fqn)
            return ["amount", "status"]

        sql = _compile(
            _single_table("business_rules:\n  - rewrite_amount\nkeep_all_columns: true\n"),
            resolve_table=lambda name: f"catalog.sandbox.{name}",
            resolve_columns=resolve_columns,
        )

        assert resolved == ["catalog.sandbox.silver.orders"]
        assert "SELECT * EXCEPT (`amount`), amount * 2 AS `amount`" in sql

    def test_new_column_proved_absent_does_not_use_except(self, register_rule):
        register_rule("double_amount", {"doubled": "amount * 2"})

        sql = _compile(
            _single_table("business_rules:\n  - double_amount\nkeep_all_columns: true\n"),
            resolve_columns=lambda _fqn: ["amount"],
        )

        assert "SELECT *, amount * 2 AS `doubled`" in sql
        assert "EXCEPT" not in sql

    def test_unknown_target_presence_is_refused_by_rule_and_column(self, register_rule):
        register_rule("uncertain_rule", {"derived": "amount * 2"})

        with pytest.raises(SqlCompilationError) as exc_info:
            _compile(
                _single_table(
                    "business_rules:\n  - uncertain_rule\nkeep_all_columns: true\n"
                )
            )

        message = str(exc_info.value)
        assert "uncertain_rule" in message
        assert "derived" in message
        assert "'fields'" in message
        assert "resolve_columns" in message

    def test_column_resolver_is_propagated_to_partials(self, register_rule):
        register_rule("derive_child", {"derived": "amount * 2"})
        resolved = []
        child = {
            "tables": [{"name": "silver.lines", "alias": "lines"}],
            "business_rules": ["derive_child"],
            "select_final": [["derived", "derived"]],
        }
        parsed = parse_to_ir(
            {
                "partials": [{"alias": "child", "schema": child}],
                "select_final": [["derived", "derived"]],
            }
        )

        sql = compile_select(
            parsed,
            resolve_table=lambda name: f"catalog.sandbox.{name}",
            resolve_columns=lambda fqn: resolved.append(fqn) or ["amount"],
        )

        assert resolved == ["catalog.sandbox.silver.lines"]
        assert "amount * 2 AS `derived`" in sql

    def test_select_final_can_reference_rule_output(self, register_rule):
        register_rule("double_amount", {"doubled": "amount * 2"})

        sql = _compile(
            _single_table("""business_rules:
  - double_amount
select_final:
  - [doubled, final_amount]
"""),
            resolve_columns=lambda _fqn: ["amount"],
        )

        assert "SELECT `doubled` AS `final_amount`\nFROM (" in sql
        assert "amount * 2 AS `doubled`" in sql

    def test_aggregate_group_by_and_having_can_reference_rule_outputs(self, register_rule):
        register_rule("derive_bucket", {"bucket": "UPPER(country)"})

        sql = _compile(
            _single_table("""business_rules:
  - derive_bucket
aggregate:
  group_by: [bucket]
  measures:
    - [amount, total_amount, sum]
  having:
    - "total_amount:greater_than:100"
"""),
            resolve_columns=lambda _fqn: ["amount", "country"],
        )

        assert "UPPER(country) AS `bucket`" in sql
        assert "GROUP BY `bucket`" in sql
        assert "HAVING `total_amount` > '100'" in sql

    def test_add_columns_can_reference_rule_output(self, register_rule):
        register_rule("derive_amount", {"doubled": "amount * 2"})

        sql = _compile(
            _single_table("""business_rules:
  - derive_amount
add_columns:
  - [doubled, rounded, [round:0]]
aggregate:
  group_by: [rounded]
  measures:
    - [amount, total_amount, sum]
"""),
            resolve_columns=lambda _fqn: ["amount"],
        )

        assert "SELECT *, ROUND(`doubled`, 0) AS `rounded`" in sql
        assert "amount * 2 AS `doubled`" in sql

    def test_allow_raw_sql_does_not_govern_registered_sql_rules(self, register_rule):
        register_rule("trusted_sql", {"doubled": "amount * 2"})

        sql = _compile(
            _single_table("business_rules:\n  - trusted_sql\nkeep_all_columns: true\n"),
            allow_raw_sql=False,
            resolve_columns=lambda _fqn: ["amount"],
        )

        assert "amount * 2 AS `doubled`" in sql


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

    def test_unknown_rule_is_rejected_by_name(self):
        with pytest.raises(SqlCompilationError, match="missing_portable_rule"):
            _compile(
                _single_table(
                    "business_rules:\n  - missing_portable_rule\nkeep_all_columns: true\n"
                )
            )

    @pytest.mark.parametrize("kind", ["projection", "transform"])
    def test_python_rule_kinds_keep_current_refusal(self, register_rule, kind):
        name = f"python_{kind}"
        register_rule(name, {}, kind=kind)

        with pytest.raises(SqlCompilationError) as exc_info:
            _compile(
                _single_table(f"business_rules:\n  - {name}\nkeep_all_columns: true\n")
            )

        assert str(exc_info.value) == (
            f"Python business_rules ['{name}'] cannot be compiled to SQL. "
            "Materialize them upstream in a silver table, then join/aggregate that table here."
        )

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


class TestDuckDBExecution:
    def test_sql_rule_and_select_final_produce_expected_values(self, register_rule):
        import duckdb

        from skifer.core.dialect import transpile

        register_rule(
            "portable_total",
            {"total": "amount * quantity", "label": "UPPER(name)"},
        )
        connection = duckdb.connect()
        connection.execute(
            "CREATE TABLE raw_lines (name VARCHAR, amount DOUBLE, quantity INTEGER)"
        )
        connection.executemany(
            "INSERT INTO raw_lines VALUES (?, ?, ?)",
            [("alpha", 2.5, 4), ("beta", 3.0, 2)],
        )
        parsed = parse_to_ir(
            {
                "tables": [{"name": "raw_lines", "alias": "lines"}],
                "business_rules": ["portable_total"],
                "select_final": [
                    ["name", "name"],
                    ["label", "label"],
                    ["total", "total"],
                ],
            }
        )

        sql = transpile(
            compile_select(
                parsed,
                resolve_columns=lambda _fqn: ["name", "amount", "quantity"],
            ),
            target="duckdb",
        )
        rows = connection.execute(sql).fetchall()

        assert rows == [("alpha", "ALPHA", 10.0), ("beta", "BETA", 6.0)]

    def test_explicit_projection_rewrite_has_one_output_column(self, register_rule):
        import duckdb

        from skifer.core.dialect import transpile

        register_rule("close_status", {"status": "'CLOSED'"})
        connection = duckdb.connect()
        connection.execute("CREATE TABLE raw_status (status VARCHAR)")
        connection.executemany(
            "INSERT INTO raw_status VALUES (?)", [("OPEN",), ("PENDING",)]
        )
        parsed = parse_to_ir(
            {
                "tables": [
                    {
                        "name": "raw_status",
                        "alias": "source",
                        "fields": [["status", "status"]],
                    }
                ],
                "business_rules": ["close_status"],
                "keep_all_columns": True,
            }
        )

        cursor = connection.execute(transpile(compile_select(parsed), target="duckdb"))

        assert [column[0] for column in cursor.description] == ["status"]
        assert cursor.fetchall() == [("CLOSED",), ("CLOSED",)]

    def test_partial_compiled_sql_returns_expected_rows(self):
        import duckdb

        from skifer.core.dialect import transpile

        connection = duckdb.connect()
        connection.execute("CREATE TABLE raw_orders (order_id INTEGER, status VARCHAR)")
        connection.executemany(
            "INSERT INTO raw_orders VALUES (?, ?)",
            [(1, "DONE"), (2, "PENDING"), (3, "DONE")],
        )
        child = {
            "tables": [
                {
                    "name": "raw_orders",
                    "alias": "orders",
                    "filter": [
                        {"column": "status", "operator": "equals", "value": "DONE"}
                    ],
                }
            ],
            "select_final": [["order_id", "order_id"]],
        }
        parsed = parse_to_ir(
            {
                "partials": [
                    {
                        "alias": "done_orders",
                        "resolved_path": "/schemas/done_orders.yaml",
                        "schema": child,
                    }
                ],
                "select_final": [["order_id", "order_id"]],
            }
        )

        sql = transpile(compile_select(parsed), target="duckdb")
        rows = connection.execute(sql).fetchall()

        assert sorted(rows) == [(1,), (3,)]

    def test_drop_duplicates_compiled_sql_keeps_one_original_row_per_key(self):
        import duckdb

        from skifer.core.dialect import transpile

        original_rows = [(1, "first"), (1, "second"), (2, "only")]
        connection = duckdb.connect()
        connection.execute("CREATE TABLE raw_events (event_key INTEGER, value VARCHAR)")
        connection.executemany("INSERT INTO raw_events VALUES (?, ?)", original_rows)
        parsed = parse_to_ir(
            {
                "tables": [
                    {
                        "name": "raw_events",
                        "alias": "events",
                        "quality_checks": {"drop_duplicates_on": ["event_key"]},
                    }
                ],
                "keep_all_columns": True,
            }
        )

        pivot_sql = compile_select(parsed, persisted_definition=False)
        rows = connection.execute(transpile(pivot_sql, target="duckdb")).fetchall()

        assert len(rows) == 2
        assert {row[0] for row in rows} == {1, 2}
        assert all(row in original_rows for row in rows)


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
