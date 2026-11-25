"""Pure rendering checks; execution/parity belong to separate tests."""

import sys

import pytest
import sqlglot

from skifer.core.dialect import DialectError, render


SQL = """SELECT `o`.`country` AS `country`,
CASE WHEN `o`.`amount` BETWEEN 1 AND 10 THEN 'small' ELSE 'big' END AS `band`,
COUNT(DISTINCT `o`.`id`) AS `orders`
FROM `orders` AS `o` LEFT JOIN `customers` AS `c` ON `o`.`id` = `c`.`id`
WHERE `o`.`country` LIKE 'F%' AND `c`.`region` IN ('EU', 'US')
GROUP BY `o`.`country`, CASE WHEN `o`.`amount` BETWEEN 1 AND 10 THEN 'small' ELSE 'big' END
HAVING COUNT(DISTINCT `o`.`id`) > 0"""


def test_databricks_is_byte_identity():
    sql = " \n" + SQL + ";\n"
    assert render(sql, "databricks") == sql


@pytest.mark.parametrize("target", ["duckdb", "snowflake"])
def test_render_quoted_select(target):
    out = render(SQL, target)
    assert '`' not in out
    assert '"country"' in out
    parsed = sqlglot.parse_one(out, read=target)
    assert parsed is not None
    for fragment in ("LIKE", " IN ", "BETWEEN", "CASE WHEN", "COUNT(DISTINCT", "GROUP BY", "HAVING", "LEFT JOIN"):
        assert fragment in out


@pytest.mark.parametrize("target", ["duckdb", "snowflake"])
def test_doubled_quote_literal_preserves_value(target):
    out = render("SELECT 'O''Brien' AS `name`", target)
    parsed = sqlglot.parse_one(out, read=target)
    assert parsed.find(sqlglot.exp.Literal).this == "O'Brien"


def test_unknown_target():
    with pytest.raises(DialectError, match="oracle"):
        render("SELECT 1", "oracle")


@pytest.mark.parametrize("target", ["duckdb", "snowflake"])
def test_missing_sqlglot(monkeypatch, target):
    monkeypatch.setitem(sys.modules, "sqlglot", None)
    with pytest.raises(DialectError, match=r"skifer\[dialects\]"):
        render("SELECT 1", target)
    assert render("SELECT 1", "databricks") == "SELECT 1"


@pytest.mark.parametrize("target", ["duckdb", "snowflake"])
def test_two_statements_refused(target):
    with pytest.raises(DialectError, match="exactly one statement"):
        render("SELECT 1; SELECT 2", target)


@pytest.mark.parametrize("target", ["duckdb", "snowflake"])
@pytest.mark.parametrize("literal,value", [
    ("''", ""), ("''''", "'"), ("'a''b'", "a'b"), ("'''x'", "'x"),
])
def test_literal_boundaries_preserve_value(target, literal, value):
    out = render(f"SELECT {literal} AS a", target)
    assert sqlglot.parse_one(out, read=target).find(sqlglot.exp.Literal).this == value


@pytest.mark.parametrize("target", ["duckdb", "snowflake"])
def test_quote_in_backticked_identifier_is_not_a_literal(target):
    out = render("SELECT `prix d'achat` AS a FROM `t` WHERE `x` = 'y'", target)
    assert '"prix d\'achat"' in out
    parsed = sqlglot.parse_one(out, read=target)
    assert parsed.find(sqlglot.exp.Column).name == "prix d'achat"
    assert parsed.find(sqlglot.exp.Literal).this == "y"


@pytest.mark.parametrize("target", ["duckdb", "snowflake"])
def test_trailing_backslash_literal(target):
    from skifer.core.sql_compiler import sql_literal

    value = "a\\"
    out = render(f"SELECT {sql_literal(value)} AS a", target)
    assert sqlglot.parse_one(out, read=target).find(sqlglot.exp.Literal).this == value


@pytest.mark.parametrize("target", ["duckdb", "snowflake"])
def test_invalid_sql_raises_dialect_error(target):
    from sqlglot.errors import SqlglotError

    with pytest.raises(DialectError) as raised:
        render("SELECT FROM WHERE", target)
    assert isinstance(raised.value.__cause__, SqlglotError)


@pytest.mark.parametrize("target", ["duckdb", "snowflake"])
def test_unsupported_rendering_raises_with_cause(monkeypatch, target):
    from sqlglot.errors import ErrorLevel, UnsupportedError

    error = UnsupportedError("x")

    def unsupported(*args, **kwargs):
        assert kwargs["unsupported_level"] is ErrorLevel.RAISE
        raise error

    monkeypatch.setattr(sqlglot, "transpile", unsupported)
    with pytest.raises(DialectError) as raised:
        render("SELECT 1", target)
    assert raised.value.__cause__ is error


def _quoted_snowflake_ast(sql):
    tree = sqlglot.parse_one(sql, read="snowflake")
    identifiers = list(tree.find_all(sqlglot.exp.Identifier))
    assert identifiers
    assert all(identifier.args.get("quoted") for identifier in identifiers)
    return tree


def test_snowflake_preserves_authored_case_and_other_targets():
    sql = "SELECT UPPER(country) AS Country, amount * 2 AS Doubled FROM Db.Sales.Orders"
    assert render(sql, "databricks") == sql
    assert render(sql, "duckdb") == sql
    out = render(sql, "snowflake")
    assert out == (
        'SELECT UPPER("country") AS "Country", "amount" * 2 AS "Doubled" '
        'FROM "Db"."Sales"."Orders"'
    )
    _quoted_snowflake_ast(out)


def test_snowflake_pipeline_identifiers():
    from skifer.core.ir import parse_to_ir
    from skifer.core.schema_loader import parse_schema
    from skifer.core.sql_compiler import compile_select

    parsed = parse_to_ir(parse_schema("""
        tables: [{name: Db.Sales.Orders, alias: Ord}]
        select_final: [[country, Country, [upper]]]
        where: ["amount:greater_than:2"]
    """))
    tree = _quoted_snowflake_ast(compile_select(parsed, dialect="snowflake"))
    assert {table.name for table in tree.find_all(sqlglot.exp.Table)} == {"Orders", "Ord"}
    assert tree.find(sqlglot.exp.Upper).this.name == "country"


@pytest.mark.parametrize("expression,node,column", [
    ("UPPER(country)", sqlglot.exp.Upper, "country"),
    ("amount * 2", sqlglot.exp.Mul, "amount"),
])
def test_snowflake_semantic_expressions(expression, node, column):
    from skifer.semantic.resolver import QueryResolver, SemanticQuery

    model = {
        "key": "orders", "table": "Db.Sales.Orders",
        "dimensions": [{"name": "Computed", "sql": expression, "type": "string"}],
        "metrics": [{"name": "Revenue", "sql": "amount", "type": "sum"}],
    }
    query = SemanticQuery("orders", metrics=["Revenue"], group_by=["Computed"],
                          filters=[{"column": "Computed", "operator": "eq", "value": "FR"}])
    out = QueryResolver(dialect="snowflake").resolve(query, model, None).full_sql
    tree = _quoted_snowflake_ast(out)
    assert tree.find(node).find(sqlglot.exp.Column).name == column
    assert tree.find(sqlglot.exp.Table).name == "Orders"
    assert tree.find(sqlglot.exp.Where) is not None
