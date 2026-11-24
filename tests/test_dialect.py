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
