"""Compile a materialized-view schema to SQL and DDL, without Spark.

    python examples/08_materialized_view/run.py
"""

from pathlib import Path

from skifer import load_schema
from skifer.core.ir import parse_to_ir
from skifer.core.sql_compiler import compile_materialized_view_ddl, compile_select, definition_hash

EXAMPLE_DIR = Path(__file__).parent

schema = load_schema(str(EXAMPLE_DIR / "revenue_by_country.yaml"))
options = schema["materialization"]
select_sql = compile_select(parse_to_ir(schema))
print("Compiled SELECT:")
print(select_sql)

# The hash covers the SELECT and the defining options: same YAML → REFRESH, changed → CREATE OR REPLACE.
same_hash = definition_hash(compile_select(parse_to_ir(load_schema(str(EXAMPLE_DIR / "revenue_by_country.yaml")))), options)
first_hash = definition_hash(select_sql, options)
changed_hash = definition_hash(select_sql, {**options, "schedule": "EVERY 12 HOURS"})
print("\nDefinition hashes:")
print(f"same schema:      {first_hash[:12]}... == {same_hash[:12]}...")
print(f"changed schedule: {changed_hash[:12]}...")
print(f"schedule changed hash: {changed_hash != first_hash}")

print("\nCREATE MATERIALIZED VIEW DDL:")
print(compile_materialized_view_ddl("gold.revenue_by_country", select_sql, options, first_hash))

try:
    load_schema(str(EXAMPLE_DIR / "refused_python_rule.yaml"))
except ValueError as exc:
    print(f"\nRefused schema: {exc}")
