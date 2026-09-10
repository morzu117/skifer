"""Compile a materialized-view schema to SQL without starting Spark.

    python examples/08_materialized_view/run.py

Both compilation and definition hashing are pure Python. A materialized view
short-circuits the DataFrame pipeline because its product is a SQL definition.
"""

from copy import deepcopy
from pathlib import Path

from skifer import load_schema
from skifer.core.ir import parse_to_ir
from skifer.core.sql_compiler import (
    compile_materialized_view_ddl,
    compile_select,
    definition_hash,
)

EXAMPLE_DIR = Path(__file__).parent


def main() -> None:
    schema_path = EXAMPLE_DIR / "revenue_by_country.yaml"
    schema = load_schema(str(schema_path))
    select_sql = compile_select(parse_to_ir(schema))

    # Reload and compile independently: identical definitions must hash equally.
    same_schema = load_schema(str(schema_path))
    same_select = compile_select(parse_to_ir(same_schema))
    first_hash = definition_hash(select_sql, schema["materialization"])
    same_hash = definition_hash(same_select, same_schema["materialization"])

    changed_options = deepcopy(schema["materialization"])
    changed_options["schedule"] = "EVERY 12 HOURS"
    changed_hash = definition_hash(select_sql, changed_options)

    print("Compiled SELECT:")
    print(select_sql)
    print("\nDefinition hashes:")
    print(f"same schema:      {first_hash[:12]}... == {same_hash[:12]}...")
    print(f"changed schedule: {changed_hash[:12]}...")
    print(f"schedule changed hash: {changed_hash != first_hash}")

    ddl = compile_materialized_view_ddl(
        "gold.revenue_by_country",
        select_sql,
        schema["materialization"],
        first_hash,
    )
    print("\nCREATE MATERIALIZED VIEW DDL:")
    print(ddl)

    try:
        load_schema(str(EXAMPLE_DIR / "refused_python_rule.yaml"))
    except ValueError as exc:
        print(f"\nRefused schema: {exc}")


if __name__ == "__main__":
    main()
