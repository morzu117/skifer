"""Compile one pipeline for two targets and execute DuckDB SQL over local Parquet.

    python examples/26_portable_sql/run.py
"""

from collections import Counter
from decimal import Decimal
from pathlib import Path
import shutil
import tempfile

import duckdb

from skifer.core.ir import parse_to_ir
from skifer.core.schema_loader import load_schema
from skifer.core.sql_compiler import SqlCompilationError, capability_report, compile_select


EXAMPLE_DIR = Path(__file__).resolve().parent
WORK = Path(tempfile.mkdtemp(prefix="skifer_ex26_"))
try:
    parquet_path = str(WORK / "orders.parquet").replace("'", "''")
    with duckdb.connect() as writer:
        writer.execute(f"""
            COPY (
                SELECT * FROM (VALUES
                    (1, 10, 'fr', 'DONE', 100.00),
                    (2, 10, 'fr', 'DONE', 50.00),
                    (3, 20, 'de', 'DONE', 20.00),
                    (4, 21, 'de', 'DONE', 30.00),
                    (5, 30, 'it', 'DONE', 10.00),
                    (6, 31, 'it', 'DONE', 15.00),
                    (7, 40, 'es', 'DONE', 70.00),
                    (8, 11, 'fr', 'PENDING', 999.00),
                    (9, 12, 'fr', 'DONE', NULL)
                ) AS orders(order_id, customer_id, country, status, amount)
            ) TO '{parquet_path}' (FORMAT PARQUET)
        """)

    with duckdb.connect() as connection:
        connection.execute("CREATE SCHEMA silver")
        connection.execute(
            f"CREATE VIEW silver.orders AS SELECT * FROM read_parquet('{parquet_path}')"
        )
        parsed = parse_to_ir(load_schema(str(EXAMPLE_DIR / "orders_by_country.yaml")))
        sql_databricks = compile_select(parsed, dialect="databricks")
        sql_duckdb = compile_select(parsed, dialect="duckdb")
        print("Databricks SQL:")
        print(sql_databricks)
        print("\nDuckDB SQL:")
        print(sql_duckdb)
        print("\nDuckDB capabilities:")
        report = capability_report(parsed, "duckdb")
        for key, status in report.items():
            reason = f" — {status.reason}" if status.kind == "conditional" else ""
            print(f"  {key}: {status.kind}{reason}")
        assert all(status.kind != "refused" for status in report.values()), report
        counts = Counter(status.kind for status in report.values())
        print("  Totals: " + ", ".join(
            f"{kind}={counts[kind]}" for kind in ("supported", "conditional", "refused")
        ))
        print("\nDuckDB result:")
        rows = sorted(connection.execute(sql_duckdb).fetchall())
        assert rows == [
            ("DE", Decimal("50.00"), 2, 2),
            ("FR", Decimal("150.00"), 2, 1),
            ("IT", Decimal("25.00"), 2, 2),
        ], rows
        for row in rows:
            print(row)

        refused = parse_to_ir(load_schema(str(EXAMPLE_DIR / "refused_expr.yaml")))
        compile_select(refused, dialect="databricks")
        try:
            compile_select(refused, dialect="duckdb")
        except SqlCompilationError as exc:
            assert "op:expr" in str(exc)
            print(f"\nRefused for duckdb: {exc}")
        else:
            raise SystemExit("duckdb should have refused op:expr")
finally:
    shutil.rmtree(WORK)
