"""Canonical rows and paired runtime types, shared by all three execution paths."""

from datetime import date
from decimal import Decimal

from pyspark.sql.types import DateType, DecimalType, DoubleType, IntegerType, StringType


TABLES = {
    "orders": (
        (
            ("order_id", IntegerType(), "INTEGER"),
            ("customer_id", IntegerType(), "INTEGER"),
            ("country", StringType(), "VARCHAR"),
            ("status", StringType(), "VARCHAR"),
            ("amount", DecimalType(12, 2), "DECIMAL(12,2)"),
            ("unit_price", DoubleType(), "DOUBLE"),
            ("order_date", DateType(), "DATE"),
            ("order_date_text", StringType(), "VARCHAR"),
            ("order_date_fr", StringType(), "VARCHAR"),
            ("sku", StringType(), "VARCHAR"),
            ("note", StringType(), "VARCHAR"),
        ),
        [
            (
                i,
                (None, 1, 2, 3, 4, 5, 6, 7, 8, 99)[(i - 1) % 10],
                ("FR", "DE", "IT", "ES", None)[(i - 1) % 5],
                ("DONE", "PENDING", "CANCELLED")[(i - 1) % 3],
                (None, Decimal("0.00"), Decimal("-2.50"), Decimal("2.50"),
                 Decimal("10.25"), Decimal("100.00"))[(i - 1) % 6],
                (2.675, 0.5, 1.5, 2.5, -2.5, 2.7)[(i - 1) % 6],
                date(2026, 1, i) if i % 7 else None,
                f"2026-01-{i:02d}" if i % 7 else None,
                f"{i:02d}/01/2026" if i % 7 else None,
                ("AB-123-X", "CD-456-Y", "EF-789-Z")[(i - 1) % 3],
                ("  Mixed Case  ", "Zoë", None, "abc", " ABC ")[(i - 1) % 5],
            )
            for i in range(1, 31)
        ],
    ),
    "customers": (
        (
            ("customer_id", IntegerType(), "INTEGER"),
            ("name", StringType(), "VARCHAR"),
            ("cust_country", StringType(), "VARCHAR"),
            ("segment", StringType(), "VARCHAR"),
        ),
        [(i, f"Customer {i}", ("FR", "DE", "IT", "ES")[i % 4],
          ("RETAIL", "BUSINESS")[i % 2]) for i in range(1, 13)],
    ),
    "countries": (
        (
            ("country", StringType(), "VARCHAR"),
            ("country_name", StringType(), "VARCHAR"),
            ("region", StringType(), "VARCHAR"),
        ),
        [("FR", "France", "EU"), ("DE", "Germany", "EU"),
         ("IT", "Italy", "EU"), ("ES", "Spain", "EU"),
         ("GB", "United Kingdom", "EUROPE"), ("US", "United States", "AMERICA")],
    ),
}
