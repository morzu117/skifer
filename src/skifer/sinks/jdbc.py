"""
JDBC sinks for external analytical databases.
"""
from __future__ import annotations

import os

VALID_WRITE_MODES = {"overwrite", "append", "ignore", "error", "errorifexists"}


class JDBCSink:
    """Write Spark DataFrames to PostgreSQL-compatible JDBC targets."""

    REQUIRED_ENV_VARS = (
        "POSTGRES_HOST",
        "POSTGRES_DB",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
    )

    def __init__(self):
        missing = [name for name in self.REQUIRED_ENV_VARS if not os.environ.get(name)]
        if missing:
            joined = ", ".join(missing)
            raise EnvironmentError(
                f"Missing required PostgreSQL environment variables: {joined}"
            )

        self.host = os.environ["POSTGRES_HOST"]
        self.port = os.environ.get("POSTGRES_PORT", "5432")
        self.database = os.environ["POSTGRES_DB"]
        self.user = os.environ["POSTGRES_USER"]
        self.password = os.environ["POSTGRES_PASSWORD"]
        self.url = f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"

    def write(self, df, schema: str, table: str, mode: str = "overwrite") -> None:
        """Write to ``schema.table`` via Spark JDBC using a valid Spark save mode."""
        if mode not in VALID_WRITE_MODES:
            raise ValueError(
                f"Unsupported JDBC write mode '{mode}'. Valid modes: {sorted(VALID_WRITE_MODES)}"
            )
        dbtable = f"{schema}.{table}" if schema else table
        (
            df.write.format("jdbc")
            .option("url", self.url)
            .option("dbtable", dbtable)
            .option("user", self.user)
            .option("password", self.password)
            .option("driver", "org.postgresql.Driver")
            .mode(mode)
            .save()
        )
