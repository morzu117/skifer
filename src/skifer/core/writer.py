"""
Writer module — DataFrame write helpers and table management.
Extracted from core.py for reuse by backend implementations.
"""
import os


def _quote_namespace(namespace):
    """Quote each namespace segment independently for Spark SQL."""
    parts = [part.strip().strip("`") for part in namespace.split(".") if part.strip()]
    if not parts:
        raise ValueError("Schema name cannot be empty.")
    return ".".join(f"`{part}`" for part in parts)


def write_dataframe(df, fqn, label, is_local, spark):
    """
    Writes a DataFrame to a Delta table, overwriting the schema.

    Args:
        df: PySpark DataFrame.
        fqn (str): Fully Qualified Name of the target table (backtick-quoted).
        label (str): Label to display in logging output.
        is_local (bool): Whether running in local mode.
        spark: SparkSession instance.
    """
    try:
        if df.isEmpty():
            print(f"     -> [Write] WARNING: Dataframe for {label} is empty. Skipping write.")
            return
    except Exception:
        # isEmpty() triggers collect() via gRPC on Databricks Connect v2 — skip safely.
        pass

    print(f"     -> [Write] Writing data to: {fqn} ...")

    if is_local:
        write_dataframe_local(df, fqn, spark)
    else:
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(fqn)

    print(f"     -> [Success] Table {label} written successfully.")


def write_dataframe_local(df, fqn, spark):
    """
    Local-mode write: saves as Delta files then registers the table in Derby metastore.

    Args:
        df: PySpark DataFrame.
        fqn (str): Fully Qualified Name (backtick-quoted) — e.g. `schema`.`table`
        spark: SparkSession instance.
    """
    schema_name, table_name = _parse_local_fqn(fqn)

    warehouse_dir = spark.conf.get("spark.sql.warehouse.dir")
    path = f"{warehouse_dir}/{schema_name}/{table_name}"

    df.write.format("delta").mode("overwrite").save(path)

    spark.sql(
        f"CREATE TABLE IF NOT EXISTS `{schema_name}`.`{table_name}` "
        f"USING DELTA LOCATION '{path}'"
    )


def _parse_local_fqn(fqn):
    """Parse an FQN into (schema, table), dropping any catalog part.

    Both the backtick-quoted form built by ``SparkBackend.build_fqn`` and a plain
    dotted identifier are accepted. Certified publication deliberately keeps its
    staging name unquoted — it is a stored identity, recorded in every run event —
    so a parser that only understood the quoted form made the very first staging
    write fail locally, and certified publication was unusable on the documented
    development path.
    """
    parts = [part.strip("`") for part in fqn.replace("`.`", ".").strip("`").split(".")]
    if len(parts) == 2:
        return parts[0], parts[1]
    if len(parts) == 3:
        return parts[1], parts[2]
    raise ValueError(f"Cannot parse FQN for local write: {fqn}")


def write_stream_dataframe_local(writer, fqn, spark):
    """
    Local-mode streaming write (Plan 27): starts the pre-configured
    ``writeStream`` writer at the warehouse path, waits for termination, then
    registers the table in the Derby metastore.

    Registration happens AFTER termination and only if the Delta log exists —
    a first run with zero input rows creates no ``_delta_log``, and a
    ``CREATE TABLE ... USING DELTA LOCATION`` on an empty dir would fail.

    Args:
        writer: A ``DataStreamWriter`` already configured with format/outputMode/
                checkpointLocation/trigger (the checkpoint must NOT live under
                the table path).
        fqn (str): Fully Qualified Name (backtick-quoted).
        spark: SparkSession instance.
    """
    schema_name, table_name = _parse_local_fqn(fqn)
    warehouse_dir = spark.conf.get("spark.sql.warehouse.dir")
    path = f"{warehouse_dir}/{schema_name}/{table_name}"

    query = writer.start(path)
    query.awaitTermination()

    delta_log = os.path.join(path.replace("file:", ""), "_delta_log")
    if os.path.isdir(delta_log):
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS `{schema_name}`.`{table_name}` "
            f"USING DELTA LOCATION '{path}'"
        )
    else:
        print(
            f"     -> [Stream] WARNING: no data written yet for {table_name} "
            f"(no _delta_log at {path}); metastore registration skipped."
        )


def drop_table_if_exists(fqn, spark, workspace_client_fn=None):
    """
    Drops a table if it exists, using two strategies in order:
    1. Spark SQL DROP TABLE IF EXISTS.
    2. Databricks SDK REST API (fallback when gRPC DDL fails).

    Args:
        fqn (str): Fully Qualified Name of the table (backtick-quoted).
        spark: SparkSession instance.
        workspace_client_fn (callable, optional): Callable returning WorkspaceClient.
    """
    print(f"     -> [Cleanup] Dropping table if exists: {fqn}")

    try:
        spark.sql(f"DROP TABLE IF EXISTS {fqn}")
        return
    except Exception:
        pass

    try:
        if workspace_client_fn:
            w = workspace_client_fn()
            if w:
                clean_fqn = fqn.replace("`", "")
                try:
                    w.tables.delete(clean_fqn)
                except Exception:
                    pass
                return
    except Exception:
        pass

    print(f"     -> [Cleanup] WARNING: Could not drop {fqn} via SQL or SDK. Continuing anyway.")


def ensure_schema_exists(schema, is_local, spark):
    """
    Creates the schema/database if it does not exist.

    Args:
        schema (str): Schema name (may be catalog.schema or just schema).
        is_local (bool): Whether running in local mode.
        spark: SparkSession instance.
    """
    if is_local:
        # Local spark_catalog only supports single-part namespaces — strip catalog prefix
        local_schema = schema.split(".")[-1]
        spark.sql(f"CREATE DATABASE IF NOT EXISTS `{local_schema}`")
    else:
        # Databricks / remote: quote each segment and create with full FQN
        parts = [p.strip().strip("`") for p in schema.split(".") if p.strip()]
        fqn = ".".join(f"`{p}`" for p in parts)
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {fqn}")
