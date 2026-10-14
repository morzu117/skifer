"""
Spark session factory.
Auto-detects the execution context and returns an appropriate SparkSession.

Priority:
  1. Active session       — Databricks notebook / cluster
  2. Connect serverless   — no cluster (Model Serving, Databricks Apps) via DATABRICKS_SERVERLESS_COMPUTE_ID
  3. Databricks Connect   — IDE (PyCharm, VS Code) + remote cluster via env vars
  4. Local PySpark        — local[*] + Delta Lake + Derby embedded metastore (no install needed)
"""
import os
import sys
from pyspark.sql import SparkSession


def get_spark_session(
    app_name: str = "Skifer",
    warehouse_dir: str = None,
) -> tuple:
    """
    Returns a (SparkSession, mode) tuple.

    mode is one of:
      - "databricks_notebook"  — active session detected (running on Databricks)
      - "databricks_connect"   — Databricks Connect v2, serverless or cluster, via DATABRICKS_* env vars
      - "local"                — local PySpark with Delta Lake (fallback / local dev)

    Args:
        app_name (str): Application name for the Spark UI (local mode only).
        warehouse_dir (str): Path to the local Spark warehouse (local mode only).
                             Defaults to .spark-warehouse/ in the current working directory.
    """
    # ------------------------------------------------------------------
    # 1. Active session — running inside a Databricks notebook or cluster
    # ------------------------------------------------------------------
    try:
        session = SparkSession.getActiveSession()
        if session is not None:
            print("   [SparkFactory] Active Databricks session detected.")
            return session, "databricks_notebook"
    except Exception:
        pass

    # ------------------------------------------------------------------
    # 2. Databricks Connect serverless — no cluster at all
    #    Requires: DATABRICKS_SERVERLESS_COMPUTE_ID (e.g. "auto") + SDK default auth
    #    (DATABRICKS_HOST with a token or a service principal). Never falls back to
    #    local: where this is set (Model Serving, Apps) there is no Java to run local[*],
    #    and the fallback would hide the real error behind a JVM one.
    # ------------------------------------------------------------------
    if os.getenv("DATABRICKS_SERVERLESS_COMPUTE_ID"):
        try:
            from databricks.connect import DatabricksSession
        except ImportError as exc:
            raise ImportError(
                "DATABRICKS_SERVERLESS_COMPUTE_ID is set but databricks-connect is not "
                "installed. Install it with: pip install 'skifer[serving]'"
            ) from exc
        try:
            session = DatabricksSession.builder.serverless().getOrCreate()
        except Exception as exc:
            raise RuntimeError(
                "Databricks Connect serverless session failed (DATABRICKS_SERVERLESS_COMPUTE_ID "
                "is set). Check DATABRICKS_HOST and its credentials (token or service "
                "principal), and that serverless compute is enabled on the workspace. "
                f"Cause: {type(exc).__name__}: {exc}"
            ) from exc
        print("   [SparkFactory] Databricks Connect serverless session initialized.")
        return session, "databricks_connect"

    # ------------------------------------------------------------------
    # 3. Databricks Connect v2 — IDE + remote cluster
    #    Requires: DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_CLUSTER_ID
    # ------------------------------------------------------------------
    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")
    cluster_id = os.getenv("DATABRICKS_CLUSTER_ID")

    if host and token and cluster_id:
        try:
            from databricks.connect import DatabricksSession
            from databricks.sdk.core import Config

            conf = Config(host=host, token=token, cluster_id=cluster_id)
            session = DatabricksSession.builder.sdkConfig(conf).getOrCreate()
            print("   [SparkFactory] Databricks Connect v2 session initialized.")
            return session, "databricks_connect"
        except Exception as e:
            print(f"   [SparkFactory] Databricks Connect failed ({e}). Falling back to local.")

    # ------------------------------------------------------------------
    # 4. Local PySpark + Delta Lake
    #    Uses Derby as the embedded Hive metastore — no installation required.
    #    Tables written via saveAsTable() are stored under warehouse_dir.
    # ------------------------------------------------------------------
    _warehouse = warehouse_dir or os.path.join(os.getcwd(), ".spark-warehouse")

    # Resolve the delta-spark JAR coordinates to add to the classpath.
    # Using spark.jars.packages is the recommended way to load Delta Lake locally
    # because it ensures the JARs (including the extension classes) are on the
    # driver classpath before the SparkSession is created.
    try:
        import delta as _delta
        _delta_version = _delta.__version__
        import pyspark as _pyspark
        _spark_short = ".".join(_pyspark.__version__.split(".")[:2])  # e.g. "4.1"
        # Scala 2.13 is used for Spark 3.3+ on Python 3.11+; safe default for 4.x
        _scala = "2.13"
        _delta_package = f"io.delta:delta-spark_{_spark_short}_{_scala}:{_delta_version}"
    except Exception:
        _delta_package = None

    # Pin the workers to the interpreter that is running, before the session starts.
    # Otherwise Spark launches whatever `python3` the PATH offers, and a system Python
    # of a different minor version fails the job with PYTHON_VERSION_MISMATCH — an
    # error that says nothing about the virtualenv the user is actually in.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    builder = SparkSession.builder.appName(app_name).master("local[*]")

    if _delta_package:
        builder = (
            builder
            .config("spark.jars.packages", _delta_package)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )
    else:
        print("   [SparkFactory] WARNING: delta-spark not found. Delta tables will not be available.")

    session = (
        builder
        .config("spark.sql.warehouse.dir", _warehouse)
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )

    # Silence noisy loggers for local dev
    session.sparkContext.setLogLevel("WARN")

    print("   [SparkFactory] Local PySpark + Delta Lake session initialized.")
    print(f"   [SparkFactory] Warehouse: {_warehouse}")
    return session, "local"