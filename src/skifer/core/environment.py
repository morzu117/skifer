"""
Environment module — Databricks environment detection helpers.
Extracted from core.py as standalone functions for testability.
"""
import os
import getpass
import logging

logger = logging.getLogger(__name__)

#: Actionable hint appended to errors raised when the SDK is required but absent.
#: The extra is unnecessary on a Databricks runtime, where the SDK ships with DBR.
DATABRICKS_SDK_INSTALL_HINT = (
    "The databricks-sdk package is required for Databricks workspace calls. "
    "Install it with: pip install 'skifer[databricks]' "
    "(unnecessary on a Databricks runtime, where the SDK is pre-installed)."
)


def is_databricks_sdk_available() -> bool:
    """True when ``databricks.sdk`` can be imported (lazy — never imported at module level)."""
    try:
        import databricks.sdk  # noqa: F401
        return True
    except ImportError:
        return False


def get_workspace_client(host=None, token=None):
    """
    Creates a WorkspaceClient if Databricks credentials are available, else returns None.

    Args:
        host (str, optional): Databricks workspace host. Defaults to DATABRICKS_HOST env var.
        token (str, optional): Databricks token. Defaults to DATABRICKS_TOKEN env var.

    Returns:
        WorkspaceClient | None
    """
    host = host or os.getenv("DATABRICKS_HOST")
    token = token or os.getenv("DATABRICKS_TOKEN")
    if not (host and token):
        return None
    try:
        from databricks.sdk import WorkspaceClient
    except ImportError:
        # Distinguished from a credentials miss so callers can say which is wrong.
        logger.warning("[Databricks] %s", DATABRICKS_SDK_INSTALL_HINT)
        return None
    try:
        return WorkspaceClient(host=host, token=token)
    except Exception as exc:  # noqa: BLE001 — bad credentials must not break init
        logger.warning("[Databricks] Could not create a WorkspaceClient: %s", exc)
        return None


def patch_connect_debugging(spark):
    """
    Workaround for a gRPC UserContext bug in Databricks Connect v2.
    Pre-populates the PySpark debugging cache to False to avoid failing gRPC calls.

    Args:
        spark: SparkSession instance.
    """
    try:
        spark.conf.get("spark.python.sql.dataFrameDebugging.enabled", "false")
    except Exception:
        try:
            import pyspark.errors.utils as _pu
            if getattr(_pu, "_enable_debugging_cache", None) is None:
                _pu._enable_debugging_cache = False
                print("   [Init] ⚙️ Applied PySpark Connect debugging cache patch (UserContext workaround).")
        except Exception:
            pass


def patch_connect_user_context(spark, workspace_client_fn=None):
    """
    Patches SparkConnectClient._user_id when it is empty or missing.

    Args:
        spark: SparkSession instance.
        workspace_client_fn (callable, optional): Callable that returns a WorkspaceClient.
                                                   Defaults to get_workspace_client().
    """
    try:
        client = getattr(spark, 'client', None)
        if client is None:
            return
        if getattr(client, '_user_id', None):
            return
        if workspace_client_fn is None:
            workspace_client_fn = get_workspace_client
        w = workspace_client_fn()
        if w:
            email = w.current_user.me().user_name
            if email:
                client._user_id = email
                print(f"   [Init] ⚙️ Applied UserContext patch: user_id injected ('{email}').")
    except Exception:
        pass


def is_running_as_job(dbutils=None, env_vars=None):
    """
    Detects if the script is launched by a Databricks Job.

    Args:
        dbutils: Databricks DBUtils instance (optional).
        env_vars (dict, optional): Environment variables to check. Defaults to os.environ.

    Returns:
        bool: True if running as a Databricks Job, False otherwise.
    """
    env = env_vars if env_vars is not None else os.environ

    # 1. Widget parameter
    if dbutils is not None:
        try:
            run_mode_param = dbutils.widgets.get("SKIFER_RUN_MODE")
            if run_mode_param and run_mode_param.lower() == "job":
                return True
        except Exception:
            pass

    # 2. Environment variable
    if env.get("SKIFER_RUN_MODE", "").lower() == "job":
        return True

    # 3. Databricks notebook context tags
    if dbutils is not None:
        try:
            context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
            job_id = context.tags().get("jobId")
            if job_id is not None and str(job_id) != "None":
                return True
        except Exception:
            pass

    return False


def get_clean_username(spark=None, config=None, dbutils=None, workspace_client_fn=None,
                       find_file_fn=None):
    """
    Gets the current user, trying multiple strategies in order.

    Args:
        spark: SparkSession instance (optional).
        config (dict, optional): Loaded config dict (used to find cache file).
        dbutils: DBUtils instance (optional).
        workspace_client_fn (callable, optional): Callable returning WorkspaceClient.
        find_file_fn (callable, optional): Callable to find a file upwards from cwd.
                                            Signature: (filename) -> str|None.

    Returns:
        str: Cleaned username.
    """
    def _clean(email):
        return email.split('@')[0].replace('.', '_').replace('-', '_').lower()

    def _save_to_file(email, suffix):
        if find_file_fn is None:
            return
        try:
            config_dir = os.path.dirname(find_file_fn("config.yaml") or "")
            if config_dir:
                user_file = os.path.join(config_dir, ".skifer_user")
                with open(user_file, "w") as f:
                    f.write("# Auto-generated by Skifer. Add to .gitignore.\n")
                    f.write(f"user_email={email}\n")
                    f.write(f"suffix={suffix}\n")
        except Exception:
            pass

    # Strategy 0: Read from .skifer_user file
    if find_file_fn is not None and config is not None:
        try:
            config_dir = os.path.dirname(find_file_fn("config.yaml") or "")
            if config_dir:
                user_file = os.path.join(config_dir, ".skifer_user")
                if os.path.exists(user_file):
                    with open(user_file) as f:
                        for line in f:
                            if line.startswith("suffix="):
                                cached = line.split("=", 1)[1].strip()
                                if cached:
                                    return cached
        except Exception:
            pass

    result = None
    email = None

    # Strategy 1: Spark SQL
    if spark is not None:
        try:
            rows = spark.sql("SELECT current_user()").collect()
            if rows and rows[0][0]:
                email = rows[0][0]
                result = _clean(email)
        except Exception:
            pass

    # Strategy 2: Databricks SDK REST API
    if not result:
        try:
            fn = workspace_client_fn or get_workspace_client
            w = fn()
            if w:
                email = w.current_user.me().user_name
                if email:
                    result = _clean(email)
        except Exception:
            pass

    # Strategy 3: DBUtils notebook context tags
    if not result and dbutils is not None:
        try:
            context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
            email = context.tags().apply('user')
            result = _clean(email)
        except Exception:
            pass

    # Strategy 4: OS user
    if not result:
        try:
            email = getpass.getuser()
            result = _clean(email)
        except Exception:
            result = "unknown_user"

    if result and email:
        _save_to_file(email, result)

    return result

