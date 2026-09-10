"""
Core Engine Module. Handles the execution flow: Schema Parsing -> Processing -> Writing.
"""

from __future__ import annotations

import logging
import yaml
from dotenv import load_dotenv
import os
from contextlib import nullcontext
from uuid import uuid4

from skifer.core.rule_analyzer import RuleAnalyzer
from skifer.core.sandbox import SandboxResolver
from skifer.core.schema_loader import _find_file_upwards as _find_file_upwards_fn
from skifer.core import environment as _env
from skifer.core.context import ExecutionContext
from skifer.core.interpreter import SchemaInterpreter
from skifer.core.patterns import PipelinePatterns
from skifer.observability.tracing import (
    NoOpTracer,
    TRACE_FORMAT_VERSION,
    TraceContext,
    configured_span_scope,
    trace_context_scope,
)

logger = logging.getLogger(__name__)
_DEFAULT_TRACER = NoOpTracer()

# Module-level cache for environment detection results.
# Key: (absolute_config_path, file_mtime) → (env_upper, catalog_or_None)
# Cleared when the config file changes (mtime differs) or explicitly via
# SkiferEngine.clear_env_detection_cache().
_ENV_DETECTION_CACHE: dict[tuple, tuple[str, str | None]] = {}

# ==============================================================================
# ENGINE CLASS
# ==============================================================================


class SkiferEngine:
    """
    Main orchestration engine for Skifer.
    Handles environment setup, Spark session initialization, and pipeline execution.
    """

    # ------------------------------------------------------------------
    # Delegation properties — all backed by self._context (plan17-2.2).
    # Provides full backward compatibility: engine.env / engine.db / etc.
    # still work exactly as before, but now read/write through the
    # unified ExecutionContext instead of isolated instance attributes.
    # ------------------------------------------------------------------

    @property
    def env(self) -> str:
        return self._context.env

    @env.setter
    def env(self, value: str) -> None:
        self._context.env = value

    @property
    def db(self) -> "str | None":
        return self._context.db

    @db.setter
    def db(self, value: "str | None") -> None:
        self._context.db = value

    @property
    def config(self) -> dict:
        return self._context.config

    @config.setter
    def config(self, value: dict) -> None:
        self._context.config = value

    @property
    def current_user(self) -> str:
        return self._context.current_user

    @current_user.setter
    def current_user(self, value: str) -> None:
        self._context.current_user = value

    @property
    def is_job_execution(self) -> bool:
        return self._context.is_job_execution

    @is_job_execution.setter
    def is_job_execution(self, value: bool) -> None:
        self._context.is_job_execution = value

    @property
    def is_local(self) -> bool:
        return self._context.is_local

    @is_local.setter
    def is_local(self, value: bool) -> None:
        self._context.is_local = value

    @property
    def schema_suffix(self) -> str:
        return self._context.schema_suffix

    @schema_suffix.setter
    def schema_suffix(self, value: str) -> None:
        self._context.schema_suffix = value

    @property
    def context(self) -> ExecutionContext:
        """The unified runtime context for this engine instance."""
        return self._context

    def __init__(
        self,
        spark=None,
        config_path=None,
        force_env=None,
        monitor=None,
        certification_store=None,
    ):
        """
        Initializes the SkiferEngine.

        Args:
            spark (SparkSession, optional): An existing SparkSession. If None, the engine
                                            will attempt to auto-detect or create one.
            config_path (str, optional): Path to the config.yaml file. If None, the engine
                                         will search for it in parent directories.
            force_env (str, optional): When provided, bypass auto-detection and force a
                                       specific environment (e.g. "LOCAL"). Raises ValueError
                                       if the env name is not in config.yaml.
            monitor (DataMonitor, optional): A DataMonitor instance. When provided,
                                             quality checks are automatically run after
                                             each run_process_to_table() call.
                                             Critical failures raise DataQualityError.
            certification_store (CertificationStore, optional): Explicit store used by
                                             certified publication when a schema declares
                                             data_product. No default store is created for
                                             batch writes.
        """
        # Initialize _context unconditionally before any property access so that a
        # partially-constructed engine (init raises mid-way) never silently exposes a
        # blank context — accessing engine.env after a failed __init__ raises
        # AttributeError on _context itself (no _context attr) rather than
        # returning an empty string.
        object.__setattr__(self, "_context", ExecutionContext())

        # ======================================================================
        # 0. CHARGEMENT DES VARIABLES D'ENVIRONNEMENT (.env)
        # ======================================================================
        env_path = self._find_file_upwards(".env")

        if env_path:
            load_dotenv(env_path)
            logger.info("[Init] Environment variables loaded from %s", env_path)
        else:
            logger.debug("[Init] No .env file found in current directory or parents.")

        # ======================================================================
        # 1. BACKEND + SPARK SESSION
        # ======================================================================
        # Connect v2 patches are applied inside SparkBackend.__init__
        from skifer.spark_factory import get_spark_session
        if spark:
            self.spark = spark
            self._spark_mode = "provided"
        else:
            self.spark, self._spark_mode = get_spark_session()

        self.is_local = (self._spark_mode == "local")

        if not self.spark:
            raise RuntimeError("CRITICAL: Failed to initialize Spark Session.")

        from skifer.core.spark_backend import SparkBackend
        self._backend = SparkBackend(spark=self.spark, is_local=self.is_local)

        if self.spark:
            try:
                from pyspark.dbutils import DBUtils
                self.dbutils = DBUtils(self.spark)
            except ImportError:
                class _MockDBUtils:
                    class _MockWidgets:
                        def get(self, name, default=""):
                            return default
                        def __getattr__(self, name):
                            return lambda *args, **kwargs: None
                    widgets = _MockWidgets()
                    notebook = None
                self.dbutils = _MockDBUtils()
        else:
            self.dbutils = None

        # ======================================================================
        # 2. AUTO-DÉTECTION DE LA CONFIG
        # ======================================================================
        if config_path:
            self._load_config_from_yaml(config_path)
        else:
            # Recherche automatique basée sur l'emplacement d'exécution
            found_config_path = self._find_file_upwards("config.yaml")

            if found_config_path:
                self._load_config_from_yaml(found_config_path)
            else:
                raise FileNotFoundError(
                    "CRITICAL: 'config.yaml' not found in current directory or any parent directory. "
                    "Please check file location or pass 'config_path' explicitly."
                )

        # force_env: bypass auto-detection after config is loaded
        if force_env:
            env_upper = force_env.upper()
            environments = self.config.get("environments", {})
            env_keys_upper = {k.upper(): k for k in environments}
            if env_upper not in env_keys_upper:
                raise ValueError(
                    f"force_env='{force_env}' not found in config.yaml environments. "
                    f"Available: {list(environments.keys())}"
                )
            matched_key = env_keys_upper[env_upper]
            self.env = env_upper
            self.db = environments[matched_key].get("catalog")
            self.is_local = (self.db is None)
            logger.info("[Config] force_env='%s': bypassing auto-detection -> catalog='%s'", force_env, self.db)

        from skifer.core.config import parse_tracing_config
        from skifer.observability.tracing_exporters import create_tracer

        self._tracing_config = parse_tracing_config(self.config)
        self._tracer = create_tracer(self._tracing_config)

        # ======================================================================
        # 3. DÉTECTION UTILISATEUR & SANDBOX
        # ======================================================================
        self.current_user = self._get_clean_username()
        self.is_job_execution = self._is_running_as_job()

        self.schema_suffix = ""
        # Sécurité par défaut : on considère que ce n'est pas la prod sauf si explicitement écrit
        is_prod_env = self._context.is_production

        # On active la Sandbox SEULEMENT si : PAS Prod ET PAS Job
        if not is_prod_env and not self.is_job_execution:
            logger.info("[Interactive Mode] Sandbox enabled for: %s", self.current_user)
            self.schema_suffix = f"_{self.current_user}"
        else:
            if self.is_job_execution:
                logger.info("[Job Mode] Automated execution detected. Sandbox disabled.")
            if is_prod_env:
                logger.info("[Prod Mode] Production environment. Sandbox disabled.")

        logger.info("Framework ready. Env: %s | DB: %s | Suffix: '%s'", self.env, self.db, self.schema_suffix)

        # ======================================================================
        # 4. DATA MONITOR / CERTIFICATION STORE (optionnels)
        # ======================================================================
        self.monitor = monitor
        self.certification_store = certification_store
        if self.monitor is not None and hasattr(self.monitor, "tracer"):
            self.monitor.tracer = self._tracer
        if self.monitor is not None and hasattr(self.monitor, "tracing_required"):
            self.monitor.tracing_required = self._tracing_config.required

        # ======================================================================
        # 5. SCHEMA INTERPRETER + PIPELINE PATTERNS (plan17-2.3/2.4)
        # ======================================================================
        self._interpreter = SchemaInterpreter(backend=self._backend, context=self._context)
        self._patterns = PipelinePatterns(engine=self)

    @property
    def tracer(self):
        """Runtime tracer; defaults to the allocation-free no-op implementation."""
        return getattr(self, "_tracer", _DEFAULT_TRACER)

    def set_tracer(self, tracer):
        """Inject a tracer without changing the constructor's public signature."""
        if tracer is None:
            raise TypeError("tracer must not be None")
        self._tracer = tracer
        if self.monitor is not None and hasattr(self.monitor, "tracer"):
            self.monitor.tracer = tracer
        return self

    def _trace_span(self, name, attributes=None):
        tracer = self.tracer
        if isinstance(tracer, NoOpTracer):
            return nullcontext()
        return configured_span_scope(
            tracer,
            name,
            attributes=attributes,
            required=getattr(self, "_tracing_config", None).required
            if getattr(self, "_tracing_config", None) is not None
            else False,
        )

    def _get_workspace_client(self):
        """
        Returns a cached WorkspaceClient if Databricks credentials are available, else None.

        Credentials are read once from DATABRICKS_HOST and DATABRICKS_TOKEN environment
        variables. The result (client or None) is cached for the lifetime of the engine
        instance — only one SDK connection is opened regardless of how many methods need it.

        Returns:
            WorkspaceClient | None
        """
        if hasattr(self, "_workspace_client_cache"):
            return self._workspace_client_cache
        self._workspace_client_cache = _env.get_workspace_client()
        return self._workspace_client_cache

    def _find_file_upwards(self, filename):
        """Searches upward from cwd for filename. Returns absolute path or None."""
        return _find_file_upwards_fn(filename)

    def _load_config_from_yaml(self, config_path):
        """
        Loads configuration from YAML and determines the active environment.
        Strictly enforces file existence.
        
        Args:
            config_path (str): The absolute path to the configuration YAML file.
            
        Raises:
            ValueError: If the file does not exist or cannot be parsed.
        """
        logger.info("[Config] Loading configuration from: %s", config_path)

        if not os.path.exists(config_path):
             raise ValueError(f"CRITICAL ❌ Config file not found at: {config_path}")

        try:
            with open(config_path, 'r') as f:
                self.config = yaml.safe_load(f)
        except Exception as e:
            raise ValueError(f"CRITICAL ❌ Error parsing YAML: {e}")

        self._config_path = os.path.abspath(config_path)

        from skifer.core.config import parse_tracing_config

        parse_tracing_config(self.config)

        # Auto-detect environment based on priority list
        self._auto_detect_environment()

    def _check_catalog_access(self, catalog):
        """
        Verifies access to a catalog via the backend.
        A null/empty catalog is treated as a local environment and always returns True.

        Returns:
            bool: True if the catalog is accessible, False otherwise.
        """
        if not catalog:
            return True
        b = self.__dict__.get("_backend")
        if b is not None:
            return b.check_catalog_access(catalog)
        return False

    @staticmethod
    def clear_env_detection_cache() -> None:
        """Clear the process-level environment detection cache."""
        _ENV_DETECTION_CACHE.clear()

    def _auto_detect_environment(self):
        """
        Iterates through 'priority_check' list from YAML.
        Tries to verify if the catalog exists/is accessible via _check_catalog_access.
        Sets self.env and self.db to the first matching environment.
        Falls back to 'default_env' from config if all checks fail (useful locally).

        Results are cached in _ENV_DETECTION_CACHE keyed by (config_path, mtime)
        unless 'env_detection_cache: false' is set in config.
        """
        # Check cache (unless disabled)
        if self.config.get("env_detection_cache", True):
            config_path = getattr(self, "_config_path", None)
            if config_path and os.path.exists(config_path):
                cache_key = (config_path, os.path.getmtime(config_path))
                if cache_key in _ENV_DETECTION_CACHE:
                    env_upper, catalog = _ENV_DETECTION_CACHE[cache_key]
                    self.env = env_upper
                    self.db = catalog
                    logger.debug(
                        "[Config] Environment detection cache hit: %s (catalog: %s)",
                        env_upper, catalog,
                    )
                    return

        logger.info("[Config] Auto-detecting active environment...")

        priority_list = self.config.get("priority_check", [])
        environments = self.config.get("environments", {})

        if not priority_list or not environments:
             raise ValueError("CRITICAL ❌ Invalid Config: 'priority_check' or 'environments' missing.")

        detected_env = None

        for env_name in priority_list:
            env_config = environments.get(env_name)
            if not env_config:
                continue

            catalog = env_config.get("catalog")
            logger.info("      -> Checking access to catalog: '%s' (%s)...", catalog, env_name)

            if self._check_catalog_access(catalog):
                if catalog:
                    logger.info("      Connected to '%s'.", catalog)
                else:
                    logger.info("      Local environment selected (no catalog).")
                detected_env = env_name
                self.env = env_name.upper()
                self.db = catalog or None
                break
            else:
                logger.info("      Failed: Cannot access '%s'. Moving to next priority.", catalog)

        if not detected_env:
            # Fallback: use 'default_env' from config (avoids hard failure in local dev)
            default_env = self.config.get("default_env")
            if default_env and default_env in environments:
                catalog = environments[default_env].get("catalog")
                logger.warning(
                    "[Fallback] All catalog checks failed. Using 'default_env': '%s' -> catalog '%s'.",
                    default_env, catalog,
                )
                self.env = default_env.upper()
                self.db = catalog
            else:
                raise ValueError("CRITICAL ❌ Could not connect to ANY defined environment catalog.")

        # Store result in process cache (unless disabled)
        if self.config.get("env_detection_cache", True):
            config_path = getattr(self, "_config_path", None)
            if config_path and os.path.exists(config_path):
                cache_key = (config_path, os.path.getmtime(config_path))
                _ENV_DETECTION_CACHE[cache_key] = (self.env, self.db)

    def _is_running_as_job(self):
        """
        Détecte si le script est lancé par un Job Databricks.
        Delegates to :func:`environment.is_running_as_job`.

        Returns:
            bool: True if running as a Databricks Job, False otherwise.
        """
        return _env.is_running_as_job(dbutils=getattr(self, "dbutils", None))

    def _get_clean_username(self):
        """
        Récupère le user courant (ex: 'jdoe').
        Delegates to :func:`environment.get_clean_username`.

        Returns:
            str: The cleaned username of the current execution context.
        """
        spark = getattr(getattr(self, "_backend", None), "spark", None)
        # Treat falsy config (empty dict from uninitialised ExecutionContext) as None
        # so that Strategy 0 (cache-file read) is only attempted after a real load.
        _cfg = getattr(self, "config", None) or None
        return _env.get_clean_username(
            spark=spark,
            config=_cfg,
            dbutils=getattr(self, "dbutils", None),
            workspace_client_fn=self._get_workspace_client,
            find_file_fn=self._find_file_upwards,
        )


    @property
    def backend(self):
        """The runtime backend, for wiring a DataMonitor or a Delta-backed store.

        Public because the documented way to build a ``DataMonitor`` needs it.
        It was reachable only as ``engine._backend``, so the docs instructed
        readers to use a private attribute in five places — which quietly makes
        any rename of that attribute a breaking change for everyone who
        followed the documentation.
        """
        return self._get_backend()

    def _get_backend(self):
        """
        Returns the SparkBackend instance (always set in __init__; tests may
        inject a duck-typed double via ``engine._backend = ...``).
        """
        return self.__dict__.get('_backend')

    def _build_fqn(self, schema: str, table: str) -> str:
        """
        Builds a fully-qualified table name.
        Delegates to self._backend.build_fqn() for platform-agnostic FQN construction.
        """
        db = getattr(self, 'db', None)
        return self._get_backend().build_fqn(db, schema, table)

    def _ensure_schema_exists(self, schema: str) -> None:
        """Creates the schema/database if it does not exist. Delegates to backend."""
        db = getattr(self, 'db', None)
        fqn = f"{db}.{schema}" if db else schema
        self._get_backend().ensure_schema_exists(fqn)

    def _drop_table_if_exists(self, fqn):
        """Drops a table if it exists. Delegates to backend."""
        self._get_backend().drop_table(fqn)

    def _write_dataframe(self, df, fqn, label, sink_config=None, materialization=None):
        """Writes a DataFrame to the configured sink."""
        if sink_config and sink_config.get("type") in ("postgres", "jdbc"):
            from skifer.sinks.jdbc import JDBCSink

            fqn_parts = fqn.replace("`", "").split(".")
            schema = sink_config.get("schema") or (fqn_parts[-2] if len(fqn_parts) >= 2 else None)
            table = sink_config.get("table") or (fqn_parts[-1] if fqn_parts else label)
            JDBCSink().write(df, schema, table)
            return

        if materialization and materialization.get("type") == "materialized_view":
            raise ValueError(
                f"[_write_dataframe] '{fqn}' declares 'materialization: materialized_view' — "
                "a materialized view is defined by SQL, not written from a DataFrame. "
                "Run it through run_process_to_table/run_from_yaml, which route "
                "materialized views to the DDL path."
            )

        if materialization and materialization.get("type") == "streaming_table":
            self._get_backend().write_stream_table(
                df,
                fqn,
                checkpoint_location=materialization["_resolved_checkpoint"],
                trigger=materialization.get("trigger", "available_now"),
                write_mode=materialization.get("write_mode", "append"),
                keys=materialization.get("keys"),
            )
            return

        self._get_backend().write_table(df, fqn)

    def resolve_checkpoint_location(self, actual_schema: str, table: str, materialization: dict) -> str:
        """
        Resolve the checkpoint location for a streaming write (Plan 27).

        Explicit ``checkpoint:`` paths are used as-is. ``auto`` resolves to
        ``{root}/{actual_schema}/{table}`` where root is the backend default
        (local: ``{warehouse}/_checkpoints``) or the ``checkpoint_base``
        environment param on Databricks. ``actual_schema`` already carries the
        sandbox suffix, so interactive users never share a checkpoint.

        Raises:
            ValueError: On Databricks with ``checkpoint: auto`` and no
                        ``checkpoint_base`` param — fail-fast BEFORE any read.
        """
        checkpoint = materialization.get("checkpoint", "auto")
        if checkpoint != "auto":
            return checkpoint

        root = self._get_backend().default_checkpoint_root()
        if root is None:
            root = self.default_params.get("checkpoint_base")
        if not root:
            raise ValueError(
                "[streaming] 'checkpoint: auto' on Databricks requires a "
                "'checkpoint_base' environment param. Add it to config.yaml:\n"
                f"  environments:\n    {self.env}:\n      params:\n"
                "        checkpoint_base: /Volumes/<catalog>/<schema>/_checkpoints\n"
                "or set an explicit 'checkpoint:' path in the materialization block."
            )
        clean_schema = actual_schema.replace("`", "")
        clean_table = table.replace("`", "")
        return f"{str(root).rstrip('/')}/{clean_schema}/{clean_table}"

    # ------------------------------------------------------------------
    # Materialized views (Plan 28)
    # ------------------------------------------------------------------

    def resolve_sql_warehouse_id(self) -> str | None:
        """
        Resolve the SQL warehouse used to run materialized-view DDL (Plan 28).

        Local mode never needs one (decision #20: the compiled SELECT is executed
        by the batch path). On Databricks the id comes from the environment
        ``params.sql_warehouse_id``.

        Returns:
            The warehouse id, or None when the DDL cannot be executed (local
            mode, or interactive mode with no warehouse configured — the caller
            then writes the SQL to a file instead).

        Raises:
            ValueError: In job or production mode with no warehouse configured.
                        A scheduled run that silently creates nothing is worse
                        than a run that fails loudly.
        """
        warehouse_id = self.default_params.get("sql_warehouse_id")
        if warehouse_id:
            return str(warehouse_id)
        if self.is_local:
            return None
        if self.is_job_execution or self._context.is_production:
            raise ValueError(
                "[materialized_view] Creating a materialized view in job/production mode "
                "requires a SQL warehouse. Add it to config.yaml:\n"
                f"  environments:\n    {self.env}:\n      params:\n"
                "        sql_warehouse_id: <warehouse-id>\n"
                "(a Pro or Serverless SQL warehouse — all-purpose clusters cannot run "
                "CREATE MATERIALIZED VIEW)."
            )
        return None

    def _write_sql_artifact(self, fqn: str, ddl: str) -> str:
        """Write generated DDL next to the project and return its path."""
        out_dir = str(self.default_params.get("sql_output_dir", "generated_sql"))
        os.makedirs(out_dir, exist_ok=True)
        filename = fqn.replace("`", "").replace(".", "_") + ".sql"
        path = os.path.join(out_dir, filename)
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(ddl + "\n")
        return path

    def _create_materialized_view(
        self, schema_dict: dict, actual_schema: str, target_table_name: str
    ) -> bool:
        """
        Compile the schema to SQL and create/refresh the materialized view.

        Decision path (#15): view absent → CREATE; stored definition hash equal
        to the compiled one → REFRESH (or nothing when ``refresh: never``);
        different or unreadable → CREATE OR REPLACE.

        Returns:
            True when DDL was actually executed, False when the SQL was only
            written to a file (interactive mode with no warehouse configured).
            The caller gates the monitor on this: an unmaterialized view has
            nothing to check.
        """
        from skifer.core.constants import MV_DEFINITION_HASH_PROPERTY
        from skifer.core.ir import parse_to_ir
        from skifer.core.sql_compiler import (
            compile_materialized_view_ddl,
            compile_select,
            definition_hash,
        )

        materialization = schema_dict.get("materialization") or {}
        # Fail-fast BEFORE compiling: a job without a warehouse must not do work
        # it cannot finish.
        warehouse_id = self.resolve_sql_warehouse_id()

        self._ensure_schema_exists(actual_schema)
        fqn = self._build_fqn(actual_schema, target_table_name)
        backend = self._get_backend()

        allow_raw_sql = self._context.env_config().get("allow_raw_sql", True)
        select_sql = compile_select(
            parse_to_ir(schema_dict),
            resolve_table=self._interpreter.resolve_source_table,
            allow_raw_sql=allow_raw_sql,
        )

        # Local mode (decision #20): no materialized views in Delta OSS — run the
        # compiled SELECT and persist it through the regular batch write. Proves
        # the generated SQL is valid and executable.
        if self.is_local:
            logger.info("   -> [MV] Local mode — materializing '%s' as a Delta table.", fqn)
            self._get_backend().write_table(backend.sql(select_sql), fqn)
            return True

        new_hash = definition_hash(select_sql, materialization)

        if warehouse_id is None:
            ddl = compile_materialized_view_ddl(
                fqn, select_sql, materialization, new_hash, or_replace=True
            )
            path = self._write_sql_artifact(fqn, ddl)
            logger.warning(
                "[materialized_view] MATERIALIZED VIEW NOT CREATED — no SQL warehouse "
                "configured. The DDL was written to '%s'. Set "
                "environments.%s.params.sql_warehouse_id in config.yaml to have the "
                "engine run it.", path, self.env,
            )
            return False

        clean = fqn.replace("`", "").split(".")
        catalog_part = clean[-3] if len(clean) >= 3 else None
        exists = backend.table_exists(catalog_part, clean[-2], clean[-1])
        current_hash = (
            backend.get_table_property(fqn, MV_DEFINITION_HASH_PROPERTY, warehouse_id)
            if exists else None
        )

        if not exists:
            logger.info("   -> [MV] Creating materialized view '%s'...", fqn)
            backend.create_materialized_view(
                compile_materialized_view_ddl(fqn, select_sql, materialization, new_hash),
                warehouse_id,
            )
            return True

        if current_hash == new_hash:
            if materialization.get("refresh", "auto") == "never":
                logger.info(
                    "   -> [MV] '%s' is up to date and refresh: never — leaving the "
                    "refresh to its SCHEDULE.", fqn,
                )
                return True
            logger.info("   -> [MV] '%s' unchanged — refreshing.", fqn)
            backend.refresh_materialized_view(fqn, warehouse_id)
            return True

        logger.info(
            "   -> [MV] Definition of '%s' changed (%s -> %s) — CREATE OR REPLACE.",
            fqn, current_hash or "<unreadable>", new_hash,
        )
        backend.create_materialized_view(
            compile_materialized_view_ddl(
                fqn, select_sql, materialization, new_hash, or_replace=True
            ),
            warehouse_id,
        )
        return True

    def full_refresh(
        self,
        target_layer: str,
        target_table_name: str,
        checkpoint: str | None = None,
        materialization: str = "streaming_table",
    ):
        """
        Full refresh of a streaming table (Plan 27): atomically purge its
        checkpoint AND drop the target table, so the next run rebuilds from
        scratch. Prevents the classic half-purged state (duplicate rows or
        incomplete backfill).

        Args:
            target_layer: Target schema/layer (sandbox suffix applied like
                          ``run_process_to_table``).
            target_table_name: Target table name.
            checkpoint: Explicit checkpoint path (matches an explicit
                        ``checkpoint:`` in the YAML). Defaults to the ``auto``
                        resolution for this target.
            materialization: ``streaming_table`` (default) or ``materialized_view``
                        — the latter has no checkpoint, so it simply drops the
                        view and lets the next run rebuild it (Plan 28).
        """
        import shutil

        actual_schema = self.get_target_schema(target_layer)
        fqn = self._build_fqn(actual_schema, target_table_name)

        if materialization == "materialized_view":
            logger.info("[FullRefresh] Dropping materialized view: %s", fqn)
            if self.is_local:
                self._drop_table_if_exists(fqn)
            else:
                self._get_backend().drop_materialized_view(
                    fqn, self.resolve_sql_warehouse_id()
                )
            logger.info(
                "[FullRefresh] Done — the next run of '%s' will recreate the view.",
                target_table_name,
            )
            return

        checkpoint_path = checkpoint or self.resolve_checkpoint_location(
            actual_schema, target_table_name, {"checkpoint": "auto"}
        )

        logger.info("[FullRefresh] Purging checkpoint: %s", checkpoint_path)
        shutil.rmtree(checkpoint_path.replace("file:", ""), ignore_errors=True)
        logger.info("[FullRefresh] Dropping target table: %s", fqn)
        self._drop_table_if_exists(fqn)
        logger.info(
            "[FullRefresh] Done — the next run of '%s' will re-ingest the full source.",
            target_table_name,
        )

    def get_agent(
        self,
        llm_provider=None,
        models_dir: str = "semantic_models",
        history: bool = False,
        session_title: str = "Analyse KPI",
        certification_store=None,
    ):
        """
        Factory method — crée un GenBIAgent prêt à l'emploi en un seul appel.

        Instancie SemanticEngine + GenBIAgent sans que l'utilisateur ait à
        gérer ces objets directement.

        Args:
            llm_provider:  Instance LLMProvider. Si None, auto-détecté depuis
                           les variables d'environnement (ANTHROPIC_API_KEY,
                           OPENAI_API_KEY, GOOGLE_API_KEY).
            models_dir:    Répertoire des modèles sémantiques YAML.
                           Relatif ou absolu. Défaut : "semantic_models".
            history:       Active l'historique conversationnel multi-turn.
            session_title: Titre de la session pour l'export PDF.
            certification_store: Store de certification sémantique. Si None,
                           un store SQLite local est utilisé.

        Returns:
            GenBIAgent configuré et prêt à l'emploi.

        Example:
            engine = SkiferEngine(spark=spark)
            agent  = engine.get_agent()
            resp   = agent.ask("Quel est le CA par région pour 2024 ?")
            if resp.success:
                resp.result.data.show()
        """
        from skifer.semantic.semantic import SemanticEngine
        from skifer.semantic.llm_provider import get_llm_provider
        from skifer.agentic.agent import GenBIAgent
        from skifer.observability.certification_store import SqliteCertificationStore

        if llm_provider is None:
            llm_provider = get_llm_provider()
        if certification_store is None:
            # Safe when certification is off: policy modes that do not check certification never call the store.
            certification_store = SqliteCertificationStore()

        semantic = SemanticEngine(self, models_dir=models_dir, certification_store=certification_store)
        return GenBIAgent(
            semantic,
            llm_provider,
            history=history,
            session_title=session_title,
        )

    def get_target_schema(self, base_layer: str) -> str:
        """
        Retourne le nom complet du schema cible en gerant la sandbox.
        Ex en interactif Dev : silver -> silver_jdoe
        Ex en Job ou en Prod : silver -> silver

        Args:
            base_layer (str): The base target schema name (e.g., bronze, silver).

        Returns:
            str: The target schema name with sandbox suffix appended if applicable.
        """
        if not self.schema_suffix:
            return base_layer

        if base_layer.endswith(self.schema_suffix):
            return base_layer

        return f"{base_layer}{self.schema_suffix}"

    @property
    def default_params(self):
        """
        Returns common engine context variables as a params dict for use with load_schema().

        Returns:
            dict: {"catalog": self.db, "env": self.env}
        """
        return self._context.default_params

    def _apply_business_rules(self, df, rules_list, fuse_rules: bool = True):
        """Delegates to SchemaInterpreter._apply_business_rules (plan17-2.3)."""
        return self._interpreter._apply_business_rules(df, rules_list, fuse_rules=fuse_rules)

    def get_select_expressions(self, field_list, allow_raw_sql=True):
        """Delegates to SchemaInterpreter.get_select_expressions (plan17-2.3)."""
        return self._interpreter.get_select_expressions(field_list, allow_raw_sql=allow_raw_sql)

    def process_schema(self, schema_dict, dataframes_in=None, intermediate_mode="inline"):
        """Delegates to SchemaInterpreter.process_schema (plan17-2.3)."""
        if isinstance(self.tracer, NoOpTracer):
            return self._interpreter.process_schema(
                schema_dict, dataframes_in=dataframes_in, intermediate_mode=intermediate_mode
            )
        with self._trace_span("skifer.source.resolve"):
            with self._trace_span("skifer.transform.execute"):
                return self._interpreter.process_schema(
                    schema_dict,
                    dataframes_in=dataframes_in,
                    intermediate_mode=intermediate_mode,
                )

    def run_process_and_split(self, schema_dict, split_values, target_layer, target_base_name, split_column):
        """Delegates to PipelinePatterns.run_process_and_split (plan17-2.4)."""
        return self._patterns.run_process_and_split(
            schema_dict, split_values, target_layer, target_base_name, split_column
        )

    def _traced_pipeline_run(self, run_id, business_call):
        """Wrap one business pipeline call in the run-level trace scope.

        The call is defined once and invoked once whatever the tracer does.
        Writing it out separately per tracing state is what let the two flows
        drift in slice 5.2, to the point of changing the persisted run_id.
        """
        tracer = self.tracer
        if isinstance(tracer, NoOpTracer):
            return business_call()
        config = getattr(self, "_tracing_config", None)
        attributes = {
            "skifer.trace_version": TRACE_FORMAT_VERSION,
            "environment": self.env,
            "run_id": run_id,
        }
        with trace_context_scope(
            tracer,
            TraceContext(run_id=run_id),
            required=config.required if config is not None else False,
        ):
            with self._trace_span("skifer.pipeline.run", attributes):
                return business_call()

    def run_process_to_table(self, schema_dict, target_layer, target_table_name, intermediate_mode="inline"):
        """Delegates to PipelinePatterns.run_process_to_table (plan17-2.4)."""
        # Minted on the business path, never only when tracing is on: this id is
        # what the certification registry persists, so an audit trail that
        # existed only under an exporter would be no audit trail at all.
        run_id = str(uuid4())
        return self._traced_pipeline_run(
            run_id,
            lambda: self._patterns.run_process_to_table(
                schema_dict,
                target_layer,
                target_table_name,
                intermediate_mode=intermediate_mode,
                run_id=run_id,
            ),
        )

    def run_from_yaml(self, yaml_path, target_layer, target_table_name=None, params=None):
        """Delegates to PipelinePatterns.run_from_yaml (plan17-2.4)."""
        run_id = str(uuid4())
        return self._traced_pipeline_run(
            run_id,
            lambda: self._patterns.run_from_yaml(
                yaml_path,
                target_layer,
                target_table_name,
                params,
                run_id=run_id,
            ),
        )

    def run_union_sources_to_table(self, schema_dict, source_partitions, source_layer, target_layer, target_table_name, source_base_names, source_alias, dedup_after_union=True):
        """Delegates to PipelinePatterns.run_union_sources_to_table (plan17-2.4)."""
        return self._patterns.run_union_sources_to_table(
            schema_dict, source_partitions, source_layer, target_layer,
            target_table_name, source_base_names, source_alias, dedup_after_union
        )

    def optimize_table(self, target_layer, target_table_name, zorder_cols=None):
        """
        Optimise une table. Delègue au backend (natif ou best-effort selon la plateforme).

        Args:
            target_layer (str): La couche cible de base (ex: "silver")
            target_table_name (str): Le nom de la table (ex: "fact_sales")
            zorder_cols (list, optional): Colonnes pour le ZORDER BY / CLUSTER BY
        """
        resolved_layer = self.get_target_schema(target_layer)
        full_target_fqn = self._build_fqn(resolved_layer, target_table_name)
        logger.info("--- Executing Pattern: optimize (Target: %s) ---", target_table_name)
        logger.info("     -> [Optimize] Target FQN: %s", full_target_fqn)
        self._get_backend().optimize_table(full_target_fqn, zorder_cols)
        logger.info("     -> [Success] optimize_table completed for %s.", target_table_name)

    def describe_schema(self, schema_dict, print_summary: bool = True) -> dict:
        """
        Preview what a schema dict would do without executing any Spark operation.
        Prints a human-readable summary (when print_summary=True) and always returns
        a structured dict describing the pipeline.

        Args:
            schema_dict (dict): The pipeline schema dict (or loaded via load_schema).
            print_summary (bool): Whether to print the human-readable summary (default: True).

        Returns:
            dict: Structured description of the pipeline schema.
        """
        from skifer.core.ir import parse_to_ir
        ps = parse_to_ir(schema_dict)

        SEP = "=" * 55
        SEP2 = "-" * 40

        def _op_str(op):
            return f"{op.name}:{','.join(op.args)}" if op.args else op.name

        # ---- Collect structured data ----

        # SOURCES
        sources_data = []
        for pt in ps.tables:
            filters = [
                f"{pf.column} {pf.operator} {pf.value}".strip() if pf.value else f"{pf.column} {pf.operator}"
                for pf in pt.filters
            ]
            quality_checks = {}
            if pt.drop_nulls_in:
                quality_checks["drop_nulls_in"] = pt.drop_nulls_in
            if pt.drop_duplicates_on:
                quality_checks["drop_duplicates_on"] = pt.drop_duplicates_on
            src_info = None
            if pt.source_type:
                src_info = {"type": pt.source_type, "path": pt.source_path, "options": pt.source_options}
            sources_data.append({
                "name": pt.name,
                "alias": pt.alias,
                "filters": filters,
                "dev_limit": pt.dev_limit,
                "quality_checks": quality_checks,
                "source": src_info,
            })

        # JOINS
        joins_data = []
        for pj in ps.joins:
            kl = pj.keys_left[0] if len(pj.keys_left) == 1 else pj.keys_left
            kr = pj.keys_right[0] if len(pj.keys_right) == 1 else pj.keys_right
            joins_data.append({
                "from": [pj.alias_left, kl],
                "to": [pj.alias_right, kr],
                "type": pj.join_type,
            })

        # BUSINESS RULES
        rules = ps.business_rules

        # QUALITY CHECKS (schema-level)
        quality_checks_data = {}
        schema_qc = schema_dict.get("quality_checks", {})
        if "drop_nulls_in" in schema_qc:
            quality_checks_data["drop_nulls_in"] = schema_qc["drop_nulls_in"]
        if "drop_duplicates_on" in schema_qc:
            quality_checks_data["drop_duplicates_on"] = schema_qc["drop_duplicates_on"]

        # OUTPUT COLUMNS
        col_defs_ir = ps.add_columns if ps.keep_all_columns else ps.select_final
        output_columns_data = []
        for cs in col_defs_ir:
            ops_strs = [_op_str(op) for op in cs.ops]
            output_columns_data.append({
                "source": cs.source or "literal",
                "target": cs.target,
                "ops": ops_strs,
            })

        # MODE / ENV
        is_prod = self._context.is_production
        mode = "JOB" if self.is_job_execution else ("PROD" if is_prod else "INTERACTIVE")

        result = {
            "sources": sources_data,
            "joins": joins_data,
            "business_rules": list(rules),
            "quality_checks": quality_checks_data,
            "output_columns": output_columns_data,
            "mode": mode,
            "env": self.env,
            "sandbox_suffix": self.schema_suffix or None,
        }

        # ---- Optional print ----
        if print_summary:
            print(f"\n{SEP}")
            print(" Schema Summary")
            print(SEP)

            # SOURCES
            print(f"\n Sources ({len(ps.tables)} table{'s' if len(ps.tables) != 1 else ''})")
            print(f" {SEP2}")

            for i, pt in enumerate(ps.tables, 1):
                print(f"  [{i}] {pt.name}   alias: {pt.alias}")

                # External source info
                if pt.source_type:
                    print(f"      source: {pt.source_type} @ {pt.source_path}")

                # Sandbox resolution preview (no actual Spark calls)
                if self.schema_suffix:
                    try:
                        b = self._get_backend()
                        resolver = SandboxResolver(backend=b)
                        _, schema_name, table_name = resolver.parse_fqn(pt.name)
                        if schema_name:
                            suffixed = f"{schema_name}{self.schema_suffix}"
                            exists_in_sandbox = resolver.table_exists(None, suffixed, table_name)
                            if exists_in_sandbox:
                                print(f"      sandbox -> {suffixed}.{table_name}")
                            else:
                                schema_exists = resolver.schema_exists(None, suffixed)
                                if schema_exists:
                                    print(f"      sandbox -> {suffixed}.{table_name}  [table not found, would clone]")
                                else:
                                    print(f"      sandbox -> {suffixed}.{table_name}  [schema not found, would create + clone]")
                    except Exception:
                        pass

                # Filters
                if pt.filters:
                    filter_strs = [
                        f"{pf.column} {pf.operator} {pf.value}" if pf.value else f"{pf.column} {pf.operator}"
                        for pf in pt.filters
                    ]
                    print(f"      filter:  {' AND '.join(filter_strs)}")

                # Quality checks
                qc_parts = []
                if pt.drop_nulls_in:
                    qc_parts.append(f"drop_nulls_in={pt.drop_nulls_in}")
                if pt.drop_duplicates_on:
                    qc_parts.append(f"drop_duplicates_on={pt.drop_duplicates_on}")
                if qc_parts:
                    print(f"      quality: {', '.join(qc_parts)}")

                if not pt.filters and not pt.drop_nulls_in and not pt.drop_duplicates_on:
                    print("      (no filter)")

            # JOINS
            if ps.joins:
                print(f"\n Joins ({len(ps.joins)})")
                print(f" {SEP2}")
                for pj in ps.joins:
                    kl = pj.keys_left[0] if len(pj.keys_left) == 1 else str(pj.keys_left)
                    kr = pj.keys_right[0] if len(pj.keys_right) == 1 else str(pj.keys_right)
                    print(f"  {pj.join_type.upper()} JOIN {pj.alias_left} -> {pj.alias_right}  on {pj.alias_left}.{kl} = {pj.alias_right}.{kr}")

            # BUSINESS RULES
            if ps.business_rules:
                print(f"\n Business Rules ({len(ps.business_rules)})")
                print(f" {SEP2}")
                for r in ps.business_rules:
                    print(f"  {r}")

            # OUTPUT COLUMNS
            if ps.keep_all_columns:
                print("\n Output Columns")
                print(f" {SEP2}")
                print("  [all source columns]")
                for cs in ps.add_columns:
                    ops_strs = [_op_str(op) for op in cs.ops]
                    ops_part = f"  [{', '.join(ops_strs)}]" if ops_strs else ""
                    print(f"  + {cs.target:<20} <- {cs.source or 'literal'}{ops_part}")
            elif ps.select_final:
                print(f"\n Output Columns ({len(ps.select_final)})")
                print(f" {SEP2}")
                for cs in ps.select_final:
                    if cs.is_conditional:
                        print(f"  {cs.target:<20} <- {cs.source or '?'}  [chained when/else]")
                    else:
                        ops_strs = [_op_str(op) for op in cs.ops]
                        ops_part = f"  [{', '.join(ops_strs)}]" if ops_strs else ""
                        print(f"  {cs.target:<20} <- {cs.source or 'literal'}{ops_part}")

            # FOOTER
            suffix_display = self.schema_suffix or "(none)"
            print(f"\n Mode: {mode}  Env: {self.env}  Sandbox suffix: {suffix_display}")
            print(f"{SEP}\n")

        return result

    def infer_output_schema(self, schema_dict: dict) -> list[dict]:
        """
        Infère les types de sortie des colonnes d'un pipeline sans exécuter Spark.

        Parcourt select_final (ou add_columns si keep_all_columns=True) et applique
        des règles statiques de déduction de type basées sur les opérations déclarées.

        Args:
            schema_dict (dict): Le dict de schéma pipeline.

        Returns:
            list[dict]: Liste de dicts {"name": str, "source": str, "ops": list, "type": str}.
        """
        from skifer.core.ir import parse_to_ir
        from skifer.core.op_catalog import OP_INFERRED_TYPE, CAST_INFERRED_TYPE

        ps = parse_to_ir(schema_dict)
        col_defs = ps.add_columns if ps.keep_all_columns else ps.select_final

        result = []
        for cs in col_defs:
            inferred_type = "unknown"
            src_str = str(cs.source or "")

            if src_str.startswith("literal:") or src_str.startswith("lit:"):
                inferred_type = "string"
            elif cs.is_conditional:
                inferred_type = "string"
            else:
                for op in cs.ops:
                    if op.name == "cast" and op.args:
                        inferred_type = CAST_INFERRED_TYPE.get(op.args[0].lower(), inferred_type)
                    elif op.name in OP_INFERRED_TYPE:
                        inferred_type = OP_INFERRED_TYPE[op.name]
                    elif op.name in ("lit", "literal"):
                        inferred_type = "string"
                    elif op.name in ("when", "else"):
                        inferred_type = "string"

            ops_strs = [f"{op.name}:{','.join(op.args)}" if op.args else op.name for op in cs.ops]
            result.append({
                "name": cs.target,
                "source": str(cs.source or "literal"),
                "ops": ops_strs,
                "type": inferred_type,
            })

        return result

    def build_lineage(
        self,
        schema_dict: dict,
        semantic_models: list | None = None,
        target_name: str | None = None,
    ):
        """
        Build a LineageGraph from a core schema dict and optional semantic models.

        Derives column-level lineage statically (no Spark execution required).

        Args:
            schema_dict:      Normalised schema dict (output of parse_schema / load_schema).
            semantic_models:  List of semantic model dicts (from SemanticEngine) to merge
                              into the same graph. Optional.
            target_name:      FQN or label for the pipeline output table. Defaults to
                              the first table's name with an "_output" suffix.

        Returns:
            LineageGraph — navigable DAG with upstream() / downstream() methods
            and Mermaid/JSON export via LineageRenderer.
        """
        from skifer.lineage.tracker import LineageTracker

        graph = LineageTracker.from_schema(schema_dict, target_name=target_name)
        for model in (semantic_models or []):
            graph.merge(LineageTracker.from_semantic_model(model))
        return graph

    def explain_rules(self, schema_dict, shared_read_threshold: int = 2):
        """
        Analyze the business rules declared in a schema dict and print a
        redundancy report without executing any pipeline.

        Performs best-effort AST introspection: rules whose source code is
        unavailable (e.g. defined in a REPL) are listed but skipped.

        Args:
            schema_dict (dict): The pipeline schema dict (or loaded via load_schema).
            shared_read_threshold (int): Minimum number of rules reading the same
                column to trigger a SHARED_READ warning. Defaults to 2.

        Returns:
            tuple[list[RuleProfile], list[RuleWarning]]: Profiles and warnings
            for programmatic use.
        """
        rule_names = schema_dict.get("business_rules", [])
        analyzer = RuleAnalyzer()
        profiles = analyzer.analyze_rules(rule_names)
        warnings = analyzer.detect_warnings(profiles, shared_read_threshold=shared_read_threshold)
        analyzer.print_report(profiles, warnings)
        return profiles, warnings
