"""
Sandbox resolver — transparent sandbox schema/table resolution for interactive mode.
"""
import os


class SandboxResolver:
    """
    Resolves source table references to their sandboxed equivalents.
    In interactive mode, ensures tables exist in the user's sandbox schema,
    cloning from the main schema if needed.

    Requires a SparkBackend (tests may pass a duck-typed double).
    Direct spark.sql() calls have been removed as of v1.0 (breaking change).
    """

    def __init__(self, backend):
        self._backend = backend
        # Memoization cache: (table_name, schema_suffix, missing_table_behavior) → resolved FQN
        # Cloned tables are never re-cloned within the same resolver instance.
        self._resolve_cache: dict[tuple, str] = {}

    def _sql(self, sql: str):
        """Execute SQL via the backend."""
        return self._backend.execute_sql(sql)

    def parse_fqn(self, fqn):
        """
        Parse a FQN into (catalog, schema, table) components.
        Returns (None, schema, table) for 2-part FQNs.
        """
        clean = fqn.replace("`", "")
        parts = clean.split(".")
        if len(parts) == 3:
            return parts[0], parts[1], parts[2]
        elif len(parts) == 2:
            return None, parts[0], parts[1]
        else:
            return None, None, clean

    def schema_exists(self, catalog, schema):
        """Check if a schema exists."""
        return self._backend.schema_exists(catalog, schema)

    def table_exists(self, catalog, schema, table):
        """Check if a table exists."""
        return self._backend.table_exists(catalog, schema, table)

    def create_schema(self, catalog, schema):
        """Create a schema if it doesn't exist."""
        fqn = f"{catalog}.{schema}" if catalog else schema
        self._backend.ensure_schema_exists(fqn)

    def clone_table(self, src_catalog, src_schema, src_table, tgt_catalog, tgt_schema, tgt_table):
        """
        Clone a table from main schema to sandbox.
        Delegates entirely to backend.clone_table() (best-effort, platform-specific).
        """
        self._backend.clone_table(src_catalog, src_schema, src_table, tgt_catalog, tgt_schema, tgt_table)

    def build_suffixed_fqn(self, catalog, schema, table, suffix):
        """Build the sandbox FQN with suffix applied to the schema."""
        suffixed_schema = f"{schema}{suffix}"
        if catalog:
            return catalog, suffixed_schema, table
        return None, suffixed_schema, table

    def resolve(self, table_name, schema_suffix, missing_table_behavior="copy"):
        """
        Resolve a table name to its sandbox equivalent.

        Results are memoized in ``_resolve_cache`` — cloned tables are not re-checked
        or re-cloned within the same resolver instance.

        Args:
            table_name (str): Original table name from schema (e.g. "silver.orders")
            schema_suffix (str): User sandbox suffix (e.g. "_jdoe")
            missing_table_behavior (str): "copy" or "error"

        Returns:
            str: The resolved table name to actually load (sandboxed or original).
        """
        cache_key = (table_name, schema_suffix, missing_table_behavior)
        if cache_key in self._resolve_cache:
            return self._resolve_cache[cache_key]

        result = self._do_resolve(table_name, schema_suffix, missing_table_behavior)
        self._resolve_cache[cache_key] = result
        return result

    def _do_resolve(self, table_name, schema_suffix, missing_table_behavior="copy"):
        """Internal resolution logic — called only when the cache misses."""
        catalog, schema, table = self.parse_fqn(table_name)

        if not schema or not table:
            return table_name  # Can't resolve, use as-is

        tgt_catalog, tgt_schema, tgt_table = self.build_suffixed_fqn(catalog, schema, table, schema_suffix)

        sandbox_schema_exists = self.schema_exists(tgt_catalog, tgt_schema)

        if sandbox_schema_exists:
            if self.table_exists(tgt_catalog, tgt_schema, tgt_table):
                # Table exists in sandbox — use it transparently
                if tgt_catalog:
                    return f"`{tgt_catalog}`.`{tgt_schema}`.`{tgt_table}`"
                return f"`{tgt_schema}`.`{tgt_table}`"
            else:
                # Table missing in sandbox
                if missing_table_behavior == "error":
                    raise ValueError(
                        f"[Sandbox] Table '{table}' not found in sandbox schema '{tgt_schema}'. "
                        f"Set sandbox.missing_table=copy in config.yaml to auto-clone."
                    )
                # Auto-clone
                print(f"    -> [Sandbox] Table '{table}' not found in '{tgt_schema}'. Cloning from '{schema}'...")
                if not self.table_exists(catalog, schema, table):
                    raise ValueError(f"[Sandbox] Source table '{schema}.{table}' not found in main schema.")
                self.clone_table(catalog, schema, table, tgt_catalog, tgt_schema, tgt_table)
                print(f"    -> [Sandbox] Clone complete. Resuming workflow.")
        else:
            # Sandbox schema doesn't exist — create it
            print(f"    -> [Sandbox] Schema '{tgt_schema}' not found. Creating...")
            self.create_schema(tgt_catalog, tgt_schema)
            print(f"    -> [Sandbox] Schema '{tgt_schema}' created.")

            if missing_table_behavior == "error":
                raise ValueError(
                    f"[Sandbox] Table '{table}' not found in sandbox schema '{tgt_schema}'. "
                    f"Set sandbox.missing_table=copy in config.yaml to auto-clone."
                )
            # Auto-clone
            print(f"    -> [Sandbox] Table '{table}' not found in '{tgt_schema}'. Cloning from '{schema}'...")
            if not self.table_exists(catalog, schema, table):
                raise ValueError(f"[Sandbox] Source table '{schema}.{table}' not found in main schema.")
            self.clone_table(catalog, schema, table, tgt_catalog, tgt_schema, tgt_table)
            print(f"    -> [Sandbox] Clone complete. Resuming workflow.")

        if tgt_catalog:
            return f"`{tgt_catalog}`.`{tgt_schema}`.`{tgt_table}`"
        return f"`{tgt_schema}`.`{tgt_table}`"
