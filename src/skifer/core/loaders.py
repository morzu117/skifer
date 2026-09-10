"""
Generic Data Loaders registered via the RuleRegistry.

Breaking change (v1.0): the legacy ``spark`` positional parameter has been removed.
Loaders now receive the backend via ``backend=`` keyword argument injected by the engine.

``load_generic_explode_union`` uses PySpark F.explode and therefore requires a backend
with a ``.spark`` attribute (i.e. SparkBackend). Passing any other backend will raise
a ``RuntimeError``.
"""
from functools import reduce
from .registry import RuleRegistry


@RuleRegistry.register_loader(name="load_and_union_tables")
def load_and_union_tables(config, table_fqns, **kwargs):
    """
    Loads a list of tables using their FQNs and performs a unionByName.
    Missing columns are filled with nulls.

    Args:
        config (dict): The configuration dictionary.
        table_fqns (list[str]): FQNs of tables to load.
        backend: Backend instance injected by the engine (required).

    Raises:
        ValueError: If none of the provided tables could be loaded.
        RuntimeError: If no backend is provided.
    """
    backend = kwargs.get("backend")
    if backend is None:
        raise RuntimeError(
            "[load_and_union_tables] A backend is required. "
            "The legacy spark positional argument was removed in v1.0."
        )
    print(f"   -> [Data Loader] Loading and Unioning {len(table_fqns)} tables...")
    list_of_dfs = []
    for table in table_fqns:
        try:
            list_of_dfs.append(backend.read_table(table))
        except Exception as e:
            print(f"      [WARNING] Could not load {table}. Skipping. ({e})")

    if not list_of_dfs:
        raise ValueError("No tables loaded successfully.")

    return backend.union_by_name(list_of_dfs)


@RuleRegistry.register_loader()
def load_generic_explode_union(config, source_fqn, filter_expr=None, union_config=None, **kwargs):
    """
    Loads a source table, explodes multiple array columns, and unions branches.
    Requires a SparkBackend (uses PySpark F.explode internally).

    Args:
        config (dict): The configuration dictionary.
        source_fqn (str): FQN of the source table.
        filter_expr (str, optional): SQL filter to apply before exploding.
        union_config (dict): Branch configuration.
        backend: SparkBackend instance injected by the engine (required).

    Raises:
        ValueError: If 'union_config' is not provided.
        RuntimeError: If no backend or backend has no .spark attribute.
    """
    from pyspark.sql import functions as F

    backend = kwargs.get("backend")
    if backend is None:
        raise RuntimeError(
            "[load_generic_explode_union] A backend is required. "
            "The legacy spark positional argument was removed in v1.0."
        )
    if getattr(backend, 'spark', None) is None:
        raise RuntimeError(
            f"[load_generic_explode_union] Requires a SparkBackend (.spark attribute). "
            f"Got: {type(backend).__name__}"
        )

    print(f"      [Loader] Generic Explode & Union on {source_fqn}")

    df_source = backend.read_table(source_fqn)
    if filter_expr:
        df_source = backend.filter(df_source, backend.expr(filter_expr))

    if not union_config:
        raise ValueError("[Loader] 'union_config' is required for load_generic_explode_union")

    common_cols = union_config.get("common_cols", [])
    branches = union_config.get("branches", [])
    dfs_to_union = []

    for i, branch_conf in enumerate(branches):
        explode_col = branch_conf["explode_col"]
        item_alias = branch_conf.get("item_alias", "item")
        mappings = branch_conf.get("mappings", {})
        defaults = branch_conf.get("defaults", {})

        print(f"        -> Processing branch {i+1}: {explode_col} (alias: {item_alias})")

        df_branch = df_source.withColumn(item_alias, F.explode(F.col(explode_col)))
        select_exprs = [F.col(c) for c in common_cols]
        mapped_col_names = []
        for target_col, source_path in mappings.items():
            select_exprs.append(F.col(source_path).alias(target_col))
            mapped_col_names.append(target_col)
        for target_col, default_val in defaults.items():
            if target_col not in mapped_col_names:
                if isinstance(default_val, float):
                    select_exprs.append(F.lit(default_val).cast("double").alias(target_col))
                elif isinstance(default_val, int):
                    select_exprs.append(F.lit(default_val).cast("int").alias(target_col))
                else:
                    select_exprs.append(F.lit(default_val).alias(target_col))
        df_branch = df_branch.select(*select_exprs)
        dfs_to_union.append(df_branch)

    if not dfs_to_union:
        print("      [Warning] No branches processed. Returning empty DF.")
        return backend.limit(df_source, 0)

    print(f"      [Union] Merging {len(dfs_to_union)} branches...")
    return backend.union_by_name(dfs_to_union)
