"""
SchemaInterpreter — translates a normalized schema dict into DataFrame operations.

Extracted from SkiferEngine (plan17-2.3) so that the engine class is a thin
facade over composable components. The interpreter owns the three stateful
processing methods:

  - get_select_expressions  — parse select_final / fields op-strings into columns
  - _apply_business_rules   — run registered rules through RulePlanner/RuleExecutor
  - process_schema          — full pipeline: load → filter → join → rules → select
"""
from __future__ import annotations

import logging
from functools import reduce
from typing import TYPE_CHECKING, Any

from skifer.core.ir import _normalise_join_type, _parse_op
from skifer.core.registry import RuleRegistry
from skifer.core.sandbox import SandboxResolver
from skifer.core.rule_analyzer import RuleAnalyzer
from skifer.core.rule_planner import RulePlanner
from skifer.core.rule_executor import RuleExecutor

if TYPE_CHECKING:
    from skifer.core.context import ExecutionContext

logger = logging.getLogger(__name__)

_LEFT_ONLY_JOIN_TYPES = {"left_anti", "left_semi"}


class SchemaInterpreter:
    """
    Translates a normalized schema dict into DataFrame operations.

    Args:
        backend: SparkBackend (tests may pass a duck-typed double).
        context: ExecutionContext providing env, config, is_job_execution, etc.
    """

    def __init__(self, backend: Any, context: "ExecutionContext") -> None:
        self._backend = backend
        self._context = context
        # Shared resolver for sandbox memoization — reused across all process_schema calls
        # on this interpreter instance so cloned tables are never re-checked.
        self._sandbox_resolver: SandboxResolver | None = None

    def resolve_source_table(self, name: str) -> str:
        """
        Resolve a declared table name to the FQN actually read (Plan 28).

        In interactive mode the sandbox suffix redirects reads to the user's own
        schema, cloning the table on demand. In job/prod mode the declared name
        is used as-is. Shared by the DataFrame path and the SQL compiler so both
        read exactly the same tables.
        """
        ctx = self._context
        if not (ctx.schema_suffix and not ctx.is_job_execution):
            return name
        missing_behavior = ctx.config.get("sandbox", {}).get("missing_table", "copy")
        if self._sandbox_resolver is None:
            self._sandbox_resolver = SandboxResolver(backend=self._backend)
        return self._sandbox_resolver.resolve(name, ctx.schema_suffix, missing_behavior)

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------

    def get_select_expressions(self, field_list: list, allow_raw_sql: bool = True) -> list:
        """
        Parse a 'select_final' or 'fields' block into a list of backend column expressions.

        Args:
            field_list: List of field definitions (list-form or dict-form).
            allow_raw_sql: When False, expr: raises ValueError.

        Returns:
            List of column expressions ready for backend.select().
        """
        b = self._backend
        select_exprs = []
        i = 0
        while i < len(field_list):
            config = field_list[i]

            # Multi-condition dict form: {source, target, ops: [{when, then}, ..., {else}]}
            if isinstance(config, dict):
                source_col = config.get("source")
                target_col = config["target"]
                ops = config.get("ops", [])
                c = b.col(source_col) if source_col else None

                when_conditions = [op for op in ops if "when" in op]
                else_op = next((op for op in ops if "else" in op), None)

                if when_conditions:
                    pairs = [
                        (
                            b.apply_op(c, _parse_op(f"when:{wc['when']}"), allow_raw_sql),
                            b.apply_op(c, _parse_op(wc["then"]), allow_raw_sql),
                        )
                        for wc in when_conditions
                    ]
                    otherwise_val = (
                        b.apply_op(c, _parse_op(else_op["else"]), allow_raw_sql)
                        if else_op else b.lit(None)
                    )
                    result_col = b.when_chain(pairs, otherwise_val)
                    select_exprs.append(result_col.alias(target_col))
                else:
                    for op_str in ops:
                        c = b.apply_op(c, _parse_op(op_str if isinstance(op_str, str) else str(op_str)), allow_raw_sql)
                    if c is not None:
                        select_exprs.append(c.alias(target_col))
                i += 1
                continue

            source_col = config[0]
            target_col = config[1]
            operations = config[2] if len(config) > 2 else []

            c = b.col(source_col) if source_col else None

            # Compact when/then/else chain — validated at load time, guarded here too
            if operations and isinstance(operations[0], str) and operations[0].startswith("when:"):
                if len(operations) != 3:
                    raise ValueError(
                        f"Compact when/then/else chain for target '{target_col}' must have exactly "
                        f"3 elements [when:..., then:..., else:...], got {len(operations)}: {operations}. "
                        "Use load_schema() to catch this at schema load time."
                    )
                if not isinstance(operations[1], str) or not operations[1].startswith("then:"):
                    raise ValueError(
                        f"Element [1] of compact when/then/else for '{target_col}' must start with "
                        f"'then:', got '{operations[1]}'."
                    )
                if not isinstance(operations[2], str) or not operations[2].startswith("else:"):
                    raise ValueError(
                        f"Element [2] of compact when/then/else for '{target_col}' must start with "
                        f"'else:', got '{operations[2]}'."
                    )
                cond_col = b.apply_op(c, _parse_op(operations[0]), allow_raw_sql)
                then_col = b.apply_op(c, _parse_op(operations[1]), allow_raw_sql)
                else_col = b.apply_op(c, _parse_op(operations[2]), allow_raw_sql)
                c = b.otherwise(b.when(cond_col, then_col), else_col)
            else:
                for op_str in operations:
                    c = b.apply_op(c, _parse_op(op_str), allow_raw_sql)

            if c is not None:
                select_exprs.append(c.alias(target_col))
            i += 1
        return select_exprs

    def _apply_business_rules(self, df: Any, rules_list: list, fuse_rules: bool = True) -> Any:
        """
        Apply a list of registered business rules to a DataFrame.

        When fuse_rules=True (default), consecutive projection rules are fused into
        a single select() call by RulePlanner/RuleExecutor. Aggregation rules sharing
        the same groupBy keys are fused into one groupBy().agg(). Transform rules run
        sequentially.

        Args:
            df: Input DataFrame.
            rules_list: List of rule names registered in RuleRegistry.
            fuse_rules: Set False to run rules sequentially without fusion.

        Returns:
            Transformed DataFrame.
        """
        if not rules_list:
            return df

        # In interactive mode, surface rule redundancy warnings once per call.
        if not self._context.is_job_execution:
            try:
                analyzer = RuleAnalyzer()
                profiles = analyzer.analyze_rules(rules_list)
                for w in analyzer.detect_warnings(profiles):
                    logger.warning("[RuleAnalyzer] %s: %s", w.code, w.message)
            except Exception:
                pass  # analysis is best-effort; never block execution

        if not fuse_rules:
            for rule_name in rules_list:
                logger.info("   -> [Rule] Applying: %s", rule_name)
                rule_spec = RuleRegistry.get_rule(rule_name)
                result = rule_spec.func(df)
                if rule_spec.kind == "projection":
                    if not isinstance(result, dict):
                        raise TypeError(
                            f"Rule '{rule_name}' is declared as kind='projection' but returned "
                            f"{type(result).__name__} instead of dict[str, Column]. "
                            "Either return a dict or change kind to 'transform'."
                        )
                    for col_name, col_expr in result.items():
                        df = df.withColumn(col_name, col_expr)
                else:
                    df = result
            return df

        planner = RulePlanner()
        executor = RuleExecutor()
        stages = planner.plan(rules_list)

        for stage in stages:
            for name in [s.name for s in stage.rules]:
                logger.info("   -> [Rule] Applying: %s", name)
            df = executor.execute(df, [stage])

        return df

    _PARTIAL_MODES = ("inline", "temp_view", "table")

    def _expand_partials(self, schema_dict: dict, intermediate_mode: str, dfs: dict) -> None:
        """Execute each nested partial and inject its output DataFrame under its alias.

        Runs before source loading. The child schema goes through the full
        ``process_schema`` path (recursively, same ``intermediate_mode``) and never
        writes a final table — the write lives in ``run_process_to_table``, not here.
        A partial alias already present in ``dfs`` (provided via ``dataframes_in``) is
        left untouched, letting callers override a partial with a pre-built DataFrame.
        """
        partials = schema_dict.get("partials")
        if not partials:
            return
        if intermediate_mode not in self._PARTIAL_MODES:
            raise ValueError(
                f"[partials] Unknown intermediate_mode '{intermediate_mode}'. "
                f"Valid modes: {list(self._PARTIAL_MODES)}"
            )
        for p in partials:
            alias = p["alias"]
            if alias in dfs:
                logger.info("    -> [Partial] '%s' provided by caller — skipping execution.", alias)
                continue
            logger.info(
                "    -> [Partial] Executing sub-transformation '%s' (mode=%s)",
                alias, intermediate_mode,
            )
            child_df = self.process_schema(p["schema"], intermediate_mode=intermediate_mode)
            dfs[alias] = self._materialize_partial(child_df, alias, intermediate_mode)

    #: Schema that holds materialized partial tables (``table`` mode), sandbox-suffixed.
    _PARTIAL_SCHEMA = "skifer_partials"

    def _partial_debug_name(self, alias: str) -> str:
        """Deterministic, sandbox-aware identifier for a partial's temp view."""
        suffix = getattr(self._context, "schema_suffix", "") or ""
        return f"{alias}_{suffix}" if suffix else alias

    def _partial_table_target(self, alias: str) -> tuple[str, str]:
        """Deterministic, env-aware (schema_fqn, table_fqn) for ``table`` mode.

        Writes into a dedicated intermediate schema suffixed with the sandbox suffix
        so concurrent interactive runs never collide. The partial ``alias`` is the
        table name — no ``debug_name`` required.
        """
        ctx = self._context
        suffix = getattr(ctx, "schema_suffix", "") or ""
        schema = f"{self._PARTIAL_SCHEMA}{suffix}"
        catalog = getattr(ctx, "db", None)
        schema_fqn = f"{catalog}.{schema}" if catalog else schema
        table_fqn = self._backend.build_fqn(catalog, schema, alias)
        return schema_fqn, table_fqn

    def _materialize_partial(self, df: Any, alias: str, mode: str) -> Any:
        """Apply the materialization mode to a partial's output DataFrame.

        ``inline`` keeps the DataFrame in memory (no storage). ``temp_view``
        registers a session-scoped view for interactive inspection.
        ``table`` writes a physical intermediate table with a deterministic,
        env-aware FQN for restartability / perf inspection. In every mode the
        parent keeps using the DataFrame directly.
        """
        if mode == "inline":
            return df
        if mode == "temp_view":
            view_name = self._partial_debug_name(alias)
            self._backend.register_temp_view(df, view_name)
            logger.info("    -> [Partial] Registered temp view '%s' for partial '%s'.", view_name, alias)
            return df
        if mode == "table":
            schema_fqn, table_fqn = self._partial_table_target(alias)
            self._backend.ensure_schema_exists(schema_fqn)
            self._backend.write_table(df, table_fqn)
            logger.info("    -> [Partial] Materialized table '%s' for partial '%s'.", table_fqn, alias)
            return df
        raise ValueError(
            f"[partials] Unknown intermediate_mode '{mode}' (partial '{alias}')."
        )

    def _streaming_preflight(self, schema_dict: dict, intermediate_mode: str) -> None:
        """
        Run-time streaming checks (Plan 27) that load-time validation cannot do.

        - ``intermediate_mode`` is a run param (invisible at load): ``table``
          would batch-write mid-pipeline — forbidden with streaming sources.
        - Rule kinds live in the RuleRegistry (requires user imports): reject
          ``aggregation`` rules — streaming aggregations need watermark support.
        - Re-check the per-table incompatibilities for hand-built schema dicts
          that bypassed ``load_schema`` (limit/window ops are illegal on streams).
        """
        if intermediate_mode == "table":
            raise ValueError(
                "[streaming] intermediate_mode='table' is incompatible with streaming "
                "sources — partials would batch-write mid-pipeline. Use 'inline' or "
                "'temp_view'."
            )

        agg_rules = []
        for rule_entry in schema_dict.get("business_rules", []):
            if not isinstance(rule_entry, str):
                continue
            try:
                kind = RuleRegistry.get_rule(rule_entry).kind
            except ValueError:
                continue  # unregistered — the rules step raises its own clear error
            if kind == "aggregation":
                agg_rules.append(rule_entry)
        if agg_rules:
            raise ValueError(
                f"[streaming] aggregation rule(s) {agg_rules} cannot run on a streaming "
                "pipeline — streaming aggregations require watermark support (planned "
                "for a later plan). Aggregate downstream in a batch pipeline (or a "
                "future materialized view) reading this streaming table."
            )

        if schema_dict.get("aggregate"):
            raise ValueError(
                "[streaming] the 'aggregate' block cannot run on a streaming pipeline — "
                "streaming aggregations require watermark support (planned for a later "
                "plan). Aggregate downstream in a batch pipeline or a materialized view "
                "reading this streaming table."
            )

        for t in schema_dict.get("tables", []):
            if not t.get("streaming"):
                continue
            label = t.get("alias") or t.get("name", "?")
            if t.get("dev_limit") or schema_dict.get("dev_limit"):
                raise ValueError(
                    f"[streaming] table '{label}': 'dev_limit' is incompatible with "
                    "streaming sources — limit() is unsupported on streaming DataFrames."
                )
            if "qualify" in (t.get("preprocess") or {}):
                raise ValueError(
                    f"[streaming] table '{label}': 'preprocess.qualify' is incompatible "
                    "with streaming sources — row_number() windows are unsupported."
                )
            if (t.get("quality_checks") or {}).get("drop_duplicates_on"):
                raise ValueError(
                    f"[streaming] table '{label}': 'drop_duplicates_on' is unbounded-state "
                    "on a stream. Use 'materialization: {type: streaming_table, "
                    "write_mode: upsert, keys: [...]}' instead."
                )

    def process_schema(
        self,
        schema_dict: dict,
        dataframes_in: dict | None = None,
        intermediate_mode: str = "inline",
    ) -> Any:
        """
        Main pipeline logic: partials → load sources → filters → joins → rules → select.

        Args:
            schema_dict: Normalized pipeline schema dict.
            dataframes_in: Pre-loaded DataFrames keyed by alias (bypass loading).
            intermediate_mode: Materialization mode for nested ``partials:``
                ("inline" | "temp_view" | "table"). Global run-scoped knob; defaults
                to "inline" (no durable storage). Propagated to nested partials.

        Returns:
            Processed DataFrame.

        Raises:
            ValueError: If no tables can be loaded.
        """
        logger.info("Processing schema logic...")
        b = self._backend
        ctx = self._context
        dfs = dataframes_in.copy() if dataframes_in else {}

        # Streaming preflight (Plan 27) — run-time checks the loader cannot do.
        if any(t.get("streaming") for t in schema_dict.get("tables", [])):
            self._streaming_preflight(schema_dict, intermediate_mode)

        # 0. PARTIALS — execute nested sub-transformations, expose them under their alias.
        self._expand_partials(schema_dict, intermediate_mode, dfs)

        allow_raw_sql = ctx.env_config().get("allow_raw_sql", True)

        # 1. LOADERS & SOURCES
        for t in schema_dict.get("tables", []):
            alias = t.get("alias", t["name"])

            if t["name"] in dfs:
                if alias != t["name"]:
                    dfs[alias] = dfs[t["name"]]
            else:
                logger.info("    -> [Load] Loading table/source: %s", t["name"])

                is_streaming_table = bool(t.get("streaming"))
                if t.get("source_type") == "loader":
                    loader_func = RuleRegistry.get_loader(t["function_name"])
                    df = loader_func(ctx.config, backend=b, **t.get("arguments", {}))
                elif "source" in t:
                    src_conf = t["source"]
                    logger.info(
                        "    -> [Load] Reading external source: %s (%s @ %s)%s",
                        t["name"], src_conf["type"], src_conf["path"],
                        " [stream]" if is_streaming_table else "",
                    )
                    read_source = b.read_source_stream if is_streaming_table else b.read_source
                    df = read_source(
                        source_type=src_conf["type"],
                        path=src_conf["path"],
                        options=src_conf.get("options", {}),
                    )
                else:
                    actual_table_name = self.resolve_source_table(t["name"])
                    if is_streaming_table:
                        logger.info("    -> [Load] Reading as stream: %s", actual_table_name)
                        df = b.read_table_stream(actual_table_name)
                    else:
                        df = b.read_table(actual_table_name)

                # Filters — filters are already normalized dicts by schema_loader
                if "filter" in t:
                    from skifer.core.ir import ParsedFilter
                    pf_list = [ParsedFilter(d["column"], d["operator"], d.get("value")) for d in t["filter"]]
                    if pf_list:
                        cond = reduce(lambda a, c: a & c, [b.build_filter(pf, allow_raw_sql) for pf in pf_list])
                        df = b.filter(df, cond)

                # Preprocess qualify
                if "preprocess" in t and "qualify" in t["preprocess"]:
                    p = t["preprocess"]["qualify"]
                    df = b.row_number_over(df, p["partition_by"], p["order_by"])
                    df = b.filter(df, b.col("_rn") == 1)
                    df = b.drop_columns(df, ["_rn"])

                # Quality checks
                if "quality_checks" in t:
                    qc = t["quality_checks"]
                    if "drop_nulls_in" in qc:
                        cols = qc["drop_nulls_in"]
                        logger.info("    -> [QualityCheck] drop_nulls_in: %s", cols)
                        df = b.drop_nulls(df, cols)
                    if "drop_duplicates_on" in qc:
                        cols = qc["drop_duplicates_on"]
                        logger.info("    -> [QualityCheck] drop_duplicates_on: %s", cols)
                        df = b.drop_duplicates(df, cols)

                # filter_groups (OR-of-ANDs)
                # Groups may be string-form when passed raw; normalize defensively.
                if "filter_groups" in t:
                    from skifer.core.ir import ParsedFilter
                    from skifer.core.schema_loader import _normalize_filters
                    group_conditions = []
                    for group in t["filter_groups"]:
                        if group and isinstance(group[0], str):
                            group = _normalize_filters(group)
                        pf_list = [ParsedFilter(d["column"], d["operator"], d.get("value")) for d in group]
                        if pf_list:
                            gc = reduce(lambda a, c: a & c, [b.build_filter(pf, allow_raw_sql) for pf in pf_list])
                            group_conditions.append(gc)
                    if group_conditions:
                        combined = reduce(lambda a, c: a | c, group_conditions)
                        df = b.filter(df, combined)

                # dev_limit AFTER filters (non-prod interactive only)
                dev_limit = t.get("dev_limit") or schema_dict.get("dev_limit")
                if dev_limit and not ctx.is_job_execution:
                    if not ctx.is_production:
                        logger.debug(
                            "    -> [Dev] dev_limit=%s applied on %s after filters.", dev_limit, t["name"]
                        )
                        df = b.limit(df, dev_limit)

                dfs[alias] = df

            # Field selection at source
            if "fields" in t:
                dfs[alias] = b.select(dfs[alias], self.get_select_expressions(t["fields"], allow_raw_sql))

        # 2. JOINS
        if not dfs:
            raise ValueError("No tables loaded to process.")

        base_alias = schema_dict.get("join", [{}])[0].get("table_from") if "join" in schema_dict else list(dfs.keys())[0]
        if isinstance(base_alias, list):
            base_alias = base_alias[0]
        if base_alias not in dfs:
            base_alias = list(dfs.keys())[0]

        df_main = dfs[base_alias]

        if "join" in schema_dict:
            for j in schema_dict["join"]:
                table_from = j["table_from"]
                table_to = j["table_to"]
                if isinstance(table_from, list):
                    alias_l, key_l = table_from[0], table_from[1]
                else:
                    alias_l = table_from
                    key_l = j.get("on_from")
                if isinstance(table_to, list):
                    alias_r, key_r = table_to[0], table_to[1]
                else:
                    alias_r = table_to
                    key_r = j.get("on_to")

                on_l = key_l if isinstance(key_l, list) else [key_l]
                on_r = key_r if isinstance(key_r, list) else [key_r]
                join_type = _normalise_join_type(j.get("type", "left"))

                logger.info("    -> [Join] %s JOIN %s(%s) -> %s(%s)", join_type.upper(), alias_l, on_l, alias_r, on_r)
                df_to = dfs[alias_r]

                if on_l == on_r:
                    df_main = b.join(df_main, df_to, on=on_l, how=join_type)
                else:
                    cond = reduce(lambda x, y: x & y, [df_main[lk] == df_to[rk] for lk, rk in zip(on_l, on_r)])
                    df_main = b.join(df_main, df_to, cond, join_type)
                    if join_type not in _LEFT_ONLY_JOIN_TYPES:
                        df_main = b.drop_columns(df_main, [df_to[r] for r in on_r])

        # 3. BUSINESS RULES
        if "business_rules" in schema_dict:
            df_main = self._apply_business_rules(df_main, schema_dict["business_rules"])

        # 4. SELECT FINAL / keep_all_columns + add_columns
        if schema_dict.get("keep_all_columns"):
            if "select_final" in schema_dict:
                raise ValueError("'keep_all_columns' and 'select_final' are mutually exclusive.")
            df_main = self._apply_add_columns(df_main, schema_dict.get("add_columns"), allow_raw_sql)
        elif "select_final" in schema_dict:
            df_main = b.select(df_main, self.get_select_expressions(schema_dict["select_final"], allow_raw_sql))
        elif "aggregate" in schema_dict:
            # add_columns run before the aggregation so they can feed group_by keys.
            df_main = self._apply_add_columns(df_main, schema_dict.get("add_columns"), allow_raw_sql)

        # 5. AGGREGATE (Plan 28) — declarative GROUP BY, terminal step
        if "aggregate" in schema_dict:
            df_main = self._apply_aggregate(df_main, schema_dict["aggregate"], allow_raw_sql)

        return df_main

    def _apply_add_columns(self, df: Any, add_columns: list | None, allow_raw_sql: bool) -> Any:
        """Append each ``add_columns`` entry to *df* without dropping existing columns."""
        if not add_columns:
            return df
        for field_def in add_columns:
            target_col = field_def["target"] if isinstance(field_def, dict) else field_def[1]
            exprs = self.get_select_expressions([field_def], allow_raw_sql)
            if exprs:
                df = self._backend.with_column(df, target_col, exprs[0])
        return df

    def _apply_aggregate(self, df: Any, aggregate: dict, allow_raw_sql: bool) -> Any:
        """Apply the normalized ``aggregate:`` block — groupBy/agg then HAVING."""
        from skifer.core.ir import ParsedFilter

        b = self._backend
        keys = aggregate["group_by"]
        measures = aggregate["measures"]
        logger.info("    -> [Aggregate] group_by=%s, %d measure(s)", keys, len(measures))

        exprs = {m["target"]: b.agg_expr(m["func"], m["source"]) for m in measures}
        df = b.group_by_agg(df, keys, exprs)

        having = aggregate.get("having")
        if having:
            pf_list = [ParsedFilter(h["column"], h["operator"], h.get("value")) for h in having]
            cond = reduce(lambda a, c: a & c, [b.build_filter(pf, allow_raw_sql) for pf in pf_list])
            df = b.filter(df, cond)
        return df
