"""
PipelinePatterns — orchestration patterns extracted from SkiferEngine (plan17-2.4).

Each method corresponds to one run_* pattern on the engine; the engine delegates
to self._patterns.<method>() so external callers see no change in the public API.
"""
from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class PipelinePatterns:
    """
    Orchestration patterns: load → process → write lifecycle for each pipeline shape.

    Args:
        engine: The SkiferEngine instance (provides _backend, _interpreter, helpers).
    """

    def __init__(self, engine: Any) -> None:
        self._engine = engine

    @staticmethod
    def _is_streaming_schema(schema_dict: dict) -> bool:
        """True when the schema declares streaming sources or materialization (Plan 27)."""
        mat = schema_dict.get("materialization")
        if mat and mat.get("type") == "streaming_table":
            return True
        return any(t.get("streaming") for t in schema_dict.get("tables", []))

    @staticmethod
    def _is_materialized_view_schema(schema_dict: dict) -> bool:
        """True when the schema declares ``materialization: materialized_view`` (Plan 28)."""
        mat = schema_dict.get("materialization")
        return bool(mat and mat.get("type") == "materialized_view")

    # ------------------------------------------------------------------
    # run_process_to_table
    # ------------------------------------------------------------------

    def run_process_to_table(
        self,
        schema_dict: dict,
        target_layer: str,
        target_table_name: str,
        intermediate_mode: str = "inline",
        run_id: str | None = None,
    ) -> None:
        e = self._engine
        sink_config = schema_dict.get("sink")
        uses_jdbc_sink = bool(sink_config and sink_config.get("type") in ("postgres", "jdbc"))
        actual_schema = e.get_target_schema(target_layer)
        fqn = e._build_fqn(actual_schema, target_table_name)

        # Streaming materialization (Plan 27): resolve the checkpoint BEFORE any
        # read so a missing checkpoint_base fails fast without touching sources.
        materialization = schema_dict.get("materialization")
        is_streaming = bool(materialization and materialization.get("type") == "streaming_table")

        has_data_product = schema_dict.get("data_product") is not None
        if has_data_product:
            if uses_jdbc_sink or is_streaming or self._is_materialized_view_schema(schema_dict):
                raise ValueError(
                    "Certified publication (schema declares 'data_product') does not support "
                    "streaming, JDBC sinks, or materialized views yet."
                )
            if e.certification_store is None or getattr(e, "monitor", None) is None:
                raise ValueError(
                    "Schema declares 'data_product' but SkiferEngine was built without "
                    "certification_store and/or monitor — pass both to enable certified publication."
                )

        # Materialized view (Plan 28): defined by SQL, so the DataFrame pipeline
        # is short-circuited entirely — no source is ever read here.
        if self._is_materialized_view_schema(schema_dict):
            logger.info(
                "--- Executing Pattern: process_to_table [materialized_view] (Target: %s) ---",
                target_table_name,
            )
            executed = e._create_materialized_view(
                schema_dict, actual_schema, target_table_name
            )
            if executed and getattr(e, "monitor", None) is not None:
                logger.info("   -> [Monitor] Running post-create quality checks on '%s'...", fqn)
                report = e.monitor.check_from_schema(fqn, schema_dict, raise_on_critical=True)
                summary = report.summary()
                logger.info(
                    "   -> [Monitor] %s — %s/%s checks passed.",
                    summary.get("status", "PASS"), summary["passed"], summary["total_checks"],
                )
            logger.info("--- Pattern 'process_to_table' completed. ---")
            return

        if is_streaming:
            materialization = {
                **materialization,
                "_resolved_checkpoint": e.resolve_checkpoint_location(
                    actual_schema, target_table_name, materialization
                ),
            }
            logger.info(
                "--- Executing Pattern: process_to_table [streaming, %s] (Target: %s, checkpoint: %s) ---",
                materialization.get("write_mode", "append"),
                target_table_name,
                materialization["_resolved_checkpoint"],
            )
        else:
            logger.info("--- Executing Pattern: process_to_table (Target: %s) ---", target_table_name)

        if not uses_jdbc_sink:
            e._ensure_schema_exists(actual_schema)

        df = e.process_schema(schema_dict, intermediate_mode=intermediate_mode)

        if has_data_product:
            from skifer.core.ir import parse_to_ir
            from skifer.observability.certification import canonicalize_contract
            from skifer.observability.checks import DataQualityError
            from skifer.observability.publication import PublicationCoordinator

            definition = canonicalize_contract(parse_to_ir(schema_dict))
            coordinator = PublicationCoordinator(
                e._get_backend(),
                e.monitor,
                e.certification_store,
                metadata_store=getattr(e, "metadata_store", None),
            )
            # Same identity from the pipeline down to the certification record,
            # so the link survives without exported traces (Plan 29).
            result = coordinator.publish(df, fqn, schema_dict, definition, run_id=run_id)
            if result.state == "QUARANTINED":
                raise DataQualityError(result.report)
            logger.info(
                "   -> [Certified Publication] run=%s state=%s (target: %s)",
                result.run.run_id, result.state, fqn,
            )
            if result.state == "PROMOTED":
                _index_promoted_metadata(e, schema_dict, fqn, result.run.run_id)
        else:
            e._write_dataframe(
                df,
                fqn,
                target_table_name,
                sink_config={
                    **sink_config,
                    "schema": sink_config.get("schema") or target_layer,
                    "table": sink_config.get("table") or target_table_name,
                } if uses_jdbc_sink else sink_config,
                materialization=materialization,
            )

            # With trigger available_now the streaming write has terminated here
            # (awaitTermination inside write_stream_table) — the monitor reads
            # complete data. interval: triggers block above and never reach this.
            if getattr(e, "monitor", None) is not None and schema_dict and not uses_jdbc_sink:
                logger.info("   -> [Monitor] Running post-write quality checks on '%s'...", fqn)
                report = e.monitor.check_from_schema(fqn, schema_dict, raise_on_critical=True)
                summary = report.summary()
                status = summary.get("status", "PASS")
                logger.info(
                    "   -> [Monitor] %s — %s/%s checks passed.",
                    status, summary["passed"], summary["total_checks"],
                )

        logger.info("--- Pattern 'process_to_table' completed. ---")

    # ------------------------------------------------------------------
    # run_process_and_split
    # ------------------------------------------------------------------

    def run_process_and_split(
        self,
        schema_dict: dict,
        split_values: list,
        target_layer: str,
        target_base_name: str,
        split_column: str,
    ) -> None:
        e = self._engine
        sink_config = schema_dict.get("sink")
        if sink_config and sink_config.get("type") in ("postgres", "jdbc"):
            raise NotImplementedError(
                "run_process_and_split does not support JDBC sinks. "
                "Use run_process_to_table for JDBC sink writes."
            )
        if self._is_materialized_view_schema(schema_dict):
            raise NotImplementedError(
                "run_process_and_split does not support materialized views — a view is "
                "one SQL definition, it cannot fan out into N targets. Declare one "
                "materialized view per slice (each with its own filter), or split "
                "downstream in batch."
            )
        if self._is_streaming_schema(schema_dict):
            raise NotImplementedError(
                "run_process_and_split does not support streaming schemas (cache() and "
                "fan-out writes are illegal on a streaming query). Instead, write one "
                "streaming pivot table, then N downstream streaming pipelines each "
                "filtering their slice (filter is stream-safe, one checkpoint per branch)."
            )

        logger.info(
            "--- Executing Pattern: process_and_split (Base: %s, Column: %s) ---",
            target_base_name, split_column,
        )
        df_full = e.process_schema(schema_dict)
        actual_schema = e.get_target_schema(target_layer)

        cached = False
        try:
            df_full = e._get_backend().cache(df_full)
            cached = True
        except Exception:
            logger.warning("   -> [Cache] cache() failed. Continuing without caching.")

        for item in split_values:
            fqn = e._build_fqn(actual_schema, f"{target_base_name}_{item['label']}")
            e._ensure_schema_exists(actual_schema)
            logger.info(
                "  -> Processing split value: %s (%s = '%s')",
                item["label"], split_column, item["value"],
            )
            df_slice = e._get_backend().filter(
                df_full, e._get_backend().col(split_column) == item["value"]
            )
            e._write_dataframe(df_slice, fqn, item["label"])

        if cached:
            e._get_backend().unpersist(df_full)

        logger.info("--- Pattern 'process_and_split' completed. ---")

    # ------------------------------------------------------------------
    # run_union_sources_to_table
    # ------------------------------------------------------------------

    def run_union_sources_to_table(
        self,
        schema_dict: dict,
        source_partitions: list,
        source_layer: str,
        target_layer: str,
        target_table_name: str,
        source_base_names: list,
        source_alias: str,
        dedup_after_union: bool = True,
    ) -> None:
        e = self._engine
        sink_config = schema_dict.get("sink")
        if sink_config and sink_config.get("type") in ("postgres", "jdbc"):
            raise NotImplementedError(
                "run_union_sources_to_table does not support JDBC sinks. "
                "Use run_process_to_table for JDBC sink writes."
            )
        if self._is_materialized_view_schema(schema_dict):
            raise NotImplementedError(
                "run_union_sources_to_table does not support materialized views — this "
                "pattern discovers its sources at run time and injects a DataFrame, "
                "which has no place in a persisted SQL definition. List the sources "
                "explicitly in 'tables:' (UNION ALL compilation is planned for a "
                "later plan)."
            )
        if self._is_streaming_schema(schema_dict):
            raise NotImplementedError(
                "run_union_sources_to_table does not support streaming schemas — "
                "multi-source streaming union is a materialized-table use case "
                "(planned for a later plan)."
            )

        actual_schema_source = e.get_target_schema(source_layer)
        actual_schema_target = e.get_target_schema(target_layer)
        fqn = e._build_fqn(actual_schema_target, target_table_name)

        logger.info("--- Executing Pattern: union_sources_to_table (Target: %s) ---", target_table_name)
        e._ensure_schema_exists(actual_schema_target)

        list_of_dfs = []
        missing_tables: list[str] = []
        for base in source_base_names:
            for partition in source_partitions:
                source_fqn = e._build_fqn(actual_schema_source, f"{base}_{partition['label']}")
                # Parse schema/table from fqn for table_exists check
                clean = source_fqn.replace("`", "")
                parts = clean.split(".")
                schema_part = parts[-2] if len(parts) >= 2 else actual_schema_source
                table_part = parts[-1]
                catalog_part = parts[-3] if len(parts) >= 3 else None
                if not e._get_backend().table_exists(catalog_part, schema_part, table_part):
                    missing_tables.append(source_fqn)
                    continue
                list_of_dfs.append(e._get_backend().read_table(source_fqn))

        if missing_tables:
            logger.warning("Skipped genuinely absent tables: %s", missing_tables)

        if not list_of_dfs:
            raise ValueError("No sources found to union.")

        logger.info(" -> [Union] Merging %d tables...", len(list_of_dfs))
        unioned_df = e._get_backend().union_by_name(list_of_dfs)
        if dedup_after_union:
            unioned_df = e._get_backend().drop_duplicates(unioned_df)
            logger.info("-> [Union] dropDuplicates enabled — duplicates removed.")
        else:
            logger.info("-> [Union] dropDuplicates disabled — keeping all rows.")

        df_final = e.process_schema(schema_dict, dataframes_in={source_alias: unioned_df})
        e._write_dataframe(df_final, fqn, target_table_name)
        logger.info("--- Pattern 'union_sources_to_table' completed. ---")

    # ------------------------------------------------------------------
    # run_from_yaml
    # ------------------------------------------------------------------

    def run_from_yaml(
        self,
        yaml_path: str,
        target_layer: str,
        target_table_name: str | None = None,
        params: dict | None = None,
        run_id: str | None = None,
    ) -> None:
        from skifer.core.schema_loader import load_schema
        e = self._engine
        if target_table_name is None:
            target_table_name = os.path.splitext(os.path.basename(yaml_path))[0]
        merged = {**e.default_params, **(params or {})}
        # The span lives here rather than in a traced copy of this method on the
        # engine: one implementation, instrumented in place.
        with e._trace_span("skifer.schema.load"):
            schema_dict = load_schema(yaml_path, params=merged)
        # Run-global materialization knob for nested partials (default: inline).
        intermediate_mode = merged.get("intermediate_mode", "inline")
        self.run_process_to_table(
            schema_dict, target_layer, target_table_name,
            intermediate_mode=intermediate_mode, run_id=run_id,
        )


def _index_promoted_metadata(e: Any, schema_dict: dict, fqn: str, run_id: str) -> None:
    """Best-effort metadata indexing hook for certified publication."""
    store = getattr(e, "metadata_store", None)
    if store is None:
        return
    try:
        from skifer.observability.metadata_index import (
            index_schema,
            upsert_index_record,
        )

        path_hint = _schema_path_hint(schema_dict, fqn)
        record = index_schema(
            schema_dict,
            path_hint,
            target_fqn=fqn,
            last_run_id=run_id,
        )
        record = _inherit_registry_classifications(store, record)
        upsert_index_record(store, record)
    except Exception as exc:
        logger.warning("   -> [Metadata] SYNC_ERROR indexing skipped (non-blocking): %s", exc)


def _inherit_registry_classifications(store: Any, record: Any) -> Any:
    """Enrich undeclared output classifications from indexed source columns."""
    from dataclasses import replace

    from skifer.core.constants import CLASSIFICATION_RANK
    from skifer.lineage.classification import resolve_field_classifications
    from skifer.lineage.tracker import LineageGraph

    graph = LineageGraph.from_dict(record.lineage)
    source_records: dict[str, Any] = {}
    inherited: dict[str, str] = {}
    for output_column in record.columns:
        if output_column.classification is not None:
            continue
        column_graph = LineageGraph()
        source_classifications: dict[str, str] = {}
        for edge in graph.upstream(record.target_fqn, output_column.name):
            column_graph.add_edge(edge)
            if edge.source_table not in source_records:
                try:
                    source_records[edge.source_table] = store.get(edge.source_table)
                except Exception:
                    source_records[edge.source_table] = None
            source_record = source_records[edge.source_table]
            if source_record is None:
                continue
            source_column = next(
                (
                    column
                    for column in source_record.columns
                    if column.name == edge.source_column
                ),
                None,
            )
            if source_column is None or source_column.classification is None:
                continue
            existing = source_classifications.get(edge.source_column)
            if existing is None or (
                CLASSIFICATION_RANK[source_column.classification]
                > CLASSIFICATION_RANK[existing]
            ):
                source_classifications[edge.source_column] = source_column.classification

        if not source_classifications:
            continue
        inherited.update(
            resolve_field_classifications(
                column_graph,
                record.target_fqn,
                {},
                source_classifications,
                mode="warn",
            )
        )

    if not inherited:
        return record
    return replace(
        record,
        columns=tuple(
            replace(column, classification=inherited[column.name])
            if column.classification is None and column.name in inherited
            else column
            for column in record.columns
        ),
    )


def _schema_path_hint(schema_dict: dict, fqn: str) -> str:
    source_path = schema_dict.get("_source_path")
    if isinstance(source_path, str) and source_path:
        return source_path
    data_product = schema_dict.get("data_product")
    if isinstance(data_product, dict):
        product_id = data_product.get("id")
        if isinstance(product_id, str) and product_id:
            return product_id
    return fqn
