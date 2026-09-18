"""Pure capability requirements for portable pipeline execution."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from skifer.core.ir import ParsedSchema


CAP_PYTHON_RULES = "python_rules"
CAP_FILE_SOURCES = "file_sources"
CAP_LOADERS = "loaders"
CAP_STREAMING = "streaming"
CAP_MATERIALIZED_VIEW = "materialized_view"
CAP_VIEW = "view"
CAP_INCREMENTAL = "incremental"
CAP_SNAPSHOT = "snapshot"
CAP_JDBC_SINK = "jdbc_sink"
CAP_DEV_LIMIT = "dev_limit"
CAP_DROP_DUPLICATES = "drop_duplicates"
CAP_PREPROCESS_QUALIFY = "preprocess_qualify"

ALL_CAPABILITIES: frozenset[str] = frozenset(
    {
        CAP_PYTHON_RULES,
        CAP_FILE_SOURCES,
        CAP_LOADERS,
        CAP_STREAMING,
        CAP_MATERIALIZED_VIEW,
        CAP_VIEW,
        CAP_INCREMENTAL,
        CAP_SNAPSHOT,
        CAP_JDBC_SINK,
        CAP_DEV_LIMIT,
        CAP_DROP_DUPLICATES,
        CAP_PREPROCESS_QUALIFY,
    }
)
DATABRICKS_CAPABILITIES: frozenset[str] = frozenset(
    {
        CAP_PYTHON_RULES,
        CAP_FILE_SOURCES,
        CAP_LOADERS,
        CAP_STREAMING,
        CAP_MATERIALIZED_VIEW,
        CAP_VIEW,
        CAP_INCREMENTAL,
        CAP_JDBC_SINK,
        CAP_DEV_LIMIT,
        CAP_DROP_DUPLICATES,
        CAP_PREPROCESS_QUALIFY,
    }
)

IMPLEMENTED_INCREMENTAL_STRATEGIES: frozenset[str] = frozenset({"append"})


class UnsupportedCapabilityError(ValueError):
    """Raised when a pipeline requires an adapter capability that is unavailable."""


_YAML_CONSTRUCTIONS = {
    CAP_PYTHON_RULES: "business_rules:",
    CAP_FILE_SOURCES: "tables[].source:",
    CAP_LOADERS: "tables[].source_type: loader",
    CAP_STREAMING: "tables[].streaming: true",
    CAP_MATERIALIZED_VIEW: "materialization: materialized_view",
    CAP_VIEW: "materialization: view",
    CAP_INCREMENTAL: "materialization: incremental",
    CAP_SNAPSHOT: "materialization: snapshot",
    CAP_JDBC_SINK: "sink.type: jdbc or postgres",
    CAP_DEV_LIMIT: "dev_limit: or tables[].dev_limit:",
    CAP_DROP_DUPLICATES: "tables[].quality_checks.drop_duplicates_on:",
    CAP_PREPROCESS_QUALIFY: "tables[].preprocess.qualify:",
}


def required_capabilities(parsed: ParsedSchema) -> frozenset[str]:
    """Return the adapter capabilities required by one parsed pipeline."""
    required: set[str] = set()
    if parsed.business_rules:
        from skifer.core.registry import RuleRegistry

        for rule_name in parsed.business_rules:
            try:
                rule = RuleRegistry.get_rule(rule_name)
            except ValueError:
                # Unknown rules remain conservatively classified as Python;
                # the execution/compiler boundary will issue the named error.
                required.add(CAP_PYTHON_RULES)
                break
            if rule.kind != "sql":
                required.add(CAP_PYTHON_RULES)
                break
    if parsed.dev_limit:
        required.add(CAP_DEV_LIMIT)

    if parsed.partials:
        from skifer.core.ir import parse_to_ir

        for partial in parsed.partials:
            required.update(required_capabilities(parse_to_ir(partial.schema)))

    for table in parsed.tables:
        if table.is_loader:
            required.add(CAP_LOADERS)
        if table.source_type:
            required.add(CAP_FILE_SOURCES)
        if table.streaming:
            required.add(CAP_STREAMING)
        if table.dev_limit:
            required.add(CAP_DEV_LIMIT)
        if table.drop_duplicates_on:
            required.add(CAP_DROP_DUPLICATES)

    materialization = parsed.materialization or {}
    materialization_capabilities = {
        "materialized_view": CAP_MATERIALIZED_VIEW,
        "view": CAP_VIEW,
        "incremental": CAP_INCREMENTAL,
        "snapshot": CAP_SNAPSHOT,
    }
    materialization_capability = materialization_capabilities.get(
        materialization.get("type")
    )
    if materialization_capability is not None:
        required.add(materialization_capability)

    sink = parsed.sink or {}
    if sink.get("type") in ("postgres", "jdbc"):
        required.add(CAP_JDBC_SINK)

    for table in parsed.raw.get("tables", []):
        if "qualify" in (table.get("preprocess") or {}):
            required.add(CAP_PREPROCESS_QUALIFY)

    return frozenset(required)


def assert_supported(
    parsed: ParsedSchema,
    *,
    adapter_name: str,
    supported: frozenset[str],
) -> None:
    """Refuse a pipeline that requires capabilities absent from its adapter."""
    missing = sorted(required_capabilities(parsed) - supported)
    if not missing:
        materialization = parsed.materialization or {}
        if materialization.get("type") == "incremental":
            strategy = materialization.get("strategy")
            if strategy not in IMPLEMENTED_INCREMENTAL_STRATEGIES:
                supported_strategies = sorted(IMPLEMENTED_INCREMENTAL_STRATEGIES)
                raise UnsupportedCapabilityError(
                    f"Adapter '{adapter_name}' does not support "
                    f"materialization: incremental strategy {strategy!r}. "
                    f"Supported strategies: {supported_strategies}."
                )
        return
    details = "; ".join(
        f"{capability} (required by YAML '{_YAML_CONSTRUCTIONS[capability]}')"
        for capability in missing
    )
    raise UnsupportedCapabilityError(
        f"Adapter '{adapter_name}' does not support required capabilities: {details}."
    )
