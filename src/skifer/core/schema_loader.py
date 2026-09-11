"""
Schema loader — loads and normalizes pipeline schemas from YAML files or inline strings.
"""

from __future__ import annotations
from copy import deepcopy
from dataclasses import dataclass
import difflib
import logging
import re
import os
import yaml

from skifer.core.constants import (
    DEFAULT_STREAMING_TRIGGER,
    MATERIALIZATION_ALLOWED_KEYS,
    VALID_MATERIALIZATION_TYPES,
    VALID_MV_REFRESH_MODES,
    VALID_MV_SCHEDULE_PREFIXES,
    VALID_SOURCE_TYPES,
    VALID_STREAMING_SOURCE_TYPES,
)
from skifer.core.ir import _normalise_join_type
from skifer.core.op_catalog import (
    AGGREGATE_FUNCTIONS,
    FILTER_OPERATORS,
    COLUMN_OPS,
    resolve_aggregate_function,
    resolve_filter_operator,
    resolve_column_op,
    suggest,
)

logger = logging.getLogger(__name__)

VALID_SINK_TYPES: frozenset[str] = frozenset({"delta", "postgres", "jdbc"})

_AGENT_READY_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_.-]+$")
_DATA_PRODUCT_ALLOWED_KEYS = frozenset({"id", "version", "owner", "description"})
_CONTRACT_ALLOWED_KEYS = frozenset({"grain", "output"})
_OUTPUT_FIELD_ALLOWED_KEYS = frozenset(
    {"logical_type", "required", "unique", "classification", "entity", "description"}
)
_SEMANTIC_SEED_ALLOWED_KEYS = frozenset(
    {"model_key", "entity", "default_time_dimension", "dimensions"}
)


@dataclass(frozen=True)
class LocalizedIssue:
    """One schema validation issue suitable for transport-neutral clients."""

    code: str
    message: str
    path: str


def _find_file_upwards(filename, start_dir=None):
    """Search for a file upward from start_dir (default: cwd)."""
    current_dir = start_dir or os.getcwd()
    while True:
        check_path = os.path.join(current_dir, filename)
        if os.path.exists(check_path):
            return check_path
        parent = os.path.dirname(current_dir)
        if parent == current_dir:
            return None
        current_dir = parent


def _inject_params(yaml_str, params):
    """Replace {{ key }} placeholders with values from params dict."""
    if params:
        for key, value in params.items():
            pattern = r'\{\{\s*' + re.escape(str(key)) + r'\s*\}\}'
            yaml_str = re.sub(pattern, str(value) if value is not None else "", yaml_str)
    # Always check for remaining unresolved placeholders
    remaining = re.findall(r'\{\{\s*(\w+)\s*\}\}', yaml_str)
    if remaining:
        raise ValueError(
            f"Missing template parameters in schema: {remaining}. "
            f"Pass them via params={{'{remaining[0]}': ...}}"
        )
    return yaml_str


def _normalize_filter_string(filter_str):
    """
    Parse compact filter string "column:operator[:value]" into a dict.
    Example: "region:equals:EMEA" -> {column: region, operator: equals, value: EMEA}
    Example: "customer_id:is_not_null" -> {column: customer_id, operator: is_not_null}
    Example: "status:in:A,B" -> {column: status, operator: in, value: ["A", "B"]}
    """
    parts = filter_str.split(":", 2)
    if len(parts) < 2:
        raise ValueError(
            f"Invalid filter string: '{filter_str}'. Expected 'column:operator' or 'column:operator:value'."
        )

    column = parts[0].strip()
    operator = parts[1].strip()

    if len(parts) == 2:
        return {"column": column, "operator": operator}

    value_str = parts[2].strip()

    # For sql operator, keep value as-is
    if operator == "sql":
        return {"column": column, "operator": operator, "value": value_str}

    # For in/not_in, parse comma-separated values
    if operator in ("in", "not_in"):
        values = [v.strip() for v in value_str.replace(";", ",").split(",")]
        # Warn if any value has leading/trailing space (possible comma-in-value mistake)
        for v in value_str.replace(";", ",").split(","):
            if v != v.strip():
                logger.warning(
                    "Filter '%s': value '%s' has leading/trailing space. "
                    "If the value contains a comma, use the dict form instead.",
                    filter_str,
                    v,
                )
        return {"column": column, "operator": operator, "value": values}

    return {"column": column, "operator": operator, "value": value_str}


def _normalize_filter_mapping(filter_dict: dict) -> list[dict]:
    """
    Normalize a filter mapping form into a list of filter dicts.

    Supports:
      column: VALUE                       → equals VALUE
      column: not_null                    → is_not_null (bare no-arg operator)
      column: {operator: VALUE}           → operator VALUE
      column: {in: [A, B]}               → in [A, B]
    """
    result = []
    for col, val in filter_dict.items():
        if val is None:
            result.append({"column": col, "operator": "is_null"})
        elif isinstance(val, dict):
            if len(val) != 1:
                raise ValueError(
                    f"[filter mapping] Column '{col}': expected a single-key dict "
                    f"like {{operator: value}}, got {val}"
                )
            op_name, op_val = next(iter(val.items()))
            canonical = resolve_filter_operator(str(op_name))
            if canonical is None:
                raise ValueError(
                    f"[filter mapping] Column '{col}': unknown filter operator '{op_name}'."
                )
            if op_val is None:
                result.append({"column": col, "operator": canonical})
            elif isinstance(op_val, list):
                result.append({"column": col, "operator": canonical, "value": [str(v) for v in op_val]})
            else:
                result.append({"column": col, "operator": canonical, "value": str(op_val)})
        elif isinstance(val, str):
            canonical = resolve_filter_operator(val)
            spec = FILTER_OPERATORS.get(canonical) if canonical else None
            if spec is not None and spec.arity == "none":
                result.append({"column": col, "operator": canonical})
            else:
                result.append({"column": col, "operator": "equals", "value": val})
        else:
            result.append({"column": col, "operator": "equals", "value": str(val)})
    return result


def _normalize_filters(filter_list):
    """Normalize a list of filter items (strings or dicts) to dicts."""
    if not filter_list:
        return filter_list
    result = []
    for item in filter_list:
        if isinstance(item, str):
            result.append(_normalize_filter_string(item))
        else:
            result.append(item)
    return result


def _normalize_join(join_item):
    """
    Normalize compact join format:
    table_from: [alias, key] -> table_from: alias, on_from: key
    """
    normalized = dict(join_item)
    if isinstance(normalized.get("table_from"), list):
        parts = normalized["table_from"]
        normalized["table_from"] = parts[0]
        normalized["on_from"] = parts[1]
    if isinstance(normalized.get("table_to"), list):
        parts = normalized["table_to"]
        normalized["table_to"] = parts[0]
        normalized["on_to"] = parts[1]
    return normalized


def _normalize_select_entry_mapping(entry: dict) -> dict:
    """
    Normalize a new-style select_final mapping entry {from, as, ops} to {source, target, ops}.

    ops items can be:
      - string "cast:double"      → kept as-is
      - single-key dict {cast: double} → "cast:double"
      - multi-key dict {when: ..., then: ...} → kept as-is (structural when/else)
    """
    ops_normalized = []
    for op in entry.get("ops", []):
        if isinstance(op, str):
            ops_normalized.append(op)
        elif isinstance(op, dict):
            if len(op) == 1:
                op_name, op_arg = next(iter(op.items()))
                ops_normalized.append(f"{op_name}:{op_arg}" if op_arg is not None else str(op_name))
            else:
                ops_normalized.append(op)  # multi-key: when/then/else structural dict
        else:
            ops_normalized.append(str(op))
    return {"source": entry.get("from"), "target": entry.get("as", "?"), "ops": ops_normalized}


def _normalize_select_final(select_list):
    """Normalize 2-element rows [src, tgt] -> [src, tgt, []] and new {from, as} mapping entries."""
    if not select_list:
        return select_list
    result = []
    for row in select_list:
        if isinstance(row, dict):
            if "from" in row or "as" in row:
                result.append(_normalize_select_entry_mapping(row))
            else:
                result.append(row)  # existing {source, target, ops} dict form — pass through
        elif isinstance(row, list):
            if len(row) >= 2 and isinstance(row[0], str) and row[0].startswith("literal:"):
                literal_val = row[0][len("literal:"):]
                target_col = row[1]
                extra_ops = row[2] if len(row) > 2 else []
                ops = [f"lit:{literal_val}"] + (extra_ops if isinstance(extra_ops, list) else [extra_ops])
                result.append([None, target_col, ops])
            elif len(row) == 2:
                result.append([row[0], row[1], []])
            else:
                result.append(row)
        else:
            result.append(row)
    return result


# ---------------------------------------------------------------------------
# Fail-fast operator validation helpers (Plan 17-1.2)
# ---------------------------------------------------------------------------

def _op_name_from_str(op_str: str) -> str:
    """Extract the leading op name from an op-string like 'cast:double' → 'cast'."""
    return op_str.split(":", 1)[0].strip()


def _validate_filter_op(op: str, column: str, location: str, errors: list) -> None:
    """Append an error to *errors* if *op* is not in the filter catalog."""
    if resolve_filter_operator(op) is None:
        hints = suggest(op, FILTER_OPERATORS)
        hint_str = f"  Did you mean: {hints}?" if hints else ""
        errors.append(
            f"  [{location}] column '{column}': unknown filter operator '{op}'.{hint_str}\n"
            f"  Valid operators: {sorted(FILTER_OPERATORS.keys())}"
        )


def _validate_col_op_str(op_str: str, context: str, errors: list) -> None:
    """Validate a single column op-string against COLUMN_OPS; append to *errors* on failure."""
    if not isinstance(op_str, str):
        return
    # Strip then:/else: prefix — these are structural keywords but the payload after them
    # is a nested op-string that must also be validated.
    prefix = _op_name_from_str(op_str)
    if prefix in ("then", "else"):
        payload = op_str.split(":", 1)[1] if ":" in op_str else ""
        if payload:
            _validate_col_op_str(payload, context, errors)
        return  # "then" / "else" themselves are always valid structural keywords

    # For "when:op:val" — validate "when" exists and validate the inner filter operator
    if prefix == "when":
        cond_str = op_str.split(":", 1)[1] if ":" in op_str else ""
        if cond_str:
            cond_op = _op_name_from_str(cond_str)
            if resolve_filter_operator(cond_op) is None:
                hints = suggest(cond_op, FILTER_OPERATORS)
                hint_str = f"  Did you mean: {hints}?" if hints else ""
                errors.append(
                    f"  [{context}] unknown when-condition operator '{cond_op}' in '{op_str}'.{hint_str}\n"
                    f"  Valid condition operators: {sorted(FILTER_OPERATORS.keys())}"
                )
        return  # "when" itself is always valid

    if resolve_column_op(prefix) is None:
        hints = suggest(prefix, COLUMN_OPS)
        hint_str = f"  Did you mean: {hints}?" if hints else ""
        errors.append(
            f"  [{context}] unknown column operation '{prefix}' in '{op_str}'.{hint_str}\n"
            f"  Valid operations: {sorted(COLUMN_OPS.keys())}"
        )


def _validate_compact_when_chain(ops: list, context: str, errors: list) -> None:
    """Validate that a compact when/then/else ops list is exactly [when:*, then:*, else:*]."""
    if not (isinstance(ops, list) and ops and isinstance(ops[0], str) and ops[0].startswith("when:")):
        return
    if len(ops) != 3:
        errors.append(
            f"  [{context}] compact when/then/else chain must have exactly 3 elements "
            f"[when:..., then:..., else:...], got {len(ops)}: {ops}"
        )
        return
    if not isinstance(ops[1], str) or not ops[1].startswith("then:"):
        errors.append(
            f"  [{context}] element [1] of compact when/then/else must start with 'then:', got '{ops[1]}'"
        )
    if not isinstance(ops[2], str) or not ops[2].startswith("else:"):
        errors.append(
            f"  [{context}] element [2] of compact when/then/else must start with 'else:', got '{ops[2]}'"
        )


def _validate_ops_in_select_list(select_list: list, section_name: str, errors: list) -> None:
    """Validate all op-strings within a select_final / add_columns list."""
    if not select_list:
        return
    for row in select_list:
        if isinstance(row, list):
            ops = row[2] if len(row) > 2 else []
            context = f"{section_name}[target={row[1] if len(row) > 1 else '?'}]"
            # Compact when/then/else structural check (before per-op validation)
            _validate_compact_when_chain(ops if isinstance(ops, list) else [], context, errors)
            for op_str in (ops if isinstance(ops, list) else [ops]):
                _validate_col_op_str(op_str, context, errors)
        elif isinstance(row, dict):
            target = row.get("target", "?")
            context = f"{section_name}[target={target}]"
            for op_entry in row.get("ops", []):
                if isinstance(op_entry, str):
                    _validate_col_op_str(op_entry, context, errors)
                elif isinstance(op_entry, dict):
                    # Dict form: {when: "equals:DONE", then: "lit:Paid"} or {else: "lit:Unknown"}
                    for key, val in op_entry.items():
                        if key == "when":
                            # Validate inner filter condition
                            cond_op = _op_name_from_str(str(val))
                            if resolve_filter_operator(cond_op) is None:
                                hints = suggest(cond_op, FILTER_OPERATORS)
                                hint_str = f"  Did you mean: {hints}?" if hints else ""
                                errors.append(
                                    f"  [{context}] unknown when-condition operator '{cond_op}' "
                                    f"in when: '{val}'.{hint_str}\n"
                                    f"  Valid condition operators: {sorted(FILTER_OPERATORS.keys())}"
                                )
                        elif key in ("then", "else"):
                            _validate_col_op_str(str(val), f"{context}/{key}", errors)


def _validate_schema_ops(schema_dict: dict) -> None:
    """
    Validate all filter operators and column op-strings in *schema_dict*
    against the operator catalog.

    Collects ALL errors before raising a single :class:`ValueError` that lists
    every problem found.
    """
    errors: list[str] = []

    for table in schema_dict.get("tables", []):
        tname = table.get("alias") or table.get("name", "?")

        # Filters are always dicts at this point (normalized by _normalize_filters above).
        for f in table.get("filter", []):
            if isinstance(f, dict):
                _validate_filter_op(
                    f.get("operator", ""),
                    f.get("column", "?"),
                    f"table '{tname}' filter",
                    errors,
                )

        for group in table.get("filter_groups", []):
            for f in group:
                if isinstance(f, dict):
                    _validate_filter_op(
                        f.get("operator", ""),
                        f.get("column", "?"),
                        f"table '{tname}' filter_groups",
                        errors,
                    )

        if "fields" in table:
            _validate_ops_in_select_list(table["fields"], f"table '{tname}' fields", errors)

    _validate_ops_in_select_list(schema_dict.get("select_final", []), "select_final", errors)
    _validate_ops_in_select_list(schema_dict.get("add_columns", []), "add_columns", errors)

    if errors:
        raise ValueError(
            f"Schema validation failed with {len(errors)} error(s):\n"
            + "\n".join(errors)
        )


# ---------------------------------------------------------------------------
# Agent-ready product / contract / semantic seed (Plan 29, slice 0.1)
# ---------------------------------------------------------------------------

def _validate_agent_ready_identifier(value, location: str, errors: list[str]) -> str | None:
    """Validate and normalize an identifier shared by data-product metadata."""
    if not isinstance(value, str) or not value.strip():
        errors.append(f"  [{location}] must be a non-empty string, got {value!r}.")
        return None
    normalized = value.strip()
    if not _AGENT_READY_IDENTIFIER_RE.fullmatch(normalized):
        errors.append(
            f"  [{location}] '{normalized}' is invalid. Use only letters, digits, '.', '_' or '-'."
        )
        return None
    return normalized


def _explicit_output_names(schema_dict: dict) -> set[str] | None:
    """Return statically explicit final columns, or ``None`` when output is open-ended.

    Slice 0.1 deliberately does not infer source-table columns for ``keep_all_columns``.
    That work belongs to OutputProjector in slice 0.2; callers must therefore only
    treat this result as a proof when it is a concrete set.
    """
    aggregate = schema_dict.get("aggregate")
    if aggregate:
        return set(aggregate.get("group_by", [])) | {
            measure["target"] for measure in aggregate.get("measures", [])
        }

    if "select_final" in schema_dict:
        names: set[str] = set()
        for entry in schema_dict.get("select_final", []):
            if isinstance(entry, list) and len(entry) >= 2 and isinstance(entry[1], str):
                names.add(entry[1])
            elif isinstance(entry, dict) and isinstance(entry.get("target"), str):
                names.add(entry["target"])
        return names

    return None


def _normalize_agent_ready_metadata(schema_dict: dict) -> None:
    """Normalize and cross-validate optional Plan 29 metadata blocks in place.

    ``data_product``, ``contract`` and ``semantic`` are intentionally optional so
    existing pipeline YAML remains fully compatible. Errors within these new blocks
    are aggregated before raising, matching the fail-fast operator validation style.
    """
    errors: list[str] = []

    # --- data_product ----------------------------------------------------
    if "data_product" in schema_dict:
        product = schema_dict["data_product"]
        if not isinstance(product, dict):
            errors.append(
                f"  [data_product] must be a mapping, got {type(product).__name__}."
            )
        else:
            unknown = set(product) - _DATA_PRODUCT_ALLOWED_KEYS
            if unknown:
                errors.append(
                    f"  [data_product] unknown keys: {sorted(unknown)}. "
                    f"Allowed keys: {sorted(_DATA_PRODUCT_ALLOWED_KEYS)}"
                )
            product_id = _validate_agent_ready_identifier(product.get("id"), "data_product.id", errors)
            version = product.get("version")
            if not isinstance(version, str) or not version.strip():
                errors.append(
                    f"  [data_product.version] must be a non-empty semantic version string, got {version!r}."
                )
            elif not re.fullmatch(
                r"\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?",
                version.strip(),
            ):
                errors.append(
                    f"  [data_product.version] '{version}' is not a valid semantic version (expected X.Y.Z)."
                )
            for key in ("owner", "description"):
                if key in product and (
                    not isinstance(product[key], str) or not product[key].strip()
                ):
                    errors.append(f"  [data_product.{key}] must be a non-empty string when provided.")

            if not errors:
                normalized_product = {"id": product_id, "version": version.strip()}
                for key in ("owner", "description"):
                    if key in product:
                        normalized_product[key] = product[key].strip()
                schema_dict["data_product"] = normalized_product

    # --- contract --------------------------------------------------------
    output_names: set[str] = set()
    if "contract" in schema_dict:
        contract = schema_dict["contract"]
        if not isinstance(contract, dict):
            errors.append(f"  [contract] must be a mapping, got {type(contract).__name__}.")
        else:
            unknown = set(contract) - _CONTRACT_ALLOWED_KEYS
            if unknown:
                errors.append(
                    f"  [contract] unknown keys: {sorted(unknown)}. "
                    f"Allowed keys: {sorted(_CONTRACT_ALLOWED_KEYS)}"
                )
            output = contract.get("output")
            if not isinstance(output, dict) or not output:
                errors.append("  [contract.output] is required and must be a non-empty mapping of output fields.")
            else:
                normalized_output: dict[str, dict] = {}
                for name, metadata in output.items():
                    field_name = _validate_agent_ready_identifier(name, "contract.output", errors)
                    if field_name is None:
                        continue
                    output_names.add(field_name)
                    if not isinstance(metadata, dict):
                        errors.append(
                            f"  [contract.output] field '{field_name}' must be a mapping, "
                            f"got {type(metadata).__name__}."
                        )
                        continue
                    field_unknown = set(metadata) - _OUTPUT_FIELD_ALLOWED_KEYS
                    if field_unknown:
                        errors.append(
                            f"  [contract.output] field '{field_name}' unknown keys: {sorted(field_unknown)}. "
                            f"Allowed keys: {sorted(_OUTPUT_FIELD_ALLOWED_KEYS)}"
                        )
                    normalized_field: dict[str, object] = {}
                    for key in ("logical_type", "classification", "entity", "description"):
                        if key in metadata:
                            value = metadata[key]
                            if not isinstance(value, str) or not value.strip():
                                errors.append(
                                    f"  [contract.output] field '{field_name}' key '{key}' "
                                    "must be a non-empty string when provided."
                                )
                            else:
                                normalized_field[key] = value.strip()
                    for key in ("required", "unique"):
                        if key in metadata:
                            if not isinstance(metadata[key], bool):
                                errors.append(
                                    f"  [contract.output] field '{field_name}' key '{key}' must be a boolean."
                                )
                            else:
                                normalized_field[key] = metadata[key]
                    normalized_output[field_name] = normalized_field

                explicit_outputs = _explicit_output_names(schema_dict)
                if explicit_outputs is not None:
                    for field_name in sorted(output_names - explicit_outputs):
                        errors.append(
                            f"  [contract.output] column '{field_name}' is not produced by the pipeline. "
                            f"Explicit outputs: {sorted(explicit_outputs)}"
                        )

            grain = contract.get("grain", [])
            if "grain" in contract and not isinstance(grain, list):
                errors.append("  [contract.grain] must be a list of output field names.")
                grain = []
            normalized_grain: list[str] = []
            if isinstance(grain, list):
                for field_name in grain:
                    normalized_name = _validate_agent_ready_identifier(field_name, "contract.grain", errors)
                    if normalized_name is not None:
                        normalized_grain.append(normalized_name)
                if len(set(normalized_grain)) != len(normalized_grain):
                    errors.append("  [contract.grain] contains duplicate output field names.")
                for field_name in sorted(set(normalized_grain) - output_names):
                    errors.append(
                        f"  [contract.grain] column '{field_name}' is not declared in contract.output."
                    )

            if not errors and isinstance(output, dict):
                schema_dict["contract"] = {"output": normalized_output}
                if "grain" in contract:
                    schema_dict["contract"]["grain"] = normalized_grain

    # --- semantic seed ---------------------------------------------------
    if "semantic" in schema_dict:
        semantic = schema_dict["semantic"]
        if not isinstance(semantic, dict):
            errors.append(f"  [semantic] must be a mapping, got {type(semantic).__name__}.")
        else:
            unknown = set(semantic) - _SEMANTIC_SEED_ALLOWED_KEYS
            if unknown:
                errors.append(
                    f"  [semantic] unknown keys: {sorted(unknown)}. "
                    f"Allowed keys: {sorted(_SEMANTIC_SEED_ALLOWED_KEYS)}"
                )
            model_key = _validate_agent_ready_identifier(semantic.get("model_key"), "semantic.model_key", errors)
            normalized_semantic: dict[str, object] = {}
            if model_key is not None:
                normalized_semantic["model_key"] = model_key
            for key in ("entity", "default_time_dimension"):
                if key in semantic:
                    value = _validate_agent_ready_identifier(semantic[key], f"semantic.{key}", errors)
                    if value is not None:
                        normalized_semantic[key] = value
            dimensions = semantic.get("dimensions", [])
            if not isinstance(dimensions, list):
                errors.append("  [semantic.dimensions] must be a list of output field names.")
                dimensions = []
            normalized_dimensions: list[str] = []
            if isinstance(dimensions, list):
                for value in dimensions:
                    normalized_value = _validate_agent_ready_identifier(value, "semantic.dimensions", errors)
                    if normalized_value is not None:
                        normalized_dimensions.append(normalized_value)
                if len(set(normalized_dimensions)) != len(normalized_dimensions):
                    errors.append("  [semantic.dimensions] contains duplicate output field names.")
            normalized_semantic["dimensions"] = normalized_dimensions

            declared_outputs = output_names or _explicit_output_names(schema_dict)
            references = set(normalized_dimensions)
            time_dimension = normalized_semantic.get("default_time_dimension")
            if isinstance(time_dimension, str):
                references.add(time_dimension)
            if references and declared_outputs is None:
                errors.append(
                    "  [semantic] cannot validate dimensions without 'contract.output' or an explicit "
                    "'select_final'/'aggregate' output. Declare contract.output first."
                )
            elif declared_outputs is not None:
                for field_name in sorted(references - declared_outputs):
                    errors.append(
                        f"  [semantic.dimensions] '{field_name}' is not a declared output field."
                    )

            if not errors:
                schema_dict["semantic"] = normalized_semantic

    if errors:
        raise ValueError(
            f"Schema validation failed with {len(errors)} error(s):\n" + "\n".join(errors)
        )

def _resolve_partial_path(path: str, base_dir: str | None, alias: str) -> str:
    """Resolve a partial ``path`` relative to the parent YAML directory.

    Relative paths require ``base_dir`` (i.e. loading the parent from a file via
    ``load_schema``). Absolute paths are honoured as-is so ``parse_schema`` (string
    form) can still reference partials by absolute path.
    """
    if os.path.isabs(path):
        return path
    if base_dir is None:
        raise ValueError(
            f"[partials] Partial '{alias}': relative path '{path}' requires loading the "
            f"parent schema from a file (load_schema). Use an absolute path with parse_schema."
        )
    return os.path.normpath(os.path.join(base_dir, path))


def _collect_table_refs(schema_dict: dict) -> set[str]:
    """Aliases and names (incl. last FQN segment) declared under ``tables:``."""
    refs: set[str] = set()
    for t in schema_dict.get("tables", []):
        if t.get("alias"):
            refs.add(t["alias"])
        if t.get("name"):
            refs.add(t["name"])
            refs.add(t["name"].split(".")[-1])
    return refs


def _normalize_partials(partials, params, base_dir, seen_paths, table_refs):
    """Validate + recursively load ``partials:`` entries into normalized child schemas.

    Each entry is a ``{alias, path}`` mapping. The child schema is parsed through the
    normal loading path (with the parent's ``params`` inherited) and attached under
    ``schema``. Cycle detection is handled by ``load_schema`` via ``_seen_paths``.
    """
    if not isinstance(partials, list):
        raise ValueError("[partials] 'partials' must be a list of {alias, path} entries.")

    result = []
    seen_aliases: set[str] = set()
    for entry in partials:
        if not isinstance(entry, dict):
            raise ValueError(
                f"[partials] Each entry must be a mapping with 'alias' and 'path', "
                f"got {type(entry).__name__}."
            )
        alias = entry.get("alias")
        path = entry.get("path")
        if not isinstance(alias, str) or not alias.strip():
            raise ValueError(f"[partials] Entry missing required string field 'alias': {entry}")
        if not isinstance(path, str) or not path.strip():
            raise ValueError(f"[partials] Partial '{alias}': missing required string field 'path'.")
        if alias in seen_aliases:
            raise ValueError(f"[partials] Duplicate partial alias '{alias}'.")
        if alias in table_refs:
            raise ValueError(
                f"[partials] Partial alias '{alias}' collides with a 'tables' alias/name. "
                f"Aliases must be unique across 'partials' and 'tables'."
            )
        seen_aliases.add(alias)

        resolved = _resolve_partial_path(path, base_dir, alias)
        child_schema = load_schema(resolved, params=params, _seen_paths=seen_paths)
        result.append({
            "alias": alias,
            "path": path,
            "resolved_path": resolved,
            "schema": child_schema,
        })
    return result


def _normalize_measure(entry, index):
    """Normalize one ``aggregate.measures`` entry to ``{source, target, func}``.

    Accepts the compact list form ``[source, target, func]`` and the mapping form
    ``{source, target, func}``. Raises ValueError with a ``[aggregate]`` prefix.
    """
    if isinstance(entry, (list, tuple)):
        if len(entry) != 3:
            raise ValueError(
                f"[aggregate] measures[{index}]: list form must be [source, target, func], "
                f"got {len(entry)} element(s): {list(entry)!r}"
            )
        source, target, func = entry
    elif isinstance(entry, dict):
        unknown = set(entry) - {"source", "target", "func"}
        if unknown:
            raise ValueError(
                f"[aggregate] measures[{index}]: unknown keys {sorted(unknown)}. "
                "Allowed keys: ['func', 'source', 'target']"
            )
        source, target, func = entry.get("source"), entry.get("target"), entry.get("func")
    else:
        raise ValueError(
            f"[aggregate] measures[{index}] must be a list [source, target, func] "
            f"or a mapping, got {type(entry).__name__}."
        )

    for label, value in (("source", source), ("target", target), ("func", func)):
        if not isinstance(value, str) or not value.strip():
            raise ValueError(
                f"[aggregate] measures[{index}]: '{label}' must be a non-empty string, got {value!r}."
            )

    canonical = resolve_aggregate_function(func.strip())
    if canonical is None:
        hints = suggest(func.strip(), AGGREGATE_FUNCTIONS)
        raise ValueError(
            f"[aggregate] measures[{index}]: unknown function '{func}'."
            + (f" Did you mean: {hints}?" if hints else "")
            + f" Valid functions: {sorted(AGGREGATE_FUNCTIONS.keys())}"
        )

    source = source.strip()
    if source == "*" and not AGGREGATE_FUNCTIONS[canonical].allows_star:
        raise ValueError(
            f"[aggregate] measures[{index}]: source '*' is only supported by "
            f"'count', not '{canonical}'."
        )

    return {"source": source, "target": target.strip(), "func": canonical}


def _normalize_aggregate(schema_dict):
    """
    Normalize the top-level ``aggregate:`` block (Plan 28).

    Shape: ``{group_by: [col…], measures: [[source, target, func]…], having: [filter…]}``.
    Normalizes in place; no-op when the key is absent, so non-aggregating schemas
    keep their exact current shape.
    """
    if "aggregate" not in schema_dict:
        return

    agg = schema_dict["aggregate"]
    if not isinstance(agg, dict):
        raise ValueError(
            "[aggregate] must be a mapping with 'group_by', 'measures' and optional 'having' keys, "
            f"got {type(agg).__name__}."
        )

    allowed_keys = {"group_by", "measures", "having"}
    unknown_keys = set(agg) - allowed_keys
    if unknown_keys:
        raise ValueError(
            f"[aggregate] Unknown keys: {sorted(unknown_keys)}. Allowed keys: {sorted(allowed_keys)}"
        )

    group_by = agg.get("group_by")
    if not isinstance(group_by, list) or not group_by:
        raise ValueError(
            "[aggregate] 'group_by' is required and must be a non-empty list of column names."
        )
    for key in group_by:
        if not isinstance(key, str) or not key.strip():
            raise ValueError(f"[aggregate] 'group_by' entries must be non-empty strings, got {key!r}.")
    group_by = [k.strip() for k in group_by]

    measures = agg.get("measures")
    if not isinstance(measures, list) or not measures:
        raise ValueError(
            "[aggregate] 'measures' is required and must be a non-empty list of "
            "[source, target, func] entries."
        )
    normalized_measures = [_normalize_measure(m, i) for i, m in enumerate(measures)]

    seen_targets = set()
    for m in normalized_measures:
        if m["target"] in seen_targets:
            raise ValueError(f"[aggregate] duplicate measure target '{m['target']}'.")
        if m["target"] in group_by:
            raise ValueError(
                f"[aggregate] measure target '{m['target']}' collides with a 'group_by' column."
            )
        seen_targets.add(m["target"])

    normalized = {"group_by": group_by, "measures": normalized_measures}

    if "having" in agg:
        having = agg["having"]
        if not isinstance(having, list):
            raise ValueError("[aggregate] 'having' must be a list of filter expressions.")
        normalized_having = _normalize_filters(having)
        known = set(group_by) | seen_targets
        for h in normalized_having:
            column = h.get("column")
            if column not in known:
                raise ValueError(
                    f"[aggregate] 'having' references unknown column '{column}'. "
                    f"Available: {sorted(known)} (group_by keys and measure targets)."
                )
        normalized["having"] = normalized_having

    # Exclusivity — the aggregate block produces the final projection itself.
    if "select_final" in schema_dict:
        raise ValueError("[aggregate] 'aggregate' and 'select_final' are mutually exclusive.")
    if schema_dict.get("keep_all_columns"):
        raise ValueError("[aggregate] 'aggregate' and 'keep_all_columns' are mutually exclusive.")

    schema_dict["aggregate"] = normalized


def _normalize_materialization(schema_dict):
    """
    Normalize the top-level ``materialization:`` block (Plan 27).

    Accepts the string shorthand (``materialization: streaming_table``) or the
    dict form. Allowed keys depend on the type (``MATERIALIZATION_ALLOWED_KEYS``),
    so streaming options cannot leak onto a materialized view and vice versa.
    Normalizes in place with defaults applied. No-op when the key is absent
    (batch schemas keep their exact current shape).
    """
    if "materialization" not in schema_dict:
        return

    mat = schema_dict["materialization"]
    if isinstance(mat, str):
        mat = {"type": mat}
    if not isinstance(mat, dict):
        raise ValueError(
            "[materialization] must be a string shorthand or a mapping with a 'type' key. "
            f"Valid types: {sorted(VALID_MATERIALIZATION_TYPES)}"
        )

    mat_type = mat.get("type")
    if not isinstance(mat_type, str) or not mat_type.strip():
        raise ValueError(
            f"[materialization] Missing required field 'type'. "
            f"Valid types: {sorted(VALID_MATERIALIZATION_TYPES)}"
        )
    mat_type = mat_type.strip().lower()
    if mat_type not in VALID_MATERIALIZATION_TYPES:
        raise ValueError(
            f"[materialization] Unknown type '{mat_type}'. "
            f"Valid types: {sorted(VALID_MATERIALIZATION_TYPES)}"
        )

    allowed_keys = MATERIALIZATION_ALLOWED_KEYS[mat_type]
    unknown_keys = set(mat) - allowed_keys
    if unknown_keys:
        raise ValueError(
            f"[materialization] Unknown keys for type '{mat_type}': {sorted(unknown_keys)}. "
            f"Allowed keys: {sorted(allowed_keys)}"
        )

    normalized = {"type": mat_type}
    if mat_type == "streaming_table":
        trigger = mat.get("trigger", DEFAULT_STREAMING_TRIGGER)
        is_valid_trigger = isinstance(trigger, str) and (
            trigger == "available_now"
            or (trigger.startswith("interval:") and trigger[len("interval:"):].strip())
        )
        if not is_valid_trigger:
            raise ValueError(
                f"[materialization] Invalid trigger '{trigger}'. "
                "Valid triggers: 'available_now' or 'interval:<duration>' "
                "(e.g. 'interval:30 seconds')."
            )
        checkpoint = mat.get("checkpoint", "auto")
        if not isinstance(checkpoint, str) or not checkpoint.strip():
            raise ValueError(
                "[materialization] 'checkpoint' must be a non-empty string "
                "('auto' or an explicit path)."
            )
        normalized["trigger"] = trigger
        normalized["checkpoint"] = checkpoint.strip()

        # CDC Type 1 upsert (foreachBatch + MERGE INTO) — Plan 27 decision #15.
        write_mode = mat.get("write_mode", "append")
        if not isinstance(write_mode, str) or write_mode.strip().lower() not in ("append", "upsert"):
            raise ValueError(
                f"[materialization] Invalid write_mode '{write_mode}'. "
                "Valid write modes: ['append', 'upsert']."
            )
        write_mode = write_mode.strip().lower()
        keys = mat.get("keys")
        if write_mode == "upsert":
            if (
                not isinstance(keys, list)
                or not keys
                or not all(isinstance(k, str) and k.strip() for k in keys)
            ):
                raise ValueError(
                    "[materialization] 'write_mode: upsert' requires 'keys' — a "
                    "non-empty list of merge-key column names "
                    "(e.g. keys: [order_id])."
                )
            normalized["keys"] = [k.strip() for k in keys]
        elif keys is not None:
            raise ValueError(
                "[materialization] 'keys' only applies to 'write_mode: upsert'."
            )
        normalized["write_mode"] = write_mode

    elif mat_type == "materialized_view":
        # Plan 28 — Databricks manages the refresh; we only describe the object.
        if "schedule" in mat:
            schedule = mat["schedule"]
            if not isinstance(schedule, str) or not schedule.strip():
                raise ValueError("[materialization] 'schedule' must be a non-empty string.")
            schedule = schedule.strip()
            if not schedule.upper().startswith(VALID_MV_SCHEDULE_PREFIXES):
                raise ValueError(
                    f"[materialization] Invalid schedule '{schedule}'. It must start with "
                    "'EVERY ' (e.g. 'EVERY 6 HOURS') or 'CRON ' "
                    "(e.g. \"CRON '0 0 6 * * ?' AT TIME ZONE 'Europe/Paris'\")."
                )
            normalized["schedule"] = schedule

        if "comment" in mat:
            comment = mat["comment"]
            if not isinstance(comment, str) or not comment.strip():
                raise ValueError("[materialization] 'comment' must be a non-empty string.")
            normalized["comment"] = comment.strip()

        for key in ("cluster_by", "partition_by"):
            if key not in mat:
                continue
            cols = mat[key]
            if (
                not isinstance(cols, list)
                or not cols
                or not all(isinstance(c, str) and c.strip() for c in cols)
            ):
                raise ValueError(
                    f"[materialization] '{key}' must be a non-empty list of column names."
                )
            normalized[key] = [c.strip() for c in cols]
        if "cluster_by" in normalized and "partition_by" in normalized:
            raise ValueError(
                "[materialization] 'cluster_by' and 'partition_by' are mutually exclusive — "
                "Databricks accepts only one clustering strategy per materialized view."
            )

        refresh = mat.get("refresh", "auto")
        if not isinstance(refresh, str) or refresh.strip().lower() not in VALID_MV_REFRESH_MODES:
            raise ValueError(
                f"[materialization] Invalid refresh '{refresh}'. "
                f"Valid refresh modes: {sorted(VALID_MV_REFRESH_MODES)}."
            )
        normalized["refresh"] = refresh.strip().lower()

    schema_dict["materialization"] = normalized


def _validate_materialized_view(schema_dict):
    """
    Cross-validate a ``materialization: materialized_view`` schema (Plan 28).

    A materialized view is defined by SQL over Unity Catalog tables, so every
    construct that only exists in the DataFrame path is rejected here — at load
    time, aggregated in one error, rather than failing halfway through a run.
    ``core/sql_compiler.py`` re-checks the same ground as a second barrier.
    """
    mat = schema_dict.get("materialization")
    if not mat or mat.get("type") != "materialized_view":
        return

    errors: list[str] = []

    if schema_dict.get("business_rules"):
        errors.append(
            "  [materialized_view] 'business_rules' cannot run in a materialized view — "
            "Python rules are not expressible in SQL. Materialize the rule output in an "
            "upstream (silver) table, then join or aggregate that table here."
        )
    if schema_dict.get("partials"):
        errors.append(
            "  [materialized_view] 'partials:' sub-transformations run Python and cannot be "
            "compiled to SQL. Materialize the partial as its own table and list it in 'tables:'."
        )
    if schema_dict.get("dev_limit"):
        errors.append(
            "  [materialized_view] schema-level 'dev_limit' is incompatible with a "
            "materialized view — a frozen LIMIT in a persisted definition silently "
            "truncates the result."
        )
    sink = schema_dict.get("sink")
    if sink and sink.get("type") in ("postgres", "jdbc"):
        errors.append(
            "  [materialized_view] 'sink' type 'postgres'/'jdbc' is incompatible with "
            "'materialization: materialized_view' — a materialized view lives in Unity Catalog."
        )

    tables = schema_dict.get("tables", [])
    if schema_dict.get("keep_all_columns") and (len(tables) > 1 or schema_dict.get("join")):
        errors.append(
            "  [materialized_view] 'keep_all_columns' is only supported on a single table "
            "with no join — 'SELECT *' over a join yields duplicate column names, which a "
            "materialized view rejects. Declare 'select_final' or 'aggregate' instead."
        )

    for t in tables:
        label = t.get("alias") or t.get("name", "?")
        if t.get("streaming"):
            errors.append(
                f"  [materialized_view] table '{label}': 'streaming: true' is incompatible "
                "with 'materialization: materialized_view' — the two materializations exclude "
                "each other."
            )
        if t.get("source"):
            errors.append(
                f"  [materialized_view] table '{label}': file sources ('source:') are not "
                "readable from a materialized view — ingest into a bronze table first, then "
                "build the view on top of it."
            )
        if t.get("source_type") == "loader":
            errors.append(
                f"  [materialized_view] table '{label}': Python loaders are not expressible "
                "in SQL. Ingest into a catalog table first."
            )
        if t.get("dev_limit"):
            errors.append(
                f"  [materialized_view] table '{label}': 'dev_limit' is incompatible with a "
                "materialized view — a frozen LIMIT in a persisted definition silently "
                "truncates the result."
            )
        if (t.get("quality_checks") or {}).get("drop_duplicates_on"):
            errors.append(
                f"  [materialized_view] table '{label}': 'quality_checks.drop_duplicates_on' "
                "has no faithful SQL form (it needs a windowed row_number with a deterministic "
                "ORDER BY). Deduplicate upstream, or group on those keys with an 'aggregate:' block."
            )
        if "qualify" in (t.get("preprocess") or {}):
            errors.append(
                f"  [materialized_view] table '{label}': 'preprocess.qualify' has no faithful "
                "SQL form here. Materialize the qualified result upstream instead."
            )

    if errors:
        raise ValueError(
            f"Schema validation failed with {len(errors)} error(s):\n" + "\n".join(errors)
        )


def _schema_contains_streaming(schema_dict) -> bool:
    """True if any table (recursively through partials) has ``streaming: true``."""
    for t in schema_dict.get("tables", []):
        if t.get("streaming"):
            return True
    for p in schema_dict.get("partials", []):
        child = p.get("schema") if isinstance(p, dict) else None
        if isinstance(child, dict) and _schema_contains_streaming(child):
            return True
    return False


def _validate_streaming(schema_dict):
    """
    Cross-validate ``streaming: true`` tables against the rest of the schema
    (Plan 27). Streaming DataFrames reject several batch operations — every
    incompatibility is surfaced here at load time, aggregated in one error.
    """
    tables = schema_dict.get("tables", [])
    streaming_tables = [t for t in tables if t.get("streaming")]
    mat = schema_dict.get("materialization")
    is_streaming_run = bool(mat and mat.get("type") == "streaming_table")

    errors: list[str] = []

    # Streaming in partials children is always invalid (batch sub-transformations only).
    for p in schema_dict.get("partials", []):
        child = p.get("schema") if isinstance(p, dict) else None
        if isinstance(child, dict) and _schema_contains_streaming(child):
            errors.append(
                f"  [partials] partial '{p.get('alias', '?')}': 'streaming: true' is not "
                f"allowed inside partial schemas — partials are batch sub-transformations."
            )

    if not streaming_tables and not is_streaming_run:
        if errors:
            raise ValueError(
                f"Schema validation failed with {len(errors)} error(s):\n" + "\n".join(errors)
            )
        return

    # Bijection: streaming reads require a streaming write and vice versa.
    if streaming_tables and not is_streaming_run:
        errors.append(
            "  [streaming] table(s) marked 'streaming: true' require a top-level "
            "'materialization: streaming_table' block — a batch overwrite of a "
            "streaming DataFrame is invalid."
        )
    if is_streaming_run and not streaming_tables:
        errors.append(
            "  [materialization] 'streaming_table' requires at least one table "
            "with 'streaming: true'."
        )

    if len(streaming_tables) > 1:
        names = [t.get("alias") or t.get("name", "?") for t in streaming_tables]
        errors.append(
            f"  [streaming] only one table may have 'streaming: true' per schema "
            f"(stream-stream joins are out of scope); found {len(names)}: {names}."
        )

    # Per streaming-table incompatibilities.
    for t in streaming_tables:
        label = t.get("alias") or t.get("name", "?")
        if t.get("source_type") == "loader":
            errors.append(
                f"  [streaming] table '{label}': 'source_type: loader' is incompatible "
                f"with 'streaming: true' — loaders return batch DataFrames."
            )
        src = t.get("source")
        if src and src.get("type") not in VALID_STREAMING_SOURCE_TYPES:
            errors.append(
                f"  [streaming] table '{label}': source type '{src.get('type')}' cannot "
                f"be read as a stream without an explicit schema. Valid streaming source "
                f"types: {sorted(VALID_STREAMING_SOURCE_TYPES)} — convert the source to "
                f"Delta or read a catalog table."
            )
        if t.get("dev_limit"):
            errors.append(
                f"  [streaming] table '{label}': 'dev_limit' is incompatible with "
                f"'streaming: true' — limit() is unsupported on streaming DataFrames."
            )
        if "preprocess" in t and "qualify" in (t.get("preprocess") or {}):
            errors.append(
                f"  [streaming] table '{label}': 'preprocess.qualify' is incompatible "
                f"with 'streaming: true' — row_number() windows are unsupported on "
                f"streaming DataFrames."
            )
        qc = t.get("quality_checks") or {}
        if qc.get("drop_duplicates_on"):
            errors.append(
                f"  [streaming] table '{label}': 'quality_checks.drop_duplicates_on' is "
                f"unbounded-state on a streaming table. Use CDC Type 1 dedup instead: "
                f"'materialization: {{type: streaming_table, write_mode: upsert, "
                f"keys: [...]}}' — the MERGE key guarantees uniqueness in the target."
            )

    if is_streaming_run:
        if schema_dict.get("dev_limit"):
            errors.append(
                "  [streaming] schema-level 'dev_limit' is incompatible with "
                "'materialization: streaming_table' — limit() is unsupported on "
                "streaming DataFrames."
            )
        sink = schema_dict.get("sink")
        if sink and sink.get("type") in ("postgres", "jdbc"):
            errors.append(
                "  [streaming] 'sink' type 'postgres'/'jdbc' is incompatible with "
                "'materialization: streaming_table' — streaming writes target Delta only."
            )
        if schema_dict.get("aggregate"):
            errors.append(
                "  [streaming] the 'aggregate' block is incompatible with "
                "'materialization: streaming_table' — streaming aggregations require "
                "watermark support (planned for a later plan). Aggregate downstream in a "
                "batch pipeline or a materialized view reading this streaming table."
            )

        # Join topology: the (single) streaming table must be the join base,
        # never a joined side, and only inner/left joins are stream-safe.
        joins = schema_dict.get("join", [])
        if streaming_tables and joins:
            st = streaming_tables[0]
            stream_refs = {st.get("alias"), st.get("name")}
            if st.get("name"):
                stream_refs.add(st["name"].split(".")[-1])
            stream_refs.discard(None)

            if joins[0].get("table_from") not in stream_refs:
                errors.append(
                    "  [streaming] the streaming table must be the join base "
                    "('table_from' of the first join) — stream-static joins are only "
                    "supported with the stream on the left side."
                )
            for i, j in enumerate(joins, start=1):
                if j.get("table_to") in stream_refs:
                    errors.append(
                        f"  [streaming] join #{i}: the streaming table cannot appear as "
                        f"'table_to' — it must be the join base."
                    )
                join_type = _normalise_join_type(j.get("type", "left"))
                if join_type not in ("inner", "left"):
                    errors.append(
                        f"  [streaming] join #{i}: type '{join_type}' is not supported "
                        f"with a streaming base. Supported types: ['inner', 'left']."
                    )

    if errors:
        raise ValueError(
            f"Schema validation failed with {len(errors)} error(s):\n" + "\n".join(errors)
        )


def _normalize_schema(schema_dict, params=None, base_dir=None, seen_paths=None):
    """Apply all normalizations to a parsed schema dict."""
    # Validate mutual exclusion before normalization
    if schema_dict.get("keep_all_columns") and "select_final" in schema_dict:
        raise ValueError("'keep_all_columns' and 'select_final' are mutually exclusive.")

    # Partials — recursively load nested schemas (relative to the parent YAML dir).
    if "partials" in schema_dict:
        schema_dict["partials"] = _normalize_partials(
            schema_dict["partials"],
            params,
            base_dir,
            seen_paths,
            _collect_table_refs(schema_dict),
        )

    # Normalize filters in each table
    for table in schema_dict.get("tables", []):
        if "filter" in table:
            filter_val = table["filter"]
            if isinstance(filter_val, dict):
                table["filter"] = _normalize_filter_mapping(filter_val)
            else:
                table["filter"] = _normalize_filters(filter_val)
        if "filter_groups" in table:
            table["filter_groups"] = [_normalize_filters(g) for g in table["filter_groups"]]

        # Validate and normalize source: block
        if "source" in table:
            src = table["source"]
            if not isinstance(src, dict):
                raise ValueError(
                    f"[source] Table '{table.get('name', '?')}': 'source' must be a mapping "
                    f"with 'type' and 'path' keys."
                )
            # Conflict check: source: and source_type: loader are mutually exclusive
            if table.get("source_type") == "loader":
                raise ValueError(
                    f"[source] Table '{table.get('name', '?')}': 'source:' and "
                    f"'source_type: loader' are mutually exclusive."
                )
            if "type" not in src:
                raise ValueError(
                    f"[source] Table '{table.get('name', '?')}': missing required field 'type' "
                    f"in source block. Valid types: {sorted(VALID_SOURCE_TYPES)}"
                )
            if "path" not in src:
                raise ValueError(
                    f"[source] Table '{table.get('name', '?')}': missing required field 'path' "
                    f"in source block."
                )
            if src["type"] not in VALID_SOURCE_TYPES:
                raise ValueError(
                    f"[source] Table '{table.get('name', '?')}': unknown source type "
                    f"'{src['type']}'. Valid types: {sorted(VALID_SOURCE_TYPES)}"
                )
            # Normalize options to dict (default {})
            if "options" not in src or src["options"] is None:
                src["options"] = {}

        # Validate streaming flag (Plan 27) — strict boolean
        if "streaming" in table and not isinstance(table["streaming"], bool):
            raise ValueError(
                f"[streaming] Table '{table.get('name', '?')}': 'streaming' must be a "
                f"boolean (true/false), got {table['streaming']!r}."
            )

    # Normalize joins
    if "join" in schema_dict:
        schema_dict["join"] = [_normalize_join(j) for j in schema_dict["join"]]

        # Validate that every table_from / table_to references a declared alias or name
        known_refs: set[str] = set()
        for t in schema_dict.get("tables", []):
            if t.get("alias"):
                known_refs.add(t["alias"])
            if t.get("name"):
                known_refs.add(t["name"])
                # Also accept the last segment of a qualified name (e.g. "catalog.schema.table" → "table")
                last_part = t["name"].split(".")[-1]
                known_refs.add(last_part)
        # Partials expose their output under the declared alias — joins may reference them.
        for p in schema_dict.get("partials", []):
            if isinstance(p, dict) and p.get("alias"):
                known_refs.add(p["alias"])

        sorted_refs = sorted(known_refs)
        join_errors: list[str] = []
        for j in schema_dict["join"]:
            for side in ("table_from", "table_to"):
                ref = j.get(side)
                if ref and ref not in known_refs:
                    suggestions = difflib.get_close_matches(ref, sorted_refs, n=3, cutoff=0.6)
                    hint = f" Did you mean: {suggestions}?" if suggestions else ""
                    join_errors.append(
                        f"  [join] '{side}' references unknown alias '{ref}'.{hint} "
                        f"Known aliases/names: {sorted(known_refs)}"
                    )
        if join_errors:
            raise ValueError(
                "Schema validation failed — invalid join references:\n" + "\n".join(join_errors)
            )

    # Normalize select_final
    if "select_final" in schema_dict:
        schema_dict["select_final"] = _normalize_select_final(schema_dict["select_final"])

    # Normalize add_columns if present
    if "add_columns" in schema_dict:
        schema_dict["add_columns"] = _normalize_select_final(schema_dict["add_columns"])

    if "sink" in schema_dict:
        sink = schema_dict["sink"]
        if not isinstance(sink, dict):
            raise ValueError("'sink' must be a mapping.")

        allowed_keys = {"type", "schema", "table"}
        unknown_keys = set(sink) - allowed_keys
        if unknown_keys:
            raise ValueError(
                f"[sink] Unknown keys: {sorted(unknown_keys)}. "
                f"Allowed keys: {sorted(allowed_keys)}"
            )

        sink_type = sink.get("type")
        if not isinstance(sink_type, str) or not sink_type.strip():
            raise ValueError(
                f"[sink] Missing required field 'type'. Valid types: {sorted(VALID_SINK_TYPES)}"
            )

        sink_type = sink_type.strip().lower()
        if sink_type not in VALID_SINK_TYPES:
            raise ValueError(
                f"[sink] Unknown sink type '{sink_type}'. Valid types: {sorted(VALID_SINK_TYPES)}"
            )

        normalized_sink = {"type": sink_type}
        for key in ("schema", "table"):
            # Optional overrides for run_process_to_table(target_layer, target_table_name).
            if key in sink:
                if not isinstance(sink[key], str):
                    raise ValueError(f"[sink] '{key}' must be a string.")
                normalized_sink[key] = sink[key]

        schema_dict["sink"] = normalized_sink

    # Declarative aggregations (Plan 28) — before materialization so the
    # aggregate block is already normalized for the cross-validations below.
    _normalize_aggregate(schema_dict)

    # Agent-ready data-product metadata (Plan 29, slice 0.1) is normalized
    # after aggregate/select normalization so explicit output targets can be
    # cross-validated without running Spark.
    _normalize_agent_ready_metadata(schema_dict)

    # Materialization (Plan 27 streaming, Plan 28 materialized views):
    # normalize once, then cross-validate each type.
    _normalize_materialization(schema_dict)
    _validate_streaming(schema_dict)
    _validate_materialized_view(schema_dict)

    # Fail-fast operator validation — must happen after normalization so all filters are dicts
    _validate_schema_ops(schema_dict)

    return schema_dict


def parse_schema(yaml_str, params=None, *, base_dir=None, _seen_paths=None):
    """
    Parse a pipeline schema from an inline YAML string.

    Args:
        yaml_str (str): YAML string defining the pipeline schema.
        params (dict, optional): Template parameters to inject ({{ key }} placeholders).
        base_dir (str, optional): Directory used to resolve relative ``partials:`` paths.
        _seen_paths (list, optional): Internal — stack of resolved paths for partial
            cycle detection (propagated by ``load_schema``).

    Returns:
        dict: Normalized schema dict ready for SkiferEngine.process_schema().

    Raises:
        ValueError: If params are missing or YAML is malformed.
    """
    try:
        injected = _inject_params(yaml_str, params or {})
    except ValueError:
        raise

    try:
        schema = yaml.safe_load(injected)
    except yaml.YAMLError as e:
        raise ValueError(f"Malformed YAML schema: {e}")

    if not isinstance(schema, dict):
        raise ValueError("Schema must be a YAML mapping (dict) at the top level.")

    return _normalize_schema(schema, params=params, base_dir=base_dir, seen_paths=_seen_paths)


_LOCALIZED_LINE_RE = re.compile(r"^\s*\[([^]]+)]\s*(.*)$")
_UNKNOWN_FILTER_RE = re.compile(r"unknown filter operator '([^']+)'")
_UNKNOWN_JOIN_RE = re.compile(
    r"'(table_from|table_to)' references unknown alias '([^']+)'"
)
_UNKNOWN_CONTRACT_OUTPUT_RE = re.compile(
    r"column '([^']+)' is not produced by the pipeline"
)


def _filter_operator(item) -> str | None:
    """Return the operator from any supported pre-normalization filter form."""
    if isinstance(item, str):
        parts = item.split(":", 2)
        return parts[1].strip() if len(parts) >= 2 else None
    if not isinstance(item, dict):
        return None
    if isinstance(item.get("operator"), str):
        return item["operator"]
    if len(item) == 1:
        value = next(iter(item.values()))
        if isinstance(value, dict) and len(value) == 1:
            return str(next(iter(value)))
    return None


def _localized_path(location: str, detail: str, schema: dict | None) -> tuple[str, str]:
    """Map existing human validation locations to stable best-effort paths."""
    schema = schema or {}
    unknown_filter = _UNKNOWN_FILTER_RE.search(detail)
    if unknown_filter and "filter" in location:
        table_name_match = re.match(r"table '([^']+)'", location)
        table_name = table_name_match.group(1) if table_name_match else None
        section = "filter_groups" if "filter_groups" in location else "filter"
        for table_index, table in enumerate(schema.get("tables", [])):
            if table_name is not None and table_name not in {
                table.get("alias"),
                table.get("name"),
            }:
                continue
            if section == "filter":
                values = table.get("filter", [])
                values = (
                    [{key: value} for key, value in values.items()]
                    if isinstance(values, dict)
                    else values
                )
                for filter_index, item in enumerate(values or []):
                    if _filter_operator(item) == unknown_filter.group(1):
                        return "filter.unknown_operator", (
                            f"tables[{table_index}].filter[{filter_index}]"
                        )
                return "filter.unknown_operator", f"tables[{table_index}].filter"
            for group_index, group in enumerate(table.get("filter_groups", [])):
                for filter_index, item in enumerate(group or []):
                    if _filter_operator(item) == unknown_filter.group(1):
                        return "filter.unknown_operator", (
                            f"tables[{table_index}].filter_groups[{group_index}]"
                            f"[{filter_index}]"
                        )
            return "filter.unknown_operator", f"tables[{table_index}].filter_groups"
        return "filter.unknown_operator", "tables[?].filter"

    unknown_join = _UNKNOWN_JOIN_RE.search(detail)
    if location == "join" and unknown_join:
        side, alias = unknown_join.groups()
        for join_index, join in enumerate(schema.get("join", [])):
            value = join.get(side)
            ref = value[0] if isinstance(value, list) and value else value
            if ref == alias:
                return "join.unknown_alias", f"join[{join_index}].{side}"
        return "join.unknown_alias", f"join[?].{side}"

    unknown_contract = _UNKNOWN_CONTRACT_OUTPUT_RE.search(detail)
    if location == "contract.output" and unknown_contract:
        column = unknown_contract.group(1)
        return "contract.output.unknown_column", f"contract.output.{column}"

    return "schema.invalid", location if "." in location else ""


def _localized_issues(message: str, schema: dict | None) -> list[LocalizedIssue]:
    """Split the loader's aggregated text into allowlisted structured issues."""
    entries: list[tuple[str, list[str]]] = []
    for line in message.splitlines():
        match = _LOCALIZED_LINE_RE.match(line)
        if match:
            entries.append((match.group(1), [match.group(2)]))
        elif entries and line.strip():
            entries[-1][1].append(line.strip())

    if not entries:
        return [LocalizedIssue(code="schema.invalid", message=message, path="")]

    issues = []
    for location, detail_lines in entries:
        detail = "\n".join(detail_lines)
        code, path = _localized_path(location, detail, schema)
        issues.append(LocalizedIssue(code=code, message=detail, path=path))
    return issues


def parse_schema_localized(
    yaml_str: str, params=None, *, base_dir: str | None = None
) -> tuple[dict | None, list[LocalizedIssue]]:
    """Parse a schema and return localized validation issues instead of raising."""
    schema = None
    original = None
    try:
        injected = _inject_params(yaml_str, params or {})
        try:
            schema = yaml.safe_load(injected)
        except yaml.YAMLError as exc:
            raise ValueError(f"Malformed YAML schema: {exc}") from exc
        if not isinstance(schema, dict):
            raise ValueError("Schema must be a YAML mapping (dict) at the top level.")
        original = deepcopy(schema)
        normalized = _normalize_schema(schema, params=params, base_dir=base_dir)
    except ValueError as exc:
        return None, _localized_issues(str(exc), original)
    return normalized, []


def load_schema(path, params=None, *, _seen_paths=None):
    """
    Load a pipeline schema from a YAML file.

    Searches upward from cwd if path is relative and not found in-place.
    Supports {{ key }} parameter injection.

    Args:
        path (str): Path to the YAML schema file (absolute or relative).
        params (dict, optional): Template parameters to inject ({{ key }} placeholders).
        _seen_paths (list, optional): Internal — stack of already-resolved schema paths,
            used to detect cyclic ``partials:`` references. Fail-fast on a repeat.

    Returns:
        dict: Normalized schema dict ready for SkiferEngine.process_schema().

    Raises:
        FileNotFoundError: If the file cannot be found.
        ValueError: If params are missing, YAML is malformed, the file is invalid,
            or a cyclic partial reference is detected.
    """
    # Try direct path first
    if os.path.isabs(path) and os.path.exists(path):
        resolved_path = path
    elif os.path.exists(path):
        resolved_path = os.path.abspath(path)
    else:
        # Search upward from cwd — first try the full relative path, then basename only
        filename = os.path.basename(path)
        resolved_path = _find_file_upwards(path)
        if resolved_path is None:
            basename_match = _find_file_upwards(filename)
            if basename_match:
                import warnings
                warnings.warn(
                    f"Schema file '{path}' not found by full path; resolved to '{basename_match}' "
                    "by filename only. Pass an absolute path to avoid ambiguity.",
                    UserWarning,
                    stacklevel=3,
                )
                resolved_path = basename_match
        if resolved_path is None:
            raise FileNotFoundError(
                f"Schema file '{path}' not found in current directory or any parent directory."
            )

    # Cycle detection for nested partials: fail fast if this file is already on the stack.
    seen_paths = _seen_paths or []
    if resolved_path in seen_paths:
        chain = " -> ".join(seen_paths + [resolved_path])
        raise ValueError(f"[partials] Cyclic schema reference detected: {chain}")

    try:
        with open(resolved_path, "r", encoding="utf-8") as f:
            yaml_str = f.read()
    except OSError as e:
        raise ValueError(f"Cannot read schema file '{resolved_path}': {e}")

    return parse_schema(
        yaml_str,
        params,
        base_dir=os.path.dirname(resolved_path),
        _seen_paths=seen_paths + [resolved_path],
    )
