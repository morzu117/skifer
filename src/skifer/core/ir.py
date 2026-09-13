"""
Intermediate Representation (IR) for Skifer pipeline schemas.

Parsed once at load time (via parse_to_ir), consumed by the interpreter, backends,
lineage tracker, and inspection helpers. Eliminates repeated op-string parsing at runtime.

Dataclasses are frozen/immutable — safe to cache and share.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from skifer.core.constants import DEFAULT_CONTRACT_STATUS

VALID_JOIN_TYPES: frozenset[str] = frozenset(
    {"left", "right", "inner", "full", "cross", "left_anti", "left_semi"}
)

_JOIN_TYPE_ALIASES: dict[str, str] = {
    "left outer": "left",
    "right outer": "right",
    "outer": "full",
    "full outer": "full",
    "full_outer": "full",
    "anti": "left_anti",
    "left anti": "left_anti",
    "leftanti": "left_anti",
    "semi": "left_semi",
    "left semi": "left_semi",
    "leftsemi": "left_semi",
}


def _normalise_join_type(raw: str) -> str:
    key = raw.strip().lower().replace("-", " ")
    return _JOIN_TYPE_ALIASES.get(key, key)


# ---------------------------------------------------------------------------
# Leaf types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ParsedOp:
    """A single column operation with its arguments.

    Examples:
        ParsedOp("cast", ["double"])
        ParsedOp("round", ["2"])
        ParsedOp("upper", [])
        ParsedOp("lit", ["ERP"])
    """
    name: str
    args: tuple[str, ...] = field(default_factory=tuple)

    def __post_init__(self):
        # Ensure args is always a tuple even if a list was passed
        object.__setattr__(self, "args", tuple(self.args))


@dataclass(frozen=True)
class ParsedFilter:
    """A single filter predicate in resolved (canonical) form.

    Examples:
        ParsedFilter("status", "equals", "ACTIVE")
        ParsedFilter("amount", "is_not_null", None)
        ParsedFilter("region", "in", ["EMEA", "APAC"])
    """
    column: str
    operator: str          # canonical name from FILTER_OPERATORS
    value: Any = None      # str, list[str], or None for nullary operators


@dataclass(frozen=True)
class WhenClause:
    """One branch of a chained CASE WHEN expression.

    `condition` is a ParsedOp representing the 'when' test (e.g. ParsedOp("equals", ["A"])).
    `then` is a ParsedOp representing the value to return (e.g. ParsedOp("lit", ["Active"])).
    """
    condition: ParsedOp
    then: ParsedOp


@dataclass
class ParsedColumnSpec:
    """A column spec in select_final, add_columns, or fields.

    Either linear ops (`ops`) OR a when/else chain (`when_chain` + `otherwise`) —
    never both simultaneously.

    Attributes:
        source: Source column name (or None for literal-only expressions).
        target: Output alias name.
        ops: Ordered sequence of linear ParsedOp transformations.
        when_chain: Ordered WHEN/THEN pairs for CASE WHEN expressions.
        otherwise: The ELSE clause of the when_chain (or None → NULL).
    """
    source: str | None
    target: str
    ops: list[ParsedOp] = field(default_factory=list)
    when_chain: list[WhenClause] = field(default_factory=list)
    otherwise: ParsedOp | None = None

    @property
    def is_conditional(self) -> bool:
        return bool(self.when_chain)


@dataclass(frozen=True)
class ParsedMeasure:
    """A single aggregate measure in the ``aggregate:`` block (Plan 28).

    Examples:
        ParsedMeasure("amount", "total_amount", "sum")
        ParsedMeasure("*", "nb_rows", "count")
    """
    source: str
    target: str
    func: str          # canonical name from AGGREGATE_FUNCTIONS


@dataclass
class ParsedAggregate:
    """Declarative GROUP BY specification (Plan 28).

    Attributes:
        group_by: Grouping key columns, in declaration order.
        measures: Aggregate measures producing the remaining output columns.
        having: Post-aggregation predicates on group keys or measure targets.
    """
    group_by: list[str] = field(default_factory=list)
    measures: list[ParsedMeasure] = field(default_factory=list)
    having: list[ParsedFilter] = field(default_factory=list)


@dataclass
class ParsedJoin:
    """A single JOIN specification."""
    alias_left: str
    keys_left: list[str]
    alias_right: str
    keys_right: list[str]
    join_type: str = "left"


@dataclass
class ParsedTable:
    """Source table spec with all pre-processing resolved."""
    name: str
    alias: str
    source_type: str | None = None        # "delta", "csv", etc., or None for catalog table
    source_path: str | None = None
    source_options: dict = field(default_factory=dict)
    filters: list[ParsedFilter] = field(default_factory=list)
    filter_groups: list[list[ParsedFilter]] = field(default_factory=list)
    fields: list[ParsedColumnSpec] = field(default_factory=list)
    drop_nulls_in: list[str] = field(default_factory=list)
    drop_duplicates_on: list[str] = field(default_factory=list)
    dev_limit: int | None = None
    is_loader: bool = False
    loader_name: str | None = None
    loader_args: dict = field(default_factory=dict)
    streaming: bool = False               # read via readStream (Plan 27)


@dataclass
class ParsedPartial:
    """A nested YAML sub-transformation exposed under ``alias``.

    Attributes:
        alias: Name under which the child output is available to joins/rules.
        resolved_path: Absolute path the child schema was loaded from.
        schema: The normalized child schema dict (executed lazily by the interpreter).
    """
    alias: str
    resolved_path: str | None = None
    schema: dict = field(default_factory=dict)


@dataclass(frozen=True)
class ParsedOwner:
    """Structured ownership metadata for one data product (Plan 31.3.2)."""

    team: str | None = None
    steward: str | None = None
    domain: str | None = None
    contact: str | None = None


@dataclass(frozen=True)
class ParsedDataProduct:
    """Versioned ownership metadata for one declarative data product (Plan 29)."""

    id: str
    version: str
    owner: str | ParsedOwner | None = None
    description: str | None = None
    domain: str | None = None

    @property
    def owner_label(self) -> str | None:
        """Stable string representation for legacy owner consumers."""
        if self.owner is None:
            return None
        if isinstance(self.owner, str):
            return self.owner
        return self.owner.team or self.owner.steward or self.owner.contact

    @property
    def owner_domain(self) -> str | None:
        """Effective owner domain: product-level domain first, then owner mapping."""
        if self.domain:
            return self.domain
        return self.owner.domain if isinstance(self.owner, ParsedOwner) else None


def _parse_data_product_owner(raw_owner: object) -> str | ParsedOwner | None:
    if isinstance(raw_owner, str):
        return raw_owner
    if isinstance(raw_owner, dict):
        return ParsedOwner(
            **{key: raw_owner.get(key) for key in ("team", "steward", "domain", "contact")}
        )
    return None


@dataclass(frozen=True)
class ParsedOutputField:
    """Declared business contract for one final output field (Plan 29)."""

    name: str
    logical_type: str | None = None
    required: bool | None = None
    unique: bool | None = None
    classification: str | None = None
    entity: str | None = None
    description: str | None = None


@dataclass(frozen=True)
class ParsedSla:
    """Contract SLA metadata (Plan 31.3.3)."""

    refresh_frequency: str | None = None
    max_latency: str | None = None


@dataclass(frozen=True)
class ParsedSecurity:
    """Contract security metadata (Plan 31.3.3)."""

    level: str | None = None
    access_policy: str | None = None


@dataclass(frozen=True)
class ParsedSemanticSeed:
    """Minimal semantic metadata co-authored with a pipeline (Plan 29)."""

    model_key: str
    entity: str | None = None
    default_time_dimension: str | None = None
    dimensions: tuple[str, ...] = field(default_factory=tuple)

    def __post_init__(self):
        object.__setattr__(self, "dimensions", tuple(self.dimensions))


@dataclass
class ParsedSchema:
    """The fully parsed pipeline schema — a single source of truth for all consumers.

    Attributes:
        partials: Nested sub-transformation specs, resolved before tables.
        tables: Source table specs in declaration order.
        joins: JOIN specs in application order.
        business_rules: Registered rule names in application order.
        select_final: Final output column specs (or empty if keep_all_columns).
        add_columns: Extra column specs added to an unchanged output.
        keep_all_columns: When True, select_final is empty and all columns pass through.
        aggregate: Declarative GROUP BY spec (Plan 28), or None when absent.
        dev_limit: Optional row cap applied at schema level (overridden by table-level).
        sink: Raw sink config dict (unchanged for now — out of IR scope).
        materialization: Normalized materialization dict (Plan 27), or None for batch.
        data_product: Optional versioned product metadata (Plan 29).
        contract_output: Explicit output-field contracts (Plan 29).
        contract_grain: Optional declared output grain (Plan 29).
        contract_status: Contract lifecycle status (Plan 31.3.3).
        contract_reviewers: Human reviewers for lifecycle governance.
        contract_effective_from: Optional inclusive ISO date lower bound.
        contract_effective_until: Optional inclusive ISO date upper bound.
        contract_sla: Optional SLA block.
        contract_security: Optional security block.
        semantic: Optional seed for a generated semantic model (Plan 29).
        raw: The original normalized schema dict (for callers not yet on IR).
    """
    partials: list[ParsedPartial] = field(default_factory=list)
    tables: list[ParsedTable] = field(default_factory=list)
    joins: list[ParsedJoin] = field(default_factory=list)
    business_rules: list[str] = field(default_factory=list)
    select_final: list[ParsedColumnSpec] = field(default_factory=list)
    add_columns: list[ParsedColumnSpec] = field(default_factory=list)
    keep_all_columns: bool = False
    aggregate: "ParsedAggregate | None" = None
    dev_limit: int | None = None
    sink: dict | None = None
    materialization: dict | None = None
    data_product: ParsedDataProduct | None = None
    contract_output: list[ParsedOutputField] = field(default_factory=list)
    contract_grain: list[str] = field(default_factory=list)
    contract_status: str = DEFAULT_CONTRACT_STATUS
    contract_reviewers: tuple[str, ...] = field(default_factory=tuple)
    contract_effective_from: str | None = None
    contract_effective_until: str | None = None
    contract_sla: ParsedSla | None = None
    contract_security: ParsedSecurity | None = None
    semantic: ParsedSemanticSeed | None = None
    raw: dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------


def parse_to_ir(schema_dict: dict) -> ParsedSchema:
    """
    Parse a normalized schema dict (output of _normalize_schema) into a ParsedSchema IR.

    Args:
        schema_dict: Normalized pipeline schema dict.

    Returns:
        ParsedSchema representing the full pipeline.
    """
    from skifer.core.op_catalog import (
        resolve_filter_operator,
        resolve_column_op,
    )

    # --- Partials ---
    partials = [
        ParsedPartial(
            alias=p["alias"],
            resolved_path=p.get("resolved_path"),
            schema=p.get("schema", {}),
        )
        for p in schema_dict.get("partials", [])
    ]

    # --- Tables ---
    tables = []
    for t in schema_dict.get("tables", []):
        tables.append(_parse_table(t, resolve_filter_operator))

    # --- Joins ---
    joins = []
    for j in schema_dict.get("join", []):
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
        raw_type = j.get("type", "left")
        join_type = _normalise_join_type(raw_type)
        if join_type not in VALID_JOIN_TYPES:
            raise ValueError(
                f"Invalid join type {raw_type!r} for join {alias_l!r} → {alias_r!r}. "
                f"Valid types: {', '.join(sorted(VALID_JOIN_TYPES))}"
            )
        joins.append(ParsedJoin(
            alias_left=alias_l,
            keys_left=key_l if isinstance(key_l, list) else [key_l],
            alias_right=alias_r,
            keys_right=key_r if isinstance(key_r, list) else [key_r],
            join_type=join_type,
        ))

    # --- select_final / add_columns ---
    select_final = [
        _parse_column_spec(f, resolve_column_op)
        for f in schema_dict.get("select_final", [])
    ]
    add_columns = [
        _parse_column_spec(f, resolve_column_op)
        for f in schema_dict.get("add_columns", [])
    ]

    # --- aggregate (Plan 28) ---
    raw_aggregate = schema_dict.get("aggregate")
    aggregate = None
    if raw_aggregate:
        aggregate = ParsedAggregate(
            group_by=list(raw_aggregate.get("group_by", [])),
            measures=[
                ParsedMeasure(source=m["source"], target=m["target"], func=m["func"])
                for m in raw_aggregate.get("measures", [])
            ],
            having=[
                ParsedFilter(
                    column=h["column"],
                    operator=resolve_filter_operator(h["operator"]) or h["operator"],
                    value=h.get("value"),
                )
                for h in raw_aggregate.get("having", [])
            ],
        )

    # --- Agent-ready product / contract / semantic seed (Plan 29) ---
    raw_product = schema_dict.get("data_product")
    data_product = None
    if raw_product:
        data_product = ParsedDataProduct(
            id=raw_product["id"],
            version=raw_product["version"],
            owner=_parse_data_product_owner(raw_product.get("owner")),
            description=raw_product.get("description"),
            domain=raw_product.get("domain"),
        )

    raw_contract = schema_dict.get("contract") or {}
    contract_output = [
        ParsedOutputField(name=name, **metadata)
        for name, metadata in raw_contract.get("output", {}).items()
    ]
    contract_grain = list(raw_contract.get("grain", []))
    contract_sla = ParsedSla(**raw_contract["sla"]) if raw_contract.get("sla") else None
    contract_security = (
        ParsedSecurity(**raw_contract["security"]) if raw_contract.get("security") else None
    )

    raw_semantic = schema_dict.get("semantic")
    semantic = None
    if raw_semantic:
        semantic = ParsedSemanticSeed(
            model_key=raw_semantic["model_key"],
            entity=raw_semantic.get("entity"),
            default_time_dimension=raw_semantic.get("default_time_dimension"),
            dimensions=tuple(raw_semantic.get("dimensions", [])),
        )

    return ParsedSchema(
        partials=partials,
        tables=tables,
        joins=joins,
        business_rules=schema_dict.get("business_rules", []),
        select_final=select_final,
        add_columns=add_columns,
        keep_all_columns=bool(schema_dict.get("keep_all_columns", False)),
        aggregate=aggregate,
        dev_limit=schema_dict.get("dev_limit"),
        sink=schema_dict.get("sink"),
        materialization=schema_dict.get("materialization"),
        data_product=data_product,
        contract_output=contract_output,
        contract_grain=contract_grain,
        contract_status=raw_contract.get("status", DEFAULT_CONTRACT_STATUS),
        contract_reviewers=tuple(raw_contract.get("reviewers", [])),
        contract_effective_from=raw_contract.get("effective_from"),
        contract_effective_until=raw_contract.get("effective_until"),
        contract_sla=contract_sla,
        contract_security=contract_security,
        semantic=semantic,
        raw=schema_dict,
    )


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _parse_table(t: dict, resolve_filter_op) -> ParsedTable:
    """Convert a single table dict to ParsedTable."""
    alias = t.get("alias", t["name"])
    filters = _parse_filter_list(t.get("filter", []), resolve_filter_op)
    filter_groups = [
        _parse_filter_list(g, resolve_filter_op)
        for g in t.get("filter_groups", [])
    ]

    # Source
    source_type = None
    source_path = None
    source_options: dict = {}
    is_loader = False
    loader_name = None
    loader_args: dict = {}

    if t.get("source_type") == "loader":
        is_loader = True
        loader_name = t.get("function_name")
        loader_args = t.get("arguments", {})
    elif "source" in t:
        src = t["source"]
        source_type = src.get("type")
        source_path = src.get("path")
        source_options = src.get("options", {})

    # Quality checks
    qc = t.get("quality_checks", {})
    drop_nulls_in = qc.get("drop_nulls_in", [])
    drop_duplicates_on = qc.get("drop_duplicates_on", [])

    # Field selection at source
    from skifer.core.op_catalog import resolve_column_op
    fields = [
        _parse_column_spec(f, resolve_column_op)
        for f in t.get("fields", [])
    ]

    return ParsedTable(
        name=t["name"],
        alias=alias,
        source_type=source_type,
        source_path=source_path,
        source_options=source_options,
        filters=filters,
        filter_groups=filter_groups,
        fields=fields,
        drop_nulls_in=list(drop_nulls_in),
        drop_duplicates_on=list(drop_duplicates_on),
        dev_limit=t.get("dev_limit"),
        is_loader=is_loader,
        loader_name=loader_name,
        loader_args=loader_args,
        streaming=bool(t.get("streaming", False)),
    )


def _parse_filter_list(filter_list: list, resolve_op) -> list[ParsedFilter]:
    """Convert a list of filter dicts (already normalized) to ParsedFilter list."""
    result = []
    for f in filter_list:
        if isinstance(f, dict):
            result.append(ParsedFilter(
                column=f["column"],
                operator=f["operator"],
                value=f.get("value"),
            ))
        # Already-normalized dicts are the expected form after _normalize_filters
    return result


def _parse_column_spec(config, resolve_column_op) -> ParsedColumnSpec:
    """Convert a single select_final / add_columns / fields entry to ParsedColumnSpec."""
    # Dict form: {source, target, ops: [{when, then}, ..., {else}]}
    if isinstance(config, dict):
        source = config.get("source")
        target = config["target"]
        ops_raw = config.get("ops", [])
        when_conditions = [op for op in ops_raw if isinstance(op, dict) and "when" in op]
        else_op = next((op for op in ops_raw if isinstance(op, dict) and "else" in op), None)

        if when_conditions:
            when_chain = [
                WhenClause(
                    condition=_parse_op(f"when:{wc['when']}"),
                    then=_parse_op(wc["then"]),
                )
                for wc in when_conditions
            ]
            otherwise = _parse_op(else_op["else"]) if else_op else None
            return ParsedColumnSpec(source=source, target=target, when_chain=when_chain, otherwise=otherwise)
        else:
            ops = [_parse_op(op) for op in ops_raw if isinstance(op, str)]
            return ParsedColumnSpec(source=source, target=target, ops=ops)

    # List form: [source, target] or [source, target, [ops...]]
    source = config[0]
    target = config[1]
    operations = config[2] if len(config) > 2 else []

    # Compact when/then/else: [source, target, ["when:...", "then:...", "else:..."]]
    if operations and isinstance(operations[0], str) and operations[0].startswith("when:"):
        # Validated at schema load time (B.2); guard here for safety
        if len(operations) == 3:
            when_chain = [WhenClause(
                condition=_parse_op(operations[0]),
                then=_parse_op(operations[1]),
            )]
            otherwise = _parse_op(operations[2])
            return ParsedColumnSpec(
                source=source, target=target, when_chain=when_chain, otherwise=otherwise
            )

    ops = [_parse_op(op) for op in operations]
    return ParsedColumnSpec(source=source, target=target, ops=ops)


def _parse_op(op_str: str) -> ParsedOp:
    """Parse an op-string like 'cast:double' or 'round:2' into a ParsedOp."""
    if not isinstance(op_str, str):
        return ParsedOp(str(op_str), ())
    parts = op_str.split(":", 1)
    name = parts[0].strip()
    if len(parts) == 1:
        return ParsedOp(name, ())
    # Some ops have multiple args separated by comma (split:sep,idx; substring:start,len)
    arg_str = parts[1]
    return ParsedOp(name, tuple(arg_str.split(",")))
