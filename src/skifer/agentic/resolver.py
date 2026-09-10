"""
QueryResolver — résolution déterministe SemanticQuery → SQL.

Le QueryResolver est un module SANS LLM : il prend une SemanticQuery
(produite par le LLM, contenant uniquement des noms) et construit le SQL
complet en lisant les définitions du YAML.

Si un nom est absent du YAML, une SemanticQueryError est levée immédiatement
— sans toucher à Spark.
"""

from __future__ import annotations

import difflib
import logging
import re
from dataclasses import dataclass, field

from ..semantic.calendar import parse_iso_date

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Erreur
# ---------------------------------------------------------------------------

class SemanticQueryError(Exception):
    """Levée quand la SemanticQuery contient un nom absent du modèle YAML."""


# ---------------------------------------------------------------------------
# SemanticQuery — objet frontière LLM / QueryResolver
# ---------------------------------------------------------------------------

@dataclass
class SemanticQuery:
    """
    Objet produit par le LLM (Step B).
    Ne contient que des noms tirés du YAML — jamais du SQL.

    Attributs:
        model_name:  Clé du modèle (ex: "kpi_orders.erp").
        metrics:     Noms de métriques à calculer (ex: ["gross_revenue"]).
        group_by:    Noms de dimensions pour le GROUP BY (ex: ["region"]).
        filters:     Filtres déclaratifs sur des dimensions du modèle.
                     Format: [{"column": "region", "operator": "eq", "value": "EMEA"}]
                     Operators: eq, neq, gt, lt, gte, lte, in, like, is_null, is_not_null.
        date_from:   Date de début ISO 8601 (ex: "2024-01-01"). Optionnel.
        date_to:     Date de fin ISO 8601 (ex: "2024-12-31"). Optionnel.
        period:      Nom d'une période déclarée dans le calendrier du modèle.
        mode:        "query" → DataFrame, "view" → CREATE VIEW.
        view_name:   Requis si mode="view" (nom court, sans schéma).
        explanation: Ce que le LLM a compris (pour debug / UX).
        response_format: Format de retour détecté ("kpi", "table", "chart", "text_analysis").
    """
    model_name: str
    metrics: list[str] = field(default_factory=list)
    group_by: list[str] = field(default_factory=list)
    filters: list[dict] = field(default_factory=list)
    date_from: str | None = None
    date_to: str | None = None
    mode: str = "query"
    view_name: str | None = None
    explanation: str = ""
    response_format: str = "table"
    period: str | None = None

    @classmethod
    def from_dict(cls, data: dict) -> "SemanticQuery":
        """Construit une SemanticQuery depuis le dict JSON retourné par le LLM."""
        return cls(
            model_name=data["model_name"],
            metrics=data.get("metrics", []),
            group_by=data.get("group_by", []),
            filters=data.get("filters", []),
            date_from=data.get("date_from"),
            date_to=data.get("date_to"),
            period=data.get("period"),
            mode=data.get("mode", "query"),
            view_name=data.get("view_name"),
            explanation=data.get("explanation", ""),
            response_format=data.get("response_format", "table"),
        )


# ---------------------------------------------------------------------------
# ResolvedQuery — résultat du QueryResolver
# ---------------------------------------------------------------------------

@dataclass
class SelectedExpression:
    """One semantic member actually selected by a query, with its definition.

    Only selected members are captured. Snapshotting the whole model would drag
    every unused metric, description and `base_filter` into the evidence, which
    is both heavier than needed and a standing invitation for a later
    serializer change to expose them.
    """
    kind: str                   # "metric" | "dimension"
    model_key: str
    name: str
    sql: str
    aggregate_type: str | None = None
    inline_filters: tuple[str, ...] = ()


@dataclass
class ResolvedQuery:
    """SQL complet prêt à exécuter, construit depuis les définitions YAML."""
    select_exprs: list[str]     # ex: ["SUM(amount_ttc) AS gross_revenue", "region AS region"]
    from_fqn: str               # ex: "`catalog`.`gold`.`fact_orders`"
    where_clauses: list[str]    # ex: ["source_system = 'ERP'", "region = 'EMEA'"]
    group_by_exprs: list[str]   # ex: ["region"]
    full_sql: str               # SQL complet assemblé
    # Logical plan (Plan 29, slice 4.2) — additive, defaulted so that every
    # existing positional construction keeps working unchanged.
    sources: tuple[str, ...] = ()
    model_keys: tuple[str, ...] = ()
    selected_expressions: tuple[SelectedExpression, ...] = ()


# ---------------------------------------------------------------------------
# QueryResolver
# ---------------------------------------------------------------------------

# Opérateurs supportés dans SemanticQuery.filters
_OPERATOR_MAP = {
    "eq":          "= {}",
    "neq":         "!= {}",
    "gt":          "> {}",
    "lt":          "< {}",
    "gte":         ">= {}",
    "lte":         "<= {}",
    "like":        "LIKE {}",
    "in":          "IN ({})",
    "is_null":     "IS NULL",
    "is_not_null": "IS NOT NULL",
}


class QueryResolver:
    """
    Résout une SemanticQuery en SQL complet.

    Déterministe et sans LLM : toute la logique SQL vient des définitions YAML.
    Lève SemanticQueryError si un nom est absent — jamais de SQL inventé.
    """

    def __init__(self, planner=None) -> None:
        self.planner = planner

    def resolve(
        self,
        query: SemanticQuery,
        model: dict,
        catalog_fqn: str | None,
    ) -> ResolvedQuery:
        """
        Valide et résout une SemanticQuery.

        Args:
            query:       SemanticQuery produite par le LLM.
            model:       Définition complète du modèle (dict YAML complet).
            catalog_fqn: FQN du catalogue Databricks (ex: "`my_catalog`").

        Returns:
            ResolvedQuery avec le SQL complet.

        Raises:
            SemanticQueryError: Si un nom est absent du modèle.
        """
        if self.planner is None:
            if query.period is not None:
                raise SemanticQueryError(
                    "A period query requires a SemanticPlanner to resolve its "
                    "declared calendar definition."
                )
            self._validate(query, model)
            return self._build_sql(query, model, catalog_fqn)

        plan = self.planner.plan(query)
        if self._is_unqualified_root_query(query, plan):
            # This branch is intentionally the pre-6.4 implementation. Existing
            # single-model queries must remain byte-for-byte stable.
            self._validate(query, model)
            return self._build_sql(query, model, catalog_fqn)
        return self.compile_plan(query, plan, catalog_fqn)

    def compile_plan(
        self,
        query: SemanticQuery,
        plan,
        catalog_fqn: str | None,
    ) -> ResolvedQuery:
        """Compile a ``SemanticPlan`` using only its curated model definitions."""
        models = {
            model_key: self.planner.semantic._get_model(model_key)
            for model_key in plan.required_models
        }
        aliases = {
            model_key: f"m{index}"
            for index, model_key in enumerate(plan.required_models)
        }
        for alias in aliases.values():
            self._assert_safe_identifier(alias, "generated table alias")

        self._validate_plan_query(query, plan)
        dimension_refs = {ref.reference: ref for ref in plan.dimensions}
        metric_refs = {ref.reference: ref for ref in plan.metrics}

        group_by_exprs: list[str] = []
        select_dims: list[str] = []
        for reference in query.group_by:
            ref = dimension_refs[reference]
            dimension = self._definition(models[ref.model_key], "dimensions", ref.name)
            expression = self._qualified_column(
                aliases[ref.model_key], dimension["sql"], "dimension"
            )
            group_by_exprs.append(expression)
            self._assert_safe_identifier(ref.name, "dimension output alias")
            select_dims.append(f"{expression} AS {ref.name}")

        select_metrics: list[str] = []
        for reference in query.metrics:
            ref = metric_refs[reference]
            metric = self._definition(models[ref.model_key], "metrics", ref.name)
            select_metrics.append(
                self._build_metric_expr(metric, qualifier=aliases[ref.model_key])
            )

        root_alias = aliases[plan.root_model]
        root_source = self._model_source(
            models[plan.root_model], catalog_fqn, root_alias
        )
        from_parts = [root_source]
        for join in plan.joins:
            source_alias = aliases[join.source_model]
            target_alias = aliases[join.target_model]
            target_source = self._model_source(
                models[join.target_model], catalog_fqn, target_alias
            )
            predicates = [
                f"{source_alias}.{self._quote_identifier(source_column)} = "
                f"{target_alias}.{self._quote_identifier(target_column)}"
                for source_column, target_column in zip(
                    join.source_key_columns,
                    join.target_key_columns,
                    strict=True,
                )
            ]
            join_keyword = "LEFT JOIN" if join.join_type == "left" else "INNER JOIN"
            from_parts.append(
                f"{join_keyword} {target_source}\n  ON " + "\n AND ".join(predicates)
            )
        from_fqn = "\n".join(from_parts)

        where_clauses: list[str] = []
        for filter_def in query.filters:
            reference = filter_def["column"]
            ref = dimension_refs[reference]
            dimension = self._definition(models[ref.model_key], "dimensions", ref.name)
            expression = self._qualified_column(
                aliases[ref.model_key], dimension["sql"], "filter dimension"
            )
            where_clauses.append(self._build_filter_clause(expression, filter_def))

        if plan.date_from or plan.date_to:
            self._validate_date_bounds(plan.date_from, plan.date_to)
            date_ref = self._planned_date_dimension(plan, models)
            if date_ref is not None:
                date_dimension = self._definition(
                    models[date_ref.model_key], "dimensions", date_ref.name
                )
                date_sql = self._qualified_column(
                    aliases[date_ref.model_key], date_dimension["sql"], "date dimension"
                )
                if plan.date_from:
                    where_clauses.append(f"{date_sql} >= '{plan.date_from}'")
                if plan.date_to:
                    where_clauses.append(f"{date_sql} <= '{plan.date_to}'")

        select_exprs = select_dims + select_metrics
        full_sql = self._assemble_sql(
            select_exprs, from_fqn, where_clauses, group_by_exprs
        )
        selected = tuple(
            self._selected_expression("dimension", dimension_refs[reference], models)
            for reference in query.group_by
        ) + tuple(
            self._selected_expression("metric", metric_refs[reference], models)
            for reference in query.metrics
        )
        return ResolvedQuery(
            select_exprs=select_exprs,
            from_fqn=from_fqn,
            where_clauses=where_clauses,
            group_by_exprs=group_by_exprs,
            full_sql=full_sql,
            sources=tuple(
                str(models[model_key].get("table", "")) or model_key
                for model_key in plan.required_models
            ),
            model_keys=tuple(plan.required_models),
            selected_expressions=selected,
        )

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def _validate(self, query: SemanticQuery, model: dict) -> None:
        self._validate_date_bounds(query.date_from, query.date_to)
        dim_names = {d["name"] for d in model.get("dimensions", [])}
        metric_names = {m["name"] for m in model.get("metrics", [])}

        for m in query.metrics:
            if m not in metric_names:
                suggestions = self._suggest_closest(m, metric_names)
                hint = f" Vouliez-vous dire : {suggestions} ?" if suggestions else ""
                raise SemanticQueryError(
                    f"Metric '{m}' introuvable dans '{query.model_name}'.{hint} "
                    f"Disponible : {sorted(metric_names)}"
                )

        for d in query.group_by:
            if d not in dim_names:
                suggestions = self._suggest_closest(d, dim_names)
                hint = f" Vouliez-vous dire : {suggestions} ?" if suggestions else ""
                raise SemanticQueryError(
                    f"Dimension '{d}' introuvable dans '{query.model_name}'.{hint} "
                    f"Disponible : {sorted(dim_names)}"
                )

        for f in query.filters:
            col = f.get("column", "")
            if col and col not in dim_names:
                suggestions = self._suggest_closest(col, dim_names)
                hint = f" Vouliez-vous dire : {suggestions} ?" if suggestions else ""
                raise SemanticQueryError(
                    f"Colonne de filtre '{col}' introuvable dans '{query.model_name}'.{hint} "
                    f"Disponible : {sorted(dim_names)}"
                )
            op = f.get("operator", "")
            if op and op not in _OPERATOR_MAP:
                raise SemanticQueryError(
                    f"Opérateur '{op}' non supporté. "
                    f"Supportés : {sorted(_OPERATOR_MAP.keys())}"
                )

    # ------------------------------------------------------------------
    # Construction SQL
    # ------------------------------------------------------------------

    def _build_sql(
        self,
        query: SemanticQuery,
        model: dict,
        catalog_fqn: str,
    ) -> ResolvedQuery:
        dim_dict = {d["name"]: d for d in model.get("dimensions", [])}
        met_dict = {m["name"]: m for m in model.get("metrics", [])}

        # FROM — table source
        table_fqn = self._resolve_table_fqn(model, catalog_fqn)

        # GROUP BY — dimensions
        group_by_exprs: list[str] = []
        select_dims: list[str] = []
        for dim_name in query.group_by:
            dim = dim_dict[dim_name]
            sql_expr = dim["sql"]
            group_by_exprs.append(sql_expr)
            select_dims.append(f"{sql_expr} AS {dim_name}")

        # SELECT — métriques
        select_metrics: list[str] = []
        for met_name in query.metrics:
            met = met_dict[met_name]
            select_metrics.append(self._build_metric_expr(met))

        select_exprs = select_dims + select_metrics

        # WHERE — base_filter YAML + filtres SemanticQuery + dates
        where_clauses: list[str] = []

        base_filter = model.get("base_filter")
        if base_filter:
            where_clauses.append(base_filter)

        for f in query.filters:
            col = f["column"]
            dim = dim_dict[col]
            clause = self._build_filter_clause(dim["sql"], f)
            where_clauses.append(clause)

        # date_from / date_to — dimension de type date
        if query.date_from or query.date_to:
            date_dim = self._find_date_dimension(model)
            if date_dim:
                date_sql = dim_dict[date_dim]["sql"]
                if query.date_from:
                    where_clauses.append(f"{date_sql} >= '{query.date_from}'")
                if query.date_to:
                    where_clauses.append(f"{date_sql} <= '{query.date_to}'")

        full_sql = self._assemble_sql(
            select_exprs, table_fqn, where_clauses, group_by_exprs
        )

        model_key = str(model.get("key") or model.get("name") or query.model_name)
        selected = tuple(
            self._describe_member("dimension", model_key, dim_dict[name])
            for name in query.group_by
        ) + tuple(
            self._describe_member("metric", model_key, met_dict[name])
            for name in query.metrics
        )
        return ResolvedQuery(
            select_exprs=select_exprs,
            from_fqn=table_fqn,
            where_clauses=where_clauses,
            group_by_exprs=group_by_exprs,
            full_sql=full_sql,
            sources=(str(model.get("table", "")) or model_key,),
            model_keys=(model_key,),
            selected_expressions=selected,
        )

    def _selected_expression(self, kind: str, ref, models: dict) -> SelectedExpression:
        block = "metrics" if kind == "metric" else "dimensions"
        definition = self._definition(models[ref.model_key], block, ref.name)
        return self._describe_member(kind, ref.model_key, definition)

    @staticmethod
    def _describe_member(kind: str, model_key: str, definition: dict) -> SelectedExpression:
        return SelectedExpression(
            kind=kind,
            model_key=model_key,
            name=str(definition.get("name", "")),
            sql=str(definition.get("sql", "")),
            aggregate_type=(
                str(definition["type"]) if kind == "metric" and definition.get("type") else None
            ),
            inline_filters=tuple(
                str(item.get("sql", ""))
                for item in (definition.get("filters") or [])
                if isinstance(item, dict)
            ),
        )

    def _resolve_table_fqn(self, model: dict, catalog_fqn: str | None) -> str:
        """
        Construit le FQN complet depuis model["table"] et le catalogue.
        model["table"] est au format "layer.table_name" (ex: "gold.fact_orders").
        catalog_fqn est au format "`my_catalog`" ou "my_catalog".
        """
        table_ref = model.get("table", "")
        parts = table_ref.split(".")
        if len(parts) == 2:
            schema, table = parts
            if catalog_fqn is None:
                return f"`{schema}`.`{table}`"
            cat = catalog_fqn.strip("`")
            return f"`{cat}`.`{schema}`.`{table}`"
        # FQN déjà complet
        return table_ref

    def _build_metric_expr(self, met: dict, qualifier: str | None = None) -> str:
        """Construit l'expression SQL d'agrégation pour une métrique."""
        name = met["name"]
        sql = met["sql"]
        agg_type = met.get("type", "sum").lower()

        if qualifier is not None:
            self._assert_safe_identifier(name, "metric output alias")
            sql = self._qualified_column(qualifier, sql, "metric")

        # Filtres métrique inline (CASE WHEN ... THEN col END)
        filters = met.get("filters", []) or []
        if filters:
            conditions = " AND ".join(
                self._qualify_metric_filter(f["sql"], qualifier)
                if qualifier is not None
                else f["sql"]
                for f in filters
            )
            inner = f"CASE WHEN {conditions} THEN {sql} END"
        else:
            inner = sql

        if agg_type == "sum":
            expr = f"SUM({inner})"
        elif agg_type == "count_distinct":
            expr = f"COUNT(DISTINCT {inner})"
        elif agg_type == "count":
            expr = f"COUNT({inner})"
        elif agg_type == "avg":
            expr = f"AVG({inner})"
        elif agg_type == "min":
            expr = f"MIN({inner})"
        elif agg_type == "max":
            expr = f"MAX({inner})"
        else:
            raise SemanticQueryError(
                f"Type d'agrégation '{agg_type}' non supporté pour la métrique '{name}'."
            )

        return f"{expr} AS {name}"

    def _validate_plan_query(self, query: SemanticQuery, plan) -> None:
        metric_refs = {ref.reference for ref in plan.metrics}
        dimension_refs = {ref.reference for ref in plan.dimensions}
        if set(query.metrics) - metric_refs:
            raise SemanticQueryError("Semantic plan is missing a requested metric.")
        if set(query.group_by) - dimension_refs:
            raise SemanticQueryError("Semantic plan is missing a requested dimension.")
        for filter_def in query.filters:
            reference = filter_def.get("column", "")
            if reference and reference not in dimension_refs:
                raise SemanticQueryError(
                    f"Semantic plan is missing filter dimension '{reference}'."
                )
            operator = filter_def.get("operator", "")
            if operator and operator not in _OPERATOR_MAP:
                raise SemanticQueryError(
                    f"Opérateur '{operator}' non supporté. "
                    f"Supportés : {sorted(_OPERATOR_MAP.keys())}"
                )

    @staticmethod
    def _definition(model: dict, member_type: str, name: str) -> dict:
        for definition in model.get(member_type, []):
            if definition.get("name") == name:
                return definition
        raise SemanticQueryError(
            f"Semantic model '{model.get('key') or model.get('name')}' no longer "
            f"declares {member_type[:-1]} '{name}'; reload the semantic catalog."
        )

    def _model_source(self, model: dict, catalog_fqn: str | None, alias: str) -> str:
        table_fqn = self._resolve_planned_table_fqn(model, catalog_fqn)
        base_filter = model.get("base_filter")
        if base_filter:
            # The legacy base_filter is curated SQL scoped to its own source. A
            # subquery prevents names in it from becoming ambiguous after joins.
            return f"(SELECT * FROM {table_fqn} WHERE {base_filter}) AS {alias}"
        return f"{table_fqn} AS {alias}"

    def _resolve_planned_table_fqn(
        self, model: dict, catalog_fqn: str | None
    ) -> str:
        table_ref = model.get("table", "")
        self._validate_table_reference(table_ref)
        parts = [part.strip("`") for part in table_ref.split(".")]
        if len(parts) == 2 and catalog_fqn is not None:
            catalog = catalog_fqn.strip("`")
            self._assert_safe_identifier(catalog, "catalog identifier")
            parts.insert(0, catalog)
        return ".".join(self._quote_identifier(part) for part in parts)

    def _qualified_column(self, alias: str, sql: str, object_type: str) -> str:
        self._assert_safe_identifier(alias, "generated table alias")
        if sql == "*":
            return "*"
        self._assert_safe_identifier(sql, f"{object_type} SQL column")
        return f"{alias}.{self._quote_identifier(sql)}"

    def _qualify_metric_filter(self, expression: str, qualifier: str) -> str:
        """Qualify the leading column in a curated, simple metric predicate."""
        match = re.fullmatch(r"\s*([A-Za-z_][A-Za-z0-9_]*)(\s+.+)\s*", expression)
        if match is None:
            raise SemanticQueryError(
                "Multi-model metric filters must start with one SQL-safe column "
                f"identifier; unsupported predicate: {expression!r}."
            )
        column, predicate = match.groups()
        return f"{qualifier}.{self._quote_identifier(column)}{predicate}"

    @staticmethod
    def _planned_date_dimension(plan, models: dict):
        for model_key in plan.required_models:
            for dimension in models[model_key].get("dimensions", []):
                if dimension.get("type", "").lower() in {
                    "date",
                    "timestamp",
                    "datetime",
                }:
                    from ..semantic.planner import DimensionRef

                    return DimensionRef(
                        model_key=model_key,
                        name=dimension["name"],
                        reference=dimension["name"],
                    )
        return None

    @staticmethod
    def _is_unqualified_root_query(query: SemanticQuery, plan) -> bool:
        return (
            query.period is None
            and plan.required_models == (plan.root_model,)
            and all(ref.model_key == plan.root_model for ref in plan.metrics)
            and all(ref.model_key == plan.root_model for ref in plan.dimensions)
            and all(ref.reference == ref.name for ref in (*plan.metrics, *plan.dimensions))
        )

    @staticmethod
    def _validate_date_bounds(date_from: str | None, date_to: str | None) -> None:
        for value, label in ((date_from, "date_from"), (date_to, "date_to")):
            if value is None:
                continue
            try:
                parse_iso_date(value, label)
            except ValueError as exc:
                raise SemanticQueryError(str(exc)) from exc

    @staticmethod
    def _assert_safe_identifier(value: str, label: str) -> None:
        if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value) is None:
            raise SemanticQueryError(
                f"Unsafe {label} '{value}'; expected a SQL-safe identifier."
            )

    @classmethod
    def _quote_identifier(cls, value: str) -> str:
        cls._assert_safe_identifier(value, "SQL identifier")
        return f"`{value}`"

    @classmethod
    def _validate_table_reference(cls, table_ref: str) -> None:
        parts = table_ref.split(".")
        if len(parts) not in {2, 3}:
            raise SemanticQueryError(
                f"Unsafe semantic table reference '{table_ref}'; expected schema.table "
                "or catalog.schema.table."
            )
        for part in parts:
            cls._assert_safe_identifier(part.strip("`"), "table identifier")

    def _build_filter_clause(self, col_sql: str, f: dict) -> str:
        """Construit une clause WHERE depuis un filtre SemanticQuery."""
        op = f.get("operator", "eq")
        value = f.get("value")
        template = _OPERATOR_MAP[op]

        if op in ("is_null", "is_not_null"):
            return f"{col_sql} {template}"

        if op == "in":
            if isinstance(value, list):
                members = value
            else:
                logger.warning(
                    "Filter operator 'in' received a scalar value '%s' — expected a list. "
                    "Wrapping as single-element list.",
                    value,
                )
                members = [value]
            quoted = ", ".join(
                self._render_literal(member, col_sql) for member in members
            )
            return f"{col_sql} {template.format(quoted)}"

        return f"{col_sql} {template.format(self._render_literal(value, col_sql))}"

    @classmethod
    def _render_literal(cls, value, col_sql: str) -> str:
        """Render one filter value as a SQL literal, or refuse it.

        Every value reaching here comes from LLM output, so the type check is
        the boundary: only scalars are renderable. Anything else used to be
        interpolated through ``str()``, which carried its quotes into the query
        verbatim.
        """
        if isinstance(value, (bool, int, float)):
            return str(value)
        if isinstance(value, str):
            cls._assert_safe_scalar(value, col_sql)
            return f"'{value}'"
        raise SemanticQueryError(
            f"Filter value for '{col_sql}' has unsupported type "
            f"'{type(value).__name__}'; only strings, numbers and booleans are "
            "accepted."
        )

    @staticmethod
    def _assert_safe_scalar(value: str, col_sql: str) -> None:
        """Raise SemanticQueryError if value contains SQL-injection-risky characters."""
        if re.search(r"[;'\"\-\-]", value):
            raise SemanticQueryError(
                f"Filter value for '{col_sql}' contains unsafe characters: {value!r}. "
                "Only plain scalar values are accepted."
            )

    def _find_date_dimension(self, model: dict) -> str | None:
        """Retourne le nom de la première dimension de type date/timestamp."""
        for dim in model.get("dimensions", []):
            if dim.get("type", "").lower() in ("date", "timestamp", "datetime"):
                return dim["name"]
        return None

    def _assemble_sql(
        self,
        select_exprs: list[str],
        from_fqn: str,
        where_clauses: list[str],
        group_by_exprs: list[str],
    ) -> str:
        """Assemble le SQL final."""
        select_part = ",\n    ".join(select_exprs) if select_exprs else "*"
        sql = f"SELECT\n    {select_part}\nFROM {from_fqn}"

        if where_clauses:
            where_part = "\n  AND ".join(where_clauses)
            sql += f"\nWHERE {where_part}"

        if group_by_exprs:
            group_part = ", ".join(group_by_exprs)
            sql += f"\nGROUP BY {group_part}"

        return sql

    # ------------------------------------------------------------------
    # Suggestions (Levenshtein approximatif via difflib)
    # ------------------------------------------------------------------

    def _suggest_closest(self, unknown: str, candidates: set[str], n: int = 3) -> list[str]:
        """Retourne les n candidats les plus proches du nom inconnu."""
        return difflib.get_close_matches(unknown, candidates, n=n, cutoff=0.4)
