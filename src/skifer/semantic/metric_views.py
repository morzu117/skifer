"""Traduit un modèle sémantique en metric view Unity Catalog, pour Genie (Plan 42).

Genie ne lit pas les YAML de `semantic_models/` : il ne connaît que Unity Catalog. Une
*metric view* est une vue Unity Catalog déclarée en YAML, avec des dimensions et des
mesures, que Genie sait interroger. Un espace Genie dont les seules sources sont les
metric views publiées ici ne peut répondre qu'avec les définitions de Skifer.

La traduction est pure (aucun Spark, aucune I/O) : `metric_view_ddl()` rend le texte du
`CREATE VIEW`, à exécuter par l'appelant (`SemanticEngine.publish_metric_views`, un
notebook, ou la CLI). L'expression d'une mesure est produite par
`QueryResolver.aggregate_expression()` — la même fonction que celle qui exécute une
requête sémantique — pour qu'une mesure Genie et une métrique Skifer ne puissent pas
calculer des chiffres différents sous le même nom.

Périmètre v1 (Plan 42, décision D4) : modèles mono-table uniquement. Une métrique
`semi_additive` est refusée — sa sémantique dépend de dimensions épinglées qu'une
metric view ne respecterait pas. `calendar` et les `relationships` ne sont pas traduits.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import re

import yaml

from ..agentic.resolver import QueryResolver, SemanticQueryError

# Même contrainte que SemanticValidator._SAFE_IDENTIFIER : un nom de dimension, de
# mesure ou de vue devient un identifiant SQL dans le DDL généré ici.
_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

_METRIC_VIEW_VERSION = "1.1"

_resolver = QueryResolver()


class MetricViewTranslationError(SemanticQueryError):
    """Un modèle sémantique ne se traduit pas sûrement en metric view."""


@dataclass(frozen=True)
class MetricViewReport:
    """Résultat d'une publication (`SemanticEngine.publish_metric_views`, Plan 42).

    Ne porte que des clés de modèle et du DDL — aucune valeur de donnée, rien à
    rédiger. `refused` nomme chaque modèle non publié (gate de certification ou
    traduction impossible) avec sa raison ; `dropped` nomme les modèles dont la
    vue existante a été supprimée (D3 : une source qui redevient décertifiée
    n'y laisse pas une vue qui continuerait de la servir à Genie).
    """

    published: tuple[str, ...] = ()
    refused: tuple[dict[str, str], ...] = field(default_factory=tuple)
    dropped: tuple[str, ...] = ()
    ddl: tuple[str, ...] = ()

    def to_dict(self) -> dict:
        return {
            "published": list(self.published),
            "refused": [dict(entry) for entry in self.refused],
            "dropped": list(self.dropped),
            "ddl": list(self.ddl),
        }


def view_name(model: dict) -> str:
    """Nom court de la vue : la dernière partie de la clé du modèle (`sales.orders` → `orders`)."""
    key = str(model.get("key") or model.get("name") or "")
    name = key.rsplit(".", 1)[-1]
    _check_identifier(name, model, "nom de vue")
    return name


def source_table(model: dict, catalog: str) -> str:
    """Table source en trois parties ; `gold.kpi_orders` prend le `catalog` donné."""
    parts = [part.strip("`") for part in str(model.get("table", "")).split(".")]
    if len(parts) == 2:
        parts = [catalog, *parts]
    if len(parts) != 3 or not all(parts):
        raise MetricViewTranslationError(
            f"[metric_view] Modèle '{model.get('key')}' : table '{model.get('table')}' "
            "invalide — attendu 'schema.table' ou 'catalog.schema.table'."
        )
    for part in parts:
        _check_identifier(part, model, "partie de nom de table")
    return ".".join(parts)


def metric_view_definition(model: dict, catalog: str) -> dict:
    """Corps YAML (`dict`) de la metric view d'un modèle. Pure — ne touche pas au disque."""
    _reject_unsupported(model)

    definition: dict = {"version": _METRIC_VIEW_VERSION}
    if model.get("description"):
        definition["comment"] = model["description"]
    definition["source"] = source_table(model, catalog)
    if model.get("base_filter"):
        definition["filter"] = model["base_filter"]
    definition["dimensions"] = [
        _member(model, dimension["name"], dimension["sql"], dimension.get("description"))
        for dimension in model.get("dimensions", [])
    ]
    definition["measures"] = [
        _member(model, metric["name"], _measure_expr(model, metric), metric.get("description"))
        for metric in model.get("metrics", [])
    ]
    return definition


def metric_view_ddl(model: dict, catalog: str, schema: str) -> str:
    """`CREATE OR REPLACE VIEW ... WITH METRICS` pour un modèle."""
    for part, what in ((catalog, "catalogue"), (schema, "schéma")):
        _check_identifier(part, model, f"nom de {what} cible")
    body = yaml.safe_dump(
        metric_view_definition(model, catalog),
        sort_keys=False,
        allow_unicode=True,
        width=1000,
    )
    if "$$" in body:
        raise MetricViewTranslationError(
            f"[metric_view] Modèle '{model.get('key')}' : '$$' ne peut apparaître dans "
            "aucune valeur — il termine le corps YAML de la vue."
        )
    return (
        f"CREATE OR REPLACE VIEW `{catalog}`.`{schema}`.`{view_name(model)}`\n"
        f"WITH METRICS\nLANGUAGE YAML\nAS $$\n{body}$$"
    )


def _measure_expr(model: dict, metric: dict) -> str:
    try:
        return _resolver.aggregate_expression(metric)
    except SemanticQueryError as exc:
        raise MetricViewTranslationError(
            f"[metric_view] Modèle '{model.get('key')}', métrique '{metric.get('name')}' : {exc}"
        ) from exc


def _member(model: dict, name: str, expr: str, comment: str | None) -> dict:
    # Genie et Skifer doivent nommer une même mesure ou dimension de façon identique.
    _check_identifier(name, model, "nom de dimension ou de mesure")
    member = {"name": name, "expr": expr}
    if comment:
        member["comment"] = comment
    return member


def _reject_unsupported(model: dict) -> None:
    if model.get("relationships"):
        raise MetricViewTranslationError(
            f"[metric_view] Modèle '{model.get('key')}' : les relationships ne sont pas "
            "traduites en v1 (Plan 42, décision D4) — modèle mono-table uniquement."
        )
    for metric in model.get("metrics", []):
        if metric.get("additivity") == "semi_additive":
            raise MetricViewTranslationError(
                f"[metric_view] Modèle '{model.get('key')}', métrique '{metric.get('name')}' : "
                "semi_additive non traduite en v1 — sa sémantique dépend de dimensions "
                "épinglées qu'une metric view ne respecterait pas."
            )


def _check_identifier(value: str, model: dict, what: str) -> None:
    if not _SAFE_IDENTIFIER.match(value):
        raise MetricViewTranslationError(
            f"[metric_view] Modèle '{model.get('key')}' : {what} '{value}' invalide — "
            "identifiant SQL simple attendu (lettres, chiffres, underscore ; ne commence "
            "pas par un chiffre)."
        )
