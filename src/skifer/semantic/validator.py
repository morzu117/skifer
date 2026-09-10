"""
SemanticValidator — valide un modèle YAML sémantique.

Appelé par SemanticBuilder lors de la boucle de validation,
et disponible en standalone pour valider des YAML existants.

Usage :
    validator = SemanticValidator()
    result = validator.validate(model_dict)
    if not result.ok:
        print(result.errors)
"""

from __future__ import annotations

from dataclasses import dataclass, field
import re

from .calendar import parse_calendar
from .domain import (
    ENTITY_ROLES,
    METRIC_ADDITIVITY,
    RELATIONSHIP_CARDINALITIES,
    RELATIONSHIP_JOIN_TYPES,
    parse_entities,
    parse_grain,
    parse_relationships,
)
from .output_projection import ProjectedSchema


# ---------------------------------------------------------------------------
# ValidationResult
# ---------------------------------------------------------------------------

@dataclass
class ValidationResult:
    """Résultat de la validation d'un modèle YAML sémantique."""
    ok: bool = True
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def add_error(self, msg: str) -> None:
        self.errors.append(msg)
        self.ok = False

    def add_warning(self, msg: str) -> None:
        self.warnings.append(msg)

    def __repr__(self) -> str:
        status = "OK" if self.ok else f"KO ({len(self.errors)} erreur(s))"
        return f"ValidationResult({status})"


# ---------------------------------------------------------------------------
# SemanticValidator
# ---------------------------------------------------------------------------

# Types d'agrégation supportés
_VALID_AGG_TYPES = {"sum", "count_distinct", "count", "avg", "min", "max"}

# Types de dimensions supportés
_VALID_DIM_TYPES = {"string", "date", "timestamp", "datetime", "integer", "float", "boolean"}

# QueryResolver interpole les noms de dimensions/métriques directement comme alias
# SQL ("<expr> AS <name>"). Un nom doit donc être un identifiant sûr : sans quoi
# un nom proposé par un LLM peut injecter du SQL arbitraire dans la requête émise.
_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class SemanticValidator:
    """
    Valide la structure et la cohérence d'un modèle YAML sémantique.

    Règles validées :
      - Champs obligatoires présents (name, table, dimensions, metrics).
      - Au moins une dimension et une métrique.
      - Chaque dimension a : name, sql, type (valide).
      - Chaque métrique a : name, sql, type (valide).
      - Pas de noms en doublon entre dimensions.
      - Pas de noms en doublon entre métriques.
      - Warnings si description manquante.
    """

    def validate(self, model: dict) -> ValidationResult:
        """
        Valide un dict modèle (issu d'un YAML parsé).

        Args:
            model: Dict représentant un modèle sémantique (un élément de models[]).

        Returns:
            ValidationResult avec ok=True si valide, ok=False + erreurs sinon.
        """
        result = ValidationResult()

        self._check_required_fields(model, result)
        if not result.ok:
            return result  # Stop early si champs de base manquants

        self._check_dimensions(model.get("dimensions", []), result)
        self._check_metrics(
            model.get("metrics", []), model.get("dimensions", []), result
        )
        self._check_domain(model, result)
        self._check_model_calendar_reference(model, result)
        self._check_descriptions(model, result)

        return result

    def validate_calendar(self, payload: dict) -> ValidationResult:
        """Validate one standalone versioned calendar definition."""
        result = ValidationResult()
        try:
            parse_calendar(payload)
        except ValueError as exc:
            result.add_error(str(exc))
        return result

    def validate_yaml(self, yaml_content: dict) -> ValidationResult:
        """
        Valide un dict issu du chargement d'un fichier YAML complet
        (avec la clé 'models' de haut niveau).

        Args:
            yaml_content: Dict avec clé 'models'.

        Returns:
            ValidationResult.
        """
        result = ValidationResult()

        if not isinstance(yaml_content, dict):
            result.add_error("Le contenu YAML doit être un dict")
            return result

        if "models" not in yaml_content:
            result.add_error("Clé 'models' manquante")
            return result

        models = yaml_content["models"]
        if not models:
            result.add_error("La liste 'models' est vide")
            return result

        available_models: dict[str, set[str]] = {}
        for model in models:
            if not isinstance(model, dict):
                continue
            model_key = str(model.get("key") or model.get("name") or "")
            if model_key:
                available_models[model_key] = {
                    entity.name for entity in parse_entities(model) if entity.name
                }

        for i, model in enumerate(models):
            sub_result = self.validate(model)
            for err in sub_result.errors:
                result.add_error(f"[model #{i}] {err}")
            for warn in sub_result.warnings:
                result.add_warning(f"[model #{i}] {warn}")
            if isinstance(model, dict):
                self._check_relationship_targets(model, available_models, i, result)

        return result

    def validate_against_projection(
        self,
        yaml_content: dict,
        projected: ProjectedSchema,
        *,
        contract_output: list[str] | None = None,
    ) -> ValidationResult:
        """Validate one semantic YAML plus its pipeline projection contract."""
        result = self.validate_yaml(yaml_content)
        projected_by_name = {field.name: field for field in projected.fields}
        projected_names = set(projected_by_name)

        for column in contract_output or []:
            if column not in projected_names:
                result.add_error(f"[contract.output] column '{column}' is not produced by the pipeline.")

        for i, model in enumerate(yaml_content.get("models", []) or []):
            for dimension in model.get("dimensions", []) or []:
                if not isinstance(dimension, dict):
                    continue
                source = dimension.get("sql")
                if source and source not in projected_names:
                    result.add_error(
                        f"[model #{i}] [semantic.dimensions] '{dimension.get('name', source)}' "
                        "is not a projected output."
                    )

            for metric in model.get("metrics", []) or []:
                if not isinstance(metric, dict):
                    continue
                source = metric.get("sql")
                metric_name = metric.get("name", source)
                projected_metric = projected_by_name.get(metric_name)
                allowed_sources = set(projected_metric.source_fields) if projected_metric is not None else set()
                if source and source != "*" and source not in projected_names and source not in allowed_sources:
                    result.add_error(
                        f"[model #{i}] [semantic.metrics] '{metric_name}' "
                        f"depends on unknown projected column '{source}'."
                    )

        return result

    # ------------------------------------------------------------------
    # Vérifications
    # ------------------------------------------------------------------

    def _check_required_fields(self, model: dict, result: ValidationResult) -> None:
        for field_name in ("name", "table"):
            if not model.get(field_name):
                result.add_error(f"Champ obligatoire manquant : '{field_name}'")

        if "dimensions" not in model:
            result.add_error("Clé 'dimensions' manquante")
        elif not model["dimensions"]:
            result.add_error("Au moins une dimension est requise")

        if "metrics" not in model:
            result.add_error("Clé 'metrics' manquante")
        elif not model["metrics"]:
            result.add_error("Au moins une métrique est requise")

    def _check_dimensions(self, dims: list, result: ValidationResult) -> None:
        seen_names: set[str] = set()

        for i, dim in enumerate(dims):
            if not isinstance(dim, dict):
                result.add_error(f"Dimension #{i} : doit être un dict")
                continue

            name = dim.get("name", "")
            if not name:
                result.add_error(f"Dimension #{i} : 'name' manquant")
            elif name in seen_names:
                result.add_error(f"Dimension : nom en doublon '{name}'")
            elif not _SAFE_IDENTIFIER.match(str(name)):
                result.add_error(
                    f"Dimension '{name}' : nom invalide — un nom de dimension est interpolé "
                    "comme alias SQL et doit être un identifiant simple "
                    "(lettres, chiffres, underscore ; ne commence pas par un chiffre)."
                )
            else:
                seen_names.add(name)

            if not dim.get("sql"):
                result.add_error(f"Dimension '{name or i}' : 'sql' manquant")

            dim_type = dim.get("type", "").lower()
            if dim_type and dim_type not in _VALID_DIM_TYPES:
                result.add_warning(
                    f"Dimension '{name or i}' : type '{dim_type}' non standard "
                    f"(attendu : {sorted(_VALID_DIM_TYPES)})"
                )

    def _check_metrics(
        self,
        metrics: list,
        dimensions: list,
        result: ValidationResult,
    ) -> None:
        seen_names: set[str] = set()
        dimension_names = {
            dimension.get("name")
            for dimension in dimensions
            if isinstance(dimension, dict) and dimension.get("name")
        }

        for i, met in enumerate(metrics):
            if not isinstance(met, dict):
                result.add_error(f"Métrique #{i} : doit être un dict")
                continue

            name = met.get("name", "")
            if not name:
                result.add_error(f"Métrique #{i} : 'name' manquant")
            elif name in seen_names:
                result.add_error(f"Métrique : nom en doublon '{name}'")
            elif not _SAFE_IDENTIFIER.match(str(name)):
                result.add_error(
                    f"Métrique '{name}' : nom invalide — un nom de métrique est interpolé "
                    "comme alias SQL et doit être un identifiant simple "
                    "(lettres, chiffres, underscore ; ne commence pas par un chiffre)."
                )
            else:
                seen_names.add(name)

            if not met.get("sql"):
                result.add_error(f"Métrique '{name or i}' : 'sql' manquant")

            agg_type = met.get("type", "").lower()
            if not agg_type:
                result.add_error(f"Métrique '{name or i}' : 'type' manquant")
            elif agg_type not in _VALID_AGG_TYPES:
                result.add_error(
                    f"Métrique '{name or i}' : type '{agg_type}' non supporté "
                    f"(supportés : {sorted(_VALID_AGG_TYPES)})"
                )

            additivity = met.get("additivity", "additive")
            restricted = met.get("non_additive_dimensions")
            if additivity not in METRIC_ADDITIVITY:
                result.add_error(
                    f"Métrique '{name or i}' : additivity '{additivity}' invalide "
                    f"(attendu : {sorted(METRIC_ADDITIVITY)})"
                )
            elif additivity == "additive" and "non_additive_dimensions" in met:
                result.add_error(
                    f"Métrique '{name or i}' : non_additive_dimensions est "
                    "incompatible avec additivity 'additive'"
                )
            elif additivity == "semi_additive":
                if not isinstance(restricted, list) or not restricted:
                    result.add_error(
                        f"Métrique '{name or i}' : semi_additive exige une liste "
                        "non vide non_additive_dimensions"
                    )
                else:
                    for dimension_name in restricted:
                        if dimension_name not in dimension_names:
                            result.add_error(
                                f"Métrique '{name or i}' : dimension non additive "
                                f"inconnue '{dimension_name}'"
                            )

            # Validation des filtres inline
            for f in met.get("filters", []) or []:
                if not isinstance(f, dict) or not f.get("sql"):
                    result.add_warning(
                        f"Métrique '{name or i}' : filtre inline sans 'sql' : {f}"
                    )

    @staticmethod
    def _check_model_calendar_reference(
        model: dict,
        result: ValidationResult,
    ) -> None:
        calendar_key = model.get("calendar")
        if calendar_key is None:
            return
        if not isinstance(calendar_key, str) or not _SAFE_IDENTIFIER.match(calendar_key):
            result.add_error(
                f"[semantic.calendar] key '{calendar_key}' is invalid: calendar "
                "keys must be safe SQL identifiers."
            )

    def _check_descriptions(self, model: dict, result: ValidationResult) -> None:
        if not model.get("description"):
            result.add_warning("Description du modèle manquante")

        for dim in model.get("dimensions", []):
            if isinstance(dim, dict) and not dim.get("description"):
                result.add_warning(
                    f"Description manquante pour la dimension '{dim.get('name', '?')}'"
                )

        for met in model.get("metrics", []):
            if isinstance(met, dict) and not met.get("description"):
                result.add_warning(
                    f"Description manquante pour la métrique '{met.get('name', '?')}'"
                )

    def _check_domain(self, model: dict, result: ValidationResult) -> None:
        model_key = str(model.get("key") or model.get("name") or "")

        # The parse layer skips anything that is not a mapping, so a malformed
        # block would otherwise validate green with zero entities. That is a
        # permissive fallback the programme forbids — and an easy mistake to
        # make, since the *catalog* stores `entities` as a list of plain names
        # while a model declares them as mappings.
        if not self._check_domain_block_shape(model, "entities", result):
            return
        if not self._check_domain_block_shape(model, "relationships", result):
            return

        entities = parse_entities(model)
        relationships = parse_relationships(model)
        entity_names = {entity.name for entity in entities if entity.name}

        self._check_grain(parse_grain(model), entity_names, result)
        self._check_entities(entities, result)
        self._check_relationships(model_key, relationships, entity_names, result)

    @staticmethod
    def _check_domain_block_shape(
        model: dict,
        block: str,
        result: ValidationResult,
    ) -> bool:
        """Reject a malformed ``entities``/``relationships`` block explicitly."""
        raw = model.get(block)
        if raw is None:
            return True
        if not isinstance(raw, list):
            result.add_error(
                f"[semantic.{block}] must be a list of mappings, got "
                f"{type(raw).__name__}. Declare each entry as a mapping."
            )
            return False

        ok = True
        for index, item in enumerate(raw):
            if not isinstance(item, dict):
                result.add_error(
                    f"[semantic.{block}] entry #{index} must be a mapping, got "
                    f"{type(item).__name__} ({item!r}). A bare name is the catalog "
                    f"summary shape; a model must declare the full entry."
                )
                ok = False
        return ok

    def _check_grain(
        self,
        grain: tuple[str, ...],
        entity_names: set[str],
        result: ValidationResult,
    ) -> None:
        seen: set[str] = set()

        for name in grain:
            if not _SAFE_IDENTIFIER.match(name):
                result.add_error(
                    f"[semantic.grain] '{name}' is invalid: grain entity names must be safe SQL identifiers."
                )
                continue
            if name in seen:
                result.add_error(f"[semantic.grain] duplicate entity '{name}'.")
                continue
            seen.add(name)
            if entity_names and name not in entity_names:
                result.add_error(
                    f"[semantic.grain] entity '{name}' is not declared in entities."
                )

    def _check_entities(self, entities: tuple, result: ValidationResult) -> None:
        seen_names: set[str] = set()

        for entity in entities:
            if not entity.name:
                result.add_error("[semantic.entities] entity name is required.")
            elif entity.name in seen_names:
                result.add_error(
                    f"[semantic.entities] duplicate entity name '{entity.name}'."
                )
            elif not _SAFE_IDENTIFIER.match(entity.name):
                result.add_error(
                    f"[semantic.entities] entity '{entity.name}' is invalid: entity names must be safe SQL identifiers."
                )
            else:
                seen_names.add(entity.name)

            if entity.role not in ENTITY_ROLES:
                result.add_error(
                    f"[semantic.entities] entity '{entity.name or '?'}' has invalid type "
                    f"'{entity.role}' (expected one of {sorted(ENTITY_ROLES)})."
                )

            if not entity.key_columns:
                result.add_error(
                    f"[semantic.entities] entity '{entity.name or '?'}' must declare a key column or ordered key column list."
                )
                continue

            seen_columns: set[str] = set()
            for column in entity.key_columns:
                if not _SAFE_IDENTIFIER.match(column):
                    result.add_error(
                        f"[semantic.entities] entity '{entity.name or '?'}' key column '{column}' is invalid: key columns must be safe SQL identifiers."
                    )
                if column in seen_columns:
                    result.add_error(
                        f"[semantic.entities] entity '{entity.name or '?'}' key repeats column '{column}'; composite key order is significant and duplicates are not allowed."
                    )
                seen_columns.add(column)

    def _check_relationships(
        self,
        model_key: str,
        relationships: tuple,
        entity_names: set[str],
        result: ValidationResult,
    ) -> None:
        seen_names: set[str] = set()

        for relationship in relationships:
            if not relationship.name:
                result.add_error("[semantic.relationships] relationship name is required.")
            elif relationship.name in seen_names:
                result.add_error(
                    f"[semantic.relationships] duplicate relationship name '{relationship.name}'."
                )
            elif not _SAFE_IDENTIFIER.match(relationship.name):
                result.add_error(
                    f"[semantic.relationships] relationship '{relationship.name}' is invalid: relationship names must be safe SQL identifiers."
                )
            else:
                seen_names.add(relationship.name)

            from_entity = relationship.from_entity.entity_name
            if not from_entity:
                result.add_error(
                    f"[semantic.relationships] relationship '{relationship.name or '?'}' must declare from_entity."
                )
            elif not _SAFE_IDENTIFIER.match(from_entity):
                result.add_error(
                    f"[semantic.relationships] relationship '{relationship.name or '?'}' from_entity '{from_entity}' is invalid: entity names must be safe SQL identifiers."
                )
            elif from_entity not in entity_names:
                result.add_error(
                    f"[semantic.relationships] relationship '{relationship.name or '?'}' references unknown local entity '{from_entity}'."
                )

            to_model = relationship.to_entity.model_key
            if not to_model:
                result.add_error(
                    f"[semantic.relationships] relationship '{relationship.name or '?'}' must declare to_model."
                )
            elif not _SAFE_IDENTIFIER.match(to_model):
                result.add_error(
                    f"[semantic.relationships] relationship '{relationship.name or '?'}' to_model '{to_model}' is invalid: model keys must be safe SQL identifiers."
                )

            to_entity = relationship.to_entity.entity_name
            if not to_entity:
                result.add_error(
                    f"[semantic.relationships] relationship '{relationship.name or '?'}' must declare to_entity."
                )
            elif not _SAFE_IDENTIFIER.match(to_entity):
                result.add_error(
                    f"[semantic.relationships] relationship '{relationship.name or '?'}' to_entity '{to_entity}' is invalid: entity names must be safe SQL identifiers."
                )

            if (
                relationship.from_entity.model_key == to_model
                and from_entity
                and from_entity == to_entity
            ):
                result.add_error(
                    f"[semantic.relationships] relationship '{relationship.name or '?'}' is a self relation on '{from_entity}'."
                )

            if relationship.cardinality not in RELATIONSHIP_CARDINALITIES:
                result.add_error(
                    f"[semantic.relationships] relationship '{relationship.name or '?'}' has invalid cardinality "
                    f"'{relationship.cardinality}' (expected one of {sorted(RELATIONSHIP_CARDINALITIES)})."
                )
            elif relationship.cardinality == "unknown":
                result.add_error(
                    "[semantic.relationships] relationship "
                    f"'{relationship.name or '?'}': Relationship cardinality is unknown; "
                    "declare or certify uniqueness before querying."
                )

            if relationship.join_type not in RELATIONSHIP_JOIN_TYPES:
                result.add_error(
                    f"[semantic.relationships] relationship '{relationship.name or '?'}' has invalid join_type "
                    f"'{relationship.join_type}' (v1 supports only {sorted(RELATIONSHIP_JOIN_TYPES)})."
                )

    def _check_relationship_targets(
        self,
        model: dict,
        available_models: dict[str, set[str]],
        model_index: int,
        result: ValidationResult,
    ) -> None:
        for relationship in parse_relationships(model):
            to_model = relationship.to_entity.model_key
            to_entity = relationship.to_entity.entity_name
            if not to_model or to_model not in available_models:
                continue
            if to_entity and to_entity not in available_models[to_model]:
                result.add_error(
                    f"[model #{model_index}] [semantic.relationships] relationship "
                    f"'{relationship.name or '?'}' references unknown entity '{to_entity}' "
                    f"in model '{to_model}'."
                )
