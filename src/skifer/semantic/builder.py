"""
SemanticBuilder — génère des modèles YAML sémantiques et maintient semantic_catalog.yaml.

Sources supportées :
  - Notebooks Jupyter (.ipynb) via NotebookExtractor
  - Business Glossary (JSON/YAML/TXT/PDF/PPTX) via GlossaryReader

Le builder génère le YAML et met immédiatement à jour le catalogue.
3 boucles de validation max — si KO, le contexte est écrit dans .errors/.

Chaque modèle est identifié par une clé unique (model_key) qui sert à la fois
de nom de fichier et de clé dans le catalogue.
Structure de fichiers : semantic_models/<model_key>.yaml (plate, sans sous-dossiers).

Usage :
    from skifer.semantic.builder import SemanticBuilder
    from skifer.semantic.llm_provider import get_llm_provider

    builder = SemanticBuilder(
        llm_provider=get_llm_provider(),
        output_dir="semantic_models",
    )
    builder.build(
        model_key="kpi_orders_erp",
        table="gold.fact_orders",
        layer="gold",
        source_notebook="notebooks/kpi_orders.ipynb",  # optionnel
        glossary_path="glossaries/orders.json",          # optionnel
        description="Métriques commandes ERP — source SAP",
        tags=["orders", "revenue", "erp"],
    )
"""

from __future__ import annotations

from copy import deepcopy
import datetime
import os
import re

import yaml

from skifer.core.ir import ParsedSchema

from .llm_provider import LLMProvider
from .output_projection import ProjectedSchema
from .persistence import build_catalog_entry, write_yaml_atomic
from .semantic import SemanticEngine
from .validator import SemanticValidator


# ---------------------------------------------------------------------------
# Prompt système
# ---------------------------------------------------------------------------

_BUILDER_SYSTEM = """Tu es un expert en modélisation sémantique BI (couche Gold Lakehouse).

Ton rôle : générer un modèle YAML sémantique complet à partir du contexte fourni.

Règles STRICTES :
- Tu DOIS répondre UNIQUEMENT avec un objet YAML valide. Aucun texte autour.
- Ne génère pas de blocs ```yaml``` — commence directement avec "models:".
- Toutes les valeurs de type texte (description, base_filter, sql) qui contiennent ":", "#", "[", "]" ou des guillemets DOIVENT être encadrées de guillemets doubles. Ex: description: "CA : chiffre d'affaires"
- Chaque métrique doit avoir un champ "sql" (expression SQL), "type" (sum/count_distinct/count/avg/min/max), "name", "description".
- Chaque dimension doit avoir un champ "sql", "type" (string/date/timestamp/integer/float), "name", "description".
- Les filtres inline de métriques sont dans "filters: [{sql: 'condition'}]".
- Le "base_filter" s'applique à toutes les métriques du modèle.
- Génère uniquement des noms et expressions SQL cohérents avec le contexte fourni.
- N'invente pas de colonnes qui n'existent pas dans le contexte.

Structure YAML attendue :
models:
  - name: <model_name>
    key: <model_name>.<split_value>
    description: <description>
    layer: <gold|silver|bronze>
    table: <layer>.<table_name>
    base_filter: "<condition SQL ou null>"
    tags: [<tag1>, <tag2>]
    dimensions:
      - name: <dim_name>
        sql: <expression_sql>
        type: <string|date|timestamp|integer|float>
        description: <description>
    metrics:
      - name: <metric_name>
        sql: <expression_sql>
        type: <sum|count_distinct|count|avg|min|max>
        description: <description>
        filters:
          - sql: "<condition SQL>"
"""

_ENRICHMENT_SYSTEM = """Tu enrichis un modèle sémantique déjà structuré et géré par le pipeline.

Règles STRICTES :
- Réponds UNIQUEMENT avec un objet YAML valide. Aucun texte autour.
- Ne génère pas de blocs ```yaml```.
- Tu peux proposer des descriptions et synonymes au niveau modèle/dimension/métrique.
- Tu peux proposer de nouvelles métriques UNIQUEMENT si elles dépendent exclusivement des sorties projetées fournies.
- N'ajoute aucune dimension.
- Ne renomme, ne retype et ne redéfinis jamais une dimension ou métrique existante.
- Ne modifie jamais table, layer, entity, default_time_dimension, metadata ou un sql géré.
- Pour les métriques proposées, le champ sql doit être soit "*", soit le nom exact d'une sortie projetée.

Structure YAML attendue :
models:
  - description: <description optionnelle>
    synonyms: [<synonyme1>, <synonyme2>]
    dimensions:
      - name: <dimension_existante>
        description: <description optionnelle>
        synonyms: [<synonyme1>, <synonyme2>]
    metrics:
      - name: <metric_existante_ou_nouvelle>
        description: <description optionnelle>
        synonyms: [<synonyme1>, <synonyme2>]
        sql: <nom_sortie_projetée_ou_* pour nouvelle métrique seulement>
        type: <sum|count_distinct|count|avg|min|max pour nouvelle métrique seulement>
"""

_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_VALID_METRIC_TYPES = {"sum", "count_distinct", "count", "avg", "min", "max"}
_MODEL_EDITABLE_KEYS = {"description", "synonyms"}
_FIELD_EDITABLE_KEYS = {"description", "synonyms"}
_MANAGED_FIELD_KEYS = {"name", "sql", "type", "entity", "filters", "needs_curation"}
_ALLOWED_NEW_METRIC_KEYS = {"name", "sql", "type", "description", "synonyms"}


class SemanticBuilder:
    """
    Génère des modèles YAML sémantiques via LLM et maintient semantic_catalog.yaml.

    Chaque appel à build() :
      1. Collecte le contexte (notebook, glossaire, docstrings).
      2. Appelle le LLM pour générer le YAML.
      3. Valide le YAML (3 boucles max).
      4. Écrit le fichier YAML dans output_dir/<model_key>.yaml (structure plate).
      5. Met à jour semantic_catalog.yaml.
    """

    MAX_VALIDATION_LOOPS = 3

    def __init__(
        self,
        llm_provider: LLMProvider,
        output_dir: str = "semantic_models",
        glossary_path: str | None = None,
    ):
        """
        Args:
            llm_provider:  Instance LLMProvider.
            output_dir:    Répertoire de sortie des YAML (relatif ou absolu).
            glossary_path: Chemin du Business Glossary par défaut (optionnel).
        """
        self.llm = llm_provider
        self.output_dir = os.path.abspath(output_dir) if not os.path.isabs(output_dir) else output_dir
        self._default_glossary = glossary_path

    def build(
        self,
        model_key: str,
        table: str,
        layer: str,
        description: str = "",
        tags: list[str] | None = None,
        source_notebook: str | None = None,
        glossary_path: str | None = None,
        extra_context: str = "",
        base_filter: str | None = None,
    ) -> str:
        """
        Génère un modèle YAML et met à jour le catalogue.

        Args:
            model_key:        Identifiant unique du modèle (ex: "kpi_orders_erp").
                              Utilisé comme nom de fichier et clé catalogue.
            table:            Table source (ex: "gold.fact_orders").
            layer:            Layer (ex: "gold").
            description:      Description lisible du modèle.
            tags:             Tags manuels (complétés par les tags auto).
            source_notebook:  Chemin vers un notebook .ipynb à analyser.
            glossary_path:    Chemin vers un glossaire (prioritaire sur le défaut).
            extra_context:    Contexte métier supplémentaire en texte libre.
            base_filter:      Filtre SQL appliqué à toutes les métriques.

        Returns:
            Chemin absolu du fichier YAML généré.

        Raises:
            RuntimeError: Si la validation échoue après MAX_VALIDATION_LOOPS tentatives.
        """
        print(f"🔨 [SemanticBuilder] Génération de '{model_key}'...")

        context = self._build_context(
            model_key=model_key,
            table=table,
            layer=layer,
            description=description,
            base_filter=base_filter,
            source_notebook=source_notebook,
            glossary_path=glossary_path or self._default_glossary,
            extra_context=extra_context,
        )

        # Génération + validation (3 boucles max)
        yaml_content = None
        last_error = None

        for attempt in range(1, self.MAX_VALIDATION_LOOPS + 1):
            print(f"  Tentative {attempt}/{self.MAX_VALIDATION_LOOPS}...")

            raw = self.llm.complete(
                system_prompt=_BUILDER_SYSTEM,
                user_message=context + (
                    f"\n\nCorrection demandée : {last_error}" if last_error else ""
                ),
                temperature=0.1,
            )

            parsed, error = self._parse_and_validate_yaml(raw, model_key)
            if parsed is not None:
                yaml_content = parsed
                last_error = None
                break
            last_error = error
            print(f"  ⚠️  Validation KO : {error}")

        if yaml_content is None:
            return self._write_error(model_key, context, last_error)

        yaml_path = self._write_yaml(model_key, yaml_content)

        catalog_entry = self._build_catalog_entry(
            model_key=model_key,
            yaml_content=yaml_content,
            tags=tags or [],
        )
        SemanticEngine.update_catalog(self.output_dir, catalog_entry)

        print(f"✅ [SemanticBuilder] '{model_key}' généré → {yaml_path}")
        return yaml_path

    def build_from_projection(
        self,
        *,
        projected: ProjectedSchema,
        schema: ParsedSchema,
        managed_draft: dict,
        extra_context: str = "",
    ) -> str:
        """
        Enrichit un draft géré avec des suggestions LLM strictement filtrées.

        Le draft/projection restent la source de vérité structurelle. Le LLM ne
        peut enrichir que les descriptions/synonymes et proposer de nouvelles
        métriques bornées aux sorties projetées.
        """
        model_key = self._model_key(managed_draft)
        print(f"🔨 [SemanticBuilder] Enrichissement contraint de '{model_key}'...")

        validator = SemanticValidator()
        managed_validation = validator.validate_against_projection(
            managed_draft,
            projected,
            contract_output=[field.name for field in schema.contract_output],
        )
        if not managed_validation.ok:
            raise ValueError(
                f"[semantic.draft] Managed draft '{model_key}' is invalid against its projection: "
                + "; ".join(managed_validation.errors)
            )

        context = self._build_enrichment_context(
            managed_draft=managed_draft,
            projected=projected,
            schema=schema,
            extra_context=extra_context,
        )

        yaml_content = None
        last_error = None

        for attempt in range(1, self.MAX_VALIDATION_LOOPS + 1):
            print(f"  Tentative {attempt}/{self.MAX_VALIDATION_LOOPS}...")

            raw = self.llm.complete(
                system_prompt=_ENRICHMENT_SYSTEM,
                user_message=context + (
                    f"\n\nCorrection demandée : {last_error}" if last_error else ""
                ),
                temperature=0.1,
            )

            parsed, error = self._parse_yaml_object(raw)
            if parsed is None:
                last_error = error
                print(f"  ⚠️  Validation KO : {error}")
                continue

            candidate = self._merge_llm_enrichment(
                managed_draft=managed_draft,
                llm_payload=parsed,
                projected=projected,
            )
            validation = validator.validate_against_projection(
                candidate,
                projected,
                contract_output=[field.name for field in schema.contract_output],
            )
            if validation.ok:
                yaml_content = candidate
                last_error = None
                break

            last_error = "; ".join(validation.errors)
            print(f"  ⚠️  Validation KO : {last_error}")

        if yaml_content is None:
            return self._write_error(model_key, context, last_error)

        yaml_path = self._write_yaml(model_key, yaml_content)
        SemanticEngine.update_catalog(
            self.output_dir,
            self._build_catalog_entry(model_key=model_key, yaml_content=yaml_content, tags=[]),
        )
        print(f"✅ [SemanticBuilder] '{model_key}' enrichi → {yaml_path}")
        return yaml_path

    # ------------------------------------------------------------------
    # Contexte LLM
    # ------------------------------------------------------------------

    def _build_context(
        self,
        model_key: str,
        table: str,
        layer: str,
        description: str,
        base_filter: str | None,
        source_notebook: str | None,
        glossary_path: str | None,
        extra_context: str,
    ) -> str:
        parts = [
            f"Modèle à générer : {model_key}",
            f"Table source     : {table}",
            f"Layer            : {layer}",
            f"Description      : {description or '(non précisée)'}",
        ]

        if base_filter:
            parts.append(f"Base filter      : {base_filter}")

        if source_notebook:
            notebook_ctx = self._extract_notebook_context(source_notebook)
            if notebook_ctx:
                parts.append(f"\n--- Contexte notebook ---\n{notebook_ctx}")

        if glossary_path:
            glossary_ctx = self._extract_glossary_context(glossary_path)
            if glossary_ctx:
                parts.append(f"\n--- Business Glossary ---\n{glossary_ctx}")

        if extra_context:
            parts.append(f"\n--- Contexte supplémentaire ---\n{extra_context}")

        return "\n".join(parts)

    def _build_enrichment_context(
        self,
        *,
        managed_draft: dict,
        projected: ProjectedSchema,
        schema: ParsedSchema,
        extra_context: str,
    ) -> str:
        model = managed_draft["models"][0]
        projected_fields = [
            {
                "name": field.name,
                "physical_type": field.physical_type,
                "logical_type": field.logical_type,
                "source_fields": list(field.source_fields),
                "transformations": list(field.transformations),
                "inference_status": field.inference_status,
            }
            for field in projected.fields
        ]
        parts = [
            f"Modèle géré        : {model.get('key', model.get('name', '<unknown>'))}",
            f"Table gérée        : {model.get('table', '')}",
            "Draft géré (source de vérité structurelle) :",
            yaml.safe_dump(managed_draft, sort_keys=False, allow_unicode=True, default_flow_style=False),
            "Sorties projetées autorisées :",
            yaml.safe_dump(
                {
                    "projected_fields": projected_fields,
                    "contract_output": [field.name for field in schema.contract_output],
                    "grain": list(projected.grain),
                },
                sort_keys=False,
                allow_unicode=True,
                default_flow_style=False,
            ),
        ]
        if extra_context:
            parts.append(f"Contexte métier supplémentaire :\n{extra_context}")
        return "\n\n".join(parts)

    def _extract_notebook_context(self, notebook_path: str) -> str:
        """Extrait le contexte d'un notebook via NotebookExtractor."""
        try:
            from .extractor import NotebookExtractor
            extractor = NotebookExtractor()
            return extractor.extract(notebook_path)
        except Exception as e:
            print(f"  ⚠️  Extraction notebook KO : {e}")
            return ""

    def _extract_glossary_context(self, glossary_path: str) -> str:
        """Extrait le contexte d'un glossaire via GlossaryReader."""
        try:
            from .glossary import GlossaryReader
            reader = GlossaryReader()
            return reader.read(glossary_path)
        except Exception as e:
            print(f"  ⚠️  Lecture glossaire KO : {e}")
            return ""

    # ------------------------------------------------------------------
    # Parse + validation YAML
    # ------------------------------------------------------------------

    def _parse_and_validate_yaml(
        self, raw: str, model_key: str
    ) -> tuple[dict | None, str | None]:
        """
        Parse et valide le YAML retourné par le LLM via SemanticValidator.

        Returns:
            (parsed_dict, None) si OK
            (None, error_message) si KO
        """
        content, error = self._parse_yaml_object(raw)
        if content is None:
            return None, error

        if not isinstance(content, dict):
            return None, "Le YAML doit être un dict avec une clé 'models'"

        result = SemanticValidator().validate_yaml(content)
        if not result.ok:
            return None, "; ".join(result.errors)

        return content, None

    @staticmethod
    def _parse_yaml_object(raw: str) -> tuple[dict | None, str | None]:
        """Parse a raw LLM YAML response after stripping optional code fences."""
        cleaned = raw.strip()
        if cleaned.startswith("```"):
            cleaned = "\n".join(cleaned.split("\n")[1:])
        if cleaned.endswith("```"):
            cleaned = "\n".join(cleaned.split("\n")[:-1])
        cleaned = cleaned.strip()

        try:
            content = yaml.safe_load(cleaned)
        except yaml.YAMLError as e:
            return None, f"YAML invalide : {e}"

        if not isinstance(content, dict):
            return None, "Le YAML doit être un dict avec une clé 'models'"
        return content, None

    def _merge_llm_enrichment(
        self,
        *,
        managed_draft: dict,
        llm_payload: dict,
        projected: ProjectedSchema,
    ) -> dict:
        """Return a curated payload by whitelisting safe LLM enrichments only."""
        merged = deepcopy(managed_draft)
        managed_model = self._single_model(managed_draft)
        merged_model = self._single_model(merged)
        llm_model = self._single_model(llm_payload)

        if llm_model is None:
            return merged

        for key in _MODEL_EDITABLE_KEYS:
            if key in llm_model:
                normalized = self._normalized_editable_value(key, llm_model[key])
                if normalized is not None:
                    merged_model[key] = normalized

        self._merge_existing_collection(
            collection_name="dimensions",
            managed_model=managed_model,
            merged_model=merged_model,
            llm_model=llm_model,
        )
        self._merge_metrics(
            managed_model=managed_model,
            merged_model=merged_model,
            llm_model=llm_model,
            projected=projected,
        )

        return merged

    def _merge_existing_collection(
        self,
        *,
        collection_name: str,
        managed_model: dict,
        merged_model: dict,
        llm_model: dict,
    ) -> None:
        managed_items = {
            item["name"]: item for item in managed_model.get(collection_name, []) if isinstance(item, dict)
        }
        merged_items = {
            item["name"]: item for item in merged_model.get(collection_name, []) if isinstance(item, dict)
        }

        for candidate in llm_model.get(collection_name, []) or []:
            if not isinstance(candidate, dict):
                continue
            name = candidate.get("name")
            if not name or name not in managed_items:
                continue
            managed_item = managed_items[name]
            if self._changes_managed_field(managed_item, candidate):
                continue
            target_item = merged_items[name]
            for key in _FIELD_EDITABLE_KEYS:
                if key in candidate:
                    normalized = self._normalized_editable_value(key, candidate[key])
                    if normalized is not None:
                        target_item[key] = normalized

    def _merge_metrics(
        self,
        *,
        managed_model: dict,
        merged_model: dict,
        llm_model: dict,
        projected: ProjectedSchema,
    ) -> None:
        managed_metrics = {
            item["name"]: item for item in managed_model.get("metrics", []) if isinstance(item, dict)
        }
        merged_metrics = merged_model.setdefault("metrics", [])
        existing_names = {item["name"] for item in merged_metrics if isinstance(item, dict) and item.get("name")}
        projected_names = {field.name for field in projected.fields}

        for candidate in llm_model.get("metrics", []) or []:
            if not isinstance(candidate, dict):
                continue
            name = candidate.get("name")
            if not name:
                continue
            if name in managed_metrics:
                managed_item = managed_metrics[name]
                if self._changes_managed_field(managed_item, candidate):
                    continue
                merged_item = next(
                    item for item in merged_metrics if isinstance(item, dict) and item.get("name") == name
                )
                for key in _FIELD_EDITABLE_KEYS:
                    if key in candidate:
                        normalized = self._normalized_editable_value(key, candidate[key])
                        if normalized is not None:
                            merged_item[key] = normalized
                continue

            new_metric = self._sanitize_new_metric(candidate, projected_names)
            if new_metric is None or new_metric["name"] in existing_names:
                continue
            merged_metrics.append(new_metric)
            existing_names.add(new_metric["name"])

    @staticmethod
    def _changes_managed_field(managed_item: dict, candidate: dict) -> bool:
        for key in _MANAGED_FIELD_KEYS:
            if key in candidate and candidate.get(key) != managed_item.get(key):
                return True
        unknown_keys = set(candidate) - (_FIELD_EDITABLE_KEYS | _MANAGED_FIELD_KEYS)
        return bool(unknown_keys)

    @staticmethod
    def _normalized_editable_value(key: str, value):
        if key == "description":
            if isinstance(value, str):
                return value
            return None
        if key == "synonyms":
            if not isinstance(value, list):
                return None
            normalized = []
            for item in value:
                if not isinstance(item, str):
                    return None
                if item not in normalized:
                    normalized.append(item)
            return normalized
        return None

    def _sanitize_new_metric(
        self,
        candidate: dict,
        projected_names: set[str],
    ) -> dict | None:
        if set(candidate) - _ALLOWED_NEW_METRIC_KEYS:
            return None
        name = candidate.get("name")
        sql = candidate.get("sql")
        metric_type = candidate.get("type")
        if not isinstance(name, str) or not isinstance(sql, str) or not isinstance(metric_type, str):
            return None
        # The name is interpolated straight into the emitted SQL as an alias
        # ("<expr> AS <name>"), so an LLM-authored name must be a plain
        # identifier or it becomes an injection vector.
        if not _SAFE_IDENTIFIER.match(name):
            return None
        if metric_type.lower() not in _VALID_METRIC_TYPES:
            return None
        if sql == "*" and metric_type.lower() != "count":
            return None
        if sql != "*" and sql not in projected_names:
            return None

        metric = {
            "name": name,
            "sql": sql,
            "type": metric_type,
        }
        for key in _FIELD_EDITABLE_KEYS:
            if key in candidate:
                normalized = self._normalized_editable_value(key, candidate[key])
                if normalized is not None:
                    metric[key] = normalized
        return metric

    @staticmethod
    def _single_model(payload: dict) -> dict | None:
        models = payload.get("models") if isinstance(payload, dict) else None
        if not isinstance(models, list) or not models:
            return None
        first = models[0]
        return first if isinstance(first, dict) else None

    def _model_key(self, payload: dict) -> str:
        model = self._single_model(payload)
        if model is None:
            raise ValueError("Le YAML doit contenir au moins un modèle sémantique.")
        return model.get("key") or model.get("name") or "<unknown>"

    # ------------------------------------------------------------------
    # Écriture YAML
    # ------------------------------------------------------------------

    def _write_yaml(self, model_key: str, content: dict) -> str:
        """Écrit le YAML dans output_dir/<model_key>.yaml (structure plate)."""
        return write_yaml_atomic(
            os.path.join(self.output_dir, f"{model_key}.yaml"), content
        )

    def _write_error(self, model_key: str, context: str, error: str | None) -> str:
        """Écrit le contexte en erreur dans .errors/ et lève RuntimeError."""
        errors_dir = os.path.join(self.output_dir, ".errors")
        os.makedirs(errors_dir, exist_ok=True)

        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        error_path = os.path.join(errors_dir, f"{model_key}_{ts}.txt")

        with open(error_path, "w", encoding="utf-8") as f:
            f.write(f"ERREUR : {error}\n\nCONTEXTE :\n{context}")

        raise RuntimeError(
            f"❌ [SemanticBuilder] Génération KO après {self.MAX_VALIDATION_LOOPS} "
            f"tentatives pour '{model_key}'. "
            f"Détails → {error_path}"
        )

    # ------------------------------------------------------------------
    # Entrée catalogue
    # ------------------------------------------------------------------

    @staticmethod
    def _build_catalog_entry(model_key: str, yaml_content: dict, tags: list[str]) -> dict:
        """Construit l'entrée catalogue légère depuis le YAML généré."""
        return build_catalog_entry(yaml_content, model_key=model_key, extra_tags=tags)
