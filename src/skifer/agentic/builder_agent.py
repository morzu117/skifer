"""
BuilderAgent — génération de YAML de pipeline ETL depuis le langage naturel ou un wizard.

Deux modes :
  - wizard()  : assistant pas-à-pas en ligne de commande (sans LLM)
  - ask()     : description en langage naturel → JSON structuré → YAML validé (LLM requis)

Les deux modes partagent la même couche de validation (CatalogInspector)
et la même logique de sauvegarde.
"""
from __future__ import annotations

import builtins
from collections import deque
import json
import os
import re
from typing import TYPE_CHECKING, Callable

import yaml

from skifer.agentic.models import BuilderResponse, OrchestratorExportResult
from skifer.agentic.orchestrator import OrchestratorExporter
from skifer.core.catalog_inspector import CatalogError, CatalogInspector, _top_suggestions

if TYPE_CHECKING:
    from skifer.core.spark_backend import SparkBackend
    from skifer.semantic.llm_provider import LLMProvider


# ---------------------------------------------------------------------------
# LLM system prompt — Step A
# ---------------------------------------------------------------------------

_STEP_A_SYSTEM = """\
Tu es un expert ETL Lakehouse. Extrais la structure du pipeline décrit par l'utilisateur.
Réponds UNIQUEMENT avec un objet JSON valide, sans texte autour.

Structure JSON attendue :
{
  "tables": [{"fqn": "...", "alias": "...", "filters": [...]}],
  "joins": [{"from": ["alias", "col"], "to": ["alias", "col"], "type": "left"}],
  "business_rules": [],
  "select_final": [{"source": "...", "target": "...", "ops": []}],
  "keep_all_columns": false,
  "dev_limit": 10000,
  "output_name": "schemas/silver/pipeline.yaml"
}

Règles :
- "tables[].filters" : liste de chaînes "colonne:opérateur[:valeur]"
- "select_final" : liste d'objets {source, target, ops:[]} ou null pour keep_all_columns
- Dans select_final, "source" est un nom de colonne nu SANS préfixe d'alias (ex: "order_id", PAS "o.order_id")
- Si keep_all_columns est true, select_final doit être null ou []
- Réponds UNIQUEMENT avec le JSON — aucun texte avant ou après.
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _extract_json(raw: str) -> dict:
    """Extract and parse the first JSON object found in a LLM response string."""
    # Strip markdown code blocks if present
    raw = re.sub(r"```(?:json)?", "", raw).strip()
    # Find the outermost { } block
    start = raw.find("{")
    end = raw.rfind("}") + 1
    if start == -1 or end == 0:
        raise ValueError(f"Aucun objet JSON trouvé dans la réponse LLM:\n{raw}")
    return json.loads(raw[start:end])


def _schema_dict_to_yaml(schema: dict) -> str:
    """Convert a schema dict to a clean YAML string."""
    return yaml.dump(schema, default_flow_style=False, allow_unicode=True, sort_keys=False)


def _build_schema_dict(etl_struct: dict) -> dict:
    """Convert the structured ETL dict (from LLM) into a Skifer schema dict."""
    schema: dict = {}

    # Tables
    tables = []
    for t in etl_struct.get("tables", []):
        entry: dict = {"name": t["fqn"]}
        if t.get("alias"):
            entry["alias"] = t["alias"]
        filters = t.get("filters") or []
        if filters:
            entry["filter"] = filters
        tables.append(entry)
    if tables:
        schema["tables"] = tables

    # Joins
    joins_raw = etl_struct.get("joins") or []
    joins = []
    for j in joins_raw:
        joins.append({
            "table_from": j["from"],
            "table_to": j["to"],
            "type": j.get("type", "left"),
        })
    if joins:
        schema["join"] = joins

    # Business rules
    rules = etl_struct.get("business_rules") or []
    if rules:
        schema["business_rules"] = rules

    # Select final
    select_raw = etl_struct.get("select_final") or []
    if select_raw:
        select = []
        for item in select_raw:
            if isinstance(item, dict):
                ops = item.get("ops") or []
                # Strip alias prefix: "o.order_id" → "order_id"
                src = item["source"]
                if "." in src:
                    src = src.split(".", 1)[1]
                select.append([src, item.get("target", src), ops])
            else:
                select.append(item)
        schema["select_final"] = select
    elif etl_struct.get("keep_all_columns"):
        schema["keep_all_columns"] = True

    # Dev limit
    dev_limit = etl_struct.get("dev_limit")
    if dev_limit:
        schema["dev_limit"] = dev_limit

    return schema


def _fqn_from_filter_string(filter_str: str) -> str | None:
    """Extract the column name from a filter string 'col:op[:val]'."""
    parts = filter_str.split(":", 1)
    return parts[0].strip() if parts else None


# ---------------------------------------------------------------------------
# BuilderAgent
# ---------------------------------------------------------------------------

class BuilderAgent:
    """
    Génère des YAML de pipeline ETL (SkiferEngine-ready) en deux modes :
    - ``wizard()``  : assistant interactif pas-à-pas (sans LLM)
    - ``ask()``     : description en langage naturel → YAML validé (LLM requis)

    Args:
        backend:      SparkBackend (accès catalogue).
        catalog:      Nom du catalog par défaut (None en mode local).
        llm_provider: Provider LLM (requis pour le mode ask()).
    """

    def __init__(
        self,
        backend: "SparkBackend",
        catalog: str | None = None,
        llm_provider: "LLMProvider | None" = None,
        input_provider: Callable[[str], str] | None = None,
    ):
        self._backend = backend
        self._catalog = catalog
        self._inspector = CatalogInspector(backend, catalog)
        self._llm = llm_provider
        self._input = input_provider or builtins.input

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def wizard(
        self,
        output_dir: str = "schemas",
        answers: list[str] | dict[str, list[str]] | None = None,
    ) -> str:
        """
        Lance le wizard interactif pas-à-pas.

        Chaque valeur saisie est validée par le CatalogInspector avant de passer
        à l'étape suivante. Retourne le chemin du fichier sauvegardé.

        ``answers`` may be an ordered response queue, consumed in the exact
        prompt order (tables, filters, joins, rules, select, options, save), or
        a mapping from exact prompt text to a response queue. When omitted, the
        provider configured at construction is used (``builtins.input`` by
        default).
        """
        previous_input = self._input
        if answers is not None:
            self._input = self._answers_provider(answers)
        try:
            return self._run_wizard(output_dir)
        finally:
            self._input = previous_input

    def _run_wizard(self, output_dir: str) -> str:
        """Run the wizard using the currently selected input provider."""
        print("\n SkiferHub — BuilderAgent Wizard")
        print("────────────────────────────────────\n")

        # --- [1/6] Tables ---
        tables = self._wizard_tables()

        # --- [2/6] Filtres ---
        filters_by_alias = self._wizard_filters(tables)

        # --- [3/6] Jointures ---
        joins = self._wizard_joins(tables)

        # --- [4/6] Business rules ---
        rules = self._wizard_rules()

        # --- [5/6] Select final ---
        select_final, keep_all = self._wizard_select(tables)

        # --- [6/6] Options ---
        dev_limit, output_name = self._wizard_options(output_dir)

        # Build schema dict
        schema: dict = {}
        table_list = []
        for alias, fqn in tables.items():
            entry: dict = {"name": fqn, "alias": alias}
            fs = filters_by_alias.get(alias, [])
            if fs:
                entry["filter"] = fs
            table_list.append(entry)
        schema["tables"] = table_list

        if joins:
            schema["join"] = joins
        if rules:
            schema["business_rules"] = rules
        if select_final:
            schema["select_final"] = select_final
        elif keep_all:
            schema["keep_all_columns"] = True
        if dev_limit:
            schema["dev_limit"] = dev_limit

        yaml_content = _schema_dict_to_yaml(schema)

        # Preview
        print("\n────────────────────────────────────")
        print("YAML généré :\n")
        print(yaml_content)

        # Save
        confirm = self._input("Sauvegarder ? [O/n] : ").strip().lower()
        if confirm in ("", "o", "y", "oui", "yes"):
            saved_path = self._save_yaml(yaml_content, output_name)
            print(f"\n✅ Fichier sauvegardé : {saved_path}")
            return saved_path
        else:
            print("\n⚠️ Sauvegarde annulée.")
            return ""

    @staticmethod
    def _answers_provider(
        answers: list[str] | dict[str, list[str]],
    ) -> Callable[[str], str]:
        """Build a deterministic provider that never falls back to stdin."""
        if isinstance(answers, list):
            queue = deque(answers)

            def ordered_provider(prompt: str) -> str:
                if not queue:
                    raise ValueError(f"No programmed answer remains for prompt: {prompt}")
                return queue.popleft()

            return ordered_provider
        if isinstance(answers, dict) and all(
            isinstance(prompt, str) and isinstance(values, list)
            for prompt, values in answers.items()
        ):
            queues = {prompt: deque(values) for prompt, values in answers.items()}

            def prompt_provider(prompt: str) -> str:
                queue = queues.get(prompt)
                if not queue:
                    raise ValueError(f"No programmed answer remains for prompt: {prompt}")
                return queue.popleft()

            return prompt_provider
        raise TypeError("answers must be a list of strings or a prompt-to-list mapping")

    def ask(self, description: str, output_dir: str = "schemas") -> BuilderResponse:
        """
        Mode LLM — description en langage naturel → YAML validé.

        Args:
            description: Description du pipeline en langage naturel.
            output_dir:  Répertoire de sauvegarde du YAML.

        Returns:
            BuilderResponse avec yaml_content et output_path si succès.

        Raises:
            ValueError: Si aucun LLM provider n'a été configuré.
        """
        if self._llm is None:
            raise ValueError(
                "LLM provider requis pour le mode conversationnel. "
                "Passez llm_provider= au constructeur de BuilderAgent."
            )

        # Step A — extraction de l'intent ETL
        try:
            raw = self._llm.complete(
                system_prompt=_STEP_A_SYSTEM,
                user_message=description,
                temperature=0.0,
                response_format={"type": "json_object"},
            )
            etl_struct = _extract_json(raw)
        except Exception as exc:
            return BuilderResponse(
                success=False,
                error=f"Erreur d'extraction LLM : {exc}",
            )

        # Step B — validation catalog
        clarification = self._validate_etl_struct(etl_struct)
        if clarification:
            return BuilderResponse(
                success=False,
                error="Validation catalog échouée.",
                clarification_question=clarification,
            )

        # Build YAML
        schema_dict = _build_schema_dict(etl_struct)
        yaml_content = _schema_dict_to_yaml(schema_dict)

        # Save
        output_name = etl_struct.get("output_name") or os.path.join(output_dir, "pipeline.yaml")
        try:
            saved_path = self._save_yaml(yaml_content, output_name)
        except Exception as exc:
            return BuilderResponse(
                success=False,
                yaml_content=yaml_content,
                error=f"Erreur de sauvegarde : {exc}",
            )

        return BuilderResponse(
            success=True,
            yaml_content=yaml_content,
            output_path=saved_path,
        )

    def export_orchestration(
        self,
        yaml_paths: list[str],
        output_dir: str = "orchestration",
        format: str = "auto",
    ) -> OrchestratorExportResult:
        """
        Génère les artefacts d'orchestration pour une liste de pipelines YAML.

        Args:
            yaml_paths:  Chemins des fichiers YAML de pipeline.
            output_dir:  Répertoire de sortie.
            format:      "auto" | "airflow" | "databricks" | "script"

        Returns:
            OrchestratorExportResult avec les chemins des artefacts générés.
        """
        exporter = OrchestratorExporter(backend=self._backend)
        return exporter.export(yaml_paths, output_dir=output_dir, format=format)

    # ------------------------------------------------------------------
    # Wizard steps (private)
    # ------------------------------------------------------------------

    def _wizard_tables(self) -> dict[str, str]:
        """Step 1 — collect tables. Returns {alias: fqn}."""
        print("[1/6] Tables sources")
        tables: dict[str, str] = {}
        while True:
            fqn = self._input("  Nom de la table (FQN, ex: catalog.silver.orders) : ").strip()
            if not fqn:
                if not tables:
                    print("  ⚠️  Au moins une table est requise.")
                    continue
                break
            # Validate
            try:
                self._inspector.validate_table(fqn)
                col_count = len(self._inspector.list_columns(fqn))
                print(f"  ✓ Table trouvée — {col_count} colonnes")
            except (CatalogError, ValueError) as exc:
                print(f"  ✗ {exc}")
                continue

            alias = self._input("  Alias : ").strip() or fqn.split(".")[-1]
            tables[alias] = fqn
            more = self._input("  Ajouter une autre table ? (entrée pour passer) : ").strip()
            if not more:
                break
        return tables

    def _wizard_filters(self, tables: dict[str, str]) -> dict[str, list[str]]:
        """Step 2 — collect filters per table alias."""
        print("\n[2/6] Filtres")
        filters_by_alias: dict[str, list[str]] = {}
        for alias, fqn in tables.items():
            print(f"  (table {alias})")
            alias_filters: list[str] = []
            while True:
                f = self._input("  Filtre (ex: region:equals:EMEA) ou entrée pour passer : ").strip()
                if not f:
                    break
                # Validate column name in filter
                col = _fqn_from_filter_string(f)
                if col:
                    try:
                        self._inspector.validate_columns(fqn, [col])
                        print(f"  ✓ Colonne '{col}' validée")
                    except (CatalogError, ValueError) as exc:
                        print(f"  ✗ {exc}")
                        continue
                alias_filters.append(f)
            if alias_filters:
                filters_by_alias[alias] = alias_filters
        return filters_by_alias

    def _wizard_joins(self, tables: dict[str, str]) -> list[dict]:
        """Step 3 — collect joins."""
        print("\n[3/6] Jointures")
        joins: list[dict] = []
        if len(tables) < 2:
            print("  (moins de 2 tables — jointures ignorées)")
            return joins
        while True:
            left_raw = self._input("  Table de gauche [alias, colonne] ou entrée pour passer : ").strip()
            if not left_raw:
                break
            right_raw = self._input("  Table de droite [alias, colonne] : ").strip()
            if not right_raw:
                break
            join_type = self._input("  Type (left/inner/right) [left] : ").strip() or "left"
            try:
                left_alias, left_col = [x.strip() for x in left_raw.split(",", 1)]
                right_alias, right_col = [x.strip() for x in right_raw.split(",", 1)]
            except ValueError:
                print("  ✗ Format invalide. Exemple : ord, customer_id")
                continue
            joins.append({
                "table_from": [left_alias, left_col],
                "table_to": [right_alias, right_col],
                "type": join_type,
            })
            more = self._input("  Ajouter une jointure ? (entrée pour passer) : ").strip()
            if not more:
                break
        return joins

    def _wizard_rules(self) -> list[str]:
        """Step 4 — collect registered business rule names."""
        print("\n[4/6] Business rules")
        rules: list[str] = []
        while True:
            rule = self._input("  Règle enregistrée (ex: flag_high_value) ou entrée pour passer : ").strip()
            if not rule:
                break
            rules.append(rule)
        return rules

    def _wizard_select(self, tables: dict[str, str]) -> tuple[list, bool]:
        """Step 5 — collect select_final or keep_all_columns."""
        print("\n[5/6] Select final")
        select: list = []
        keep_all = False
        first = self._input("  Colonne source (ou entrée pour keep_all_columns) : ").strip()
        if not first:
            keep_all = True
            return select, keep_all
        # Process first column
        alias_col = self._input("  Alias : ").strip() or first
        ops_raw = self._input("  Opérations (ex: cast:double, round:2) ou entrée pour aucune : ").strip()
        ops = [o.strip() for o in ops_raw.split(",")] if ops_raw else []
        select.append([first, alias_col, ops])
        # Additional columns
        while True:
            col = self._input("  Colonne suivante ou entrée pour terminer : ").strip()
            if not col:
                break
            alias_col = self._input("  Alias : ").strip() or col
            ops_raw = self._input("  Opérations ou entrée pour aucune : ").strip()
            ops = [o.strip() for o in ops_raw.split(",")] if ops_raw else []
            select.append([col, alias_col, ops])
        return select, keep_all

    def _wizard_options(self, output_dir: str) -> tuple[int | None, str]:
        """Step 6 — dev_limit and output filename."""
        print("\n[6/6] Options")
        dev_limit_raw = self._input("  dev_limit (entrée pour aucune limite) : ").strip()
        dev_limit = int(dev_limit_raw) if dev_limit_raw.isdigit() else None
        default_name = os.path.join(output_dir, "pipeline.yaml")
        output_name = self._input(f"  Nom du fichier de sortie [{default_name}] : ").strip() or default_name
        return dev_limit, output_name

    # ------------------------------------------------------------------
    # LLM catalog validation (private)
    # ------------------------------------------------------------------

    def _validate_etl_struct(self, etl_struct: dict) -> str | None:
        """
        Validate tables and columns in the ETL structure against the catalog.

        Returns a clarification question string if validation fails,
        or None if everything is valid.
        """
        for table_entry in etl_struct.get("tables", []):
            fqn = table_entry.get("fqn", "")
            if not fqn:
                continue
            try:
                self._inspector.validate_table(fqn)
            except (CatalogError, ValueError) as exc:
                return (
                    f"La table '{fqn}' n'a pas été trouvée dans le catalog. "
                    f"Détail : {exc}. "
                    "Pouvez-vous vérifier le nom ou utiliser une table disponible ?"
                )

            # Validate filter columns
            filter_cols = [
                _fqn_from_filter_string(f)
                for f in (table_entry.get("filters") or [])
                if _fqn_from_filter_string(f)
            ]
            if filter_cols:
                try:
                    self._inspector.validate_columns(fqn, filter_cols)
                except CatalogError as exc:
                    return (
                        f"Colonnes de filtre inconnues dans '{fqn}'. "
                        f"Détail : {exc}. "
                        "Pouvez-vous corriger les noms de colonnes ?"
                    )

        # Validate select_final columns against the union of all tables' columns
        select_items = etl_struct.get("select_final") or []
        if select_items:
            all_cols: set[str] = set()
            for table_entry in etl_struct.get("tables", []):
                fqn = table_entry.get("fqn", "")
                if fqn:
                    try:
                        cols = self._inspector.list_columns(fqn)
                        all_cols.update(c.lower() for c in cols)
                    except Exception:
                        pass

            if all_cols:
                unknown = []
                for item in select_items:
                    if isinstance(item, dict):
                        src = item.get("source", "")
                    elif isinstance(item, list) and item:
                        src = item[0]
                    else:
                        continue
                    # strip alias prefix (e.g. "o.order_id" → "order_id")
                    if "." in src:
                        src = src.split(".", 1)[1]
                    # skip computed sources (literal:, expr:, etc.)
                    if ":" in src or not src:
                        continue
                    if src.lower() not in all_cols:
                        unknown.append(src)

                if unknown:
                    suggestions = _top_suggestions(unknown[0], list(all_cols))
                    return (
                        f"Colonnes inconnues dans select_final : {unknown}. "
                        f"Suggestions pour '{unknown[0]}' : {suggestions}. "
                        "Pouvez-vous corriger les noms de colonnes ?"
                    )

        return None

    # ------------------------------------------------------------------
    # Save
    # ------------------------------------------------------------------

    def _save_yaml(self, yaml_content: str, output_path: str) -> str:
        """Validate via parse_schema() then write to output_path."""
        from skifer.core.schema_loader import parse_schema
        parse_schema(yaml_content)  # raises ValueError if schema is invalid
        os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as fh:
            fh.write(yaml_content)
        return output_path
