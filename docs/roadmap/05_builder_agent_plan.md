# Plan 05 — BuilderAgent : génération de YAML de pipeline ETL

> Branche : `builder_agent`
> Statut : En attente de validation (29 avril 2026)

---

## Contexte

Le `BuilderAgent` est la brique manquante du layer agentic.
Son rôle : permettre à l'utilisateur de créer des YAML de pipeline ETL
(`SkiferEngine`-ready : tables, joins, filtres, business_rules, select_final)
en langage naturel ou via un assistant pas-à-pas, sans jamais écrire le YAML à la main.

Il est **distinct du `SemanticBuilder`** (qui génère des modèles sémantiques Gold pour le `GenBIAgent`).
Le `SemanticEngine` parsera ensuite les YAML produits — toute incohérence (table/colonne inexistante)
est donc bloquante : le catalog awareness est non négociable.

---

## Deux modes

| Mode | Déclenchement | Public cible |
|---|---|---|
| **Wizard** | `builder.wizard()` | Utilisateur technique — sait ce qu'il veut, veut aller vite |
| **LLM** | `builder.ask("crée un pipeline...")` | Utilisateur moins technique — guidé par le hub conversationnel |

Les deux modes partagent la même couche de validation et de sauvegarde.

---

## Fichiers à créer / modifier

### Créer
| Fichier | Rôle |
|---|---|
| `src/skifer/core/catalog_inspector.py` | `CatalogInspector` — liste tables, colonnes, valide FQN |
| `src/skifer/agentic/builder_agent.py` | `BuilderAgent` — wizard + LLM + validation + save |
| `tests/test_builder_agent.py` | Tests unitaires (mocks Spark + LLM) |

### Modifier
| Fichier | Changement |
|---|---|
| `src/skifer/core/backend.py` | Ajouter `list_tables(schema)` et `list_columns(fqn)` au `Backend` Protocol |
| `src/skifer/backends/spark.py` | Implémenter les deux méthodes (`SHOW TABLES IN`, `SHOW COLUMNS IN`) |
| `src/skifer/backends/sql_base.py` | Implémenter via `INFORMATION_SCHEMA` |
| `src/skifer/backends/bigquery.py` | Implémenter via `INFORMATION_SCHEMA.COLUMNS` |
| `src/skifer/backends/snowpark.py` | Implémenter via `SHOW COLUMNS IN TABLE` |
| `src/skifer/__init__.py` | Exporter `BuilderAgent` |
| `CHANGELOG.md` | Entrée `[Unreleased]` |

---

## Phases d'implémentation

### Phase 1 — CatalogInspector (fondation catalog awareness)

Ajouter au `Backend` Protocol :
```python
def list_tables(self, schema: str, catalog: str | None = None) -> list[str]:
    """Liste les tables d'un schema. Retourne les noms simples."""
    ...

def list_columns(self, fqn: str) -> list[str]:
    """Liste les colonnes d'une table par FQN. Retourne les noms simples."""
    ...
```

Implémenter dans chaque backend, puis créer `CatalogInspector` :

```python
# core/catalog_inspector.py
class CatalogInspector:
    def __init__(self, backend: Backend, catalog: str | None = None): ...

    def list_tables(self, schema: str) -> list[str]: ...
    def list_columns(self, fqn: str) -> list[str]: ...

    def validate_table(self, fqn: str) -> None:
        """Lève CatalogError si la table n'existe pas."""
        ...

    def validate_columns(self, fqn: str, columns: list[str]) -> None:
        """Lève CatalogError avec les colonnes inconnues + suggestions Levenshtein."""
        ...

    def fqn(self, schema: str, table: str) -> str:
        """Construit le FQN correct selon catalog présent ou non."""
        ...
```

**Vérification** : `pytest tests/test_catalog_inspector.py -x`

---

### Phase 2 — Wizard (mode technique pas-à-pas)

Interface inspirée du générateur d'entités Symfony — chaque section est une étape,
deux Entrées vides = passage à la section suivante.

```
SkiferHub — BuilderAgent Wizard
────────────────────────────────────

[1/6] Tables sources
  Nom de la table (FQN, ex: catalog.silver.orders) : catalog.silver.orders
  ✓ Table trouvée — 12 colonnes
  Alias : ord
  Ajouter une autre table ? (entrée pour passer) :

[2/6] Filtres (table ord)
  Filtre (ex: region:equals:EMEA) ou entrée pour passer : region:equals:EMEA
  ✓ Colonne 'region' validée
  Filtre suivant ou entrée pour passer :

[3/6] Jointures
  Table de gauche [alias, colonne] : ord, customer_id
  Table de droite [alias, colonne] : cust, id
  Type (left/inner/right) [left] :
  Ajouter une jointure ? (entrée pour passer) :

[4/6] Business rules
  Règle enregistrée (ex: flag_high_value) ou entrée pour passer :

[5/6] Select final
  Colonne source (ou entrée pour keep_all_columns) : amount
  Alias : amount_eur
  Opérations (ex: cast:double, round:2) : cast:double, round:2
  Colonne suivante ou entrée pour terminer :

[6/6] Options
  dev_limit [10000] :
  Nom du fichier de sortie [schemas/silver/orders_customers.yaml] :

────────────────────────────────────
YAML généré :

tables:
  - name: "catalog.silver.orders"
    alias: ord
    filter:
      - "region:equals:EMEA"
...

Sauvegarder ? [O/n] :
```

Chaque valeur saisie est validée par le `CatalogInspector` avant de passer à la suivante.
Un filtre avec une colonne inconnue affiche les suggestions et redemande.

**Vérification** : `pytest tests/test_builder_agent.py::test_wizard -x`

---

### Phase 3 — Mode LLM (conversationnel)

L'utilisateur décrit son pipeline en langage naturel. Le LLM extrait la structure,
le `CatalogInspector` valide chaque table/colonne avant de finaliser.

**Pipeline 2 étapes** (aligné sur le pattern `GenBIAgent`) :

```
Step A : extraction de l'intent ETL → JSON structuré (tables, joins, filtres, select)
Step B : validation catalog + génération YAML final
```

Prompt Step A :
```
Tu es un expert ETL Lakehouse. Extrais la structure du pipeline décrit.
Réponds UNIQUEMENT avec un JSON valide :
{
  "tables": [{"fqn": "...", "alias": "...", "filters": [...]}],
  "joins": [{"from": ["alias", "col"], "to": ["alias", "col"], "type": "left"}],
  "business_rules": [...],
  "select_final": [{"source": "...", "target": "...", "ops": [...]}],
  "keep_all_columns": false,
  "dev_limit": 10000,
  "output_name": "schemas/silver/xxx.yaml"
}
```

Après Step A, le `CatalogInspector` valide tables et colonnes.
Si une table est inconnue → question de clarification à l'user (pas de hallucination silencieuse).
Si une colonne est inconnue → suggestions Levenshtein + confirmation.

Une fois validé, le YAML est affiché et l'user confirme la sauvegarde.

**Vérification** : `pytest tests/test_builder_agent.py::test_llm_mode -x`

---

### Phase 4 — Génération d'orchestration

Après sauvegarde du YAML de pipeline, le `BuilderAgent` peut générer l'artefact d'orchestration
correspondant. Trois niveaux, du plus universel au plus natif.

#### Stratégie de sélection

```
Backend détecté       Format primaire              Fallback
─────────────────────────────────────────────────────────────────
Spark / Databricks    Airflow DAG + DAB YAML       Python script
Snowflake             Airflow DAG                  Python script
BigQuery              Airflow DAG                  Python script
Local / inconnu       Python script uniquement
```

#### Format primaire — Airflow DAG (universel)

Providers officiels : `apache-airflow-providers-databricks`, `apache-airflow-providers-snowflake`,
`apache-airflow-providers-google`. Un seul format lisible, versionnable, cross-plateforme.

```python
# orchestration/airflow/dag_orders_emea.py  (généré)
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime

with DAG("orders_emea_pipeline", start_date=datetime(2026, 1, 1), schedule="@daily") as dag:
    bronze = DatabricksRunNowOperator(task_id="bronze_orders", job_id=1001)
    silver = DatabricksRunNowOperator(task_id="silver_orders_emea", job_id=1002)
    bronze >> silver
```

#### Format natif Databricks — Asset Bundle YAML (optionnel)

Généré en complément si le backend est Spark/Databricks.
S'intègre directement dans un projet DAB (`databricks bundle deploy`).

```yaml
# orchestration/databricks/bundle.yml  (généré)
bundle:
  name: skifer_pipelines

resources:
  jobs:
    orders_emea_pipeline:
      name: orders_emea_pipeline
      tasks:
        - task_key: silver_orders_emea
          notebook_task:
            notebook_path: notebooks/silver_orders_emea
          depends_on: []
```

#### Fallback universel — Python script

Fonctionne sans aucun orchestrateur. Appelle `SkiferEngine.run_process_to_table()`
séquentiellement. Exécutable avec un simple `python run_pipelines.py`.

```python
# orchestration/scripts/run_orders_emea.py  (généré)
from skifer import SkiferEngine, load_schema

engine = SkiferEngine("config.yaml")

schema = load_schema("schemas/silver/orders_emea.yaml", params=engine.default_params)
engine.run_process_to_table(schema)
```

#### Interface `BuilderAgent` étendue

```python
def export_orchestration(
    self,
    yaml_paths: list[str],
    output_dir: str = "orchestration",
    format: str = "auto",   # "auto" | "airflow" | "databricks" | "script"
) -> OrchestratorExportResult:
    """
    Génère les artefacts d'orchestration pour une liste de pipelines YAML.
    "auto" détecte le backend et choisit le format le plus adapté.
    Génère toujours le fallback Python script en complément.
    """
    ...
```

`OrchestratorExportResult` (ajout dans `models.py`) :
```python
@dataclass
class OrchestratorExportResult:
    format: str                  # "airflow" | "databricks" | "script"
    primary_path: str            # chemin du DAG ou bundle
    fallback_script_path: str    # toujours présent
    pipelines: list[str]         # yaml_paths traités
```

**Fichiers à créer** :
- `src/skifer/agentic/orchestrator.py` — `OrchestratorExporter`
- `tests/test_orchestrator.py`

**Vérification** : `pytest tests/test_orchestrator.py -x`

---

### Phase 5 — Interface `BuilderAgent` + export public API

```python
# agentic/builder_agent.py
class BuilderAgent:
    def __init__(
        self,
        core_engine: SkiferEngine,
        llm_provider: LLMProvider | None = None,
    ):
        self._inspector = CatalogInspector(core_engine._get_backend(), core_engine.db)
        self._llm = llm_provider
        self._core = core_engine

    def wizard(self, output_dir: str = "schemas") -> str:
        """Lance le wizard interactif. Retourne le chemin du fichier sauvegardé."""
        ...

    def ask(self, description: str, output_dir: str = "schemas") -> BuilderResponse:
        """Mode LLM — description en langage naturel → YAML validé."""
        if self._llm is None:
            raise ValueError("LLM provider requis pour le mode conversationnel.")
        ...

    def _validate_and_save(self, schema_dict: dict, output_path: str) -> str:
        """parse_schema() + écriture fichier YAML."""
        ...
```

`BuilderResponse` (ajout dans `models.py`) :
```python
@dataclass
class BuilderResponse:
    success: bool
    yaml_content: str
    output_path: str | None = None
    error: str | None = None
    clarification_question: str | None = None
```

**Vérification finale** : `pytest tests/ -x --tb=short` (suite complète)

---

## Stratégie de validation catalog

```
Table non trouvée    → CatalogError + liste des tables disponibles dans le schema
Colonne non trouvée  → CatalogError + top-3 Levenshtein depuis les colonnes réelles
FQN mal formé        → ValueError immédiate (avant toute requête catalog)
```

Le wizard bloque sur chaque erreur et redemande.
Le mode LLM génère une `clarification_question` dans le `BuilderResponse`.

---

## Dépendances

- Nécessite un backend connecté (Spark local, Databricks Connect, ou autre).
- LLM provider optionnel — le wizard fonctionne sans LLM.
- `parse_schema()` (existant) utilisé pour validation finale avant sauvegarde.
- Pas de dépendance vers `SemanticEngine` ou `GenBIAgent`.
- Orchestration Airflow : `apache-airflow-providers-*` optionnels (groupe `[orchestration]` dans `pyproject.toml`).
- DAB : aucune dépendance Python — génère du YAML pur.

---

## Risques

| Risque | Mitigation |
|---|---|
| Backend ne supporte pas `list_columns` | Fallback : `table_exists()` only, colonnes non validées (warning) |
| LLM hallucine un FQN | Step A retourne un JSON — le `CatalogInspector` valide avant Step B |
| Wizard trop verbeux pour les purists | Ajouter un flag `--compact` (une ligne par champ) en v2 |
| Plateforme sans orchestrateur natif | Le fallback Python script est toujours généré — aucun blocage |
| DAB format change (Databricks) | Le DAB est optionnel, le DAG Airflow reste le format de référence |
