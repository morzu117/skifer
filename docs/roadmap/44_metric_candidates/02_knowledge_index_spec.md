# S02 — Graphe de connaissance et index BM25

> **Décisions :** D7 (BM25 seul dans ce lot ; le dense est S08), D8 (synchro par fingerprints)
> **Dépendances :** aucune
> **Slices / commits :** 4
> **Branche :** `feat/plan44-s02-knowledge-index`
> **Résultat :** `skifer knowledge sync` puis `skifer knowledge search "…"` trouvent, sans Spark
> et sans LLM, les tables, colonnes, métriques et règles pertinentes pour une question.

## 1. Contexte

L'agent de repli (S03) doit trouver *où chercher* avant de demander quoi que ce soit au LLM.
Le graphe réunit ce que Skifer sait déjà, plus ce qu'Unity Catalog sait de ce que Skifer ne
gère pas :

- les pipelines : l'index de métadonnées du Plan 31 (`observability/metadata_store.py`,
  `metadata_index.py`) fournit les datasets, les colonnes et le lineage ;
- les modèles sémantiques : `semantic_catalog.yaml` et les modèles qu'il référence, lus par la
  synchro en batch. C'est un job, pas le démarrage du moteur, et le chargement paresseux de
  `SemanticEngine` n'est pas touché ;
- les règles : `RuleRegistry.list_rules()` et leurs docstrings ;
- Unity Catalog : `CatalogInspector` (`core/catalog_inspector.py` : `list_tables`,
  `list_columns`, `describe_table`) et `system.access.table_lineage` quand il est lisible.

Pas de Vector Search ni de base de graphe. Le graphe est stocké dans deux tables et l'index BM25
est calculé en Python au chargement, sur quelques milliers de nœuds.

## 2. Contrat

```python
# knowledge/models.py
NODE_KINDS = ("table", "column", "metric", "dimension", "rule", "domain")
NODE_STATUSES = ("certified", "candidate", "reference")
EDGE_KINDS = ("has_column", "joins", "derives", "belongs_to", "shortcut")

@dataclass(frozen=True)
class Node:
    node_id: str            # "<kind>:<fqn>" — ex. "column:main.sales.orders.amount"
    kind: str
    fqn: str
    label: str              # nom court affichable
    text: str               # description + synonymes, ce que BM25 indexe en plus du nom
    synonyms: tuple[str, ...]
    status: str
    domain: str | None      # "<catalog>.<schema>" ; None pour un domaine lui-même
    source: str             # "pipeline" | "semantic" | "rule" | "unity_catalog"
    fingerprint: str        # sha256:v1: du contenu canonique du nœud

@dataclass(frozen=True)
class Edge:
    edge_id: str            # sha256:v1: de (kind, from, to, keys)
    kind: str
    from_id: str
    to_id: str
    keys: tuple[str, ...]   # clés de jointure pour "joins"/"shortcut", vide sinon
    weight: float           # (0, 1]
    source: str

@dataclass(frozen=True)
class SearchHit:
    node: Node
    score: float
    matched_terms: tuple[str, ...]   # triés ; servent à l'explication et à S07

@dataclass(frozen=True)
class SyncReport:
    added: int
    updated: int
    unchanged: int
    removed: int
    unavailable: tuple[str, ...]     # ex. ("system.access.table_lineage",) — pas une erreur
```

```python
# knowledge/store.py
class KnowledgeStore(Protocol):
    def upsert_nodes(self, nodes: Iterable[Node]) -> tuple[int, int, int]   # (added, updated, unchanged)
    def upsert_edges(self, edges: Iterable[Edge]) -> tuple[int, int, int]
    def mark_removed(self, node_ids: Iterable[str]) -> int                  # soft delete
    def list_nodes(self, *, kinds: Iterable[str] | None = None,
                   domain: str | None = None, include_removed: bool = False) -> list[Node]
    def list_edges(self, *, node_id: str | None = None,
                   kinds: Iterable[str] | None = None) -> list[Edge]

class SqliteKnowledgeStore:   # ".skifer_knowledge.db"
class DeltaKnowledgeStore:    # "<catalog>._skifer_knowledge", tables knowledge_nodes / knowledge_edges
def default_knowledge_store(engine) -> KnowledgeStore
```

```python
# knowledge/sync.py
class KnowledgeSynchronizer:
    def __init__(self, store, *, engine=None, metadata_store=None, catalog_inspector=None,
                 rule_registry=RuleRegistry, domains: Iterable[str] | None = None): ...
    def sync(self, *, dry_run: bool = False) -> SyncReport
```

Ordre de synchro : pipelines, sémantique, règles, puis Unity Catalog. Un nœud déjà fourni par une
source plus haute n'est pas écrasé par une source plus basse, et un nœud UC n'efface jamais un
nœud `semantic`. Les nœuds UC prennent le statut `reference`, ceux des métriques du catalogue le
statut `certified`. `domains`, ou `knowledge_domains` dans la config, borne la synchro UC aux
schémas listés, ce qui pare au risque R3. Les arêtes `shortcut` sont dérivées des jointures
déclarées dans les pipelines et des `relationships` des modèles, avec `weight = 1.0`. S05 en
ajoute pour les candidates promues.

```python
# knowledge/index.py
BM25_K1 = 1.2
BM25_B = 0.75
FIELD_WEIGHTS = {"label": 3.0, "synonyms": 2.0, "text": 1.0}

def tokenize(text: str) -> tuple[str, ...]
    # minuscules, accents retirés (NFKD), coupe snake_case, camelCase, chiffres/lettres, ponctuation ;
    # mots vides FR + EN d'une liste figée dans le module ; aucune racinisation en v1

class KnowledgeIndex:
    @classmethod
    def from_store(cls, store: KnowledgeStore) -> "KnowledgeIndex"
    def search(self, query: str, *, k: int = 10, kinds: Iterable[str] | None = None,
               domain: str | None = None) -> list[SearchHit]
    def neighbors(self, node_id: str, *, max_depth: int = 2, min_weight: float = 0.5,
                  kinds: Iterable[str] | None = None) -> list[tuple[Node, int]]   # (nœud, distance)
```

CLI :

```bash
skifer knowledge sync [--schema CAT.SCHEMA ...] [--dry-run] [--json]
skifer knowledge search "QUESTION" [--kind table|column|metric|...] [--domain CAT.SCHEMA] [-k N] [--json]
```

Codes : `0` succès, `1` erreur technique, `2` usage.

## 3. Exigences

- **S02-R1** — La synchro DOIT être idempotente : un second `sync()` sur des sources inchangées
  rapporte `added == updated == removed == 0` et n'émet **aucune** écriture (vérifié sur le fake
  backend).
- **S02-R2** — Un changement de description, de synonyme ou de colonne d'un seul objet DOIT
  produire exactement un `updated` pour ce nœud, et aucun autre.
- **S02-R3** — Un objet disparu d'une source DOIT être marqué `removed` (soft delete) et exclu
  de `search()`. Il NE DOIT PAS être supprimé physiquement.
- **S02-R4** — Une table présente dans UC et inconnue de Skifer DOIT devenir un nœud `reference`,
  et une métrique du catalogue un nœud `certified`.
- **S02-R5** — L'indisponibilité de `system.access.table_lineage`, ou une permission refusée sur
  un schéma, DOIT apparaître dans `SyncReport.unavailable` et NE DOIT PAS lever d'exception.
- **S02-R6** — `tokenize("totalAmountEur")` et `tokenize("total_amount_eur")` DOIVENT donner
  `("total", "amount", "eur")`. `tokenize("Chiffre d'affaires")` DOIT donner `("chiffre", "affaires")`.
- **S02-R7** — `search()` DOIT être déterministe : à scores égaux, les résultats sont triés par
  `node_id`, et le résultat est identique sous n'importe quel `PYTHONHASHSEED`.
- **S02-R8** — Un match sur `label` DOIT l'emporter sur un match du même terme dans `text`
  seulement (pondération par champ).
- **S02-R9** — `neighbors()` DOIT respecter `max_depth` et `min_weight`, ne jamais boucler sur un
  cycle, et renvoyer chaque nœud une seule fois à sa distance minimale.
- **S02-R10** — Le module NE DOIT importer ni Spark ni `sentence_transformers` au niveau module
  (test `sys.modules`). Le chemin SQLite fonctionne sans session Spark.
- **S02-R11** — `SemanticEngine` NE DOIT PAS charger plus de modèles au démarrage qu'avant ce lot
  (le test existant du chargement paresseux reste vert, sans modification).

## 4. Fichiers

**À créer**

- `src/skifer/knowledge/__init__.py`, `models.py`, `store.py`, `sync.py`, `index.py`
- `tests/test_knowledge_models.py`, `tests/test_knowledge_store.py`,
  `tests/test_knowledge_sync.py`, `tests/test_knowledge_index.py`

**À modifier**

- `src/skifer/core/spark_backend.py` : primitives `upsert_knowledge_rows(schema, table, rows,
  id_column)` en `MERGE INTO` sur l'identifiant, avec condition `fingerprint` différent pour ne
  rien réécrire d'inchangé, et `list_knowledge_rows(schema, table, **filters)`.
- `tests/fakes/fake_backend.py` : les mêmes primitives.
- `src/skifer/cli.py` : sous-commande `knowledge` (`sync`, `search`), sur le patron de
  `adaptive`.
- `.gitignore` : `.skifer_knowledge.db`.
- `CHANGELOG.md`.

## 5. Slices

- **S02.1 Modèles et fingerprints.** `knowledge/models.py`.
- **S02.2 Stores.** SQLite, Delta, fake et `default_knowledge_store`.
- **S02.3 Synchro.** Les quatre sources, l'ordre de priorité, le rapport et `domains`. Les tests
  utilisent les schémas et modèles d'`examples/`, avec un `CatalogInspector` factice qui expose
  une table inconnue de Skifer.
- **S02.4 Index et CLI.** `tokenize`, BM25, `neighbors`, `skifer knowledge sync|search`.

## 6. Tests d'acceptation

| Exigence | Test |
|---|---|
| S02-R1 | `test_knowledge_sync.py::test_second_sync_writes_nothing` |
| S02-R2 | `…::test_single_change_updates_single_node` |
| S02-R3 | `…::test_removed_object_is_soft_deleted_and_unsearchable` |
| S02-R4 | `…::test_unity_catalog_only_table_becomes_reference_node`, `…::test_catalog_metric_is_certified_node` |
| S02-R5 | `…::test_unreadable_system_lineage_is_reported_not_raised` |
| S02-R6 | `test_knowledge_index.py::test_tokenize_cases` (paramétré) |
| S02-R7 | `…::test_search_is_deterministic_across_hash_seeds` (sous-processus) |
| S02-R8 | `…::test_label_match_outranks_text_match` |
| S02-R9 | `…::test_neighbors_bounded_and_cycle_safe` |
| S02-R10 | `test_knowledge_models.py::test_package_imports_without_spark_or_embeddings` |
| S02-R11 | test existant du lazy-load de `SemanticEngine`, inchangé et vert |

## 7. Erreurs

| Cas | Comportement |
|---|---|
| store inaccessible | exception du store, code `1`, message nommant le store et la base |
| `--schema` mal formé | code `2`, `--schema expects CATALOG.SCHEMA` |
| index vide (`search` avant `sync`) | liste vide, avec en CLI la ligne `index is empty — run 'skifer knowledge sync'` et le code `0` |
| permission UC refusée | entrée dans `unavailable`, pas d'exception |

## 8. Hors périmètre

- embeddings, recherche dense ou hybride (S08) ;
- arêtes `shortcut` issues des candidates (S05) ;
- toute synchro planifiée (un job Databricks l'appelle, ce lot ne crée pas de job) ;
- Change Data Feed sur des tables non Skifer (D8 : snapshot par fingerprints seulement).

## 9. Definition of Done

- les 11 exigences ont leur test vert ;
- `skifer knowledge sync` puis `search "chiffre d'affaires par pays"` sur `examples/` renvoie la
  métrique attendue dans les 3 premiers résultats ; ce cas sert de test CLI.
