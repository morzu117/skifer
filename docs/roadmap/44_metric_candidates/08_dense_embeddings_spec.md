# S08 — Embeddings denses en option

> **Décision :** D7, en suspens. Ce lot **rend la mesure possible**, il ne tranche rien.
> **Dépendances :** S02 mergée
> **Slices / commits :** 2
> **Branche :** `feat/plan44-s08-dense-embeddings`
> **Résultat :** avec l'extra `[knowledge-embeddings]`, l'index sait chercher en `dense` et en
> `hybrid`. Sans l'extra, rien ne change et rien ne s'importe.

## 1. Contexte

Le benchmark de S09 doit comparer BM25 seul à deux modèles multilingues petits, exécutés sur le
driver sans endpoint (contrainte « sans coût fixe » du brief) :

- `intfloat/multilingual-e5-small`, avec les préfixes `query: ` et `passage: ` ;
- `BAAI/bge-m3`.

Quelques milliers de nœuds tiennent en mémoire : la similarité cosinus se calcule en Python, ou
en numpy si l'extra l'apporte. Ni Vector Search, ni FAISS.

## 2. Contrat

`pyproject.toml`, avec l'extra seulement, sans toucher à la version :

```toml
knowledge-embeddings = ["sentence-transformers>=3"]
```

```python
# knowledge/embeddings.py
SUPPORTED_MODELS = {
    "multilingual-e5-small": {"hf_id": "intfloat/multilingual-e5-small",
                              "query_prefix": "query: ", "passage_prefix": "passage: "},
    "bge-m3": {"hf_id": "BAAI/bge-m3", "query_prefix": "", "passage_prefix": ""},
}

class EmbeddingsUnavailableError(RuntimeError): ...   # nomme l'extra et le cache

class Embedder(Protocol):
    model_name: str
    def embed_passages(self, texts: Sequence[str]) -> list[list[float]]
    def embed_query(self, text: str) -> list[float]

def load_embedder(model_name: str, *, cache_dir: str | None = None,
                  local_files_only: bool = True) -> Embedder
    # import paresseux de sentence_transformers ; local_files_only=True par défaut :
    # aucun téléchargement implicite sur un driver ; erreur explicite si absent du cache

def reciprocal_rank_fusion(rankings: Sequence[Sequence[str]], *, k: int = 60) -> list[tuple[str, float]]
```

Côté store, deux colonnes sont ajoutées à `knowledge_nodes` : `embedding` (`ARRAY<FLOAT>` en
Delta, JSON en SQLite) et `embedding_model`. C'est une évolution additive :

- Delta : `ALTER TABLE … ADD COLUMNS` si les colonnes sont absentes, sans recréer la table ;
- SQLite : `ALTER TABLE ADD COLUMN`.

`KnowledgeIndex.search(..., mode="bm25" | "dense" | "hybrid")`, avec `bm25` par défaut pour
garder la signature de S02. `dense` et `hybrid` exigent un embedder injecté (`KnowledgeIndex.from_store(store,
embedder=…)`) et lèvent `EmbeddingsUnavailableError` sinon. `hybrid` est le RRF des classements
BM25 et dense.

La commande `skifer knowledge sync --embed MODEL` ne ré-embedde que les nœuds dont le
`fingerprint` a changé, ou dont `embedding_model` diffère.

Configuration :

```yaml
knowledge_embedding_model: null      # null | multilingual-e5-small | bge-m3
knowledge_model_cache: null          # chemin local ou /Volumes/<cat>/<schema>/<vol>/models
knowledge_search_mode: null          # null = bm25 tant que D7 n'est pas tranchée (S09)
```

## 3. Exigences

- **S08-R1** — `import skifer.knowledge` (et `skifer`) NE DOIT PAS importer `sentence_transformers`
  ni `torch` (`sys.modules`), avec ou sans l'extra installé.
- **S08-R2** — Sans l'extra, `mode="dense"` ou `--embed` DOIVENT lever
  `EmbeddingsUnavailableError`, dont le message nomme
  `pip install "skifer[knowledge-embeddings]"`. Le mode `bm25` continue de fonctionner.
- **S08-R3** — Un modèle absent du cache DOIT lever `EmbeddingsUnavailableError`, en nommant le
  chemin de cache et l'option `local_files_only`. Aucun téléchargement implicite n'a lieu.
- **S08-R4** — Les préfixes `query: ` et `passage: ` DOIVENT être appliqués pour e5, et seulement
  pour lui. Le test passe par un embedder factice qui enregistre ses entrées.
- **S08-R5** — `reciprocal_rank_fusion` DOIT être déterministe et départager les égalités par
  identifiant. Il est testé sur des classements injectés avec un résultat attendu codé en dur.
- **S08-R6** — Un second `sync --embed` sans changement DOIT n'embedder aucun nœud. Changer de
  modèle DOIT ré-embedder tous les nœuds.
- **S08-R7** — L'ajout des colonnes NE DOIT PAS perdre de ligne existante. La table de S02 est
  migrée en place, ce que vérifie un test SQLite et un test Delta sur le fake.
- **S08-R8** — Le défaut de `search()` DOIT rester `bm25`. Les tests de S02 restent inchangés.

## 4. Fichiers

**À créer** : `src/skifer/knowledge/embeddings.py` et `tests/test_knowledge_embeddings.py`.

**À modifier** :

- `pyproject.toml`, pour l'extra seulement ;
- `src/skifer/knowledge/index.py` (`mode`, `embedder`) et `store.py` (les colonnes et la
  migration) ;
- `src/skifer/core/spark_backend.py` et `tests/fakes/fake_backend.py`, pour l'`ADD COLUMNS` ;
- `src/skifer/cli.py`, pour `--embed` ;
- `src/skifer/core/config.py` ;
- `CHANGELOG.md`.

## 5. Slices

- **S08.1 Embedder et fusion.** `embeddings.py`, l'extra, la migration du store, `sync --embed`.
- **S08.2 Modes de recherche.** `dense` et `hybrid` dans `KnowledgeIndex`, et la config.

## 6. Tests d'acceptation

| Exigence | Test (`tests/test_knowledge_embeddings.py::…`) |
|---|---|
| S08-R1 | `test_core_never_imports_sentence_transformers` |
| S08-R2 | `test_dense_without_extra_names_the_extra` |
| S08-R3 | `test_missing_model_names_cache_and_never_downloads` |
| S08-R4 | `test_e5_prefixes_only_for_e5` |
| S08-R5 | `test_rrf_expected_ranking` |
| S08-R6 | `test_embed_only_changed_nodes`, `test_model_change_reembeds_all` |
| S08-R7 | `test_store_migration_keeps_rows[sqlite|delta]` |
| S08-R8 | tests de S02, inchangés |

Aucun test ne télécharge ni ne charge un vrai modèle : l'embedder est factice et produit des
vecteurs déterministes à partir d'un hash du texte.

## 7. Erreurs

Le seul type nouveau est `EmbeddingsUnavailableError`, dont les messages sont fixés par S08-R2 et
S08-R3. Un nom de modèle hors de `SUPPORTED_MODELS` donne une erreur de config au chargement.

## 8. Hors périmètre

- **le choix du défaut**, qui est la décision D7 et revient à S09 ;
- le fine-tuning, d'autres modèles, Vector Search ou un endpoint d'embeddings ;
- l'installation de l'extra dans les jobs CI par défaut. Un job optionnel pourra venir après D7.

## 9. Definition of Done

- les 8 exigences ont leur test vert, avec et sans l'extra installé (`uv run` sans
  `--extra knowledge-embeddings`).
