# Feature 0 — Projection sémantique parallèle depuis le pipeline

> **Priorité :** P0 — fondation du programme
> **Dépendances :** Plans 17/18 (IR), Plan 28 (`aggregate`)
> **Slices / commits :** 6
> **Branche suggérée :** `feat/semantic-projection`
> **Résultat :** un pipeline non déployé produit un draft sémantique déterministe et un drift check CI

## 1. Problème à résoudre

`SemanticBuilder` construit aujourd'hui un modèle principalement depuis un notebook, un glossaire et
un prompt. Il intervient donc après ou à côté du modèle de données. La feature doit faire du pipeline
YAML et de son IR la source déterministe du futur schéma physique, afin que la couche sémantique soit
préparée dans la même PR que la table.

## 2. Architecture cible

```text
pipeline YAML
  ├── transformations existantes
  ├── data_product
  ├── contract.output
  └── semantic seed
          │
          ▼
schema_loader → ParsedSchema
                    │
                    ▼
             OutputProjector (pur)
                    │ ProjectedSchema
          ┌─────────┴──────────┐
          ▼                    ▼
SemanticDraftBuilder      CrossValidator
          │                    │
          ▼                    ▼
.drafts/model.yaml        drift/conflicts CI
          │
          ▼ explicite seulement
semantic_models/model.yaml → SemanticEngine lazy
```

### 2.1 Nouveaux modèles

```python
@dataclass(frozen=True)
class ParsedDataProduct:
    id: str
    version: str
    owner: str | None
    description: str | None

@dataclass(frozen=True)
class ParsedOutputField:
    name: str
    logical_type: str | None
    required: bool | None
    unique: bool | None
    classification: str | None
    entity: str | None
    description: str | None

@dataclass(frozen=True)
class ParsedSemanticSeed:
    model_key: str
    entity: str | None
    default_time_dimension: str | None
    dimensions: tuple[str, ...]

@dataclass(frozen=True)
class ProjectedField:
    name: str
    physical_type: str | None
    logical_type: str | None
    source_fields: tuple[str, ...]
    transformations: tuple[str, ...]
    inference_status: Literal["known", "declared", "unknown"]
    inference_reason: str | None

@dataclass(frozen=True)
class ProjectedSchema:
    data_product_id: str
    target_hint: str | None
    fields: tuple[ProjectedField, ...]
    grain: tuple[str, ...]
    definition_hash: str
```

Les dataclasses exactes peuvent évoluer, mais les propriétés immuables et le diagnostic
`unknown + reason` sont obligatoires.

### 2.2 Surface YAML

Ajouter trois blocs optionnels de haut niveau : `data_product`, `contract`, `semantic`. Leur absence
préserve exactement le comportement actuel.

Contraintes :

- `data_product.id` et `semantic.model_key` suivent `[a-zA-Z0-9_.-]+` ;
- `contract.output` est un mapping indexé par nom de colonne finale ;
- `contract.grain` référence des outputs existants ;
- `semantic.dimensions` et `default_time_dimension` référencent des outputs existants ;
- `aggregate.measures.target` devient une métrique candidate ;
- aucun SQL n'est accepté dans `semantic` ;
- une rule Python non introspectable produit un champ/type inconnu, jamais une supposition.

### 2.3 Propriété et fusion

Le draft contient des champs `managed` et `curated` conceptuellement séparés. Ne pas compter sur des
commentaires YAML pour délimiter les zones. Stocker les métadonnées de synchronisation :

```yaml
metadata:
  source_contract_id: sales.orders
  source_contract_version: 1.0.0
  source_definition_hash: abc123
  generated_fields: [order_id, customer_id, net_revenue]
```

Lors d'un resync :

- les champs physiques gérés sont mis à jour ;
- descriptions/synonymes humains sont conservés ;
- une dépendance supprimée devient conflit ;
- un rename n'est jamais appliqué sur simple similarité ; il est proposé ;
- le modèle publié n'est jamais modifié sans `--promote` explicite.

## 3. Fichiers

### À créer

- `src/skifer/semantic/output_projection.py`
- `src/skifer/semantic/draft_builder.py`
- `src/skifer/semantic/sync.py`
- `tests/test_semantic_projection.py`
- `tests/test_semantic_sync.py`

### À modifier

- `src/skifer/core/ir.py`
- `src/skifer/core/schema_loader.py`
- `src/skifer/core/json_schema.py`
- `schemas/skifer-pipeline.schema.json`
- `src/skifer/semantic/builder.py`
- `src/skifer/semantic/validator.py`
- `src/skifer/cli.py`
- `src/skifer/semantic/__init__.py`
- `tests/test_ir.py`, `tests/test_schema_loader.py`, `tests/test_json_schema.py`
- `tests/test_semantic_builder.py`, `tests/test_cli.py`
- `docs/yaml_spec.md`, `docs/agentic.md`, `README.md`, `CHANGELOG.md`

Ne pas modifier `SemanticEngine._load_catalog()` pour précharger les drafts.

## 4. Slices exécutables

### Slice 0.1 — IR du produit, contrat et seed

1. Écrire les tests loader/IR rouges.
2. Ajouter normalisation avec agrégation de toutes les erreurs, style `schema_loader` existant.
3. Ajouter dataclasses et parsing IR.
4. Étendre le JSON Schema depuis le générateur puis régénérer l'artefact.
5. Ajouter changelog.

Tests minimum : ancien YAML inchangé ; nominal ; mauvaise version ; output non mapping ; grain,
dimension et time dimension absents ; clés inconnues ; params injectés dans descriptions/owner.

Commit : `feat(planNN-0.1): add output contract and semantic seed IR`.

### Slice 0.2 — `OutputProjector`

Ordre de résolution :

1. `aggregate` : group keys + measure targets ;
2. sinon `select_final` : target + ops ;
3. sinon `keep_all_columns` : projection incomplète sans catalogue, marquée `unknown` ;
4. intégrer `add_columns` si elles survivent dans la sortie ;
5. appliquer les déclarations `contract.output` ;
6. calculer un hash canonique avec JSON trié, sans chemins absolus ni timestamps.

Ne pas démarrer Spark. Ajouter des tests de parité sur les types inférables (`cast`, `lit`, `length`,
agrégations) et des diagnostics pour `expr`, rules Python, coalesce hétérogène.

Commit : `feat(planNN-0.2): project output schema without Spark`.

### Slice 0.3 — Draft déterministe

Le builder :

- ne reçoit aucun LLM ;
- mappe types physiques/logiques vers les types sémantiques existants ;
- produit dimensions uniquement si déclarées ou sûres ;
- projette les mesures `aggregate` avec leur fonction canonique ;
- écrit atomiquement dans `.drafts` via fichier temporaire + rename ;
- refuse d'écraser un fichier non reconnu comme draft géré.

Snapshots YAML avec `sort_keys=False` mais assertions sur le dict parsé, pas seulement le texte.

Commit : `feat(planNN-0.3): generate deterministic semantic drafts`.

### Slice 0.4 — Merge et conflits

Créer les types `SemanticChange`, `SemanticConflict`, `SyncReport`. Implémenter une fusion à partir
des métadonnées de dernière génération, pas d'un simple diff entre deux fichiers.

Cas obligatoires : ajout compatible, suppression non référencée, suppression référencée par metric,
type élargi/rétréci, grain changé, rename suggéré, description curatée conservée, rerun idempotent.

Toute ambiguïté retourne un rapport non vide et ne modifie aucun fichier.

Commit : `feat(planNN-0.4): synchronize drafts without overwriting curation`.

### Slice 0.5 — CLI/CI

Ajouter sous-commandes avec `argparse` selon le style existant :

```text
skifer semantic sync PIPELINE --check
skifer semantic sync PIPELINE --write-draft
skifer semantic sync PIPELINE --promote
skifer semantic validate PIPELINE MODEL
```

Codes : `0` identique/valide, `1` erreur technique/validation, `2` drift nécessitant mise à jour,
`3` conflit. `--check` n'écrit rien. `--promote` refuse un draft avec conflits ou validation KO et
met à jour le catalogue via la voie existante.

Commit : `feat(planNN-0.5): add semantic sync CI workflow`.

### Slice 0.6 — Enrichissement LLM contraint et E2E

Refactorer `SemanticBuilder` pour accepter un `ProjectedSchema`/draft. Après réponse LLM :

- rejeter toute dimension/metric source inconnue ;
- rejeter tout changement de champ géré ;
- accepter descriptions, synonymes et métriques dont toutes les dépendances existent ;
- passer le résultat au même `SemanticValidator` et au cross-validator.

E2E sans table : pipeline YAML → draft → fake LLM → modèle promu → catalogue lazy → validation.

Commit : `feat(planNN-0.6): enrich projected semantic models safely`.

## 5. Erreurs obligatoires

- `[contract.output] column 'x' is not produced by the pipeline.`
- `[semantic.dimensions] 'x' is not a projected output.`
- `Semantic projection cannot infer type for 'x': Python rule '<rule>'. Declare logical_type.`
- `Semantic sync conflict: metric 'm' depends on removed column 'x'.`
- `Refusing to overwrite curated model without --promote.`

Le texte exact peut être adapté, mais section, objet, cause et action doivent être présents.

## 6. Hors périmètre

- découverte live du schéma Unity Catalog ;
- import complet ODCS ;
- planification de joins multi-modèles (Feature 6) ;
- certification runtime ;
- génération automatique de business metrics contestables ;
- auto-commit ou ouverture automatique de PR.

## 7. Definition of Done spécifique

- projection sans Spark démontrée ;
- ancien format YAML entièrement compatible ;
- draft reproductible byte-for-byte pour une même entrée ;
- curation humaine préservée par tests ;
- CI bloque une métrique cassée avant déploiement ;
- `SemanticEngine` continue de charger uniquement le catalogue au démarrage.
