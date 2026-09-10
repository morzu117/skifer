# Feature 6 — Entités, relations et jointures sémantiques

> **Priorité :** P0 après projection sémantique
> **Dépendance :** Feature 0
> **Slices / commits :** 6
> **Branche suggérée :** `feat/semantic-domain-graph`
> **Résultat :** modèles liés et SQL multi-table déterministe sans join LLM

## 1. Problème

Le modèle actuel définit une table, des dimensions et des métriques. Il ne peut pas exprimer qu'un
order appartient à un customer ni compiler une question couvrant plusieurs modèles. Les pipelines
contiennent déjà des joins, mais un join physique ne suffit pas à prouver son sens, son grain ou sa
cardinalité.

## 2. Architecture cible

```text
pipeline joins + contracts ──▶ RelationshipCandidates ──▶ curation
semantic YAMLs lazy ─────────────────────────────────────▶ DomainGraph
                                                               │
SemanticQuery (names only) ──▶ SemanticPlanner ────────────────┤
                                                               ▼
                                                       SemanticPlan
                                                  sources + join path
                                                               │
                                                               ▼
                                                       QueryResolver SQL
```

Le `DomainGraph` est construit pour les modèles nécessaires à une question, pas pour tout le
catalogue au démarrage. Les catalog entries portent seulement un résumé d'entités/relations pour
identifier les candidats ; les YAML complets restent lazy.

## 3. Format cible indicatif

```yaml
models:
  - key: orders
    table: gold.orders
    grain: [order]
    entities:
      - name: order
        type: primary
        key: order_id
      - name: customer
        type: foreign
        key: customer_id
    relationships:
      - name: orders_customer
        from_entity: customer
        to_model: customers
        to_entity: customer
        cardinality: many_to_one
        join_type: left
    dimensions: [...]
    metrics: [...]
```

Décisions :

- relation dirigée pour planification, navigable dans les deux sens si explicitement sûr ;
- cardinalités : `one_to_one`, `many_to_one`, `one_to_many`, `many_to_many`, `unknown` ;
- `many_to_many` et `unknown` interdits par défaut pour une metric query ;
- join types sémantiques v1 : `inner`, `left` seulement ;
- cross/anti/semi ne deviennent pas relations de domaine automatiques ;
- IDs logiques stables, noms physiques séparés.

## 4. Nouveaux types

```python
@dataclass(frozen=True)
class EntityDef:
    name: str
    model_key: str
    key_columns: tuple[str, ...]
    role: Literal["primary", "foreign", "unique"]

@dataclass(frozen=True)
class RelationshipDef:
    name: str
    from_entity: EntityRef
    to_entity: EntityRef
    cardinality: str
    join_type: str
    verified_by_contract: bool

@dataclass(frozen=True)
class SemanticPlan:
    root_model: str
    required_models: tuple[str, ...]
    joins: tuple[PlannedJoin, ...]
    metrics: tuple[MetricRef, ...]
    dimensions: tuple[DimensionRef, ...]
    grain: tuple[str, ...]
```

## 5. Fichiers

### À créer

- `src/skifer/semantic/domain.py`
- `src/skifer/semantic/domain_graph.py`
- `src/skifer/semantic/planner.py`
- `tests/test_semantic_domain.py`
- `tests/test_semantic_planner.py`

### À modifier

- `src/skifer/semantic/validator.py`
- `src/skifer/semantic/semantic.py`
- `src/skifer/semantic/builder.py`, `draft_builder.py`
- `src/skifer/agentic/resolver.py`
- `src/skifer/agentic/agent.py`
- `src/skifer/lineage/tracker.py`, `lineage/dictionary.py`
- `tests/test_validator.py`, `tests/test_semantic_engine_catalog.py`
- `tests/test_resolver.py`, `tests/test_agent.py`, `tests/test_lineage_tracker.py`
- `docs/yaml_spec.md`, `docs/agentic.md`, `CHANGELOG.md`

## 6. Slices

### Slice 6.1 — Spécification, validator et catalog summaries

- Parser grain, entities, relationships.
- Valider modèle/clé existante, composite key, noms/IDs, self relation, cardinalité/join type.
- Catalogue stocke noms d'entités + related model keys, pas définitions SQL complètes.
- Tests prouvent qu'au startup aucun model YAML complet n'est ouvert.

### Slice 6.2 — Candidats depuis pipeline

- Feature 0 projette entity annotations et join keys.
- `UniqueCheck`/grain peut justifier le côté `one`; sinon cardinalité `unknown`.
- Générer candidats dans draft avec `status: proposed` ou rapport séparé ; ils ne sont pas queryables
  avant curation/promotion.
- Joins composites conservés dans l'ordre.

### Slice 6.3 — DomainGraph lazy

- Charger root model, puis uniquement les voisins nécessaires avec limite/cycle guard.
- API : neighbors, find_paths, get_entity, related_models.
- Retourner tous les chemins minimaux pour que planner détecte ambiguïté ; ne pas choisir par ordre de
  dictionnaire.
- Cache invalidé par model definition hash/catalog reload.

### Slice 6.4 — SemanticPlanner et QueryResolver multi-model

- Étendre les refs en restant compatible avec noms non qualifiés du modèle root.
- Résoudre metric owners/dimension owners, construire path.
- Zéro chemin ou plusieurs chemins de même priorité = `SemanticQueryError` actionnable.
- SQL avec alias déterministes et colonnes qualifiées ; réutiliser quoting/literal validation.
- Le LLM produit toujours `SemanticQuery` de noms, jamais join conditions.

### Slice 6.5 — Grain, compatibilité et calendrier

- Déclarer additive behavior des métriques : `additive`, `semi_additive`, `non_additive` et dimensions
  temporelles autorisées si nécessaire.
- Détecter fanout selon cardinalité et grain ; refuser avant SQL.
- Calendrier versionné : mapping de période déterministe, idéalement modèle/table déclarée plutôt que
  dates codées en Python.
- Q3 fiscal doit être résolu par définition, pas par LLM.

### Slice 6.6 — Adversarial tests et E2E

Dataset fixture : orders/customers/products/calendar avec chemins alternatifs.

Questions/tests : métrique inconnue, relation absente, chemin ambigu, many-to-many, fanout,
dimension incompatible, composite join, fiscal quarter, injection dans value, modèle lazy non lié
jamais chargé. Comparer SQL/rows Spark local pour cas nominaux.

## 7. Erreurs obligatoires

- `No declared relationship connects metric 'revenue' to dimension 'segment'.`
- `Ambiguous semantic join path: orders→customers→regions or orders→stores→regions.`
- `Unsafe fanout: metric grain 'order' crosses one_to_many relationship 'order_lines'.`
- `Relationship cardinality is unknown; declare or certify uniqueness before querying.`

## 8. Hors périmètre

- RDF, OWL, SHACL ou base graphe ;
- parcours récursif de profondeur non bornée ;
- knowledge graph de documents ;
- relation inventée depuis similarité de noms ;
- optimisation cost-based Spark ;
- join many-to-many automatique.

## 9. Definition of Done

- deux modèles reliés répondent via SQL déterministe ;
- ambiguity/fanout bloquent avant backend ;
- pipeline propose des relations sans les auto-approuver ;
- ancien modèle single-table produit le même SQL qu'avant ;
- lazy loading prouvé par tests d'accès fichiers/cache.
