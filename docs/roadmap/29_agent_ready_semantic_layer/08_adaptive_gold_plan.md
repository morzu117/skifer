# Feature 8 — Adaptive Gold supervisé

> **Priorité :** P3 optimisation guidée par l'usage
> **Dépendances :** Features 4–5, Feature 0, Plan 28
> **Slices / commits :** 6
> **Branche suggérée :** `feat/adaptive-gold`
> **Résultat :** propositions Gold/MV explicables, jamais déployées automatiquement

## 1. Architecture cible

```text
SemanticEvidence / traces ──▶ UsageEventStore ──▶ PatternAggregator
Databricks query history ─optional─┘                    │
                                                       ▼
                                              RecommendationEngine
                                                       │
                                                       ▼
                                              OptimizationProposal
                                              evidence + YAML + estimate
                                                       │
                                            human accept/reject CLI
                                                       │ accept
                                                       ▼
                                            schema proposal + semantic draft
                                                       │ normal review/deploy
                                                       ▼
                                              OutcomeEvaluator
```

Le moteur ne modifie jamais `schemas/`, ne lance jamais `engine.run_*`, ne crée jamais une MV et ne
commite jamais. Il écrit des propositions dans un répertoire dédié ignoré ou explicitement revu.

## 2. Modèles

```python
@dataclass(frozen=True)
class SemanticUsageEvent:
    event_id: str
    occurred_at: datetime
    environment: str
    consumer_class: str
    model_hashes: tuple[str, ...]
    metric_ids: tuple[str, ...]
    dimension_ids: tuple[str, ...]
    normalized_filter_shape: tuple[str, ...]
    query_fingerprint: str
    duration_ms: int | None
    rows_returned: int | None
    bytes_scanned: int | None
    status: str

@dataclass(frozen=True)
class OptimizationProposal:
    proposal_id: str
    kind: Literal["materialized_view", "aggregate_table", "semantic_gap", "deprecation"]
    rule_id: str
    rule_version: str
    evidence_event_ids: tuple[str, ...]
    expected_benefit: dict
    risks: tuple[str, ...]
    generated_schema_path: str | None
    generated_semantic_draft_path: str | None
    source_definition_hashes: tuple[str, ...]
    status: Literal["proposed", "accepted", "rejected", "stale", "measured"]
```

Pas de question brute dans `SemanticUsageEvent`. Le fingerprint utilise des IDs logiques et formes de
filtres, jamais les valeurs sensibles.

## 3. Règles v1 autorisées

1. `frequent_aggregate` : même metrics/dimensions, fréquence et coût au-dessus des seuils → MV.
2. `repeated_join_path` : même path multi-model coûteux → proposition de Gold préparé, seulement si
   grains/cardinalités sûrs.
3. `missing_dimension` : questions rejetées répétitivement sur le même concept connu → semantic gap,
   pas création physique automatique.
4. `unused_generated_asset` : objet proposé par Skifer peu utilisé → recommandation de review de
   dépréciation, jamais drop.

Ne pas ajouter de modèle ML dans v1.

## 4. Fichiers

### À créer

- `src/skifer/adaptive/__init__.py`
- `src/skifer/adaptive/models.py`
- `src/skifer/adaptive/store.py`
- `src/skifer/adaptive/aggregator.py`
- `src/skifer/adaptive/recommender.py`
- `src/skifer/adaptive/generator.py`
- `src/skifer/adaptive/evaluator.py`
- `tests/test_adaptive_store.py`
- `tests/test_adaptive_aggregator.py`
- `tests/test_adaptive_recommender.py`
- `tests/test_adaptive_generator.py`
- `tests/test_adaptive_evaluator.py`

### À modifier

- `src/skifer/semantic/evidence.py` ou tracing hook pour émettre événement
- `src/skifer/cli.py`
- `.gitignore` pour répertoire local de propositions si retenu
- `tests/test_cli.py`
- `docs/agentic.md`, `docs/core.md`, `README.md`, `CHANGELOG.md`

## 5. Slices

### Slice 8.1 — Événement et store

- Convertir SemanticEvidence en usage event allowlisté.
- Fingerprint canonique versionné ; l'ordre non sémantique n'influence pas le hash.
- SQLite local et Delta Databricks ou réutilisation prudente du pattern Feature 2.
- Rétention configurable ; aucune purge dans cette slice si destructive.

### Slice 8.2 — Agrégation de patterns

- Fenêtres `7d/30d` configurables, minimum count, percentile duration si assez de points.
- Partition stricte par environment, consumer class et model definition hash.
- Événements failed séparés des succès.
- Algorithmes Python/SQL déterministes, horloge injectable.

### Slice 8.3 — Recommendation rules

- Registry statique de règles versionnées, pas plugins arbitraires.
- Score explicable composé de seuils, pas opaque.
- Guardrails : sources certifiées, cardinalités sûres, pas de raw SQL/rule Python non compilable,
  coût minimal prouvé.
- Proposal contient toutes les raisons et contre-indications.

### Slice 8.4 — Génération YAML/semantic draft

- Réutiliser structures Plan 28 : `materialization`, `aggregate`, `sql_compiler`.
- Ne pas construire SQL séparément.
- Écrire sous `.skifer_proposals/<proposal_id>/pipeline.yaml` avec `proposal.json`.
- Appeler Feature 0 pour draft sémantique.
- `load_schema` et `compile_select` doivent accepter la proposition avant qu'elle soit montrée.

### Slice 8.5 — Workflow CLI humain

```text
skifer adaptive list
skifer adaptive show PROPOSAL
skifer adaptive diff PROPOSAL
skifer adaptive accept PROPOSAL --output schemas/gold/x.yaml
skifer adaptive reject PROPOSAL --reason "..."
```

- `accept` refuse output existant sans option explicite non destructive ; idéalement patch/diff.
- Revalider source hashes : si dérive, statut `stale`, pas d'accept.
- Aucun git command automatique.

### Slice 8.6 — Outcome evaluation

- Lier asset livré via `proposal_id` metadata/tag.
- Comparer fenêtres avant/après avec minimum data ; ne pas attribuer causalité si données insuffisantes.
- Résultat : improved/regressed/inconclusive + métriques.
- Une régression génère recommandation de review, jamais rollback/drop.

## 6. Tests obligatoires

- fingerprint sans valeur sensible ;
- mélange de versions/environnements interdit ;
- seuil juste dessous/dessus ;
- join path unsafe → aucune proposition physique ;
- proposal YAML passe loader/compiler ;
- stale proposal non acceptée ;
- output existant préservé ;
- avant/après insuffisant = inconclusive ;
- query history Databricks absente → feature fonctionne avec traces internes.

## 7. Hors périmètre

- déploiement automatique ;
- création de cluster/warehouse ;
- optimisation ML ;
- recommandation Z-order/index sans plan dédié ;
- suppression/rollback automatique ;
- analyse de requêtes SQL non issues de Skifer obligatoire.

## 8. Definition of Done

- proposition reproductible depuis mêmes événements/règles ;
- chaque proposition cite preuves et rule version ;
- aucun effet production ;
- YAML et draft valides avant review ;
- accept/reject/stale idempotents ;
- bénéfice post-déploiement mesurable ou explicitement inconclusive.
