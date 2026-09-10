# Feature 4 — Réponses sémantiques avec preuves

> **Priorité :** P2 explicabilité
> **Dépendances :** Features 2, 3 et 6
> **Slices / commits :** 4
> **Branche suggérée :** `feat/semantic-evidence`
> **Résultat :** chaque réponse est reliée à ses définitions, sources et certifications

## 1. Architecture cible

```text
SemanticQuery
   │
   ├── DependencyPlan + PolicyEvaluation
   ├── ResolvedQuery + definition hashes
   ├── Certification snapshots
   ├── Static lineage subset
   └── Execution metadata
                │
                ▼
        SemanticEvidence (sans Spark)
                │
       ┌────────┴────────┐
       ▼                 ▼
SemanticResult       serving serializer
(df + evidence)      (human + machine)
```

`SemanticEngine.query()` reste compatible et retourne un DataFrame. La nouvelle API
`query_with_evidence()` retourne un conteneur. Ne pas changer le type de retour existant.

## 2. Modèle cible

```python
@dataclass(frozen=True)
class SourceEvidence:
    dataset: str
    contract_id: str | None
    contract_version: str | None
    definition_hash: str | None
    certification_status: str
    certified_at: datetime | None
    load_age_seconds: float | None
    data_age_seconds: float | None
    certification_run_id: str | None

@dataclass(frozen=True)
class MetricEvidence:
    name: str
    model_key: str
    definition_hash: str
    source_columns: tuple[str, ...]

@dataclass(frozen=True)
class SemanticEvidence:
    schema_version: str
    evidence_id: str
    trace_id: str | None
    model_keys: tuple[str, ...]
    metrics: tuple[MetricEvidence, ...]
    dimensions: tuple[str, ...]
    normalized_filters: tuple[dict, ...]
    sources: tuple[SourceEvidence, ...]
    policy: PolicyEvaluation
    sql_hash: str
    sql_text: str | None
    statement_id: str | None
    compiled_at: datetime
    executed_at: datetime | None

@dataclass
class SemanticResult:
    dataframe: Any
    evidence: SemanticEvidence
```

La preuve est JSON sérialisable via une méthode explicite ; ne pas utiliser `dataclasses.asdict`
aveuglément si cela expose de nouveaux champs sensibles à l'avenir.

## 3. Règles de sécurité et déterminisme

- `evidence_id` dérive d'un UUID de requête, pas du texte de question.
- Le SQL brut est `None` par défaut ; `sql_hash` est toujours présent après compilation.
- Les valeurs de filtres sont masquées ou incluses selon policy ; la structure/opérateur reste.
- Aucune question brute, prompt complet, token, sample de données ou chaîne de pensée.
- Les certifications sont des snapshots : une future évolution du registre ne modifie pas la preuve.
- Les datetimes sont UTC ISO-8601 dans la serialization.
- Les collections ont un ordre stable.

## 4. Fichiers

### À créer

- `src/skifer/semantic/evidence.py`
- `tests/test_semantic_evidence.py`

### À modifier

- `src/skifer/semantic/semantic.py`
- `src/skifer/agentic/resolver.py`
- `src/skifer/lineage/tracker.py` ou façade de sélection de sous-graphe
- `src/skifer/agentic/models.py`
- `src/skifer/agentic/agent.py`
- `src/skifer/serving/_response_serializer.py`
- `src/skifer/serving/chat_model.py`
- `src/skifer/semantic/__init__.py`
- `tests/test_resolver.py`, `tests/test_lineage_tracker.py`
- `tests/test_agent.py`, `tests/test_serving_response_serializer.py`,
  `tests/test_serving_chat_model.py`
- `docs/agentic.md`, `docs/api_reference.md`, `CHANGELOG.md`

## 5. Slices

### Slice 4.1 — Dataclasses, serialization et API additive

- Créer les modèles avec `schema_version="1"`.
- Serializer allowlisté `to_dict(include_sql=False, include_filter_values=False)`.
- Ajouter `SemanticEngine.query_with_evidence()` mais construire d'abord une preuve minimale.
- `query()` peut appeler la nouvelle implémentation et retourner `.dataframe`, ou garder son chemin ;
  choisir une seule source d'exécution pour éviter deux comportements.

Tests : JSON round-trip, UTC, ordre stable, query historique retourne toujours DF, échec avant SQL.

### Slice 4.2 — Preuve de compilation et lineage

- Étendre `ResolvedQuery` avec plan logique : sources, model definitions, selected expressions, pas
  seulement `full_sql`.
- Hasher SQL normalisé avec algorithme/version documentés.
- Extraire uniquement le sous-graphe nécessaire aux métriques sélectionnées.
- Redaction des filter values et SQL selon `EvidencePolicy` pure.

Tests : même query = même hash malgré whitespace ; formule change = hash change ; deux métriques ;
join multi-model ; source sans lineage best-effort explicitement marquée.

### Slice 4.3 — Preuve d'exécution et certification

- Snapshot des certifications évaluées au preflight/recheck ; conserver la dernière décision utilisée.
- Backend peut retourner `ExecutionResult(dataframe, statement_id=None, metadata={})` de manière
  additive, ou fournir un hook ; ne pas casser tous les appels `execute_sql` inutilement.
- Capturer timings via horloge monotone et timestamps UTC.
- En exception, attacher une preuve partielle sûre à `SemanticExecutionError`.

Tests : statement ID absent, résultat vide, SQL error, certification expirée au recheck, WARN.

### Slice 4.4 — Hub/serving et formats

- Ajouter `evidence` optionnelle aux réponses agentic existantes.
- Texte humain : source + certification + freshness en une ligne, pas un dump complet.
- Réponse OpenAI-compatible : conserver `choices`; ajouter un bloc top-level `skifer` versionné
  si MLflow ChatModel l'accepte, sinon inclure un JSON metadata dans la structure supportée. Vérifier
  la compatibilité réelle avant décision.
- Tests snapshot serializers et client ancien ignorant metadata.

## 6. Erreurs obligatoires

- preuve partielle marquée `execution_status=failed`, jamais présentée comme complète ;
- certification snapshot manquant sous ALLOW impossible ;
- serializer refuse un type non allowlisté ;
- demande de SQL brut refusée si policy ne l'autorise pas.

## 7. Hors périmètre

- stockage des traces (Feature 5) ;
- signature cryptographique de preuves ;
- preuve réglementaire juridiquement suffisante ;
- stockage de résultats de données dans la preuve ;
- génération d'explication libre par LLM comme source de vérité.

## 8. Definition of Done

- API historique inchangée ;
- preuve autonome et sérialisable après fin de session Spark ;
- formule/source/certification reconstructibles ;
- filtres/SQL/secrets masqués selon tests ;
- erreur d'exécution conserve une preuve partielle exploitable.
