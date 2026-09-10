# Feature 5 — Tracing runtime OpenTelemetry / MLflow

> **Priorité :** P2 observabilité transverse
> **Dépendance :** Feature 4.1 pour les IDs de preuve
> **Slices / commits :** 5
> **Branche suggérée :** `feat/runtime-tracing`
> **Résultat :** une requête ou publication est reconstructible par traces/spans sans fuite sensible

## 1. Architecture cible

```text
code métier
   │ utilise interface interne
   ▼
SkiferTracer
   ├── NoOpTracer (défaut/test simple)
   ├── InMemoryTracer (tests)
   └── OTelTracer
          ├── OTLP exporter
          └── MLflow tracing / UC trace location
```

Le cœur n'importe jamais `mlflow` ou `opentelemetry` au niveau module. L'adaptateur est chargé
paresseusement par factory. Le comportement métier ne dépend pas du succès d'export, sauf mode
`tracing_required=true` explicitement configuré.

## 2. Taxonomie de spans

Noms stables, bas niveau de cardinalité :

```text
skifer.pipeline.run
skifer.schema.load
skifer.source.resolve
skifer.transform.execute
skifer.publication.stage
skifer.contract.evaluate
skifer.publication.promote
skifer.semantic.query
skifer.semantic.model_select
skifer.semantic.policy
skifer.semantic.compile
skifer.sql.execute
skifer.llm.complete
skifer.agent.route
skifer.response.serialize
skifer.mcp.resource_read
skifer.mcp.tool_call
```

Attributs communs allowlistés : `skifer.trace_version`, `environment`, `run_id`, `evidence_id`,
`contract_id`, `contract_version`, `model_key`, `metric_names`, `decision`, `status`, durées/counts.
Ne pas mettre `statement_text`, question, filter values ou user email par défaut.

## 3. API interne

```python
class Tracer(Protocol):
    def start_span(self, name: str, *, attributes: Mapping[str, Scalar] | None = None) -> Span: ...
    def current_trace_id(self) -> str | None: ...

class Span(Protocol):
    def set_attribute(self, key: str, value: Scalar) -> None: ...
    def add_event(self, name: str, attributes=None) -> None: ...
    def record_exception(self, exc: Exception) -> None: ...
    def end(self, status: str = "OK") -> None: ...
```

Fournir un context manager pour garantir `end()` en exception. Utiliser `contextvars` pour isolation
async/thread ; ne pas stocker le span courant dans une globale mutable.

## 4. Fichiers

### À créer

- `src/skifer/observability/tracing.py`
- `src/skifer/observability/tracing_exporters.py`
- `tests/test_tracing.py`
- `tests/test_tracing_integration.py`

### À modifier

- `src/skifer/core/core.py`, `core/patterns.py`, `core/interpreter.py`
- `src/skifer/observability/monitor.py`, `observability/publication.py`
- `src/skifer/semantic/semantic.py`
- `src/skifer/agentic/agent.py`, `agentic/hub.py`
- `src/skifer/semantic/llm_provider.py`
- `src/skifer/serving/chat_model.py`
- `src/skifer/core/config.py`
- `pyproject.toml` uniquement pour extra optionnel, jamais version package
- tests des modules instrumentés
- `docs/observability.md`, `docs/agentic.md`, `CHANGELOG.md`

## 5. Slices

### Slice 5.1 — Interface no-op/in-memory et propagation

- Implémenter IDs W3C compatibles ou laisser OTel les générer ; format stable exposé.
- `InMemoryTracer` capture ordre, parent, status, events et attributs pour tests.
- Context manager marque ERROR et record_exception avant re-raise.
- `TraceContext` transporte trace/run/session/evidence IDs.

Tests : nesting, exception, deux threads/contextes, span oublié, invalid attribute type, no-op.

### Slice 5.2 — Pipeline/certification

- Instrumenter seulement les frontières, pas chaque opération colonne.
- Propager run ID vers publication/certification.
- Counts exacts seulement s'ils existent déjà ; ne pas lancer un `count()` Spark pour tracer.
- Export failure best-effort loggé une fois/rate-limité.

### Slice 5.3 — Semantic/agentic/LLM

- Root span par `hub.ask` ou `semantic.query_with_evidence` direct.
- LLM span : provider/model, latency, tokens si retournés ; prompts désactivés par défaut.
- SQL span : sql hash, warehouse/compute type, statement ID, pas SQL text.
- `SemanticEvidence.trace_id` doit correspondre à la racine active.

### Slice 5.4 — OTel/MLflow exporters

- Factory config : `none`, `otlp`, `mlflow`, `dual`.
- Imports paresseux avec erreur d'installation actionnable.
- MLflow UC trace location configurable ; ne pas la créer implicitement en prod sans permission.
- Paramètres endpoint lus de config/env dédiés ; aucun token loggé.

Tests avec modules monkeypatchés, aucun réseau.

### Slice 5.5 — Redaction, rétention et conformité

- `TraceAttributePolicy` allowlist + pseudonymisation HMAC avec secret externe optionnel ; si secret
  absent, omettre user ID plutôt que hash faible réversible.
- Limites taille/nombre d'attributs et événements.
- Tests canary avec token, email, valeur PII, prompt, SQL littéral.
- Guide UC permissions, retention et dual export.

## 6. Configuration proposée

```yaml
observability:
  tracing:
    exporter: none        # none | otlp | mlflow | dual
    required: false
    capture_prompts: false
    capture_sql: false
    user_identity: omit   # omit | hmac
    trace_location: null
```

Ne pas réutiliser `observability:` pipeline pour la config globale si cela crée une ambiguïté ; la
décision finale peut placer ce bloc dans `config.yaml`. Le loader concerné doit être explicite.

## 7. Hors périmètre

- dashboards d'observabilité ;
- stockage de chaîne de pensée ;
- sampling adaptatif complexe ;
- garantie réglementaire ;
- instrumentation automatique de bibliothèques non utilisées.

## 8. Definition of Done

- trace end-to-end hub → SQL en test ;
- pipeline publication/quarantine tracé ;
- export désactivé = overhead et comportement minimaux ;
- export failure ne masque jamais une erreur métier ;
- canary secrets/PII vert ;
- aucune dépendance obligatoire ajoutée à l'installation core.

## 9. Suite identifiée — identité de run de bout en bout

> **Fait** (commit dédié, après le merge de la Feature 5). Repéré pendant la
> slice 5.2 et volontairement laissé de côté à ce moment-là : ce n'était pas un
> effet de bord d'une slice d'instrumentation.

Le `run_id` frappé par `run_process_to_table` / `run_from_yaml` descend désormais
jusqu'au registre de certification : un run de pipeline = une identité d'audit.
Il est frappé **sur le chemin métier** et les tests le figent identique tracing
allumé ou éteint. `SkiferEngine.run_from_yaml` a perdu au passage sa seconde
implémentation réservée au tracing — les deux avaient déjà divergé.

**Changement de donnée persistée** : les `run_id` écrits par les versions
antérieures ne suivent pas cette convention ; tout outil externe qui les corrèle
doit être vérifié.

### Constat d'origine

Le pipeline et la publication avaient chacun leur `run_id` :
`SkiferEngine.run_process_to_table` en génère un pour la trace, et
`start_publication_run` en génère un autre, seul persisté dans le registre de
certification. Les deux ne sont reliés que par le `trace_id` partagé.

Pour un audit, une **identité unique de bout en bout** serait plus forte : le
`run_id` du pipeline descendrait jusqu'à l'enregistrement de certification, de
sorte que le lien pipeline → publication survive même sans traces exportées.

Ce que cela implique, et pourquoi ça mérite sa propre décision :

- le `run_id` doit être généré sur le **chemin métier**, pas seulement quand le
  tracing est actif — sinon l'identité dépendrait de l'observabilité, ce que la
  slice 5.2 a justement corrigé ;
- `PipelinePatterns` doit le transmettre à `PublicationCoordinator.publish()` ;
- cela **change la donnée persistée** dans le registre de certification, y
  compris pour les déploiements existants : les `run_id` déjà écrits ne suivent
  pas cette convention, et tout outil externe qui les corrèle doit être vérifié.
