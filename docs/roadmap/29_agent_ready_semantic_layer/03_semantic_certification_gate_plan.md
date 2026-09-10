# Feature 3 — Garde de certification dans SemanticEngine

> **Priorité :** P1 frontière agent-ready
> **Dépendances :** Features 1 et 2
> **Slices / commits :** 4
> **Branche suggérée :** `feat/semantic-certification-gate`
> **Résultat :** une query agent ne touche pas une source interdite ou non certifiée

## 1. Architecture cible

```text
SemanticQuery + ConsumerContext
              │
              ▼
        DependencyResolver
              │ datasets/contracts
              ▼
       CertificationPolicy
              │ ALLOW/WARN/DENY/REQUIRE_HUMAN
              ├── DENY ──▶ erreur structurée, zéro SQL
              ▼
        QueryResolver.compile
              │
              ▼
       policy recheck immédiat
              │
              ▼
          execute_sql
```

La double vérification protège contre une certification expirant entre le début de la requête et
l'exécution. Aucun prompt ne décide de cette policy.

## 2. Modèles et configuration

```python
@dataclass(frozen=True)
class ConsumerContext:
    consumer_id: str
    consumer_class: Literal["dashboard", "agent_read", "agent_action"]
    user_id: str | None
    scopes: frozenset[str]
    trace_id: str | None

class CertificationDecision(str, Enum):
    ALLOW = "ALLOW"
    WARN = "WARN"
    DENY = "DENY"
    REQUIRE_HUMAN = "REQUIRE_HUMAN"

@dataclass(frozen=True)
class PolicyEvaluation:
    decision: CertificationDecision
    reasons: tuple[PolicyReason, ...]
    evaluated_at: datetime
```

Configuration proposée :

```yaml
environments:
  prod:
    semantic_certification_policy: enforce
    semantic_consumer_class: agent_read
```

Valeurs : `off`, `warn`, `enforce`, `supervised`. Clé invalide = erreur au démarrage.
Ces clés vivent à la racine du bloc environnement, comme `allow_raw_sql`, et non
sous `params:` : `params` est réservé à l'injection `{{ key }}` dans les schémas
YAML de pipeline, donc y placer une clé de gouvernance polluerait cet espace de noms.

## 3. Fichiers

### À créer

- `src/skifer/semantic/access_policy.py`
- `src/skifer/semantic/dependencies.py`
- `tests/test_semantic_access_policy.py`
- `tests/test_semantic_certification_gate.py`

### À modifier

- `src/skifer/semantic/semantic.py`
- `src/skifer/agentic/resolver.py` si `ResolvedQuery` doit exposer ses sources
- `src/skifer/core/config.py`
- `src/skifer/agentic/agent.py`
- `src/skifer/serving/chat_model.py`
- `tests/test_semantic_engine_catalog.py`, `tests/test_resolver.py`
- `tests/test_agent.py`, `tests/test_serving_chat_model.py`, `tests/test_config.py`
- `docs/agentic.md`, `docs/observability.md`, `CHANGELOG.md`

## 4. Slices

### Slice 3.1 — Policy pure et configuration

- Implémenter une fonction pure `evaluate(certifications, context, mode, now)`.
- Raisons codées : `MISSING`, `EXPIRED`, `FAILED_CHECK`, `HASH_MISMATCH`, `UNAUTHORIZED`,
  `STORE_UNAVAILABLE`, `OVERRIDDEN`.
- En prod agentique, store indisponible sous `enforce` = DENY.
- Sous `off`, ne pas appeler le store pour préserver compatibilité/performance.

### Slice 3.2 — Résolution de dépendances

- Modèle table unique : `model["table"]`.
- Modèle multi-source Feature 6 : toutes les sources du plan de join.
- Matérialized view : dépendance physique finale + contract attendu, pas toutes les sources si la MV a
  sa propre certification.
- Retour stable, dédupliqué, avec contract/hash attendus.

### Slice 3.3 — Gate query/create_view

- Ajouter API additive acceptant `consumer_context`; conserver appels existants sous mode config.
- Évaluer avant compilation ; recheck avant `execute_sql`.
- `create_view` stocke dans commentaire/properties non sensibles l'identité de définition, pas user.
- Spy tests : DENY implique zéro `resolve` si dépendances suffisent et toujours zéro backend SQL.

### Slice 3.4 — Overrides, Hub et migration

- Override objet signé/loggé conceptuellement : reason non vide, expiration courte, actor et trace.
- Ne pas accepter `force=True` booléen sans contexte.
- Hub transforme PolicyEvaluation en réponse actionnable.
- Guide `off → warn → enforce`, métriques de warnings avant activation stricte.

## 5. Matrice minimale

| Mode | Certified | Warning check | Expired/Missing | Store error |
|---|---|---|---|---|
| off | ALLOW | ALLOW | ALLOW | ALLOW sans appel |
| warn | ALLOW | WARN | WARN | WARN |
| enforce | ALLOW | policy | DENY | DENY |
| supervised | ALLOW | REQUIRE_HUMAN | REQUIRE_HUMAN | REQUIRE_HUMAN |

La décision `warning check` dépend de la sévérité définie par contrat ; écrire les cas dans tests.

## 6. Erreurs obligatoires

`SemanticAccessDenied` est structurée : `model_key`, datasets, decision, reasons, evaluated_at,
recommended_action. Son `str()` est sûr et ne contient ni token ni valeurs de données.

## 7. Hors périmètre

- permissions UC/ABAC automatiques ;
- auth MCP (Feature 7) ;
- write actions ;
- heuristique LLM de confiance ;
- retry silencieux sur une autre table non déclarée.

## 8. Definition of Done

- matrice complète testée avec horloge injectée ;
- DENY = aucun SQL exécuté ;
- recheck race testé ;
- override attribuable, borné et tracé ;
- catalog-first/lazy inchangé.
