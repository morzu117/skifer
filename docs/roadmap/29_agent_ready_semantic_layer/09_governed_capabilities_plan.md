# Feature 9 — Capability model et write-back gouverné

> **Priorité :** P4 — dernière étape, ne pas anticiper
> **Dépendances :** Features 5 et 7 ; certification/evidence recommandées
> **Slices / commits :** 8
> **Branche suggérée :** `feat/governed-capabilities`
> **Résultat :** une première capacité métier réversible fonctionne en shadow/supervised

## 1. Avertissement de périmètre

Cette feature étend Skifer au-delà de son cœur data/semantic. Ne pas la commencer tant que
les features de tracing et de service MCP ne sont pas fusionnées et éprouvées. La v1 ne livre ni
full autonomy, ni capacité financière, ni wrapper REST générique.

## 2. Architecture cible

```text
capability catalog YAML (lazy)
          │
          ▼
CapabilityRegistry ──▶ CapabilitySelector (IDs/names only)
          │                        │
          │                        ▼
          │                 CapabilityRequest
          ▼                        │
live state readers ──▶ PreconditionsEvaluator
                                   │ deny/unknown
                                   ├────────▶ supervised/escalate
                                   ▼ allow
                           AutonomyStateMachine
                                   │ shadow/supervised/guarded
                                   ▼
                            CredentialProvider JIT
                                   │
                                   ▼
                       IdempotentCapabilityExecutor
                                   │
                          audit/evidence/trace
```

Le LLM sélectionne un `capability_id` et remplit un input schema. Il ne décide jamais de la policy,
des préconditions, de l'approbation, des credentials ni de l'idempotence.

## 3. Format cible

```yaml
capabilities:
  - id: support.create_ticket
    version: 1.0.0
    owner: support-platform
    description: Create a support ticket after deterministic duplicate checks
    mode: write
    executor: support_ticket_v1
    acting_as: delegated_user
    required_scopes: [tickets:create]
    input_schema:
      type: object
      additionalProperties: false
      required: [request_id, category, description]
      properties:
        request_id: {type: string, maxLength: 128}
        category: {type: string, enum: [payment, network, access]}
        description: {type: string, maxLength: 2000}
    preconditions:
      - rule: service_is_known
      - rule: no_open_duplicate
    reversibility: compensatable
    compensation: support.close_ticket
    approval: supervised
    idempotency_key: request_id
    provenance:
      policy_uri: policies/support-ticket-v3.md
      policy_hash: abc123
```

Valeurs de réversibilité : `reversible`, `compensatable`, `irreversible`. Une action irréversible ne
peut pas dépasser supervised dans v1.

## 4. Modèles runtime

```python
class PreconditionDecision(str, Enum):
    ALLOW = "ALLOW"
    DENY = "DENY"
    UNKNOWN = "UNKNOWN"

class AutonomyMode(str, Enum):
    SHADOW = "SHADOW"
    SUPERVISED = "SUPERVISED"
    GUARDED = "GUARDED"

class ExecutionState(str, Enum):
    PROPOSED = "PROPOSED"
    PRECONDITIONS_PASSED = "PRECONDITIONS_PASSED"
    PENDING_APPROVAL = "PENDING_APPROVAL"
    APPROVED = "APPROVED"
    EXECUTING = "EXECUTING"
    EXECUTED = "EXECUTED"
    FAILED = "FAILED"
    COMPENSATING = "COMPENSATING"
    COMPENSATED = "COMPENSATED"
```

Transitions explicites et testées. Aucun setter arbitraire d'état.

## 5. Fichiers

### À créer

- `src/skifer/capabilities/__init__.py`
- `src/skifer/capabilities/models.py`
- `src/skifer/capabilities/validator.py`
- `src/skifer/capabilities/registry.py`
- `src/skifer/capabilities/preconditions.py`
- `src/skifer/capabilities/autonomy.py`
- `src/skifer/capabilities/credentials.py`
- `src/skifer/capabilities/executor.py`
- `src/skifer/capabilities/history.py`
- `src/skifer/mcp/capability_tools.py`
- `tests/test_capability_validator.py`
- `tests/test_capability_registry.py`
- `tests/test_capability_preconditions.py`
- `tests/test_capability_autonomy.py`
- `tests/test_capability_credentials.py`
- `tests/test_capability_executor.py`
- `tests/test_mcp_capabilities.py`

### À modifier

- `src/skifer/agentic/hub.py`, `agentic/models.py`
- `src/skifer/observability/tracing.py`
- `src/skifer/core/config.py`
- `pyproject.toml` seulement si pilote requiert extra optionnel
- `docs/agentic.md`, `README.md`, `CHANGELOG.md`

## 6. Slices

### Slice 9.1 — Schema/validator/catalog lazy

- Valider IDs/version/owner/description, JSON Schema fermé, scopes, rules, reversibility, approval,
  idempotency et compensation.
- `mode=write` exige idempotency key.
- `compensatable` exige compensation déclarée ; `irreversible` exige supervised.
- Catalog summary au startup, définition complète lazy.

### Slice 9.2 — Registry et première capacité live-read

- Decorator/registry d'executors explicites importés avant run, analogue RuleRegistry mais types
  capability séparés.
- Pas d'import depuis string arbitraire du YAML.
- Livrer une fausse capacité read-only en tests pour valider sélection/input/result/evidence.
- Le Hub route vers capability uniquement sur intent explicite/structuré.

### Slice 9.3 — Preconditions déterministes

- Registry de rules pures/async contrôlées ; chaque rule retourne décision + reason code + observed
  state hash/timestamp.
- Évaluer contre état live, puis recheck juste avant exécution.
- `UNKNOWN` ne devient jamais ALLOW ; passe à supervised/escalation.
- Retrieved text/LLM output interdit comme return direct d'une rule.

### Slice 9.4 — Autonomy state machine

- Config par capability/environnement : shadow ou supervised défaut.
- Shadow enregistre proposition et résultat simulé si executor supporte dry-run, sans credential write.
- Approval record : actor, timestamp, request hash, expiration, approve/deny reason.
- Toute modification input après approval invalide approval.

### Slice 9.5 — Credentials délégués/JIT

```python
class CredentialProvider(Protocol):
    def issue(self, subject, audience, scopes, ttl_seconds) -> CredentialLease: ...
```

- Le provider est injecté ; Skifer ne devient pas authorization server.
- Lease utilisée en mémoire et jamais sérialisée/loggée.
- Vérifier audience/resource, subject, scopes, expiry et TTL plafond.
- Aucun token persistant dans YAML/config/historique.

### Slice 9.6 — Executor idempotent et compensation

- Request hash couvre capability version + normalized input + subject.
- History store écrit state events append-only avant/après appel externe.
- Sur retry après réponse perdue, consulter idempotency record/provider au lieu de rappeler aveuglément.
- Compensation est une nouvelle action auditée, pas un rollback magique.

### Slice 9.7 — MCP Tools gouvernés

- Lister uniquement capabilities autorisées par scope/autonomy mode.
- Un tool par capacité ou façade paramétrée : privilégier peu de capacités bien décrites ; décider
  après test de sélection, jamais wrapper tous endpoints.
- Appel renvoie proposed/pending/executed avec evidence/trace ; approval séparée et sécurisée.
- Les annotations MCP ne sont pas une frontière de sécurité.

### Slice 9.8 — Harness d'évaluation et pilote

- Replay store de fake live states, fake credentials et fake external system.
- Dataset : allow, deny, unknown, stale approval, state changed, duplicate request, timeout, lost
  response, compensation failure, prompt-injected description.
- Mesures : precision décisions, taux escalation, duplicate side effects = 0.
- Pilote recommandé : ticket support compensatable, uniquement shadow puis supervised.

## 7. Transitions interdites

- `PROPOSED → EXECUTING` sans preconditions ;
- `PENDING_APPROVAL → EXECUTING` sans approval valide ;
- `FAILED → EXECUTED` sans nouvel événement/reconciliation ;
- `EXECUTED → COMPENSATED` sans état `COMPENSATING` et compensation déclarée ;
- toute transition vers guarded pour `irreversible` ;
- re-use d'un approval si request hash ou state precondition hash change.

## 8. Sécurité obligatoire

- allowlist executor/rules ;
- input JSON Schema fermé et tailles bornées ;
- no secrets in trace/history/evidence ;
- least scopes et TTL plafond ;
- SSRF/URL arbitraire impossible depuis input si adapter HTTP pilote ;
- output externe traité non fiable et borné ;
- policy provenance hashée, changement → review/approval invalidés.

## 9. Hors périmètre

- full autonomy ;
- actions financières/irréversibles autonomes ;
- génération d'executor depuis OpenAPI ;
- API-to-MCP automatique ;
- authorization server ;
- interprétation runtime d'un document de policy pour autoriser ;
- orchestration multi-capabilities complexe/saga générique.

## 10. Definition of Done

- pilote shadow puis supervised sans side effect double ;
- toutes transitions et interdictions testées ;
- UNKNOWN/escalation testé ;
- credential jamais observable dans logs/traces/history ;
- recheck live state avant write ;
- action irréversible impossible en guarded ;
- MCP et appel Python utilisent le même service/executor sécurisé.
