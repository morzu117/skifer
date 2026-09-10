# Feature 7 — Exposition MCP read-only

> **Priorité :** P3 accès standardisé
> **Dépendances :** Features 3, 4, 5 et 6
> **Slices / commits :** 5
> **Branche suggérée :** `feat/mcp-readonly`
> **Résultat :** un agent externe découvre et interroge uniquement la couche gouvernée

## 1. Architecture cible

```text
MCP transport (stdio/http)
          │ auth/request context
          ▼
MCP adapters (Resources + read-only Tool)
          │ aucun accès direct aux moteurs
          ▼
AgentReadyDataService
  ├── semantic catalog/domain
  ├── contracts/certifications
  ├── lineage/dictionary
  └── query_with_evidence
          │
          ▼
policies + tracing + SparkBackend
```

Le service applicatif est la frontière de sécurité. Les handlers MCP sont minces et ne contournent
jamais certification, scopes, limite de résultats ou tracing.

## 2. Surface MCP v1

### Resources/templates

| URI | Contenu | Scope |
|---|---|---|
| `skifer://semantic/catalog` | résumés des modèles visibles | `models:read` |
| `skifer://semantic/models/{key}` | modèle gouverné sans SQL sensible | `models:read` |
| `skifer://contracts/{id}/{version}` | contrat publié | `contracts:read` |
| `skifer://certification/{dataset}` | statut courant + freshness | `contracts:read` |
| `skifer://lineage/{dataset}/{column}` | provenance statique | `lineage:read` |

### Tool read-only

```text
query_semantic_model(SemanticQueryInput) -> SemanticQueryOutput
```

Le Tool accepte uniquement model/metric/dimension/filter/date/limit structurés. Aucun champ SQL,
table FQN arbitraire, file path ou Python expression.

## 3. API applicative

```python
@dataclass(frozen=True)
class RequestContext:
    subject: str
    scopes: frozenset[str]
    consumer_class: str
    trace_context: TraceContext

class AgentReadyDataService:
    def list_models(self, ctx, cursor=None, limit=50) -> Page[ModelSummary]: ...
    def get_model(self, ctx, key) -> GovernedModelView: ...
    def get_contract(self, ctx, contract_id, version) -> ContractView: ...
    def get_certification(self, ctx, dataset) -> CertificationView: ...
    def get_lineage(self, ctx, dataset, column) -> LineageView: ...
    def query(self, ctx, query: SemanticQuery, limit=100) -> QueryEnvelope: ...
```

Les vues sont DTO sérialisables allowlistés, jamais les dicts YAML bruts.

## 4. Fichiers

### À créer

- `src/skifer/agentic/data_service.py`
- `src/skifer/mcp/__init__.py`
- `src/skifer/mcp/server.py`
- `src/skifer/mcp/resources.py`
- `src/skifer/mcp/tools.py`
- `src/skifer/mcp/auth.py`
- `tests/test_agent_ready_data_service.py`
- `tests/test_mcp_resources.py`
- `tests/test_mcp_tools.py`
- `tests/test_mcp_auth.py`

### À modifier

- `src/skifer/cli.py`
- `src/skifer/semantic/semantic.py` si pagination/view API manque
- `src/skifer/core/config.py`
- `pyproject.toml` extra `[mcp]` et script si nécessaire, sans bump version
- `tests/test_cli.py`, `tests/test_config.py`
- `docs/agentic.md`, `docs/getting_started.md`, `README.md`, `CHANGELOG.md`

## 5. Slices

### Slice 7.1 — `AgentReadyDataService`

- Implémenter scope check central : `require_scope(ctx, scope)` fail-closed.
- Pagination cursor opaque signé/hashé ou index stable ; limiter `1..100`.
- `query` applique hard row limit, certification gate et evidence.
- Aucune dépendance MCP dans ce module.

Tests : chaque méthode sans scope, ressource cachée, pagination, limite, dataset non certifié, subject
différent.

### Slice 7.2 — Resources/templates MCP

- Choisir SDK MCP officiel compatible Python/package au moment du build ; pinner une plage raisonnable
  dans extra.
- Handlers traduisent erreurs métier en erreurs MCP sans stack/secrets.
- Supporter `list`, `read`, resource templates, pagination/nextCursor si protocole le permet.
- ETag/hash dans metadata pour cache client.

### Slice 7.3 — Tool `query_semantic_model`

- JSON Schema d'entrée fermé (`additionalProperties: false`).
- Opérateurs identiques à `SemanticQuery`; limites dates/list lengths/value length.
- Annotation read-only si SDK supporte, mais enforcement serveur reste obligatoire.
- Output : rows sérialisées bornées + evidence safe + truncated flag ; pas de DataFrame.
- Timeout/cancel propagés si API le permet.

Tests : champ SQL rejeté, injection value traitée par resolver, trop de rows, type Spark non JSON,
timeout, warning certification.

### Slice 7.4 — Auth et transport

- stdio : config locale explicite, scopes statiques, jamais flow OAuth HTTP.
- HTTP : valider bearer token via composant injectable ; audience/resource, expiry, issuer, scopes.
- Le plan n'implémente pas un Authorization Server.
- Credentials par requête, jamais dans singleton/connection state.
- Traces user subject en omit/HMAC selon Feature 5.

### Slice 7.5 — Packaging, CLI et smoke

- `skifer mcp serve --transport stdio|http --config ...`.
- Refuser bind public sans auth/config explicite.
- Health endpoint sans données métier ; logs startup sans secrets.
- Smoke client officiel : list/read/query ; matrice versions documentée.
- Guide d'intégration Claude/ChatGPT/Codex générique sans dépendre d'un client.

## 6. Budgets et limites obligatoires

- max 100 resources/page ;
- max 1 000 résultats internes avant format, défaut 100 ;
- max payload configurable avec plafond dur ;
- max longueur filter value et nombre de filters ;
- timeout query ;
- aucune resource ne livre automatiquement samples distincts sensibles.

Les valeurs précises peuvent être configurées mais les plafonds durs doivent exister.

## 7. Hors périmètre

- MCP write tools ;
- serveur d'autorisation OAuth ;
- raw SQL ;
- accès Bronze/Silver par défaut ;
- fichiers arbitraires ;
- prompts MCP autorisant une action.

## 8. Definition of Done

- service testé indépendamment du transport ;
- resources filtrées par scopes ;
- query passe par gate/evidence/tracing ;
- aucun raw table/SQL endpoint ;
- stdio et HTTP smoke testés sans service externe réel ;
- extra MCP absent n'empêche pas l'import core.
