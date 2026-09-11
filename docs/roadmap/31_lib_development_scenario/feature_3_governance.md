# Plan 31 — Feature 3.1 à 3.5 : Gouvernance dans le YAML

**But** : rendre la gouvernance d'un produit de données (classification, ownership, cycle de vie,
SLA/sécurité, import ODCS, diff de contrat) déclarable directement dans le YAML de pipeline, exposée
partout où le contrat est déjà consommé (hash de certification, ODCS, lineage, evidence, access policy,
UC mirror), **sans casser la rétrocompatibilité stricte** : un YAML dépourvu de ces blocs charge, se
hache et s'exécute exactement comme avant.

**Arbitrages §7 (2026-09-11) bakés dans ce plan** :
- **§7.3** — Le hash de contrat inclut `sla` et `security` ; il **exclut** `status`, `reviewers`,
  `effective_from`, `effective_until`. `CANONICALIZATION_VERSION` passe de **1 → 2**. Les anciens hashs
  (version 1) restent lisibles ; test de non-régression sur des fixtures.
- **§7.5** — La propagation de classification est **`warn` en v1, pas `strict`** : une élévation inférée
  non déclarée émet un WARNING au load, elle **n'échoue pas** le load. L'abaissement explicite reste
  autorisé mais **journalisé**. Comportement piloté par un flag (`classification_propagation="warn"`)
  pour que la Feature 31.7 le bascule en `strict` plus tard.
- **§7.6** — La Feature 31.3 est livrée **avant** 31.2 (elle alimente l'éditeur UI primaire). 31.3.1
  expose la classification dans `ColumnRecord` (31.2) — **couplage forward** noté, pas implémenté ici.

**Règle de commit** : **une slice = un commit** `feat(plan31-3.M): …` + tests + une entrée
`CHANGELOG.md` sous `## [Unreleased]`. **Ne jamais** bumper la version de `pyproject.toml`.
La première slice qui touche `CHANGELOG.md` doit **créer** la section `## [Unreleased]` (le sommet
actuel est `## [2.1.0] - 2026-09-10`, il n'y a pas de section `[Unreleased]`).

---

## État actuel du code

### `src/skifer/observability/certification.py`
```python
CANONICALIZATION_VERSION = 1
HASH_ALGORITHM = "sha256"

@dataclass(frozen=True)
class ContractDefinition:
    contract_id: str
    contract_version: str
    definition_hash: str
    canonical_json: str
    data_product_id: str
    owner: str | None
    hash_algorithm: str = HASH_ALGORITHM
    canonicalization_version: int = CANONICALIZATION_VERSION
    status: str = "DRAFT"          # statut du store de certification, PAS le lifecycle du contrat
    created_at: datetime | None = None

def canonicalize_contract(schema: ParsedSchema) -> ContractDefinition: ...
```
Le hash actuel est calculé ainsi (charge utile → `json.dumps(payload, sort_keys=True,
separators=(",", ":"), ensure_ascii=False)` puis `sha256(...).hexdigest()`) :
```python
payload = {
    "canonicalization_version": CANONICALIZATION_VERSION,   # 1
    "data_product": {"id": ..., "version": ...},            # PAS owner/description
    "contract": {
        "grain": list(schema.contract_grain),
        "output": [
            {"name", "logical_type", "required", "unique", "classification", "entity"}  # par field, ordre conservé
            for field in schema.contract_output
        ],
    },
    "semantic": {"model_key", "entity", "default_time_dimension", "dimensions"} | None,
}
```
Points clés : `owner`/`description` **déjà exclus** du hash ; `sort_keys=True` trie les clés de mapping
mais **l'ordre de la liste `output` est conservé** (significatif pour les générateurs). `canonicalize_contract`
lève `ValueError` si `data_product` absent, version non-semver, ou `contract_output` vide.

### `src/skifer/observability/odcs.py`
```python
@dataclass(frozen=True)
class OdcsExport:
    document: dict[str, Any]
    warnings: tuple[str, ...]

def export_odcs_31(schema: ParsedSchema, definition: ContractDefinition) -> OdcsExport: ...
```
Le document produit contient aujourd'hui :
```python
document = {
    "apiVersion": "v3.1.0", "kind": "DataContract",
    "id": definition.contract_id, "version": definition.contract_version,
    "status": definition.status.lower(),                 # <- lifecycle du STORE, pas du contrat
    "description": product.description if product else None,
    "schema": {"properties": fields},                    # fields = [{name, logicalType, description, classification}]
    "quality": quality,                                  # required/unique + grain
    "team": [{"name": definition.owner}] if definition.owner else [],   # owner = STRING
    "roles": [{"role": "reader", "access": "read"}],
    "slaProperties": [],                                 # <- VIDE (slice 3.3 le remplit)
    "customProperties": {"skifer.definition_hash", "skifer.hash_algorithm", "skifer.canonicalization_version"},
}
```
**Pas de fonction d'import** aujourd'hui (slice 3.4 ajoute `import_odcs_31`).

### `src/skifer/core/schema_loader.py`
Frozensets de clés autorisées (à étendre) :
```python
_DATA_PRODUCT_ALLOWED_KEYS = frozenset({"id", "version", "owner", "description"})
_CONTRACT_ALLOWED_KEYS      = frozenset({"grain", "output"})
_OUTPUT_FIELD_ALLOWED_KEYS  = frozenset({"logical_type", "required", "unique", "classification", "entity", "description"})
```
La normalisation vit dans `_normalize_agent_ready_metadata(schema_dict)` (accumule les erreurs, lève
un seul `ValueError`). `data_product.owner` est validé aujourd'hui comme **string non vide uniquement**
(`for key in ("owner", "description"): ... must be a non-empty string`). `contract.classification`
n'est **pas** validé contre une taxonomie (juste "string non vide" via `_OUTPUT_FIELD_ALLOWED_KEYS`).

### `src/skifer/core/ir.py`
```python
@dataclass(frozen=True)
class ParsedDataProduct:
    id: str; version: str; owner: str | None = None; description: str | None = None

@dataclass(frozen=True)
class ParsedOutputField:
    name: str; logical_type: str | None = None; required: bool | None = None
    unique: bool | None = None; classification: str | None = None
    entity: str | None = None; description: str | None = None

@dataclass
class ParsedSchema:
    ...
    data_product: ParsedDataProduct | None = None
    contract_output: list[ParsedOutputField] = field(default_factory=list)
    contract_grain: list[str] = field(default_factory=list)
    semantic: ParsedSemanticSeed | None = None
    raw: dict = field(default_factory=dict)
```
`parse_to_ir` construit `contract_output` via `ParsedOutputField(name=name, **metadata)` — donc **toute
clé ajoutée à `_OUTPUT_FIELD_ALLOWED_KEYS` doit exister comme champ de `ParsedOutputField`**, sinon
`TypeError`. Il n'existe **aucun** champ IR pour `contract.status/reviewers/effective_*/sla/security`.

### `src/skifer/core/json_schema.py`
`generate_json_schema()` (draft-07) : `$defs` déjà présents `DataProductDef` (owner = `{"type":"string"}`),
`OutputFieldDef` (classification = `{"type":"string","minLength":1}`), `ContractDef`
(required `["output"]`, keys `grain`/`output`), `SemanticSeedDef`. `additionalProperties: False` partout.

### `src/skifer/lineage/tracker.py`
```python
@dataclass
class LineageEdge:
    source_table: str; source_column: str; target_table: str; target_column: str
    transformations: list[str] = field(default_factory=list)
    edge_type: Literal["select", "join", "rule", "metric"] = "select"

class LineageGraph:
    def upstream(self, table, column) -> list[LineageEdge]   # provenance (target == (t,c))
    def downstream(self, table, column) -> list[LineageEdge]  # impact

class LineageTracker:
    @staticmethod
    def from_schema(schema_dict: dict, target_name: str | None = None) -> LineageGraph: ...
```
Dans `from_schema`, le `target` (table de sortie) est `target_name or f"{primary_table}_output"`, et
chaque colonne de sortie a une arête `source_column → target_column` avec `edge_type` `select`/`rule`/`join`.
Les colonnes sources d'une colonne dérivée `X` sont donc `graph.upstream(target, X)` → `e.source_column`.

### `src/skifer/semantic/evidence.py`
```python
@dataclass(frozen=True)
class EvidencePolicy:
    include_sql: bool = False
    include_filter_values: bool = False
    @classmethod
    def redacted(cls) -> "EvidencePolicy": return cls()

@dataclass(frozen=True)
class SourceEvidence:
    dataset: str; contract_id: str | None; contract_version: str | None
    definition_hash: str | None; certification_status: str
    certified_at: datetime | None; load_age_seconds: float | None
    data_age_seconds: float | None; certification_run_id: str | None
```
`SemanticEvidence._serialize_filter(...)` rédige `value` en `"<redacted>"` **sauf si**
`include_filter_values`. La policy est une **fonction pure** (aucune I/O). Aujourd'hui `EvidencePolicy`
n'a **aucune** notion de classification par colonne.

### `src/skifer/semantic/access_policy.py`
```python
class CertificationDecision(str, Enum):
    ALLOW = "ALLOW"; WARN = "WARN"; DENY = "DENY"; REQUIRE_HUMAN = "REQUIRE_HUMAN"

@dataclass(frozen=True)
class PolicyEvaluation:
    decision: CertificationDecision; reasons: tuple[str, ...]; evaluated_at: datetime

def evaluate(certifications, context, mode: str, now: datetime,
             override: CertificationOverride | None = None,
             max_age: str | None = None, count_warning: bool = True) -> PolicyEvaluation: ...
```
Raisons actuelles (strings dans `reasons`) : `MISSING`, `FAILED_CHECK`, `EXPIRED`, `OVERRIDDEN`.
`mode ∈ {off, warn, enforce, supervised}`. Cette fonction ne connaît **rien** au lifecycle de contrat
(deprecated / fenêtre effective) — slice 3.3 ajoute une **nouvelle** fonction dédiée, sans toucher à
`evaluate()` (utilisée par le gate de certification sémantique du Plan 29).

### `src/skifer/observability/contracts.py`
`ContractExtractor.extract(schema_dict: dict) -> list[DataContract]`. Consomme le **dict normalisé**
(pas l'IR). Dérive Null/Unique/Type/FilterInvariant + section `observability:` (Freshness/Volume/…).
Ne lit **pas** `contract.sla` aujourd'hui.

### `src/skifer/observability/checks.py`
```python
@dataclass
class DataContract:
    table: str; severity: str = "warning"; scope: ClassVar[ContractScope] = ContractScope.DATASET

@dataclass
class LoadFreshnessCheck(DataContract):
    max_delay: str = "24h"
    store: Any = field(default=None, repr=False, compare=False)          # get_latest_promoted(table)
    clock: Callable[[], datetime] | None = field(default=None, repr=False, compare=False)
    def evaluate(self, backend, fqn) -> CheckResult: ...   # âge = now - dernier promote ; PASS si <= max_delay
```
`FreshnessCheck._parse_delay("24h"|"30m"|"1d"|"3600s")` est réutilisable (staticmethod, suffixes h/m/d/s).

### `src/skifer/observability/uc_mirror.py`
```python
_ALLOWED = frozenset({"skifer_owner", "contract_version", "certification", "definition_hash"})
def mirror_certification(backend, table_fqn, certification: Certification, owner: str | None = None) -> UcSyncResult: ...
```
Émet `ALTER TABLE ... SET TAGS (...)` pour les tags non nuls. `skifer_owner` déjà présent (valeur =
`owner` string). Pas de `skifer_domain`.

### `src/skifer/cli.py`
`main()` : `subparsers = parser.add_subparsers(dest="command")`. Commandes existantes : `validate`,
`hub`, `semantic` (sous-commandes `sync`/`validate`), `mcp`, `adaptive`. Dispatch par
`if args.command == "...": _run_...(args)`. **Pas** de commande `contract`.

### `src/skifer/services/` — **n'existe pas encore**
`services/governance.py` (`GovernanceService`) est créé par la **Feature 31.1.4**. La slice 3.5 y
expose `diff_contracts` ; si le module n'existe pas au moment d'implémenter 3.5, créer un stub minimal
(voir slice 3.5, section dépendances).

### Tests / fixtures
Un fichier par module : `tests/test_<module>.py`. Le fixture `spark` (dans `tests/conftest.py`) n'est
requis **que** pour les tests construisant un `F.col(...)`. `tests/test_certification.py` contient déjà
`BASE_YAML` et les tests d'identité de hash — c'est là que va le **test de non-régression de hash**.

---

## Slice 31.3.1 — Taxonomie `classification` + propagation lineage + exposition

### Objectif
1. Valider `classification ∈ {public, internal, confidential, restricted, pii}` au load **et** dans le
   JSON Schema (aujourd'hui n'importe quelle string passe).
2. **Propager** la classification le long des arêtes de lineage : une colonne dérivée hérite du
   **niveau le plus élevé** de ses sources, sauf déclaration explicite. En **v1 = `warn`** : une
   élévation inférée non déclarée émet un WARNING (ne bloque pas). Un **abaissement explicite** (niveau
   déclaré < niveau inféré) est autorisé mais **journalisé**.
3. Exposer la classification effective dans `SourceEvidence`, dans `EvidencePolicy` (une colonne
   `pii`/`restricted` **conserve** ses filter values au lieu d'être rédigée), et dans l'export ODCS
   (déjà présent au niveau field ; garantir la cohérence).
4. **Forward-coupling 31.2** : la classification effective doit être atteignable pour peupler
   `ColumnRecord` (31.2). Exposer une fonction pure réutilisable (`resolve_field_classifications`).

### Fichiers
- `src/skifer/core/constants.py` — **modification** : ajouter la taxonomie ordonnée.
  ```python
  # Ordered from least to most sensitive — index = severity rank (Plan 31.3.1).
  CLASSIFICATION_LEVELS: tuple[str, ...] = (
      "public", "internal", "confidential", "restricted", "pii",
  )
  CLASSIFICATION_RANK: dict[str, int] = {name: i for i, name in enumerate(CLASSIFICATION_LEVELS)}
  # pii et restricted conservent leurs valeurs de filtre dans l'evidence.
  SENSITIVE_CLASSIFICATIONS: frozenset[str] = frozenset({"restricted", "pii"})
  ```
- `src/skifer/core/schema_loader.py` — **modification** : dans `_normalize_agent_ready_metadata`, valider
  `metadata["classification"]` contre `CLASSIFICATION_LEVELS` (après le check "string non vide").
- `src/skifer/core/json_schema.py` — **modification** : `OutputFieldDef.classification` devient un enum.
- `src/skifer/lineage/classification.py` — **création** : fonction pure de propagation.
- `src/skifer/semantic/evidence.py` — **modification** : `EvidencePolicy` gagne un champ
  `sensitive_columns: frozenset[str]` et `_serialize_filter` ne rédige pas une valeur de colonne sensible.
- `src/skifer/observability/odcs.py` — **modification** : (déjà exporte `classification` par field ;
  aucun changement de structure requis — ajouter seulement un test de couverture ; voir 3.1 tests).

### Signatures Python
```python
# src/skifer/lineage/classification.py  (création)
from __future__ import annotations
import logging
from skifer.core.constants import CLASSIFICATION_LEVELS, CLASSIFICATION_RANK
from skifer.lineage.tracker import LineageGraph

logger = logging.getLogger(__name__)

class ClassificationPropagationWarning(UserWarning):
    """Une élévation de classification inférée mais non déclarée (mode warn)."""

def _max_level(levels: list[str]) -> str | None:
    ranked = [l for l in levels if l in CLASSIFICATION_RANK]
    return max(ranked, key=CLASSIFICATION_RANK.__getitem__) if ranked else None

def resolve_field_classifications(
    graph: LineageGraph,
    target_table: str,
    declared: dict[str, str],              # target_column -> classification déclarée dans contract.output
    source_classifications: dict[str, str],# source_column -> classification connue (des sources amont)
    *,
    mode: str = "warn",                    # "warn" (v1) | "strict" (flip 31.7)
) -> dict[str, str]:
    """Classification EFFECTIVE par colonne de sortie.

    Pour chaque colonne cible de `target_table` : niveau inféré = max des classifications des
    colonnes sources (via graph.upstream). Règles :
      - pas de déclaration + inféré présent  -> effective = inféré
      - déclaré >= inféré                     -> effective = déclaré (élévation explicite OK, silencieuse)
      - déclaré <  inféré  (abaissement)      -> effective = déclaré, LOGGE un warning explicite
      - inféré  >  déclaré  et rien déclaré... (couvert par 1er cas)
    Une ÉLÉVATION INFÉRÉE non déclarée (déclaré absent, inféré > public) :
      - mode="warn"   -> warnings.warn(ClassificationPropagationWarning), effective = inféré (NE bloque pas)
      - mode="strict" -> raise ValueError (réservé 31.7)
    Retourne le mapping complet {colonne: classification effective}.
    """
```
Modification `schema_loader._normalize_agent_ready_metadata` (dans la boucle par field, section `for key
in ("logical_type", "classification", "entity", "description")`), **après** le check string-non-vide :
```python
if key == "classification" and value.strip() not in CLASSIFICATION_LEVELS:
    errors.append(
        f"  [contract.output] field '{field_name}' classification '{value.strip()}' is invalid. "
        f"Allowed: {list(CLASSIFICATION_LEVELS)}"
    )
```
Modification `json_schema.OutputFieldDef` :
```python
"classification": {"type": "string", "enum": list(CLASSIFICATION_LEVELS)},   # au lieu de minLength:1
```
(importer `CLASSIFICATION_LEVELS` depuis `skifer.core.constants` en tête de `json_schema.py`.)

Modification `evidence.EvidencePolicy` :
```python
@dataclass(frozen=True)
class EvidencePolicy:
    include_sql: bool = False
    include_filter_values: bool = False
    sensitive_columns: frozenset[str] = frozenset()   # colonnes pii/restricted : valeurs conservées
```
> **Note de sémantique à respecter** : "conserver les filter values d'une colonne pii/restricted" veut
> dire que, pour ces colonnes, la valeur est **incluse** (elle appartient au périmètre gouverné et doit
> figurer dans la preuve d'audit), là où une colonne ordinaire reste rédigée sauf `include_filter_values`.
> `SemanticEvidence._serialize_filter` reçoit donc le set de colonnes sensibles et inclut la valeur si
> `include_filter_values` **ou** `filter_def["column"] in sensitive_columns`. Threader `sensitive_columns`
> depuis l'appelant (`to_dict`) sans changer la signature publique par défaut :
```python
# dans SemanticEvidence.to_dict(...), passer self._sensitive au _serialize_filter — mais SemanticEvidence
# ne porte pas de policy. Pattern retenu : ajouter un paramètre kw-only à to_dict :
def to_dict(self, *, include_sql=False, include_filter_values=False,
            sensitive_columns: frozenset[str] = frozenset()) -> dict: ...
# et _serialize_filter(filter_def, index, include_filter_values, sensitive_columns)
#   include = include_filter_values or filter_def.get("column") in sensitive_columns
```

### Comportement & règles
- **Taxonomie fermée** : au load, une valeur hors `CLASSIFICATION_LEVELS` est une erreur agrégée
  (`ValueError` unique, style fail-fast existant). Un YAML **sans** `classification` reste valide.
- **Propagation `warn` (arbitrage §7.5)** : le mode par défaut est `"warn"`. Une élévation inférée non
  déclarée journalise/`warnings.warn` mais **retourne quand même** la classification effective (haute) —
  le load n'échoue jamais. Le flag `mode` est le point d'entrée que 31.7 passera à `"strict"`.
- **Abaissement explicite** : `déclaré < inféré` est autorisé (l'auteur assume la déclassification) mais
  **loggé** en WARNING avec colonne, niveau inféré et niveau déclaré.
- **Evidence fail-closed conservé** : par défaut (`include_filter_values=False`, `sensitive_columns=∅`),
  le comportement est identique à aujourd'hui (tout est `<redacted>`). Seules les colonnes explicitement
  déclarées sensibles voient leur valeur incluse.
- **Rien dans le hash** ne change dans cette slice : `classification` est **déjà** dans le payload de
  `canonicalize_contract` (au niveau field). On ne modifie donc pas `CANONICALIZATION_VERSION` ici.

### Cas de test — `tests/test_schema_loader.py`, `tests/test_lineage_classification.py` (création), `tests/test_evidence.py`, `tests/test_odcs_export.py`
- `test_classification_unknown_value_refused` : YAML avec `classification: secret` → `parse_schema`
  lève `ValueError` mentionnant `Allowed:` et la taxonomie. (Pas de spark.)
- `test_classification_all_valid_levels_accepted` : les 5 niveaux passent le load.
- `test_classification_absent_still_loads` : field sans `classification` → OK (rétrocompat).
- `test_propagation_inherits_highest_source_level` : graphe où `net_amount` dérive de deux sources
  `internal` + `confidential`, non déclarée → effective `confidential` ; WARNING émis
  (`pytest.warns(ClassificationPropagationWarning)`). (Pas de spark — on construit un `LineageGraph`
  à la main ou via `LineageTracker.from_schema(parse_schema(...))`.)
- `test_propagation_via_join_key` : classification propagée le long d'une arête `join`.
- `test_explicit_lowering_is_allowed_and_logged` : source inférée `pii`, déclarée `internal` →
  effective `internal`, WARNING loggé (`caplog`), **pas** d'exception.
- `test_explicit_elevation_is_silent` : déclaré `restricted` >= inféré `internal` → effective
  `restricted`, aucun warning.
- `test_strict_mode_raises_on_inferred_elevation` : `mode="strict"` + élévation non déclarée →
  `ValueError` (verrou pour 31.7).
- `test_evidence_keeps_sensitive_filter_values` (`tests/test_evidence.py`) : filtre sur colonne
  `sensitive_columns={"ssn"}`, `include_filter_values=False` → la valeur du filtre `ssn` est présente,
  celle d'une colonne ordinaire reste `<redacted>`.
- `test_json_schema_classification_is_enum` (`tests/test_json_schema.py` si présent, sinon
  `tests/test_schema_loader.py`) : `generate_json_schema()["$defs"]["OutputFieldDef"]["properties"]
  ["classification"]["enum"] == list(CLASSIFICATION_LEVELS)`.
- `test_odcs_exports_field_classification` (`tests/test_odcs_export.py`) : un field `pii` apparaît avec
  `classification: "pii"` dans `document["schema"]["properties"]`.

### Commit
`feat(plan31-3.1): classification taxonomy, lineage propagation (warn) and evidence exposure`
CHANGELOG (créer `## [Unreleased]` → `### Added`) :
- `Field-level data classification (\`public|internal|confidential|restricted|pii\`) is now validated at load and in the JSON Schema, propagated along column lineage (a derived column inherits the highest source level; inferred elevation warns, explicit lowering is logged), and sensitive (\`pii\`/\`restricted\`) columns retain their filter values in query evidence.`

### DoD
Load refuse une valeur hors taxonomie ; les 5 niveaux passent ; propagation testée (join + règle) ;
abaissement explicite loggé sans échec ; mode strict prêt mais non activé ; JSON Schema = enum ;
evidence conserve les valeurs sensibles ; suite verte ; `CANONICALIZATION_VERSION` **inchangé**.

---

## Slice 31.3.2 — Ownership : `owner` string|mapping, `data_product.domain`, ODCS team[], UC tags

### Objectif
`owner` accepte soit une **string** (rétrocompat), soit un mapping `{team, steward, domain, contact}`.
Ajouter `data_product.domain`. Remplir `team[]` dans ODCS et les tags UC `skifer_owner`, `skifer_domain`.
`owner` reste **toujours exclu** du hash (déjà le cas).

### Fichiers
- `src/skifer/core/schema_loader.py` — **modification** : `owner` string OU mapping ; `domain` ajouté.
- `src/skifer/core/ir.py` — **modification** : `ParsedDataProduct` gagne les champs d'ownership.
- `src/skifer/core/json_schema.py` — **modification** : `DataProductDef.owner` = string OU objet ;
  ajout `domain`.
- `src/skifer/observability/odcs.py` — **modification** : `team[]` peuplé depuis l'owner mapping.
- `src/skifer/observability/uc_mirror.py` — **modification** : tag `skifer_domain` (+ `skifer_owner`).
- `src/skifer/observability/certification.py` — **modification mineure** : `ContractDefinition.owner`
  doit rester une string lisible ; voir règles.

### Signatures Python
```python
# ir.py
@dataclass(frozen=True)
class ParsedOwner:
    team: str | None = None
    steward: str | None = None
    domain: str | None = None
    contact: str | None = None

@dataclass(frozen=True)
class ParsedDataProduct:
    id: str
    version: str
    owner: "str | ParsedOwner | None" = None   # string rétrocompat OU mapping
    description: str | None = None
    domain: str | None = None                  # data_product.domain (niveau produit)

    @property
    def owner_label(self) -> str | None:
        """Représentation string stable de l'owner pour ODCS team/UC tag/ContractDefinition.owner."""
        if self.owner is None: return None
        if isinstance(self.owner, str): return self.owner
        return self.owner.team or self.owner.steward or self.owner.contact

    @property
    def owner_domain(self) -> str | None:
        """Domaine effectif : data_product.domain sinon owner.domain (mapping)."""
        if self.domain: return self.domain
        return self.owner.domain if isinstance(self.owner, ParsedOwner) else None
```
Modification `parse_to_ir` (bloc data_product) :
```python
raw_owner = raw_product.get("owner")
owner = raw_owner if isinstance(raw_owner, str) else (
    ParsedOwner(**{k: raw_owner.get(k) for k in ("team", "steward", "domain", "contact")})
    if isinstance(raw_owner, dict) else None
)
data_product = ParsedDataProduct(
    id=raw_product["id"], version=raw_product["version"],
    owner=owner, description=raw_product.get("description"),
    domain=raw_product.get("domain"),
)
```
Modification `schema_loader` (`_DATA_PRODUCT_ALLOWED_KEYS`, et validation owner) :
```python
_DATA_PRODUCT_ALLOWED_KEYS = frozenset({"id", "version", "owner", "description", "domain"})
_OWNER_ALLOWED_KEYS = frozenset({"team", "steward", "domain", "contact"})
# validation owner : string non vide OU mapping à clés dans _OWNER_ALLOWED_KEYS, chaque valeur string non vide.
# 'domain' (niveau produit) : string non vide quand présent.
# Normalisation : conserver la forme d'origine (string reste string, dict reste dict de strings strippées).
```
Modification `certification.canonicalize_contract` — l'attribut lu pour `ContractDefinition.owner` :
```python
owner=schema.data_product.owner_label,   # <- au lieu de schema.data_product.owner (peut être un mapping)
```
Modification `odcs.export_odcs_31` (bloc `team`) :
```python
team = []
if product and getattr(product, "owner", None) is not None:
    if isinstance(product.owner, str):
        team = [{"name": product.owner}]
    else:  # ParsedOwner
        member = {"name": product.owner.team or product.owner.steward or product.owner.contact}
        if product.owner.steward: member["role"] = "steward"
        team = [member]
document["team"] = team
# domaine exposé en customProperties (pas de champ ODCS 3.1 dédié standard) :
if product and product.owner_domain:
    document["customProperties"]["skifer.domain"] = product.owner_domain
```
> Attention : `export_odcs_31` reçoit `definition: ContractDefinition` **et** `schema: ParsedSchema`.
> Le domaine et l'owner mapping viennent de `schema.data_product` (accessible via `schema.data_product`),
> `definition.owner` restant la string label. Utiliser `schema.data_product` pour team/domain.
Modification `uc_mirror.mirror_certification` :
```python
_ALLOWED = frozenset({"skifer_owner", "skifer_domain", "contract_version", "certification", "definition_hash"})
def mirror_certification(backend, table_fqn, certification, owner=None, domain=None) -> UcSyncResult:
    tags = {"skifer_owner": owner, "skifer_domain": domain,
            "contract_version": certification.contract_version,
            "certification": certification.status, "definition_hash": certification.definition_hash}
    # ... reste inchangé (n'émet que les tags non nuls)
```
Modification `json_schema.DataProductDef.properties.owner` :
```python
"owner": {"oneOf": [
    {"type": "string", "minLength": 1},
    {"type": "object", "additionalProperties": False, "properties": {
        "team": {"type": "string", "minLength": 1},
        "steward": {"type": "string", "minLength": 1},
        "domain": {"type": "string", "minLength": 1},
        "contact": {"type": "string", "minLength": 1},
    }},
]},
"domain": {"type": "string", "minLength": 1},
```

### Comportement & règles
- **Rétrocompat stricte** : `owner: data-platform` (string) produit **exactement** le même
  `ContractDefinition` (mêmes `owner_label`, même hash — owner hors hash) qu'aujourd'hui. Test explicite.
- **owner mapping ⇒ même ContractDefinition qu'une string équivalente** au sens du hash : le hash ne
  dépend pas de `owner`, donc `owner: {team: X}` et `owner: X` donnent le **même** `definition_hash`.
- **owner toujours hors hash** : ne rien ajouter au payload de `canonicalize_contract` concernant owner
  ou domain. (Le domaine est de la métadonnée d'ownership, pas de la définition métier → hors hash.)
- UC mirror n'émet un tag que si sa valeur est non nulle (`skifer_domain` omis si pas de domaine).

### Cas de test — `tests/test_schema_loader.py`, `tests/test_certification.py`, `tests/test_odcs_export.py`, `tests/test_uc_mirror.py`
- `test_owner_string_and_mapping_same_definition` : deux YAML (owner string vs owner `{team: ...}`) →
  **même** `definition_hash` ; `ContractDefinition.owner` = label attendu dans les deux cas.
- `test_owner_mapping_fields_parsed` : `parse_to_ir` expose `team/steward/domain/contact`.
- `test_owner_unknown_key_refused` : `owner: {teem: x}` → `ValueError`.
- `test_data_product_domain_parsed` : `data_product.domain` remonté en IR (`owner_domain`).
- `test_odcs_team_filled_from_mapping` : `document["team"] == [{"name": "...", "role": "steward"}]` ;
  `document["customProperties"]["skifer.domain"]` présent.
- `test_odcs_team_backcompat_string_owner` : owner string → `team == [{"name": owner}]` (inchangé).
- `test_uc_mirror_emits_owner_and_domain_tags` : SQL `SET TAGS` contient `skifer_owner` et
  `skifer_domain` ; domaine absent ⇒ tag omis. (Backend fake avec `execute_sql` capturant la requête.)
- `test_json_schema_owner_oneof` : `DataProductDef.properties.owner` a un `oneOf` string|object.

### Commit
`feat(plan31-3.2): structured ownership (owner string|mapping, data_product.domain) into ODCS team and UC tags`
CHANGELOG `### Added` :
- `\`data_product.owner\` now accepts a structured mapping (\`team\`/\`steward\`/\`domain\`/\`contact\`) in addition to a plain string, plus a top-level \`data_product.domain\`. Ownership flows into the ODCS \`team[]\` block and the Unity Catalog \`skifer_owner\`/\`skifer_domain\` tags. Ownership stays excluded from the contract hash, so a string and an equivalent mapping produce the same definition hash.`

### DoD
String et mapping donnent le même `definition_hash` ; owner mapping parsé ; domaine remonté ; ODCS
`team[]` + UC `skifer_domain` remplis ; owner hors hash ; JSON Schema décrit owner oneOf + domain ;
suite verte.

---

## Slice 31.3.3 — Lifecycle : status/reviewers/effective_*, sla, security + gate + hash v2

### Objectif
Ajouter au bloc `contract:` : `status ∈ {draft, active, deprecated}` (défaut `active`), `reviewers[]`,
`effective_from`/`effective_until` (dates ISO), `sla: {refresh_frequency, max_latency}`,
`security: {level, access_policy}`. `ContractExtractor` dérive un `LoadFreshnessCheck` depuis la SLA.
Nouvelle fonction `access_policy.evaluate_lifecycle()` → WARN si `deprecated`, DENY hors fenêtre
effective. **Hash (arbitrage §7.3)** : `sla` et `security` **DANS** le hash ; `status`, `reviewers`,
`effective_from`, `effective_until` **HORS** hash. **Bump `CANONICALIZATION_VERSION` 1 → 2**, anciens
hashs lisibles.

### Fichiers
- `src/skifer/core/schema_loader.py` — **modification** : `_CONTRACT_ALLOWED_KEYS` étendu + validation.
- `src/skifer/core/ir.py` — **modification** : champs lifecycle sur `ParsedSchema`.
- `src/skifer/core/json_schema.py` — **modification** : `ContractDef` décrit les nouveaux champs.
- `src/skifer/observability/certification.py` — **modification** : bump version, payload sla/security.
- `src/skifer/observability/contracts.py` — **modification** : `LoadFreshnessCheck` depuis la SLA.
- `src/skifer/semantic/access_policy.py` — **modification** : `evaluate_lifecycle()` + raisons.
- `src/skifer/observability/odcs.py` — **modification** : `slaProperties` + `status` lifecycle.

### Signatures Python
```python
# core/constants.py
VALID_CONTRACT_STATUSES: frozenset[str] = frozenset({"draft", "active", "deprecated"})
DEFAULT_CONTRACT_STATUS = "active"

# ir.py — nouveaux dataclasses + champs sur ParsedSchema
@dataclass(frozen=True)
class ParsedSla:
    refresh_frequency: str | None = None   # ex "1h", "daily" (chaîne libre validée non vide)
    max_latency: str | None = None         # ex "24h" (parsable par FreshnessCheck._parse_delay si suffixe h/m/d/s)

@dataclass(frozen=True)
class ParsedSecurity:
    level: str | None = None               # chaîne libre non vide (ex "restricted")
    access_policy: str | None = None       # chaîne libre non vide (ex "row_filter:region")

@dataclass
class ParsedSchema:
    ...
    contract_status: str = DEFAULT_CONTRACT_STATUS
    contract_reviewers: tuple[str, ...] = field(default_factory=tuple)
    contract_effective_from: str | None = None    # "YYYY-MM-DD" (validé au load)
    contract_effective_until: str | None = None
    contract_sla: ParsedSla | None = None
    contract_security: ParsedSecurity | None = None
```
```python
# schema_loader.py
_CONTRACT_ALLOWED_KEYS = frozenset({
    "grain", "output", "status", "reviewers",
    "effective_from", "effective_until", "sla", "security",
})
_SLA_ALLOWED_KEYS = frozenset({"refresh_frequency", "max_latency"})
_SECURITY_ALLOWED_KEYS = frozenset({"level", "access_policy"})
# Règles de validation à ajouter dans _normalize_agent_ready_metadata (bloc contract) :
#  - status : si présent, doit ∈ VALID_CONTRACT_STATUSES ; défaut "active" appliqué à la normalisation.
#  - reviewers : liste de strings non vides (sinon erreur agrégée).
#  - effective_from/until : strings ISO "YYYY-MM-DD" (datetime.date.fromisoformat en try/except → erreur).
#       si les deux présents et from > until -> erreur "effective_from must be <= effective_until".
#  - sla : mapping, clés ⊆ _SLA_ALLOWED_KEYS, valeurs strings non vides.
#  - security : mapping, clés ⊆ _SECURITY_ALLOWED_KEYS, valeurs strings non vides.
#  - NORMALISATION : réécrire schema_dict["contract"] en incluant ces clés normalisées (le bloc actuel
#    ne réémet aujourd'hui QUE {output, grain} — il faut préserver les nouvelles clés).
```
> **Piège à corriger** : aujourd'hui, en fin de bloc contract, le code fait
> `schema_dict["contract"] = {"output": normalized_output}` puis ajoute `grain`. **Il écrase donc tout
> le reste.** La normalisation doit repartir de ce dict et y rajouter `status`/`reviewers`/dates/`sla`/
> `security` normalisés, sinon ils sont perdus avant `parse_to_ir`.

```python
# certification.py — bump + payload
CANONICALIZATION_VERSION = 2   # 1 -> 2 (arbitrage §7.3)

# dans canonicalize_contract, ajouter au payload["contract"] (PAS status/reviewers/dates) :
payload["contract"]["sla"] = (
    {"refresh_frequency": schema.contract_sla.refresh_frequency,
     "max_latency": schema.contract_sla.max_latency}
    if schema.contract_sla else None
)
payload["contract"]["security"] = (
    {"level": schema.contract_security.level,
     "access_policy": schema.contract_security.access_policy}
    if schema.contract_security else None
)
# canonicalization_version en tête du payload passe mécaniquement à 2.
```
```python
# access_policy.py — NOUVELLE fonction, ne touche pas evaluate()
class LifecycleReason(str, Enum):
    DEPRECATED = "DEPRECATED"
    NOT_YET_EFFECTIVE = "NOT_YET_EFFECTIVE"
    EXPIRED_WINDOW = "EXPIRED_WINDOW"

def evaluate_lifecycle(
    *,
    status: str,
    effective_from: str | None,     # "YYYY-MM-DD" ou None
    effective_until: str | None,
    now: datetime,
) -> PolicyEvaluation:
    """WARN si deprecated ; DENY hors fenêtre effective ; sinon ALLOW.
    Fail-closed : now naïf -> traité comme UTC ; date illisible -> ValueError (le load l'a déjà validée).
    Précédence : hors-fenêtre (DENY) l'emporte sur deprecated (WARN) — une source périmée est refusée
    même si elle est encore 'active'. reasons cumule les causes applicables.
    """
```
```python
# contracts.py — LoadFreshnessCheck depuis la SLA
# Dans ContractExtractor.extract, après la boucle tables, si schema_dict["contract"]["sla"]["max_latency"]
# est présent ET parsable (suffixe h/m/d/s) :
sla = (schema_dict.get("contract") or {}).get("sla") or {}
max_latency = sla.get("max_latency")
if max_latency:
    contracts.append(LoadFreshnessCheck(table=first_table_fqn, max_delay=str(max_latency),
                                        severity="critical"))
# store/clock injectés par l'appelant du monitor (comme aujourd'hui) — non fournis ici.
```
```python
# odcs.py — slaProperties + status lifecycle
sla_props = []
if schema.contract_sla:
    if schema.contract_sla.refresh_frequency:
        sla_props.append({"property": "refreshFrequency", "value": schema.contract_sla.refresh_frequency})
    if schema.contract_sla.max_latency:
        sla_props.append({"property": "latency", "value": schema.contract_sla.max_latency})
document["slaProperties"] = sla_props
# status ODCS : préférer le lifecycle du contrat s'il est déclaré, sinon fallback historique.
document["status"] = schema.contract_status or definition.status.lower()
```

### Comportement & règles
- **Hash (arbitrage §7.3)** : `sla` et `security` entrent dans le payload (donc dans le hash) ; `status`,
  `reviewers`, `effective_from`, `effective_until` **n'y entrent pas** (métadonnées de cycle de vie, un
  changement de statut ne doit pas invalider une certification).
- **`CANONICALIZATION_VERSION = 2`** : tout nouveau hash porte `canonicalization_version: 2`. Les
  `ContractDefinition` persistés en version 1 restent **lisibles** (le champ est stocké). Voir slice de
  risque : la constante ne recalcule jamais un vieux hash, elle marque simplement les nouveaux.
- **Gate lifecycle** : `deprecated` ⇒ WARN (utilisable, signalé) ; `now < effective_from` ⇒ DENY
  (`NOT_YET_EFFECTIVE`) ; `now > effective_until` ⇒ DENY (`EXPIRED_WINDOW`). Fail-closed sur `now` naïf.
- **`evaluate_lifecycle` est séparée de `evaluate`** (gate de certification Plan 29) : on ne modifie pas
  la signature ni le comportement de `evaluate()` (utilisée par `SemanticEngine`/`GenBIAgent`).
- **Rétrocompat** : un `contract:` sans ces clés ⇒ `contract_status="active"`, tout le reste `None`/vide,
  `sla`/`security` = `None` dans le payload → **même hash qu'avant la slice UNIQUEMENT si version 1**.
  ⚠ Comme la version passe à 2, le hash d'un contrat existant **change** (le champ
  `canonicalization_version` est dans le payload). C'est **voulu et documenté** (voir Risques). Le test
  de non-régression vérifie que la version 1 reste **recalculable/lisible**, pas que le hash v2 == v1.

### Cas de test — `tests/test_schema_loader.py`, `tests/test_certification.py`, `tests/test_access_policy.py`, `tests/test_contracts.py`, `tests/test_odcs_export.py`
- `test_status_change_does_not_change_hash` : deux YAML identiques sauf `status: active` vs
  `status: deprecated` → **même** `definition_hash` (status hors hash).
- `test_reviewers_and_dates_out_of_hash` : ajouter `reviewers`/`effective_*` ne change pas le hash.
- `test_sla_change_produces_new_hash` : modifier `sla.max_latency` → hash différent.
- `test_security_change_produces_new_hash` : modifier `security.level` → hash différent.
- `test_canonicalization_version_is_2` : `canonicalize_contract(...).canonicalization_version == 2` et
  `json.loads(canonical_json)["canonicalization_version"] == 2`.
- `test_gate_warns_on_deprecated` : `evaluate_lifecycle(status="deprecated", ...)` → `WARN`,
  reasons contient `"DEPRECATED"`.
- `test_gate_denies_before_effective_from` / `test_gate_denies_after_effective_until` → `DENY` +
  raison dédiée. (Pas de spark ; `now` fixe timezone-aware.)
- `test_gate_allows_active_within_window` → `ALLOW`.
- `test_invalid_status_refused` : `status: retired` → `ValueError` au load.
- `test_effective_from_after_until_refused` : from > until → `ValueError`.
- `test_sla_derives_load_freshness_check` (`tests/test_contracts.py`) : `sla.max_latency: 12h` ⇒
  `ContractExtractor.extract` renvoie un `LoadFreshnessCheck(table=..., max_delay="12h", severity="critical")`.
- `test_odcs_sla_properties_filled` (`tests/test_odcs_export.py`) : `document["slaProperties"]` contient
  `refreshFrequency`/`latency` ; `document["status"]` reflète le lifecycle (`"deprecated"`).
- **`test_legacy_v1_hashes_remain_readable`** (non-régression, `tests/test_certification.py`) : voir
  slice Risques — construire un `ContractDefinition(..., canonicalization_version=1, definition_hash=<hash v1 gelé>)`
  et vérifier qu'il est lisible (round-trip d'attributs) sans exception, et que le hash v1 gelé est
  reproductible par une fonction de recalcul en version 1 figée dans le test.

### Commit
`feat(plan31-3.3): contract lifecycle (status/reviewers/effective window), SLA and security; hash v2`
CHANGELOG `### Added` + `### Changed` :
- Added : `The \`contract:\` block gains lifecycle metadata (\`status\`: draft/active/deprecated, \`reviewers[]\`, \`effective_from\`/\`effective_until\`), an \`sla\` (\`refresh_frequency\`/\`max_latency\`) and a \`security\` (\`level\`/\`access_policy\`) block. A \`LoadFreshnessCheck\` is derived from the SLA, and \`access_policy.evaluate_lifecycle()\` warns on deprecated contracts and denies reads outside the effective window.`
- Changed : `The contract canonicalization version moved from 1 to 2: \`sla\` and \`security\` are now part of the contract hash, while \`status\`, \`reviewers\` and the effective dates are deliberately excluded. Version-1 definition hashes remain readable; a documentation- or lifecycle-only change never invalidates a certification.`

### DoD
`status`/`reviewers`/dates hors hash ; `sla`/`security` dans le hash ; version = 2 ; anciens hashs
lisibles (test) ; gate WARN deprecated / DENY hors fenêtre ; `LoadFreshnessCheck` dérivé ; ODCS
`slaProperties` + status lifecycle ; JSON Schema décrit tous les champs ; suite verte.

---

## Slice 31.3.4 — Import ODCS 3.1 + CLI `skifer contract import`

### Objectif
`import_odcs_31(doc) -> {data_product, contract, warnings}`, symétrique de `export_odcs_31` sur les
champs mappés. CLI `skifer contract import FILE` qui imprime le bloc YAML (`data_product:` + `contract:`).

### Fichiers
- `src/skifer/observability/odcs.py` — **modification** : `import_odcs_31`.
- `src/skifer/cli.py` — **modification** : sous-parseur `contract` + `import`.

### Signatures Python
```python
# odcs.py
@dataclass(frozen=True)
class OdcsImport:
    data_product: dict[str, Any]     # {id, version, owner?, description?, domain?}
    contract: dict[str, Any]         # {output: {...}, grain?, status?, sla?, security?}
    warnings: tuple[str, ...]
    def __post_init__(self):
        object.__setattr__(self, "warnings", tuple(self.warnings))

def import_odcs_31(doc: dict[str, Any]) -> OdcsImport:
    """Reconstruit les blocs YAML Skifer depuis un DataContract ODCS 3.1.

    Mappe : id/version/status ; schema.properties[].{name,logicalType->logical_type,description,
    classification} -> contract.output ; quality[type=unique/required] -> required/unique + grain
    (unique single-field -> grain candidate) ; team[] -> data_product.owner (string si 1 nom simple,
    sinon mapping {team/steward/contact}) ; slaProperties[refreshFrequency/latency] -> contract.sla ;
    customProperties['skifer.domain'] -> data_product.domain.
    Tout champ ODCS non mappé (roles, servers, customProperties inconnus, etc.) est reporté en warnings,
    JAMAIS silencieusement ignoré (symétrie stricte avec la politique loss-aware de l'export).
    Validation minimale : apiVersion commence par 'v3.', kind == 'DataContract', id et version présents.
    """
```
```python
# cli.py — nouveau parseur (dans main(), après le bloc adaptive)
contract_parser = subparsers.add_parser("contract", help="Data-contract utilities (Plan 31).")
contract_sub = contract_parser.add_subparsers(dest="contract_command")
contract_import = contract_sub.add_parser("import", help="Import an ODCS 3.1 DataContract and print the Skifer YAML block.")
contract_import.add_argument("file", metavar="FILE", help="Path to an ODCS 3.1 YAML/JSON document.")

# dispatch
elif args.command == "contract":
    _run_contract(args)

def _run_contract(args) -> None:
    sys.exit(run_contract_command(args))

def run_contract_command(args) -> int:
    """0 succès ; 1 erreur technique/validation ; 2 usage."""
    import yaml
    from skifer.observability.odcs import import_odcs_31
    if getattr(args, "contract_command", None) != "import":
        print("Contract command missing. Use 'skifer contract import FILE'."); return 2
    try:
        with open(args.file, "r", encoding="utf-8") as f:
            doc = yaml.safe_load(f)          # YAML est un sur-ensemble de JSON -> gère les deux
        result = import_odcs_31(doc)
    except (OSError, ValueError) as exc:
        print(f"Import failed: {exc}"); return 1
    block = {"data_product": result.data_product, "contract": result.contract}
    print(yaml.safe_dump(block, sort_keys=False, allow_unicode=True))
    for w in result.warnings:
        print(f"# warning: {w}")
    return 0
```

### Comportement & règles
- **Symétrie sur les champs mappés** : `export → import → export` idempotent sur `id`, `version`,
  `status`, `output` (nom/logical_type/classification/description), `grain` (single-field unique),
  `sla`. Les champs sans mapping (roles, `semantic`, `entity` par field côté export) sont **reportés**.
- **Loss-aware** (comme l'export) : tout ce qui n'est pas remonté produit un warning explicite.
- **Fail-closed CLI** : document illisible/non conforme ⇒ exit 1 avec message ; commande absente ⇒ 2.
- Le YAML imprimé est **directement collable** dans un pipeline (clés `data_product:`/`contract:`).

### Cas de test — `tests/test_odcs_import.py` (création), `tests/test_cli.py`
- `test_export_import_export_idempotent_on_mapped_fields` : partir d'un `ParsedSchema` (via
  `parse_schema`), `export_odcs_31`, `import_odcs_31`, ré-`export_odcs_31` du bloc réimporté et comparer
  les sous-dicts mappés (id/version/status/schema.properties/quality/slaProperties). (Pas de spark.)
- `test_import_reports_unmapped_fields` : doc avec `roles`/`servers` → présents dans `warnings`.
- `test_import_rejects_non_datacontract` : `kind: DataProduct` → `ValueError`.
- `test_import_team_single_name_becomes_string_owner` et `test_import_team_multi_becomes_mapping`.
- `test_import_sla_roundtrip` : slaProperties → `contract.sla.max_latency`/`refresh_frequency`.
- `test_cli_contract_import_prints_yaml_block` (`tests/test_cli.py`) : exécuter `run_contract_command`
  sur une fixture temporaire, capturer stdout, vérifier que `yaml.safe_load` du stdout contient
  `data_product` + `contract`. Exit code 0. (Écrire la fixture dans `tmp_path`.)
- `test_cli_contract_import_missing_file_exit_1` et `test_cli_contract_missing_subcommand_exit_2`.

### Commit
`feat(plan31-3.4): import_odcs_31 and 'skifer contract import' CLI`
CHANGELOG `### Added` :
- `\`import_odcs_31(doc)\` reconstructs the Skifer \`data_product:\`/\`contract:\` YAML blocks from an ODCS 3.1 DataContract (symmetric to the export, loss-aware: unmapped fields are reported, never dropped), exposed through the new \`skifer contract import FILE\` command.`

### DoD
`export→import→export` idempotent sur les champs mappés ; champs non mappés reportés ; CLI imprime un
bloc YAML collable ; codes de sortie 0/1/2 ; suite verte.

---

## Slice 31.3.5 — `diff_contracts` + intégration `semantic sync` + GovernanceService

### Objectif
`diff_contracts(a, b) -> ContractDiff(added, removed, retyped, required_changed, classification_changed,
sla_changed, breaking: bool)`. Utilisé par `semantic sync` (reporting) et exposé par `GovernanceService`.

### Fichiers
- `src/skifer/observability/certification.py` — **modification** : `ContractDiff` + `diff_contracts`.
- `src/skifer/semantic/sync.py` — **modification** : appel de `diff_contracts` pour le reporting.
- `src/skifer/services/governance.py` — **création ou modification** : méthode `diff_contracts`.

### Signatures Python
```python
# certification.py
@dataclass(frozen=True)
class ContractDiff:
    added: tuple[str, ...] = ()                    # colonnes présentes dans b, absentes de a
    removed: tuple[str, ...] = ()                  # colonnes présentes dans a, absentes de b
    retyped: tuple[tuple[str, str, str], ...] = () # (colonne, logical_type_a, logical_type_b)
    required_changed: tuple[tuple[str, bool, bool], ...] = ()      # (col, required_a, required_b)
    classification_changed: tuple[tuple[str, str, str], ...] = ()  # (col, class_a, class_b)
    sla_changed: bool = False
    breaking: bool = False

def diff_contracts(a: ParsedSchema, b: ParsedSchema) -> ContractDiff:
    """Compare deux contrats (a = ancien, b = nouveau) au niveau contract.output + sla.

    breaking est True si l'une de ces conditions (au moins) :
      - removed non vide (une colonne disparaît)
      - retyped non vide (changement de logical_type)
      - un champ passe required False->True (durcissement) OU un abaissement de classification
        (b < a au sens CLASSIFICATION_RANK) — une déclassification est breaking pour la gouvernance
      - sla_changed avec RELÂCHEMENT (max_latency de b > max_latency de a quand parsable h/m/d/s)
    NB : élévation de classification et resserrement de SLA sont des changements NON breaking (plus stricts).
    """
```
> Détails d'implémentation : itérer sur `{f.name: f}` de `a.contract_output` et `b.contract_output`.
> Pour `classification`, comparer via `CLASSIFICATION_RANK` (None traité comme `public`/rang 0).
> Pour `sla_changed`, comparer `a.contract_sla` vs `b.contract_sla` (tuple des deux champs) ; le
> caractère "relâchement" pour `breaking` se juge sur `max_latency` parsé par `FreshnessCheck._parse_delay`
> quand les deux valeurs ont un suffixe h/m/d/s (sinon : tout changement de SLA marque `sla_changed=True`
> mais n'est breaking que si on ne peut pas prouver un resserrement → **fail-closed : SLA changée non
> comparable ⇒ breaking=True**).

```python
# services/governance.py  (créé par 31.1.4 ; si absent, créer ce stub minimal)
from __future__ import annotations
from skifer.core.ir import ParsedSchema
from skifer.observability.certification import ContractDiff, diff_contracts

class GovernanceService:
    """Frontière applicative pour les opérations de gouvernance (Plan 31)."""
    def diff_contracts(self, a: ParsedSchema, b: ParsedSchema) -> ContractDiff:
        return diff_contracts(a, b)
```
```python
# sync.py — reporting only (n'altère PAS safe_to_apply)
# Là où le SyncReport est construit pour un pipeline qui porte un contract.output, calculer le diff
# entre la génération précédente et la nouvelle et l'ajouter aux suggestions/changes reportés.
# Contrainte : diff_contracts ne DÉCIDE de rien côté sync (report-first) — il enrichit le rapport.
```

### Comportement & règles
- **Catégories** : `added`, `removed`, `retyped`, `required_changed`, `classification_changed`,
  `sla_changed` — chacune indépendamment peuplée.
- **`breaking`** True sur : removal, retype, durcissement `required` (F→T), **abaissement** de
  classification (déclassification), **relâchement** de SLA (latence max plus grande), et SLA modifiée
  **non comparable** (fail-closed).
- **Non breaking** : ajout de colonne, `required` T→F, **élévation** de classification, resserrement SLA.
- **Reporting only dans sync** : `diff_contracts` ne modifie pas `safe_to_apply` ; il documente le
  rapport (le contrat CI des codes de sortie de `semantic sync` reste inchangé).

### Cas de test — `tests/test_certification.py` (ou `tests/test_contract_diff.py` création), `tests/test_governance.py` (création), `tests/test_semantic_sync.py`
- `test_diff_added_removed_retyped` : chaque catégorie peuplée sur des exemples ciblés.
- `test_diff_required_changed` : F→T et T→F remontés ; F→T ⇒ `breaking`.
- `test_diff_classification_changed_downgrade_is_breaking` : `restricted → internal` ⇒ `breaking`.
- `test_diff_classification_upgrade_not_breaking` : `internal → restricted` ⇒ pas `breaking`.
- `test_diff_breaking_on_removal` / `test_diff_breaking_on_retype`.
- `test_diff_sla_relaxation_is_breaking` : `max_latency 12h → 24h` ⇒ `sla_changed` + `breaking`.
- `test_diff_sla_tightening_not_breaking` : `24h → 12h` ⇒ `sla_changed`, pas `breaking`.
- `test_diff_sla_non_comparable_is_breaking` : `daily → weekly` (non parsable) ⇒ `breaking` (fail-closed).
- `test_governance_service_exposes_diff` (`tests/test_governance.py`) : `GovernanceService().diff_contracts(a,b)`
  renvoie le même `ContractDiff` que la fonction pure.
- `test_semantic_sync_reports_contract_diff` (`tests/test_semantic_sync.py`) : un pipeline dont le
  contrat évolue produit un rapport contenant le diff (sans changer `safe_to_apply`).
- (Tous sans spark : `ParsedSchema` construit via `parse_to_ir(parse_schema(...))`.)

### Commit
`feat(plan31-3.5): diff_contracts with breaking detection, wired into semantic sync and GovernanceService`
CHANGELOG `### Added` :
- `\`diff_contracts(a, b)\` reports contract deltas (added/removed/retyped/required/classification/SLA) and flags breaking changes (column removal, retype, \`required\` hardening, classification downgrade, SLA relaxation or any non-comparable SLA change). It is surfaced in \`semantic sync\` reporting and exposed by \`GovernanceService\`.`

### DoD
Chaque catégorie testée ; `breaking` correct sur removal/retype/downgrade/SLA-relaxation + fail-closed
SLA non comparable ; exposé par `GovernanceService` ; intégré au reporting `semantic sync` sans changer
son contrat CI ; suite verte.

---

## Ordre & dépendances internes

```
3.1 (taxonomie + constants CLASSIFICATION_LEVELS + propagation + evidence)
      │  fournit CLASSIFICATION_LEVELS/RANK (constants) et resolve_field_classifications
      ▼
3.2 (ownership : owner string|mapping, domain, ODCS team, UC tags)   — indépendant de 3.1, mais MÊME
      │  fichier schema_loader/ir/json_schema/odcs → séquencer après 3.1 pour éviter les conflits d'édition
      ▼
3.3 (lifecycle + sla/security + hash v2 + gate)   — dépend de 3.1 (CLASSIFICATION pour rien ici) mais
      │  surtout doit venir APRÈS 3.2 car 3.2 touche déjà le payload owner_label de canonicalize_contract
      ▼
3.4 (import ODCS + CLI)   — dépend de 3.2 (team/owner mapping) et 3.3 (sla/status) pour la symétrie
      │
      ▼
3.5 (diff_contracts + sync + GovernanceService)   — dépend de 3.1 (CLASSIFICATION_RANK), 3.3 (sla)
```
- **Ordre imposé : 3.1 → 3.2 → 3.3 → 3.4 → 3.5.** (3.4 et 3.5 pourraient être parallèles, mais 3.4
  n'apporte rien à 3.5 ; les faire séquentiels évite les conflits sur `odcs.py`/`certification.py`.)
- **Forward-coupling 31.2 (`ColumnRecord`)** : 3.1 expose `resolve_field_classifications` (fonction pure)
  précisément pour que 31.2 peuple `ColumnRecord.classification` avec la classification **effective**
  (déclarée ou propagée), sans réimplémenter la propagation. Ne pas inliner cette logique dans un
  consommateur.
- **Forward-coupling 31.7 (strict flip)** : la propagation prend un paramètre `mode="warn"|"strict"`.
  31.7 (audit) mesurera la couverture puis basculera le défaut/flag en `"strict"` — **ne pas** coder en
  dur le `warn`, garder le paramètre et un point de configuration unique.
- **GovernanceService (31.1.4)** : `services/governance.py` est censé exister via 31.1.4. Si la Feature
  31.1 n'est pas encore livrée au moment de 3.5, créer le **stub minimal** décrit en 3.5 (une classe
  `GovernanceService` avec la seule méthode `diff_contracts`), que 31.1.4 étoffera ensuite.

---

## Risques & pièges

### 1. Changement de canonicalisation du hash (le point délicat — arbitrage §7.3)
- **Ce qui change** : `CANONICALIZATION_VERSION` passe de 1 à 2, et le payload gagne
  `contract.sla` + `contract.security`. **Conséquence directe** : *tout* contrat existant (même sans
  bloc `sla`/`security`) voit son `definition_hash` **changer**, parce que `canonicalization_version: 2`
  fait partie de la charge hachée et parce que `contract.sla=None`/`contract.security=None` sont ajoutés
  aux clés. **C'est attendu.** Un re-hash de tous les contrats a lieu au prochain `canonicalize_contract`.
- **Lisibilité des anciens hashs** : un `ContractDefinition` persisté porte déjà
  `canonicalization_version` et `definition_hash`. On ne re-hache **jamais** un enregistrement stocké :
  on le lit tel quel. Il faut donc s'assurer que **rien** dans le code ne compare un hash v1 stocké à un
  hash v2 recalculé comme s'ils étaient de même nature (le préfixe/version est le discriminant, comme
  pour `sql_hash` `sha256:v1:` dans `evidence.py`). Vérifier les consommateurs de `definition_hash`
  (`certification_store`, `uc_mirror`, `odcs.customProperties`, publication) : ils stockent/relaient la
  valeur + la version, ils ne la recomputent pas → OK, mais **le confirmer** en implémentant 3.3.
- **Test de non-régression obligatoire** (`tests/test_certification.py`) :
  - Geler la chaîne canonique v1 et son hash dans le test (copier la fonction de payload v1 **dans le
    test**, indépendante de la constante courante), calculer le hash v1 attendu à partir de `BASE_YAML`,
    et vérifier qu'un `ContractDefinition(canonicalization_version=1, definition_hash=<v1>)` est **lisible**
    (attributs round-trip, `hash_algorithm == "sha256"`).
  - Vérifier `canonicalize_contract(BASE_YAML).canonicalization_version == 2` et que le hash v2 **diffère**
    du hash v1 gelé (preuve que la bascule est effective, pas silencieuse).
  - Vérifier qu'un YAML **sans** `sla`/`security` produit un payload avec `sla: null`/`security: null`
    (et donc un hash stable d'un run à l'autre en v2).
- **Piège d'édition dans `schema_loader`** : le bloc contract réémet aujourd'hui `{"output":..., "grain":...}`
  et **écrase** le reste. Si on n'ajoute pas explicitement `status/reviewers/dates/sla/security` à ce
  dict normalisé, ils n'atteignent jamais `parse_to_ir` → hash sans SLA malgré une SLA déclarée. À
  couvrir par `test_sla_change_produces_new_hash` (échouerait si le bug était présent).

### 2. `owner` mapping et `ContractDefinition.owner`
`ContractDefinition.owner: str | None` et `odcs.team[{"name": definition.owner}]` supposent une string.
Si on laisse `ParsedDataProduct.owner` être un `ParsedOwner`, `canonicalize_contract` doit lire
`owner_label` (string) — sinon `ContractDefinition.owner` devient un objet non sérialisable et `odcs`
casse. Test `test_owner_string_and_mapping_same_definition` protège ce point.

### 3. `ParsedOutputField(**metadata)` et clés inconnues
`parse_to_ir` fait `ParsedOutputField(name=name, **metadata)`. Toute nouvelle clé autorisée par
`_OUTPUT_FIELD_ALLOWED_KEYS` **doit** exister comme champ du dataclass. En 3.1/3.3 on n'ajoute **pas** de
clé par-field (classification existe déjà ; sla/security sont au niveau contract, pas field) — donc pas
de risque ici, mais le noter pour toute extension future.

### 4. `evaluate_lifecycle` vs `evaluate` (gate Plan 29)
Ne **pas** fusionner : `evaluate()` pilote le gate de certification sémantique (Plan 29) consommé par
`SemanticEngine`/`GenBIAgent`, avec ses propres modes/raisons. `evaluate_lifecycle()` est une fonction
distincte, sans impact sur les chemins existants. Le mélange briserait la rétrocompat du gate sémantique.

### 5. JSON Schema `additionalProperties: False`
Chaque nouveau champ (`domain`, `status`, `reviewers`, `effective_*`, `sla`, `security`, owner objet)
**doit** être décrit dans le `$def` correspondant, sinon un YAML valide au load serait rejeté par un
validateur JSON Schema externe. Acceptance : « le JSON Schema décrit tous les nouveaux champs. »

### 6. Import ODCS et YAML/JSON
`yaml.safe_load` gère JSON (sur-ensemble) : un ODCS `.json` passe. Mais un import qui devine des champs
Skifer non exprimables (entity par field, semantic seed) doit **reporter** en warnings, jamais fabriquer
de valeur — cohérent avec l'export loss-aware.
