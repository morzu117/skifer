# S01 — Registre des candidates et historique de certification

> **Décision :** D2 du plan parent (« la structure, pas les valeurs »)
> **Dépendances :** aucune
> **Slices / commits :** 4
> **Branche :** `feat/plan44-s01-candidate-registry`
> **Résultat :** un registre testé où tout ce qui suit écrit — candidates, exécutions,
> historique append-only, questions de référence — sans jamais persister une valeur de filtre.

## 1. Contexte

`adaptive/` stocke des événements d'usage **sans question, sans SQL, sans valeur** et doit le
rester. Le cycle de certification a besoin de la question d'origine et de la structure de la
requête : il lui faut un second store, avec sa propre classification (`internal`). Le patron à
suivre est celui d'`adaptive/store.py` (Protocol + `Sqlite*` + `Delta*` sur primitives
`SparkBackend`) et d'`adaptive/models.py` (dataclasses figées, validation à la construction,
fingerprint `sha256:v1:` canonique).

Ce lot ne contient **aucune logique métier** : ni agent, ni validation, ni promotion. Seulement
des types, des transitions autorisées, des stores et la redaction.

## 2. Contrat

### 2.1 Package `skifer.candidates`

```python
# candidates/models.py
CANDIDATE_STATES = (
    "candidate", "validated", "needs_review", "promoted",
    "under_review", "demoted", "rejected",
)
ALLOWED_TRANSITIONS: dict[str, frozenset[str]] = {
    "candidate":    frozenset({"validated", "needs_review", "rejected"}),
    "validated":    frozenset({"promoted", "needs_review", "rejected"}),
    "needs_review": frozenset({"promoted", "rejected"}),
    "promoted":     frozenset({"under_review", "demoted"}),
    "under_review": frozenset({"promoted", "demoted"}),
    "demoted":      frozenset({"candidate"}),
    "rejected":     frozenset(),
}
EVENT_TYPES = (
    "created", "validated", "needs_review", "promoted", "under_review", "demoted",
    "rejected", "replaced", "reactivated", "certification_requested", "certified",
)

def check_transition(previous: str | None, new: str) -> None   # ValueError si interdit

@dataclass(frozen=True)
class MetricCandidate:
    candidate_id: str                 # "candidate:v1:<sha256>" — dérivé de fingerprint
    metric_name: str                  # ^[A-Za-z_][A-Za-z0-9_]*$
    metric_description: str | None
    origin_question: str | None       # None en mode redacted
    origin_question_hash: str         # "sha256:v1:<…>" de la question normalisée, toujours
    schema_dict: dict                 # valeurs de filtre paramétrées ({{ p1 }}, …)
    sql_hash: str                     # sha256:v1: du SQL compilé (normalisation de evidence.py)
    fingerprint: str                  # fingerprint_candidate(schema_dict)
    tables_used: tuple[str, ...]
    columns_used: tuple[str, ...]
    joins_used: tuple[tuple[str, str, str], ...]     # (left, right, key)
    business_rules_refs: tuple[str, ...]
    residues: tuple[dict, ...]        # [{"code","message","blocking"}]; vide = auto-promouvable
    state: str
    request_count: int                # >= 1
    distinct_requesters: tuple[str, ...]   # pseudonymes ; vide si pseudonymisation indisponible
    stability_score: float | None     # [0, 1]
    coherence: dict | None            # {"verdict": bool, "reasons": [...]}
    trace_id: str | None
    reviewer: str | None              # "human:<pseudonyme>" du dernier arbitre, None sinon
    published_view: str | None
    published_metric_view: str | None
    created_at: datetime              # aware
    updated_at: datetime              # aware
    last_requested_at: datetime | None

@dataclass(frozen=True)
class CandidateRun:
    run_id: str
    candidate_id: str                 # "candidate:v1:…", ou "reference_set:<version>" pour un
                                      # rapport d'évaluation (S09) — aucune autre forme
    executed_at: datetime
    purpose: str                      # "answer" | "stability" | "evaluation"
    result_hash: str | None
    row_count: int | None
    duration_ms: int | None
    trace_id: str | None
    error_type: str | None            # nom de classe seulement, jamais le message

@dataclass(frozen=True)
class CertificationEvent:
    event_id: str                     # uuid4 hex
    metric_id: str                    # candidate_id, ou "model:<model_key>.<metric>" pour un certifié
    metric_name: str | None
    event_type: str                   # EVENT_TYPES
    previous_state: str | None
    new_state: str
    decided_by: str                   # "human:<user>" | "agent:<chain>" | "system:<detector>"
    justification: str | None
    evidence: dict                    # chiffres au moment de la décision ; JSON-natif
    replaced_by_metric: str | None
    replaces_metric: str | None
    semantic_layer_version: str | None   # sha256:v1: de semantic_catalog.yaml
    trace_id: str | None
    event_at: datetime

@dataclass(frozen=True)
class ReferenceQuestion:
    question_id: str
    question: str
    family: str                       # "A" | "B" | "C"
    expected: dict                    # A: {model_key, metric} ; B: {schema_dict, result_hash} ; C: {outcome: refuse|clarify}
                                      # une colonne JSON `expected` plutôt que les colonnes à plat du
                                      # parent : la forme dépend de la famille, et S09 la valide
    source: str                       # "yaml_bootstrap" | "promoted" | "rejected" | "human"
    semantic_layer_version: str | None
    created_at: datetime

def fingerprint_candidate(schema_dict: dict) -> str     # "sha256:v1:<hex>"
def candidate_id_for(fingerprint: str) -> str           # "candidate:v1:<hex>"
```

Chaque dataclass a `to_dict()` par **allowlist champ par champ** (pas `asdict`) et une fonction
`*_from_row(row)` symétrique.

`fingerprint_candidate` : JSON canonique (`sort_keys`, séparateurs compacts) d'une forme
normalisée du `schema_dict` où (a) les listes à ordre non sémantique sont triées (`tables` par
alias, `filter` d'une table, `group_by`, `measures` par `target`), (b) **chaque valeur de filtre
est remplacée par `"?"`**, (c) `dev_limit` et les clés de présentation (`description`) sont
retirées. Deux requêtes qui ne diffèrent que par une valeur de filtre ont le **même**
fingerprint : c'est la même métrique.

### 2.2 Redaction

```python
# candidates/redaction.py
def parameterize_filters(schema_dict: dict) -> tuple[dict, dict[str, Any]]
    # → (schema_dict avec {{ p1 }}, {{ p2 }}… à la place des valeurs, {"p1": valeur, …})
    # le second élément n'est JAMAIS persisté : il sert à la ré-exécution immédiate seulement
def question_hash(question: str) -> str                 # sha256:v1: de la question normalisée
def requester_pseudonym(user_id: str | None, policy: TraceAttributePolicy) -> str | None
    # délègue à policy.pseudonymize ; None si pas de secret HMAC
```

Les filtres concernés : `tables[].filter`, `tables[].filter_groups`, `where`, `where_groups`,
`aggregate.having`, sous leurs deux formes (chaîne `col:op:val` et dict). Les opérateurs sans
valeur (`is_null`, `is_not_null`) restent intacts. L'opérateur `sql` est **refusé** par
`parameterize_filters` (`ValueError`) : une candidate n'en contient jamais (S03 le garantit en
amont).

### 2.3 Stores

```python
# candidates/store.py
class CandidateStore(Protocol):
    def upsert_candidate(self, candidate: MetricCandidate) -> None
    def get_candidate(self, candidate_id: str) -> MetricCandidate | None
    def find_by_fingerprint(self, fingerprint: str) -> MetricCandidate | None
    def list_candidates(self, *, state: str | None = None, limit: int | None = None) -> list[MetricCandidate]
    def append_run(self, run: CandidateRun) -> None
    def list_runs(self, candidate_id: str, *, purpose: str | None = None) -> list[CandidateRun]
    def append_event(self, event: CertificationEvent) -> None
    def list_events(self, *, metric_id: str | None = None,
                    since: datetime | None = None, until: datetime | None = None) -> list[CertificationEvent]
    def append_reference(self, question: ReferenceQuestion) -> None
    def list_references(self, *, family: str | None = None) -> list[ReferenceQuestion]

class SqliteCandidateStore:   # db_path=".skifer_candidates.db"
class DeltaCandidateStore:    # backend, schema="<catalog>._skifer_candidates"
def default_candidate_store(engine) -> CandidateStore
```

Tables (mêmes noms en SQLite et Delta) : `metric_candidates`, `metric_candidate_runs`,
`metric_certification_history`, `reference_questions`. Colonnes = champs des dataclasses ;
tuples et dicts sérialisés en JSON canonique (`STRING`/`TEXT`). Delta : création par
`CREATE TABLE IF NOT EXISTS … USING DELTA TBLPROPERTIES (delta.enableChangeDataFeed = true)`,
puis écriture ; `metric_candidates` en `MERGE INTO` sur `candidate_id` ; les trois autres en
append idempotent sur leur identifiant (patron `append_semantic_usage_event`).
`request_count` : défaut `1` posé par le code, **pas** par un `DEFAULT` de DDL (exigerait la
table feature `allowColumnDefaults`).

Il n'existe **aucune** méthode d'update ou de delete pour `metric_certification_history`, ni
dans le Protocol, ni dans les adaptateurs, ni dans les primitives `SparkBackend`.

### 2.4 Configuration (racine d'environnement, `config.yaml`)

```yaml
candidates_store_questions: full     # full (défaut) | redacted
candidates_retention_days: 365       # lu et validé ; aucune purge dans ce lot
```

## 3. Exigences

- **S01-R1** — `check_transition` DOIT refuser toute transition absente d'`ALLOWED_TRANSITIONS`,
  et `None → "candidate"` DOIT être la seule création autorisée.
- **S01-R2** — Les dataclasses DOIVENT refuser à la construction : datetime naïf, état ou type
  d'événement inconnu, `request_count < 1`, `stability_score` hors `[0, 1]`, `metric_name` non
  conforme à `^[A-Za-z_][A-Za-z0-9_]*$`, `family` hors `A/B/C`.
- **S01-R3** — `fingerprint_candidate` DOIT être identique pour deux `schema_dict` qui ne
  diffèrent que par l'ordre non sémantique ou par une valeur de filtre, et différent dès qu'une
  table, une jointure, une colonne, un opérateur ou un agrégat change. Il DOIT être stable sous
  n'importe quel `PYTHONHASHSEED`.
- **S01-R4** — Aucune valeur de filtre NE DOIT apparaître dans une ligne persistée, sous aucune
  des formes de filtre, dans aucun des deux stores.
- **S01-R5** — En mode `redacted`, `origin_question` DOIT être `None` en base et
  `origin_question_hash` renseigné.
- **S01-R6** — `metric_certification_history` NE DOIT exposer aucune opération de modification
  ou de suppression (test au niveau source sur `candidates/store.py` et sur les primitives
  `SparkBackend` ajoutées).
- **S01-R7** — Les deux stores DOIVENT faire un aller-retour sans perte de chaque type et de
  chaque champ (y compris tuples vides, `None`, datetimes aware).
- **S01-R8** — `upsert_candidate` DOIT être idempotent ; `append_*` DOIT ignorer un identifiant
  déjà présent (pas de doublon, pas d'erreur).
- **S01-R9** — `default_candidate_store` DOIT renvoyer SQLite pour un moteur local ou sans
  catalogue, Delta qualifié par le catalogue de l'environnement sinon (même règle que
  `default_certification_store`, à relire avant d'écrire).
- **S01-R10** — Le DDL Delta DOIT activer le Change Data Feed et NE DOIT PAS utiliser `DEFAULT`.
- **S01-R11** — Une chaîne échappée (apostrophe, antislash) dans un identifiant DOIT être
  correctement échappée dans le SQL Delta (patron de
  `test_delta_lookup_escapes_backslashes_and_apostrophes_in_sql`).

## 4. Fichiers

**À créer**

- `src/skifer/candidates/__init__.py` (exports publics)
- `src/skifer/candidates/models.py`
- `src/skifer/candidates/redaction.py`
- `src/skifer/candidates/store.py`
- `tests/test_candidates_models.py`
- `tests/test_candidates_redaction.py`
- `tests/test_candidates_store.py`

**À modifier**

- `src/skifer/core/spark_backend.py` — primitives `upsert_metric_candidate`, `get_metric_candidate`,
  `list_metric_candidates`, `append_candidate_row(schema, table, row, id_column)` (runs, historique,
  références), `list_candidate_rows(schema, table, **filters)`. Patron :
  `append_semantic_usage_event` / `list_semantic_usage_events`.
- `tests/fakes/fake_backend.py` — les mêmes primitives, en mémoire, pour les tests Delta sans Spark.
- `src/skifer/core/config.py` — lecture/validation de `candidates_store_questions` et
  `candidates_retention_days` (patron de `semantic_certification_policy`).
- `.gitignore` — `.skifer_candidates.db`.
- `CHANGELOG.md`.

## 5. Slices

**S01.1 — Modèles et transitions.** `models.py` complet, sans store.

**S01.2 — Fingerprint et redaction.** `fingerprint_candidate`, `candidate_id_for`,
`redaction.py`.

**S01.3 — Store SQLite.** Protocol + `SqliteCandidateStore` + config + `.gitignore`.

**S01.4 — Store Delta.** Primitives `SparkBackend` + fake + `DeltaCandidateStore` +
`default_candidate_store`. Un test Spark réel (fixture `spark`) fait un aller-retour complet sur
une table Delta locale et lit `delta.enableChangeDataFeed` dans `SHOW TBLPROPERTIES`.

## 6. Tests d'acceptation

| Exigence | Test | Prouve |
|---|---|---|
| S01-R1 | `test_candidates_models.py::test_transition_matrix` | table complète autorisé/interdit, paramétrée sur les 7×7 couples |
| S01-R2 | `…::test_models_reject_invalid_fields` | chaque refus listé, un cas par champ |
| S01-R3 | `test_candidates_redaction.py::test_fingerprint_ignores_order_and_filter_values` ; `…::test_fingerprint_changes_on_structure` ; `…::test_fingerprint_stable_across_hash_seeds` (sous-processus avec deux `PYTHONHASHSEED`) | |
| S01-R4 | `test_candidates_store.py::test_no_filter_value_is_persisted[sqlite|delta]` | valeurs canari (`"CANARY-7f3a"`) dans les 5 emplacements de filtre et les 2 formes ; aucune trace dans les lignes brutes |
| S01-R5 | `…::test_redacted_mode_stores_only_question_hash` | |
| S01-R6 | `…::test_history_store_has_no_update_or_delete` | inspection du source : aucun `UPDATE`, `DELETE`, `MERGE` ciblant `metric_certification_history`, aucune méthode `update_event`/`delete_event` |
| S01-R7 | `…::test_round_trip_every_field[sqlite|delta]` | |
| S01-R8 | `…::test_upsert_and_append_are_idempotent[sqlite|delta]` | |
| S01-R9 | `…::test_default_store_selection` | |
| S01-R10 | `…::test_delta_ddl_enables_cdf_without_defaults` (fake : SQL enregistré) ; `…::test_delta_round_trip_on_local_spark` (fixture `spark`) | |
| S01-R11 | `…::test_delta_escapes_identifiers` | |

## 7. Erreurs

| Cas | Type |
|---|---|
| transition interdite | `ValueError("Transition 'promoted' → 'candidate' is not allowed.")` |
| champ invalide | `ValueError` nommant le champ et la valeur (jamais la valeur d'un filtre) |
| opérateur `sql` dans `parameterize_filters` | `ValueError("Raw SQL filters cannot be stored in a candidate.")` |
| `candidates_store_questions` invalide | erreur de config au chargement, comme `semantic_certification_policy` |

## 8. Hors périmètre

- purge de rétention (la valeur est lue, rien n'est supprimé) ;
- toute logique de validation, de promotion, de routage ;
- écrire depuis `adaptive/` ou modifier `adaptive/` ;
- exposer ces tables par MCP ou par l'API.

## 9. Definition of Done

- les 11 exigences ont leur test vert, SQLite **et** Delta (fake + un test Spark réel) ;
- le cœur s'importe sans Spark (`import skifer.candidates` dans un test sans fixture `spark`) ;
- `tests/test_adaptive_*` inchangés et verts.
