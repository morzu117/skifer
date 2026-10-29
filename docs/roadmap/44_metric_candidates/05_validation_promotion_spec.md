# S05 — Chaîne de validation et promotion de niveau 1

> **Décision :** D1, niveau 1 (publication automatique **non certifiée**)
> **Dépendances :** S03 mergée (et donc S00, S01, S02)
> **Slices / commits :** 4
> **Branche :** `feat/plan44-s05-validation-promotion`
> **Résultat :** `skifer candidates validate` juge chaque candidate sur trois critères chiffrés.
> Il publie celles qui passent en vue SQL et en metric view, marquées CANDIDATE, et il range les
> autres en `needs_review`.

## 1. Contexte

C'est la décision 11 du brief, cohérence, fréquence et stabilité, dans la forme explicable
d'`adaptive` : chaque critère rend un verdict, des raisons nommées, la valeur observée et le
seuil, comme `RecommendationRefusal`. La promotion de niveau 1 réutilise la traduction du
Plan 42 (`semantic/metric_views.py` : `metric_view_ddl(model, catalog, schema)`, `view_name`,
`source_table`, `MetricViewTranslationError`) sur une **vue** construite depuis le `schema_dict`.

**Risque R1 à lever en premier :** une metric view accepte-t-elle une vue comme `source` ? La
slice S05.1 commence par un test live **manuel**, documenté dans la PR et jamais automatisé
(aucun workspace en CI), sur `sts_demo_catalog`. Si c'est non, le repli prévu par le parent
s'applique : le niveau 1 ne publie que la vue SQL, et `published_metric_view` reste `None`.
Cette spec couvre les deux branches, qui se sélectionnent par la constante
`LEVEL1_PUBLISHES_METRIC_VIEW`, réglée selon le résultat du test et justifiée dans le commit.

## 2. Contrat

### 2.1 Validation

```python
# candidates/validator.py
@dataclass(frozen=True)
class CriterionResult:
    criterion: str                 # "coherence" | "frequency" | "stability"
    passed: bool
    reasons: tuple[dict, ...]      # {"code", "observed", "threshold", "verdict"}

@dataclass(frozen=True)
class ValidationReport:
    candidate_id: str
    results: tuple[CriterionResult, CriterionResult, CriterionResult]
    outcome: str                   # "validated" | "needs_review" | "pending"
    semantic_layer_version: str    # sha256:v1: de semantic_catalog.yaml au moment du jugement

@dataclass(frozen=True)
class ValidationThresholds:
    min_requests: int = 5
    min_distinct_requesters: int = 2
    min_days: int = 2
    stability_runs: int = 3
    min_stability: float = 1.0

class CandidateValidator:
    def __init__(self, store, engine, index, *, thresholds: ValidationThresholds, clock): ...
    def validate(self, candidate_id: str, *, execute: bool = True) -> ValidationReport
```

Les critères :

- **Cohérence.**
  - `parse_schema` passe ;
  - `residues` est vide (`residue_present`) ;
  - chaque jointure correspond à une arête `joins`/`shortcut` de l'index ou à une `relationship`
    d'un modèle (`undeclared_join`) ;
  - pas de fanout ;
  - chaque table existe encore (`missing_table`) ;
  - le gate de certification de l'environnement autorise les sources (`source_not_certified`).
- **Fréquence.**
  - `request_count ≥ min_requests` ;
  - `len(distinct_requesters) ≥ min_distinct_requesters` ;
  - au moins `min_days` jours calendaires UTC distincts parmi les runs `answer`.

  Si la pseudonymisation est indisponible, `distinct_requesters` est vide : le critère est
  `passed=False` avec la raison `requesters_unavailable`, jamais un succès par défaut.
- **Stabilité.** `stability_runs` exécutions (`purpose="stability"`) de
  `metric_definition(schema_dict)`, c'est-à-dire ce qui sera publié, et pas la question d'origine
  avec ses valeurs, qui ne sont pas stockées. `stability_score` est la part des `result_hash`
  égaux au plus fréquent. Un résultat non déterministe, par exemple un `first`/`last` sans ordre
  ou une source qui change entre deux runs, fait baisser le score. C'est exactement ce qu'on veut
  voir avant de publier.

Le résultat :

- les trois critères passent → `validated` ;
- la cohérence échoue → `needs_review` ;
- la fréquence n'est pas encore atteinte → `pending` : la candidate reste `candidate`, ce n'est
  pas un échec ;
- la stabilité échoue → `needs_review`.

### 2.2 Promotion de niveau 1

```python
# candidates/promotion.py
LEVEL1_PUBLISHES_METRIC_VIEW: bool          # fixé par S05.1 selon R1
CANDIDATES_SCHEMA_SUFFIX = "_candidates"

def metric_definition(schema_dict: dict) -> tuple[dict, tuple[str, ...]]
    # un schema_dict stocké porte des filtres paramétrés ({{ pN }}) issus d'UNE question :
    # ils ne peuvent pas définir une métrique publiée. Chaque filtre paramétré est retiré et sa
    # colonne ajoutée à aggregate.group_by (elle devient une dimension). Les filtres sans valeur
    # (is_null…) restent. Renvoie (définition, colonnes converties, triées). Fonction pure,
    # partagée avec S06 — c'est elle qui garantit R7 (niveau 1 ≡ niveau 2).
def level1_view_ddl(candidate, catalog, schema) -> str
    # CREATE OR REPLACE VIEW `<cat>`.`<schema>_candidates`.`<metric_name>`
    # COMMENT 'skifer candidate <candidate_id> sql_hash=<…>'
    # AS <compile_select(parse_to_ir(metric_definition(schema_dict)[0]))>
def level1_model(candidate) -> dict
    # modèle sémantique mono-table sur la vue : la métrique, les dimensions = group_by
def promote_level1(candidate, *, backend, catalog, schema, dry_run=False) -> tuple[str, ...]
    # DDL émis, dans l'ordre : CREATE SCHEMA IF NOT EXISTS, vue, [metric view], tags
```

Le tag `skifer.certification = CANDIDATE` est posé sur la vue, et sur la metric view si elle est
publiée, par `ALTER VIEW … SET TAGS`, comme le miroir de tags du Plan 41.

En local (`catalog: null`), la vue est créée en Spark SQL dans `<schema>_candidates` et la metric
view est sautée, avec une ligne dans le rapport : les metric views n'existent pas hors UC.

Transitions écrites :

- `candidate → validated → promoted`, avec deux événements dont le second porte
  `decided_by="agent:validator_v1"` et `evidence = ValidationReport` sérialisé ;
- `candidate → needs_review`, avec un événement.

`published_view` et `published_metric_view` sont mis à jour. Deux ajouts vont dans l'index :

- un nœud `metric` de statut `candidate`, avec `metric_name`, `metric_description` et la question
  d'origine si elle est stockée. C'est lui que S03 retrouve pour la parade R6 ;
- une arête `shortcut` pour chaque jointure de la candidate promue (`source="candidate"`,
  `weight=0.8`).

### 2.3 Job

```bash
skifer candidates validate [--candidate ID ...] [--dry-run] [--json]
```

Il traite les candidates `candidate`, puis les `validated` non publiées.

- `0` : rien à signaler ;
- `1` : erreur technique ;
- `2` : au moins une candidate est passée en `needs_review` pendant ce run, pour qu'un job puisse
  notifier.

`--dry-run` calcule et affiche sans exécuter de requête de stabilité, sans DDL et sans
transition.

Seuils lus dans la config de l'environnement :

- `candidates_min_requests` ;
- `candidates_min_distinct_requesters` ;
- `candidates_min_days` ;
- `candidates_stability_runs` ;
- `candidates_min_stability`.

## 3. Exigences

- **S05-R1** — Une candidate dont `residues` n'est pas vide NE DOIT JAMAIS être promue, quels que
  soient ses chiffres. Le test met les seuils à 0 et la stabilité à 1.0.
- **S05-R2** — Chaque critère DOIT être testable seul avec des seuils injectés. Chaque raison
  porte `code`, `observed` et `threshold`.
- **S05-R3** — La fréquence NE DOIT PAS passer sur des demandeurs indisponibles
  (`requesters_unavailable`).
- **S05-R4** — Une fréquence non atteinte DOIT laisser la candidate en `candidate` (`pending`),
  sans événement.
- **S05-R5** — En `enforce`, une source non certifiée DOIT mener à `needs_review`, jamais à
  `promoted`.
- **S05-R6** — Le SQL de la vue de niveau 1 DOIT être identique à
  `compile_select(parse_to_ir(metric_definition(schema_dict)[0]))`. Aucun `{{ pN }}` NE DOIT
  subsister dans un DDL émis. C'est la parade au risque R7, préparée pour S06.
- **S05-R7** — `promote_level1` NE DOIT écrire que dans `<schema>_candidates`. Un test vérifie
  qu'aucun DDL émis ne cible un autre schéma, ni le schéma des metric views certifiées.
- **S05-R8** — `--dry-run` NE DOIT émettre aucun DDL, aucune requête et aucune écriture de store
  (espions).
- **S05-R9** — Le code de sortie DOIT valoir `2` si et seulement si au moins une candidate est
  passée en `needs_review` pendant ce run.
- **S05-R10** — `ValidationReport.semantic_layer_version` DOIT être recalculé à chaque
  validation, et sert de base au `StaleCandidateError` de S06.
- **S05-R11** — Les deux branches de `LEVEL1_PUBLISHES_METRIC_VIEW` DOIVENT être testées :
  `monkeypatch` de la constante, DDL attendu dans chaque cas.

## 4. Fichiers

**À créer** :

- `src/skifer/candidates/validator.py` ;
- `src/skifer/candidates/promotion.py` ;
- `tests/test_candidates_validator.py` ;
- `tests/test_candidates_promotion.py` ;
- `tests/test_candidates_cli.py`, qui recevra aussi les commandes de S06.

**À modifier** :

- `src/skifer/cli.py`, avec la sous-commande `candidates` (et `validate` seul dans ce lot) ;
- `src/skifer/core/config.py`, pour les seuils ;
- `src/skifer/knowledge/store.py`, si l'ajout d'arête demande une méthode, qui doit alors rester
  additive ;
- `CHANGELOG.md`.

## 5. Slices

- **S05.1 Vérification R1.** Test live manuel, consigné dans la PR, puis
  `LEVEL1_PUBLISHES_METRIC_VIEW` et `level1_view_ddl`/`level1_model`, avec un test sur le DDL
  généré.
- **S05.2 Critères.** `validator.py` : cohérence, fréquence, stabilité.
- **S05.3 Promotion.** `promote_level1`, les transitions, les arêtes `shortcut`.
- **S05.4 Job CLI.** `skifer candidates validate`.

## 6. Tests d'acceptation

| Exigence | Test |
|---|---|
| S05-R1 | `test_candidates_validator.py::test_residue_never_promoted_even_with_zero_thresholds` |
| S05-R2 | `…::test_coherence_reasons`, `…::test_frequency_reasons`, `…::test_stability_reasons` |
| S05-R3 | `…::test_frequency_fails_without_pseudonyms` |
| S05-R4 | `…::test_pending_frequency_keeps_state_and_writes_no_event` |
| S05-R5 | `…::test_uncertified_source_goes_to_needs_review_under_enforce` |
| S05-R6 | `test_candidates_promotion.py::test_level1_sql_equals_compile_select` |
| S05-R7 | `…::test_ddl_targets_only_candidates_schema` |
| S05-R8 | `test_candidates_cli.py::test_validate_dry_run_has_no_side_effect` |
| S05-R9 | `…::test_validate_exit_code_two_on_needs_review` |
| S05-R10 | `…::test_semantic_layer_version_recomputed` |
| S05-R11 | `test_candidates_promotion.py::test_level1_with_and_without_metric_view` |

## 7. Erreurs

| Cas | Comportement |
|---|---|
| `MetricViewTranslationError` | candidate passée en `needs_review` avec la raison `metric_view_translation` et le message de l'exception, qui ne cite que des noms de modèle |
| échec DDL | run `error_type`, candidate laissée dans son état précédent, code `1` |
| seuil négatif ou stabilité hors `[0, 1]` | erreur de config au chargement |

## 8. Hors périmètre

- toute écriture dans `semantic_models/` ou `schemas/`, et toute promotion au certifié (S06) ;
- la rétrogradation et le `DROP` (S06) ;
- la planification du job : la doc de S10 montre comment le créer, ce lot n'en crée aucun.

## 9. Definition of Done

- les 11 exigences ont leur test vert ;
- le résultat du test live R1 est écrit dans la PR et dans le plan parent (R1 : levé ou repli
  appliqué).
