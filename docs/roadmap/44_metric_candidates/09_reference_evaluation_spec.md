# S09 — Jeu de questions de référence, évaluation et benchmark

> **Décisions :** décision 16 du brief. Ce lot produit la mesure qui **tranche D7**.
> **Dépendances :** S06, S07 et S08 mergées (S09 étend `drift.py`)
> **Slices / commits :** 3
> **Branche :** `feat/plan44-s09-reference-evaluation`
> **Résultat :** `skifer candidates evaluate` et `skifer knowledge benchmark` disent, chiffres à
> l'appui, si le repli répond juste et quelle recherche trouve le mieux. Le défaut de D7 est
> ensuite consigné dans le plan parent.

## 1. Contexte

Il y a trois familles de questions, stockées dans `reference_questions` (S01) :

- **A** : une question à laquelle le semantic layer répond. L'attendu est
  `{model_key, metric}`, et l'on mesure le rappel de l'index et l'exactitude du routage GenBI ;
- **B** : une question hors du semantic layer, avec une réponse vérifiée. L'attendu est
  `{schema_dict, result_hash}`, et l'on mesure l'exactitude du repli ;
- **C** : une question à laquelle il faut refuser ou demander une précision. L'attendu est
  `{outcome: refuse|clarify}`, et l'on mesure le taux de refus correct.

Le bootstrap est **déterministe**, par gabarits, sans LLM : deux exécutions produisent le même
jeu, ce qui rend les comparaisons d'une version à l'autre légitimes.

## 2. Contrat

```python
# candidates/reference.py
TEMPLATES_FR = ("Quel est le {label} ?", "{label} par {dimension}")   # figés, versionnés
REFERENCE_SET_VERSION = "v1"

def bootstrap_from_models(engine) -> list[ReferenceQuestion]
    # famille A : pour chaque métrique du catalogue, un gabarit par (label | synonyme), dimension
    # = première dimension déclarée ; question_id = sha256:v1: de (question, expected) ;
    # idempotent (append ignore les ids existants)
def on_certified(event, store) -> ReferenceQuestion | None     # événement certified → famille A
def on_rejected(event, store) -> ReferenceQuestion | None      # rejet pour incohérence → famille C
def add_reference(store, *, question, family, expected) -> ReferenceQuestion   # import humain (B surtout)

@dataclass(frozen=True)
class EvaluationReport:
    reference_set_version: str
    semantic_layer_version: str
    question_count: dict[str, int]                  # par famille
    metrics: dict[str, float | None]                # voir ci-dessous ; None = famille vide
    failures: tuple[dict, ...]                      # question_id, famille, attendu, obtenu (sans valeur de filtre)
    def to_dict(self) -> dict

def evaluate(hub, index, store, *, k_values=(5, 10)) -> EvaluationReport
```

Les métriques :

- `A.recall@5` et `A.recall@10` : la métrique attendue figure dans les k premiers hits `metric` ;
- `A.routing_accuracy` : GenBI répond par le bon `model_used` et la bonne métrique ;
- `B.exact_match` : le `result_hash` du repli est égal à l'attendu ;
- `C.correct_refusal` : la question a bien reçu un refus ;
- `C.false_answer` : une réponse a été produite là où il fallait refuser ;
- `C.false_refusal` : calculé sur A et B, c'est un refus là où une réponse était attendue.

```python
# knowledge/benchmark.py
def benchmark(store, questions: Sequence[ReferenceQuestion], *,
              configs: Sequence[str]) -> dict
    # configs ⊂ {"bm25", "dense:<model>", "hybrid:<model>"} ; par config :
    # recall@5, recall@10, mean_rank, latency_ms_p50 (horloge monotone), index_build_s
```

CLI :

```bash
skifer candidates reference bootstrap
skifer candidates reference add --family B --question "…" --expected-file PATH
skifer candidates reference list [--family A|B|C] [--json]
skifer candidates evaluate [--json] [--mlflow]
skifer knowledge benchmark --config bm25 --config hybrid:multilingual-e5-small ... [--json]
```

`--mlflow` (extra `[tracing]`, import paresseux) crée un run qui logge les métriques, avec les
paramètres `reference_set_version` et `semantic_layer_version`, et le rapport JSON en artefact.
Il n'y met aucune question en clair si `candidates_store_questions: redacted`.

Le hook `on_certified`/`on_rejected` est appelé par `CandidateService` (S06) et par le hook
`--promote`, en best-effort.

**Signal de drift supplémentaire :** `compute_drift` (S07) gagne un signal optionnel
`reference_degradation`. Il compare le dernier `EvaluationReport` stocké au précédent et liste
les métriques A ou B en baisse. Le rapport JSON complet est écrit dans `.skifer_evaluations/<horodatage>.json` en local
(ajouté au `.gitignore`), et dans le run MLflow avec `--mlflow`. Pour retrouver les rapports, une
ligne est ajoutée dans `metric_candidate_runs` :

- `candidate_id="reference_set:<REFERENCE_SET_VERSION>"` ;
- `purpose="evaluation"` ;
- `result_hash` égal au hash du rapport.

C'est par ces lignes que `reference_degradation` retrouve le dernier et l'avant-dernier rapport.
Il n'y a aucune nouvelle table, et rien ne va dans `metric_certification_history`, qui ne
concerne que des métriques.

## 3. Exigences

- **S09-R1** — Deux `bootstrap` successifs DOIVENT produire exactement les mêmes `question_id`,
  le second n'écrivant rien.
- **S09-R2** — Le bootstrap NE DOIT faire aucun appel LLM, ce que vérifie un provider espion.
- **S09-R3** — Chaque métrique de `EvaluationReport` DOIT être calculée par une formule testée sur
  un hub factice dont les réponses sont choisies. Le résultat attendu est codé en dur.
- **S09-R4** — Une famille vide DOIT produire `None`, ni `0` ni `1`.
- **S09-R5** — `failures` NE DOIT contenir aucune valeur de filtre. Le test utilise des canaris
  dans des questions B.
- **S09-R6** — Sans l'extra `[tracing]`, `--mlflow` DOIT échouer avec un message nommant l'extra,
  avec le code `1`. Sans `--mlflow`, MLflow NE DOIT PAS être importé.
- **S09-R7** — `benchmark` DOIT accepter `bm25` sans l'extra d'embeddings, et refuser
  `dense:`/`hybrid:` sans lui avec l'erreur de S08.
- **S09-R8** — `on_certified` DOIT ajouter une question A dont l'attendu est la métrique certifiée.
  `on_rejected`, quand le motif est l'incohérence, DOIT ajouter une question C.
- **S09-R9** — `reference_degradation` DOIT lister une baisse de `B.exact_match` entre deux
  rapports, et être `inconclusive` quand il n'y en a qu'un.

## 4. Fichiers

**À créer** :

- `src/skifer/candidates/reference.py` ;
- `src/skifer/knowledge/benchmark.py` ;
- `tests/test_candidates_reference.py` ;
- `tests/test_candidates_evaluate.py` ;
- `tests/test_knowledge_benchmark.py`.

**À modifier** :

- `src/skifer/cli.py` ;
- `src/skifer/candidates/service.py`, pour appeler les hooks ;
- `src/skifer/candidates/drift.py`, pour le signal optionnel ;
- `.gitignore`, pour `.skifer_evaluations/` ;
- `CHANGELOG.md`.

## 5. Slices

- **S09.1 Jeu de référence.** Bootstrap, hooks, `reference add|list|bootstrap`.
- **S09.2 Évaluation.** `evaluate`, `--mlflow`, `reference_degradation`.
- **S09.3 Benchmark.** `knowledge/benchmark.py` et la CLI. **Puis, hors code** : lancer le
  benchmark sur `examples/` et sur le catalogue de démonstration, écrire le tableau de résultats et
  la décision D7 dans le plan parent, dans un commit `docs(plan44-S09.3): trancher D7`. **Cette
  décision revient à l'utilisateur** : le commit propose, avec les chiffres, et n'est mergé
  qu'après son accord.

## 6. Tests d'acceptation

| Exigence | Test |
|---|---|
| S09-R1 | `test_candidates_reference.py::test_bootstrap_is_idempotent_and_deterministic` |
| S09-R2 | `…::test_bootstrap_never_calls_llm` |
| S09-R3 | `test_candidates_evaluate.py::test_metric_formulas` (paramétré) |
| S09-R4 | `…::test_empty_family_is_none` |
| S09-R5 | `…::test_failures_never_carry_filter_values` |
| S09-R6 | `…::test_mlflow_flag_requires_extra`, `…::test_no_mlflow_import_without_flag` |
| S09-R7 | `test_knowledge_benchmark.py::test_bm25_without_extra`, `…::test_dense_config_requires_extra` |
| S09-R8 | `test_candidates_reference.py::test_certified_adds_family_a`, `…::test_incoherent_reject_adds_family_c` |
| S09-R9 | `test_candidates_drift.py::test_reference_degradation_signal` |

## 7. Erreurs

| Cas | Comportement |
|---|---|
| `--expected-file` qui ne correspond pas à la famille | code `2`, message nommant la clé attendue |
| jeu vide au moment de `evaluate` | rapport valide, toutes les métriques à `None`, code `0`, et la ligne `reference set is empty — run 'skifer candidates reference bootstrap'` |
| `--config` inconnue | code `2` |

## 8. Hors périmètre

- la génération de questions par LLM ;
- l'exécution périodique de l'évaluation : S10 documente comment planifier le job ;
- un seuil de qualité qui bloquerait la CI.

## 9. Definition of Done

- les 9 exigences ont leur test vert ;
- le tableau du benchmark et la proposition de décision D7 sont dans le plan parent, en attente
  de validation par l'utilisateur.
