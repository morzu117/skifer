# S06 — Arbitrage humain, promotion niveau 2, rétrogradation

> **Décisions :** D1 niveau 2 (le certifié passe par git), D6 révisée (notebook + CLI sur un
> seul `CandidateService`, pas d'API ni d'App en v1)
> **Dépendances :** S05 mergée
> **Slices / commits :** 4
> **Branche :** `feat/plan44-s06-arbitration`
> **Résultat :** un humain tranche les candidates depuis un notebook Databricks ou depuis la CLI.
> Une candidate approuvée devient des drafts gérés, qu'une PR transforme en métrique certifiée.

## 1. Contexte

La décision humaine a une seule implémentation, `CandidateService`. La CLI et le notebook n'en
sont que des adaptateurs minces : si l'on veut un jour des routes `api/` ou une App, ce qui
correspond au risque R8, elles se brancheront sur ce même service.

Le passage au certifié **n'est pas un état du registre**. Le certifié, c'est la présence du
modèle dans `semantic_catalog.yaml` après `skifer semantic sync --promote`. Le service s'arrête
aux drafts, et un humain fait la PR.

À réutiliser :

- le dépôt sans écrasement d'`adaptive/workflow.py` (vers la ligne 170 :
  `os.O_WRONLY | os.O_CREAT | os.O_EXCL`) et ses codes de sortie ;
- le marqueur `_generated_by` d'`analyzer/emitter.py` ;
- `OutputProjector().project(parsed)` et `SemanticDraftBuilder(output_dir).write_draft(projected,
  parsed)` ;
- `services/identity.LocalIdentity` pour le décideur côté CLI, et
  `core/environment.get_clean_username()` côté notebook ;
- `candidates.redaction.requester_pseudonym` pour la pseudonymisation.

## 2. Contrat

### 2.1 Service

```python
# candidates/service.py
class CandidateError(Exception): ...
class StaleCandidateError(CandidateError): ...     # exit 3
class StateConflictError(CandidateError): ...      # exit 4
class OutputExistsError(CandidateError): ...       # exit 4
class ApprovalRefusedError(CandidateError): ...    # exit 4 — raison non levable par un humain
class DeciderIdentityError(CandidateError): ...    # exit 1

@dataclass(frozen=True)
class CandidateView:        # DTO par allowlist, même rendu pour display() et pour --json
    candidate_id: str; metric_name: str; state: str; question: str | None
    tables_used: tuple[str, ...]; request_count: int; distinct_requesters: int
    stability_score: float | None; criteria: tuple[dict, ...]   # dernier ValidationReport
    published_view: str | None; published_metric_view: str | None
    updated_at: str
    def to_dict(self) -> dict

class CandidateService:
    def __init__(self, store, *, engine=None, backend=None, index_store=None,
                 decider_identity: str = "hmac", policy=None, clock=None): ...
    def list(self, *, state: str | None = None, limit: int = 50) -> list[CandidateView]
    def show(self, candidate_id: str) -> CandidateView
    def approve(self, candidate_id: str, *, decider: str, reason: str) -> CandidateView
    def reject(self, candidate_id: str, *, decider: str, reason: str) -> CandidateView
    def promote(self, candidate_id: str, *, decider: str, reason: str,
                pipeline_output: str | None = None) -> dict    # {"pipeline_draft", "semantic_draft"}
    def review(self, candidate_id: str, *, decider: str, reason: str,
               signal: str | None = None) -> CandidateView      # promoted → under_review
    def demote(self, candidate_id: str, *, decider: str, reason: str,
               replaced_by: str | None = None) -> CandidateView
    def history(self, candidate_id: str) -> list[dict]          # événements, ordre chronologique
```

Les décisions :

- **`approve`** fait passer `needs_review` à `promoted` par `promote_level1` de S05. Avant cela,
  la cohérence est **revalidée**. Un humain peut lever `residue_present` et `undeclared_join`.
  Il ne peut **jamais** lever `missing_table`, `source_not_certified`, `unsafe_fanout` ni un
  échec de `parse_schema`, qui donnent `ApprovalRefusedError`.
- **`reject`** fait passer `candidate`, `validated` ou `needs_review` à `rejected`, avec un
  événement.
- **`promote`** ne s'applique qu'à `promoted` ou `validated`, et ne change pas l'état. Il écrit
  deux drafts et un événement `certification_requested` :
  1. le **pipeline**, dans `pipeline_output`, par défaut
     `schemas/.drafts/candidates/<metric_name>.yaml`. Le chemin DOIT contenir un segment
     `.drafts`, sinon c'est une erreur d'usage. Le contenu est le `schema_dict`, plus
     `data_product` (`id: candidates.<metric_name>`, `version: 0.1.0`), `contract.grain` égal à
     `aggregate.group_by`, et un bloc `semantic` avec `model_key: candidates.<metric_name>`,
     `dimensions` égal à `group_by` et `source_candidate_id`. On y ajoute le marqueur
     `_generated_by: metric_candidates`. Le contenu est construit sur
     `metric_definition(schema_dict)` de S05, et pas sur le `schema_dict` brut : les filtres
     paramétrés deviennent des dimensions. La liste des colonnes converties est rendue sous
     `filters_to_dimensions`, pour que la PR le montre ;
  2. le **modèle sémantique**, par `OutputProjector`, puis `SemanticDraftBuilder`, dans
     `semantic_models/.drafts/`.

  L'écriture se fait en `O_EXCL`. Si l'un des deux fichiers existe, aucun n'est écrit et c'est
  `OutputExistsError`. Le pipeline est validé par `load_schema` **avant** d'être rendu visible :
  staging, puis rename, comme `ProposalGenerator`.
- **`review`** fait passer `promoted` à `under_review`. C'est la seule entrée vers
  `under_review` en v1, la détection de S07 n'appliquant rien d'elle-même.
- **`demote`** fait passer `promoted` ou `under_review` à `demoted`. Il fait un `DROP VIEW IF
  EXISTS` sur `published_metric_view` puis sur `published_view`, et met les deux champs à `None`.
  Si `replaced_by` est donné, `replaced_by_metric` est renseigné sur l'événement, et un événement
  symétrique `replaced` est écrit sur la métrique remplaçante, avec `replaces_metric`.
- **Péremption.** Une décision sur une candidate dont le dernier `ValidationReport` porte un
  `semantic_layer_version` différent de l'actuel lève `StaleCandidateError`. La réponse est de
  relancer `validate`. `reject` est exempté, puisqu'on peut toujours refuser.
- **Décideur.** Avec `decider_identity="hmac"`, l'identité est `human:<pseudonyme>`. Si le secret
  `SKIFER_TRACING_HMAC_SECRET` est absent, c'est `DeciderIdentityError`, avec un message qui nomme
  la variable et l'option : une décision sans auteur traçable n'est pas enregistrée. Avec
  `decider_identity="plain"` (config `candidates_decider_identity`, à réserver au local et aux
  exemples), l'identité est `human:<nom>`.
- **`reviewer`.** La candidate le reçoit à chaque décision humaine.

### 2.2 Hook de certification

`skifer semantic sync PIPELINE --promote` : quand le seed `semantic:` du pipeline porte
`source_candidate_id` et que la promotion écrit le modèle, un événement `certified` est ajouté à
l'historique, avec `metric_id = candidate_id` et `semantic_layer_version` recalculé. C'est la
seule modification de la couche sémantique du Plan 29. Elle tient en trois parties :

- `semantic.source_candidate_id` est une clé optionnelle du seed, validée au format
  `candidate:v1:<hex>` ;
- la clé est propagée telle quelle dans `metadata` du draft ;
- le hook écrit l'événement en best-effort : un store indisponible donne un WARNING, jamais un
  échec de `--promote`, sur la même discipline que les hooks d'indexation du Plan 31.

### 2.3 CLI

```bash
skifer candidates list [--state STATE] [--json]
skifer candidates show ID [--json]
skifer candidates approve ID --reason "…"
skifer candidates reject  ID --reason "…"
skifer candidates promote ID --reason "…" [--pipeline-output PATH]
skifer candidates review  ID --reason "…" [--signal NAME]
skifer candidates demote  ID --reason "…" [--replaced-by ID]
skifer candidates history ID [--json]
```

Codes de sortie :

| Code | Signification |
|---|---|
| `0` | succès |
| `1` | erreur technique, ou `DeciderIdentityError` |
| `2` | usage, y compris un `--pipeline-output` hors `.drafts` |
| `3` | `StaleCandidateError` |
| `4` | `StateConflictError`, `OutputExistsError` ou `ApprovalRefusedError` |

Le décideur est `LocalIdentity`.

### 2.4 Notebook d'arbitrage

`examples/25_metric_candidates/notebooks/arbitrage.py`, au format source Databricks
(`# Databricks notebook source`, cellules séparées par `# COMMAND ----------`) :

0. widgets `config_path`, `candidate_id`, `decision` (`approve|reject|promote|review|demote`),
   `reason` et `replaced_by` ; construction du service avec `default_candidate_store` ;
1. `display()` des `CandidateView` en `needs_review`, puis en `under_review` ;
2. si `candidate_id` et `decision` sont remplis, appel de la méthode du service avec
   `get_clean_username(spark=spark, dbutils=dbutils)` comme décideur. Une `CandidateError`
   s'affiche en clair et ne fait pas échouer le notebook ;
3. `display()` de `history(candidate_id)`.

Le notebook ne contient **aucune logique de décision**. Tout passe par le service.

## 3. Exigences

- **S06-R1** — Chaque transition autorisée DOIT être réalisable par le service, et chaque
  transition interdite DOIT lever `StateConflictError`. Le test est paramétré sur la matrice de
  S01.
- **S06-R2** — `approve` DOIT refuser (`ApprovalRefusedError`) une candidate dont la cohérence
  échoue sur une raison non levable, et DOIT accepter une candidate qui n'échoue que sur
  `residue_present` ou `undeclared_join`.
- **S06-R3** — `promote` NE DOIT JAMAIS écraser un fichier. Si l'un des deux existe, aucun n'est
  écrit. La garantie est au niveau syscall : `O_EXCL`, et un test place le fichier entre la
  vérification et l'écriture.
- **S06-R4** — `promote` NE DOIT écrire que sous un chemin qui contient `.drafts`, et jamais dans
  `semantic_catalog.yaml`.
- **S06-R5** — Le pipeline draft DOIT charger par `load_schema`, et sa projection DOIT passer
  `SemanticValidator`.
- **S06-R6** — Les filtres paramétrés DOIVENT être convertis en dimensions et listés dans
  `filters_to_dimensions`. Aucun `{{ pN }}` ne subsiste dans le draft.
- **S06-R7** — Le SQL compilé du pipeline draft, sans ses blocs
  `data_product`/`contract`/`semantic`, DOIT être égal au SELECT de la vue de niveau 1 de la même
  candidate. C'est le risque R7 du parent.
- **S06-R8** — Une décision sur une candidate périmée DOIT lever `StaleCandidateError`, sauf
  `reject`.
- **S06-R9** — `demote` DOIT supprimer les objets de niveau 1, et
  `history(Y)` DOIT reconstituer « X rétrogradée au profit de Y ». Cela suppose les deux
  événements, `replaced_by_metric` et `replaces_metric`.
- **S06-R10** — Sans secret HMAC et en mode `hmac`, une décision DOIT lever
  `DeciderIdentityError`. Elle NE DOIT écrire aucun événement.
- **S06-R11** — `semantic sync --promote` d'un pipeline porteur de `source_candidate_id` DOIT
  écrire un événement `certified`. Un store indisponible NE DOIT PAS faire échouer `--promote`.
- **S06-R12** — Un pipeline **sans** `source_candidate_id` DOIT produire des drafts et un
  catalogue identiques à ceux d'avant le lot. Les tests du Plan 29 restent inchangés.
- **S06-R13** — Chaque code de sortie de la CLI DOIT être couvert par un test.
- **S06-R14** — Le notebook DOIT s'exécuter de bout en bout dans la suite :
  `runpy.run_path(..., init_globals={"dbutils": FakeDbutils(widgets), "display": capture,
  "spark": None})`, contre une config `catalog: null` en SQLite temporaire. Il est exécuté une
  fois sans décision et une fois avec `approve`.
- **S06-R15** — Le notebook NE DOIT appeler que des méthodes publiques de `CandidateService`.
  Le test inspecte son source.

## 4. Fichiers

**À créer** :

- `src/skifer/candidates/service.py` ;
- `examples/25_metric_candidates/notebooks/arbitrage.py` ;
- `tests/test_candidates_service.py` ;
- `tests/test_candidates_notebook.py` ;
- `tests/fakes/fake_dbutils.py`.

**À modifier** :

- `src/skifer/cli.py`, pour les sous-commandes de `candidates` ;
- `src/skifer/core/config.py`, pour `candidates_decider_identity` ;
- le parsing du seed `semantic:` dans `src/skifer/core/ir.py` et `schema_loader.py`, à relire
  avant d'écrire, pour `source_candidate_id` ;
- `src/skifer/semantic/draft_builder.py`, pour la propagation dans `metadata` ;
- le chemin `--promote` (`semantic/sync.py` ou `cli.py`), pour le hook ;
- `tests/test_candidates_cli.py` ;
- `CHANGELOG.md`.

`examples/25_metric_candidates/` ne contient **que** le notebook dans ce lot : pas de `run.py`,
donc `test_examples.py` ne le ramasse pas encore. L'exemple complet est S10.

## 5. Slices

- **S06.1 Service : lecture et transitions simples.** `list`, `show`, `history`, `reject`,
  `review`, l'identité du décideur, la péremption et les erreurs typées.
- **S06.2 Service : `approve`, `promote`, `demote`.** Le niveau 1 par S05, les drafts `O_EXCL`,
  la conversion des filtres, le `DROP`, le remplacement.
- **S06.3 Hook de certification.** Le seed `source_candidate_id`, `metadata`, et l'événement
  `certified` dans `--promote`.
- **S06.4 CLI et notebook.** Les sous-commandes, les codes de sortie, le notebook et son test.

## 6. Tests d'acceptation

| Exigence | Test |
|---|---|
| S06-R1 | `test_candidates_service.py::test_transition_matrix_through_service` |
| S06-R2 | `…::test_approve_refuses_non_waivable_reasons`, `…::test_approve_waives_residue_and_undeclared_join` |
| S06-R3 | `…::test_promote_never_overwrites_even_under_race` |
| S06-R4 | `…::test_promote_writes_only_under_drafts` |
| S06-R5 | `…::test_pipeline_draft_loads_and_projects` |
| S06-R6 | `…::test_parameterized_filters_become_dimensions` |
| S06-R7 | `…::test_level1_and_level2_sql_match` |
| S06-R8 | `…::test_stale_candidate_refused_except_reject` |
| S06-R9 | `…::test_demote_drops_views_and_history_tells_replacement` |
| S06-R10 | `…::test_hmac_without_secret_refuses_and_writes_nothing` |
| S06-R11 | `test_semantic_sync*.py::test_promote_records_certified_event`, `…::test_promote_survives_unavailable_candidate_store` |
| S06-R12 | tests existants du Plan 29 (drafts, sync), inchangés et verts |
| S06-R13 | `test_candidates_cli.py::test_exit_codes` (paramétré 0/1/2/3/4) |
| S06-R14 | `test_candidates_notebook.py::test_notebook_runs_without_decision`, `…::test_notebook_runs_approve` |
| S06-R15 | `…::test_notebook_uses_only_public_service_methods` |

## 7. Erreurs

Voir le §2.1 pour les types et le §2.3 pour les codes. Les messages nomment la candidate, l'état
courant et l'action attendue. Exemple :
`Candidate candidate:v1:3f2a… is stale (semantic layer changed since validation) — run 'skifer candidates validate --candidate …'.`

## 8. Hors périmètre

- les routes `api/`, une Databricks App ou un dashboard AI/BI (D6, R8) ;
- l'ouverture d'une PR ou toute commande git, qui reste humaine comme dans `adaptive` ;
- la retransition automatique vers `under_review` (S07 observe, sans rien appliquer) ;
- `reactivate` (`demoted → candidate`) : la transition existe dans S01, mais aucune commande ne
  l'expose en v1.

## 9. Definition of Done

- les 15 exigences ont leur test vert ;
- un parcours complet est démontré par un test : candidate, `needs_review`, `approve`, niveau 1,
  `promote`, drafts, `semantic sync --promote`, événement `certified`, puis `history` lisible.
