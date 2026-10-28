# Specs de développement — Plan 44

> **Statut :** découpage validable, rien d'implémenté (28 octobre 2026).
> **Document parent :** [`../44_metric_candidates_plan.md`](../44_metric_candidates_plan.md) — objectif,
> constats, décisions D1 à D9, risques. Les specs ne rediscutent pas ces décisions : elles les
> exécutent. Une spec qui contredit le parent est un bug de la spec, à remonter.
> **Public cible :** agent de développement qui n'a pas le contexte de conception.

## Ce qu'est une spec ici

Chaque fichier est un **contrat de build autonome** pour un lot. Il contient, dans cet ordre :

1. **Contexte** — le problème, en trois phrases, et ce qui existe déjà à réutiliser.
2. **Contrat** — les types, signatures et formats exacts que le lot expose. C'est la partie
   normative : un autre lot peut s'appuyer dessus sans lire le code.
3. **Exigences** — numérotées `SNN-Rk`, chacune formulée en « DOIT / NE DOIT JAMAIS », chacune
   vérifiable par un test. Rien qui ne soit pas testable n'est une exigence.
4. **Fichiers** — à créer, à modifier. Les numéros de ligne cités peuvent avoir dérivé : relire.
5. **Slices** — l'ordre des commits. Une slice = un commit `feat(plan44-SNN.k): …`.
6. **Tests d'acceptation** — une ligne par exigence : le nom du test et ce qu'il prouve. Une
   exigence sans test listé n'est pas livrée.
7. **Erreurs** — chaque échec attendu, son type, son message ou son code de sortie.
8. **Hors périmètre** — ce que le lot NE fait PAS, même si c'est tentant.
9. **Definition of Done** — conditions objectives de sortie.

Quand une spec dit « relire X avant d'écrire », ce n'est pas une politesse : la signature
citée vient d'une lecture du 28 octobre 2026 et le code a pu bouger.

## Ordre d'exécution

| Ordre | Spec | Décisions | Slices | Peut commencer quand… |
|---:|---|---|---:|---|
| 1 | [`00_evidence_metric_certification_spec.md`](00_evidence_metric_certification_spec.md) | D4 | 2 | immédiatement |
| 2 | [`01_candidate_registry_spec.md`](01_candidate_registry_spec.md) | D2 | 4 | immédiatement (parallèle à S00) |
| 3 | [`02_knowledge_index_spec.md`](02_knowledge_index_spec.md) | D7 (BM25), D8 | 4 | immédiatement (parallèle à S00/S01) |
| 4 | [`03_candidate_agent_spec.md`](03_candidate_agent_spec.md) | D3 | 4 | S00, S01, S02 mergées |
| 5 | [`04_hub_routing_spec.md`](04_hub_routing_spec.md) | D5 | 2 | S03 mergée |
| 6 | [`05_validation_promotion_spec.md`](05_validation_promotion_spec.md) | D1 (niveau 1) | 4 | S03 mergée |
| 7 | [`06_arbitration_spec.md`](06_arbitration_spec.md) | D1 (niveau 2), D6 | 4 | S05 mergée |
| 8 | [`07_usage_drift_spec.md`](07_usage_drift_spec.md) | décision 14 du brief | 2 | S04 et S06 mergées |
| 9 | [`08_dense_embeddings_spec.md`](08_dense_embeddings_spec.md) | D7 | 2 | S02 mergée |
| 10 | [`09_reference_evaluation_spec.md`](09_reference_evaluation_spec.md) | décision 16 du brief, D7 | 3 | S06, S07 et S08 mergées |
| 11 | [`10_example_docs_spec.md`](10_example_docs_spec.md) | — | 2 | toutes les autres mergées |

Total : **33 slices**. S00, S01 et S02 sont indépendantes et peuvent être menées en parallèle.
S08 peut être menée en parallèle de S03 à S07. **D7 est tranchée à la fin de S09**, et la
décision est consignée dans le plan parent avant S10.

```text
S00 evidence ──┐
S01 registre ──┼─▶ S03 agent ─┬─▶ S04 routage ──────────────────┐
S02 index ─────┘              └─▶ S05 validation ─▶ S06 arbitrage ┴─▶ S07 drift ─┐
  └─────────────▶ S08 embeddings ────────────────────────────────────────────────┴─▶ S09 référence ─▶ S10 exemple + docs
```

## Correspondance avec les points du plan parent

| Point du parent | Spec |
|---|---|
| 44.0 modèles, états, stores | S01 |
| 44.1 graphe + BM25 | S02 |
| 44.2 agent de repli | S03 (et le champ d'evidence, sorti en S00) |
| 44.3 routage + affichage | S04 (l'affichage est sorti en S00) |
| 44.4 validation + niveau 1 | S05 |
| 44.5 arbitrage + niveau 2 | S06 |
| 44.6 drift | S07 |
| 44.7 référence + benchmark | S09 |
| 44.8 embeddings | S08 |
| 44.9 exemple + docs | S10 |

L'affichage de confiance (D4) est sorti dans sa propre spec S00 : il ne dépend de rien, il est
petit, et c'est lui qui garantit la promesse la plus importante du plan — une réponse de repli
ne ressemble jamais à une réponse certifiée. Il doit être en place **avant** que l'agent de
repli puisse produire une seule réponse.

## Écarts assumés avec le plan parent

En écrivant les contrats, quelques points du parent se sont révélés imprécis ou incohérents avec
le code. Les specs tranchent ainsi, et le parent renvoie ici :

| Point | Parent | Spec | Pourquoi |
|---|---|---|---|
| Déclencheur du repli | `candidate_fallback_min_similarity` | supprimé : liste de suggestions vide, ou `no_model` (S04) | GenBI ne renvoie pas de score, seulement une liste déjà filtrée par Levenshtein |
| Réutilisation d'une candidate promue (R6) | dans le routeur | dans le contexte LLM de l'agent, plus le dédoublonnage par fingerprint (S03, S04) | le `schema_dict` stocké est paramétré, donc les valeurs de la nouvelle question doivent venir du LLM |
| Filtres d'une candidate publiée | non traité | `metric_definition()` : un filtre paramétré devient une dimension (S05, S06) | une métrique publiée ne peut pas figer les valeurs d'une question, et c'est ce qui garantit R7 |
| Stabilité | ré-exécution « mêmes paramètres » | ré-exécution de `metric_definition` (S05) | les valeurs ne sont pas stockées (D2) |
| `approve` d'une `needs_review` | promotion directe | cohérence revalidée ; seuls `residue_present` et `undeclared_join` sont levables par un humain (S06) | un humain ne contourne pas le gate de certification, ce qui reste le rôle du break-glass |
| Identité du décideur | pseudonymisée | `hmac` par défaut, refus sans secret ; `plain` explicite pour le local (S06) | une décision sans auteur traçable ne vaut rien, et un hash sans clé est réversible |
| Lien candidate ↔ certifié | `metadata.source_candidate_id` | clé optionnelle `semantic.source_candidate_id` du seed, propagée au draft (S06) | `metadata` est généré par le builder : il faut une entrée déclarée côté pipeline |
| Drift | « route API en lecture » | retirée (S07) | D6 révisée exclut l'API en v1 |
| `reference_questions` | colonnes `expected_*` à plat | une colonne JSON `expected` validée par famille (S01, S09) | la forme dépend de la famille |
| Affichage de confiance | dans 44.3 | spec autonome S00, première à livrer | indépendant, petit, et prérequis de toute réponse candidate |

## Règles communes non négociables

Celles du [README du Plan 29](../29_agent_ready_semantic_layer/README.md#règles-communes-non-négociables)
s'appliquent toutes. En plus :

1. **Aucun SQL libre n'est exécuté.** Tout ce qui s'exécute est sorti de `compile_select` sur un
   `schema_dict` passé par `load_schema`/`parse_schema`. Le SQL du LLM n'est qu'une entrée du
   mapper du Plan 43.
2. **Aucune valeur de filtre n'est persistée** dans un store de ce plan : les valeurs sont
   remplacées par des paramètres `{{ pN }}` avant écriture. Un test par store le vérifie.
3. **Un lot ne promeut jamais au certifié.** Le certifié, c'est `semantic_catalog.yaml` dans git
   (D1). Aucun code de ce plan n'écrit dans `semantic_models/` hors `.drafts/`, ni dans `schemas/`
   hors `.drafts/`.
4. **L'historique de certification est append-only** : aucune méthode d'update ou de delete
   n'existe sur son store, et un test le vérifie au niveau source.
5. **Le cœur s'importe sans les extras** : `sentence_transformers`, `mlflow`, `sqlglot`,
   `databricks.sdk` sont importés paresseusement (test `sys.modules`).
6. **Le comportement par défaut ne change pas** : `candidate_fallback: off` par défaut ; tant
   qu'un environnement ne l'active pas, aucune réponse existante n'est modifiée (S04 le teste).
7. Stores : `Sqlite*` en local, `Delta*` sur Databricks, sélection par `default_*_store(engine)`
   sur le modèle de `default_certification_store` (`observability/certification_store.py`). Les
   adaptateurs Delta passent par des primitives nommées de `SparkBackend`, comme
   `append_semantic_usage_event` (`core/spark_backend.py`) : `CREATE SCHEMA IF NOT EXISTS`, DDL
   explicite, idempotence par identifiant, **jamais de `count()`**.

## Branches et commits

```text
plan    : memory-improvements                    (ce dossier et le plan parent)
build   : feat/plan44-sNN-<slug>                 une branche par spec, partie de la branche
                                                 qui contient ses dépendances mergées
commit  : feat(plan44-SNN.k): <résultat de la slice>
          test(plan44-SNN.k): …  si la slice ne touche que des tests
          docs(plan44-SNN.k): …  si la slice ne touche que de la doc
```

Après chaque slice : `pytest tests/ -x --tb=short && ruff check src/` (et `mkdocs build --strict`
si `docs/` est touché). Toute modification de `src/` a son test et son entrée `CHANGELOG.md`
sous `## [Unreleased]`. Jamais de bump de version.

## Checklist avant le premier commit d'une spec

- lire ce README, la spec, et les décisions du plan parent qu'elle cite ;
- vérifier que les specs dont elle dépend sont **mergées** (pas seulement commencées) ;
- relire les fichiers source cités : signatures et numéros de ligne peuvent avoir bougé ;
- `git status --short` : préserver les changements de l'utilisateur ;
- créer la branche `feat/plan44-sNN-<slug>`.

## Definition of Done commune

- toutes les slices de la spec sont des commits séparés et lisibles ;
- chaque exigence `SNN-Rk` a son test d'acceptation, vert ;
- gate complet vert (`pytest tests/ -x`, `ruff check src/`, `mkdocs build --strict` si docs) ;
- `CHANGELOG.md` à jour ; aucun secret, aucun artefact local (`.db`, `.skifer_*`) commité ;
- un écart avec la spec est écrit dans la PR, pas découvert en review ;
- aucune spec dépendante n'est commencée sur la même branche.
