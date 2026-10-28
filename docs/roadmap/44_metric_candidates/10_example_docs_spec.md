# S10 — Exemple complet et documentation

> **Dépendances :** toutes les autres specs mergées, et D7 tranchée et consignée (S09.3)
> **Slices / commits :** 2
> **Branche :** `feat/plan44-s10-example-docs`
> **Résultat :** un utilisateur qui découvre le module peut le faire tourner en local en une
> commande, et comprendre en une page ce qui est stocké, ce qui ne l'est jamais et qui décide de
> quoi.

## 1. Contexte

`examples/` est un livrable : `tests/test_examples.py` exécute chaque `run.py` dans un
sous-processus. Le dossier `examples/25_metric_candidates/` existe déjà avec le notebook de S06.
Ce lot y ajoute l'exemple exécutable. La doc suit la nav « Semantic & agentic » de `mkdocs.yml`.

## 2. Contrat

### 2.1 Exemple

```text
examples/25_metric_candidates/
  README.md
  config.yaml                 # catalog: null, candidate_fallback: on, candidates_decider_identity: plain
  semantic_catalog.yaml
  semantic_models/            # deux modèles certifiés (ventes, clients)
  schemas/                    # les pipelines qui les produisent
  data/                       # CSV minuscules
  run.py
  notebooks/arbitrage.py      # S06, inchangé hors cellule drift optionnelle
```

`run.py`, déterministe, avec un LLM factice dont les réponses sont écrites dans le script et
aucun appel réseau :

1. construit les tables et enregistre la certification des sources ;
2. lance `knowledge sync` ;
3. pose une question couverte, qui reçoit une réponse certifiée, puis une question hors
   périmètre, qui produit une réponse candidate avec la ligne « non certifiée » ;
4. repose la question hors périmètre avec d'autres valeurs et d'autres demandeurs factices,
   jusqu'à franchir les seuils, abaissés dans la config de l'exemple ;
5. lance `candidates validate`, qui publie au niveau 1 (vue locale) ;
6. lance `candidates promote`, qui écrit les drafts dans un répertoire temporaire, jamais dans
   l'arborescence de l'exemple ;
7. lance `candidates drift`, puis `candidates reference bootstrap`, puis `evaluate` ;
8. affiche `history`.

Chaque étape affiche une ligne qui commence par `[NN]`, et le test vérifie leur présence et leur
ordre.

### 2.2 Documentation

- `docs/metric_candidates.md`, dans la nav « Semantic & agentic ». Il couvre :
  - pourquoi le module existe ;
  - le schéma du cycle ;
  - les deux niveaux de promotion ;
  - un tableau de **ce qui est stocké, et ce qui ne l'est jamais** ;
  - la configuration complète, avec les clés de toutes les specs ;
  - l'arbitrage : notebook sur Databricks, CLI en local, et pourquoi il n'y a pas d'app en v1 ;
  - les codes de sortie ;
  - les signaux de drift et ce qu'ils ne font pas ;
  - le jeu de référence ;
  - le benchmark et la décision D7, avec ses chiffres ;
  - comment planifier `knowledge sync`, `candidates validate` et `evaluate` en jobs Lakeflow ;
  - les limites.
- `docs/databricks_supervisor.md` : une section sur ce que l'agent de repli ajoute à Genie.
- `docs/adaptive.md` : la frontière, en une phrase. `adaptive` ne stocke pas la question ;
  `candidates` la stocke (classification `internal`, mode `redacted`), et la page explique
  pourquoi.
- `CLAUDE.md` et `AGENTS.md` :
  - les lignes de structure `candidates/`, `knowledge/` et `agentic/candidate_agent.py` ;
  - une section courte sur le modèle des autres ;
  - la ligne du Plan 44 dans « Active plans ».
- `examples/README.md`, ou l'index des exemples s'il existe : l'entrée 25.

## 3. Exigences

- **S10-R1** — `run.py` DOIT passer dans `tests/test_examples.py` sans clé API, sans réseau et
  sans workspace.
- **S10-R2** — `run.py` NE DOIT laisser aucun fichier dans l'arborescence de l'exemple : bases
  SQLite, drafts et vues locales vont dans un répertoire temporaire. Le test compare
  `git status` avant et après.
- **S10-R3** — Chaque clé de config introduite par S01 à S09 DOIT apparaître dans
  `docs/metric_candidates.md`. Le test croise la liste des clés lues par `core/config.py` et le
  texte de la page.
- **S10-R4** — `mkdocs build --strict` DOIT rester sans warning.
- **S10-R5** — Chaque code de sortie de `skifer candidates` et de `skifer knowledge` DOIT être
  documenté. Le test croise les codes utilisés dans `cli.py` et ceux de la page.

## 4. Fichiers

**À créer** : les fichiers de l'exemple (§2.1, sauf le notebook) et `docs/metric_candidates.md`.

**À modifier** :

- `mkdocs.yml` ;
- `docs/databricks_supervisor.md` ;
- `docs/adaptive.md` ;
- `CLAUDE.md` ;
- `AGENTS.md` ;
- `examples/25_metric_candidates/notebooks/arbitrage.py`, pour la cellule drift optionnelle ;
- `tests/test_docs*.py`, ou un nouveau `tests/test_metric_candidates_docs.py`, pour R3 et R5 ;
- `CHANGELOG.md`.

## 5. Slices

- **S10.1 Exemple.** `examples/25_metric_candidates/` exécutable et testé.
- **S10.2 Documentation.** Les pages, la nav, `CLAUDE.md` et `AGENTS.md`, les tests de
  cohérence entre doc et code.

## 6. Tests d'acceptation

| Exigence | Test |
|---|---|
| S10-R1 | `tests/test_examples.py`, qui ramasse le nouvel exemple sans modification |
| S10-R2 | `test_examples.py::test_metric_candidates_example_leaves_tree_clean` (ou un test dédié) |
| S10-R3 | `test_metric_candidates_docs.py::test_every_config_key_is_documented` |
| S10-R4 | `mkdocs build --strict` |
| S10-R5 | `test_metric_candidates_docs.py::test_every_exit_code_is_documented` |

## 7. Erreurs

Sans objet : aucun nouveau comportement d'erreur.

## 8. Hors périmètre

- le bump de version (D9 : 4.1.0, que décide l'utilisateur) et le workflow `Release` ;
- un tutoriel vidéo, des slides ou une page de marketing ;
- un déploiement réel sur un workspace. Le runbook le décrit, mais ce lot ne l'exécute pas.

## 9. Definition of Done

- les 5 exigences ont leur test vert, et le gate complet est vert, avec `mkdocs`.
- La ligne du Plan 44 dans `CLAUDE.md` est passée à « Implémenté ».
- Le plan parent porte le statut final de chaque risque (R1 à R8).
