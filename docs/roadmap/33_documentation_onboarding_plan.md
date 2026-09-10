# Plan 33 — Refonte de la documentation d'appropriation

> **Demandé le :** 9 septembre 2026, à la clôture du Plan 29
> **Objectif :** que quelqu'un d'extérieur au projet puisse se l'approprier
> **Branche :** `docs/onboarding-overhaul`
> **Statut :** rédigé, **en attente de validation**

## 1. Le constat, mesuré

La documentation a grossi par accrétion : chaque feature a ajouté sa section à `docs/`, `README.md`,
`CLAUDE.md` et `AGENTS.md`. Elle décrit correctement chaque garantie prise isolément. Personne ne l'a
relue du point de vue de quelqu'un qui découvre le projet.

Cinq constats, tous vérifiés sur le dépôt :

**1. Les deux pages qu'on lit en premier ignorent totalement le Plan 29.** Occurrences de
`certification`, `capability`, `evidence`, `tracing`, `mcp`, `adaptive`, `data_product` dans
`docs/getting_started.md` et `docs/index.md` : **zéro**. Dix features de gouvernance sont invisibles
sur le chemin d'entrée.

**2. Le modèle mental de la page d'accueil est périmé.** `docs/index.md` annonce « Four layers, one
framework » avec un schéma Core / Observability / Semantic / Agentic. La gouvernance — publication
certifiée, preuves, tracing, exposition MCP, adaptive gold, capacités — n'y a **aucune place**. Ce
n'est pas un détail à mettre à jour, c'est le cadre qui ne décrit plus le produit.

**3. Il n'existe aucun répertoire `examples/`.** Aucun fichier du dépôt n'y fait référence. Les
exemples vivent en fragments inertes dans les pages de doc, jamais exécutés, donc jamais vérifiés.

**4. Quatre pages sont absentes de la navigation mkdocs** : `install_databricks.md`, `rules.md`,
`sandbox_project_bootstrap.md`, `ROADMAP_EVOLUTION.md`. Un lecteur du site ne voit donc jamais
comment installer sur Databricks ni comment écrire une règle métier.

**5. `docs/agentic.md` fait 1062 lignes** — la plus grosse page du projet. MCP, adaptive gold et
capacités gouvernées y ont été empilés parce que c'était le fichier « le plus proche du sujet ». La
racine du dépôt porte aussi `init.md` et deux `dev-handoff:*.md`, qui sont du process interne.

## 2. Ce que ce plan n'est pas

Ce n'est pas la checklist de release (point 5 : « mkdocs à jour »), qui vérifie que la doc **suit**
le code. C'est une refonte du **parcours** : ce qu'on lit en premier, dans quel ordre, avec quoi
d'exécutable sous la main.

## 3. Phases

### Phase A — Réparer le modèle mental (bloquante pour le reste)

- `docs/index.md` : remplacer le schéma quatre couches par un modèle qui inclut la gouvernance.
  Décision à prendre explicitement : la gouvernance est-elle une **cinquième couche** ou une
  **dimension transversale** aux quatre autres ? Le code suggère la seconde (la certification touche
  le core, la sémantique et l'agentique), mais un schéma transversal est plus dur à lire.
- `README.md` : l'ouverture doit dire en trois phrases ce que fait le produit *aujourd'hui*, Plan 29
  compris. Les « Key Capabilities » ont grossi à 12 puces par ajouts successifs ; les regrouper.
- Fichiers : `docs/index.md`, `README.md`.

### Phase B — Un chemin d'entrée exécutable

- Créer `examples/`, avec des exemples **qui tournent** en local (`catalog: null`, PySpark + Delta) :
  1. `01_first_pipeline/` — un Bronze → Silver minimal, YAML + données d'entrée + commande.
  2. `02_quality_and_contract/` — `quality_checks`, `data_product`, publication certifiée.
  3. `03_semantic_and_question/` — modèle sémantique + une question en langue naturelle.
  4. `04_governed_capability/` — une capacité en `shadow`, sans système externe.
- `docs/getting_started.md` réécrit **autour** de ces exemples plutôt qu'autour d'extraits inertes.
- Chaque exemple porte un `README.md` court : ce qu'il montre, comment le lancer, ce qu'on doit voir.

### Phase C — Découper et compléter le site

- Scinder `docs/agentic.md` : la couche agentique conversationnelle d'un côté ; MCP, adaptive gold et
  capacités gouvernées dans leurs propres pages (`docs/mcp.md`, `docs/adaptive.md`,
  `docs/capabilities.md`).
- Ajouter une page `docs/governance.md` qui raconte la gouvernance de bout en bout — contrat,
  certification, preuve, trace — plutôt que dispersée par feature.
- Compléter la navigation mkdocs : aucune page de `docs/` ne doit être orpheline.
- Fichiers : `mkdocs.yml`, `docs/agentic.md` et les nouvelles pages.

### Phase D — Empêcher la reprise de l'accrétion

- `tests/test_examples.py` : chaque exemple de `examples/` est **exécuté** par la suite. Un exemple
  qui ne tourne plus casse le build, comme n'importe quelle régression.
- Un test qui vérifie qu'aucune page de `docs/` n'est absente de la nav mkdocs.
- Sortir `init.md` et les `dev-handoff:*.md` de la racine (vers `docs/contributing/` ou suppression
  s'ils sont périmés) — décision à confirmer, ce sont des fichiers de process.

## 4. Risques

- **Phase A engage une décision produit** (cinquième couche ou dimension transversale) que je ne
  peux pas trancher seul : elle fixe la manière dont le projet se présente.
- **Phase B ajoute du code exécuté en CI.** Les exemples locaux démarrent une session Spark ; le coût
  en temps de suite doit être mesuré avant de tout brancher (aujourd'hui ~100 s pour 2367 tests).
- **Phase C casse des liens** vers `docs/agentic.md#...` s'il en existe à l'extérieur du dépôt.
- Aucune de ces phases ne touche `src/` : le risque de régression fonctionnelle est nul, sauf pour
  les tests d'exemples de la phase D.

## 5. Vérification

- La suite reste verte à chaque phase.
- Critère d'acceptation de la phase B, à faire valider par quelqu'un qui ne connaît pas le projet :
  installer, lancer l'exemple 1 et comprendre ce qui s'est passé, **sans poser de question**.
- Critère de la phase D : supprimer une page de la nav ou casser un exemple fait échouer la suite.

## 6. Ordre et validation

A → B → C → D. La phase A est bloquante : réécrire les exemples avant d'avoir fixé le modèle mental
reviendrait à illustrer un cadre qu'on s'apprête à changer.

**Ce plan attend une validation explicite avant implémentation**, et la phase A demande en plus une
décision sur la question de la cinquième couche.
