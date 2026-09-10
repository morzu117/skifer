# Plan 22 — Orchestration autonome (Modèle B) + sous-tâches à routage de modèle

> **Branche :** `feat/autonomous-orchestration`
> **Statut :** Plan v1 — en attente de validation
> **Architecte :** Claude Opus 4.8
> **Superviseur :** Julien Houbart
> **Date :** 2026-06-25
> **Étend le [Plan 20](20_multi_agent_workflow_plan.md)** : passe du relais humain (Modèle A) à l'orchestration autonome (Modèle B).

---

## Contexte & objectif

Deux évolutions demandées :
1. **Orchestration autonome** : une fois le besoin défini et le **GO** explicite donné, Claude
   lance Codex (dev) **et** Antigravity (review) **sans intervention manuelle** — fini le passe-plat.
2. **Sous-tâches à routage de modèle** : les plans décomposent une tâche en **sous-tâches**,
   chacune notée en difficulté et associée à un **modèle adapté** (économique pour le trivial,
   frontier pour le complexe). Meilleur fit **et** levier coût #1.

Prérequis techniques **vérifiés** : `codex exec -m <model> --dangerously-bypass-approvals-and-sandbox "<brief>"`
(non-interactif, choix de modèle) et `agy -p --model <model> --dangerously-skip-permissions "<brief>"`
(testé, accès GBrain OK).

## Décisions actées (validées)

1. **Niveau d'autonomie = autonome avec escalade.** Claude gère seul build + review + correctifs
   **mineurs** (bug / test-gap / nit). Escalade au superviseur **uniquement** pour : décisions
   **design** (re-plan), échec build répété, garde-fou atteint, ambiguïté.
2. **Routage modèle = Claude propose, superviseur voit/override au GO.** Mapping difficulté→modèle
   par défaut ; visible dans le plan ; modifiable avant le GO.
3. **Garde-fou par défaut** : cap d'itérations + escalade (jamais de run qui s'emballe).

## Schéma de sous-tâche (nouveau dans les plans)

Chaque plan de feature gagne une **table de sous-tâches** :

| # | Sous-tâche | Difficulté | Agent · Modèle | Fichiers | Critère d'acceptation | Dépend de |
|---|---|---|---|---|---|---|
| S1 | … | trivial | Codex · *cheap* | … | test X vert | — |
| S2 | … | complexe | Codex · *frontier* | … | test Y vert | S1 |

- **Difficulté** : `trivial` / `standard` / `complexe` (Claude propose, justifie en une ligne).
- **Dépendances** : ordonnancement (séquentiel en v1 ; voir hors-scope pour le parallèle).
- Une sous-tâche = idéalement un commit (cohérent avec « 1 point de plan = 1 commit »).

## Mapping difficulté → modèle (défaut, surchargage au GO)

**Dev (Codex `-m`)** — IDs épinglés (compte OpenAI du superviseur) :
| Difficulté | Modèle |
|---|---|
| trivial (rename, config, doc, 1-liner) | `gpt-5.4-mini` (léger/rapide) |
| standard (feature classique + tests) | `gpt-5.4` (défaut) |
| complexe (logique fine, transverse, perf) | `gpt-5.5` (frontier, haut de gamme) |

> `codex-auto-review` (modèle interne d'auto-review de Codex) n'est **pas** utilisé : la review
> est faite par Antigravity (`agy`), pas par Codex.

**Review (Antigravity `--model`)** :
| Risque sous-tâche | Modèle |
|---|---|
| faible | `Gemini 3.5 Flash (High)` (rapide, peu cher) |
| élevé / complexe | `Gemini 3.1 Pro (High)` (review profonde) |

**Archi / Plan / Triage (Claude)** : Opus pour conception & triage ; le reste de l'orchestration
tourne dans la session Claude courante.

> Modèles Codex épinglés ci-dessus. Mapping global modifiable au GO selon la feature.

## La boucle d'orchestration autonome

**Gate** : besoin défini + plan validé (sous-tâches + modèles proposés) + **« GO »** explicite.

Pour chaque sous-tâche (dans l'ordre des dépendances) :
```
1. BUILD   codex exec -m <model> --dangerously-bypass-approvals-and-sandbox \
             "<brief + DoD + CONTRAINTE DE PÉRIMÈTRE>"  < /dev/null   # ⚠️ stdin fermé (cf. F1)
2. GATE    Claude vérifie la DoD : commit présent + CHANGELOG + dev-handoff,
           lance la SUITE COMPLÈTE (JAVA_HOME=openjdk@17 pytest tests/ -q)  # F4
           ET un CHECK ANTI-SCOPE-CREEP du diff (lignes hors cible = reformatage
           non demandé → escalade/revert)  # F3 : le brief seul ne suffit pas
3. REVIEW  vibe -p "$(cat <prompt+diff>)" --max-turns 3 --output json \
             --max-price 0.50 < /dev/null     # Mistral — indépendant & fiable (F2)
           → cap turns = pas de rabbit-hole ; agy -p écarté (cf. § Review)
4. TRIAGE  Claude lit la review et route :
             • bug / test-gap / nit  → re-dev auto (retour étape 1, dans le cap)
             • design / archi / scope-creep → ESCALADE au superviseur
5. CLEAN   rm -f *:*.md   # purge le mirroir write-through gbrain (gitignoré, non-durable)
```
Quand toutes les sous-tâches sont vertes et sans finding bloquant → **escalade finale** :
résumé + diff + `/ship` proposé.

**Routine d'hygiène (obligatoire)** : `gbrain put` (dev-handoff/plan/triage/…) mirroir chaque
page en `<slug>.md` à la racine du repo (la source code est pinnée via `.gbrain-source`). Ces
fichiers sont **gitignorés** (`*:*.md`) et **non-durables** (le cerveau PGLite fait foi) → les
**purger en fin de cycle** (`rm -f *:*.md`) pour ne pas polluer le repo/l'IDE.

**Invocations éprouvées (rodage cycle 1)** :
- `codex exec … < /dev/null` — **impératif** : sans EOF sur stdin, `codex exec` se bloque
  indéfiniment (run zombie). + brief avec **contrainte de périmètre explicite** (F3).
- Gate = **suite complète** (un gate étroit a raté un drift `schema.json`, F4).

**Mécanique** : lancés en sous-processus (Bash, souvent en arrière-plan + monitoring). Tout sur
la **branche dédiée**.

## Règles d'escalade (ce qui te revient)

J'arrête et je te sollicite **uniquement** si :
- un finding **design/archi** apparaît (décision re-plan),
- une sous-tâche échoue le build après **N=2** re-dev,
- le **garde-fou** est atteint (cap d'itérations ou seuil coût/tokens),
- ambiguïté irréductible dans le besoin/plan.
Sinon : je déroule et je te présente le **résultat final**.

## Garde-fou (budget & boucles)

- **Cap re-dev** : 2 par sous-tâche, puis escalade.
- **Cap global** : seuil d'itérations / de tokens-coût par feature → stop + rapport.
- Tout est **sur branche** (réversible) ; jamais de push `main` sans ton aval.

## Ce que ça change vs Plan 20

| | Plan 20 (Modèle A) | Plan 22 (Modèle B) |
|---|---|---|
| Transitions de phase | déclenchées par toi | **autonomes** après GO |
| Toi = | routeur/passe-plat | décideur (GO + escalades design) |
| Granularité | tâche | **sous-tâches** notées en difficulté |
| Modèle | implicite | **routé par difficulté** (proposé, override au GO) |
| Triage | tu valides chaque routage | auto pour mineur, escalade pour design |

## Risques & mitigations

- **Run qui s'emballe** → garde-fou cap + escalade (intégré).
- **Mauvaise décomposition en sous-tâches** → le plan reste validé par toi au GO.
- **Modèle sous-dimensionné** sur une sous-tâche → re-dev auto, et escalade si échec répété
  (signal qu'il fallait monter en gamme).
- **Fiabilité `codex exec` non-interactif** → éprouvée au rodage 1 : OK avec `< /dev/null` (cf. F1).
- **Coût d'un run autonome** → cap budget + le routage cheap/frontier limite la casse.

## Hors-scope v1 / futur

- **Parallélisme des sous-tâches indépendantes** (dépendances en DAG) → nécessite GBrain
  Postgres/Supabase (concurrence) ; v1 reste séquentielle.
- **Watch mode** (auto-dispatch sur issues) — plus tard.
- Épingler un mapping difficulté→modèle par projet (le futur projet ++++ aura le sien).

## Review (F2 — résolu : `vibe`/Mistral, indépendant)

**Cause racine élucidée** : `agy -p` (Antigravity non-interactif) est une **session agent
exploratoire**. Pour tout prompt non-trivial il part ailleurs (lance la suite Spark, **cherche
sur le web ce que fait son propre flag `--dangerously-skip-permissions`**, liste ses
permissions) au lieu d'exécuter la tâche. Aucun mode headless en config. → **inadapté.**

**Reviewer retenu** : **`vibe`** (Mistral Vibe CLI), en mode programmatique :
```
vibe -p "$(cat <fichier prompt+diff>)" --max-turns 3 --output json --max-price 0.50 < /dev/null
```
Pourquoi c'est le bon : **`--max-turns`** borne l'exploration (le rabbit-hole d'agy devient
impossible), **`--max-price`/`--max-tokens`** = garde-fou budget natif, **`--output json`** =
findings parsables, **`--enabled-tools`** peut tout désactiver. Éprouvé : a **détecté une faille
SQL-injection plantée** → `{CRITICAL, SQL injection…}`, format respecté, zéro dérapage. Free tier
suffit (abo si ça bloque). **Mécanique** : Claude génère le diff (`git show`/`git diff`), l'injecte
dans le prompt (via fichier pour éviter le mangling), vibe juge le texte. Indépendance préservée
car c'est un **autre modèle** (Mistral) qui juge.

**Indépendance retrouvée** : Mistral ≠ OpenAI (dev) ≠ Claude (archi) → vrai œil cross-modèle,
**sans** le caveat « même provider ». Bonus : **EU data residency**.

**Fallbacks** : `codex exec review --base <branche> -m <model≠dev>` (fiable mais même provider) ;
`agy -i` en interactif pour une review manuelle Gemini à fort enjeu quand le superviseur est là.

## Rodage cycle 1 (2026-06-25) — feature `between` (Plan 23) : RÉSULTAT

3 sous-tâches enchaînées en autonome (mini→`gpt-5.4`→`gpt-5.5`), **1306 tests verts**, mergé (PR #44).
Findings intégrés ci-dessus :

| # | Finding | Correction (bakée) |
|---|---|---|
| **F1** | `codex exec` se bloque sur stdin (run zombie 23 min) | invocation `… < /dev/null` (impératif) |
| **F2** | `agy -p` inutilisable en review (session exploratoire, ignore le brief) | **résolu** : `vibe`/Mistral (indépendant, `--max-turns` borne l'exploration) |
| **F3** | Codex reformate hors-scope (E701) — **brief insuffisant** : a récidivé au cycle 2 (S3) malgré la contrainte explicite | **check anti-scope-creep au GATE** (mécanique) + escalade ; décision « garder » = précédent (net-positif E701) |
| **F4** | Gate étroit rate un drift en aval (`schema.json`) | gate = **suite complète** ; régénérer le schéma dès qu'un opérateur est ajouté |

**Ce qui a marché** : routage modèle (mini→frontier), escalade (scope-creep remonté au
superviseur), garde-fou (kill du run S1 zombie + des agy emballés). **La boucle Modèle B tient
côté Dev**, et F2 est résolu via **`vibe`/Mistral** — reviewer indépendant ET fiable.

## Rodage cycle 2 (2026-06-25) — `not_between` (Plan 24)

3 sous-tâches autonomes (mini→`gpt-5.4`→`gpt-5.5`), **1312 tests verts**, mergé (PR #45).
**Bien plus fluide que le cycle 1** : vibe fiable du 1er coup (3 reviews CLEAN), zéro run zombie
(F1 baké), drift schéma (F4) rattrapé au gate. **Seul finding** : F3 a **récidivé** (S3 a
reformaté malgré le brief) → confirme que la contrainte-brief ne suffit pas → **check
anti-scope-creep au gate** (décidé par le superviseur ; commit S3 gardé, précédent net-positif).
