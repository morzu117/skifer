# Plan 20 — Workflow de développement multi-agents (Claude / Codex / Antigravity)

> **Branche :** `chore/multi-agent-workflow` (doc) — le workflow s'applique ensuite à toutes les branches de feature
> **Statut :** Protocole v1 — relais humain (modèle A)
> **Architecte :** Claude Opus 4.8 (conception)
> **Superviseur :** Julien Houbart
> **Date :** 2026-06-24

---

## Contexte & objectif

Structurer les développements autour de **trois agents spécialisés**, chacun sur son LLM,
coordonnés non pas par des appels directs mais par un **contexte partagé**. L'objectif est
de maximiser la qualité à chaque étape en jouant la spécialité de chaque agent, tout en
gardant un humain dans la boucle aux transitions.

Repose sur l'infra déjà en place : gstack (skills) + GBrain (mémoire/contexte sémantique
local, `~/.gbrain`, embeddings `voyage-code-3`). Voir `CLAUDE.md` § gstack.

## Principe fondateur — le tableau noir (*blackboard*)

Les agents **ne se parlent pas directement**. Ils coordonnent via des **artefacts partagés** :

```
        ┌─────────────── CONTEXTE PARTAGÉ ───────────────┐
        │   GBrain (sens/décisions)  +  git (code/diffs)  │
        │              +  doc de plan (le contrat)         │
        └──────┬──────────────┬──────────────┬────────────┘
               │              │              │
          CLAUDE/Opus    CODEX (ChatGPT)   ANTIGRAVITY
          Conception      Développement    Review
```

Comme les trois agents tournent sur la **même machine**, ils partagent le **même cerveau
GBrain** via le CLI `gbrain` (sur le PATH). Aucun réseau, aucun MCP distant requis.

## Les trois agents

| Agent | LLM / CLI | Rôle | Phases gstack |
|---|---|---|---|
| **Claude** | Opus 4.8 / Claude Code | Conception, architecture, **triage des findings**, ship | Think · Plan · (triage) · Reflect |
| **Codex** | OpenAI / `codex` CLI | Développement (implémentation + tests) | Build |
| **Antigravity** | Google / `agy` CLI | Review critique du code vs intention | Review |

> **Vocabulaire :** sur un framework Python (pas de web app), il n'y a pas de « QA navigateur ».
> Les **tests automatisés `pytest`** font partie de la *Definition of Done* du Dev (responsabilité Codex).
> Antigravity fait la **review critique**, pas du test runtime.

## Modèle d'orchestration — A : relais humain, séquentiel

- **Le superviseur déclenche chaque transition de phase.** Aucun agent n'en
  lance un autre automatiquement (v1).
- **Séquentiel** : un seul agent écrit à la fois → PGLite (mono-writer) suffit, pas de
  contention. Le parallélisme (→ migration Postgres/Supabase) est hors-scope v1.
- Modèles B (Claude chef d'orchestre en shell-out) et C (orchestrateur dédié) : évolutions
  futures, seulement après que les coutures de A soient éprouvées.

## Le cycle de sprint

```
  [Plan]            [Build]              [Review]                [Triage]
  CLAUDE  ──(toi)──▶ CODEX  ──(toi)────▶ ANTIGRAVITY  ──────────────▶ CLAUDE ──▶ ship
   plan doc          dev-handoff          findings              route re-dev/re-plan
   + GBrain          + pytest vert        catégorisés                │
      ▲                                                              │
      └──────────────── re-plan (défaut de conception) ◀────────────┤
                        re-dev  (bug/test/nit) ──▶ CODEX ◀───────────┘
```

Chaque flèche `(toi)` = transition déclenchée manuellement par le superviseur.

## Contrats de handoff

### ① Claude → Codex (déclenché par toi : « passe au dev »)

**Artefact :** doc de plan exécutable dans `docs/roadmap/NN_*.md` **+** page GBrain
`plan:<tâche>` (décisions & pourquoi). Le plan contient :
- fichiers à créer/modifier, dans l'ordre,
- contraintes d'architecture à respecter,
- tests attendus (cas nominaux + limites),
- critères d'acceptation.

### ② Codex → Antigravity (déclenché par toi : « passe en review », **si DoD satisfaite**)

**Definition of Done du Dev (objective, machine-vérifiable — pas au feeling) :**
1. branche commitée (1 point de plan = 1 commit),
2. **`pytest tests/ -x` au vert** (règle projet : pas de modif `src/` sans tests),
3. `CHANGELOG.md` `[Unreleased]` à jour (règle projet),
4. **note de handoff écrite**.

**Note de handoff Dev** = page GBrain `dev-handoff:<tâche>` (l'équivalent du doc de plan,
côté dev) :
- périmètre réellement livré,
- fichiers touchés,
- **écarts au plan + justification**,
- tests ajoutés,
- risques / incertitudes auto-signalés,
- mention explicite « prêt pour review ».

**Ce qu'Antigravity reçoit :** `git diff <base>..<branche>` + le doc de plan (intention) + la
note de handoff + accès GBrain (`gbrain search` pour les conventions).

### ③ Antigravity → Claude (review → triage)

**Sortie d'Antigravity :** findings catégorisés dans une page GBrain `review:<tâche>`.
Schéma d'un finding :

| Champ | Valeurs |
|---|---|
| `sévérité` | `bloquant` · `majeur` · `mineur` |
| `catégorie` | `bug` · `design` · `test-gap` · `nit` |
| `emplacement` | `fichier:ligne` |
| `description` | constat |
| `action_suggérée` | correctif proposé |

**Triage (par Claude, toi valides le routage) :**
- `bug` / `test-gap` / `nit` → **Codex** corrige (petite boucle Dev),
- `design` / `archi` → **re-plan par Claude** (le plan était faux, pas juste le code).

L'architecte trie parce qu'il est le mieux placé pour distinguer bug-de-code de
défaut-de-conception → **un seul point de coordination** au lieu de juger chaque finding.

### ④ Sortie de boucle

Review sans finding `bloquant`/`majeur` ouvert → Claude (ou toi) lance `/ship`.

## Definition of Done — récapitulatif par phase

| Phase | Agent | DoD (porte de sortie) |
|---|---|---|
| Plan | Claude | doc `docs/roadmap/NN_*.md` + page GBrain `plan:<tâche>`, validé par toi |
| Build | Codex | branche commitée + `pytest` vert + CHANGELOG à jour + `dev-handoff:<tâche>` |
| Review | Antigravity | page `review:<tâche>` avec findings catégorisés selon le schéma |
| Triage | Claude | chaque finding routé (re-dev / re-plan), validé par toi |

## Conventions GBrain

| Page | Écrite par | Contenu |
|---|---|---|
| `plan:<tâche>` | Claude | décisions d'archi & rationale |
| `dev-handoff:<tâche>` | Codex | ce qui a été livré, écarts, risques |
| `review:<tâche>` | Antigravity | findings catégorisés |
| (rétro) | Claude | leçons, via `/retro` `/learn` |

`<tâche>` = identifiant stable de l'unité de travail (idéalement le point de plan / nom de branche).
Granularité cible : une tâche tient sur **une branche** et **un cycle de review**.

## Concurrence & isolation

- **Séquentiel strict** en v1 : pas deux agents qui écrivent en même temps.
- **Une branche git par tâche.**
- gstack `/freeze` / `/guard` pour borner les zones d'édition d'un agent si besoin.
- Ne **pas** lancer `/sync-gbrain` pendant qu'un `gbrain autopilot` tourne (refus
  destructif anti-course, cf. gstack #1734).

## État des prérequis (machine, 2026-06-24)

- ✅ Claude Code (Opus 4.8) + gstack (55 skills) + GBrain (repo indexé, 2429 chunks)
- ✅ `codex` CLI 0.142.0 — `OPENAI_API_KEY` configurée
- ✅ Antigravity CLI (`agy`) — compte Google (successeur de Gemini CLI, sunset 06/2026). Accès GBrain vérifié ✓.
- ✅ Codex câblé via `AGENTS.md` (rôle Dev). Antigravity utilise des **plugins** (`agy plugin`), pas le `skills link` de Gemini ; gstack-review en plugin = optionnel (le brief porte la méthodo).

## Hors-scope v1 / évolutions futures

- **Modèle B** (Claude pilote codex/agy en shell-out, mode headless `agy -p`,
  `codex` non-interactif) — automatiser d'abord les bouts mécaniques (ex. déclenchement
  de la review sur diff).
- **Parallélisme** → migration GBrain PGLite → Postgres/Supabase (`/setup-gbrain --switch`).

## Décisions actées

1. Orchestration = **modèle A** (relais humain, séquentiel).
2. Transitions de phase = **déclenchées par le superviseur** (toi).
3. DoD du Dev = **objective** (pytest vert + CHANGELOG + note de handoff), pas au feeling.
4. Triage des findings = **par Claude** (architecte), routage validé par toi.
5. Coordination = **blackboard** (GBrain + git + doc de plan), pas d'appels inter-agents.