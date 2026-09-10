# Plan 24 — Opérateur de filtre `not_between`

> **Branche :** `feat/filter-not-between`
> **Exécution :** orchestration autonome (Modèle B, Plan 22) — 2ᵉ cycle
> **Reviewer :** vibe (Mistral) · **Date :** 2026-06-25

## Contexte
Complément de `between` (Plan 23, déjà sur main) : `"col:not_between:lo,hi"` → `col < lo OR col > hi`
(strictement hors de l'intervalle [lo,hi]). Calque `between` en négué. Additif, faible risque.

## Sous-tâches & routage modèle
| # | Sous-tâche | Difficulté | Dev (Codex `-m`) | Review (vibe) | Dépend |
|---|---|---|---|---|---|
| S1 | `not_between` dans op_catalog + docs (calque `between`) | trivial | `gpt-5.4-mini` | léger | — |
| S2 | Spark `_build_filter_expression` : `(c<lo)\|(c>hi)` + tests | standard | `gpt-5.4` | léger | S1 |
| S3 | SQL `build_filter` : `(c < lo OR c > hi)` + tests | complexe | `gpt-5.5` | **vibe approfondi** | S2 |

## Briefs (deltas vs `between`)
- S1 : ajouter `not_between` (arity="list") à `FILTER_OPERATORS`, comme `between`. Docs CLAUDE.md/AGENTS.md.
- S2 : brancher `op == "not_between"` dans `_build_filter_expression` ; parser 2 valeurs (sinon ValueError),
  retourner `(c < lo) | (c > hi)`. Tests : hors-bornes, bornes EXCLUES, erreur arité.
- S3 : brancher dans `build_filter` (sql_base) ; émettre `({c} < {lo} OR {c} > {hi})` via `_smart_val`. Tests SQL.
- **Contrainte de périmètre** (F3) : ne modifier QUE la logique `not_between`, aucun reformatage.

## DoD / gate (par sous-tâche)
commit + `pytest tests/ -x` vert (gate = SUITE COMPLÈTE, F4) + CHANGELOG + dev-handoff. Invocation `codex exec … < /dev/null` (F1).

## Risque
`not_between` exclut les bornes (≠ `between` qui les inclut) — à bien tester. Sémantique NULL : ligne exclue si col NULL (ok).
