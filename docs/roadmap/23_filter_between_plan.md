# Plan 23 — Opérateur de filtre `between`

> **Branche :** `feat/filter-between`
> **Statut :** Plan v1 — en attente de validation (GO)
> **Architecte :** Claude Opus 4.8 (Plan)
> **Exécution :** orchestration autonome (Modèle B, [Plan 22](22_autonomous_orchestration_plan.md))
> **Reviewer :** Antigravity (`agy`) · **Superviseur :** Julien Houbart
> **Date :** 2026-06-25
> **1er rodage du Modèle B (orchestration autonome + routage modèle par sous-tâche).**

---

## Contexte

Ajouter l'opérateur de filtre **`between`** : `"col:between:lo,hi"` → `lo <= col <= hi` (inclusif).
Aujourd'hui il faut deux opérateurs (`greater_than_equal` + `less_than_equal`) ou un `sql:` brut.
Purement **additif** (aucun opérateur existant modifié), faible risque. Valeurs séparées par
virgule — même mécanique que `in`/`not_in`.

## Décisions

- 3 sous-tâches **séquentielles** (S1 → S2 → S3), difficulté croissante → routage modèle.
- Forme : `between:lo,hi` (exactement 2 valeurs). Bornes **inclusives**.
- Hors-scope : `not_between` (idée différée), bornes exclusives, valeurs date typées (Spark/SQL
  gèrent la comparaison native sur le type de colonne).

## Sous-tâches & routage modèle

| # | Sous-tâche | Difficulté | Dev (Codex `-m`) | Review (agy) | Dépend |
|---|---|---|---|---|---|
| S1 | Enregistrer `between` dans `op_catalog` + docs | trivial | `gpt-5.4-mini` | Gemini 3.5 Flash (High) | — |
| S2 | Implémentation Spark + tests | standard | `gpt-5.4` | Gemini 3.5 Flash (High) | S1 |
| S3 | Émission SQL (`sql_base`) + tests | complexe | `gpt-5.5` | **Gemini 3.1 Pro (High)** | S2 |

## Briefs par sous-tâche (contrat remis à Codex)

### S1 — Registre + docs (trivial)
- `src/skifer/core/op_catalog.py` : ajouter une entrée `between` à `FILTER_OPERATORS`
  (`OperatorSpec(canonical="between", arity=…, description="Passes rows where lo <= column <= hi (inclusive). Two comma-separated values.")`).
  L'arité prend **2 valeurs** — réutiliser/introduire l'arité adéquate de façon cohérente avec
  `in` (voir comment `in` est déclaré). Pas d'alias requis.
- Docs : ajouter `between` à la liste des opérateurs de filtre dans `CLAUDE.md` et `AGENTS.md`.
- **DoD** : `pytest tests/test_op_catalog.py -x` vert ; opérateur listé ; aucune régression.

### S2 — Spark (standard)
- `src/skifer/core/operations.py`, fonction `_build_filter_expression` : ajouter la
  branche `between`. Parser la valeur comme `in` (split virgule, `.strip()`, attendre **exactement
  2** éléments → sinon `ValueError` explicite). Retourner `(c >= lo) & (c <= hi)`.
- Tests : ajouter aux tests des filtres Spark (localiser via `grep greater_than_equal tests/`,
  probablement `tests/test_core.py`) : cas nominal, bornes incluses, erreur si ≠ 2 valeurs.
- **DoD** : `pytest tests/ -x` vert ; `ruff check src/skifer/core/operations.py` propre.

### S3 — SQL backends (complexe)
- `src/skifer/backends/sql_base.py`, méthode `build_filter` : ajouter `between`.
  Émettre `{c_expr} BETWEEN {lo} AND {hi}` (ou `{c_expr} >= {lo} AND {c_expr} <= {hi}`).
  **Attention** : échappement via `_sql_escape` pour les valeurs chaîne, gestion numérique vs
  chaîne (cf. comment les comparaisons existantes formatent leurs valeurs), parsing des 2 valeurs
  (split virgule, exactement 2).
- Tests : `tests/test_backend_sql_base.py` — SQL généré correct (valeurs échappées, 2 bornes),
  erreur si ≠ 2 valeurs.
- **DoD** : `pytest tests/ -x` vert (1300+) ; `ruff` propre sur le fichier ; CHANGELOG `[Unreleased]`.

## DoD globale (avant ship)
- `pytest tests/ -x` vert intégral.
- `CHANGELOG.md` `[Unreleased]` mentionne l'opérateur `between`.
- `CLAUDE.md` + `AGENTS.md` listent `between`.
- Page GBrain `dev-handoff:filter-between` écrite (par sous-tâche ou cumulée).

## Escalade (Modèle B)
Auto-géré : bug / test-gap / nit (re-dev dans le cap de 2). **Escalade** au superviseur si :
finding **design** (ex. faut-il une arité `pair` générique ? bornes exclusives ?), build échoué
2× sur une sous-tâche, garde-fou atteint.

## Risques
- **Parsing virgule** : une valeur contenant une virgule casse le split → documenter (même
  limite que `in`), pas de sur-ingénierie.
- **Types/échappement SQL** (S3) : le point sensible → `gpt-5.5` + review Pro.
- **Arité** : si `op_catalog` n'a pas d'arité « 2 valeurs », décider S1 (réutiliser `multi` ou
  introduire `pair`) — escalader si ça touche la validation générique.
