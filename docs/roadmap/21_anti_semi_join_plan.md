# Plan 21 — Anti-join & semi-join (`left_anti` / `left_semi`)

> **Branche :** `feat/anti-semi-join`
> **Statut :** Plan validé — prêt pour Build (Codex)
> **Architecte :** Claude Opus 4.8 (Plan)
> **Exécutant :** Codex (Build)
> **Reviewer :** Gemini (Review)
> **Superviseur :** Julien Houbart
> **Date :** 2026-06-24
> **Premier rodage du workflow multi-agents (Plan 20).**

---

## Contexte

Les schémas YAML `join:` n'acceptent aujourd'hui que `left / right / inner / full / cross`.
Le parseur IR (`core/ir.py`) applique une **whitelist stricte** (`VALID_JOIN_TYPES`) et lève
`Invalid join type` sur tout le reste — donc `anti` / `semi` sont rejetés au parse.

Objectif : supporter **`left_anti`** et **`left_semi`** sur le backend **Spark** (DataFrame,
le primaire), les autres backends SQL **échouant proprement** (fail-fast) en attendant une
éventuelle traduction `NOT EXISTS` / `EXISTS` (plan de suivi).

## Décisions d'architecture (validées)

1. **Portée backend** : **seul le backend Spark (DataFrame)** implémente anti/semi. **Tous
   les autres backends fail-fast**, ce qui couvre DEUX chemins distincts :
   - les **backends SQL-string** (`BigQuery`, générique — via `sql_base.py`) → ne pas émettre
     de `LEFT ANTI JOIN` invalide ;
   - le **backend Snowpark** (`snowpark.py`, DataFrame **non** SQL-string) → bien que Snowflake
     supporte nativement anti/semi, on **fail-fast quand même** tant que ce n'est pas
     explicitement supporté+testé (évite un comportement non testé hors périmètre).

   > **Amendement (post-review, cycle 1)** : la v1 du plan ne visait que le chemin SQL-string et
   > avait oublié que Snowpark est un 3ᵉ chemin DataFrame. Trou détecté par Gemini en Review,
   > tranché par l'architecte en Triage. Voir §3 bis.
2. **Types** : `left_anti` **et** `left_semi` (mêmes points de contact, ajoutés ensemble).
3. **Forme canonique** = `left_anti` / `left_semi` (tokens acceptés tels quels par
   `DataFrame.join(how=...)` de PySpark), avec alias d'entrée tolérants.

## Points de contact (contrat d'implémentation)

### 1. `core/ir.py` — accepter les nouveaux types
- Ajouter `"left_anti"` et `"left_semi"` à `VALID_JOIN_TYPES`.
- Étendre `_JOIN_TYPE_ALIASES` (clé déjà `.lower().replace("-", " ")`, underscore conservé) :
  - `"anti" → "left_anti"`, `"left anti" → "left_anti"`, `"leftanti" → "left_anti"`
  - `"semi" → "left_semi"`, `"left semi" → "left_semi"`, `"leftsemi" → "left_semi"`
- La validation `if join_type not in VALID_JOIN_TYPES` continue de lever pour tout le reste.

### 2. `core/interpreter.py` — ⚠️ le piège à éviter
Un anti/semi-join **ne renvoie que les colonnes de gauche**. Dans la branche « clés
différentes » (`on_l != on_r`), le code fait actuellement
`b.drop_columns(df_main, [df_to[r] for r in on_r])` **après** le join — ce qui n'a pas de sens
pour anti/semi (les colonnes de droite ne sont pas dans le résultat) et peut lever.
→ **Ne pas dropper les colonnes de droite quand `join_type ∈ {left_anti, left_semi}`.**
Le `how=join_type` reste passé tel quel à `b.join(...)` (PySpark gère).

### 3. `backends/sql_base.py` — fail-fast sur les backends SQL
Dans la construction de la clause JOIN (`_build`, ~ligne 341), si le type normalisé est
`left_anti` / `left_semi` (ou leurs formes upper « LEFT ANTI » / « LEFT SEMI ») :
→ lever `NotImplementedError` avec un message explicite :
« anti/semi join only supported on the Spark DataFrame backend so far (see Plan 21) ».
Ne **pas** émettre `LEFT ANTI JOIN` (invalide en BigQuery/Snowflake).

### 3 bis. `backends/snowpark.py` — fail-fast (ajout post-review)
La méthode `SnowparkBackend.join(left, right, on, how)` passe actuellement `how` tel quel à
`left.join(right, on=on, how=how)` → anti/semi s'exécuteraient sur Snowflake (non testé, hors
périmètre). Ajouter en tête de `join()` :
→ si `how` (normalisé) ∈ {`left_anti`, `left_semi`} (et alias `anti`/`semi`/…), lever
`NotImplementedError` avec le **même message** que `sql_base.py`.
Réutiliser la normalisation/alias de `core/ir.py` plutôt que de redupliquer la liste.

### 4. Tests (`tests/`) — règle obligatoire
Fichiers existants à étendre (réutiliser leur style) :
- **`tests/test_core_join.py`** — joins Spark via `MagicMock`, on asserte le `how=` passé
  (`df_a.join.assert_called_once_with(df_b, on=[...], how="...")`) et l'**invariant
  `.columns` jamais appelé** pendant le planning. Ajouter :
  - `how="left_anti"` / `how="left_semi"` bien transmis ;
  - alias (`anti`, `left semi`, …) → canonique ;
  - **cas clés différentes** : anti/semi **ne déclenche pas** le drop des colonnes de droite
    (régression du piège §2 ; cf. `test_different_keys_uses_expression_join_and_drop`).
- **`tests/test_backend_sql_base.py`** — anti/semi lève `NotImplementedError` clair.
- **`tests/test_backend_snowpark.py`** — `SnowparkBackend.join(...)` lève `NotImplementedError`
  pour `left_anti`/`left_semi` (et alias), et reste inchangé pour les types normaux.
- Tests de parse IR : `left_anti`/`left_semi` acceptés, alias normalisés, type bidon toujours
  rejeté (là où `VALID_JOIN_TYPES` est testé).

> Note : ces tests mockent Spark (pas de cluster). La sémantique runtime réelle (anti = lignes
> gauche sans match ; semi = lignes gauche avec match, sans duplication) est garantie par
> PySpark lui-même via le `how=` ; on teste donc le **câblage** (le bon `how`, pas de drop),
> pas le moteur Spark.

### 5. Docs (règle obligatoire)
- `CHANGELOG.md` `[Unreleased]`.
- `CLAUDE.md` + `AGENTS.md` : ajouter `left_anti` / `left_semi` à la liste des types de join
  (en notant « Spark-only pour l'instant »).
- Doc mkdocs du bloc `join:` si elle liste les types.

## Critères d'acceptation (Definition of Done du Dev / Codex)

- `pytest tests/ -x` au vert (incluant les nouveaux tests anti/semi).
- `ruff check src/` propre.
- `CHANGELOG.md` à jour.
- Anti/semi fonctionnent sur Spark (DataFrame) ; SQL-string backends lèvent proprement.
- Page GBrain `dev-handoff:anti-semi-join` écrite (périmètre, écarts, risques).

## Risques / cas limites (à surveiller en Review — Gemini)

- **Drop des colonnes de droite** (§2) : c'est LE point sensible. Vérifier qu'anti/semi avec
  clés différentes ne tente pas le drop.
- **Token PySpark** : confirmer que `how="left_anti"` / `"left_semi"` est bien accepté par la
  version de PySpark du projet (formes alternatives `anti`/`semi` aussi acceptées, mais on
  fige le canonique sur `left_*`).
- **Sémantique semi vs inner** : un semi-join ne duplique pas les lignes de gauche même en cas
  de multiples correspondances à droite — à tester explicitement.
- **Fail-fast SQL** : message clair, pas de SQL silencieusement cassé.

## Hors-scope (plan de suivi éventuel)

- Traduction `NOT EXISTS` (anti) / `EXISTS` (semi) pour BigQuery / Snowflake.
- `right_anti` / `right_semi` (PySpark ne les a pas nativement — nécessiteraient une inversion).
