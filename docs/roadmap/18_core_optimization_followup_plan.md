# Plan 18 — Suite de l'optimisation du core : reste du plan 17 + correctifs revue

> **Branche :** `feat/plan18-core-followup`
> **Statut :** En attente de validation utilisateur
> **Exécutant :** Claude Sonnet 4.6 (en session, supervisé)
> **Superviseur :** Julien Houbart
> **Date :** 2026-06-12

---

## Contexte

La PR #29 (Copilot) a livré **12 des 21 points** du
[plan 17](17_core_optimization_plan.md) : lot 1 complet (fail-fast + catalogue
d'opérateurs), lot 0 complet (quick wins), et les phases 2.1–2.2 du refactor engine.

Restent non implémentés :

- **2.3 / 2.4** — `core.py` fait toujours ~1 370 lignes ; `process_schema`,
  `get_select_expressions` et les patterns `run_*` y sont toujours.
- **Lot 3 entier** — pas de `ir.py` ; le parsing d'op-strings est toujours tripliqué
  (~25 `startswith` chacun dans `core/operations.py`, `backends/snowpark.py`,
  `backends/sql_base.py`) ; `process_schema` ré-importe `_normalize_filters` au runtime.
- **Lot 4 entier** — pas de `json_schema.py`, pas de `skifer validate`.
- **Règle transversale n°6** non appliquée à `core.py` : 69 `print()` restants,
  docstrings en français.

Une revue complémentaire (2026-06-12) a en outre identifié **7 nouveaux points**
hors périmètre du plan 17 (lot B ci-dessous), dont deux problèmes de correctness.

---

## Règles d'exécution (identiques au plan 17)

1. **Un point du plan = un commit séparé.** Message conventionnel :
   `feat(plan18-B.2): validate when/then/else structure at load time`.
   Les points repris du plan 17 gardent leur référence d'origine :
   `refactor(plan17-2.3): extract SchemaInterpreter from SkiferEngine`.
2. **Après chaque point** : `pytest tests/ -x --tb=short` au vert + `ruff check src/`
   propre avant de committer.
3. **Chaque modification de `src/`** : test associé + entrée `CHANGELOG.md` sous
   `## [Unreleased]` (même commit).
4. **Ne jamais toucher au numéro de version** (`pyproject.toml`).
5. **Aucun breaking change sur l'API publique** — exceptions assumées documentées
   en `### Changed` dans le CHANGELOG (B.1, B.2, B.4).
6. **Transversal** : dans chaque commit qui modifie substantiellement un fichier,
   remplacer ses `print()` par `logging` et passer ses docstrings/messages en anglais.

---

## Vue d'ensemble et ordre d'implémentation

```
Lot B′ — Correctness immédiats (B.1, B.2, B.3)   indépendant — à faire AVANT le refactor
Lot 2-fin — Refactor engine (2.3, 2.4)           dépend de rien ; mécanique
Lot 3 — IR partagé (3.1 → 3.5)                   dépend de 2.3 ; absorbe B.7 (en 3.3) et B.4
Lot 4 — Outillage (4.1, 4.2)                     dépend de 3.1
Lot B″ — Perf opportunistes (B.5, B.6)           optionnel, indépendant, en dernier
```

Ordre linéaire recommandé :
**B.1 → B.2 → B.3 → 2.3 → 2.4 → 3.1 → 3.2 → 3.3 (+B.7) → 3.4 → 3.5 → B.4 → 4.1 → 4.2 → B.5 → B.6**

Rationale : B.1–B.3 corrigent du comportement sur le code actuel pendant que les tests
existants servent encore d'oracle direct ; les refactors 2.3+ déplacent ensuite un code
déjà corrigé.

---

## Lot B′ — Correctness immédiats (nouveaux points, revue 2026-06-12)

### B.1 — Écriture atomique : supprimer le drop-avant-calcul

- **Modifier :** `core/core.py` (`run_process_to_table` ~l.894, `run_process_and_split`
  ~l.864, `run_union_sources_to_table` ~l.969), `core/writer.py` / `backends/spark.py`
  si nécessaire.
- Problème : `_drop_table_if_exists(fqn)` est appelé **avant** `process_schema`.
  Si le traitement échoue, la table précédente est déjà détruite — perte de données
  en cas d'échec de job.
- Cible : écrire en `mode("overwrite")` (atomique sur Delta) et supprimer le drop
  préalable. Conserver le drop uniquement là où le backend ne supporte pas
  l'overwrite atomique (le déclarer dans le Protocol si besoin :
  `supports_atomic_overwrite: bool`).
- Vérifier l'interaction avec `overwriteSchema` (changement de schéma de sortie
  entre deux runs) : reproduire le comportement actuel drop+create → option
  `overwriteSchema=true` sur Delta.
- **CHANGELOG : `### Changed`** (l'ancienne table survit désormais à un run échoué).
- **Tests :** `tests/test_core.py` — la table précédente survit à une exception
  levée pendant `process_schema` ; l'overwrite remplace bien les données ;
  changement de schéma de sortie accepté.
- **Commit :** `fix(plan18-B.1): atomic overwrite writes — stop dropping target table before processing`

### B.2 — Valider la structure des chaînes when/then/else compactes

- **Modifier :** `core/schema_loader.py` (validation au chargement),
  `core/core.py` (`get_select_expressions`, filet runtime).
- Problème : forme compacte `["when:...", "then:...", "else:..."]` — si la liste
  d'ops commence par `when:` mais contient **moins de 3 éléments**, rien n'est
  appliqué (colonne passée inchangée, silencieusement) ; si elle en contient
  **plus de 3**, les ops au-delà du `else` sont ignorées (core.py ~l.645-651).
- Cible :
  - Au chargement (`_validate_ops_in_select_list`) : une liste d'ops commençant
    par `when:` doit être exactement `[when, then, else]` (3 éléments, préfixes
    corrects) → sinon `ValueError` agrégée avec les autres erreurs du schéma.
  - Au runtime (`get_select_expressions`) : même condition violée → `ValueError`
    (défense en profondeur pour les dicts construits à la main).
- **CHANGELOG : `### Changed`** (schémas silencieusement cassés → erreur explicite).
- **Tests :** `tests/test_schema_loader.py` + `tests/test_core.py` — 2 éléments,
  4 éléments, préfixes manquants, cas nominal 3 éléments inchangé.
- **Commit :** `fix(plan18-B.2): fail fast on malformed compact when/then/else chains`

### B.3 — Chaînage when multi-conditions via le backend (fuite PySpark)

- **Modifier :** `core/core.py` (`get_select_expressions`, forme dict ~l.624),
  `core/backend.py` (Protocol), `backends/snowpark.py`, `backends/sql_base.py`.
- Problème : la forme dict à `when` multiples chaîne `result_col.when(...)`
  **directement sur l'objet colonne** (commentaire existant : « still
  PySpark-style ») — comportement non garanti sur Snowpark/`SQLColumn`.
- Cible : ajouter au Protocol une méthode `when_chain(conditions: list[tuple[cond,
  then]], otherwise) -> Column` (ou équivalent) implémentée par chaque backend ;
  `get_select_expressions` l'utilise au lieu du chaînage direct.
- Vérifier d'abord si `SQLColumn`/Snowpark exposent un `.when()` fonctionnel —
  si oui, le test de parité suffit ; sinon, c'est un bugfix multi-plateforme.
- **Tests :** `tests/test_backend_snowpark.py`, `tests/test_backend_sql_base.py` —
  forme dict avec 2+ `when` produit le bon SQL/colonne sur chaque backend.
- **Commit :** `fix(plan18-B.3): route multi-when chains through backend (remove PySpark-only .when chaining)`

---

## Lot 2-fin — Refactor engine (repris du plan 17, points 2.3 et 2.4)

Spécifications détaillées : voir [plan 17 §Lot 2](17_core_optimization_plan.md).

### 2.3 — `SchemaInterpreter` (`core/interpreter.py`)

- Déplacer `process_schema`, `get_select_expressions`, `_apply_business_rules`
  vers `SchemaInterpreter(backend, context, config)` ; délégations une-ligne dans
  l'engine. Déplacement **mécanique**, aucune réécriture dans le même commit.
- Tests d'interprétation déplacés vers `tests/test_interpreter.py`.
- **Commit :** `refactor(plan17-2.3): extract SchemaInterpreter from SkiferEngine`

### 2.4 — Patterns d'exécution (`core/patterns.py`)

- Déplacer `run_process_to_table`, `run_process_and_split`,
  `run_union_sources_to_table`, `run_from_yaml` ; délégations dans l'engine.
- Tests déplacés vers `tests/test_patterns.py`.
- **Commit :** `refactor(plan17-2.4): extract run patterns into core/patterns.py`

> Au terme de 2.4, appliquer la règle transversale n°6 au reliquat de `core.py`
> (print→logging, docstrings EN) — l'engine résiduel doit être ≤ ~300 lignes.

---

## Lot 3 — IR partagé (repris du plan 17, points 3.1 → 3.5)

Spécifications détaillées : voir [plan 17 §Lot 3](17_core_optimization_plan.md).

### 3.1 — Dataclasses IR + parseur (`core/ir.py`)

- `ParsedOp`, `ParsedFilter`, `ParsedColumnSpec`, `ParsedSchema`,
  `parse_to_ir(normalized_schema_dict)`. `load_schema`/`parse_schema` retournent
  toujours le dict (compat) avec l'IR attaché.
- **Inclure la structure when/then/else validée en B.2** dans `ParsedColumnSpec`
  (champ `when_chain` typé — plus de listes positionnelles).
- **Commit :** `feat(plan17-3.1): add intermediate representation (ir.py) parsed once at load`

### 3.2 — Les backends consomment l'IR

- `apply_op(c, op: ParsedOp)` / `build_filter(f: ParsedFilter)` en tables de
  dispatch `{op_name: fonction}` par backend ; suppression des trois parseurs
  `startswith` ; anciennes méthodes string en façade dépréciée.
- `process_schema`/interpreter ne doit plus ré-appeler `_normalize_filters` au
  runtime (l'IR est déjà normalisé au chargement).
- **Commit :** `refactor(plan17-3.2): backends consume IR via per-op dispatch tables`

### 3.3 — Inspection sur l'IR (+ B.7)

- `describe_schema` / `infer_output_schema` consomment `ParsedSchema`.
- **B.7 intégré ici :** déplacer la table `_TYPE_FROM_OP` codée en dur dans
  `infer_output_schema` (core.py ~l.1258) vers `op_catalog.py` (champ
  `output_type` sur les `OpSpec`) — une seule source de vérité pour les types.
- **Commit :** `refactor(plan17-3.3): describe_schema and infer_output_schema consume IR; op output types in catalog`

### 3.4 — Lineage sur l'IR

- `LineageTracker.from_schema` consomme l'IR ; sortie inchangée.
- **Commit :** `refactor(plan17-3.4): lineage tracker consumes IR`

### 3.5 — Formes YAML structurées (mapping style)

- Formes mapping pour `filter` et `select_final` **en plus** des formes compactes
  (alias valides, même IR). Documenter dans `CLAUDE.md`/`AGENTS.md`/mkdocs.
- **Commit :** `feat(plan17-3.5): structured YAML forms for filters and ops (mapping style)`

### B.4 — Erreurs de lecture explicites dans `run_union_sources_to_table`

- **Modifier :** `core/patterns.py` (après 2.4).
- Problème : le `try/except Exception` autour de `read_table` (~l.975-978 de
  l'actuel core.py) traite **toute** erreur (permission, FQN invalide, réseau)
  comme « table manquante » avec un simple warning.
- Cible : tester l'existence via le backend (`table_exists`) et ne tolérer que
  l'absence réelle ; toute autre exception remonte. Logger en `logger.warning`
  la liste des tables réellement absentes.
- **CHANGELOG : `### Changed`**.
- **Tests :** `tests/test_patterns.py` — table absente tolérée, erreur de
  permission propagée.
- **Commit :** `fix(plan18-B.4): only tolerate genuinely missing tables in union pattern`

---

## Lot 4 — Outillage (repris du plan 17, points 4.1 et 4.2)

Spécifications détaillées : voir [plan 17 §Lot 4](17_core_optimization_plan.md).

### 4.1 — JSON Schema généré depuis le catalogue

- `core/json_schema.py` → `schemas/skifer-pipeline.schema.json` committé,
  test de non-dérive. Inclure les formes mapping de 3.5. Doc mkdocs
  (branchement VS Code / PyCharm).
- **Commit :** `feat(plan17-4.1): generate pipeline JSON Schema from operator catalog`

### 4.2 — `skifer validate`

- Sous-commande CLI : globs, params sentinelle pour `{{ }}`, agrégation de toutes
  les erreurs, exit code 1, zéro dépendance Spark.
- **Commit :** `feat(plan17-4.2): add 'skifer validate' CLI subcommand`

---

## Lot B″ — Perf opportunistes (optionnel, faible priorité)

### B.5 — Cache de la détection d'environnement au démarrage

- **Modifier :** `core/core.py` (`_auto_detect_environment`) ou `core/config.py`.
- La détection teste les catalogues séquentiellement (un appel réseau par entrée
  de `priority_check`) à chaque instanciation d'engine. Cible : cache de session
  (mémoire process) + option config `env_detection_cache: false` pour désactiver.
- **Commit :** `perf(plan18-B.5): cache environment detection within a session`

### B.6 — Cache de la résolution sandbox

- **Modifier :** `core/sandbox.py`.
- `SandboxResolver.resolve` refait `table_exists`/`schema_exists` à chaque run
  pour chaque table. Cible : mémoïsation par instance d'engine (invalidable),
  les clones une fois faits ne sont plus re-vérifiés dans la même session.
- **Commit :** `perf(plan18-B.6): memoize sandbox resolution per engine session`

---

## Risques & mitigations

| Risque | Impact | Mitigation |
|---|---|---|
| B.1 : overwrite Delta ≠ drop+create sur les changements de schéma | Moyen | `overwriteSchema=true` + test dédié changement de schéma ; drop conservé si backend sans overwrite atomique |
| B.2/B.3 cassent des schémas existants silencieusement défectueux | Faible | C'est le but (même logique que plan17-1.3) ; CHANGELOG `### Changed`, message d'erreur actionnable |
| 2.3 : régression subtile en déplaçant `process_schema` | Élevé | Déplacement mécanique, tests existants = oracle, aucune modification de logique dans le même commit ; B.1–B.3 faits **avant** pour ne pas mélanger fix et move |
| 3.2 : divergence ancien parseur string ↔ dispatch IR | Élevé | Tests backend existants inchangés comme oracle de parité ; façade dépréciée pour les appels directs |
| B.4 : `table_exists` plus coûteux que try/except | Faible | Un appel métadonnées par table source ; négligeable devant la lecture |

## Stratégie de vérification

- `pytest tests/ -x --tb=short` après **chaque point** ; `ruff check src/` avant
  chaque commit.
- Lots 2-fin et 3 : tests existants = oracle de non-régression — ne pas les
  modifier dans le même commit que le refactor (sauf déplacement à l'identique).
- Fin de chaque lot : exécuter l'exemple de `example/` en mode LOCAL si applicable.

## Checklist de montée de version (rappel)

1. Aucune nouvelle feature sans tests associés et validés
2. Tests existants au vert — ou mis à jour avec raison documentée
3. `CHANGELOG.md` à jour
4. `CLAUDE.md` / `AGENTS.md` à jour
5. Documentation mkdocs (`docs/`) à jour
