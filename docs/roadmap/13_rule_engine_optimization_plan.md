# Plan 13 — Rule Engine Optimization (typage + fusion d'exécution)

## Contexte

Le Plan 06 a livré `RuleAnalyzer` comme **linter statique** : il détecte OVERWRITE, SHARED_READ et DUPLICATE_EXPR via AST, mais reste purement informatif (`engine.explain_rules()`). Le moteur exécute toujours les règles séquentiellement (`core.py:551-566`) :

```python
for rule_name in rules_list:
    df = RuleRegistry.get_rule(rule_name)(df)
```

Le Plan 06 anticipait explicitement un **Feature 7 — Runtime Resource Optimization** qui transformerait l'analyzer en moteur de décision. Ce Plan 13 est cette suite.

### Problème de fond

Le contrat actuel `df → df` impose à chaque règle d'appeler `withColumn()`. Sur N règles ajoutant chacune M colonnes :
- Catalyst doit re-parcourir le plan logique en **O(N²)** (anti-pattern Spark documenté, cf. SPARK-26224).
- Aucune fusion possible côté framework : chaque règle est une boîte noire qui retourne un DataFrame.
- `RuleAnalyzer` détecte les expressions dupliquées mais ne peut pas agir.
- Les règles complexes incitent à grossir au lieu de découper, faute de pouvoir le faire sans coût.

### Cible

- L'API n'est pas distribuée → **rupture API contrôlée acceptable**.
- Cas d'usage observé : 4-5 règles par pipeline, majoritairement `withColumn` simples/multiples + agrégats.
- Plateforme cible : Databricks + Spark uniquement.
- Le moteur peut réordonner les règles, à condition de **le logger**.

## Objectifs

1. **Typage explicite** des règles via le décorateur (`projection` / `aggregation` / `transform`).
2. **Fusion automatique** des règles `projection` consécutives en un seul `select()`, avec tri topologique des dépendances détectées par `RuleAnalyzer`.
3. **Fusion des agrégations** partageant la même clé `groupBy`.
4. **Lints performance** (UDF Python qui casse Photon, complexité excessive).
5. Permettre le **découpage naturel** des règles complexes sans pénalité d'exécution.

## Non-objectifs

- Aucune réécriture côté backend non-Spark (SQL, DuckDB, BigQuery — reportés à un futur plan).
- Pas de cache automatique pour ce plan (pas de signal clair côté utilisateur — voir « Évolutions futures »).
- Pas de remontée automatique des règles `transform` vers `projection` (impossible à garantir, donc on ne tente pas).

---

## Architecture

### Contrat de règle typé

```python
@RuleRegistry.register_rule(kind="projection")  # par défaut
def flag_high_value(df) -> dict[str, Column]:
    return {
        "is_high_value": F.when(F.col("amount") >= 1000, 1).otherwise(0),
    }

@RuleRegistry.register_rule(kind="aggregation")
def revenue_by_region(df) -> DataFrame:
    return df.groupBy("region").agg(F.sum("amount").alias("total"))

@RuleRegistry.register_rule(kind="transform")  # échappatoire
def complex_logic(df) -> DataFrame:
    return df.filter(...).withColumn(...)
```

| Kind | Signature | Comportement moteur |
|---|---|---|
| `projection` | `df → dict[str, Column]` | Toutes les règles `projection` consécutives sont **fusionnées en un seul `select(*existing, *new_cols)`**. Tri topologique sur les dépendances inter-règles. Dédup `DUPLICATE_EXPR`. |
| `aggregation` | `df → DataFrame` | Un étage par règle. Si plusieurs règles `aggregation` partagent **le même `groupBy`**, fusion en un seul `agg(...)`. Sinon, séquentiel. |
| `transform` | `df → DataFrame` | Identique au comportement actuel. Échappatoire pour cas tordus (filtres complexes, fenêtres, opérations chainées). |

### Schéma d'exécution

Le moteur regroupe les règles en **étages homogènes** dans l'ordre YAML :

```
[proj_a, proj_b, agg_c, proj_d, trans_e, proj_f]
↓
[stage(proj_a, proj_b)] → [stage(agg_c)] → [stage(proj_d)] → [stage(trans_e)] → [stage(proj_f)]
```

Chaque étage `projection` est fusionné en un seul `select()`. Les étages `aggregation` adjacents avec même `groupBy` sont fusionnés.

### Tri topologique intra-étage

Pour un étage `projection`, on construit un DAG : `règle B dépend de règle A` si B lit une colonne écrite par A. `RuleAnalyzer` fournit déjà `input_columns` / `output_columns`. Détection de cycles → erreur explicite. Réordonnancement → log INFO.

---

## Fichiers créés / modifiés

| Fichier | Action | Rôle |
|---|---|---|
| `src/skifer/core/registry.py` | Modifié | `register_rule(kind="projection"\|"aggregation"\|"transform")` + métadonnées stockées |
| `src/skifer/core/rule_planner.py` | **Créé** | `RulePlanner` : regroupe les règles en étages, tri topo, fusion projection/agg |
| `src/skifer/core/rule_executor.py` | **Créé** | `RuleExecutor` : exécute le plan généré par `RulePlanner` |
| `src/skifer/core/core.py` | Modifié | `_apply_business_rules()` délègue à `RulePlanner` + `RuleExecutor` |
| `src/skifer/core/rule_analyzer.py` | Modifié | Nouvelle méthode `build_dependency_graph(profiles) → DAG` |
| `src/skifer/__init__.py` | Modifié | Export `RulePlanner`, `RuleExecutor` |
| `tests/test_rule_planner.py` | **Créé** | Tests unitaires : regroupement, fusion, tri topo, cycles |
| `tests/test_rule_executor.py` | **Créé** | Tests d'intégration avec backend Spark fake/local |
| `tests/test_core.py` | Modifié | Tests bout-en-bout : règles `projection` fusionnées, mixage avec `aggregation` |
| `tests/test_registry.py` | Modifié | Tests `kind=` paramètre + validation signatures |
| `docs/rules.md` | **Créé** ou modifié | Documentation utilisateur : nouveau contrat, migration |
| `CHANGELOG.md` | Modifié | Entrée `[Unreleased]` avec breaking change explicite |

---

## Phases

### Phase 1 — Typage explicite (`kind=`)

**Objectif :** poser le contrat sans changer le comportement d'exécution.

- `RuleRegistry.register_rule(kind="projection"|"aggregation"|"transform")` — par défaut `"projection"`.
- Métadonnées stockées : `_rules[name] = {"func": ..., "kind": ..., "signature": ...}`.
- `RuleRegistry.get_rule(name)` retourne désormais un objet `RuleSpec` (dataclass) au lieu de la fonction brute. Compat backward via `.func`.
- Validation à l'enregistrement : signature inspectée. Si `kind="projection"` mais la fonction retourne autre chose qu'un dict (vérifié dynamiquement au premier appel), erreur explicite avec message d'orientation.
- `_apply_business_rules` adapté : appelle `rule.func(df)`, branche selon `kind` (mais sans fusion à ce stade — toujours séquentiel, sauf que `projection` doit produire un dict que le moteur applique via un `select` ligne par règle, pas encore fusionné).

**Critère de sortie :** tous les tests existants passent, les règles existantes sont migrées vers `kind="projection"` et retournent `dict[str, Column]`. CHANGELOG mentionne la rupture API.

**Commit :** `feat(plan-13/phase-1): introduce typed rule contract (kind=projection|aggregation|transform)`

### Phase 2 — Fusion `projection` + tri topologique

**Objectif :** fusionner toutes les règles `projection` consécutives en un seul `select()`.

- `RulePlanner.plan(rule_names) → list[RuleStage]` : regroupe par `kind` en étages homogènes.
- Pour chaque étage `projection` :
  - Appel de chaque règle pour récupérer le `dict[str, Column]`.
  - `RuleAnalyzer.build_dependency_graph()` calcule les dépendances inter-règles.
  - Tri topo des règles, détection cycles → `RuleCycleError`.
  - Logs INFO si l'ordre YAML diffère de l'ordre topologique : `[Planner] Reordered rules: [A,B,C] → [B,A,C] (B writes col X read by A)`.
  - Dédup `DUPLICATE_EXPR` : si deux règles produisent la même expression, alias commun généré une fois, référencé par les consommateurs.
  - Génération d'un unique `df.select(*existing_cols, *all_new_cols_ordered)`.
- `RuleExecutor.execute(df, stages)` applique séquentiellement les étages.

**Critère de sortie :** tests démontrent que 5 règles `projection` produisent **un seul nœud `Project` dans le plan logique** Spark (vérifié via `df.explain(extended=True)` ou comptage de nœuds). Performance : test avec 20 règles produit un plan O(1) au lieu de O(N).

**Commit :** `feat(plan-13/phase-2): fuse consecutive projection rules into a single select()`

### Phase 3 — Fusion `aggregation` à même `groupBy`

**Objectif :** éviter N shuffles quand plusieurs règles agrègent sur les mêmes clés.

- Pour un étage `aggregation` contenant plusieurs règles :
  - Inspection de chaque DataFrame retourné pour extraire `groupBy keys` et `agg exprs` (via `df.queryExecution.analyzed` ou en demandant aux règles de retourner un format intermédiaire — à arbitrer en début de phase).
  - **Option A (statique)** : changer le contrat `aggregation` pour retourner `(group_keys: list[str], agg_exprs: dict[str, Column])`. Plus propre, mais nouvelle rupture API.
  - **Option B (dynamique)** : inspecter le plan logique Spark. Plus magique, plus fragile.
  - **Décision prévue : Option A** (cohérent avec la philosophie déclarative du framework).
- Si plusieurs règles partagent les mêmes clés → fusion en un seul `groupBy(*keys).agg(*all_exprs)`.
- Sinon : séquentiel, chaque règle dans son propre étage.

**Critère de sortie :** test avec 3 règles `aggregation` sur même clé → 1 shuffle observé (vérifié via plan).

**Commit :** `feat(plan-13/phase-3): fuse aggregation rules sharing the same groupBy key`

### Phase 4 — Lints performance

**Objectif :** rendre visible ce qui casse Photon ou ce qui devrait être découpé.

- `RuleAnalyzer.detect_warnings()` enrichi :
  - **PHOTON_BREAKING** : détecte `@udf`, `@pandas_udf`, `F.udf(...)` dans la source de la règle → warning explicite avec lien doc Photon.
  - **COMPLEXITY_HIGH** : seuils sur LOC (>30 lignes) ou nombre de `withColumn` (>5) → suggestion de découper.
- `engine.explain_rules()` affiche ces nouveaux warnings.
- Pas de modification de l'exécution — purement informatif (comme Plan 06).

**Critère de sortie :** tests unitaires sur chaque détecteur, exemple dans `docs/rules.md`.

**Commit :** `feat(plan-13/phase-4): add Photon-breaking and complexity lints to RuleAnalyzer`

### Phase 5 — Documentation + migration guide

**Objectif :** ne pas laisser les utilisateurs (internes) dans le noir.

- `docs/rules.md` : nouveau contrat, exemples par `kind`, guide de migration depuis l'ancien `df → df`.
- README et `getting_started.md` mis à jour.
- `CHANGELOG.md` finalisé avec section « Breaking changes » détaillée.

**Commit :** `docs(plan-13/phase-5): document new rule contract and migration path`

---

## Rupture API & migration

**Avant :**
```python
@RuleRegistry.register_rule()
def flag_high_value(df):
    return df.withColumn("is_high_value", F.when(F.col("amount") >= 1000, 1).otherwise(0))
```

**Après :**
```python
@RuleRegistry.register_rule(kind="projection")
def flag_high_value(df):
    return {"is_high_value": F.when(F.col("amount") >= 1000, 1).otherwise(0)}
```

Migration script potentielle (`scripts/migrate_rules_to_v13.py`) — best-effort, à évaluer en Phase 5. Comme l'API n'est pas distribuée, l'impact est limité au prototype interne et aux exemples du repo.

Les règles `transform` (échappatoire) acceptent l'ancien `df → df`, donc une règle non migrable peut juste être tagguée `kind="transform"` et continuer à fonctionner — au prix de la fusion perdue, mais sans casse.

---

## Risques & mitigations

| Risque | Impact | Mitigation |
|---|---|---|
| Détection AST incomplète sur règles complexes (closures, classes) | Tri topo manque une dépendance → ordre incorrect | `RuleAnalyzer` signale `source_available=False` ; dans ce cas, fallback sur l'ordre YAML strict pour la règle concernée + warning explicite |
| Fusion produit un plan Catalyst qui se comporte mal sur certains cas | Régression de perf au lieu d'amélioration | Tests de non-régression avec `df.explain()` ; possibilité de désactiver la fusion via flag `engine.run_process_to_table(..., fuse_rules=False)` |
| Option A pour les aggregations contraint l'expressivité | Certaines agg complexes (pivot, multi-niveau) impossibles | Garder `kind="transform"` comme échappatoire |
| Détection `DUPLICATE_EXPR` produit des faux positifs (expressions structurellement égales mais sémantiquement différentes via closures) | Dédup incorrecte | Comparer les `str(expr)` après normalisation ; si doute, ne pas dédupliquer (conservatisme) |
| Tri topo détecte un cycle légitime (rule A overwrites col X, rule B reads new X) | Erreur sur cas valide | Le cas « overwrite intentionnel » est déjà un OVERWRITE warning Plan 06 ; on documente que la dépendance doit être linéaire dans un étage `projection`. Sinon → split en deux étages ou passer en `transform` |

---

## Vérification

```bash
# Tests ciblés
pytest tests/test_rule_planner.py -v
pytest tests/test_rule_executor.py -v
pytest tests/test_registry.py -v
pytest tests/test_rule_analyzer.py -v

# Suite complète
pytest tests/ -x --tb=short

# Lint
ruff check src/
```

**Critères de succès quantitatifs :**

1. **Plan logique** : un pipeline avec 10 règles `projection` produit **1 seul nœud `Project`** dans le plan analysé (vs 10 aujourd'hui). Vérifié via `df.queryExecution.analyzed.toString().count("Project")`.
2. **Régression zéro** : 100 % des tests existants passent après migration.
3. **Lisibilité** : un découpage d'une règle de 30 lignes en 3 mini-règles `projection` produit un plan d'exécution **identique** à la version monolithique (test dédié).
4. **Logs** : tout réordonnancement est tracé dans stdout avec format `[Planner] Reordered ...`.

---

## Évolutions futures (hors scope de ce plan)

- **Cache heuristique** : matérialisation automatique de colonnes lues par N+ règles aval. Reporté faute de signal utilisateur.
- **Backends non-Spark** : porter la fusion vers SQL/DuckDB/BigQuery — nécessite un IR intermédiaire (Column → SQL string).
- **Pushdown filtre** : règles `transform` qui ne font que `df.filter(...)` pourraient être remontées dans le bloc `filter` YAML automatiquement. Risqué — laissé manuel.
- **UI/lineage** : afficher les étages de fusion dans le LineageRenderer existant.

---

## Branche & PR

- Branche cible : `feat/rule-engine-optimization` (à créer depuis `main` après merge des branches actives).
- PR finale : 5 commits (un par phase) + 1 commit plan (celui-ci).
- Validation utilisateur requise avant Phase 1 (cf. workflow CLAUDE.md).