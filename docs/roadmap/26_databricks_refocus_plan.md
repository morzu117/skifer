# Plan 26 — Recentrage Databricks : aplatissement Spark-only

> Branche de travail : `refactor/26-databricks-refocus`
> Statut : Validé — en cours d'implémentation (29 juillet 2026)

## Contexte

Le produit est stabilisé sur Databricks (prod) + PySpark/Delta local (dev). Les backends legacy (`snowpark.py` 516 L, `bigquery.py` 336 L, `sql_base.py` 942 L) et l'abstraction `Backend` Protocol (312 L, ~40 méthodes) ne sont plus le focus produit. Décision utilisateur : **aplatissement complet** — suppression des 3 backends legacy **et** du Protocol ; `SkiferEngine` devient nativement Spark. `pyspark`/`delta-spark` restent un extra optionnel `[spark]` (le runtime Databricks fournit pyspark).

Ce refacto est aussi le prérequis des évolutions suivantes demandées (streaming tables, SCD1/2, métadonnées déclaratives) : elles se grefferont sur un chemin d'écriture unique Spark/Delta.

**Découverte critique pendant l'exploration** : `between`/`not_between` (Plans 23/24) et `ceil` sont déclarés dans `op_catalog.py` mais **absents de `_SPARK_FILTER_DISPATCH`/`_SPARK_OP_DISPATCH`** ([spark.py:41-76](src/skifer/backends/spark.py)). Leurs seules implémentations sont dans `core/operations.py` (mort — aucun import src) et `sql_base.py` (à supprimer). Sur le chemin Spark réel, un filtre `between:` lève « Unknown filter operator ». Le refacto doit porter ces opérateurs, sinon il enterre des features documentées.

**Constats clés validés** (2 agents d'exploration + contre-vérification) :
- Aucun module src hors `backends/` n'importe snowpark/bigquery/sql_base. `backends/__init__.py` est vide. Seule arête entrante : `core/core.py:166` → `SparkBackend`.
- Aucun test n'appelle `SkiferEngine(backend=...)` — les 78 tests FakeBackend injectent via `engine._backend = fake` (duck typing pur). Supprimer le kwarg `backend=` est quasi gratuit.
- Seul branchement runtime sur noms de plateformes : `agentic/orchestrator.py` (`_detect_backend_type`, branches Snowflake/BigQuery des DAG Airflow).
- Seul consommateur runtime de `capabilities` : la garde `temp_view` dans `interpreter.py:265-270` (partials).
- `interpreter.py` appelle encore le chemin string `apply_operation` sur 8 sites (:83-132) ; les filtres passent déjà par l'IR (`build_filter`).

## Décisions de conception

| Question | Décision |
|---|---|
| `VALID_SOURCE_TYPES` | Nouveau module `core/constants.py` (évite import circulaire) ; `json_schema.py:245` dérive son enum via `sorted(VALID_SOURCE_TYPES)` |
| `SparkBackend` | `git mv` → `core/spark_backend.py`, suppression du package `backends/`, export dans `skifer/__init__.py` |
| Kwarg `backend=` du moteur | **Supprimé** — signature : `__init__(spark=None, config_path=None, force_env=None, monitor=None)` |
| `_get_backend()` | Conservé (simple accesseur interne, appelants vivants : core.py, patterns.py, agent.py) |
| `capabilities` | **Mécanisme supprimé** (propriété SparkBackend + garde interpreter + param FakeBackend) — Spark supporte toujours temp_view |
| `apply_operation`/`build_filter_expression` (dépréciés) | Supprimés après migration des 8 sites interpreter vers `apply_op(_parse_op(...))` |
| `core/operations.py` | Supprimé après portage de `ceil`/`between`/`not_between` dans les dispatch Spark |
| FakeBackend (tests) | **Conservé, aminci** — double duck-typé de SparkBackend (aucune abstraction src) + un test garde-dérive par réflexion (chaque méthode publique de FakeBackend doit exister sur SparkBackend) |
| Orchestrator | 2 types : `"spark"`/`"local"` via `is_local` ; sortie = Databricks DAB, Airflow-Databricks générique, ou script Python |

## Phases (branche `refactor/26-databricks-refocus` — 1 phase = 1 commit ; gate : `pytest tests/ -x --tb=short`)

### Phase 0 — Document de plan + ménage roadmap (docs only)
- Créer `docs/roadmap/26_databricks_refocus_plan.md` (ce plan).
- `docs/roadmap/01_multi_plateforme.md:3-4` et `01_multi_plateforme_plan.md:3-5` : statut → « Abandonné — remplacé par le Plan 26 ».
- `CLAUDE.md` table roadmap : ligne Plan 01 → Abandonné ; ajouter ligne Plan 26.
- Ne pas toucher les plans historiques mergés 02-25.

### Phase 1 — Suppression des backends legacy + extras
- **Supprimer** : `src/skifer/backends/{sql_base,snowpark,bigquery}.py` ; `tests/test_backend_{sql_base,snowpark,bigquery}.py` (−217 tests).
- `pyproject.toml` : supprimer L53-60 (extras `snowflake`/`bigquery` + commentaire « Legacy connectors »). Garder `[spark]`.
- Rafraîchir `uv.lock`. ⚠️ `uv.lock` et `.python-version` sont actuellement **non trackés** — à signaler dans la PR (décision utilisateur), ne pas commiter silencieusement.
- `CHANGELOG.md` [Unreleased] : Removed — backends Snowflake/BigQuery/SQL-base + extras (**BREAKING**).

### Phase 2 — Aplatissement du Protocol (commit cœur, atomique)
- **Créer** `core/constants.py` (`VALID_SOURCE_TYPES` déplacé de backend.py:12-14).
- **Supprimer** `core/backend.py` et `tests/test_backend_protocol.py`.
- Importeurs → `core.constants` : `core/schema_loader.py:10`, `backends/spark.py:18` ; `core/json_schema.py:245` → `sorted(VALID_SOURCE_TYPES)`.
- `core/core.py` : retirer `backend=` + branche d'injection (:146-150) ; supprimer shims dépréciés `_patch_connect_*` (:268-283) ; corriger :431 (`_spark` privé → propriété publique `.spark`) ; docstring exemple :508.
- `core/interpreter.py` : supprimer la garde capabilities (:265-270) — `register_temp_view` inconditionnel.
- `backends/spark.py` : supprimer la propriété `capabilities` (:151-153) ; docstrings sans « Backend Protocol ».
- Imports TYPE_CHECKING → `SparkBackend` : `core/catalog_inspector.py:13`, `agentic/builder_agent.py:25`, `agentic/orchestrator.py:26`.
- Tests : FakeBackend aminci (sans `capabilities`) ; suppression des tests protocol/capabilities dans `test_engine_fake_backend.py`, `test_engine_with_backend.py`, `test_backend_spark.py`, `test_partials.py:449-461` ; **ajout du test garde-dérive** (réflexion FakeBackend ↔ SparkBackend) ; imports `VALID_SOURCE_TYPES` corrigés.
- `CHANGELOG.md` : Removed — Protocol, kwarg `backend=`, `capabilities` (**BREAKING**).

### Phase 3 — Suppression du chemin string legacy + fix ceil/between
- `core/interpreter.py` :83-132 : `b.apply_operation(c, s, ...)` → `b.apply_op(c, _parse_op(s), ...)` (8 sites).
- `backends/spark.py` : supprimer `apply_operation`/`build_filter_expression` (:448-462) ; **ajouter** `ceil` à `_SPARK_OP_DISPATCH`, `between`/`not_between` à `_SPARK_FILTER_DISPATCH` (sémantique + erreurs d'arité portées d'`operations.py:136,200-227`).
- **Supprimer** `core/operations.py` ; retirer les 2 wrappers de FakeBackend.
- Tests : porter ~40 tests d'`operations.py` (dans `test_core.py`) vers `SparkBackend.apply_op`/`build_filter` dans `test_backend_spark.py` — obligatoires : ceil, between/not_between (nominal + bornes + arité), apostrophes/chaînes vides/chaînes numériques, gouvernance `allow_raw_sql`. Adapter `test_interpreter.py:64,92` (mock `apply_op`) et `test_engine_fake_backend.py:37`.
- `CHANGELOG.md` : Fixed — **`ceil`, `between`, `not_between` fonctionnent désormais sur le chemin d'exécution Spark** ; Removed — module string-ops.

### Phase 4 — Relocalisation SparkBackend, suppression de `backends/`
- `git mv backends/spark.py core/spark_backend.py` ; supprimer le package `backends/`.
- Corriger importeurs : `core/core.py:166` + 6 fichiers tests + hints TYPE_CHECKING.
- `skifer/__init__.py` : exporter `SparkBackend` (chemin public canonique).
- `CHANGELOG.md` : Changed — `skifer.backends.spark` → `skifer.core.spark_backend` (**BREAKING**).

### Phase 5 — Simplification orchestrator + observability
- `agentic/orchestrator.py` : `_detect_backend_type` → `"local" if backend.is_local else "spark"` ; supprimer branches Snowflake/BigQuery de `_render_airflow_dag` (:76-97) et `_resolve_format` (:275-277) ; table docstring :9-15.
- `observability/monitor.py:57`, `checks.py:4-5` : contrat documenté = SparkBackend (`.sql()`).
- Tests : `test_orchestrator.py` — supprimer 5 tests snowflake/bigquery, adapter 3 tests de détection. `test_observability.py` (79) et `test_quality_agent.py` (31) : **zéro changement** (fakes `.sql()` autonomes).

### Phase 6 — Passe documentation (commit final)
- `mkdocs.yml:2` : site_description → Databricks/Spark uniquement.
- `docs/core.md`, `api_reference.md`, `observability.md`, `index.md:170`, `agentic.md`, `lineage.md:235` : retirer le récit multi-backend, signatures à jour.
- `CLAUDE.md` (:5, :27-31 carte repo, :71), `AGENTS.md` (:13, :82, :134), `.gstack/coding-standards.md:17`, `README.md:306`.
- `docs/briefing_benchmark.txt` : note d'en-tête datée (« historique — produit Databricks-only depuis Plan 26 »).
- `mkdocs build` si dispo.

## Bilan tests

| Fichier | Sort | Coût |
|---|---|---|
| test_backend_sql_base/snowpark/bigquery/protocol (220 tests) | Supprimés | 0 |
| test_core.py (~40 tests operations) | Portés vers test_backend_spark.py (IR) | le gros morceau (~½ j) |
| test_orchestrator.py (24) | −5, 3 adaptés | ~1 h |
| test_engine_fake_backend.py (25) | −2, +1 garde-dérive | ~1 h |
| test_partials.py (23) / test_engine_with_backend.py (7) | −1 chacun | minutes |
| test_sink_jdbc (15), test_observability (79), test_quality_agent (31) | Intacts | 0 |
| test_loaders (8), test_config, test_core_join, test_core_connect_patch | Chemins d'import (Phase 4) | minutes |

Pas de réécriture massive des 78 tests FakeBackend : l'injection par attribut (`engine._backend = fake`) survit en duck typing pur.

## Risques

- **API publique** : `__all__` de `skifer/__init__.py` inchangé (Phase 4 ajoute seulement `SparkBackend`). L'usage documenté `SkiferEngine(spark=spark)` / `SkiferEngine()` est intact.
- **BREAKING à flaguer au CHANGELOG** (version majeure — gérée par l'utilisateur, ne jamais bumper) : kwarg `backend=`, imports `skifer.backends.*` et `core.backend`, `capabilities`, extras snowflake/bigquery, méthodes dépréciées, shims `_patch_connect_*`.
- **Deltas de comportement** : partials temp_view non gardés (sûr, Spark le supporte toujours) ; chemin select via `apply_op(_parse_op(...))` au lieu des strings (même code une frame plus bas, couvert par les ports de tests).
- **Séquencement** : la Phase 2 est la seule non-divisible (le Protocol ne peut pas à moitié exister) — un seul commit. Les autres phases sont indépendamment vertes.

## Vérification

- Par phase : `pytest tests/ -x --tb=short`.
- Après Phases 1/4/5 : `grep -rn "snowpark\|bigquery\|sql_base\|core.backend\|capabilities" src/` → zéro hit (docs historiques exemptés).
- Final : `python -c "import skifer; skifer.SparkBackend"` ; `pip install -e ".[spark]"` frais ; `mkdocs build` ; smoke end-to-end local (session Delta) sur un pipeline d'`example/` exerçant un filtre `between:` pour prouver le fix.
