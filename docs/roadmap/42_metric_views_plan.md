# Plan 42 — Couche sémantique publiée en metric views, et builder fiable

> **Statut :** décisions D1 à D5 validées le 15 octobre 2026 (recommandations retenues).
> 42.0 vérifié sur `sts_demo_catalog` (R1 et R2 levés : `version: 1.1` accepté avec `comment` par
> membre, une mesure peut porter le nom de sa colonne source). 42.1 implémenté.
> Branche : `feat/plan42-metric-views`, partie de `main` (`822ff9e`).

## Objectif

Qu'un espace Genie ne puisse répondre **qu'avec les définitions de la couche sémantique Skifer**.

Genie ne lit pas les YAML de `semantic_models/` : il ne connaît que Unity Catalog. Une *metric view*
est une vue Unity Catalog déclarée en YAML, avec des dimensions et des mesures, que Genie sait
interroger. Skifer publie chaque modèle sémantique en metric view ; l'espace Genie n'a que ces vues
pour sources.

```text
semantic_models/*.yaml ─▶ metric_views.py (traduction pure) ─▶ CREATE VIEW … WITH METRICS
                                     │                                 │
                     gate de certification (D3)         <catalog>.semantic.<modèle>
                                                                       │
                                                         espace Genie : seules sources
```

## Constats qui motivent le plan (mesurés le 15 octobre 2026 sur demo_sts)

1. **Aucun module ne crée de metric view.** `SemanticEngine.create_view()` crée une vue SQL classique
   pour *une* requête figée (métriques déjà agrégées sur un `GROUP BY` donné) : Genie ne peut pas la
   ré-agréger selon d'autres dimensions.
2. **Le `SemanticBuilder` produit des modèles cassés, et le validateur les accepte.** Sur deux
   modèles générés pour demo_sts :
   - les quatre métriques de `sales.orders` avaient `sql: SUM(revenue)` avec `type: sum` → le resolver
     produit `SUM(SUM(revenue))`, refusé par Spark à chaque requête. Le prompt (`_BUILDER_SYSTEM`)
     décrit `sql` comme « expression SQL » sans dire que `type` porte l'agrégat ;
     `SemanticValidator._check_metrics` vérifie la présence de `sql`, pas son contenu ;
   - la clé de `sales.customers` valait `sales.customers.customer_id` : le gabarit du prompt impose
     `key: <model_name>.<split_value>`.
3. **La traduction est mécanique.** Un modèle Skifer se traduit sans perte en metric view mono-table :
   `table` → `source`, `base_filter` → `filter`, dimension `sql` → `expr`, métrique → mesure dont
   l'expression est celle de `QueryResolver._build_metric_expr` (agrégat de `type` autour de `sql`,
   `filters` en `CASE WHEN`). Prototype et 9 tests dans demo_sts (`metric_views.py`, commit `ebcdbf5`).

## Décisions à valider

| # | Décision | Recommandation | Alternative écartée |
|---|---|---|---|
| D1 | Où vit la fonctionnalité | Traduction **pure** dans `semantic/metric_views.py` (sans Spark, testable seule) ; publication par `SemanticEngine.publish_metric_views()`, qui réutilise le gate de certification, la résolution de schéma et `execute_sql` de `create_view()`. | Tout dans `SemanticEngine` : la traduction ne serait plus testable sans moteur. |
| D2 | Schéma cible | Clé d'environnement `semantic_metric_views_schema` (défaut `semantic`), avec le suffixe sandbox en mode interactif, comme `semantic_views_schema`. L'espace Genie pointe sur le schéma sans suffixe, alimenté en job/prod. | Ignorer le sandbox : un notebook interactif écraserait les vues que Genie sert. |
| D3 | Certification à la publication | Le gate s'applique **par modèle**, avec la politique de l'environnement. Un modèle refusé (`DENY`, `REQUIRE_HUMAN`) n'est pas publié et **sa vue existante est supprimée** : sinon Genie continuerait de servir une définition dont la source n'est plus certifiée. Le rapport nomme chaque modèle publié, refusé ou supprimé, avec ses raisons. | Laisser l'ancienne vue en place : fail-open, Genie servirait des données décertifiées sans que rien ne le signale. |
| D4 | Périmètre v1 | Modèles **mono-table**. Les `relationships` ne sont pas traduites (les metric views savent joindre, ce sera un plan suivant) ; une métrique `semi_additive` est **refusée** avec sa raison (sa sémantique dépend de dimensions épinglées que Genie ne respecterait pas) ; `calendar` est ignoré. | Traduire les jointures dès maintenant : la sécurité de grain (fanout) du `SemanticPlanner` serait à réimplémenter côté vue. |
| D5 | Version | Nouvelle fonctionnalité → **3.1.0**, décidée par l'utilisateur au moment de la release. | — |

## Points

Chaque point est un commit séparé (`feat(plan42-N): …` ou `fix(plan42-N): …`), avec ses tests et son
entrée `CHANGELOG.md` sous `## [Unreleased]`. Ne pas toucher à la version dans `pyproject.toml`.

### 42.0 — Validation sur le workspace (fait)

Les deux metric views créées sur `sts_demo_catalog.semantic` (`orders`, `customers`) via le
warehouse serverless, en SQL généré par le prototype demo_sts. Vérifié avec `MEASURE()` :

- **R1 levé** — `version: 1.1` avec un `comment` par dimension et par mesure est accepté.
- **R2 levé** — `customers.total_revenue` (`SUM(total_revenue)`) coexiste sans conflit avec la
  colonne source du même nom.
- Chiffres plausibles obtenus par région (`orders`) et par segment (`customers`).

R3 (Databricks Connect) n'a pas été testé séparément : le DDL est passé par la Statement Execution
API du warehouse, pas par Connect. À vérifier au 42.3 si la publication passe par Connect.

### 42.1 — Builder et validateur : `sql` d'une métrique sans agrégat (fait)

- `semantic/validator.py` : `_check_metrics` refuse un `sql` qui commence par une fonction d'agrégat
  (`SUM`, `COUNT`, `AVG`, `MIN`, `MAX`, insensible à la casse et aux espaces), avec un message qui dit
  que `type` porte l'agrégat. Aucun modèle valide n'est cassé : un tel modèle échoue déjà à chaque
  requête. Le builder renvoie l'erreur au LLM dans sa boucle de correction (3 essais).
- `semantic/builder.py` : `_BUILDER_SYSTEM` dit que `sql` est la colonne ou l'expression *non
  agrégée* et que `type` applique l'agrégat (avec un exemple), et que `key` vaut le nom du modèle.
- **Tests :** `tests/test_validator.py` (refus `SUM(x)` / `count( x )`, accepte `x`, `a * b`,
  `CASE WHEN … END`) ; `tests/test_semantic_builder.py` (une réponse LLM fautive puis corrigée passe
  à la 2e tentative) ; vérifier qu'aucun modèle de `examples/` ni des tests n'est refusé.

### 42.2 — Traduction pure : `semantic/metric_views.py`

Port du prototype demo_sts, durci :

- `metric_view_definition(model, catalog) -> dict` et `metric_view_ddl(model, catalog, schema) -> str`.
- Expressions de mesure produites par la **même** fonction que le resolver : extraire de
  `QueryResolver._build_metric_expr` la construction sans alias, et l'appeler des deux côtés. Deux
  implémentations divergeraient, et la promesse « mêmes définitions que Skifer » serait fausse.
- Identifiants validés (`_SAFE_IDENTIFIER`), table en trois parties, `$$` refusé dans le corps.
- Refus explicites (D4) : `semi_additive`, agrégat inconnu. Une erreur nomme le modèle et le membre.
- `comment` de la vue : description du modèle + ligne de provenance (`model=…`, hash de définition).
- **Tests :** `tests/test_metric_views.py`, dont un test d'égalité entre l'expression de mesure et
  celle que le resolver met dans son SQL, pour chaque type d'agrégat et avec `filters`.

### 42.3 — Publication : `SemanticEngine.publish_metric_views()`

- Signature : `publish_metric_views(model_keys=None, *, consumer_context=None, dry_run=False)
  -> MetricViewReport` ; `model_keys=None` publie tout le catalogue (chargement paresseux, modèle par
  modèle).
- Schéma résolu comme D2 ; `CREATE SCHEMA IF NOT EXISTS` puis un DDL par modèle via
  `self.core._get_backend().execute_sql`.
- Gate par modèle (D3) ; un modèle refusé déclenche `DROP VIEW IF EXISTS` sur sa vue.
- `dry_run=True` n'exécute rien et rend les DDL dans le rapport.
- `MetricViewReport` : `published`, `refused` (avec raisons), `dropped`, `ddl` ; sérialisable par
  allowlist, sans valeur de donnée.
- **Tests :** backend factice qui enregistre les SQL exécutés : publication, sandbox, refus + drop en
  `enforce`, `warn` publie et compte, `off` publie sans résoudre de dépendance, `dry_run` n'exécute
  rien.

### 42.4 — CLI : `skifer semantic metric-views`

`skifer semantic metric-views --catalog <c> [--schema semantic] [--models-dir …] [--model KEY]` affiche
les DDL **sans Spark** (traduction seule, pas de gate). Code de sortie `0` si tout se traduit, `1` si
un modèle est refusé par la traduction. La publication reste dans un notebook ou un job (42.3).

### 42.5 — Documentation

- Page `docs/genie_metric_views.md` (nav « Semantic & agentic ») : pourquoi, configuration,
  publication, espace Genie limité au schéma, interaction avec la certification, limites (D4).
- `docs/databricks_supervisor.md` : l'espace Genie pointe sur les metric views plutôt que sur les
  tables taguées ; ce que l'endpoint Skifer apporte encore (refus, preuve).
- `docs/semantic.md` : la règle « `sql` est non agrégé ».
- `CLAUDE.md` / `AGENTS.md` : ligne de structure `metric_views.py`, section courte.
- `mkdocs build --strict`.

### 42.6 — demo_sts bascule sur Skifer (hors dépôt, après release)

Le notebook `04_publish_metric_views` appelle `SemanticEngine.publish_metric_views()` ;
`metric_views.py` est supprimé de demo_sts.

## Risques

| # | Risque | Parade |
|---|---|---|
| R1 | Le format `version: 1.1` n'est pas accepté par le runtime utilisé | 42.0 le vérifie ; repli `version: 0.1` sans `comment` par membre (DBR 16.4+). |
| R2 | Une mesure ne peut pas porter le nom d'une colonne source | 42.0 le vérifie ; sinon la traduction refuse le modèle en nommant le conflit, plutôt que de renommer la mesure (Genie et Skifer nommeraient différemment le même chiffre). |
| R3 | Le DDL `WITH METRICS` ne passe pas par Databricks Connect | 42.0 l'essaie depuis Connect aussi ; sinon `publish_metric_views` documente le cluster ou le warehouse comme prérequis. |
| R4 | Le refus du validateur (42.1) casse un modèle existant ailleurs | Un tel modèle échoue déjà à chaque requête ; on parcourt `examples/` et `tests/` avant de livrer. |

## Vérification

- Gate complet : `pytest tests/ -x --tb=short` avec Java, `ruff check src/`, `mkdocs build --strict`.
- Live (42.0 puis après 42.3) : publication sur `sts_demo_catalog`, `MEASURE()` sur chaque vue, et
  une question à l'espace Genie dont la réponse cite une mesure de la vue.
