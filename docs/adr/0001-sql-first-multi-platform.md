# ADR 0001 — Skifer multi-plateforme : SQL-first, connecteurs minces, pas de dispatch par plateforme

**Date :** 23 novembre 2026 · **Statut :** accepté · **Plan :** [46](../roadmap/46_multi_platform_plan.md)

## Contexte

Skifer a été multi-plateforme une première fois (Plan 01) : un `Backend` Protocol d'une quarantaine de
méthodes, implémenté pour Spark, Snowpark, BigQuery et un `sql_base` générique. Le Plan 26 a tout
supprimé pour recentrer le produit sur Spark/Databricks. La raison n'était pas le manque d'ambition :
chaque plateforme réimplémentait le sens métier des opérations YAML, et cette duplication a fait dériver
**la plateforme principale elle-même** (`between`, `not_between`, `ceil` déclarés dans `op_catalog`,
absents du dispatch Spark réel). Un dispatch par plateforme a autant de vérités que de plateformes.

Depuis, le cœur a acquis des briques sans Spark : `compile_select` (YAML → SQL pur Python),
`QueryResolver(dialect=…)` (transpilation `sqlglot`), `SemanticEngine.from_backend` (runtime sémantique
sur n'importe quel exécuteur SQL, utilisé par skifer-board). L'extension à d'autres plateformes est
redevenue un objectif ferme le 9 novembre 2026.

## Décision

1. **SQL-first.** Un schéma YAML est compilé **une fois** en SQL relationnel commun (`core/sql_compiler`),
   puis **rendu** dans le dialecte de la cible par `sqlglot`. Aucune opération n'est réimplémentée par
   plateforme. Hors Spark, Skifer exécute du SQL compilé uniquement ; les règles Python (`RuleRegistry`)
   restent un atout du runtime Spark et **ne sont pas promises hors Spark**.
2. **Connecteurs minces.** Une plateforme est atteinte par trois contrats étroits — exécution, catalogue,
   publication — jamais par un DataFrame ou un Protocol universel.
3. **Capacité explicite.** Une matrice op × cible porte un statut `supported`, `conditional(motif)` ou
   `refused(motif)`. Ce qui n'y est pas est refusé. `supported` exige une **parité exécutée** : même
   résultat sur Spark DataFrame, Spark SQL compilé et SQL compilé de la cible.
4. **Distribution.** Le dépôt `skifer` garde le langage (`constants`, `op_catalog`, `ir`, `schema_loader`,
   `sql_compiler`, `json_schema`, contrats, matrice, dialecte — futur `skifer-core`) et la couche
   gouvernée. Les runtimes vivent dans des dépôts distincts : `skifer-engine` (Spark/Databricks),
   `skifer-duckdb`, `skifer-snowflake`. La découpe en wheels n'est faite qu'après le second connecteur.
5. **Atlas.** Le générateur sémantique vit dans skifer-atlas ; Skifer possède le format, son JSON Schema
   et le validateur. Une divergence de sens d'un KPI est **arbitrée dans Atlas** ; Skifer valide et
   refuse ce qu'il ne peut pas prouver, il n'offre aucun override du sens.

## Les cinq interdits

1. Aucun dispatch métier par plateforme.
2. Aucun DataFrame ni Protocol universel.
3. Aucune capacité implicite.
4. L'égalité des clés n'est pas l'égalité de sens : une op est portable quand son résultat exécuté l'est.
5. Aucune dégradation silencieuse : un connecteur qui ne sait pas faire lève.

Ils sont gardés mécaniquement : contrats `import-linter` en CI (la couche gouvernée ne peut pas entrer
dans le cœur), drift-guard entre `op_catalog` et la matrice, tests de parité comme oracle de tout
`supported`.

## Alternatives écartées

- **Revenir au Protocol universel du Plan 01.** C'est le défaut documenté qui a conduit au Plan 26.
- **Dispatch DataFrame par plateforme (Snowpark, etc.).** Duplique le sens ; refusé pour la même raison.
- **Monorepo pour les runtimes.** Proposé par Claude pour la vitesse ; Julien a choisi des dépôts
  distincts pour que chaque runtime ait sa propre cadence, sa propre CI et ses propres dépendances
  lourdes.
- **Découper les wheels dès maintenant.** Double la maintenance sans second connecteur pour prouver que
  les contrats ne sont pas « Spark-shaped ».

## Conséquences

- `import skifer.core.sql_compiler` doit devenir importable sans charger la couche gouvernée (Plan 46.1).
- `compile_select` gagne un paramètre `dialect` dont la valeur `databricks` est un no-op octet pour octet.
- Toute nouvelle op de `op_catalog` doit déclarer sa ligne de matrice et son test de parité, ou être
  `refused` hors Spark.
- BigQuery, Fabric, règles Python portables et orchestration multi-cible sont hors périmètre.
