# Plan 46 — reprise sur une autre machine

État au 25 novembre 2026. Source de vérité : [`46_multi_platform_plan.md`](46_multi_platform_plan.md)
(chaque sous-plan clos y porte une note « Réalisé »). Ce document ne dit que ce qu'il faut pour
reprendre ; il se supprime une fois 46.7 lancé ou le plan fusionné.

## Où on en est

| Sous-plan | État |
|---|---|
| 46.0 à 46.5 | Clos. Jalon M1 atteint côté cœur ; le générateur côté Atlas n'existe pas encore. |
| 46.6 Snowflake | Développé et relu **sur faux curseur uniquement**. Jamais exécuté contre un compte : son critère « run d'intégration réel » n'est pas satisfait. Matrice : 0 `Supported`, 51 `Conditional`, 6 `Refused`. |
| 46.7 découpe des wheels | **Non commencé. Ne pas le lancer sans décision** (voir plus bas). |

Tout est poussé, aucune PR ouverte, rien de fusionné.

## Récupérer le travail

| Dépôt | Branche à récupérer |
|---|---|
| `skifer` | `feat/plan46-46.6-snowflake-connector` — tête de la pile, contient 46.0 à 46.6 |
| `skifer-duckdb` | `feat/plan46-46.4-connector` |
| `skifer-snowflake` | `feat/plan46-46.6-connector` |

Les branches du cœur sont empilées dans l'ordre `multi-platform` → `46.0-foundations` →
`46.1-import-boundaries` → `46.2-connector-contracts` → `46.3-parity` → `46.4-duckdb-connector` →
`46.5-atlas-contract` → `46.6-snowflake-connector`.

Restés sur l'ancienne machine, non versionnés : le dossier `demo/` (non suivi) et les briefs,
handoffs et reviews de travail (`/tmp/plan46/`), dont l'essentiel est repris dans les notes du plan.

## Remettre l'environnement debout

Cœur (Python 3.10+, JDK 17 pour les 246 tests Spark) :

```bash
brew install openjdk@17
export JAVA_HOME=/opt/homebrew/opt/openjdk@17 PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
python -m venv .venv && .venv/bin/pip install -e ".[dev,spark,analyzer,docs]"
```

Gate complet du cœur (environ 4 minutes, 4486 tests attendus) :

```bash
.venv/bin/pytest tests/ -x --tb=short -q && .venv/bin/ruff check src/ tests/parity examples/26_portable_sql && .venv/bin/lint-imports && .venv/bin/mkdocs build --strict -q
```

Connecteurs : ils déclarent `skifer[dialects]>=4.2.0`, version **non publiée** (le cœur est encore
estampillé 4.1.0). Installer le cœur local, puis le connecteur sans ses dépendances :

```bash
python -m venv .venv
.venv/bin/pip install -e "<chemin du cœur>[dialects,dev]"
.venv/bin/pip install --no-deps -e . && .venv/bin/pip install "duckdb>=1.5.6,<2"   # skifer-duckdb seulement
.venv/bin/pytest -q && .venv/bin/ruff check src/ tests/ scripts/
```

Attendu : `skifer-duckdb` 79 verts ; `skifer-snowflake` 125 verts et 32 sautés. Leur CI GitHub est
rouge tant que le cœur 4.2 n'est pas sur `main` et publié — ce n'est pas une régression.

## Décisions en attente

1. **Lancer 46.7 sans run Snowflake réel ?** Le plan exige 46.6 « vert ». Avec un compte (même
   d'essai) : poser `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, et `SNOWFLAKE_PASSWORD` ou
   `SNOWFLAKE_AUTHENTICATOR` / `SNOWFLAKE_PRIVATE_KEY_FILE` (voir le README de `skifer-snowflake`),
   installer `snowflake-connector-python`, lancer `pytest tests/test_integration.py`. Chaque
   scénario lève une hypothèse nommée du README ; les capacités se promeuvent ensuite dans la
   matrice du cœur, cas de parité par cas de parité.
2. **Deux changements de comportement non prévus par le plan**, à valider : `sql: 1` est refusé
   dans un modèle sémantique (écrire `sql: "1"`) ; le rendu Snowflake cite tous les identifiants à
   casse exacte, donc un YAML à casse incohérente (`AS Total … ORDER BY total`) casse sur Snowflake.
3. **Fusion** : ouvrir les PR dans l'ordre de la pile, ou une seule PR depuis la tête.
4. **Board** : le remplacement de son backend par `DuckDBConnector(connection=…)` n'a pas été testé.

## Défauts pré-existants relevés, non corrigés

- `QueryResolver._assert_safe_scalar` refuse toute valeur de filtre contenant un tiret : un `eq`
  sur une date ISO est refusé.
- Les valeurs de filtre sont rendues en littéraux de chaîne (`HAVING x > '1'`).
- L'enum de jointure du JSON Schema de pipeline ne contient pas `left_semi` / `left_anti`.
- Les modèles sémantiques des exemples 03 et 25 n'ont pas de `name`.
- DuckDB 2.x arrondit `round(2.675::DOUBLE, 2)` à 2.67 (Spark et DuckDB 1.5.6 : 2.68), d'où
  l'épinglage `duckdb<2`.

## Méthode de travail utilisée

Par sous-plan : plan avec l'index de code, développement délégué à Codex (`codex exec … < /dev/null`),
review par `claude -p` sans outil sur le diff en ligne, tri des findings en les vérifiant dans le
code, corrections par Codex, puis note « Réalisé » dans le plan. Un point de plan = un commit, gate
complet avant chaque commit, jamais de bump de version.

## Prochaine action

Trancher la décision 1. Si un compte Snowflake est disponible : exécuter le test d'intégration,
corriger ce qu'il révèle, promouvoir les capacités prouvées, compléter la note 46.6. Sinon, décider
explicitement que 46.7 démarre sur la base actuelle et l'écrire dans le plan.
