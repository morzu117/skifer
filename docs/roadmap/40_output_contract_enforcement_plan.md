# Plan 40 — Contrat de sortie vérifié en environnement strict

**Branche :** `feat/plan40-output-contract`
**Statut :** en cours

## Contexte

La décision D14 (Plan 39) a fermé le SQL écrit à la main là où le framework le
déclare : `expr:`, le filtre `sql`, les règles et loaders `kind="sql"` sont
refusés sous `allow_raw_sql: false`. Il reste une porte : une règle
`kind="projection"` est du Python arbitraire et peut appeler `F.expr("…")`.

Deux remèdes ont été pesés. Interdire `F.expr` par analyse statique dans
`RuleAnalyzer` agit sur **l'entrée** : c'est un lint, contournable par
`getattr`, par un helper dans un autre module, par une chaîne construite à
l'exécution. Vérifier le contrat agit sur **la sortie** — le schéma et les
valeurs réellement produits — et aucune ruse Python n'y échappe.

Ce plan livre le second.

### Le défaut mesuré

`contract.output` déclare `logical_type`, `required`, `unique` et
`classification` par champ. Ces déclarations alimentent l'identité de
certification, l'export ODCS, le diff de contrat, la projection sémantique,
les drafts et l'index de métadonnées.

**Aucune ne produit de vérification contre la donnée produite.**
`ContractExtractor.extract()` ne lit rien de `contract.output` : il ne dérive
ses contrôles que de `tables[].quality_checks`, `tables[].filter`, des casts de
`select_final`, du bloc `observability:` et de `contract.sla`. Un contrat qui
déclare `order_id: {required: true, unique: true}` n'est vérifié nulle part.

Le loader vérifie bien que tout nom déclaré est produit
(`schema_loader.py:741`) — mais dans ce sens seulement. **Une colonne produite
et non déclarée passe**, et `_explicit_output_names` renvoie `None` sous
`keep_all_columns`, ce qui saute le contrôle entièrement. C'est le trou par
lequel passe une règle Python qui invente une colonne.

Pourquoi ce remède plutôt que la projection statique : `OutputProjector` marque
tous les champs `inference_status="unknown"` dès qu'un `business_rules` est
présent. Elle est **aveugle** au cas précis qui inquiète. Seule la vérification
à l'exécution voit ce qu'une règle Python a réellement produit.

## Décisions

| Question | Décision |
|---|---|
| Exhaustivité | Le contrat définit **toutes** les colonnes de sortie ; une colonne produite non déclarée est une violation |
| Sanction | **Refus d'écrire** — la table cible n'est jamais touchée |
| Types non vérifiables (`identifier`) | **Ensemble fermé, refus au chargement** ; rien n'est sauté en silence |
| Comment refuser d'écrire | L'environnement strict **exige `data_product:`**, donc le staging existant |
| Contrat d'entrée | **Hors périmètre** — plan séparé (voir « Suite ») |

## Le piège à éviter : le contrôle vide

`docs/yaml_spec.md:38` documente `logical_type: identifier`. Ce n'est pas un
type SQL — un identifiant peut être `string` ou `bigint`. Une correspondance
naïve logique→physique produirait, pour tout type non mappable, soit une
comparaison fausse, soit un `skip` silencieux : un contrôle qui ne peut pas
échouer. C'est exactement le défaut du garde-fou ODCS corrigé en Plan 39.

Règle tenue dans tout ce plan : **ce qui ne peut pas être vérifié est refusé,
jamais sauté.**

## Un défaut préexistant, corrigé d'abord

`TypeCheck` et `SchemaDriftCheck` lisent les clés `col_name` / `data_type` d'un
`DESCRIBE` (`checks.py:206`, `checks.py:532`). **DuckDB renvoie `column_name` /
`column_type`** — ces deux contrôles lèvent donc `KeyError` sur l'adaptateur
DuckDB. Aucun test ne l'attrape : tous les tests de `DESCRIBE` utilisent un faux
backend qui fabrique les clés Spark.

Sans ce correctif, le mode strict serait cassé sur toute la voie SQL-first.

## Phases

Un point = un commit.

### 40.1 — Primitive `column_types` et correction DuckDB

Sortir le `DESCRIBE` brut des contrôles. `SparkBackend.list_column_types`
existe déjà (`spark_backend.py:1040`) ; `DuckDBAdapter` a son introspection
native (`duckdb.py:382`, via `SELECT * FROM … LIMIT 0` et `cursor.description`).

Exposer `column_types(fqn) -> dict[str, str]` à la frontière backend et faire
lire `TypeCheck` / `SchemaDriftCheck` par cette primitive. Un seul SQL
spécifique par moteur, à un seul endroit.

*Fichiers :* `core/backend.py`, `core/spark_backend.py`,
`core/adapters/duckdb.py`, `observability/checks.py`.

### 40.2 — Ensemble fermé de types logiques + `LogicalTypeCheck`

Nouveau module `core/logical_types.py` : correspondance logique → types
physiques acceptés, **par dialecte** (DuckDB renvoie en majuscules, Spark en
minuscules ; comparaison insensible à la casse).

```
string · integer · long · double · decimal · boolean · date · timestamp
```

Nouvelle classe `LogicalTypeCheck` dans `checks.py` — et non une branche de
plus dans `TypeCheck`, qui sert déjà les casts de `select_final` et n'a pas à
porter deux sémantiques. Elle résout les types acceptés depuis `backend.name`
au moment d'`evaluate()`.

*Fichiers :* `core/logical_types.py` (nouveau), `observability/checks.py`.

### 40.3 — `ContractExtractor` dérive de `contract.output`

| Déclaration | Contrôle dérivé |
|---|---|
| `required: true` | `NullCheck` |
| `unique: true` | `UniqueCheck` |
| `logical_type: X` | `LogicalTypeCheck` |
| l'ensemble des noms déclarés | `SchemaDriftCheck(expected_columns=…)` |

`SchemaDriftCheck` rapporte déjà `added` et `removed` : c'est exactement
l'exhaustivité demandée, sans nouvelle classe. Une colonne inventée par une
règle apparaît en `added` et fait échouer le contrôle.

`extract()` prend un argument `contract_enforcement` **sans valeur par
défaut** : c'est la leçon de D14, où un défaut aurait laissé un quatrième
chemin arriver silencieusement permissif.

*Fichiers :* `observability/contracts.py`, `observability/monitor.py`,
`observability/publication.py`, `core/patterns.py`, `agentic/quality_agent.py`.

### 40.4 — Drapeau d'environnement `contract_enforcement`

`off` (défaut) · `warn` · `strict`, sur le modèle de
`semantic_certification_policy` et de `classification_propagation`.

- `off` — comportement actuel à l'identique ; rien ne change pour l'existant.
- `warn` — les contrôles tournent en `warning` : on mesure avant d'imposer.
- `strict` — les contrôles sont `critical`, et les refus de 40.5 s'appliquent.

*Fichiers :* `core/config.py` (validation au chargement, à côté de
`classification_propagation`), `core/context.py` (accesseur), `config.yaml`
(commentaire documentaire).

### 40.5 — Refus en mode strict

Le loader est **aveugle à l'environnement** : `parse_schema` / `load_schema` /
`_normalize_agent_ready_metadata` ne reçoivent ni `ExecutionContext`, ni
config, ni nom d'environnement. Les refus vont donc à la couture déjà
environment-aware de `patterns.py:232`, où vit le bloc
`classification_propagation == "strict"`. Même forme, même endroit.

Sous `strict`, refus avant toute exécution :

1. pas de `data_product:` → refus (sans lui, pas de staging, donc pas de refus
   d'écrire possible) ;
2. pas de `contract:` → refus (le loader exige déjà `output` dès qu'un
   `contract:` existe) ;
3. un `logical_type` hors de l'ensemble fermé → refus ;
4. `monitor=` ou `certification_store=` absent → **déjà refusé**
   (`patterns.py:227`), acquis gratuitement.

`keep_all_columns` n'est pas refusé : le contrôle d'exhaustivité est fait à
l'exécution sur les colonnes réellement produites, là où la projection statique
était aveugle.

*Fichiers :* `core/patterns.py`.

### 40.6 — Exemples et documentation

Deux exemples deviennent rouges, et c'est la preuve que le contrôle n'est pas
vide :

- `examples/02_quality_and_contract/gold_orders.yaml` déclare
  `amount_eur: {logical_type: decimal}` mais applique `cast:double`. Le contrat
  et le pipeline se contredisent depuis toujours ; personne ne pouvait le voir.
- `examples/23_openlineage` utilise `logical_type: identifier`, hors de
  l'ensemble fermé.

*Fichiers :* les deux exemples, `docs/yaml_spec.md` (l'exemple `identifier`),
`docs/observability.md`, `CLAUDE.md` (section Gouvernance), `AGENTS.md`.

## Risques

| Risque | Traitement |
|---|---|
| Rupture de l'existant | `off` par défaut : aucun pipeline ne change de comportement sans opt-in explicite. `warn` fournit l'étape de mesure avant `strict`. |
| Contrôle de type vide | Ensemble fermé + refus au chargement ; garde-fou de dérive qui échoue si un type est ajouté sans entrée par dialecte. |
| Divergence Spark / DuckDB | La primitive `column_types` est testée **sur moteur réel** des deux côtés ; un faux backend ne peut pas attraper la divergence `col_name`/`column_name`. |
| Contrôles déclarés mais jamais exécutés | En `strict`, l'absence de `monitor` / `certification_store` est un refus au démarrage, pas un saut silencieux. |

## Vérification

Chaque test est muté avant d'être accepté : inverser l'assertion ou casser le
code doit le faire échouer. Un test vert des deux côtés ne prouve rien.

**Le test qui porte l'objectif** : un pipeline dont une règle `kind="projection"`
ajoute une colonne absente de `contract.output` doit être **quarantainé**, la
table cible inchangée. C'est la démonstration que la porte `F.expr` est fermée
par la sortie.

Les autres :

- dérivation des quatre contrôles depuis `contract.output` ; sévérité
  `critical` sous `strict`, `warning` sous `warn`, **aucun contrôle** sous `off` ;
- `LogicalTypeCheck` sur les deux dialectes, types DuckDB en majuscules inclus ;
- les quatre refus du mode strict, un test chacun ;
- **sur moteur réel** : `column_types` et `LogicalTypeCheck` contre une vraie
  base DuckDB, sur le modèle de
  `test_null_and_unique_checks_execute_on_real_duckdb` ;
- **garde-fou de dérive** : un test qui échoue si un type est ajouté à
  l'ensemble fermé sans entrée pour chaque dialecte, et qui refuse de passer
  à vide.

Portes complètes :

```bash
pytest tests/ -x --tb=short && ruff check src/
mkdocs build --strict
```

## Suite

Ce plan contraint la sortie. Une règle Python peut toujours **lire** une
colonne source qu'elle ne devrait pas. C'est l'objet du plan suivant — le
contrat d'entrée — qui se garantit par construction plutôt que par
vérification : ne remettre aux règles que les colonnes déclarées, de sorte
qu'une colonne non autorisée n'existe pas dans le DataFrame qu'elles reçoivent.
