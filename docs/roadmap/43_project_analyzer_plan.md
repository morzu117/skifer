# Plan 43 — Project analyzer : du SQL existant aux pipelines Skifer

> **Statut :** plan rédigé le 26 octobre 2026, décisions D1 à D8 validées. Implémentation en cours.
> Branche : `feat/plan43-project-analyzer`.

## Objectif

Faciliter l'adoption de Skifer sur un projet qui a déjà des traitements SQL : scripts `.sql`,
notebooks Databricks, cellules `spark.sql(...)`. L'analyzer lit ce corpus et produit, **sans
appel LLM**, des premières versions de pipelines YAML, de règles partagées et de découpage en
partials ou tables intermédiaires, avec un rapport qui dit exactement ce qu'il a su convertir
et ce qu'il a laissé à l'humain.

Le principe directeur est **déterministe d'abord**. Tout ce qui est dans le SQL (tables,
jointures, filtres, projections, agrégats) se convertit par analyse syntaxique, et la
conversion est **prouvée** par round-trip : le YAML produit est recompilé en SQL par le
compilateur existant et comparé à la source. Le LLM n'intervient qu'en option, après, pour
nommer et décrire ; sa sortie est traitée comme non fiable, comme déjà dans
`SemanticBuilder.build_from_projection()`.

```text
.sql / notebooks / query history ──▶ front-ends ──▶ sqlglot AST (dialecte databricks)
                                                            │
                                 ┌──────────────────────────┤
                                 ▼                          ▼
                     mapper : statement → schéma    DAG de statements → découpage
                     (catalogue d'ops fermé)        (partials, tables intermédiaires, couches)
                                 │                          │
                                 ▼                          ▼
                     oracle round-trip            fingerprints inter-scripts
                     compile_select ⇄ source      → règles partagées, divergences
                                 │                          │
                                 └──────────┬───────────────┘
                                            ▼
                     emitter : drafts YAML gérés + stubs de règles + rapport (couverture)
                                            │ (option) run de parité sur sandbox
                                            │ (option) enrichissement LLM : noms, descriptions
                                            ▼
                     schemas/.drafts/<cible>.yaml   rules/_generated/<nom>.py   rapport .md/.json
```

## Constats sur l'existant (26 octobre 2026)

1. **`core/sql_compiler.py` est l'inverse de ce qu'on construit.** `compile_select()` sait
   produire un `WITH … SELECT` à partir d'un `ParsedSchema`. Le mapper fait le chemin retour, et
   la paire donne un oracle de vérification sans LLM : parser le SQL legacy, émettre le YAML,
   recompiler, comparer les deux arbres normalisés.
2. **`core/op_catalog.py` est le vocabulaire cible, fermé.** 19 opérateurs de filtre, 19 ops de
   colonne (`cast`, `upper`, `trim`, `round`, `coalesce`, `nvl`, `substring`, `split`, `to_date`,
   `when`/`then`/`else`, `lit`, `expr`…), 12 fonctions d'agrégat. Le travail du mapper se résume
   à « nœud AST → entrée du catalogue, sinon résidu ».
3. **Le pattern de sortie existe déjà.** `SemanticDraftBuilder` (drafts gérés, marqueur
   `_generated_by`, hash de source, écriture atomique, refus d'écraser un fichier humain) et
   `ProposalGenerator` (staging, validation par `load_schema` avant publication). L'analyzer
   suit ces deux patterns à l'identique.
4. **Le fingerprint versionné existe.** `adaptive/models.py` produit des `sha256:v1:…`
   insensibles à l'ordre non sémantique ; `adaptive/recommender.py` montre des seuils nommés et
   explicables. La détection de traitements récurrents réutilise cette discipline.
5. **`RuleService.generate_snippet()`** émet déjà du code de règle depuis une spec typée
   (`constant`, `cast`, `when_otherwise`, `with_column`). Il sert d'émetteur de base pour les
   stubs.
6. **`NotebookExtractor`** détecte le SQL par mot-clé et renvoie du texte pour un LLM. Il est
   remplacé par un vrai front-end ; il reste en place pour `SemanticBuilder`.
7. **Aucun parseur SQL** en dépendance directe (`sqlparse` n'est que transitif dans `uv.lock`).
   `sqlglot` a un dialecte `databricks`, parse `QUALIFY`, `GROUP BY ALL`, les chemins `:` JSON,
   et fournit normalisation, `qualify` des colonnes et lineage de colonnes.
8. **La grammaire n'a pas de filtre post-jointure.** Les filtres sont par table et appliqués
   **avant** le join (`interpreter.process_schema`, étape 1). Or `LEFT JOIN b … WHERE b.x = 'A'`
   n'est pas équivalent à filtrer `b` puis joindre : le premier élimine les lignes de gauche sans
   correspondance, le second les garde avec des nulls. C'est un motif très fréquent en SQL
   legacy ; sans bloc dédié, l'analyzer devrait générer une règle `transform` pour chaque cas.
9. **`run_union_sources_to_table`** existe : un `UNION` se mappe sur un pattern déjà livré.

## Décisions

| # | Décision | Choix | Alternative écartée |
|---|---|---|---|
| D1 | Parseur SQL | `sqlglot` en extra optionnel `[analyzer]`, import strictement paresseux dans `skifer/analyzer/`. Le cœur s'importe sans lui (test par inspection de `sys.modules`, comme pour `tracing`). **Validé.** | `sqlparse` : tokenizer sans AST, pas de dialecte, pas de lineage. Parseur maison : hors de portée. |
| D2 | Filtre post-jointure | Ajout du bloc `where:` à la grammaire, appliqué **après les joins et avant `business_rules`**. Compilé en `WHERE` sur l'arbre de jointure dans `compile_select`. **Validé.** | Règles `transform` générées par l'analyzer : du Python pour exprimer un `WHERE`, illisible et hors du périmètre statique (lineage, MV). |
| D3 | Corpus de calibration | Corpus **synthétique** sous `examples/24_project_analyzer/legacy_sql/`, écrit pour couvrir chaque construction visée et chaque piège sémantique. Il sert d'exemple exécuté par la suite **et** de fixture de tests. Les seuils de détection sont fixés dessus, puis réévalués sur un corpus réel dès qu'il en existe un. **Validé.** | Attendre un corpus client : bloque tout le plan. |
| D4 | Syntaxe de `where:` | Miroir exact du niveau table : `where:` (liste AND, grammaire `col:op[:val]` et forme dict) et `where_groups:` (OR-de-ANDs). Colonnes **non qualifiées**, résolues sur le DataFrame joint. **Validé.** | Un seul bloc avec listes imbriquées : une troisième grammaire de filtre à documenter. |
| D5 | Emplacement des sorties | `schemas/.drafts/<cible>.yaml` et `rules/_generated/<nom>.py`, chacun portant `_generated_by: project_analyzer` et le hash de la source. Même règle que `SemanticDraftBuilder` : jamais d'écrasement d'un fichier sans marqueur. Le rapport va dans `analysis/<horodatage>/report.{md,json}`, ou sur stdout en `--dry-run`. **Validé.** | Écrire dans `schemas/` directement : un draft doit être promu par un humain, jamais exécuté par surprise. |
| D6 | Push-down des prédicats | Un prédicat du `WHERE` descend en filtre de table **seulement s'il est prouvablement équivalent** : il référence une seule table et cette table n'est pas du côté nullable d'un outer join dans l'arbre (côté droit d'un `left`, gauche d'un `right`, les deux d'un `full`). Sinon il reste dans `where:`. **Validé.** | Tout dans `where:` : correct mais produit un YAML non idiomatique ; tout en push-down : faux. |
| D7 | Résidus | Par défaut une expression non mappable devient `expr:<sql>` (ou l'opérateur de filtre `sql`) : le pipeline **tourne dès le premier jour**, et chaque résidu est listé dans le rapport avec son fragment. `--strict` refuse d'émettre dès qu'il y a un résidu. Les constructions sans équivalent (fenêtre hors dédup, explode, pivot, UDF) sortent en `needs_curation:` dans les métadonnées du draft, fragment conservé, statement non émis en YAML. **Validé.** | Tout en `needs_curation` : rien ne tourne, l'adoption n'avance pas. |
| D8 | Version | Nouvelle fonctionnalité → montée de version **3.2.0**, déclenchée par l'utilisateur au moment de la release. **Validé.** | — |

## Périmètre v1 et exclusions

**Convertit** : `SELECT` mono-statement et multi-CTE, `FROM`/`JOIN` (inner, left, right, full,
cross, anti et semi via `NOT EXISTS`/`EXISTS` simples), `WHERE` en conjonction et en
OR-de-ANDs borné, projections et ops du catalogue, `CASE WHEN`, littéraux, `GROUP BY`/`HAVING`
avec les agrégats du catalogue, `DISTINCT` sur toutes les colonnes, dédup par
`ROW_NUMBER() … = 1` (→ `preprocess.qualify`), `UNION [ALL]` entre sources homogènes,
`CREATE [OR REPLACE] TABLE … AS`, `INSERT OVERWRITE`, `CREATE [TEMP] VIEW`, `MERGE INTO` simple
(→ `streaming_table` + `write_mode: upsert`, proposé mais marqué à relire).

**Résidus assumés** (`needs_curation`) : fonctions fenêtre autres que la dédup, `LATERAL VIEW` /
`explode`, `PIVOT`, CTE récursives, UDF, sous-requêtes corrélées dans le `SELECT`, `MERGE` avec
plusieurs `WHEN`.

**Hors v1** : l'API DataFrame PySpark (`df.filter(...).join(...)`) comme front-end, les
notebooks Scala, dbt (le Jinja empêche le parse direct ; `ref()` → table est une v2 raisonnable).

## Points

Chaque point est un commit séparé (`feat(plan43-N): …`), avec ses tests et son entrée
`CHANGELOG.md` sous `## [Unreleased]`. Ne pas toucher à la version dans `pyproject.toml`.
Après chaque point : `pytest tests/ -x --tb=short && ruff check src/`.

### 43.0 — Corpus synthétique, extra `[analyzer]`, inventaire des constructions

**But** : disposer du jeu de calibration et du squelette de parsing avant toute conversion.

- `pyproject.toml` : extra `analyzer = ["sqlglot>=25"]`. Aucune autre dépendance.
- `examples/24_project_analyzer/legacy_sql/` : un flux retail complet en 8 à 10 scripts
  (`01_stg_orders.sql` … `08_kpi_monthly.sql`) qui couvre **à dessein** :
  - une expression récurrente identique dans ≥ 3 scripts (`UPPER(TRIM(country))`,
    `COALESCE(discount, 0)`) ;
  - une expression récurrente **avec variante de littéral** (`amount * 1.2` vs `amount * 1.21`) ;
  - une **divergence métier** : `net_margin` dérivée de deux façons différentes ;
  - une CTE réutilisée dans deux branches d'un même script (→ partial) ;
  - une sous-requête identique dans deux scripts (→ table intermédiaire) ;
  - un `LEFT JOIN` suivi d'un `WHERE` sur une colonne du côté droit (→ `where:`, pas de push-down) ;
  - un `WHERE` sur la table de base (→ push-down en `filter`) ;
  - un `ROW_NUMBER() OVER (...) = 1` (→ `qualify`), une fenêtre `LAG` (→ résidu) ;
  - un `UNION ALL`, un `CASE WHEN` à trois branches, un `GROUP BY` + `HAVING` ;
  - des littéraux avec apostrophe (`'O\'Brien'` — voir note sur `sqlglot` ci-dessous) et un
    `NOT IN` sur colonne nullable.
- `src/skifer/analyzer/__init__.py`, `frontends.py` (front-end `.sql` uniquement à ce stade :
  découpe en statements, origine fichier/ligne), `parser.py` (import paresseux de sqlglot,
  dialecte `databricks`, `parse_script() → list[Statement]`, erreur explicite si l'extra manque).
- `inventory.py` : compte les types de nœuds AST par statement et par corpus (joins par type,
  fonctions, fenêtres, agrégats, sous-requêtes). C'est la **baseline de couverture brute**,
  indépendante du mapper.
- CLI : `skifer analyze <chemins…> --inventory` imprime l'inventaire. Codes de sortie : `0`
  succès, `1` erreur technique (fichier illisible, SQL non parsable : le statement fautif et sa
  position sont nommés).
- Tests : `tests/test_analyzer_parser.py`, `tests/test_analyzer_inventory.py`. Le cœur
  s'importe sans `sqlglot` (`sys.modules`), `skifer analyze` sans l'extra renvoie `1` avec le
  message d'installation.

**Statut : implémenté** (commit `feat(plan43-43.0)`). Le corpus et l'inventaire produisent,
vérifié de bout en bout : `join_types={'left': 2, 'inner': 3}`, `upper/trim/coalesce` à 3
occurrences chacun, `aggregate_names={'sum': 9, 'lag': 1}`, `window=2`, `subquery=3`, `cte=3`,
`union=2`, `case=2`, `group_by=1`, `having=1` — correspond au patron requis ci-dessus. Trouvaille
en cours de route, documentée dans le tableau de risques : le dialecte `databricks` de `sqlglot`
ne lit **pas** `''` comme guillemet échappé (contrairement à Spark/Databricks réel, qui accepte
les deux formes) ; le corpus et le découpeur utilisent `\'` exclusivement pour cette raison.

### 43.1 — Bloc `where:` / `where_groups:` dans la grammaire

**But** : fermer le trou sémantique avant que le mapper ne l'exploite. Point utile seul, même
sans analyzer.

- `core/schema_loader.py` : clés top-level `where` et `where_groups`, normalisées par
  `_normalize_filters` (mêmes formes string et dict, mêmes alias d'opérateurs, même gouvernance
  `allow_raw_sql` pour l'opérateur `sql`). Accepté en streaming (un filtre post-join est sans
  état) et en materialized view (compilable). Refusé seulement sur un schéma sans `tables`.
- `core/ir.py` : `ParsedSchema.where: list[ParsedFilter]`, `where_groups: list[list[ParsedFilter]]`.
- `core/interpreter.py` : étape **2.5**, entre les joins et `business_rules`, même construction
  que les filtres de table (`reduce` sur `build_filter`).
- `core/sql_compiler.py` : `WHERE` sur l'arbre de jointure, avant `GROUP BY` ; les filtres du
  `where_groups` en `(… AND …) OR (…)`. Participe au hash de définition des MV (il définit la
  vue).
- `core/json_schema.py` : exposition des deux clés. `lineage/tracker.py` : aucun changement
  (un filtre ne crée pas d'arête), test qui l'épingle. `semantic/output_projection.py` : aucun
  effet sur la projection, test qui l'épingle.
- Docs : `docs/core.md`, bloc YAML de `CLAUDE.md`/`AGENTS.md`. (`docs/yaml_spec.md` documente le
  modèle sémantique, pas le schéma de pipeline — hors périmètre de ce point, corrigé après coup.)
- Tests : `tests/test_schema_loader.py`, `tests/test_ir.py`, `tests/test_interpreter.py`,
  `tests/test_sql_compiler.py`, `tests/test_json_schema.py`, et **un test Spark** (fixture
  `spark`) qui montre que `where: ["b_status:equals:A"]` après un left join ne donne **pas** le
  même résultat que le filtre de table — c'est la raison d'être du bloc, elle doit être exécutée,
  pas supposée. Pinning tests : `tests/test_lineage_tracker.py` (le graphe est identique avec et
  sans `where`/`where_groups`), `tests/test_semantic_projection.py` (même `definition_hash`).

**Statut : implémenté** (commit `feat(plan43-43.1)`). Le test Spark a confirmé que la comparaison
`order_id:equals:"3"` (colonne entière vs valeur filtre string) passe sans erreur de cast ANSI sous
Spark 4.1 — pas de garde-fou de type nécessaire côté `build_filter`.

### 43.2 — Mapper statement → schéma, résidus, oracle de round-trip, couverture

**But** : la conversion d'un statement et sa preuve.

- `analyzer/mapper.py` : `map_statement(stmt) → MappedStatement(schema_dict, residues,
  target)`.
  - `FROM`/`JOIN` → `tables`/`join` ; `ON a.k = b.k [AND …]` → clés compactes (listes pour les
    clés composites) ; condition non-equi → résidu de jointure, statement en `needs_curation`.
  - `WHERE` → conjonction en `filter` (push-down selon D6) ou `where:` ; `OR` → mise en forme
    normale disjonctive **bornée** (au-delà de 8 groupes : opérateur `sql`), `where_groups` ou
    `filter_groups` selon le push-down. `IS NULL`, `IN`, `BETWEEN`, `LIKE` et leurs négations
    mappés sur le catalogue ; `NOT IN` sur colonne nullable signalé dans le rapport (sémantique
    SQL à trois valeurs conservée, mais piège documenté).
  - `SELECT` → `select_final` avec ops : `CAST`, `UPPER`, `LOWER`, `TRIM`, `ROUND`, `ABS`,
    `LENGTH`, `CEIL`, `COALESCE`, `NVL`, `SUBSTRING`, `SPLIT_PART`/`SPLIT`, `TO_DATE`,
    `CASE WHEN … THEN … ELSE` → chaîne `when`/`then`/`else`, littéral → `literal:`. Composition
    d'ops dans l'ordre d'évaluation. Tout le reste → `expr:` (D7).
  - `GROUP BY` → `aggregate` quand toutes les fonctions sont dans `AGGREGATE_FUNCTIONS`
    (`COUNT(DISTINCT)` → `count_distinct`, `COUNT(*)` → `source: "*"`) ; `HAVING` → `having`
    sur les alias ; `GROUP BY ALL` et positions ordinales résolus par `sqlglot.optimizer.qualify`.
  - `DISTINCT` sur toutes les colonnes → `drop_duplicates_on` ; `ROW_NUMBER() OVER (PARTITION
    BY … ORDER BY …) = 1` en `QUALIFY` ou en sous-requête → `preprocess.qualify`.
  - Cible : `CREATE TABLE … AS` / `INSERT OVERWRITE` → nom de table, couche déduite du schéma
    cible (`bronze|silver|gold` si le nom le dit, sinon `unknown`, à trancher en 43.3).
- `analyzer/roundtrip.py` : `verify(mapped, source_stmt) → RoundTripResult`. Le `schema_dict`
  passe par `load_schema` (dict) puis `parse_to_ir` puis `compile_select` ; source et
  recompilation sont normalisées par sqlglot (`normalize_identifiers`, `qualify`,
  `canonicalize`, `optimizer.simplify` sur les prédicats) et comparées structurellement.
  Résultat `verified | unverified(reason) | not_applicable` (statement avec résidu
  `needs_curation`). **Un `unverified` n'empêche pas l'émission** : il est visible dans le
  rapport et dans `metadata.roundtrip` du draft.
- `analyzer/report.py` : `AnalysisReport` sérialisé **par allowlist champ par champ**
  (jamais `asdict`) : par statement la cible, la couverture (expressions mappées / `expr:` /
  `needs_curation`), le verdict de round-trip, les résidus avec fragment et position ; par
  corpus les totaux. Rendu Markdown et JSON.
- CLI : `skifer analyze <chemins…> --dry-run [--format md|json]` : analyse et rapport, **rien
  n'est écrit**. Codes : `0` tout mappé et vérifié · `2` résidus ou round-trip non vérifié ·
  `1` erreur technique.
- Tests : `tests/test_analyzer_mapper.py` (un test par construction du périmètre, un par
  piège : left join + where, `NOT IN` nullable, apostrophe, DNF bornée),
  `tests/test_analyzer_roundtrip.py` (propriété sur tout le corpus 43.0 : chaque statement sans
  `needs_curation` est `verified`), `tests/test_analyzer_report.py` (allowlist : un champ ajouté
  au dataclass n'apparaît pas tant qu'il n'est pas admis).

**Statut : implémenté.** Sur le corpus 43.0 : 5/8 statements mappent et vérifient (`verified`) —
01, 02, 03, 04, 06 ; 3/8 restent `needs_curation` — 05 (CTE réutilisée par les deux branches d'un
`UNION ALL`), 07 (sous-requête scalaire répétée), 08 (`UNION ALL`) — résolus par le DAG/splitting
de 43.3. Écarts par rapport à l'esquisse ci-dessus : `GROUP BY ALL`/positionnel est un résidu
bloquant (pas de résolution via `sqlglot.optimizer.qualify`) ; le verdict de l'oracle est
`verified | divergent | unverified | skipped` (`skipped` pour `needs_curation`, pas
`not_applicable`) ; la comparaison WHERE est une DNF (ensemble de branches OR, chacune un ensemble
de conjoints ANDés) insensible aux parenthèses et à l'ordre des opérandes, pas une simple
conjonction à plat.

Quatre bugs réels trouvés et corrigés en testant au-delà du corpus (aucun n'était exercé par les
8 statements) : (1) la détection de CTE lisait `body.args.get("with")`, toujours `None` — la clé
réelle de sqlglot est `"with_"` (suffixe ajouté par collision avec le mot-clé Python `with`) — un
`WITH … SELECT` était donc mappé en silence comme si le nom de la CTE était une vraie table
physique, perdant son filtre et sa table source ; (2) `NOT LIKE` se parse en `Like(negate=True)`,
pas en `Not(Like(...))` comme `NOT IN`/`NOT BETWEEN`/`IS NOT NULL` — la négation était
silencieusement perdue ; (3) le pattern de dédup `ROW_NUMBER()=1` laissait le mapper WHERE
générique retraiter le même prédicat déjà consommé, ajoutant un `filter: [rn:equals:1]` parasite
sur une colonne qui n'existe plus post-dédup ; (4) un `OR` dont une branche `AND` est parenthésée
(`(a = 1 AND b = 2) OR c = 3`) n'était pas reconnu car l'aplatissement ne traversait pas le nœud
`Paren`. Documente aussi (et contourne, côté oracle uniquement) une limitation propre à cette
version de `sqlglot` : son dialecte `databricks` lit l'échappement ANSI `''` de `compile_select()`
comme une concaténation implicite de littéraux plutôt qu'un seul littéral échappé (Spark/Databricks
accepte `''` correctement) — l'oracle réécrit `''` en `\'` avant sa propre re-parse interne pour que
cette particularité de `sqlglot` ne se lise pas comme une fausse divergence ; le SQL compilé renvoyé
à l'appelant n'est pas touché et reste valide.

### 43.3 — DAG de script : découpage, partials, tables intermédiaires, couches

**But** : passer d'un statement à un projet.

- `analyzer/dag.py` : graphe « statement écrit X / lit Y » sur l'ensemble du corpus. Les vues
  temporaires et les CTE sont des nœuds non matérialisés ; les tables créées sont matérialisées.
  Cycle → erreur explicite nommant le cycle.
- `analyzer/splitter.py` : un draft YAML par nœud matérialisé. Un nœud non matérialisé lu par
  **plusieurs branches d'un même script** → `partials:` (fichier `_partials/<alias>.yaml`
  relatif au draft parent, comme le Plan 25). Lu par **plusieurs scripts** → table intermédiaire
  proposée : un draft dédié dans la couche Silver et les consommateurs réécrits pour la lire.
  Lu une seule fois → inliné dans son consommateur (CTE fusionnée en tables/joins).
- Couches : degré entrant zéro et non créé par le corpus → référence Bronze ; nœud avec
  `aggregate` ou dont tous les consommateurs sont externes → Gold ; le reste → Silver. Le nom de
  schéma cible, s'il contient `bronze|silver|gold|stg|staging|dwh|mart`, l'emporte sur
  l'heuristique. L'affectation est toujours écrite dans `metadata.layer_reason`.
- Rapport : section « découpage » avec le DAG en Mermaid (réutilise `lineage/renderer.py`
  pour le rendu), la liste des partials et des tables intermédiaires proposées, chacune avec
  ses lecteurs.
- Tests : `tests/test_analyzer_dag.py`, `tests/test_analyzer_splitter.py` sur le corpus 43.0
  (la CTE réutilisée devient un partial, la sous-requête inter-scripts une table intermédiaire,
  les couches attendues sont obtenues et justifiées).

### 43.4 — Fingerprints, règles partagées, divergences, émission

**But** : la demande d'origine, les traitements récurrents en règles partagées, et l'écriture
des artefacts.

- `analyzer/fingerprint.py` : `expression_fingerprint(expr, *, abstract_literals: bool)` →
  `sha256:v1:…` sur l'AST canonisé (identifiants normalisés, alias retirés, opérandes des ops
  commutatives triés, littéraux remplacés par un placeholder typé quand `abstract_literals`).
  `subquery_fingerprint` sur `(tables, filtres, projection)`. Insensible à `PYTHONHASHSEED`,
  itération sur clés triées.
- `analyzer/patterns.py` : seuils **nommés et explicables** comme `adaptive/recommender.py`
  (`min_occurrences=3`, `min_scripts=2`, chacun cité dans la raison). Produit :
  - `SharedRuleCandidate` : expression récurrente, occurrences (fichier, ligne, alias de
    sortie), kind déduit (`projection` si ligne à ligne, `aggregation` si agrégat, `transform`
    sinon), nom proposé par vote majoritaire des alias de sortie, paramètres si les littéraux
    varient ;
  - `DivergenceFinding` : même alias de sortie, fingerprints différents, dans ≥ 2 endroits —
    les deux expressions sont nommées, jamais arbitrées ;
  - un candidat sous le seuil n'est pas un silence : il apparaît dans une section « sous le
    seuil » avec la valeur observée et le seuil.
- `analyzer/emitter.py` :
  - drafts YAML dans `schemas/.drafts/` (D5) via `write_yaml_atomic`, avec
    `_generated_by: project_analyzer`, `metadata: {source: {file, statement_index,
    sql_sha256}, coverage, roundtrip, needs_curation: [...], layer_reason}` ;
  - stubs de règles dans `rules/_generated/<nom>.py` : traduction `F.*` pour le sous-ensemble du
    catalogue (par réutilisation de `RuleService.generate_snippet` étendu), repli
    `F.expr("<fragment>")` sinon, chaque fichier avec marqueur et hash ; les drafts qui
    utilisent la règle la référencent dans `business_rules` et retirent l'expression de leur
    `select_final` ;
  - **staging puis validation** : chaque draft passe `load_schema`, chaque stub `ast.parse`
    puis `RuleAnalyzer.analyze_rule` avant publication (pattern `ProposalGenerator`) ; un
    artefact qui ne charge pas n'est jamais publié, et le rapport dit pourquoi ;
  - refus d'écraser un fichier sans marqueur ; un fichier avec marqueur et hash de source
    identique n'est pas réécrit (byte-for-byte reproductible).
- CLI : `skifer analyze <chemins…> --emit [--out DIR] [--min-occurrences N] [--strict]`.
  Codes : `0` émis · `2` émis avec résidus (ou refusé en `--strict`) · `4` sortie existante non
  gérée · `1` erreur.
- Tests : `tests/test_analyzer_fingerprint.py` (invariance à l'ordre, aux alias, au hashseed),
  `tests/test_analyzer_patterns.py` (le corpus produit exactement les candidats attendus, la
  divergence `net_margin` est trouvée, la variante de littéral devient un paramètre),
  `tests/test_analyzer_emitter.py` (marqueurs, refus d'écrasement, reproductibilité, validation
  en staging), `examples/24_project_analyzer/run.py` (analyse du corpus, émission dans un
  répertoire temporaire, assertions sur le rapport), exécuté par `tests/test_examples.py`.

### 43.5 — Run de parité

**But** : prouver l'équivalence sur les données avant de basculer.

- `analyzer/parity.py` : `ParityRunner(engine)` exécute un draft via `run_process_to_table`
  vers une table sandbox puis la compare à la table legacy : volumes, colonnes (noms, types),
  hash de lignes triées sur une clé déclarée ou sur toutes les colonnes, et par colonne le
  nombre de lignes différentes. `ParityReport` sérialisé par allowlist ; **aucune valeur de
  donnée** dans le rapport, seulement des comptes et des noms de colonnes (même règle que les
  alertes d'incident).
- CLI : `skifer analyze parity <draft.yaml> --against <fqn_legacy> [--key col,…]`. Codes :
  `0` identique · `5` écart mesuré · `1` erreur.
- Tests : `tests/test_analyzer_parity.py` avec la fixture `spark` : un draft converti du corpus
  reproduit la table legacy construite par le SQL source ; un draft volontairement faux (filtre
  poussé à tort) est détecté.

### 43.6 — Enrichissement LLM contraint (optionnel)

**But** : noms et descriptions lisibles, sans jamais toucher à la structure.

- `analyzer/llm_enrichment.py` : prend un `AnalysisReport` et propose, via `LLMProvider`,
  pour chaque candidat de règle un nom (`^[A-Za-z_][A-Za-z0-9_]*$`, refusé sinon) et une
  docstring, pour chaque draft une `description`, pour les fingerprints proches un regroupement
  **proposé** (jamais appliqué sans `--accept-groups`). La sortie est parsée comme JSON fermé ;
  toute clé hors allowlist est rejetée. Les noms de tables, conditions de jointure et valeurs
  de filtre **ne passent jamais** par le LLM.
- Pour un fragment `needs_curation`, propose un corps de règle Python, validé par `ast.parse`
  et `RuleAnalyzer` ; publié dans `rules/_generated/` avec `_needs_review: true`.
- CLI : `--enrich` sur `--emit`. Sans provider configuré : no-op silencieux et rapport inchangé.
- Tests : LLM mocké ; une réponse qui tente de renommer une table ou d'ajouter une jointure
  est rejetée avec sa raison.

### 43.7 — Front-ends notebooks et historique de requêtes

- `frontends.py` : notebooks Databricks `.py` (cellules `# MAGIC %sql`, littéraux dans
  `spark.sql("…")` lus par l'AST Python, f-strings refusées avec position), `.ipynb` (cellules
  `%%sql` et `spark.sql`). Chaque statement garde son origine (fichier, cellule, ligne).
  `NotebookExtractor` n'est pas modifié.
- Historique de requêtes Databricks (sous-point séparé, extra `[databricks]`) :
  `skifer analyze history --schema <cat.sch> --days N --profile <p>` lit
  `system.query.history` via le SDK, dédoublonne par fingerprint, et alimente le même pipeline.
  Aucune donnée de résultat n'est lue, uniquement le texte des requêtes ; l'identité de
  l'auteur est **omise** du rapport.
- Tests : `tests/test_analyzer_frontends.py` (fixtures `.py` et `.ipynb` dans `tests/fixtures/`),
  historique mocké.

### 43.8 — Documentation et synchronisation

- `docs/project_analyzer.md` : parcours complet sur l'exemple 24 (inventaire → dry-run → emit →
  parité → promotion manuelle d'un draft), lecture du rapport, codes de sortie, limites et
  résidus, règle « le LLM ne décide jamais de la structure ».
- `mkdocs.yml` : entrée de nav. `mkdocs build --strict` vert.
- `CLAUDE.md` / `AGENTS.md` : arborescence `analyzer/`, section « Project analyzer (Plan 43) »,
  statut du plan.
- `examples/README.md` : ligne pour l'exemple 24.

## Risques

| Risque | Effet | Mitigation |
|---|---|---|
| Écart entre l'oracle SQL↔SQL et l'exécution DataFrame | Un statement `verified` qui tourne différemment sur Spark | Le round-trip prouve la fidélité **au compilateur** ; la parité (43.5) prouve la fidélité **aux données**. Les tests Spark de 43.1 et 43.5 épinglent les cas connus (outer join, `NOT IN` nullable). |
| Sémantique à trois valeurs (`NOT IN`, `!=` avec NULL) | Résultats silencieusement différents | Conservée telle quelle (le catalogue a les mêmes sémantiques SQL), mais chaque occurrence sur colonne potentiellement nullable est signalée dans le rapport. |
| Explosion de la DNF sur des `WHERE` complexes | Temps de calcul, YAML illisible | Borne à 8 groupes, au-delà opérateur `sql` et signalement. |
| Collision de noms de colonnes après join | `where:` ambigu, erreur Spark à l'exécution | Le mapper détecte les colonnes homonymes entre tables jointes et émet un `fields:` de renommage ou un résidu ; test dédié. |
| `sqlglot` (dialecte `databricks`) ne lit **pas** `''` comme guillemet échappé dans un littéral — Spark/Databricks SQL accepte `''` et `\'`, mais `sqlglot` ne reconnaît que `\'` ; `'O''Brien'` est lu comme deux littéraux adjacents concaténés implicitement (`CONCAT('O','Brien')`), perdant l'apostrophe. Vérifié empiriquement sur un vrai `SparkSession.sql(...)` (43.0) : Spark accepte les deux formes, `sqlglot` une seule. | Un script legacy réel utilisant `''` serait mappé vers la mauvaise valeur, silencieusement | Documenté ici et dans `examples/24_project_analyzer/legacy_sql/02_stg_customers.sql`. Le corpus utilise `\'`, la forme que `sqlglot` lit correctement. Le mapper (43.2) devra détecter un `Concat` de littéraux adjacents sans séparateur et le signaler comme résidu probable (pas le traduire en `concat:` silencieusement) plutôt que de supposer que tout `CONCAT` du AST vient du SQL source. |
| Dialecte : constructions Databricks non parsées par sqlglot | Statement perdu | `1` avec position ; le statement est listé « non parsé », jamais avalé. Version de sqlglot épinglée en borne basse, testée en CI. |
| Faux positifs de règles partagées (expressions triviales récurrentes) | Bruit, perte de confiance | Liste d'exclusion explicite (`CAST` seul, `COALESCE(x, 0)` d'un seul nœud), seuils nommés, section « sous le seuil ». |
| Taille de `cli.py` | Point d'entrée déjà long | Sous-commandes de l'analyzer dans `analyzer/cli.py`, enregistrées depuis `cli.py` en une ligne. |
| Dérive de l'oracle quand la grammaire évolue | Un nouvel op du catalogue sans mapping | Test de complétude : chaque entrée de `COLUMN_OPS`, `FILTER_OPERATORS`, `AGGREGATE_FUNCTIONS` a un cas de round-trip, sinon le test échoue en nommant l'entrée. |

## Vérification

- **Sans Spark** : tout `analyzer/` sauf `parity.py` est pur Python + sqlglot ; les tests
  tournent dans le lot rapide (~27 s aujourd'hui). Propriété de round-trip sur l'intégralité
  du corpus 43.0.
- **Avec Spark** (fixture `spark`) : le test de raison d'être du `where:` (43.1), la parité sur
  deux drafts (43.5).
- **Exemple** : `examples/24_project_analyzer/run.py` exécuté par `tests/test_examples.py`.
- **Isolation** : le cœur s'importe sans `sqlglot` ; `skifer --help` fonctionne sans l'extra.
- **Gate** : `pytest tests/ -x --tb=short && ruff check src/` après chaque point,
  `mkdocs build --strict` en 43.8.

## Ordre et dépendances

```text
43.0 corpus + parser ──▶ 43.2 mapper + oracle ──▶ 43.3 DAG ──▶ 43.4 patterns + émission ──▶ 43.5 parité
43.1 where: ────────────┘                                                                  ├─▶ 43.6 LLM
                                                                                           ├─▶ 43.7 front-ends
                                                                                           └─▶ 43.8 docs
```

43.0 et 43.1 sont indépendants et peuvent être livrés dans n'importe quel ordre ; 43.2 a besoin
des deux. 43.5 à 43.7 sont indépendants entre eux.
