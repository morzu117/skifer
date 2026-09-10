# Plan 17 — Optimisation du core : fail-fast, refactor engine, IR partagé

> **Branche :** `feat/plan17-core-optimization`
> **Statut :** En attente de validation utilisateur
> **Exécutant :** GitHub Copilot (Sonnet 4.6 ou GPT 5.4) — handoff
> **Superviseur :** Julien Houbart
> **Date :** 2026-06-12

---

## Contexte

Une revue complète du core (juin 2026) a identifié trois problèmes structurels :

1. **Échecs silencieux** : un opérateur de filtre inconnu produit `lit(True)` (la condition
   est ignorée — risque de corruption de données en prod), une condition `when:` inconnue
   produit `lit(False)`, une opération inconnue laisse la colonne inchangée. Tout cela avec
   un simple `print` comme seule trace.
2. **Interprétation des op-strings tripliquée** : `"cast:double"`, `"region:equals:EMEA"` sont
   parsés par des chaînes de `startswith` dans `core/operations.py` (Spark),
   `backends/snowpark.py` et `backends/sql_base.py`. Chaque nouvel opérateur doit être ajouté
   à 3+ endroits. Côté moteur, `process_schema`, `describe_schema`, `infer_output_schema` et
   `lineage/tracker.py` ré-interprètent chacun les formes compactes à leur façon.
3. **`SkiferEngine` est une god class** (~1 400 lignes) : bootstrap env/config/user/sandbox,
   exécution de schéma, patterns d'écriture, inspection, lineage, factory d'agent. Elle
   duplique en outre ~150 lignes déjà extraites dans `core/environment.py`.

La solution cible : **un catalogue d'opérateurs + une représentation intermédiaire (IR)
parsée une seule fois au chargement**, des **backends réduits à une table
`{opérateur: fonction}`**, et un **engine façade** composé de modules extraits.

---

## Règles d'exécution pour l'agent implémenteur (Copilot)

Ces règles sont **obligatoires** et s'ajoutent au workflow décrit dans `AGENTS.md` / `CLAUDE.md` :

1. **Un point du plan = un commit séparé.** Message conventionnel référençant le point :
   `feat(plan17-2.3): extract SchemaInterpreter from SkiferEngine`.
   Ne jamais regrouper deux points dans un commit.
2. **Après chaque point** : `pytest tests/ -x --tb=short` au vert + `ruff check src/` propre
   avant de committer.
3. **Chaque modification de `src/`** : test associé écrit/mis à jour + entrée
   `CHANGELOG.md` sous `## [Unreleased]` (dans le même commit).
4. **Ne jamais toucher au numéro de version** (`pyproject.toml`).
5. **Aucun breaking change sur l'API publique** : les signatures de `SkiferEngine`,
   `load_schema`, `parse_schema` et les formes YAML existantes restent valides.
   Exception assumée et documentée : le point 1.3 (fail-fast runtime) transforme des
   warnings silencieux en erreurs — c'est voulu, à inscrire en `### Changed` dans le CHANGELOG.
6. **Transversal — dans chaque commit qui modifie substantiellement un fichier** :
   remplacer les `print()` de ce fichier par `logging` (logger module-level,
   `logger = logging.getLogger(__name__)`) et passer les docstrings/messages en anglais.
   Ne pas faire de commit "big bang" logging séparé.
7. **Respecter l'ordre des lots et les dépendances** ci-dessous. À l'intérieur d'un lot,
   suivre l'ordre des points.

---

## Vue d'ensemble : lots, dépendances, ordre

```
Lot 1 — Fail-fast & catalogue d'opérateurs   (indépendant — PRIORITÉ : correctness)
Lot 0 — Quick wins                            (indépendant, sauf 0.6 → 0.4)
Lot 2 — Refactor engine (phases A→D)          (indépendant des lots 0/1, séquentiel interne)
Lot 3 — IR partagé & formes YAML structurées  (dépend de : Lot 1 [catalogue] + Lot 2.3 [interpreter])
Lot 4 — Outillage (JSON Schema, CLI validate) (dépend de : Lot 3.1)
```

Ordre d'implémentation recommandé (linéaire) :
**1.1 → 1.2 → 1.3 → 1.4 → 0.1 → 0.2 → 0.3 → 0.4 → 0.5 → 0.6 → 2.1 → 2.2 → 2.3 → 2.4 → 3.1 → 3.2 → 3.3 → 3.4 → 3.5 → 4.1 → 4.2**

Chaque lot peut faire l'objet d'une PR distincte (recommandé : 5 PRs, une par lot).

---

## Lot 1 — Fail-fast & catalogue d'opérateurs

**Objectif :** plus aucune faute de frappe silencieuse. Toute erreur de schéma sort au
chargement (`load_schema`/`parse_schema`) ou au plus tard au démarrage du run, jamais en
cours de job avec un résultat faux.

### 1.1 — Créer le catalogue d'opérateurs (`core/op_catalog.py`)

- **Créer :** `src/skifer/core/op_catalog.py`
- Contenu : structures de données **pures** (aucun import Spark) décrivant :
  - `FILTER_OPERATORS` : dict `{nom_canonique: OperatorSpec}` avec aliases
    (`eq`, `gt`, `gte`…), arité (sans valeur / valeur simple / liste), description courte
    (servira au JSON Schema du Lot 4).
  - `COLUMN_OPS` : dict `{nom: OpSpec}` pour `cast`, `round`, `upper`, `lit`, `expr`,
    `split`, `substring`, `to_date`, `nvl`, `coalesce`, `when/then/else`, etc.
    Avec : nombre/type d'arguments, flag `requires_raw_sql` (pour `expr`/`sql`).
  - Helper `suggest(name, catalog)` → suggestions via `difflib.get_close_matches`
    (même pattern que `agentic/resolver.py`).
- La source de vérité des listes : sections « Filter operators » et « select_final
  operations » de `CLAUDE.md` + implémentations actuelles de `core/operations.py`.
- **Tests :** `tests/test_op_catalog.py` — exhaustivité (chaque opérateur géré par
  `operations.py` est dans le catalogue), résolution d'alias, suggestions.
- **Commit :** `feat(plan17-1.1): add operator catalog (op_catalog.py) as single source of truth`

### 1.2 — Validation fail-fast au chargement du schéma

- **Modifier :** `core/schema_loader.py`
- Dans `_normalize_filters` / `_normalize_schema` : valider chaque opérateur de filtre et
  chaque op de `select_final`/`add_columns`/`fields` contre le catalogue 1.1.
  Opérateur inconnu → `ValueError` listant : table/colonne concernée, opérateur fautif,
  suggestions (`Did you mean 'equals'?`), liste des opérateurs valides.
- **Collecter toutes les erreurs** du schéma avant de lever (une seule exception listant
  tout, pas une erreur à la fois).
- **Tests :** `tests/test_schema_loader.py` — cas nominal inchangé, faute de frappe sur
  filtre, sur op, sur `when:`, erreurs multiples agrégées.
- **Commit :** `feat(plan17-1.2): fail-fast schema validation against operator catalog`

### 1.3 — Fail-fast à l'exécution (filets de sécurité)

- **Modifier :** `core/operations.py`, `backends/snowpark.py`, `backends/sql_base.py`
- Remplacer les comportements silencieux par des levées de `ValueError` :
  - filtre inconnu : supprimer le `return F.lit(True)` (ligne ~185)
  - condition `when:` inconnue : supprimer le `return F.lit(False)` (ligne ~72)
  - opération inconnue : supprimer le pass-through (ligne ~113)
  - aligner snowpark (qui retourne `c` silencieusement) sur le même comportement.
- Ces chemins ne devraient plus être atteignables après 1.2 (défense en profondeur pour
  les dicts construits à la main passés directement à `process_schema`).
- **CHANGELOG : entrée `### Changed` explicite** (comportement durci).
- **Tests :** mettre à jour les tests existants qui s'appuyaient sur le passthrough.
- **Commit :** `feat(plan17-1.3): raise on unknown operators at runtime (was silent no-op)`

### 1.4 — Validation des alias de join au chargement

- **Modifier :** `core/schema_loader.py`
- Vérifier que chaque `table_from`/`table_to` d'un `join` référence un alias (ou nom)
  déclaré dans `tables`. Alias inconnu → `ValueError` avec suggestions
  (aujourd'hui : `KeyError` brut en plein run dans `process_schema`).
- **Tests :** `tests/test_schema_loader.py` + non-régression `tests/test_core_join.py`.
- **Commit :** `feat(plan17-1.4): validate join aliases at schema load time`

---

## Lot 0 — Quick wins

### 0.1 — Supprimer le `count()` avant `dropDuplicates`

- **Modifier :** `core/core.py` (`run_union_sources_to_table`, ligne ~1009) — le
  `before_count` ne sert qu'à un message de log et coûte un job Spark complet.
  Log sans le count.
- **Tests :** ajuster `tests/test_core.py` si le count y est asserté.
- **Commit :** `perf(plan17-0.1): drop full count() before dropDuplicates in union pattern`

### 0.2 — Appliquer `dev_limit` après les filtres

- **Modifier :** `core/core.py` (`process_schema`) — déplacer l'application de `dev_limit`
  **après** `filter`/`quality_checks`/`filter_groups` (aujourd'hui un `limit(5000)` avant
  `filter(region=EMEA)` peut produire 0 ligne en dev).
- **CHANGELOG : `### Changed`** (changement de comportement en mode dev uniquement).
- **Tests :** `tests/test_core.py` — vérifier l'ordre filtre→limit.
- **Commit :** `fix(plan17-0.2): apply dev_limit after filters, not before`

### 0.3 — `add_columns` passe par `get_select_expressions`

- **Modifier :** `core/core.py` — la branche `keep_all_columns`+`add_columns`
  (lignes ~838-847) réimplémente la boucle d'ops sans support `when/else` ni forme dict.
  La faire passer par `get_select_expressions` (ou le helper commun) pour un comportement
  identique à `select_final`.
- **Tests :** `tests/test_core.py` — `add_columns` avec forme dict `when/else`.
- **Commit :** `feat(plan17-0.3): support when/else and dict form in add_columns`

### 0.4 — Cache des profils AST sur `RuleSpec`

- **Modifier :** `core/registry.py` (champ `profile` lazy sur `RuleSpec`),
  `core/rule_analyzer.py` (utilise le cache), `core/rule_planner.py` (ne ré-analyse plus).
- Invalidation : le ré-enregistrement d'une règle (même nom) remplace le `RuleSpec`,
  donc le cache suit naturellement.
- **Tests :** `tests/test_rule_analyzer.py` — l'analyse n'est exécutée qu'une fois par
  fonction (compteur/mock sur `inspect.getsource`).
- **Commit :** `perf(plan17-0.4): cache AST rule profiles on RuleSpec`

### 0.5 — Canonicalisation AST pour `DUPLICATE_EXPR`

- **Modifier :** `core/rule_analyzer.py`
- Avant comparaison des expressions : `ast.NodeTransformer` qui (a) trie les opérandes
  des opérations commutatives (`+`, `*`, `&`, `|`, `==`, `!=`), (b) normalise
  `F.col("x")` / `col("x")` / `df["x"]` vers une forme unique, (c) compare les
  `ast.unparse` des formes canoniques.
- **Tests :** `tests/test_rule_analyzer.py` — `F.col("a") >= 1000` vs
  `1000 <= F.col("a")` détectés comme doublons ; faux positifs évités
  (`a - b` vs `b - a` NON équivalents).
- **Commit :** `feat(plan17-0.5): canonicalize AST before duplicate expression detection`

### 0.6 — Warnings `explain_rules` automatiques en mode interactif

- **Dépend de :** 0.4 (l'analyse doit être gratuite).
- **Modifier :** `core/core.py` (`_apply_business_rules`) — en mode interactif
  (`not is_job_execution` et non-prod), émettre les warnings de `RuleAnalyzer.detect_warnings`
  via `logger.warning` au moment du plan. **Aucune modification de l'exécution** —
  information seule. Silencieux en job/prod.
- **Tests :** `tests/test_core.py` — warnings émis en interactif, absents en job.
- **Commit :** `feat(plan17-0.6): surface rule redundancy warnings automatically in interactive mode`

---

## Lot 2 — Refactor `SkiferEngine` (façade)

**Objectif :** engine ≤ ~300 lignes, API publique strictement inchangée. Chaque phase
laisse la suite de tests au vert sans modification des tests publics (seuls les tests
de méthodes privées déplacées sont déplacés avec elles).

### 2.1 — Phase A : déduplication vers `environment.py`

- **Modifier :** `core/core.py` — supprimer les corps de `_is_running_as_job`,
  `_get_clean_username`, `_get_workspace_client` et déléguer aux fonctions existantes de
  `core/environment.py` (`is_running_as_job`, `get_clean_username`, `get_workspace_client`).
  Vérifier la parité de comportement (stratégies de fallback, fichier `.skifer_user`)
  avant suppression ; si `environment.py` diverge, l'aligner d'abord.
- **Tests :** `tests/test_core_username.py`, `tests/test_core_env_detection.py` au vert
  sans modification.
- **Commit :** `refactor(plan17-2.1): deduplicate engine env/user detection into environment.py`

### 2.2 — Phase B : `ExecutionContext`

- **Créer :** `src/skifer/core/context.py` — dataclass `ExecutionContext` :
  `env`, `db`, `config`, `current_user`, `is_job_execution`, `is_local`, `schema_suffix`,
  `default_params` (property).
- **Modifier :** `core/core.py` — construit `self.context` dans `__init__` ; les attributs
  historiques (`self.env`, `self.db`, `self.schema_suffix`…) deviennent des properties de
  délégation (compat totale).
- **Tests :** `tests/test_context.py` (nouveau) + suite existante au vert.
- **Commit :** `refactor(plan17-2.2): introduce ExecutionContext dataclass`

### 2.3 — Phase C : `SchemaInterpreter`

- **Créer :** `src/skifer/core/interpreter.py` — classe `SchemaInterpreter`
  (constructeur : `backend`, `context`, `config`). Y déplacer : `process_schema`,
  `get_select_expressions`, `_apply_business_rules`.
- **Modifier :** `core/core.py` — les méthodes deviennent des délégations d'une ligne
  vers `self._interpreter` (API publique inchangée, y compris `process_schema(schema_dict,
  dataframes_in=)`).
- **Tests :** **déplacer** les tests de logique d'interprétation vers
  `tests/test_interpreter.py` ; garder dans `test_core.py` des tests de délégation.
- **Commit :** `refactor(plan17-2.3): extract SchemaInterpreter from SkiferEngine`

### 2.4 — Phase D : patterns d'exécution

- **Créer :** `src/skifer/core/patterns.py` — fonctions
  `run_process_to_table`, `run_process_and_split`, `run_union_sources_to_table`,
  `run_from_yaml` prenant `(engine_ou_composants, …)`.
- **Modifier :** `core/core.py` — méthodes = délégations.
- **Tests :** `tests/test_patterns.py` (déplacés depuis `test_core.py`).
- **Commit :** `refactor(plan17-2.4): extract run patterns into core/patterns.py`

---

## Lot 3 — IR partagé & formes YAML structurées

**Dépend de :** Lot 1 (catalogue) + 2.3 (interpreter isolé).
**Objectif :** parser une fois, exécuter partout. Suppression de la triplication.

### 3.1 — Dataclasses IR + parseur

- **Créer :** `src/skifer/core/ir.py` :
  - `ParsedOp(name, args)` — ex. `ParsedOp("cast", ["double"])`
  - `ParsedFilter(column, operator, value)`
  - `ParsedColumnSpec(source, target, ops | when_chain)`
  - `ParsedSchema` (tables, joins, rules, select, sink…)
  - `parse_to_ir(normalized_schema_dict) -> ParsedSchema` — consomme la sortie actuelle
    de `_normalize_schema`, valide contre le catalogue 1.1.
- `load_schema`/`parse_schema` continuent de retourner le **dict** (compat) mais y
  attachent l'IR (clé privée `"_ir"` ou cache par id) ; l'interpreter consomme l'IR.
- **Tests :** `tests/test_ir.py` — round-trip de toutes les formes YAML documentées
  dans `CLAUDE.md` (compactes, dict, literal:, filter_groups, when/else).
- **Commit :** `feat(plan17-3.1): add intermediate representation (ir.py) parsed once at load`

### 3.2 — Les backends consomment l'IR

- **Modifier :** `core/operations.py`, `backends/spark.py`, `backends/snowpark.py`,
  `backends/sql_base.py`, `core/backend.py` (Protocol)
- Nouveau contrat backend : `apply_op(c, op: ParsedOp)` et
  `build_filter(f: ParsedFilter)` implémentés comme **table de correspondance**
  `{op_name: fonction}` par backend. Les anciennes méthodes string
  (`apply_operation(c, "cast:double")`) restent en façade dépréciée : elles parsent
  via l'IR puis délèguent (compat pour les appels directs).
- Supprimer les trois parseurs `startswith` dupliqués.
- **Tests :** `tests/test_backend_spark.py`, `test_backend_snowpark.py`,
  `test_backend_sql_base.py` — parité de comportement op par op (réutiliser les tests
  existants comme oracle).
- **Commit :** `refactor(plan17-3.2): backends consume IR via per-op dispatch tables`

### 3.3 — Inspection sur l'IR

- **Modifier :** `core/core.py` (`describe_schema`, `infer_output_schema`) — consommer
  `ParsedSchema` au lieu de ré-interpréter les formes compactes. L'inférence de type de
  `infer_output_schema` se base sur les specs du catalogue (type de sortie déclaré par op).
- **Tests :** non-régression sur les sorties structurées existantes.
- **Commit :** `refactor(plan17-3.3): describe_schema and infer_output_schema consume IR`

### 3.4 — Lineage sur l'IR

- **Modifier :** `lineage/tracker.py` (`LineageTracker.from_schema`) — consommer l'IR.
- **Tests :** `tests/test_lineage_tracker.py` au vert sans changement de sortie.
- **Commit :** `refactor(plan17-3.4): lineage tracker consumes IR`

### 3.5 — Formes YAML structurées (alternative au mini-langage)

- **Modifier :** `core/schema_loader.py` (normalisation → IR identique)
- Nouvelles formes, **en plus** des formes actuelles (qui restent des alias valides) :
  ```yaml
  filter:                                # forme mapping
    region: EMEA                         # equals implicite
    status: { in: [ACTIVE, PENDING] }
    customer_id: not_null
    amount: { greater_than: 100 }

  select_final:
    - { from: amount, as: amount_eur, ops: [cast: double, round: 2] }
    - { from: status, as: status_label, ops: [upper, trim] }
  ```
  (`ops: [cast: double, round: 2]` parse nativement en YAML flow vers
  `[{cast: double}, {round: 2}]` — aucun parsing de string.)
- **Documenter** les deux formes dans `CLAUDE.md`/`AGENTS.md`/mkdocs (`docs/core.md`).
- **Tests :** `tests/test_schema_loader.py` — équivalence stricte forme mapping ↔ forme
  compacte (même IR).
- **Commit :** `feat(plan17-3.5): structured YAML forms for filters and ops (mapping style)`

---

## Lot 4 — Outillage

**Dépend de :** 3.1 (et 3.5 pour l'intérêt complet).

### 4.1 — JSON Schema généré depuis le catalogue

- **Créer :** `src/skifer/core/json_schema.py` — `generate_json_schema() -> dict`
  construit depuis `op_catalog.py` (enum des opérateurs, descriptions, formes
  mapping + compactes via `oneOf`). Sortie versionnée committée :
  `schemas/skifer-pipeline.schema.json` (régénérée par test de non-dérive :
  le test échoue si le fichier committé ≠ généré).
- **Documenter** dans mkdocs (`docs/getting_started.md`) : branchement VS Code
  (`# yaml-language-server: $schema=…` ou setting `yaml.schemas`) et PyCharm
  (JSON Schema Mappings).
- **Tests :** `tests/test_json_schema.py` — schéma valide (méta-validation), non-dérive,
  un YAML d'exemple valide passe / un opérateur inconnu échoue.
- **Commit :** `feat(plan17-4.1): generate pipeline JSON Schema from operator catalog`

### 4.2 — `skifer validate`

- **Modifier :** `cli.py` — sous-commande `validate <paths…>` (globs acceptés) :
  charge chaque schéma via `load_schema` (params factices injectés automatiquement pour
  les `{{ }}` : valeur sentinelle), agrège **toutes** les erreurs, sortie lisible
  (fichier → erreurs), exit code 1 si au moins une erreur. Aucune dépendance Spark.
- **Tests :** `tests/test_cli.py` — schéma valide (exit 0), invalide (exit 1, message),
  multi-fichiers.
- **Commit :** `feat(plan17-4.2): add 'skifer validate' CLI subcommand`

---

## Risques & mitigations

| Risque | Impact | Mitigation |
|---|---|---|
| 1.3 casse des pipelines existants qui dépendaient (sans le savoir) d'un opérateur ignoré | Moyen | C'est précisément le but — CHANGELOG `### Changed` explicite ; le message d'erreur donne la correction (suggestions) |
| 2.3 régression subtile en déplaçant `process_schema` | Élevé | Déplacement mécanique sans réécriture ; les tests existants servent d'oracle ; aucune modification de logique dans le même commit |
| 3.2 divergence de comportement entre ancien parseur string et dispatch IR | Élevé | Garder les tests backend existants inchangés comme oracle de parité ; la façade dépréciée garantit les appels directs |
| `environment.py` et copies engine ont divergé silencieusement (2.1) | Moyen | Diff comportemental explicite avant suppression ; aligner `environment.py` d'abord si besoin |
| Formes YAML mapping ambiguës (3.5) | Faible | La forme mapping et la forme compacte normalisent vers le même IR — testé par équivalence stricte |

## Stratégie de vérification

- `pytest tests/ -x --tb=short` après **chaque point** (pas seulement chaque lot).
- `ruff check src/` avant chaque commit.
- Lots 2 et 3 : les tests existants sont l'oracle de non-régression — ne pas les modifier
  dans le même commit que le refactor (sauf déplacement de fichier à l'identique).
- Fin de chaque lot : exécuter le notebook/exemple de `example/` en mode LOCAL si applicable.

## Checklist de montée de version (rappel — voir CLAUDE.md/AGENTS.md)

À chaque montée de version (gérée par l'utilisateur, jamais par l'agent), 5 points
obligatoires :

1. Aucune nouvelle feature sans tests associés et validés
2. Tests existants au vert — ou mis à jour s'ils sont KO pour une raison documentée
3. `CHANGELOG.md` à jour
4. `CLAUDE.md` / `AGENTS.md` à jour
5. Documentation mkdocs (`docs/`) à jour
