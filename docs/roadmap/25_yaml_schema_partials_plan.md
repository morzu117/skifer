# Plan 25 — Sous-transformations YAML inline (`partials:`)

## Statut

- **Branche** : `feat/yaml-schema-partials`
- **État** : **implémenté** — phases 25.1 → 25.5 (inline / temp_view / table, cycle detection, docs)

## 1. Contexte & problème

Le modèle d'exécution actuel est un ordre figé :

```
load sources → fields → filters → joins → business_rules → select_final → write
```

Conséquence : **un join ne peut pas dépendre d'une colonne produite par une
business rule**. Dès qu'un join a besoin d'une colonne calculée
(`explode_to_daily → work_day`, `compute_join_keys`,
`convert_amount_currency`), il faut matérialiser une table intermédiaire et
la recharger dans un YAML suivant. D'où la prolifération de notebooks et de
tables techniques pour **une seule transformation logique**.

Les deux contournements actuels sont mauvais :

1. charger les lookups dans des rules Python → casse le principe *What/How* (les
   sources et joins doivent être déclarés en YAML) ;
2. splitter en N notebooks/tables → bruit d'orchestration + churn de stockage.

## 2. Décisions de design (validées avec l'utilisateur)

Deux arbitrages ont été tranchés avant rédaction :

| Sujet | Décision retenue | Alternative écartée |
|---|---|---|
| **Surface YAML** | **Bloc top-level `partials:` dédié** produisant des DataFrames nommés, utilisables comme n'importe quel alias dans `join`/`business_rules`. | `source.type: yaml_schema` — écarté : mélange feuille I/O et sous-DAG, brouille le sens de `tables:`. |
| **Matérialisation (debug)** | **Param de run global `intermediate_mode`** (`inline` \| `temp_view` \| `table`), passé à `run_from_yaml(params=…)`. Le schéma de prod reste pur. | `mode` + `debug_name` dans le bloc source (RFC initiale) — écarté : métadonnée de debug qui fuit dans le schéma de prod. |

Conséquences directes de ces choix :

- **`tables:` conserve son invariant « feuille I/O »** (csv/parquet/delta/table
  catalogue). Aucun nouveau `source.type`, `VALID_SOURCE_TYPES` **inchangé** →
  rétro-compat totale.
- **`debug_name` de la RFC est supprimé** : l'**`alias`** du partial sert
  d'identifiant pour la vue/table de debug (+ suffixe sandbox). Un identifiant,
  pas deux.
- **Le mode ne vit pas dans le YAML** : par défaut `inline` partout, debug
  opt-in **à l'exécution** via `params={..., "intermediate_mode": "temp_view"}`.

### Forme cible

```yaml
# schemas/silver/customer_orders.yaml
partials:
  - alias: dly
    path: _partials/prepared_orders.yaml    # relatif au dossier du YAML parent
  - alias: qtr
    path: _partials/qtr_calendar.yaml

tables:
  - name: "{{ catalog }}.silver.date_dim"
    alias: cal
    fields:
      - [date, _dim_date]
      - [year_quarter, fiscal_qtr]

join:
  - table_from: [dly, work_day]      # work_day vient d'une rule DANS le partial dly
    table_to: [cal, _dim_date]
    type: left
  - table_from: [dly, fiscal_qtr]
    table_to: [qtr, _qtr_key]
    type: left

business_rules:
  - enrich_with_date_dim
  - compute_qtr_totals
```

```python
# Prod — inline par défaut, zéro table intermédiaire
engine.run_from_yaml("schemas/silver/customer_orders.yaml",
                     target_layer="silver", target_table_name="fct_customer_orders")

# Debug — matérialise chaque partial en vue temporaire nommée par son alias
engine.run_from_yaml("schemas/silver/customer_orders.yaml",
                     target_layer="silver", target_table_name="fct_customer_orders",
                     params={**engine.default_params, "intermediate_mode": "temp_view"})
```

## 3. Points de couplage (inventaire)

| Fichier | Rôle actuel | Impact |
|---|---|---|
| `core/schema_loader.py` | `load_schema`/`parse_schema` + `_normalize_schema`. Injection `{{param}}`, validation. | **Modif** : reconnaître/normaliser/valider `partials:`. Parsing **récursif** des enfants avec résolution de path relative au parent + détection de cycle. Nécessite de threader le **base-dir** (dossier du YAML parent) dans `parse_schema`. |
| `core/ir.py` | `ParsedSchema`, `ParsedTable`, `parse_to_ir`. | **Modif** : nouveau champ `ParsedSchema.partials: list[ParsedPartial]` (`alias`, `child_schema` dict, `resolved_path`). |
| `core/interpreter.py` | `process_schema(schema_dict, dataframes_in=…)` — boucle `tables:` → `dfs[alias]`. | **Modif** : avant la boucle `tables:`, **expandre les partials** → exécuter récursivement `process_schema(child)` → injecter dans `dfs[alias]`. Applique la matérialisation selon `intermediate_mode`. |
| `core/patterns.py` | `run_from_yaml` merge params → `load_schema` → `run_process_to_table` → `process_schema`. | **Modif** : extraire `intermediate_mode` de `params` (défaut `inline`) et le threader jusqu'à `process_schema`. |
| `core/core.py` | `process_schema(...)` délègue à l'interpréteur. | **Modif** : ajouter l'argument optionnel `intermediate_mode`. |
| `core/backend.py` (Protocol) | Contrat plateforme + `capabilities`. | **Modif** : nouvelle capability `"temp_view"` + méthode `register_temp_view(df, name)`. |
| `backends/spark.py` | Impl. Spark. Pas de helper temp view aujourd'hui. | **Modif** : `register_temp_view` → `createOrReplaceTempView` ; ajouter `"temp_view"` aux `capabilities`. |
| `backends/{snowpark,bigquery,sql_base}.py` | Backends non-Spark. | **Modif** : `register_temp_view` best-effort ou no-op loggé ; **gate** sur `capabilities` pour `temp_view`/`table` (fallback → `inline` + warning). |
| `core/sandbox.py` | `SandboxResolver` — nommage déterministe sandbox. | **Réutilisé** pour le nommage en mode `table` (déterministe, env-aware, anti-collision). |
| `tests/` | 1 fichier par module. | **Nouveau** : `tests/test_partials.py` + ajouts dans `test_schema_loader.py`, `test_interpreter.py`. |
| `CHANGELOG.md`, `CLAUDE.md`, `AGENTS.md`, `docs/` | Sync obligatoire. | **Modif** en fin de plan. |

## 4. Sémantique détaillée

### 4.1 Résolution & parsing (load-time, sans Spark)

- `load_schema(parent)` connaît le dossier du parent → **base-dir**.
- Pour chaque entrée `partials:`, résoudre `path` **relatif au dossier du YAML
  parent** (principe de moindre surprise, comme un `import`). Fallback racine
  projet **non** retenu.
- Parsing récursif de l'enfant via `load_schema(child, params=<hérités>)`.
- **Scoping des params** : l'enfant **hérite** des params résolus du parent
  (`default_params` + params du parent), avec possibilité d'override par entrée
  (`params:` optionnel sur l'entrée partial) dans un lot ultérieur si besoin. V1 :
  héritage simple.
- **Détection de cycle** : pile de chemins **absolus résolus** propagée dans la
  récursion. Un partial qui se référence directement ou indirectement →
  `ValueError` fail-fast au load avec la chaîne de cycle affichée.
- **Validation** : `alias` obligatoire et **unique** sur l'union
  `partials ∪ tables` (réutiliser la logique de collision d'alias) ; `path`
  obligatoire ; fichier introuvable → erreur claire.

### 4.2 Exécution & matérialisation (interpreter, avec backend)

Avant la boucle `tables:` dans `process_schema` :

```
pour chaque partial (ordre déclaré) :
    child_df = self.process_schema(child_schema)   # récursif, NE write PAS
    child_df = _materialize(child_df, alias, intermediate_mode)
    dfs[alias] = child_df
```

- `inline` (défaut) : DataFrame en mémoire, aucune vue/table. Marche sur **tous**
  les backends (c'est juste un DataFrame).
- `temp_view` : `backend.register_temp_view(child_df, name=<alias[+suffixe sandbox]>)`
  pour inspection interactive ; le parent continue d'utiliser le DataFrame
  directement. Gate `capabilities`.
- `table` : `backend.write_table(child_df, fqn)` où `fqn` est **déterministe et
  env-aware** via `SandboxResolver` (anti-collision en run sandbox), puis
  `read_table` ou réutilisation du DataFrame. Gate `capabilities`.
- Un partial exécuté via `process_schema` **ne déclenche jamais de write final**
  (le write vit dans `run_process_to_table`, pas dans `process_schema`) → mode
  `inline`/`temp_view` sans stockage durable garanti par construction.

### 4.3 Threading du mode

- `run_from_yaml(params=…)` : `intermediate_mode = params.get("intermediate_mode", "inline")`
  → passé à `process_schema(..., intermediate_mode=…)`.
- Chemin direct `run_process_to_table(schema_dict, …)` (sans params) : défaut
  `inline`. Le param global est le point d'entrée documenté pour le debug.

## 5. Phases (1 phase = 1 commit)

| Phase | Contenu | Fichiers | Test |
|---|---|---|---|
| **25.1** | IR + parsing/normalisation/validation de `partials:` (load-time, **sans exécution**) : champ IR, résolution path relative parent, héritage params, unicité alias, **détection de cycle** fail-fast. | `ir.py`, `schema_loader.py` | `test_schema_loader.py` : parse OK, cycle direct/indirect, alias dup, path introuvable, injection params enfant. |
| **25.2** | Expansion **`inline`** dans `process_schema` (récursif → `dataframes_in`). Mode par défaut, aucun changement backend. | `interpreter.py`, `core.py`, `patterns.py` | `test_partials.py` : partial inline dispo sous alias, join sur colonne produite par rule du partial, partials imbriqués. |
| **25.3** | Backend : capability `"temp_view"` + `register_temp_view`. Mode `temp_view` + gate `capabilities` (fallback inline + warning). | `backend.py`, `backends/spark.py`, autres backends | `test_partials.py` + `test_spark_backend.py` (mock) : vue enregistrée sous alias, backend sans capability → inline + warning. |
| **25.4** | Mode `table` : nommage déterministe via `SandboxResolver`, gate `capabilities`. | `interpreter.py`, `sandbox.py` (réutilisé) | `test_partials.py` : table matérialisée avec FQN déterministe, pas de collision en sandbox. |
| **25.5** | Doc & sync : `CHANGELOG.md`, `CLAUDE.md`, `AGENTS.md`, `docs/` (page dédiée partials + exemple dédié). | docs | — |

Après **chaque** phase : `pytest tests/ -x --tb=short` doit rester vert.

## 6. Risques & mitigations

| Risque | Mitigation |
|---|---|
| Récursion infinie / cycles | Détection au **load** (pile de paths absolus), fail-fast avant tout Spark. |
| Base-dir non disponible dans `parse_schema` (string) | Threader un `base_dir` optionnel ; partials à path **relatif** ne sont supportés que via `load_schema` (fichier). Un path absolu reste possible en `parse_schema`. |
| Collision d'alias `partials`/`tables` | Validation d'unicité sur l'union, message d'erreur avec suggestions. |
| `temp_view`/`table` non supportés hors Spark | Gate sur `capabilities` → fallback `inline` + warning explicite (jamais d'échec dur pour un knob de debug). |
| Explosion du plan Catalyst (partials imbriqués profonds) | `inline` garde les DataFrames lazy ; `table`/`temp_view` disponibles comme points de coupure explicites pour le debug perf. |
| Rétro-compat | Aucun `source.type` ajouté, `VALID_SOURCE_TYPES` inchangé, `partials:` absent = comportement identique. Tests de non-régression existants inchangés. |

## 7. Stratégie de vérification

- **Unitaire** : `tests/test_partials.py` couvrant les 4 critères d'acceptation
  (inline, temp_view, table, cycle) + imbrication + injection params + collision
  alias. Spark mocké (fixture `spark` locale), aucun cluster.
- **Non-régression** : suite complète verte après chaque phase.
- **Fumée manuelle** (optionnelle, hors CI) : reconstruire un mini
  `03_fct_customer_orders` avec 2 partials en local Delta.

## 8. Critères d'acceptation (rappel RFC, adaptés au design retenu)

- [x] Un YAML parent peut déclarer des sous-transformations via `partials:`.
- [x] La sortie d'un partial est disponible sous son `alias` (join/rules).
- [x] Modes `inline` / `temp_view` / `table` via `intermediate_mode` (param de run).
- [x] Injection de params sur parent **et** enfants (héritage).
- [x] Références récursives/cycliques → échec clair et précoce.
- [x] Types de `source:` existants inchangés (rétro-compat).
- [x] Tests unitaires : inline, temp view, table, cycle.
