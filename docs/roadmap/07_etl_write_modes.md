# Roadmap 07 — Modes d'écriture ETL (Write Modes)

## Contexte

Aujourd'hui, `SkiferEngine` écrit toujours en mode **replace-all** : la table cible est
supprimée puis recréée à chaque exécution (`overwrite`). C'est simple et idempotent, mais
coûteux : sur des tables volumineuses, on retraite et réécrit 100 % des données même quand
seule une fraction a changé.

L'objectif est d'introduire un **mode `update`** (merge/upsert) qui :

1. **Insère** les nouvelles lignes absentes de la table cible.
2. **Met à jour** les lignes existantes dont au moins un champ a changé.
3. **Optionnellement supprime** les lignes de la cible qui n'existent plus dans la source
   (mode `sync`).

---

## Modes cibles

| Mode YAML | Comportement | Équivalent Delta |
|---|---|---|
| `overwrite` *(défaut actuel)* | Recrée la table entièrement | `CREATE OR REPLACE TABLE … AS SELECT` |
| `append` | Ajoute sans déduplication | `INSERT INTO` |
| `update` | Upsert : insert + update des lignes modifiées | `MERGE INTO … WHEN MATCHED … WHEN NOT MATCHED` |
| `sync` | Upsert + suppression des lignes disparues | `MERGE INTO … WHEN NOT MATCHED BY SOURCE THEN DELETE` |

---

## Syntaxe YAML proposée

```yaml
write_mode: update          # overwrite | append | update | sync

merge_keys:                 # obligatoire pour update / sync
  - order_id

# Optionnel — colonnes exclues de la comparaison pour décider si une ligne a changé.
# Par défaut, toutes les colonnes hors merge_keys sont comparées.
ignore_on_compare:
  - updated_at
  - etl_load_ts
```

Exemple complet :

```yaml
tables:
  - name: "{{ catalog }}.bronze.orders_raw"
    alias: ord

select_final:
  - [order_id]
  - [amount]
  - [status]

write_mode: update
merge_keys:
  - order_id
ignore_on_compare:
  - etl_load_ts
```

---

## Design technique

### Clé de merge
- `merge_keys` est la liste des colonnes qui identifient une ligne de façon unique (PK logique).
- Sans `merge_keys`, le mode `update` ou `sync` lève une `SchemaValidationError` explicite.

### Condition de mise à jour
- Générer automatiquement une clause `WHEN MATCHED AND (col1 <> col2 OR col3 <> col4 …)` sur
  toutes les colonnes hors `merge_keys` et `ignore_on_compare`.
- Éviter les updates no-op qui génèrent du journal Delta inutile.

### Implémentation Delta Lake (Spark)
```python
from delta.tables import DeltaTable

delta_tbl = DeltaTable.forName(spark, fqn)
(
    delta_tbl.alias("target")
    .merge(df.alias("source"), merge_condition)
    .whenMatchedUpdateAll(condition=update_condition)
    .whenNotMatchedInsertAll()
    .execute()
)
```

### Backend multi-plateforme (lien roadmap 01)
- **Spark / Delta** : `MERGE INTO` natif — implémentation principale.
- **Databricks SQL** : même syntaxe, disponible nativement.
- **BigQuery** (roadmap 01) : `MERGE INTO` SQL standard.
- **Snowflake** (roadmap 01) : `MERGE INTO` SQL standard.
- **Local (non-Delta)** : fallback `overwrite` avec avertissement si Delta non dispo.

### Création initiale
Si la table cible n'existe pas encore, le mode `update` se rabat automatiquement sur
`overwrite` pour la première exécution (comportement standard d'un premier load).

---

## Fichiers à créer / modifier

| Fichier | Changement |
|---|---|
| `src/skifer/core/schema_loader.py` | Valider `write_mode` et `merge_keys` à la lecture du YAML |
| `src/skifer/core/core.py` | Lire `write_mode` dans le schema, router vers `_write_dataframe` ou `_merge_dataframe` |
| `src/skifer/core/core.py` | Ajouter `_merge_dataframe(df, fqn, merge_keys, ignore_on_compare)` |
| `src/skifer/core/loaders.py` | Adapter si des loaders ont une logique d'écriture propre |
| `tests/test_core.py` | Tests unitaires : overwrite (actuel), append, update (mock MERGE), sync |
| `CHANGELOG.md` | Entrée `[Unreleased]` |
| `CLAUDE.md` | Ajouter `write_mode` dans la section *YAML schema patterns* |

---

## Risques & points d'attention

- **Schema evolution** : si la source ajoute une colonne, le `MERGE` peut échouer. Envisager
  `mergeSchema=true` sur le writer Delta ou une vérification préalable.
- **Performance** : la clause `WHEN MATCHED AND (…)` peut être lourde sur des tables larges.
  Documenter que `ignore_on_compare` est là pour alléger cette comparaison.
- **Null safety** : `col1 <> col2` est faux si l'un est NULL. Utiliser
  `NOT (col1 <=> col2)` (null-safe equals en Spark SQL) ou équivalent.
- **Split mode** (`run_process_to_split_tables`) : à gérer séparément, chaque slice pouvant
  avoir sa propre stratégie de merge.

---

## Statut

| Étape | Statut |
|---|---|
| Rédaction roadmap | ✅ Fait |
| Validation design | En attente |
| Implémentation | Non démarré |
