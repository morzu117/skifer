# Business Rules — Contract and Migration Guide

## Vue d'ensemble

Les Business Rules Skifer sont des fonctions Python décorées avec
`@RuleRegistry.register_rule()`.  Depuis le **Plan 13**, chaque règle porte un
**`kind`** explicite qui détermine son contrat de signature et son comportement
d'exécution.

---

## Kinds de règles

| Kind | Signature de retour | Comportement moteur |
|---|---|---|
| `projection` (**défaut**) | `dict[str, Column]` | Toutes les règles consécutives sont **fusionnées en un seul `select()`**. Tri topologique automatique des dépendances. |
| `aggregation` | `(list[str], dict[str, Column])` ou `DataFrame` | Règles partageant les mêmes clés `groupBy` fusionnées en un seul `groupBy(...).agg(...)`. |
| `transform` | `DataFrame` | Exécution séquentielle, contrat hérité `df → df`. Échappatoire pour la logique complexe. |

---

## `kind="projection"` (recommandé)

La règle retourne un dictionnaire `{nom_colonne: expression_Column}`.

```python
from pyspark.sql import functions as F
from skifer import RuleRegistry

@RuleRegistry.register_rule(kind="projection")  # kind="projection" est le défaut
def flag_high_value(df):
    return {
        "is_high_value": F.when(F.col("amount") >= 1000, 1).otherwise(0),
    }

@RuleRegistry.register_rule(kind="projection")
def flag_vip(df):
    return {
        "is_vip": F.when(F.col("amount") >= 5000, 1).otherwise(0),
        "vip_label": F.when(F.col("is_vip") == 1, F.lit("VIP")).otherwise(F.lit("Standard")),
    }
```

> **Fusion automatique** : si `flag_high_value` et `flag_vip` apparaissent
> consécutivement dans le YAML, le moteur émet un seul `select()` avec toutes
> les colonnes — aucun surcoût O(N²) Catalyst.

### Tri topologique

Si `flag_vip` lit `is_vip` écrit par `flag_high_value`, le planificateur
détecte la dépendance et garantit l'ordre correct, même si l'ordre YAML est
inversé.  Un log `[Planner] Reordered rules: ...` est émis.

---

## `kind="aggregation"`

### Option A — contrat déclaratif (recommandé)

La règle retourne `(group_keys, agg_exprs)` et déclare `agg_keys` sur la
fonction pour permettre la fusion inter-règles :

```python
@RuleRegistry.register_rule(kind="aggregation")
def revenue_by_region(df):
    return (
        ["region"],
        {"total_revenue": F.sum("amount"), "order_count": F.count("*")},
    )

revenue_by_region.agg_keys = ["region"]  # nécessaire pour la fusion

@RuleRegistry.register_rule(kind="aggregation")
def avg_by_region(df):
    return (
        ["region"],
        {"avg_revenue": F.avg("amount")},
    )

avg_by_region.agg_keys = ["region"]
```

Avec les mêmes clés `["region"]`, le moteur produit un seul shuffle :

```sql
SELECT region, SUM(amount) AS total_revenue,
       COUNT(*) AS order_count,
       AVG(amount) AS avg_revenue
FROM ...
GROUP BY region
```

### Option B — contrat DataFrame (fallback)

La règle retourne directement un `DataFrame` (pratique pour les pivots,
multi-niveaux, etc.) :

```python
@RuleRegistry.register_rule(kind="aggregation")
def pivot_by_category(df):
    return df.groupBy("region").pivot("category").agg(F.sum("amount"))
```

---

## `kind="transform"` — échappatoire

Pour toute logique qui ne peut pas s'exprimer comme une projection ou une
agrégation simple (filtres conditionnels, fenêtres, joins, multi-passes) :

```python
@RuleRegistry.register_rule(kind="transform")
def complex_dedup_logic(df):
    from pyspark.sql import Window
    w = Window.partitionBy("customer_id").orderBy(F.col("created_at").desc())
    return df.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")
```

---

## Utilisation dans le YAML

Le `kind` est transparent pour l'utilisateur du YAML — les règles sont listées
comme avant :

```yaml
business_rules:
  - flag_high_value
  - flag_vip
  - revenue_by_region
  - complex_dedup_logic
```

`examples/05_rules_join_aggregate/` is the runnable artifact for this boundary:
the YAML names a registered `classify_order` projection rule, and the script
prints its `priority`/`standard` output after a join, followed by a declarative
aggregate filtered with `having`.

---

## Désactiver la fusion (debug)

```python
engine.run_process_to_table(schema, fuse_rules=False)
```

Ou via `process_schema` si vous appelez l'API bas niveau directement.

---

## Lints performance (`explain_rules`)

`examples/19_rule_optimizer/` runs this report on rules written to earn their warnings: a second
rule that overwrites a column a first one produced, and three rules reading the same column. It
prints both entry points, because they differ in cost — `RuleAnalyzer` needs no Spark and is what a
CI check would call, while `engine.explain_rules()` starts a local session in the engine
constructor even though the analysis executes nothing.

```python
from skifer import RuleAnalyzer

analyzer = RuleAnalyzer()
profiles = analyzer.analyze_rules(["flag_high_value", "revenue_by_region"])
warnings = analyzer.detect_warnings(profiles)
analyzer.print_report(profiles, warnings)
```

### Codes de warning

| Code | Niveau | Description |
|---|---|---|
| `OVERWRITE` | warning | Plusieurs règles écrivent la même colonne |
| `SHARED_READ` | info | Plusieurs règles lisent la même colonne (possible déduplication) |
| `DUPLICATE_EXPR` | warning | Même expression dans plusieurs règles |
| `PHOTON_BREAKING` | warning | UDF Python détecté (`@udf` / `@pandas_udf`) — désactive l'accélération Photon |
| `COMPLEXITY_HIGH` | info | Règle trop longue (>30 lignes) ou trop de `withColumn` (>5) — suggère de découper |

---

## Guide de migration depuis l'ancien contrat

### Avant (Plan ≤ 12)

```python
@RuleRegistry.register_rule()
def flag_high_value(df):
    return df.withColumn("is_high_value", F.when(F.col("amount") >= 1000, 1).otherwise(0))
```

### Après (Plan 13+)

```python
@RuleRegistry.register_rule(kind="projection")  # ou simplement @RuleRegistry.register_rule()
def flag_high_value(df):
    return {"is_high_value": F.when(F.col("amount") >= 1000, 1).otherwise(0)}
```

### Migration impossible → `kind="transform"`

Si la règle contient des filtres, des joins ou de la logique multi-passes,
taggez-la `kind="transform"` sans modifier la signature :

```python
@RuleRegistry.register_rule(kind="transform")   # ← seul changement
def legacy_complex_rule(df):
    return df.filter(...).withColumn(...)         # ← inchangé
```

Elle sera exécutée séquentiellement, sans bénéficier de la fusion, mais sans
rien casser.

---

## Cycles de dépendances

Si deux règles `projection` du même étage ont une dépendance circulaire (A
écrit `x` lu par B, et B écrit `y` lu par A), le moteur lève une
`RuleCycleError`.  Solutions :

1. Séparer les deux règles dans des étages distincts avec une règle `transform`
   intermédiaire.
2. Fusionner A et B en une seule règle `projection`.
3. Utiliser `kind="transform"` pour l'une des deux.
