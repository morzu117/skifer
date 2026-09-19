# Plan 38 — Lineage du tracker : agrégats, jointures, intermédiaires

> **Ceci n'est pas encore un plan.** `CLAUDE.md` référençait ce document comme « plan finalisé,
> non démarré » depuis un moment, mais il n'a jamais existé : aucun fichier, aucune branche,
> aucun commit dans l'historique. Le lien était mort.
>
> Ce document contient ce qui manquait pour en écrire un : **le comportement actuel, mesuré**.
> La conception reste à faire, en mode plan et après validation — c'est le processus décrit dans
> `CLAUDE.md`, et il n'y a aucune raison de le court-circuiter ici.
>
> *Constats établis le 19 septembre 2026, en marge du Plan 39.*

## Pourquoi c'est tombé maintenant

La tranche 39.6.2 devait construire un graphe inter-pipelines « depuis l'index Plan 31 ». Le
réflexe était de le dériver du lineage colonne déjà stocké dans chaque `DatasetRecord`. La mesure
l'a interdit — et c'est cette mesure qui documente le tracker.

## Ce que le tracker fait aujourd'hui, mesuré

### 1. Une colonne non qualifiée est attribuée à la table de base

```yaml
tables:
  - {name: silver.orders,    alias: o}
  - {name: silver.customers, alias: c}
join:
  - {table_from: [o, customer_id], table_to: [c, customer_id], type: left}
select_final:
  - [amount, amount]
  - [customer_name, customer_name]
```

Arêtes produites :

```
select  silver.orders.amount        -> gold.fact.amount
select  silver.orders.customer_name -> gold.fact.customer_name     ← faux
join    silver.orders.customer_id   -> silver.customers.customer_id
```

`customer_name` vient de `silver.customers`. Le tracker l'attribue à la table de base parce
qu'aucune information de schéma ne lui dit à qui appartient un nom non qualifié — il ne devine
pas au hasard, il applique une règle par défaut. Mais **le résultat est affirmé sans réserve** :
rien dans le graphe ne signale que cette arête est une hypothèse.

C'est la distinction qui compte pour la conception à venir : le problème n'est pas que le
tracker ignore la réponse, c'est qu'il ne dit pas qu'il l'ignore. Le reste du framework a une
position constante là-dessus — `OutputProjector` marque `needs_curation` plutôt que de coercer
un type, `SemanticPlanner` refuse deux chemins minimaux plutôt que d'en choisir un. Le tracker
est le seul endroit qui tranche en silence.

### 2. Une jointure relie les deux entrées entre elles

L'arête `join` va de `silver.orders` vers `silver.customers`. Lue comme une dépendance, elle dit
que `silver.orders` **alimente** `silver.customers`, ce qui est faux : le pipeline lit les deux.
C'est un modèle de la *condition* de jointure, pas du flux de données, et les deux vivent dans le
même graphe sans que le type d'arête suffise à les distinguer à l'usage.

**Conséquence concrète et vérifiée** : construire le graphe inter-pipelines depuis ce lineage
aurait produit « `gold.fact` ne dépend que de `silver.orders` » et « `silver.orders` alimente
`silver.customers` ». Dans `skifer run --select`, ces deux erreurs décident de l'ordre
d'exécution. La tranche 39.6.2 dérive donc ses arêtes des `tables:` déclarées de l'IR, pas d'ici.

### 3. Un pipeline d'agrégation n'a aucun lineage

```yaml
tables:
  - {name: silver.orders, alias: o}
aggregate:
  group_by: [country]
  measures:
    - [amount, total, sum]
```

**Zéro arête.** Ni `country -> country`, ni `amount -> total`. Le bloc `aggregate:` du Plan 28
n'a jamais été câblé dans le tracker, alors que c'est la forme la plus courante d'une table Gold.
Un dictionnaire ou une analyse d'impact sur une table agrégée est donc vide — et vide se lit
comme « aucune dépendance », pas comme « non analysé ».

### 4. Les règles sont opaques — connu et assumé

Un pipeline dont les colonnes viennent de `business_rules` produit zéro arête. C'est
**intentionnel** et déjà documenté (`examples/11_lineage_and_dictionary` en fait sa démonstration
explicite). À distinguer des trois points ci-dessus : ici le silence est un choix affiché.

## Ce qu'un plan devra trancher

1. **Comment un doute se représente.** Une arête « probable » et une arête sûre ne peuvent pas
   se ressembler. Faut-il un champ de confiance sur `LineageEdge`, ou l'absence d'arête plus un
   signalement séparé ? Le reste du framework penche pour « refuser/marquer », pas pour « pondérer ».
2. **Si le tracker doit pouvoir lire un schéma.** Résoudre `customer_name` demande de savoir quelles
   colonnes chaque table possède — donc un catalogue, donc une connexion. Le tracker est
   aujourd'hui Spark-free et sans connexion, ce qui est une qualité : il tourne dans
   `skifer index`, en CI, sans entrepôt. Un résolveur **injecté et optionnel** préserverait ça.
3. **Séparer la condition de jointure du flux.** Soit deux types d'arêtes clairement distincts,
   soit deux graphes.
4. **Câbler `aggregate:`.** Le moins ambigu des quatre points : `group_by` et `measures` nomment
   explicitement leurs sources, il n'y a rien à deviner.

## Rayon d'impact

Établi par lecture directe — le serveur `codegraph` n'a pas démarré sur cette machine, donc ce
périmètre est un plancher, pas une garantie.

| Fichier | Rôle |
|---|---|
| `src/skifer/lineage/tracker.py` | `LineageTracker.from_schema`, `LineageGraph`, `LineageEdge` |
| `src/skifer/lineage/renderer.py` | rendu Mermaid/JSON — doit savoir montrer un doute |
| `src/skifer/lineage/dictionary.py` | consomme les `sources` de colonnes |
| `src/skifer/observability/metadata_index.py` | `index_schema` stocke `graph.to_dict()` |
| `src/skifer/observability/metadata_store.py` | `MetadataRegistryQuery.upstream/downstream/impact` |
| `src/skifer/lineage/classification.py` | propage la classification **le long de ces arêtes** — une arête absente ne propage rien, y compris en `mode="strict"` (voir plus bas) |
| `examples/11_lineage_and_dictionary/` | livrable exécuté par la suite ; changer le comportement change l'exemple |

## Le point le plus grave : la classification n'est pas propagée sur un agrégat

La propagation `public→pii` du Plan 31 suit ces arêtes. L'absence d'arête sur un `aggregate:`
n'est donc pas seulement une lacune de documentation — c'est un **fail-open de gouvernance**.

La même colonne source, deux chemins, mesurés avec `source_classifications={"email": "pii"}` :

```python
# select_final: [[email, contacts]]
{'contacts': 'pii'}          # + ClassificationPropagationWarning

# aggregate: measures: [[email, contacts, first]]
{}                           # rien. Et mode="strict" passe aussi.
```

`first` est une fonction d'agrégation supportée (`AGGREGATE_FUNCTIONS`), et elle reporte la
**valeur** telle quelle. Une colonne `pii` traversant une table Gold par agrégation perd donc
toute classification, et `mode="strict"` — le réglage dont c'est exactement le rôle — ne peut
rien y voir : il rejette les élévations *inférées non déclarées*, et il n'y a aucune inférence à
rejeter quand le tracker n'a rien émis.

Un contrôle ne peut pas attraper ce qui ne lui est jamais présenté. C'est ce qui fait passer le
câblage d'`aggregate:` du rang d'amélioration à celui de correctif.

**Correction d'une affirmation trop forte.** J'avais d'abord écrit qu'une arête attribuée à la
mauvaise table propage une classification à la mauvaise colonne. C'est **faux** :
`resolve_field_classifications` indexe `source_classifications` par **nom de colonne seul**, sans
la table, donc une mauvaise attribution de table ne change pas le niveau propagé. Le défaut du
point 1 est ailleurs — il tronque la chaîne amont lors d'une traversée multi-pipelines, puisque
`upstream` suit le couple (table, colonne). Mesurer avant d'écrire vaut aussi pour les documents
de constat.
