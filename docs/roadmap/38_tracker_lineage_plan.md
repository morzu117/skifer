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

### 3. Un pipeline d'agrégation n'avait aucun lineage — **corrigé le 20 septembre 2026**

```yaml
tables:
  - {name: silver.orders, alias: o}
aggregate:
  group_by: [country]
  measures:
    - [amount, total, sum]
```

**Zéro arête.** Ni `country -> country`, ni `amount -> total`. Le bloc `aggregate:` du Plan 28
n'avait jamais été câblé dans le tracker, alors que c'est la forme la plus courante d'une table
Gold. Un dictionnaire ou une analyse d'impact sur une table agrégée était donc vide — et vide se
lit comme « aucune dépendance », pas comme « non analysé ».

> **Livré.** C'était le seul des quatre points sans ambiguïté de conception : `group_by` et
> `measures` nomment explicitement leurs sources, il n'y avait rien à deviner. Une clé de groupe
> porte une arête `select` marquée `group_by` ; une mesure porte une arête `metric` marquée de sa
> fonction. Deux précisions qui comptent :
>
> - **`count:*` part de la source, pas de la cible.** Les lignes comptées sont celles de la source.
>   L'arête est émise malgré l'absence de colonne source, sans quoi la colonne de sortie
>   disparaîtrait du dictionnaire — et une colonne absente se lit « aucune dépendance ».
> - **Une clé de groupe issue d'`add_columns` n'est pas réattribuée à la table source.**
>   `add_columns` s'applique avant l'agrégat, donc la clé peut être dérivée ; émettre
>   `silver.orders.amount_bucket` nommerait une colonne introuvable. Son arête `add_columns`
>   porte déjà la vraie origine.
>
> Les trois autres points restent ouverts : ils demandent de décider comment un doute se
> représente, si le tracker peut lire un schéma, et comment séparer la condition de jointure du
> flux.

### 4. Seules les règles que l'analyse statique ne perce pas sont opaques

**Correction du 20 septembre 2026.** Cette section affirmait qu'un pipeline dont les colonnes
viennent de `business_rules` produit *zéro* arête. C'est faux, et `examples/11_lineage_and_dictionary`
— cité à l'appui — démontre exactement l'inverse : il imprime `from raw_orders.amount [rule] via
rule:classify_order`. Depuis le Plan 34, `RuleAnalyzer` lit les sorties des règles `projection`
comme des `transform`, et le tracker en émet des arêtes.

Ce qui reste opaque est plus étroit : une règle dont l'analyse statique ne détecte **aucune**
sortie (`profile.source_available` faux, ou aucune colonne trouvée). L'exemple l'affiche sous
`opaque_rule declares outputs: <none detected>`. Là, le silence est un choix affiché.

### 5. Une règle renommée par `select_final` perdait sa provenance — **corrigé le 20 septembre 2026**

Une règle nomme sa colonne ; `select_final` décide de ce qui est publié. Quand les deux noms
diffèrent, l'arête de règle portait le nom **interne** — une colonne que la cible n'a pas — et la
colonne réellement publiée se retrouvait sans provenance. Mesuré de bout en bout :

```text
silver.contacts.email  (pii)  --règle mask_email-->  email_masked
                                select_final: [email_masked, hashed_contact]

avant :  skifer index --strict  ->  aucune violation, hashed_contact = None
après :  skifer index --strict  ->  VIOLATION sur 'hashed_contact'
```

C'est la même famille que le point 3 : l'arête manquante ne propage rien, et `mode="strict"` n'a
rien à refuser. Le cas identité (`[order_class, order_class]`) et `keep_all_columns` étaient déjà
corrects, ce qui explique que le défaut ait survécu — l'exemple 11 et le test du tracker couvraient
tous deux le cas identité.

Corollaire livré dans le même correctif : une colonne de règle que `select_final` ne mentionne
jamais n'émet plus d'arête vers la cible. Elle est calculée puis jetée ; l'annoncer inventait une
colonne pour laquelle `skifer lineage` répondait et que le dictionnaire listait.

**Reste ouvert, mesuré au passage.** Avec `keep_all_columns: true`, `OutputProjector` ne rend
**aucune** colonne — il ne peut pas les connaître sans lire un schéma. L'enregistrement indexé
annonce donc un dataset sans colonnes, et l'héritage de classification, qui itère sur
`record.columns`, n'examine rien. C'est le point 2 de la section suivante, pas celui-ci.

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
4. ~~**Câbler `aggregate:`.**~~ **Livré le 20 septembre 2026** — voir le point 3 ci-dessus.
5. ~~**Le nom publié d'une colonne de règle.**~~ **Livré le 20 septembre 2026** — voir le
   point 5 ci-dessus. Aucune ambiguïté de conception : la colonne publiée est celle que le
   pipeline écrit, il n'y avait rien à arbitrer.

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

## Le point le plus grave : la classification n'était pas propagée sur un agrégat — **fermé**

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

Un contrôle ne peut pas attraper ce qui ne lui est jamais présenté. C'est ce qui a fait passer le
câblage d'`aggregate:` du rang d'amélioration à celui de correctif — et ce qui l'a fait livrer
seul, sans attendre la conception des trois autres points.

**Depuis le correctif**, la même mesure donne `{'contacts': 'pii'}` en `warn` (avec
`ClassificationPropagationWarning`) et une `ClassificationViolationError` en `strict`.

**Correction d'une affirmation trop forte.** J'avais d'abord écrit qu'une arête attribuée à la
mauvaise table propage une classification à la mauvaise colonne. C'est **faux** :
`resolve_field_classifications` indexe `source_classifications` par **nom de colonne seul**, sans
la table, donc une mauvaise attribution de table ne change pas le niveau propagé. Le défaut du
point 1 est ailleurs — il tronque la chaîne amont lors d'une traversée multi-pipelines, puisque
`upstream` suit le couple (table, colonne). Mesurer avant d'écrire vaut aussi pour les documents
de constat.
