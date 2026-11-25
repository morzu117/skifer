# Intégrer Atlas à Skifer

Le contrat suit ce chemin : **graphe virtuel Atlas → vue SQL nommée → modèle
sémantique Skifer → publication certifiée**. Atlas génère ; Skifer valide et
possède le format. Une divergence de définition d'un KPI s'arbitre **dans Atlas**,
puis se repropage dans la vue et le modèle. Skifer ne propose pas d'override du sens.

## État du pont

Le générateur de modèles Skifer côté Atlas **n'existe pas encore** dans le checkout
étudié. Atlas y décrit des documents `kind: kpi`, avec un graphe de transformations.
La [fixture de contrat](https://github.com/morzu117/skifer/tree/main/tests/fixtures/atlas)
est **reconstruite** : elle montre la forme attendue du futur pont, pas une sortie
actuelle de ce générateur. Son README donne la provenance et le SHA Atlas du KPI
copié à l'identique. Remplacer ces reconstructions par des sorties réelles lorsque
le pont existera, en conservant les assertions de valeurs.

## 1. Nommer et vérifier la relation de sortie

Le document `revenu-net` décrit les factures validées, le calcul
`montant_ttc - montant_taxes - avoirs`, puis une agrégation par
`(mois_fiscal, entite)`. Exposer cette sortie dans une relation nommée :

```text
main.atlas_revenu_net(mois_fiscal, entite, revenu_net)
```

La vue doit porter les transformations du graphe, les filtres et la politique de
nulls. Le calendrier fiscal doit avoir des bornes explicites, fournies par Atlas ;
il ne se déduit pas du mot « mois ». La fixture emploie des périodes illustratives
par entité, avec début inclus et fin exclue. Le document KPI ne fournit pas les
bornes du calendrier `fiscal-nordis` : ces données de test ne prétendent pas les
reproduire. Les variations nationales de formule restent une responsabilité Atlas.

Le test crée les vues sur **DuckDB brut en mémoire**, sans `skifer-duckdb`, sans
Spark et sans LLM. Il vérifie le revenu total (395), la répartition via une relation
entre modèles (Nord 215, Sud 180) et l'effectif à un mois donné (30).

## 2. Décrire la vue dans le format Skifer

```yaml
models:
  - name: revenu_net
    key: revenu_net
    table: main.atlas_revenu_net
    dimensions:
      - {name: mois_fiscal, sql: mois_fiscal, type: date}
      - {name: entite, sql: entite, type: string}
    metrics:
      - {name: revenu_net, sql: revenu_net, type: sum, additivity: additive}
```

`sql` est une expression **non agrégée** : la vue porte déjà le premier `SUM`,
la métrique lit sa colonne, et le résolveur applique l'agrégat du modèle. Ne pas
écrire `sql: SUM(revenu_net)`. Les noms et les expressions SQL sont des chaînes ;
mettre entre guillemets les noms YAML tels que `"no"`, `"on"` ou `"yes"`.

Le [JSON Schema sémantique publié dans le dépôt](https://github.com/morzu117/skifer/blob/main/schemas/skifer-semantic-model.schema.json)
est en draft-07, avec `$id: https://skifer.io/schemas/semantic-model/v1`.
Cet identifiant versionne le contrat ; la CLI n'effectue aucune résolution HTTP.
Le schéma est **nécessaire, pas suffisant** : les références entre membres,
l'unicité de leurs noms et les règles sémantiques restent vérifiées par
`SemanticValidator`. Un type de dimension non standard produit aujourd'hui un
warning et reste accepté. Les clés inconnues restent admises là où le validateur
les tolère ; `row_policy` est fermé.

## 3. Réagréger selon les garanties réelles

Le comportement est celui de
[`SemanticPlanner._check_metric_additivity`](https://github.com/morzu117/skifer/blob/main/src/skifer/semantic/planner.py)
et de [`QueryResolver`](https://github.com/morzu117/skifer/blob/main/src/skifer/semantic/resolver.py).

| Déclaration | Contrôle effectué à la requête |
|---|---|
| `additive` (défaut) | Pas de restriction supplémentaire d'additivité. Une jointure qui multiplie le grain de la métrique reste refusée. |
| `semi_additive` | Chaque `non_additive_dimensions` doit être déclarée et présente dans `group_by` ou fixée par un filtre `eq`. Une plage ou un filtre `in` ne remplace pas cette égalité. |
| `non_additive` | Toute requête comportant une jointure est refusée. Une requête sur le seul modèle reste autorisée ; ce marqueur ne bloque pas toute réagrégation mono-table. |

Une somme de revenus par mois et entité peut se sommer à nouveau si ces groupes
sont disjoints et conservent la même définition métier. Une moyenne de moyennes,
un ratio ou un distinct count ne deviennent pas réagrégeables parce que la forme
YAML est valide. Atlas doit choisir une représentation correcte (par exemple les
composants somme et effectif) et arbitrer le KPI ; le cœur ne déduit pas cette
preuve à partir du nom des colonnes. Les déclarations de grain, d'entités et de
cardinalité documentent les garanties du producteur, elles ne remplacent pas les
contrôles de données. Le planner refuse les fanouts et les chemins ambigus ; la
validation du document seule n'exécute aucune jointure.

## 4. Valider, projeter, puis certifier

Pour la CI Atlas, à partir de la publication de la version qui contient ce contrat :

```bash
pip install 'skifer[schema]>=4.2'
skifer semantic validate --schema modeles/revenu_net.yaml modeles/entites.yaml
```

Épingler la version résolue dans le lockfile Atlas. Le minimum cible est
`skifer>=4.2` ; ce chantier ne publie pas cette version et ne modifie pas la version
du dépôt. `--schema` lit chaque fichier, applique le schéma généré par le package,
puis le validateur complet, sans pipeline, Spark, LLM ni réseau. Codes : `0` si
tous sont valides, `1` sinon. Les erreurs indiquent le fichier ; celles du schéma
indiquent aussi le chemin JSON. Le fichier `schemas/*.json` n'est pas requis dans
le wheel. L'usage historique reste disponible :

```bash
skifer semantic validate pipeline.yaml modele.yaml
```

Le pipeline de pont déclare `data_product`, `contract` et `semantic`. Dans la
fixture, il projette les trois colonnes de la vue vers une future table Delta
`gold.atlas_revenu_net`. `OutputProjector` et
`SemanticValidator.validate_against_projection` vérifient les sorties sans runtime.
Les modèles de test interrogent les vues nommées : ce test **ne certifie pas** ces
vues et ne simule pas une publication Delta.

Pour publier, exécuter le pipeline sur le runtime de publication supporté, avec
un monitor et un certification store : staging, checks, promotion ou quarantaine.
Faire pointer le modèle de consommation vers la **table physique promue** et
configurer sa politique de certification (`enforce` ou `supervised` selon le cas).
Une création de vue ou une validation statique ne constitue jamais un certificat.
Voir [publication et certification](../../semantic.md) pour le gate et ses refus.

## Responsabilités et revue

| Atlas | Skifer |
|---|---|
| Graphes KPI, variantes, calendrier et génération du pont | Format sémantique, validateur, JSON Schema versionné |
| Arbitrage des divergences de KPI et propagation des corrections | Projection, exécution gouvernée et certification |
| CI de validation des modèles générés, fixtures et assertions métier | Non-dérive du schéma et corpus de contrats |

Une PR Atlas livre ensemble le graphe, la vue nommée, le modèle et les valeurs
attendues ; sa CI appelle `skifer semantic validate --schema`. Une modification du
format exige une PR Skifer avec tests et changelog, revue par le propriétaire du
cœur. [CODEOWNERS](https://github.com/morzu117/skifer/blob/main/.github/CODEOWNERS)
protège `src/skifer/core/`, `src/skifer/semantic/`, `schemas/`, CI et packaging.
L'espace d'intégration est `docs/integrations/atlas/` et `examples/atlas/` ; Julien
reste propriétaire actuel, le handle de Pierre est à renseigner lorsqu'il sera
fourni. Consommer le contrat ne donne pas un accès en écriture au cœur.
