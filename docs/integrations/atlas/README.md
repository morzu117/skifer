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
dont les octets copiés sont gelés par hash (sans comparaison à un checkout externe).
Remplacer ces reconstructions par des sorties réelles lorsque
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
entre modèles (Nord 215, Sud 180) et l'effectif de deux snapshots (janvier 26,
février 30). Le référentiel `entites` contient une ligne par entité ; les snapshots
sont dans `effectifs`, un troisième modèle indépendant. Le test
`test_reconstructed_named_views_and_semantic_values` prouve que la jointure vers
le référentiel conserve le revenu malgré ces deux mois d'effectif.
`test_kpi_output_matches_view_bridge_and_semantic_columns` relie les colonnes du
nœud de sortie KPI à `DESCRIBE` de la vue, à la projection du pont et aux membres
du modèle. `test_combined_atlas_validation_rejects_mutated_remote_entity` prouve
le contrôle de cible par une mutation en mémoire.

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
Une constante numérique SQL doit aussi être textuelle : `sql: 1` doit s'écrire `sql: "1"`.

Le [JSON Schema sémantique publié dans le dépôt](https://github.com/morzu117/skifer/blob/main/schemas/skifer-semantic-model.schema.json)
est en draft-07, avec `$id: https://skifer.io/schemas/semantic-model/v1`.
Cet identifiant versionne le contrat ; la CLI n'effectue aucune résolution HTTP.
La règle de version est : `v1` reste `v1` tant que le schéma ne fait que s'élargir ;
tout resserrement exige `v2`.
Le schéma est **nécessaire, pas suffisant** : les références entre membres,
l'unicité de leurs noms et les règles sémantiques restent vérifiées par
`SemanticValidator`. Un type de dimension non standard produit aujourd'hui un
warning et reste accepté. Les clés inconnues restent admises là où le validateur
les tolère ; `row_policy` est fermé.

## 3. Réagréger selon les garanties réelles

Le comportement est celui de
[`SemanticPlanner._check_metric_additivity`](https://github.com/morzu117/skifer/blob/main/src/skifer/semantic/planner.py)
et de [`QueryResolver`](https://github.com/morzu117/skifer/blob/main/src/skifer/semantic/resolver.py).

| Déclaration | Comportement prouvé | Test nommé |
|---|---|---|
| `additive` | Le revenu de la vue se somme à 395 et se répartit via le référentiel sans fanout (Nord 215, Sud 180). | `test_atlas_contract.py::test_reconstructed_named_views_and_semantic_values` |
| Grain et jointure | Un parcours `one_to_many` qui multiplie le grain de la métrique est refusé. | `test_semantic_planner.py::test_one_to_many_fanout_is_refused_with_mandatory_message` |
| `semi_additive` | Le mois dans `group_by` permet la requête (26 en janvier, 30 en février) ; sans mois épinglé, refus avant SQL. | `test_atlas_contract.py::test_effectifs_two_snapshots_pinned_by_group_by`, `test_effectifs_without_pinned_month_is_refused_before_sql` |
| `non_additive` | Refus avec jointure, acceptation sans jointure. | `test_semantic_planner.py::test_non_additive_metric_is_refused_across_join`, `test_non_additive_metric_is_allowed_without_join` |

Le planner reconnaît aussi l'épinglage par `eq` :
`test_semantic_planner.py::test_semi_additive_metric_is_pinned_by_equality_filter`.
Cela ne garantit pas que toute valeur atteigne le moteur SQL : le résolveur actuel
refuse le tiret dans une valeur de filtre telle que `2026-02-01`, avant DuckDB.
`test_atlas_contract.py::test_effectifs_iso_equality_filter_currently_refused_before_sql`
épingle cette limite préexistante. La fixture exécute la sélection de février avec
`group_by=["mois_snapshot"]` et des bornes `date_from` / `date_to` explicites.

Ces tests bornent les garanties décrites ici. L’épinglage par un filtre `in` ou
par une plage seule (sans `group_by`) n’est pas testé dans cette fixture.

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

La version cible `skifer[schema]>=4.2` **n'est pas publiée**. Aujourd'hui, depuis
un clone Git de Skifer contenant ces commits, installer directement son `HEAD` :

```bash
python -m pip install "skifer[schema] @ git+file://$(git rev-parse --show-toplevel)"
skifer semantic validate --schema modeles/revenu_net.yaml modeles/entites.yaml modeles/effectifs.yaml
```

Cette commande utilise le dépôt Git local, sans attendre un tag ou une branche
publiés. Pour la CI Atlas, rendre le commit disponible dans son clone et épingler
ce SHA ; après publication, épingler la version résolue dans le lockfile.
Ce chantier ne publie aucune version et ne change pas celle du dépôt.

`semantic_catalog.yaml` contient des résumés de découverte : **ce n'est pas un
modèle**, ne pas le passer à `--schema` (éviter un glob qui l'inclurait).
`--schema` vérifie le JSON Schema par fichier puis effectue une validation commune
sur les modèles fournis. Une cible `to_model` fournie dans un autre fichier est
contrôlée, notamment son `to_entity`. Une clé dupliquée est une erreur attribuée
aux deux fichiers ; les autres erreurs sont attribuées au fichier déclarant le
modèle. Une cible absente de tous les fichiers reste acceptée, avec le warning
« cible non fournie, non vérifiée ». Tous les warnings du validateur sont affichés
sous la forme `<fichier>: warning: …`, sans changer le code de sortie.
Codes : `0` si tous sont valides, `1` sinon ; usage incorrect : `2` par argparse.
Le schéma vient du générateur inclus dans le package, sans pipeline, Spark, LLM ou
réseau. Les erreurs de schéma indiquent aussi le chemin JSON ; le fichier
`schemas/*.json` n'est pas nécessaire dans le wheel. L'usage historique reste disponible :

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
L'espace d'intégration est `docs/integrations/atlas/` et la fixture
`tests/fixtures/atlas/` ; Julien
reste propriétaire actuel, le handle de Pierre est à renseigner lorsqu'il sera
fourni. Consommer le contrat ne donne pas un accès en écriture au cœur.
