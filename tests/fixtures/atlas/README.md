# Contrat Atlas — fixture reconstruite

Cette fixture est **reconstruite**. Le checkout Atlas étudié ne contient pas de
pont générant des modèles sémantiques Skifer ; il décrit des documents KPI
`kind: kpi` sous forme de graphes. Les modèles et les vues ci-dessous représentent
la forme que le futur pont devrait émettre. Les remplacer par ses sorties réelles
quand il existera, en conservant les tests de valeurs.

| Fichier | Provenance et rôle |
|---|---|
| `kpi/revenu-net.yaml` | Copie de référence du dépôt skifer-atlas, `core/tests/fixtures/documents/revenu-net.yaml`, commit `b6af79908409ff998499925779249b0ac5d321d1`. Le hash gèle les octets de cette copie ; le test ne compare pas un checkout Atlas externe. Document jamais interprété comme pipeline Skifer. |
| `views/revenu_net.sql` | Reconstruction manuelle des nœuds n01 à n05 : source, factures validées, déduction taxes/avoirs, périodes fiscales déclarées, somme par mois/entité. |
| `views/entites.sql` | Vue de référentiel illustrative pour exercer la jointure ; aucune prétention de provenance KPI Atlas. |
| `models/revenu_net.yaml` | Modèle reconstruit sur `main.atlas_revenu_net`, avec grain composite et relation `many_to_one` vers `entites`. |
| `models/entites.yaml` | Référentiel pur sur `main.atlas_entites`, une ligne par entité, sans snapshot. |
| `models/effectifs.yaml`, `views/effectifs.sql` | Snapshots synthétiques séparés, métrique d'effectif `semi_additive` par mois. |
| `models/semantic_catalog.yaml` | Catalogue reconstruit, limité aux résumés nécessaires à la découverte paresseuse. |
| `bridge.yaml` | Pipeline reconstruit : projection de la vue, contrat et déclaration sémantique, cible Delta `gold.atlas_revenu_net`. |
| `../../test_atlas_contract.py` | Données synthétiques et assertions de projection, validation et valeurs sur DuckDB brut en mémoire. |

Les périodes fiscales sont des **données illustratives explicites** par entité
(début inclus, fin exclue). Le KPI nomme `fiscal-nordis` version `2026.1` mais ne
fournit pas ses bornes ; la fixture n'invente pas une définition de ce calendrier.
Elle fournit seulement les bornes nécessaires à son propre jeu de données.
L'effectif et les segments (Nord/Sud) sont également synthétiques. Le référentiel
contient exactement une ligne par entité ; les deux mois de snapshots vivent dans
`effectifs`, pour ne pas multiplier le revenu lors de la jointure.

Attendus : revenu janvier A = 140, janvier B = 180, février A = 75 ; total = 395 ;
par relation vers le segment, Nord = 215 et Sud = 180 ; effectif janvier = 26 et février = 30.
Les factures annulées et de statut nul sont exclues. Une requête d'effectif sans
mois fixé est refusée avant exécution.

Les modèles de test lisent les **vues**. Le pipeline projette leurs colonnes mais
n'est pas publié dans ce test. La validation contre la projection ne certifie
pas une relation et ne prouve pas son identité physique. Après publication réelle,
le modèle de consommation doit viser la table promue et son certificat.

```bash
skifer semantic validate --schema tests/fixtures/atlas/models/revenu_net.yaml \
  tests/fixtures/atlas/models/entites.yaml tests/fixtures/atlas/models/effectifs.yaml
pytest tests/test_atlas_contract.py -q
```

DuckDB est fourni par `[dev]` ; aucun import de `skifer-duckdb`, aucune session
Spark, aucun appel LLM et aucune connexion réseau ne sont nécessaires. Comme les
tests DuckDB existants du dépôt, ces tests n'ont pas de marqueur Spark et ne
demandent pas la fixture `spark`.

`semantic_catalog.yaml` est un catalogue de découverte, pas un modèle : ne pas le
passer à `--schema`. La CLI contrôle les cibles de relations entre tous les fichiers
fournis, refuse les clés dupliquées et affiche les warnings, y compris les cibles
non fournies et donc non vérifiées.

`test_kpi_output_matches_view_bridge_and_semantic_columns` compare la sortie du
nœud KPI (`group_by` et alias de mesures), les colonnes réelles via `DESCRIBE`, la
projection du pont et les dimensions/métriques de `revenu_net`.
`test_kpi_reference_copy_is_frozen_by_hash` garantit seulement le gel de la copie.

Limite préexistante épinglée par
`test_effectifs_iso_equality_filter_currently_refused_before_sql` : une date ISO
avec tirets dans un filtre `eq` est refusée par le résolveur avant DuckDB.
Le mois de février est exécuté avec `group_by` et les bornes de date dédiées ;
`test_effectifs_two_snapshots_pinned_by_group_by` en vérifie la valeur 30.
