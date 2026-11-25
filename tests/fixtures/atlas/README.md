# Contrat Atlas — fixture reconstruite

Cette fixture est **reconstruite**. Le checkout Atlas étudié ne contient pas de
pont générant des modèles sémantiques Skifer ; il décrit des documents KPI
`kind: kpi` sous forme de graphes. Les modèles et les vues ci-dessous représentent
la forme que le futur pont devrait émettre. Les remplacer par ses sorties réelles
quand il existera, en conservant les tests de valeurs.

| Fichier | Provenance et rôle |
|---|---|
| `kpi/revenu-net.yaml` | Copie byte-for-byte de `/Users/jhoubart/Projects/skifer-atlas/core/tests/fixtures/documents/revenu-net.yaml`, checkout Atlas `b6af79908409ff998499925779249b0ac5d321d1`. Document de référence, jamais interprété par Skifer. |
| `views/revenu_net.sql` | Reconstruction manuelle des nœuds n01 à n05 : source, factures validées, déduction taxes/avoirs, périodes fiscales déclarées, somme par mois/entité. |
| `views/entites.sql` | Vue de référentiel illustrative pour exercer la jointure ; aucune prétention de provenance KPI Atlas. |
| `models/revenu_net.yaml` | Modèle mono-vue simple reconstruit sur `main.atlas_revenu_net`. |
| `models/revenu_par_entite.yaml` | Variante reconstruite avec grain composite, entités et relation `many_to_one` vers `entites`. |
| `models/entites.yaml` | Modèle reconstruit sur `main.atlas_entites`, métrique d'effectif `semi_additive` par mois de snapshot. |
| `models/semantic_catalog.yaml` | Catalogue reconstruit, limité aux résumés nécessaires à la découverte paresseuse. |
| `bridge.yaml` | Pipeline reconstruit : projection de la vue, contrat et déclaration sémantique, cible Delta `gold.atlas_revenu_net`. |
| `../../test_atlas_contract.py` | Données synthétiques et assertions de projection, validation et valeurs sur DuckDB brut en mémoire. |

Les périodes fiscales sont des **données illustratives explicites** par entité
(début inclus, fin exclue). Le KPI nomme `fiscal-nordis` version `2026.1` mais ne
fournit pas ses bornes ; la fixture n'invente pas une définition de ce calendrier.
Elle fournit seulement les bornes nécessaires à son propre jeu de données.
L'effectif et les segments sont également synthétiques.

Attendus : revenu janvier A = 140, janvier B = 180, février A = 75 ; total = 395 ;
par relation vers le segment, Nord = 215 et Sud = 180 ; effectif février = 30.
Les factures annulées et de statut nul sont exclues. Une requête d'effectif sans
mois fixé est refusée avant exécution.

Les modèles de test lisent les **vues**. Le pipeline projette leurs colonnes mais
n'est pas publié dans ce test. La validation contre la projection ne certifie
pas une relation et ne prouve pas son identité physique. Après publication réelle,
le modèle de consommation doit viser la table promue et son certificat.

```bash
skifer semantic validate --schema tests/fixtures/atlas/models/revenu_net.yaml \
  tests/fixtures/atlas/models/revenu_par_entite.yaml tests/fixtures/atlas/models/entites.yaml
pytest tests/test_atlas_contract.py -q
```

DuckDB est fourni par `[dev]` ; aucun import de `skifer-duckdb`, aucune session
Spark, aucun appel LLM et aucune connexion réseau ne sont nécessaires. Comme les
tests DuckDB existants du dépôt, ces tests n'ont pas de marqueur Spark et ne
demandent pas la fixture `spark`.
