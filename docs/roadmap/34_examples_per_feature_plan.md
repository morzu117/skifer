# Plan 34 — Un exemple parlant pour chaque feature

> **Demandé le :** 9 septembre 2026, après le Plan 33
> **Objectif :** que chaque feature du framework ait un artefact exécutable qui la montre
> **Branche :** `docs/examples-per-feature`
> **Statut :** **implémenté** — phases 0 à 6, 18 exemples, tous exécutés par la suite

## 0. Résultat

Dix-huit exemples livrés, la totalité du jeu cible. Trois défauts du code livré en sont sortis :
l'arité d'opération que le catalogue déclarait sans que rien ne l'applique, les workers Spark
locaux lancés sur un autre interpréteur que le venv, et une lineage qui nommait comme source une
colonne qu'aucune table ne contient. Deux écarts au plan, tous deux documentés en section 4 et
dans les commits : le partage d'un processus entre exemples a été écarté, et deux exemples sont
passés hors Spark. Coût mesuré sur la suite : 158 s avant, 260 s après.

## 1. Le constat, mesuré

Le Plan 33 a livré quatre exemples exécutés par la suite. Ils forment une colonne vertébrale
d'onboarding cohérente, mais ils couvrent une fraction étroite de la surface du framework.

Occurrences dans `examples/`, vérifiées :

| Bloc YAML | Exemple qui le montre |
|---|---|
| `filter`, `quality_checks`, `select_final`, `source` | 01 |
| `data_product`, `contract` | 02 |
| modèle sémantique, preuve | 03 |
| capacité gouvernée | 04 |
| `business_rules` | **aucun** |
| `join` | **aucun** (le mot n'apparaît que dans une phrase de README) |
| `aggregate`, `having` | **aucun** |
| `partials` | **aucun** |
| `materialization` (MV, streaming) | **aucun** |
| `add_columns`, `filter_groups`, `dev_limit`, loaders | **aucun** |
| lineage, dictionnaire | **aucun** |
| tracing | **aucun** |
| projection sémantique, drafts, `sync` | **aucun** |
| graphe de domaine, calendriers | **aucun** |
| agent GenBI | **aucun** |
| exposition MCP | **aucun** |
| adaptive Gold | **aucun** |

Le manque le plus structurant est le premier. Le principe de conception affiché est la séparation
du **quoi** (YAML) et du **comment** (règles Python). Le **comment** n'a aujourd'hui aucun artefact
exécutable : ni règle enregistrée, ni `@RuleRegistry.register_rule`. Un lecteur parcourt tout le
chemin d'appropriation sans jamais voir la moitié Python du modèle, qui est pourtant la première
chose qu'il écrira.

## 2. Ce qu'« exemple parlant » veut dire ici

Un exemple est retenu s'il satisfait les cinq critères. Un exemple qui n'en satisfait que quatre
n'est pas livré : il devient de l'illustration, et l'illustration périme sans que personne ne le voie.

1. **Il s'exécute localement** — pas de cluster, pas de clé d'API, pas de réseau.
2. **Il est exécuté par la suite de tests.** Un exemple cassé casse le build.
3. **Il imprime un résultat qui montre le point**, pas seulement « OK ».
4. **Il montre au moins un refus** quand la feature a une garde. Une garde qu'on ne voit jamais
   refuser n'est pas comprise comme une garde.
5. **Son README dit trois choses** : ce qu'il montre, la sortie exacte attendue, et la phrase à
   retenir.

## 3. Le jeu d'exemples cible

Dix-huit exemples, dont quatre existent. Colonne « Spark » : l'exemple démarre-t-il une session.

| # | Exemple | Features couvertes | Spark |
|---|---|---|---|
| 01 | `first_pipeline` *(existe)* | source, `filter`, `quality_checks`, `select_final` | oui |
| 02 | `quality_and_contract` *(existe)* | `data_product`, `contract`, publication certifiée | oui |
| 03 | `semantic_and_question` *(existe)* | modèle sémantique, `query_with_evidence` | oui |
| 04 | `governed_capability` *(existe)* | catalogue de capacités, autonomie `shadow` | non |
| 05 | `rules_join_aggregate` | `business_rules`, `join`, `aggregate`, `having` | oui |
| 06 | `nested_partials` | `partials`, `intermediate_mode` | oui |
| 07 | `sources_and_shaping` | sources fichier, `filter_groups`, `add_columns`, loader enregistré | oui |
| 08 | `materialized_view` | `materialization: materialized_view`, compilation SQL | non |
| 09 | `streaming_table` | `materialization: streaming_table`, `available_now`, upsert | oui |
| 10 | `monitor_and_quarantine` | checks, `DataMonitor`, quarantaine d'un lot fautif | oui |
| 11 | `lineage_and_dictionary` | `LineageTracker`, `DataDictionary`, rendu Mermaid | non |
| 12 | `tracing` | `InMemoryTracer`, redaction des attributs | non |
| 13 | `semantic_projection` | draft géré, `sync --check`, codes de sortie, `--promote` | non |
| 14 | `domain_graph` | `relationships`, refus de fanout, calendrier versionné | non |
| 15 | `genbi_agent` | sélection de noms par LLM bouchonné, refus avec suggestions | non |
| 16 | `mcp_readonly` | scopes, découverte filtrée, schéma d'outil fermé | non |
| 17 | `adaptive_gold` | événements d'usage, proposition, `accept`/`reject` | non |
| 18 | `capability_execution` | approbation liée, idempotence, compensation | non |

Sept exemples démarrent Spark, onze non.

**Hors périmètre, assumé :** sink JDBC (demande un Postgres), déploiement Databricks (demande un
workspace), serving MLflow (extra lourd). Ces trois-là restent documentés en référence. L'exemple 16
saute proprement si l'extra `mcp` n'est pas installé.

## 4. Le coût, et la manière qu'on écarte

Chaque exemple Spark paie son propre démarrage de JVM. Mesuré : les quatre exemples actuels
prennent 45 s, soit une douzaine de secondes de démarrage par exemple qui touche Spark.

**L'idée évidente est de tout exécuter dans un seul processus** pour ne payer la JVM qu'une fois.
**On l'écarte.** Elle réintroduit exactement le défaut corrigé le 9 septembre dans la suite : dix
tests ne passaient que parce qu'un voisin avait démarré la session avant eux. Un exemple qui ne
fonctionne que précédé d'un autre est un exemple cassé pour le lecteur, qui l'exécute seul — et un
harnais partagé rend ce défaut invisible. La faute serait d'autant plus difficile à voir ici que
`RuleRegistry` est un registre de processus : deux exemples enregistrant une règle de même nom se
contamineraient sans qu'aucun ne le signale.

**Donc : un sous-processus par exemple, comme le lecteur les lance.** Le levier sur le coût n'est
pas le partage de processus, c'est de **ne démarrer Spark que lorsque l'exécution fait partie de la
leçon**. Deux exemples changent de camp à ce titre : la vue matérialisée montre le SQL compilé, et
le graphe de domaine montre le chemin de jointure et le refus de fanout. Ni l'un ni l'autre n'a
besoin d'exécuter quoi que ce soit pour se faire comprendre.

Reste sept exemples Spark, dont trois existent. Coût attendu de la suite : environ deux minutes de
plus, et il tombe dans l'étage lent de la CI, jamais sur chaque push. **Chiffre à confirmer par la
mesure à la fin.**

## 5. Phases

Une phase = un commit.

- **Phase 0 — harnais.** Vérifier que chaque exemple passe **lancé seul**, et comparer la sortie
  imprimée à celle que son README annonce. Aucun exemple ajouté.
- **Phase 1 — écriture de pipelines.** 05, 06, 07.
- **Phase 2 — matérialisation.** 08, 09.
- **Phase 3 — observabilité.** 10, 11, 12.
- **Phase 4 — sémantique.** 13, 14, 15.
- **Phase 5 — surface agent.** 16, 17, 18.
- **Phase 6 — raccordement.** Index `examples/README.md`, table « où aller ensuite » de
  `getting_started.md`, et chaque page de `docs/` pointant vers l'exemple qui la démontre.

## 6. Risques

**Un exemple qui ment est pire que pas d'exemple.** Le Plan 33 a montré que l'écriture d'exemples
réellement exécutés fait sortir des défauts du code livré — cinq, dont une feature entière
inutilisable en local. Il faut s'attendre à en trouver d'autres, et les corriger fait partie du
travail, pas d'un plan ultérieur.

**La suite s'allonge.** Traité en phase 0, sur mesure.

**Les exemples périment.** Ils sont exécutés par la suite, donc un changement de comportement les
casse. Ce qui n'est pas protégé, c'est la *sortie imprimée* décrite dans les README : un exemple
peut continuer à s'exécuter en imprimant autre chose. La phase 0 ajoute donc une comparaison de la
sortie attendue pour les exemples dont le README annonce un résultat exact.

## 7. Vérification

- `pytest tests/ -q` vert après chaque phase.
- `mkdocs build --strict` sans warning.
- Chaque exemple lancé isolément à la main, sortie relue.
- Temps de suite mesuré avant et après la phase 0, et à la fin.
