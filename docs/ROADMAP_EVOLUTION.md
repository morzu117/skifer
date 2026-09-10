# Skifer — Pistes d'evolution

> Document de reflexion issu d'un echange du 21 avril 2026.
> Branche locale `roadmap/evolution-tracks` — non destinee a etre mergee en l'etat.

---

## Pistes de developpement

Chaque piste a son propre document detaille dans `docs/roadmap/`, destine a servir de base pour le plan de developpement.

| Priorite | Piste | Document | Statut |
|---|---|---|---|
| 1 | **Stabilisation Spark/Databricks** | [12_stabilisation_preproduction_plan.md](roadmap/12_stabilisation_preproduction_plan.md) | Priorite courante |
| 2 | **Lineage & dictionnaire de donnees** | [03_lineage_dictionnaire.md](roadmap/03_lineage_dictionnaire.md) | Livre (v0.10.0) |
| 3 | **Data observability** | [05_data_observability.md](roadmap/05_data_observability.md) | Livre (v0.11.0) |
| 4 | **Hub agentic Databricks** | [04_hub_agentic.md](roadmap/04_hub_agentic.md) | A cadrer sur Spark uniquement |
| 5 | **Client graphique Databricks** | [02_client_graphique.md](roadmap/02_client_graphique.md) | A cadrer sur Unity Catalog / Spark |
| Archive | **Abstraction multi-plateforme** | [01_multi_plateforme.md](roadmap/01_multi_plateforme.md) | Gele : hors focus produit courant |

---

## Ordre de priorite — justification

1. **Stabilisation Spark/Databricks** — Priorite produit : fiabiliser un seul runtime, Databricks Lakehouse + local PySpark/Delta, avant toute extension.
2. **Lineage & dictionnaire** — Besoin marche le plus frequent ("d'ou vient ce champ ?"), valorise les YAML existants.
3. **Observabilite** — Exploite l'avantage unique des YAML-as-contract, differenciateur fort vs dbt.
4. **Hub agentic Databricks** — Federe les capacites des pistes 2 et 3 dans une interface conversationnelle executee sur Spark.
5. **Client graphique Databricks** — Forte valeur ajoutee mais gros investissement frontend. A faire quand le core Spark est stabilise.

---

## Positionnement vs dbt

Le vrai differenciateur n'est pas de "faire pareil que dbt en Python" — c'est l'approche **YAML-as-contract + Python rules + LLM-native** qui n'a pas d'equivalent direct sur le marche.

| Axe | dbt | Skifer |
|---|---|---|
| Paradigme | SQL + Jinja | YAML declaratif + Python |
| Abstraction | Faible (le dev ecrit le SQL) | Forte (le dev declare QUOI, le framework genere le HOW) |
| Sandbox | Inexistant (batch) | Natif Spark/Databricks (isolation auto par utilisateur) |
| Semantic layer | MetricFlow (add-on) | SemanticEngine + QueryResolver + GenBIAgent (natif, LLM-native) |
| Data contracts | Tests separes du modele | Les YAML **sont** les contrats |
| Regles metier | Macros Jinja | `@register_rule()` — Python composable, testable |

**Manques a combler** : incremental, CI/CD, ecosysteme communautaire.

---

*Document genere le 21 avril 2026. A reviser regulierement.*
