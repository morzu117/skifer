# S04 — Routage du hub : `off | shadow | on`

> **Décision :** D5 (le routage vit dans `AgenticHub`)
> **Dépendances :** S03 mergée
> **Slices / commits :** 2
> **Branche :** `feat/plan44-s04-hub-routing`
> **Résultat :** l'agent de repli est branché derrière GenBI, et rien ne change tant qu'un
> environnement ne l'active pas.

## 1. Contexte

`AgenticHub._dispatch` (`agentic/hub.py`, vers la ligne 328) envoie les questions métier à
GenBI par `_fallback_to_genbi()`. GenBI répond `mode="needs_clarification"` avec des
`suggestions` quand un nom est inconnu : `_suggest_closest` applique déjà un seuil de
Levenshtein, donc une liste vide signifie « rien de proche ». Il répond `mode="no_model"` quand
aucun modèle ne convient. Ce sont les deux seuls cas où le repli a un sens : partout ailleurs,
GenBI a une réponse ou une erreur légitime.

**Écart avec le parent :** `candidate_fallback_min_similarity` n'est pas introduit. GenBI ne
renvoie pas de score de similarité, seulement une liste déjà filtrée, et en recalculer un
doublerait la règle. Le déclencheur est donc une liste de suggestions vide.

## 2. Contrat

Config par environnement, à la racine de l'environnement comme `semantic_certification_policy` :

```yaml
candidate_fallback: off        # off (défaut) | shadow | on
```

Déclencheur, évalué sur la réponse de GenBI et seulement si `candidate_fallback != off` :

```text
(mode == "no_model")  OR  (mode == "needs_clarification" AND suggestions == [])
```

Les modes `ambiguous`, `error`, `query` et `view`, ainsi que les refus de policy
(`access_denied=True`), ne déclenchent **jamais** le repli. Un refus de certification ne se
contourne pas par une candidate.

La parade R6 du parent, qui empêche une candidate promue d'être reproposée, vit dans l'agent
et non dans le hub. `MetricCandidateAgent` place en tête du contexte du LLM les candidates
`promoted` que l'index associe à la question, et le dédoublonnage par fingerprint de S03
(`request_count += 1`) fait le reste. Il n'y a pas de réutilisation sans LLM : le `schema_dict`
stocké est paramétré, et les valeurs de la nouvelle question doivent bien venir de quelque part.

| Mode | Ce qui est construit | Ce qui est renvoyé |
|---|---|---|
| `off` | rien | la réponse de GenBI, inchangée |
| `shadow` | candidate et run, persistés | la réponse de GenBI, avec `explanation` complétée de `Une réponse non certifiée a été préparée (candidate <id court>).` |
| `on` | candidate et run, persistés | la réponse `candidate` (ou `candidate_refused`) de l'agent |

En `shadow`, un échec de l'agent ne change pas la réponse renvoyée. Il est journalisé une fois,
avec le nom de classe seulement.

`hub_response_to_markdown` / `hub_response_to_text` préfixent une réponse `mode="candidate"` par
la ligne `> Réponse non certifiée — calculée hors du semantic layer, à valider.`, puis le
contenu, puis la ligne de provenance de S00.

`AgenticHub.__init__` gagne `candidate_agent: MetricCandidateAgent | None = None`. Absent,
`candidate_fallback` est sans effet. `serving/_agent_core.build_hub` le construit seulement si
`candidate_fallback != off` dans l'environnement résolu.

## 3. Exigences

- **S04-R1** — En `off`, ou sans `candidate_agent`, la réponse du hub DOIT être identique à
  celle d'avant le lot pour toutes les questions du jeu de tests existant. Le test compare
  `to_dict` et le texte.
- **S04-R2** — En `off`, l'agent NE DOIT PAS être appelé et le store NE DOIT PAS être écrit
  (espions).
- **S04-R3** — Le repli NE DOIT se déclencher que sur `no_model` ou sur `needs_clarification`
  sans suggestion. Le test est paramétré sur les modes `query`, `view`, `ambiguous`, `error`,
  `needs_clarification` avec suggestion, et `access_denied`.
- **S04-R4** — En `shadow`, la réponse renvoyée DOIT être celle de GenBI, avec la seule mention
  ajoutée, et la candidate DOIT être persistée.
- **S04-R5** — En `shadow`, une exception de l'agent NE DOIT PAS changer la réponse ni lever.
- **S04-R6** — En `on`, la réponse DOIT être celle de l'agent, et son texte DOIT commencer par
  la ligne d'avertissement.
- **S04-R7** — Une question qui retombe sur une candidate `promoted` DOIT incrémenter son
  `request_count` sans créer de nouvelle candidate. Le LLM factice renvoie la même structure avec
  une autre valeur, et le contexte qu'il reçoit cite la candidate promue en premier.
- **S04-R8** — Le serving MLflow DOIT porter la réponse `candidate` avec son evidence v2 dans
  `custom_outputs["skifer"]`. Le test est ajouté au job CI `serving`.
- **S04-R9** — `build_hub` NE DOIT PAS construire l'agent, ni ouvrir l'index ou le store, quand
  `candidate_fallback` vaut `off`.

## 4. Fichiers

**À modifier** :

- `src/skifer/agentic/hub.py` : `__init__`, `_dispatch`, et une fonction pure `_should_fallback`
  testable seule ;
- `src/skifer/core/config.py` : `candidate_fallback` ;
- `src/skifer/serving/_agent_core.py` : `build_hub` ;
- `src/skifer/serving/_response_serializer.py` : le préfixe ;
- `tests/test_hub.py`, `tests/test_serving_response_serializer.py` et
  `tests/test_serving_mlflow_roundtrip.py`, avec un seul cas ajouté ;
- `CHANGELOG.md`.

## 5. Slices

- **S04.1 Routage.** Config, `_should_fallback`, `_dispatch`, réutilisation, `build_hub`.
- **S04.2 Affichage et serving.** Le préfixe et le test MLflow.

## 6. Tests d'acceptation

| Exigence | Test |
|---|---|
| S04-R1 | `test_hub.py::test_off_mode_is_byte_identical` |
| S04-R2 | `…::test_off_mode_never_touches_agent_or_store` |
| S04-R3 | `…::test_should_fallback_matrix` (paramétré) |
| S04-R4 | `…::test_shadow_returns_genbi_and_persists_candidate` |
| S04-R5 | `…::test_shadow_swallows_agent_failure` |
| S04-R6 | `…::test_on_returns_candidate_with_warning_prefix` |
| S04-R7 | `…::test_promoted_candidate_is_reused_not_duplicated` |
| S04-R8 | `test_serving_mlflow_roundtrip.py::test_candidate_response_roundtrip` |
| S04-R9 | `test_serving_agent_core.py::test_build_hub_skips_candidate_agent_when_off` (le fichier existant ou son équivalent) |

## 7. Erreurs

| Cas | Comportement |
|---|---|
| `candidate_fallback` invalide | erreur de config au chargement, nommant les valeurs admises |
| `on` sans index synchronisé | l'agent refuse `NO_TABLE_FOUND`, ce qui n'est pas une erreur du hub |

## 8. Hors périmètre

- tout changement du comportement de GenBI ;
- le routage d'autres agents (lineage, qualité…) vers le repli ;
- une interface utilisateur au-delà du préfixe texte.

## 9. Definition of Done

- les 9 exigences ont leur test vert, et le job `serving` est vert ;
- la suite complète passe sans aucune modification d'un test existant, preuve que `off` est un
  no-op.
