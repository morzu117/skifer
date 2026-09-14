# Plan 37 — Durcissement des chemins best-effort et injection de paramètres

> Rédigé le 14 septembre 2026, après la fusion des Plans 35 (#2) et 36 (#3) sur `main`.
> Reprend trois points du §8 du [Plan 36](36_openlineage_emitter_plan.md) ; le plan dédié au tracker de lineage
> (`aggregate:` et colonnes de jointure) suit celui-ci.
> Statut : **validé le 14 septembre 2026** — D1 et D3 telles que recommandées ; D2 discutée puis adoptée en option A
> (refus avec indice), la substitution après lecture du YAML (option C, §4) étant retenue comme candidate à un plan dédié.
> Branche `fix/plan37-best-effort-hardening`, partie de `main` (`f7e9735`).
> **Implémenté (14 septembre 2026)** — 37.0 `ff3253d` · 37.1 `c0b5842` · 37.2 `3f8760c` · 37.3 `8b6b459` · 37.4 `1647044`.
> Dev Codex `gpt-5.6-sol`, review Claude sans outil : cinq ACCEPT sans redev. Gate Windows vert à chaque commit contre une
> base réenregistrée sur `main` (248 échecs connus ; 244 après 37.4, exemples 11 et 21 réparés). Contre-preuve sur
> instantané figé pour chaque test de protection ; rejeu sans mock des alertes sous `python -W error`. CI Linux à confirmer à la PR.
> Tâche mémoire : `skifer:plan:best-effort-hardening`.

## 1. Contexte

Trois défauts pré-existants, constatés pendant le cycle 36 et **vérifiés à nouveau sur `main`** avant rédaction :

1. **Un chemin « jamais bloquant » peut bloquer.** Sous un filtre « warnings as errors » (`python -W error`,
   `filterwarnings = error` dans pytest, `warnings.simplefilter("error")`), `warnings.warn` lève. Le Plan 36 a
   protégé ses propres sites (`_warn_best_effort`, `observability/openlineage.py:276`) ; les autres chemins
   best-effort ne le sont pas :
   - `observability/publication.py` : 6 sites (alerte d'incident, lecture de la publication précédente, alerte de
     changement cassant, indexation reprise, ouverture et résolution d'incidents) ;
   - `core/patterns.py:60` : `_build_alert_router` ;
   - `observability/alerts.py` : `_notify_channel` et les 3 sites d'envoi.
   Sous ce filtre, l'avertissement émis dans le `except` propage une exception hors d'un bloc censé tout avaler :
   une alerte qui échoue fait échouer la publication qu'elle observe.
2. **Un avertissement d'alerte peut divulguer un secret.** `alerts.py` recopie le message d'exception dans 3
   avertissements (`_send_webhook:226`, `_send_email:296`, `_send_redacted_email:322`) — le §8 du Plan 36 en
   annonçait quatre, le quatrième (`_notify_channel`) n'écrit déjà que le nom de classe. Une erreur réseau cite
   souvent l'URL du webhook, qui est un secret ; une erreur SMTP peut citer l'hôte ou le compte.
   **Défaut associé découvert à la rédaction** : `_send_webhook` et les deux `_send_*email` avalent leur propre
   exception, donc `_notify_channel` ne voit jamais l'échec et **déclare le canal notifié alors que l'envoi a
   échoué**. `test_failed_channel_isolated` ne le voit pas : il remplace `_send_webhook` par un stub qui lève.
3. **Un paramètre contenant `\` casse le chargement d'un schéma.** `_inject_params` (`core/schema_loader.py:168`)
   passe la valeur comme *template* de remplacement à `re.sub`, qui interprète `\U`, `\1`, `\g<0>` :
   `re.error: bad escape \U` pour tout chemin Windows. Reproduit : une fois ce défaut corrigé, la valeur arrive
   dans `path: "{{ example_dir }}/…"` et YAML refuse à son tour `\U` **dans une chaîne entre guillemets doubles**
   (`ScannerError`) ; en slashs (`as_posix()`), le schéma charge. Les 8 exemples qui passent
   `"example_dir": str(…)` sont exposés sous Windows (01, 02, 03, 05, 06, 07, 11, 21), pas seulement l'exemple 11.

## 2. Rayon d'impact déclaré

Obtenu par `codegraph impact` / `codegraph_explore`. Projet piloté par YAML : **plancher, pas périmètre.**

| Zone | Symboles |
|---|---|
| Helper | nouveau `observability/best_effort.py` ; `_warn_best_effort` (`openlineage.py`, appelé aussi par `_resolve_lineage_dataset_namespace` et `SkiferEngine.__init__` dans `core/core.py`) |
| Publication | `PublicationCoordinator._publish_run` (alertes), `_index_resumed_metadata`, `_record_incidents`, `_resolve_recovered` (`observability/publication.py`) |
| Orchestration | `_build_alert_router` (`core/patterns.py`) |
| Alertes | `AlertDispatcher._notify_channel`, `_send_webhook`, `_send_email`, `_send_redacted_email` ; appelants `dispatch`, `dispatch_incident`, `_send_slack/_msteams/_google_chat`, `QualityAgent.check`, `AlertRouter.alert_incident/alert_breaking_change` |
| Chargement | `_inject_params` → `parse_schema`, `parse_schema_localized`, `load_schema` (238 symboles touchés : tout chargement de schéma) |
| Tests | nouveau `tests/test_best_effort.py` ; `test_certified_publication.py`, `test_incidents.py`, `test_patterns.py`, `test_alerts.py`, `test_observability.py` (`TestAlertDispatcher`), `test_schema_loader.py`, `test_openlineage.py`, `test_core.py` |
| Non-code | `CHANGELOG.md` ; `examples/{01,02,03,05,06,07,11,21}/run.py` (exécutés par `tests/test_examples.py`) |

**Hors périmètre, délibérément** : `lineage/classification.py` (l'avertissement du mode `warn` est le signal métier,
pas un best-effort — sous `-W error`, lever est le comportement attendu) ; `schema_loader.load_schema` (UserWarning de
résolution par nom de fichier, idem) ; `observability/tracing*.py` (journalise via `logging`, pas `warnings`).

## 3. Sous-tâches

Une sous-tâche = un commit `fix(plan37-N): …`, chacun avec tests et entrée `CHANGELOG.md [Unreleased]`.
**Contre-preuve obligatoire** (leçon du Plan 36) : chaque test de protection échoue sur `main` (`f7e9735`), vérifié
sur un instantané figé (`git archive`), jamais sur l'arbre de travail.

| # | Sous-tâche | Fichiers visés | Acceptation | Difficulté |
|---|---|---|---|---|
| 37.0 | **Helper partagé** `warn_best_effort(message, category=RuntimeWarning)` dans `observability/best_effort.py` (bibliothèque standard, aucun import Spark ni skifer) : `warnings.warn` dans un `try/except Exception: pass`. `openlineage._warn_best_effort` délègue au helper sans changer de nom ni de signature (les appelants de `core.py` restent intacts) | `observability/best_effort.py`, `observability/openlineage.py`, `tests/test_best_effort.py` | sous `simplefilter("error")` aucun raise ; filtre par défaut → un `RuntimeWarning` portant le message ; tests OpenLineage et `test_core.py` existants inchangés et verts | standard |
| 37.1 | **Chemins best-effort protégés** : les 6 sites de `publication.py` et `_build_alert_router` passent par le helper, messages inchangés à l'octet | `observability/publication.py`, `core/patterns.py`, `tests/test_certified_publication.py`, `tests/test_incidents.py`, `tests/test_patterns.py` | sous `simplefilter("error")`, avec store / alert router / metadata store qui lèvent : la publication retourne le même `PublicationResult` (PROMOTED ou QUARANTINED) et persiste les mêmes lignes que sans filtre ; `_build_alert_router` retourne `(None, {})` ; chaque test échoue sur `f7e9735` | standard |
| 37.2 | **Alertes : échec visible, aucun secret** (D1) : `_send_webhook`, `_send_email`, `_send_redacted_email` laissent l'exception remonter à `_notify_channel` (seul point de capture, déjà présent sur tous les chemins d'appel) ; `_notify_channel` avertit via le helper au seul nom de classe | `observability/alerts.py`, `tests/test_alerts.py`, `tests/test_observability.py` | `urlopen` réel qui lève avec l'URL secrète dans son message → canal **absent** de la liste notifiée, un seul avertissement, ni l'URL ni le message dans aucun avertissement (canari) ; même chose pour SMTP ; sous `simplefilter("error")` `dispatch` / `dispatch_incident` ne lèvent pas et les autres canaux sont notifiés | standard |
| 37.3 | **Injection littérale** (D2) : `_inject_params` remplace par fonction (`lambda _m: text`), la valeur n'est plus un template de `re.sub`. Sur `yaml.YAMLError` après injection, si une valeur injectée contient `\`, le message « Malformed YAML schema » ajoute un indice nommant **la clé** (jamais la valeur) : utiliser des `/` ou une chaîne entre apostrophes | `core/schema_loader.py`, `tests/test_schema_loader.py` | `C:\Users\x`, `\1`, `\g<0>` injectés à l'identique (scalaire non quoté et entre apostrophes) ; entre guillemets doubles → `ValueError` nommant la clé, sans la valeur ; aucun changement pour une valeur sans `\` (golden sur un schéma existant) | standard |
| 37.4 | **Exemples portables** : les 8 `run.py` passent `Path.as_posix()` pour `example_dir` | `examples/{01,02,03,05,06,07,11,21}/run.py`, `CHANGELOG.md` | l'exemple 11 (Spark-free) passe sous Windows ; les 8 exemples passent dans la CI Linux ; aucun autre changement de sortie | standard |

Dépendances : 37.0 → 37.1 → 37.2 ; 37.3 → 37.4 ; les deux chaînes sont indépendantes.

**Routage modèle** (écrasable au GO). Dev Codex si le quota le permet, sinon `claude-dev` comme au Plan 36.
Review Claude sans outil, `claude-sonnet-5`, consignes portant les faits vérifiés du code (§1).

## 4. Options écartées

- **Un `warnings.catch_warnings()` global autour de la publication** : masquerait aussi les avertissements métier
  (classification `warn`) et n'est pas sûr entre threads.
- **`logging` au lieu de `warnings`** sur ces chemins : change le canal observé par les utilisateurs et les tests
  existants (`pytest.warns`) ; hors d'un plan de correctifs.
- **Normaliser les `\` en `/` dans `_inject_params`** : le moteur ne peut pas savoir si une valeur est un chemin ;
  réécrire silencieusement une valeur métier est pire que refuser avec un indice.
- **Échapper la valeur pour YAML** : le moteur ne sait pas si le placeholder est entre guillemets doubles,
  apostrophes ou non quoté ; un échappement juste dans un cas corrompt les autres.
- **Substituer les placeholders après lecture du YAML** (option C de D2) : correct par construction — la valeur arrive
  intacte quel que soit le quoting — mais change le modèle de templating (plus d'injection de structure YAML ni de
  scalaire typé non quoté ; `{{ x }}` non quoté devient un refus explicite) et touche tout chargement de schéma,
  partials et chargement localisé compris. **Retenue comme candidate à un plan dédié** (décision humaine du
  14 septembre 2026). Constat pour ce plan futur : aucun placeholder non quoté dans le repo (exemples, doc, tests).
- **Traiter ici les MINEURs du Plan 36** (docstring du namespace, formulations de la doc OpenLineage, nom d'hôte à un
  label) : restent au §8 du Plan 36 ; le nom d'hôte à un label est une décision humaine distincte.

## 5. Décisions à valider (conception)

| # | Question | Recommandation |
|---|---|---|
| D1 | Alertes : où capturer l'échec d'envoi | **Supprimer les `try/except` internes** de `_send_webhook` / `_send_email` / `_send_redacted_email` : `_notify_channel` enveloppe déjà tous les chemins d'appel (vérifié), et c'est la seule façon qu'un échec ne soit plus compté comme notifié. Suppression de lignes existantes **déclarée** (dérogation au contrôle insertion-only, 3 blocs) |
| D2 | Paramètre avec `\` dans une chaîne YAML entre guillemets doubles | **Refuser avec un indice nommant la clé** (37.3) plutôt que normaliser ou échapper ; les exemples passent `as_posix()` (37.4) |
| D3 | Portée de la protection `-W error` | Les chemins best-effort listés au §2 uniquement ; les avertissements métier (classification `warn`, résolution de schéma par nom) continuent de lever sous `-W error` |

## 6. Risques

| Risque | Mitigation |
|---|---|
| D1 change la liste de canaux retournée par `dispatch` quand un envoi échoue (correction du défaut, mais comportement observable) | test explicite ; entrée CHANGELOG en « Fixed » qui le dit ; `QualityAgent.check` relu (seul appelant externe) |
| Tests existants qui attendent l'ancien texte `webhook POST failed: <message>` | inventaire au dev (`test_observability.py::TestAlertDispatcher`) ; message remplacé, jamais d'assertion retirée sans équivalent |
| `_inject_params` touche tout chargement de schéma (238 symboles) | golden sur des schémas existants sans `\` ; suite complète |
| Messages de publication modifiés par mégarde (des tests les matchent) | acceptation « inchangés à l'octet » en 37.1 |
| Poste Windows (winutils, `:` dans des noms de fichiers) | gate comparé à la base de 248 échecs connus ; CI Linux fait foi pour les exemples Spark |

## 7. Vérification

- Gate de chaque sous-tâche : suite complète comparée à la base enregistrée, aucun échec nouveau ;
  `ruff check src/` ; `tests/test_examples.py` pour 37.4.
- Contre-preuve sur `f7e9735` figé pour chaque test de protection (37.1, 37.2, 37.3).
- Rejeu orchestrateur du vrai chemin : publication certifiée locale sous `python -W error` avec un webhook injoignable
  (37.1 + 37.2) ; `python examples/11_lineage_and_dictionary/run.py` sous Windows (37.3 + 37.4).
- Périmètre : `git --no-pager show <COMMIT> | grep -E "^-" | grep -vE "^---" | wc -l` proche de zéro, hors
  suppressions déclarées (D1 ; ligne `re.sub` de 37.3 ; 8 lignes d'exemple en 37.4 ; corps de `_warn_best_effort` en 37.0 ;
  7 appels `warnings.warn` en 37.1).
- CI Linux à la PR.

## 8. Suivi hors plan (constaté pendant le cycle)

| Sujet | Nature | Suite proposée |
|---|---|---|
| 4 tests `*_when_warnings_are_errors` de `test_certified_publication.py` vérifient l'état retourné mais pas les événements persistés | review 37.1 (MINEUR) | ajouter l'assertion sur les événements de run si ces chemins évoluent |
| `_backslash_param_hint` trie les clés sur `repr` ; motif du placeholder dupliqué avec `_inject_params` | review 37.3 (MINEUR) | factoriser le motif ; trier sur la clé |
| La réponse d'un `HTTPError` levé par `urlopen` n'est pas fermée explicitement → `ResourceWarning: unclosed socket` au ramasse-miettes (jamais propagé) | rejeu 37.2, pré-existant | fermer `exc` dans `_notify_channel` ou dans `_send_webhook` |
| Substitution des placeholders après lecture du YAML (option C de D2) | décision humaine | plan dédié ; aucun placeholder non quoté dans le repo |
| Exemples encore rouges sous Windows (winutils, `:` dans des noms de fichiers) ; exemple 20 : sortie README avec antislashs | pré-existant, hors Plan 37 | à traiter si le poste Windows doit devenir un environnement de validation complet |
| Pendant `codex exec`, le `gbrain serve` MCP de la session Codex tient le verrou PGLite : `gbrain put` en CLI échoue ; `codegraph.ps1` bloqué par la politique d'exécution PowerShell côté Codex | outillage | écrire la mémoire entre deux runs de dev ; invoquer codegraph via `codegraph.cmd` ou ajuster la politique |
