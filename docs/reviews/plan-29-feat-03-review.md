# Revue approfondie — branche `plan-29-feat-03`

Date de revue : 2026-09-02  
Branche revue : `plan-29-feat-03` (`49e9a76`)  
Base de comparaison : `plan-29-feat-02` (`709c64f`)  
Statut recommandé : **changements requis avant merge**

## 1. Résumé exécutif

La branche apporte les types et les premiers points d'entrée nécessaires à une garde de
certification sémantique, mais les garanties de sécurité décrites par le plan 29 ne sont pas encore
assurées dans le runtime réel.

Les principaux bloqueurs sont les suivants :

1. la configuration montrée dans le plan (`environments.<env>.params`) est ignorée par la garde ;
2. aucun chemin de construction produit ne fournit de `CertificationStore` au `SemanticEngine`, et
   le store Delta ne sait pas répondre à `get_certification()` ;
3. le chemin `GenBIAgent` compile avant la garde et ne fait pas le second contrôle juste avant SQL ;
4. la policy assimile toute certification au statut `CERTIFIED` à une autorisation valide, sans
   vérifier expiration, checks, version/hash ou droits du consommateur ;
5. le flux de publication inclus dans la branche permet de promouvoir un staging sans validation,
   puis le registre présente cette promotion comme certifiée.

Les tests existants sont tous verts, mais ils vérifient surtout les chemins heureux et des doubles
isolés. Ils ne couvrent pas l'intégration produit ni les frontières de panne exigées par le plan.

## 2. Périmètre réellement comparé

La plage `plan-29-feat-02...plan-29-feat-03` contient huit commits :

- quatre commits de Feature 1 : slices 1.2 à 1.5 ;
- quatre commits de Feature 3 : slices 3.1 à 3.4.

La revue couvre donc les ajouts de publication certifiée présents dans cette plage, en plus de la
garde sémantique. Les défauts hérités de `plan-29-feat-02` ne sont signalés que lorsqu'ils bloquent
directement le fonctionnement de Feature 3 ; ils sont explicitement marqués comme tels.

## 3. Constats bloquants et priorité haute

### F01 — P1 — La configuration documentée est ignorée

**Emplacements :**

- `src/skifer/core/config.py:96-110`
- `src/skifer/semantic/semantic.py:372-380`
- `src/skifer/core/context.py:58-73`
- `docs/roadmap/29_agent_ready_semantic_layer/03_semantic_certification_gate_plan.md:58-68`

Le plan place `semantic_certification_policy` et `semantic_consumer_class` dans
`environments.<env>.params`. `ConfigurationManager` valide uniquement des clés placées directement
dans le bloc environnement, et `SemanticEngine` les lit également uniquement à ce niveau.

Conséquences :

- une configuration conforme au plan revient silencieusement à `off` ;
- une valeur invalide placée dans `params` n'échoue pas au démarrage ;
- la priorité annoncée entre paramètres d'environnement et configuration n'est pas testée ;
- la documentation de migration ne montre pas de YAML complet permettant d'éviter l'ambiguïté.

**Correction attendue :** choisir une source de vérité unique. Si la clé reste dans `params`, lire
via `ExecutionContext.env_params()` et valider à cet endroit. Ajouter des tests avec la structure
YAML exacte documentée et avec des noms d'environnement de casse différente.

### F02 — P1 — La garde stricte est inutilisable dans les chemins runtime réels

**Emplacements :**

- `src/skifer/core/core.py:744-752`
- `src/skifer/serving/chat_model.py:51-61`
- `src/skifer/semantic/semantic.py:392-399`
- `src/skifer/observability/certification_store.py:144-180`

`SkiferEngine.get_agent()` et `SkiferChatModel.load_context()` construisent tous deux
`SemanticEngine` sans `certification_store`. En mode `enforce`, chaque dépendance devient donc
manquante et toutes les requêtes sont refusées.

Même si l'on injectait `DeltaCertificationStore`, celui-ci n'implémente pas
`get_certification()` ni `list_history()`, pourtant déclarés par le protocole. L'`AttributeError`
serait avalée par `_get_certification()` et transformée en certification absente. Ce second point est
hérité de `plan-29-feat-02`, mais bloque directement Feature 3 en production Databricks.

**Correction attendue :** introduire une factory de store dépendant du contexte local/Databricks,
l'injecter dans tous les constructeurs officiels, compléter le store Delta, puis tester
`get_agent()` et `load_context()` avec une policy stricte.

### F03 — P1 — `GenBIAgent` contourne l'ordre et le double contrôle de la garde

**Emplacements :**

- `src/skifer/agentic/agent.py:255-310`
- `src/skifer/agentic/agent.py:398-415`
- `src/skifer/semantic/semantic.py:223-244`

Le chemin direct `SemanticEngine.query()` effectue correctement un préflight, compile, puis refait
un contrôle. Le chemin agent, qui exécute son propre SQL, suit un autre ordre :

```text
QueryResolver.resolve
        ↓
enforce_certification_gate
        ↓
execute_sql
```

Il manque donc à la fois le contrôle avant compilation et le recheck après compilation. Le test
agent ajouté vérifie seulement « aucun SQL pour une certification déjà refusée » ; il ne vérifie ni
zéro appel à `resolve`, ni une certification qui change entre préflight et exécution.

Le mode `view` compile une première fois avant la garde dans `_process()`, puis
`SemanticEngine.create_view()` compile une seconde fois avec ses propres contrôles.

**Correction attendue :** centraliser compilation et exécution dans `SemanticEngine`, ou exposer un
objet de préflight réutilisable par l'agent. Ajouter un spy sur `resolve()` et un store basculant
`CERTIFIED → UNCERTIFIED` dans le test agent.

### F04 — P1 — La décision ne vérifie que le statut nominal

**Emplacements :**

- `src/skifer/semantic/access_policy.py:96-122`
- `src/skifer/semantic/dependencies.py:6-19`
- `src/skifer/observability/certification_store.py:42-49`

`evaluate()` autorise toute valeur dont `status == "CERTIFIED"`. Le contexte consommateur est reçu
mais jamais utilisé. Les raisons prévues par le plan ne sont pas implémentées, à l'exception de
`MISSING` et `OVERRIDDEN` :

- `EXPIRED` : aucun contrôle de durée/SLA ;
- `FAILED_CHECK` : aucun résultat de check n'entre dans la décision ;
- `HASH_MISMATCH` : version et hash ne sont jamais comparés ;
- `UNAUTHORIZED` : `scopes` et classe consommateur ne participent pas à la policy ;
- `STORE_UNAVAILABLE` : toute exception est convertie en `None`, donc en `MISSING`.

Une certification ancienne, issue d'une autre définition de contrat, reste donc autorisée sous
`enforce`. En mode `warn`, une panne du registre est présentée comme un dataset manquant, ce qui
fausse le diagnostic opérationnel.

**Correction attendue :** enrichir le modèle de certification avec expiration/checks/SLA, comparer
les valeurs attendues de chaque dépendance, représenter explicitement l'indisponibilité du store et
appliquer les règles liées au `ConsumerContext`.

### F05 — P1 — Une promotion non validée devient « certifiée »

**Emplacements :**

- `src/skifer/observability/publication.py:51-73`
- `src/skifer/observability/certification_store.py:120-131`

`promote_staging()` exige uniquement que le dernier état soit `STAGED`. Cet état atteste une écriture
réussie, pas l'exécution ni le succès des checks. Après la copie vers la cible, un événement
`PROMOTED` est écrit. `SqliteCertificationStore.get_certification()` traduit ensuite toute dernière
promotion en `CERTIFIED`, sans consulter les `check_results`.

La chaîne suivante est donc possible :

```text
staging écrit sans check
        ↓
promote_staging()
        ↓
événement PROMOTED
        ↓
get_certification() == CERTIFIED
        ↓
garde sémantique ALLOW
```

Ce défaut invalide la frontière de confiance centrale du plan 29.

**Correction attendue :** matérialiser un état de validation réussi lié au run et au hash du
contrat, refuser la promotion sans cette preuve, et dériver la certification depuis le run validé et
ses checks plutôt que depuis `PROMOTED` seul.

### F06 — P1 — Le registre et la garde n'utilisent pas la même identité de dataset

**Emplacements :**

- `src/skifer/observability/publication.py:45-70`
- `src/skifer/observability/certification_store.py:120-131`
- `src/skifer/semantic/dependencies.py:13-19`

Les événements de publication enregistrent `definition.data_product_id` dans `RunEvent.dataset`.
La garde recherche quant à elle une certification avec la table physique issue de
`model["table"]`. Ces identités sont généralement différentes, par exemple `sales.orders` et
`gold.fact_orders`.

Même une future publication correctement validée peut donc rester invisible pour la garde, qui
conclura `MISSING`. Inversement, aucune résolution explicite ne prouve qu'un produit certifié couvre
la table physique demandée.

**Correction attendue :** choisir et documenter une identité canonique commune, ou stocker une
relation explicite `data_product_id ↔ physical_dataset` vérifiée avec le contract/hash. Tester le
cycle publication → lecture du registre → autorisation sémantique sans doubles incompatibles.

### F07 — P1 — Le flux de publication n'est relié à aucun pipeline d'écriture

**Emplacements :**

- `src/skifer/core/core.py:441-471`
- `src/skifer/observability/publication.py:32-74`
- `src/skifer/observability/quarantine.py:18-47`

Les fonctions de staging, quarantaine et promotion ne sont appelées par aucun chemin de
`SkiferEngine`, `PipelinePatterns`, monitor ou writer. Les pipelines batch continuent à appeler
directement `write_table()`. Il n'existe pas de `PublicationCoordinator.publish()` ou `resume()` tel
que prévu par le plan.

En conséquence, les nouveaux helpers ne protègent aucune publication réelle et l'invariant « une
version invalide ne remplace jamais la cible certifiée » n'est pas exercé en E2E.

**Correction attendue :** intégrer un coordinateur au chemin batch explicitement opt-in par contrat,
refuser streaming/JDBC/MV, et ajouter le scénario Delta local v1 valide / v2 invalide / v3 valide.

### F08 — P1 — Aucune reprise n'est possible après un crash en `PROMOTING`

**Emplacement :** `src/skifer/observability/publication.py:62-73`

L'événement `PROMOTING` est persisté avant l'écriture de la cible. Si le processus tombe pendant
l'écriture ou après celle-ci mais avant `PROMOTED`, le prochain appel refuse le run, car seule la
transition depuis `STAGED` est acceptée. Il n'existe pas de `resume(run_id)`.

La cible peut alors être partiellement/entièrement mise à jour tandis que le registre reste bloqué en
`PROMOTING`. L'idempotence testée ne couvre que le retry après `PROMOTED`, qui est la frontière la
moins risquée.

**Correction attendue :** définir les transitions de reprise pour `STAGING`, `STAGED` et
`PROMOTING`, avec une stratégie idempotente fondée sur l'identité de version Delta et des tests de
crash à chaque frontière.

### F09 — P1 — La quarantaine row-level annoncée n'est pas implémentée

**Emplacements :**

- `src/skifer/observability/quarantine.py:32-47`
- `tests/test_certified_publication.py:40-44`

La slice 1.4 devait construire `_violations`, `_run_id`, `_contract_version`, produire les lignes de
quarantaine et contrôler les collisions au chargement. Le code retourne seulement un dictionnaire de
prédicats SQL ; aucune colonne n'est ajoutée, aucune ligne n'est filtrée, aucune métadonnée n'est
écrite et `validate_reserved_columns()` n'est appelée par aucun loader.

**Correction attendue :** implémenter la transformation DataFrame via le backend Spark, appeler la
validation de colonnes durant le chargement du schéma et tester plusieurs violations simultanées,
les checks dataset et les métadonnées réservées.

### F10 — P1 — Les dépendances peuvent désactiver complètement la garde

**Emplacements :**

- `src/skifer/semantic/dependencies.py:13-19`
- `src/skifer/semantic/semantic.py:332-335`

Le resolver ne reconnaît que `model["table"]`. Sans cette clé, il renvoie `()`, puis la garde retourne
sans erreur, y compris en `enforce`. Les modèles multi-source et matérialisés prévus par le plan ne
sont pas traités. `contract_version` et `definition_hash` restent toujours `None`.

Une évolution du format sémantique ou un modèle incomplet peut donc passer en fail-open.

**Correction attendue :** en mode strict, refuser un modèle exécutable dont les dépendances ne
peuvent pas être résolues. Implémenter les formes supportées et dédupliquer de manière stable avec
les identités de contrat attendues.

## 4. Constats priorité moyenne

### F11 — P2 — L'override est attribué syntaxiquement, mais ni autorisé ni audité

**Emplacements :**

- `src/skifer/semantic/access_policy.py:36-41`
- `src/skifer/semantic/access_policy.py:81-107`
- `src/skifer/semantic/semantic.py:206-240`

Tout appelant direct capable de construire `CertificationOverride` obtient `ALLOW`, quel que soit le
mode, le contexte, les scopes ou une policy d'autorisation. L'actor et la trace sont seulement des
chaînes non vides ; ils ne sont pas liés au `ConsumerContext`, signés ou persistés dans un journal.

Une datetime naïve dans `expires_at` provoque par ailleurs un `TypeError` lors de sa comparaison avec
l'horloge UTC aware, au lieu d'une erreur de validation structurée.

**Correction attendue :** contrôler un scope break-glass explicite, vérifier la cohérence
actor/trace/contexte, exiger une datetime timezone-aware et écrire un événement d'audit durable pour
chaque tentative acceptée ou refusée.

### F12 — P2 — Les vues ne conservent pas la certification réellement utilisée

**Emplacements :**

- `src/skifer/semantic/semantic.py:299-315`
- `src/skifer/semantic/semantic.py:407-429`

Le commentaire de vue est construit depuis `SemanticDependency`, dont version et hash sont toujours
vides. Il ne contient ni le mode de policy, ni la décision, ni la version/hash effectivement renvoyés
par le store. Il ne permet donc pas d'établir avec quelle preuve la vue a été créée.

**Correction attendue :** retourner un objet de préflight contenant les certifications observées et
persister une identité non sensible complète dans les propriétés/commentaires de la vue.

### F13 — P2 — La réponse serving perd les champs actionnables de la policy

**Emplacements :**

- `src/skifer/agentic/models.py:107-113`
- `src/skifer/serving/_response_serializer.py:58-66`

`GenBIAgent` renseigne `policy_decision`, `policy_reasons` et `recommended_action`, mais le serializer
retourne seulement `Error: {response.error}`. Le Hub/serving n'expose donc pas l'action recommandée de
façon structurée et stable ; le client doit parser le `str()` de l'exception.

**Correction attendue :** sérialiser explicitement la décision, les raisons et l'action recommandée,
et ajouter les tests CLI/serving correspondants.

### F14 — P2 — Le SQL est journalisé avant la garde dans le chemin agent

**Emplacement :** `src/skifer/agentic/agent.py:408-415`

`resolved.full_sql` est imprimé avant l'appel à la garde. Une requête finalement refusée peut donc
laisser dans les logs les noms de sources et les littéraux de filtres fournis par l'utilisateur.

**Correction attendue :** déplacer tout log SQL après la décision finale, utiliser le logger et
prévoir une politique de redaction.

### F15 — P2 — Le compteur de warnings n'est pas une métrique de trafic fiable

**Emplacements :**

- `src/skifer/semantic/semantic.py:67-68`
- `src/skifer/semantic/semantic.py:363-368`
- `docs/agentic.md:35-47`

Une requête directe en mode `warn` incrémente le compteur deux fois, une fois au préflight et une
fois au recheck. Le test encode ce double comptage. Le compteur est en mémoire, par instance, remis à
zéro à chaque redémarrage et non agrégé entre pods ; il ne permet pas d'observer une fenêtre de
trafic représentative comme le suggère la documentation.

**Correction attendue :** distinguer les événements de préflight/recheck avec un identifiant de
requête et exporter une métrique durable/agrégeable. Si le compteur public est conservé, compter les
requêtes averties plutôt que les évaluations.

### F16 — P2 — L'identité serving provient de paramètres déclaratifs du client

**Emplacement :** `src/skifer/serving/chat_model.py:79-88`

`consumer_id`, `user_id` et `trace_id` sont copiés directement depuis `params`. Ils ne sont pas liés
à une identité authentifiée du endpoint. Ce n'est pas exploité par la policy actuelle, mais le
deviendra dès que `UNAUTHORIZED` ou les scopes seront implémentés.

**Correction attendue :** séparer les métadonnées fournies par le client de l'identité de sécurité,
et alimenter cette dernière depuis le contexte authentifié de serving.

## 5. Lacunes de tests

Les tests sont nombreux mais plusieurs critères de la Definition of Done ne sont pas couverts :

- matrice complète avec `EXPIRED`, `FAILED_CHECK`, `HASH_MISMATCH`, `UNAUTHORIZED` et
  `STORE_UNAVAILABLE` ;
- priorité et validation de la configuration sous `params` ;
- construction runtime via `get_agent()` et `load_context()` avec un vrai store ;
- store Delta conforme au protocole ;
- `GenBIAgent` : zéro `resolve` sur deny et recheck de course ;
- modèle sans dépendance, multi-source et identité contract/hash ;
- override non autorisé, audit durable et datetime naïve ;
- sérialisation actionnable des refus ;
- échec de write staging et état `RUN_FAILED` ;
- quarantaine qui échoue en conservant staging et les deux causes d'erreur ;
- promotion refusée sans validation ;
- reprise aux frontières `STAGING`, `STAGED`, `PROMOTING` ;
- E2E Delta local v1 valide / v2 invalide / v3 valide avec cible précédente intacte ;
- vraie quarantaine row-level avec `_violations`, `_run_id` et `_contract_version`.

Les cinq tests de `test_certified_publication.py` vérifient uniquement le chemin heureux des helpers
isolés. Ils ne prouvent pas l'intégration ni les invariants de panne.

## 6. Résultats des vérifications

Commandes exécutées depuis la branche revue :

```text
./.venv/bin/pytest tests/ -x --tb=short
→ 1429 passed, 15 skipped

./.venv/bin/ruff check <fichiers src modifiés par la branche>
→ All checks passed!

git diff --check plan-29-feat-02...HEAD
→ OK

python3 -m compileall -q src tests
→ OK
```

`./.venv/bin/ruff check src/` remonte 15 erreurs dans des fichiers non modifiés par la plage revue.
Elles constituent une dette existante et ne sont pas attribuées à cette branche.

## 7. Ordre de traitement recommandé

1. Définir la source de vérité de configuration et la factory de `CertificationStore`.
2. Rendre `DeltaCertificationStore` conforme et brancher le store dans `get_agent()`/serving.
3. Refondre l'évaluation pour traiter expiration, checks, hash et autorisation.
4. Centraliser le préflight/compile/recheck/execute afin que l'agent ne puisse pas diverger.
5. Fermer le fail-open des dépendances et persister les preuves utilisées par les vues.
6. Construire le coordinateur de publication avec validation obligatoire et reprise.
7. Implémenter la quarantaine row-level réelle et l'intégration batch E2E.
8. Finaliser override, audit, réponses actionnables et métriques.

## 8. Conclusion

La branche constitue un bon squelette d'API, mais ne doit pas encore être considérée comme une
frontière « agent-ready ». En l'état, `off` peut rester actif malgré une configuration conforme au
plan, `enforce` refuse tout dans les constructeurs produit, et une promotion non validée peut être
interprétée comme certifiée lorsqu'un store est injecté manuellement.

Le merge devrait être différé jusqu'à résolution au minimum de F01 à F10 et ajout des tests
d'intégration associés.
