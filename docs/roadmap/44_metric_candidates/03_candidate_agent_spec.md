# S03 — Agent de repli `MetricCandidateAgent`

> **Décision :** D3 (sortie bornée à la grammaire Skifer)
> **Dépendances :** S00, S01, S02 mergées
> **Slices / commits :** 4
> **Branche :** `feat/plan44-s03-candidate-agent`
> **Résultat :** une question hors du semantic layer produit soit une réponse calculée, marquée
> `CANDIDATE` et persistée sans valeur de filtre, soit un refus qui dit ce qui manque.

## 1. Contexte

Le LLM ne produit **jamais** du SQL exécuté. Il produit un `schema_dict`, c'est-à-dire la
grammaire YAML de Skifer, ou à défaut un `SELECT` que le mapper du Plan 43 traduit en
`schema_dict`. Dans les deux cas, ce qui s'exécute sort de `compile_select`. La boucle de
correction reprend celle de `SemanticBuilder` : l'erreur du validateur est renvoyée au LLM,
3 essais au maximum.

À réutiliser :

- `core/schema_loader.parse_schema`, qui normalise et valide ;
- `core/ir.parse_to_ir` et `core/sql_compiler.compile_select(parsed, resolve_table=None,
  allow_raw_sql=True)` ;
- `analyzer.mapper.map_statement(stmt) -> MappedStatement`, qui expose `.schema_dict`,
  `.residues`, `.blocking_residues` et `.needs_curation`. L'import est paresseux et l'extra
  `[analyzer]` est requis pour ce chemin seulement ;
- `semantic.planner.find_unsafe_fanout` ;
- `semantic.evidence` (S00) et `candidates` (S01) ;
- `knowledge.KnowledgeIndex` (S02) ;
- `semantic.llm_provider.LLMProvider` ;
- le tracing (`observability/tracing.py`) avec un span de taxonomie fixe.

## 2. Contrat

```python
# agentic/candidate_agent.py
CANDIDATE_SPAN_NAME = "skifer.candidate.build"
MAX_CORRECTION_ATTEMPTS = 3
ALLOWED_SCHEMA_KEYS = frozenset({
    "tables", "join", "where", "where_groups", "select_final", "add_columns", "aggregate",
})
# exclus : partials, business_rules, sink, materialization, data_product, contract,
#          semantic, keep_all_columns, source (une candidate ne lit que des tables du catalogue)

class CandidateRefusal(str, Enum):
    NO_TABLE_FOUND = "no_table_found"
    UNKNOWN_TABLE = "unknown_table"            # table hors de l'index ou hors du catalogue
    JOIN_WITHOUT_KEY = "join_without_key"
    UNSAFE_FANOUT = "unsafe_fanout"
    RAW_SQL_REJECTED = "raw_sql_rejected"      # expr:, filtre sql
    BLOCKING_RESIDUE = "blocking_residue"
    SQL_PATH_UNAVAILABLE = "sql_path_unavailable"   # extra [analyzer] absent
    INVALID_AFTER_RETRIES = "invalid_after_retries"
    EXECUTION_FAILED = "execution_failed"

class MetricCandidateAgent:
    def __init__(self, semantic_engine, llm_provider, index: KnowledgeIndex,
                 store: CandidateStore, *, policy: TraceAttributePolicy | None = None,
                 row_limit: int = 1000, timeout_seconds: int = 60, tracer=None,
                 clock=None): ...
    def ask(self, question: str, *, consumer_context=None,
            requester_id: str | None = None) -> AgentResponse
```

Déroulé de `ask()`, dans cet ordre :

1. `index.search(question, k=20)`. Si aucun hit `table`/`column` n'apparaît, refus
   `NO_TABLE_FOUND` sans appel au LLM.
2. Le contexte compact, avec en tête les candidates `promoted` que l'index associe à la question
   (parade R6 du parent, exploitée par S04) : tables avec leurs colonnes et types, métriques voisines, règles, arêtes
   `joins`/`shortcut` entre ces tables. Il est borné à 8 tables et 40 colonnes par table, dans
   l'ordre des scores.
3. Le prompt système demande un JSON `{"metric_name", "metric_description", "schema_dict"}` ou
   `{"metric_name", "metric_description", "sql"}`. Les clés autorisées de `schema_dict` sont
   listées, ainsi que les opérateurs et agrégats issus de `core/op_catalog.py`.
4. Validation, dans l'ordre :
   - le JSON est fermé, toute clé inconnue est refusée ;
   - `sql` passe par le mapper, et un résidu bloquant vaut refus ;
   - les clés hors `ALLOWED_SCHEMA_KEYS` sont refusées ;
   - chaque table doit être un nœud de l'index ;
   - aucun `expr:` ni filtre `sql` ;
   - `parse_schema` ;
   - chaque jointure doit avoir des clés, sinon c'est le refus `JOIN_WITHOUT_KEY`. Une jointure
     avec clés qui ne correspond à aucune arête `joins`/`shortcut` ni à aucune `relationship` est
     acceptée, mais notée `undeclared_join` dans `coherence` : c'est S05 qui la juge ;
   - `find_unsafe_fanout`.

   Une erreur des étapes 1 à 6 est renvoyée au LLM pour l'essai suivant.
5. `compile_select`, `EXPLAIN`, puis exécution bornée par `row_limit` et `timeout_seconds`.
6. Persistance :
   - `parameterize_filters` ;
   - `fingerprint_candidate` ;
   - si le fingerprint existe déjà : `request_count += 1`, ajout du demandeur pseudonymisé et
     `last_requested_at`, puis `upsert` ;
   - sinon : `MetricCandidate(state="candidate")` et un événement `created` avec
     `decided_by="agent:candidate_agent_v1"` ;
   - toujours un `CandidateRun(purpose="answer")` avec `result_hash`, le sha256:v1: des lignes
     triées et sérialisées en JSON canonique.
7. Réponse : `AgentResponse(mode="candidate", …, evidence=SemanticEvidence(...))` où chaque
   `MetricEvidence` porte `certification="CANDIDATE"` et `candidate_id`.

`AgentResponse` (`agentic/models.py`) gagne `candidate_id: str | None = None`. **Attention** : le
champ existant `candidates` désigne les modèles candidats d'une ambiguïté. Il n'est pas
réutilisé, et le nouveau champ ne le remplace pas.

Un refus renvoie `AgentResponse(mode="candidate_refused", error=None,
explanation=<phrase humaine>, recommended_action=<ce qu'il faudrait ajouter>)`, avec le code
`CandidateRefusal` dans `policy_reasons`. Il ne crée aucune candidate. Un événement `rejected` est
écrit seulement si la candidate existait.

## 3. Exigences

- **S03-R1** — Aucun texte SQL produit par le LLM NE DOIT atteindre le backend. Ce qui est
  exécuté est toujours égal à `compile_select(parse_to_ir(schema_dict_validé))`. Le test le
  vérifie avec un backend espion.
- **S03-R2** — Un `schema_dict` invalide puis un valide DOIVENT aboutir au 2e essai. Trois
  invalides DOIVENT donner `INVALID_AFTER_RETRIES`, avec au plus 3 appels LLM.
- **S03-R3** — Une clé hors `ALLOWED_SCHEMA_KEYS`, un `expr:` ou un filtre `sql` DOIVENT être
  refusés quel que soit `allow_raw_sql` dans l'environnement.
- **S03-R4** — Une table absente de l'index DOIT être refusée (`UNKNOWN_TABLE`) avant toute
  compilation.
- **S03-R5** — Un `SELECT` avec `LAG()` DOIT produire un refus `BLOCKING_RESIDUE` nommant le
  résidu. Un résidu non bloquant DOIT produire une candidate dont `residues` n'est pas vide.
- **S03-R6** — Sans l'extra `[analyzer]`, une réponse `sql` du LLM DOIT donner
  `SQL_PATH_UNAVAILABLE`, et le chemin `schema_dict` DOIT continuer à fonctionner. Le test
  masque `sqlglot`.
- **S03-R7** — Une question sans hit `table`/`column` DOIT être refusée **sans aucun appel au
  LLM**.
- **S03-R8** — Un fanout DOIT être refusé avec le message de `find_unsafe_fanout`.
- **S03-R9** — La même question posée deux fois, avec deux valeurs de filtre différentes, DOIT
  donner une seule candidate avec `request_count == 2` et deux runs.
- **S03-R10** — Aucune valeur de filtre NE DOIT apparaître dans le store, dans les attributs de
  span ou dans `AgentResponse.evidence.to_dict()` par défaut. Le test utilise des canaris.
- **S03-R11** — L'evidence de la réponse DOIT porter `certification="CANDIDATE"` et
  `schema_version == "2"`, et `_certification_text` (S00) NE DOIT PAS dire « certifié le ».
- **S03-R12** — Un échec d'exécution DOIT donner `EXECUTION_FAILED`, avec un run
  `error_type=<nom de classe>` et jamais le message du backend.
- **S03-R13** — Le `trace_id` du span `skifer.candidate.build` DOIT être enregistré sur la
  candidate et sur le run quand le tracing est actif, et `None` sinon. Le comportement métier
  reste identique dans les deux cas : comparaison des sorties avec `NoOpTracer` et avec
  `InMemoryTracer`.
- **S03-R14** — Le gate de certification de l'environnement DOIT s'appliquer aux tables sources :
  en `enforce`, une source non certifiée donne la décision du gate existant, jamais une
  exécution.

## 4. Fichiers

**À créer** : `src/skifer/agentic/candidate_agent.py`, `tests/test_candidate_agent.py`, et
`tests/fakes/fake_llm.py` s'il n'existe pas déjà un LLM factice réutilisable. Il faut vérifier
d'abord ce que `tests/test_semantic_builder*.py` utilise.

**À modifier** : `src/skifer/agentic/models.py` (`candidate_id`, et le mode `candidate` /
`candidate_refused` dans la docstring), `CHANGELOG.md`.

## 5. Slices

- **S03.1 Contexte et prompt.** Recherche, contexte borné, prompt, parsing JSON fermé, refus
  `NO_TABLE_FOUND`.
- **S03.2 Validation et boucle de correction.** Étapes 4.1 à 4.8, chemin `sql` par le mapper,
  refus typés.
- **S03.3 Exécution et persistance.** Compilation, exécution bornée, `result_hash`,
  fingerprint, dédoublonnage, runs, événements.
- **S03.4 Réponse et tracing.** `AgentResponse(mode="candidate")`, evidence v2, span, gate de
  certification.

## 6. Tests d'acceptation

| Exigence | Test (`tests/test_candidate_agent.py::…`) |
|---|---|
| S03-R1 | `test_only_compiled_sql_reaches_backend` |
| S03-R2 | `test_correction_loop_recovers_on_second_attempt`, `test_three_invalid_attempts_refuse` |
| S03-R3 | `test_forbidden_keys_and_raw_sql_rejected[allow_raw_sql=True|False]` |
| S03-R4 | `test_unknown_table_refused_before_compile` |
| S03-R5 | `test_lag_is_blocking_residue`, `test_non_blocking_residue_kept_on_candidate` |
| S03-R6 | `test_sql_path_disabled_without_analyzer_extra` |
| S03-R7 | `test_no_table_refusal_makes_no_llm_call` |
| S03-R8 | `test_fanout_refused_with_planner_message` |
| S03-R9 | `test_same_structure_different_values_dedupes` |
| S03-R10 | `test_no_filter_value_leaks_anywhere` |
| S03-R11 | `test_response_evidence_is_candidate_v2` |
| S03-R12 | `test_execution_failure_records_class_name_only` |
| S03-R13 | `test_trace_id_recorded_and_behaviour_tracing_independent` |
| S03-R14 | `test_certification_gate_applies_to_candidate_sources` |

Le LLM est toujours factice et ne fait aucun appel réseau. L'exécution passe par le fake
backend, avec un seul test sur la fixture `spark` pour la chaîne réelle `compile_select`, puis
`EXPLAIN`, puis exécution.

## 7. Erreurs

Toutes les erreurs attendues sont des refus typés (`CandidateRefusal`), pas des exceptions. Une
exception ne remonte que pour un store ou un provider LLM inutilisable. Dans ce cas, c'est
l'exception existante du composant qui remonte, jamais une nouvelle classe.

## 8. Hors périmètre

- le branchement dans le hub (S04) ;
- la validation, la promotion et toute transition au-delà de `created`/`rejected` (S05) ;
- les embeddings (S08) ;
- `business_rules` dans une candidate : un `business_rules_refs` reste vide en v1, et la
  métrique ne s'appuie que sur la grammaire déclarative.

## 9. Definition of Done

- les 14 exigences ont leur test vert, avec un LLM factice ;
- une question hors périmètre sur les données d'`examples/` produit une candidate de bout en bout
  dans un test Spark réel ;
- `grep -n "spark.sql\|\.sql(" src/skifer/agentic/candidate_agent.py` ne montre aucun appel
  direct, l'exécution passant par le backend.
