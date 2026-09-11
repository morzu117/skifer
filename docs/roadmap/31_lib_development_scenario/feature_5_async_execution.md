# Plan 31 — Feature 5 : Exécution asynchrone (`services/execution.py`)

**But** : poser un `ExecutionService` **transport-neutre, fail-closed, sans Spark à la frontière** qui (1) ouvre et surveille **une** session Spark par configuration active, (2) soumet et pilote des **jobs** de pipeline en arrière-plan (`preview | run | full_refresh | check`) avec **un seul job actif par projet en v1**, et (3) renvoie des **résultats JSON-natifs bornés** (`ResultView`) portant le rapport qualité et la décision de publication. L'identité d'audit est **de bout en bout** : le `job_id` du job **est** le `run_id` frappé par le pipeline.

Arbitrages §7 (2026-09-11) **appliqués** dans ce document :
- **§7.7** : **UN seul job actif par projet** en v1 — la concurrence est **refusée proprement** (erreur explicite `JobConflict`), jamais ignorée ni mise en file. Une file d'attente est un ajout **ultérieur non cassant** (nouveau champ d'état + politique d'admission ; la surface publique `submit/status/cancel/logs/result` ne change pas). Dit tel quel dans « Risques & pièges ».
- **Identité** : `job_id == run_id` frappé par le pipeline (`SkiferEngine.run_*`). Comme `PublicationCoordinator.start_publication_run` valide `str(UUID(run_id))`, **le `job_id` est un `uuid4()`**.
- **Session** : échec de session **exposé** (`state="failed"` + `cause`), **jamais levé**. TTL + annulation du job actif à la fermeture de session.
- **Frontière** : aucun objet Spark ne franchit `services/`. `ResultView.rows` ≤ 1000, JSON-natifs (`services/serialization.py`), `total` exact.

**Discipline de commit** : *une slice = un commit* `feat(plan31-5.M): …` + tests + une entrée `CHANGELOG.md` sous `## [Unreleased]`. **Ne jamais bumper la version** de `pyproject.toml`. Le dépôt est sur `release: 2.1.0` et **n'a pas** de section `[Unreleased]` — **la créer dans la première slice qui touche `CHANGELOG.md` (5.1)**.

**Dépendances amont** (features 31.1.1 et 31.1.4, déjà planifiées) :
- `services/context.py` : `RequestContext`, `require_scope`, `SCOPE_EXECUTE_RUN = "execute:run"`, `SCOPE_CONTRACTS_READ`, hiérarchie d'erreurs (`AgentReadyDataError`, `ScopeDenied`, `InvalidRequest`, `ResourceNotFound`, `ResourceUnavailable`, `SerializationError`), `HARD_MAX_QUERY_ROWS = 1_000`.
- `services/serialization.py` : `row_to_json(row, index)`, `to_json_value(value, field_name)`.
- `services/governance.py` : `GovernanceService` (lecture bornée de quarantaine, `read_quarantine`). La slice 5.3 lui **ajoute** un lecteur `get_run` par `run_id`.

---

## État actuel du code

Chemins absolus sous `src/skifer/`. Signatures citées telles qu'elles existent aujourd'hui (HEAD = `release: 2.1.0`).

### `core/core.py` — `SkiferEngine`
- **Constructeur** (ligne 117) :
  ```python
  def __init__(self, spark=None, config_path=None, force_env=None, monitor=None, certification_store=None): ...
  ```
  - Session Spark obtenue via `from skifer.spark_factory import get_spark_session` (ligne 167) : `self.spark, self._spark_mode = get_spark_session()` sauf si `spark` est fourni (`self._spark_mode = "provided"`). `self.is_local = (self._spark_mode == "local")`.
  - `force_env` : après chargement config, force `self.env` / `self.db` ; lève `ValueError` si l'env n'existe pas dans `config.yaml` (lignes 217-230).
  - Détection utilisateur/sandbox : `self.current_user = self._get_clean_username()`, `self.is_job_execution = self._is_running_as_job()`, `self.schema_suffix = f"_{self.current_user}"` **sauf** prod ou job (lignes 241-256).
- **Propriétés déléguées** (via `self._context: ExecutionContext`) : `env`, `db`, `current_user`, `is_job_execution`, `is_local`, `schema_suffix`, `config`, `context`.
- **`context.is_production`** : `ExecutionContext.is_production` → `bool(env_config().get("is_production", False))` (context.py ligne 53).
- **Où le `run_id` est frappé** — **le point le plus important de cette feature** :
  ```python
  # core/core.py:925
  def run_process_to_table(self, schema_dict, target_layer, target_table_name, intermediate_mode="inline"):
      run_id = str(uuid4())                       # ← frappé ICI, sur le chemin métier
      return self._traced_pipeline_run(
          run_id,
          lambda: self._patterns.run_process_to_table(
              schema_dict, target_layer, target_table_name,
              intermediate_mode=intermediate_mode, run_id=run_id,
          ),
      )

  # core/core.py:942
  def run_from_yaml(self, yaml_path, target_layer, target_table_name=None, params=None):
      run_id = str(uuid4())                       # ← frappé ICI
      return self._traced_pipeline_run(
          run_id,
          lambda: self._patterns.run_from_yaml(
              yaml_path, target_layer, target_table_name, params, run_id=run_id,
          ),
      )
  ```
  **`_traced_pipeline_run` (ligne 901) renvoie `business_call()`**, et `PipelinePatterns.run_process_to_table` / `run_from_yaml` renvoient **`None`** (patterns.py). **Donc `run_id` est frappé et propagé jusqu'au store de certification, mais N'EST PAS retourné à l'appelant aujourd'hui.** La slice 5.2 doit *exposer* ce `run_id` (le laisser être **injecté** puis **retourné**), sans changer le comportement métier.
- **`full_refresh(self, target_layer, target_table_name, checkpoint=None, materialization="streaming_table")`** (ligne 733) : purge checkpoint + drop table (ou drop MV). **Ne frappe pas de `run_id`**, ne persiste pas de certification. Renvoie `None`.
- **`describe_schema(self, schema_dict, print_summary=True) -> dict`** (ligne 979) : preview **sans Spark** (parse IR, imprime + renvoie une structure). `mode = "JOB"|"PROD"|"INTERACTIVE"`.
- **`process_schema(self, schema_dict, dataframes_in=None, intermediate_mode="inline")`** (ligne 881) : renvoie un **DataFrame** (objet Spark — ne doit jamais franchir `services/`).
- **`monitor`** / **`certification_store`** : attributs optionnels posés au `__init__` ; `monitor.check_from_schema(fqn, schema_dict, raise_on_critical) -> MonitorReport`.
- Helpers utiles pour composer un FQN cible **sans lire** : `get_target_schema(base_layer) -> str` (applique le suffixe sandbox), `_build_fqn(schema, table) -> str`.

### `core/patterns.py` — `PipelinePatterns.run_process_to_table` (ligne 48)
- Signature : `(self, schema_dict, target_layer, target_table_name, intermediate_mode="inline", run_id=None) -> None`.
- Si `schema_dict.get("data_product")` : exige `certification_store` **et** `monitor` (sinon `ValueError`), refuse streaming/JDBC/MV, puis :
  ```python
  coordinator = PublicationCoordinator(e._get_backend(), e.monitor, e.certification_store)
  result = coordinator.publish(df, fqn, schema_dict, definition, run_id=run_id)  # PublicationResult
  if result.state == "QUARANTINED": raise DataQualityError(result.report)
  ```
- Sinon écrit via `_write_dataframe` puis, si `monitor`, `report = e.monitor.check_from_schema(fqn, schema_dict, raise_on_critical=True)`. **Le report/décision ne sont pas retournés** (seulement loggés).

### `spark_factory.py` — `get_spark_session(app_name="Skifer", warehouse_dir=None) -> (SparkSession, mode)`
- Ordre de détection : **(1)** `SparkSession.getActiveSession()` → `"databricks_notebook"` ; **(2)** `DATABRICKS_HOST`+`TOKEN`+`CLUSTER_ID` → Databricks Connect v2 → `"databricks_connect"` ; **(3)** local PySpark + Delta → `"local"`. `mode` ∈ `{"databricks_notebook","databricks_connect","local"}` ; `SkiferEngine.__init__` ajoute `"provided"` quand `spark=` est passé.

### `core/config.py` — `ConfigurationManager`
- `__init__(config_path=None, backend=None)` : charge YAML, `current_env_name = _detect_environment()`, `db_prefix = get_value("catalog") or None`. Un `catalog` nul = env local sélectionné immédiatement.
- Détection prod / suffixe sandbox : **pas** dans `ConfigurationManager` mais dans `ExecutionContext` (voir ci-dessous) ; `SkiferEngine` les expose via `env`, `db`, `is_production`, `schema_suffix`, `current_user`.

### `core/context.py` — `ExecutionContext` (source des champs de `SessionView`)
```python
env: str = ""                 # upper-case ("LOCAL"|"DEV"|"QA"|"PROD")
db: str | None = None         # catalogue UC, None en local
current_user: str = ""        # suffixe utilisateur ("jdoe")
is_job_execution: bool = False
is_local: bool = False
schema_suffix: str = ""       # "_jdoe" en interactif, "" en prod/job
@property
def is_production(self) -> bool: return bool(self.env_config().get("is_production", False))
```

### `observability/monitor.py` — `MonitorReport` (ligne 24)
```python
@dataclass
class MonitorReport:
    table: str
    results: list[CheckResult]
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    def has_critical_failures(self) -> bool: ...
    def failures(self) -> list[CheckResult]: ...
    def summary(self) -> dict[str, Any]: ...   # {table, timestamp(iso), total_checks, passed,
                                               #  failed, critical_failures, status_counts, status}
```
`summary()` est **déjà JSON-natif** (str/int/dict) — c'est la forme que `ResultView.monitor_report` transporte.

### `observability/publication.py`
```python
@dataclass(frozen=True) class PublicationRun:    run_id; target_fqn; staging_fqn; state
@dataclass(frozen=True) class PublicationResult: run: PublicationRun; report: MonitorReport | None; state: str  # "PROMOTED"|"QUARANTINED"
def start_publication_run(target_fqn, definition, store, run_id=None) -> PublicationRun:
    run_id = run_id or str(uuid4())
    run_id = str(UUID(run_id))          # ← VALIDE que run_id est un UUID, sinon ValueError("run_id must be a UUID.")
```
**Conséquence dure pour 5.2** : pour un schéma `data_product`, le `run_id` injecté **doit** être un `uuid4()`. Donc le `job_id` de l'`ExecutionService` **est** un `str(uuid4())`.

### `observability/certification_store.py` — Protocol `CertificationStore` (lecture par `run_id`)
```python
@dataclass class RunEvent: run_id: str; ...; state: str; ...; target_fqn: str | None = None; staging_fqn=...; quarantine_fqn=...
def append_run_event(event: RunEvent) -> None
def get_run(run_id: str) -> RunEvent | None            # ← décision de publication LISIBLE PAR run_id
def get_check_results(run_id: str) -> list[StoredCheckResult]
def get_certification(dataset, consumer_class="default") -> Certification
def list_history(dataset, limit=50) -> list[RunEvent]
```
`store.get_run(job_id).state` donne `"PROMOTED"`/`"QUARANTINED"` **par `run_id` seul** — pas besoin de connaître `target_fqn`. C'est la source de `ResultView.publication_decision` (slice 5.3).

### Convention de tests
Un fichier par module `tests/test_<module>.py`. LLM mocké. Fixture `spark` (`tests/conftest.py`) requise **uniquement** pour un test qui exécute réellement un job (Delta local réel : `preview`/`run`/`check` de bout en bout, égalité `run_id`, sérialisation). La logique concurrence/annulation/TTL/session se teste avec un **faux `engine_factory`** (aucun Spark). Aucun DataFrame ne franchit `services/`.

---

## Slice 31.5.1 — `ExecutionService.connect` : une session par config active, états `connecting/ready/failed`

**Objectif** : construire l'`ExecutionService` et sa gestion de session. `connect(config_path, force_env)` renvoie une `SessionView`. **Une** session par configuration active ; changer de config **ferme** l'ancienne. États `connecting → ready` (ou `→ failed` avec `cause`). **Un échec de construction du moteur est EXPOSÉ dans la vue, jamais levé.**

### Fichiers
- `src/skifer/services/execution.py` — **création** : `SessionView`, `ExecutionService` (session uniquement dans cette slice ; jobs en 5.2), `_default_engine_factory`.
- `CHANGELOG.md` — **modification** : **créer** `## [Unreleased]` (absente aujourd'hui), y ajouter l'entrée 5.1.

### Signatures Python
```python
# services/execution.py
from __future__ import annotations
import threading, time
from dataclasses import dataclass, field
from typing import Any, Callable
from skifer.services.context import (
    RequestContext, require_scope, SCOPE_EXECUTE_RUN,
    InvalidRequest, ResourceNotFound, ResourceUnavailable,
)

EngineFactory = Callable[[str | None, str | None], Any]  # (config_path, force_env) -> SkiferEngine-like

@dataclass(frozen=True)
class SessionView:
    session_id: str
    mode: str                 # "local"|"databricks_connect"|"databricks_notebook"|"provided"
    env: str                  # engine.env  ("" si state=="failed")
    catalog: str | None       # engine.db
    user: str                 # engine.current_user
    sandbox_suffix: str       # engine.schema_suffix ("" quand pas de sandbox)
    is_production: bool       # engine.context.is_production
    state: str                # "connecting"|"ready"|"failed"
    cause: str | None = None  # "<ClassName>: <message court>" quand state=="failed", sinon None
    config_path: str | None = None
    force_env: str | None = None
    def to_dict(self) -> dict[str, Any]:
        return {
            "session_id": self.session_id, "mode": self.mode, "env": self.env,
            "catalog": self.catalog, "user": self.user, "sandbox_suffix": self.sandbox_suffix,
            "is_production": self.is_production, "state": self.state, "cause": self.cause,
            "config_path": self.config_path, "force_env": self.force_env,
        }

def _default_engine_factory(config_path: str | None, force_env: str | None):
    """Construit un SkiferEngine câblé pour la publication certifiée + l'historique.
    Isolé pour être remplaçable en test par un faux factory (aucun Spark)."""
    from skifer.core.core import SkiferEngine
    from skifer.observability.monitor import DataMonitor
    from skifer.observability.history import SqliteHistoryStore
    from skifer.observability.certification_store import SqliteCertificationStore
    store = SqliteCertificationStore()
    engine = SkiferEngine(config_path=config_path, force_env=force_env, certification_store=store)
    engine.monitor = DataMonitor(engine.backend, history_store=SqliteHistoryStore())
    return engine

class ExecutionService:
    def __init__(
        self,
        *,
        engine_factory: EngineFactory | None = None,
        clock: Callable[[], float] = time.monotonic,
        session_ttl_seconds: float = 3600.0,
        job_ttl_seconds: float = 3600.0,
    ):
        self._engine_factory = engine_factory or _default_engine_factory
        self._clock = clock
        self._session_ttl = session_ttl_seconds
        self._job_ttl = job_ttl_seconds
        self._lock = threading.RLock()
        self._engine = None                       # objet moteur vivant (jamais exposé)
        self._session: SessionView | None = None
        self._session_key: tuple[str | None, str | None] | None = None
        self._last_access: float = 0.0
        # (5.2) : registre de job à slot unique
        self._job = None                          # _Job | None

    # --- Session (5.1) ---
    def connect(self, ctx: RequestContext, config_path: str | None = None,
                force_env: str | None = None) -> SessionView:
        require_scope(ctx, SCOPE_EXECUTE_RUN)
        ...

    def session(self, ctx: RequestContext) -> SessionView | None:
        require_scope(ctx, SCOPE_EXECUTE_RUN)
        ...

    def close(self, ctx: RequestContext) -> None:
        require_scope(ctx, SCOPE_EXECUTE_RUN)
        ...

    # --- interne ---
    def _view_from_engine(self, engine, session_id, config_path, force_env) -> SessionView: ...
    def _close_locked(self) -> None: ...          # annule le job actif (5.2) puis ferme la session
    def _sweep_ttl_locked(self) -> None: ...       # ferme si expirée (clock injectée)
```

### Comportement & règles
- **Scope** : toutes les méthodes publiques appellent `require_scope(ctx, SCOPE_EXECUTE_RUN)` en premier — `ScopeDenied` sinon (fail-closed, hérité de `services/context`).
- **Une session par config active** : la clé de session est `(config_path, force_env)`. `connect(...)` :
  1. `with self._lock:` `self._sweep_ttl_locked()`.
  2. Si une session existe avec **la même** clé et `state == "ready"` → **réutilise** (rafraîchit `_last_access`, renvoie la `SessionView` existante).
  3. Si une session existe avec une clé **différente** → `self._close_locked()` (ferme l'ancienne, annule son job actif) **avant** de construire la nouvelle. **C'est la recréation sur changement de config.**
  4. Construit via `self._engine_factory(config_path, force_env)` dans un `try/except`.
- **États** :
  - Avant appel factory, la vue logique est `connecting` (utile si on rendait la construction asynchrone ; en v1 la construction est synchrone dans `connect`, donc l'appelant voit directement `ready` ou `failed`. On expose néanmoins `state="connecting"` transitoirement dans la vue stockée pendant la construction pour ne pas mentir si un autre thread lit `session()` en concurrence — la vue `connecting` porte `env=""`).
  - Succès → `SessionView(state="ready", …champs moteur…)`.
  - **Échec EXPOSÉ, jamais levé** : toute exception du factory est capturée ; on stocke et renvoie `SessionView(state="failed", cause=f"{type(exc).__name__}: {exc}"[:200], env="", catalog=None, user="", sandbox_suffix="", is_production=False, mode="unknown", …)`. `connect` **ne relève pas**. Le moteur vivant reste `None`.
- **`_view_from_engine`** mappe : `mode = getattr(engine, "_spark_mode", "unknown")`, `env = engine.env`, `catalog = engine.db`, `user = engine.current_user`, `sandbox_suffix = engine.schema_suffix or ""`, `is_production = engine.context.is_production`.
- **`session()`** : `_sweep_ttl_locked()` puis renvoie la vue courante (ou `None` si aucune / expirée). Ne recrée jamais.
- **`close()`** : `_close_locked()` — idempotent. Ferme la session Spark **seulement si `mode == "local"`** (`engine.spark.stop()`), jamais une session `provided`/`databricks_notebook` partagée. Remet `_engine=_session=_session_key=None`.
- **TTL** : `_sweep_ttl_locked` ferme la session si `self._clock() - self._last_access > self._session_ttl`. Balayage **paresseux** (à chaque appel public), horloge **injectée** → déterministe en test, pas de thread reaper.
- **Aucun objet Spark exposé** : `SessionView` ne contient que des scalaires. Le moteur/`SparkSession` restent internes.

### Cas de test — `tests/test_services_execution.py` (Spark-free ; faux `engine_factory`)
- `test_connect_requires_execute_scope` : `connect(ctx_sans_execute_run)` lève `ScopeDenied`.
- `test_connect_returns_ready_view` : faux factory renvoyant un `FakeEngine(_spark_mode="local", env="DEV", db="cat", current_user="jdoe", schema_suffix="_jdoe", context.is_production=False)` → `SessionView(state="ready", mode="local", env="DEV", catalog="cat", user="jdoe", sandbox_suffix="_jdoe", is_production=False)`.
- `test_connect_failure_is_exposed_not_raised` : factory qui `raise RuntimeError("no catalog")` → `connect` **ne lève pas** ; renvoie `state="failed"`, `cause` commence par `"RuntimeError: no catalog"`, `env == ""`.
- `test_session_recreated_on_config_change` : `connect(config_path="a")` → `id1` ; `connect(config_path="b")` → nouvelle vue, `session_id != id1`, et le premier `FakeEngine.closed is True` (prouve la fermeture de l'ancienne).
- `test_same_config_reuses_session` : deux `connect` mêmes args → **même** `session_id`, factory appelé **une** fois.
- `test_close_is_idempotent_and_stops_local_only` : `close()` deux fois sans erreur ; `FakeEngine(mode="local").spark.stopped is True` ; un `FakeEngine(mode="provided")` → `spark.stopped is False`.
- `test_session_ttl_expiry` : clock injectée ; après `connect`, avancer au-delà de `session_ttl` → `session()` renvoie `None` et l'ancien engine est fermé.

### Commit
```
feat(plan31-5.1): ExecutionService session lifecycle — one session per config, failure exposed not raised
```
CHANGELOG `[Unreleased]` (créer la section) :
```
- **Plan 31 (5.1)** — `services/execution.py` : `ExecutionService.connect/session/close` et `SessionView(session_id, mode, env, catalog, user, sandbox_suffix, is_production, state, cause)`. Une seule session par config active (recréée au changement de `config_path`/`force_env`), TTL paresseux à horloge injectée, fermeture qui n'arrête Spark qu'en mode local. Un échec de session est exposé (`state="failed"` + `cause`), jamais levé. Sous scope `execute:run`.
```

### DoD
- [ ] `SessionView` scalaire, aucun objet Spark exposé.
- [ ] Recréation sur changement de config prouvée par test.
- [ ] Échec exposé (jamais levé) prouvé par test.
- [ ] TTL déterministe (horloge injectée).
- [ ] Section `## [Unreleased]` créée dans `CHANGELOG.md` ; `ruff check src/` propre.

---

## Slice 31.5.2 — Jobs : `submit/status/cancel/logs`, un seul job actif, `job_id == run_id`

**Objectif** : soumettre un job de pipeline en arrière-plan (`preview | run | full_refresh | check`), le surveiller (`status`), l'annuler (`cancel`), lire ses logs (`logs`). **Un seul job actif par projet** — la concurrence est **refusée** (`JobConflict`). TTL + annulation du job à la fermeture de session. **Le `job_id` du job EST le `run_id` frappé par le pipeline.**

### Fichiers
- `src/skifer/services/execution.py` — **modification** : `_Job`, `JobStatusView`, `LogsView`, exceptions `JobConflict`/`NoActiveSession`, méthodes `submit/status/cancel/logs`, worker thread, job groups Spark, intégration `_close_locked`.
- `src/skifer/core/core.py` — **modification** : `run_process_to_table` et `run_from_yaml` **acceptent** un `run_id: str | None = None` (mint si absent) et **retournent** ce `run_id` (rétrocompatible : renvoyaient `None`). `full_refresh` accepte aussi `run_id` (logué, non persistant).

### Signatures Python
```python
# core/core.py — modifications (le corps métier ne change pas ; on injecte + on retourne le run_id)
def run_process_to_table(self, schema_dict, target_layer, target_table_name,
                         intermediate_mode="inline", run_id=None):
    run_id = run_id or str(uuid4())
    self._traced_pipeline_run(
        run_id,
        lambda: self._patterns.run_process_to_table(
            schema_dict, target_layer, target_table_name,
            intermediate_mode=intermediate_mode, run_id=run_id),
    )
    return run_id

def run_from_yaml(self, yaml_path, target_layer, target_table_name=None, params=None, run_id=None):
    run_id = run_id or str(uuid4())
    self._traced_pipeline_run(
        run_id,
        lambda: self._patterns.run_from_yaml(
            yaml_path, target_layer, target_table_name, params, run_id=run_id),
    )
    return run_id

def full_refresh(self, target_layer, target_table_name, checkpoint=None,
                 materialization="streaming_table", run_id=None):
    # run_id purement pour corrélation/log (full_refresh ne persiste pas de certification)
    ...  # corps inchangé
    return run_id or str(uuid4())
```
```python
# services/execution.py — ajouts
from concurrent.futures import Future

class JobConflict(InvalidRequest): ...          # concurrence refusée proprement
class NoActiveSession(ResourceUnavailable): ...  # submit sans connect préalable réussi

_VALID_KINDS = ("preview", "run", "full_refresh", "check")

@dataclass
class _Job:                                     # état interne (jamais exposé tel quel)
    job_id: str                                 # == run_id (uuid4)
    kind: str
    path: str
    params: dict
    state: str                                  # "running"|"succeeded"|"failed"|"cancelled"
    submitted_at: float
    started_at: float | None = None
    finished_at: float | None = None
    error: str | None = None
    logs: list[tuple[float, str, str]] = field(default_factory=list)  # (ts, level, message)
    result: "ResultView | None" = None          # rempli par le worker (voir 5.3)
    cancel_requested: bool = False
    _thread: "threading.Thread | None" = None

@dataclass(frozen=True)
class JobStatusView:
    job_id: str
    run_id: str                                 # identique à job_id (identité de bout en bout)
    kind: str
    state: str                                  # "running"|"succeeded"|"failed"|"cancelled"
    submitted_at: float
    started_at: float | None
    finished_at: float | None
    error: str | None
    def to_dict(self) -> dict[str, Any]: ...

@dataclass(frozen=True)
class LogsView:
    job_id: str
    entries: tuple[dict[str, Any], ...]         # [{"ts": float, "level": str, "message": str}]
    next_offset: int                            # à repasser en `after` pour la suite
    def to_dict(self) -> dict[str, Any]: ...

class ExecutionService:                          # (suite)
    def submit(self, ctx: RequestContext, kind: str, path: str,
               params: dict | None = None) -> str:
        require_scope(ctx, SCOPE_EXECUTE_RUN)
        ...   # renvoie job_id (== run_id)

    def status(self, ctx: RequestContext, job_id: str) -> JobStatusView:
        require_scope(ctx, SCOPE_EXECUTE_RUN); ...

    def cancel(self, ctx: RequestContext, job_id: str) -> JobStatusView:
        require_scope(ctx, SCOPE_EXECUTE_RUN); ...

    def logs(self, ctx: RequestContext, job_id: str, after: int = 0) -> LogsView:
        require_scope(ctx, SCOPE_EXECUTE_RUN); ...
```

### Comportement & règles
- **Précondition session** : `submit` exige une session `ready` (`self._session.state == "ready"` et `self._engine is not None`) sinon `NoActiveSession`. `_sweep_ttl_locked()` d'abord.
- **UN seul job actif (arbitrage §7.7)** : sous `self._lock`, si `self._job is not None and self._job.state == "running"` → **`raise JobConflict(f"A job is already running (job_id={self._job.job_id}); v1 allows one active job per project.")`**. **Refus propre, jamais file d'attente ni écrasement silencieux.** Un job terminé (`succeeded/failed/cancelled`) est remplacé au `submit` suivant (le précédent reste lisible via `status/logs/result` jusqu'à son remplacement ou son expiration TTL).
- **`job_id == run_id`** : `job_id = str(uuid4())` frappé par l'`ExecutionService` ; injecté dans le moteur. Pour `run`/`full_refresh` l'`ExecutionService` **passe `run_id=job_id`** aux méthodes moteur, qui le retournent — le worker **assert** que le retour == `job_id`. Pour un schéma `data_product`, `start_publication_run` re-valide l'UUID : comme `job_id` est un `uuid4()`, la validation passe et **le `run_id` persisté dans le store de certification == `job_id`** (identité de bout en bout).
- **Mapping `kind` → appel moteur** (dans le worker, `params` porte les clés réservées `target_layer`, `target_table`, `limit`, plus les `params` de `run_from_yaml`) :
  | kind | appel | notes |
  |---|---|---|
  | `run` | `engine.run_from_yaml(path, params["target_layer"], params.get("target_table"), params.get("run_params"), run_id=job_id)` | écrit la table ; monitor/publication déclenchés dans patterns |
  | `full_refresh` | `engine.full_refresh(params["target_layer"], params["target_table"], run_id=job_id)` | purge/drop |
  | `preview` | `df = engine.process_schema(load_schema(path, params=engine.default_params)); rows=df.limit(N).collect()` puis sérialisation (5.3) | **n'écrit rien**, borné ; N = `min(params.get("limit",100), HARD_MAX_QUERY_ROWS)` |
  | `check` | `engine.monitor.check_from_schema(fqn, schema_dict, raise_on_critical=False) -> MonitorReport` | `fqn` recomposé via `engine._build_fqn(engine.get_target_schema(layer), table)` |
  - `submit` valide `kind in _VALID_KINDS` (sinon `InvalidRequest`) et la présence des clés `params` requises selon le `kind` (`target_layer`/`target_table` pour `run`/`full_refresh`/`check`), **avant** de démarrer le thread — fail-fast synchrone.
- **Modèle d'exécution : thread in-process** (voir « Ordre & dépendances »). Le worker : pose `started_at`, `state="running"`, **définit un job group Spark** pour l'annulation (best-effort, voir cancel), exécute le mapping, **collecte et sérialise le résultat côté worker** (aucun DataFrame stocké), pose `result` (5.3), puis `state="succeeded"` / `finished_at`. Toute exception → `state="failed"`, `error=f"{type(exc).__name__}: {exc}"[:500]`, log niveau `"error"`. Un `DataQualityError` (quarantaine) n'est **pas** un échec technique : le worker le capture, pose `state="succeeded"` **et** enregistre la décision `QUARANTINED` dans le `ResultView` (5.3) — un run quarantiné a bien tourné.
- **Job group / annulation** : avant l'exécution, `sc = getattr(engine.spark, "sparkContext", None)` ; si présent (Spark classique/local) `sc.setJobGroup(job_id, kind, interruptOnCancel=True)`. `cancel(job_id)` : sous lock, si le job cible est `running` → `job.cancel_requested=True`, `state="cancelled"`, `finished_at=now`, log `"cancelled"`, puis best-effort `sc.cancelJobGroup(job_id)` (classique) **ou** `engine.spark.interruptTag(job_id)` (Spark Connect, si l'API existe). Si le job est déjà terminé → **no-op**, renvoie le `JobStatusView` courant. La garantie v1 : **l'état logique bascule toujours `cancelled` immédiatement** ; l'interruption JVM est best-effort (cf. Risques).
- **`logs(after)`** : renvoie `entries[after:]` sérialisés + `next_offset = len(entries)`. Curseur strictement croissant, jamais réinterprété.
- **`status`/`logs`/`cancel` sur `job_id` inconnu** → `ResourceNotFound`. On ne garde que le **dernier** job (slot unique) + éventuellement le job terminé courant ; un `job_id` qui n'est ni l'actif ni le dernier connu → `ResourceNotFound`.
- **Fermeture de session annule le job** : `_close_locked()` (5.1) appelle `cancel`-équivalent sur le job actif avant d'arrêter le moteur. TTL de job : `_sweep_ttl_locked` marque `cancelled` un job `running` orphelin au-delà de `job_ttl` (défense en profondeur ; un job local ne devrait pas durer si la session est fermée).
- **Aucun objet Spark ne franchit la frontière** : `submit` renvoie une `str` ; `status/cancel` des `JobStatusView` scalaires ; `logs` des dicts. Le `Future`/`Thread`/`SparkSession` restent internes.

### Cas de test — `tests/test_services_execution.py`
Fakes (Spark-free) pour concurrence/annulation :
- `test_submit_requires_active_session` : `submit` sans `connect` réussi → `NoActiveSession`.
- `test_submit_requires_execute_scope` : `ScopeDenied` sans `execute:run`.
- `test_invalid_kind_refused` : `submit(kind="delete")` → `InvalidRequest`.
- `test_one_active_job_refused_cleanly` : faux engine dont `run_from_yaml` bloque sur un `threading.Event` ; premier `submit` OK, second `submit` → **`JobConflict`** (message cite le `job_id` actif) ; débloquer, le premier finit `succeeded`.
- `test_cancellation_flips_state_and_calls_cancel` : faux engine bloquant + faux `sparkContext` enregistrant `cancelJobGroup(job_id)` ; `cancel(job_id)` → `state=="cancelled"`, `cancelJobGroup` appelé avec le bon `job_id`.
- `test_cancel_finished_job_is_noop` : job court terminé → `cancel` renvoie `state=="succeeded"` sans erreur.
- `test_logs_cursor_advances` : worker émet 3 logs → `logs(after=0).next_offset==3` ; `logs(after=3).entries==()`.
- `test_status_unknown_job_not_found` : `status("nope")` → `ResourceNotFound`.
- `test_close_cancels_active_job` : job bloquant en cours → `close()` → `status` du job `cancelled`, engine fermé.
- **Nécessite `spark` fixture** — `test_run_job_run_id_equals_pipeline_run_id` : `engine_factory` réel (Delta local) ; `submit(kind="run", path=<yaml sans data_product>, params={target_layer, target_table})` ; attendre la fin (`status` → `succeeded`) ; asserter `status.run_id == job_id`. Pour un schéma **`data_product`** : après complétion, `store.get_run(job_id) is not None` et `store.get_run(job_id).run_id == job_id` (identité persistée).
- **Nécessite `spark` fixture** — `test_full_refresh_job_succeeds` : `submit(kind="full_refresh", …)` sur une table existante → `succeeded`.

> Attente déterministe en test : boucler `status(...).state` avec un petit timeout, ou injecter un `engine_factory` synchrone + débloquer l'`Event` avant de lire. Ne jamais `sleep` en dur.

### Commit
```
feat(plan31-5.2): background pipeline jobs — one active job per project, cancel, identical run_id
```
CHANGELOG :
```
- **Plan 31 (5.2)** — `ExecutionService.submit/status/cancel/logs` : jobs de pipeline en arrière-plan (`preview|run|full_refresh|check`), **un seul job actif par projet** (concurrence refusée par `JobConflict` — la file d'attente est un ajout ultérieur non cassant), annulation par job group Spark (best-effort) + bascule d'état immédiate, logs à curseur, annulation à la fermeture de session. `SkiferEngine.run_process_to_table`/`run_from_yaml` acceptent et **retournent** désormais le `run_id` ; le `job_id` **est** ce `run_id` (identité d'audit de bout en bout, `run_id` persisté == `job_id` en publication certifiée).
```

### DoD
- [ ] Concurrence refusée par `JobConflict` (test), jamais mise en file ni écrasée.
- [ ] `job_id == run_id` retourné par le moteur (test) ; `run_id` persisté == `job_id` pour un `data_product` (test spark).
- [ ] Annulation : bascule d'état immédiate + `cancelJobGroup` best-effort (test).
- [ ] Fermeture de session annule le job actif (test).
- [ ] `run_process_to_table`/`run_from_yaml` restent rétrocompatibles (retour ignoré = comportement inchangé) ; suite existante verte.
- [ ] Aucun objet Spark exposé par `submit/status/cancel/logs`.

---

## Slice 31.5.3 — Résultats : `result(job_id) -> ResultView`, JSON-natif borné, quarantaine bornée

**Objectif** : exposer le résultat d'un job : lignes ≤ 1000 **JSON-natives** (`services/serialization`), `total` **exact**, `schema`, `monitor_report` (via `summary()`), `publication_decision` (via le store, par `run_id`), et une **lecture bornée de quarantaine** pour un run quarantiné.

### Fichiers
- `src/skifer/services/execution.py` — **modification** : `ResultView`, `result(...)`, et le code du worker (5.2) qui **construit** le `ResultView` côté Spark (collect + sérialisation) avant de le stocker.
- `src/skifer/services/governance.py` — **modification** : ajouter `GovernanceService.get_run(ctx, run_id) -> RunOutcomeView | None` (lecture de la décision de publication **par `run_id`** via `store.get_run` + `store.get_check_results`), consommé par l'`ExecutionService`. `read_quarantine(ctx, dataset, limit)` existe déjà (31.1.4).

### Signatures Python
```python
# services/execution.py — ajouts
from skifer.services.serialization import row_to_json
from skifer.services.context import HARD_MAX_QUERY_ROWS

@dataclass(frozen=True)
class ResultView:
    job_id: str
    run_id: str                                  # == job_id
    kind: str
    state: str                                   # état du job à la production du résultat
    rows: tuple[dict[str, Any], ...]             # <= 1000, JSON-native
    total: int                                   # exact (count() complet, indépendant du plafond de rows)
    schema: tuple[dict[str, str], ...]           # [{"name":.., "type":..}] (df.dtypes / table schema)
    monitor_report: dict | None                  # MonitorReport.summary() ou None
    publication_decision: str | None             # "PROMOTED" | "QUARANTINED" | None
    quarantine: dict | None = None               # QuarantineView.to_dict() borné, seulement si QUARANTINED
    error: str | None = None
    def to_dict(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id, "run_id": self.run_id, "kind": self.kind, "state": self.state,
            "rows": [dict(r) for r in self.rows], "total": self.total,
            "schema": [dict(s) for s in self.schema],
            "monitor_report": self.monitor_report,
            "publication_decision": self.publication_decision,
            "quarantine": self.quarantine, "error": self.error,
        }

class ExecutionService:                           # (suite)
    def result(self, ctx: RequestContext, job_id: str) -> ResultView:
        require_scope(ctx, SCOPE_EXECUTE_RUN)
        ...   # ResourceNotFound si job inconnu ; InvalidRequest si job encore "running"

    # helper worker (interne, tourne côté Spark, AVANT de franchir la frontière) :
    def _materialize_rows(self, df, limit: int) -> tuple[list[dict], int, list[dict]]:
        """total = df.count() (exact) ; rows = [row_to_json(r, i) for i, r in enumerate(df.limit(n).collect())]
        avec n = min(limit, HARD_MAX_QUERY_ROWS) ; schema = [{"name": f, "type": t} for f, t in df.dtypes].
        Renvoie (rows, total, schema) — que des types JSON-natifs, aucun objet Spark."""
```
```python
# services/governance.py — ajout
@dataclass(frozen=True)
class RunOutcomeView:
    run_id: str
    state: str                                    # RunEvent.state ("PROMOTED"|"QUARANTINED"|...)
    target_fqn: str | None
    quarantine_fqn: str | None
    checks_passed: bool                           # dérivé de get_check_results (aucun critique en échec)
    def to_dict(self) -> dict[str, Any]: ...

class GovernanceService:                          # (suite)
    def get_run(self, ctx: RequestContext, run_id: str) -> RunOutcomeView | None:
        require_scope(ctx, SCOPE_CONTRACTS_READ)
        run = self._store.get_run(run_id)         # RunEvent | None
        if run is None:
            return None
        results = self._store.get_check_results(run_id)
        checks_passed = not any(
            (r.severity == "critical" and r.status is CheckStatus.FAIL) for r in results
        )
        return RunOutcomeView(run.run_id, run.state, run.target_fqn,
                              getattr(run, "quarantine_fqn", None), checks_passed)
```

### Comportement & règles
- **Production du `ResultView` côté worker (aucun DataFrame ne franchit `services/`)** : le worker (5.2) collecte/sérialise **dans le thread Spark**, stocke un `ResultView` **entièrement JSON-natif** dans `_Job.result`. `result(job_id)` ne fait que le relire.
- **`rows` ≤ 1000, JSON-natif** : `n = min(limit, HARD_MAX_QUERY_ROWS)` ; `row_to_json`/`to_json_value` (31.1.1) convertissent `Decimal → str`, `date/datetime → isoformat()`, `None → None`, `float` non fini → `SerializationError`. Une `SerializationError` pendant la matérialisation fait basculer le job en `failed` (le résultat n'est pas exposable partiellement).
- **`total` exact** : `df.count()` **complet**, calculé indépendamment du plafond `rows` — donc `total` peut dépasser `len(rows)`.
- **Par `kind`** :
  - `preview` : `df = process_schema(...)` (non écrit) → `_materialize_rows`. `monitor_report=None`, `publication_decision=None`.
  - `check` : exécute le monitor → `monitor_report = report.summary()` ; `rows=()`, `total=0`, `schema=()`, `publication_decision=None`.
  - `run` : après l'écriture, `monitor_report` = `summary()` du dernier rapport si disponible ; `publication_decision` et `quarantine` obtenus **par `run_id`** :
    - Si le moteur a un `certification_store`, l'`ExecutionService` appelle `GovernanceService(store).get_run(ctx, job_id)` → `RunOutcomeView`. `publication_decision = outcome.state` (`"PROMOTED"`/`"QUARANTINED"`).
    - Si `outcome.state == "QUARANTINED"`, appeler `GovernanceService.read_quarantine(ctx, dataset=outcome.target_fqn, limit=n)` → `quarantine = QuarantineView.to_dict()` **borné**.
    - Les `rows`/`total`/`schema` du `run` lisent un **échantillon borné de la table cible** : `df = engine.backend.sql(f"SELECT * FROM {fqn}")` puis `_materialize_rows`. (Pour un run quarantiné où la cible n'a pas été promue, `rows=()`, `total=0` et l'échantillon vient de la quarantaine.)
  - `full_refresh` : `rows=()`, `total=0`, `schema=()`, `monitor_report=None`, `publication_decision=None` (opération de purge).
- **`result` fail-closed** : `job_id` inconnu → `ResourceNotFound` ; job encore `running` → `InvalidRequest("job still running")` (ne jamais renvoyer un résultat partiel). Un job `failed` → `ResultView(state="failed", rows=(), total=0, error=<message>)`. Un job `cancelled` → `ResultView(state="cancelled", …)`.
- **Quarantaine bornée** : `read_quarantine` (31.1.4) plafonne déjà à `min(limit, max_rows)` et pose `truncated`. `result` ne dé-plafonne jamais.
- **Le `monitor_report` transporté est un `dict`** (issu de `summary()`), pas l'objet `MonitorReport` — cohérent avec « aucun objet non-JSON ne franchit la frontière ».

### Cas de test — `tests/test_services_execution.py` (+ `tests/test_services_governance.py` pour `get_run`)
Fakes (Spark-free) pour la logique de mapping/lecture :
- `test_result_unknown_job_not_found` : `result("nope")` → `ResourceNotFound`.
- `test_result_running_job_refused` : job bloquant → `result` → `InvalidRequest`.
- `test_governance_get_run_by_run_id` (`test_services_governance.py`) : faux store dont `get_run(rid)` renvoie un `RunEvent(state="QUARANTINED", target_fqn="cat.silver.t", quarantine_fqn=...)` et `get_check_results` un critique en échec → `RunOutcomeView(state="QUARANTINED", checks_passed=False)`.
- `test_governance_get_run_missing_returns_none` : store `get_run → None` → `None`.
Nécessitent `spark` fixture (Delta local réel) :
- `test_preview_rows_json_native_types` : YAML/select produisant colonnes `Decimal`, `date`, `timestamp`, et une valeur `null` → `submit(kind="preview")` → `ResultView.rows` : `Decimal → str`, `date → "YYYY-MM-DD"`, `timestamp → isoformat`, `null → None`. `json.dumps(result.to_dict())` réussit (preuve de JSON-nativité).
- `test_preview_total_exact_and_rows_bounded` : source de 5 lignes, `limit=2` → `total == 5`, `len(rows) == 2`.
- `test_preview_rows_capped_at_hard_max` : source > 1000 lignes, `limit=5000` → `len(rows) == 1000`, `total == <count réel>`.
- `test_check_returns_monitor_report_summary` : `submit(kind="check")` → `monitor_report` a les clés `status`/`total_checks`/`passed` ; `rows == ()`.
- `test_run_publication_decision_promoted` : schéma `data_product` valide → `run` → `publication_decision == "PROMOTED"`, `store.get_run(job_id).run_id == job_id`.
- `test_run_quarantined_bounded_read` : schéma `data_product` dont un check critique échoue → `run` termine `succeeded`, `publication_decision == "QUARANTINED"`, `quarantine` présent et **borné** (`len(quarantine["rows"]) <= max_rows`).

### Commit
```
feat(plan31-5.3): job results — bounded JSON-native rows, exact total, monitor report, publication decision
```
CHANGELOG :
```
- **Plan 31 (5.3)** — `ExecutionService.result` → `ResultView(rows<=1000 JSON-natifs, total exact, schema, monitor_report, publication_decision, quarantine bornée)`. Lignes/valeurs sérialisées via `services/serialization` (Decimal→str, date/timestamp→isoformat, null→None) côté worker ; aucun DataFrame ne franchit `services/`. Décision de publication et échantillon de quarantaine lus **par `run_id`** via `GovernanceService.get_run` (nouveau) et `read_quarantine`.
```

### DoD
- [ ] `rows` ≤ 1000 et JSON-natifs (Decimal/date/timestamp/null testés) ; `to_dict()` `json.dumps`-able.
- [ ] `total` exact (peut dépasser `len(rows)`).
- [ ] `publication_decision` lue par `run_id` (== `job_id`).
- [ ] Quarantaine bornée (jamais dé-plafonnée).
- [ ] `result` sur job `running` refusé ; job inconnu → `ResourceNotFound`.
- [ ] Aucun objet Spark dans `ResultView`.

---

## Ordre & dépendances internes

1. **5.1 → 5.2 → 5.3**, strictement. 5.2 ajoute les jobs sur la session de 5.1 ; 5.3 remplit le `ResultView` construit par le worker de 5.2.
2. **Amont obligatoire** : `services/context.py` (31.1.1 — `RequestContext`, `require_scope`, `SCOPE_EXECUTE_RUN`, erreurs, `HARD_MAX_QUERY_ROWS`), `services/serialization.py` (31.1.1 — `row_to_json`/`to_json_value`), `services/governance.py` (31.1.4 — `GovernanceService.read_quarantine`, étendu en 5.3 par `get_run`). Ces slices doivent être livrées avant 31.5.
3. **Modèle de threading — thread in-process recommandé (justification)** :
   - Les appels moteur (`run_from_yaml`, `process_schema.collect()`, `full_refresh`) sont **bloquants** et passent l'essentiel du temps dans la **JVM** (Spark), qui **relâche le GIL** — un `threading.Thread` (ou `ThreadPoolExecutor(max_workers=1)`) donne un vrai parallélisme worker/appelant sans complexité `asyncio`.
   - `asyncio` obligerait de toute façon un `run_in_executor` (Spark n'est pas awaitable) : on paierait la boucle d'événements sans bénéfice.
   - Le multi-processus casserait l'identité de session/`SparkSession` (non picklable) et la lecture partagée du registre.
   - **Slot unique** (§7.7) : un seul worker à la fois, gardé par `self._lock` (`RLock`). La **file d'attente future** est un ajout non cassant : remplacer le refus `JobConflict` par une mise en file + un dispatcher, sans toucher `submit/status/cancel/logs/result`.
   - **Isolation Spark** : la `SparkSession` locale est thread-safe pour la soumission ; le **job group** (`setJobGroup(job_id, …, interruptOnCancel=True)`) isole l'annulation par job.
4. **Registre** : à slot unique en v1 (`self._job`), gardé par le même `RLock` que la session — pas de course entre `submit`, `cancel`, `close` et le worker qui écrit `state/result`.

## Risques & pièges

1. **`run_id` doit être un UUID** : `start_publication_run` fait `str(UUID(run_id))`. Un `job_id` non-UUID **casserait** toute publication certifiée. → `job_id = str(uuid4())` **obligatoire** (test dédié). Ne jamais dériver le `job_id` d'un nom lisible.
2. **Plomberie du `run_id` dans `core.py`** : aujourd'hui `run_process_to_table`/`run_from_yaml` **frappent** le `run_id` en interne et **ne le retournent pas** (renvoient `None` via `_traced_pipeline_run`). La modification (accepter `run_id=` + `return run_id`) doit **rester rétrocompatible** : les exemples/tests qui ignorent le retour continuent de marcher, et le corps métier (tracing, patterns, publication) **ne doit pas changer** — ne pas dupliquer le flux selon l'état du tracing (c'est précisément ce qui, en 5.2 du Plan 29, faisait dériver le `run_id` persisté). Vérifier que `_traced_pipeline_run` reste l'unique wrapper.
3. **Annulation d'un job Spark en cours** : `cancelJobGroup` (Spark classique/local) interrompt les *tasks* d'un job Spark, **pas** le code Python entre deux actions. Sur **Databricks Connect** `sparkContext` peut être absent — utiliser `spark.interruptTag(job_id)` si disponible, sinon **best-effort**. Garantie v1 : l'**état logique** bascule `cancelled` immédiatement et le worker vérifie `cancel_requested` aux points de contrôle ; l'interruption JVM est opportuniste. Documenter que `full_refresh` (drop/purge) peut être non interruptible une fois lancé.
4. **Cycle de vie session-par-config & courses** : `connect` (changement de config) → `_close_locked` (annule le job actif + `spark.stop()` en local) doit se faire **sous `self._lock`** pour éviter qu'un worker écrive `result` sur un engine déjà fermé. Le worker doit tolérer un engine fermé sous lui (capturer l'exception → `failed`, ne pas planter le service). Ne **jamais** `spark.stop()` une session `provided`/`databricks_notebook` (partagée) — seulement `local`.
5. **TTL et jobs orphelins** : balayage **paresseux** à horloge injectée (déterministe en test). Un job `running` dont la session a expiré est marqué `cancelled` (défense en profondeur). Éviter tout thread reaper global (non testable proprement, fuite entre tests).
6. **Frontière sans Spark** : le piège classique est de stocker le DataFrame dans `_Job.result` et de sérialiser dans `result()`. **Non** : la matérialisation (`collect()` + `row_to_json` + `count()` + `dtypes`) se fait **dans le worker**, côté Spark ; `ResultView` ne contient que des scalaires/dicts. Un `float('nan')`/`inf` doit lever `SerializationError` → job `failed`, jamais un JSON invalide exposé.
7. **`total` vs `rows`** : ne jamais dériver `total` de `len(rows)` (borné à 1000). `total = df.count()` complet — deux passes Spark assumées (une `count`, une `limit().collect()`).
8. **Isolation des tests** : les tests de concurrence/annulation/TTL utilisent un **faux `engine_factory`** (bloque sur `threading.Event`, expose un faux `sparkContext` enregistrant `cancelJobGroup`) — **aucun `spark`**. Seuls les tests de bout en bout (`run_id`, sérialisation, publication) prennent la fixture `spark` (Delta local réel). Ne jamais `sleep` en dur : attendre sur un `Event` ou boucler `status()` avec timeout court.
