# Plan — Piste 5 : Data Observability

> Statut : Design (28 avril 2026)
> Branche : `data_observability`

---

## Contexte

Skifer dispose déjà de data contracts implicites dans chaque YAML de schema :
- `quality_checks` → contraintes de nullité et unicité
- `filter` → invariants métier attendus sur les données
- `select_final` → schéma de sortie attendu (colonnes, types via `cast:`)

L'idée centrale : **le YAML est le contrat — pas besoin de le redéclarer ailleurs.**

La Piste 3 (Lineage) est en place. Le `LineageTracker` et le `DataDictionary` sont disponibles pour enrichir les rapports d'observabilité avec l'impact downstream d'une anomalie.

---

## Objectif

Permettre à Skifer d'exécuter automatiquement des data quality checks dérivés des YAMLs existants, de stocker l'historique des résultats, et de déclencher des alertes — sans aucune configuration supplémentaire pour les checks de base.

---

## Structure cible

```
src/skifer/
  observability/
    __init__.py
    contracts.py       # ContractExtractor — dérive les DataContracts depuis un schema YAML
    checks.py          # Définitions des checks (NullCheck, UniqueCheck, TypeCheck, FilterCheck, ...)
    monitor.py         # DataMonitor — exécute les checks, retourne un MonitorReport
    history.py         # HistoryStore ABC + SqliteHistoryStore + DeltaHistoryStore
    reporter.py        # MonitorReporter — génère JSON / texte / HTML
    alerts.py          # AlertDispatcher — webhook, Slack, email
```

---

## Phase 1 — Contrats et checks de base

### Fichier : `observability/checks.py`

Définitions des checks comme des dataclasses évaluables :

```python
@dataclass
class DataContract:
    table: str
    severity: str = "warning"   # info | warning | critical

    def evaluate(self, backend, fqn: str) -> CheckResult: ...

@dataclass
class NullCheck(DataContract):
    column: str

@dataclass
class UniqueCheck(DataContract):
    columns: list[str]

@dataclass
class TypeCheck(DataContract):
    column: str
    expected_type: str          # "double", "string", "date", ...

@dataclass
class FilterInvariantCheck(DataContract):
    column: str
    operator: str               # "equals", "in", "is_not_null", ...
    value: str | None = None
```

Résultat d'un check :

```python
@dataclass
class CheckResult:
    contract: DataContract
    passed: bool
    actual_value: Any           # valeur mesurée (ex: nb de nulls trouvés)
    expected_value: Any         # valeur attendue (ex: 0)
    message: str
    severity: str
    timestamp: datetime
```

### Fichier : `observability/contracts.py`

`ContractExtractor` lit un schema YAML normalisé et retourne une liste de `DataContract` :

| Source YAML | Contract généré |
|---|---|
| `quality_checks.drop_nulls_in: [col_a]` | `NullCheck(column="col_a", severity="critical")` |
| `quality_checks.drop_duplicates_on: [id]` | `UniqueCheck(columns=["id"], severity="critical")` |
| `filter: "status:in:ACTIVE,PENDING"` | `FilterInvariantCheck(operator="in", value="ACTIVE,PENDING", severity="warning")` |
| `select_final: [..., [cast:double]]` | `TypeCheck(expected_type="double", severity="warning")` |

Les checks dérivés de `quality_checks` sont `critical` par défaut (ce sont des règles déjà appliquées en runtime — une violation post-écriture indique un bug). Les checks dérivés de `filter` sont `warning` par défaut (invariant attendu, peut ne pas couvrir 100% des données).

### Fichier : `observability/monitor.py`

```python
class DataMonitor:
    def __init__(self, backend, history_store=None):
        self.backend = backend
        self.history = history_store

    def check_table(self, fqn: str, contracts: list[DataContract]) -> MonitorReport:
        """Exécute tous les checks et retourne le rapport."""

    def check_from_schema(self, fqn: str, schema_dict: dict) -> MonitorReport:
        """Raccourci : dérive les contrats depuis le YAML puis exécute."""
        contracts = ContractExtractor().extract(schema_dict)
        return self.check_table(fqn, contracts)
```

`MonitorReport` :

```python
@dataclass
class MonitorReport:
    table: str
    results: list[CheckResult]
    timestamp: datetime

    def has_critical_failures(self) -> bool: ...
    def failures(self) -> list[CheckResult]: ...
    def summary(self) -> dict: ...
```

---

## Phase 2 — Checks configurables dans le YAML

Ajout d'une section `observability:` optionnelle dans le YAML schema :

```yaml
observability:
  freshness:
    max_delay: "2h"
    timestamp_column: updated_at
  volume:
    min_rows: 1000
    max_rows: 10000000
    variation_threshold: 0.3    # alerte si variation > 30% vs run précédent
  schema_drift:
    enabled: true               # détecte les colonnes ajoutées/supprimées vs le YAML
  custom_checks:
    - sql: "SELECT COUNT(*) FROM {table} WHERE amount < 0"
      expect: 0
      severity: critical
```

Nouveaux checks correspondants :

| Config | Check |
|---|---|
| `freshness` | `FreshnessCheck` — compare `MAX(timestamp_column)` au temps courant |
| `volume.min_rows` / `volume.max_rows` | `VolumeCheck` — COUNT(*) entre les bornes |
| `volume.variation_threshold` | `VolumeVariationCheck` — nécessite l'historique |
| `schema_drift` | `SchemaDriftCheck` — compare les colonnes Spark au YAML |
| `custom_checks` | `CustomSqlCheck` — exécute le SQL et compare au expect |

---

## Phase 3 — Historique et stockage

### Fichier : `observability/history.py`

```python
class HistoryStore(Protocol):
    def store(self, report: MonitorReport) -> None: ...
    def get_last_n(self, table: str, n: int) -> list[MonitorReport]: ...
    def get_latest(self, table: str) -> MonitorReport | None: ...

class SqliteHistoryStore:
    """Stockage local — développement et tests."""
    def __init__(self, db_path: str = ".skifer_observability.db"): ...

class DeltaHistoryStore:
    """Stockage production — Delta table sur Databricks."""
    def __init__(self, backend, table_fqn: str = "_observability.check_history"): ...
```

Le fichier `.skifer_observability.db` doit être ajouté au `.gitignore`.

---

## Phase 4 — Intégration dans le pipeline core

Dans `core.py`, après `_write_dataframe`, si un monitor est configuré :

```python
# Dans SkiferEngine.__init__
def __init__(self, ..., monitor: DataMonitor | None = None):
    self.monitor = monitor

# Dans run_process_to_table, après l'écriture :
if self.monitor and schema_dict:
    report = self.monitor.check_from_schema(fqn, schema_dict)
    if report.has_critical_failures():
        raise DataQualityError(report)
```

`DataQualityError` est une nouvelle exception dans `core/exceptions.py` (ou directement dans `core.py`).

---

## Phase 5 — Reporter et alertes

### Fichier : `observability/reporter.py`

```python
class MonitorReporter:
    def to_json(self, report: MonitorReport) -> str: ...
    def to_text(self, report: MonitorReport) -> str: ...
    def to_html(self, report: MonitorReport, output_path: str) -> None: ...
```

### Fichier : `observability/alerts.py`

```python
class AlertDispatcher:
    def dispatch(self, report: MonitorReport, config: dict) -> None:
        """
        config keys: webhook_url, slack_channel, email
        Envoie seulement sur les failures selon la sévérité.
        """
```

---

## Niveaux de sévérité

| Niveau | Comportement |
|---|---|
| `info` | Log uniquement |
| `warning` | Log + alerte (non-bloquant) |
| `critical` | Log + alerte + arrêt du pipeline si `DataMonitor` est actif |

---

## Dépendances avec les autres pistes

| Piste | Impact |
|---|---|
| Piste 1 (Multi-plateforme) | L'exécution des checks utilise le `backend` abstrait déjà en place |
| Piste 3 (Lineage) | Le lineage enrichit les rapports — impact downstream d'une anomalie (Phase 5+) |
| Piste 4 (Hub agentic) | Le `QualityAgent` consomme directement le `DataMonitor` |
| Piste 2 (Client graphique) | Dashboard de monitoring (hors scope de ce plan) |

---

## Tests

Un fichier `tests/test_observability.py` couvre chaque phase. Convention : tous les tests mockent le backend (pas de Spark réel) sauf `SqliteHistoryStore` qui tourne en mémoire.

### Phase 1 — ContractExtractor

```python
def test_extract_null_checks():
    schema = parse_schema("""
    tables:
      - name: silver.orders
        quality_checks:
          drop_nulls_in: [amount, customer_id]
    """)
    contracts = ContractExtractor().extract(schema)
    null_checks = [c for c in contracts if isinstance(c, NullCheck)]
    assert len(null_checks) == 2
    assert {c.column for c in null_checks} == {"amount", "customer_id"}
    assert all(c.severity == "critical" for c in null_checks)

def test_extract_unique_check():
    schema = parse_schema("""
    tables:
      - name: silver.orders
        quality_checks:
          drop_duplicates_on: [order_id]
    """)
    contracts = ContractExtractor().extract(schema)
    unique = [c for c in contracts if isinstance(c, UniqueCheck)]
    assert len(unique) == 1
    assert unique[0].columns == ["order_id"]

def test_extract_filter_invariant():
    schema = parse_schema("""
    tables:
      - name: silver.orders
        filter:
          - status:in:ACTIVE,PENDING
    """)
    contracts = ContractExtractor().extract(schema)
    filters = [c for c in contracts if isinstance(c, FilterInvariantCheck)]
    assert len(filters) == 1
    assert filters[0].operator == "in"
    assert filters[0].severity == "warning"

def test_extract_type_check_from_cast():
    schema = parse_schema("""
    tables:
      - name: silver.orders
    select_final:
      - [amount, amount_eur, [cast:double]]
    """)
    contracts = ContractExtractor().extract(schema)
    type_checks = [c for c in contracts if isinstance(c, TypeCheck)]
    assert any(c.column == "amount_eur" and c.expected_type == "double" for c in type_checks)
```

### Phase 1 — DataContract.evaluate() avec backend mocké

```python
class FakeBackend:
    """Backend de test — retourne des valeurs configurables via mock_results."""
    def __init__(self, mock_results: dict): ...
    def sql(self, query: str): ...

def test_null_check_passes():
    backend = FakeBackend({"null_count": 0})
    check = NullCheck(table="silver.orders", column="amount", severity="critical")
    result = check.evaluate(backend, "silver.orders")
    assert result.passed
    assert result.actual_value == 0

def test_null_check_fails():
    backend = FakeBackend({"null_count": 42})
    check = NullCheck(table="silver.orders", column="amount", severity="critical")
    result = check.evaluate(backend, "silver.orders")
    assert not result.passed
    assert result.severity == "critical"

def test_unique_check_fails():
    backend = FakeBackend({"total": 1000, "distinct": 995})
    check = UniqueCheck(table="silver.orders", columns=["order_id"], severity="critical")
    result = check.evaluate(backend, "silver.orders")
    assert not result.passed
    assert "5" in result.message  # 5 doublons
```

### Phase 1 — DataMonitor end-to-end

```python
def test_monitor_check_from_schema_no_failures(mock_backend):
    schema = parse_schema("tables:\n  - name: silver.orders\n    quality_checks:\n      drop_nulls_in: [amount]")
    monitor = DataMonitor(backend=mock_backend)
    report = monitor.check_from_schema("silver.orders", schema)
    assert isinstance(report, MonitorReport)
    assert report.table == "silver.orders"
    assert not report.has_critical_failures()

def test_monitor_raises_on_critical_failure(mock_backend_with_nulls):
    # mock_backend_with_nulls simule 10 nulls sur 'amount'
    schema = parse_schema("tables:\n  - name: silver.orders\n    quality_checks:\n      drop_nulls_in: [amount]")
    monitor = DataMonitor(backend=mock_backend_with_nulls)
    with pytest.raises(DataQualityError) as exc_info:
        monitor.check_from_schema("silver.orders", schema, raise_on_critical=True)
    assert "amount" in str(exc_info.value)
```

### Phase 3 — SqliteHistoryStore (en mémoire, pas de mock)

```python
def test_sqlite_store_roundtrip():
    store = SqliteHistoryStore(db_path=":memory:")
    report = MonitorReport(table="silver.orders", results=[], timestamp=datetime.now())
    store.store(report)
    latest = store.get_latest("silver.orders")
    assert latest is not None
    assert latest.table == "silver.orders"

def test_sqlite_get_last_n():
    store = SqliteHistoryStore(db_path=":memory:")
    for i in range(5):
        store.store(MonitorReport(table="silver.orders", results=[], timestamp=datetime.now()))
    history = store.get_last_n("silver.orders", n=3)
    assert len(history) == 3
```

### Phase 5 — MonitorReporter

```python
def test_reporter_to_json(sample_report):
    reporter = MonitorReporter()
    data = json.loads(reporter.to_json(sample_report))
    assert data["table"] == "silver.orders"
    assert "results" in data
    assert "summary" in data

def test_reporter_to_text_contains_status(sample_report):
    reporter = MonitorReporter()
    text = reporter.to_text(sample_report)
    assert "PASS" in text or "FAIL" in text
```

---

## Vérification

```bash
pytest tests/test_observability.py -x --tb=short
ruff check src/skifer/observability/
```

---

## Notebook de démonstration — `example/demo_observability.ipynb`

### Objectif

Illustrer le cycle complet : YAML existant → checks dérivés automatiquement → rapport → historique.
Aucune connexion Spark réelle requise (FakeBackend injecté, comme dans `demo_lineage_tracker.ipynb`).

### Structure des sections

| Section | Contenu | Sortie attendue |
|---|---|---|
| **1. Setup** | Imports, `sys.path`, `FakeBackend` | `✅ Observability module importé` |
| **2. Le YAML comme contrat** | YAML schema avec `quality_checks`, `filter`, `select_final` | Affichage du YAML annoté |
| **3. ContractExtractor** | `ContractExtractor().extract(schema)` | Liste des checks dérivés avec sévérité |
| **4. Checks individuels** | `NullCheck`, `UniqueCheck`, `TypeCheck` évalués sur FakeBackend | Tableau PASS/FAIL |
| **5. DataMonitor** | `monitor.check_from_schema(fqn, schema)` | `MonitorReport` avec summary |
| **6. Failure critique** | FakeBackend simulant des nulls → `DataQualityError` | `try/except` qui catch et affiche le rapport |
| **7. Historique** | `SqliteHistoryStore` — store 3 runs, `get_last_n(3)` | Tableau des runs précédents |
| **8. Checks configurables** | YAML avec section `observability:` (freshness, volume) | Checks supplémentaires extraits |
| **9. Reporter** | `MonitorReporter.to_json()` + `to_text()` | JSON formaté + rapport texte |
| **10. Intégration lineage** | Lien avec `LineageTracker` — impact downstream d'un check FAIL | Upstream/downstream de la colonne fautive |
| **11. Limitations connues** | Ce qui n'est pas détectable statiquement | Texte explicatif |

### Scénario fil rouge

Pipeline `bronze.raw_orders` → `silver.fact_orders` (réutilisation du YAML du demo lineage).
- Run 1 : tout passe (FakeBackend propre)
- Run 2 : 42 nulls sur `amount` (FakeBackend pollué) → FAIL critique
- Run 3 : corrigé → PASS
- Historique montre la régression et le retour à la normale

### Principe de rendu

Même style que `demo_lineage_tracker.ipynb` :
- Outputs de cellules visibles dans le fichier (pas de `# Output: ...` en commentaire)
- Les cellules markdown expliquent le **pourquoi**, pas le **quoi**
- Chaque section commence par une cellule markdown avec le contexte métier

---

## Plan d'implémentation

| Phase | Fichiers créés / modifiés | Prérequis |
|---|---|---|
| 1 | `observability/checks.py`, `observability/contracts.py`, `observability/monitor.py`, `tests/test_observability.py` | — |
| 2 | `observability/checks.py` (nouveaux checks), `core/schema_loader.py` (parsing section `observability:`) | Phase 1 |
| 3 | `observability/history.py`, `observability/monitor.py` (store integration) | Phase 1 |
| 4 | `core/core.py` (intégration DataMonitor), `core/exceptions.py` | Phases 1, 3 |
| 5 | `observability/reporter.py`, `observability/alerts.py` | Phase 1 |