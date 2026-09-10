# Piste 5 — Data observability

> Priorite : 3/5 — Exploite l'avantage unique des YAML-as-contract, differenciateur fort vs dbt.
> Statut : Reflexion initiale (21 avril 2026)

---

## Pourquoi c'est une opportunite unique pour Skifer

Les YAML de schemas sont de facto des **data contracts** :
- `quality_checks` definit les contraintes (nulls, duplicates).
- `filter` definit les conditions attendues sur les donnees.
- `select_final` definit le schema de sortie (colonnes, types via `cast:`).
- Les modeles semantiques definissent les metriques et leurs formules.

La plupart des outils d'observabilite (Great Expectations, Soda, Monte Carlo) demandent de **redefinir ces regles separement**. Skifer les a deja dans les YAML — zero configuration supplementaire pour les checks de base.

---

## Architecture proposee

```
skifer/
  observability/
    contracts.py     # Extraction automatique des attentes depuis les YAML
    monitor.py       # DataMonitor — execute les checks et collecte les resultats
    reporter.py      # Genere les rapports (JSON, HTML, ou feed vers le hub agentic)
    alerts.py        # Systeme d'alertes (webhook, email, Slack)
    history_store.py # Stockage historique des resultats (Delta table ou SQLite local)
```

---

## Checks derivables automatiquement des YAML

| Source YAML | Check genere |
|---|---|
| `quality_checks.drop_nulls_in: [col_a]` | `ASSERT count_null(col_a) == 0` |
| `quality_checks.drop_duplicates_on: [id]` | `ASSERT count == count_distinct(id)` |
| `filter: "status:in:ACTIVE,PENDING"` | `ASSERT all(status IN ('ACTIVE','PENDING'))` |
| `select_final: [amount, amount_eur, [cast:double]]` | `ASSERT typeof(amount_eur) == DOUBLE` |
| Semantic metric `gross_revenue: "SUM(amount)"` | Anomaly detection sur la serie temporelle |

C'est le coeur du differenciateur : le YAML **est** le contrat, pas besoin de le redeclarer ailleurs.

---

## Checks supplementaires configurables

```yaml
# Dans le YAML schema ou un fichier dedie
observability:
  freshness:
    max_delay: "2h"             # la table ne doit pas avoir plus de 2h de retard
    timestamp_column: updated_at
  volume:
    min_rows: 1000              # alerte si < 1000 lignes
    max_rows: 10000000          # alerte si > 10M lignes
    variation_threshold: 0.3    # alerte si variation > 30% vs run precedent
  schema_drift:
    enabled: true               # detecte les colonnes ajoutees/supprimees vs le YAML
  custom_checks:
    - sql: "SELECT COUNT(*) FROM {table} WHERE amount < 0"
      expect: 0
      severity: critical
```

### Niveaux de severite

| Niveau | Comportement |
|---|---|
| `info` | Log uniquement |
| `warning` | Log + alerte (non-bloquant) |
| `critical` | Log + alerte + arret du pipeline si configure |

---

## ContractExtractor — derivation automatique

```python
class ContractExtractor:
    """Extrait les attentes implicites d'un schema YAML."""

    def extract(self, schema_dict: dict) -> list[DataContract]:
        contracts = []

        # Quality checks
        for table in schema_dict.get("tables", []):
            qc = table.get("quality_checks", {})
            if "drop_nulls_in" in qc:
                for col in qc["drop_nulls_in"]:
                    contracts.append(NullCheck(table=table["name"], column=col))
            if "drop_duplicates_on" in qc:
                contracts.append(UniqueCheck(table=table["name"], columns=qc["drop_duplicates_on"]))

        # Filters → invariants
        # select_final → type checks
        # ...
        return contracts
```

---

## DataMonitor — execution des checks

```python
class DataMonitor:
    """Execute les checks et collecte les resultats."""

    def __init__(self, backend, history_store=None):
        self.backend = backend
        self.history = history_store

    def check_table(self, fqn: str, contracts: list[DataContract]) -> MonitorReport:
        """Execute tous les checks sur une table et retourne le rapport."""
        results = []
        for contract in contracts:
            result = contract.evaluate(self.backend, fqn)
            results.append(result)

        report = MonitorReport(table=fqn, results=results, timestamp=now())

        if self.history:
            self.history.store(report)

        return report
```

---

## Integration avec le pipeline

Deux modes d'execution :

### 1. Post-run (automatique)

Apres chaque `run_process_to_table`, le monitor s'execute automatiquement sur la table ecrite :

```python
# Dans core.py, apres _write_dataframe :
if self.monitor:
    contracts = ContractExtractor().extract(schema_dict)
    report = self.monitor.check_table(fqn, contracts)
    if report.has_critical_failures():
        raise DataQualityError(report)
```

### 2. Standalone (schedule)

Execution independante sur n'importe quelle table, planifiable via cron ou Databricks Workflow :

```python
monitor = DataMonitor(backend=engine.backend)
contracts = ContractExtractor().extract(schema_dict)
report = monitor.check_table("catalog.silver.fact_orders", contracts)
reporter.generate_html(report, output="report.html")
```

---

## Stockage historique

| Mode | Backend | Usage |
|---|---|---|
| Local | SQLite | Developpement, tests |
| Databricks | Delta table (`_observability.check_history`) | Production |
| Generique | Tout backend via le Protocol (piste 1) | Multi-plateforme |

L'historique permet :
- La comparaison run-to-run (regression detection).
- L'anomaly detection sur les metriques (volume, freshness).
- Le reporting sur une periode (dashboard).

---

## Alertes

```python
class AlertDispatcher:
    """Envoie des alertes selon la severite."""

    def dispatch(self, report: MonitorReport, config: dict):
        for result in report.failures():
            if result.severity == "critical":
                self.send_webhook(config.get("webhook_url"), result)
                self.send_slack(config.get("slack_channel"), result)
            elif result.severity == "warning":
                self.send_slack(config.get("slack_channel"), result)
```

---

## Positionnement vs outils existants

| Outil | Approche | Limite |
|---|---|---|
| Great Expectations | Config declarative decouple du pipeline | Il faut re-declarer les attentes separement |
| Soda | SQL checks + YAML config | Meme probleme — config separee du pipeline |
| Monte Carlo | Observabilite SaaS, ML-based | Cout eleve, boite noire, pas de data contract |
| **Skifer** | Les YAML de pipeline **sont** les contrats | Zero config supplementaire pour les checks de base |

---

## Dependances avec les autres pistes

| Piste | Impact |
|---|---|
| Piste 1 (Multi-plateforme) | L'execution des checks necessite le backend abstrait |
| Piste 2 (Client graphique) | Dashboard de monitoring integre au client |
| Piste 3 (Lineage) | Le lineage enrichit les rapports (impact d'une anomalie sur le downstream) |
| Piste 4 (Hub agentic) | Le `QualityAgent` consomme directement le `DataMonitor` |
