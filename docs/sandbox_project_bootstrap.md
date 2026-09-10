# Bootstrap — skifer-sandbox

Document d'instructions pour Claude Code.  
Ce projet est un environnement de test end-to-end de Skifer sur Databricks réel, avec un dataset synthétique généré en Python.  
L'objectif est de valider chaque feature principale avant de construire le Hub Agentic.

---

## Contexte

Skifer est un framework déclaratif de data engineering (YAML schemas + Python rules) pour Databricks Lakehouse.  
Ce repo consomme Skifer comme dépendance pip et teste ses features sur un vrai backend Databricks Unity Catalog.

Repo source : `skifer` (framework)  
Ce repo : `skifer-sandbox` (test end-to-end)

---

## Structure cible du projet

```
skifer-sandbox/
  data_generation/
    generate_bronze.py       # Génère les données synthétiques et les écrit dans les tables Bronze
    generators/
      orders.py              # Génère les commandes aléatoires
      customers.py           # Génère les clients
      products.py            # Génère le catalogue produit
  schemas/
    bronze/
      raw_orders.yaml        # Pas de pipeline — ingestion directe
      raw_customers.yaml
      raw_products.yaml
    silver/
      fact_orders.yaml       # Pipeline principal : join orders + customers + products
      dim_customers.yaml     # Dédupliqué, enrichi
    gold/
      kpi_orders.yaml        # Agrégations métier (CA, volume, panier moyen)
  rules/
    __init__.py
    order_rules.py           # Business rules enregistrées (@RuleRegistry.register_rule)
  semantic_models/           # Générés par SemanticBuilder (vide au départ)
  semantic_catalog.yaml      # Index des modèles sémantiques (vide au départ)
  notebooks/
    01_generate_data.ipynb   # Exécution de la génération de données
    02_run_pipeline.ipynb    # Exécution du pipeline Bronze → Silver → Gold
    03_observability.ipynb   # Vérification des checks post-write
    04_lineage.ipynb         # Exploration du lineage column-level
    05_semantic_agent.ipynb  # Test du GenBIAgent sur les KPIs Gold
  config.yaml
  .env.template
  pyproject.toml
  CLAUDE.md
```

---

## Dataset synthétique — spécifications

### Tables Bronze (données brutes)

#### `bronze.raw_orders`
| Colonne | Type | Description | Génération |
|---|---|---|---|
| `order_id` | string | UUID unique | `uuid4()` |
| `customer_id` | string | FK vers raw_customers | random choice parmi les customer_ids |
| `product_id` | string | FK vers raw_products | random choice parmi les product_ids |
| `order_date` | date | Date de commande | random dans les 2 dernières années |
| `quantity` | int | Quantité commandée | randint(1, 20) |
| `unit_price` | double | Prix unitaire brut | random entre 5.0 et 500.0 (2 décimales) |
| `discount` | double | Taux de remise | random parmi [0.0, 0.05, 0.10, 0.15, 0.20] |
| `status` | string | Statut de la commande | random parmi ['completed', 'shipped', 'pending', 'cancelled'] — 70% completed/shipped |
| `channel` | string | Canal de vente | random parmi ['web', 'retail', 'marketplace', 'B2B'] |
| `region` | string | Région géographique | random parmi ['EMEA', 'APAC', 'AMER'] |
| `created_at` | timestamp | Date d'insertion | `datetime.now()` au moment de la génération |

Volume cible : **50 000 lignes** — introduire volontairement ~2% de nulls sur `unit_price` et ~1% de doublons sur `order_id` pour tester les quality checks.

#### `bronze.raw_customers`
| Colonne | Type | Description | Génération |
|---|---|---|---|
| `customer_id` | string | UUID unique | `uuid4()` |
| `first_name` | string | Prénom | faker ou liste statique |
| `last_name` | string | Nom | faker ou liste statique |
| `email` | string | Email | `{first}.{last}@{domain}.com` |
| `country` | string | Pays | random parmi ['FR', 'DE', 'GB', 'US', 'JP', 'AU'] |
| `segment` | string | Segment client | random parmi ['premium', 'standard', 'occasional'] |
| `signup_date` | date | Date d'inscription | random dans les 5 dernières années |
| `is_active` | boolean | Actif | 90% True |
| `created_at` | timestamp | Date d'insertion | `datetime.now()` |

Volume cible : **5 000 lignes** — sans doublons ni nulls.

#### `bronze.raw_products`
| Colonne | Type | Description | Génération |
|---|---|---|---|
| `product_id` | string | UUID unique | `uuid4()` |
| `product_name` | string | Nom produit | `f"Product_{category}_{i}"` |
| `category` | string | Catégorie | random parmi ['Electronics', 'Clothing', 'Food', 'Books', 'Sports'] |
| `brand` | string | Marque | liste statique de 20 marques fictives |
| `cost_price` | double | Coût de revient | entre 1.0 et 200.0 |
| `is_active` | boolean | Disponible | 95% True |
| `created_at` | timestamp | Date d'insertion | `datetime.now()` |

Volume cible : **500 lignes**.

### Génération — instructions

- Utiliser **uniquement** `random`, `uuid`, `datetime` de la stdlib Python + `pyspark` pour écrire en Delta.
- Ne pas utiliser `faker` (éviter les dépendances inutiles).
- Le script `generate_bronze.py` prend en argument `--catalog` et `--rows` (défaut: 50000).
- Les tables sont écrites en mode `overwrite` à chaque génération.
- Ajouter un `random.seed(42)` pour la reproductibilité — mais laisser un flag `--random-seed` pour override.
- Log le nombre de lignes écrites pour chaque table.

---

## Pipeline YAML — spécifications

### `schemas/silver/fact_orders.yaml`

```yaml
tables:
  - name: "{{ catalog }}.bronze.raw_orders"
    alias: ord
    filter:
      - status:in:completed,shipped
    quality_checks:
      drop_nulls_in: [order_id, customer_id, product_id, unit_price]
      drop_duplicates_on: [order_id]

  - name: "{{ catalog }}.bronze.raw_customers"
    alias: cust

  - name: "{{ catalog }}.bronze.raw_products"
    alias: prod

join:
  - table_from: [ord, customer_id]
    table_to: [cust, customer_id]
    type: left
  - table_from: [ord, product_id]
    table_to: [prod, product_id]
    type: left

business_rules:
  - compute_amount
  - compute_margin
  - flag_high_value

select_final:
  - [order_id,    order_id]
  - [order_date,  order_date]
  - [customer_id, customer_id]
  - [product_id,  product_id]
  - [quantity,    quantity]
  - [unit_price,  unit_price,   [cast:double]]
  - [discount,    discount,     [cast:double]]
  - [amount,      amount,       [cast:double, round:2]]
  - [margin,      margin,       [cast:double, round:2]]
  - [channel,     channel,      [upper]]
  - [region,      region,       [upper]]
  - [status,      status]
  - [segment,     customer_segment]
  - [country,     country]
  - [category,    product_category]

observability:
  freshness:
    max_delay: 24h
    timestamp_column: order_date
  volume:
    min_rows: 1000
    variation_threshold: 0.5
  schema_drift:
    enabled: true
  custom_checks:
    - sql: "SELECT COUNT(*) AS result FROM {table} WHERE amount < 0"
      expect: 0
      severity: critical
```

### `schemas/gold/kpi_orders.yaml`

```yaml
tables:
  - name: "{{ catalog }}.silver.fact_orders"
    alias: fo
    filter:
      - status:in:COMPLETED,SHIPPED

business_rules:
  - flag_vip_customer

select_final:
  - [order_id,          order_id]
  - [order_date,        order_date]
  - [customer_id,       customer_id]
  - [product_category,  product_category]
  - [channel,           channel]
  - [region,            region]
  - [customer_segment,  customer_segment]
  - [country,           country]
  - [amount,            revenue,      [cast:double, round:2]]
  - [margin,            margin,       [cast:double, round:2]]
  - [quantity,          quantity]
```

### Business rules — `rules/order_rules.py`

```python
from skifer import RuleRegistry
from pyspark.sql import functions as F

@RuleRegistry.register_rule(name="compute_amount", inputs=["unit_price", "quantity", "discount"], outputs=["amount"])
def compute_amount(df):
    """Calcule le montant TTC après remise : unit_price * quantity * (1 - discount)."""
    return df.withColumn(
        "amount",
        F.round(F.col("unit_price") * F.col("quantity") * (1 - F.col("discount")), 2)
    )

@RuleRegistry.register_rule(name="compute_margin", inputs=["amount", "cost_price", "quantity"], outputs=["margin"])
def compute_margin(df):
    """Calcule la marge brute : amount - (cost_price * quantity)."""
    return df.withColumn(
        "margin",
        F.round(F.col("amount") - (F.col("cost_price") * F.col("quantity")), 2)
    )

@RuleRegistry.register_rule(name="flag_high_value", inputs=["amount"], outputs=["is_high_value"])
def flag_high_value(df):
    """Marque les commandes à haute valeur (amount >= 500)."""
    return df.withColumn(
        "is_high_value",
        F.when(F.col("amount") >= 500, 1).otherwise(0)
    )

@RuleRegistry.register_rule(name="flag_vip_customer", inputs=["customer_segment", "amount"], outputs=["is_vip"])
def flag_vip_customer(df):
    """VIP = segment premium ET commande >= 200."""
    return df.withColumn(
        "is_vip",
        F.when(
            (F.col("customer_segment") == "premium") & (F.col("amount") >= 200), 1
        ).otherwise(0)
    )
```

---

## Configuration — `config.yaml`

```yaml
environments:
  LOCAL:
    catalog: null

  DEV:
    catalog: "<YOUR_DEV_CATALOG>"

  PROD:
    catalog: "<YOUR_PROD_CATALOG>"

default_env: LOCAL

sandbox:
  missing_table: copy
```

Remplacer `<YOUR_DEV_CATALOG>` et `<YOUR_PROD_CATALOG>` avec les valeurs du workspace Databricks.

## `.env.template`

```
DATABRICKS_HOST=https://<workspace>.azuredatabricks.net
DATABRICKS_TOKEN=<personal_access_token>
DATABRICKS_CLUSTER_ID=<cluster_id>
```

Copier en `.env` (ajouté au `.gitignore`).

---

## Installation

Skifer est publié sur PyPI sous le nom `skifer`. Aucun token, aucune authentification.

```bash
pip install "skifer[spark]"
```

L'extra `spark` ajoute PySpark et Delta, nécessaires pour exécuter un pipeline en local. Sans lui, seules les parties qui ne dépendent pas de Spark sont disponibles : `skifer validate`, la couche sémantique, le lineage.

### Épingler une version

```bash
pip install "skifer[spark]==2.0.0"
```

### Installer un état de développement

TestPyPI reçoit une build à chaque push sur `main` :

```bash
pip install --index-url https://test.pypi.org/simple/ \
            --extra-index-url https://pypi.org/simple/ "skifer[spark]"
```

Pour un état non publié, l'installation directe depuis le dépôt reste possible, sans token puisque le repo est public :

```bash
pip install "skifer[spark] @ git+https://github.com/morzu117/skifer.git@main"
```

### Sur Databricks (cluster ou job)

Ajouter `skifer[spark]` dans les `Libraries` du cluster, ou dans une cellule de notebook :

```python
%pip install "skifer[spark]"
dbutils.library.restartPython()
```

Sur un Databricks Runtime, PySpark et Delta sont déjà fournis. Préférer alors `skifer` sans extra, pour ne pas écraser les versions du runtime :

```python
%pip install skifer
dbutils.library.restartPython()
```

---

## `pyproject.toml`

```toml
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[project]
name = "skifer-sandbox"
version = "0.1.0"
requires-python = ">=3.9"

dependencies = [
    "skifer[spark]>=2.0.0",
    "python-dotenv>=1.0.0",
]

[project.optional-dependencies]
llm = ["skifer[spark,llm-anthropic]>=2.0.0"]
dev = ["pytest", "ruff", "jupyter"]
```

```bash
pip install -e ".[dev]"
```

---

## Plan de test — ordre et critères de validation

### Phase 1 — Core pipeline Bronze → Silver → Gold

**Objectif** : valider que `run_process_to_table` fonctionne sur un vrai backend Databricks Unity Catalog.

Notebook : `02_run_pipeline.ipynb`

Étapes :
1. Initialiser `SkiferEngine()` (auto-détection DEV)
2. Charger `schemas/silver/fact_orders.yaml` avec `load_schema()`
3. Exécuter `engine.run_process_to_table(schema, target_layer="silver", target_table_name="fact_orders")`
4. Charger `schemas/gold/kpi_orders.yaml` et exécuter vers `gold.kpi_orders`
5. Valider avec `spark.table("catalog.gold.kpi_orders").count()` et `.show(5)`

Critères de succès :
- Aucune exception
- Les nulls et doublons intentionnels de bronze ont bien été supprimés en Silver
- Les colonnes calculées (`amount`, `margin`, `is_high_value`) sont présentes et cohérentes
- Les filtres (`status IN ('completed', 'shipped')`) sont bien appliqués

Points à surveiller :
- FQN (2-part vs 3-part selon le mode)
- Sandbox mode en DEV — vérifier que les tables résolvent bien vers `schema_XXXX.table`
- Performance sur 50 000 lignes

---

### Phase 2 — Data Observability

**Objectif** : valider que `DataMonitor` fonctionne post-write sur un vrai backend Spark, et que les checks SQL sont exécutés correctement.

Notebook : `03_observability.ipynb`

Étapes :
1. Instancier `DataMonitor(backend=engine.backend, history_store=DeltaHistoryStore(engine.backend))`
2. Charger le schema Silver et appeler `monitor.check_from_schema("catalog.silver.fact_orders", schema)`
3. Afficher le rapport avec `MonitorReporter().to_text(report)`
4. Ré-exécuter le pipeline avec `raise_on_critical=True` (via `monitor` dans `SkiferEngine`)
5. Tester un check custom : vérifier que `amount >= 0` tient

Critères de succès :
- Tous les checks PASS (les données synthétiques ont été nettoyées en Silver)
- `DeltaHistoryStore` écrit bien dans `_observability.check_history`
- Le rapport texte/HTML est lisible
- Le `FreshnessCheck` passe (données récentes)

Points à surveiller :
- `DESCRIBE <table>` fonctionne sur Unity Catalog (syntax Delta)
- `TypeCheck` : vérifier que les types Spark retournés par DESCRIBE matchent les expected_types du YAML
- `VolumeVariationCheck` : skip au premier run (pas d'historique), passe au second

---

### Phase 3 — Lineage

**Objectif** : valider que le lineage statique est cohérent avec le pipeline réellement exécuté.

Notebook : `04_lineage.ipynb`

Étapes :
1. Charger le schema Silver et appeler `LineageTracker.from_schema(schema, target_name="silver.fact_orders")`
2. Vérifier les edges (select, join, rule)
3. Tester `graph.upstream("silver.fact_orders", "amount")` — doit remonter à `raw_orders.unit_price`, `raw_orders.quantity`, `raw_orders.discount`
4. Tester `graph.downstream("bronze.raw_orders", "unit_price")` — doit descendre vers `amount`, `margin`, `is_high_value`
5. Exporter le diagramme Mermaid et vérifier visuellement la cohérence avec le pipeline

Critères de succès :
- Les edges `select` couvrent toutes les colonnes de `select_final`
- Les edges `join` reflètent les deux jointures (customer_id, product_id)
- Les edges `rule` détectent bien les inputs/outputs des 3 règles (via `inputs=`/`outputs=` déclarés)
- Le `DataDictionary` liste toutes les colonnes de `silver.fact_orders`
- L'enrichissement glossaire fonctionne si un fichier de glossaire est fourni

Points à surveiller :
- Colonnes calculées (`amount`, `margin`) : source_table bien attribuée à `raw_orders` ?
- Comportement si une règle n'a pas `inputs=`/`outputs=` dans le décorateur

---

### Phase 4 — Semantic Layer + GenBIAgent

**Objectif** : valider que `SemanticBuilder` génère un modèle YAML cohérent et que `GenBIAgent` exécute des requêtes sans halluciner de SQL.

Notebook : `05_semantic_agent.ipynb`

Étapes :
1. Utiliser `SemanticBuilder` pour générer un modèle KPI depuis la table `gold.kpi_orders`
2. Vérifier le YAML généré (dimensions, metrics, types)
3. Instancier `engine.get_agent()` et tester 5 questions types :
   - "Quel est le chiffre d'affaires total ?"
   - "Quel est le CA par région ?"
   - "Quels sont les 5 pays avec le plus de commandes ?"
   - "Quel est le panier moyen pour le segment premium ?"
   - "Quelle est la marge brute par catégorie de produit ?"
4. Vérifier les `AgentResponse` : `success=True`, DataFrame non vide, pas de `SemanticQueryError`

Critères de succès :
- Aucune hallucination SQL (toutes les requêtes passent par `QueryResolver`)
- Les 5 questions retournent un `AgentResponse(success=True)`
- Les DataFrames retournés sont cohérents avec les données synthétiques (volumes plausibles)

Points à surveiller :
- Variables d'environnement LLM (`ANTHROPIC_API_KEY` dans `.env`)
- Temps de réponse de l'agent (2 appels LLM par question)
- Cas d'ambiguïté : tester une question avec un nom de champ ambigu

---

## CLAUDE.md pour ce projet

Le fichier `CLAUDE.md` du repo sandbox doit contenir :

```markdown
# skifer-sandbox

Projet de test end-to-end de Skifer sur Databricks réel.

## Setup

1. Copier `.env.template` en `.env` et remplir les variables Databricks
2. Mettre à jour `config.yaml` avec les noms de catalogs DEV/PROD
3. `pip install -e ".[dev]"`
4. Générer les données : `python data_generation/generate_bronze.py --catalog <catalog> --rows 50000`
5. Exécuter les notebooks dans l'ordre (01 → 05)

## Structure

- `data_generation/` — scripts de génération des tables Bronze
- `schemas/` — YAML schemas des pipelines Silver et Gold
- `rules/` — business rules enregistrées
- `notebooks/` — notebooks de test par feature
- `semantic_models/` — modèles sémantiques générés (ne pas committer les .yaml LLM-générés)

## Notes

- Le sandbox mode Databricks résout les tables vers `schema_XXXX.table` — ne pas s'en alarmer.
- `.env` et `.skifer_user` sont dans `.gitignore`.
- Les modèles sémantiques dans `semantic_models/` sont générés — les committer après validation manuelle.
```

---

## Checklist de setup initiale

- [ ] Créer le repo `skifer-sandbox` sur GitHub
- [ ] Initialiser `pyproject.toml` et la structure de dossiers
- [ ] Créer `config.yaml` et `.env.template`
- [ ] Implémenter les scripts de génération (`data_generation/`)
- [ ] Créer les règles métier (`rules/order_rules.py`)
- [ ] Créer les schemas YAML (`schemas/silver/` et `schemas/gold/`)
- [ ] Remplir `.env` avec les credentials Databricks (ne pas committer)
- [ ] Exécuter `generate_bronze.py` et vérifier les tables Bronze
- [ ] Exécuter les 4 phases de test dans l'ordre
- [ ] Documenter les anomalies trouvées et les corrections à apporter à Skifer
