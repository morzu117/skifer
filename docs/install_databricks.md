# Installing Skifer on Databricks

Skifer is published on PyPI as [`skifer`](https://pypi.org/project/skifer/). No token and no
manually built wheel are needed.

## Prerequisites

- A Databricks workspace
- A running cluster (Databricks Runtime 11.x or higher recommended)
- Outbound access to PyPI from the cluster

> **Which extra to install** : a Databricks Runtime already ships PySpark and Delta. Install plain
> `skifer` there, so the runtime's own versions are left untouched. Use `skifer[spark]` only for
> local development, where PySpark and Delta have to come from PyPI.

---

## Option 1 — Cluster-level installation (recommended)

Installing at the cluster level makes the library available to every notebook attached to that
cluster, with no `%pip install` line in the notebooks themselves.

1. In your workspace, go to **Compute** in the left sidebar.
2. Click your cluster name.
3. Open the **Libraries** tab.
4. Click **Install new**.
5. Select **PyPI** as the source.
6. Enter `skifer` as the package name, or `skifer==2.1.0` to pin a version.
7. Click **Install**.
8. **Restart the cluster** for the library to take effect.

Once the cluster restarts, the library status shows **Installed** and you can import it from any
attached notebook:

```python
from skifer import SkiferEngine, RuleRegistry
```

---

## Option 2 — Notebook-level installation

If you cannot modify the cluster configuration, install from a notebook cell instead:

```python
%pip install skifer
dbutils.library.restartPython()
```

To pin a version:

```python
%pip install skifer==2.1.0
dbutils.library.restartPython()
```

> **Attention** : `%pip install` restarts the Python kernel. Put it in the first cell of the
> notebook, before any other import, and re-run the cells below it afterwards.

---

## Verifying the installation

```python
import skifer

print(skifer.__version__)   # e.g. 2.1.0
```

---

## Optional dependencies

Skifer declares optional dependency groups. Install them as extras rather than one package at a
time, so version constraints stay consistent:

| Feature | Install |
|---|---|
| Databricks workspace API (SQL warehouses, materialized views) | `skifer[databricks]` |
| LLM — OpenAI | `skifer[llm-openai]` |
| LLM — Anthropic | `skifer[llm-anthropic]` |
| LLM — Google Gemini | `skifer[llm-google]` |
| PDF export | `skifer[semantic-pdf]` |
| Full semantic layer | `skifer[semantic-full]` |
| Read-only MCP server | `skifer[mcp]` |
| Runtime tracing | `skifer[tracing]` |
| Model Serving deployment | `skifer[serving]` |

Combine them in one install:

```python
%pip install "skifer[databricks,llm-anthropic]"
dbutils.library.restartPython()
```

The same names work in the cluster **Libraries** tab, in the PyPI package field.

---

## Pre-release builds

Every push to `main` publishes a build to TestPyPI. To install one:

```python
%pip install --index-url https://test.pypi.org/simple/ \
             --extra-index-url https://pypi.org/simple/ skifer
dbutils.library.restartPython()
```

The second index is required: Skifer's dependencies live on PyPI, not on TestPyPI.

---

## Troubleshooting

**`ModuleNotFoundError: No module named 'skifer'`**
→ The cluster was not restarted after a cluster-level install. Restart it and reattach the notebook.
After a notebook-level install, check that `dbutils.library.restartPython()` actually ran.

**Library status shows `Failed` in the cluster Libraries tab**
→ Most often the cluster has no outbound access to PyPI. Check with your workspace administrator,
who may need to configure an internal package mirror.

**`skifer.__version__` reports an unexpected version**
→ Two installs are shadowing each other, typically a cluster-level library plus a notebook-level
`%pip install`. Remove one of the two.

**Spark or Delta errors right after installing `skifer[spark]` on a cluster**
→ That extra pulls PySpark and Delta from PyPI and overrides the runtime's own versions. Install
plain `skifer` on Databricks instead.
