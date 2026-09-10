# Installing Skifer on Azure Databricks

## Prerequisites

- An Azure Databricks workspace
- A running cluster (Databricks Runtime 11.x or higher recommended)
- The `.whl` file provided by your administrator

---

## Option 1 — Cluster-level installation (recommended)

Installing at the cluster level makes the library available to all notebooks attached to that cluster without any extra `%pip install` line.

1. In your Databricks workspace, go to **Compute** in the left sidebar.
2. Click on your cluster name.
3. Open the **Libraries** tab.
4. Click **Install new**.
5. Select **Upload** as the source, then choose **Python Whl**.
6. Upload the file `skifer-0.6.5-py3-none-any.whl`.
7. Click **Install**.
8. **Restart the cluster** for the library to take effect.

Once the cluster restarts, the library status will show **Installed**. You can then import it in any attached notebook:

```python
from skifer import SkiferEngine, RuleRegistry
```

---

## Option 2 — Notebook-level installation

If you do not have permission to modify the cluster configuration, you can install the library directly from a notebook.

### From DBFS

Upload the `.whl` file to DBFS first (via the Databricks UI: **Catalog** → **Browse DBFS** → upload to `/FileStore/libs/`), then run in a notebook cell:

```python
%pip install /dbfs/FileStore/libs/skifer-0.6.5-py3-none-any.whl
```

### From a local path (Databricks Connect / local dev)

```python
%pip install /path/to/skifer-0.6.5-py3-none-any.whl
```

> **Note:** `%pip install` automatically restarts the Python kernel. Place it in the first cell of your notebook, before any other imports.

---

## Verifying the installation

Run the following cell to confirm the library is correctly installed:

```python
import skifer
print(skifer.__version__)  # expected: 0.6.5
```

---

## Optional dependencies

Skifer has optional dependency groups. Install them alongside the wheel if needed:

| Feature | Extra packages to install |
|---|---|
| LLM (OpenAI) | `pip install openai>=1.0.0` |
| LLM (Anthropic) | `pip install anthropic>=0.30.0` |
| LLM (Google Gemini) | `pip install google-generativeai>=0.5.0` |
| PDF export | `pip install fpdf2>=2.7.0 matplotlib>=3.7.0` |

Install them the same way — either via the cluster **Libraries** tab or with `%pip install` in a notebook cell.

---

## Troubleshooting

**`ModuleNotFoundError: No module named 'skifer'`**
→ The cluster was not restarted after installation. Restart the cluster and reattach your notebook.

**`%pip install` output shows a warning about the kernel restarting**
→ This is expected. Re-run the cells below the `%pip install` cell after the restart.

**Library status shows `Failed` in the cluster Libraries tab**
→ Check that the `.whl` file is not corrupted (re-download or re-upload it) and that the Databricks Runtime version is 11.x or higher.
