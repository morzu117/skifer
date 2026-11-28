# skifer-engine

`skifer-engine` is the Spark and Databricks runtime of Skifer: it executes compiled pipelines on PySpark and Delta (Databricks or local), with the built-in loaders, streaming tables and materialized-view writes. Install it through `skifer[spark]`.

## Install by platform

| Platform | Command |
|---|---|
| Databricks (notebook, job, Databricks Connect) or local Spark + Delta | `pip install 'skifer[spark]'` |
| Databricks Model Serving (Genie supervisor, serving endpoint) | `pip install 'skifer[serving]'` |
| DuckDB | `pip install skifer-duckdb` |
| Snowflake | `pip install skifer-snowflake` |
| Governed layer without Spark (semantic layer, MCP server, local API) | `pip install skifer` |
| SQL compiler and dialect rendering only | `pip install 'skifer-core[dialects]'` |

Local Spark needs a Java runtime (JDK 8–17); Databricks and Databricks Connect do not.

## Learn more

- Documentation: https://morzu117.github.io/skifer
- Source, examples and changelog: https://github.com/morzu117/skifer
