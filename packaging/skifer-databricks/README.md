# skifer-databricks

`skifer-databricks` is the Databricks Model Serving layer of Skifer: an MLflow `ResponsesAgent` over the governed semantic layer, for a Genie supervisor or a serving endpoint. Install it through `skifer[serving]`.

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
