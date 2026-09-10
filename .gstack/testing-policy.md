# Testing Policy

## Core Rules

- Run the closest relevant tests for every implementation step.
- Add or update tests when behavior changes.
- Do not hide or ignore failed tests.
- If a verification step cannot be run, record why in the feature summary.

## Default Commands

For source changes:

```bash
.venv/bin/python -m pytest tests/ -x --tb=short
.venv/bin/python -m ruff check src/
```

For targeted source work:

```bash
.venv/bin/python -m pytest tests/test_<module>.py -x --tb=short
```

For documentation/metadata-only changes:

```bash
python3 -c "import tomllib; tomllib.load(open('pyproject.toml','rb'))"
```

Spark tests may need to run outside the filesystem sandbox because Py4J binds to
localhost.

