"""A ``dbutils`` double for running Databricks source notebooks in the suite (Plan 44, S06)."""

from __future__ import annotations


class FakeWidgets:
    def __init__(self, values: dict[str, str]):
        self.values = dict(values)
        self.declared: list[str] = []

    def _declare(self, name: str, default: str = "", *_args, **_kwargs) -> None:
        self.declared.append(name)
        self.values.setdefault(name, default)

    text = dropdown = combobox = multiselect = _declare

    def get(self, name: str) -> str:
        if name not in self.declared:
            raise ValueError(f"Widget {name!r} was never declared.")
        return self.values[name]


class _Context:
    def __init__(self, user: str):
        self.user = user

    def tags(self):
        return self

    def apply(self, key: str) -> str:
        return {"user": self.user}[key]


class _EntryPoint:
    def __init__(self, user: str):
        self.context = _Context(user)

    def getDbutils(self):
        return self

    def notebook(self):
        return self

    def getContext(self):
        return self.context


class _Notebook:
    def __init__(self, user: str):
        self.entry_point = _EntryPoint(user)


class FakeDbutils:
    """Widgets read from a dict; the notebook context reports ``user``."""

    def __init__(self, widgets: dict[str, str] | None = None, *, user: str = "alice@example.com"):
        self.widgets = FakeWidgets(widgets or {})
        self.notebook = _Notebook(user)
