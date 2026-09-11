"""Spark-free rule discovery, inspection, snippet generation, and persistence."""

from __future__ import annotations

import ast
from dataclasses import dataclass
import hashlib
import importlib.util
import os
from pathlib import Path
import re
import sys
import tempfile
from typing import Any

from skifer.core.op_catalog import resolve_filter_operator
from skifer.core.registry import RuleRegistry
from skifer.core.rule_analyzer import RuleAnalyzer, RuleProfile
from skifer.services.context import (
    InvalidRequest,
    RequestContext,
    ResourceNotFound,
    SCOPE_PROJECT_READ,
    SCOPE_RULES_WRITE,
    require_scope,
)


_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_CAST_TYPE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*(?:\(\d+(?:,\d+)?\))?$")
_SNIPPET_KINDS = frozenset({"constant", "cast", "when_otherwise", "with_column"})


@dataclass(frozen=True)
class RuleView:
    name: str
    kind: str
    reads: tuple[str, ...]
    writes: tuple[str, ...]
    warnings: tuple[str, ...]
    file: str
    line: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "kind": self.kind,
            "reads": list(self.reads),
            "writes": list(self.writes),
            "warnings": list(self.warnings),
            "file": self.file,
            "line": self.line,
        }


@dataclass(frozen=True)
class ScanReport:
    rules: tuple[RuleView, ...]
    invalid_modules: tuple[dict[str, str], ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "rules": [rule.to_dict() for rule in self.rules],
            "invalid_modules": [dict(module) for module in self.invalid_modules],
        }


@dataclass(frozen=True)
class SnippetSpec:
    kind: str
    target: str
    value: Any = None
    source: str | None = None
    to_type: str | None = None
    condition: str | None = None
    then: Any = None
    expression: str | None = None


class RuleService:
    """Application service for rule files, with no Spark runtime boundary."""

    def __init__(self, project_dir: str, extra_paths: tuple[str, ...] = ()):
        self._root = Path(project_dir).expanduser().resolve()
        self._extra_paths = extra_paths
        self._last_report = ScanReport(rules=(), invalid_modules=())
        self._has_scanned = False
        self._scan_generation = 0

    def scan(
        self, ctx: RequestContext, paths: tuple[str, ...] = ()
    ) -> ScanReport:
        require_scope(ctx, SCOPE_PROJECT_READ)
        analyzer = RuleAnalyzer()
        discovered: dict[str, tuple[RuleView, RuleProfile]] = {}
        invalid_modules: list[dict[str, str]] = []
        self._scan_generation += 1

        for index, module_path in enumerate(self._module_paths(paths)):
            display_path = self._display_path(module_path)
            module_key = f"{module_path}\0{self._scan_generation}\0{index}"
            module_digest = hashlib.sha256(module_key.encode("utf-8")).hexdigest()
            module_name = f"_skifer_rule_scan_{module_digest}"
            registry_before = dict(RuleRegistry._rules)
            try:
                spec = importlib.util.spec_from_file_location(module_name, module_path)
                if spec is None or spec.loader is None:
                    raise ImportError("No import loader is available for the rule module.")
                module = importlib.util.module_from_spec(spec)
                sys.modules[module_name] = module
                try:
                    spec.loader.exec_module(module)
                finally:
                    sys.modules.pop(module_name, None)
            except BaseException as exc:
                RuleRegistry._rules.clear()
                RuleRegistry._rules.update(registry_before)
                invalid_modules.append(
                    {"file": display_path, "error": type(exc).__name__}
                )
                continue

            for rule_name in RuleRegistry.list_rules():
                rule_spec = RuleRegistry.get_rule(rule_name)
                func = rule_spec.func
                if getattr(func, "__module__", None) != module_name:
                    continue
                profile = analyzer.analyze_rule(func, rule_name)
                discovered[rule_name] = (
                    RuleView(
                        name=rule_name,
                        kind=rule_spec.kind,
                        reads=tuple(profile.input_columns),
                        writes=tuple(profile.output_columns),
                        warnings=(),
                        file=display_path,
                        line=func.__code__.co_firstlineno,
                    ),
                    profile,
                )

        profiles = [item[1] for item in discovered.values()]
        warnings_by_rule: dict[str, list[str]] = {
            name: [] for name in discovered
        }
        for warning in analyzer.detect_warnings(profiles):
            for rule_name in warning.rules:
                if (
                    rule_name in warnings_by_rule
                    and warning.code not in warnings_by_rule[rule_name]
                ):
                    warnings_by_rule[rule_name].append(warning.code)

        views = tuple(
            RuleView(
                name=view.name,
                kind=view.kind,
                reads=view.reads,
                writes=view.writes,
                warnings=tuple(warnings_by_rule[name]),
                file=view.file,
                line=view.line,
            )
            for name, (view, _profile) in sorted(discovered.items())
        )
        self._last_report = ScanReport(
            rules=views,
            invalid_modules=tuple(invalid_modules),
        )
        self._has_scanned = True
        return self._last_report

    def list(self, ctx: RequestContext) -> tuple[RuleView, ...]:
        require_scope(ctx, SCOPE_PROJECT_READ)
        if not self._has_scanned:
            return self.scan(ctx).rules
        return self._last_report.rules

    def dependency_graph(
        self, ctx: RequestContext, names: tuple[str, ...]
    ) -> dict[str, list[str]]:
        require_scope(ctx, SCOPE_PROJECT_READ)
        if not self._has_scanned:
            self.scan(ctx)
        analyzer = RuleAnalyzer()
        return analyzer.build_dependency_graph(analyzer.analyze_rules(list(names)))

    def generate_snippet(self, ctx: RequestContext, spec: SnippetSpec) -> str:
        require_scope(ctx, SCOPE_PROJECT_READ)
        if not isinstance(spec, SnippetSpec):
            raise InvalidRequest("Snippet spec must be a SnippetSpec instance.")
        if spec.kind not in _SNIPPET_KINDS:
            raise InvalidRequest("Unknown rule snippet kind.")
        self._require_identifier(spec.target, "target")
        name = spec.target

        if spec.kind == "constant":
            expression = f"F.lit({spec.value!r})"
        elif spec.kind == "cast":
            self._require_identifier(spec.source, "source")
            if not isinstance(spec.to_type, str) or not _CAST_TYPE.fullmatch(
                spec.to_type
            ):
                raise InvalidRequest("Cast type is invalid.")
            expression = f'F.col("{spec.source}").cast("{spec.to_type}")'
        elif spec.kind == "when_otherwise":
            condition = self._compile_condition(spec.condition)
            expression = (
                f"F.when({condition}, F.lit({spec.then!r}))"
                f".otherwise(F.lit({spec.value!r}))"
            )
        else:
            self._require_identifier(spec.source, "source")
            if not isinstance(spec.expression, str) or not spec.expression.strip():
                raise InvalidRequest("with_column expression must be non-empty text.")
            try:
                expression_tree = ast.parse(spec.expression, mode="eval")
            except SyntaxError as exc:
                raise InvalidRequest("with_column expression is invalid Python.") from exc
            self._validate_expression_columns(expression_tree)
            return (
                f"def {name}(df):\n"
                f'    return df.withColumn("{spec.target}", {spec.expression})\n'
            )

        return f"def {name}(df):\n    return {{\"{spec.target}\": {expression}}}\n"

    def write_rule(self, ctx: RequestContext, file: str, code: str) -> str:
        require_scope(ctx, SCOPE_RULES_WRITE)
        target = self._resolve_project_path(file)
        if not isinstance(code, str):
            raise InvalidRequest("Rule code must be a string.")
        try:
            ast.parse(code)
        except (SyntaxError, IndentationError) as exc:
            raise InvalidRequest("Rule code is invalid Python.") from exc

        target.parent.mkdir(parents=True, exist_ok=True)
        temporary_path = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=target.parent,
                prefix=f".{target.stem}.",
                suffix=".tmp",
                delete=False,
            ) as handle:
                temporary_path = handle.name
                handle.write(code)
            os.replace(temporary_path, target)
        except Exception:
            if temporary_path is not None:
                try:
                    os.unlink(temporary_path)
                except FileNotFoundError:
                    pass
            raise
        return str(target)

    def _module_paths(self, paths: tuple[str, ...]) -> tuple[Path, ...]:
        candidates: list[Path] = []
        default_rules = self._root / "rules"
        if default_rules.is_dir():
            candidates.extend(default_rules.rglob("*.py"))

        for declared in (*paths, *self._extra_paths):
            if not isinstance(declared, str) or not declared.strip():
                raise InvalidRequest("Rule scan paths must be non-empty text.")
            candidate = Path(declared).expanduser()
            if not candidate.is_absolute():
                candidate = self._root / candidate
            candidate = candidate.resolve()
            if candidate.is_dir():
                candidates.extend(candidate.rglob("*.py"))
            elif candidate.is_file() and candidate.suffix == ".py":
                candidates.append(candidate)
            else:
                raise ResourceNotFound("Rule scan path was not found.")

        return tuple(sorted(set(path.resolve() for path in candidates)))

    def _resolve_project_path(self, file: str) -> Path:
        if not isinstance(file, str) or not file.strip():
            raise InvalidRequest("Rule file must be non-empty text.")
        candidate = (self._root / file).resolve()
        if not candidate.is_relative_to(self._root):
            raise InvalidRequest("path escapes the project root")
        return candidate

    def _display_path(self, path: Path) -> str:
        try:
            return path.relative_to(self._root).as_posix()
        except ValueError:
            return str(path)

    @staticmethod
    def _require_identifier(value: Any, field_name: str) -> None:
        if not isinstance(value, str) or not _IDENTIFIER.fullmatch(value):
            raise InvalidRequest(f"{field_name} must be a valid identifier.")

    def _compile_condition(self, condition: str | None) -> str:
        if not isinstance(condition, str):
            raise InvalidRequest("when_otherwise condition must be text.")
        parts = condition.split(":", 2)
        if len(parts) < 2:
            raise InvalidRequest("when_otherwise condition must use col:op:val grammar.")
        column, operator = parts[:2]
        value = parts[2] if len(parts) == 3 else None
        self._require_identifier(column, "condition column")
        canonical = resolve_filter_operator(operator)
        if canonical is None or canonical == "sql":
            raise InvalidRequest("when_otherwise condition operator is invalid.")

        col = f'F.col("{column}")'
        comparisons = {
            "equals": "==",
            "not_equals": "!=",
            "greater_than": ">",
            "less_than": "<",
            "greater_than_equal": ">=",
            "less_than_equal": "<=",
        }
        if canonical in comparisons:
            if value is None:
                raise InvalidRequest("when_otherwise condition requires a value.")
            return f"{col} {comparisons[canonical]} {value!r}"
        if canonical == "is_null":
            return f"{col}.isNull()"
        if canonical == "is_not_null":
            return f"{col}.isNotNull()"
        if value is None:
            raise InvalidRequest("when_otherwise condition requires a value.")
        methods = {
            "contains": "contains",
            "starts_with": "startswith",
            "ends_with": "endswith",
            "like": "like",
        }
        if canonical in methods:
            return f"{col}.{methods[canonical]}({value!r})"
        if canonical in {"not_contains", "not_like"}:
            method = "contains" if canonical == "not_contains" else "like"
            return f"~{col}.{method}({value!r})"
        if canonical in {"in", "not_in"}:
            values = [item.strip() for item in value.split(",")]
            rendered = f"{col}.isin({', '.join(repr(item) for item in values)})"
            return f"~{rendered}" if canonical == "not_in" else rendered
        if canonical in {"between", "not_between"}:
            values = [item.strip() for item in value.split(",")]
            if len(values) != 2:
                raise InvalidRequest("between conditions require exactly two values.")
            rendered = f"{col}.between({values[0]!r}, {values[1]!r})"
            return f"~{rendered}" if canonical == "not_between" else rendered
        raise InvalidRequest("when_otherwise condition operator is unsupported.")

    def _validate_expression_columns(self, tree: ast.AST) -> None:
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or not node.args:
                continue
            func = node.func
            is_col = (
                isinstance(func, ast.Attribute)
                and func.attr == "col"
            ) or (isinstance(func, ast.Name) and func.id == "col")
            if not is_col:
                continue
            argument = node.args[0]
            if not isinstance(argument, ast.Constant):
                raise InvalidRequest("Expression columns must be string literals.")
            self._require_identifier(argument.value, "expression column")


__all__ = ["RuleService", "RuleView", "ScanReport", "SnippetSpec"]
