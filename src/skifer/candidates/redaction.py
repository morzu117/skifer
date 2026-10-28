"""Keep the structure of a question, never its values (Plan 44, D2).

A filter value is what a user typed: a customer name, an amount, an e-mail. The
candidate registry must be able to replay a question's *shape* without ever
holding one, so every value is replaced by a ``{{ pN }}`` parameter before a
candidate is built. The mapping from parameter to value is returned to the caller
for an immediate re-execution and is never persisted.

Filters live in five places — ``tables[].filter``, ``tables[].filter_groups``,
``where``, ``where_groups`` and ``aggregate.having`` — and each accepts the string
form (``"col:op:value"``), the normalized dict form (``{column, operator, value}``)
and, for ``filter``/``where``, the mapping form (``{col: value}``).
"""

from __future__ import annotations

import copy
import hashlib
import re
import unicodedata
from typing import Any, Callable

from skifer.core.op_catalog import FILTER_OPERATORS, resolve_filter_operator


HASH_PREFIX = "sha256:v1:"
RAW_SQL_REFUSAL = "Raw SQL filters cannot be stored in a candidate."
_WHITESPACE = re.compile(r"\s+")


def _canonical_operator(operator: Any) -> str:
    name = str(operator).strip()
    return resolve_filter_operator(name) or name


def _arity(canonical: str) -> str:
    spec = FILTER_OPERATORS.get(canonical)
    return spec.arity if spec is not None else "single"


# --- generic walk over the five filter locations -------------------------------------


def _walk_filter_locations(schema_dict: dict, on_container: Callable[[Any], Any]) -> None:
    """Replace, in place, each filter container of ``schema_dict`` by ``on_container(c)``.

    Groups (``filter_groups``/``where_groups``) are lists of containers. The walk
    order is fixed — tables in list order, then ``where``, ``where_groups`` and
    ``having`` — so parameter numbering is deterministic.
    """
    for table in schema_dict.get("tables") or []:
        if not isinstance(table, dict):
            continue
        if table.get("filter"):
            table["filter"] = on_container(table["filter"])
        if table.get("filter_groups"):
            table["filter_groups"] = [on_container(group) for group in table["filter_groups"]]
    if schema_dict.get("where"):
        schema_dict["where"] = on_container(schema_dict["where"])
    if schema_dict.get("where_groups"):
        schema_dict["where_groups"] = [
            on_container(group) for group in schema_dict["where_groups"]
        ]
    aggregate = schema_dict.get("aggregate")
    if isinstance(aggregate, dict) and aggregate.get("having"):
        aggregate["having"] = on_container(aggregate["having"])


def _is_filter_dict(item: Any) -> bool:
    return isinstance(item, dict) and "column" in item and "operator" in item


# --- parameterization -----------------------------------------------------------------


class _Parameterizer:
    def __init__(self) -> None:
        self.values: dict[str, Any] = {}

    def placeholder(self, value: Any) -> str:
        name = f"p{len(self.values) + 1}"
        self.values[name] = value
        return "{{ " + name + " }}"

    def container(self, container: Any) -> Any:
        if isinstance(container, dict) and not _is_filter_dict(container):
            return {column: self.mapping_value(value) for column, value in container.items()}
        if isinstance(container, list):
            return [self.item(item) for item in container]
        return self.item(container)

    def item(self, item: Any) -> Any:
        if isinstance(item, str):
            return self.string_filter(item)
        if _is_filter_dict(item):
            return self.dict_filter(item)
        if isinstance(item, dict):
            # A mapping-form filter nested in a list: same rules as a container.
            return {column: self.mapping_value(value) for column, value in item.items()}
        return item

    def string_filter(self, text: str) -> str:
        parts = text.split(":", 2)
        if len(parts) < 3:
            return text
        canonical = _canonical_operator(parts[1])
        if canonical == "sql":
            raise ValueError(RAW_SQL_REFUSAL)
        if _arity(canonical) == "none":
            return text
        raw = parts[2].strip()
        if _arity(canonical) == "list":
            value = ",".join(
                self.placeholder(element.strip())
                for element in raw.replace(";", ",").split(",")
            )
        else:
            value = self.placeholder(raw)
        return f"{parts[0]}:{parts[1]}:{value}"

    def dict_filter(self, item: dict) -> dict:
        canonical = _canonical_operator(item["operator"])
        if canonical == "sql":
            raise ValueError(RAW_SQL_REFUSAL)
        result = dict(item)
        if "value" not in item or item["value"] is None or _arity(canonical) == "none":
            return result
        if isinstance(item["value"], list):
            result["value"] = [self.placeholder(element) for element in item["value"]]
        else:
            result["value"] = self.placeholder(item["value"])
        return result

    def mapping_value(self, value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, dict):
            parameterized = {}
            for operator, operand in value.items():
                canonical = _canonical_operator(operator)
                if canonical == "sql":
                    raise ValueError(RAW_SQL_REFUSAL)
                if operand is None or _arity(canonical) == "none":
                    parameterized[operator] = operand
                elif isinstance(operand, list):
                    parameterized[operator] = [self.placeholder(item) for item in operand]
                else:
                    parameterized[operator] = self.placeholder(operand)
            return parameterized
        if isinstance(value, str):
            canonical = resolve_filter_operator(value)
            if canonical is not None and _arity(canonical) == "none":
                return value  # a bare no-value operator, e.g. ``col: not_null``
        return self.placeholder(value)


def parameterize_filters(schema_dict: dict) -> tuple[dict, dict[str, Any]]:
    """Replace every filter value by ``{{ pN }}``; return (schema, {pN: value}).

    The schema is deep-copied, never mutated. The second element holds the real
    values for an immediate re-execution and must never be persisted. A raw
    ``sql`` filter is refused: its "value" is an expression, which can neither be
    parameterized nor stored.
    """
    if not isinstance(schema_dict, dict):
        raise TypeError("schema_dict must be a dict.")
    result = copy.deepcopy(schema_dict)
    parameterizer = _Parameterizer()
    _walk_filter_locations(result, parameterizer.container)
    return result, parameterizer.values


# --- fingerprint normal form -----------------------------------------------------------


def _shape_items(container: Any) -> list[dict]:
    """Value-free canonical shapes of a filter container, in a stable order."""
    shapes: list[dict] = []

    def add(column: Any, operator: Any, has_value: bool, raw_value: Any = None) -> None:
        canonical = _canonical_operator(operator)
        shape = {"column": str(column).strip(), "operator": canonical}
        if has_value and _arity(canonical) != "none":
            # A raw SQL expression is structure, not a user value.
            shape["value"] = raw_value if canonical == "sql" else "?"
        shapes.append(shape)

    def mapping(entries: dict) -> None:
        for column, value in entries.items():
            if value is None:
                add(column, "is_null", False)
            elif isinstance(value, dict):
                for operator, operand in value.items():
                    add(column, operator, operand is not None, operand)
            elif isinstance(value, str) and resolve_filter_operator(value) is not None and (
                _arity(resolve_filter_operator(value)) == "none"
            ):
                add(column, value, False)
            else:
                add(column, "equals", True, value)

    items = container if isinstance(container, list) else [container]
    if isinstance(container, dict) and not _is_filter_dict(container):
        mapping(container)
        items = []
    for item in items:
        if isinstance(item, str):
            parts = item.split(":", 2)
            if len(parts) >= 2:
                add(parts[0], parts[1], len(parts) == 3, parts[2].strip() if len(parts) == 3 else None)
            else:
                shapes.append({"unparsed": "?"})
        elif _is_filter_dict(item):
            add(item["column"], item["operator"], item.get("value") is not None, item.get("value"))
        elif isinstance(item, dict):
            mapping(item)
    return shapes


def _sorted_shapes(container: Any, canonical_json: Callable[[Any], str]) -> list[dict]:
    return sorted(_shape_items(container), key=canonical_json)


_PRESENTATION_KEYS = frozenset({"dev_limit", "description"})


def _strip_presentation(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: _strip_presentation(item)
            for key, item in value.items()
            if key not in _PRESENTATION_KEYS
        }
    if isinstance(value, list):
        return [_strip_presentation(item) for item in value]
    return value


def _measure_form(measure: Any) -> Any:
    if isinstance(measure, list) and len(measure) == 3:
        return {"source": measure[0], "target": measure[1], "func": measure[2]}
    return measure


def fingerprint_normal_form(schema_dict: dict, canonical_json: Callable[[Any], str]) -> dict:
    """The value-free, order-insensitive form of ``schema_dict`` that is hashed."""
    if not isinstance(schema_dict, dict):
        raise TypeError("schema_dict must be a dict.")
    form = _strip_presentation(copy.deepcopy(schema_dict))

    def container(value: Any) -> list[dict]:
        return _sorted_shapes(value, canonical_json)

    for table in form.get("tables") or []:
        if not isinstance(table, dict):
            continue
        if table.get("filter"):
            table["filter"] = container(table["filter"])
        if table.get("filter_groups"):
            table["filter_groups"] = sorted(
                (container(group) for group in table["filter_groups"]), key=canonical_json
            )
    if isinstance(form.get("tables"), list):
        form["tables"] = sorted(
            form["tables"],
            key=lambda table: canonical_json(
                [table.get("alias") or table.get("name"), table]
                if isinstance(table, dict) else [None, table]
            ),
        )
    if form.get("where"):
        form["where"] = container(form["where"])
    if form.get("where_groups"):
        form["where_groups"] = sorted(
            (container(group) for group in form["where_groups"]), key=canonical_json
        )
    aggregate = form.get("aggregate")
    if isinstance(aggregate, dict):
        if isinstance(aggregate.get("group_by"), list):
            aggregate["group_by"] = sorted(aggregate["group_by"], key=canonical_json)
        if isinstance(aggregate.get("measures"), list):
            aggregate["measures"] = sorted(
                (_measure_form(measure) for measure in aggregate["measures"]),
                key=lambda measure: canonical_json(
                    [measure.get("target") if isinstance(measure, dict) else None, measure]
                ),
            )
        if aggregate.get("having"):
            aggregate["having"] = container(aggregate["having"])
    return form


# --- question and requester ------------------------------------------------------------


def normalize_question(question: str) -> str:
    """NFKC, case-folded, whitespace-collapsed: reformatting is not a new question."""
    if not isinstance(question, str) or not question.strip():
        raise ValueError("question must be a non-empty string.")
    return _WHITESPACE.sub(" ", unicodedata.normalize("NFKC", question)).strip().casefold()


def question_hash(question: str) -> str:
    """``sha256:v1:`` hash of the normalized question, kept even in ``redacted`` mode."""
    digest = hashlib.sha256(normalize_question(question).encode("utf-8")).hexdigest()
    return f"{HASH_PREFIX}{digest}"


def requester_pseudonym(user_id: str | None, policy: Any) -> str | None:
    """Pseudonymize a requester through the tracing ``TraceAttributePolicy``.

    Returns ``None`` when the policy is not ``hmac`` or has no secret: an unkeyed
    hash of an identity is reversible, so no identity beats a fake one.
    """
    if policy is None:
        return None
    return policy.pseudonymize(user_id)
