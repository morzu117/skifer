"""
Tests for the ``partials:`` block (Plan 25) — nested YAML sub-transformations.

Phase 25.1 covers load-time behaviour only: recursive parsing, path resolution,
param inheritance, alias uniqueness and cycle detection. No Spark execution here.
"""
import pytest

from skifer.core.schema_loader import load_schema, parse_schema
from skifer.core.ir import parse_to_ir
from tests.fakes.fake_backend import FakeBackend, FakeDataFrame


def _write(dirpath, name, content):
    p = dirpath / name
    p.write_text(content, encoding="utf-8")
    return p


def _make_engine_with_fake(tables=None):
    """SkiferEngine backed by FakeBackend — no PySpark (mirrors test_engine_fake_backend)."""
    from skifer.core.context import ExecutionContext
    from skifer.core.interpreter import SchemaInterpreter
    from skifer.core.core import SkiferEngine
    engine = object.__new__(SkiferEngine)
    ctx = ExecutionContext()
    object.__setattr__(engine, "_context", ctx)
    engine.spark = None
    engine.is_local = True
    engine.db = None
    engine.env = "local"
    engine.config = {"environments": {"local": {}}}
    engine.schema_suffix = ""
    engine.is_job_execution = False
    b = FakeBackend(tables=tables or {})
    engine._backend = b
    engine._interpreter = SchemaInterpreter(backend=b, context=ctx)
    return engine


# ==============================================================================
# Parsing & path resolution
# ==============================================================================

def test_partial_loaded_and_attached(tmp_path):
    """A partial entry loads the child schema and attaches it under 'schema'."""
    _write(tmp_path, "child.yaml", """
tables:
  - name: silver.base
    alias: base
select_final:
  - [id, id, []]
""")
    parent = _write(tmp_path, "parent.yaml", """
partials:
  - alias: dly
    path: child.yaml
tables:
  - name: silver.cal
    alias: cal
select_final:
  - [id, id, []]
""")
    schema = load_schema(str(parent))
    assert len(schema["partials"]) == 1
    entry = schema["partials"][0]
    assert entry["alias"] == "dly"
    assert entry["resolved_path"].endswith("child.yaml")
    # child schema is fully normalized
    assert entry["schema"]["tables"][0]["name"] == "silver.base"


def test_partial_path_resolves_relative_to_parent_dir(tmp_path):
    """Relative partial paths resolve from the parent YAML directory, not cwd."""
    sub = tmp_path / "_partials"
    sub.mkdir()
    _write(sub, "base.yaml", """
tables:
  - name: silver.base
    alias: base
select_final:
  - [id, id, []]
""")
    parent = _write(tmp_path, "parent.yaml", """
partials:
  - alias: base
    path: _partials/base.yaml
tables:
  - name: silver.cal
    alias: cal
select_final:
  - [id, id, []]
""")
    schema = load_schema(str(parent))
    assert schema["partials"][0]["resolved_path"].endswith("_partials/base.yaml")


def test_partial_alias_usable_in_join(tmp_path):
    """A join may reference a partial alias without triggering unknown-alias errors."""
    _write(tmp_path, "child.yaml", """
tables:
  - name: silver.base
    alias: base
select_final:
  - [id, id, []]
  - [work_day, work_day, []]
""")
    parent = _write(tmp_path, "parent.yaml", """
partials:
  - alias: dly
    path: child.yaml
tables:
  - name: silver.cal
    alias: cal
join:
  - table_from: [dly, work_day]
    table_to: [cal, cal_date]
    type: left
select_final:
  - [id, id, []]
""")
    # Should not raise on join-reference validation.
    schema = load_schema(str(parent))
    assert schema["join"][0]["table_from"] == "dly"


# ==============================================================================
# Param inheritance
# ==============================================================================

def test_partial_inherits_parent_params(tmp_path):
    """Child schema resolves {{ param }} placeholders from the parent's params."""
    _write(tmp_path, "child.yaml", """
tables:
  - name: "{{ catalog }}.silver.base"
    alias: base
select_final:
  - [id, id, []]
""")
    parent = _write(tmp_path, "parent.yaml", """
partials:
  - alias: dly
    path: child.yaml
tables:
  - name: "{{ catalog }}.silver.cal"
    alias: cal
select_final:
  - [id, id, []]
""")
    schema = load_schema(str(parent), params={"catalog": "prod_cat"})
    assert schema["partials"][0]["schema"]["tables"][0]["name"] == "prod_cat.silver.base"


def test_partial_missing_param_fails(tmp_path):
    """An unresolved placeholder in the child raises (inherited param missing)."""
    _write(tmp_path, "child.yaml", """
tables:
  - name: "{{ catalog }}.silver.base"
    alias: base
select_final:
  - [id, id, []]
""")
    parent = _write(tmp_path, "parent.yaml", """
partials:
  - alias: dly
    path: child.yaml
tables:
  - name: silver.cal
    alias: cal
select_final:
  - [id, id, []]
""")
    with pytest.raises(ValueError, match="Missing template parameters"):
        load_schema(str(parent))


# ==============================================================================
# Validation
# ==============================================================================

def test_partial_alias_collides_with_table(tmp_path):
    """A partial alias equal to a table alias fails fast."""
    _write(tmp_path, "child.yaml", """
tables:
  - name: silver.base
    alias: base
select_final:
  - [id, id, []]
""")
    parent = _write(tmp_path, "parent.yaml", """
partials:
  - alias: cal
    path: child.yaml
tables:
  - name: silver.cal
    alias: cal
select_final:
  - [id, id, []]
""")
    with pytest.raises(ValueError, match="collides with a 'tables' alias"):
        load_schema(str(parent))


def test_partial_duplicate_alias(tmp_path):
    """Two partials with the same alias fail fast."""
    _write(tmp_path, "child.yaml", """
tables:
  - name: silver.base
    alias: base
select_final:
  - [id, id, []]
""")
    parent = _write(tmp_path, "parent.yaml", """
partials:
  - alias: dly
    path: child.yaml
  - alias: dly
    path: child.yaml
tables:
  - name: silver.cal
    alias: cal
select_final:
  - [id, id, []]
""")
    with pytest.raises(ValueError, match="Duplicate partial alias"):
        load_schema(str(parent))


def test_partial_missing_path(tmp_path):
    """A partial entry without 'path' fails fast."""
    parent = _write(tmp_path, "parent.yaml", """
partials:
  - alias: dly
tables:
  - name: silver.cal
    alias: cal
select_final:
  - [id, id, []]
""")
    with pytest.raises(ValueError, match="missing required string field 'path'"):
        load_schema(str(parent))


def test_partial_file_not_found(tmp_path):
    """A partial pointing to a missing file raises FileNotFoundError."""
    parent = _write(tmp_path, "parent.yaml", """
partials:
  - alias: dly
    path: does_not_exist.yaml
tables:
  - name: silver.cal
    alias: cal
select_final:
  - [id, id, []]
""")
    with pytest.raises(FileNotFoundError):
        load_schema(str(parent))


def test_partial_relative_path_without_base_dir_fails():
    """parse_schema (string form) rejects a relative partial path."""
    yaml_str = """
partials:
  - alias: dly
    path: child.yaml
tables:
  - name: silver.cal
    alias: cal
select_final:
  - [id, id, []]
"""
    with pytest.raises(ValueError, match="relative path 'child.yaml' requires"):
        parse_schema(yaml_str)


# ==============================================================================
# Cycle detection
# ==============================================================================

def test_partial_self_reference_cycle(tmp_path):
    """A schema referencing itself as a partial fails fast."""
    parent = _write(tmp_path, "parent.yaml", """
partials:
  - alias: me
    path: parent.yaml
tables:
  - name: silver.cal
    alias: cal
select_final:
  - [id, id, []]
""")
    with pytest.raises(ValueError, match="Cyclic schema reference"):
        load_schema(str(parent))


def test_partial_indirect_cycle(tmp_path):
    """An A -> B -> A partial cycle fails fast."""
    _write(tmp_path, "a.yaml", """
partials:
  - alias: b
    path: b.yaml
tables:
  - name: silver.a
    alias: a
select_final:
  - [id, id, []]
""")
    _write(tmp_path, "b.yaml", """
partials:
  - alias: a
    path: a.yaml
tables:
  - name: silver.b
    alias: b
select_final:
  - [id, id, []]
""")
    with pytest.raises(ValueError, match="Cyclic schema reference"):
        load_schema(str(tmp_path / "a.yaml"))


# ==============================================================================
# IR
# ==============================================================================

def test_partials_in_ir(tmp_path):
    """parse_to_ir exposes partials as ParsedPartial entries."""
    _write(tmp_path, "child.yaml", """
tables:
  - name: silver.base
    alias: base
select_final:
  - [id, id, []]
""")
    parent = _write(tmp_path, "parent.yaml", """
partials:
  - alias: dly
    path: child.yaml
tables:
  - name: silver.cal
    alias: cal
select_final:
  - [id, id, []]
""")
    ir = parse_to_ir(load_schema(str(parent)))
    assert len(ir.partials) == 1
    assert ir.partials[0].alias == "dly"
    assert ir.partials[0].resolved_path.endswith("child.yaml")
    assert ir.partials[0].schema["tables"][0]["name"] == "silver.base"


# ==============================================================================
# Execution — inline mode (Phase 25.2, FakeBackend, no Spark)
# ==============================================================================

def test_inline_partial_output_available_under_alias():
    """A partial's output is exposed under its alias and drives the parent output."""
    engine = _make_engine_with_fake({
        "silver.base": [{"id": 1, "work_day": "2026-01-01"},
                        {"id": 2, "work_day": "2026-01-02"},
                        {"id": 3, "work_day": "2026-01-03"}],
    })
    schema = {
        "partials": [{"alias": "dly", "schema": {
            "tables": [{"name": "silver.base", "alias": "base"}],
        }}],
        "keep_all_columns": True,
    }
    result = engine.process_schema(schema)
    assert isinstance(result, FakeDataFrame)
    assert len(result) == 3


def test_inline_partial_joined_with_table():
    """A join can reference a partial alias; result combines partial + table."""
    engine = _make_engine_with_fake({
        "silver.base": [{"id": 1, "work_day": "D1"}, {"id": 2, "work_day": "D2"}],
        "silver.cal": [{"work_day": "D1", "label": "Mon"}, {"work_day": "D2", "label": "Tue"}],
    })
    schema = {
        "partials": [{"alias": "dly", "schema": {
            "tables": [{"name": "silver.base", "alias": "base"}],
        }}],
        "tables": [{"name": "silver.cal", "alias": "cal"}],
        "join": [{"table_from": "dly", "on_from": "work_day",
                  "table_to": "cal", "on_to": "work_day", "type": "left"}],
        "keep_all_columns": True,
    }
    result = engine.process_schema(schema)
    assert len(result) == 2


def test_nested_partials_recursion():
    """A partial that itself declares a partial is expanded recursively."""
    engine = _make_engine_with_fake({
        "silver.leaf": [{"id": 1}, {"id": 2}],
    })
    inner = {"tables": [{"name": "silver.leaf", "alias": "leaf"}]}
    child = {
        "partials": [{"alias": "leaf_p", "schema": inner}],
        "keep_all_columns": True,
    }
    parent = {
        "partials": [{"alias": "mid", "schema": child}],
        "keep_all_columns": True,
    }
    result = engine.process_schema(parent)
    assert len(result) == 2


def test_dataframes_in_overrides_partial():
    """A partial alias supplied via dataframes_in is not re-executed."""
    engine = _make_engine_with_fake()  # no tables — child would fail if executed
    prebuilt = FakeDataFrame([{"id": 9}], name="prebuilt")
    schema = {
        "partials": [{"alias": "dly", "schema": {
            "tables": [{"name": "silver.missing", "alias": "m"}],
        }}],
        "keep_all_columns": True,
    }
    result = engine.process_schema(schema, dataframes_in={"dly": prebuilt})
    assert result is prebuilt


def test_temp_view_mode_registers_view():
    """intermediate_mode='temp_view' registers a session view named after the alias."""
    engine = _make_engine_with_fake({"silver.base": [{"id": 1}, {"id": 2}]})
    schema = {
        "partials": [{"alias": "dly", "schema": {
            "tables": [{"name": "silver.base", "alias": "base"}],
        }}],
        "keep_all_columns": True,
    }
    result = engine.process_schema(schema, intermediate_mode="temp_view")
    assert "dly" in engine._backend._temp_views
    # Parent still uses the DataFrame directly.
    assert len(result) == 2


def test_partial_debug_name_is_sandbox_aware():
    """The view/table identifier is alias-only, or alias+suffix when sandboxed."""
    engine = _make_engine_with_fake({"silver.base": [{"id": 1}]})
    interp = engine._interpreter
    assert interp._partial_debug_name("dly") == "dly"
    interp._context.schema_suffix = "u1234"
    assert interp._partial_debug_name("dly") == "dly_u1234"


def test_table_mode_writes_intermediate_table():
    """intermediate_mode='table' writes a physical table and ensures its schema."""
    engine = _make_engine_with_fake({"silver.base": [{"id": 1}, {"id": 2}]})
    schema = {
        "partials": [{"alias": "dly", "schema": {
            "tables": [{"name": "silver.base", "alias": "base"}],
        }}],
        "keep_all_columns": True,
    }
    result = engine.process_schema(schema, intermediate_mode="table")
    written = engine._backend._written
    # A table whose FQN carries the partials schema + alias was written.
    assert any("skifer_partials" in fqn and "dly" in fqn for fqn in written)
    assert any("skifer_partials" in s for s in engine._backend._schemas_created)
    assert len(result) == 2


def test_table_mode_target_is_sandbox_aware():
    """The intermediate schema/table FQN is env-aware (catalog + sandbox suffix)."""
    engine = _make_engine_with_fake()
    interp = engine._interpreter
    interp._context.db = "prod_cat"
    interp._context.schema_suffix = "_u1234"
    schema_fqn, table_fqn = interp._partial_table_target("dly")
    assert schema_fqn == "prod_cat.skifer_partials_u1234"
    assert "skifer_partials_u1234" in table_fqn
    assert "dly" in table_fqn
    assert "prod_cat" in table_fqn


def test_unknown_intermediate_mode_rejected():
    """An unknown intermediate_mode fails fast."""
    engine = _make_engine_with_fake({"silver.base": [{"id": 1}]})
    schema = {
        "partials": [{"alias": "dly", "schema": {
            "tables": [{"name": "silver.base", "alias": "base"}],
        }}],
        "keep_all_columns": True,
    }
    with pytest.raises(ValueError, match="Unknown intermediate_mode"):
        engine.process_schema(schema, intermediate_mode="bogus")
