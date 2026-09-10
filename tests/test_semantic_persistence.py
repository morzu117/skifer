"""Tests for the shared semantic persistence helpers (Plan 29)."""
import subprocess
import sys

import yaml

from skifer.semantic.persistence import build_catalog_entry, write_yaml_atomic


def _payload():
    return {
        "models": [
            {
                "key": "orders_summary",
                "layer": "gold",
                "table": "gold.fact_orders",
                "description": "Orders",
                "dimensions": [{"name": "country"}],
                "metrics": [{"name": "orders"}],
            }
        ]
    }


def test_auto_tags_are_deterministic_across_interpreter_runs():
    # list(set(...)) ordering varies with PYTHONHASHSEED, which made
    # semantic_catalog.yaml churn on every --promote.
    script = (
        "import sys; sys.path.insert(0, 'src');"
        "from skifer.semantic.persistence import build_catalog_entry;"
        f"print(build_catalog_entry({_payload()!r})['tags'])"
    )
    runs = {
        subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True,
            text=True,
            env={"PYTHONHASHSEED": str(seed), "PATH": "/usr/bin:/bin"},
            check=True,
        ).stdout.strip()
        for seed in (0, 1, 42, 1234)
    }

    assert len(runs) == 1, f"tag order varies with hash seed: {runs}"


def test_catalog_entry_merges_auto_extra_and_model_tags_without_duplicates():
    entry = build_catalog_entry(_payload(), extra_tags=["revenue", "gold"])

    assert entry["key"] == "orders_summary"
    assert entry["file"] == "orders_summary.yaml"
    assert entry["dimensions"] == ["country"]
    assert entry["metrics"] == ["orders"]
    # auto tags first (sorted), then caller tags, deduplicated
    assert entry["tags"] == ["gold", "orders_summary", "revenue"]


def test_write_yaml_atomic_leaves_no_temp_file_behind(tmp_path):
    target = tmp_path / "nested" / "model.yaml"

    path = write_yaml_atomic(target, _payload())

    assert path == str(target)
    assert yaml.safe_load(target.read_text(encoding="utf-8")) == _payload()
    assert [p.name for p in target.parent.iterdir()] == ["model.yaml"]


def test_write_yaml_atomic_does_not_clobber_on_serialization_failure(tmp_path):
    target = tmp_path / "model.yaml"
    write_yaml_atomic(target, _payload())

    class _Unserializable:
        pass

    try:
        write_yaml_atomic(target, {"models": [{"key": _Unserializable()}]})
    except yaml.YAMLError:
        pass

    # the previous content survives and no temp file is left over
    assert yaml.safe_load(target.read_text(encoding="utf-8")) == _payload()
    assert [p.name for p in tmp_path.iterdir()] == ["model.yaml"]
