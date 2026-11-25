"""Atlas's structural contract is necessary, never a replacement for validation."""
import json
from pathlib import Path
import subprocess
import sys

import jsonschema
import pytest
import yaml

from skifer.semantic.json_schema import generate_semantic_json_schema
from skifer.semantic.validator import SemanticValidator

ROOT = Path(__file__).resolve().parents[1]
CORPUS = ROOT / "tests/fixtures/semantic_schema_corpus"
CASES = yaml.safe_load((CORPUS / "corpus.yaml").read_text(encoding="utf-8"))


def test_committed_semantic_schema_does_not_drift():
    generated = generate_semantic_json_schema()
    committed = json.loads((ROOT / "schemas/skifer-semantic-model.schema.json").read_text())
    assert generated == committed
    assert generated["$id"] == "https://skifer.io/schemas/semantic-model/v1"
    jsonschema.Draft7Validator.check_schema(generated)


def test_manifest_matches_directory_and_has_required_coverage():
    listed = [case["file"] for case in CASES]
    assert len(listed) == len(set(listed))
    assert set(listed) == {str(p.relative_to(CORPUS)) for p in CORPUS.rglob("*") if p.is_file() and p.name != "corpus.yaml"}
    assert sum(case["valid"] for case in CASES) >= 10
    assert sum(not case["valid"] for case in CASES) >= 10
    for case in CASES:
        assert isinstance(case["valid"], bool)
        assert isinstance(case["schema_catches"], bool)
        assert case["reason"] and case["provenance"]
        if case["provenance"].startswith("examples/"):
            assert (CORPUS / case["file"]).read_bytes() == (ROOT / case["provenance"]).read_bytes()


@pytest.mark.parametrize("case", CASES, ids=lambda case: case["file"])
def test_semantic_corpus_no_false_rejections_and_exact_negative_labels(case):
    payload = yaml.safe_load((CORPUS / case["file"]).read_text(encoding="utf-8"))
    result = SemanticValidator().validate_yaml(payload)
    errors = list(jsonschema.Draft7Validator(generate_semantic_json_schema()).iter_errors(payload))
    assert result.ok == case["valid"], result.errors
    assert bool(errors) == case["schema_catches"], [e.message for e in errors]
    if result.ok:
        assert not errors
    else:
        assert result.errors  # each negative must actually be rejected by the validator


def test_semantic_schema_import_needs_no_spark_or_agentic():
    result = subprocess.run([sys.executable, "-c", """
import sys
from skifer.semantic.json_schema import generate_semantic_json_schema
generate_semantic_json_schema()
assert not any(n == 'pyspark' or n.startswith('pyspark.') or n.startswith('skifer.agentic') for n in sys.modules)
assert 'jsonschema' not in sys.modules
"""], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
