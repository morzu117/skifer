"""Reconstructed Atlas bridge acceptance, executed on raw in-memory DuckDB."""
from copy import deepcopy
from datetime import date
from decimal import Decimal
from pathlib import Path

import duckdb
import jsonschema
import pytest
import yaml

from skifer.core.ir import parse_to_ir
from skifer.core.schema_loader import parse_schema
from skifer.semantic.json_schema import generate_semantic_json_schema
from skifer.semantic.output_projection import OutputProjector
from skifer.semantic.resolver import SemanticQuery, SemanticQueryError
from skifer.semantic.semantic import SemanticEngine
from skifer.semantic.validator import SemanticValidator

FIXTURE = Path(__file__).parent / "fixtures/atlas"
MODEL_KEYS = ("revenu_net", "entites", "effectifs")


def _payload(key):
    return yaml.safe_load((FIXTURE / "models" / f"{key}.yaml").read_text(encoding="utf-8"))


@pytest.mark.parametrize("key", MODEL_KEYS)
def test_reconstructed_models_validate(key):
    payload = _payload(key)
    jsonschema.Draft7Validator(generate_semantic_json_schema()).validate(payload)
    result = SemanticValidator().validate_yaml(payload)
    assert result.ok, result.errors


def test_kpi_output_matches_view_bridge_and_semantic_columns(atlas_runtime):
    schema = parse_to_ir(parse_schema((FIXTURE / "bridge.yaml").read_text()))
    projection = OutputProjector().project(schema)
    assert projection.is_complete
    # Read the KPI as a document, never through the pipeline parser.
    kpi = yaml.safe_load((FIXTURE / "kpi/revenu-net.yaml").read_text())
    output = next(node for node in kpi["graph"]["nodes"] if node["id"] == kpi["graph"]["output"])
    assert output["type"] == "aggregate"
    kpi_columns = output["group_by"] + [measure[1] for measure in output["measures"]]
    _, backend = atlas_runtime
    view_columns = [row[0] for row in backend.connection.execute("DESCRIBE main.atlas_revenu_net").fetchall()]
    projected_columns = [field.name for field in projection.fields]
    model = _payload("revenu_net")["models"][0]
    semantic_columns = [member["name"] for block in ("dimensions", "metrics") for member in model[block]]
    assert kpi_columns == view_columns == projected_columns == semantic_columns
    result = SemanticValidator().validate_against_projection(
        _payload("revenu_net"), projection,
        contract_output=[field.name for field in schema.contract_output],
    )
    assert result.ok, result.errors
    combined = {"models": [m for key in MODEL_KEYS for m in _payload(key)["models"]]}
    assert SemanticValidator().validate_yaml(combined).ok


class DuckBackend:
    """The same execute_sql seam as test_board_semantic_contract, without a connector package."""
    def __init__(self, connection):
        self.connection = connection
        self.sql = []

    def execute_sql(self, sql):
        self.sql.append(sql)
        return self.connection.execute(sql).fetchall()


@pytest.fixture
def atlas_runtime():
    connection = duckdb.connect(":memory:")
    try:
        connection.execute("""
            CREATE TABLE fact_facturation (
                facture_id VARCHAR, entite VARCHAR, statut VARCHAR, date_facture DATE,
                montant_ttc DECIMAL(12,2), montant_taxes DECIMAL(12,2), avoirs DECIMAL(12,2)
            );
            INSERT INTO fact_facturation VALUES
                ('1', 'A', 'validée', '2025-12-26', 120, 20, 10),
                ('2', 'A', 'validée', '2026-01-20', 60, 10, 0),
                ('3', 'A', 'validée', '2026-01-26', 100, 20, 5),
                ('4', 'B', 'validée', '2026-01-15', 240, 40, 20),
                ('5', 'A', 'annulée', '2026-01-10', 1000, 0, 0),
                ('6', 'A', NULL, '2026-01-10', 200, 0, 0);
            CREATE TABLE periodes_fiscales (
                entite VARCHAR, mois_fiscal DATE, date_debut DATE, date_fin DATE
            );
            INSERT INTO periodes_fiscales VALUES
                ('A', '2026-01-01', '2025-12-26', '2026-01-26'),
                ('A', '2026-02-01', '2026-01-26', '2026-02-26'),
                ('B', '2026-01-01', '2025-12-26', '2026-01-26'),
                ('B', '2026-02-01', '2026-01-26', '2026-02-26');
            CREATE TABLE referentiel_entites (
                entite VARCHAR PRIMARY KEY, segment VARCHAR
            );
            INSERT INTO referentiel_entites VALUES ('A', 'Nord'), ('B', 'Sud');
            CREATE TABLE snapshots_effectifs (
                entite VARCHAR, mois_snapshot DATE, effectif INTEGER,
                PRIMARY KEY (entite, mois_snapshot)
            );
            INSERT INTO snapshots_effectifs VALUES
                ('A', '2026-01-01', 8), ('B', '2026-01-01', 18),
                ('A', '2026-02-01', 10), ('B', '2026-02-01', 20);
        """)
        for path in sorted((FIXTURE / "views").glob("*.sql")):
            connection.execute(path.read_text(encoding="utf-8"))
        backend = DuckBackend(connection)
        yield SemanticEngine.from_backend(
            backend, models_dir=str((FIXTURE / "models").resolve()), dialect="duckdb"
        ), backend
    finally:
        connection.close()


def test_reconstructed_named_views_and_semantic_values(atlas_runtime):
    engine, backend = atlas_runtime
    assert backend.connection.execute(
        "SELECT * FROM main.atlas_revenu_net ORDER BY mois_fiscal, entite"
    ).fetchall() == [
        (date(2026, 1, 1), "A", Decimal("140.00")),
        (date(2026, 1, 1), "B", Decimal("180.00")),
        (date(2026, 2, 1), "A", Decimal("75.00")),
    ]
    assert engine.query(SemanticQuery("revenu_net", metrics=["revenu_net"])) == [(Decimal("395.00"),)]
    joined = engine.query(SemanticQuery(
        "revenu_net", metrics=["revenu_net"], group_by=["entites.segment"]
    ))
    assert sorted(joined) == [("Nord", Decimal("215.00")), ("Sud", Decimal("180.00"))]
    assert "JOIN" in backend.sql[-1].upper()
    assert engine.query(SemanticQuery("entites", metrics=["nombre_entites"])) == [(2,)]


def test_effectifs_two_snapshots_pinned_by_group_by(atlas_runtime):
    engine, _ = atlas_runtime
    snapshots = engine.query(SemanticQuery("effectifs", metrics=["effectif"], group_by=["mois_snapshot"]))
    assert sorted(snapshots) == [(date(2026, 1, 1), 26), (date(2026, 2, 1), 30)]
    pinned = engine.query(SemanticQuery(
        "effectifs", metrics=["effectif"], group_by=["mois_snapshot"],
        date_from="2026-02-01", date_to="2026-02-01",
    ))
    assert pinned == [(date(2026, 2, 1), 30)]


def test_effectifs_iso_equality_filter_compiles_and_runs(atlas_runtime):
    # A lone hyphen in an ISO date is safe: the eq filter now compiles and runs,
    # matching the date_from/date_to bounds path above. Injection vectors (quote,
    # semicolon, -- comment) stay refused — see the semantic domain e2e test.
    engine, backend = atlas_runtime
    result = engine.query(SemanticQuery(
        "effectifs", metrics=["effectif"], group_by=["mois_snapshot"],
        filters=[{"column": "mois_snapshot", "operator": "eq", "value": "2026-02-01"}],
    ))
    assert result == [(date(2026, 2, 1), 30)]
    assert any("'2026-02-01'" in sql for sql in backend.sql)


def test_effectifs_without_pinned_month_is_refused_before_sql(atlas_runtime):
    engine, backend = atlas_runtime
    before = len(backend.sql)
    with pytest.raises(SemanticQueryError, match="semi_additive"):
        engine.query(SemanticQuery("effectifs", metrics=["effectif"]))
    assert len(backend.sql) == before


def test_combined_atlas_validation_rejects_mutated_remote_entity():
    combined = {"models": [m for key in MODEL_KEYS for m in _payload(key)["models"]]}
    assert SemanticValidator().validate_yaml(combined).ok
    changed = deepcopy(combined)
    changed["models"][0]["relationships"][0]["to_entity"] = "missing_entite"
    result = SemanticValidator().validate_yaml(changed)
    assert result.ok is False
    assert any("unknown entity 'missing_entite' in model 'entites'" in error for error in result.errors)


def test_kpi_reference_copy_is_frozen_by_hash():
    import hashlib

    assert hashlib.sha256((FIXTURE / "kpi/revenu-net.yaml").read_bytes()).hexdigest() == 'd34f03649d065bbb36d7df34cf7e093ea316d347c2280623c1693714b921d9fb'
