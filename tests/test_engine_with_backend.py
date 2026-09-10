"""
Tests for SkiferEngine with an injected test-double backend.
The double is injected via ``engine._backend`` (duck typing) — the engine
itself always builds a SparkBackend in ``__init__``.
"""
from tests.fakes.fake_backend import FakeBackend, FakeDataFrame
from skifer.core.core import SkiferEngine


def _make_minimal_engine(backend):
    """Create a SkiferEngine bypassing __init__, with an injected backend."""
    from skifer.core.context import ExecutionContext
    from skifer.core.interpreter import SchemaInterpreter
    engine = object.__new__(SkiferEngine)
    ctx = ExecutionContext()
    object.__setattr__(engine, "_context", ctx)
    engine.spark = getattr(backend, 'spark', None)
    engine.is_local = backend.is_local
    engine.db = None
    engine.env = "local"
    engine.config = {"environments": {"local": {}}}
    engine.schema_suffix = ""
    engine.is_job_execution = False
    from skifer.core.patterns import PipelinePatterns
    engine._backend = backend
    engine._interpreter = SchemaInterpreter(backend=backend, context=ctx)
    engine._patterns = PipelinePatterns(engine=engine)
    return engine


class TestEngineWithBackend:

    def test_engine_uses_provided_fake_backend(self):
        """Engine uses FakeBackend when injected — no Spark calls."""
        tables = {"silver.orders": [{"id": 1, "amount": 50}]}
        backend = FakeBackend(tables=tables)
        engine = _make_minimal_engine(backend)

        schema = {"tables": [{"name": "silver.orders", "alias": "ord"}]}
        result = engine.process_schema(schema)

        assert isinstance(result, FakeDataFrame)
        assert len(result) == 1

    def test_engine_build_fqn_delegates_to_backend(self):
        """_build_fqn delegates to backend.build_fqn."""
        backend = FakeBackend()
        engine = _make_minimal_engine(backend)
        # db is None → 2-part FQN
        fqn = engine._build_fqn("silver", "orders")
        assert fqn == "`silver`.`orders`"

    def test_engine_build_fqn_with_catalog(self):
        """_build_fqn uses catalog when db is set."""
        backend = FakeBackend()
        engine = _make_minimal_engine(backend)
        engine.db = "my_catalog"
        fqn = engine._build_fqn("silver", "orders")
        assert fqn == "`my_catalog`.`silver`.`orders`"

    def test_engine_get_backend_returns_set_backend(self):
        """_get_backend() returns the backend set on the engine (no lazy creation)."""
        engine = object.__new__(SkiferEngine)
        fake = FakeBackend(tables={})
        engine._backend = fake
        assert engine._get_backend() is fake

    def test_engine_get_backend_returns_none_when_not_set(self):
        """_get_backend() returns None when engine has no _backend (e.g. object.__new__)."""
        engine = object.__new__(SkiferEngine)
        assert engine._get_backend() is None

    def test_engine_write_flow_with_fake_backend(self):
        """run_process_to_table writes via FakeBackend."""
        tables = {"silver.orders": [{"id": 1, "amount": 100}]}
        backend = FakeBackend(tables=tables)
        engine = _make_minimal_engine(backend)

        schema = {"tables": [{"name": "silver.orders", "alias": "ord"}]}
        engine.run_process_to_table(schema, "gold", "fact_orders")

        # FakeBackend should record the write
        assert any("fact_orders" in fqn for fqn in backend._written)



