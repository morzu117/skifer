"""Tests for Plan 29 semantic certification gates."""

from datetime import datetime, timedelta, timezone

import pytest
import yaml

from skifer.agentic.resolver import SemanticQuery
from skifer.agentic.resolver import ResolvedQuery
from skifer.observability.certification_store import Certification
from skifer.semantic.access_policy import (
    CertificationDecision,
    CertificationOverride,
    ConsumerContext,
    OVERRIDE_SCOPE,
    SemanticAccessDenied,
    evaluate,
)
from skifer.semantic.semantic import SemanticEngine


class FakeBackend:
    def __init__(self):
        self.sql: list[str] = []

    def execute_sql(self, sql: str):
        self.sql.append(sql)
        return {"sql": sql}


class FakeContext:
    def __init__(self, env: str, config: dict):
        self.env = env
        self.config = config

    def env_config(self) -> dict:
        environments = self.config.get("environments", {})
        for name, value in environments.items():
            if name.lower() == self.env.lower():
                return value
        return {}


class FakeCore:
    def __init__(self, env_config: dict, *, env: str = "dev", config_env_name: str | None = None):
        self.db = "main"
        self.env = env
        config_env_name = config_env_name or env
        self.config = {
            "environments": {
                config_env_name: {
                    "semantic_views_schema": "semantic_views",
                    **env_config,
                }
            }
        }
        self.schema_suffix = ""
        self._context = FakeContext(env, self.config)
        self.backend = FakeBackend()

    def _get_backend(self):
        return self.backend


class BombStore:
    def get_certification(self, dataset: str, consumer_class: str = "default"):
        raise AssertionError("get_certification must not be called")


class StaticStore:
    def __init__(
        self,
        status: str,
        *,
        certified_at: datetime | None = None,
        checks_passed: bool = True,
    ):
        self.status = status
        self.certified_at = certified_at or datetime(2026, 1, 1, tzinfo=timezone.utc)
        self.checks_passed = checks_passed
        self.calls: list[tuple[str, str]] = []

    def get_certification(self, dataset: str, consumer_class: str = "default"):
        self.calls.append((dataset, consumer_class))
        return Certification(
            dataset=dataset,
            consumer_class=consumer_class,
            status=self.status,
            contract_version="v1",
            definition_hash="hash1",
            certified_at=self.certified_at,
            checks_passed=self.checks_passed,
        )


class MappingStore:
    def __init__(self, certifications: dict[str, Certification | None]):
        self.certifications = certifications
        self.calls: list[tuple[str, str]] = []

    def get_certification(self, dataset: str, consumer_class: str = "default"):
        self.calls.append((dataset, consumer_class))
        certification = self.certifications.get(dataset)
        if certification is None:
            return None
        return Certification(
            dataset=certification.dataset,
            consumer_class=consumer_class,
            status=certification.status,
            contract_version=certification.contract_version,
            definition_hash=certification.definition_hash,
            certified_at=certification.certified_at,
            checks_passed=certification.checks_passed,
        )


class FlippingStore:
    def __init__(self):
        self.calls = 0

    def get_certification(self, dataset: str, consumer_class: str = "default"):
        self.calls += 1
        status = "CERTIFIED" if self.calls == 1 else "UNCERTIFIED"
        return Certification(
            dataset=dataset,
            consumer_class=consumer_class,
            status=status,
            contract_version="v1",
            definition_hash="hash1",
            certified_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )


class ResolveSpy:
    def __init__(self):
        self.calls = 0

    def __call__(self, original):
        def wrapped(resolver, query, model, catalog_fqn):
            self.calls += 1
            return original(resolver, query, model, catalog_fqn)

        return wrapped


@pytest.fixture
def models_dir(tmp_path):
    root = tmp_path / "semantic_models"
    root.mkdir()
    model = {
        "models": [
            {
                "key": "sales.orders",
                "table": "gold.fact_orders",
                "dimensions": [
                    {"name": "region", "sql": "region", "type": "string"},
                ],
                "metrics": [
                    {"name": "revenue", "sql": "amount", "type": "sum"},
                ],
            }
        ]
    }
    catalog = {
        "models": [
            {
                "key": "sales.orders",
                "file": "sales_orders.yaml",
                "description": "Sales orders",
                "dimensions": ["region"],
                "metrics": ["revenue"],
            }
        ]
    }
    (root / "sales_orders.yaml").write_text(yaml.safe_dump(model), encoding="utf-8")
    (root / "semantic_catalog.yaml").write_text(
        yaml.safe_dump(catalog),
        encoding="utf-8",
    )
    return str(root)


@pytest.fixture
def semantic_query():
    return SemanticQuery(
        model_name="sales.orders",
        metrics=["revenue"],
        group_by=["region"],
    )


@pytest.fixture
def models_dir_without_table(tmp_path):
    root = tmp_path / "semantic_models"
    root.mkdir()
    model = {
        "models": [
            {
                "key": "sales.orders",
                "dimensions": [
                    {"name": "region", "sql": "region", "type": "string"},
                ],
                "metrics": [
                    {"name": "revenue", "sql": "amount", "type": "sum"},
                ],
            }
        ]
    }
    catalog = {
        "models": [
            {
                "key": "sales.orders",
                "file": "sales_orders.yaml",
                "description": "Sales orders",
                "dimensions": ["region"],
                "metrics": ["revenue"],
            }
        ]
    }
    (root / "sales_orders.yaml").write_text(yaml.safe_dump(model), encoding="utf-8")
    (root / "semantic_catalog.yaml").write_text(
        yaml.safe_dump(catalog),
        encoding="utf-8",
    )
    return str(root)


def _engine(
    mode: str,
    models_dir: str,
    store=None,
    *,
    semantic_certification_max_age: str | None = None,
) -> SemanticEngine:
    env_config = {
        "semantic_certification_policy": mode,
        "semantic_consumer_class": "agent_read",
    }
    if semantic_certification_max_age is not None:
        env_config["semantic_certification_max_age"] = semantic_certification_max_age
    core = FakeCore(
        env_config,
    )
    return SemanticEngine(core, models_dir=models_dir, certification_store=store)


def _patch_resolver_for_missing_table(monkeypatch):
    from skifer.agentic import resolver as resolver_module

    def fake_resolve(self, query, model, catalog_fqn):
        return ResolvedQuery(
            select_exprs=["region AS region", "SUM(amount) AS revenue"],
            from_fqn="",
            where_clauses=[],
            group_by_exprs=["region"],
            full_sql=(
                "SELECT region AS region, SUM(amount) AS revenue "
                "FROM placeholder GROUP BY region"
            ),
        )

    monkeypatch.setattr(resolver_module.QueryResolver, "resolve", fake_resolve)


def test_off_mode_query_and_create_view_never_call_certification_store(
    models_dir,
    semantic_query,
    monkeypatch,
):
    from skifer.semantic import semantic as semantic_module

    def fail_resolve_dependencies(model):
        raise AssertionError("resolve_dependencies must not be called")

    monkeypatch.setattr(
        semantic_module,
        "resolve_dependencies",
        fail_resolve_dependencies,
    )
    engine = _engine("off", models_dir, store=BombStore())

    engine.query(semantic_query)
    engine.create_view(semantic_query, view_name="v_orders")

    assert len(engine.core.backend.sql) == 2


def test_enforce_uncertified_denies_before_resolve_and_execute(
    models_dir,
    semantic_query,
    monkeypatch,
):
    from skifer.agentic import resolver as resolver_module

    spy = ResolveSpy()
    monkeypatch.setattr(
        resolver_module.QueryResolver,
        "resolve",
        spy(resolver_module.QueryResolver.resolve),
    )
    engine = _engine("enforce", models_dir, store=StaticStore("UNCERTIFIED"))

    with pytest.raises(SemanticAccessDenied) as exc_info:
        engine.query(semantic_query)

    assert exc_info.value.decision == CertificationDecision.DENY
    assert spy.calls == 0
    assert engine.core.backend.sql == []


def test_enforce_certified_executes_normally(models_dir, semantic_query):
    store = StaticStore("CERTIFIED")
    engine = _engine("enforce", models_dir, store=store)

    result = engine.query(semantic_query)

    assert result["sql"].startswith("SELECT")
    assert len(store.calls) == 2
    assert len(engine.core.backend.sql) == 1


def test_enforce_expired_certification_denies_end_to_end(
    models_dir,
    semantic_query,
):
    store = StaticStore(
        "CERTIFIED",
        certified_at=datetime.now(timezone.utc) - timedelta(hours=2),
    )
    engine = _engine(
        "enforce",
        models_dir,
        store=store,
        semantic_certification_max_age="1h",
    )

    with pytest.raises(SemanticAccessDenied) as exc_info:
        engine.query(semantic_query)

    assert exc_info.value.decision == CertificationDecision.DENY
    assert exc_info.value.reasons == ("EXPIRED",)
    assert engine.core.backend.sql == []


def test_warn_uncertified_continues(models_dir, semantic_query):
    store = StaticStore("UNCERTIFIED")
    engine = _engine("warn", models_dir, store=store)

    engine.query(semantic_query)

    assert len(store.calls) == 2
    assert len(engine.core.backend.sql) == 1
    assert engine.certification_warning_count == 1


def test_env_config_case_insensitive_policy_lookup(models_dir, semantic_query):
    core = FakeCore(
        {
            "semantic_certification_policy": "enforce",
            "semantic_consumer_class": "agent_read",
        },
        env="PROD",
        config_env_name="Prod",
    )
    engine = SemanticEngine(
        core,
        models_dir=models_dir,
        certification_store=StaticStore("UNCERTIFIED"),
    )

    with pytest.raises(SemanticAccessDenied) as exc_info:
        engine.query(semantic_query)

    assert exc_info.value.decision == CertificationDecision.DENY
    assert engine.core.backend.sql == []


def test_supervised_uncertified_requires_human(models_dir, semantic_query):
    engine = _engine("supervised", models_dir, store=StaticStore("UNCERTIFIED"))

    with pytest.raises(SemanticAccessDenied) as exc_info:
        engine.query(semantic_query)

    assert exc_info.value.decision == CertificationDecision.REQUIRE_HUMAN
    assert engine.core.backend.sql == []


def test_enforce_with_no_certification_store_denies_dependency(
    models_dir,
    semantic_query,
):
    engine = _engine("enforce", models_dir, store=None)

    with pytest.raises(SemanticAccessDenied):
        engine.query(semantic_query)

    assert engine.core.backend.sql == []


def test_recheck_denies_before_execute_when_certification_changes(
    models_dir,
    semantic_query,
):
    store = FlippingStore()
    engine = _engine("enforce", models_dir, store=store)

    with pytest.raises(SemanticAccessDenied) as exc_info:
        engine.query(semantic_query)

    assert exc_info.value.decision == CertificationDecision.DENY
    assert store.calls == 2
    assert engine.core.backend.sql == []


def test_create_view_sql_has_definition_comment_without_consumer_pii(
    models_dir,
    semantic_query,
):
    context = ConsumerContext(
        consumer_id="agent-1",
        consumer_class="agent_read",
        user_id="user-secret@example.com",
        trace_id="trace-secret",
    )
    engine = _engine("enforce", models_dir, store=StaticStore("CERTIFIED"))

    view_fqn = engine.create_view(
        semantic_query,
        view_name="v_orders",
        consumer_context=context,
    )

    ddl = engine.core.backend.sql[0]
    assert view_fqn == "`main`.`semantic_views`.`v_orders`"
    assert "-- skifer semantic definition: model=sales.orders" in ddl
    assert "dataset=gold.fact_orders" in ddl
    assert "mode=enforce" in ddl
    assert "user-secret@example.com" not in ddl
    assert "trace-secret" not in ddl


def test_create_view_sql_uses_real_certification_provenance(
    models_dir,
    semantic_query,
):
    store = MappingStore(
        {
            "gold.fact_orders": Certification(
                dataset="gold.fact_orders",
                consumer_class="agent_read",
                status="CERTIFIED",
                contract_version="contract-v2026.09",
                definition_hash="sha256-real-cert-hash",
                certified_at=datetime(2026, 9, 2, tzinfo=timezone.utc),
            )
        }
    )
    engine = _engine("warn", models_dir, store=store)

    engine.create_view(semantic_query, view_name="v_orders")

    ddl = engine.core.backend.sql[0]
    assert "mode=warn" in ddl
    assert "dataset=gold.fact_orders" in ddl
    assert "status=CERTIFIED" in ddl
    assert "contract_version=contract-v2026.09" in ddl
    assert "definition_hash=sha256-real-cert-hash" in ddl
    assert "contract_version=None" not in ddl
    assert "definition_hash=None" not in ddl
    # preflight + recheck only; the recheck's certifications are reused for the
    # comment (bug_003 review follow-up removed the 3rd re-fetch).
    assert len(store.calls) == 2


def test_create_view_with_no_resolved_dependencies_writes_readable_comment(
    models_dir_without_table,
    semantic_query,
    monkeypatch,
):
    _patch_resolver_for_missing_table(monkeypatch)
    engine = _engine("warn", models_dir_without_table, store=BombStore())

    view_fqn = engine.create_view(semantic_query, view_name="v_orders")

    ddl = engine.core.backend.sql[0]
    assert view_fqn == "`main`.`semantic_views`.`v_orders`"
    assert "mode=warn" in ddl
    # The comment now reuses the gate's own certifications tuple (bug_003 fix),
    # which already renders "no resolved deps" as the F10 (None,) sentinel —
    # naming the model key instead of the generic empty-tuple message.
    assert "dependency(dataset=sales.orders, status=unresolved)" in ddl


def test_evaluate_valid_override_allows_otherwise_denied_certification():
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    context = ConsumerContext(
        "operator",
        "agent_read",
        scopes=frozenset({OVERRIDE_SCOPE}),
    )
    override = CertificationOverride(
        reason="break-glass incident",
        actor="operator",
        trace_id="trace-1",
        expires_at=now + timedelta(hours=1),
    )

    evaluation = evaluate(
        [None],
        context,
        "enforce",
        now,
        override=override,
    )

    assert evaluation.decision is CertificationDecision.ALLOW
    assert evaluation.reasons == ("OVERRIDDEN",)


def test_evaluate_valid_override_logs_warning(caplog):
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    context = ConsumerContext(
        "operator",
        "agent_read",
        scopes=frozenset({OVERRIDE_SCOPE}),
    )
    override = CertificationOverride(
        reason="break-glass incident",
        actor="operator",
        trace_id="trace-1",
        expires_at=now + timedelta(hours=1),
    )
    certification = Certification(
        dataset="gold.fact_orders",
        consumer_class="agent_read",
        status="UNCERTIFIED",
        contract_version="v1",
        definition_hash="hash1",
        certified_at=now,
    )

    caplog.set_level("WARNING", logger="skifer.semantic.access_policy")

    evaluation = evaluate(
        [certification],
        context,
        "enforce",
        now,
        override=override,
    )

    assert evaluation.decision is CertificationDecision.ALLOW
    assert any(
        record.levelname == "WARNING"
        and "Certification override accepted" in record.message
        and "actor=operator" in record.message
        and "trace_id=trace-1" in record.message
        and "gold.fact_orders" in record.message
        for record in caplog.records
    )


def test_evaluate_override_requires_consumer_scope(caplog):
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    context = ConsumerContext("operator", "agent_read")
    override = CertificationOverride(
        reason="break-glass incident",
        actor="operator",
        trace_id="trace-1",
        expires_at=now + timedelta(hours=1),
    )

    caplog.set_level("ERROR", logger="skifer.semantic.access_policy")

    with pytest.raises(ValueError, match="certification_override"):
        evaluate(
            [None],
            context,
            "enforce",
            now,
            override=override,
        )

    assert any(
        record.levelname == "ERROR"
        and "Certification override rejected" in record.message
        and "actor=operator" in record.message
        and "trace_id=trace-1" in record.message
        and "certification_override" in record.message
        for record in caplog.records
    )


def test_evaluate_override_actor_must_match_consumer_id():
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    context = ConsumerContext(
        "operator",
        "agent_read",
        scopes=frozenset({OVERRIDE_SCOPE}),
    )
    override = CertificationOverride(
        reason="break-glass incident",
        actor="alice",
        trace_id="trace-1",
        expires_at=now + timedelta(hours=1),
    )

    with pytest.raises(ValueError, match="actor must match"):
        evaluate(
            [None],
            context,
            "enforce",
            now,
            override=override,
        )


def test_evaluate_override_rejects_naive_expires_at():
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    context = ConsumerContext(
        "operator",
        "agent_read",
        scopes=frozenset({OVERRIDE_SCOPE}),
    )
    override = CertificationOverride(
        reason="break-glass incident",
        actor="operator",
        trace_id="trace-1",
        expires_at=datetime(2026, 1, 1, 1),
    )

    with pytest.raises(ValueError, match="timezone-aware"):
        evaluate(
            [None],
            context,
            "enforce",
            now,
            override=override,
        )


@pytest.mark.parametrize(
    ("mode", "decision"),
    [
        ("warn", CertificationDecision.WARN),
        ("enforce", CertificationDecision.DENY),
        ("supervised", CertificationDecision.REQUIRE_HUMAN),
    ],
)
def test_evaluate_failed_check_uses_failed_check_reason(mode, decision):
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    context = ConsumerContext("operator", "agent_read")
    certification = Certification(
        dataset="gold.fact_orders",
        consumer_class="agent_read",
        status="CERTIFIED",
        contract_version="v1",
        definition_hash="hash1",
        certified_at=now,
        checks_passed=False,
    )

    evaluation = evaluate([certification], context, mode, now)

    assert evaluation.decision is decision
    assert evaluation.reasons == ("FAILED_CHECK",)


def test_evaluate_certification_expiration_respects_max_age():
    now = datetime(2026, 1, 1, 12, tzinfo=timezone.utc)
    context = ConsumerContext("operator", "agent_read")
    expired = Certification(
        dataset="gold.fact_orders",
        consumer_class="agent_read",
        status="CERTIFIED",
        contract_version="v1",
        definition_hash="hash1",
        certified_at=now - timedelta(hours=2),
    )
    recent = Certification(
        dataset="gold.fact_orders",
        consumer_class="agent_read",
        status="CERTIFIED",
        contract_version="v1",
        definition_hash="hash1",
        certified_at=now - timedelta(minutes=30),
    )

    expired_evaluation = evaluate(
        [expired],
        context,
        "enforce",
        now,
        max_age="1h",
    )
    recent_evaluation = evaluate(
        [recent],
        context,
        "enforce",
        now,
        max_age="1h",
    )

    assert expired_evaluation.decision is CertificationDecision.DENY
    assert expired_evaluation.reasons == ("EXPIRED",)
    assert recent_evaluation.decision is CertificationDecision.ALLOW
    assert recent_evaluation.reasons == ()


def test_evaluate_without_max_age_ignores_old_certified_at():
    now = datetime(2026, 1, 1, 12, tzinfo=timezone.utc)
    context = ConsumerContext("operator", "agent_read")
    certification = Certification(
        dataset="gold.fact_orders",
        consumer_class="agent_read",
        status="CERTIFIED",
        contract_version="v1",
        definition_hash="hash1",
        certified_at=now - timedelta(days=365),
    )

    evaluation = evaluate([certification], context, "enforce", now)

    assert evaluation.decision is CertificationDecision.ALLOW
    assert evaluation.reasons == ()


@pytest.mark.parametrize(
    "override",
    [
        CertificationOverride(
            reason="incident",
            actor="operator",
            trace_id="trace-1",
            expires_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        ),
        CertificationOverride(
            reason=" ",
            actor="operator",
            trace_id="trace-1",
            expires_at=datetime(2026, 1, 1, 1, tzinfo=timezone.utc),
        ),
        CertificationOverride(
            reason="incident",
            actor="operator",
            trace_id="trace-1",
            expires_at=datetime(2026, 1, 1, 5, tzinfo=timezone.utc),
        ),
    ],
)
def test_evaluate_invalid_override_raises(override):
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)

    with pytest.raises(ValueError):
        evaluate(
            [None],
            ConsumerContext(
                "operator",
                "agent_read",
                scopes=frozenset({OVERRIDE_SCOPE}),
            ),
            "enforce",
            now,
            override=override,
        )


def test_evaluate_override_requires_actor_and_trace_id():
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)

    with pytest.raises(ValueError, match="actor"):
        evaluate(
            [None],
            ConsumerContext(
                "operator",
                "agent_read",
                scopes=frozenset({OVERRIDE_SCOPE}),
            ),
            "enforce",
            now,
            override=CertificationOverride(
                reason="incident",
                actor=" ",
                trace_id="trace-1",
                expires_at=now + timedelta(hours=1),
            ),
        )

    with pytest.raises(ValueError, match="trace_id"):
        evaluate(
            [None],
            ConsumerContext(
                "operator",
                "agent_read",
                scopes=frozenset({OVERRIDE_SCOPE}),
            ),
            "enforce",
            now,
            override=CertificationOverride(
                reason="incident",
                actor="operator",
                trace_id=" ",
                expires_at=now + timedelta(hours=1),
            ),
        )


def test_query_valid_override_short_circuits_deny_and_executes(
    models_dir,
    semantic_query,
    caplog,
):
    now = datetime.now(timezone.utc)
    context = ConsumerContext(
        "alice",
        "agent_read",
        scopes=frozenset({OVERRIDE_SCOPE}),
    )
    override = CertificationOverride(
        reason="operator incident",
        actor="alice",
        trace_id="trace-1",
        expires_at=now + timedelta(hours=1),
    )
    engine = _engine("enforce", models_dir, store=None)

    caplog.set_level("WARNING", logger="skifer.semantic.access_policy")

    result = engine.query(
        semantic_query,
        consumer_context=context,
        override=override,
    )

    assert result["sql"].startswith("SELECT")
    assert len(engine.core.backend.sql) == 1
    # Preflight logs the override audit trail; the recheck (count_warning=False)
    # must not duplicate it — this used to double the sole audit record
    # (merged_bug_001).
    audit_records = [
        record for record in caplog.records
        if "Certification override accepted" in record.message
    ]
    assert len(audit_records) == 1


def test_certification_warning_count_only_tracks_warn_decisions(
    models_dir,
    semantic_query,
):
    warn_engine = _engine("warn", models_dir, store=StaticStore("UNCERTIFIED"))
    warn_engine.query(semantic_query)
    warn_engine.query(semantic_query)
    assert warn_engine.certification_warning_count == 2

    off_engine = _engine("off", models_dir, store=BombStore())
    off_engine.query(semantic_query)
    assert off_engine.certification_warning_count == 0

    enforce_engine = _engine("enforce", models_dir, store=StaticStore("UNCERTIFIED"))
    with pytest.raises(SemanticAccessDenied):
        enforce_engine.query(semantic_query)
    assert enforce_engine.certification_warning_count == 0

    supervised_engine = _engine(
        "supervised",
        models_dir,
        store=StaticStore("UNCERTIFIED"),
    )
    with pytest.raises(SemanticAccessDenied):
        supervised_engine.query(semantic_query)
    assert supervised_engine.certification_warning_count == 0


def test_enforce_no_resolved_dependencies_denies_before_execute(
    models_dir_without_table,
    semantic_query,
):
    engine = _engine("enforce", models_dir_without_table, store=BombStore())

    with pytest.raises(SemanticAccessDenied) as exc_info:
        engine.query(semantic_query)

    assert exc_info.value.decision == CertificationDecision.DENY
    assert exc_info.value.reasons == ("MISSING",)
    assert exc_info.value.datasets == ("sales.orders",)
    assert engine.core.backend.sql == []


def test_warn_no_resolved_dependencies_logs_and_counts_warning(
    models_dir_without_table,
    semantic_query,
    monkeypatch,
    capsys,
):
    _patch_resolver_for_missing_table(monkeypatch)
    engine = _engine("warn", models_dir_without_table, store=BombStore())

    result = engine.query(semantic_query)

    captured = capsys.readouterr()
    assert result["sql"].startswith("SELECT")
    assert engine.certification_warning_count == 1
    # Preflight prints the banner; the recheck (count_warning=False) must stay
    # silent — this used to fire twice per logical query() call (merged_bug_001).
    assert captured.out.count(
        "Certification policy warning for model 'sales.orders': MISSING"
    ) == 1
    assert len(engine.core.backend.sql) == 1


def test_supervised_no_resolved_dependencies_requires_human(
    models_dir_without_table,
    semantic_query,
):
    engine = _engine("supervised", models_dir_without_table, store=BombStore())

    with pytest.raises(SemanticAccessDenied) as exc_info:
        engine.query(semantic_query)

    assert exc_info.value.decision == CertificationDecision.REQUIRE_HUMAN
    assert exc_info.value.reasons == ("MISSING",)
    assert exc_info.value.datasets == ("sales.orders",)
    assert engine.core.backend.sql == []


def test_off_mode_with_no_resolved_dependencies_still_executes_without_store_calls(
    models_dir_without_table,
    semantic_query,
    monkeypatch,
):
    _patch_resolver_for_missing_table(monkeypatch)
    engine = _engine("off", models_dir_without_table, store=BombStore())

    result = engine.query(semantic_query)

    assert result["sql"].startswith("SELECT")
    assert len(engine.core.backend.sql) == 1
    assert engine.certification_warning_count == 0


def test_valid_override_allows_no_resolved_dependencies_in_enforce_mode(
    models_dir_without_table,
    semantic_query,
    monkeypatch,
):
    _patch_resolver_for_missing_table(monkeypatch)
    now = datetime.now(timezone.utc)
    context = ConsumerContext(
        "alice",
        "agent_read",
        scopes=frozenset({OVERRIDE_SCOPE}),
    )
    override = CertificationOverride(
        reason="operator incident",
        actor="alice",
        trace_id="trace-1",
        expires_at=now + timedelta(hours=1),
    )
    engine = _engine("enforce", models_dir_without_table, store=BombStore())

    result = engine.query(
        semantic_query,
        consumer_context=context,
        override=override,
    )

    assert result["sql"].startswith("SELECT")
    assert len(engine.core.backend.sql) == 1
