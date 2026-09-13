"""
SemanticEngine — interface entre les modèles YAML et l'exécution Spark/SQL.

Architecture catalog-first avec chargement lazy :
  - Au démarrage : seul semantic_catalog.yaml est chargé (O(1)).
  - Les YAML complets sont chargés à la demande (cache en mémoire).
  - Le LLM reçoit uniquement le résumé du catalogue (~500 tokens).
  - Spark n'est touché qu'à l'exécution (query / create_view).
"""

from __future__ import annotations

import datetime
from dataclasses import replace
import os
from pathlib import Path
import time
from typing import Any
from uuid import uuid4

import yaml

from ..observability.tracing import (
    TRACE_FORMAT_VERSION,
    NoOpTracer,
    TraceContext,
    configured_span_scope,
    current_trace_context,
    is_valid_trace_id,
    set_span_attribute,
    trace_context_scope,
)

from .access_policy import (
    CertificationDecision,
    CertificationOverride,
    ConsumerContext,
    SemanticAccessDenied,
    evaluate,
)
from .calendar import CalendarDef, parse_calendar
from .dependencies import SemanticDependency, resolve_dependencies
from .evidence import (
    EvidencePolicy,
    MissingCertificationSnapshot,
    SemanticExecutionError,
    SemanticEvidence,
    SemanticResult,
    SourceEvidence,
    build_metric_evidence,
    hash_sql,
)


class SemanticEngine:
    """
    Interface entre les modèles sémantiques YAML et l'exécution Spark.

    Chargement :
        engine = SemanticEngine(core_engine)
        # → charge semantic_catalog.yaml uniquement (léger)

    Navigation (sans Spark) :
        engine.list_models()
        engine.list_models(tags=["gold"], summary=True)
        engine.get_model_summary("kpi_orders.erp")

    Exécution :
        from skifer.agentic.resolver import SemanticQuery
        sq = SemanticQuery(model_name="kpi_orders.erp", metrics=["gross_revenue"],
                           group_by=["region"])
        df = engine.query(sq)                    # → DataFrame
        fqn = engine.create_view(sq, "v_ca_emea")  # → FQN vue
    """

    def __init__(
        self,
        core_engine,
        models_dir: str = "semantic_models",
        certification_store=None,
        *,
        utc_now=None,
        monotonic=None,
    ):
        """
        Args:
            core_engine: Instance SkiferEngine (session Spark + config).
            models_dir:  Chemin vers le répertoire semantic_models/.
                         Peut être absolu ou relatif à la racine du projet.
            certification_store: Store optionnel de certification sémantique.
        """
        self.core = core_engine
        # Note: self.spark supprimé — toutes les requêtes SQL passent par self.core._get_backend().execute_sql()
        self.models_dir = self._resolve_models_dir(models_dir)
        self.certification_store = certification_store
        self._utc_now = utc_now or (lambda: datetime.datetime.now(datetime.timezone.utc))
        self._monotonic = monotonic or time.monotonic
        self.certification_warning_count = 0
        self._catalog: dict[str, dict] = self._load_catalog()
        self._cache: dict[str, dict] = {}  # cache des YAML complets chargés
        self._calendar_cache: dict[str, CalendarDef] = {}

        print(
            f"🧠 [SemanticEngine] Catalogue chargé — "
            f"{len(self._catalog)} modèles indexés."
        )

    @property
    def tracer(self):
        """Share the core tracer without adding a constructor dependency."""
        return getattr(self.core, "tracer", None) or getattr(
            self.core, "_tracer", NoOpTracer()
        )

    @property
    def _tracing_required(self) -> bool:
        config = getattr(self.core, "_tracing_config", None)
        return bool(getattr(config, "required", False))

    def _trace_span(self, name: str, attributes: dict | None = None):
        return configured_span_scope(
            self.tracer,
            name,
            attributes=attributes,
            required=self._tracing_required,
        )

    # ------------------------------------------------------------------
    # Résolution du répertoire
    # ------------------------------------------------------------------

    def _resolve_models_dir(self, models_dir: str) -> str:
        if os.path.isabs(models_dir):
            return models_dir

        # src/skifer/semantic/semantic.py → parents[3] = project root
        project_root = Path(__file__).parents[3]
        return str(project_root / models_dir)

    # ------------------------------------------------------------------
    # Catalogue
    # ------------------------------------------------------------------

    def _load_catalog(self) -> dict[str, dict]:
        """
        Charge semantic_catalog.yaml.
        Retourne un dict {model_key: catalog_entry}.
        """
        catalog_path = os.path.join(self.models_dir, "semantic_catalog.yaml")
        if not os.path.exists(catalog_path):
            return {}

        with open(catalog_path, encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}

        models = raw.get("models", [])
        return {m["key"]: m for m in models}

    def reload_catalog(self) -> None:
        """Recharge le catalogue depuis le disque (après un SemanticBuilder.build())."""
        self._catalog = self._load_catalog()
        self._cache.clear()
        self._calendar_cache.clear()
        print(
            f"🔄 [SemanticEngine] Catalogue rechargé — "
            f"{len(self._catalog)} modèles."
        )

    # ------------------------------------------------------------------
    # Lazy loading
    # ------------------------------------------------------------------

    def _get_model(self, model_key: str) -> dict:
        """
        Retourne le modèle complet (YAML chargé).
        Charge le fichier uniquement si pas déjà en cache.

        Raises:
            ValueError: Si la clé n'existe pas dans le catalogue.
        """
        if model_key not in self._cache:
            entry = self._catalog.get(model_key)
            if entry is None:
                available = self.list_models(summary=True)
                raise ValueError(
                    f"❌ Modèle '{model_key}' introuvable dans le catalogue. "
                    f"Disponible : {[m['key'] for m in available]}"
                )
            file_path = os.path.join(self.models_dir, entry["file"])
            with open(file_path, encoding="utf-8") as f:
                content = yaml.safe_load(f)
            # Un fichier YAML peut contenir plusieurs modèles : on prend le premier
            models_in_file = content.get("models", [])
            if not models_in_file:
                raise ValueError(
                    f"❌ Le fichier '{entry['file']}' ne contient aucun modèle."
                )
            self._cache[model_key] = models_in_file[0]

        return self._cache[model_key]

    def _get_calendar(self, calendar_key: str) -> CalendarDef:
        """Load ``calendars/<key>.yaml`` only when a period query needs it."""
        if calendar_key not in self._calendar_cache:
            calendar_path = os.path.join(
                self.models_dir, "calendars", f"{calendar_key}.yaml"
            )
            try:
                with open(calendar_path, encoding="utf-8") as handle:
                    payload = yaml.safe_load(handle)
            except (OSError, yaml.YAMLError) as exc:
                raise ValueError(
                    f"Calendar '{calendar_key}' could not be loaded from "
                    f"'{calendar_path}'."
                ) from exc
            try:
                calendar = parse_calendar(payload)
            except ValueError as exc:
                raise ValueError(
                    f"Calendar '{calendar_key}' is invalid: {exc}"
                ) from exc
            if calendar.key != calendar_key:
                raise ValueError(
                    f"Calendar file for '{calendar_key}' declares key "
                    f"'{calendar.key}'."
                )
            self._calendar_cache[calendar_key] = calendar
        return self._calendar_cache[calendar_key]

    # ------------------------------------------------------------------
    # Navigation (catalogue uniquement — pas de lecture YAML)
    # ------------------------------------------------------------------

    def list_models(
        self,
        tags: list[str] | None = None,
        layer: str | None = None,
        summary: bool = False,
    ) -> list[dict]:
        """
        Liste les modèles depuis le catalogue uniquement.

        Args:
            tags:    Filtre sur les tags (ex: ["orders", "gold"]).
                     Logique OR : retourne si le modèle a AU MOINS un des tags.
            layer:   Filtre sur le layer (ex: "gold").
            summary: Si True, retourne uniquement key + description
                     (format compact pour injection dans les prompts LLM).

        Returns:
            Liste de dicts catalogue (ou résumés si summary=True).
        """
        results = list(self._catalog.values())

        if tags:
            results = [
                m for m in results
                if any(t in m.get("tags", []) for t in tags)
            ]
        if layer:
            results = [m for m in results if m.get("layer") == layer]

        if summary:
            return [
                {
                    "key": m["key"],
                    "description": m.get("description", ""),
                    "dimensions": m.get("dimensions", []),
                    "metrics": m.get("metrics", []),
                    "entities": m.get("entities", []),
                    "related_models": m.get("related_models", []),
                }
                for m in results
            ]
        return results

    def get_model_summary(self, model_key: str) -> dict:
        """Retourne l'entrée catalogue d'un modèle (sans charger le YAML complet)."""
        entry = self._catalog.get(model_key)
        if entry is None:
            raise ValueError(f"❌ Modèle '{model_key}' introuvable dans le catalogue.")
        return entry

    # ------------------------------------------------------------------
    # Exécution — query (DataFrame)
    # ------------------------------------------------------------------

    def query(
        self,
        semantic_query: "SemanticQuery",  # noqa: F821
        consumer_context: ConsumerContext | None = None,
        override: CertificationOverride | None = None,
    ) -> Any:
        """
        Exécute une SemanticQuery et retourne un DataFrame PySpark.

        Args:
            semantic_query: SemanticQuery produite par le GenBIAgent (via QueryResolver).

        Returns:
            DataFrame PySpark résultant de la requête sémantique.
        """
        return self.query_with_evidence(
            semantic_query,
            consumer_context=consumer_context,
            override=override,
        ).dataframe

    def query_with_evidence(
        self,
        semantic_query: "SemanticQuery",  # noqa: F821
        consumer_context: ConsumerContext | None = None,
        override: CertificationOverride | None = None,
        *,
        evidence_policy: EvidencePolicy | None = None,
        evidence_id: str | None = None,
        compiled_at: datetime.datetime | None = None,
    ) -> SemanticResult:
        """Execute a semantic query and return its DataFrame with safe evidence.

        The returned evidence snapshots the final certification recheck and
        records safe compilation and execution metadata without retaining a
        live backend or DataFrame reference.
        """
        # Precedence is deliberate: an already-active root owns correlation.
        # Otherwise an explicit ConsumerContext.trace_id seeds the new root.
        # With a NoOpTracer there is no active root, so evidence retains the
        # historical explicit ConsumerContext.trace_id behaviour.
        active = current_trace_context(self.tracer)
        requested_trace_id = (
            consumer_context.trace_id if consumer_context is not None else None
        )
        seed_trace_id = (
            requested_trace_id
            if requested_trace_id is not None and is_valid_trace_id(requested_trace_id)
            else None
        )
        root_context = TraceContext(trace_id=active.trace_id or seed_trace_id)
        with trace_context_scope(
            self.tracer, root_context, required=self._tracing_required
        ):
            with self._trace_span(
                "skifer.semantic.query",
                {"skifer.trace_version": TRACE_FORMAT_VERSION},
            ):
                return self._query_with_evidence(
                    semantic_query,
                    consumer_context=consumer_context,
                    override=override,
                    evidence_policy=evidence_policy,
                    evidence_id=evidence_id,
                    compiled_at=compiled_at,
                )

    def _query_with_evidence(
        self,
        semantic_query: "SemanticQuery",  # noqa: F821
        consumer_context: ConsumerContext | None = None,
        override: CertificationOverride | None = None,
        *,
        evidence_policy: EvidencePolicy | None = None,
        evidence_id: str | None = None,
        compiled_at: datetime.datetime | None = None,
    ) -> SemanticResult:
        """Business flow for :meth:`query_with_evidence`, shared by all tracers."""
        from ..agentic.resolver import QueryResolver
        from .planner import SemanticPlanner

        model = self._get_model(semantic_query.model_name)
        with self._trace_span(
            "skifer.semantic.policy", {"model_key": semantic_query.model_name}
        ) as policy_span:
            mode, context, deps, _certifications, preflight_policy = (
                self._enforce_certification_gate_with_evaluation(
                    semantic_query.model_name,
                    model,
                    consumer_context,
                    override=override,
                )
            )
            set_span_attribute(
                policy_span,
                "decision",
                preflight_policy.decision.value,
                required=self._tracing_required,
            )
        resolver = QueryResolver(SemanticPlanner(self))
        with self._trace_span(
            "skifer.semantic.compile", {"model_key": semantic_query.model_name}
        ):
            resolved = resolver.resolve(semantic_query, model, self.core.db)
        return self._query_with_evidence_from_resolved(
            semantic_query,
            model,
            resolved,
            context=context,
            mode=mode,
            deps=deps,
            override=override,
            evidence_policy=evidence_policy,
            evidence_id=evidence_id,
            compiled_at=compiled_at,
        )

    def _query_with_evidence_from_resolved(
        self,
        semantic_query: "SemanticQuery",  # noqa: F821
        model: dict,
        resolved: Any,
        *,
        context: ConsumerContext | None,
        mode: str,
        deps: tuple,
        override: CertificationOverride | None = None,
        evidence_policy: EvidencePolicy | None = None,
        evidence_id: str | None = None,
        compiled_at: datetime.datetime | None = None,
    ) -> SemanticResult:
        """Execute an already gated and compiled query with evidence.

        This private seam lets ``GenBIAgent`` retain its preflight and
        clarification flow while sharing the single evidence execution path.
        It always performs the final dependency-aware certification recheck.
        """
        # The preflight can only see the root model — the join path does not
        # exist yet. The recheck runs against every dataset the compiled plan
        # actually reads: a multi-model query otherwise obtains an ALLOW that
        # was decided on the root table alone, and reads the joined datasets
        # without any of them ever being certified.
        with self._trace_span(
            "skifer.semantic.policy", {"model_key": semantic_query.model_name}
        ) as policy_span:
            _mode, _context, deps, certifications, policy = (
                self._enforce_certification_gate_with_evaluation(
                    semantic_query.model_name,
                    model,
                    context,
                    mode=mode,
                    # In `off` mode the gate short-circuits before looking at
                    # dependencies at all, so resolving them would be work the
                    # mode exists to avoid.
                    deps=(
                        deps
                        if mode == "off"
                        else self._plan_dependencies(resolved, model)
                    ),
                    count_warning=False,
                    override=override,
                )
            )
            set_span_attribute(
                policy_span,
                "decision",
                policy.decision.value,
                required=self._tracing_required,
            )

        compiled_at = compiled_at or self._utc_now()
        evidence_policy = evidence_policy or EvidencePolicy.redacted()
        evidence = SemanticEvidence(
            evidence_id=evidence_id or str(uuid4()),
            trace_id=(
                current_trace_context(self.tracer).trace_id
                or (context.trace_id if context is not None else None)
            ),
            model_keys=tuple(resolved.model_keys) or (semantic_query.model_name,),
            metrics=build_metric_evidence(
                resolved.selected_expressions,
                self._lineage_columns(resolved),
            ),
            dimensions=tuple(semantic_query.group_by),
            normalized_filters=self._normalized_filters(
                semantic_query, evidence_policy
            ),
            sources=self._source_evidence(
                resolved, deps, certifications, policy=policy, mode=mode
            ),
            policy=policy,
            sql_hash=hash_sql(resolved.full_sql),
            sql_text=resolved.full_sql if evidence_policy.include_sql else None,
            sql_redacted=not evidence_policy.include_sql,
            statement_id=None,
            compiled_at=compiled_at,
            executed_at=None,
        )
        print(f"🔎 [SemanticEngine] SQL :\n{resolved.full_sql}")
        backend = self.core._get_backend()
        started = self._monotonic()
        try:
            execute_with_metadata = getattr(backend, "execute_sql_with_metadata", None)
            compute_type = "statement_execution" if execute_with_metadata else "spark"
            with self._trace_span(
                "skifer.sql.execute",
                {"sql_hash": evidence.sql_hash, "compute_type": compute_type},
            ) as sql_span:
                if execute_with_metadata is None:
                    dataframe = backend.execute_sql(resolved.full_sql)
                    statement_id = None
                else:
                    execution_result = execute_with_metadata(resolved.full_sql)
                    dataframe = execution_result.dataframe
                    statement_id = execution_result.statement_id
                set_span_attribute(
                    sql_span,
                    "statement_id",
                    statement_id,
                    required=self._tracing_required,
                )
        except Exception as exc:
            failed_evidence = replace(
                evidence,
                executed_at=self._utc_now(),
                execution_status="failed",
                execution_duration_seconds=max(0.0, self._monotonic() - started),
                execution_error_type=type(exc).__name__,
            )
            raise SemanticExecutionError(str(exc), evidence=failed_evidence) from exc

        completed_evidence = replace(
            evidence,
            statement_id=statement_id,
            executed_at=self._utc_now(),
            execution_status="succeeded",
            execution_duration_seconds=max(0.0, self._monotonic() - started),
        )
        return SemanticResult(dataframe=dataframe, evidence=completed_evidence)


    # ------------------------------------------------------------------
    # Compilation evidence (Plan 29, slice 4.2)
    # ------------------------------------------------------------------

    def _lineage_columns(self, resolved) -> dict:
        """Resolve source columns for the selected metrics only.

        Models already sit in the lazy cache at this point: the compilation just
        read them. Nothing extra is loaded here.
        """
        from ..lineage.tracker import LineageTracker

        selected_by_model: dict[str, set[str]] = {}
        for expression in resolved.selected_expressions:
            selected_by_model.setdefault(expression.model_key, set()).add(expression.name)

        columns: dict[tuple[str, str], set[str]] = {}
        for model_key, names in selected_by_model.items():
            model = self._cache.get(model_key)
            if model is None:
                # Lineage stays unresolved rather than triggering a load, and
                # `build_metric_evidence` records that instead of reporting the
                # metric as having no sources at all.
                continue
            subgraph = LineageTracker.selected_subgraph(model, names)
            for edge in subgraph.edges:
                columns.setdefault((model_key, edge.target_column), set()).add(
                    edge.source_column
                )
        return columns


    def _plan_dependencies(self, resolved, root_model: dict) -> tuple[SemanticDependency, ...]:
        """Every dataset the compiled plan reads, in a stable order.

        ``resolve_dependencies`` handles one model at a time; a multi-model plan
        needs each joined model's table too, or the gate rules on a strict
        subset of what the query touches.
        """
        dependencies: list[SemanticDependency] = []
        seen: set[str] = set()
        for model_key in resolved.model_keys or ():
            model = self._cache.get(model_key) or (
                root_model if model_key == root_model.get("key") else None
            )
            if model is None:
                continue
            for dependency in resolve_dependencies(model):
                if dependency.dataset not in seen:
                    seen.add(dependency.dataset)
                    dependencies.append(dependency)
        if not dependencies:
            return resolve_dependencies(root_model)
        return tuple(dependencies)

    @staticmethod
    def _source_evidence(
        resolved,
        dependencies,
        certifications,
        *,
        policy,
        mode: str,
    ) -> tuple[SourceEvidence, ...]:
        # Keyed by the certification's own dataset rather than zipped against
        # `dependencies`: a positional zip silently truncates to the shorter of
        # the two, dropping the tail without a word.
        certification_by_dataset = {
            certification.dataset: certification
            for certification in certifications
            if getattr(certification, "dataset", None)
        }
        overridden = "OVERRIDDEN" in policy.reasons
        if (
            mode != "off"
            and policy.decision == CertificationDecision.ALLOW
            and not overridden
        ):
            missing = tuple(
                dataset
                for dataset in resolved.sources
                if certification_by_dataset.get(dataset) is None
            )
            if missing:
                raise MissingCertificationSnapshot(
                    "Certification snapshot missing under ALLOW for: "
                    + ", ".join(missing)
                    + ". The gate allowed the query without evaluating these "
                    "datasets, so the decision does not cover what was read."
                )

        return tuple(
            SourceEvidence(
                dataset=dataset,
                contract_id=None,
                contract_version=getattr(certification, "contract_version", None),
                definition_hash=getattr(certification, "definition_hash", None),
                certification_status=(
                    "OVERRIDDEN"
                    if overridden and certification is None
                    else getattr(certification, "status", "MISSING")
                    if mode != "off"
                    else "NOT_EVALUATED"
                ),
                certified_at=getattr(certification, "certified_at", None),
                load_age_seconds=None,
                data_age_seconds=None,
                certification_run_id=None,
            )
            for dataset in resolved.sources
            for certification in (certification_by_dataset.get(dataset),)
        )

    @staticmethod
    def _normalized_filters(semantic_query, evidence_policy) -> tuple[dict, ...]:
        normalized = []
        for item in semantic_query.filters:
            entry = {"column": item.get("column"), "operator": item.get("operator")}
            if "value" in item:
                entry["value"] = (
                    item["value"]
                    if (
                        evidence_policy.include_filter_values
                        and item.get("column") not in evidence_policy.sensitive_columns
                    )
                    else "<redacted>"
                )
            normalized.append(entry)
        return tuple(normalized)

    # ------------------------------------------------------------------
    # Exécution — create_view
    # ------------------------------------------------------------------

    def create_view(
        self,
        semantic_query: "SemanticQuery",  # noqa: F821
        view_name: str | None = None,
        consumer_context: ConsumerContext | None = None,
        override: CertificationOverride | None = None,
    ) -> str:
        """
        Crée une vue SQL dans Databricks depuis une SemanticQuery.

        Le schéma cible est résolu depuis semantic_views_schema dans config.yaml
        (avec suffix sandbox si applicable).

        Args:
            semantic_query: SemanticQuery avec mode="view".
            view_name:      Nom court de la vue (prioritaire sur semantic_query.view_name).

        Returns:
            FQN complet de la vue créée (ex: "`catalog`.`semantic_views_dev`.`v_ca_region`").
        """
        from ..agentic.resolver import QueryResolver
        from .planner import SemanticPlanner

        name = view_name or semantic_query.view_name
        if not name:
            raise ValueError(
                "❌ create_view() requiert un nom de vue (view_name ou "
                "semantic_query.view_name)."
            )

        model = self._get_model(semantic_query.model_name)
        mode, context, deps, _certifications = self.enforce_certification_gate(
            semantic_query.model_name,
            model,
            consumer_context,
            override=override,
        )
        catalog_fqn = self.core.db

        resolver = QueryResolver(SemanticPlanner(self))
        resolved = resolver.resolve(semantic_query, model, catalog_fqn)
        mode, context, deps, certifications = self.enforce_certification_gate(
            semantic_query.model_name,
            model,
            context,
            mode=mode,
            deps=deps,
            count_warning=False,
            override=override,
        )

        target_schema = self._resolve_view_schema()
        view_fqn = f"`{self.core.db}`.`{target_schema}`.`{name}`"

        ddl = f"CREATE OR REPLACE VIEW {view_fqn} AS\n{resolved.full_sql}"
        if mode != "off":
            definition_comment = self._semantic_definition_comment(
                semantic_query.model_name,
                mode,
                certifications,
            )
            ddl = (
                f"CREATE OR REPLACE VIEW {view_fqn} AS\n"
                f"{definition_comment}\n"
                f"{resolved.full_sql}"
            )
        print(f"🏗️  [SemanticEngine] Création de la vue : {view_fqn}")
        self.core._get_backend().execute_sql(ddl)
        print(f"✅ [SemanticEngine] Vue créée : {view_fqn}")
        return view_fqn

    def enforce_certification_gate(
        self,
        model_key: str,
        model: dict,
        consumer_context: ConsumerContext | None,
        *,
        mode: str | None = None,
        deps: tuple[SemanticDependency, ...] | None = None,
        count_warning: bool = True,
        override: CertificationOverride | None = None,
    ) -> tuple[str, ConsumerContext | None, tuple[SemanticDependency, ...], tuple[Any, ...]]:
        """Apply the public certification gate without exposing evaluation details."""
        mode, context, dependencies, certifications, _evaluation = (
            self._enforce_certification_gate_with_evaluation(
                model_key,
                model,
                consumer_context,
                mode=mode,
                deps=deps,
                count_warning=count_warning,
                override=override,
            )
        )
        return mode, context, dependencies, certifications

    def _enforce_certification_gate_with_evaluation(
        self,
        model_key: str,
        model: dict,
        consumer_context: ConsumerContext | None,
        *,
        mode: str | None = None,
        deps: tuple[SemanticDependency, ...] | None = None,
        count_warning: bool = True,
        override: CertificationOverride | None = None,
    ) -> tuple[
        str,
        ConsumerContext | None,
        tuple[SemanticDependency, ...],
        tuple[Any, ...],
        Any,
    ]:
        mode = mode or self._semantic_certification_mode()
        if mode == "off":
            policy = evaluate(
                (),
                consumer_context or self._default_consumer_context(),
                mode,
                now=self._utc_now(),
            )
            return mode, consumer_context, (), (), policy

        context = consumer_context or self._default_consumer_context()
        deps = resolve_dependencies(model) if deps is None else deps
        certifications = (
            (None,)
            if not deps
            else tuple(
                self._get_certification(dep.dataset, context.consumer_class)
                for dep in deps
            )
        )
        max_age = self._semantic_certification_max_age()
        evaluation = evaluate(
            certifications,
            context,
            mode,
            now=self._utc_now(),
            override=override,
            max_age=max_age,
            count_warning=count_warning,
        )

        if evaluation.decision in {
            CertificationDecision.DENY,
            CertificationDecision.REQUIRE_HUMAN,
        }:
            datasets = (
                tuple(dep.dataset for dep in deps)
                if deps
                else (model_key,)
            )
            raise SemanticAccessDenied(
                model_key=model_key,
                datasets=datasets,
                decision=evaluation.decision,
                reasons=evaluation.reasons,
                evaluated_at=evaluation.evaluated_at,
                recommended_action=self._recommended_certification_action(datasets),
            )

        if evaluation.decision == CertificationDecision.WARN and count_warning:
            self.certification_warning_count += 1
            print(
                "⚠️  [SemanticEngine] Certification policy warning for "
                f"model '{model_key}': {', '.join(evaluation.reasons)}"
            )

        return mode, context, deps, certifications, evaluation

    def _semantic_certification_mode(self) -> str:
        env_config = self._env_config()
        return env_config.get("semantic_certification_policy", "off")

    def _semantic_certification_max_age(self) -> str | None:
        env_config = self._env_config()
        return env_config.get("semantic_certification_max_age")

    def _default_consumer_context(self) -> ConsumerContext:
        env_config = self._env_config()
        return ConsumerContext(
            consumer_id="legacy",
            consumer_class=env_config.get("semantic_consumer_class", "dashboard"),
        )

    def _env_config(self) -> dict:
        context = getattr(self.core, "_context", None)
        if context is not None and hasattr(context, "env_config"):
            return context.env_config()

        env = getattr(self.core, "env", "DEV").lower()
        config = getattr(self.core, "config", {})
        return config.get("environments", {}).get(env, {})

    def _get_certification(self, dataset: str, consumer_class: str):
        if self.certification_store is None:
            return None

        try:
            return self.certification_store.get_certification(dataset, consumer_class)
        except Exception:
            return None

    @staticmethod
    def _recommended_certification_action(datasets: tuple[str, ...]) -> str:
        target = ", ".join(datasets) if datasets else "the semantic model dependencies"
        return f"Contact the data owner to certify {target}"

    @staticmethod
    def _semantic_definition_comment(
        model_key: str,
        mode: str,
        certifications: tuple[Any, ...],
    ) -> str:
        def safe(value: str) -> str:
            return value.replace("\n", " ").replace("\r", " ")

        parts = [f"model={safe(model_key)}", f"mode={safe(mode)}"]
        if not certifications:
            parts.append("dependency(status=unresolved, reason=no_resolved_dependencies)")
        for certification in certifications:
            if certification is None:
                parts.append(
                    f"dependency(dataset={safe(model_key)}, status=unresolved)"
                )
                continue

            dep_parts = [
                f"dataset={safe(certification.dataset)}",
                f"status={safe(certification.status)}",
            ]
            if certification.contract_version:
                dep_parts.append(
                    f"contract_version={safe(certification.contract_version)}"
                )
            if certification.definition_hash:
                dep_parts.append(
                    f"definition_hash={safe(certification.definition_hash)}"
                )
            parts.append("dependency(" + ", ".join(dep_parts) + ")")

        return "-- skifer semantic definition: " + "; ".join(parts)

    def _resolve_view_schema(self) -> str:
        """
        Résout le schéma cible pour les vues sémantiques.
        Lit semantic_views_schema depuis config.yaml (+ suffix sandbox si applicable).
        Fallback sur 'semantic_views' avec warning.
        """
        env = getattr(self.core, "env", "DEV").lower()
        config = getattr(self.core, "config", {})

        base_schema = (
            config
            .get("environments", {})
            .get(env, {})
            .get("semantic_views_schema")
        )

        if not base_schema:
            print(
                f"⚠️  [SemanticEngine] 'semantic_views_schema' non configuré pour "
                f"l'env '{env.upper()}'. Fallback sur 'semantic_views'. "
                f"Ajoutez cette clé dans config.yaml pour personnaliser."
            )
            base_schema = "semantic_views"

        suffix = getattr(self.core, "schema_suffix", "")
        return f"{base_schema}{suffix}"

    # ------------------------------------------------------------------
    # Context LLM (rétro-compatibilité + usage interne agent)
    # ------------------------------------------------------------------

    def get_llm_context(self, model_key: str) -> str:
        """
        Génère un JSON lisible du contexte sémantique d'un modèle,
        conçu pour être injecté dans un prompt LLM.

        Masque le SQL et les filtres inline — expose uniquement les noms
        et descriptions des dimensions et métriques.

        Args:
            model_key: Clé du modèle (ex: "kpi_orders.erp").

        Returns:
            Chaîne JSON formatée.
        """
        import json

        model = self._get_model(model_key)

        ctx = {
            "model_key": model_key,
            "description": model.get("description", ""),
            "available_dimensions": [
                {
                    "name": d["name"],
                    "type": d.get("type", "string"),
                    "description": d.get("description", ""),
                }
                for d in model.get("dimensions", [])
            ],
            "available_metrics": [
                {
                    "name": m["name"],
                    "description": m.get("description", ""),
                }
                for m in model.get("metrics", [])
            ],
        }
        return json.dumps(ctx, indent=2, ensure_ascii=False)

    # ------------------------------------------------------------------
    # Mise à jour catalogue (utilisé par SemanticBuilder)
    # ------------------------------------------------------------------

    @staticmethod
    def update_catalog(output_dir: str, new_entry: dict) -> None:
        """
        Met à jour semantic_catalog.yaml avec un nouveau modèle.
        Si le modèle existe déjà (même clé), il est remplacé.

        Args:
            output_dir: Répertoire semantic_models/.
            new_entry:  Dict catalogue du nouveau modèle.
        """
        catalog_path = os.path.join(output_dir, "semantic_catalog.yaml")

        if os.path.exists(catalog_path):
            with open(catalog_path, encoding="utf-8") as f:
                catalog = yaml.safe_load(f) or {"models": []}
        else:
            catalog = {"models": []}

        # Remplacer ou ajouter
        catalog["models"] = [
            m for m in catalog["models"] if m.get("key") != new_entry["key"]
        ]
        catalog["models"].append(new_entry)

        # Trier par clé pour la lisibilité
        catalog["models"].sort(key=lambda m: m["key"])
        catalog["_generated_at"] = datetime.datetime.now().isoformat()
        catalog["_total_models"] = len(catalog["models"])

        os.makedirs(output_dir, exist_ok=True)
        with open(catalog_path, "w", encoding="utf-8") as f:
            yaml.dump(catalog, f, allow_unicode=True, sort_keys=False)
