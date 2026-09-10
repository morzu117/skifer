"""
GenBIAgent — couche agentic natural language → sémantique → Spark.

Architecture pipeline 2 étapes LLM :
  Step A : sélection du modèle depuis le catalogue compact (~500 tokens)
  Step B : traduction en SemanticQuery (structured output JSON)
  QueryResolver : validation + construction SQL (déterministe, 0 LLM)
  SemanticEngine : exécution Spark (query ou create_view)

Principe fondamental :
  - Le LLM ne voit jamais de données et ne touche jamais à Spark.
  - Le LLM ne produit que des noms (metrics, dimensions) — jamais du SQL.
  - Si un nom est absent du YAML, le QueryResolver rejette sans exécuter.
"""

from __future__ import annotations

import base64
import datetime
import json
import logging
import re
from io import BytesIO
from typing import Any

from ..semantic.access_policy import ConsumerContext, SemanticAccessDenied
from ..semantic.evidence import SemanticExecutionError
from ..semantic.llm_provider import LLMProvider, llm_span_scope
from ..semantic.semantic import SemanticEngine
from ..semantic.planner import SemanticPlanner
from ..observability.tracing import (
    TRACE_FORMAT_VERSION,
    TraceContext,
    configured_span_scope,
    current_trace_context,
    is_valid_trace_id,
    set_span_attribute,
    trace_context_scope,
)
from .history import HistoryEntry, SessionHistory
from .models import AgentResponse, FormattedResult, ResponseFormat
from .resolver import QueryResolver, SemanticQuery, SemanticQueryError

try:
    import matplotlib
    matplotlib.use("Agg")
except ImportError:
    matplotlib = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Prompts système
# ---------------------------------------------------------------------------

_STEP_A_SYSTEM = """Tu es un expert en sélection de modèles sémantiques BI.

Ton rôle : choisir LE modèle sémantique le plus pertinent pour répondre à la question de l'utilisateur.

Règles :
- Tu DOIS répondre UNIQUEMENT avec un objet JSON valide.
- Aucun texte avant ou après le JSON.
- Si un seul modèle est pertinent :
  {{"selected": "<model_key>", "candidates": [], "reason": "<explication courte>"}}
- Si plusieurs modèles sont candidats (ambiguïté) :
  {{"selected": null, "candidates": ["<key1>", "<key2>"], "reason": "<pourquoi ambigu>"}}
- Si aucun modèle ne correspond :
  {{"selected": null, "candidates": [], "reason": "<pourquoi aucun>"}}

Catalogue des modèles disponibles :
{catalog_summary}
"""

_STEP_B_SYSTEM = """Tu es un expert en traduction de questions en requêtes sémantiques BI.

Ton rôle : traduire la question en une SemanticQuery JSON en utilisant UNIQUEMENT les noms listés.

Modèle sémantique sélectionné :
{model_context}

Règles STRICTES :
- Tu DOIS répondre UNIQUEMENT avec un objet JSON valide. Aucun texte autour.
- N'invente JAMAIS un nom de métrique ou de dimension absent de la liste ci-dessus.
- Si une information manque (ex: dimension inconnue), utilise quand même les noms disponibles les plus proches.
- Si le modèle expose "calendar", utilise UNIQUEMENT un nom listé dans "calendar.periods"
  et laisse "date_from"/"date_to" à null : une période fiscale se résout par définition déclarée.
- Sans "calendar", renseigne "date_from"/"date_to" au format YYYY-MM-DD et laisse "period" à null.
- Ne renseigne JAMAIS "period" et une borne de date en même temps.
- Pour le champ "mode" : "query" si l'utilisateur veut voir des données, "view" si il veut créer une vue SQL.
- Pour le champ "response_format" : "kpi" (valeur unique), "table" (données), "chart" (graphique), "text_analysis" (analyse narrative).

Structure JSON attendue :
{{
  "model_name": "<model_key>",
  "metrics": ["<metric_name>"],
  "group_by": ["<dimension_name>"],
  "filters": [{{"column": "<dim_name>", "operator": "<op>", "value": "<val>"}}],
  "date_from": "<YYYY-MM-DD ou null>",
  "date_to": "<YYYY-MM-DD ou null>",
  "period": "<calendar_period_name ou null>",
  "mode": "query",
  "view_name": null,
  "response_format": "table",
  "explanation": "<ce que tu as compris en français>"
}}

Opérateurs disponibles : eq, neq, gt, lt, gte, lte, in, like, is_null, is_not_null.
"""

_STEP_D_SYSTEM = """Tu es un expert en analyse de données BI.
Rédige une analyse concise en 3-5 phrases à partir des résultats fournis.
Ne réinvente pas de données — base-toi UNIQUEMENT sur les chiffres du tableau.
Réponds en français.
"""


# ---------------------------------------------------------------------------
# GenBIAgent
# ---------------------------------------------------------------------------

class GenBIAgent:
    """
    Agent BI conversationnel — natural language → SemanticEngine.

    Usage :
        from skifer.semantic.llm_provider import get_llm_provider
        from skifer.semantic.semantic import SemanticEngine
        from skifer.agentic.agent import GenBIAgent

        engine = SemanticEngine(core)
        agent  = GenBIAgent(engine, get_llm_provider(), history=True)

        resp = agent.ask("Quel est le CA par région pour 2024 ?")
        if resp.success:
            resp.result.data.show()      # FormattedResult.data = DataFrame
        elif resp.needs_clarification:
            print(resp.clarification_question)

        agent.session.to_pdf("rapport.pdf")
    """

    def __init__(
        self,
        semantic_engine: SemanticEngine,
        llm_provider: LLMProvider,
        history: bool = False,
        session_title: str = "Analyse KPI",
    ):
        """
        Args:
            semantic_engine: SemanticEngine configuré (catalog-first).
            llm_provider:    Instance LLMProvider (OpenAI, Anthropic, …).
            history:         Active l'historique conversationnel multi-turn.
            session_title:   Titre de la session pour l'export PDF.
        """
        self.semantic = semantic_engine
        self.llm = llm_provider
        self._conv_history: list[dict] | None = [] if history else None
        self._session = SessionHistory(session_title)
        self._resolver = QueryResolver(SemanticPlanner(semantic_engine))

    # ------------------------------------------------------------------
    # Interface publique
    # ------------------------------------------------------------------

    def ask(
        self,
        question: str,
        mode: str = "auto",
        tags: list[str] | None = None,
        layer: str | None = None,
        consumer_context: ConsumerContext | None = None,
    ) -> AgentResponse:
        """
        Prend une question en langage naturel et retourne une AgentResponse.

        Args:
            question: Question en langage naturel.
            mode:     "auto" (détecté par LLM) | "query" | "view".
            tags:     Restreindre la sélection de modèles par tags.
            layer:    Restreindre la sélection par layer (ex: "gold").
            consumer_context: Contexte consommateur optionnel pour la certification.

        Returns:
            AgentResponse avec le résultat ou une demande de clarification.
        """
        active = current_trace_context(self.tracer)
        requested_trace_id = (
            consumer_context.trace_id if consumer_context is not None else None
        )
        seed_trace_id = (
            requested_trace_id
            if requested_trace_id is not None and is_valid_trace_id(requested_trace_id)
            else None
        )
        with trace_context_scope(
            self.tracer,
            TraceContext(trace_id=active.trace_id or seed_trace_id),
            required=self._tracing_required,
        ):
            with configured_span_scope(
                self.tracer,
                "skifer.semantic.query",
                attributes={"skifer.trace_version": TRACE_FORMAT_VERSION},
                required=self._tracing_required,
            ):
                return self._ask(
                    question,
                    mode=mode,
                    tags=tags,
                    layer=layer,
                    consumer_context=consumer_context,
                )

    def _ask(
        self,
        question: str,
        mode: str = "auto",
        tags: list[str] | None = None,
        layer: str | None = None,
        consumer_context: ConsumerContext | None = None,
    ) -> AgentResponse:
        """Single business flow used identically by no-op and recording tracers."""
        resp = self._process(
            question,
            mode=mode,
            tags=tags,
            layer=layer,
            consumer_context=consumer_context,
        )
        self._session.add(self._to_history_entry(resp))

        # Mise à jour historique conversationnel
        if self._conv_history is not None:
            self._conv_history.append({"role": "user", "content": question})
            self._conv_history.append({
                "role": "assistant",
                "content": resp.explanation or resp.clarification_question or "",
            })

        return resp

    @property
    def tracer(self):
        return self.semantic.tracer

    @property
    def _tracing_required(self) -> bool:
        return self.semantic._tracing_required

    def reset_history(self) -> None:
        """Vide l'historique conversationnel (pas la SessionHistory)."""
        if self._conv_history is not None:
            self._conv_history = []

    @property
    def session(self) -> SessionHistory:
        """Accès à la SessionHistory pour export PDF/JSON."""
        return self._session

    # ------------------------------------------------------------------
    # Pipeline interne
    # ------------------------------------------------------------------

    def _process(
        self,
        question: str,
        mode: str = "auto",
        tags: list[str] | None = None,
        layer: str | None = None,
        consumer_context: ConsumerContext | None = None,
    ) -> AgentResponse:
        """Orchestre le pipeline A → B → Resolver → Engine."""

        # ── Step A : sélection du modèle ─────────────────────────────
        catalog_models = self.semantic.list_models(tags=tags, layer=layer, summary=True)

        if not catalog_models:
            return AgentResponse(
                question=question,
                mode="no_model",
                explanation="Aucun modèle sémantique disponible dans le catalogue.",
                clarification_question=(
                    "Le catalogue est vide. "
                    "Lancez d'abord SemanticBuilder.build() pour générer des modèles."
                ),
            )

        with configured_span_scope(
            self.tracer,
            "skifer.semantic.model_select",
            required=self._tracing_required,
        ) as model_span:
            step_a_result = self._step_a_select_model(question, catalog_models)
            selected_for_trace = step_a_result.get("selected")
            if isinstance(selected_for_trace, str):
                set_span_attribute(
                    model_span,
                    "model_key",
                    selected_for_trace,
                    required=self._tracing_required,
                )

        selected = step_a_result.get("selected")
        candidates = step_a_result.get("candidates", [])
        reason = step_a_result.get("reason", "")

        if not selected and not candidates:
            return AgentResponse(
                question=question,
                mode="no_model",
                explanation=reason,
                clarification_question=(
                    f"Aucun modèle ne couvre ce sujet. {reason} "
                    f"Modèles disponibles : {[m['key'] for m in catalog_models]}"
                ),
            )

        if not selected and candidates:
            return AgentResponse(
                question=question,
                mode="ambiguous",
                explanation=reason,
                clarification_question=reason,
                candidates=candidates,
            )

        model_key = selected
        # ── Step B : traduction SemanticQuery ────────────────────────
        try:
            model_full = self.semantic._get_model(model_key)
        except ValueError as e:
            return AgentResponse(
                question=question,
                mode="error",
                model_used=model_key,
                error=str(e),
            )

        semantic_query = self._step_b_translate(question, model_key, model_full)

        # Override mode si explicite
        if mode != "auto":
            semantic_query.mode = mode

        gate_mode = None
        gate_context = None
        gate_deps = None
        if semantic_query.mode != "view":
            try:
                with configured_span_scope(
                    self.tracer,
                    "skifer.semantic.policy",
                    attributes={"model_key": model_key},
                    required=self._tracing_required,
                ):
                    gate_mode, gate_context, gate_deps, _gate_certifications = (
                        self.semantic.enforce_certification_gate(
                            model_key,
                            model_full,
                            consumer_context,
                        )
                    )
            except SemanticAccessDenied as exc:
                return AgentResponse(
                    question=question,
                    mode="error",
                    model_used=model_key,
                    explanation=semantic_query.explanation,
                    error=str(exc),
                    access_denied=True,
                    policy_decision=exc.decision.value,
                    policy_reasons=exc.reasons,
                    recommended_action=exc.recommended_action,
                )

        # ── QueryResolver : validation + SQL ─────────────────────────
        try:
            with configured_span_scope(
                self.tracer,
                "skifer.semantic.compile",
                attributes={"model_key": model_key},
                required=self._tracing_required,
            ):
                resolved = self._resolver.resolve(
                    semantic_query, model_full, self.semantic.core.db
                )
        except SemanticQueryError as exc:
            # Clarification loop : nom inconnu
            suggestions = self._resolver._suggest_closest(
                str(exc).split("'")[1] if "'" in str(exc) else "",
                {d["name"] for d in model_full.get("dimensions", [])}
                | {m["name"] for m in model_full.get("metrics", [])},
            )
            return AgentResponse(
                question=question,
                mode="needs_clarification",
                model_used=model_key,
                explanation=str(exc),
                clarification_question=self._build_clarification_question(str(exc)),
                suggestions=suggestions,
            )

        # ── Exécution ─────────────────────────────────────────────────
        if semantic_query.mode == "view":
            return self._execute_view(
                question,
                semantic_query,
                model_key,
                consumer_context=consumer_context,
            )

        return self._execute_query(
            question,
            semantic_query,
            model_key,
            resolved,
            model_full,
            consumer_context=consumer_context,
            gate_mode=gate_mode,
            gate_context=gate_context,
            gate_deps=gate_deps,
        )

    # ------------------------------------------------------------------
    # Step A — sélection du modèle
    # ------------------------------------------------------------------

    def _step_a_select_model(
        self, question: str, catalog_models: list[dict]
    ) -> dict:
        """Appel LLM pour sélectionner le modèle depuis le catalogue compact."""
        catalog_summary = json.dumps(catalog_models, ensure_ascii=False, indent=2)
        system = _STEP_A_SYSTEM.format(catalog_summary=catalog_summary)

        try:
            with llm_span_scope(
                self.llm, self.tracer, required=self._tracing_required
            ):
                raw = self.llm.complete(
                    system_prompt=system,
                    user_message=question,
                    temperature=0.0,
                    response_format={"type": "json_object"},
                )
        except Exception as exc:
            logger.error("[GenBIAgent] Step A LLM call failed: %s", exc, exc_info=True)
            return {"selected": None, "candidates": [], "reason": f"LLM unavailable: {exc}"}
        return self._parse_json(raw, default={"selected": None, "candidates": [], "reason": ""})

    # ------------------------------------------------------------------
    # Step B — traduction SemanticQuery
    # ------------------------------------------------------------------

    def _step_b_translate(
        self, question: str, model_key: str, model_full: dict
    ) -> SemanticQuery:
        """Appel LLM pour traduire la question en SemanticQuery."""
        model_context = self._build_model_context(model_key, model_full)
        system = _STEP_B_SYSTEM.format(model_context=model_context)

        try:
            if self._conv_history:
                history = list(self._conv_history) + [{"role": "user", "content": question}]
                with llm_span_scope(
                    self.llm, self.tracer, required=self._tracing_required
                ):
                    raw = self.llm.complete_with_history(
                        system_prompt=system,
                        history=history,
                        temperature=0.0,
                        response_format={"type": "json_object"},
                    )
            else:
                with llm_span_scope(
                    self.llm, self.tracer, required=self._tracing_required
                ):
                    raw = self.llm.complete(
                        system_prompt=system,
                        user_message=question,
                        temperature=0.0,
                        response_format={"type": "json_object"},
                    )
        except Exception as exc:
            logger.error("[GenBIAgent] Step B LLM call failed: %s", exc, exc_info=True)
            raw = ""

        data = self._parse_json(raw, default={})
        data.setdefault("model_name", model_key)
        return SemanticQuery.from_dict(data)

    def _build_model_context(self, model_key: str, model_full: dict) -> str:
        """Construit le contexte LLM lisible depuis le YAML complet."""
        ctx = {
            "model_key": model_key,
            "description": model_full.get("description", ""),
            "dimensions": [
                {
                    "name": d["name"],
                    "type": d.get("type", "string"),
                    "description": d.get("description", ""),
                }
                for d in model_full.get("dimensions", [])
            ],
            "metrics": [
                {
                    "name": m["name"],
                    "description": m.get("description", ""),
                }
                for m in model_full.get("metrics", [])
            ],
        }
        calendar_key = model_full.get("calendar")
        if calendar_key:
            calendar = self.semantic._get_calendar(calendar_key)
            ctx["calendar"] = {
                "key": calendar.key,
                "version": calendar.version,
                "periods": [period.name for period in calendar.periods],
            }
        return json.dumps(ctx, ensure_ascii=False, indent=2)

    # ------------------------------------------------------------------
    # Exécution query
    # ------------------------------------------------------------------

    def _execute_query(
        self,
        question: str,
        semantic_query: SemanticQuery,
        model_key: str,
        resolved: Any,
        model_full: dict,
        consumer_context: ConsumerContext | None = None,
        gate_mode: str | None = None,
        gate_context: ConsumerContext | None = None,
        gate_deps: tuple | None = None,
    ) -> AgentResponse:
        """Exécute via spark.sql et formate le résultat."""
        try:
            semantic_result = self.semantic._query_with_evidence_from_resolved(
                semantic_query,
                model_full,
                resolved,
                context=gate_context or consumer_context,
                mode=gate_mode,
                deps=gate_deps,
            )
            formatted = self._format_result(semantic_result.dataframe, semantic_query)
            formatted.evidence = semantic_result.evidence

            return AgentResponse(
                question=question,
                mode="query",
                model_used=model_key,
                explanation=semantic_query.explanation,
                query_params={
                    "metrics": semantic_query.metrics,
                    "group_by": semantic_query.group_by,
                    "filters": semantic_query.filters,
                    "date_from": semantic_query.date_from,
                    "date_to": semantic_query.date_to,
                    "period": semantic_query.period,
                },
                result=formatted,
                evidence=semantic_result.evidence,
            )
        except SemanticExecutionError as exc:
            logger.error("[GenBIAgent] Query execution failed: %s", exc, exc_info=True)
            return AgentResponse(
                question=question,
                mode="error",
                model_used=model_key,
                explanation=semantic_query.explanation,
                error=str(exc),
                evidence=exc.evidence,
            )
        except SemanticAccessDenied as exc:
            logger.error("[GenBIAgent] Query access denied: %s", exc, exc_info=True)
            return AgentResponse(
                question=question,
                mode="error",
                model_used=model_key,
                explanation=semantic_query.explanation,
                error=str(exc),
                access_denied=True,
                policy_decision=exc.decision.value,
                policy_reasons=exc.reasons,
                recommended_action=exc.recommended_action,
            )
        except Exception as exc:
            logger.error("[GenBIAgent] Query execution failed: %s", exc, exc_info=True)
            return AgentResponse(
                question=question,
                mode="error",
                model_used=model_key,
                explanation=semantic_query.explanation,
                error=str(exc),
            )

    # ------------------------------------------------------------------
    # Exécution create_view
    # ------------------------------------------------------------------

    def _execute_view(
        self,
        question: str,
        semantic_query: SemanticQuery,
        model_key: str,
        consumer_context: ConsumerContext | None = None,
    ) -> AgentResponse:
        """Crée une vue via SemanticEngine.create_view()."""
        try:
            view_fqn = self.semantic.create_view(
                semantic_query,
                consumer_context=consumer_context,
            )
            return AgentResponse(
                question=question,
                mode="view",
                model_used=model_key,
                explanation=semantic_query.explanation,
                query_params={
                    "metrics": semantic_query.metrics,
                    "group_by": semantic_query.group_by,
                    "filters": semantic_query.filters,
                    "view_name": semantic_query.view_name,
                },
                result=view_fqn,
            )
        except SemanticAccessDenied as exc:
            logger.error("[GenBIAgent] View access denied: %s", exc, exc_info=True)
            return AgentResponse(
                question=question,
                mode="error",
                model_used=model_key,
                explanation=semantic_query.explanation,
                error=str(exc),
                access_denied=True,
                policy_decision=exc.decision.value,
                policy_reasons=exc.reasons,
                recommended_action=exc.recommended_action,
            )
        except Exception as exc:
            logger.error("[GenBIAgent] View creation failed: %s", exc, exc_info=True)
            return AgentResponse(
                question=question,
                mode="error",
                model_used=model_key,
                explanation=semantic_query.explanation,
                error=str(exc),
            )

    # ------------------------------------------------------------------
    # Formatage du résultat
    # ------------------------------------------------------------------

    def _format_result(
        self, df: Any, semantic_query: SemanticQuery
    ) -> FormattedResult:
        """Produit un FormattedResult depuis le DataFrame et la SemanticQuery."""
        fmt = ResponseFormat(semantic_query.response_format)
        title = self._build_title(semantic_query)

        if fmt == ResponseFormat.KPI:
            return self._format_kpi(df, semantic_query, title)
        elif fmt == ResponseFormat.CHART:
            return self._format_chart(df, semantic_query, title)
        elif fmt == ResponseFormat.TEXT_ANALYSIS:
            return self._format_text_analysis(df, semantic_query, title)
        else:  # TABLE (default)
            return FormattedResult(format=ResponseFormat.TABLE, data=df, title=title)

    def _format_kpi(
        self, df: Any, semantic_query: SemanticQuery, title: str
    ) -> FormattedResult:
        """Extrait la valeur scalaire d'une agrégation globale."""
        met_name = semantic_query.metrics[0] if semantic_query.metrics else ""
        try:
            rows = df.collect()
            value = rows[0][met_name] if rows and met_name else None
        except (IndexError, KeyError):
            value = None
        return FormattedResult(
            format=ResponseFormat.KPI,
            data=df,
            title=title,
            kpi_value=value,
            kpi_label=semantic_query.metrics[0] if semantic_query.metrics else "",
        )

    def _format_chart(
        self, df: Any, semantic_query: SemanticQuery, title: str
    ) -> FormattedResult:
        """Construit la config chart + génère le PNG base64 via matplotlib."""
        x_field = semantic_query.group_by[0] if semantic_query.group_by else ""
        y_field = semantic_query.metrics[0] if semantic_query.metrics else ""

        chart_config = {
            "type": "bar",
            "title": title,
            "x_axis": {"field": x_field, "label": x_field},
            "y_axis": {"field": y_field, "label": y_field},
            "series": semantic_query.metrics[1:],
        }

        return FormattedResult(
            format=ResponseFormat.CHART,
            data=df,
            title=title,
            chart_config=chart_config,
        )

    def _format_text_analysis(
        self, df: Any, semantic_query: SemanticQuery, title: str
    ) -> FormattedResult:
        """Appel LLM Step D pour générer un résumé narratif des données agrégées."""
        summary = None
        try:
            rows = df.collect()
            if rows:
                table_md = self._df_to_markdown(rows, max_rows=50)
                prompt = (
                    f"Question : {semantic_query.explanation or 'Analyse des données'}\n\n"
                    f"Résultats :\n{table_md}"
                )
                with llm_span_scope(
                    self.llm, self.tracer, required=self._tracing_required
                ):
                    summary = self.llm.complete(
                        system_prompt=_STEP_D_SYSTEM,
                        user_message=prompt,
                        temperature=0.2,
                    )
        except Exception:
            summary = None

        return FormattedResult(
            format=ResponseFormat.TEXT_ANALYSIS,
            data=df,
            title=title,
            text_summary=summary,
        )

    def _build_title(self, sq: SemanticQuery) -> str:
        parts = []
        if sq.metrics:
            parts.append(", ".join(sq.metrics))
        if sq.group_by:
            parts.append("par " + ", ".join(sq.group_by))
        if sq.date_from or sq.date_to:
            date_range = f"{sq.date_from or ''}–{sq.date_to or ''}"
            parts.append(date_range)
        elif sq.period:
            parts.append(sq.period)
        return " — ".join(parts) if parts else sq.model_name

    # ------------------------------------------------------------------
    # Historique session
    # ------------------------------------------------------------------

    def _to_history_entry(self, resp: AgentResponse) -> HistoryEntry:
        """Convertit un AgentResponse en HistoryEntry."""
        entry = HistoryEntry(
            timestamp=datetime.datetime.now().isoformat(),
            question=resp.question,
            model_used=resp.model_used,
            explanation=resp.explanation,
            query_params=resp.query_params,
            mode=resp.mode,
            error=resp.error,
        )

        if resp.success and resp.result and isinstance(resp.result, FormattedResult):
            fmt = resp.result
            entry.response_format = fmt.format.value

            if fmt.format == ResponseFormat.KPI:
                entry.kpi_value = fmt.kpi_value
                entry.kpi_label = fmt.kpi_label

            elif fmt.format == ResponseFormat.TABLE:
                try:
                    rows = fmt.data.collect()
                    if rows:
                        entry.table_markdown = self._df_to_markdown(rows, max_rows=100)
                except Exception:
                    pass

            elif fmt.format == ResponseFormat.CHART:
                entry.chart_config = fmt.chart_config
                entry.chart_image_b64 = self._generate_chart_image(fmt)

            elif fmt.format == ResponseFormat.TEXT_ANALYSIS:
                entry.text_summary = fmt.text_summary

        return entry

    def _generate_chart_image(self, fmt: FormattedResult) -> str | None:
        """Génère un PNG base64 depuis matplotlib pour l'historique PDF."""
        try:
            import matplotlib.pyplot as plt

            cfg = fmt.chart_config or {}
            x_field = cfg.get("x_axis", {}).get("field", "")
            y_field = cfg.get("y_axis", {}).get("field", "")
            chart_type = cfg.get("type", "bar")

            rows = fmt.data.collect()
            if not rows or not x_field or not y_field:
                return None

            x_vals = [str(r[x_field]) for r in rows]
            y_vals = [r[y_field] for r in rows]

            fig, ax = plt.subplots(figsize=(8, 4))
            if chart_type == "bar":
                ax.bar(x_vals, y_vals)
            elif chart_type == "line":
                ax.plot(x_vals, y_vals, marker="o")
            elif chart_type == "pie":
                ax.pie(y_vals, labels=x_vals, autopct="%1.1f%%")
            else:
                ax.bar(x_vals, y_vals)

            ax.set_title(fmt.title)
            ax.set_xlabel(cfg.get("x_axis", {}).get("label", x_field))
            ax.set_ylabel(cfg.get("y_axis", {}).get("label", y_field))
            plt.tight_layout()

            buf = BytesIO()
            plt.savefig(buf, format="png", dpi=100)
            plt.close(fig)
            buf.seek(0)
            return base64.b64encode(buf.read()).decode("utf-8")

        except ImportError:
            return None
        except Exception:
            return None

    # ------------------------------------------------------------------
    # Utilitaires
    # ------------------------------------------------------------------

    def _parse_json(self, raw: str, default: dict) -> dict:
        """Parse un JSON depuis une réponse LLM (tolère les blocs ```json```)."""
        if not raw:
            return default
        # Retire les blocs markdown code
        cleaned = re.sub(r"```(?:json)?", "", raw).replace("```", "").strip()
        try:
            return json.loads(cleaned)
        except json.JSONDecodeError:
            return default

    def _df_to_markdown(self, rows: list, max_rows: int = 50) -> str:
        """Converts a list of Spark Row objects to a markdown table string."""
        if not rows:
            return ""
        cols = list(rows[0].asDict().keys())
        header = " | ".join(cols)
        sep = " | ".join(["---"] * len(cols))
        lines = [" | ".join(str(r[c]) for c in cols) for r in rows[:max_rows]]
        return f"| {header} |\n| {sep} |\n" + "\n".join(
            f"| {line} |" for line in lines
        )

    def _build_clarification_question(self, error_msg: str) -> str:
        """Formule une question de clarification lisible depuis l'erreur."""
        if "Metric" in error_msg and "Vouliez-vous dire" in error_msg:
            return f"La métrique demandée n'existe pas. {error_msg}"
        if "Dimension" in error_msg and "Vouliez-vous dire" in error_msg:
            return f"La dimension demandée n'existe pas. {error_msg}"
        if "Colonne de filtre" in error_msg:
            return f"La colonne de filtre demandée n'existe pas. {error_msg}"
        return f"Je ne peux pas exécuter cette requête : {error_msg}"
