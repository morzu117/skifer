"""
AgenticHub — point d'entrée unique pour toutes les interactions agentiques.

Le hub route chaque question vers l'agent spécialisé approprié :
  - LineageAgent   : provenance, impact, upstream/downstream
  - DictionaryAgent: définitions, glossaire
  - QualityAgent   : santé, anomalies, checks
  - BuilderAgent   : génération de YAML de pipeline
  - GenBIAgent     : fallback — requêtes KPI / métier

Routage déterministe (mots-clés) en priorité, fallback LLM optionnel.
"""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Any, Union

from .agent import GenBIAgent
from .builder_agent import BuilderAgent
from .dictionary_agent import DictionaryAgent
from .history import SessionHistory
from .lineage_agent import LineageAgent
from .models import (
    AgentResponse,
    BuilderResponse,
    CapabilityRequest,
    CapabilityResponse,
    DictionaryResponse,
    LineageResponse,
    QualityResponse,
)
from .quality_agent import QualityAgent
from .user_profile import UserProfile, _normalize
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
from ..semantic.llm_provider import llm_span_scope

if TYPE_CHECKING:
    from ..capabilities.invoker import CapabilityInvoker
    from ..semantic.llm_provider import LLMProvider

logger = logging.getLogger(__name__)

HubResponse = Union[
    AgentResponse,
    LineageResponse,
    QualityResponse,
    DictionaryResponse,
    BuilderResponse,
]

# ---------------------------------------------------------------------------
# Keyword sets — déterministe, sans LLM
# ---------------------------------------------------------------------------

_LINEAGE_KEYWORDS = frozenset({
    "d'ou", "d'où", "vient", "provenance", "upstream", "origine", "source de",
    "where does", "come from", "provient",
})
_LINEAGE_IMPACT_KEYWORDS = frozenset({
    "impact", "downstream", "affecte", "utilise par", "depend",
    "depends on", "affected",
})
_DICTIONARY_KEYWORDS = frozenset({
    "que signifie", "definis", "definition", "glossaire", "vocabulaire",
    "what is", "what does mean", "signification",
})
_QUALITY_KEYWORDS = frozenset({
    "qualite", "saine", "anomalie", "checks", "nulls", "doublons",
    "health", "monitor",
})
_BUILDER_KEYWORDS = frozenset({
    "cree", "genere", "construis", "yaml", "pipeline", "schema",
    "build", "create",
})

_INTENT_TO_KEYWORDS = [
    ("lineage",    _LINEAGE_KEYWORDS | _LINEAGE_IMPACT_KEYWORDS),
    ("dictionary", _DICTIONARY_KEYWORDS),
    ("quality",    _QUALITY_KEYWORDS),
    ("builder",    _BUILDER_KEYWORDS),
]


def _detect_intent(question: str) -> str:
    """Retourne l'intent déterministe : 'lineage' | 'dictionary' | 'quality' | 'builder' | 'genbi'."""
    normalized = _normalize(question)
    for intent, keywords in _INTENT_TO_KEYWORDS:
        if any(kw in normalized for kw in keywords):
            return intent
    return "genbi"


# ---------------------------------------------------------------------------
# AgenticHub
# ---------------------------------------------------------------------------

class AgenticHub:
    """
    Point d'entrée unique pour toutes les interactions agentiques.

    Usage minimal (sans LLM, sans Spark) :
        hub = AgenticHub(lineage_agent=LineageAgent(...))
        response = hub.ask("D'où vient le champ amount_eur ?")

    Tous les agents sont optionnels. Si l'agent cible d'une question n'est
    pas injecté, ask() retourne un AgentResponse en mode "error".
    """

    def __init__(
        self,
        semantic_engine: Any = None,
        llm_provider: "LLMProvider | None" = None,
        lineage_agent: LineageAgent | None = None,
        quality_agent: QualityAgent | None = None,
        dictionary_agent: DictionaryAgent | None = None,
        builder_agent: BuilderAgent | None = None,
        genbi_agent: GenBIAgent | None = None,
        session_title: str = "SkiferHub",
        profile: UserProfile | None = None,
        capability_invoker: "CapabilityInvoker | None" = None,
    ) -> None:
        self._semantic_engine = semantic_engine
        self._llm_provider = llm_provider

        # Agents injectés directement ou construits si les dépendances sont fournies
        self._genbi: GenBIAgent | None = genbi_agent
        if self._genbi is None and semantic_engine is not None and llm_provider is not None:
            self._genbi = GenBIAgent(
                semantic_engine=semantic_engine,
                llm_provider=llm_provider,
            )

        self._lineage: LineageAgent | None = lineage_agent
        self._quality: QualityAgent | None = quality_agent
        self._dictionary: DictionaryAgent | None = dictionary_agent
        self._builder: BuilderAgent | None = builder_agent

        self._history = SessionHistory(session_title=session_title)
        self._profile: UserProfile | None = profile
        self._capability_invoker = capability_invoker

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def ask(self, question: str, **kwargs: Any) -> HubResponse:
        """
        Analyse la question, route vers le bon agent, et retourne la réponse.

        kwargs sont transmis à l'agent cible si pertinents (ex: output_dir pour BuilderAgent).
        """
        consumer_context = kwargs.get("consumer_context")
        requested_trace_id = getattr(consumer_context, "trace_id", None)
        active = current_trace_context(self.tracer)
        seed_trace_id = (
            requested_trace_id
            if isinstance(requested_trace_id, str)
            and is_valid_trace_id(requested_trace_id)
            else None
        )
        with trace_context_scope(
            self.tracer,
            TraceContext(trace_id=active.trace_id or seed_trace_id),
            required=self._tracing_required,
        ):
            with configured_span_scope(
                self.tracer,
                "skifer.agent.route",
                attributes={"skifer.trace_version": TRACE_FORMAT_VERSION},
                required=self._tracing_required,
            ) as route_span:
                return self._ask(question, route_span=route_span, **kwargs)

    def invoke_capability(self, request: CapabilityRequest) -> CapabilityResponse:
        """Invoke only an explicitly structured capability request."""
        if not isinstance(request, CapabilityRequest):
            raise TypeError("request must be a CapabilityRequest.")
        if self._capability_invoker is None:
            from ..capabilities.executors import CapabilityExecutorError

            raise CapabilityExecutorError("No capability invoker is configured in this hub.")
        result = self._capability_invoker.invoke(
            request.capability_id,
            request.arguments,
        )
        return CapabilityResponse(
            capability_id=result.capability_id,
            capability_version=result.capability_version,
            result=result,
        )

    def _ask(self, question: str, *, route_span: Any, **kwargs: Any) -> HubResponse:
        """Route through one business path irrespective of tracer implementation."""
        # 1. Pré-résolution des alias utilisateur
        resolved = self._resolve_aliases(question)

        # 2. Détection de l'intent
        intent = _detect_intent(resolved)

        # 3. Fallback LLM si ambiguïté et provider disponible
        if intent == "genbi" and self._llm_provider is not None:
            llm_intent = self._llm_classify_intent(resolved)
            if llm_intent:
                intent = llm_intent

        # 4. Dispatch
        set_span_attribute(
            route_span,
            "route",
            intent,
            required=self._tracing_required,
        )
        return self._dispatch(intent, question, resolved, **kwargs)

    @property
    def tracer(self):
        if self._semantic_engine is not None:
            return getattr(self._semantic_engine, "tracer", NoOpTracer())
        if self._genbi is not None:
            return getattr(self._genbi, "tracer", NoOpTracer())
        return NoOpTracer()

    @property
    def _tracing_required(self) -> bool:
        if self._semantic_engine is not None:
            return bool(getattr(self._semantic_engine, "_tracing_required", False))
        return False

    @property
    def history(self) -> SessionHistory:
        return self._history

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _resolve_aliases(self, question: str) -> str:
        """
        Remplace les alias utilisateur par les noms canoniques.

        Utilise des frontières de mots (\\b) pour éviter de remplacer les alias
        qui sont des sous-chaînes d'autres mots (ex: 'ca' ne doit pas altérer 'cascade').
        """
        if self._profile is None:
            return question
        resolved = question
        for term, canonical in self._profile.aliases.items():
            # re.escape protège les caractères spéciaux dans le terme
            resolved = re.sub(
                r"\b" + re.escape(term) + r"\b",
                canonical,
                resolved,
                flags=re.IGNORECASE,
            )
        return resolved

    def _llm_classify_intent(self, question: str) -> str | None:
        """
        Appel LLM minimal : classification de l'intent.
        Retourne l'un des 5 intents ou None si appel échoue.
        """
        system_prompt = (
            "Classe la question de l'utilisateur dans l'un de ces intents : "
            "genbi, lineage, quality, dictionary, builder. "
            "Réponds uniquement avec le mot-clé de l'intent, sans aucun autre texte."
        )
        try:
            with llm_span_scope(
                self._llm_provider,
                self.tracer,
                required=self._tracing_required,
            ):
                raw = self._llm_provider.complete(
                    system_prompt=system_prompt,
                    user_message=question,
                    temperature=0.0,
                ).strip().lower()
            if raw in {"genbi", "lineage", "quality", "dictionary", "builder"}:
                return raw
        except Exception:
            logger.warning("LLM intent classification failed", exc_info=True)
        return None

    def _dispatch(
        self,
        intent: str,
        original_question: str,
        resolved_question: str,
        **kwargs: Any,
    ) -> HubResponse:
        """Route vers l'agent cible et retourne sa réponse."""

        def _unavailable(agent_name: str) -> AgentResponse:
            return AgentResponse(
                question=original_question,
                mode="error",
                error=f"Agent '{agent_name}' non configure dans ce hub.",
            )

        if intent == "lineage":
            if self._lineage is None:
                return _unavailable("lineage")
            return self._lineage.ask(resolved_question)

        if intent == "dictionary":
            if self._dictionary is None:
                return _unavailable("dictionary")
            return self._dictionary.ask(resolved_question)

        if intent == "quality":
            if self._quality is None:
                return _unavailable("quality")
            return self._quality.ask(resolved_question)

        if intent == "builder":
            if self._builder is None:
                return _unavailable("builder")
            output_dir = kwargs.get("output_dir", "schemas")
            return self._builder.ask(resolved_question, output_dir=output_dir)

        # fallback → GenBIAgent
        if self._genbi is None:
            return _unavailable("genbi")
        return self._genbi.ask(
            resolved_question,
            consumer_context=kwargs.get("consumer_context"),
        )
