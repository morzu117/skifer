"""Show the GenBI names-only boundary with an offline stub LLM.

    python examples/15_genbi_agent/run.py

The two agent interpretation steps run normally. The example deliberately
stops after deterministic SQL resolution, before any Spark execution.
"""

from collections import deque
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from skifer.agentic.agent import GenBIAgent
from skifer.agentic.resolver import SemanticQueryError
from skifer.semantic.llm_provider import LLMProvider
from skifer.semantic.semantic import SemanticEngine


EXAMPLE_DIR = Path(__file__).parent


class StubLLMProvider(LLMProvider):
    """A deterministic provider: no SDK, credential, API call, or network."""

    def __init__(self, responses: list[dict[str, Any]]):
        self._responses = deque(
            json.dumps(response, separators=(",", ":")) for response in responses
        )
        self.returned: list[str] = []

    @property
    def provider_name(self) -> str:
        return "offline_stub"

    def complete(
        self,
        system_prompt: str,
        user_message: str,
        temperature: float = 0.0,
        response_format: dict | None = None,
        **kwargs: Any,
    ) -> str:
        response = self._responses.popleft()
        self.returned.append(response)
        return response

    def complete_with_history(
        self,
        system_prompt: str,
        history: list[dict],
        temperature: float = 0.0,
        response_format: dict | None = None,
        **kwargs: Any,
    ) -> str:
        return self.complete(
            system_prompt,
            history[-1]["content"],
            temperature,
            response_format,
            **kwargs,
        )


def _agent_proposal(
    semantic: SemanticEngine, provider: StubLLMProvider, question: str
):
    agent = GenBIAgent(semantic, provider)
    selection = agent._step_a_select_model(
        question, semantic.list_models(summary=True)
    )
    model_key = selection["selected"]
    model = semantic._get_model(model_key)
    query = agent._step_b_translate(question, model_key, model)
    return agent, selection, model, query


def main() -> None:
    semantic = SemanticEngine(
        SimpleNamespace(), models_dir=str(EXAMPLE_DIR / "semantic_models")
    )
    question = "Show revenue by region"
    valid_stub = StubLLMProvider(
        [
            {
                "selected": "orders",
                "candidates": [],
                "reason": "The question asks about sales revenue.",
            },
            {
                "model_name": "orders",
                "metrics": ["revenue"],
                "group_by": ["region"],
            },
        ]
    )
    agent, selection, model, query = _agent_proposal(
        semantic, valid_stub, question
    )
    resolved = agent._resolver.resolve(query, model, None)

    print(f"Question: {question}")
    print(f"Step A — stub LLM returned: {valid_stub.returned[0]}")
    print(f"Step A — GenBIAgent selected model: {selection['selected']}")
    print(f"Step B — stub LLM returned names only: {valid_stub.returned[1]}")
    print(
        "Step B — SemanticQuery: "
        f"model={query.model_name}, metrics={query.metrics}, group_by={query.group_by}"
    )
    print("QueryResolver built from those names:")
    print(resolved.full_sql)
    print("SQL shown, not executed: this example stays Spark-free.")

    invalid_stub = StubLLMProvider(
        [
            {"selected": "orders", "candidates": [], "reason": "Sales model."},
            {
                "model_name": "orders",
                "metrics": ["revenue"],
                "group_by": ["regions"],
            },
        ]
    )
    bad_agent, _, bad_model, bad_query = _agent_proposal(
        semantic, invalid_stub, "Show revenue by regions"
    )
    print(f"Bad stub LLM returned names only: {invalid_stub.returned[1]}")
    try:
        bad_agent._resolver.resolve(bad_query, bad_model, None)
    except SemanticQueryError as exc:
        print(f"Resolver refusal: {exc}")


if __name__ == "__main__":
    main()
