"""The LLM proposes names only; the deterministic resolver compiles them to SQL or refuses them.

    python examples/15_genbi_agent/run.py
"""

import json
from pathlib import Path
from types import SimpleNamespace

from skifer.agentic.agent import GenBIAgent
from skifer.agentic.resolver import SemanticQueryError
from skifer.semantic.llm_provider import LLMProvider
from skifer.semantic.semantic import SemanticEngine


class StubLLM(LLMProvider):
    """Offline LLM: returns canned JSON answers in order."""

    def __init__(self, *answers):
        self.answers = [json.dumps(a, separators=(",", ":")) for a in answers]

    provider_name = "offline_stub"

    def complete(self, system_prompt, user_message, **kwargs):
        return self.answers.pop(0)

    def complete_with_history(self, system_prompt, history, **kwargs):
        return self.answers.pop(0)


semantic = SemanticEngine(
    SimpleNamespace(), models_dir=str(Path(__file__).parent / "semantic_models")
)


def ask(question, llm):
    # Step A picks a model from the catalog, step B translates the question into names.
    agent = GenBIAgent(semantic, llm)
    model_key = agent._step_a_select_model(question, semantic.list_models(summary=True))["selected"]
    model = semantic._get_model(model_key)
    return agent, model_key, model, agent._step_b_translate(question, model_key, model)


selection = {"selected": "orders", "candidates": [], "reason": "The question asks about sales revenue."}
names = {"model_name": "orders", "metrics": ["revenue"], "group_by": ["region"]}
llm = StubLLM(selection, names)
step_a, step_b = llm.answers

question = "Show revenue by region"
agent, selected, model, query = ask(question, llm)
print(f"Question: {question}")
print(f"Step A — stub LLM returned: {step_a}")
print(f"Step A — GenBIAgent selected model: {selected}")
print(f"Step B — stub LLM returned names only: {step_b}")
print(f"Step B — SemanticQuery: model={query.model_name}, metrics={query.metrics}, group_by={query.group_by}")
print("QueryResolver built from those names:")
print(agent._resolver.resolve(query, model, None).full_sql)
print("SQL shown, not executed: this example stays Spark-free.")

bad_names = {**names, "group_by": ["regions"]}
bad_llm = StubLLM(selection, bad_names)
print(f"Bad stub LLM returned names only: {bad_llm.answers[1]}")
agent, _, model, query = ask("Show revenue by regions", bad_llm)
try:
    agent._resolver.resolve(query, model, None)
except SemanticQueryError as exc:
    print(f"Resolver refusal: {exc}")
