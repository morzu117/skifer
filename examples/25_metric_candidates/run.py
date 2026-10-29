"""From an uncovered question to a validated, promotable candidate metric — offline.

    python examples/25_metric_candidates/run.py

Every LLM answer is written below (ScriptedLLM): no API key, no network, no workspace.
Everything the run writes (SQLite stores, Delta tables, drafts, reports) goes to a
temporary directory; the example's own tree is never touched.
"""

from argparse import Namespace
import contextlib
import io
import json
import os
import re
from pathlib import Path
import shutil
import tempfile

EXAMPLE_DIR = Path(__file__).resolve().parent
WORK = Path(tempfile.mkdtemp(prefix="skifer_ex25_"))
for name in ("config.yaml", "semantic_models", "schemas", "data"):
    source = EXAMPLE_DIR / name
    (shutil.copytree if source.is_dir() else shutil.copy)(source, WORK / name)
os.chdir(WORK)  # the default SQLite stores (.skifer_*.db) and spark-warehouse land here

from skifer import SkiferEngine, cli  # noqa: E402
from skifer.agentic.candidate_agent import MetricCandidateAgent  # noqa: E402
from skifer.agentic.hub import AgenticHub  # noqa: E402
from skifer.candidates import default_candidate_store  # noqa: E402
from skifer.knowledge import KnowledgeIndex, default_knowledge_store  # noqa: E402
from skifer.observability.certification_store import default_certification_store  # noqa: E402
from skifer.observability.monitor import DataMonitor  # noqa: E402
from skifer.observability.tracing import TraceAttributePolicy  # noqa: E402
from skifer.semantic.llm_provider import LLMProvider  # noqa: E402
from skifer.semantic.access_policy import ConsumerContext  # noqa: E402
from skifer.semantic.semantic import SemanticEngine  # noqa: E402
from skifer.serving._response_serializer import hub_response_to_text  # noqa: E402

CONFIG = str(WORK / "config.yaml")
MODELS = str(WORK / "semantic_models")


class ScriptedLLM(LLMProvider):
    """Offline LLM: answers each prompt kind from the script below, never from the network."""

    provider_name = "offline_script"

    def complete(self, system_prompt, user_message, **kwargs):
        question = user_message.split("\n", 1)[0].removeprefix("Question: ").lower()
        if system_prompt.startswith("You answer a business question that no certified"):
            if "panier" not in question:
                return "{}"  # nothing sensible to propose: the agent refuses after its retries
            # The fallback agent: a metric in the declarative grammar, never executed SQL.
            region = question.rsplit(" ", 1)[-1].upper()
            return json.dumps({
                "metric_name": "average_basket",
                "metric_description": "Average order amount per sales channel.",
                "schema_dict": {
                    "tables": [{"name": "gold.fact_orders", "alias": "o",
                                "filter": [f"region:equals:{region}"]}],
                    "aggregate": {"group_by": ["channel"],
                                  "measures": [["amount", "average_basket", "avg"]]},
                },
            })
        if "sélection de modèles" in system_prompt:  # GenBI step A: a catalogue model or none
            selected = None
            if "customer" in question:
                selected = "sales.customers"
            elif re.search(r"revenue|chiffre d'affaires|\bca\b", question):
                selected = "sales.orders"
            return json.dumps({"selected": selected, "candidates": [], "reason": "scripted"})
        if "traduction de questions" in system_prompt:  # GenBI step B: names only
            customers = "sales.customers" in system_prompt
            return json.dumps({
                "model_name": "sales.customers" if customers else "sales.orders",
                "metrics": ["customer_count" if customers else "total_revenue"],
                "group_by": [("segment" if customers else "region")] if " par " in question else [],
            })
        return "genbi"  # hub intent classification

    def complete_with_history(self, system_prompt, history, **kwargs):
        return self.complete(system_prompt, history[-1]["content"], **kwargs)


def skifer(command, **options):
    """Run a `skifer` CLI command in this process; returns (exit code, stdout)."""
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        code = command(Namespace(**options))
    return code, out.getvalue()


# [01] Build the two certified source tables.
engine = SkiferEngine(config_path=CONFIG)
engine.schema_suffix = ""
engine.monitor = DataMonitor(engine.backend)
engine.certification_store = default_certification_store(engine)
for table in ("orders", "customers"):
    engine.run_from_yaml(str(WORK / "schemas" / f"gold_{table}.yaml"), "gold",
                         {"orders": "fact_orders", "customers": "dim_customers"}[table],
                         params={**engine.default_params, "example_dir": WORK.as_posix()})
print("[01] gold.fact_orders and gold.dim_customers published and certified")

# [02] Index the pipelines and the semantic models into the knowledge graph.
skifer(cli.run_index_command, paths=[str(p) for p in sorted((WORK / "schemas").glob("*.yaml"))],
       db=".skifer_metadata.db", target_fqn=None, strict=False, rules=None)
code, out = skifer(cli.run_knowledge_command, knowledge_command="sync", schema=None, rules=None,
                   config=CONFIG, models_dir=MODELS, metadata_db=".skifer_metadata.db",
                   db=".skifer_knowledge.db", dry_run=False, json=False, embed=None,
                   model_cache=None)
print(f"[02] knowledge sync: {out.strip()}")

# [03] One covered question, one outside the semantic layer.
semantic = SemanticEngine(engine, models_dir=MODELS,
                          certification_store=engine.certification_store)
llm = ScriptedLLM()
candidates = default_candidate_store(engine)
knowledge = default_knowledge_store(engine)
# Requesters are counted through an HMAC pseudonym, never stored in clear. This key exists for
# the example only; a real one comes from SKIFER_TRACING_HMAC_SECRET, never from a file.
policy = TraceAttributePolicy(user_identity="hmac", hmac_secret="example-only-not-a-secret")
agent = MetricCandidateAgent(semantic, llm, KnowledgeIndex.from_store(knowledge), candidates,
                             policy=policy)
hub = AgenticHub(semantic_engine=semantic, llm_provider=llm, candidate_agent=agent)

covered = hub.ask("chiffre d'affaires par région",
                  consumer_context=ConsumerContext(consumer_id="alice", consumer_class="dashboard"))
print(f"[03] covered question -> {covered.mode} (certified semantic layer)")
uncovered = hub.ask("panier moyen par canal pour la région EMEA",
                    consumer_context=ConsumerContext(consumer_id="alice",
                                                     consumer_class="dashboard"))
print(f"[03] uncovered question -> {uncovered.mode}")
print("     " + hub_response_to_text(uncovered).splitlines()[0])

# [04] The same need, other filter values and other people: one candidate, more evidence.
for requester, region in (("bob", "APAC"), ("carol", "AMER"), ("bob", "EMEA")):
    response = hub.ask(f"panier moyen par canal pour la région {region}",
                       consumer_context=ConsumerContext(consumer_id=requester,
                                                        consumer_class="dashboard"))
    assert response.candidate_id == uncovered.candidate_id
candidate_id = uncovered.candidate_id
print(f"[04] 4 requests from 3 requesters, one candidate: {candidate_id[:24]}…")

# [05] Validation: coherence, frequency, stability (thresholds lowered in config.yaml).
code, out = skifer(lambda a: cli.run_candidates_command(a, store=candidates, semantic=semantic,
                                                        knowledge_store=knowledge),
                   candidates_command="validate", candidate=None, dry_run=False, json=True,
                   config=CONFIG, models_dir=MODELS)
print(f"[05] candidates validate -> exit {code}, state {candidates.get_candidate(candidate_id).state}")

# [06] Level 2: drafts for a pull request, written in the temporary copy only.
code, out = skifer(cli.run_candidates_arbitration, candidates_command="promote",
                   candidate_id=candidate_id, reason="checked with finance",
                   pipeline_output=None, config=CONFIG, models_dir=MODELS)
drafts = sorted(str(p.relative_to(WORK)) for p in WORK.rglob("*")
                if p.is_file() and ".drafts" in p.parts)
print(f"[06] candidates promote -> exit {code}, drafts for a pull request:")
for draft in drafts:
    print(f"     {draft}")

# [07] Observation and measurement: drift, reference set, evaluation.
code, out = skifer(cli.run_candidates_drift, since=None, until=None, json=True, environment=None,
                   usage_store=".skifer_adaptive.db", evaluations_dir=".skifer_evaluations",
                   config=CONFIG, models_dir=MODELS)
drift = {signal["name"]: signal["status"] for signal in json.loads(out)["signals"]}
print(f"[07] candidates drift -> exit {code}, observation only: {drift}")
code, out = skifer(lambda a: cli.run_candidates_reference(a, store=candidates, semantic=semantic),
                   reference_command="bootstrap", config=CONFIG, models_dir=MODELS)
print(f"[07] reference bootstrap -> exit {code}: {out.strip()}")
code, out = skifer(lambda a: cli.run_candidates_evaluate(
    a, hub=hub, index=KnowledgeIndex.from_store(knowledge), store=candidates),
    json=True, mlflow=False, evaluations_dir=".skifer_evaluations")
metrics = json.loads(out)["metrics"]
print(f"[07] candidates evaluate -> exit {code}, A.recall@5 = {metrics['A.recall@5']}, "
      f"A.routing_accuracy = {metrics['A.routing_accuracy']}")

# [08] The candidate's certification history, oldest first.
code, out = skifer(cli.run_candidates_arbitration, candidates_command="history",
                   candidate_id=candidate_id, json=False, config=CONFIG, models_dir=MODELS)
print("[08] candidates history")
print(out.rstrip())
