"""Lint business rules statically, then through the engine report.

    python examples/19_rule_optimizer/run.py
"""

from pyspark.sql import functions as F

from skifer import RuleRegistry, SkiferEngine
from skifer.core.rule_analyzer import RuleAnalyzer


@RuleRegistry.register_rule(kind="transform")
def make_priority(df):
    return df.withColumn("is_priority", F.col("amount") >= 1000)


@RuleRegistry.register_rule(kind="transform")
def rewrite_priority(df):
    return df.withColumn("is_priority", F.col("amount") >= 2000)


@RuleRegistry.register_rule(kind="transform")
def make_discount(df):
    return df.withColumn("discount", F.col("amount") * 0.05)


# A builtin has no inspectable source: both paths report the gap instead of guessing.
RuleRegistry.register_rule(name="source_unavailable", kind="transform")(len)
RULES = ["make_priority", "rewrite_priority", "make_discount", "source_unavailable"]


def print_warnings(warnings):
    for warning in warnings:
        print(f"  {warning.code}: rules={warning.rules}, column={warning.column or '<none>'}")


analyzer = RuleAnalyzer()
one = analyzer.analyze_rule(make_priority, name="make_priority")
print("Direct RuleAnalyzer path (Spark-free; suitable for CI):")
print(f"  make_priority: source_available={one.source_available}, reads={one.input_columns}, writes={one.output_columns}")

profiles = analyzer.analyze_rules(RULES)
print_warnings(analyzer.detect_warnings(profiles, shared_read_threshold=2, loc_threshold=30, withcolumn_threshold=5))
unavailable = next(p for p in profiles if p.name == "source_unavailable")
print(f"  source_unavailable: source_available={unavailable.source_available}; skipped, not guessed")

print("\nEngine path (documented API; constructor starts local Spark):")
engine = SkiferEngine(force_env="LOCAL")
engine_profiles, engine_warnings = engine.explain_rules({"business_rules": RULES})
print(f"Engine returned: profiles={len(engine_profiles)}, warnings={len(engine_warnings)}")
print_warnings(engine_warnings)
