"""Compare the Spark-free rule linter with the engine convenience API."""

from pyspark.sql import functions as F

from skifer import RuleRegistry, SkiferEngine
from skifer.core.rule_analyzer import RuleAnalyzer


@RuleRegistry.register_rule(name="make_priority", kind="transform")
def make_priority(df):
    return df.withColumn("is_priority", F.col("amount") >= 1000)


@RuleRegistry.register_rule(name="rewrite_priority", kind="transform")
def rewrite_priority(df):
    return df.withColumn("is_priority", F.col("amount") >= 2000)


@RuleRegistry.register_rule(name="make_discount", kind="transform")
def make_discount(df):
    return df.withColumn("discount", F.col("amount") * 0.05)


# ``inspect.getsource(len)`` is unavailable. Registering it lets both analyzer
# paths demonstrate that they report the gap instead of guessing.
RuleRegistry.register_rule(name="source_unavailable", kind="transform")(len)

RULE_NAMES = [
    "make_priority",
    "rewrite_priority",
    "make_discount",
    "source_unavailable",
]


def print_warnings(warnings) -> None:
    for warning in warnings:
        column = warning.column if warning.column is not None else "<none>"
        print(
            f"  {warning.code}: rules={warning.rules}, column={column}"
        )


def main() -> None:
    analyzer = RuleAnalyzer()
    one = analyzer.analyze_rule(make_priority, name="make_priority")
    print("Direct RuleAnalyzer path (Spark-free; suitable for CI):")
    print(
        "  make_priority: "
        f"source_available={one.source_available}, "
        f"reads={one.input_columns}, writes={one.output_columns}"
    )

    profiles = analyzer.analyze_rules(RULE_NAMES)
    warnings = analyzer.detect_warnings(
        profiles,
        shared_read_threshold=2,
        loc_threshold=30,
        withcolumn_threshold=5,
    )
    print_warnings(warnings)
    unavailable = next(p for p in profiles if p.name == "source_unavailable")
    print(
        "  source_unavailable: "
        f"source_available={unavailable.source_available}; skipped, not guessed"
    )

    print("\nEngine path (documented API; constructor starts local Spark):")
    engine = SkiferEngine(force_env="LOCAL")
    try:
        engine_profiles, engine_warnings = engine.explain_rules(
            {"business_rules": RULE_NAMES}
        )
        print(
            "Engine returned: "
            f"profiles={len(engine_profiles)}, warnings={len(engine_warnings)}"
        )
        print_warnings(engine_warnings)
    finally:
        engine.spark.stop()


if __name__ == "__main__":
    main()
