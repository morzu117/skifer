"""The Python half of the pipeline: one named business concept."""

from pyspark.sql import functions as F

from skifer import RuleRegistry


@RuleRegistry.register_rule()
def classify_order(df):
    return {
        "order_class": F.when(F.col("amount") >= 500, "priority").otherwise("standard")
    }
