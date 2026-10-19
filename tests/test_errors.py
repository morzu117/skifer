"""Tests for sanitize_exception_message — no Spark, no LLM required."""
from __future__ import annotations

from skifer.agentic._errors import sanitize_exception_message


# A message shaped like a real Spark Connect AnalysisException, as reported in
# the "Skifer 3.1.1 : une erreur Spark brute fuite à l'utilisateur" bug report.
_SPARK_CONNECT_MESSAGE = (
    '[DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES] Cannot resolve "(is_vip = true)" '
    "due to data type mismatch: the left and right operands of the binary operator "
    "have incompatible types (\"INT\" and \"BOOLEAN\").\n"
    "JVM stacktrace:\n"
    "org.apache.spark.sql.AnalysisException: [DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES] "
    'Cannot resolve "(is_vip = true)"\n'
    "\tat org.apache.spark.sql.catalyst.analysis.CheckAnalysis$.$anonfun$checkAnalysis0$1(CheckAnalysis.scala:271)\n"
    "\tat org.apache.spark.sql.catalyst.analysis.CheckAnalysis.checkAnalysis(CheckAnalysis.scala:170)\n"
    "\tat com.databricks.sql.connect.service.ExecuteThreadRunner.run(ExecuteThreadRunner.scala:120)\n"
    "\tat java.base/java.lang.Thread.run(Thread.java:1583)\n"
)


def test_strips_jvm_stacktrace_keeps_readable_prefix():
    cleaned = sanitize_exception_message(_SPARK_CONNECT_MESSAGE)

    assert "JVM stacktrace:" not in cleaned
    assert "\tat " not in cleaned
    assert "at org.apache.spark" not in cleaned
    assert "at com.databricks" not in cleaned
    assert cleaned.startswith("[DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES]")
    assert 'Cannot resolve "(is_vip = true)"' in cleaned


def test_clean_short_message_is_left_unchanged():
    message = "Table or view not found: gold.fact_orders"
    assert sanitize_exception_message(message) == message


def test_strips_stack_frames_without_jvm_stacktrace_marker():
    # Py4J can surface frame lines without the "JVM stacktrace:" marker.
    message = (
        "An error occurred while calling o123.sql.\n"
        "\tat py4j.Gateway.invoke(Gateway.java:282)\n"
        "\tat py4j.commands.AbstractCommand.invoke(AbstractCommand.java:132)\n"
    )
    cleaned = sanitize_exception_message(message)
    assert cleaned == "An error occurred while calling o123.sql."
    assert "py4j" not in cleaned


def test_caps_message_length():
    message = "x" * 1000
    cleaned = sanitize_exception_message(message, max_length=500)
    assert len(cleaned) == 501  # max_length + the truncation ellipsis
    assert cleaned.endswith("…")
