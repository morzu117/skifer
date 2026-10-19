"""
Sanitize an exception's text before it reaches an ``AgentResponse``/``QualityResponse``
``error`` field — and, from there, a chat surface (Playground, supervisor).

A Spark Connect ``AnalysisException`` embeds its full JVM stack trace in ``str(exc)``,
after a ``JVM stacktrace:`` marker: thousands of characters of ``at org.apache.spark...``
that are noise to a chat user and belong in the logs, not the response. The readable
prefix — error code, short message — is kept.
"""

from __future__ import annotations

import re

_JVM_STACKTRACE_MARKER = "JVM stacktrace:"
_FRAME_LINE_RE = re.compile(r"^\s*at [\w.$<>]+\(")
_DEFAULT_MAX_LENGTH = 500


def sanitize_exception_message(message: str, *, max_length: int = _DEFAULT_MAX_LENGTH) -> str:
    """Return ``message`` with any JVM/Py4J stack trace removed and length-capped.

    Only the textual message is affected — ``exc_info=True`` logging keeps the full
    traceback independently, so no detail is lost, just kept out of the user-facing text.
    """
    cut_at = message.find(_JVM_STACKTRACE_MARKER)
    if cut_at != -1:
        message = message[:cut_at]
    else:
        kept = []
        for line in message.splitlines():
            if _FRAME_LINE_RE.match(line):
                break
            kept.append(line)
        message = "\n".join(kept)
    message = message.strip()
    if len(message) > max_length:
        message = message[:max_length].rstrip() + "…"
    return message
