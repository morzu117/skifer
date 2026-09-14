"""Warning helpers for best-effort paths that must never block their caller."""

import warnings


def warn_best_effort(
    message: str,
    category: type[Warning] = RuntimeWarning,
    *,
    stacklevel: int = 1,
) -> None:
    """Emit a warning without raising, even when warnings are treated as errors."""
    try:
        warnings.warn(message, category, stacklevel=stacklevel + 1)
    except Exception:
        pass
