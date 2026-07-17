"""Decorator for timing a call and logging its elapsed wall-clock time.

Avoids hand-rolled ``time.perf_counter()`` bookkeeping at each call site.
Logs through the standard :mod:`logging` module, so output is captured by
whatever handler/level ``cli.py``'s ``logging.basicConfig`` (or the
Databricks job's logging setup) already configures.
"""

from __future__ import annotations

import functools
import logging
import time
from typing import Any, Callable, Optional, TypeVar

F = TypeVar("F", bound=Callable[..., Any])


def timed(name: Optional[str] = None, *, logger: Optional[logging.Logger] = None) -> Callable[[F], F]:
    """Log the wrapped callable's elapsed wall-clock time on return or raise.

    ``name`` overrides the logged label (defaults to the function's
    ``__qualname__``, e.g. ``FredPipeline.run``). ``logger`` overrides the
    logger used (defaults to one named after the wrapped function's module).
    """

    def decorator(func: F) -> F:
        label = name or func.__qualname__
        log = logger or logging.getLogger(func.__module__)

        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            start = time.perf_counter()
            try:
                result = func(*args, **kwargs)
            except Exception:
                log.exception("%s failed after %.2fs", label, time.perf_counter() - start)
                raise
            log.info("%s finished in %.2fs", label, time.perf_counter() - start)
            return result

        return wrapper  # type: ignore[return-value]

    return decorator
