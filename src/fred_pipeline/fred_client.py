"""Back-compat shim.

The FRED client moved to :mod:`fred_pipeline.sources.fred` when the HTTP
transport was generalized into :mod:`fred_pipeline.sources.base` so multiple
sources (FRED, BLS, ...) can share it. Existing imports of
``fred_pipeline.fred_client`` keep working via the re-exports below; new code
should import from :mod:`fred_pipeline.sources.fred`.
"""

from __future__ import annotations

from fred_pipeline.sources.base import RETRYABLE_STATUS, RateLimiter
from fred_pipeline.sources.fred import (
    VINTAGE_FALLBACK_YEARS,
    FredAPIError,
    FredClient,
    _coalesce_observations,
    _is_vintage_cap_error,
)

__all__ = [
    "FredClient",
    "FredAPIError",
    "RateLimiter",
    "RETRYABLE_STATUS",
    "VINTAGE_FALLBACK_YEARS",
    "_coalesce_observations",
    "_is_vintage_cap_error",
]
