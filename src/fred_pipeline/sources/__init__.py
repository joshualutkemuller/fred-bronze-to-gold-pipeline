"""Pluggable data-source clients.

Every source (FRED, BLS, EIA, ...) speaks the same
:class:`~fred_pipeline.sources.base.SourceClient` contract to the pipeline:
fetch a raw payload for a series, and normalize it into the canonical silver
row schema. The shared HTTP transport — session management, a rate limiter,
retry/backoff — lives once in :mod:`fred_pipeline.sources.base`; each source
subclass supplies only what is genuinely source-specific (auth params, error
shape, response layout).
"""

from fred_pipeline.sources.base import (
    HTTPSource,
    RateLimiter,
    SourceClient,
    SourceError,
)

__all__ = ["HTTPSource", "RateLimiter", "SourceClient", "SourceError"]
