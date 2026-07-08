"""FRED Bronze-to-Gold ingestion pipeline.

A manifest-driven, auditable pipeline for ingesting FRED (Federal Reserve
Economic Data) series into a Bronze / Silver / Gold medallion architecture on
Databricks + Delta Lake.

The package is deliberately split into a *pure-Python core* (manifest parsing,
FRED response normalization, data-quality rules) that is fully unit-testable
without Spark or network access, and a thin *Spark I/O layer* (bronze/silver/
gold writers) whose PySpark dependency is imported lazily. This keeps the
business logic portable and the tests fast.
"""

from __future__ import annotations

__version__ = "0.1.0"

from fred_pipeline.config import PipelineConfig, Environment
from fred_pipeline.manifest import Manifest, SeriesSpec, load_manifests

__all__ = [
    "__version__",
    "PipelineConfig",
    "Environment",
    "Manifest",
    "SeriesSpec",
    "load_manifests",
]
