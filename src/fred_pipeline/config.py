"""Runtime configuration for the FRED pipeline.

Configuration is environment-aware (dev / test / prod) so the same code can be
promoted across Unity Catalog catalogs without edits. Nothing here hardcodes a
secret: the FRED API key is resolved from (in priority order) an explicit
argument, a Databricks secret scope, or an environment variable.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class Environment(str, Enum):
    """Deployment environment. Maps 1:1 to a Unity Catalog catalog."""

    DEV = "dev"
    TEST = "test"
    PROD = "prod"

    @property
    def catalog(self) -> str:
        return f"macro_{self.value}"


# Schema names are stable across environments; only the catalog changes.
SCHEMAS = ("meta", "audit", "bronze", "silver", "gold", "sandbox")


@dataclass(frozen=True)
class PipelineConfig:
    """Resolved configuration for a single pipeline invocation.

    Attributes
    ----------
    environment:
        Which catalog to target (dev/test/prod).
    fred_api_key:
        Resolved FRED API key. Never logged.
    fred_base_url:
        Base URL for the FRED REST API.
    secret_scope / secret_key:
        Databricks secret scope + key used to resolve the API key when one is
        not passed explicitly.
    request_timeout_seconds / max_retries / rate_limit_per_minute:
        HTTP client tuning knobs.
    raw_volume_path:
        Unity Catalog volume path where raw JSON payloads are archived.
    """

    environment: Environment = Environment.DEV
    fred_api_key: str = field(repr=False, default="")
    fred_base_url: str = "https://api.stlouisfed.org/fred"
    secret_scope: str = "fred"
    secret_key: str = "api_key"
    request_timeout_seconds: int = 30
    max_retries: int = 5
    rate_limit_per_minute: int = 120
    raw_volume_path: str = ""

    @property
    def catalog(self) -> str:
        return self.environment.catalog

    def table(self, schema: str, name: str) -> str:
        """Fully-qualified Unity Catalog table name: catalog.schema.name."""
        if schema not in SCHEMAS:
            raise ValueError(f"Unknown schema {schema!r}; expected one of {SCHEMAS}")
        return f"{self.catalog}.{schema}.{name}"

    @property
    def raw_archive_path(self) -> str:
        if self.raw_volume_path:
            return self.raw_volume_path
        return f"/Volumes/{self.catalog}/bronze/raw"

    @classmethod
    def resolve(
        cls,
        environment: Environment | str = Environment.DEV,
        fred_api_key: Optional[str] = None,
        *,
        dbutils=None,
        **overrides,
    ) -> "PipelineConfig":
        """Build a config, resolving the API key from secrets/env if needed.

        Resolution order for the API key:
          1. ``fred_api_key`` argument
          2. Databricks secret scope (``dbutils.secrets.get``)
          3. ``FRED_API_KEY`` environment variable
        """
        if isinstance(environment, str):
            environment = Environment(environment)

        secret_scope = overrides.get("secret_scope", "fred")
        secret_key = overrides.get("secret_key", "api_key")

        key = fred_api_key
        if not key and dbutils is not None:
            try:
                key = dbutils.secrets.get(scope=secret_scope, key=secret_key)
            except Exception:  # pragma: no cover - depends on Databricks runtime
                key = None
        if not key:
            key = os.environ.get("FRED_API_KEY", "")

        return cls(environment=environment, fred_api_key=key or "", **overrides)
