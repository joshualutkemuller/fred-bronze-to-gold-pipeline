"""Runtime configuration for the FRED pipeline.

Configuration is environment-aware (dev / test / prod) so the same code can be
promoted across Unity Catalog catalogs without edits. Settings can come from a
YAML file (``config/config.yaml`` by default), environment variables, explicit
arguments, or a Databricks secret scope.

Precedence (highest wins): explicit argument > environment variable >
config file > built-in default. The FRED API key additionally falls back to a
Databricks secret scope when it is not set by any of the above. Nothing here
hardcodes a secret, and the real ``config/config.yaml`` is git-ignored.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

try:
    import yaml
except ImportError:  # pragma: no cover - PyYAML is a core dependency
    yaml = None  # type: ignore


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

# Default location for the (git-ignored) config file, relative to the CWD.
DEFAULT_CONFIG_PATH = "config/config.yaml"

# Config fields that may be set from file / env / overrides (not `environment`).
_SETTING_FIELDS = (
    "fred_api_key",
    "fred_base_url",
    "secret_scope",
    "secret_key",
    "request_timeout_seconds",
    "max_retries",
    "rate_limit_per_minute",
    "raw_volume_path",
    "restate_last_n",
)

# Environment-variable name for each setting (12-factor style overrides).
_ENV_OVERRIDES = {
    "fred_api_key": "FRED_API_KEY",
    "fred_base_url": "FRED_BASE_URL",
    "secret_scope": "FRED_SECRET_SCOPE",
    "secret_key": "FRED_SECRET_KEY",
    "request_timeout_seconds": "FRED_REQUEST_TIMEOUT_SECONDS",
    "max_retries": "FRED_MAX_RETRIES",
    "rate_limit_per_minute": "FRED_RATE_LIMIT_PER_MINUTE",
    "raw_volume_path": "FRED_RAW_VOLUME_PATH",
    "restate_last_n": "FRED_RESTATE_LAST_N",
}

_INT_FIELDS = {
    "request_timeout_seconds",
    "max_retries",
    "rate_limit_per_minute",
    "restate_last_n",
}


def load_config_file(
    path: Optional[str] = None, environment: str = "dev"
) -> dict[str, Any]:
    """Load settings from a YAML config file, merged for the given environment.

    The file may be either a flat mapping of settings, or structured with a
    ``default:`` block plus per-environment overrides under ``environments:``::

        default:
          rate_limit_per_minute: 120
          secret_scope: fred
        environments:
          prod:
            rate_limit_per_minute: 60

    Resolution of the path: explicit ``path`` argument, else ``FRED_CONFIG_FILE``
    env var, else ``config/config.yaml``. A missing file yields ``{}`` (so the
    file is entirely optional). Only recognized setting keys are returned.
    """
    resolved = path or os.environ.get("FRED_CONFIG_FILE") or DEFAULT_CONFIG_PATH
    if not resolved or not os.path.isfile(resolved):
        return {}
    if yaml is None:  # pragma: no cover
        raise RuntimeError("PyYAML is required to read config files")
    with open(resolved, "r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Config file {resolved} must be a mapping at the top level")

    settings: dict[str, Any] = {}
    if "default" in data or "environments" in data:
        settings.update(data.get("default") or {})
        env_section = (data.get("environments") or {}).get(environment) or {}
        settings.update(env_section)
    else:
        settings.update(data)

    # Keep only recognized settings; ignore comments/unknown keys quietly.
    return {k: v for k, v in settings.items() if k in _SETTING_FIELDS}


def _env_var_settings() -> dict[str, Any]:
    out: dict[str, Any] = {}
    for field_name, env_name in _ENV_OVERRIDES.items():
        val = os.environ.get(env_name)
        if val is not None and val != "":
            out[field_name] = val
    return out


def _coerce_settings(settings: dict[str, Any]) -> dict[str, Any]:
    out = dict(settings)
    for name in _INT_FIELDS:
        if out.get(name) is not None:
            out[name] = int(out[name])
    return out


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
    restate_last_n:
        Default number of most-recent observations to re-pull ("restate") on an
        incremental load, so recent revisions are captured. A series with no data
        yet is always loaded in full. Overridable per series via the manifest's
        ``restate_records``.
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
    restate_last_n: int = 90

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
        config_file: Optional[str] = None,
        **overrides,
    ) -> "PipelineConfig":
        """Build a config from file + env + args + Databricks secrets.

        Precedence for every setting (highest wins):
          1. explicit keyword argument (``fred_api_key=`` or ``**overrides``)
          2. environment variable (see ``_ENV_OVERRIDES``)
          3. config file (``config_file`` / ``FRED_CONFIG_FILE`` /
             ``config/config.yaml``), merged for this environment
          4. built-in dataclass default

        The API key additionally falls back to the Databricks secret scope
        (whose name/key can themselves come from any layer above) when it is not
        provided by 1–3.
        """
        if isinstance(environment, str):
            environment = Environment(environment)

        # Layer the sources: file < env < explicit overrides.
        settings: dict[str, Any] = {}
        settings.update(load_config_file(config_file, environment.value))
        settings.update(_env_var_settings())
        settings.update({k: v for k, v in overrides.items() if v is not None})
        if fred_api_key is not None:
            settings["fred_api_key"] = fred_api_key
        settings = _coerce_settings(settings)

        # Resolve the API key, falling back to the Databricks secret scope.
        scope = settings.get("secret_scope", "fred")
        secret_key = settings.get("secret_key", "api_key")
        key = settings.get("fred_api_key")
        if not key and dbutils is not None:
            try:
                key = dbutils.secrets.get(scope=scope, key=secret_key)
            except Exception:  # pragma: no cover - depends on Databricks runtime
                key = None
        settings["fred_api_key"] = key or ""

        # Only pass recognized fields to the (frozen) dataclass.
        kwargs = {k: v for k, v in settings.items() if k in _SETTING_FIELDS}
        return cls(environment=environment, **kwargs)
