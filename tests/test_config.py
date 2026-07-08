import textwrap

import pytest

from fred_pipeline.config import Environment, PipelineConfig, load_config_file

# Env vars that could leak in from the surrounding shell and skew precedence tests.
_ENV_VARS = [
    "FRED_API_KEY", "FRED_BASE_URL", "FRED_SECRET_SCOPE", "FRED_SECRET_KEY",
    "FRED_REQUEST_TIMEOUT_SECONDS", "FRED_MAX_RETRIES",
    "FRED_RATE_LIMIT_PER_MINUTE", "FRED_RAW_VOLUME_PATH", "FRED_CONFIG_FILE",
]


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for var in _ENV_VARS:
        monkeypatch.delenv(var, raising=False)


def _write(tmp_path, text):
    p = tmp_path / "config.yaml"
    p.write_text(textwrap.dedent(text))
    return str(p)


def test_missing_file_returns_empty(tmp_path):
    assert load_config_file(str(tmp_path / "nope.yaml")) == {}


def test_flat_layout(tmp_path):
    path = _write(tmp_path, """
        fred_api_key: from-file
        rate_limit_per_minute: 42
    """)
    settings = load_config_file(path)
    assert settings["fred_api_key"] == "from-file"
    assert settings["rate_limit_per_minute"] == 42


def test_nested_layout_merges_environment(tmp_path):
    path = _write(tmp_path, """
        default:
          rate_limit_per_minute: 120
          secret_scope: fred
        environments:
          prod:
            rate_limit_per_minute: 60
    """)
    dev = load_config_file(path, "dev")
    prod = load_config_file(path, "prod")
    assert dev["rate_limit_per_minute"] == 120
    assert prod["rate_limit_per_minute"] == 60      # env override wins
    assert prod["secret_scope"] == "fred"           # default carried through


def test_unknown_keys_ignored(tmp_path):
    path = _write(tmp_path, """
        fred_api_key: k
        not_a_real_setting: 1
    """)
    settings = load_config_file(path)
    assert "not_a_real_setting" not in settings


def test_resolve_uses_config_file(tmp_path):
    path = _write(tmp_path, """
        fred_api_key: file-key
        rate_limit_per_minute: 30
        max_retries: 9
    """)
    cfg = PipelineConfig.resolve(environment="dev", config_file=path)
    assert cfg.fred_api_key == "file-key"
    assert cfg.rate_limit_per_minute == 30
    assert cfg.max_retries == 9


def test_env_var_overrides_file(tmp_path, monkeypatch):
    path = _write(tmp_path, "fred_api_key: file-key\nrate_limit_per_minute: 30\n")
    monkeypatch.setenv("FRED_API_KEY", "env-key")
    monkeypatch.setenv("FRED_RATE_LIMIT_PER_MINUTE", "77")
    cfg = PipelineConfig.resolve(environment="dev", config_file=path)
    assert cfg.fred_api_key == "env-key"
    assert cfg.rate_limit_per_minute == 77  # coerced to int


def test_explicit_arg_overrides_everything(tmp_path, monkeypatch):
    path = _write(tmp_path, "fred_api_key: file-key\n")
    monkeypatch.setenv("FRED_API_KEY", "env-key")
    cfg = PipelineConfig.resolve(
        environment="dev", fred_api_key="arg-key", config_file=path
    )
    assert cfg.fred_api_key == "arg-key"


def test_fred_config_file_env_var(tmp_path, monkeypatch):
    path = _write(tmp_path, "fred_api_key: via-env-path\n")
    monkeypatch.setenv("FRED_CONFIG_FILE", path)
    cfg = PipelineConfig.resolve(environment="dev")
    assert cfg.fred_api_key == "via-env-path"


def test_secret_scope_fallback_when_no_key(tmp_path):
    path = _write(tmp_path, "secret_scope: myscope\nsecret_key: mykey\n")

    class FakeSecrets:
        def get(self, scope, key):
            assert scope == "myscope" and key == "mykey"
            return "secret-key"

    class FakeDbutils:
        secrets = FakeSecrets()

    cfg = PipelineConfig.resolve(
        environment="dev", config_file=path, dbutils=FakeDbutils()
    )
    assert cfg.fred_api_key == "secret-key"


def test_defaults_when_no_file_no_env():
    cfg = PipelineConfig.resolve(environment="prod", config_file="does-not-exist.yaml")
    assert cfg.fred_api_key == ""
    assert cfg.rate_limit_per_minute == 120
    assert cfg.restate_last_n == 90
    assert cfg.catalog == "macro_prod"


def test_restate_last_n_from_env(monkeypatch):
    monkeypatch.setenv("FRED_RESTATE_LAST_N", "15")
    cfg = PipelineConfig.resolve(environment="dev", config_file="nope.yaml")
    assert cfg.restate_last_n == 15  # coerced to int


def test_shipped_example_config_is_valid():
    # The committed template must parse and only contain known settings.
    settings = load_config_file("config/config.example.yaml", "prod")
    assert "rate_limit_per_minute" in settings
