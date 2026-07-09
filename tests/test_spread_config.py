import pytest

from fred_pipeline.spread_config import (
    FALLBACK_SPREADS,
    SpreadConfigError,
    load_spread_defs,
)


def _write(tmp_path, text):
    path = tmp_path / "spreads.yml"
    path.write_text(text)
    return str(path)


def test_missing_file_falls_back_to_hardcoded_defaults(tmp_path):
    missing = str(tmp_path / "does_not_exist.yml")
    assert load_spread_defs(missing) == list(FALLBACK_SPREADS)


def test_loads_valid_spread_and_ratio_defs(tmp_path):
    path = _write(tmp_path, """
spreads:
  - name: T10Y2Y
    long_leg: DGS10
    short_leg: DGS2
    op: spread
  - name: REAL10Y
    long_leg: DGS10
    short_leg: T10YIE
    op: ratio
    description: illustrative only
""")
    defs = load_spread_defs(path)
    assert len(defs) == 2
    assert defs[0].name == "T10Y2Y" and defs[0].op == "spread"
    assert defs[1].name == "REAL10Y" and defs[1].op == "ratio"
    assert defs[1].description == "illustrative only"


def test_op_defaults_to_spread_when_omitted(tmp_path):
    path = _write(tmp_path, """
spreads:
  - name: X
    long_leg: A
    short_leg: B
""")
    defs = load_spread_defs(path)
    assert defs[0].op == "spread"


def test_invalid_op_raises(tmp_path):
    path = _write(tmp_path, """
spreads:
  - name: X
    long_leg: A
    short_leg: B
    op: divide
""")
    with pytest.raises(SpreadConfigError, match="invalid op"):
        load_spread_defs(path)


def test_missing_required_field_raises(tmp_path):
    path = _write(tmp_path, """
spreads:
  - name: X
    long_leg: A
""")
    with pytest.raises(SpreadConfigError, match="missing required field"):
        load_spread_defs(path)


def test_unknown_field_raises(tmp_path):
    path = _write(tmp_path, """
spreads:
  - name: X
    long_leg: A
    short_leg: B
    typo_field: oops
""")
    with pytest.raises(SpreadConfigError, match="unknown field"):
        load_spread_defs(path)


def test_duplicate_name_raises(tmp_path):
    path = _write(tmp_path, """
spreads:
  - name: X
    long_leg: A
    short_leg: B
  - name: X
    long_leg: C
    short_leg: D
""")
    with pytest.raises(SpreadConfigError, match="Duplicate spread name"):
        load_spread_defs(path)


def test_missing_top_level_key_raises(tmp_path):
    path = _write(tmp_path, """
not_spreads:
  - name: X
""")
    with pytest.raises(SpreadConfigError, match="top-level 'spreads' list"):
        load_spread_defs(path)


def test_env_var_override(tmp_path, monkeypatch):
    path = _write(tmp_path, """
spreads:
  - name: ENVTEST
    long_leg: A
    short_leg: B
""")
    monkeypatch.setenv("FRED_SPREADS_FILE", path)
    defs = load_spread_defs()
    assert len(defs) == 1 and defs[0].name == "ENVTEST"
