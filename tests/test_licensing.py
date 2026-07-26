"""Per-source data-licensing register (governance/licensing.py)."""

from __future__ import annotations

from datetime import date

import pytest

from fred_pipeline.governance.licensing import (
    DataLicensingConfigError,
    SourceLicense,
    check_commercial_use,
    load_data_licensing_config,
)


def _spec(source: str):
    class _Spec:
        def __init__(self, source: str):
            self.source = source
    return _Spec(source)


def test_load_repo_licensing_config_has_every_active_source():
    licensing = load_data_licensing_config("config/data_licensing.yml")
    expected = {
        "fred", "bls", "treasury", "census", "bea", "sec", "eia",
        "worldbank", "tiingo", "stooq", "ishares",
    }
    assert expected <= set(licensing)
    for src, lic in licensing.items():
        assert lic.source == src
        assert lic.last_reviewed_date is not None


def test_load_missing_file_returns_empty_dict(tmp_path):
    assert load_data_licensing_config(str(tmp_path / "nope.yml")) == {}


def test_load_rejects_invalid_license_type(tmp_path):
    bad = tmp_path / "bad.yml"
    bad.write_text(
        "sources:\n  - source: foo\n    license_type: not-a-real-type\n"
    )
    with pytest.raises(DataLicensingConfigError):
        load_data_licensing_config(str(bad))


def test_load_rejects_unknown_field(tmp_path):
    bad = tmp_path / "bad.yml"
    bad.write_text(
        "sources:\n  - source: foo\n    license_type: public-domain\n"
        "    bogus_field: 1\n"
    )
    with pytest.raises(DataLicensingConfigError):
        load_data_licensing_config(str(bad))


def test_load_rejects_duplicate_source(tmp_path):
    bad = tmp_path / "bad.yml"
    bad.write_text(
        "sources:\n"
        "  - source: foo\n    license_type: public-domain\n"
        "  - source: foo\n    license_type: public-domain\n"
    )
    with pytest.raises(DataLicensingConfigError):
        load_data_licensing_config(str(bad))


def test_check_commercial_use_flags_disallowed_source():
    licensing = {
        "fred": SourceLicense(
            source="fred", license_type="public-domain",
            redistribution_allowed=True, commercial_use_allowed=True,
            attribution_required=False, terms_url="", last_reviewed_date=None,
        ),
        "tiingo": SourceLicense(
            source="tiingo", license_type="free-tier-personal-use",
            redistribution_allowed=False, commercial_use_allowed=False,
            attribution_required=False, terms_url="", last_reviewed_date=None,
        ),
    }
    specs = [_spec("fred"), _spec("fred"), _spec("tiingo")]
    violations = check_commercial_use(specs, licensing)

    assert len(violations) == 1
    assert violations[0].source == "tiingo"
    assert violations[0].series_count == 1
    assert "does not allow commercial use" in violations[0].reason


def test_check_commercial_use_flags_unregistered_source():
    licensing: dict = {}
    specs = [_spec("newsource")]
    violations = check_commercial_use(specs, licensing)

    assert len(violations) == 1
    assert violations[0].source == "newsource"
    assert "unreviewed" in violations[0].reason


def test_check_commercial_use_no_violations_when_all_cleared():
    licensing = {
        "fred": SourceLicense(
            source="fred", license_type="public-domain",
            redistribution_allowed=True, commercial_use_allowed=True,
            attribution_required=False, terms_url="", last_reviewed_date=None,
        ),
    }
    assert check_commercial_use([_spec("fred")], licensing) == []


def test_source_license_rejects_missing_source():
    with pytest.raises(DataLicensingConfigError):
        SourceLicense(
            source="", license_type="public-domain",
            redistribution_allowed=True, commercial_use_allowed=True,
            attribution_required=False, terms_url="",
            last_reviewed_date=date(2026, 1, 1),
        )
