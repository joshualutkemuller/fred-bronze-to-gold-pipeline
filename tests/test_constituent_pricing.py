import datetime as dt

import pytest

from fred_pipeline.constituent_pricing import (
    is_tiingo_priceable_ticker,
    plan_tiingo_constituent_pricing,
    specs_for_tiingo_candidates,
)


def test_plan_tiingo_constituent_pricing_selects_missing_and_stale_by_weight():
    constituents = [
        {"ticker": "NVDA", "weight_rank": 1, "weight_pct": 7.7},
        {"ticker": "AAPL", "weight_rank": 2, "weight_pct": 7.5},
        {"ticker": "ESU6", "weight_rank": 3, "weight_pct": 0.1},
        {"ticker": "MSFT", "weight_rank": 4, "weight_pct": 4.6},
        {"ticker": "AMZN", "weight_rank": 5, "weight_pct": 3.8},
    ]
    latest_prices = [
        {"ticker": "AAPL", "latest_price_date": "2026-07-17"},
        {"ticker": "MSFT", "latest_price_date": "2026-07-01"},
    ]

    plan = plan_tiingo_constituent_pricing(
        constituents,
        latest_prices,
        as_of_date=dt.date(2026, 7, 18),
        stale_days=7,
        limit=2,
    )

    assert plan.total_constituents == 5
    assert plan.already_fresh == 1
    assert plan.skipped_unpriceable == ("ESU6",)
    assert [(c.ticker, c.reason) for c in plan.candidates] == [
        ("NVDA", "missing"),
        ("MSFT", "stale"),
        ("AMZN", "missing"),
    ]
    assert [c.ticker for c in plan.batch] == ["NVDA", "MSFT"]


def test_specs_for_tiingo_candidates_builds_active_tiingo_specs():
    plan = plan_tiingo_constituent_pricing(
        [{"ticker": "NVDA", "weight_rank": 1}],
        [],
        as_of_date=dt.date(2026, 7, 18),
    )

    specs = specs_for_tiingo_candidates(plan.batch)

    assert len(specs) == 1
    assert specs[0].series_id == "NVDA"
    assert specs[0].source == "tiingo"
    assert specs[0].active is True
    assert specs[0].vintage_enabled is False


@pytest.mark.parametrize(
    ("ticker", "expected"),
    [
        ("AAPL", True),
        ("BRK.B", True),
        ("BRK-B", True),
        ("ESU6", False),
        ("", False),
    ],
)
def test_is_tiingo_priceable_ticker(ticker, expected):
    assert is_tiingo_priceable_ticker(ticker) is expected
