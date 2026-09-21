"""Tests for `ProtoOATrendbarPeriod` <-> `BarType` mapping."""

from __future__ import annotations

import pytest
from nautilus_trader.model.data import BarType

from nautilus_ctrader.enums import trendbar_period_for


@pytest.mark.parametrize(
    ("bar_type_str", "period"),
    [
        ("EURUSD.CTRADER-15-MINUTE-BID-EXTERNAL", 7),
        ("EURUSD.CTRADER-1-HOUR-BID-EXTERNAL", 9),
        ("EURUSD.CTRADER-1-DAY-BID-EXTERNAL", 12),
    ],
)
def test_trendbar_period_for_maps_supported_bar_types(bar_type_str: str, period: int) -> None:
    assert trendbar_period_for(BarType.from_str(bar_type_str)) == period


def test_rejects_non_bid_price_type() -> None:
    bar_type = BarType.from_str("EURUSD.CTRADER-1-HOUR-LAST-EXTERNAL")
    with pytest.raises(ValueError, match="BID"):
        trendbar_period_for(bar_type)


def test_rejects_non_external_aggregation_source() -> None:
    bar_type = BarType.from_str("EURUSD.CTRADER-1-HOUR-BID-INTERNAL")
    with pytest.raises(ValueError, match="EXTERNAL"):
        trendbar_period_for(bar_type)


@pytest.mark.parametrize(
    "bar_type_str",
    [
        "EURUSD.CTRADER-1-WEEK-BID-EXTERNAL",
        # 20 divides 60 (so Nautilus's own BarSpecification accepts it), but it is not one of
        # the step/aggregation combinations cTrader's trendbar endpoint serves.
        "EURUSD.CTRADER-20-MINUTE-BID-EXTERNAL",
    ],
)
def test_rejects_unsupported_step_aggregation(bar_type_str: str) -> None:
    bar_type = BarType.from_str(bar_type_str)
    with pytest.raises(ValueError, match="not supported"):
        trendbar_period_for(bar_type)
