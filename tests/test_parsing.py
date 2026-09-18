"""Tests for protocol -> Nautilus converters (instruments, prices, bars, quotes).

Every scaling assertion is checked against `tests/fixtures/m2_recorded.json`, recorded from a
real, read-only broker connection. Never against hand-built symbol specs.
"""

from __future__ import annotations

import pytest
from nautilus_trader.model.data import BarType
from nautilus_trader.model.enums import AssetClass
from nautilus_trader.model.instruments import Cfd, CurrencyPair
from nautilus_trader.model.objects import Price, Quantity

from nautilus_ctrader.common import parsing
from nautilus_ctrader.common.errors import CTraderProtocolError
from tests.fixtures import load_recorded

REC = load_recorded()
ASSETS = {a.assetId: a for a in REC["assets"][0].asset}
LIGHT = {s.symbolName: s for s in REC["symbols"][0].symbol}
SPECS = {s.symbolId: s for s in REC["symbol_specs"][0].symbol}


def _inst(name, overrides=None):
    light = LIGHT[name]
    return parsing.instrument_from_symbol(
        SPECS[light.symbolId],
        light,
        ASSETS,
        overrides or {},
        ts_init=0,
    )


def test_eurusd_is_a_currency_pair_in_base_units() -> None:
    i = _inst("EURUSD")
    assert isinstance(i, CurrencyPair)
    assert str(i.id) == "EURUSD.CTRADER"
    assert i.base_currency.code == "EUR" and i.quote_currency.code == "USD"
    assert i.price_precision == 5 and i.size_precision == 0
    assert i.size_increment == Quantity(1000, 0)
    assert i.min_quantity == Quantity(1000, 0)
    assert i.lot_size == Quantity(100_000, 0)
    assert i.info["symbol_id"] == LIGHT["EURUSD"].symbolId


def test_ger40_is_an_eur_quoted_cfd_in_hundredths_of_a_contract() -> None:
    i = _inst("GER40.cash")
    assert isinstance(i, Cfd)
    assert i.quote_currency.code == "EUR" and i.base_currency is None
    assert i.asset_class == AssetClass.ALTERNATIVE
    assert i.price_precision == 2 and i.size_precision == 2
    assert i.size_increment == Quantity(0.01, 2) and i.lot_size == Quantity(1, 2)


def test_asset_class_override_applies_to_cfd() -> None:
    assert _inst("GER40.cash", {"GER40.cash": AssetClass.INDEX}).asset_class == AssetClass.INDEX


def test_xauusd_is_a_commodity_cfd_with_xau_base() -> None:
    i = _inst("XAUUSD")
    assert isinstance(i, Cfd) and i.asset_class == AssetClass.COMMODITY
    assert i.base_currency.code == "XAU" and i.price_precision == 2
    assert i.lot_size == Quantity(100, 0) and i.min_quantity == Quantity(1, 0)


def test_info_round_trips_through_to_dict() -> None:
    i = _inst("GER40.cash")
    assert type(i).from_dict(type(i).to_dict(i)).info == i.info
    for key in (
        "lot_size_cents",
        "step_volume_cents",
        "pip_position",
        "commission_type",
        "swap_long",
        "leverage_id",
        "schedule_time_zone",
        "trading_mode",
    ):
        assert key in i.info, key


def test_price_from_raw_is_exact_or_refuses() -> None:
    assert parsing.price_from_raw(114_830, 5) == Price.from_str("1.14830")
    assert parsing.price_from_raw(2_528_164_000, 2) == Price.from_str("25281.64")
    with pytest.raises(CTraderProtocolError):
        parsing.price_from_raw(2_528_164_001, 2)


def test_recorded_h1_trendbar_converts_with_close_timestamp() -> None:
    res = REC["trendbars_h1"][0]
    tb = res.trendbar[-1]
    light = next(s for s in LIGHT.values() if s.symbolId == res.symbolId)
    spec = SPECS[res.symbolId]
    bar_type = BarType.from_str(f"{light.symbolName}.CTRADER-1-HOUR-BID-EXTERNAL")
    bar = parsing.bar_from_trendbar(tb, bar_type, spec.digits, 2, ts_init=0)
    boundary = parsing.bar_boundary_secs(tb.utcTimestampInMinutes, 3600)
    assert bar.ts_event == (boundary + 3600) * 1_000_000_000
    assert bar.low == parsing.price_from_raw(tb.low, spec.digits)
    assert bar.high == parsing.price_from_raw(tb.low + tb.deltaHigh, spec.digits)
    assert bar.low <= bar.open <= bar.high and bar.low <= bar.close <= bar.high


def test_currency_lookup_is_strict_only(monkeypatch) -> None:
    # `Currency` is a Cython extension type: `Currency.from_str` cannot be monkeypatched
    # directly (assigning to it raises TypeError on this build). Route the check through the
    # module-level `parsing.Currency` name instead, which `parsing._currency` calls through.
    calls: list[bool] = []
    from nautilus_trader.model.objects import Currency as RealCurrency

    class _RecordingCurrency:
        @staticmethod
        def from_str(code, strict=False):
            calls.append(strict)
            return RealCurrency.from_str(code, strict=strict)

    monkeypatch.setattr(parsing, "Currency", _RecordingCurrency)
    _inst("GER40.cash")
    assert calls and all(calls)
