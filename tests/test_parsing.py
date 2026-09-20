"""Tests for protocol -> Nautilus converters (instruments, prices, bars, quotes).

Every scaling assertion is checked against `tests/fixtures/m2_recorded.json`, recorded from a
real, read-only broker connection. Never against hand-built symbol specs.
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from nautilus_trader.model.data import BarType
from nautilus_trader.model.enums import AssetClass
from nautilus_trader.model.instruments import Cfd, CurrencyPair
from nautilus_trader.model.objects import Price, Quantity

from nautilus_ctrader.common import parsing
from nautilus_ctrader.common.errors import CTraderProtocolError
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as om
from tests.fixtures import load_recorded

REC = load_recorded()
ASSETS = {a.assetId: a for a in REC["assets"][0].asset}
LIGHT = {s.symbolName: s for s in REC["symbols"][0].symbol}
SPECS = {s.symbolId: s for s in REC["symbol_specs"][0].symbol}

# The flat, snake_case `info` keys `instrument_from_symbol` must always populate.
_INFO_KEYS = (
    "symbol_id",
    "lot_size_cents",
    "min_volume_cents",
    "step_volume_cents",
    "max_volume_cents",
    "pip_position",
    "measurement_units",
    "trading_mode",
    "schedule_time_zone",
    "commission_type",
    "precise_trading_commission_rate",
    "precise_min_commission",
    "swap_long",
    "swap_short",
    "swap_calculation_type",
    "leverage_id",
    "sl_distance",
    "tp_distance",
    "distance_set_in",
)


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


@pytest.mark.parametrize(
    ("base", "quote", "expected"),
    [
        # `FX` is reachable only from here: through `instrument_from_symbol`, two
        # currencies build a `CurrencyPair`, which derives its own asset class.
        ("EUR", "USD", AssetClass.FX),
        ("XAU", "USD", AssetClass.COMMODITY),
        ("BTC", "USD", AssetClass.CRYPTOCURRENCY),
        ("BTC", "ETH", AssetClass.CRYPTOCURRENCY),
        ("USD", "BTC", AssetClass.ALTERNATIVE),
        ("GER40", "EUR", AssetClass.ALTERNATIVE),
    ],
)
def test_asset_class_for_covers_every_branch(base: str, quote: str, expected: AssetClass) -> None:
    assert parsing.asset_class_for(base, quote) == expected


@pytest.mark.parametrize("name", ["EURUSD", "GER40.cash"])
def test_info_has_every_key_and_round_trips_through_to_dict(name: str) -> None:
    i = _inst(name)
    assert set(i.info) == set(_INFO_KEYS)
    assert type(i).from_dict(type(i).to_dict(i)).info == i.info


def test_price_from_raw_is_exact_or_refuses() -> None:
    assert parsing.price_from_raw(114_830, 5) == Price.from_str("1.14830")
    assert parsing.price_from_raw(2_528_164_000, 2) == Price.from_str("25281.64")
    with pytest.raises(CTraderProtocolError):
        parsing.price_from_raw(2_528_164_001, 2)


def test_price_from_raw_accepts_a_negative_on_grid_value() -> None:
    assert parsing.price_from_raw(-114_830, 5) == Price.from_str("-1.14830")


def test_price_from_raw_refuses_more_than_five_digits() -> None:
    with pytest.raises(CTraderProtocolError):
        parsing.price_from_raw(114_830_0, 6)


def test_recorded_h1_trendbar_converts_with_close_timestamp() -> None:
    res = REC["trendbars_h1"][0]
    tb = res.trendbar[-1]
    light = next(s for s in LIGHT.values() if s.symbolId == res.symbolId)
    spec = SPECS[res.symbolId]
    instrument = _inst(light.symbolName)
    bar_type = BarType.from_str(f"{light.symbolName}.CTRADER-1-HOUR-BID-EXTERNAL")
    bar = parsing.bar_from_trendbar(
        tb,
        bar_type,
        spec.digits,
        instrument.size_precision,
        ts_init=0,
    )
    boundary = parsing.bar_boundary_secs(tb.utcTimestampInMinutes, 3600)
    assert bar.ts_event == (boundary + 3600) * 1_000_000_000
    assert bar.open == parsing.price_from_raw(tb.low + tb.deltaOpen, spec.digits)
    assert bar.high == parsing.price_from_raw(tb.low + tb.deltaHigh, spec.digits)
    assert bar.low == parsing.price_from_raw(tb.low, spec.digits)
    assert bar.close == parsing.price_from_raw(tb.low + tb.deltaClose, spec.digits)


def test_bar_from_trendbar_rejects_a_period_that_disagrees_with_the_bar_type() -> None:
    res = REC["trendbars_h1"][0]
    tb = om.ProtoOATrendbar()
    tb.CopyFrom(res.trendbar[-1])
    tb.period = om.ProtoOATrendbarPeriod.Value("M15")
    light = next(s for s in LIGHT.values() if s.symbolId == res.symbolId)
    spec = SPECS[res.symbolId]
    bar_type = BarType.from_str(f"{light.symbolName}.CTRADER-1-HOUR-BID-EXTERNAL")
    with pytest.raises(CTraderProtocolError, match="period"):
        parsing.bar_from_trendbar(tb, bar_type, spec.digits, 2, ts_init=0)


def test_bar_from_trendbar_accepts_a_live_bar_whose_period_matches() -> None:
    # Live spot-event trend bars, unlike historical ones, do set `period`.
    event = next(e for e in REC["spot_events"] if e.trendbar)
    tb = event.trendbar[0]
    light = next(s for s in LIGHT.values() if s.symbolId == event.symbolId)
    spec = SPECS[event.symbolId]
    bar_type = BarType.from_str(f"{light.symbolName}.CTRADER-1-MINUTE-BID-EXTERNAL")
    bar = parsing.bar_from_trendbar(tb, bar_type, spec.digits, 2, ts_init=0)
    assert bar.low <= bar.close <= bar.high


def test_quote_from_prices_uses_recorded_eurusd_spot_prices() -> None:
    i = _inst("EURUSD")
    quote = parsing.quote_from_prices(
        i.id,
        114_811,
        114_813,
        5,
        Decimal(0),
        i.size_precision,
        ts_event=0,
        ts_init=0,
    )
    assert quote.bid_price == Price.from_str("1.14811")
    assert quote.ask_price == Price.from_str("1.14813")
    assert quote.bid_size == Quantity(0, i.size_precision)
    assert quote.ask_size == Quantity(0, i.size_precision)

    synthetic = parsing.quote_from_prices(
        i.id,
        114_811,
        114_813,
        5,
        Decimal(1000),
        i.size_precision,
        ts_event=0,
        ts_init=0,
    )
    assert synthetic.bid_size == Quantity(1000, i.size_precision)
    assert synthetic.ask_size == Quantity(1000, i.size_precision)


def test_instrument_from_symbol_refuses_digits_over_five() -> None:
    light = LIGHT["EURUSD"]
    spec = om.ProtoOASymbol()
    spec.CopyFrom(SPECS[light.symbolId])
    spec.digits = 6
    with pytest.raises(CTraderProtocolError, match="digits"):
        parsing.instrument_from_symbol(spec, light, ASSETS, {}, ts_init=0)


def test_instrument_from_symbol_refuses_a_min_volume_finer_than_the_step() -> None:
    light = LIGHT["XAUUSD"]
    spec = om.ProtoOASymbol()
    spec.CopyFrom(SPECS[light.symbolId])
    spec.minVolume = 1  # stepVolume is 100 cents; 1 cent cannot round-trip at that precision.
    with pytest.raises(CTraderProtocolError) as exc_info:
        parsing.instrument_from_symbol(spec, light, ASSETS, {}, ts_init=0)
    message = str(exc_info.value)
    assert "XAUUSD" in message
    assert "minVolume" in message


def test_instrument_from_symbol_refuses_an_unknown_quote_currency() -> None:
    light = LIGHT["GER40.cash"]
    assets = dict(ASSETS)
    fake_quote = om.ProtoOAAsset()
    fake_quote.CopyFrom(assets[light.quoteAssetId])
    fake_quote.name = "NOT_A_CURRENCY"
    assets[light.quoteAssetId] = fake_quote
    with pytest.raises(CTraderProtocolError) as exc_info:
        parsing.instrument_from_symbol(SPECS[light.symbolId], light, assets, {}, ts_init=0)
    message = str(exc_info.value)
    assert "GER40.cash" in message
    assert "NOT_A_CURRENCY" in message


def test_instrument_from_symbol_refuses_a_missing_asset_id() -> None:
    light = LIGHT["EURUSD"]
    assets = {k: v for k, v in ASSETS.items() if k != light.quoteAssetId}
    with pytest.raises(CTraderProtocolError) as exc_info:
        parsing.instrument_from_symbol(SPECS[light.symbolId], light, assets, {}, ts_init=0)
    message = str(exc_info.value)
    assert "EURUSD" in message
    assert str(light.quoteAssetId) in message


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
