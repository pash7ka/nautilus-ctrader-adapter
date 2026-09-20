"""Checks that Nautilus itself behaves as the instrument and quote design assumes.

Nothing here exercises adapter code beyond the converters: the assertions are about
`Cache.get_xrate`, `DataEngine` and `OrderBook`, whose behaviour decided how symbols are
mapped (`CurrencyPair` vs `Cfd`) and why quotes may need a synthetic size. If one of these
fails after a Nautilus upgrade, a design decision needs revisiting, not a bug fixing.
"""

from __future__ import annotations

from decimal import Decimal

from nautilus_trader.data.engine import DataEngine
from nautilus_trader.model.book import OrderBook
from nautilus_trader.model.currencies import EUR, JPY, USD
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.enums import AssetClass, BookType
from nautilus_trader.model.instruments import Cfd, CurrencyPair, Instrument
from nautilus_trader.test_kit.stubs.component import TestComponentStubs

from nautilus_ctrader.common import parsing
from nautilus_ctrader.constants import CTRADER_VENUE
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as om
from tests.fixtures import load_recorded

REC = load_recorded()
ASSETS = {a.assetId: a for a in REC["assets"][0].asset}
LIGHT = {s.symbolName: s for s in REC["symbols"][0].symbol}
SPECS = {s.symbolId: s for s in REC["symbol_specs"][0].symbol}
EURUSD_SPOT = REC["spot_events"][0]

USD_ASSET_ID = next(a.assetId for a in ASSETS.values() if a.name == "USD")
JPY_ASSET_ID = next(a.assetId for a in ASSETS.values() if a.name == "JPY")


def _instrument(light: om.ProtoOALightSymbol) -> Instrument:
    return parsing.instrument_from_symbol(SPECS[light.symbolId], light, ASSETS, {}, ts_init=0)


def _usdjpy_light() -> om.ProtoOALightSymbol:
    """A second currency pair the recorded account does not carry, so a chain has two legs."""
    light = om.ProtoOALightSymbol()
    light.CopyFrom(LIGHT["EURUSD"])
    light.symbolName = "USDJPY"
    light.baseAssetId = USD_ASSET_ID
    light.quoteAssetId = JPY_ASSET_ID
    return light


def _as_cfd(pair: CurrencyPair) -> Cfd:
    """The pair's currencies on the type every symbol outside FX is mapped to."""
    return Cfd(
        instrument_id=pair.id,
        raw_symbol=pair.raw_symbol,
        asset_class=AssetClass.FX,
        quote_currency=pair.quote_currency,
        price_precision=pair.price_precision,
        size_precision=pair.size_precision,
        price_increment=pair.price_increment,
        size_increment=pair.size_increment,
        ts_event=0,
        ts_init=0,
        base_currency=pair.base_currency,
    )


def _quote(instrument: Instrument, bid_raw: int, ask_raw: int, size: Decimal) -> QuoteTick:
    return parsing.quote_from_prices(
        instrument.id,
        bid_raw,
        ask_raw,
        instrument.price_precision,
        size,
        instrument.size_precision,
        ts_event=0,
        ts_init=0,
    )


def _mid(instrument: Instrument, bid_raw: int, ask_raw: int) -> float:
    digits = instrument.price_precision
    bid = float(parsing.price_from_raw(bid_raw, digits))
    ask = float(parsing.price_from_raw(ask_raw, digits))
    return (bid + ask) / 2


def test_get_xrate_reads_a_currency_pair_quote_in_both_directions() -> None:
    eurusd = _instrument(LIGHT["EURUSD"])
    assert isinstance(eurusd, CurrencyPair)
    cache = TestComponentStubs.cache()
    cache.add_instrument(eurusd)
    cache.add_quote_tick(_quote(eurusd, EURUSD_SPOT.bid, EURUSD_SPOT.ask, Decimal(0)))

    expected = _mid(eurusd, EURUSD_SPOT.bid, EURUSD_SPOT.ask)

    assert cache.get_xrate(CTRADER_VENUE, EUR, USD) == expected
    assert cache.get_xrate(CTRADER_VENUE, USD, EUR) == 1 / expected

    # The other half of the same decision: a `Cfd` is not a source of rates, whatever its
    # `base_currency`, so only symbols mapped to a `CurrencyPair` can price a position.
    cfd_cache = TestComponentStubs.cache()
    cfd_cache.add_instrument(_as_cfd(eurusd))
    cfd_cache.add_quote_tick(_quote(eurusd, EURUSD_SPOT.bid, EURUSD_SPOT.ask, Decimal(0)))

    assert cfd_cache.get_xrate(CTRADER_VENUE, EUR, USD) is None


def test_get_xrate_walks_a_two_symbol_chain() -> None:
    eurusd = _instrument(LIGHT["EURUSD"])
    usdjpy = _instrument(_usdjpy_light())
    assert isinstance(usdjpy, CurrencyPair)
    cache = TestComponentStubs.cache()
    for instrument in (eurusd, usdjpy):
        cache.add_instrument(instrument)
    cache.add_quote_tick(_quote(eurusd, EURUSD_SPOT.bid, EURUSD_SPOT.ask, Decimal(0)))
    cache.add_quote_tick(_quote(usdjpy, 15_000_000, 15_000_200, Decimal(0)))

    rate = cache.get_xrate(CTRADER_VENUE, EUR, JPY)

    assert rate is not None
    expected = _mid(eurusd, EURUSD_SPOT.bid, EURUSD_SPOT.ask) * _mid(usdjpy, 15_000_000, 15_000_200)
    assert rate == expected


def test_an_unsubscribed_quote_still_reaches_the_cache() -> None:
    """Conversion quotes are published with no subscriber, and must still price positions."""
    eurusd = _instrument(LIGHT["EURUSD"])
    cache = TestComponentStubs.cache()
    cache.add_instrument(eurusd)
    engine = DataEngine(TestComponentStubs.msgbus(), cache, TestComponentStubs.clock())
    quote = _quote(eurusd, EURUSD_SPOT.bid, EURUSD_SPOT.ask, Decimal(0))

    engine.process(quote)

    assert cache.quote_tick(eurusd.id) == quote


def test_an_l1_book_needs_a_nonzero_quote_size() -> None:
    """The reason `synthetic_quote_size` exists: the venue's spots carry no size."""
    eurusd = _instrument(LIGHT["EURUSD"])
    sized = OrderBook(eurusd.id, BookType.L1_MBP)
    unsized = OrderBook(eurusd.id, BookType.L1_MBP)

    sized.update_quote_tick(_quote(eurusd, EURUSD_SPOT.bid, EURUSD_SPOT.ask, Decimal(1)))
    unsized.update_quote_tick(_quote(eurusd, EURUSD_SPOT.bid, EURUSD_SPOT.ask, Decimal(0)))

    assert sized.best_bid_price() is not None
    assert sized.best_ask_price() is not None
    assert unsized.best_bid_price() is None
    assert unsized.best_ask_price() is None
