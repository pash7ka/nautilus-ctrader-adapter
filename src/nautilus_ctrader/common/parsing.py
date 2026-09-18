"""Pure converters from cTrader protocol messages to Nautilus instruments, prices, bars, quotes.

No I/O, no protocol knowledge beyond decoding what a message already carries. Scaling is the
one place a plausible-looking converter is silently wrong, so every constant here is checked
in `tests/test_parsing.py` against a recorded real response, never a hand-built message.
"""

from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal

from nautilus_trader.model.data import Bar, BarType, QuoteTick
from nautilus_trader.model.enums import AssetClass, CurrencyType
from nautilus_trader.model.identifiers import InstrumentId, Symbol
from nautilus_trader.model.instruments import Cfd, CurrencyPair
from nautilus_trader.model.objects import Currency, Price, Quantity

from nautilus_ctrader.common.errors import CTraderProtocolError
from nautilus_ctrader.constants import CTRADER_VENUE
from nautilus_ctrader.enums import PERIOD_SECS, trendbar_period_for
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as om

# Trendbar and spot-event prices are specified in 1/100000 of a unit of price.
PRICE_SCALE: int = 100_000
# Volumes are specified in cents of a unit.
VOLUME_SCALE: int = 100

_METALS = frozenset({"XAU", "XAG", "XPT", "XPD"})


def _currency(name: str) -> Currency | None:
    return Currency.from_str(name, strict=True)


def _is_pair_currency(currency: Currency | None) -> bool:
    return currency is not None and currency.currency_type in (
        CurrencyType.FIAT,
        CurrencyType.CRYPTO,
    )


def asset_class_for(base_name: str, quote_name: str) -> AssetClass:
    """`AssetClass` for a CFD symbol, from its base and quote asset names."""
    base, quote = _currency(base_name), _currency(quote_name)
    if (
        base is not None
        and quote is not None
        and base.currency_type == CurrencyType.FIAT
        and quote.currency_type == CurrencyType.FIAT
    ):
        return AssetClass.FX
    if base_name in _METALS:
        return AssetClass.COMMODITY
    if base is not None and base.currency_type == CurrencyType.CRYPTO:
        return AssetClass.CRYPTOCURRENCY
    return AssetClass.ALTERNATIVE


def volume_to_units(volume_cents: int) -> Decimal:
    """Volume in cents-of-a-unit to a plain unit amount."""
    return Decimal(volume_cents) / VOLUME_SCALE


def _precision(value: Decimal) -> int:
    return max(0, -value.normalize().as_tuple().exponent)


def price_from_raw(raw: int, digits: int) -> Price:
    """A raw 1/100000-scaled price at the symbol's digits.

    Raises `CTraderProtocolError` if `raw` does not land exactly on `digits` decimal places.
    """
    # TODO(verify): every venue price lands exactly on the symbol's digits.
    step = 10 ** (5 - digits) if digits <= 5 else None
    if step is None or raw % step:
        raise CTraderProtocolError(f"price {raw} is not representable at {digits} digits")
    return Price(Decimal(raw) / PRICE_SCALE, digits)


def bar_boundary_secs(utc_minutes: int, period_secs: int) -> int:
    """The period-aligned open time, in seconds, for a trendbar's `utcTimestampInMinutes`."""
    # TODO(verify): open times are period-aligned for every period; D1/H4 boundaries.
    secs = utc_minutes * 60
    return secs - secs % period_secs


def _optional_field(message, field: str):
    return getattr(message, field) if message.HasField(field) else None


def _optional_enum_field(message, field: str, enum_type):
    return enum_type.Name(getattr(message, field)) if message.HasField(field) else None


def _optional_quantity(symbol: om.ProtoOASymbol, field: str, precision: int) -> Quantity | None:
    if not symbol.HasField(field):
        return None
    return Quantity(volume_to_units(getattr(symbol, field)), precision)


def _info_for(symbol: om.ProtoOASymbol, light: om.ProtoOALightSymbol) -> dict[str, object]:
    return {
        "symbol_id": light.symbolId,
        "lot_size_cents": _optional_field(symbol, "lotSize"),
        "min_volume_cents": _optional_field(symbol, "minVolume"),
        "step_volume_cents": _optional_field(symbol, "stepVolume"),
        "max_volume_cents": _optional_field(symbol, "maxVolume"),
        "pip_position": symbol.pipPosition,
        "measurement_units": _optional_field(symbol, "measurementUnits"),
        "trading_mode": _optional_enum_field(symbol, "tradingMode", om.ProtoOATradingMode),
        "schedule_time_zone": _optional_field(symbol, "scheduleTimeZone"),
        "commission_type": _optional_enum_field(symbol, "commissionType", om.ProtoOACommissionType),
        "precise_trading_commission_rate": _optional_field(symbol, "preciseTradingCommissionRate"),
        "precise_min_commission": _optional_field(symbol, "preciseMinCommission"),
        "swap_long": _optional_field(symbol, "swapLong"),
        "swap_short": _optional_field(symbol, "swapShort"),
        "swap_calculation_type": _optional_enum_field(
            symbol,
            "swapCalculationType",
            om.ProtoOASwapCalculationType,
        ),
        "leverage_id": _optional_field(symbol, "leverageId"),
        "sl_distance": _optional_field(symbol, "slDistance"),
        "tp_distance": _optional_field(symbol, "tpDistance"),
        "distance_set_in": _optional_enum_field(
            symbol,
            "distanceSetIn",
            om.ProtoOASymbolDistanceType,
        ),
    }


def instrument_from_symbol(
    symbol: om.ProtoOASymbol,
    light: om.ProtoOALightSymbol,
    assets: Mapping[int, om.ProtoOAAsset],
    asset_class_overrides: Mapping[str, AssetClass],
    ts_init: int,
) -> CurrencyPair | Cfd:
    """A `CurrencyPair` or `Cfd` from a symbol's full spec, light entry and asset table.

    `asset_class_overrides` is keyed by symbol name and applies to `Cfd` results only; an
    override for a symbol that resolves to a `CurrencyPair` is ignored (currency pairs derive
    their class from their currencies), not reported here — the caller logs that case.
    """
    name = light.symbolName
    raw_symbol = Symbol(name)
    instrument_id = InstrumentId(raw_symbol, CTRADER_VENUE)

    base_name = assets[light.baseAssetId].name
    quote_name = assets[light.quoteAssetId].name
    base_currency = _currency(base_name)
    quote_currency = _currency(quote_name)

    digits = symbol.digits
    price_increment = Price(Decimal(1).scaleb(-digits), digits)

    step = volume_to_units(symbol.stepVolume)
    size_precision = _precision(step)
    size_increment = Quantity(step, size_precision)
    lot_size = _optional_quantity(symbol, "lotSize", size_precision)
    min_quantity = _optional_quantity(symbol, "minVolume", size_precision)
    max_quantity = _optional_quantity(symbol, "maxVolume", size_precision)

    info = _info_for(symbol, light)

    if _is_pair_currency(base_currency) and _is_pair_currency(quote_currency):
        return CurrencyPair(
            instrument_id=instrument_id,
            raw_symbol=raw_symbol,
            base_currency=base_currency,
            quote_currency=quote_currency,
            price_precision=digits,
            size_precision=size_precision,
            price_increment=price_increment,
            size_increment=size_increment,
            ts_event=ts_init,
            ts_init=ts_init,
            lot_size=lot_size,
            max_quantity=max_quantity,
            min_quantity=min_quantity,
            maker_fee=Decimal(0),
            taker_fee=Decimal(0),
            info=info,
        )

    asset_class = asset_class_overrides.get(name, asset_class_for(base_name, quote_name))
    return Cfd(
        instrument_id=instrument_id,
        raw_symbol=raw_symbol,
        asset_class=asset_class,
        quote_currency=quote_currency,
        price_precision=digits,
        size_precision=size_precision,
        price_increment=price_increment,
        size_increment=size_increment,
        ts_event=ts_init,
        ts_init=ts_init,
        base_currency=base_currency,
        lot_size=lot_size,
        max_quantity=max_quantity,
        min_quantity=min_quantity,
        maker_fee=Decimal(0),
        taker_fee=Decimal(0),
        info=info,
    )


def bar_from_trendbar(
    tb: om.ProtoOATrendbar,
    bar_type: BarType,
    price_precision: int,
    size_precision: int,
    ts_init: int,
) -> Bar:
    """A closed `Bar` from a `ProtoOATrendbar`.

    `ts_event` is the bar's close time: its period-aligned open boundary plus the period.

    The period comes from `bar_type`, not `tb.period`: recorded historical responses leave
    `ProtoOATrendbar.period` unset on every individual bar (the real period is only carried on
    the enclosing `ProtoOAGetTrendbarsRes`), so reading it here would silently default to M1.
    """
    open_price = price_from_raw(tb.low + tb.deltaOpen, price_precision)
    high_price = price_from_raw(tb.low + tb.deltaHigh, price_precision)
    low_price = price_from_raw(tb.low, price_precision)
    close_price = price_from_raw(tb.low + tb.deltaClose, price_precision)
    volume = Quantity(tb.volume, size_precision)

    period_secs = PERIOD_SECS[trendbar_period_for(bar_type)]
    boundary = bar_boundary_secs(tb.utcTimestampInMinutes, period_secs)
    ts_event = (boundary + period_secs) * 1_000_000_000

    return Bar(bar_type, open_price, high_price, low_price, close_price, volume, ts_event, ts_init)


def quote_from_prices(
    instrument_id: InstrumentId,
    bid_raw: int,
    ask_raw: int,
    digits: int,
    size: Decimal,
    size_precision: int,
    ts_event: int,
    ts_init: int,
) -> QuoteTick:
    """A `QuoteTick` from raw bid/ask prices, sharing one size on both sides of the book."""
    bid_price = price_from_raw(bid_raw, digits)
    ask_price = price_from_raw(ask_raw, digits)
    quantity = Quantity(size, size_precision)
    return QuoteTick(instrument_id, bid_price, ask_price, quantity, quantity, ts_event, ts_init)
