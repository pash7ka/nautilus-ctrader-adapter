"""Pure converters from cTrader protocol messages to Nautilus instruments, prices, bars, quotes.

No I/O, no protocol knowledge beyond decoding what a message already carries. Scaling is the
one place a plausible-looking converter is silently wrong, so every constant here is checked
in `tests/test_parsing.py` against a recorded real response, never a hand-built message.
"""

from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal

from google.protobuf.internal.enum_type_wrapper import EnumTypeWrapper
from google.protobuf.message import Message
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
SECS_PER_HOUR: int = 3_600

_METALS = frozenset({"XAU", "XAG", "XPT", "XPD"})


def _currency(name: str) -> Currency | None:
    return Currency.from_str(name, strict=True)


def _is_pair_currency(currency: Currency | None) -> bool:
    return currency is not None and currency.currency_type in (
        CurrencyType.FIAT,
        CurrencyType.CRYPTO,
    )


def asset_class_for(base_name: str, quote_name: str) -> AssetClass:
    """`AssetClass` for a CFD symbol, from its base and quote asset names.

    `instrument_from_symbol` never reaches the `FX` branch: a currency-against-currency
    symbol is built as a `CurrencyPair`, which derives its own asset class.
    """
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


def bar_boundary_secs(utc_minutes: int) -> int:
    """A trendbar's `utcTimestampInMinutes` as its open time in seconds.

    The venue's value is authoritative and is never moved onto a multiple of the period: a
    period's boundaries are not necessarily aligned to the Unix epoch. A daily bar opens at
    21:00 or 22:00 UTC depending on the time of year, so flooring one put it on the previous
    calendar day.
    """
    return utc_minutes * 60


def is_unaligned_boundary(boundary_secs: int, period_secs: int) -> bool:
    """Whether an open time breaks the epoch alignment expected of `period_secs`.

    Only the periods dividing an hour are expected to be epoch-aligned, and that is arithmetic
    rather than a guess: the trading day starts on the hour, so a period that divides an hour
    divides the day's offset too and keeps its alignment whatever that offset is. A longer
    period carries the offset instead, so nothing is expected of it.

    Confirmed live for every period this adapter supports: M1, M15 and H1 open on multiples of
    their own length, while H4, H12 and D1 open on the trading day's phase. That phase is not
    constant: the trading day rolls at 17:00 in New York, so it follows US daylight saving and
    the daily open alternates between 21:00 and 22:00 UTC (see docs/protocol.md). Nothing here
    depends on its current value - only on whether a period divides an hour, which holds
    whatever the phase is.
    """
    return SECS_PER_HOUR % period_secs == 0 and boundary_secs % period_secs != 0


def _optional_field(message: Message, field: str) -> object | None:
    return getattr(message, field) if message.HasField(field) else None


def _optional_enum_field(message: Message, field: str, enum_type: EnumTypeWrapper) -> str | None:
    return enum_type.Name(getattr(message, field)) if message.HasField(field) else None


def _optional_exact_quantity(
    symbol_name: str,
    symbol: om.ProtoOASymbol,
    field: str,
    precision: int,
) -> Quantity | None:
    """A cents-of-a-unit volume field as a `Quantity`, refusing silent rounding.

    `precision` comes from `stepVolume`; a `minVolume`/`maxVolume`/`lotSize` finer than that
    step would otherwise round away, in the worst case to zero.
    """
    if not symbol.HasField(field):
        return None
    raw = getattr(symbol, field)
    units = volume_to_units(raw)
    if _precision(units) > precision:
        raise CTraderProtocolError(
            f"{symbol_name}: {field}={raw} ({units} units) is not exact at the symbol's "
            f"size precision of {precision} (derived from stepVolume)",
        )
    return Quantity(units, precision)


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


def _asset_name(assets: Mapping[int, om.ProtoOAAsset], asset_id: int, symbol_name: str) -> str:
    asset = assets.get(asset_id)
    if asset is None:
        raise CTraderProtocolError(
            f"{symbol_name}: asset id {asset_id} not found in the asset table",
        )
    return asset.name


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

    Raises `CTraderProtocolError` if a base/quote asset id is missing from `assets`, if the
    quote asset does not resolve to a known `Currency`, if `digits` exceeds the 1/100000 price
    scale, if `stepVolume` is missing or zero, or if `minVolume`/`maxVolume`/`lotSize` is not
    exact at the precision `stepVolume` implies.
    """
    name = light.symbolName
    raw_symbol = Symbol(name)
    instrument_id = InstrumentId(raw_symbol, CTRADER_VENUE)

    digits = symbol.digits
    if digits > 5:
        raise CTraderProtocolError(
            f"{name}: digits={digits} exceeds the supported 1/100000 price scale",
        )
    price_increment = Price(Decimal(1).scaleb(-digits), digits)

    base_name = _asset_name(assets, light.baseAssetId, name)
    quote_name = _asset_name(assets, light.quoteAssetId, name)
    base_currency = _currency(base_name)
    quote_currency = _currency(quote_name)
    if quote_currency is None:
        raise CTraderProtocolError(f"{name}: quote asset {quote_name!r} is not a known currency")

    step = volume_to_units(symbol.stepVolume)
    if step <= 0:
        # `stepVolume` is optional in the schema, and every size here is a multiple of it.
        raise CTraderProtocolError(f"{name}: stepVolume is missing or zero")
    size_precision = _precision(step)
    size_increment = Quantity(step, size_precision)
    lot_size = _optional_exact_quantity(name, symbol, "lotSize", size_precision)
    min_quantity = _optional_exact_quantity(name, symbol, "minVolume", size_precision)
    max_quantity = _optional_exact_quantity(name, symbol, "maxVolume", size_precision)

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

    `ts_event` is the bar's close time: the venue's own open time plus the period.

    The period comes from `bar_type`, not `tb.period`: recorded historical responses leave
    `ProtoOATrendbar.period` unset on every individual bar (the real period is only carried on
    the enclosing `ProtoOAGetTrendbarsRes`), so reading it here would silently default to M1.
    Live bars (from a spot event subscription) do set `tb.period`; when they do, it must agree
    with `bar_type`, or `CTraderProtocolError` is raised rather than silently trusting one over
    the other.
    """
    period = trendbar_period_for(bar_type)
    if tb.HasField("period") and tb.period != period:
        raise CTraderProtocolError(
            f"{bar_type}: trendbar period {tb.period} does not match the bar type's period "
            f"{period}",
        )

    open_price = price_from_raw(tb.low + tb.deltaOpen, price_precision)
    high_price = price_from_raw(tb.low + tb.deltaHigh, price_precision)
    low_price = price_from_raw(tb.low, price_precision)
    close_price = price_from_raw(tb.low + tb.deltaClose, price_precision)
    # TODO(verify): trendbar volume is a tick count, not scaled.
    volume = Quantity(tb.volume, size_precision)

    period_secs = PERIOD_SECS[period]
    ts_event = (bar_boundary_secs(tb.utcTimestampInMinutes) + period_secs) * 1_000_000_000

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
