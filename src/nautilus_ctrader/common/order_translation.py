"""Nautilus orders to cTrader requests. Nothing is sent and nothing is remembered here.

In cTrader a stop-loss and a take-profit are levels on a position, not orders. A Nautilus
bracket - an entry with a stop-loss leg and a take-profit leg - therefore goes out as one order
carrying both levels. The translation is literal: what the venue cannot express is refused with
a reason, never approximated.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Decimal

from nautilus_trader.model.enums import (
    ContingencyType,
    OrderSide,
    OrderType,
    PositionSide,
    TimeInForce,
    TriggerType,
)
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.model.objects import Price, Quantity
from nautilus_trader.model.orders import Order

from nautilus_ctrader.common import order_record
from nautilus_ctrader.common.order_record import LegIds
from nautilus_ctrader.common.parsing import PRICE_SCALE, VOLUME_SCALE
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as om

_TRADE_SIDE = {OrderSide.BUY: om.BUY, OrderSide.SELL: om.SELL}


class Unsupported(ValueError):
    """An order the venue model cannot express, or outside what this adapter supports.

    The message is the reason reported for the refused order.
    """


@dataclass(frozen=True)
class Bracket:
    """A bracket as the one request that opens it, and what must follow once it is filled.

    `stop_loss` and `take_profit` are the exact prices asked for. The request carries them only
    as distances from the fill, so the position is amended to the exact prices afterwards.
    """

    request: oa.ProtoOANewOrderReq
    stop_loss_id: ClientOrderId | None
    take_profit_id: ClientOrderId | None
    stop_loss: Price | None
    take_profit: Price | None


def volume_from_quantity(quantity: Quantity) -> int:
    """A quantity in units as the venue's volume, in hundredths of a unit.

    Refused rather than rounded if it is finer than that: a rounded volume is a different order.
    """
    # TODO(verify): the schema states volume in 0.01 of a unit; a live minimum-size order confirms
    # that this factor and the instrument's quantity units agree.
    scaled = quantity.as_decimal() * VOLUME_SCALE
    if scaled <= 0 or scaled != scaled.to_integral_value():
        raise Unsupported(
            f"quantity {quantity} cannot be expressed in the venue's hundredths of a unit",
        )
    return int(scaled)


def relative_level(reference: Decimal, level: Decimal, *, below: bool) -> int:
    """The distance from `reference` to `level` in 1/100000 of a price.

    `below` says which side of the reference the level must be on. The venue applies the
    distance from the fill price, so a level at or beyond the reference has no valid distance.
    """
    # TODO(verify): the schema states the distance in 1/100000 of a price, applied from the fill;
    # a live bracket on a minimum-size order confirms the factor and the direction.
    if reference <= 0:
        raise Unsupported(f"reference price {reference} must be positive")
    if reference * PRICE_SCALE != (reference * PRICE_SCALE).to_integral_value():
        raise Unsupported(f"reference price {reference} is finer than the venue's price scale")
    scaled = ((reference - level) if below else (level - reference)) * PRICE_SCALE
    if scaled <= 0:
        raise Unsupported(f"level {level} is on the wrong side of the market ({reference})")
    if scaled != scaled.to_integral_value():
        raise Unsupported(f"level {level} is finer than the venue's price scale")
    return int(scaled)


def _require_instrument(instrument: Instrument, order: Order) -> None:
    if order.instrument_id != instrument.id:
        raise Unsupported(
            f"an order for instrument {order.instrument_id} was given {instrument.id}"
        )


def _require_on_price_grid(instrument: Instrument, price: Price) -> None:
    value = price.as_decimal()
    if value != value.quantize(Decimal(1).scaleb(-instrument.price_precision)):
        raise Unsupported(
            f"price {price} is finer than the instrument's price precision "
            f"of {instrument.price_precision} decimals",
        )


# A market order fills now whatever Nautilus calls it; the request says so explicitly.
_MARKET_TIME_IN_FORCE = (TimeInForce.GTC, TimeInForce.IOC)


def _require_expressible(order: Order, *, market: bool = False) -> None:
    """Refuse what no request built here can carry, on every order this module reads."""
    if order.is_quote_quantity:
        raise Unsupported("a quantity in the quote currency is not supported, only base units")
    if order.emulation_trigger != TriggerType.NO_TRIGGER:
        raise Unsupported("emulated orders are not supported: the venue has no local trigger")
    if order.exec_algorithm_id is not None:
        raise Unsupported("an execution algorithm is not supported: orders go out as given")
    allowed = _MARKET_TIME_IN_FORCE if market else (TimeInForce.GTC,)
    if order.time_in_force not in allowed:
        raise Unsupported(f"time in force {TimeInForce(order.time_in_force).name} is not supported")


def _require_not_linked(order: Order) -> None:
    if order.contingency_type != ContingencyType.NO_CONTINGENCY:
        raise Unsupported("an order linked to others must be submitted as a bracket")


def _require_opening_market_order(instrument: Instrument, entry: Order) -> None:
    _require_instrument(instrument, entry)
    if entry.order_type != OrderType.MARKET:
        raise Unsupported(
            f"{OrderType(entry.order_type).name} entry orders are not supported, only MARKET",
        )
    if entry.is_reduce_only:
        raise Unsupported("a reduce-only order cannot open a position; it must name the position")
    _require_expressible(entry, market=True)


def _new_order(
    account_id: int,
    instrument: Instrument,
    entry: Order,
    legs: LegIds,
) -> oa.ProtoOANewOrderReq:
    volume = volume_from_quantity(entry.quantity)
    entry_id = entry.client_order_id.value
    try:
        # `RecordTooLong` is a `ValueError` too, so one clause covers both refusals.
        client_order_id = order_record.check_client_order_id(entry_id)
        label = order_record.encode_label(entry_id)
        comment = order_record.encode_comment(legs)
    except ValueError as e:
        raise Unsupported(str(e)) from e
    # TODO(verify): the broker's own terminal sends market orders immediate-or-cancel; the first
    # order the adapter sends, read back from the broker, confirms it fills the same way.
    return oa.ProtoOANewOrderReq(
        ctidTraderAccountId=account_id,
        symbolId=instrument.info["symbol_id"],
        orderType=om.MARKET,
        tradeSide=_TRADE_SIDE[entry.side],
        volume=volume,
        timeInForce=om.IMMEDIATE_OR_CANCEL,
        clientOrderId=client_order_id,
        label=label,
        comment=comment,
    )


def market_order(account_id: int, instrument: Instrument, order: Order) -> oa.ProtoOANewOrderReq:
    """An opening market order with no protective levels."""
    _require_opening_market_order(instrument, order)
    _require_not_linked(order)
    return _new_order(account_id, instrument, order, LegIds(None, None))


def _require_protective_leg(instrument: Instrument, entry: Order, leg: Order) -> None:
    _require_instrument(instrument, leg)
    _require_expressible(leg)
    if leg.order_type not in (OrderType.STOP_MARKET, OrderType.LIMIT):
        raise Unsupported(f"{OrderType(leg.order_type).name} protective legs are not supported")
    if leg.side == entry.side:
        raise Unsupported("a protective leg must be on the opposite side to its entry")
    if leg.quantity != entry.quantity:
        raise Unsupported(
            "a protective level covers the whole position and cannot have its own size",
        )
    if not leg.is_reduce_only:
        raise Unsupported("a protective leg must be reduce-only: a level cannot open a position")
    if leg.parent_order_id != entry.client_order_id:
        raise Unsupported("a protective leg must have the bracket's entry as its parent order")


def bracket(
    account_id: int,
    instrument: Instrument,
    orders: Sequence[Order],
    *,
    bid: Decimal,
    ask: Decimal,
) -> Bracket:
    """A market entry and its protective legs as one request.

    `bid` and `ask` are the market now. The entry stays a market order and fills where the
    market is; they only turn each leg's price into a distance, measured from the side the
    entry fills on.
    """
    if not 2 <= len(orders) <= 3:
        raise Unsupported(
            f"a bracket is an entry and one or two protective legs, not {len(orders)} orders",
        )
    entry, *legs = orders
    _require_opening_market_order(instrument, entry)

    stop_loss: Order | None = None
    take_profit: Order | None = None
    for leg in legs:
        _require_protective_leg(instrument, entry, leg)
        if leg.order_type == OrderType.STOP_MARKET:
            if stop_loss is not None:
                raise Unsupported("a position holds one stop-loss")
            if leg.trigger_type != TriggerType.DEFAULT:
                raise Unsupported(
                    f"trigger type {TriggerType(leg.trigger_type).name} is not supported",
                )
            stop_loss = leg
        else:
            if take_profit is not None:
                raise Unsupported("a position holds one take-profit")
            if leg.display_qty is not None:
                raise Unsupported("a display quantity is not supported on a take-profit")
            # `is_post_only` is deliberately not read. The venue's take-profit is a level on a
            # position with no maker-only notion, a target on the wrong side of the market is
            # refused below so it cannot be marketable when placed, and the order factory sets
            # the flag on a bracket's target by default.
            take_profit = leg

    request = _new_order(
        account_id,
        instrument,
        entry,
        LegIds(
            stop_loss=None if stop_loss is None else stop_loss.client_order_id.value,
            take_profit=None if take_profit is None else take_profit.client_order_id.value,
        ),
    )
    buying = entry.side == OrderSide.BUY
    reference = ask if buying else bid
    if stop_loss is not None:
        # TODO(verify): which trigger method the venue applies to a stop-loss level set by a
        # relative distance (a new order cannot set the position's `stopLossTriggerMethod`,
        # whose schema default is TRADE); reading the position back after placement shows it.
        _require_on_price_grid(instrument, stop_loss.trigger_price)
        request.relativeStopLoss = relative_level(
            reference, stop_loss.trigger_price.as_decimal(), below=buying
        )
    if take_profit is not None:
        _require_on_price_grid(instrument, take_profit.price)
        request.relativeTakeProfit = relative_level(
            reference, take_profit.price.as_decimal(), below=not buying
        )
    return Bracket(
        request=request,
        stop_loss_id=None if stop_loss is None else stop_loss.client_order_id,
        take_profit_id=None if take_profit is None else take_profit.client_order_id,
        stop_loss=None if stop_loss is None else stop_loss.trigger_price,
        take_profit=None if take_profit is None else take_profit.price,
    )


def close_position(
    account_id: int,
    position_id: int,
    order: Order,
    *,
    position_side: PositionSide,
) -> oa.ProtoOAClosePositionReq:
    """A reduce-only market order as a close of the position it names.

    The venue's close request carries no side, so `position_side` is what stops an order that
    would add to the position from going out as a close.
    """
    if order.order_type != OrderType.MARKET or not order.is_reduce_only:
        raise Unsupported("a position is closed by a reduce-only MARKET order")
    _require_expressible(order, market=True)
    _require_not_linked(order)
    if position_side not in (PositionSide.LONG, PositionSide.SHORT):
        raise Unsupported(
            f"a position that is {PositionSide(position_side).name} cannot be reduced"
        )
    if (position_side == PositionSide.LONG) == (order.side == OrderSide.BUY):
        raise Unsupported(
            f"a {OrderSide(order.side).name} order would not reduce a "
            f"{PositionSide(position_side).name} position",
        )
    return oa.ProtoOAClosePositionReq(
        ctidTraderAccountId=account_id,
        positionId=position_id,
        volume=volume_from_quantity(order.quantity),
    )


def amend_levels(
    account_id: int,
    position_id: int,
    *,
    stop_loss: Price | None,
    take_profit: Price | None,
) -> oa.ProtoOAAmendPositionSLTPReq:
    """The position's levels as they should stand: a level given is set, one left out is removed.

    The request describes both levels at once, so the caller always passes the whole intended
    state, including a level it is not changing.
    """
    # TODO(verify): that leaving a level out removes it, rather than leaving it unchanged. A
    # live amend on an open position settles it; if it does not remove, this is the one place
    # to change.
    request = oa.ProtoOAAmendPositionSLTPReq(
        ctidTraderAccountId=account_id,
        positionId=position_id,
    )
    if stop_loss is not None:
        request.stopLoss = stop_loss.as_double()
    if take_profit is not None:
        request.takeProfit = take_profit.as_double()
    return request
