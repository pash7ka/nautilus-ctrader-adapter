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

from nautilus_trader.model.enums import OrderSide, OrderType, TimeInForce, TriggerType
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.model.objects import Price, Quantity
from nautilus_trader.model.orders import Order

from nautilus_ctrader.common import order_record
from nautilus_ctrader.common.order_record import LegIds, RecordTooLong
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
    distance = (reference - level) if below else (level - reference)
    scaled = distance * PRICE_SCALE
    if scaled <= 0:
        raise Unsupported(
            f"level {level} is on the wrong side of the market ({reference})",
        )
    if scaled != scaled.to_integral_value():
        raise Unsupported(f"level {level} is finer than the venue's price scale")
    return int(scaled)


def _require_supported(order: Order) -> None:
    if order.time_in_force is not TimeInForce.GTC:
        raise Unsupported(
            f"time in force {TimeInForce(order.time_in_force).name} is not supported",
        )


def _new_order(
    account_id: int,
    instrument: Instrument,
    entry: Order,
    legs: LegIds,
) -> oa.ProtoOANewOrderReq:
    if entry.order_type is not OrderType.MARKET:
        raise Unsupported(
            f"{OrderType(entry.order_type).name} entry orders are not supported, only MARKET",
        )
    _require_supported(entry)
    entry_id = entry.client_order_id.value
    try:
        return oa.ProtoOANewOrderReq(
            ctidTraderAccountId=account_id,
            symbolId=instrument.info["symbol_id"],
            orderType=om.MARKET,
            tradeSide=_TRADE_SIDE[entry.side],
            volume=volume_from_quantity(entry.quantity),
            clientOrderId=order_record.check_client_order_id(entry_id),
            label=order_record.encode_label(entry_id),
            comment=order_record.encode_comment(legs),
        )
    except RecordTooLong as e:
        raise Unsupported(str(e)) from e


def market_order(account_id: int, instrument: Instrument, order: Order) -> oa.ProtoOANewOrderReq:
    """An opening market order with no protective levels."""
    if order.is_reduce_only:
        raise Unsupported(
            "on a hedging account a reduce-only order must name its position",
        )
    return _new_order(account_id, instrument, order, LegIds(None, None))


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
    entry, *legs = orders
    stop_loss: Order | None = None
    take_profit: Order | None = None
    for leg in legs:
        _require_supported(leg)
        if leg.side is entry.side:
            raise Unsupported("a protective leg must be on the opposite side to its entry")
        if leg.quantity != entry.quantity:
            raise Unsupported(
                "a protective level covers the whole position and cannot have its own size",
            )
        if leg.order_type is OrderType.STOP_MARKET:
            if stop_loss is not None:
                raise Unsupported("a position holds one stop-loss")
            if leg.trigger_type is not TriggerType.DEFAULT:
                raise Unsupported(
                    f"trigger type {TriggerType(leg.trigger_type).name} is not supported",
                )
            stop_loss = leg
        elif leg.order_type is OrderType.LIMIT:
            if take_profit is not None:
                raise Unsupported("a position holds one take-profit")
            take_profit = leg
        else:
            raise Unsupported(
                f"{OrderType(leg.order_type).name} protective legs are not supported",
            )

    request = _new_order(
        account_id,
        instrument,
        entry,
        LegIds(
            stop_loss=None if stop_loss is None else stop_loss.client_order_id.value,
            take_profit=None if take_profit is None else take_profit.client_order_id.value,
        ),
    )
    buying = entry.side is OrderSide.BUY
    reference = ask if buying else bid
    if stop_loss is not None:
        request.relativeStopLoss = relative_level(
            reference, stop_loss.trigger_price.as_decimal(), below=buying
        )
    if take_profit is not None:
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


def close_position(account_id: int, position_id: int, order: Order) -> oa.ProtoOAClosePositionReq:
    """A reduce-only market order as a close of the position it names."""
    if order.order_type is not OrderType.MARKET or not order.is_reduce_only:
        raise Unsupported("a position is closed by a reduce-only MARKET order")
    _require_supported(order)
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
