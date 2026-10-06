"""The recorded execution session, and hand-built events, as input for the venue model's tests.

Every position in the recording was opened by hand, so none carries the node's record.
`as_ours` writes on the chosen positions' entry orders the records the node would have written,
and the entry's client order id on their protective orders (the broker copies it there), and
changes nothing else.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence

from nautilus_ctrader.common import order_record
from nautilus_ctrader.common.order_record import LegIds
from nautilus_ctrader.common.reconciliation import PositionHistory
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as om
from tests.fixtures import load_execution_recording

RECORDING = load_execution_recording()
FIRST, SECOND, PENDING = 5_000_001, 5_000_002, 5_000_003
SYMBOL = 324  # traded with 2-decimal prices


def entry_id(position_id: int) -> str:
    return f"O-E-{position_id}"


def stop_id(position_id: int) -> str:
    return f"O-SL-{position_id}"


def target_id(position_id: int) -> str:
    return f"O-TP-{position_id}"


def precision(symbol_id: int) -> int | None:
    return 2 if symbol_id == SYMBOL else None


class NoOperations:
    """Nothing in flight: every client action seen is somebody else's."""

    def amending(self, position_id: int) -> bool:
        return False

    def closing(self, position_id: int, volume: int, created_ms: int, order_id: int) -> str | None:
        return None


def events() -> list[oa.ProtoOAExecutionEvent]:
    return [
        item["message"]
        for item in RECORDING["timeline"]
        if isinstance(item["message"], oa.ProtoOAExecutionEvent)
    ]


def _orders_in(message) -> list[om.ProtoOAOrder]:
    if isinstance(message, om.ProtoOAOrder):
        return [message]
    if isinstance(message, oa.ProtoOAExecutionEvent):
        return [message.order] if message.HasField("order") else []
    # A reconcile response or an order list.
    return list(message.order) if hasattr(message, "order") else []


def _mark(order: om.ProtoOAOrder) -> None:
    position_id = order.positionId
    if order.orderType == om.STOP_LOSS_TAKE_PROFIT:
        order.clientOrderId = entry_id(position_id)
    elif not order.closingOrder:
        order.clientOrderId = entry_id(position_id)
        order.tradeData.label = order_record.encode_label(entry_id(position_id))
        order.tradeData.comment = order_record.encode_comment(
            LegIds(stop_id(position_id), target_id(position_id)),
        )


def as_ours(messages: Iterable, position_ids: Iterable[int]) -> list:
    """Copies of `messages` with the given positions' orders carrying the node's records."""
    wanted = set(position_ids)
    marked = []
    for message in messages:
        copy = type(message)()
        copy.CopyFrom(message)
        for order in _orders_in(copy):
            if order.positionId in wanted:
                _mark(order)
        marked.append(copy)
    return marked


def snapshot_with_protection(t: float) -> oa.ProtoOAReconcileRes:
    """The snapshot taken with protection orders at timeline time `t`."""
    for item in RECORDING["timeline"]:
        if item["t"] == t and "returnProtectionOrders=true" in item["note"]:
            return item["message"]
    raise LookupError(f"no snapshot with protection orders at {t}")


def position_orders() -> dict[int, list[om.ProtoOAOrder]]:
    """Each recorded position's own order list, as the closing by-position requests returned."""
    found: dict[int, list[om.ProtoOAOrder]] = {}
    for response in RECORDING["closing"]["position_orders"]:
        if response.order:
            found[response.order[0].positionId] = list(response.order)
    return found


def history(position_id: int, *, until_ms: int | None = None) -> PositionHistory:
    """A position's recorded order and deal lists, as they stood at `until_ms`.

    An order last changed, or a deal executed, after `until_ms` is left out.
    """
    orders = [
        order
        for response in RECORDING["closing"]["position_orders"]
        for order in response.order
        if order.positionId == position_id
        and (until_ms is None or order.utcLastUpdateTimestamp <= until_ms)
    ]
    deals = [
        deal
        for response in RECORDING["closing"]["position_deals"]
        for deal in response.deal
        if deal.positionId == position_id
        and (until_ms is None or deal.executionTimestamp <= until_ms)
    ]
    return PositionHistory(tuple(orders), tuple(deals))


def window_deals(*position_ids: int, until_ms: int | None = None) -> tuple[om.ProtoOADeal, ...]:
    """The account's recorded deals of the given positions, as they stood at `until_ms`."""
    return tuple(
        deal
        for response in RECORDING["closing"]["account_deals"]
        for deal in response.deal
        if deal.positionId in position_ids
        and (until_ms is None or deal.executionTimestamp <= until_ms)
    )


def snapshot_at(t: float) -> oa.ProtoOAReconcileRes:
    """The snapshot of the last pair taken at or before timeline time `t` that lists orders.

    Snapshots come in pairs, the second asked with protection orders. When neither of the pair
    taken by `t` lists an order, the last of them is returned.
    """
    taken = [
        item
        for item in RECORDING["timeline"]
        if item["t"] <= t and isinstance(item["message"], oa.ProtoOAReconcileRes)
    ]
    if not taken:
        raise LookupError(f"no snapshot at or before {t}")
    second = "returnProtectionOrders=true" in taken[-1]["note"]
    pair = taken[-2:] if second else taken[-1:]
    listing = [item for item in pair if item["message"].order]
    return (listing or pair)[-1]["message"]


# Hand-built messages, for what the recording did not hold.


def make_order(
    order_id: int,
    position_id: int,
    *,
    order_type: int = om.MARKET,
    side: int = om.BUY,
    volume: int = 100,
    closing: bool = False,
    utc: int = 1,
    stop: float | None = None,
    limit: float | None = None,
    label: str = "",
    comment: str = "",
    client_order_id: str = "",
    stop_out: bool = False,
    symbol: int = SYMBOL,
) -> om.ProtoOAOrder:
    order = om.ProtoOAOrder(
        orderId=order_id,
        orderType=order_type,
        orderStatus=om.ORDER_STATUS_ACCEPTED,
        closingOrder=closing,
        utcLastUpdateTimestamp=utc,
        positionId=position_id,
        clientOrderId=client_order_id,
    )
    order.tradeData.symbolId = symbol
    order.tradeData.volume = volume
    order.tradeData.tradeSide = side
    if label:
        order.tradeData.label = label
    if comment:
        order.tradeData.comment = comment
    if stop is not None:
        order.stopPrice = stop
    if limit is not None:
        order.limitPrice = limit
    if stop_out:
        order.isStopOut = True
    return order


def make_position(
    position_id: int,
    *,
    side: int = om.BUY,
    volume: int = 100,
    status: int = om.POSITION_STATUS_OPEN,
    symbol: int = SYMBOL,
) -> om.ProtoOAPosition:
    # `swap` is required on the wire, so a fake server can send the position.
    position = om.ProtoOAPosition(positionId=position_id, positionStatus=status, swap=0)
    position.tradeData.symbolId = symbol
    position.tradeData.volume = volume
    position.tradeData.tradeSide = side
    return position


def make_deal(
    deal_id: int,
    order_id: int,
    position_id: int,
    *,
    side: int,
    volume: int,
    price: float,
    ts: int,
    commission: int = 0,
    money_digits: int = 2,
) -> om.ProtoOADeal:
    return om.ProtoOADeal(
        dealId=deal_id,
        orderId=order_id,
        positionId=position_id,
        volume=volume,
        filledVolume=volume,
        symbolId=SYMBOL,
        executionPrice=price,
        tradeSide=side,
        executionTimestamp=ts,
        commission=commission,
        moneyDigits=money_digits,
        # Required on the wire, so a fake server can send the deal.
        createTimestamp=ts,
        dealStatus=om.FILLED,
    )


def make_event(
    kind: int,
    order: om.ProtoOAOrder,
    *,
    position: om.ProtoOAPosition | None = None,
    deal: om.ProtoOADeal | None = None,
    server: bool = False,
    error: str = "",
) -> oa.ProtoOAExecutionEvent:
    event = oa.ProtoOAExecutionEvent(
        ctidTraderAccountId=1_000_001,
        executionType=kind,
        isServerEvent=server,
    )
    event.order.CopyFrom(order)
    if position is not None:
        event.position.CopyFrom(position)
    if deal is not None:
        event.deal.CopyFrom(deal)
    if error:
        event.errorCode = error
    return event


def our_entry(position_id: int, order_id: int, *, side: int = om.BUY, utc: int = 1, **kw):
    """The node's entry order: its records in `label` and `comment`, legs for both levels."""
    return make_order(
        order_id,
        position_id,
        side=side,
        utc=utc,
        label=order_record.encode_label(entry_id(position_id)),
        comment=order_record.encode_comment(LegIds(stop_id(position_id), target_id(position_id))),
        client_order_id=entry_id(position_id),
        **kw,
    )


def first_n(messages: Sequence, position_id: int) -> list:
    """The events of one position, in recorded order."""
    return [m for m in messages if m.HasField("order") and m.order.positionId == position_id]
