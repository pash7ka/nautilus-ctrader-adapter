"""The recorded live partial close, made by hand in the broker's terminal, through the client.

A EURUSD position of 3000 with a stop-loss and a take-profit, built by two market orders, was
reduced to 2000 by a close of 1000. Three execution events came: the closing order accepted, its
fill with the deal, then the protective order replaced at the volume left.
"""

from __future__ import annotations

from decimal import Decimal

from google.protobuf.message import Message
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.events import OrderFilled
from nautilus_trader.model.identifiers import (
    ClientOrderId,
    PositionId,
    StrategyId,
    TradeId,
    VenueOrderId,
)
from nautilus_trader.model.orders import Order

from nautilus_ctrader.common.venue_records import Level
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as om
from tests.execution_replay import as_ours, stop_id, target_id
from tests.execution_venue import (
    EURUSD_ID,
    ExecutionVenue,
    Harness,
    harness,
    on_fake_account,
    push,
    started,
)
from tests.fixtures import load_partial_close_recording
from tests.polling import wait_until

LIVE = load_partial_close_recording()
POSITION, PROTECTIVE, CLOSE, CLOSE_DEAL = 5_000_001, 6_000_001, 6_000_012, 7_000_011
# The legs carry the id of the position's later opening order, the one the start adopts.
STOP, TARGET = "6000002-SL", "6000002-TP"
OWN_STOP, OWN_TARGET = stop_id(POSITION), target_id(POSITION)
LEVELS = {Level.STOP_LOSS: Decimal("1.11787"), Level.TAKE_PROFIT: Decimal("1.12988")}
EXTERNAL = StrategyId("EXTERNAL")


def answers(request: str) -> list[Message]:
    """The broker's answer to each `request` of the run, in order, on the fake account."""
    found, asked = [], False
    for item in LIVE["timeline"]:
        if item["kind"] == "marker":
            asked = item["note"] == f"request {request}"
        elif asked and item["note"] == "answer":
            found.append(item["message"])
            asked = False
    return on_fake_account(found)


SNAPSHOTS = answers("ProtoOAReconcileReq")
CLOSE_EVENTS = on_fake_account(
    item["message"]
    for item in LIVE["timeline"]
    if isinstance(item["message"], oa.ProtoOAExecutionEvent)
)


def live_venue() -> ExecutionVenue:
    """The account as the watch found it: the position of 3000 with both levels."""
    venue = ExecutionVenue()
    venue.snapshot = SNAPSHOTS[0]
    venue.position_orders = {
        POSITION: [
            order
            for listed in answers("ProtoOAOrderListByPositionIdReq")[:1]
            for order in listed.order
        ],
    }
    venue.position_deals = {
        POSITION: [
            deal
            for listed in answers("ProtoOADealListByPositionIdReq")
            for deal in listed.deal
            if deal.positionId == POSITION
        ],
    }
    return venue


def by_venue_id(h: Harness, venue_order_id: str | int) -> Order:
    client_order_id = h.cache.client_order_id(VenueOrderId(str(venue_order_id)))
    assert client_order_id is not None, venue_order_id
    return h.cache.order(client_order_id)


def leg_quantities(h: Harness) -> list[Decimal]:
    return [by_venue_id(h, leg).quantity.as_decimal() for leg in (STOP, TARGET)]


def own_leg_quantities(h: Harness) -> list[Decimal]:
    return [
        h.cache.order(ClientOrderId(leg)).quantity.as_decimal() for leg in (OWN_STOP, OWN_TARGET)
    ]


def fills(order: Order) -> list[OrderFilled]:
    return [e for e in order.events if isinstance(e, OrderFilled)]


def test_the_recorded_close_is_three_pushed_execution_events() -> None:
    recorded = [
        item for item in LIVE["timeline"] if isinstance(item["message"], oa.ProtoOAExecutionEvent)
    ]

    assert [(item["kind"], item["note"]) for item in recorded] == [("event", "")] * 3
    accepted, filled, replaced = (item["message"] for item in recorded)
    assert [e.executionType for e in (accepted, filled, replaced)] == [
        om.ORDER_ACCEPTED,
        om.ORDER_FILLED,
        om.ORDER_REPLACED,
    ]
    for event in (accepted, filled):
        assert (event.order.orderId, event.order.orderType) == (CLOSE, om.MARKET)
        assert event.order.closingOrder and event.order.timeInForce == om.IMMEDIATE_OR_CANCEL
        assert event.order.tradeData.volume == 100_000
        assert not event.isServerEvent
    # The acceptance carries the position as it stood; the fill, already reduced.
    assert accepted.position.tradeData.volume == 300_000
    assert filled.deal.filledVolume == 100_000 and filled.deal.HasField("closePositionDetail")
    assert filled.position.tradeData.volume == 200_000
    # The protective order keeps its id, its volume is the total left, and it comes after the fill.
    assert SNAPSHOTS[0].order[0].orderId == replaced.order.orderId == PROTECTIVE
    assert replaced.order.orderType == om.STOP_LOSS_TAKE_PROFIT
    assert replaced.order.tradeData.volume == 200_000
    assert replaced.order.HasField("executedVolume") and replaced.order.executedVolume == 0
    assert replaced.isServerEvent
    assert replaced.position.tradeData.volume == 200_000
    after_ms = replaced.order.utcLastUpdateTimestamp - filled.deal.executionTimestamp
    assert after_ms == 10


async def test_the_recorded_partial_close_of_a_foreign_position_reaches_nautilus() -> None:
    accepted, filled, replaced = CLOSE_EVENTS
    async with harness(execution_venue=live_venue(), instruments=(EURUSD_ID,)) as h:
        await started(h)
        position_id = PositionId(str(POSITION))
        assert h.cache.position(position_id).quantity.as_decimal() == Decimal(3000)
        assert leg_quantities(h) == [Decimal(3000)] * 2

        await push(h, accepted, filled)
        await wait_until(lambda: h.engine.evt_qsize() == 0, description="event queue drained")

        # The fill reduces the position; the legs follow the protective order, not the deal.
        close = by_venue_id(h, CLOSE)
        assert close.strategy_id == EXTERNAL
        assert close.status == OrderStatus.FILLED
        (fill,) = fills(close)
        assert (fill.trade_id, fill.last_qty.as_decimal()) == (TradeId(str(CLOSE_DEAL)), 1000)
        assert fill.position_id == position_id
        assert h.cache.position(position_id).quantity.as_decimal() == Decimal(2000)
        assert leg_quantities(h) == [Decimal(3000)] * 2

        await push(h, replaced)
        await wait_until(
            lambda: leg_quantities(h) == [Decimal(2000)] * 2,
            description="the legs followed the protective order",
        )

        position = h.cache.position(position_id)
        assert position.is_open and position.quantity.as_decimal() == Decimal(2000)
        assert position.strategy_id == EXTERNAL
        for leg in (STOP, TARGET):
            order = by_venue_id(h, leg)
            assert order.status == OrderStatus.ACCEPTED
            assert order.filled_qty.as_decimal() == 0
            assert fills(order) == []
        assert h.client._book.view(POSITION).levels == LEVELS
        assert h.logger.errors() == []


async def test_the_recorded_partial_close_of_the_nodes_position_reaches_nautilus() -> None:
    # Relabelled as the node's, both opening orders carry its records; the node restarts with no
    # cache, so Nautilus rebuilds its orders under the node's ids.
    venue = live_venue()
    (venue.snapshot,) = as_ours([venue.snapshot], [POSITION])
    venue.position_orders = {POSITION: as_ours(venue.position_orders[POSITION], [POSITION])}
    accepted, filled, replaced = as_ours(CLOSE_EVENTS, [POSITION])
    async with harness(execution_venue=venue, instruments=(EURUSD_ID,)) as h:
        await started(h)
        position_id = PositionId(str(POSITION))
        assert own_leg_quantities(h) == [Decimal(3000)] * 2
        seen = len(h.activity)

        await push(h, accepted, filled)
        await wait_until(lambda: h.engine.evt_qsize() == 0, description="event queue drained")

        (fill,) = fills(by_venue_id(h, CLOSE))
        assert (fill.trade_id, fill.last_qty.as_decimal()) == (TradeId(str(CLOSE_DEAL)), 1000)
        assert h.cache.position(position_id).quantity.as_decimal() == Decimal(2000)
        assert own_leg_quantities(h) == [Decimal(3000)] * 2

        await push(h, replaced)
        await wait_until(
            lambda: own_leg_quantities(h) == [Decimal(2000)] * 2,
            description="the legs followed the protective order",
        )

        assert h.cache.position(position_id).quantity.as_decimal() == Decimal(2000)
        for leg in (OWN_STOP, OWN_TARGET):
            order = h.cache.order(ClientOrderId(leg))
            assert order.status == OrderStatus.ACCEPTED
            assert h.kinds_of(leg) == ["OrderUpdated"]
            assert fills(order) == []
        (activity,) = h.activity[seen:]
        assert (activity.action, activity.volume) == ("partially_closed", Decimal(1000))
        assert h.client._book.view(POSITION).levels == LEVELS
        assert h.logger.errors() == []
