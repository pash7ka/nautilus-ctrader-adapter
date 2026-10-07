"""The stage-2 recorded session through the execution client: the node's own brackets on EURUSD.

The node closed its first position itself. The second it opened; a trader then moved its
stop-loss, closed part of it, and later closed the rest, all by hand.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable
from decimal import Decimal

from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.messages import CancelOrder, SubmitOrder
from nautilus_trader.execution.reports import FillReport, OrderStatusReport
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.enums import OrderSide, OrderStatus
from nautilus_trader.model.events import OrderFilled
from nautilus_trader.model.identifiers import ClientOrderId, PositionId, TradeId, VenueOrderId
from nautilus_trader.model.objects import Money, Price, Quantity

from nautilus_ctrader.activity import ACCOUNT_ACTIVITY_TOPIC
from nautilus_ctrader.common.venue_records import Fill, OrderEvent, OrderEventKind
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as om
from tests.execution_replay import as_ours
from tests.execution_venue import (
    EURUSD_ID,
    EURUSD_SYMBOL_ID,
    STRATEGY_ID,
    TRADER_ID,
    ExecutionVenue,
    Harness,
    bracket,
    harness,
    on_fake_account,
    push,
    push_spot,
    status,
    submit_bracket,
    submitted,
)
from tests.fixtures import load_stage2_recording
from tests.polling import wait_until

RECORDING = load_stage2_recording()
FIRST, SECOND = 5_000_001, 5_000_002


def position_events(position_id: int) -> list[oa.ProtoOAExecutionEvent]:
    """The recorded events of one position, made the node's, addressed to the fake account."""
    found = [
        item["message"]
        for item in RECORDING["timeline"]
        if isinstance(item["message"], oa.ProtoOAExecutionEvent)
        and item["message"].order.positionId == position_id
    ]
    return on_fake_account(as_ours(found, [position_id]))


def eurusd_bracket(h: Harness, position_id: int, quantity: str, stop: str, target: str):
    """The node's bracket on EURUSD, under the ids `as_ours` writes for `position_id`."""
    return bracket(
        h,
        entry=f"O-E-{position_id}",
        stop=f"O-SL-{position_id}",
        target=f"O-TP-{position_id}",
        quantity=quantity,
        stop_price=stop,
        target_price=target,
        instrument_id=EURUSD_ID,
    )


def second_bracket(h: Harness):
    return eurusd_bracket(h, SECOND, "2000", "1.12469", "1.12669")


def answers(*replies) -> Callable:
    """A request handler that answers each request with the next of `replies`."""
    left = list(replies)
    return lambda _request: left.pop(0)


async def replay(h: Harness, messages) -> None:
    """Push `messages` one by one, each once Nautilus has applied what the one before sent.

    The recorded events came at least 15 ms apart, time enough for the engine's queue to drain.
    """
    for message in messages:
        await push(h, message)
        await wait_until(lambda: h.engine.evt_qsize() == 0, description="event queue drained")


def fills_by_trade(h: Harness) -> Counter:
    return Counter(
        event.trade_id.value
        for order in h.cache.orders()
        for event in order.events
        if isinstance(event, OrderFilled)
    )


def external(h: Harness, venue_order_id: str):
    return h.cache.order(h.cache.client_order_id(VenueOrderId(venue_order_id)))


async def test_a_partial_close_by_hand_is_in_the_position_when_its_activity_arrives() -> None:
    async with harness(instruments=(EURUSD_ID,)) as h:
        position_id = PositionId(str(SECOND))
        seen: list[Decimal] = []

        def on_activity(activity) -> None:
            if activity.action == "partially_closed":
                seen.append(h.cache.position(position_id).quantity.as_decimal())

        h.client._msgbus.subscribe(topic=ACCOUNT_ACTIVITY_TOPIC, handler=on_activity)
        await submitted(h, second_bracket(h))
        await replay(h, position_events(SECOND))
        await wait_until(lambda: h.cache.position(position_id).is_closed)

        assert seen == [Decimal(1000)]
        (partial,) = [e for e in external(h, "6000006").events if isinstance(e, OrderFilled)]
        assert partial.trade_id == TradeId("7000004")
        assert partial.commission == Money(Decimal("0.03"), USD)
        assert partial.position_id == position_id
        assert fills_by_trade(h) == {"7000003": 1, "7000004": 1, "7000005": 1}
        assert status(h, f"O-SL-{SECOND}") == OrderStatus.CANCELED
        assert status(h, f"O-TP-{SECOND}") == OrderStatus.CANCELED
        assert h.logger.errors() == []


async def test_a_fill_reported_twice_is_applied_once() -> None:
    async with harness(instruments=(EURUSD_ID,)) as h:
        await submitted(h, second_bracket(h))
        events = position_events(SECOND)
        # Up to the partial close's fill.
        await replay(h, events[:6])
        deal = events[5].deal
        fill = Fill(
            trade_id=str(deal.dealId),
            venue_position_id=str(SECOND),
            side="SELL",
            units=Decimal(1000),
            price=Decimal("1.12567"),
            commission=Decimal("-0.03"),
            ts_ms=deal.executionTimestamp,
        )

        h.client._handle_records(
            [OrderEvent(OrderEventKind.FILLED, "6000006", None, fill.ts_ms, fill=fill)],
        )

        assert [type(r).__name__ for r in h.reports][-2:] == ["FillReport", "FillReport"]
        assert fills_by_trade(h)["7000004"] == 1
        assert h.cache.position(PositionId(str(SECOND))).quantity == Quantity.from_str("1000")
        assert h.logger.errors() == []


async def test_the_recorded_session_through_the_nodes_own_commands() -> None:
    first, second = position_events(FIRST), position_events(SECOND)
    execution_venue = ExecutionVenue()
    execution_venue.server.on(om.PROTO_OA_NEW_ORDER_REQ, answers(first[:3], second[:3]))
    # The bracket's correcting amend, then the take-profit's cancel.
    execution_venue.server.on(om.PROTO_OA_AMEND_POSITION_SLTP_REQ, answers(first[3], first[4]))
    execution_venue.server.on(om.PROTO_OA_CLOSE_POSITION_REQ, answers(first[5]))
    async with harness(execution_venue=execution_venue, instruments=(EURUSD_ID,)) as h:
        quantities: list[tuple[str, Decimal]] = []

        def on_activity(activity) -> None:
            position = h.cache.position(PositionId(str(SECOND)))
            quantities.append((activity.action, position.quantity.as_decimal()))

        h.client._msgbus.subscribe(topic=ACCOUNT_ACTIVITY_TOPIC, handler=on_activity)

        # The first bracket, its take-profit cancelled, then the position closed by the node.
        await push_spot(h, 112_569, 112_570, symbol_id=EURUSD_SYMBOL_ID)
        await submit_bracket(h, eurusd_bracket(h, FIRST, "1000", "1.12470", "1.12670"))
        await wait_until(lambda: status(h, f"O-TP-{FIRST}") == OrderStatus.ACCEPTED)
        await wait_until(lambda: len(h.received(oa.ProtoOAAmendPositionSLTPReq)) == 1)
        await h.client._cancel_order(
            CancelOrder(
                trader_id=TRADER_ID,
                strategy_id=STRATEGY_ID,
                instrument_id=EURUSD_ID,
                client_order_id=ClientOrderId(f"O-TP-{FIRST}"),
                venue_order_id=None,
                command_id=UUID4(),
                ts_init=0,
            ),
        )
        await wait_until(lambda: status(h, f"O-TP-{FIRST}") == OrderStatus.CANCELED)
        close = h.factory.market(
            EURUSD_ID,
            OrderSide.SELL,
            Quantity.from_str("1000"),
            reduce_only=True,
            client_order_id=ClientOrderId(f"O-C-{FIRST}"),
        )
        h.cache.add_order(close, position_id=PositionId(str(FIRST)))
        await h.client._submit_order(
            SubmitOrder(
                trader_id=TRADER_ID,
                strategy_id=STRATEGY_ID,
                order=close,
                command_id=UUID4(),
                ts_init=0,
                position_id=PositionId(str(FIRST)),
            ),
        )
        await replay(h, first[6:])
        await wait_until(lambda: h.cache.position(PositionId(str(FIRST))).is_closed)

        # The second bracket; a trader then moves its stop, closes part of it, and the rest.
        await push_spot(h, 112_568, 112_569, symbol_id=EURUSD_SYMBOL_ID)
        await submit_bracket(h, second_bracket(h))
        await wait_until(lambda: status(h, f"O-TP-{SECOND}") == OrderStatus.ACCEPTED)
        await replay(h, second[3:])
        await wait_until(lambda: h.cache.position(PositionId(str(SECOND))).is_closed)

        assert h.kinds_of(f"O-E-{FIRST}") == ["OrderSubmitted", "OrderAccepted", "OrderFilled"]
        assert h.kinds_of(f"O-C-{FIRST}") == ["OrderSubmitted", "OrderAccepted", "OrderFilled"]
        assert h.kinds_of(f"O-TP-{FIRST}") == [
            "OrderSubmitted",
            "OrderAccepted",
            "OrderUpdated",  # the correcting amend
            "OrderCanceled",
        ]
        assert status(h, f"O-SL-{FIRST}") == OrderStatus.CANCELED
        (entry_fill,) = [e for e in h.events_of(f"O-E-{FIRST}") if isinstance(e, OrderFilled)]
        assert entry_fill.last_px == Price.from_str("1.12569")
        assert entry_fill.last_qty == Quantity.from_str("1000")
        assert len(h.received(oa.ProtoOAAmendPositionSLTPReq)) == 2
        assert [(type(r), r.venue_order_id.value) for r in h.reports] == [
            (OrderStatusReport, "6000006"),
            (FillReport, "6000006"),
            (OrderStatusReport, "6000007"),
            (FillReport, "6000007"),
        ]
        assert quantities == [
            ("level_moved", Decimal(2000)),
            ("partially_closed", Decimal(1000)),
            ("closed", Decimal(0)),
        ]
        assert status(h, f"O-SL-{SECOND}") == OrderStatus.CANCELED
        assert status(h, f"O-TP-{SECOND}") == OrderStatus.CANCELED
        assert fills_by_trade(h) == {str(deal): 1 for deal in range(7_000_001, 7_000_006)}
        assert h.logger.errors() == []


async def test_a_close_in_the_same_burst_as_the_entry_fill_waits_for_the_entry() -> None:
    async with harness(instruments=(EURUSD_ID,)) as h:
        position_id = PositionId(str(SECOND))
        seen: list[tuple[str, str, Decimal]] = []
        external_positions: list = []

        def on_activity(activity) -> None:
            position = h.cache.position(position_id)
            seen.append(
                (activity.action, position.strategy_id.value, position.signed_decimal_qty())
            )

        h.client._msgbus.subscribe(topic=ACCOUNT_ACTIVITY_TOPIC, handler=on_activity)
        h.client._msgbus.subscribe(
            topic="events.position.EXTERNAL", handler=external_positions.append
        )
        await submitted(h, second_bracket(h))
        events = position_events(SECOND)

        # The entry's acceptance and fill, then the trader's close of half, in one read.
        for event in (events[0], events[1], events[4], events[5]):
            h.client._on_execution_event(event)
        await wait_until(lambda: seen, description="activity published")
        await wait_until(lambda: h.engine.evt_qsize() == 0, description="event queue drained")

        assert seen == [("partially_closed", "S-001", Decimal(1000))]
        assert external_positions == []
        position = h.cache.position(position_id)
        assert position.strategy_id.value == "S-001"
        assert position.signed_decimal_qty() == Decimal(1000)
        assert fills_by_trade(h) == {"7000003": 1, "7000004": 1}
        assert h.logger.errors() == []


async def test_a_stop_moved_by_hand_is_on_the_leg_when_its_activity_arrives() -> None:
    async with harness(instruments=(EURUSD_ID,)) as h:
        seen: list[tuple[str, Price]] = []

        def on_activity(activity) -> None:
            stop = h.cache.order(ClientOrderId(f"O-SL-{SECOND}"))
            seen.append((activity.action, stop.trigger_price))

        h.client._msgbus.subscribe(topic=ACCOUNT_ACTIVITY_TOPIC, handler=on_activity)
        await submitted(h, second_bracket(h))

        # Filled, protected, and the stop moved by hand, in one read.
        for event in position_events(SECOND)[:4]:
            h.client._on_execution_event(event)
        await wait_until(lambda: seen, description="activity published")

        assert seen == [("level_moved", Price.from_str("1.12384"))]
        assert h.logger.errors() == []
