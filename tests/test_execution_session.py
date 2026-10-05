"""The whole recorded session through the execution client, as a node would live it.

The first position is made the node's; the second, traded by hand and closed by its
take-profit, and the pending order placed and cancelled by hand stay foreign.
"""

from __future__ import annotations

from decimal import Decimal

from nautilus_trader.execution.reports import OrderStatusReport
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.enums import OrderStatus, OrderType
from nautilus_trader.model.events import OrderFilled
from nautilus_trader.model.identifiers import PositionId, TradeId, VenueOrderId
from nautilus_trader.model.objects import Money, Price

from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as om
from tests.execution_replay import FIRST, PENDING, SECOND, as_ours, events
from tests.execution_venue import (
    ENTRY,
    FIRST_EVENTS,
    STOP,
    TARGET,
    ExecutionVenue,
    bracket,
    harness,
    on_us100,
    push,
    push_spot,
    status,
    submit_bracket,
    submitted,
    sync,
)
from tests.polling import wait_until

SESSION = on_us100(as_ours(events(), [FIRST]))


def external(h, venue_order_id: str):
    client_order_id = h.cache.client_order_id(VenueOrderId(venue_order_id))
    return None if client_order_id is None else h.cache.order(client_order_id)


def settled(h) -> bool:
    pending = external(h, "6000006")
    second = h.cache.position(PositionId(str(SECOND)))
    return (
        pending is not None
        and pending.status == OrderStatus.CANCELED
        and second is not None
        and second.is_closed
    )


async def test_the_recorded_session_reaches_nautilus_as_the_venue_told_it() -> None:
    async with harness() as h:
        await submitted(h)
        await push(h, *SESSION)
        await wait_until(lambda: settled(h))

        # The node's own position, step by step as in the client's tests.
        assert status(h, STOP) == OrderStatus.FILLED
        assert status(h, TARGET) == OrderStatus.CANCELED
        assert h.cache.position(PositionId(str(FIRST))).is_closed
        # Every other order is reported once, as it stood before any fill, then followed.
        assert [(type(r).__name__, r.venue_order_id.value) for r in h.reports] == [
            ("OrderStatusReport", "6000003"),
            ("OrderStatusReport", "6000004"),
            ("OrderStatusReport", "6000005"),
            ("FillReport", "6000005"),
            ("OrderStatusReport", "6000006"),
        ]
        reported = {
            r.venue_order_id.value: r for r in h.reports if isinstance(r, OrderStatusReport)
        }
        assert reported["6000004"].order_type == OrderType.MARKET
        assert not reported["6000004"].reduce_only
        target = reported["6000005"]
        assert target.order_type == OrderType.LIMIT
        assert target.price == Price.from_str("85179.30")
        assert target.reduce_only
        assert target.venue_position_id == PositionId(str(SECOND))
        assert reported["6000006"].order_type == OrderType.LIMIT
        assert not reported["6000006"].reduce_only
        assert reported["6000006"].venue_position_id == PositionId(str(PENDING))
        # A foreign close carries the deal's id and commission, not a fill Nautilus made up.
        closing = external(h, "6000005")
        assert closing.status == OrderStatus.FILLED
        (fill,) = [e for e in closing.events if isinstance(e, OrderFilled)]
        assert fill.trade_id == TradeId("7000005")
        assert fill.commission == Money(Decimal("27.69"), USD)
        assert external(h, "6000004").status == OrderStatus.FILLED
        # Only the node's own position has a trader acting on it.
        assert len(h.activity) == 5
        assert h.logger.errors() == []


async def test_the_session_fed_twice_changes_nothing_the_second_time() -> None:
    async with harness() as h:
        await submitted(h)
        await push(h, *SESSION)
        await wait_until(lambda: settled(h))
        before = (len(h.events), len(h.reports), len(h.activity), len(h.states))

        await push(h, *SESSION)
        await sync(h)

        assert (len(h.events), len(h.reports), len(h.activity), len(h.states)) == before
        assert h.logger.errors() == []


async def test_a_bracket_the_node_sends_and_a_trader_then_handles_ends_as_recorded() -> None:
    execution_venue = ExecutionVenue()
    execution_venue.server.on(om.PROTO_OA_NEW_ORDER_REQ, lambda _r: FIRST_EVENTS[:3])
    async with harness(execution_venue=execution_venue) as h:
        await push_spot(h, 8_528_600_000, 8_528_721_000)
        await submit_bracket(h, bracket(h))
        await wait_until(lambda: status(h, TARGET) == OrderStatus.ACCEPTED)
        await push(h, *FIRST_EVENTS[3:])
        await wait_until(lambda: status(h, STOP) == OrderStatus.FILLED)

        # The venue answers with the acceptance and pushes the fill right behind it; the fill can
        # be applied first, and the late acceptance then has nothing new to report.
        assert h.kinds_of(ENTRY) in (
            ["OrderSubmitted", "OrderFilled"],
            ["OrderSubmitted", "OrderAccepted", "OrderFilled"],
        )
        assert h.kinds_of(TARGET) == ["OrderSubmitted", "OrderAccepted", "OrderCanceled"]
        stop = h.cache.order(h.cache.client_order_id(VenueOrderId("6000001-SL")))
        assert stop.client_order_id.value == STOP
        assert stop.trigger_price == Price.from_str("85206.20")
        assert h.received(oa.ProtoOAAmendPositionSLTPReq) == []
        assert h.cache.position(PositionId(str(FIRST))).is_closed
        assert len(h.client._brackets) == 0
        assert h.logger.errors() == []
