"""The whole recorded session through the execution client, as a node would live it.

The first position is made the node's; the second, traded by hand and closed by its
take-profit, and the pending order placed and cancelled by hand stay foreign. The second's levels
reach Nautilus as external legs.
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
from tests.account_venue import ACCOUNT_ID
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
    trader,
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
        # Every other order is reported as it stood before any fill, then each fill and each
        # later change as a report of its own. The foreign position's levels are its legs; its
        # protective order is never reported under its own id.
        assert [(type(r).__name__, r.venue_order_id.value) for r in h.reports] == [
            ("OrderStatusReport", "6000003"),
            ("FillReport", "6000003"),
            ("OrderStatusReport", "6000004"),
            ("FillReport", "6000004"),
            ("OrderStatusReport", "6000004-SL"),
            ("OrderStatusReport", "6000004-TP"),
            *[("OrderStatusReport", "6000004-SL")] * 2,
            *[("OrderStatusReport", "6000004-TP")] * 5,
            ("FillReport", "6000004-TP"),
            ("OrderStatusReport", "6000004-SL"),
            ("OrderStatusReport", "6000006"),
            ("OrderStatusReport", "6000006"),
        ]
        assert h.reports[-1].order_status == OrderStatus.CANCELED
        reported = {}
        for r in h.reports:
            if isinstance(r, OrderStatusReport):
                reported.setdefault(r.venue_order_id.value, r)
        assert reported["6000004"].order_type == OrderType.MARKET
        assert not reported["6000004"].reduce_only
        for venue_order_id, order_type in (
            ("6000004-SL", OrderType.STOP_MARKET),
            ("6000004-TP", OrderType.LIMIT),
        ):
            leg = reported[venue_order_id]
            assert leg.order_type == order_type
            assert leg.reduce_only
            assert leg.venue_position_id == PositionId(str(SECOND))
            assert leg.parent_order_id is None and not leg.linked_order_ids
        assert reported["6000006"].order_type == OrderType.LIMIT
        assert not reported["6000006"].reduce_only
        assert reported["6000006"].venue_position_id == PositionId(str(PENDING))
        # The triggered level carries the deal's id and commission, not a fill Nautilus made up.
        target = external(h, "6000004-TP")
        assert target.status == OrderStatus.FILLED
        assert target.price == Price.from_str("85179.30")
        (fill,) = [e for e in target.events if isinstance(e, OrderFilled)]
        assert fill.trade_id == TradeId("7000005")
        assert fill.commission == Money(Decimal("27.69"), USD)
        stop = external(h, "6000004-SL")
        assert stop.status == OrderStatus.CANCELED
        assert stop.trigger_price == Price.from_str("85031.14")
        assert external(h, "6000005") is None
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

        assert h.kinds_of(ENTRY) == ["OrderSubmitted", "OrderAccepted", "OrderFilled"]
        assert h.kinds_of(TARGET) == ["OrderSubmitted", "OrderAccepted", "OrderCanceled"]
        stop = h.cache.order(h.cache.client_order_id(VenueOrderId("6000001-SL")))
        assert stop.client_order_id.value == STOP
        assert stop.trigger_price == Price.from_str("85206.20")
        assert h.received(oa.ProtoOAAmendPositionSLTPReq) == []
        assert h.cache.position(PositionId(str(FIRST))).is_closed
        assert len(h.client._brackets) == 0
        assert h.logger.errors() == []


async def test_an_account_event_right_behind_the_trader_read_is_applied() -> None:
    execution_venue = ExecutionVenue()
    execution_venue.trader = trader(balance=100_000, balanceVersion=10)
    pushed = trader(balance=123_456, balanceVersion=11).trader
    execution_venue.server.on(
        om.PROTO_OA_TRADER_REQ,
        lambda _r: [
            execution_venue.trader,
            oa.ProtoOATraderUpdatedEvent(ctidTraderAccountId=ACCOUNT_ID, trader=pushed),
        ],
    )
    async with harness(execution_venue=execution_venue) as h:
        assert h.states[-1].balances[0].total == Money(Decimal("1234.56"), USD)
        assert all(state.base_currency == USD for state in h.states)
        assert h.logger.errors() == []
