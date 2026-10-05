"""Nautilus's order queries, answered from the broker's own lists.

Nautilus queries every order in flight and resolves one that stays unanswered on its own. An
answer with fills reaches it as a one-order mass status, so it never infers a fill of its own.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Iterable
from decimal import Decimal

from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.messages import GenerateOrderStatusReport, QueryOrder
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.enums import OrderSide, OrderStatus
from nautilus_trader.model.events import OrderFilled, OrderPendingCancel
from nautilus_trader.model.identifiers import (
    ClientOrderId,
    InstrumentId,
    PositionId,
    Symbol,
    TradeId,
    VenueOrderId,
)
from nautilus_trader.model.objects import Money, Price, Quantity

from nautilus_ctrader.common import order_record
from nautilus_ctrader.common.order_record import LegIds
from nautilus_ctrader.constants import CTRADER_VENUE
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as om
from tests.execution_replay import FIRST, make_order
from tests.execution_venue import (
    CLOSE,
    ENTRY,
    FIRST_EVENTS,
    OURS,
    STOP,
    STRATEGY_ID,
    TARGET,
    TRADER_ID,
    US100_ID,
    US100_SYMBOL_ID,
    ExecutionVenue,
    Harness,
    bracket,
    close_sent,
    exec_config,
    harness,
    node_close_events,
    our_market_position,
    push,
    push_spot,
    serve,
    started,
    status,
    submit_bracket,
    submitted,
)
from tests.polling import wait_until

OPEN_AT = 358.0  # FIRST open with 0.99 after a manual partial close, both levels set
# The recorded entry filled at 85287.21: an ask there gives the recorded distances.
BID, ASK = 8_528_600_000, 8_528_721_000
MARKET_ID = "O-M-1"
MARKET_ORDER, MARKET_POSITION = 6_100_001, 5_100_001
MINUTE_MS = 60_000
# A new order's answer is given up on quickly, so a test can lose it.
LOSING = exec_config(order_request_timeout_secs=0.2)


def now_ms() -> int:
    return int(time.time() * 1000)


def query(h: Harness, client_order_id: str) -> QueryOrder:
    order = h.cache.order(ClientOrderId(client_order_id))
    return QueryOrder(
        trader_id=TRADER_ID,
        strategy_id=STRATEGY_ID,
        instrument_id=US100_ID if order is None else order.instrument_id,
        client_order_id=ClientOrderId(client_order_id),
        venue_order_id=None,
        command_id=UUID4(),
        ts_init=0,
    )


def report_command(client_order_id: str) -> GenerateOrderStatusReport:
    return GenerateOrderStatusReport(
        instrument_id=US100_ID,
        client_order_id=ClientOrderId(client_order_id),
        venue_order_id=None,
        command_id=UUID4(),
        ts_init=0,
    )


def debug_lines(h: Harness) -> list[str]:
    return [message for level, message in h.logger.lines if level == "debug"]


def trade_ids(h: Harness, client_order_id: str) -> list[TradeId]:
    """The trade id of every fill the cache holds for the order, an inferred one included."""
    order = h.cache.order(ClientOrderId(client_order_id))
    return [e.trade_id for e in order.events if isinstance(e, OrderFilled)]


def recent(orders: Iterable[om.ProtoOAOrder], *, at: int) -> list[om.ProtoOAOrder]:
    """Copies of `orders` last changed at `at`, so the order list's window holds them."""
    copies = []
    for order in orders:
        copy = om.ProtoOAOrder()
        copy.CopyFrom(order)
        copy.utcLastUpdateTimestamp = at
        copies.append(copy)
    return copies


async def lost_bracket(h: Harness) -> None:
    """The node's bracket sent, its answer never coming."""
    await push_spot(h, BID, ASK)
    await submit_bracket(h, bracket(h))
    assert status(h, ENTRY) == OrderStatus.SUBMITTED


def filled_at_the_broker(venue: ExecutionVenue) -> None:
    """The bracket's entry filled at the broker, as its lists show it now."""
    serve(venue, OPEN_AT)
    venue.orders = recent(venue.position_orders[FIRST], at=now_ms() - MINUTE_MS)


def market_entry(*, order_status: int, utc: int) -> om.ProtoOAOrder:
    """The node's market order `MARKET_ID` as the broker lists it (hand-built)."""
    order = make_order(
        MARKET_ORDER,
        MARKET_POSITION,
        utc=utc,
        label=order_record.encode_label(MARKET_ID),
        comment=order_record.encode_comment(LegIds(None, None)),
        client_order_id=MARKET_ID,
        symbol=US100_SYMBOL_ID,
    )
    order.orderStatus = order_status
    return order


async def market_submitted(h: Harness, client_order_id: str = MARKET_ID, instrument_id=US100_ID):
    order = h.factory.market(
        instrument_id,
        OrderSide.BUY,
        Quantity.from_str("1.00"),
        client_order_id=ClientOrderId(client_order_id),
    )
    h.cache.add_order(order)
    h.client.generate_order_submitted(STRATEGY_ID, instrument_id, order.client_order_id, 0)
    await wait_until(lambda: status(h, client_order_id) == OrderStatus.SUBMITTED)


async def test_query_for_a_filled_entry_brings_the_real_fill() -> None:
    venue = ExecutionVenue()
    async with harness(execution_venue=venue, config=LOSING) as h:
        await lost_bracket(h)
        filled_at_the_broker(venue)

        await h.client._query_order(query(h, ENTRY))

        await wait_until(lambda: status(h, ENTRY) == OrderStatus.FILLED)
        assert trade_ids(h, ENTRY) == [TradeId("7000001")]
        (filled,) = [
            e for e in h.cache.order(ClientOrderId(ENTRY)).events if isinstance(e, OrderFilled)
        ]
        assert filled.commission == Money(Decimal("27.72"), USD)
        (built,) = h.mass_statuses
        assert list(built.order_reports) == [VenueOrderId("6000001")]
        assert h.reports == []


async def test_a_generated_report_with_fills_finds_its_order_already_reconciled() -> None:
    venue = ExecutionVenue()
    async with harness(execution_venue=venue, config=LOSING) as h:
        await lost_bracket(h)
        filled_at_the_broker(venue)

        report = await h.client.generate_order_status_report(report_command(ENTRY))
        # What the engine does with its own targeted query's report: no fills of its own.
        assert h.engine._reconcile_order_report(report, trades=[])

        assert report.order_status == OrderStatus.FILLED
        assert status(h, ENTRY) == OrderStatus.FILLED
        assert trade_ids(h, ENTRY) == [TradeId("7000001")]
        assert len(h.mass_statuses) == 1


async def test_query_for_a_pending_entry_answers_accepted() -> None:
    venue = ExecutionVenue()
    async with harness(execution_venue=venue) as h:
        await market_submitted(h)
        venue.snapshot.order.append(
            market_entry(order_status=om.ORDER_STATUS_ACCEPTED, utc=now_ms())
        )

        await h.client._query_order(query(h, MARKET_ID))

        await wait_until(lambda: status(h, MARKET_ID) == OrderStatus.ACCEPTED)
        (report,) = h.reports
        assert report.client_order_id == ClientOrderId(MARKET_ID)
        assert report.venue_order_id == VenueOrderId(str(MARKET_ORDER))
        assert h.mass_statuses == []
        # Found among the pending orders, so the order list is never asked.
        assert h.received(oa.ProtoOAOrderListReq) == []


async def test_query_for_a_rejected_entry_answers_rejected() -> None:
    venue = ExecutionVenue()
    async with harness(execution_venue=venue) as h:
        await market_submitted(h)
        venue.orders = [
            market_entry(order_status=om.ORDER_STATUS_REJECTED, utc=now_ms() - MINUTE_MS)
        ]

        await h.client._query_order(query(h, MARKET_ID))

        await wait_until(lambda: status(h, MARKET_ID) == OrderStatus.REJECTED)
        (report,) = h.reports
        assert report.order_status == OrderStatus.REJECTED
        assert report.filled_qty == Quantity.from_str("0.00")
        assert trade_ids(h, MARKET_ID) == []
        assert h.mass_statuses == []


async def test_a_filled_entry_whose_fills_are_not_listed_is_not_answered() -> None:
    venue = ExecutionVenue()
    async with harness(execution_venue=venue) as h:
        await market_submitted(h)
        venue.orders = [market_entry(order_status=om.ORDER_STATUS_FILLED, utc=now_ms())]

        await h.client._query_order(query(h, MARKET_ID))

        # A filled report without them would make Nautilus infer a fill.
        assert h.reports == []
        assert h.mass_statuses == []
        assert status(h, MARKET_ID) == OrderStatus.SUBMITTED


async def test_query_for_a_live_leg_answers_accepted_at_the_broker_level() -> None:
    venue = ExecutionVenue()
    serve(venue, OPEN_AT)
    async with harness(execution_venue=venue) as h:
        await started(h)
        stop = h.cache.order(ClientOrderId(STOP))
        h.engine.process(
            OrderPendingCancel(
                TRADER_ID,
                STRATEGY_ID,
                US100_ID,
                stop.client_order_id,
                stop.venue_order_id,
                h.client.account_id,
                UUID4(),
                0,
                0,
            ),
        )
        await wait_until(lambda: status(h, STOP) == OrderStatus.PENDING_CANCEL)
        venue.snapshot.position[0].stopLoss = 85150.0

        await h.client._query_order(query(h, STOP))

        await wait_until(lambda: status(h, STOP) == OrderStatus.ACCEPTED)
        assert h.cache.order(ClientOrderId(STOP)).trigger_price == Price.from_str("85150.00")
        (report,) = h.reports
        assert report.venue_order_id == VenueOrderId("6000001-SL")
        assert h.mass_statuses == []


async def test_query_for_a_leg_whose_entry_is_in_flight_sends_nothing() -> None:
    venue = ExecutionVenue()
    async with harness(execution_venue=venue, config=LOSING) as h:
        await lost_bracket(h)
        # Even with the broker holding the position: the model has no level to look at yet.
        filled_at_the_broker(venue)
        asked = len(h.server.received)

        await h.client._query_order(query(h, STOP))
        assert await h.client.generate_order_status_report(report_command(TARGET)) is None

        assert len(h.server.received) == asked
        assert h.reports == []
        assert h.mass_statuses == []
        assert status(h, STOP) == OrderStatus.SUBMITTED
        in_flight = [line for line in debug_lines(h) if "entry is still in flight" in line]
        assert len(in_flight) == 2


async def test_query_for_a_matched_close_answers_from_its_position() -> None:
    now = now_ms()
    venue = ExecutionVenue()
    our_market_position(venue, opened=now - 2 * MINUTE_MS)
    accepted, _ = node_close_events(closed=now - MINUTE_MS)
    venue.server.on(om.PROTO_OA_CLOSE_POSITION_REQ, lambda _r: accepted)
    async with harness(execution_venue=venue) as h:
        await started(h)
        await asyncio.wait_for(await close_sent(h), timeout=10)
        assert status(h, CLOSE) == OrderStatus.ACCEPTED
        # The fill's event never came; the broker's lists hold it.
        our_market_position(venue, opened=now - 2 * MINUTE_MS, closed=now - MINUTE_MS)

        await h.client._query_order(query(h, CLOSE))

        await wait_until(lambda: status(h, CLOSE) == OrderStatus.FILLED)
        assert trade_ids(h, CLOSE) == [TradeId("7300002")]
        assert h.cache.position(PositionId(str(OURS))).is_closed
        (built,) = h.mass_statuses
        assert list(built.order_reports) == [VenueOrderId("6300002")]


async def test_unanswerable_queries_send_nothing() -> None:
    venue = ExecutionVenue()
    async with harness(execution_venue=venue) as h:
        # The broker lists nothing of it.
        await market_submitted(h)
        await market_submitted(
            h, "O-M-2", instrument_id=InstrumentId(Symbol("EURUSD"), CTRADER_VENUE)
        )
        for client_order_id in (MARKET_ID, "O-M-2", "O-M-9"):
            await h.client._query_order(query(h, client_order_id))
            assert (
                await h.client.generate_order_status_report(report_command(client_order_id)) is None
            )
        await h.client._disconnect()
        await h.client._query_order(query(h, MARKET_ID))

        assert h.reports == []
        assert h.mass_statuses == []
        assert status(h, MARKET_ID) == OrderStatus.SUBMITTED
        lines = debug_lines(h)
        for client_order_id, reason in (
            (MARKET_ID, "not found"),
            ("O-M-2", "not loaded"),
            ("O-M-9", "not found"),
            (MARKET_ID, "not connected"),
        ):
            assert any(client_order_id in line and reason in line for line in lines), reason


async def test_protective_and_closing_orders_are_never_taken_for_the_entry() -> None:
    venue = ExecutionVenue()
    async with harness(execution_venue=venue, config=LOSING) as h:
        await lost_bracket(h)
        serve(venue, OPEN_AT)
        at = now_ms() - MINUTE_MS
        listed = [*venue.position_orders[FIRST], *venue.snapshot.order]
        (entry,) = [o for o in listed if o.orderId == 6000001]
        others = recent((o for o in listed if o.orderId != 6000001), at=at - 1_000)
        for order in others:
            order.clientOrderId = ENTRY
            order.tradeData.label = order_record.encode_label(ENTRY)
        assert any(o.orderType == om.STOP_LOSS_TAKE_PROFIT for o in others)
        assert any(o.closingOrder for o in others)
        venue.orders = [*others, *recent([entry], at=at)]

        await h.client._query_order(query(h, ENTRY))

        await wait_until(lambda: status(h, ENTRY) == OrderStatus.FILLED)
        assert h.cache.order(ClientOrderId(ENTRY)).venue_order_id == VenueOrderId("6000001")
        assert trade_ids(h, ENTRY) == [TradeId("7000001")]
        (built,) = h.mass_statuses
        assert list(built.order_reports) == [VenueOrderId("6000001")]


async def test_a_fill_after_the_in_flight_check_gave_up_still_opens_our_position() -> None:
    async with harness() as h:
        await submitted(h)
        # What Nautilus does once its queries of the entry go unanswered.
        h.client.generate_order_rejected(STRATEGY_ID, US100_ID, ClientOrderId(ENTRY), "UNKNOWN", 0)
        await wait_until(lambda: status(h, ENTRY) == OrderStatus.REJECTED)

        await push(h, *FIRST_EVENTS[:2])

        await wait_until(lambda: h.cache.position(PositionId(str(FIRST))) is not None)
        position = h.cache.position(PositionId(str(FIRST)))
        assert position.opening_order_id == ClientOrderId(ENTRY)
        assert position.quantity == Quantity.from_str("1.00")
        assert status(h, ENTRY) == OrderStatus.REJECTED
