"""Nautilus's order queries, answered from the broker's own lists.

Nautilus queries every order in flight and resolves one that stays unanswered on its own. An
answer with fills reaches it as a one-order mass status, so it never infers a fill of its own.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Iterable
from decimal import Decimal

import pytest
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
from nautilus_trader.model.orders import Order

from nautilus_ctrader.common import order_record
from nautilus_ctrader.common.order_record import LegIds
from nautilus_ctrader.constants import CTRADER_VENUE
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as om
from tests.execution_replay import FIRST, make_deal, make_order, make_position
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
    answer_closes,
    bracket,
    close_sent,
    exec_config,
    harness,
    node_close_events,
    on_us100,
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


# The broker's clock runs this far behind the node's in the tests that skew them.
BROKER_BEHIND_MS = 60_000


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
    answer_closes(venue, behind_ms=BROKER_BEHIND_MS)
    async with harness(execution_venue=venue) as h:
        await started(h)
        await asyncio.wait_for(await close_sent(h), timeout=10)
        assert status(h, CLOSE) == OrderStatus.ACCEPTED
        # The fill's event never came; the broker's lists hold it.
        our_market_position(venue, opened=now - 2 * MINUTE_MS, closed=now_ms() - BROKER_BEHIND_MS)

        await h.client._query_order(query(h, CLOSE))

        await wait_until(lambda: status(h, CLOSE) == OrderStatus.FILLED)
        assert trade_ids(h, CLOSE) == [TradeId("7300002")]
        assert h.cache.position(PositionId(str(OURS))).is_closed
        (built,) = h.mass_statuses
        assert list(built.order_reports) == [VenueOrderId("6300002")]


async def test_query_for_a_close_in_flight_answers_under_the_node_id() -> None:
    now = now_ms()
    venue = ExecutionVenue()
    our_market_position(venue, opened=now - 2 * MINUTE_MS)
    # The close's answer never comes; the query arrives while it is awaited.
    config = exec_config(order_request_timeout_secs=2.0)
    async with harness(execution_venue=venue, config=config) as h:
        await started(h)
        closing = await close_sent(h)
        # The broker executed it, its clock a minute behind the node's; its lists hold the fill.
        closed = now_ms() - BROKER_BEHIND_MS
        our_market_position(venue, opened=now - 2 * MINUTE_MS, closed=closed)

        await h.client._query_order(query(h, CLOSE))

        await wait_until(lambda: status(h, CLOSE) == OrderStatus.FILLED)
        assert trade_ids(h, CLOSE) == [TradeId("7300002")]
        (built,) = h.mass_statuses
        assert built.order_reports[VenueOrderId("6300002")].client_order_id == ClientOrderId(CLOSE)
        assert h.cache.client_order_id(VenueOrderId("6300002")) == ClientOrderId(CLOSE)
        assert h.cache.position(PositionId(str(OURS))).is_closed
        # The broker's events of the close come late, while it is still in flight.
        await push(h, *node_close_events(closed=closed))
        await asyncio.wait_for(closing, timeout=10)
        assert trade_ids(h, CLOSE) == [TradeId("7300002")]
        assert not any(a.kind == "manual_change" for a in h.activity)
        assert h.reports == []


async def test_a_close_named_by_a_query_keeps_its_late_events_after_the_timeout() -> None:
    now = now_ms()
    venue = ExecutionVenue()
    our_market_position(venue, opened=now - 2 * MINUTE_MS)
    config = exec_config(order_request_timeout_secs=1.5)
    async with harness(execution_venue=venue, config=config) as h:
        await started(h)
        closing = await close_sent(h)
        closed = now_ms() - BROKER_BEHIND_MS
        our_market_position(venue, opened=now - 2 * MINUTE_MS, closed=closed)
        await h.client._query_order(query(h, CLOSE))
        await wait_until(lambda: status(h, CLOSE) == OrderStatus.FILLED)
        # The close's answer never came; it is no longer in flight.
        await asyncio.wait_for(closing, timeout=10)
        assert h.client._operations.close_position(CLOSE) is None

        await push(h, *node_close_events(closed=closed))

        await wait_until(lambda: "OrderFilled" in h.kinds_of(CLOSE))
        assert trade_ids(h, CLOSE) == [TradeId("7300002")]
        assert h.reports == []
        assert not any(a.kind == "manual_change" for a in h.activity)


async def test_query_for_a_close_in_flight_never_claims_an_earlier_close() -> None:
    now = now_ms()
    venue = ExecutionVenue()
    our_market_position(venue, opened=now - 2 * MINUTE_MS)
    config = exec_config(order_request_timeout_secs=2.0)
    async with harness(execution_venue=venue, config=config) as h:
        await started(h)
        # A trader's close of the same volume, made before a spot the node saw, then the node's.
        await push_spot(h, BID, ASK, timestamp=now - MINUTE_MS // 2)
        closing = await close_sent(h)
        our_market_position(venue, opened=now - 2 * MINUTE_MS, closed=now - MINUTE_MS)

        await h.client._query_order(query(h, CLOSE))

        assert h.mass_statuses == []
        assert h.reports == []
        assert status(h, CLOSE) == OrderStatus.SUBMITTED
        assert any(CLOSE in line and "not found" in line for line in debug_lines(h))
        await asyncio.wait_for(closing, timeout=10)


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


async def test_an_entry_answered_filled_settles_its_bracket() -> None:
    venue = ExecutionVenue()
    async with harness(execution_venue=venue, config=LOSING) as h:
        await push_spot(h, BID, ASK)
        # At the levels the broker holds at `OPEN_AT`, so no correcting amend is needed.
        await submit_bracket(h, bracket(h, stop_price="85200.20", target_price="85353.42"))
        filled_at_the_broker(venue)

        await h.client._query_order(query(h, ENTRY))
        await wait_until(lambda: status(h, ENTRY) == OrderStatus.FILLED)
        assert len(h.client._brackets) == 0

        await h.client._query_order(query(h, STOP))

        await wait_until(lambda: status(h, STOP) == OrderStatus.ACCEPTED)
        assert h.cache.order(ClientOrderId(STOP)).trigger_price == Price.from_str("85200.20")
        (report,) = h.reports
        assert report.venue_order_id == VenueOrderId("6000001-SL")
        assert h.received(oa.ProtoOAAmendPositionSLTPReq) == []


async def test_a_partly_filled_pending_entry_is_answered_with_its_fill() -> None:
    at = now_ms() - MINUTE_MS
    venue = ExecutionVenue()
    async with harness(execution_venue=venue) as h:
        await market_submitted(h)
        entry = market_entry(order_status=om.ORDER_STATUS_ACCEPTED, utc=at)
        entry.executedVolume = 50
        position = make_position(MARKET_POSITION, volume=50, symbol=US100_SYMBOL_ID)
        position.price = 85000.0
        position.utcLastUpdateTimestamp = at
        deal = make_deal(
            7_100_001, MARKET_ORDER, MARKET_POSITION, side=om.BUY, volume=50, price=85000.0, ts=at
        )
        venue.snapshot.order.append(entry)
        venue.snapshot.position.append(position)
        venue.position_orders = {MARKET_POSITION: [entry]}
        venue.position_deals = {MARKET_POSITION: on_us100([deal])}

        await h.client._query_order(query(h, MARKET_ID))

        await wait_until(lambda: status(h, MARKET_ID) == OrderStatus.PARTIALLY_FILLED)
        assert trade_ids(h, MARKET_ID) == [TradeId("7100001")]
        assert h.cache.order(ClientOrderId(MARKET_ID)).filled_qty == Quantity.from_str("0.50")
        (built,) = h.mass_statuses
        assert list(built.order_reports) == [VenueOrderId(str(MARKET_ORDER))]


async def test_an_entry_answered_filled_leaves_a_known_position_unrebuilt() -> None:
    venue = ExecutionVenue()
    config = exec_config(order_request_timeout_secs=0.2, protective_order_timeout_secs=30.0)
    async with harness(execution_venue=venue, config=config) as h:
        await lost_bracket(h)
        # The model learns the position from its events; the protective order has not come.
        await push(h, *FIRST_EVENTS[:2])
        assert all(alive for _, alive in h.client._book.view(FIRST).legs.values())
        filled_at_the_broker(venue)
        rebuilds: list[None] = []
        load = h.client._load
        h.client._load = lambda: (rebuilds.append(None), load())[1]

        await h.client._query_order(query(h, ENTRY))

        assert len(h.mass_statuses) == 1
        assert rebuilds == []
        assert all(alive for _, alive in h.client._book.view(FIRST).legs.values())
        assert len(h.client._brackets) == 1


# -- Orders Nautilus holds under an id of its own: looked up by venue order id -----------------

SL, TP = "6000001-SL", "6000001-TP"
CLOSED_AT = 408.8  # FIRST closed by its stop-loss, nothing open
# A trader's resting limit buy, hand-built; Nautilus learns it from the start's reconciliation.
RESTING, RESTING_POSITION = 6_800_001, 5_800_001


def held(h: Harness, venue_order_id: str | int) -> Order:
    client_order_id = h.cache.client_order_id(VenueOrderId(str(venue_order_id)))
    assert client_order_id is not None, venue_order_id
    return h.cache.order(client_order_id)


async def asked_to_cancel(h: Harness, venue_order_id: str | int) -> QueryOrder:
    """Put the order in `PENDING_CANCEL`; return the query Nautilus then sends, with both ids."""
    order = held(h, venue_order_id)
    h.engine.process(
        OrderPendingCancel(
            TRADER_ID,
            order.strategy_id,
            order.instrument_id,
            order.client_order_id,
            order.venue_order_id,
            h.client.account_id,
            UUID4(),
            0,
            0,
        ),
    )
    await wait_until(lambda: held(h, venue_order_id).status == OrderStatus.PENDING_CANCEL)
    return QueryOrder(
        trader_id=TRADER_ID,
        strategy_id=order.strategy_id,
        instrument_id=order.instrument_id,
        client_order_id=order.client_order_id,
        venue_order_id=order.venue_order_id,
        command_id=UUID4(),
        ts_init=0,
    )


def never_walked(h: Harness) -> None:
    assert h.received(oa.ProtoOAOrderListReq) == []


def details_asked(h: Harness) -> list[int]:
    return [request.orderId for request in h.received(oa.ProtoOAOrderDetailsReq)]


def foreign_venue() -> ExecutionVenue:
    venue = ExecutionVenue()
    serve(venue, OPEN_AT, mine=False)
    return venue


@pytest.mark.parametrize(
    ("stop_loss", "trigger"),
    [pytest.param(None, "85200.20", id="standing"), pytest.param(85150.0, "85150.00", id="moved")],
)
async def test_query_for_a_foreign_leg_answers_its_level_at_the_broker(
    stop_loss: float | None, trigger: str
) -> None:
    venue = foreign_venue()
    async with harness(execution_venue=venue) as h:
        await started(h)
        asked = await asked_to_cancel(h, SL)
        if stop_loss is not None:
            venue.snapshot.position[0].stopLoss = stop_loss

        await h.client._query_order(asked)

        await wait_until(lambda: held(h, SL).status == OrderStatus.ACCEPTED)
        stop = held(h, SL)
        assert stop.client_order_id == asked.client_order_id
        assert stop.trigger_price == Price.from_str(trigger)
        assert stop.quantity == Quantity.from_str("0.99")
        (report,) = h.reports
        assert report.venue_order_id == VenueOrderId(SL)
        assert report.client_order_id == asked.client_order_id
        assert h.mass_statuses == []
        never_walked(h)
        assert details_asked(h) == []
        assert h.logger.errors() == []


async def test_query_for_a_foreign_leg_whose_level_is_gone_answers_canceled() -> None:
    venue = foreign_venue()
    async with harness(execution_venue=venue) as h:
        await started(h)
        asked = await asked_to_cancel(h, TP)
        venue.snapshot.position[0].ClearField("takeProfit")

        await h.client._query_order(asked)

        await wait_until(lambda: held(h, TP).status == OrderStatus.CANCELED)
        # At the price Nautilus held: the broker lists none for a level gone.
        assert held(h, TP).price == Price.from_str("85353.42")
        (report,) = h.reports
        assert report.client_order_id == asked.client_order_id
        assert held(h, SL).status == OrderStatus.ACCEPTED
        never_walked(h)
        assert details_asked(h) == []
        assert h.logger.errors() == []


@pytest.mark.parametrize("rebuilt", [False, True], ids=["known", "rebuilt"])
async def test_query_for_a_foreign_leg_whose_position_closed_answers_its_closing(
    rebuilt: bool,
) -> None:
    venue = foreign_venue()
    async with harness(execution_venue=venue) as h:
        await started(h)
        asked_stop = await asked_to_cancel(h, SL)
        asked_target = await asked_to_cancel(h, TP)
        # The stop-loss triggered; its events never came.
        serve(venue, CLOSED_AT, mine=False)
        if rebuilt:
            # A model rebuilt since holds no closed position: the entry's details name it.
            await h.client._load()

        await h.client._query_order(asked_stop)
        await h.client._query_order(asked_target)

        await wait_until(lambda: held(h, SL).status == OrderStatus.FILLED)
        await wait_until(lambda: held(h, TP).status == OrderStatus.CANCELED)
        assert held(h, SL).trade_ids == [TradeId("7000003")]
        assert h.cache.position(PositionId(str(FIRST))).is_closed
        (built,) = h.mass_statuses
        assert list(built.order_reports) == [VenueOrderId(SL)]
        assert [report.venue_order_id for report in h.reports] == [VenueOrderId(TP)]
        assert details_asked(h) == ([6000001, 6000001] if rebuilt else [])
        never_walked(h)
        assert h.logger.errors() == []


def resting(*, status: int = om.ORDER_STATUS_ACCEPTED) -> om.ProtoOAOrder:
    order = make_order(
        RESTING,
        RESTING_POSITION,
        order_type=om.LIMIT,
        utc=now_ms() - MINUTE_MS,
        limit=84000.0,
        volume=200,
        symbol=US100_SYMBOL_ID,
    )
    order.orderStatus = status
    return order


async def test_query_for_a_pending_external_order_answers_from_the_snapshot() -> None:
    venue = ExecutionVenue()
    venue.snapshot.order.append(resting())
    async with harness(execution_venue=venue) as h:
        await started(h)
        asked = await asked_to_cancel(h, RESTING)
        # Moved by the trader meanwhile.
        venue.snapshot.order[0].limitPrice = 84100.0

        await h.client._query_order(asked)

        await wait_until(lambda: held(h, RESTING).status == OrderStatus.ACCEPTED)
        assert held(h, RESTING).price == Price.from_str("84100.00")
        (report,) = h.reports
        assert report.client_order_id == asked.client_order_id
        generated = await h.client.generate_order_status_report(
            GenerateOrderStatusReport(
                instrument_id=US100_ID,
                client_order_id=asked.client_order_id,
                venue_order_id=asked.venue_order_id,
                command_id=UUID4(),
                ts_init=0,
            ),
        )
        assert generated.order_status == OrderStatus.ACCEPTED
        assert generated.client_order_id == asked.client_order_id
        never_walked(h)
        assert details_asked(h) == []
        assert h.logger.errors() == []


async def test_query_for_a_pending_external_order_gone_answers_from_its_details() -> None:
    venue = ExecutionVenue()
    venue.snapshot.order.append(resting())
    async with harness(execution_venue=venue) as h:
        await started(h)
        asked = await asked_to_cancel(h, RESTING)
        # Cancelled at the broker; the answer to the cancel never came.
        del venue.snapshot.order[:]
        venue.orders = [resting(status=om.ORDER_STATUS_CANCELLED)]

        await h.client._query_order(asked)

        await wait_until(lambda: held(h, RESTING).status == OrderStatus.CANCELED)
        (report,) = h.reports
        assert report.client_order_id == asked.client_order_id
        assert details_asked(h) == [RESTING]
        never_walked(h)
        assert h.mass_statuses == []
        assert h.logger.errors() == []


def walked(h: Harness) -> bool:
    return bool(h.received(oa.ProtoOAOrderListReq))


def details_fail(venue: ExecutionVenue, how: str) -> None:
    """The order details request fails: with an error, or as the venue holds no such order."""
    if how == "error":
        venue.fail.add(om.PROTO_OA_ORDER_DETAILS_REQ)


DETAILS_FAIL = [pytest.param("error", id="details-error"), pytest.param("unknown", id="unknown-id")]


async def market_numbered(h: Harness) -> None:
    """The node's market order `MARKET_ID`, accepted under its broker order id."""
    await market_submitted(h)
    h.client.generate_order_accepted(
        STRATEGY_ID, US100_ID, ClientOrderId(MARKET_ID), VenueOrderId(str(MARKET_ORDER)), 0
    )
    await wait_until(lambda: status(h, MARKET_ID) == OrderStatus.ACCEPTED)


async def test_an_own_numbered_entry_that_filled_is_answered_from_its_details() -> None:
    venue = ExecutionVenue()
    async with harness(execution_venue=venue, config=LOSING) as h:
        await push_spot(h, BID, ASK)
        # At the levels the broker holds at `OPEN_AT`, so no correcting amend is needed.
        await submit_bracket(h, bracket(h, stop_price="85200.20", target_price="85353.42"))
        # The broker numbered the entry and filled it; no event of it came.
        h.client.generate_order_accepted(
            STRATEGY_ID, US100_ID, ClientOrderId(ENTRY), VenueOrderId("6000001"), 0
        )
        await wait_until(lambda: status(h, ENTRY) == OrderStatus.ACCEPTED)
        serve(venue, OPEN_AT)

        await h.client._query_order(query(h, ENTRY))

        await wait_until(lambda: status(h, ENTRY) == OrderStatus.FILLED)
        assert trade_ids(h, ENTRY) == [TradeId("7000001")]
        (built,) = h.mass_statuses
        assert built.order_reports[VenueOrderId("6000001")].client_order_id == ClientOrderId(ENTRY)
        assert details_asked(h) == [6000001]
        never_walked(h)
        # The bracket is settled from the rebuilt model.
        assert len(h.client._brackets) == 0
        assert h.logger.errors() == []


async def test_an_own_numbered_order_cancelled_is_answered_from_its_details() -> None:
    venue = ExecutionVenue()
    async with harness(execution_venue=venue) as h:
        await market_numbered(h)
        venue.orders = [
            market_entry(order_status=om.ORDER_STATUS_CANCELLED, utc=now_ms() - MINUTE_MS)
        ]

        await h.client._query_order(query(h, MARKET_ID))

        await wait_until(lambda: status(h, MARKET_ID) == OrderStatus.CANCELED)
        (report,) = h.reports
        assert report.client_order_id == ClientOrderId(MARKET_ID)
        assert details_asked(h) == [MARKET_ORDER]
        never_walked(h)
        assert h.mass_statuses == []


async def test_a_numbered_order_whose_details_fail_is_found_in_the_order_list() -> None:
    venue = ExecutionVenue()
    async with harness(execution_venue=venue) as h:
        await market_numbered(h)
        details_fail(venue, "error")
        venue.orders = [
            market_entry(order_status=om.ORDER_STATUS_CANCELLED, utc=now_ms() - MINUTE_MS)
        ]

        await h.client._query_order(query(h, MARKET_ID))

        await wait_until(lambda: status(h, MARKET_ID) == OrderStatus.CANCELED)
        (report,) = h.reports
        assert report.client_order_id == ClientOrderId(MARKET_ID)
        assert details_asked(h) == [MARKET_ORDER]
        assert walked(h)
        assert any("details could not be read" in line for line in debug_lines(h))


async def test_a_numbered_order_found_nowhere_is_not_answered() -> None:
    venue = ExecutionVenue()
    async with harness(execution_venue=venue) as h:
        await market_numbered(h)
        details_fail(venue, "error")

        await h.client._query_order(query(h, MARKET_ID))

        assert walked(h)
        assert h.reports == []
        assert h.mass_statuses == []
        assert status(h, MARKET_ID) == OrderStatus.ACCEPTED
        assert any(MARKET_ID in line and "not found" in line for line in debug_lines(h))
        assert h.logger.errors() == []


@pytest.mark.parametrize("how", DETAILS_FAIL)
async def test_a_pending_external_order_found_nowhere_is_not_answered(how: str) -> None:
    venue = ExecutionVenue()
    venue.snapshot.order.append(resting())
    async with harness(execution_venue=venue) as h:
        await started(h)
        asked = await asked_to_cancel(h, RESTING)
        del venue.snapshot.order[:]
        details_fail(venue, how)

        await h.client._query_order(asked)

        assert details_asked(h) == [RESTING]
        assert walked(h)
        assert h.reports == []
        assert held(h, RESTING).status == OrderStatus.PENDING_CANCEL
        lines = debug_lines(h)
        assert any("details could not be read" in line for line in lines)
        assert any(str(asked.client_order_id) in line and "not found" in line for line in lines)
        assert h.logger.errors() == []


@pytest.mark.parametrize("how", DETAILS_FAIL)
async def test_a_foreign_leg_whose_entry_details_fail_is_not_answered(how: str) -> None:
    venue = foreign_venue()
    async with harness(execution_venue=venue) as h:
        await started(h)
        asked = await asked_to_cancel(h, SL)
        serve(venue, CLOSED_AT, mine=False)
        await h.client._load()
        venue.position_orders = {}
        details_fail(venue, how)

        await h.client._query_order(asked)

        assert details_asked(h) == [6000001]
        assert h.reports == []
        assert h.mass_statuses == []
        assert held(h, SL).status == OrderStatus.PENDING_CANCEL
        assert any(
            str(asked.client_order_id) in line and "not answered" in line for line in debug_lines(h)
        )
        never_walked(h)
        assert h.logger.errors() == []


async def test_query_for_a_later_generation_of_a_foreign_leg_answers_that_generation() -> None:
    later = "6000001-SL-2"
    venue = foreign_venue()
    async with harness(execution_venue=venue) as h:
        await started(h)
        first = held(h, SL)
        # Nautilus holds the stop-loss's first leg ended, so the level standing is a new leg.
        h.client.generate_order_canceled(
            first.strategy_id, first.instrument_id, first.client_order_id, first.venue_order_id, 0
        )
        await wait_until(lambda: held(h, SL).status == OrderStatus.CANCELED)
        await started(h)
        await wait_until(lambda: h.cache.client_order_id(VenueOrderId(later)) is not None)
        asked = await asked_to_cancel(h, later)
        venue.snapshot.position[0].stopLoss = 85150.0

        await h.client._query_order(asked)

        await wait_until(lambda: held(h, later).status == OrderStatus.ACCEPTED)
        assert held(h, later).trigger_price == Price.from_str("85150.00")
        assert [report.venue_order_id for report in h.reports] == [VenueOrderId(later)]
        assert h.reports[0].client_order_id == asked.client_order_id
        assert held(h, SL).status == OrderStatus.CANCELED
        never_walked(h)
        assert details_asked(h) == []
        assert h.logger.errors() == []
