"""The recorded live partial close, made by hand in the broker's terminal, through the client.

An EURUSD position of 3000 with a stop-loss and a take-profit, built by two market orders, was
reduced to 2000 by a close of 1000. Three execution events came: the closing order accepted, its
fill with the deal, then the protective order replaced at the volume left.

The position was opened at 2000 and raised by hand to 3000 later, so its order list, newest
first, names the later opening order before the earlier one. The add-on's own events were not
recorded: the tests that play it live build them from the listed orders and deals.
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

from nautilus_ctrader import execution
from nautilus_ctrader.common.reconciliation import PositionHistory, reconcile
from nautilus_ctrader.common.venue_book import VenueBook, entry_of
from nautilus_ctrader.common.venue_records import (
    Action,
    Activity,
    ActivityKind,
    ExternalOrder,
    Level,
    OrderEvent,
    OrderEventKind,
    ReportStatus,
)
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as om
from tests.execution_replay import (
    NoOperations,
    as_ours,
    entry_id,
    make_event,
    stop_id,
    target_id,
)
from tests.execution_venue import (
    EURUSD_ID,
    EURUSD_SYMBOL_ID,
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
# The position's first opening order, and the one that raised it by hand.
OPENING, ADD_ON = 6_000_003, 6_000_002
OPENING_DEAL, ADD_ON_DEAL = 7_000_002, 7_000_001
STOP, TARGET = "6000003-SL", "6000003-TP"
OWN_STOP, OWN_TARGET = stop_id(POSITION), target_id(POSITION)
LEVELS = {Level.STOP_LOSS: Decimal("1.11787"), Level.TAKE_PROFIT: Decimal("1.12988")}
EXTERNAL = StrategyId("EXTERNAL")
NOTHING = NoOperations()


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
LISTED = list(answers("ProtoOAOrderListByPositionIdReq")[0].order)
DEALS = [
    deal
    for listed in answers("ProtoOADealListByPositionIdReq")
    for deal in listed.deal
    if deal.positionId == POSITION
]


def precision(symbol_id: int) -> int | None:
    return 5 if symbol_id == EURUSD_SYMBOL_ID else None


def copied[M: Message](message: M) -> M:
    copy = type(message)()
    copy.CopyFrom(message)
    return copy


def listed(order_id: int) -> om.ProtoOAOrder:
    return copied(next(order for order in LISTED if order.orderId == order_id))


def dealt(deal_id: int) -> om.ProtoOADeal:
    return copied(next(deal for deal in DEALS if deal.dealId == deal_id))


def accepted(order_id: int) -> om.ProtoOAOrder:
    """A listed opening order as it stood when the broker accepted it, before its fill."""
    order = listed(order_id)
    order.orderStatus = om.ORDER_STATUS_ACCEPTED
    order.ClearField("executionPrice")
    order.ClearField("executedVolume")
    order.tradeData.ClearField("closeTimestamp")
    order.utcLastUpdateTimestamp = order.tradeData.openTimestamp
    return order


OPENED_MS = dealt(OPENING_DEAL).executionTimestamp
PROTECTED_MS = SNAPSHOTS[0].order[0].tradeData.openTimestamp
RAISED_MS = SNAPSHOTS[0].order[0].utcLastUpdateTimestamp


def position_at(volume: int, utc: int) -> om.ProtoOAPosition:
    """The recorded position at `volume`; a position of 0 is the one its entry created."""
    position = copied(SNAPSHOTS[0].position[0])
    position.tradeData.volume = volume
    position.utcLastUpdateTimestamp = utc
    if not volume:
        position.positionStatus = om.POSITION_STATUS_CREATED
        position.tradeData.ClearField("openTimestamp")
        position.ClearField("stopLoss")
        position.ClearField("takeProfit")
    return position


def protective_at(volume: int, utc: int) -> om.ProtoOAOrder:
    order = copied(SNAPSHOTS[0].order[0])
    order.tradeData.volume = volume
    order.utcLastUpdateTimestamp = utc
    return order


def opening_events() -> list[oa.ProtoOAExecutionEvent]:
    """The first opening order of 2000 accepted and filled, then its levels set."""
    return [
        make_event(
            om.ORDER_ACCEPTED,
            accepted(OPENING),
            position=position_at(0, listed(OPENING).tradeData.openTimestamp),
        ),
        make_event(
            om.ORDER_FILLED,
            listed(OPENING),
            position=position_at(200_000, OPENED_MS),
            deal=dealt(OPENING_DEAL),
        ),
        make_event(
            om.ORDER_ACCEPTED,
            protective_at(200_000, PROTECTED_MS),
            position=position_at(200_000, PROTECTED_MS),
            server=True,
        ),
    ]


def add_on_events() -> list[oa.ProtoOAExecutionEvent]:
    """The raise by 1000 accepted and filled, then the protective order at the volume reached."""
    return [
        make_event(
            om.ORDER_ACCEPTED, accepted(ADD_ON), position=position_at(200_000, PROTECTED_MS)
        ),
        make_event(
            om.ORDER_FILLED,
            listed(ADD_ON),
            position=position_at(300_000, dealt(ADD_ON_DEAL).executionTimestamp),
            deal=dealt(ADD_ON_DEAL),
        ),
        make_event(
            om.ORDER_REPLACED,
            protective_at(300_000, RAISED_MS),
            position=position_at(300_000, RAISED_MS),
            server=True,
        ),
    ]


def before_the_raise() -> oa.ProtoOAReconcileRes:
    """The snapshot while the position was 2000, before the raise."""
    snapshot = copied(SNAPSHOTS[0])
    snapshot.position[0].CopyFrom(position_at(200_000, PROTECTED_MS))
    snapshot.position[0].price = dealt(OPENING_DEAL).executionPrice
    snapshot.order[0].CopyFrom(protective_at(200_000, PROTECTED_MS))
    return snapshot


def own[M: Message](messages: list[M]) -> list[M]:
    """`messages` as the node's position: the node's records on its first opening order alone."""
    marked = as_ours(messages, [POSITION])
    for message in marked:
        for order in _orders_in(message):
            if order.orderId == ADD_ON:
                order.CopyFrom(_unmarked(order))
    return marked


def _orders_in(message: Message) -> list[om.ProtoOAOrder]:
    if isinstance(message, om.ProtoOAOrder):
        return [message]
    if isinstance(message, oa.ProtoOAExecutionEvent):
        return [message.order]
    return list(message.order)


def _unmarked(order: om.ProtoOAOrder) -> om.ProtoOAOrder:
    plain = copied(order)
    plain.tradeData.ClearField("label")
    plain.tradeData.ClearField("comment")
    plain.ClearField("clientOrderId")
    return plain


def applied(book: VenueBook, events: list[oa.ProtoOAExecutionEvent]) -> list:
    return [record for event in events for record in book.apply(event, NOTHING)]


def grown() -> dict[int, PositionHistory]:
    return {POSITION: PositionHistory(tuple(LISTED), tuple(DEALS))}


def rows(orders) -> list[tuple]:
    return [
        (
            o.venue_order_id,
            o.client_order_id,
            o.status,
            o.units,
            o.filled_units,
            [f.trade_id for f in o.fills],
        )
        for o in orders
    ]


# The venue model and reconciliation, pure


def test_the_entry_of_a_grown_position_is_its_earliest_opening_order() -> None:
    # The broker lists the newest first.
    assert [order.orderId for order in LISTED] == [ADD_ON, OPENING]

    assert entry_of(LISTED).orderId == OPENING
    assert entry_of(LISTED[::-1]).orderId == OPENING


def test_a_grown_position_loaded_at_start_holds_its_earliest_opening_order() -> None:
    book = VenueBook(precision)
    book.load(SNAPSHOTS[0], {POSITION: LISTED})

    assert book.view(POSITION).entry_order_id == OPENING
    # A later opening order seen live adds to the position and changes no entry.
    applied(book, add_on_events()[:2])
    assert book.view(POSITION).entry_order_id == OPENING


def test_reconciliation_reports_every_opening_order_of_a_grown_position() -> None:
    built = reconcile(SNAPSHOTS[0], grown(), (), precision, {}, NOTHING)

    filled, accepted_ = ReportStatus.FILLED, ReportStatus.ACCEPTED
    assert rows(built.orders) == [
        (str(OPENING), None, filled, 2000, 2000, [str(OPENING_DEAL)]),
        (str(ADD_ON), None, filled, 1000, 1000, [str(ADD_ON_DEAL)]),
        (STOP, None, accepted_, 3000, 0, []),
        (TARGET, None, accepted_, 3000, 0, []),
    ]
    (position,) = built.positions
    assert position.units == 3000


def test_a_raise_filled_in_its_entrys_millisecond_is_reported_after_it() -> None:
    # Hand-built: the raise, whose broker id is the lower, filled at the entry's fill time.
    deals = [copied(deal) for deal in DEALS]
    for deal in deals:
        deal.executionTimestamp = OPENED_MS
    history = {POSITION: PositionHistory(tuple(LISTED), tuple(deals))}

    built = reconcile(SNAPSHOTS[0], history, (), precision, {}, NOTHING)

    assert ADD_ON < OPENING
    assert [o.venue_order_id for o in built.orders][:2] == [str(OPENING), str(ADD_ON)]


def test_live_and_reconciliation_name_a_grown_positions_legs_alike() -> None:
    book = VenueBook(precision)
    records = applied(book, opening_events() + add_on_events())

    reported = [r for r in records if isinstance(r, ExternalOrder)]
    assert [r.venue_order_id for r in reported] == [str(OPENING), STOP, TARGET, str(ADD_ON)]
    fills = [r for r in records if isinstance(r, OrderEvent) and r.fill is not None]
    assert [(f.venue_order_id, f.fill.trade_id) for f in fills] == [
        (str(OPENING), str(OPENING_DEAL)),
        (str(ADD_ON), str(ADD_ON_DEAL)),
    ]
    # The legs follow the protective order's volume, raised with the position.
    raised = [r for r in records if isinstance(r, OrderEvent) and r.kind == OrderEventKind.UPDATED]
    assert [(r.venue_order_id, r.quantity) for r in raised] == [(STOP, 3000), (TARGET, 3000)]
    view = book.view(POSITION)
    assert view.entry_order_id == OPENING
    assert view.leg_units == {Level.STOP_LOSS: 3000, Level.TAKE_PROFIT: 3000}

    built = reconcile(SNAPSHOTS[0], grown(), (), precision, {}, NOTHING)
    legs = {r.venue_order_id for r in built.orders if r.reduce_only}
    assert legs == set(view.foreign_legs.values()) == {STOP, TARGET}


def test_a_later_opening_order_seen_first_is_never_taken_for_the_entry() -> None:
    book = VenueBook(precision)
    # Loaded at 2000 with an order list that names no opening order.
    book.load(before_the_raise(), {})

    records = applied(book, add_on_events())

    assert book.view(POSITION).entry_order_id is None
    reported = [r for r in records if isinstance(r, ExternalOrder)]
    assert [(r.venue_order_id, r.reduce_only) for r in reported] == [(str(ADD_ON), False)]
    assert book.view(POSITION).foreign_legs == {}

    # The order list read afterwards names the earliest opening order.
    found = book.entry_found(POSITION, LISTED)
    assert book.view(POSITION).entry_order_id == OPENING
    assert [r.venue_order_id for r in found if isinstance(r, ExternalOrder)] == [STOP, TARGET]
    assert all(r.units == 3000 for r in found if isinstance(r, ExternalOrder))

    # Never seen before, a position first met through its raise is no different.
    fresh = VenueBook(precision)
    applied(fresh, add_on_events()[:2])
    assert fresh.view(POSITION).entry_order_id is None


def test_a_hand_raise_of_the_nodes_position_is_never_its_entry() -> None:
    orders = own(LISTED)
    assert entry_of(orders).orderId == OPENING

    book = VenueBook(precision)
    (snapshot,) = own([SNAPSHOTS[0]])
    book.load(snapshot, {POSITION: orders})
    view = book.view(POSITION)
    assert view.ours and view.entry_order_id == OPENING
    assert view.legs == {
        Level.STOP_LOSS: (OWN_STOP, True),
        Level.TAKE_PROFIT: (OWN_TARGET, True),
    }

    built = reconcile(
        snapshot,
        {POSITION: PositionHistory(tuple(orders), tuple(DEALS))},
        (),
        precision,
        {},
        NOTHING,
    )
    filled, accepted_ = ReportStatus.FILLED, ReportStatus.ACCEPTED
    assert rows(built.orders) == [
        (str(OPENING), entry_id(POSITION), filled, 2000, 2000, [str(OPENING_DEAL)]),
        (str(ADD_ON), None, filled, 1000, 1000, [str(ADD_ON_DEAL)]),
        (STOP, OWN_STOP, accepted_, 3000, 0, []),
        (TARGET, OWN_TARGET, accepted_, 3000, 0, []),
    ]


def test_a_hand_raise_of_the_nodes_position_is_an_external_fill_and_a_manual_change() -> None:
    book = VenueBook(precision)
    (snapshot,) = own([before_the_raise()])
    book.load(snapshot, {POSITION: own([listed(OPENING)])})

    records = applied(book, own(add_on_events()))

    reported = [r for r in records if isinstance(r, ExternalOrder)]
    assert [(r.venue_order_id, r.reduce_only, r.fills) for r in reported] == [
        (str(ADD_ON), False, ())
    ]
    fills = [r for r in records if isinstance(r, OrderEvent) and r.fill is not None]
    assert [(f.venue_order_id, f.client_order_id, f.fill.trade_id) for f in fills] == [
        (str(ADD_ON), None, str(ADD_ON_DEAL)),
    ]
    activity = [r for r in records if isinstance(r, Activity)]
    assert [(a.kind, a.action, a.subject, a.side, a.units) for a in activity] == [
        (ActivityKind.MANUAL_CHANGE, Action.OPENED, "position", "BUY", 1000),
    ]
    raised = [r for r in records if isinstance(r, OrderEvent) and r.kind == OrderEventKind.UPDATED]
    assert [(r.client_order_id, r.quantity) for r in raised] == [
        (OWN_STOP, 3000),
        (OWN_TARGET, 3000),
    ]
    assert book.view(POSITION).entry_order_id == OPENING


# Through the client and a Nautilus engine


def live_venue() -> ExecutionVenue:
    """The account as the watch found it: the position of 3000 with both levels."""
    venue = ExecutionVenue()
    venue.snapshot = SNAPSHOTS[0]
    venue.position_orders = {POSITION: list(LISTED)}
    venue.position_deals = {POSITION: list(DEALS)}
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


def held(h: Harness) -> dict[str, tuple[OrderStatus, Decimal, Decimal]]:
    """Every order Nautilus holds on the instrument: its status, quantity and filled quantity."""
    orders = h.cache.orders(instrument_id=EURUSD_ID)
    found = {
        order.venue_order_id.value: (
            order.status,
            order.quantity.as_decimal(),
            order.filled_qty.as_decimal(),
        )
        for order in orders
    }
    assert len(found) == len(orders), "two orders share a venue order id"
    return found


def client_ids(h: Harness) -> dict[str, str]:
    """The client order id of every order Nautilus holds on the instrument, by venue order id."""
    return {
        order.venue_order_id.value: order.client_order_id.value
        for order in h.cache.orders(instrument_id=EURUSD_ID)
    }


async def reconnected(h: Harness) -> None:
    await h.server.drop_connections()
    await wait_until(lambda: len(h.mass_statuses) == 1, timeout_secs=10)
    await wait_until(lambda: not h.client._outbox, description="records delivered")


GROWN = {
    str(OPENING): (OrderStatus.FILLED, Decimal(2000), Decimal(2000)),
    str(ADD_ON): (OrderStatus.FILLED, Decimal(1000), Decimal(1000)),
    STOP: (OrderStatus.ACCEPTED, Decimal(3000), Decimal(0)),
    TARGET: (OrderStatus.ACCEPTED, Decimal(3000), Decimal(0)),
}


async def test_a_grown_foreign_position_starts_and_reconnects_with_one_set_of_legs() -> None:
    async with harness(execution_venue=live_venue(), instruments=(EURUSD_ID,)) as h:
        await started(h)
        await wait_until(lambda: not h.client._outbox, description="records delivered")
        position_id = PositionId(str(POSITION))

        # Every opening order with its own fill: Nautilus infers no order for the rest.
        assert held(h) == GROWN
        assert h.cache.position(position_id).quantity.as_decimal() == Decimal(3000)

        await reconnected(h)

        assert held(h) == GROWN
        assert h.cache.position(position_id).quantity.as_decimal() == Decimal(3000)
        assert h.logger.errors() == []


async def test_the_nodes_raised_position_starts_and_reconnects_under_the_nodes_ids() -> None:
    venue = live_venue()
    (venue.snapshot,) = own([venue.snapshot])
    venue.position_orders = {POSITION: own(venue.position_orders[POSITION])}
    ours = {
        str(OPENING): entry_id(POSITION),
        STOP: OWN_STOP,
        TARGET: OWN_TARGET,
    }

    def assert_named(h: Harness) -> None:
        assert held(h) == GROWN
        named = client_ids(h)
        # The node's orders under its own ids; only the raise has an id Nautilus made.
        assert {venue_id: named[venue_id] for venue_id in ours} == ours
        assert named[str(ADD_ON)] not in ours.values()
        assert by_venue_id(h, ADD_ON).strategy_id == EXTERNAL
        assert h.cache.position(PositionId(str(POSITION))).quantity.as_decimal() == 3000

    async with harness(execution_venue=venue, instruments=(EURUSD_ID,)) as h:
        await started(h)
        await wait_until(lambda: not h.client._outbox, description="records delivered")
        assert_named(h)

        await reconnected(h)

        assert_named(h)
        assert h.activity == []
        assert h.logger.errors() == []


async def test_an_order_list_that_never_ends_is_an_error(monkeypatch) -> None:
    monkeypatch.setattr(execution, "_MAX_ORDER_PAGES", 2)
    venue = live_venue()
    pages: list[int] = []

    def endless(request: Message) -> Message:
        # Each page one more raise, older than the last, and always more to come.
        pages.append(len(pages))
        order = listed(ADD_ON)
        order.orderId = 6_100_000 + len(pages)
        order.utcLastUpdateTimestamp -= len(pages)
        return oa.ProtoOAOrderListByPositionIdRes(
            ctidTraderAccountId=request.ctidTraderAccountId, order=[order], hasMore=True
        )

    venue.server.on(om.PROTO_OA_ORDER_LIST_BY_POSITION_ID_REQ, endless)
    async with harness(execution_venue=venue, instruments=(EURUSD_ID,)) as h:
        await h.client.generate_mass_status()

        truncated = (
            f"Position {POSITION}: its order list did not end within 2 pages; its entry may "
            "be missing"
        )
        assert pages and set(h.logger.errors()) == {truncated}


async def test_a_foreign_position_raised_live_keeps_its_legs_across_a_reconnect() -> None:
    venue = ExecutionVenue()
    venue.snapshot = before_the_raise()
    venue.position_orders = {POSITION: [listed(OPENING)]}
    venue.position_deals = {POSITION: [dealt(OPENING_DEAL)]}
    async with harness(execution_venue=venue, instruments=(EURUSD_ID,)) as h:
        await started(h)
        position_id = PositionId(str(POSITION))
        assert leg_quantities(h) == [Decimal(2000)] * 2

        await push(h, *on_fake_account(add_on_events()))
        await wait_until(
            lambda: leg_quantities(h) == [Decimal(3000)] * 2,
            description="the legs followed the protective order",
        )
        raise_ = by_venue_id(h, ADD_ON)
        assert raise_.strategy_id == EXTERNAL and raise_.status == OrderStatus.FILLED
        (fill,) = fills(raise_)
        assert fill.trade_id == TradeId(str(ADD_ON_DEAL)) and fill.position_id == position_id
        assert held(h) == GROWN
        assert h.cache.position(position_id).quantity.as_decimal() == Decimal(3000)

        # The broker now lists both opening orders, the newest first.
        venue.snapshot = SNAPSHOTS[0]
        venue.position_orders = {POSITION: list(LISTED)}
        venue.position_deals = {POSITION: list(DEALS)}
        await reconnected(h)

        assert held(h) == GROWN
        assert h.cache.position(position_id).quantity.as_decimal() == Decimal(3000)
        assert h.logger.errors() == []


# The recorded close


def test_the_recorded_close_is_three_pushed_execution_events() -> None:
    recorded = [
        item for item in LIVE["timeline"] if isinstance(item["message"], oa.ProtoOAExecutionEvent)
    ]

    assert [(item["kind"], item["note"]) for item in recorded] == [("event", "")] * 3
    accepted_, filled, replaced = (item["message"] for item in recorded)
    assert [e.executionType for e in (accepted_, filled, replaced)] == [
        om.ORDER_ACCEPTED,
        om.ORDER_FILLED,
        om.ORDER_REPLACED,
    ]
    for event in (accepted_, filled):
        assert (event.order.orderId, event.order.orderType) == (CLOSE, om.MARKET)
        assert event.order.closingOrder and event.order.timeInForce == om.IMMEDIATE_OR_CANCEL
        assert event.order.tradeData.volume == 100_000
        assert not event.isServerEvent
    # The acceptance carries the position as it stood; the fill, already reduced.
    assert accepted_.position.tradeData.volume == 300_000
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
    accepted_, filled, replaced = CLOSE_EVENTS
    async with harness(execution_venue=live_venue(), instruments=(EURUSD_ID,)) as h:
        await started(h)
        position_id = PositionId(str(POSITION))
        assert h.cache.position(position_id).quantity.as_decimal() == Decimal(3000)
        assert leg_quantities(h) == [Decimal(3000)] * 2

        await push(h, accepted_, filled)
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
    # Relabelled as the node's: its first opening order carries the node's records, the raise
    # by hand none. The node restarts with no cache, so Nautilus rebuilds its orders under the
    # node's ids.
    venue = live_venue()
    (venue.snapshot,) = own([venue.snapshot])
    venue.position_orders = {POSITION: own(venue.position_orders[POSITION])}
    accepted_, filled, replaced = own(CLOSE_EVENTS)
    async with harness(execution_venue=venue, instruments=(EURUSD_ID,)) as h:
        await started(h)
        position_id = PositionId(str(POSITION))
        assert own_leg_quantities(h) == [Decimal(3000)] * 2
        entry = h.cache.order(ClientOrderId(entry_id(POSITION)))
        assert entry.venue_order_id == VenueOrderId(str(OPENING))
        assert entry.filled_qty.as_decimal() == Decimal(2000)
        assert by_venue_id(h, ADD_ON).strategy_id == EXTERNAL
        seen = len(h.activity)

        await push(h, accepted_, filled)
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
