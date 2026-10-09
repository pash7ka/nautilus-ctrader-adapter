"""The recorded live run of commands on orders a person placed by hand, through the client.

A pending EURUSD limit order was amended twice and cancelled, its details then read; a cancel
and a details request named an order id that does not exist. The stop-loss of an open position
was moved and its take-profit removed, after which the broker moved the trailing stop-loss. The
fake venue gives each request the broker's recorded answer.
"""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal

from google.protobuf.message import Message
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.messages import CancelOrder, ModifyOrder, QueryOrder
from nautilus_trader.execution.reports import OrderStatusReport
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.events import (
    OrderCancelRejected,
    OrderPendingCancel,
    OrderUpdated,
)
from nautilus_trader.model.identifiers import StrategyId, VenueOrderId
from nautilus_trader.model.objects import Price, Quantity
from nautilus_trader.model.orders import Order

from nautilus_ctrader.common.venue_book import VenueBook
from nautilus_ctrader.common.venue_records import Level
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as om
from tests.execution_replay import NoOperations
from tests.execution_venue import (
    EURUSD_ID,
    EURUSD_SYMBOL_ID,
    STRATEGY_ID,
    TRADER_ID,
    ExecutionVenue,
    Harness,
    harness,
    on_fake_account,
    push,
    started,
)
from tests.fake_server import Pushed
from tests.fixtures import load_external_commands_recording
from tests.polling import wait_until

LIVE = load_external_commands_recording()
POSITION, PENDING = 5_000_001, 6_000_002
STOP, TARGET = "6000003-SL", "6000003-TP"
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


def pushed(cls: type[Message]) -> list[Message]:
    """The events of type `cls` the broker pushed during the run, on the fake account."""
    return on_fake_account(
        item["message"] for item in LIVE["timeline"] if isinstance(item["message"], cls)
    )


SNAPSHOTS = answers("ProtoOAReconcileReq")
ORDER_AMENDS = answers("ProtoOAAmendOrderReq")
CANCELS = answers("ProtoOACancelOrderReq")
DETAILS = answers("ProtoOAOrderDetailsReq")
LEVEL_AMENDS = answers("ProtoOAAmendPositionSLTPReq")
TRAILED = pushed(oa.ProtoOATrailingSLChangedEvent)


def live_venue() -> ExecutionVenue:
    """The account as the run found it: the position with both levels, the pending order."""
    venue = ExecutionVenue()
    venue.snapshot = SNAPSHOTS[0]
    venue.position_orders = {
        POSITION: [
            order
            for listed in answers("ProtoOAOrderListByPositionIdReq")
            for order in listed.order
            if order.positionId == POSITION
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


def replay(recorded: list[Message], sent: list[Message]) -> Callable[[Message], Message]:
    """A request handler giving each request the next recorded answer; keeps each in `sent`."""
    left = list(recorded)

    def answer(request: Message) -> Message:
        sent.append(request)
        return left.pop(0)

    return answer


def by_venue_id(h: Harness, venue_order_id: str | int) -> Order:
    client_order_id = h.cache.client_order_id(VenueOrderId(str(venue_order_id)))
    assert client_order_id is not None, venue_order_id
    return h.cache.order(client_order_id)


def modify(order: Order, **change) -> ModifyOrder:
    return ModifyOrder(
        trader_id=TRADER_ID,
        strategy_id=STRATEGY_ID,
        instrument_id=EURUSD_ID,
        client_order_id=order.client_order_id,
        venue_order_id=None,
        quantity=change.get("quantity"),
        price=change.get("price"),
        trigger_price=change.get("trigger_price"),
        command_id=UUID4(),
        ts_init=0,
    )


def cancel(order: Order) -> CancelOrder:
    return CancelOrder(
        trader_id=TRADER_ID,
        strategy_id=STRATEGY_ID,
        instrument_id=EURUSD_ID,
        client_order_id=order.client_order_id,
        venue_order_id=None,
        command_id=UUID4(),
        ts_init=0,
    )


def sent_about(h: Harness, order: Order) -> list:
    """The events the client itself sent Nautilus about `order`."""
    return [e for e in h.events if e.client_order_id == order.client_order_id]


def reports_of(h: Harness, venue_order_id: str | int) -> list[OrderStatusReport]:
    return [
        r
        for r in h.reports
        if isinstance(r, OrderStatusReport)
        and r.venue_order_id == VenueOrderId(str(venue_order_id))
    ]


async def asked_to_cancel(h: Harness, order: Order) -> QueryOrder:
    """Put `order` in `PENDING_CANCEL`; return the query Nautilus then sends about it."""
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
    await wait_until(lambda: by_venue_id(h, PENDING).status == OrderStatus.PENDING_CANCEL)
    return QueryOrder(
        trader_id=TRADER_ID,
        strategy_id=order.strategy_id,
        instrument_id=order.instrument_id,
        client_order_id=order.client_order_id,
        venue_order_id=order.venue_order_id,
        command_id=UUID4(),
        ts_init=0,
    )


def stop_moves(h: Harness) -> list[Price]:
    """The trigger price of each `OrderUpdated` of the stop-loss leg."""
    return [e.trigger_price for e in by_venue_id(h, STOP).events if isinstance(e, OrderUpdated)]


def debug_lines(h: Harness) -> list[str]:
    return [message for level, message in h.logger.lines if level == "debug"]


async def test_the_recorded_amends_and_cancel_of_a_pending_order_reach_nautilus_as_reports() -> (
    None
):
    venue = live_venue()
    amends: list = []
    cancels: list = []
    venue.server.on(om.PROTO_OA_AMEND_ORDER_REQ, replay(ORDER_AMENDS, amends))
    venue.server.on(om.PROTO_OA_CANCEL_ORDER_REQ, replay(CANCELS[:1], cancels))
    async with harness(execution_venue=venue, instruments=(EURUSD_ID,)) as h:
        await started(h)
        order = by_venue_id(h, PENDING)
        assert order.strategy_id == EXTERNAL
        assert order.price == Price.from_str("1.11000")
        assert order.quantity.as_decimal() == Decimal(1000)

        await h.client._modify_order(modify(order, price=Price.from_str("1.10990")))
        await wait_until(lambda: by_venue_id(h, PENDING).price == Price.from_str("1.10990"))
        await h.client._modify_order(modify(order, quantity=Quantity.from_int(2000)))
        await wait_until(lambda: by_venue_id(h, PENDING).quantity.as_decimal() == Decimal(2000))
        await h.client._cancel_order(cancel(order))
        await wait_until(lambda: by_venue_id(h, PENDING).status == OrderStatus.CANCELED)

        # Each amend sends the order's levels again as the distances it holds them in.
        assert [(a.limitPrice, a.volume) for a in amends] == [(1.1099, 100_000), (1.1099, 200_000)]
        for amend in amends:
            assert amend.relativeStopLoss == 200
            assert amend.relativeTakeProfit == 1001
            assert not amend.HasField("stopLoss") and not amend.HasField("takeProfit")
            assert amend.trailingStopLoss
        assert [c.orderId for c in cancels] == [PENDING]
        kinds = [type(e).__name__ for e in by_venue_id(h, PENDING).events]
        assert kinds[-3:] == ["OrderUpdated", "OrderUpdated", "OrderCanceled"]
        assert reports_of(h, PENDING)[-1].order_status == OrderStatus.CANCELED
        assert sent_about(h, order) == []
        assert h.client._book.open_order(PENDING) is None
        assert h.logger.errors() == []


async def test_the_recorded_refusal_of_a_cancel_is_a_cancel_rejected() -> None:
    venue = live_venue()
    refusal = CANCELS[1]
    assert isinstance(refusal, oa.ProtoOAOrderErrorEvent)
    venue.server.on(om.PROTO_OA_CANCEL_ORDER_REQ, lambda _r: refusal)
    async with harness(execution_venue=venue, instruments=(EURUSD_ID,)) as h:
        await started(h)
        order = by_venue_id(h, PENDING)

        await h.client._cancel_order(cancel(order))
        await wait_until(lambda: sent_about(h, order))

        (rejected,) = sent_about(h, order)
        assert isinstance(rejected, OrderCancelRejected)
        assert rejected.reason.startswith("ORDER_NOT_FOUND")
        assert rejected.strategy_id == EXTERNAL
        assert by_venue_id(h, PENDING).status == OrderStatus.ACCEPTED


async def test_the_recorded_details_of_the_cancelled_order_answer_its_query() -> None:
    venue = live_venue()
    ended = DETAILS[0]
    assert ended.order.orderStatus == om.ORDER_STATUS_CANCELLED and not ended.deal
    asked: list = []
    venue.server.on(om.PROTO_OA_ORDER_DETAILS_REQ, replay([ended], asked))
    async with harness(execution_venue=venue, instruments=(EURUSD_ID,)) as h:
        await started(h)
        query = await asked_to_cancel(h, by_venue_id(h, PENDING))
        # Cancelled at the broker; the answer to the cancel never came.
        venue.snapshot = SNAPSHOTS[-1]

        await h.client._query_order(query)

        await wait_until(lambda: by_venue_id(h, PENDING).status == OrderStatus.CANCELED)
        assert [request.orderId for request in asked] == [PENDING]
        assert reports_of(h, PENDING)[-1].client_order_id == query.client_order_id
        assert h.received(oa.ProtoOAOrderListReq) == []
        assert h.logger.errors() == []


async def test_the_recorded_details_refusal_of_an_unknown_order_leaves_the_query_unanswered() -> (
    None
):
    venue = live_venue()
    refusal = DETAILS[1]
    assert isinstance(refusal, oa.ProtoOAErrorRes) and refusal.errorCode == "ORDER_NOT_FOUND"
    venue.server.on(om.PROTO_OA_ORDER_DETAILS_REQ, lambda _r: refusal)
    async with harness(execution_venue=venue, instruments=(EURUSD_ID,)) as h:
        await started(h)
        query = await asked_to_cancel(h, by_venue_id(h, PENDING))
        venue.snapshot = SNAPSHOTS[-1]
        before = len(h.reports)

        await h.client._query_order(query)

        # The order list over the fill window is searched next, and holds nothing.
        assert h.received(oa.ProtoOAOrderListReq)
        assert len(h.reports) == before
        assert by_venue_id(h, PENDING).status == OrderStatus.PENDING_CANCEL
        lines = debug_lines(h)
        assert any("details could not be read" in line for line in lines)
        assert any(str(query.client_order_id) in line and "not found" in line for line in lines)
        assert h.logger.errors() == []


async def test_the_recorded_level_amends_and_trailing_moves_drive_the_foreign_legs() -> None:
    venue = live_venue()
    amends: list = []
    venue.server.on(om.PROTO_OA_AMEND_POSITION_SLTP_REQ, replay(LEVEL_AMENDS, amends))
    async with harness(execution_venue=venue, instruments=(EURUSD_ID,)) as h:
        await started(h)
        stop, target = by_venue_id(h, STOP), by_venue_id(h, TARGET)
        assert stop.trigger_price == Price.from_str("1.11788")
        assert target.price == Price.from_str("1.12978")

        await h.client._modify_order(modify(stop, trigger_price=Price.from_str("1.11778")))
        await wait_until(lambda: by_venue_id(h, STOP).trigger_price == Price.from_str("1.11778"))
        await h.client._cancel_order(cancel(target))
        await wait_until(lambda: by_venue_id(h, TARGET).status == OrderStatus.CANCELED)
        await push(h, *TRAILED)
        last = SNAPSHOTS[-1].position[0].stopLoss
        await wait_until(
            lambda: by_venue_id(h, STOP).trigger_price == Price(last, 5),
            description="the stop-loss leg followed the broker",
        )

        # Every amend sends the trailing stop's terms again; the take-profit's removal leaves it
        # out.
        assert [(a.stopLoss, a.HasField("takeProfit")) for a in amends] == [
            (1.11778, True),
            (1.11778, False),
        ]
        for amend in amends:
            assert amend.trailingStopLoss and not amend.guaranteedStopLoss
            assert amend.stopLossTriggerMethod == om.TRADE
        assert stop_moves(h) == [Price(t, 5) for t in (1.11778, *(e.stopPrice for e in TRAILED))]
        assert by_venue_id(h, STOP).status == OrderStatus.ACCEPTED
        view = h.client._book.view(POSITION)
        assert view.levels == {Level.STOP_LOSS: Price(last, 5).as_decimal()}
        assert view.terms.trailing_stop_loss
        assert sent_about(h, stop) == sent_about(h, target) == []
        assert h.logger.errors() == []


async def test_a_recorded_trailing_move_ahead_of_the_amends_answer_is_kept() -> None:
    # The broker moved the stop-loss after it answered the take-profit's removal. Delivered the
    # other way round, the answer's older stop-loss must not undo the move.
    venue = live_venue()
    removal, move = LEVEL_AMENDS[1], TRAILED[0]
    assert move.utcLastUpdateTimestamp > removal.order.utcLastUpdateTimestamp
    moved_to = Price(move.stopPrice, 5)
    venue.server.on(
        om.PROTO_OA_AMEND_POSITION_SLTP_REQ,
        replay([LEVEL_AMENDS[0], [Pushed(move), removal]], []),
    )
    async with harness(execution_venue=venue, instruments=(EURUSD_ID,)) as h:
        await started(h)
        stop, target = by_venue_id(h, STOP), by_venue_id(h, TARGET)
        await h.client._modify_order(modify(stop, trigger_price=Price.from_str("1.11778")))
        await wait_until(lambda: by_venue_id(h, STOP).trigger_price == Price.from_str("1.11778"))

        await h.client._cancel_order(cancel(target))
        await wait_until(lambda: by_venue_id(h, TARGET).status == OrderStatus.CANCELED)

        assert by_venue_id(h, STOP).trigger_price == moved_to
        assert stop_moves(h) == [Price.from_str("1.11778"), moved_to]
        assert reports_of(h, STOP)[-1].trigger_price == moved_to
        assert h.client._book.view(POSITION).levels == {Level.STOP_LOSS: moved_to.as_decimal()}
        assert sent_about(h, stop) == []
        assert h.logger.errors() == []


def test_the_model_keeps_a_trailing_move_newer_than_the_answer_applied_after_it() -> None:
    book = VenueBook(lambda symbol_id: 5 if symbol_id == EURUSD_SYMBOL_ID else None)
    book.load(SNAPSHOTS[0], {})
    book.apply(LEVEL_AMENDS[0], NoOperations())
    removal, move = LEVEL_AMENDS[1], TRAILED[0]

    book.trailing_stop_moved(move)
    book.apply(removal, NoOperations())

    assert book.view(POSITION).levels == {Level.STOP_LOSS: Decimal("1.11779")}

    # A change the broker made after the move sets its own stop-loss (hand-built).
    later = type(removal)()
    later.CopyFrom(removal)
    later.order.stopPrice = 1.1177
    later.order.utcLastUpdateTimestamp = move.utcLastUpdateTimestamp + 1
    book.apply(later, NoOperations())

    assert book.view(POSITION).levels == {Level.STOP_LOSS: Decimal("1.11770")}


def at(move: oa.ProtoOATrailingSLChangedEvent, *, utc: int, stop: float):
    """A copy of the recorded `move`, made at `utc` to `stop` (hand-built)."""
    copy = type(move)()
    copy.CopyFrom(move)
    copy.utcLastUpdateTimestamp = utc
    copy.stopPrice = stop
    return copy


def test_the_model_drops_a_trailing_move_older_than_the_answer_applied_before_it() -> None:
    book = VenueBook(lambda symbol_id: 5 if symbol_id == EURUSD_SYMBOL_ID else None)
    book.load(SNAPSHOTS[0], {})
    answer = LEVEL_AMENDS[0]
    book.apply(answer, NoOperations())
    answered_ms = answer.order.utcLastUpdateTimestamp

    assert book.trailing_stop_moved(at(TRAILED[0], utc=answered_ms - 1, stop=1.1179)) == []
    assert book.view(POSITION).levels[Level.STOP_LOSS] == Decimal("1.11778")

    # One made in the answer's own millisecond, delivered after it, is taken.
    book.trailing_stop_moved(at(TRAILED[0], utc=answered_ms, stop=1.11779))
    assert book.view(POSITION).levels[Level.STOP_LOSS] == Decimal("1.11779")


def test_an_answer_without_its_time_sets_its_stop_loss_on_a_trailed_position() -> None:
    book = VenueBook(lambda symbol_id: 5 if symbol_id == EURUSD_SYMBOL_ID else None)
    book.load(SNAPSHOTS[0], {})
    book.trailing_stop_moved(TRAILED[0])
    unstamped = type(LEVEL_AMENDS[1])()
    unstamped.CopyFrom(LEVEL_AMENDS[1])
    unstamped.order.ClearField("utcLastUpdateTimestamp")

    book.apply(unstamped, NoOperations())

    assert book.view(POSITION).levels == {Level.STOP_LOSS: Decimal("1.11778")}
