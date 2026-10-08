"""An order the node did not send, through the execution client into a Nautilus engine.

Nautilus learns of it from reports alone: one for the order, one for each fill and one for each
later change. Each test applies its events one right after another, with no chance for the
engine to work through its queue in between, as a burst from the venue can arrive.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from decimal import Decimal

from nautilus_trader.execution.reports import FillReport, OrderStatusReport
from nautilus_trader.model.enums import OrderStatus, OrderType
from nautilus_trader.model.events import OrderFilled
from nautilus_trader.model.identifiers import PositionId, StrategyId, TradeId, VenueOrderId
from nautilus_trader.model.objects import Price, Quantity

from nautilus_ctrader.common.venue_records import (
    EntryUnknown,
    ExternalOrder,
    ExternalType,
    OrderEvent,
    OrderEventKind,
)
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as om
from tests.account_venue import ACCOUNT_ID
from tests.execution_replay import (
    FIRST,
    make_deal,
    make_event,
    make_order,
    make_position,
)
from tests.execution_venue import (
    FOREIGN_EVENTS,
    US100_SYMBOL_ID,
    ExecutionVenue,
    Harness,
    harness,
    on_us100,
    push,
)
from tests.polling import wait_until

ORDER, POSITION = 6_800_001, 5_800_001


def limit(kind: int, utc: int, *, price: float = 84000.0, volume: int = 200, **kw):
    """The trader's limit buy `ORDER`, in an event of `kind` at `utc`."""
    order = make_order(ORDER, POSITION, order_type=om.LIMIT, utc=utc, limit=price, volume=volume)
    return make_event(kind, order, **kw)


def created() -> om.ProtoOAPosition:
    return make_position(POSITION, volume=0, status=om.POSITION_STATUS_CREATED)


def filled(utc: int, deal_id: int, volume: int, left: int, *, price: float, order_volume: int):
    deal = make_deal(deal_id, ORDER, POSITION, side=om.BUY, volume=volume, price=price, ts=utc)
    kind = om.ORDER_PARTIAL_FILL if left else om.ORDER_FILLED
    held = make_position(POSITION, volume=order_volume - left)
    return limit(kind, utc, price=price, volume=order_volume, position=held, deal=deal)


def external(h: Harness):
    client_order_id = h.cache.client_order_id(VenueOrderId(str(ORDER)))
    return None if client_order_id is None else h.cache.order(client_order_id)


def kinds(h: Harness) -> list[str]:
    return [type(e).__name__ for e in external(h).events]


def reported(h: Harness) -> list[tuple[str, OrderStatus | None]]:
    return [
        (type(r).__name__, r.order_status if isinstance(r, OrderStatusReport) else None)
        for r in h.reports
    ]


def burst(h: Harness, *events) -> None:
    for event in on_us100(events):
        h.client._on_execution_event(event)


@contextmanager
def no_events_of_the_order(h: Harness) -> Iterator[None]:
    yield
    assert not any(e.venue_order_id == VenueOrderId(str(ORDER)) for e in h.events)


async def test_an_order_changed_then_filled_at_once_is_filled_at_its_new_price() -> None:
    async with harness() as h:
        with no_events_of_the_order(h):
            burst(
                h,
                limit(om.ORDER_ACCEPTED, 10, position=created()),
                limit(om.ORDER_REPLACED, 11, price=84100.0, volume=300, position=created()),
                filled(12, 7_800_001, 300, 0, price=84100.0, order_volume=300),
            )
            await wait_until(lambda: external(h) is not None and external(h).is_closed)

        order = external(h)
        assert order.status == OrderStatus.FILLED
        assert kinds(h)[-3:] == ["OrderAccepted", "OrderUpdated", "OrderFilled"]
        assert order.price == Price.from_str("84100.00")
        assert order.quantity == Quantity.from_str("3.00")
        assert order.filled_qty == Quantity.from_str("3.00")
        assert reported(h) == [
            (OrderStatusReport.__name__, OrderStatus.ACCEPTED),
            (OrderStatusReport.__name__, OrderStatus.ACCEPTED),
            (FillReport.__name__, None),
        ]
        assert h.logger.errors() == []


async def test_an_order_changed_after_a_partial_fill_keeps_its_fills_as_reported() -> None:
    async with harness() as h:
        with no_events_of_the_order(h):
            burst(
                h,
                limit(om.ORDER_ACCEPTED, 10, position=created()),
                filled(11, 7_800_001, 100, 100, price=84000.0, order_volume=200),
                limit(
                    om.ORDER_REPLACED,
                    12,
                    price=84100.0,
                    position=make_position(POSITION, volume=100),
                ),
                filled(13, 7_800_002, 100, 0, price=84100.0, order_volume=200),
            )
            await wait_until(lambda: external(h) is not None and external(h).is_closed)

        order = external(h)
        assert order.status == OrderStatus.FILLED
        assert order.price == Price.from_str("84100.00")
        assert kinds(h)[-4:] == ["OrderAccepted", "OrderFilled", "OrderUpdated", "OrderFilled"]
        # Only the venue's deals: a report that overstated the fills would add one of Nautilus's.
        fills = [e for e in order.events if isinstance(e, OrderFilled)]
        assert [f.trade_id for f in fills] == [TradeId("7800001"), TradeId("7800002")]
        update = h.reports[2]
        assert update.order_status == OrderStatus.PARTIALLY_FILLED
        assert update.filled_qty == Quantity.from_str("1.00")
        assert h.logger.errors() == []


async def test_an_order_changed_then_cancelled_ends_cancelled_at_its_new_price() -> None:
    async with harness() as h:
        with no_events_of_the_order(h):
            burst(
                h,
                limit(om.ORDER_ACCEPTED, 10, position=created()),
                limit(om.ORDER_REPLACED, 11, price=84100.0, position=created()),
                limit(
                    om.ORDER_CANCELLED,
                    12,
                    price=84100.0,
                    position=make_position(POSITION, volume=0, status=om.POSITION_STATUS_CLOSED),
                ),
            )
            await wait_until(lambda: external(h) is not None and external(h).is_closed)

        order = external(h)
        assert order.status == OrderStatus.CANCELED
        assert kinds(h)[-3:] == ["OrderAccepted", "OrderUpdated", "OrderCanceled"]
        assert order.price == Price.from_str("84100.00")
        assert order.filled_qty == Quantity.zero(2)
        assert reported(h)[-1] == (OrderStatusReport.__name__, OrderStatus.CANCELED)
        assert h.logger.errors() == []


async def test_news_of_an_order_nautilus_has_closed_is_not_reported() -> None:
    async with harness() as h:
        burst(
            h,
            limit(om.ORDER_ACCEPTED, 10, position=created()),
            filled(11, 7_800_001, 200, 0, price=84000.0, order_volume=200),
        )
        await wait_until(lambda: external(h) is not None and external(h).is_closed)
        before = len(h.reports)

        h.client._handle_records([OrderEvent(OrderEventKind.CANCELED, str(ORDER), None, 20)])

        assert len(h.reports) == before
        assert external(h).status == OrderStatus.FILLED
        assert any("already FILLED" in line for line in h.logger.warnings())


def stop(kind: int, utc: int, *, price: float) -> object:
    """The trader's stop buy `ORDER`, in an event of `kind` at `utc`."""
    order = make_order(ORDER, POSITION, order_type=om.STOP, utc=utc, stop=price, volume=200)
    return make_event(kind, order, position=created())


async def test_a_stop_order_moved_takes_its_new_trigger_price() -> None:
    async with harness() as h:
        with no_events_of_the_order(h):
            burst(h, stop(om.ORDER_ACCEPTED, 10, price=86000.0))
            burst(h, stop(om.ORDER_REPLACED, 11, price=86100.0))
            await wait_until(lambda: "OrderUpdated" in kinds(h))

        order = external(h)
        assert order.order_type == OrderType.STOP_MARKET
        assert order.status == OrderStatus.ACCEPTED
        assert order.trigger_price == Price.from_str("86100.00")
        update = h.reports[-1]
        assert update.order_status == OrderStatus.ACCEPTED
        assert update.trigger_price == Price.from_str("86100.00")
        assert update.price is None
        assert h.logger.errors() == []


async def test_an_order_that_expires_ends_expired() -> None:
    async with harness() as h:
        with no_events_of_the_order(h):
            burst(
                h,
                limit(om.ORDER_ACCEPTED, 10, position=created()),
                limit(om.ORDER_EXPIRED, 11, position=created()),
            )
            await wait_until(lambda: external(h) is not None and external(h).is_closed)

        assert external(h).status == OrderStatus.EXPIRED
        assert kinds(h)[-2:] == ["OrderAccepted", "OrderExpired"]
        assert reported(h)[-1] == (OrderStatusReport.__name__, OrderStatus.EXPIRED)
        assert h.logger.errors() == []


async def test_an_order_the_venue_rejects_ends_rejected_with_its_reason() -> None:
    async with harness() as h:
        with no_events_of_the_order(h):
            burst(
                h,
                limit(om.ORDER_ACCEPTED, 10, position=created()),
                limit(om.ORDER_REJECTED, 11, position=created(), error="NOT_ENOUGH_MONEY"),
            )
            await wait_until(lambda: external(h) is not None and external(h).is_closed)

        order = external(h)
        assert order.status == OrderStatus.REJECTED
        assert order.events[-1].reason == "NOT_ENOUGH_MONEY"
        assert reported(h)[-1] == (OrderStatusReport.__name__, OrderStatus.REJECTED)
        assert h.logger.errors() == []


async def test_a_fill_nautilus_refuses_is_an_error() -> None:
    async with harness() as h:
        burst(
            h,
            limit(om.ORDER_ACCEPTED, 10, position=created()),
            # More than the order holds: Nautilus refuses an overfill.
            filled(11, 7_800_001, 300, 0, price=84000.0, order_volume=200),
        )
        await wait_until(lambda: h.logger.errors())

        (error,) = h.logger.errors()
        assert "fill 7800001 of order 6800001" in error
        assert "may differ from the broker's" in error
        assert external(h).filled_qty == Quantity.zero(2)


# The levels of a position the node did not open: external legs, through reports only.

# The recorded first position as traded by hand, on US100.cash: protected, its take-profit
# removed and put back, partly closed, then closed by its stop-loss.
FOREIGN_FIRST = FOREIGN_EVENTS
FOREIGN_SL, FOREIGN_TP, FOREIGN_TP_AGAIN = "6000001-SL", "6000001-TP", "6000001-TP-2"


def by_venue_id(h: Harness, venue_order_id: str):
    client_order_id = h.cache.client_order_id(VenueOrderId(venue_order_id))
    return None if client_order_id is None else h.cache.order(client_order_id)


def foreign_closed(h: Harness) -> bool:
    position = h.cache.position(PositionId(str(FIRST)))
    again = by_venue_id(h, FOREIGN_TP_AGAIN)
    return position is not None and position.is_closed and again is not None and again.is_closed


async def test_a_foreign_positions_levels_live_as_external_legs_until_its_stop_loss_fills() -> None:
    async with harness() as h:
        await push(h, *FOREIGN_FIRST)
        await wait_until(lambda: foreign_closed(h), description="the foreign position closed")

        stop = by_venue_id(h, FOREIGN_SL)
        assert stop.order_type == OrderType.STOP_MARKET and stop.is_reduce_only
        assert stop.strategy_id == StrategyId("EXTERNAL")
        assert stop.status == OrderStatus.FILLED
        assert stop.trigger_price == Price.from_str("85206.20")
        assert stop.filled_qty == Quantity.from_str("0.99")
        (fill,) = [e for e in stop.events if isinstance(e, OrderFilled)]
        assert fill.trade_id == TradeId("7000003")
        first_target = by_venue_id(h, FOREIGN_TP)
        assert first_target.order_type == OrderType.LIMIT and first_target.is_reduce_only
        assert first_target.status == OrderStatus.CANCELED
        assert first_target.price == Price.from_str("85387.22")
        # Put back after its leg was cancelled: a new order, cancelled when the position closed.
        again = by_venue_id(h, FOREIGN_TP_AGAIN)
        assert again.status == OrderStatus.CANCELED
        assert again.price == Price.from_str("85353.42")
        assert again.quantity == Quantity.from_str("0.99")
        # The protective order is its position's levels, never an order of its own.
        assert by_venue_id(h, "6000002") is None
        # Every report about a leg Nautilus holds carries the id Nautilus gave it.
        seen = set()
        for report in h.reports:
            venue_order_id = report.venue_order_id
            if venue_order_id.value.startswith("6000001-"):
                if venue_order_id in seen:
                    assert report.client_order_id == h.cache.client_order_id(venue_order_id)
                seen.add(venue_order_id)
        assert not any(
            e.venue_order_id is not None and e.venue_order_id.value.startswith("6000001-")
            for e in h.events
        )
        assert h.logger.errors() == []


def trailed(stop: float, utc: int) -> oa.ProtoOATrailingSLChangedEvent:
    """The broker's move of the first position's trailing stop-loss."""
    return oa.ProtoOATrailingSLChangedEvent(
        ctidTraderAccountId=ACCOUNT_ID,
        positionId=FIRST,
        orderId=6000002,
        stopPrice=stop,
        utcLastUpdateTimestamp=utc,
    )


async def test_a_trailing_stop_move_moves_the_foreign_stop_loss_leg() -> None:
    async with harness() as h:
        # Entry accepted and filled, then protected.
        await push(h, *FOREIGN_FIRST[:3])
        await wait_until(lambda: by_venue_id(h, FOREIGN_SL) is not None)

        await push(h, trailed(85210.5, 1600000130000))
        await wait_until(
            lambda: by_venue_id(h, FOREIGN_SL).trigger_price == Price.from_str("85210.50"),
            description="the stop-loss leg moved",
        )

        stop = by_venue_id(h, FOREIGN_SL)
        assert stop.status == OrderStatus.ACCEPTED
        assert stop.quantity == Quantity.from_str("1.00")
        assert h.logger.errors() == []


async def test_an_external_order_nautilus_holds_is_reported_under_its_client_order_id() -> None:
    async with harness() as h:
        burst(h, limit(om.ORDER_ACCEPTED, 10, position=created()))
        await wait_until(lambda: external(h) is not None)
        held = external(h).client_order_id
        record = ExternalOrder(
            str(ORDER),
            US100_SYMBOL_ID,
            "BUY",
            ExternalType.LIMIT,
            Decimal("2"),
            False,
            str(POSITION),
            10,
            price=Decimal("84000.00"),
        )

        h.client._handle_records([record])

        assert h.reports[-1].client_order_id == held
        assert len(h.cache.orders()) == 1
        assert h.logger.errors() == []


async def test_a_foreign_position_with_no_known_entry_is_a_debug_line() -> None:
    async with harness() as h:
        h.client._handle_records([EntryUnknown(FIRST)])

        assert (
            "debug",
            f"Position {FIRST} has levels but no known entry; its legs wait until "
            "the entry is known",
        ) in h.logger.lines
        await wait_until(lambda: h.received(oa.ProtoOAOrderListByPositionIdReq))
        assert h.logger.warnings() == []


async def test_levels_seen_before_their_entry_get_legs_from_the_order_list() -> None:
    venue = ExecutionVenue()
    # What the broker lists for the position: its entry, and the protective order.
    venue.position_orders[FIRST] = [FOREIGN_FIRST[1].order, FOREIGN_FIRST[2].order]
    async with harness(execution_venue=venue) as h:
        await push(h, FOREIGN_FIRST[2])
        await wait_until(lambda: by_venue_id(h, FOREIGN_TP) is not None, description="legs")

        (asked,) = h.received(oa.ProtoOAOrderListByPositionIdReq)
        assert asked.positionId == FIRST
        stop, target = by_venue_id(h, FOREIGN_SL), by_venue_id(h, FOREIGN_TP)
        assert stop.status == target.status == OrderStatus.ACCEPTED
        assert stop.trigger_price == Price.from_str("85197.20")
        assert target.price == Price.from_str("85387.22")

        # The entry's own events follow: the entry is reported, the legs not again.
        await push(h, *FOREIGN_FIRST[:2])
        await wait_until(lambda: by_venue_id(h, "6000001") is not None, description="entry")
        assert by_venue_id(h, "6000001").status == OrderStatus.FILLED
        reported = [r.venue_order_id.value for r in h.reports if isinstance(r, OrderStatusReport)]
        assert reported.count(FOREIGN_SL) == reported.count(FOREIGN_TP) == 1
        assert h.logger.warnings() == []
        assert h.logger.errors() == []


async def test_a_trailing_stop_move_during_a_rebuild_waits_for_the_model() -> None:
    async with harness() as h:
        await push(h, *FOREIGN_FIRST[:3])
        await wait_until(lambda: by_venue_id(h, FOREIGN_SL) is not None)
        h.client._hold_buffer()

        await push(h, trailed(85210.5, 1600000130000))
        held = by_venue_id(h, FOREIGN_SL).trigger_price
        h.client._release_buffer()

        assert held == Price.from_str("85197.20")
        await wait_until(
            lambda: by_venue_id(h, FOREIGN_SL).trigger_price == Price.from_str("85210.50"),
            description="the stop-loss leg moved",
        )
