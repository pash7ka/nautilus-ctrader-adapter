"""An order the node did not send, through the execution client into a Nautilus engine.

Nautilus learns of it from reports alone: one for the order, one for each fill and one for each
later change. Each test applies its events one right after another, with no chance for the
engine to work through its queue in between, as a burst from the venue can arrive.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from nautilus_trader.execution.reports import FillReport, OrderStatusReport
from nautilus_trader.model.enums import OrderStatus, OrderType
from nautilus_trader.model.events import OrderFilled
from nautilus_trader.model.identifiers import TradeId, VenueOrderId
from nautilus_trader.model.objects import Price, Quantity

from nautilus_ctrader.common.venue_records import OrderEvent, OrderEventKind
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as om
from tests.execution_replay import make_deal, make_event, make_order, make_position
from tests.execution_venue import Harness, harness, on_us100
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
