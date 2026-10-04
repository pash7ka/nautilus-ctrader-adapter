"""The venue model on hand-built events: one rule per test.

None of these sequences was recorded; each builds only the fields the rule reads. The recorded
session itself is replayed in `test_venue_replay.py`.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from nautilus_ctrader.common.order_record import LegIds
from nautilus_ctrader.common.venue_book import VenueBook
from nautilus_ctrader.common.venue_records import (
    Action,
    Activity,
    ActivityKind,
    AwaitProtection,
    ExternalOrder,
    ExternalType,
    Fill,
    Level,
    Notice,
    OrderEvent,
    OrderEventKind,
    ProtectionMissing,
)
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as om
from tests.execution_replay import (
    SYMBOL,
    NoOperations,
    entry_id,
    make_deal,
    make_event,
    make_order,
    make_position,
    our_entry,
    precision,
    stop_id,
    target_id,
)

P, ENTRY, PROTECTIVE = 9_000_001, 9_100_001, 9_100_002
NOTHING = NoOperations()


class Amending(NoOperations):
    def amending(self, position_id: int) -> bool:
        return True


class Closing(NoOperations):
    def __init__(self, client_order_id: str) -> None:
        self.client_order_id = client_order_id

    def closing(self, position_id: int, volume: int) -> str | None:
        return self.client_order_id


def opened(book: VenueBook, *, side: int = om.BUY, stop: float = 85000.0, target: float = 85500.0):
    """The node's bracket accepted, filled and protected; returns the records of the fill."""
    position_side = side
    protective_side = om.SELL if side == om.BUY else om.BUY
    book.apply(
        make_event(
            om.ORDER_ACCEPTED,
            our_entry(P, ENTRY, side=side, utc=10),
            position=make_position(
                P, side=position_side, volume=0, status=om.POSITION_STATUS_CREATED
            ),
        ),
        NOTHING,
    )
    filled = book.apply(
        make_event(
            om.ORDER_FILLED,
            our_entry(P, ENTRY, side=side, utc=20),
            position=make_position(P, side=position_side),
            deal=make_deal(9_200_001, ENTRY, P, side=side, volume=100, price=85250.0, ts=20),
        ),
        NOTHING,
    )
    protected = book.apply(
        make_event(
            om.ORDER_ACCEPTED,
            make_order(
                PROTECTIVE,
                P,
                order_type=om.STOP_LOSS_TAKE_PROFIT,
                side=protective_side,
                closing=True,
                utc=21,
                stop=stop,
                limit=target,
            ),
            position=make_position(P, side=position_side),
            server=True,
        ),
        NOTHING,
    )
    return filled, protected


def protective(utc: int, *, side: int = om.SELL, stop=None, limit=None, volume: int = 100):
    return make_order(
        PROTECTIVE,
        P,
        order_type=om.STOP_LOSS_TAKE_PROFIT,
        side=side,
        closing=True,
        utc=utc,
        stop=stop,
        limit=limit,
        volume=volume,
    )


def leg(kind, level, ts, **kw) -> OrderEvent:
    client = stop_id(P) if level == Level.STOP_LOSS else target_id(P)
    return OrderEvent(kind, f"{ENTRY}-{level.value}", client, ts, **kw)


def manual(action, ts, units="1") -> Activity:
    return Activity(
        ActivityKind.MANUAL_CHANGE, SYMBOL, "position", "BUY", Decimal(units), action, ts
    )


def book() -> VenueBook:
    return VenueBook(precision)


def test_an_entry_is_accepted_filled_and_its_legs_accepted_on_the_protective_order() -> None:
    b = book()
    filled, protected = opened(b)

    assert filled == [
        OrderEvent(
            OrderEventKind.FILLED,
            str(ENTRY),
            entry_id(P),
            20,
            fill=Fill(
                "9200001", str(P), "BUY", Decimal("1"), Decimal("85250.00"), Decimal("0"), 20
            ),
        ),
        AwaitProtection(P),
    ]
    assert protected == [
        leg(
            OrderEventKind.ACCEPTED,
            Level.STOP_LOSS,
            21,
            quantity=Decimal("1"),
            trigger_price=Decimal("85000.00"),
        ),
        leg(
            OrderEventKind.ACCEPTED,
            Level.TAKE_PROFIT,
            21,
            quantity=Decimal("1"),
            price=Decimal("85500.00"),
        ),
    ]
    view = b.view(P)
    assert view.ours and view.open and view.protective_order_id == PROTECTIVE
    assert view.legs == {
        Level.STOP_LOSS: (stop_id(P), True),
        Level.TAKE_PROFIT: (target_id(P), True),
    }


@pytest.mark.parametrize(
    ("side", "stop", "target", "fill", "expected"),
    [
        (om.BUY, 85000.0, 85500.0, 85500.0, Level.TAKE_PROFIT),  # exactly at the target
        (om.BUY, 85000.0, 85500.0, 84990.0, Level.STOP_LOSS),
        (om.BUY, 85000.0, 85500.0, 85200.0, Level.STOP_LOSS),  # between: the stop
        (om.SELL, 85500.0, 85000.0, 85000.0, Level.TAKE_PROFIT),
        (om.SELL, 85500.0, 85000.0, 85510.0, Level.STOP_LOSS),
    ],
)
def test_the_triggered_level_follows_the_protective_orders_own_rule(
    side, stop, target, fill, expected
) -> None:
    b = book()
    opened(b, side=side, stop=stop, target=target)
    closing_side = om.SELL if side == om.BUY else om.BUY

    records = b.apply(
        make_event(
            om.ORDER_FILLED,
            protective(30, side=closing_side, stop=stop, limit=target),
            position=make_position(P, side=side, volume=0, status=om.POSITION_STATUS_CLOSED),
            deal=make_deal(
                9_200_002, PROTECTIVE, P, side=closing_side, volume=100, price=fill, ts=30
            ),
            server=True,
        ),
        NOTHING,
    )

    other = Level.STOP_LOSS if expected == Level.TAKE_PROFIT else Level.TAKE_PROFIT
    side_name = "SELL" if side == om.BUY else "BUY"
    assert records == [
        leg(
            OrderEventKind.FILLED,
            expected,
            30,
            fill=Fill(
                "9200002",
                str(P),
                side_name,
                Decimal("1"),
                Decimal(f"{fill:.2f}"),
                Decimal("0"),
                30,
            ),
        ),
        leg(OrderEventKind.CANCELED, other, 30),
    ]


def test_inverted_levels_follow_the_rule_with_a_notice() -> None:
    b = book()
    opened(b, stop=85000.0, target=85500.0)
    b.apply(make_event(om.ORDER_REPLACED, protective(25, stop=85600.0, limit=85500.0)), NOTHING)

    records = b.apply(
        make_event(
            om.ORDER_FILLED,
            protective(30, stop=85600.0, limit=85500.0),
            position=make_position(P, volume=0, status=om.POSITION_STATUS_CLOSED),
            deal=make_deal(
                9_200_002, PROTECTIVE, P, side=om.SELL, volume=100, price=85550.0, ts=30
            ),
            server=True,
        ),
        NOTHING,
    )

    assert isinstance(records[0], Notice)
    assert records[1].kind == OrderEventKind.FILLED
    assert records[1].client_order_id == target_id(P)


def test_a_partial_entry_fills_twice_and_awaits_protection_once() -> None:
    b = book()
    b.apply(
        make_event(
            om.ORDER_ACCEPTED,
            our_entry(P, ENTRY, utc=10),
            position=make_position(P, volume=0, status=om.POSITION_STATUS_CREATED),
        ),
        NOTHING,
    )
    first = b.apply(
        make_event(
            om.ORDER_PARTIAL_FILL,
            our_entry(P, ENTRY, utc=11),
            position=make_position(P, volume=40),
            deal=make_deal(1, ENTRY, P, side=om.BUY, volume=40, price=85250.0, ts=11),
        ),
        NOTHING,
    )
    second = b.apply(
        make_event(
            om.ORDER_FILLED,
            our_entry(P, ENTRY, utc=12),
            position=make_position(P, volume=100),
            deal=make_deal(2, ENTRY, P, side=om.BUY, volume=60, price=85251.0, ts=12),
        ),
        NOTHING,
    )

    assert [type(r).__name__ for r in first] == ["OrderEvent", "AwaitProtection"]
    assert [type(r).__name__ for r in second] == ["OrderEvent"]
    assert second[0].fill.units == Decimal("0.6")


def test_an_entry_cancelled_without_a_fill_cancels_its_legs() -> None:
    b = book()
    b.apply(
        make_event(
            om.ORDER_ACCEPTED,
            our_entry(P, ENTRY, utc=10),
            position=make_position(P, volume=0, status=om.POSITION_STATUS_CREATED),
        ),
        NOTHING,
    )

    records = b.apply(
        make_event(
            om.ORDER_CANCELLED,
            our_entry(P, ENTRY, utc=11),
            position=make_position(P, volume=0, status=om.POSITION_STATUS_CLOSED),
        ),
        NOTHING,
    )

    assert records == [
        OrderEvent(OrderEventKind.CANCELED, str(ENTRY), entry_id(P), 11),
        leg(OrderEventKind.CANCELED, Level.STOP_LOSS, 11),
        leg(OrderEventKind.CANCELED, Level.TAKE_PROFIT, 11),
    ]


def test_a_cancelled_remainder_leaves_the_legs() -> None:
    b = book()
    b.apply(
        make_event(
            om.ORDER_ACCEPTED,
            our_entry(P, ENTRY, utc=10),
            position=make_position(P, volume=0, status=om.POSITION_STATUS_CREATED),
        ),
        NOTHING,
    )
    b.apply(
        make_event(
            om.ORDER_PARTIAL_FILL,
            our_entry(P, ENTRY, utc=11),
            position=make_position(P, volume=40),
            deal=make_deal(1, ENTRY, P, side=om.BUY, volume=40, price=85250.0, ts=11),
        ),
        NOTHING,
    )

    records = b.apply(
        make_event(
            om.ORDER_CANCELLED, our_entry(P, ENTRY, utc=12), position=make_position(P, volume=40)
        ),
        NOTHING,
    )

    assert records == [OrderEvent(OrderEventKind.CANCELED, str(ENTRY), entry_id(P), 12)]


def test_a_rejected_entry_cancels_its_legs() -> None:
    b = book()

    records = b.apply(
        make_event(om.ORDER_REJECTED, our_entry(P, ENTRY, utc=10), error="NOT_ENOUGH_MONEY"),
        NOTHING,
    )

    assert records == [
        OrderEvent(OrderEventKind.REJECTED, str(ENTRY), entry_id(P), 10, reason="NOT_ENOUGH_MONEY"),
        leg(OrderEventKind.CANCELED, Level.STOP_LOSS, 10),
        leg(OrderEventKind.CANCELED, Level.TAKE_PROFIT, 10),
    ]


def test_an_entry_refused_before_acceptance_is_rejected_with_its_legs_cancelled() -> None:
    records = book().reject_entry("O-X", LegIds("O-X-SL", None), "MARKET_CLOSED", 5)

    assert records == [
        OrderEvent(OrderEventKind.REJECTED, None, "O-X", 5, reason="MARKET_CLOSED"),
        OrderEvent(OrderEventKind.CANCELED, None, "O-X-SL", 5),
    ]


def test_the_nodes_own_amend_is_no_activity() -> None:
    b = book()
    opened(b)

    records = b.apply(
        make_event(om.ORDER_REPLACED, protective(25, stop=85100.0, limit=85500.0)), Amending()
    )

    assert records == [
        leg(OrderEventKind.UPDATED, Level.STOP_LOSS, 25, trigger_price=Decimal("85100.00"))
    ]


def test_a_trader_cancelling_the_protective_order_cancels_both_legs() -> None:
    b = book()
    opened(b)

    records = b.apply(
        make_event(om.ORDER_CANCELLED, protective(25, stop=85000.0, limit=85500.0)), NOTHING
    )

    assert records == [
        leg(OrderEventKind.CANCELED, Level.STOP_LOSS, 25),
        manual(Action.LEVEL_REMOVED, 25),
        leg(OrderEventKind.CANCELED, Level.TAKE_PROFIT, 25),
        manual(Action.LEVEL_REMOVED, 25),
    ]


def test_a_level_added_back_under_a_new_protective_order_is_the_traders() -> None:
    b = book()
    opened(b)
    b.apply(make_event(om.ORDER_REPLACED, protective(25)), NOTHING)  # both levels removed

    records = b.apply(
        make_event(
            om.ORDER_ACCEPTED,
            make_order(
                9_100_009,
                P,
                order_type=om.STOP_LOSS_TAKE_PROFIT,
                side=om.SELL,
                closing=True,
                utc=26,
                stop=84900.0,
            ),
        ),
        NOTHING,
    )

    assert records == [manual(Action.LEVEL_ADDED, 26)]
    view = b.view(P)
    assert view.protective_order_id == 9_100_009
    assert view.legs[Level.STOP_LOSS] == (stop_id(P), False)


def test_the_nodes_own_close_is_reported_under_its_id_to_the_end() -> None:
    b = book()
    opened(b)
    close = make_order(9_100_003, P, side=om.SELL, closing=True, utc=40)

    accepted = b.apply(
        make_event(om.ORDER_ACCEPTED, close, position=make_position(P)), Closing("O-C")
    )
    filled = b.apply(
        make_event(
            om.ORDER_FILLED,
            close,
            position=make_position(P, volume=0, status=om.POSITION_STATUS_CLOSED),
            deal=make_deal(9_200_003, 9_100_003, P, side=om.SELL, volume=100, price=85300.0, ts=41),
        ),
        NOTHING,  # the operation has left the table; the model remembers the order
    )

    assert accepted == [OrderEvent(OrderEventKind.ACCEPTED, "9100003", "O-C", 40)]
    assert filled[0].client_order_id == "O-C" and filled[0].kind == OrderEventKind.FILLED
    assert filled[1:] == [
        leg(OrderEventKind.CANCELED, Level.STOP_LOSS, 41),
        leg(OrderEventKind.CANCELED, Level.TAKE_PROFIT, 41),
    ]


def test_a_traders_full_close_of_the_nodes_position() -> None:
    b = book()
    opened(b)
    close = make_order(9_100_003, P, side=om.SELL, closing=True, utc=40)
    deal = make_deal(9_200_003, 9_100_003, P, side=om.SELL, volume=100, price=85300.0, ts=41)

    records = b.apply(
        make_event(
            om.ORDER_FILLED,
            close,
            position=make_position(P, volume=0, status=om.POSITION_STATUS_CLOSED),
            deal=deal,
        ),
        NOTHING,
    )

    fill = Fill("9200003", str(P), "SELL", Decimal("1"), Decimal("85300.00"), Decimal("0"), 41)
    assert records == [
        ExternalOrder(
            "9100003",
            SYMBOL,
            "SELL",
            ExternalType.MARKET,
            Decimal("1"),
            True,
            str(P),
            40,
            fills=(fill,),
        ),
        manual(Action.CLOSED, 41),
        leg(OrderEventKind.CANCELED, Level.STOP_LOSS, 41),
        leg(OrderEventKind.CANCELED, Level.TAKE_PROFIT, 41),
    ]


def test_a_stop_out_is_its_own_activity() -> None:
    b = book()
    opened(b)
    close = make_order(9_100_003, P, side=om.SELL, closing=True, utc=40, stop_out=True)

    records = b.apply(
        make_event(
            om.ORDER_FILLED,
            close,
            position=make_position(P, volume=0, status=om.POSITION_STATUS_CLOSED),
            deal=make_deal(9_200_003, 9_100_003, P, side=om.SELL, volume=100, price=84000.0, ts=41),
            server=True,
        ),
        NOTHING,
    )

    stop_out = Activity(
        ActivityKind.STOP_OUT, SYMBOL, "position", "BUY", Decimal("1"), Action.CLOSED, 41
    )
    assert stop_out in records


def test_an_unloaded_symbol_is_only_activity() -> None:
    other = make_order(7, 8, symbol=279, utc=50)

    records = book().apply(
        make_event(
            om.ORDER_FILLED,
            other,
            position=make_position(8, symbol=279),
            deal=make_deal(9, 7, 8, side=om.BUY, volume=100, price=25000.0, ts=51),
        ),
        NOTHING,
    )

    assert records == [
        Activity(
            ActivityKind.UNLOADED_SYMBOL, 279, "position", "BUY", Decimal("1"), Action.OPENED, 51
        ),
    ]


def test_a_missing_protective_order_is_reported_once_its_wait_ends() -> None:
    b = book()
    b.apply(
        make_event(
            om.ORDER_ACCEPTED,
            our_entry(P, ENTRY, utc=10),
            position=make_position(P, volume=0, status=om.POSITION_STATUS_CREATED),
        ),
        NOTHING,
    )
    b.apply(
        make_event(
            om.ORDER_FILLED,
            our_entry(P, ENTRY, utc=20),
            position=make_position(P),
            deal=make_deal(1, ENTRY, P, side=om.BUY, volume=100, price=85250.0, ts=20),
        ),
        NOTHING,
    )

    assert b.protection_timed_out(P) == [ProtectionMissing(P, stop_id(P), target_id(P))]


def test_no_missing_protection_once_the_protective_order_came() -> None:
    b = book()
    opened(b)

    assert b.protection_timed_out(P) == []


def test_an_unreadable_leg_record_is_a_notice_and_no_legs() -> None:
    b = book()
    entry = our_entry(P, ENTRY, utc=10)
    entry.tradeData.comment = "ntca1|sl=O-SL|sl=again"

    records = b.apply(
        make_event(
            om.ORDER_ACCEPTED,
            entry,
            position=make_position(P, volume=0, status=om.POSITION_STATUS_CREATED),
        ),
        NOTHING,
    )

    assert isinstance(records[0], Notice)
    assert records[1:] == [OrderEvent(OrderEventKind.ACCEPTED, str(ENTRY), entry_id(P), 10)]
    assert b.view(P).legs == {}


def test_an_event_without_an_order_means_nothing_here() -> None:
    event = make_event(om.SWAP, make_order(1, 2))
    event.ClearField("order")

    assert book().apply(event, NOTHING) == []


def test_a_protective_order_of_an_unknown_foreign_position_means_nothing() -> None:
    records = book().apply(
        make_event(
            om.ORDER_REPLACED,
            make_order(5, 6, order_type=om.STOP_LOSS_TAKE_PROFIT, closing=True, stop=1.0),
            position=make_position(6),
        ),
        NOTHING,
    )

    assert records == []
