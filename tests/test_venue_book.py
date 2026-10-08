"""The venue model on hand-built events: one rule per test.

None of these sequences was recorded; each builds only the fields the rule reads. The recorded
session itself is replayed in `test_venue_replay.py`.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from nautilus_ctrader.common.operations import OperationsInFlight
from nautilus_ctrader.common.order_record import LegIds, encode_comment, encode_label
from nautilus_ctrader.common.venue_book import VenueBook
from nautilus_ctrader.common.venue_records import (
    Action,
    Activity,
    ActivityKind,
    AwaitProtection,
    EntryUnknown,
    Exposure,
    ExternalOrder,
    ExternalType,
    Fill,
    Level,
    LevelTerms,
    Notice,
    OrderEvent,
    OrderEventKind,
    ProtectionMissing,
    leg_venue_order_id,
    units_of,
)
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
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

    def closing(self, position_id: int, volume: int, created_ms: int, order_id: int) -> str | None:
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

    fill = Fill("9200002", str(P), "SELL", Decimal("1"), Decimal("85550.00"), Decimal("0"), 30)
    assert records == [
        Notice(
            "a protective order with inverted levels (stop-loss 85600.00, take-profit 85500.00) "
            "filled at 85550.00; read as the take-profit"
        ),
        leg(OrderEventKind.FILLED, Level.TAKE_PROFIT, 30, fill=fill),
        leg(OrderEventKind.CANCELED, Level.STOP_LOSS, 30),
    ]


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

    assert first == [
        OrderEvent(
            OrderEventKind.FILLED,
            str(ENTRY),
            entry_id(P),
            11,
            fill=fill(1, "85250.00", 11, units="0.4", side="BUY"),
        ),
        AwaitProtection(P),
    ]
    assert second == [
        OrderEvent(
            OrderEventKind.FILLED,
            str(ENTRY),
            entry_id(P),
            12,
            fill=fill(2, "85251.00", 12, units="0.6", side="BUY"),
        ),
    ]


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
        leg(
            OrderEventKind.UPDATED,
            Level.STOP_LOSS,
            25,
            quantity=Decimal("1"),
            trigger_price=Decimal("85100.00"),
        )
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


def test_a_close_created_before_the_node_sent_its_own_is_not_the_nodes() -> None:
    b = book()
    opened(b)
    table = OperationsInFlight()
    # The node last heard from the broker at 45, broker time, when it sent the close.
    table.begin_close("O-C", P, 100, 45)
    earlier = make_order(9_100_003, P, side=om.SELL, closing=True, utc=40)
    later = make_order(9_100_004, P, side=om.SELL, closing=True, utc=50)

    trader = b.apply(make_event(om.ORDER_ACCEPTED, earlier, position=make_position(P)), table)
    node = b.apply(make_event(om.ORDER_ACCEPTED, later, position=make_position(P)), table)

    assert not any(isinstance(r, OrderEvent) and r.client_order_id == "O-C" for r in trader)
    assert node == [OrderEvent(OrderEventKind.ACCEPTED, "9100004", "O-C", 50)]


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
    assert filled == [
        OrderEvent(
            OrderEventKind.FILLED, "9100003", "O-C", 41, fill=fill(9_200_003, "85300.00", 41)
        ),
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


def stop_out(b: VenueBook, operations) -> list:
    """The broker's stop-out of the whole of P."""
    close = make_order(9_100_003, P, side=om.SELL, closing=True, utc=40, stop_out=True)
    return b.apply(
        make_event(
            om.ORDER_FILLED,
            close,
            position=make_position(P, volume=0, status=om.POSITION_STATUS_CLOSED),
            deal=make_deal(9_200_003, 9_100_003, P, side=om.SELL, volume=100, price=84000.0, ts=41),
            server=True,
        ),
        operations,
    )


def stopped_out() -> list:
    fill = Fill("9200003", str(P), "SELL", Decimal("1"), Decimal("84000.00"), Decimal("0"), 41)
    return [
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
        Activity(ActivityKind.STOP_OUT, SYMBOL, "position", "BUY", Decimal("1"), Action.CLOSED, 41),
        leg(OrderEventKind.CANCELED, Level.STOP_LOSS, 41),
        leg(OrderEventKind.CANCELED, Level.TAKE_PROFIT, 41),
    ]


def test_a_stop_out_is_its_own_activity() -> None:
    b = book()
    opened(b)

    assert stop_out(b, NOTHING) == stopped_out()


def test_a_stop_out_is_never_the_nodes_close_in_flight() -> None:
    b = book()
    opened(b)

    assert stop_out(b, Closing("O-C")) == stopped_out()


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


def test_a_foreign_level_with_no_known_entry_says_so_once() -> None:
    b = book()

    def replaced(utc: int, stop: float) -> list:
        return b.apply(
            make_event(
                om.ORDER_REPLACED,
                make_order(
                    5, 6, order_type=om.STOP_LOSS_TAKE_PROFIT, closing=True, utc=utc, stop=stop
                ),
                position=make_position(6),
            ),
            NOTHING,
        )

    assert replaced(10, 1.0) == [EntryUnknown(6)]
    assert replaced(11, 2.0) == []
    assert b.view(6).foreign_legs == {}


# Helpers for the rules below.


def entered(b: VenueBook) -> None:
    """The node's entry accepted and filled, with no protective order yet."""
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
            deal=make_deal(9_200_001, ENTRY, P, side=om.BUY, volume=100, price=85250.0, ts=20),
        ),
        NOTHING,
    )


def fill(deal_id, price, ts, *, units="1", side="SELL", position=P) -> Fill:
    return Fill(str(deal_id), str(position), side, Decimal(units), Decimal(price), Decimal("0"), ts)


def closed(position_id: int = P, **kw) -> om.ProtoOAPosition:
    return make_position(position_id, volume=0, status=om.POSITION_STATUS_CLOSED, **kw)


def trader_close(b: VenueBook, order_id, volume, left, ts, operations=NOTHING) -> list:
    """A closing market order of `volume` filled at 85300; `left` is None for no position."""
    position = None
    if left is not None:
        position = make_position(P, volume=left) if left else closed()
    return b.apply(
        make_event(
            om.ORDER_FILLED,
            make_order(order_id, P, side=om.SELL, closing=True, utc=ts - 1, volume=volume),
            position=position,
            deal=make_deal(
                order_id, order_id, P, side=om.SELL, volume=volume, price=85300.0, ts=ts
            ),
        ),
        operations,
    )


def external_close(order_id, units, ts) -> ExternalOrder:
    return ExternalOrder(
        str(order_id),
        SYMBOL,
        "SELL",
        ExternalType.MARKET,
        Decimal(units),
        True,
        str(P),
        ts - 1,
        fills=(fill(order_id, "85300.00", ts, units=units),),
    )


def trigger(
    b: VenueBook, deal_id, price, ts, *, deal_volume=100, left=0, kind=om.ORDER_FILLED, **order
) -> list:
    """P's protective order filled at `price`; `order` sets its side, levels and volume."""
    side = order.get("side", om.SELL)
    position_side = om.BUY if side == om.SELL else om.SELL
    return b.apply(
        make_event(
            kind,
            protective(ts, **order),
            position=(
                make_position(P, side=position_side, volume=left)
                if left
                else closed(side=position_side)
            ),
            deal=make_deal(
                deal_id, PROTECTIVE, P, side=side, volume=deal_volume, price=price, ts=ts
            ),
            server=True,
        ),
        NOTHING,
    )


# A close the broker made, and a close id matched once.


@pytest.mark.parametrize(("server", "stop_out_flag"), [(True, False), (False, True)])
def test_a_close_the_broker_made_is_never_the_nodes(server, stop_out_flag) -> None:
    b = book()
    opened(b)
    close = make_order(9_100_003, P, side=om.SELL, closing=True, utc=40, stop_out=stop_out_flag)

    records = b.apply(
        make_event(om.ORDER_ACCEPTED, close, position=make_position(P), server=server),
        Closing("O-C"),
    )

    assert [type(r) for r in records] == [ExternalOrder]


def test_a_close_id_names_one_broker_order() -> None:
    b = book()
    opened(b)
    in_flight = Closing("O-C")

    taken = trader_close(b, 9_100_004, 40, 60, 41, in_flight)
    real = trader_close(b, 9_100_005, 40, 20, 43, in_flight)

    assert [(r.kind, r.client_order_id) for r in taken] == [
        (OrderEventKind.ACCEPTED, "O-C"),
        (OrderEventKind.FILLED, "O-C"),
    ]
    assert real == [
        external_close(9_100_005, "0.4", 43),
        manual(Action.PARTIALLY_CLOSED, 43, "0.4"),
    ]


# A protective order filling in parts.


def test_a_partial_trigger_fills_the_leg_twice_and_ends_it_with_the_position() -> None:
    b = book()
    opened(b)
    levels = {"stop": 85000.0, "limit": 85500.0}

    first = trigger(
        b, 1, 84990.0, 30, kind=om.ORDER_PARTIAL_FILL, deal_volume=40, left=60, **levels
    )
    alive = b.view(P).legs[Level.STOP_LOSS]
    second = trigger(b, 2, 84980.0, 31, deal_volume=60, volume=60, **levels)

    assert first == [
        leg(OrderEventKind.FILLED, Level.STOP_LOSS, 30, fill=fill(1, "84990.00", 30, units="0.4"))
    ]
    assert alive == (stop_id(P), True)
    assert second == [
        leg(OrderEventKind.FILLED, Level.STOP_LOSS, 31, fill=fill(2, "84980.00", 31, units="0.6")),
        leg(OrderEventKind.CANCELED, Level.TAKE_PROFIT, 31),
    ]
    assert b.view(P).legs[Level.STOP_LOSS] == (stop_id(P), False)


def test_a_partly_triggered_leg_has_its_remainder_cancelled_when_the_position_closes() -> None:
    b = book()
    opened(b)
    trigger(
        b,
        1,
        84990.0,
        30,
        kind=om.ORDER_PARTIAL_FILL,
        deal_volume=40,
        left=60,
        stop=85000.0,
        limit=85500.0,
    )

    records = trader_close(b, 9_100_004, 60, 0, 41)

    assert records == [
        external_close(9_100_004, "0.6", 41),
        manual(Action.CLOSED, 41, "0.6"),
        leg(OrderEventKind.CANCELED, Level.STOP_LOSS, 41),
        leg(OrderEventKind.CANCELED, Level.TAKE_PROFIT, 41),
    ]


def test_a_partial_trigger_of_an_unknown_entry_is_reported_under_its_protective_id() -> None:
    b = book()

    def event(kind, deal_id, volume, left, ts):
        order = make_order(
            77,
            66,
            order_type=om.STOP_LOSS_TAKE_PROFIT,
            side=om.SELL,
            closing=True,
            utc=ts,
            stop=85000.0,
            limit=85500.0,
            volume=100 if left else volume,
        )
        position = make_position(66, volume=left) if left else closed(66)
        deal = make_deal(deal_id, 77, 66, side=om.SELL, volume=volume, price=84990.0, ts=ts)
        return make_event(kind, order, position=position, deal=deal, server=True)

    first = b.apply(event(om.ORDER_PARTIAL_FILL, 1, 40, 60, 30), NOTHING)
    second = b.apply(event(om.ORDER_FILLED, 2, 60, 0, 31), NOTHING)

    assert first == [
        ExternalOrder(
            "77",
            SYMBOL,
            "SELL",
            ExternalType.STOP_MARKET,
            Decimal("1"),
            True,
            "66",
            30,
            trigger_price=Decimal("85000.00"),
            fills=(fill(1, "84990.00", 30, units="0.4", position=66),),
        ),
    ]
    assert second == [
        OrderEvent(
            OrderEventKind.FILLED,
            "77",
            None,
            31,
            fill=fill(2, "84990.00", 31, units="0.6", position=66),
        ),
    ]


# Orders without a position, and a position learnt without one.


def test_entries_rejected_without_a_position_id_are_each_reported() -> None:
    b = book()

    def unplaced(order_id: int, client_id: str) -> om.ProtoOAOrder:
        order = make_order(
            order_id,
            0,
            utc=10,
            client_order_id=client_id,
            label=encode_label(client_id),
            comment=encode_comment(LegIds(f"{client_id}-SL", None)),
        )
        order.ClearField("positionId")
        return order

    first = b.apply(make_event(om.ORDER_REJECTED, unplaced(101, "O-A"), error="X"), NOTHING)
    second = b.apply(make_event(om.ORDER_REJECTED, unplaced(102, "O-B"), error="X"), NOTHING)

    assert first == [
        OrderEvent(OrderEventKind.REJECTED, "101", "O-A", 10, reason="X"),
        OrderEvent(OrderEventKind.CANCELED, "101-SL", "O-A-SL", 10),
    ]
    assert second == [
        OrderEvent(OrderEventKind.REJECTED, "102", "O-B", 10, reason="X"),
        OrderEvent(OrderEventKind.CANCELED, "102-SL", "O-B-SL", 10),
    ]
    assert b.view(0) is None


def test_a_position_first_seen_through_its_protective_order_takes_the_opposite_side() -> None:
    b = book()
    b.apply(
        make_event(
            om.ORDER_REPLACED,
            make_order(
                78,
                67,
                order_type=om.STOP_LOSS_TAKE_PROFIT,
                side=om.SELL,
                closing=True,
                utc=29,
                stop=85000.0,
                limit=85500.0,
            ),
        ),
        NOTHING,
    )

    assert b.view(67).side == "BUY"


# Protection that came short.


def test_a_leg_whose_level_the_protective_order_lacks_is_missing_after_the_wait() -> None:
    b = book()
    entered(b)
    b.apply(
        make_event(
            om.ORDER_ACCEPTED, protective(21, stop=85000.0), position=make_position(P), server=True
        ),
        NOTHING,
    )

    assert b.protection_timed_out(P) == [ProtectionMissing(P, None, target_id(P))]


@pytest.mark.parametrize(
    ("operations", "activity"),
    [(NOTHING, [manual(Action.LEVEL_ADDED, 22)]), (Amending(), [])],
)
def test_a_level_set_on_a_leg_not_yet_accepted_accepts_it_at_the_brokers_price(
    operations, activity
) -> None:
    b = book()
    entered(b)
    b.apply(
        make_event(
            om.ORDER_ACCEPTED, protective(21, stop=85000.0), position=make_position(P), server=True
        ),
        NOTHING,
    )

    records = b.apply(
        make_event(om.ORDER_REPLACED, protective(22, stop=85000.0, limit=85600.0)), operations
    )

    accepted = leg(
        OrderEventKind.ACCEPTED,
        Level.TAKE_PROFIT,
        22,
        quantity=Decimal("1"),
        price=Decimal("85600.00"),
    )
    assert records == [accepted, *activity]


# A protective fill without levels.


def test_a_protective_fill_without_levels_uses_the_levels_held_before() -> None:
    b = book()
    opened(b)

    records = trigger(b, 4, 85600.0, 30)

    assert records == [
        leg(OrderEventKind.FILLED, Level.TAKE_PROFIT, 30, fill=fill(4, "85600.00", 30)),
        leg(OrderEventKind.CANCELED, Level.STOP_LOSS, 30),
    ]


def test_a_protective_fill_with_no_level_known_is_a_notice_and_a_market_close() -> None:
    records = book().apply(
        make_event(
            om.ORDER_FILLED,
            make_order(
                77, 66, order_type=om.STOP_LOSS_TAKE_PROFIT, side=om.SELL, closing=True, utc=30
            ),
            position=closed(66),
            deal=make_deal(5, 77, 66, side=om.SELL, volume=100, price=85600.0, ts=30),
            server=True,
        ),
        NOTHING,
    )

    assert records == [
        Notice("a protective order filled at 85600.00 with no level known; read as a market close"),
        ExternalOrder(
            "77",
            SYMBOL,
            "SELL",
            ExternalType.MARKET,
            Decimal("1"),
            True,
            "66",
            30,
            fills=(fill(5, "85600.00", 30, position=66),),
        ),
    ]


# Stale and repeated events.


def test_events_of_a_replaced_protective_order_id_are_ignored() -> None:
    b = book()
    opened(b)
    b.apply(
        make_event(
            om.ORDER_ACCEPTED,
            make_order(
                9_100_009,
                P,
                order_type=om.STOP_LOSS_TAKE_PROFIT,
                side=om.SELL,
                closing=True,
                utc=26,
                stop=85000.0,
                limit=85500.0,
            ),
            server=True,
        ),
        NOTHING,
    )

    cancelled = b.apply(
        make_event(om.ORDER_CANCELLED, protective(27, stop=85000.0, limit=85500.0), server=True),
        NOTHING,
    )
    replaced = b.apply(
        make_event(om.ORDER_REPLACED, protective(28, stop=84000.0, limit=85500.0)), NOTHING
    )

    assert cancelled == [] and replaced == []
    view = b.view(P)
    assert view.protective_order_id == 9_100_009
    assert view.levels == {
        Level.STOP_LOSS: Decimal("85000.00"),
        Level.TAKE_PROFIT: Decimal("85500.00"),
    }
    assert view.legs == {
        Level.STOP_LOSS: (stop_id(P), True),
        Level.TAKE_PROFIT: (target_id(P), True),
    }


def test_an_event_seen_before_means_nothing_new() -> None:
    b = book()
    opened(b)
    event = make_event(om.ORDER_REPLACED, protective(25, stop=85100.0, limit=85500.0))

    assert b.apply(event, Amending()) != []
    assert b.apply(event, Amending()) == []


def test_two_replaces_in_the_same_millisecond_both_apply() -> None:
    b = book()
    opened(b)

    b.apply(make_event(om.ORDER_REPLACED, protective(25, stop=85100.0, limit=85500.0)), Amending())
    records = b.apply(
        make_event(om.ORDER_REPLACED, protective(25, stop=85200.0, limit=85500.0)), Amending()
    )

    assert records == [
        leg(
            OrderEventKind.UPDATED,
            Level.STOP_LOSS,
            25,
            quantity=Decimal("1"),
            trigger_price=Decimal("85200.00"),
        )
    ]


def test_an_event_whose_handling_failed_can_be_applied_again(monkeypatch) -> None:
    b = book()
    opened(b)
    event = make_event(om.ORDER_REPLACED, protective(25, stop=85100.0, limit=85500.0))

    def broken(*args):
        raise RuntimeError("handler failed")

    monkeypatch.setattr(b, "_protective", broken)
    with pytest.raises(RuntimeError):
        b.apply(event, Amending())
    monkeypatch.undo()

    assert b.apply(event, Amending()) == [
        leg(
            OrderEventKind.UPDATED,
            Level.STOP_LOSS,
            25,
            quantity=Decimal("1"),
            trigger_price=Decimal("85100.00"),
        )
    ]


def test_an_entry_and_the_nodes_close_are_accepted_once_each() -> None:
    b = book()
    created = make_position(P, volume=0, status=om.POSITION_STATUS_CREATED)
    first = b.apply(
        make_event(om.ORDER_ACCEPTED, our_entry(P, ENTRY, utc=10), position=created), NOTHING
    )
    again = b.apply(
        make_event(om.ORDER_ACCEPTED, our_entry(P, ENTRY, utc=11), position=created), NOTHING
    )
    assert len(first) == 1 and again == []

    b = book()
    opened(b)
    in_flight = Closing("O-C")
    close = make_order(9_100_003, P, side=om.SELL, closing=True, utc=40)
    first = b.apply(make_event(om.ORDER_ACCEPTED, close, position=make_position(P)), in_flight)
    close.utcLastUpdateTimestamp = 45
    again = b.apply(make_event(om.ORDER_ACCEPTED, close, position=make_position(P)), in_flight)
    assert first == [OrderEvent(OrderEventKind.ACCEPTED, "9100003", "O-C", 40)] and again == []


def test_a_full_close_without_the_position_is_told_by_the_volumes() -> None:
    b = book()
    opened(b)

    records = trader_close(b, 9_100_006, 100, None, 41)

    assert records == [
        external_close(9_100_006, "1", 41),
        manual(Action.CLOSED, 41),
        leg(OrderEventKind.CANCELED, Level.STOP_LOSS, 41),
        leg(OrderEventKind.CANCELED, Level.TAKE_PROFIT, 41),
    ]
    assert not b.view(P).open


# Restoring from a snapshot.


def snapshot(*, stop=None, target=None, volume=100, orders=()) -> oa.ProtoOAReconcileRes:
    response = oa.ProtoOAReconcileRes(ctidTraderAccountId=1_000_001)
    position = make_position(P, volume=volume)
    if stop is not None:
        position.stopLoss = stop
    if target is not None:
        position.takeProfit = target
    response.position.append(position)
    response.order.append(protective(5, stop=stop, limit=target, volume=volume))
    response.order.extend(orders)
    return response


def test_load_revives_a_leg_only_where_its_level_stands() -> None:
    b = book()

    notices = b.load(snapshot(stop=85000.0), {P: [our_entry(P, ENTRY), protective(5)]})

    assert notices == []
    view = b.view(P)
    assert view.ours and view.open and view.protective_order_id == PROTECTIVE
    assert view.levels == {Level.STOP_LOSS: Decimal("85000.00")}
    assert view.legs == {
        Level.STOP_LOSS: (stop_id(P), True),
        Level.TAKE_PROFIT: (target_id(P), False),
    }


def test_load_holds_the_protective_orders_volume() -> None:
    b = book()
    b.load(snapshot(stop=85000.0, target=85500.0), {P: [our_entry(P, ENTRY)]})

    moved = b.apply(
        make_event(om.ORDER_REPLACED, protective(6, stop=85100.0, limit=85500.0)), Amending()
    )
    reduced = b.apply(
        make_event(
            om.ORDER_REPLACED,
            protective(7, stop=85100.0, limit=85500.0, volume=60),
            position=make_position(P, volume=60),
            server=True,
        ),
        NOTHING,
    )

    assert moved == [
        leg(
            OrderEventKind.UPDATED,
            Level.STOP_LOSS,
            6,
            quantity=Decimal("1"),
            trigger_price=Decimal("85100.00"),
        )
    ]
    assert reduced == [
        leg(OrderEventKind.UPDATED, Level.STOP_LOSS, 7, quantity=Decimal("0.6")),
        leg(OrderEventKind.UPDATED, Level.TAKE_PROFIT, 7, quantity=Decimal("0.6")),
    ]


def test_load_marks_the_other_open_orders_as_reported() -> None:
    b = book()
    pending = make_order(7, 8, order_type=om.LIMIT, utc=3, limit=84000.0)
    b.load(snapshot(orders=[pending]), {})

    accepted = b.apply(make_event(om.ORDER_ACCEPTED, pending), NOTHING)
    pending.utcLastUpdateTimestamp = 12
    cancelled = b.apply(make_event(om.ORDER_CANCELLED, pending), NOTHING)

    assert accepted == []
    assert cancelled == [OrderEvent(OrderEventKind.CANCELED, "7", None, 12)]


def test_load_an_unreadable_leg_record_is_a_notice() -> None:
    b = book()
    entry = our_entry(P, ENTRY)
    entry.tradeData.comment = "ntca1|sl=O-SL|sl=again"

    notices = b.load(snapshot(stop=85000.0), {P: [entry]})

    assert notices == [
        Notice(
            f"position {P} is the node's own, but its legs' record cannot be read; "
            "its levels stay at the broker without legs"
        )
    ]
    assert b.view(P).ours and b.view(P).legs == {}


# Branches of the entry, the protective order and the closes.


def test_an_expired_entry_cancels_its_legs() -> None:
    b = book()
    b.apply(
        make_event(
            om.ORDER_ACCEPTED,
            our_entry(P, ENTRY, utc=10),
            position=make_position(P, volume=0, status=om.POSITION_STATUS_CREATED),
        ),
        NOTHING,
    )

    records = b.apply(make_event(om.ORDER_EXPIRED, our_entry(P, ENTRY, utc=11)), NOTHING)

    assert records == [
        OrderEvent(OrderEventKind.EXPIRED, str(ENTRY), entry_id(P), 11),
        leg(OrderEventKind.CANCELED, Level.STOP_LOSS, 11),
        leg(OrderEventKind.CANCELED, Level.TAKE_PROFIT, 11),
    ]


def test_an_entry_first_seen_at_its_fill() -> None:
    b = book()

    records = b.apply(
        make_event(
            om.ORDER_FILLED,
            our_entry(P, ENTRY, utc=20),
            position=make_position(P),
            deal=make_deal(9_200_001, ENTRY, P, side=om.BUY, volume=100, price=85250.0, ts=20),
        ),
        NOTHING,
    )
    late = b.apply(
        make_event(
            om.ORDER_ACCEPTED,
            our_entry(P, ENTRY, utc=10),
            position=make_position(P, volume=0, status=om.POSITION_STATUS_CREATED),
        ),
        NOTHING,
    )

    # Accepted at the fill, so the late acceptance says nothing.
    assert records == [
        OrderEvent(OrderEventKind.ACCEPTED, str(ENTRY), entry_id(P), 20),
        OrderEvent(
            OrderEventKind.FILLED,
            str(ENTRY),
            entry_id(P),
            20,
            fill=fill(9_200_001, "85250.00", 20, side="BUY"),
        ),
        AwaitProtection(P),
    ]
    assert late == []


def test_the_nodes_close_first_seen_at_its_fill_is_accepted_then_filled() -> None:
    b = book()
    opened(b)
    close = make_order(9_100_003, P, side=om.SELL, closing=True, utc=42)

    filled = b.apply(
        make_event(
            om.ORDER_FILLED,
            close,
            position=make_position(P, volume=0, status=om.POSITION_STATUS_CLOSED),
            deal=make_deal(9_200_003, 9_100_003, P, side=om.SELL, volume=100, price=85300.0, ts=41),
        ),
        Closing("O-C"),
    )
    close.utcLastUpdateTimestamp = 40
    late = b.apply(make_event(om.ORDER_ACCEPTED, close, position=make_position(P)), NOTHING)

    assert filled == [
        OrderEvent(OrderEventKind.ACCEPTED, "9100003", "O-C", 42),
        OrderEvent(
            OrderEventKind.FILLED, "9100003", "O-C", 41, fill=fill(9_200_003, "85300.00", 41)
        ),
        leg(OrderEventKind.CANCELED, Level.STOP_LOSS, 41),
        leg(OrderEventKind.CANCELED, Level.TAKE_PROFIT, 41),
    ]
    assert late == []


def test_an_entry_restored_at_load_is_never_accepted_again_at_its_fill() -> None:
    b = book()
    b.load(snapshot(stop=85000.0, volume=50), {P: [our_entry(P, ENTRY, utc=10)]})

    records = b.apply(
        make_event(
            om.ORDER_PARTIAL_FILL,
            our_entry(P, ENTRY, utc=30),
            position=make_position(P),
            deal=make_deal(9_200_002, ENTRY, P, side=om.BUY, volume=50, price=85250.0, ts=30),
        ),
        NOTHING,
    )

    entry_kinds = [
        r.kind for r in records if isinstance(r, OrderEvent) and r.client_order_id == entry_id(P)
    ]
    assert entry_kinds == [OrderEventKind.FILLED]


@pytest.mark.parametrize(
    ("order", "price", "expected"),
    [
        ({"limit": 85500.0}, 84900.0, Level.TAKE_PROFIT),
        ({"stop": 85000.0}, 85600.0, Level.STOP_LOSS),
    ],
)
def test_with_one_level_left_a_trigger_is_that_level(order, price, expected) -> None:
    b = book()
    opened(b)
    b.apply(make_event(om.ORDER_REPLACED, protective(25, **order)), Amending())

    records = trigger(b, 4, price, 30, **order)

    assert records == [leg(OrderEventKind.FILLED, expected, 30, fill=fill(4, f"{price:.2f}", 30))]


def test_inverted_levels_on_a_short_follow_the_rule_with_a_notice() -> None:
    b = book()
    opened(b, side=om.SELL, stop=85500.0, target=85000.0)
    b.apply(
        make_event(om.ORDER_REPLACED, protective(25, side=om.BUY, stop=84900.0, limit=85000.0)),
        Amending(),
    )

    records = trigger(b, 4, 84950.0, 30, side=om.BUY, stop=84900.0, limit=85000.0)

    assert records == [
        Notice(
            "a protective order with inverted levels (stop-loss 84900.00, take-profit 85000.00) "
            "filled at 84950.00; read as the take-profit"
        ),
        leg(OrderEventKind.FILLED, Level.TAKE_PROFIT, 30, fill=fill(4, "84950.00", 30, side="BUY")),
        leg(OrderEventKind.CANCELED, Level.STOP_LOSS, 30),
    ]


def test_a_traders_partial_close_then_the_protective_order_follows_the_volume() -> None:
    b = book()
    opened(b)

    closing = trader_close(b, 9_100_004, 40, 60, 41)
    followed = b.apply(
        make_event(
            om.ORDER_REPLACED,
            protective(42, stop=85000.0, limit=85500.0, volume=60),
            position=make_position(P, volume=60),
            server=True,
        ),
        NOTHING,
    )

    assert closing == [
        external_close(9_100_004, "0.4", 41),
        manual(Action.PARTIALLY_CLOSED, 41, "0.4"),
    ]
    assert followed == [
        leg(OrderEventKind.UPDATED, Level.STOP_LOSS, 42, quantity=Decimal("0.6")),
        leg(OrderEventKind.UPDATED, Level.TAKE_PROFIT, 42, quantity=Decimal("0.6")),
    ]


def test_the_nodes_own_amend_removing_a_level_cancels_its_leg_only() -> None:
    b = book()
    opened(b)

    records = b.apply(make_event(om.ORDER_REPLACED, protective(25, stop=85000.0)), Amending())

    assert records == [leg(OrderEventKind.CANCELED, Level.TAKE_PROFIT, 25)]


def test_the_nodes_own_cancel_of_the_protective_order_cancels_both_legs_only() -> None:
    b = book()
    opened(b)

    records = b.apply(
        make_event(om.ORDER_CANCELLED, protective(25, stop=85000.0, limit=85500.0)), Amending()
    )

    assert records == [
        leg(OrderEventKind.CANCELED, Level.STOP_LOSS, 25),
        leg(OrderEventKind.CANCELED, Level.TAKE_PROFIT, 25),
    ]


def test_the_nodes_close_rejected_leaves_the_legs() -> None:
    b = book()
    opened(b)
    close = make_order(9_100_003, P, side=om.SELL, closing=True, utc=40)

    records = b.apply(make_event(om.ORDER_REJECTED, close, error="MARKET_CLOSED"), Closing("O-C"))

    assert records == [
        OrderEvent(OrderEventKind.REJECTED, "9100003", "O-C", 40, reason="MARKET_CLOSED")
    ]
    assert b.view(P).legs == {
        Level.STOP_LOSS: (stop_id(P), True),
        Level.TAKE_PROFIT: (target_id(P), True),
    }


def test_a_foreign_order_on_a_loaded_symbol_is_reported_then_followed() -> None:
    b = book()
    created = make_position(8, volume=0, status=om.POSITION_STATUS_CREATED)

    def order(utc, limit=84000.0):
        return make_order(7, 8, order_type=om.LIMIT, utc=utc, limit=limit)

    accepted = b.apply(make_event(om.ORDER_ACCEPTED, order(10), position=created), NOTHING)
    replaced = b.apply(make_event(om.ORDER_REPLACED, order(11, 84100.0), position=created), NOTHING)
    cancelled = b.apply(
        make_event(om.ORDER_CANCELLED, order(12, 84100.0), position=closed(8)), NOTHING
    )

    assert accepted == [
        ExternalOrder(
            "7",
            SYMBOL,
            "BUY",
            ExternalType.LIMIT,
            Decimal("1"),
            False,
            "8",
            10,
            price=Decimal("84000.00"),
        )
    ]
    assert replaced == [
        OrderEvent(
            OrderEventKind.UPDATED,
            "7",
            None,
            11,
            quantity=Decimal("1"),
            price=Decimal("84100.00"),
        )
    ]
    assert cancelled == [OrderEvent(OrderEventKind.CANCELED, "7", None, 12)]


def test_a_foreign_order_filled_after_its_report_is_a_fill() -> None:
    b = book()
    order = make_order(17, 18, order_type=om.LIMIT, utc=10, limit=84000.0)
    b.apply(
        make_event(
            om.ORDER_ACCEPTED,
            order,
            position=make_position(18, volume=0, status=om.POSITION_STATUS_CREATED),
        ),
        NOTHING,
    )

    records = b.apply(
        make_event(
            om.ORDER_FILLED,
            order,
            position=make_position(18),
            deal=make_deal(19, 17, 18, side=om.BUY, volume=100, price=84000.0, ts=11),
        ),
        NOTHING,
    )

    assert records == [
        OrderEvent(
            OrderEventKind.FILLED,
            "17",
            None,
            11,
            fill=fill(19, "84000.00", 11, side="BUY", position=18),
        )
    ]


# Symbols the node has not loaded.

UNLOADED = 279


@pytest.mark.parametrize("with_position", [True, False])
def test_an_unloaded_protective_order_is_a_change_of_its_position(with_position) -> None:
    order = make_order(
        70,
        8,
        order_type=om.STOP_LOSS_TAKE_PROFIT,
        side=om.SELL,
        closing=True,
        utc=50,
        stop=1.0,
        symbol=UNLOADED,
    )
    position = make_position(8, symbol=UNLOADED) if with_position else None

    records = book().apply(make_event(om.ORDER_REPLACED, order, position=position), NOTHING)

    assert records == [
        Activity(
            ActivityKind.UNLOADED_SYMBOL,
            UNLOADED,
            "position",
            "BUY",
            Decimal("1"),
            Action.CHANGED,
            50,
        )
    ]


def test_an_unloaded_pending_order_opens_and_closes_as_an_order() -> None:
    b = book()

    def order(utc):
        return make_order(71, 9, order_type=om.LIMIT, utc=utc, limit=1.0, symbol=UNLOADED)

    opened_ = b.apply(make_event(om.ORDER_ACCEPTED, order(51)), NOTHING)
    cancelled = b.apply(make_event(om.ORDER_CANCELLED, order(52)), NOTHING)

    def activity(action, ts):
        return Activity(
            ActivityKind.UNLOADED_SYMBOL, UNLOADED, "order", "BUY", Decimal("1"), action, ts
        )

    assert opened_ == [activity(Action.OPENED, 51)]
    assert cancelled == [activity(Action.CLOSED, 52)]


@pytest.mark.parametrize(
    ("stop_out_flag", "left", "kind", "action", "units"),
    [
        (False, 60, ActivityKind.UNLOADED_SYMBOL, Action.PARTIALLY_CLOSED, "0.4"),
        (True, 0, ActivityKind.STOP_OUT, Action.CLOSED, "1"),
    ],
)
def test_an_unloaded_close_is_activity(stop_out_flag, left, kind, action, units) -> None:
    volume = 100 - left
    order = make_order(
        72,
        8,
        side=om.SELL,
        closing=True,
        utc=52,
        volume=volume,
        symbol=UNLOADED,
        stop_out=stop_out_flag,
    )
    position = (
        make_position(8, volume=left, symbol=UNLOADED) if left else closed(8, symbol=UNLOADED)
    )

    records = book().apply(
        make_event(
            om.ORDER_FILLED,
            order,
            position=position,
            deal=make_deal(73, 72, 8, side=om.SELL, volume=volume, price=1.0, ts=53),
            server=stop_out_flag,
        ),
        NOTHING,
    )

    assert records == [
        Activity(kind, UNLOADED, "position", "BUY", Decimal(units), action, 53),
    ]


# When the wait for protection ends with nothing to report.


def test_no_missing_protection_for_a_closed_position() -> None:
    b = book()
    entered(b)
    trader_close(b, 9_100_004, 100, 0, 41)

    assert b.protection_timed_out(P) == []


def test_no_missing_protection_for_a_foreign_position() -> None:
    b = book()
    b.apply(
        make_event(
            om.ORDER_FILLED,
            make_order(17, 18),
            position=make_position(18),
            deal=make_deal(19, 17, 18, side=om.BUY, volume=100, price=84000.0, ts=11),
        ),
        NOTHING,
    )

    assert b.protection_timed_out(18) == []
    assert b.protection_timed_out(12345) == []


def test_no_missing_protection_once_every_leg_has_ended() -> None:
    b = book()
    opened(b)
    b.apply(make_event(om.ORDER_CANCELLED, protective(25, stop=85000.0, limit=85500.0)), NOTHING)

    assert b.protection_timed_out(P) == []


@pytest.mark.parametrize(
    ("legs", "cancelled"),
    [(LegIds("O-X-SL", "O-X-TP"), ["O-X-SL", "O-X-TP"]), (LegIds(None, None), [])],
)
def test_an_entry_refused_before_acceptance_takes_whatever_legs_it_has(legs, cancelled) -> None:
    records = book().reject_entry("O-X", legs, "MARKET_CLOSED", 5)

    assert records == [
        OrderEvent(OrderEventKind.REJECTED, None, "O-X", 5, reason="MARKET_CLOSED"),
        *(OrderEvent(OrderEventKind.CANCELED, None, leg_id, 5) for leg_id in cancelled),
    ]


# Fills that arrive without the position.


def entry_filled_alone(b: VenueBook, volume: int = 100, kind=om.ORDER_FILLED) -> None:
    """The node's entry accepted, then filled by `volume` with no position in the event."""
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
            kind,
            our_entry(P, ENTRY, utc=20),
            deal=make_deal(9_200_001, ENTRY, P, side=om.BUY, volume=volume, price=85250.0, ts=20),
        ),
        NOTHING,
    )


def test_an_entry_fill_without_the_position_opens_it() -> None:
    b = book()
    entry_filled_alone(b)

    assert b.view(P).open
    assert b.protection_timed_out(P) == [ProtectionMissing(P, stop_id(P), target_id(P))]


def test_a_partial_close_after_an_entry_fill_without_the_position_leaves_the_legs() -> None:
    b = book()
    entry_filled_alone(b)

    records = trader_close(b, 9_100_004, 40, None, 41)

    assert records == [
        external_close(9_100_004, "0.4", 41),
        manual(Action.PARTIALLY_CLOSED, 41, "0.4"),
    ]
    assert b.view(P).legs == {
        Level.STOP_LOSS: (stop_id(P), True),
        Level.TAKE_PROFIT: (target_id(P), True),
    }


def test_a_remainder_cancelled_after_a_partial_fill_without_the_position_leaves_the_legs() -> None:
    b = book()
    entry_filled_alone(b, 40, om.ORDER_PARTIAL_FILL)

    records = b.apply(make_event(om.ORDER_CANCELLED, our_entry(P, ENTRY, utc=21)), NOTHING)

    assert records == [OrderEvent(OrderEventKind.CANCELED, str(ENTRY), entry_id(P), 21)]
    assert b.view(P).legs == {
        Level.STOP_LOSS: (stop_id(P), True),
        Level.TAKE_PROFIT: (target_id(P), True),
    }


# The protective order's volume is what is left of it.


def test_a_protective_order_reporting_its_executed_volume_counts_only_the_remainder() -> None:
    b = book()
    opened(b)
    trigger(
        b,
        1,
        84990.0,
        30,
        kind=om.ORDER_PARTIAL_FILL,
        deal_volume=40,
        left=60,
        stop=85000.0,
        limit=85500.0,
    )
    replaced = protective(31, stop=85000.0, limit=85500.0)
    replaced.executedVolume = 40

    records = b.apply(
        make_event(om.ORDER_REPLACED, replaced, position=make_position(P, volume=60), server=True),
        NOTHING,
    )

    assert records == [
        leg(OrderEventKind.UPDATED, Level.TAKE_PROFIT, 31, quantity=Decimal("0.6")),
    ]


def test_load_counts_only_the_protective_orders_remainder() -> None:
    b = book()
    response = snapshot(stop=85000.0, target=85500.0, volume=60)
    response.order[0].tradeData.volume = 100
    response.order[0].executedVolume = 40
    b.load(response, {P: [our_entry(P, ENTRY)]})

    records = b.apply(
        make_event(
            om.ORDER_REPLACED,
            protective(6, stop=85000.0, limit=85500.0, volume=60),
            position=make_position(P, volume=60),
            server=True,
        ),
        NOTHING,
    )

    assert records == []


# A retired protective order.


def test_every_event_of_a_retired_protective_order_is_ignored() -> None:
    b = book()
    opened(b)
    b.apply(
        make_event(
            om.ORDER_ACCEPTED,
            make_order(
                9_100_009,
                P,
                order_type=om.STOP_LOSS_TAKE_PROFIT,
                side=om.SELL,
                closing=True,
                utc=26,
                stop=85000.0,
                limit=85500.0,
            ),
            server=True,
        ),
        NOTHING,
    )

    late = b.apply(
        make_event(om.ORDER_ACCEPTED, protective(27, stop=84000.0, limit=86000.0), server=True),
        NOTHING,
    )
    current = b.apply(
        make_event(
            om.ORDER_REPLACED,
            make_order(
                9_100_009,
                P,
                order_type=om.STOP_LOSS_TAKE_PROFIT,
                side=om.SELL,
                closing=True,
                utc=28,
                stop=85100.0,
                limit=85500.0,
            ),
        ),
        Amending(),
    )

    assert late == []
    assert b.view(P).protective_order_id == 9_100_009
    assert current == [
        leg(
            OrderEventKind.UPDATED,
            Level.STOP_LOSS,
            28,
            quantity=Decimal("1"),
            trigger_price=Decimal("85100.00"),
        )
    ]


# A fill is never dropped, whatever the order's id or flags.


def new_protective(utc: int) -> om.ProtoOAOrder:
    return make_order(
        9_100_009,
        P,
        order_type=om.STOP_LOSS_TAKE_PROFIT,
        side=om.SELL,
        closing=True,
        utc=utc,
        stop=85000.0,
        limit=85500.0,
    )


def retired_notice(order_id: int = PROTECTIVE) -> Notice:
    return Notice(
        f"protective order {order_id} filled after it was replaced or cancelled; "
        "the fill is reported all the same"
    )


def test_a_fill_on_a_replaced_protective_id_closes_the_position() -> None:
    b = book()
    opened(b)
    b.apply(make_event(om.ORDER_ACCEPTED, new_protective(26), server=True), NOTHING)

    records = trigger(b, 4, 84990.0, 30, stop=85000.0, limit=85500.0)

    assert records == [
        retired_notice(),
        leg(OrderEventKind.FILLED, Level.STOP_LOSS, 30, fill=fill(4, "84990.00", 30)),
        leg(OrderEventKind.CANCELED, Level.TAKE_PROFIT, 30),
    ]
    view = b.view(P)
    assert not view.open and view.protective_order_id is None and view.levels == {}


def test_a_fill_on_a_cancelled_then_reaccepted_protective_id_is_reported() -> None:
    b = book()
    opened(b)
    b.apply(make_event(om.ORDER_CANCELLED, protective(25, stop=85000.0, limit=85500.0)), NOTHING)
    b.apply(make_event(om.ORDER_ACCEPTED, protective(26, stop=84900.0)), NOTHING)

    records = trigger(b, 4, 84890.0, 30, stop=84900.0)

    assert records == [
        retired_notice(),
        ExternalOrder(
            str(PROTECTIVE),
            SYMBOL,
            "SELL",
            ExternalType.STOP_MARKET,
            Decimal("1"),
            True,
            str(P),
            30,
            trigger_price=Decimal("84900.00"),
            fills=(fill(4, "84890.00", 30),),
        ),
    ]
    assert not b.view(P).open


def test_a_stop_out_without_closing_order_still_ends_the_legs() -> None:
    b = book()
    opened(b)
    order = make_order(9_100_003, P, side=om.SELL, utc=40, stop_out=True)

    records = b.apply(
        make_event(
            om.ORDER_FILLED,
            order,
            position=closed(),
            deal=make_deal(9_200_003, 9_100_003, P, side=om.SELL, volume=100, price=84000.0, ts=41),
            server=True,
        ),
        NOTHING,
    )

    assert records == [
        ExternalOrder(
            "9100003",
            SYMBOL,
            "SELL",
            ExternalType.MARKET,
            Decimal("1"),
            False,
            str(P),
            40,
            fills=(fill(9_200_003, "84000.00", 41),),
        ),
        Activity(ActivityKind.STOP_OUT, SYMBOL, "position", "BUY", Decimal("1"), Action.CLOSED, 41),
        leg(OrderEventKind.CANCELED, Level.STOP_LOSS, 41),
        leg(OrderEventKind.CANCELED, Level.TAKE_PROFIT, 41),
    ]
    view = b.view(P)
    assert view.protective_order_id is None and view.levels == {}


def test_a_close_the_broker_made_for_no_named_reason_is_a_notice() -> None:
    b = book()
    opened(b)

    records = b.apply(
        make_event(
            om.ORDER_FILLED,
            make_order(9_100_004, P, side=om.SELL, closing=True, utc=40),
            position=closed(),
            deal=make_deal(9_100_004, 9_100_004, P, side=om.SELL, volume=100, price=85300.0, ts=41),
            server=True,
        ),
        NOTHING,
    )

    assert records == [
        Notice(
            f"the broker closed 1 of position {P} on its own, for a reason the event does not name"
        ),
        external_close(9_100_004, "1", 41),
        leg(OrderEventKind.CANCELED, Level.STOP_LOSS, 41),
        leg(OrderEventKind.CANCELED, Level.TAKE_PROFIT, 41),
    ]


# The protective order known before the entry's fill.


def test_legs_are_accepted_at_the_fill_when_the_protective_order_came_first() -> None:
    b = book()
    b.apply(
        make_event(
            om.ORDER_ACCEPTED,
            protective(21, stop=85000.0, limit=85500.0),
            position=make_position(P),
            server=True,
        ),
        NOTHING,
    )
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
            om.ORDER_FILLED,
            our_entry(P, ENTRY, utc=20),
            position=make_position(P),
            deal=make_deal(9_200_001, ENTRY, P, side=om.BUY, volume=100, price=85250.0, ts=20),
        ),
        NOTHING,
    )

    assert records == [
        OrderEvent(
            OrderEventKind.FILLED,
            str(ENTRY),
            entry_id(P),
            20,
            fill=fill(9_200_001, "85250.00", 20, side="BUY"),
        ),
        leg(
            OrderEventKind.ACCEPTED,
            Level.STOP_LOSS,
            20,
            quantity=Decimal("1"),
            trigger_price=Decimal("85000.00"),
        ),
        leg(
            OrderEventKind.ACCEPTED,
            Level.TAKE_PROFIT,
            20,
            quantity=Decimal("1"),
            price=Decimal("85500.00"),
        ),
    ]
    assert b.protection_timed_out(P) == []


# Every leg update carries the leg's quantity.


def test_a_level_and_volume_change_in_one_replace_is_one_update_per_leg() -> None:
    b = book()
    opened(b)

    records = b.apply(
        make_event(
            om.ORDER_REPLACED,
            protective(42, stop=85100.0, limit=85500.0, volume=60),
            position=make_position(P, volume=60),
        ),
        Amending(),
    )

    assert records == [
        leg(
            OrderEventKind.UPDATED,
            Level.STOP_LOSS,
            42,
            quantity=Decimal("0.6"),
            trigger_price=Decimal("85100.00"),
        ),
        leg(OrderEventKind.UPDATED, Level.TAKE_PROFIT, 42, quantity=Decimal("0.6")),
    ]


# What an external order's report carries for its time in force.


def test_a_foreign_good_till_date_order_carries_its_expiry_and_acceptance_time() -> None:
    b = book()
    order = make_order(7, 8, order_type=om.LIMIT, utc=10, limit=84000.0)
    order.timeInForce = om.GOOD_TILL_DATE
    order.expirationTimestamp = 99_000
    order.tradeData.openTimestamp = 9

    accepted = b.apply(make_event(om.ORDER_ACCEPTED, order), NOTHING)
    order.utcLastUpdateTimestamp = 99_000
    expired = b.apply(make_event(om.ORDER_EXPIRED, order), NOTHING)

    assert accepted == [
        ExternalOrder(
            "7",
            SYMBOL,
            "BUY",
            ExternalType.LIMIT,
            Decimal("1"),
            False,
            "8",
            10,
            price=Decimal("84000.00"),
            time_in_force="GOOD_TILL_DATE",
            expire_ts_ms=99_000,
            ts_accepted_ms=9,
        )
    ]
    assert expired == [OrderEvent(OrderEventKind.EXPIRED, "7", None, 99_000)]


# A never-opened entry leaves nothing behind.

_ENDED_AS = {
    om.ORDER_REJECTED: OrderEventKind.REJECTED,
    om.ORDER_CANCELLED: OrderEventKind.CANCELED,
    om.ORDER_EXPIRED: OrderEventKind.EXPIRED,
}


@pytest.mark.parametrize(
    ("kind", "error"), [(om.ORDER_REJECTED, "X"), (om.ORDER_CANCELLED, ""), (om.ORDER_EXPIRED, "")]
)
def test_a_never_opened_entry_that_ended_is_dropped(kind, error) -> None:
    b = book()
    created = make_position(P, volume=0, status=om.POSITION_STATUS_CREATED)
    b.apply(make_event(om.ORDER_ACCEPTED, our_entry(P, ENTRY, utc=10), position=created), NOTHING)

    ended = b.apply(make_event(kind, our_entry(P, ENTRY, utc=11), error=error), NOTHING)
    late = b.apply(
        make_event(om.ORDER_ACCEPTED, our_entry(P, ENTRY, utc=12), position=created), NOTHING
    )

    assert [r.kind for r in ended] == [
        _ENDED_AS[kind],
        OrderEventKind.CANCELED,
        OrderEventKind.CANCELED,
    ]
    assert late == []
    assert b.view(P) is None


def test_an_entry_whose_remainder_ended_after_a_fill_is_kept() -> None:
    b = book()
    entry_filled_alone(b, 40, om.ORDER_PARTIAL_FILL)

    b.apply(make_event(om.ORDER_CANCELLED, our_entry(P, ENTRY, utc=21)), NOTHING)

    assert b.view(P) is not None and b.view(P).open


# Residuals: server fills on the opening route, fills on an ended entry, partial protection.


def test_a_server_fill_that_reduces_a_position_on_the_opening_route_is_a_notice() -> None:
    b = book()
    opened(b)

    records = b.apply(
        make_event(
            om.ORDER_FILLED,
            make_order(9_100_003, P, side=om.SELL, utc=40),
            position=closed(),
            deal=make_deal(9_200_003, 9_100_003, P, side=om.SELL, volume=100, price=84000.0, ts=41),
            server=True,
        ),
        NOTHING,
    )

    assert records == [
        Notice(
            f"the broker closed 1 of position {P} on its own, for a reason the event does not name"
        ),
        ExternalOrder(
            "9100003",
            SYMBOL,
            "SELL",
            ExternalType.MARKET,
            Decimal("1"),
            False,
            str(P),
            40,
            fills=(fill(9_200_003, "84000.00", 41),),
        ),
        leg(OrderEventKind.CANCELED, Level.STOP_LOSS, 41),
        leg(OrderEventKind.CANCELED, Level.TAKE_PROFIT, 41),
    ]


def test_a_triggered_pending_order_is_a_server_fill_without_a_notice() -> None:
    b = book()
    order = make_order(17, 18, order_type=om.LIMIT, utc=10, limit=84000.0)
    b.apply(
        make_event(
            om.ORDER_ACCEPTED,
            order,
            position=make_position(18, volume=0, status=om.POSITION_STATUS_CREATED),
        ),
        NOTHING,
    )

    records = b.apply(
        make_event(
            om.ORDER_FILLED,
            order,
            position=make_position(18),
            deal=make_deal(19, 17, 18, side=om.BUY, volume=100, price=84000.0, ts=11),
            server=True,
        ),
        NOTHING,
    )

    assert records == [
        OrderEvent(
            OrderEventKind.FILLED,
            "17",
            None,
            11,
            fill=fill(19, "84000.00", 11, side="BUY", position=18),
        )
    ]


def test_a_fill_on_an_ended_entry_is_reported_with_a_notice_and_its_legs_dead() -> None:
    b = book()
    b.apply(
        make_event(
            om.ORDER_ACCEPTED,
            our_entry(P, ENTRY, utc=10),
            position=make_position(P, volume=0, status=om.POSITION_STATUS_CREATED),
        ),
        NOTHING,
    )
    b.apply(make_event(om.ORDER_CANCELLED, our_entry(P, ENTRY, utc=11)), NOTHING)

    records = b.apply(
        make_event(
            om.ORDER_PARTIAL_FILL,
            our_entry(P, ENTRY, utc=12),
            position=make_position(P, volume=40),
            deal=make_deal(1, ENTRY, P, side=om.BUY, volume=40, price=85250.0, ts=12),
        ),
        NOTHING,
    )

    assert records == [
        Notice(f"entry {ENTRY} filled after it ended"),
        OrderEvent(
            OrderEventKind.FILLED,
            str(ENTRY),
            entry_id(P),
            12,
            fill=fill(1, "85250.00", 12, units="0.4", side="BUY"),
        ),
    ]
    view = b.view(P)
    assert view.open
    assert view.legs == {
        Level.STOP_LOSS: (stop_id(P), False),
        Level.TAKE_PROFIT: (target_id(P), False),
    }


def test_a_leg_the_early_protective_order_lacks_still_awaits_protection() -> None:
    b = book()
    b.apply(
        make_event(
            om.ORDER_ACCEPTED,
            protective(15, stop=85000.0),
            position=make_position(P),
            server=True,
        ),
        NOTHING,
    )
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
            om.ORDER_FILLED,
            our_entry(P, ENTRY, utc=19),
            position=make_position(P),
            deal=make_deal(9_200_001, ENTRY, P, side=om.BUY, volume=100, price=85250.0, ts=20),
        ),
        NOTHING,
    )

    assert records == [
        OrderEvent(
            OrderEventKind.FILLED,
            str(ENTRY),
            entry_id(P),
            20,
            fill=fill(9_200_001, "85250.00", 20, side="BUY"),
        ),
        # The legs are accepted at the deal's time, never before the entry's fill.
        leg(
            OrderEventKind.ACCEPTED,
            Level.STOP_LOSS,
            20,
            quantity=Decimal("1"),
            trigger_price=Decimal("85000.00"),
        ),
        AwaitProtection(P),
    ]
    assert b.protection_timed_out(P) == [ProtectionMissing(P, None, target_id(P))]


def test_a_leg_is_found_by_its_client_order_id() -> None:
    b = book()
    opened(b)

    assert b.leg_position(stop_id(P)) == (P, Level.STOP_LOSS)
    assert b.leg_position(target_id(P)) == (P, Level.TAKE_PROFIT)
    assert b.leg_position(entry_id(P)) is None
    assert b.leg_position("O-unknown") is None


def test_the_view_holds_the_quantity_of_each_accepted_live_leg() -> None:
    b = book()
    opened(b)
    assert b.view(P).leg_units == {
        Level.STOP_LOSS: Decimal("1"),
        Level.TAKE_PROFIT: Decimal("1"),
    }

    b.apply(make_event(om.ORDER_REPLACED, protective(25, stop=85000.0)), Amending())

    assert b.view(P).leg_units == {Level.STOP_LOSS: Decimal("1")}


def test_a_leg_whose_level_the_broker_lacks_is_cancelled_without_a_request() -> None:
    b = book()
    entry_filled_alone(b)

    assert b.cancel_leg(P, Level.STOP_LOSS, 30) == [
        leg(OrderEventKind.CANCELED, Level.STOP_LOSS, 30)
    ]
    assert b.view(P).legs[Level.STOP_LOSS] == (stop_id(P), False)
    assert b.cancel_leg(P, Level.STOP_LOSS, 31) == []


def test_a_leg_whose_level_stands_takes_an_amend_to_cancel() -> None:
    b = book()
    opened(b)

    assert b.cancel_leg(P, Level.STOP_LOSS, 30) == []
    assert b.view(P).legs[Level.STOP_LOSS] == (stop_id(P), True)


def test_legs_never_accepted_are_rejected_with_the_brokers_reason() -> None:
    b = book()
    entry_filled_alone(b)
    reason = "TRADING_BAD_STOPS: invalid stops"

    assert b.reject_legs(P, reason, 30) == [
        leg(OrderEventKind.REJECTED, Level.STOP_LOSS, 30, reason=reason),
        leg(OrderEventKind.REJECTED, Level.TAKE_PROFIT, 30, reason=reason),
    ]
    assert b.protection_timed_out(P) == []


def test_accepted_legs_are_never_rejected() -> None:
    b = book()
    opened(b)

    assert b.reject_legs(P, "any", 30) == []


def test_a_deal_moves_the_volume_once_even_if_its_first_handling_failed() -> None:
    b = book()
    b.apply(
        make_event(
            om.ORDER_ACCEPTED,
            our_entry(P, ENTRY, utc=10),
            position=make_position(P, volume=0, status=om.POSITION_STATUS_CREATED),
        ),
        NOTHING,
    )

    def filled(price: float) -> oa.ProtoOAExecutionEvent:
        # No position in the event: the deal's own volume moves the position.
        return make_event(
            om.ORDER_FILLED,
            our_entry(P, ENTRY, utc=20),
            deal=make_deal(9_200_001, ENTRY, P, side=om.BUY, volume=100, price=price, ts=20),
        )

    with pytest.raises(ValueError):
        b.apply(filled(float("nan")), NOTHING)
    b.apply(filled(85250.0), NOTHING)

    assert b.view(P).units == Decimal("1")


def test_a_late_event_never_takes_a_position_back_to_an_older_state() -> None:
    b = book()
    position = make_position(P)
    position.utcLastUpdateTimestamp = 20
    filled = make_event(
        om.ORDER_FILLED,
        our_entry(P, ENTRY, utc=20),
        position=position,
        deal=make_deal(9_200_001, ENTRY, P, side=om.BUY, volume=100, price=85250.0, ts=20),
    )
    # A created position carries no update time: it precedes every other state of it.
    accepted = make_event(
        om.ORDER_ACCEPTED,
        our_entry(P, ENTRY, utc=10),
        position=make_position(P, volume=0, status=om.POSITION_STATUS_CREATED),
    )

    b.apply(filled, NOTHING)
    b.apply(accepted, NOTHING)

    assert b.view(P).open
    assert b.view(P).units == Decimal("1")


# Exposure on symbols the node has not loaded, and the closes the model has matched.


def test_exposure_lists_unloaded_positions_and_orders_sorted() -> None:
    far, near = UNLOADED + 1, UNLOADED
    response = oa.ProtoOAReconcileRes(ctidTraderAccountId=1_000_001)
    response.position.append(make_position(8, volume=250, symbol=far, side=om.SELL))
    response.position.append(make_position(P))
    response.order.append(
        make_order(71, 0, order_type=om.LIMIT, limit=1.0, volume=40, symbol=near, side=om.BUY)
    )
    response.order.append(
        make_order(72, 8, order_type=om.STOP_LOSS_TAKE_PROFIT, stop=1.0, side=om.BUY, symbol=far)
    )
    b = book()

    b.load(response, {})

    assert b.exposure() == (
        Exposure(near, "order", "BUY", units_of(40)),
        Exposure(far, "position", "SELL", units_of(250)),
    )


def test_exposure_follows_unloaded_events() -> None:
    b = book()
    b.load(oa.ProtoOAReconcileRes(ctidTraderAccountId=1_000_001), {})

    def pending(utc):
        return make_order(71, 0, order_type=om.LIMIT, utc=utc, limit=1.0, symbol=UNLOADED)

    def apply(event):
        b.apply(event, NOTHING)
        return b.exposure()

    after_accept = apply(make_event(om.ORDER_ACCEPTED, pending(51)))
    after_cancel = apply(make_event(om.ORDER_CANCELLED, pending(52)))
    after_entry = apply(
        make_event(
            om.ORDER_FILLED,
            make_order(72, 8, utc=53, symbol=UNLOADED),
            position=make_position(8, symbol=UNLOADED),
            deal=make_deal(9, 72, 8, side=om.BUY, volume=100, price=1.0, ts=53),
        )
    )
    after_close = apply(
        make_event(
            om.ORDER_FILLED,
            make_order(73, 8, side=om.SELL, closing=True, utc=54, symbol=UNLOADED),
            position=closed(8, symbol=UNLOADED),
            deal=make_deal(10, 73, 8, side=om.SELL, volume=100, price=1.0, ts=54),
        )
    )

    assert after_accept == (Exposure(UNLOADED, "order", "BUY", Decimal("1")),)
    assert after_cancel == ()
    assert after_entry == (Exposure(UNLOADED, "position", "BUY", Decimal("1")),)
    assert after_close == ()


def test_close_order_is_known_once_matched() -> None:
    b = book()
    opened(b)
    close = make_order(9_100_003, P, side=om.SELL, closing=True, utc=40)
    assert b.close_order("O-C-1") is None

    b.apply(make_event(om.ORDER_ACCEPTED, close, position=make_position(P)), Closing("O-C-1"))

    assert b.close_order("O-C-1") == (9_100_003, P)
    assert b.close_order("O-C-2") is None
    assert b.known_closes() == {9_100_003: "O-C-1"}
    b.known_closes().clear()
    assert b.known_closes() == {9_100_003: "O-C-1"}


def test_a_fill_that_cannot_be_built_does_not_consume_the_acceptance() -> None:
    b = book()

    def filled(price: float, deal_id: int) -> oa.ProtoOAExecutionEvent:
        return make_event(
            om.ORDER_FILLED,
            our_entry(P, ENTRY, utc=20),
            position=make_position(P),
            deal=make_deal(deal_id, ENTRY, P, side=om.BUY, volume=100, price=price, ts=20),
        )

    with pytest.raises(ValueError):
        b.apply(filled(float("nan"), 9_200_001), NOTHING)
    records = b.apply(filled(85250.0, 9_200_002), NOTHING)

    assert records[0] == OrderEvent(OrderEventKind.ACCEPTED, str(ENTRY), entry_id(P), 20)


def test_a_fill_of_the_nodes_close_that_cannot_be_built_does_not_consume_the_acceptance() -> None:
    b = book()
    opened(b)
    close = make_order(9_100_003, P, side=om.SELL, closing=True, utc=40)

    def filled(price: float, deal_id: int) -> oa.ProtoOAExecutionEvent:
        return make_event(
            om.ORDER_FILLED,
            close,
            position=make_position(P, volume=0, status=om.POSITION_STATUS_CLOSED),
            deal=make_deal(deal_id, 9_100_003, P, side=om.SELL, volume=100, price=price, ts=41),
        )

    with pytest.raises(ValueError):
        b.apply(filled(float("nan"), 9_200_003), Closing("O-C"))
    records = b.apply(filled(85300.0, 9_200_004), Closing("O-C"))

    assert records[0] == OrderEvent(OrderEventKind.ACCEPTED, "9100003", "O-C", 40)


def test_a_replaced_pending_order_changes_its_units_and_a_partial_fill_takes_what_filled() -> None:
    b = book()
    b.load(oa.ProtoOAReconcileRes(ctidTraderAccountId=1_000_001), {})

    def pending(utc, volume):
        return make_order(
            71, 0, order_type=om.LIMIT, utc=utc, limit=1.0, volume=volume, symbol=UNLOADED
        )

    b.apply(make_event(om.ORDER_ACCEPTED, pending(51, 100)), NOTHING)
    b.apply(make_event(om.ORDER_REPLACED, pending(52, 300)), NOTHING)
    b.apply(
        make_event(
            om.ORDER_PARTIAL_FILL,
            pending(53, 300),
            position=make_position(8, volume=100, symbol=UNLOADED),
            deal=make_deal(9, 71, 8, side=om.BUY, volume=100, price=1.0, ts=53),
        ),
        NOTHING,
    )

    assert b.exposure() == (
        Exposure(UNLOADED, "order", "BUY", Decimal("2")),
        Exposure(UNLOADED, "position", "BUY", Decimal("1")),
    )


# A position the node did not open: its levels are external legs named after its entry.

FP, FENTRY, FPROTECTIVE = 9_000_005, 9_100_005, 9_100_006
PROTECTED_AT = 21
BOTH = {"stop": 85000.0, "limit": 85500.0}


def foreign_protective(utc: int, *, stop=None, limit=None, volume: int = 100):
    order = make_order(
        FPROTECTIVE,
        FP,
        order_type=om.STOP_LOSS_TAKE_PROFIT,
        side=om.SELL,
        closing=True,
        utc=utc,
        stop=stop,
        limit=limit,
        volume=volume,
    )
    order.tradeData.openTimestamp = PROTECTED_AT
    return order


def foreign_entry_filled() -> oa.ProtoOAExecutionEvent:
    return make_event(
        om.ORDER_FILLED,
        make_order(FENTRY, FP, utc=20),
        position=make_position(FP),
        deal=make_deal(9_200_005, FENTRY, FP, side=om.BUY, volume=100, price=85250.0, ts=20),
    )


def foreign_opened(b: VenueBook) -> list:
    """A trader's market buy FP, filled, then protected; returns the records of the protection."""
    b.apply(
        make_event(
            om.ORDER_ACCEPTED,
            make_order(FENTRY, FP, utc=10),
            position=make_position(FP, volume=0, status=om.POSITION_STATUS_CREATED),
        ),
        NOTHING,
    )
    b.apply(foreign_entry_filled(), NOTHING)
    return b.apply(
        make_event(
            om.ORDER_ACCEPTED,
            foreign_protective(PROTECTED_AT, **BOTH),
            position=make_position(FP),
            server=True,
        ),
        NOTHING,
    )


def foreign_leg_id(level: Level, generation: int = 1) -> str:
    return leg_venue_order_id(FENTRY, level, generation)


def first_seen(level: Level, ts: int, price: str, *, generation=1) -> ExternalOrder:
    stop = level == Level.STOP_LOSS
    return ExternalOrder(
        foreign_leg_id(level, generation),
        SYMBOL,
        "SELL",
        ExternalType.STOP_MARKET if stop else ExternalType.LIMIT,
        Decimal("1"),
        True,
        str(FP),
        ts,
        price=None if stop else Decimal(price),
        trigger_price=Decimal(price) if stop else None,
        time_in_force="GOOD_TILL_CANCEL",
        ts_accepted_ms=PROTECTED_AT,
    )


def foreign(kind, level, ts, *, generation=1, **kw) -> OrderEvent:
    return OrderEvent(kind, foreign_leg_id(level, generation), None, ts, **kw)


def foreign_trigger(deal_id, price, ts, *, deal_volume=100, left=0, **order):
    position = make_position(FP, volume=left) if left else closed(FP)
    return make_event(
        om.ORDER_PARTIAL_FILL if left else om.ORDER_FILLED,
        foreign_protective(ts, **order),
        position=position,
        deal=make_deal(
            deal_id, FPROTECTIVE, FP, side=om.SELL, volume=deal_volume, price=price, ts=ts
        ),
        server=True,
    )


def foreign_close(order_id: int, volume: int, left: int, ts: int):
    return make_event(
        om.ORDER_FILLED,
        make_order(order_id, FP, side=om.SELL, closing=True, utc=ts - 1, volume=volume),
        position=make_position(FP, volume=left) if left else closed(FP),
        deal=make_deal(order_id, order_id, FP, side=om.SELL, volume=volume, price=85300.0, ts=ts),
    )


def test_a_foreign_positions_levels_are_first_seen_as_reduce_only_external_orders() -> None:
    b = book()

    assert foreign_opened(b) == [
        first_seen(Level.STOP_LOSS, PROTECTED_AT, "85000.00"),
        first_seen(Level.TAKE_PROFIT, PROTECTED_AT, "85500.00"),
    ]
    view = b.view(FP)
    assert not view.ours and view.entry_order_id == FENTRY
    assert view.legs == {}
    assert view.foreign_legs == {
        Level.STOP_LOSS: f"{FENTRY}-SL",
        Level.TAKE_PROFIT: f"{FENTRY}-TP",
    }
    assert view.leg_units == {Level.STOP_LOSS: Decimal("1"), Level.TAKE_PROFIT: Decimal("1")}


@pytest.mark.parametrize(
    ("event", "expected"),
    [
        pytest.param(
            make_event(om.ORDER_REPLACED, foreign_protective(25, stop=85100.0, limit=85500.0)),
            [
                foreign(
                    OrderEventKind.UPDATED,
                    Level.STOP_LOSS,
                    25,
                    quantity=Decimal("1"),
                    trigger_price=Decimal("85100.00"),
                )
            ],
            id="stop-loss moved",
        ),
        pytest.param(
            make_event(om.ORDER_REPLACED, foreign_protective(25, stop=85000.0, limit=85400.0)),
            [
                foreign(
                    OrderEventKind.UPDATED,
                    Level.TAKE_PROFIT,
                    25,
                    quantity=Decimal("1"),
                    price=Decimal("85400.00"),
                )
            ],
            id="take-profit moved",
        ),
        pytest.param(
            make_event(om.ORDER_REPLACED, foreign_protective(25, stop=85000.0)),
            [foreign(OrderEventKind.CANCELED, Level.TAKE_PROFIT, 25)],
            id="take-profit removed",
        ),
        pytest.param(
            make_event(om.ORDER_CANCELLED, foreign_protective(25, **BOTH)),
            [
                foreign(OrderEventKind.CANCELED, Level.STOP_LOSS, 25),
                foreign(OrderEventKind.CANCELED, Level.TAKE_PROFIT, 25),
            ],
            id="protective order cancelled",
        ),
        pytest.param(
            make_event(
                om.ORDER_REPLACED,
                foreign_protective(42, volume=60, **BOTH),
                position=make_position(FP, volume=60),
                server=True,
            ),
            [
                foreign(OrderEventKind.UPDATED, Level.STOP_LOSS, 42, quantity=Decimal("0.6")),
                foreign(OrderEventKind.UPDATED, Level.TAKE_PROFIT, 42, quantity=Decimal("0.6")),
            ],
            id="protective order follows a partial close",
        ),
        pytest.param(
            foreign_trigger(9_200_006, 84990.0, 30, **BOTH),
            [
                foreign(
                    OrderEventKind.FILLED,
                    Level.STOP_LOSS,
                    30,
                    fill=fill(9_200_006, "84990.00", 30, position=FP),
                ),
                foreign(OrderEventKind.CANCELED, Level.TAKE_PROFIT, 30),
            ],
            id="stop-loss triggered",
        ),
        pytest.param(
            foreign_trigger(9_200_006, 85510.0, 30, **BOTH),
            [
                foreign(
                    OrderEventKind.FILLED,
                    Level.TAKE_PROFIT,
                    30,
                    fill=fill(9_200_006, "85510.00", 30, position=FP),
                ),
                foreign(OrderEventKind.CANCELED, Level.STOP_LOSS, 30),
            ],
            id="take-profit triggered",
        ),
        pytest.param(
            foreign_close(9_100_007, 100, 0, 41),
            [
                ExternalOrder(
                    "9100007",
                    SYMBOL,
                    "SELL",
                    ExternalType.MARKET,
                    Decimal("1"),
                    True,
                    str(FP),
                    40,
                    fills=(fill(9_100_007, "85300.00", 41, position=FP),),
                ),
                foreign(OrderEventKind.CANCELED, Level.STOP_LOSS, 41),
                foreign(OrderEventKind.CANCELED, Level.TAKE_PROFIT, 41),
            ],
            id="closed by a trader",
        ),
    ],
)
def test_a_foreign_legs_later_news_is_order_events_on_its_venue_order_id(event, expected) -> None:
    b = book()
    foreign_opened(b)

    assert b.apply(event, NOTHING) == expected


@pytest.mark.parametrize(
    ("held_closed", "generation"),
    [
        # Nautilus may not have applied the first leg's cancel yet: its id is never reused.
        (set(), 2),
        ({f"{FENTRY}-TP-2"}, 3),
    ],
)
def test_a_foreign_level_put_back_after_its_leg_closed_is_a_new_generation(
    held_closed, generation
) -> None:
    b = VenueBook(precision, held_closed=held_closed.__contains__)
    foreign_opened(b)
    b.apply(make_event(om.ORDER_REPLACED, foreign_protective(25, stop=85000.0)), NOTHING)

    records = b.apply(
        make_event(om.ORDER_REPLACED, foreign_protective(26, stop=85000.0, limit=85600.0)),
        NOTHING,
    )

    assert records == [first_seen(Level.TAKE_PROFIT, 26, "85600.00", generation=generation)]
    assert b.view(FP).foreign_legs[Level.TAKE_PROFIT] == foreign_leg_id(
        Level.TAKE_PROFIT, generation
    )


@pytest.mark.parametrize(
    ("held_closed", "generation"),
    [(set(), 1), ({f"{FENTRY}-SL"}, 2)],
)
def test_a_foreign_leg_takes_the_first_generation_nautilus_does_not_hold_closed(
    held_closed, generation
) -> None:
    b = VenueBook(precision, held_closed=held_closed.__contains__)

    assert foreign_opened(b) == [
        first_seen(Level.STOP_LOSS, PROTECTED_AT, "85000.00", generation=generation),
        first_seen(Level.TAKE_PROFIT, PROTECTED_AT, "85500.00"),
    ]


def test_a_partial_close_then_a_trigger_before_the_protective_order_follows() -> None:
    b = book()
    foreign_opened(b)
    b.apply(foreign_close(9_100_007, 1, 99, 41), NOTHING)

    records = b.apply(foreign_trigger(9_200_008, 84990.0, 42, deal_volume=99, **BOTH), NOTHING)

    # The stop-loss fills what was left; the rest of its older quantity is cancelled with it.
    assert records == [
        foreign(
            OrderEventKind.FILLED,
            Level.STOP_LOSS,
            42,
            fill=fill(9_200_008, "84990.00", 42, units="0.99", position=FP),
        ),
        foreign(OrderEventKind.CANCELED, Level.STOP_LOSS, 42),
        foreign(OrderEventKind.CANCELED, Level.TAKE_PROFIT, 42),
    ]


def test_a_foreign_legs_quantity_is_raised_before_a_fill_above_it() -> None:
    b = book()
    foreign_opened(b)
    b.apply(
        make_event(
            om.ORDER_REPLACED,
            foreign_protective(25, volume=60, **BOTH),
            position=make_position(FP, volume=60),
            server=True,
        ),
        NOTHING,
    )

    records = b.apply(foreign_trigger(9_200_008, 84990.0, 30, **BOTH), NOTHING)

    assert records == [
        foreign(OrderEventKind.UPDATED, Level.STOP_LOSS, 30, quantity=Decimal("1")),
        foreign(
            OrderEventKind.FILLED,
            Level.STOP_LOSS,
            30,
            fill=fill(9_200_008, "84990.00", 30, position=FP),
        ),
        foreign(OrderEventKind.CANCELED, Level.TAKE_PROFIT, 30),
    ]


def test_a_partial_trigger_fills_a_foreign_leg_twice() -> None:
    b = book()
    foreign_opened(b)

    first = b.apply(foreign_trigger(1, 84990.0, 30, deal_volume=40, left=60, **BOTH), NOTHING)
    second = b.apply(foreign_trigger(2, 84980.0, 31, deal_volume=60, volume=60, **BOTH), NOTHING)

    assert first == [
        foreign(
            OrderEventKind.FILLED,
            Level.STOP_LOSS,
            30,
            fill=fill(1, "84990.00", 30, units="0.4", position=FP),
        ),
    ]
    assert second == [
        foreign(
            OrderEventKind.FILLED,
            Level.STOP_LOSS,
            31,
            fill=fill(2, "84980.00", 31, units="0.6", position=FP),
        ),
        foreign(OrderEventKind.CANCELED, Level.TAKE_PROFIT, 31),
    ]


def test_levels_seen_before_their_entry_become_legs_once_the_entry_is_seen() -> None:
    b = book()
    unknown = b.apply(
        make_event(
            om.ORDER_ACCEPTED,
            foreign_protective(PROTECTED_AT, **BOTH),
            position=make_position(FP),
            server=True,
        ),
        NOTHING,
    )

    learnt = b.apply(foreign_entry_filled(), NOTHING)

    assert unknown == [EntryUnknown(FP)]
    assert isinstance(learnt[0], ExternalOrder) and learnt[0].venue_order_id == str(FENTRY)
    # Never older than the protective order that holds the levels.
    assert learnt[1:] == [
        first_seen(Level.STOP_LOSS, PROTECTED_AT, "85000.00"),
        first_seen(Level.TAKE_PROFIT, PROTECTED_AT, "85500.00"),
    ]


def test_load_takes_a_foreign_entry_and_says_when_none_is_listed() -> None:
    def foreign_snapshot() -> oa.ProtoOAReconcileRes:
        response = oa.ProtoOAReconcileRes(ctidTraderAccountId=1_000_001)
        position = make_position(FP)
        position.stopLoss = 85000.0
        response.position.append(position)
        response.order.append(foreign_protective(5, stop=85000.0))
        return response

    known, unknown = book(), book()

    assert known.load(foreign_snapshot(), {FP: [make_order(FENTRY, FP)]}) == []
    assert unknown.load(foreign_snapshot(), {}) == [EntryUnknown(FP)]
    assert known.view(FP).foreign_legs == {Level.STOP_LOSS: f"{FENTRY}-SL"}
    assert unknown.view(FP).foreign_legs == {}


def trailing(position_id: int, order_id: int, stop: float, utc: int):
    return oa.ProtoOATrailingSLChangedEvent(
        ctidTraderAccountId=1_000_001,
        positionId=position_id,
        orderId=order_id,
        stopPrice=stop,
        utcLastUpdateTimestamp=utc,
    )


def test_a_trailing_stop_moves_a_foreign_stop_loss_leg_once() -> None:
    b = book()
    foreign_opened(b)
    event = trailing(FP, FPROTECTIVE, 85050.0, 30)

    assert b.trailing_stop_moved(event) == [
        foreign(
            OrderEventKind.UPDATED,
            Level.STOP_LOSS,
            30,
            quantity=Decimal("1"),
            trigger_price=Decimal("85050.00"),
        )
    ]
    assert b.trailing_stop_moved(event) == []
    assert b.view(FP).levels[Level.STOP_LOSS] == Decimal("85050.00")


def test_a_trailing_stop_moves_the_nodes_stop_loss_leg_with_no_activity() -> None:
    b = book()
    opened(b)

    assert b.trailing_stop_moved(trailing(P, PROTECTIVE, 85050.0, 30)) == [
        leg(
            OrderEventKind.UPDATED,
            Level.STOP_LOSS,
            30,
            quantity=Decimal("1"),
            trigger_price=Decimal("85050.00"),
        )
    ]


def test_a_trailing_move_of_another_protective_order_or_position_means_nothing() -> None:
    b = book()
    foreign_opened(b)

    assert b.trailing_stop_moved(trailing(FP, 1, 85050.0, 30)) == []
    assert b.trailing_stop_moved(trailing(77, FPROTECTIVE, 85050.0, 30)) == []
    assert b.view(FP).levels[Level.STOP_LOSS] == Decimal("85000.00")


def test_a_trailing_move_of_a_position_with_no_protective_order_means_nothing() -> None:
    theirs, own = book(), book()
    theirs.apply(foreign_entry_filled(), NOTHING)
    entered(own)

    assert theirs.trailing_stop_moved(trailing(FP, FPROTECTIVE, 85050.0, 30)) == []
    assert own.trailing_stop_moved(trailing(P, PROTECTIVE, 85050.0, 30)) == []
    assert theirs.view(FP).levels == {}
    assert own.view(P).levels == {}


def protected_first(b: VenueBook) -> list:
    """FP's protective order, seen before anything else of FP."""
    return b.apply(
        make_event(
            om.ORDER_ACCEPTED,
            foreign_protective(PROTECTED_AT, **BOTH),
            position=make_position(FP),
            server=True,
        ),
        NOTHING,
    )


def test_an_entry_found_in_the_order_list_gives_the_levels_their_legs() -> None:
    b = book()
    assert protected_first(b) == [EntryUnknown(FP)]
    listed = [foreign_protective(PROTECTED_AT, **BOTH), make_order(FENTRY, FP, utc=20)]

    found = b.entry_found(FP, listed)

    assert found == [
        first_seen(Level.STOP_LOSS, PROTECTED_AT, "85000.00"),
        first_seen(Level.TAKE_PROFIT, PROTECTED_AT, "85500.00"),
    ]
    assert b.view(FP).entry_order_id == FENTRY
    # Known now: a second list, and the entry's own fill, add no leg.
    assert b.entry_found(FP, listed) == []
    learnt = b.apply(foreign_entry_filled(), NOTHING)
    assert [type(r) for r in learnt] == [ExternalOrder]
    assert learnt[0].venue_order_id == str(FENTRY)


def test_an_order_list_without_the_entry_or_position_changes_nothing() -> None:
    b = book()
    protected_first(b)

    assert b.entry_found(FP, [foreign_protective(PROTECTED_AT, **BOTH)]) == []
    assert b.entry_found(77, [make_order(FENTRY, 77)]) == []
    assert b.view(FP).entry_order_id is None
    assert b.view(FP).foreign_legs == {}


def test_the_nodes_own_entry_found_in_the_order_list_waits_for_its_events() -> None:
    b = book()
    b.apply(
        make_event(om.ORDER_ACCEPTED, protective(21, **BOTH), position=make_position(P)),
        NOTHING,
    )

    assert b.entry_found(P, [our_entry(P, ENTRY, utc=10)]) == []

    view = b.view(P)
    assert view.ours
    assert view.foreign_legs == {}
    assert view.legs == {
        Level.STOP_LOSS: (stop_id(P), True),
        Level.TAKE_PROFIT: (target_id(P), True),
    }
    filled = b.apply(
        make_event(
            om.ORDER_FILLED,
            our_entry(P, ENTRY, utc=20),
            position=make_position(P),
            deal=make_deal(9_200_001, ENTRY, P, side=om.BUY, volume=100, price=85250.0, ts=20),
        ),
        NOTHING,
    )
    assert [r.kind for r in filled if isinstance(r, OrderEvent)] == [
        OrderEventKind.ACCEPTED,
        OrderEventKind.FILLED,
        OrderEventKind.ACCEPTED,
        OrderEventKind.ACCEPTED,
    ]


# What an amend must send again besides the levels, from the position's last known state.

TRAILING = LevelTerms(
    trailing_stop_loss=True,
    guaranteed_stop_loss=True,
    stop_loss_trigger_method=om.OPPOSITE,
)
PLAIN = LevelTerms(
    trailing_stop_loss=False,
    guaranteed_stop_loss=False,
    stop_loss_trigger_method=om.TRADE,
)


def trailing_position(utc: int) -> om.ProtoOAPosition:
    position = make_position(FP)
    position.trailingStopLoss = True
    position.guaranteedStopLoss = True
    position.stopLossTriggerMethod = om.OPPOSITE
    position.utcLastUpdateTimestamp = utc
    return position


def test_the_view_holds_how_the_positions_stop_loss_works_as_last_known() -> None:
    b = book()
    foreign_opened(b)
    assert b.view(FP).terms == PLAIN

    b.apply(
        make_event(
            om.ORDER_REPLACED,
            foreign_protective(30, stop=85100.0, limit=85500.0),
            position=trailing_position(30),
        ),
        NOTHING,
    )
    assert b.view(FP).terms == TRAILING

    # An older state applied late changes nothing.
    stale = make_position(FP)
    stale.utcLastUpdateTimestamp = 29
    b.apply(
        make_event(
            om.ORDER_REPLACED,
            foreign_protective(29, stop=85050.0, limit=85500.0),
            position=stale,
        ),
        NOTHING,
    )
    assert b.view(FP).terms == TRAILING

    # A field the venue leaves out is the schema's default.
    later = make_position(FP)
    later.utcLastUpdateTimestamp = 31
    b.apply(
        make_event(
            om.ORDER_REPLACED,
            foreign_protective(31, stop=85100.0, limit=85500.0),
            position=later,
        ),
        NOTHING,
    )
    assert b.view(FP).terms == PLAIN


def test_load_takes_how_the_positions_stop_loss_works_from_the_snapshot() -> None:
    b = book()
    loaded = snapshot(stop=85000.0)
    loaded.position[0].trailingStopLoss = True
    loaded.position[0].guaranteedStopLoss = True
    loaded.position[0].stopLossTriggerMethod = om.OPPOSITE

    b.load(loaded, {P: [our_entry(P, ENTRY)]})

    assert b.view(P).terms == TRAILING


def test_a_position_never_seen_in_a_state_has_no_known_terms() -> None:
    b = book()
    b.apply(
        make_event(
            om.ORDER_FILLED,
            our_entry(P, ENTRY, utc=20),
            deal=make_deal(9_200_001, ENTRY, P, side=om.BUY, volume=100, price=85250.0, ts=20),
        ),
        NOTHING,
    )

    assert b.view(P).open
    assert b.view(P).terms is None


def test_a_foreign_leg_is_found_by_its_venue_order_id_in_any_generation() -> None:
    b = book()
    foreign_opened(b)
    opened(b)

    assert b.foreign_leg_position(foreign_leg_id(Level.STOP_LOSS)) == (FP, Level.STOP_LOSS)
    assert b.foreign_leg_position(foreign_leg_id(Level.TAKE_PROFIT, 2)) == (FP, Level.TAKE_PROFIT)
    # The node's own legs, an entry never seen and anything else are no foreign leg.
    assert b.foreign_leg_position(leg_venue_order_id(ENTRY, Level.STOP_LOSS)) is None
    assert b.foreign_leg_position(leg_venue_order_id(9_999_999, Level.STOP_LOSS)) is None
    assert b.foreign_leg_position(str(FENTRY)) is None
    assert b.foreign_leg_position(stop_id(P)) is None


# The last state of each open pending order Nautilus knows from reports, which an amend sends
# again.


def resting(utc: int, *, limit: float = 84000.0, volume: int = 100, kind: int = om.LIMIT):
    return make_order(7, 8, order_type=kind, utc=utc, limit=limit, volume=volume)


CREATED = make_position(8, volume=0, status=om.POSITION_STATUS_CREATED)


def test_an_open_pending_order_is_held_as_last_changed() -> None:
    b = book()
    b.apply(make_event(om.ORDER_ACCEPTED, resting(10)), NOTHING)
    assert b.open_order(7) == resting(10)

    b.apply(make_event(om.ORDER_REPLACED, resting(12, limit=84100.0)), NOTHING)
    # A response applied after a later event leaves the later state.
    b.apply(make_event(om.ORDER_REPLACED, resting(11, limit=84050.0)), NOTHING)

    assert b.open_order(7) == resting(12, limit=84100.0)


def test_a_partly_filled_pending_order_is_held_with_its_fill() -> None:
    b = book()
    b.apply(make_event(om.ORDER_ACCEPTED, resting(10), position=CREATED), NOTHING)
    partly = resting(11)
    partly.executedVolume = 40
    deal = make_deal(9_300_001, 7, 8, side=om.BUY, volume=40, price=84000.0, ts=11)

    b.apply(
        make_event(om.ORDER_PARTIAL_FILL, partly, position=make_position(8), deal=deal), NOTHING
    )

    assert b.open_order(7) == partly


@pytest.mark.parametrize("kind", [om.ORDER_CANCELLED, om.ORDER_EXPIRED, om.ORDER_FILLED])
def test_an_ended_pending_order_is_no_longer_held(kind) -> None:
    b = book()
    b.apply(make_event(om.ORDER_ACCEPTED, resting(10), position=CREATED), NOTHING)
    deal = make_deal(9_300_001, 7, 8, side=om.BUY, volume=100, price=84000.0, ts=11)

    b.apply(
        make_event(
            kind,
            resting(11),
            position=make_position(8),
            deal=deal if kind == om.ORDER_FILLED else None,
        ),
        NOTHING,
    )
    b.apply(make_event(om.ORDER_REPLACED, resting(12, limit=84100.0)), NOTHING)

    assert b.open_order(7) is None
    assert b.standing_order(7, 13) == []


def test_a_market_order_is_never_held() -> None:
    b = book()
    b.apply(make_event(om.ORDER_ACCEPTED, make_order(7, 8, utc=10), position=CREATED), NOTHING)

    assert b.open_order(7) is None


def test_load_holds_the_open_pending_orders_and_forgets_the_rest() -> None:
    b = book()
    b.apply(make_event(om.ORDER_ACCEPTED, resting(10)), NOTHING)
    other = make_order(17, 18, order_type=om.STOP, utc=3, stop=85000.0)

    b.load(snapshot(orders=[other]), {})

    assert b.open_order(7) is None
    assert b.open_order(17) == other


def test_a_held_order_stands_as_an_update_of_its_terms() -> None:
    b = book()
    b.apply(make_event(om.ORDER_ACCEPTED, resting(10, volume=200)), NOTHING)

    assert b.standing_order(7, 15) == [
        OrderEvent(
            OrderEventKind.UPDATED, "7", None, 15, quantity=Decimal("2"), price=Decimal("84000.00")
        )
    ]


def test_the_held_order_is_a_copy() -> None:
    b = book()
    b.apply(make_event(om.ORDER_ACCEPTED, resting(10)), NOTHING)

    b.open_order(7).limitPrice = 1.0

    assert b.open_order(7).limitPrice == 84000.0
