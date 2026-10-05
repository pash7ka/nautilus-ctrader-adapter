"""The recorded session through the venue model, as recorded and as if the node had traded it."""

from __future__ import annotations

from decimal import Decimal

from nautilus_ctrader.common.venue_book import PositionView, VenueBook
from nautilus_ctrader.common.venue_records import (
    Action,
    Activity,
    ActivityKind,
    AwaitProtection,
    ExternalOrder,
    ExternalType,
    Fill,
    Level,
    OrderEvent,
    OrderEventKind,
)
from tests.execution_replay import (
    FIRST,
    PENDING,
    SECOND,
    SYMBOL,
    NoOperations,
    as_ours,
    entry_id,
    events,
    first_n,
    position_orders,
    precision,
    snapshot_with_protection,
    stop_id,
    target_id,
)

NOTHING = NoOperations()
A, F, U, C = (
    OrderEventKind.ACCEPTED,
    OrderEventKind.FILLED,
    OrderEventKind.UPDATED,
    OrderEventKind.CANCELED,
)


def run(messages) -> list:
    book = VenueBook(precision)
    records = []
    for message in messages:
        records += book.apply(message, NOTHING)
    return records


def d(value: str) -> Decimal:
    return Decimal(value)


def fill(deal, position, side, units, price, commission, ts) -> Fill:
    return Fill(str(deal), str(position), side, d(units), d(price), d(commission), ts)


def sl(kind, position, entry, ts, **kw) -> OrderEvent:
    return OrderEvent(kind, f"{entry}-SL", stop_id(position), ts, **kw)


def tp(kind, position, entry, ts, **kw) -> OrderEvent:
    return OrderEvent(kind, f"{entry}-TP", target_id(position), ts, **kw)


def trader(action, ts, units="1") -> Activity:
    return Activity(ActivityKind.MANUAL_CHANGE, SYMBOL, "position", "BUY", d(units), action, ts)


FIRST_AS_OURS = [
    OrderEvent(A, "6000001", entry_id(FIRST), 1600000124634),
    OrderEvent(
        F,
        "6000001",
        entry_id(FIRST),
        1600000124772,
        fill=fill(7000001, FIRST, "BUY", "1", "85287.21", "-27.72", 1600000124772),
    ),
    AwaitProtection(FIRST),
    sl(A, FIRST, 6000001, 1600000124775, quantity=d("1"), trigger_price=d("85197.20")),
    tp(A, FIRST, 6000001, 1600000124775, quantity=d("1"), price=d("85387.22")),
]
FIRST_AFTER_PROTECTION = [
    sl(U, FIRST, 6000001, 1600000168184, quantity=d("1"), trigger_price=d("85200.20")),
    trader(Action.LEVEL_MOVED, 1600000168184),
    tp(C, FIRST, 6000001, 1600000226406),
    trader(Action.LEVEL_REMOVED, 1600000226406),
    trader(Action.LEVEL_ADDED, 1600000254312),
    ExternalOrder(
        "6000003",
        SYMBOL,
        "SELL",
        ExternalType.MARKET,
        d("0.01"),
        True,
        str(FIRST),
        1600000355411,
        time_in_force="IMMEDIATE_OR_CANCEL",
        ts_accepted_ms=1600000355411,
    ),
    OrderEvent(
        F,
        "6000003",
        None,
        1600000355547,
        fill=fill(7000002, FIRST, "SELL", "0.01", "85219.06", "-0.28", 1600000355547),
    ),
    trader(Action.PARTIALLY_CLOSED, 1600000355547, units="0.01"),
    sl(U, FIRST, 6000001, 1600000355553, quantity=d("0.99")),
    sl(U, FIRST, 6000001, 1600000405318, quantity=d("0.99"), trigger_price=d("85206.20")),
    trader(Action.LEVEL_MOVED, 1600000405318, units="0.99"),
    sl(
        F,
        FIRST,
        6000001,
        1600000406510,
        fill=fill(7000003, FIRST, "SELL", "0.99", "85205.58", "-27.41", 1600000406510),
    ),
]

SECOND_AS_OURS = [
    OrderEvent(A, "6000004", entry_id(SECOND), 1600000442831),
    OrderEvent(
        F,
        "6000004",
        entry_id(SECOND),
        1600000442966,
        fill=fill(7000004, SECOND, "BUY", "1", "85209.10", "-27.69", 1600000442966),
    ),
    AwaitProtection(SECOND),
    sl(A, SECOND, 6000004, 1600000442969, quantity=d("1"), trigger_price=d("85119.09")),
    tp(A, SECOND, 6000004, 1600000442969, quantity=d("1"), price=d("85309.11")),
    sl(U, SECOND, 6000004, 1600000480280, quantity=d("1"), trigger_price=d("85089.09")),
    trader(Action.LEVEL_MOVED, 1600000480280),
    sl(U, SECOND, 6000004, 1600000486155, quantity=d("1"), trigger_price=d("85031.14")),
    trader(Action.LEVEL_MOVED, 1600000486155),
    tp(U, SECOND, 6000004, 1600000502124, quantity=d("1"), price=d("85224.52")),
    trader(Action.LEVEL_MOVED, 1600000502124),
    tp(U, SECOND, 6000004, 1600000516910, quantity=d("1"), price=d("85220.52")),
    trader(Action.LEVEL_MOVED, 1600000516910),
    tp(U, SECOND, 6000004, 1600000620673, quantity=d("1"), price=d("85168.40")),
    trader(Action.LEVEL_MOVED, 1600000620673),
    tp(U, SECOND, 6000004, 1600000622601, quantity=d("1"), price=d("85206.52")),
    trader(Action.LEVEL_MOVED, 1600000622601),
    tp(U, SECOND, 6000004, 1600000657476, quantity=d("1"), price=d("85179.30")),
    trader(Action.LEVEL_MOVED, 1600000657476),
    tp(
        F,
        SECOND,
        6000004,
        1600000658056,
        fill=fill(7000005, SECOND, "SELL", "1", "85187.89", "-27.69", 1600000658056),
    ),
    sl(C, SECOND, 6000004, 1600000658056),
]

PENDING_AS_RECORDED = [
    ExternalOrder(
        "6000006",
        SYMBOL,
        "BUY",
        ExternalType.LIMIT,
        d("1"),
        False,
        str(PENDING),
        1600000694986,
        price=d("85100.00"),
        time_in_force="GOOD_TILL_CANCEL",
        ts_accepted_ms=1600000694986,
    ),
    OrderEvent(C, "6000006", None, 1600000760269),
]


def test_the_first_position_as_the_nodes() -> None:
    records = run(first_n(as_ours(events(), [FIRST]), FIRST))

    assert records == FIRST_AS_OURS + FIRST_AFTER_PROTECTION
    # `Decimal` equality ignores the exponent; the text Nautilus receives does not.
    assert str(records[1].fill.price) == "85287.21"
    assert str(records[3].trigger_price) == "85197.20"


def test_the_second_position_as_the_nodes_take_profit_moved_through_the_market() -> None:
    assert run(first_n(as_ours(events(), [SECOND]), SECOND)) == SECOND_AS_OURS


def test_the_first_position_as_recorded_is_foreign() -> None:
    assert run(first_n(events(), FIRST)) == [
        ExternalOrder(
            "6000001",
            SYMBOL,
            "BUY",
            ExternalType.MARKET,
            d("1"),
            False,
            str(FIRST),
            1600000124634,
            time_in_force="IMMEDIATE_OR_CANCEL",
            ts_accepted_ms=1600000124634,
        ),
        OrderEvent(
            F,
            "6000001",
            None,
            1600000124772,
            fill=fill(7000001, FIRST, "BUY", "1", "85287.21", "-27.72", 1600000124772),
        ),
        ExternalOrder(
            "6000003",
            SYMBOL,
            "SELL",
            ExternalType.MARKET,
            d("0.01"),
            True,
            str(FIRST),
            1600000355411,
            time_in_force="IMMEDIATE_OR_CANCEL",
            ts_accepted_ms=1600000355411,
        ),
        OrderEvent(
            F,
            "6000003",
            None,
            1600000355547,
            fill=fill(7000002, FIRST, "SELL", "0.01", "85219.06", "-0.28", 1600000355547),
        ),
        ExternalOrder(
            "6000002",
            SYMBOL,
            "SELL",
            ExternalType.STOP_MARKET,
            d("0.99"),
            True,
            str(FIRST),
            1600000406510,
            trigger_price=d("85206.20"),
            fills=(fill(7000003, FIRST, "SELL", "0.99", "85205.58", "-27.41", 1600000406510),),
            time_in_force="GOOD_TILL_CANCEL",
            ts_accepted_ms=1600000124775,
        ),
    ]


def test_the_second_positions_take_profit_as_recorded_is_a_foreign_limit_close() -> None:
    assert run(first_n(events(), SECOND)) == [
        ExternalOrder(
            "6000004",
            SYMBOL,
            "BUY",
            ExternalType.MARKET,
            d("1"),
            False,
            str(SECOND),
            1600000442831,
            time_in_force="IMMEDIATE_OR_CANCEL",
            ts_accepted_ms=1600000442831,
        ),
        OrderEvent(
            F,
            "6000004",
            None,
            1600000442966,
            fill=fill(7000004, SECOND, "BUY", "1", "85209.10", "-27.69", 1600000442966),
        ),
        ExternalOrder(
            "6000005",
            SYMBOL,
            "SELL",
            ExternalType.LIMIT,
            d("1"),
            True,
            str(SECOND),
            1600000658056,
            price=d("85179.30"),
            fills=(fill(7000005, SECOND, "SELL", "1", "85187.89", "-27.69", 1600000658056),),
            time_in_force="GOOD_TILL_CANCEL",
            ts_accepted_ms=1600000442969,
        ),
    ]


def test_the_pending_order_as_recorded() -> None:
    assert run(first_n(events(), PENDING)) == PENDING_AS_RECORDED


def test_every_event_twice_changes_nothing_the_second_time() -> None:
    once = run(as_ours(events(), [FIRST, SECOND]))
    book = VenueBook(precision)
    twice = []
    for message in as_ours(events(), [FIRST, SECOND]):
        twice += book.apply(message, NOTHING)
        twice += book.apply(message, NOTHING)

    assert twice == once


def test_a_model_loaded_mid_session_continues_like_one_that_saw_it_all() -> None:
    book = VenueBook(precision)
    (snapshot,) = as_ours([snapshot_with_protection(127.187)], [FIRST])
    orders = {
        position_id: as_ours(listed, [FIRST]) for position_id, listed in position_orders().items()
    }

    assert book.load(snapshot, orders) == []
    assert book.view(FIRST) == PositionView(
        position_id=FIRST,
        symbol_id=SYMBOL,
        side="BUY",
        units=d("1"),
        open=True,
        ours=True,
        entry_order_id=6000001,
        entry_client_order_id=entry_id(FIRST),
        protective_order_id=6000002,
        levels={Level.STOP_LOSS: d("85197.20"), Level.TAKE_PROFIT: d("85387.22")},
        legs={Level.STOP_LOSS: (stop_id(FIRST), True), Level.TAKE_PROFIT: (target_id(FIRST), True)},
        leg_units={Level.STOP_LOSS: d("1"), Level.TAKE_PROFIT: d("1")},
    )

    later = [
        m
        for m in first_n(as_ours(events(), [FIRST]), FIRST)
        if m.order.utcLastUpdateTimestamp > 1600000124775
    ]
    records = []
    for message in later:
        records += book.apply(message, NOTHING)
    assert records == FIRST_AFTER_PROTECTION
