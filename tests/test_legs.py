"""Tests for `leg_position_id`: a leg's position, read from the cache by the id of its entry."""

from __future__ import annotations

from collections.abc import Callable

import pytest
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import TestClock
from nautilus_trader.common.factories import OrderFactory
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.identifiers import (
    AccountId,
    PositionId,
    StrategyId,
    TraderId,
    VenueOrderId,
)
from nautilus_trader.model.objects import Price, Quantity
from nautilus_trader.model.orders import Order
from nautilus_trader.test_kit.providers import TestInstrumentProvider
from nautilus_trader.test_kit.stubs.events import TestEventStubs

import nautilus_ctrader
from nautilus_ctrader import CTRADER_VENUE, leg_position_id

ACCOUNT_ID = AccountId("CTRADER-9000001")
EURUSD = TestInstrumentProvider.default_fx_ccy("EURUSD", venue=CTRADER_VENUE)
ENTRY_VENUE_ID = 6_000_001
POSITION = PositionId("5000001")
QUANTITY = Quantity.from_int(1000)


class Book:
    """A real cache and a factory, with orders walked to the states a report or a fill gives."""

    def __init__(self) -> None:
        self.cache = Cache()
        self.cache.add_instrument(EURUSD)
        self.factory = OrderFactory(
            trader_id=TraderId("TESTER-001"),
            strategy_id=StrategyId("EXTERNAL"),
            clock=TestClock(),
        )

    def accept(self, order: Order, venue_order_id: str | None) -> Order:
        """Cache `order`, submitted and accepted under `venue_order_id` (none: left submitted)."""
        self.cache.add_order(order)
        order.apply(TestEventStubs.order_submitted(order, account_id=ACCOUNT_ID))
        if venue_order_id is not None:
            event = TestEventStubs.order_accepted(
                order, account_id=ACCOUNT_ID, venue_order_id=VenueOrderId(venue_order_id)
            )
            order.apply(event)
        self.cache.update_order(order)
        return order

    def fill(self, order: Order, position_id: PositionId | None) -> Order:
        order.apply(
            TestEventStubs.order_filled(
                order,
                EURUSD,
                account_id=ACCOUNT_ID,
                position_id=position_id,
                last_px=Price.from_str("1.10000"),
            )
        )
        self.cache.update_order(order)
        return order

    def entry(self, *, position_id: PositionId | None = POSITION) -> Order:
        """A filled market entry, known to the broker as `ENTRY_VENUE_ID`."""
        order = self.factory.market(EURUSD.id, OrderSide.BUY, QUANTITY)
        self.accept(order, str(ENTRY_VENUE_ID))
        return self.fill(order, position_id) if position_id is not None else order

    def stop(self, venue_order_id: str | None) -> Order:
        order = self.factory.stop_market(
            EURUSD.id,
            OrderSide.SELL,
            QUANTITY,
            Price.from_str("1.09000"),
            reduce_only=True,
        )
        return self.accept(order, venue_order_id)

    def take_profit(self, venue_order_id: str | None) -> Order:
        order = self.factory.limit(
            EURUSD.id, OrderSide.SELL, QUANTITY, Price.from_str("1.11000"), reduce_only=True
        )
        return self.accept(order, venue_order_id)


@pytest.fixture
def book() -> Book:
    return Book()


def test_a_filled_leg_answers_with_its_own_position_id(book: Book) -> None:
    leg = book.fill(book.stop(f"{ENTRY_VENUE_ID}-SL"), POSITION)

    assert leg.position_id == POSITION
    assert leg_position_id(book.cache, leg) == POSITION


def test_a_legs_own_position_id_wins_over_its_entrys(book: Book) -> None:
    book.entry(position_id=PositionId("5000009"))
    leg = book.fill(book.stop(f"{ENTRY_VENUE_ID}-SL"), POSITION)

    assert leg_position_id(book.cache, leg) == POSITION


def test_an_open_foreign_stop_takes_its_entrys_position(book: Book) -> None:
    book.entry()
    leg = book.stop(f"{ENTRY_VENUE_ID}-SL")

    assert leg.position_id is None
    assert leg_position_id(book.cache, leg) == POSITION


def test_a_take_profit_leg_takes_its_entrys_position(book: Book) -> None:
    book.entry()
    leg = book.take_profit(f"{ENTRY_VENUE_ID}-TP")

    assert leg_position_id(book.cache, leg) == POSITION


@pytest.mark.parametrize("suffix", ["SL-2", "SL-3", "TP-2", "TP-12"])
def test_a_later_generation_takes_its_entrys_position(book: Book, suffix: str) -> None:
    book.entry()
    make = book.stop if suffix.startswith("SL") else book.take_profit
    leg = make(f"{ENTRY_VENUE_ID}-{suffix}")

    assert leg_position_id(book.cache, leg) == POSITION


def test_an_entry_missing_from_the_cache_gives_none(book: Book) -> None:
    leg = book.stop(f"{ENTRY_VENUE_ID}-SL")

    assert leg_position_id(book.cache, leg) is None


def test_an_entry_under_another_venue_id_gives_none(book: Book) -> None:
    book.entry()
    leg = book.stop(f"{ENTRY_VENUE_ID + 1}-SL")

    assert leg_position_id(book.cache, leg) is None


def test_an_entry_without_a_position_id_gives_none(book: Book) -> None:
    book.entry(position_id=None)
    leg = book.stop(f"{ENTRY_VENUE_ID}-SL")

    assert leg_position_id(book.cache, leg) is None


@pytest.mark.parametrize(
    "make",
    [
        pytest.param(
            lambda b: b.accept(b.factory.market(EURUSD.id, OrderSide.BUY, QUANTITY), "77"),
            id="market",
        ),
        pytest.param(
            lambda b: b.accept(
                b.factory.limit(EURUSD.id, OrderSide.BUY, QUANTITY, Price.from_str("1.09000")),
                str(ENTRY_VENUE_ID),
            ),
            id="pending-limit",
        ),
        pytest.param(lambda b: b.stop("not-a-leg-SL"), id="unparsable-id"),
        pytest.param(lambda b: b.stop(f"{ENTRY_VENUE_ID}-SL-1"), id="generation-one-spelled-out"),
    ],
)
def test_an_ordinary_order_gives_none(book: Book, make: Callable[[Book], Order]) -> None:
    book.entry()
    order = make(book)

    assert leg_position_id(book.cache, order) is None


def test_an_order_without_a_venue_order_id_gives_none(book: Book) -> None:
    book.entry()
    order = book.stop(None)

    assert order.venue_order_id is None
    assert leg_position_id(book.cache, order) is None


def test_the_nodes_own_leg_takes_its_entrys_position(book: Book) -> None:
    bracket = book.factory.bracket(
        EURUSD.id,
        OrderSide.BUY,
        QUANTITY,
        sl_trigger_price=Price.from_str("1.09000"),
        tp_price=Price.from_str("1.11000"),
    )
    entry, stop_loss, take_profit = bracket.orders
    book.accept(entry, str(ENTRY_VENUE_ID))
    book.fill(entry, POSITION)
    book.accept(stop_loss, f"{ENTRY_VENUE_ID}-SL")
    book.accept(take_profit, f"{ENTRY_VENUE_ID}-TP")

    assert leg_position_id(book.cache, stop_loss) == POSITION
    assert leg_position_id(book.cache, take_profit) == POSITION


def test_the_name_is_exported_from_the_package_root() -> None:
    assert "leg_position_id" in nautilus_ctrader.__all__
    assert nautilus_ctrader.leg_position_id is leg_position_id
