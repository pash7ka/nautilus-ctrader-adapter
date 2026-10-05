"""Restarts through Nautilus's own engine: the broker's lists reconciled as one mass status.

Each test builds the records with `reconcile`, converts them with `mass_status` and hands the
result to the live execution engine, then reads what Nautilus made of it from the cache.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import replace
from decimal import Decimal

import pytest
from nautilus_trader.cache.cache import Cache
from nautilus_trader.execution.reports import ExecutionMassStatus
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.enums import ContingencyType, OrderStatus, OrderType
from nautilus_trader.model.events import OrderFilled
from nautilus_trader.model.identifiers import ClientOrderId, PositionId, TradeId, VenueOrderId
from nautilus_trader.model.objects import Money, Price

from nautilus_ctrader.common import order_record
from nautilus_ctrader.common.execution_reports import mass_status
from nautilus_ctrader.common.order_record import LegIds
from nautilus_ctrader.common.reconciliation import PositionHistory, Reconciliation, reconcile
from nautilus_ctrader.common.venue_records import Fill, ReportedOrder, money_of
from nautilus_ctrader.constants import CTRADER_VENUE
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as om
from tests.account_venue import ACCOUNT_ID
from tests.execution_replay import (
    FIRST,
    NoOperations,
    as_ours,
    history,
    make_deal,
    make_order,
    snapshot_at,
    window_deals,
)
from tests.execution_venue import (
    ENTRY,
    FIRST_EVENTS,
    STOP,
    TARGET,
    US100_ID,
    US100_SYMBOL_ID,
    Harness,
    harness,
    on_us100,
    push,
    status,
    submitted,
)
from tests.polling import wait_until

CLOSED_AT = 408.8  # FIRST closed by its stop-loss, nothing open
OPEN_AT = 358.0  # FIRST open with 0.99 after a manual partial close, both levels set
REMOVED_AT = 228.8  # FIRST open, its take-profit removed by hand
UUID_SHAPED = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")


def precision(symbol_id: int) -> int | None:
    return 2 if symbol_id == US100_SYMBOL_ID else None


def broker_lists(
    at: float, *, mine: bool = True
) -> tuple[oa.ProtoOAReconcileRes, dict[int, PositionHistory], tuple[om.ProtoOADeal, ...]]:
    """What the broker lists for the first position at timeline time `at`, on US100.cash."""
    snapshot = snapshot_at(at)
    until = snapshot.position[0].utcLastUpdateTimestamp if snapshot.position else None
    found = history(FIRST, until_ms=until)
    orders = list(found.orders)
    if mine:
        snapshot = as_ours([snapshot], [FIRST])[0]
        orders = as_ours(orders, [FIRST])
    moved = PositionHistory(tuple(on_us100(orders)), tuple(on_us100(found.deals)))
    deals = tuple(on_us100(window_deals(FIRST, until_ms=until)))
    return on_us100([snapshot])[0], {FIRST: moved}, deals


def records(snapshot, histories, deals) -> Reconciliation:
    return reconcile(snapshot, histories, deals, precision, {}, NoOperations())


def records_at(at: float, *, mine: bool = True) -> Reconciliation:
    return records(*broker_lists(at, mine=mine))


def held_price(h: Harness) -> Callable[[str], Decimal | None]:
    """The price Nautilus holds for a leg, read from the harness's cache."""

    def held(client_order_id: str) -> Decimal | None:
        order = h.cache.order(ClientOrderId(client_order_id))
        if order is None:
            return None
        if order.order_type == OrderType.STOP_MARKET:
            return order.trigger_price.as_decimal()
        if order.order_type == OrderType.LIMIT:
            return order.price.as_decimal()
        return None

    return held


def convert(
    h: Harness, reconciliation: Reconciliation
) -> tuple[ExecutionMassStatus, tuple[ReportedOrder, ...]]:
    return mass_status(
        h.client.id,
        h.client.account_id,
        CTRADER_VENUE,
        reconciliation,
        lambda symbol_id: h.cache.instrument(US100_ID) if symbol_id == US100_SYMBOL_ID else None,
        USD,
        ts_init=0,
        held_price=held_price(h),
    )


def into_nautilus(h: Harness, reconciliation: Reconciliation) -> tuple[ReportedOrder, ...]:
    """Reconcile `reconciliation` in the harness's engine; returns what was left out."""
    built, left_out = convert(h, reconciliation)
    h.engine.reconcile_execution_mass_status(built)
    return left_out


def records_with_legs(at: float, legs: LegIds) -> Reconciliation:
    """The node's records at `at`, its entry's record naming only `legs`."""
    snapshot, histories, deals = broker_lists(at)
    orders = list(histories[FIRST].orders)
    for order in orders:
        if order.orderId == 6000001:
            order.tradeData.comment = order_record.encode_comment(legs)
    found = PositionHistory(tuple(orders), histories[FIRST].deals)
    return records(snapshot, {FIRST: found}, deals)


def kinds(h: Harness, client_order_id: str) -> list[str]:
    """Every event of the order as the cache holds it, the engine's own included."""
    return [type(e).__name__ for e in h.cache.order(ClientOrderId(client_order_id)).events]


def order_at(h: Harness, venue_order_id: str):
    return h.cache.order(h.cache.client_order_id(VenueOrderId(venue_order_id)))


async def legs_accepted() -> Cache:
    """The cache of a node that sent the bracket and saw both legs accepted, then stopped."""
    async with harness() as h:
        await submitted(h)
        await push(h, *FIRST_EVENTS[:3])
        await wait_until(lambda: status(h, TARGET) == OrderStatus.ACCEPTED)
        return h.cache


async def position_closed() -> Cache:
    """The cache of a node that saw the whole first position, then stopped."""
    async with harness() as h:
        await submitted(h)
        await push(h, *FIRST_EVENTS)
        await wait_until(lambda: status(h, STOP) == OrderStatus.FILLED)
        return h.cache


async def test_restart_without_cache_rebuilds_under_node_ids() -> None:
    async with harness() as h:
        assert into_nautilus(h, records_at(OPEN_AT)) == ()

        position = h.cache.position(PositionId("5000001"))
        assert position.opening_order_id == ClientOrderId(ENTRY)
        assert position.is_open
        assert str(position.quantity) == "0.99"
        assert status(h, ENTRY) == OrderStatus.FILLED
        for leg in (STOP, TARGET):
            order = h.cache.order(ClientOrderId(leg))
            assert order.status == OrderStatus.ACCEPTED
            assert order.parent_order_id == ClientOrderId(ENTRY)
        # Only the trader's manual close, not the node's, gets an id Nautilus made up.
        generated = [o for o in h.cache.orders() if UUID_SHAPED.match(o.client_order_id.value)]
        assert [o.venue_order_id for o in generated] == [VenueOrderId("6000003")]


async def test_closed_position_ends_closed_not_reversed() -> None:
    async with harness() as h:
        into_nautilus(h, records_at(CLOSED_AT))

        assert h.cache.position(PositionId("5000001")).is_closed
        assert h.cache.positions_open() == []
        assert status(h, STOP) == OrderStatus.FILLED
        assert h.cache.order(ClientOrderId(STOP)).trade_ids == [TradeId("7000003")]
        assert status(h, TARGET) == OrderStatus.CANCELED


async def test_restart_with_cache_cancels_a_removed_leg() -> None:
    cache = await legs_accepted()

    async with harness(cache=cache) as h:
        into_nautilus(h, records_at(REMOVED_AT))

        assert status(h, TARGET) == OrderStatus.CANCELED
        assert status(h, STOP) == OrderStatus.ACCEPTED
        # Cancelled at the price Nautilus held: no update without a price before it.
        assert kinds(h, TARGET) == [
            "OrderInitialized",
            "OrderSubmitted",
            "OrderAccepted",
            "OrderCanceled",
        ]


async def test_a_priceless_leg_still_in_flight_is_accepted_then_cancelled() -> None:
    async with harness() as h:
        await submitted(h)

        assert into_nautilus(h, records_at(REMOVED_AT)) == ()

        assert kinds(h, TARGET) == [
            "OrderInitialized",
            "OrderSubmitted",
            "OrderAccepted",
            "OrderCanceled",
        ]
        assert status(h, STOP) == OrderStatus.ACCEPTED


async def test_a_priceless_leg_nautilus_does_not_hold_is_left_out() -> None:
    reconciliation = records_at(REMOVED_AT)
    (removed,) = (r for r in reconciliation.orders if r.client_order_id == TARGET)
    assert removed.price is None

    async with harness() as h:
        built, left_out = convert(h, reconciliation)
        assert left_out == (removed,)
        reports = built.order_reports
        assert reports[VenueOrderId("6000001")].linked_order_ids == [ClientOrderId(STOP)]
        assert reports[VenueOrderId("6000001-SL")].linked_order_ids is None

        h.engine.reconcile_execution_mass_status(built)

        assert h.cache.order(ClientOrderId(TARGET)) is None
        assert status(h, STOP) == OrderStatus.ACCEPTED
        assert h.cache.position(PositionId("5000001")).is_open
        linked = {i for order in h.cache.orders() for i in order.linked_order_ids or []}
        assert linked == {ClientOrderId(STOP)}
        assert all(h.cache.order(i) is not None for i in linked)


async def test_a_priceless_leg_with_a_fill_reaches_nautilus_at_its_fill_price() -> None:
    # Hand-built: the recording holds no leg partly filled before its level was removed.
    reconciliation = records_at(REMOVED_AT)
    (removed,) = (r for r in reconciliation.orders if r.client_order_id == TARGET)
    fills = (
        Fill(
            "7000010",
            "5000001",
            "SELL",
            Decimal("0.10"),
            Decimal("85353.41"),
            Decimal("-0.30"),
            removed.ts_ms - 2,
        ),
        Fill(
            "7000011",
            "5000001",
            "SELL",
            Decimal("0.30"),
            Decimal("85353.42"),
            Decimal("-0.93"),
            removed.ts_ms - 1,
        ),
    )
    partly = replace(
        removed, filled_units=Decimal("0.40"), avg_price=Decimal("85353.42"), fills=fills
    )
    orders = tuple(partly if r is removed else r for r in reconciliation.orders)
    # Without a position report: the broker's position would not match these fills.
    hand_built = Reconciliation(orders, (), ())

    async with harness() as h:
        assert into_nautilus(h, hand_built) == ()

        target = h.cache.order(ClientOrderId(TARGET))
        assert target.status == OrderStatus.CANCELED
        assert target.price == Price.from_str("85353.42")
        assert str(target.filled_qty) == "0.40"
        filled = [e for e in target.events if isinstance(e, OrderFilled)]
        assert [(e.trade_id, e.commission) for e in filled] == [
            (TradeId("7000010"), Money(Decimal("0.30"), USD)),
            (TradeId("7000011"), Money(Decimal("0.93"), USD)),
        ]


async def test_restart_with_cache_fills_a_triggered_leg() -> None:
    cache = await legs_accepted()
    (deal,) = (d for d in history(FIRST).deals if d.dealId == 7_000_003)

    async with harness(cache=cache) as h:
        into_nautilus(h, records_at(CLOSED_AT))

        assert status(h, STOP) == OrderStatus.FILLED
        (filled,) = [
            e for e in h.cache.order(ClientOrderId(STOP)).events if isinstance(e, OrderFilled)
        ]
        assert filled.trade_id == TradeId("7000003")
        assert filled.commission == Money(-money_of(deal.commission, deal.moneyDigits), USD)
        assert status(h, TARGET) == OrderStatus.CANCELED
        assert h.cache.position(PositionId("5000001")).is_closed


async def test_restart_with_cache_changes_nothing_known() -> None:
    cache = await position_closed()
    before = {o.client_order_id: len(o.events) for o in cache.orders()}

    async with harness(cache=cache) as h:
        into_nautilus(h, records_at(CLOSED_AT))

        assert {o.client_order_id: len(o.events) for o in h.cache.orders()} == before
        assert len(h.cache.positions()) == 1


async def test_a_one_leg_bracket_is_taken_by_nautilus() -> None:
    async with harness() as h:
        into_nautilus(h, records_with_legs(OPEN_AT, LegIds(STOP, None)))

        stop = h.cache.order(ClientOrderId(STOP))
        assert stop.status == OrderStatus.ACCEPTED
        assert stop.parent_order_id == ClientOrderId(ENTRY)
        assert stop.contingency_type == ContingencyType.NO_CONTINGENCY
        assert h.cache.order(ClientOrderId(ENTRY)).linked_order_ids == [ClientOrderId(STOP)]
        assert h.cache.order(ClientOrderId(TARGET)) is None


def stop_out() -> tuple[Reconciliation, str, str]:
    # Hand-built: the recording holds no stop-out.
    pid = 5_000_201
    opened = make_order(6_000_201, pid, utc=1_600_000_001_000, symbol=US100_SYMBOL_ID)
    closed = make_order(
        6_000_202,
        pid,
        side=om.SELL,
        closing=True,
        utc=1_600_000_002_000,
        stop_out=True,
        symbol=US100_SYMBOL_ID,
    )
    for order in (opened, closed):
        order.orderStatus = om.ORDER_STATUS_FILLED
    deals = tuple(
        on_us100(
            [
                make_deal(
                    7_000_201,
                    6_000_201,
                    pid,
                    side=om.BUY,
                    volume=100,
                    price=85000.0,
                    ts=1_600_000_001_000,
                ),
                make_deal(
                    7_000_202,
                    6_000_202,
                    pid,
                    side=om.SELL,
                    volume=100,
                    price=84000.0,
                    ts=1_600_000_002_000,
                ),
            ]
        )
    )
    snapshot = oa.ProtoOAReconcileRes(ctidTraderAccountId=ACCOUNT_ID)
    found = PositionHistory((opened, closed), deals)
    return records(snapshot, {pid: found}, deals), "6000202", str(pid)


CLOSES = {
    "manual close": lambda: (records_at(CLOSED_AT), "6000003", "5000001"),
    # The node's own position with no stop leg, closed by its stop-loss.
    "no-leg level trigger": lambda: (
        records_with_legs(CLOSED_AT, LegIds(None, TARGET)),
        "6000002",
        "5000001",
    ),
    "stop-out": stop_out,
    "foreign close": lambda: (records_at(CLOSED_AT, mine=False), "6000003", "5000001"),
}


@pytest.mark.parametrize("case", list(CLOSES))
async def test_every_close_from_a_report_is_reduce_only(case: str) -> None:
    reconciliation, venue_order_id, position_id = CLOSES[case]()

    async with harness() as h:
        into_nautilus(h, reconciliation)

        order = order_at(h, venue_order_id)
        assert order.is_reduce_only
        assert order.status == OrderStatus.FILLED
        assert order.position_id == PositionId(position_id)
        assert h.cache.position(PositionId(position_id)).is_closed


async def test_same_reports_with_and_without_cache() -> None:
    cache = await legs_accepted()
    lists = broker_lists(OPEN_AT)

    async with harness() as fresh:
        without = records(*lists)
        into_nautilus(fresh, without)
        fresh_states = {o.venue_order_id: o.status for o in fresh.cache.orders()}
    async with harness(cache=cache) as restarted:
        with_cache = records(*lists)
        into_nautilus(restarted, with_cache)
        restarted_states = {o.venue_order_id: o.status for o in restarted.cache.orders()}

    assert with_cache == without
    assert restarted_states == fresh_states
