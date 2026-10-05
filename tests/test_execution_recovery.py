"""Restarts through Nautilus's own engine: the broker's lists reconciled as one mass status.

The first tests build the records with `reconcile`, convert them with `mass_status` and hand
the result to the live execution engine, then read what Nautilus made of it from the cache. The
later ones run the client's own reconciliation pass against the fake venue's lists.
"""

from __future__ import annotations

import asyncio
import json
import re
import time
from collections.abc import Callable
from dataclasses import replace
from decimal import Decimal

import pytest
from nautilus_trader.cache.cache import Cache
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.messages import (
    GenerateOrderStatusReports,
    ModifyOrder,
    SubmitOrder,
)
from nautilus_trader.execution.reports import ExecutionMassStatus
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.enums import ContingencyType, OrderSide, OrderStatus, OrderType
from nautilus_trader.model.events import OrderFilled
from nautilus_trader.model.identifiers import ClientOrderId, PositionId, TradeId, VenueOrderId
from nautilus_trader.model.objects import Money, Price, Quantity

from nautilus_ctrader.common import order_record
from nautilus_ctrader.common.execution_reports import mass_status
from nautilus_ctrader.common.order_record import LegIds
from nautilus_ctrader.common.reconciliation import PositionHistory, Reconciliation, reconcile
from nautilus_ctrader.common.venue_records import Fill, Level, ReportedOrder, money_of
from nautilus_ctrader.constants import CTRADER_VENUE, UNLOADED_EXPOSURE_KEY
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as om
from tests.account_venue import ACCOUNT_ID, HeldReplies
from tests.execution_replay import (
    FIRST,
    NoOperations,
    as_ours,
    history,
    make_deal,
    make_event,
    make_order,
    make_position,
    snapshot_at,
    window_deals,
)
from tests.execution_venue import (
    ENTRY,
    FIRST_EVENTS,
    STOP,
    STRATEGY_ID,
    TARGET,
    TRADER_ID,
    US100_ID,
    US100_SYMBOL_ID,
    ExecutionVenue,
    Harness,
    exec_config,
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


# -- The client's reconciliation pass --------------------------------------------------------

EURUSD_SYMBOL_ID = 1  # not loaded by the harness
MINUTE_MS = 60_000


def serve(venue: ExecutionVenue, at: float) -> None:
    """Make the venue's snapshot and lists the node's first position at timeline time `at`."""
    snapshot, histories, deals = broker_lists(at)
    venue.snapshot = snapshot
    venue.position_orders = {pid: list(found.orders) for pid, found in histories.items()}
    venue.position_deals = {pid: list(found.deals) for pid, found in histories.items()}
    venue.deals = list(deals)


def serving(at: float = OPEN_AT) -> ExecutionVenue:
    """A venue whose snapshot and lists are the node's first position at timeline time `at`."""
    venue = ExecutionVenue()
    serve(venue, at)
    return venue


def unloaded_position(position_id: int) -> om.ProtoOAPosition:
    return make_position(position_id, symbol=EURUSD_SYMBOL_ID)


def exposure(h: Harness) -> list[dict]:
    return json.loads(h.cache.get(UNLOADED_EXPOSURE_KEY))


def foreign_position(venue: ExecutionVenue, n: int, *, ts: int) -> None:
    """An open position the node did not open, its entry filled at `ts`, on `US100.cash`."""
    position_id, order_id = 5_100_000 + n, 6_100_000 + n
    position = make_position(position_id, symbol=US100_SYMBOL_ID)
    position.price = 85000.0
    position.utcLastUpdateTimestamp = ts
    venue.snapshot.position.append(position)
    entry = make_order(order_id, position_id, utc=ts, symbol=US100_SYMBOL_ID)
    entry.orderStatus = om.ORDER_STATUS_FILLED
    venue.position_orders[position_id] = [entry]
    deal = make_deal(
        7_100_000 + n, order_id, position_id, side=om.BUY, volume=100, price=85000.0, ts=ts
    )
    venue.position_deals[position_id] = on_us100([deal])


def closed_position(
    venue: ExecutionVenue, n: int, *, opened: int, closed: int
) -> list[om.ProtoOADeal]:
    """A foreign position opened at `opened` and closed at `closed`; returns its two deals."""
    position_id, entry_id, close_id = 5_200_000 + n, 6_200_000 + 2 * n, 6_200_001 + 2 * n
    entry = make_order(entry_id, position_id, utc=opened, symbol=US100_SYMBOL_ID)
    close = make_order(
        close_id, position_id, side=om.SELL, closing=True, utc=closed, symbol=US100_SYMBOL_ID
    )
    for order in (entry, close):
        order.orderStatus = om.ORDER_STATUS_FILLED
    deals = on_us100(
        [
            make_deal(
                7_200_000 + 2 * n,
                entry_id,
                position_id,
                side=om.BUY,
                volume=100,
                price=85000.0,
                ts=opened,
            ),
            make_deal(
                7_200_001 + 2 * n,
                close_id,
                position_id,
                side=om.SELL,
                volume=100,
                price=85100.0,
                ts=closed,
            ),
        ]
    )
    venue.position_orders[position_id] = [entry, close]
    venue.position_deals[position_id] = deals
    venue.deals += deals
    return deals


async def test_generate_mass_status_reports_the_recorded_state() -> None:
    async with harness(execution_venue=serving()) as h:
        built = await h.client.generate_mass_status()

        ids = {report.client_order_id for report in built.order_reports.values()}
        assert {ClientOrderId(ENTRY), ClientOrderId(STOP), ClientOrderId(TARGET)} <= ids
        assert len(built.position_reports[US100_ID]) == 1
        view = h.client._book.view(FIRST)
        assert view.legs == {Level.STOP_LOSS: (STOP, True), Level.TAKE_PROFIT: (TARGET, True)}


async def test_an_event_during_the_start_pass_is_applied_after_reconciliation() -> None:
    venue = serving()
    async with harness(execution_venue=venue) as h:
        held = HeldReplies(
            h.server,
            om.PROTO_OA_DEAL_LIST_BY_POSITION_ID_REQ,
            venue.replies[om.PROTO_OA_DEAL_LIST_BY_POSITION_ID_REQ],
        )
        passing = asyncio.create_task(h.client.generate_mass_status())
        await asyncio.wait_for(held.arrived.wait(), timeout=10)
        # The stop-loss moved by hand, then the protective order filled at it.
        await push(h, *FIRST_EVENTS[-2:])
        await held.stop_holding()
        built = await asyncio.wait_for(passing, timeout=10)

        h.engine.reconcile_execution_mass_status(built)

        await wait_until(lambda: status(h, STOP) == OrderStatus.FILLED)
        assert kinds(h, STOP)[-1] == "OrderFilled"
        assert not any("No Nautilus order" in line for line in h.logger.warnings())


async def test_buffer_is_released_when_reconciliation_never_comes() -> None:
    async with harness(execution_venue=serving()) as h:
        h.client._reconciled_wait_secs = 0.2
        assert await h.client.generate_mass_status() is not None

        await push(h, *FIRST_EVENTS[-2:])

        await wait_until(lambda: not h.client._book.view(FIRST).open)
        assert any("0.2s" in line for line in h.logger.warnings())


async def test_a_failed_list_releases_the_buffer() -> None:
    venue = serving()
    venue.fail = {om.PROTO_OA_DEAL_LIST_REQ}
    async with harness(execution_venue=venue) as h:
        assert await h.client.generate_mass_status() is None

        await push(h, *FIRST_EVENTS[-2:])

        assert not h.client._book.view(FIRST).open
        assert h.logger.errors() == []
        assert any("INTERNAL_SERVER_ERROR" in line for line in h.logger.warnings())


async def test_the_pass_asks_each_position_once(monkeypatch: pytest.MonkeyPatch) -> None:
    # The suite need not wait out the venue's historical rate for 50 lists.
    monkeypatch.setattr("nautilus_ctrader.common.session.HISTORICAL_RATE_LIMIT_PER_SEC", 1000.0)
    venue = ExecutionVenue()
    for n in range(25):
        foreign_position(venue, n, ts=1_600_000_000_000 + n)
    async with harness(execution_venue=venue) as h:
        orders_before = len(h.received(oa.ProtoOAOrderListByPositionIdReq))
        deals_before = len(h.received(oa.ProtoOADealListByPositionIdReq))

        built = await h.client.generate_mass_status()

        assert len(h.received(oa.ProtoOAOrderListByPositionIdReq)) - orders_before == 25
        assert len(h.received(oa.ProtoOADealListByPositionIdReq)) - deals_before == 25
        assert len(built.position_reports[US100_ID]) == 25
        # The snapshot, the window's deal list, and two lists per position.
        assert any(level == "info" and "52 requests" in line for level, line in h.logger.lines)


async def test_deal_pages_are_followed() -> None:
    now = int(time.time() * 1000)
    venue = ExecutionVenue()
    venue.page_size = 2
    in_window = closed_position(venue, 1, opened=now - 50_000, closed=now - 40_000)
    in_window += closed_position(venue, 2, opened=now - 30_000, closed=now - 20_000)
    # Opened before the window, closed in it.
    before = now - 2 * 1440 * MINUTE_MS
    in_window += closed_position(venue, 3, opened=before, closed=now - 10_000)[1:]
    assert len(in_window) == 5
    async with harness(execution_venue=venue) as h:
        built = await h.client.generate_mass_status()

        trade_ids = [f.trade_id.value for fills in built.fill_reports.values() for f in fills]
        assert len(trade_ids) == len(set(trade_ids))
        assert {str(deal.dealId) for deal in in_window} <= set(trade_ids)
        assert len(h.received(oa.ProtoOADealListReq)) > 1


async def test_window_uses_the_default_lookback() -> None:
    async with harness() as h:
        await h.client.generate_mass_status(lookback_mins=None)

        (asked,) = h.received(oa.ProtoOADealListReq)
        expected = int(time.time() * 1000) - 1440 * MINUTE_MS
        assert abs(asked.fromTimestamp - expected) < 5_000


async def test_exposure_written_at_connect_and_on_change() -> None:
    async with harness() as h:
        assert h.cache.get(UNLOADED_EXPOSURE_KEY) == b"[]"

        order = make_order(6_900_002, 5_900_002, symbol=EURUSD_SYMBOL_ID)
        order.orderStatus = om.ORDER_STATUS_FILLED
        deal = make_deal(
            7_900_002, 6_900_002, 5_900_002, side=om.BUY, volume=100, price=1.1, ts=1_000
        )
        deal.symbolId = EURUSD_SYMBOL_ID
        await push(
            h,
            make_event(om.ORDER_FILLED, order, position=unloaded_position(5_900_002), deal=deal),
        )

        await wait_until(lambda: len(exposure(h)) == 1)
        (item,) = exposure(h)
        assert (item["symbol"], item["subject"], item["side"]) == ("EURUSD", "position", "BUY")

    async with harness(connect=False) as h:
        h.cache.add(UNLOADED_EXPOSURE_KEY, b"stale")

        await h.client._connect()

        assert h.cache.get(UNLOADED_EXPOSURE_KEY) == b"[]"


async def test_a_pass_publishes_no_unloaded_activity() -> None:
    venue = ExecutionVenue()
    venue.snapshot.position.append(unloaded_position(5_900_003))
    async with harness(execution_venue=venue) as h:
        await h.client.generate_mass_status()

        assert h.activity == []
        assert [item["subject"] for item in exposure(h)] == ["position"]


async def test_the_key_is_fresh_when_the_reconciliation_topic_fires() -> None:
    async with harness() as h:
        seen: list[bytes] = []
        h.client._msgbus.subscribe(
            topic=f"reports.execution.{CTRADER_VENUE}",
            handler=lambda _status: seen.append(h.cache.get(UNLOADED_EXPOSURE_KEY)),
        )
        h.venue.snapshot.position.append(unloaded_position(5_900_004))

        built = await h.client.generate_mass_status()
        h.engine.reconcile_execution_mass_status(built)

        assert seen
        assert [item["subject"] for item in json.loads(seen[0])] == ["position"]


async def test_reports_outside_a_mass_status_never_carry_a_filled_order() -> None:
    async with harness(execution_venue=serving()) as h:
        found = await h.client.generate_order_status_reports(
            GenerateOrderStatusReports(
                instrument_id=None,
                start=None,
                end=None,
                open_only=False,
                command_id=UUID4(),
                ts_init=0,
            ),
        )

        assert all(report.order_status != OrderStatus.FILLED for report in found)
        assert {report.client_order_id for report in found} == {
            ClientOrderId(STOP),
            ClientOrderId(TARGET),
        }


# -- Reconnect ---------------------------------------------------------------------------------

TP_BACK_AT = 256.5  # FIRST open, its take-profit set again by hand
OURS = 5_300_001
CLOSE = "O-C-5300001"


async def started(h: Harness) -> ExecutionMassStatus:
    """The start's reconciliation, reconciled by the engine as a node would."""
    built = await h.client.generate_mass_status()
    h.engine.reconcile_execution_mass_status(built)
    return built


async def test_reconnect_sends_a_mass_status_not_events() -> None:
    venue = serving(TP_BACK_AT)
    async with harness(execution_venue=venue) as h:
        await started(h)
        assert status(h, TARGET) == OrderStatus.ACCEPTED

        # What the broker holds once the session is back.
        serve(venue, REMOVED_AT)
        await h.server.drop_connections()
        await wait_until(lambda: len(h.mass_statuses) == 1, timeout_secs=10)

        assert status(h, TARGET) == OrderStatus.CANCELED
        # The cancel came from the engine's reconciliation, not from the client.
        assert "OrderCanceled" not in h.kinds_of(TARGET)
        assert any("Rebuilding the venue model" in line for line in h.logger.warnings())


async def test_a_reconnect_during_the_start_wait_releases_after_its_own_mass_status() -> None:
    venue = serving()
    async with harness(execution_venue=venue) as h:
        h.client._reconciled_wait_secs = 60.0
        assert await h.client.generate_mass_status() is not None
        # Nautilus has not reconciled the start's mass status yet.
        await push(h, *FIRST_EVENTS[-2:])
        held_when_reconciled: list[int] = []
        h.client._msgbus.subscribe(
            topic=f"reports.execution.{CTRADER_VENUE}",
            handler=lambda _status: held_when_reconciled.append(len(h.client._buffer or [])),
        )
        held = HeldReplies(
            h.server,
            om.PROTO_OA_ORDER_LIST_BY_POSITION_ID_REQ,
            venue.replies[om.PROTO_OA_ORDER_LIST_BY_POSITION_ID_REQ],
        )

        await h.server.drop_connections()
        await asyncio.wait_for(held.arrived.wait(), timeout=10)

        # The start's wait is over once the reconnect pass runs; its timer can release nothing.
        assert h.client._awaited_report is None
        assert h.client._release_timer is None
        assert len(h.client._buffer) == 2
        await held.stop_holding()
        await wait_until(lambda: len(h.mass_statuses) == 1, timeout_secs=10)
        assert held_when_reconciled == [2]
        assert h.client._buffer is None
        await wait_until(lambda: status(h, STOP) == OrderStatus.FILLED)
        assert not any("No Nautilus order" in line for line in h.logger.warnings())


def our_market_position(venue: ExecutionVenue, *, opened: int, closed: int | None = None) -> None:
    """The node's market order on `US100.cash`, filled at `opened`; closed at `closed` if given."""
    entry_id = "O-M-5300001"
    entry = make_order(
        6_300_001,
        OURS,
        utc=opened,
        label=order_record.encode_label(entry_id),
        comment=order_record.encode_comment(LegIds(None, None)),
        client_order_id=entry_id,
        symbol=US100_SYMBOL_ID,
    )
    entry.orderStatus = om.ORDER_STATUS_FILLED
    orders = [entry]
    deals = [
        make_deal(7_300_001, 6_300_001, OURS, side=om.BUY, volume=100, price=85000.0, ts=opened)
    ]
    venue.snapshot = oa.ProtoOAReconcileRes(ctidTraderAccountId=ACCOUNT_ID)
    if closed is None:
        position = make_position(OURS, symbol=US100_SYMBOL_ID)
        position.price = 85000.0
        position.utcLastUpdateTimestamp = opened
        venue.snapshot.position.append(position)
    else:
        close = make_order(
            6_300_002, OURS, side=om.SELL, closing=True, utc=closed, symbol=US100_SYMBOL_ID
        )
        close.orderStatus = om.ORDER_STATUS_FILLED
        orders.append(close)
        deals.append(
            make_deal(
                7_300_002, 6_300_002, OURS, side=om.SELL, volume=100, price=85100.0, ts=closed
            ),
        )
    venue.position_orders = {OURS: orders}
    venue.position_deals = {OURS: on_us100(deals)}
    venue.deals = on_us100(deals)


async def test_a_close_in_flight_across_a_reconnect_keeps_the_node_id() -> None:
    # Hand-built: the recording holds no close of the node's.
    now = int(time.time() * 1000)
    venue = ExecutionVenue()
    our_market_position(venue, opened=now - 120_000)
    async with harness(execution_venue=venue) as h:
        await started(h)
        position_id = PositionId(str(OURS))
        assert h.cache.position(position_id).is_open
        order = h.factory.market(
            US100_ID,
            OrderSide.SELL,
            Quantity.from_str("1.00"),
            reduce_only=True,
            client_order_id=ClientOrderId(CLOSE),
        )
        h.cache.add_order(order, position_id=position_id)
        # The venue never answers the close.
        closing = asyncio.create_task(
            h.client._submit_order(
                SubmitOrder(
                    trader_id=TRADER_ID,
                    strategy_id=STRATEGY_ID,
                    order=order,
                    command_id=UUID4(),
                    ts_init=0,
                    position_id=position_id,
                ),
            ),
        )
        await wait_until(lambda: len(h.received(oa.ProtoOAClosePositionReq)) == 1)

        our_market_position(venue, opened=now - 120_000, closed=now - 60_000)
        await h.server.drop_connections()
        await asyncio.wait_for(closing, timeout=10)
        await wait_until(lambda: len(h.mass_statuses) == 1, timeout_secs=10)

        (built,) = h.mass_statuses
        assert built.order_reports[VenueOrderId("6300002")].client_order_id == ClientOrderId(CLOSE)
        assert not any(UUID_SHAPED.match(o.client_order_id.value) for o in h.cache.orders())
        assert status(h, CLOSE) == OrderStatus.FILLED
        assert h.cache.position(position_id).is_closed
        assert h.client._operations.closing(OURS, 100) is None


def target_moved_to(price: float) -> oa.ProtoOAExecutionEvent:
    """The broker's answer to an amend moving the take-profit of the position at `OPEN_AT`."""
    at = snapshot_at(OPEN_AT).position[0].utcLastUpdateTimestamp
    (last,) = [
        e
        for e in FIRST_EVENTS
        if e.executionType == om.ORDER_REPLACED and e.order.utcLastUpdateTimestamp == at
    ]
    event = type(last)()
    event.CopyFrom(last)
    event.isServerEvent = False
    event.order.limitPrice = price
    event.order.utcLastUpdateTimestamp = at + 1_000
    event.position.takeProfit = price
    event.position.utcLastUpdateTimestamp = at + 1_000
    return event


async def test_an_amend_answered_during_a_rebuild_reads_the_rebuilt_view() -> None:
    venue = serving()
    venue.server.on(om.PROTO_OA_AMEND_POSITION_SLTP_REQ, lambda _r: target_moved_to(85400.0))
    async with harness(execution_venue=venue) as h:
        await started(h)
        held = HeldReplies(
            h.server,
            om.PROTO_OA_ORDER_LIST_BY_POSITION_ID_REQ,
            venue.replies[om.PROTO_OA_ORDER_LIST_BY_POSITION_ID_REQ],
        )
        rebuilding = asyncio.create_task(h.client._reload())
        await asyncio.wait_for(held.arrived.wait(), timeout=10)
        modifying = asyncio.create_task(
            h.client._modify_order(
                ModifyOrder(
                    trader_id=TRADER_ID,
                    strategy_id=STRATEGY_ID,
                    instrument_id=US100_ID,
                    client_order_id=ClientOrderId(TARGET),
                    venue_order_id=None,
                    quantity=None,
                    price=Price.from_str("85400.00"),
                    trigger_price=None,
                    command_id=UUID4(),
                    ts_init=0,
                ),
            ),
        )
        await wait_until(lambda: len(h.client._buffer or []) == 1)

        await held.stop_holding()
        await asyncio.wait_for(rebuilding, timeout=10)
        await asyncio.wait_for(modifying, timeout=10)

        await wait_until(
            lambda: h.cache.order(ClientOrderId(TARGET)).price == Price.from_str("85400.00"),
        )
        assert "OrderModifyRejected" not in h.kinds_of(TARGET)
        assert h.kinds_of(TARGET)[-1] == "OrderUpdated"
        assert h.logger.errors() == []


async def test_an_amend_waits_for_the_model_no_longer_than_the_connect_timeout() -> None:
    async with harness(config=exec_config(connect_timeout_secs=0.1)) as h:
        h.client._hold_buffer()

        await asyncio.wait_for(h.client._wait_for_model(), timeout=5)

        assert any("not rebuilt within 0.1s" in line for line in h.logger.warnings())


async def test_a_failed_reconnect_pass_releases_the_buffer() -> None:
    venue = serving()
    async with harness(execution_venue=venue) as h:
        await started(h)
        venue.fail = {om.PROTO_OA_DEAL_LIST_REQ}

        await h.server.drop_connections()
        await wait_until(lambda: any("Restore" in line for line in h.logger.errors()))

        assert h.client._buffer is None
        assert h.client._model_standing.is_set()
        assert h.mass_statuses == []
