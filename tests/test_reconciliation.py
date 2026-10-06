"""Reconciliation records built from the recorded session's lists, as foreign and as the node's."""

from __future__ import annotations

import copy
from decimal import Decimal

from nautilus_ctrader.common import order_record
from nautilus_ctrader.common.reconciliation import (
    PositionHistory,
    Reconciliation,
    entry_named,
    one_position,
    open_volume,
    reconcile,
    unfilled_order,
)
from nautilus_ctrader.common.venue_book import levels_of
from nautilus_ctrader.common.venue_records import (
    Contingency,
    ExternalType,
    Level,
    ReportedOrder,
    ReportStatus,
    units_of,
)
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as om
from tests.execution_replay import (
    FIRST,
    RECORDING,
    SECOND,
    NoOperations,
    as_ours,
    entry_id,
    history,
    make_deal,
    make_order,
    make_position,
    our_entry,
    precision,
    snapshot_at,
    stop_id,
    target_id,
)

NOTHING = NoOperations()
CLOSED_AT = 408.8  # FIRST closed by its stop-loss, nothing open
EMPTY_AT = 762.7  # SECOND closed too, nothing open or pending
OPEN_AT = 358.0  # FIRST open with 99 after a manual partial close, both levels set
UNLOADED = 5_000_004


def window(*position_ids: int) -> tuple[om.ProtoOADeal, ...]:
    return tuple(
        deal
        for response in RECORDING["closing"]["account_deals"]
        for deal in response.deal
        if deal.positionId in position_ids
    )


def ours(found: PositionHistory, position_id: int) -> PositionHistory:
    return PositionHistory(tuple(as_ours(found.orders, [position_id])), found.deals)


def run(snapshot, histories, deals, *, known_closes=None, operations=NOTHING) -> Reconciliation:
    return reconcile(snapshot, histories, deals, precision, known_closes or {}, operations)


def by_id(result: Reconciliation) -> dict[str, ReportedOrder]:
    return {report.venue_order_id: report for report in result.orders}


def trade_ids(report: ReportedOrder) -> tuple[str, ...]:
    return tuple(fill.trade_id for fill in report.fills)


def closed_first(*, mine: bool) -> Reconciliation:
    snapshot = snapshot_at(CLOSED_AT)
    found = history(FIRST)
    if mine:
        snapshot = as_ours([snapshot], [FIRST])[0]
        found = ours(found, FIRST)
    return run(snapshot, {FIRST: found}, window(FIRST))


def open_first(*, mine: bool, at: float = OPEN_AT):
    snapshot = snapshot_at(at)
    until = snapshot.position[0].utcLastUpdateTimestamp
    found = history(FIRST, until_ms=until)
    if mine:
        snapshot = as_ours([snapshot], [FIRST])[0]
        found = ours(found, FIRST)
    deals = tuple(deal for deal in window(FIRST) if deal.executionTimestamp <= until)
    return snapshot, {FIRST: found}, deals


def test_closed_position_reports_entry_before_its_closes() -> None:
    result = closed_first(mine=True)

    ids = [report.venue_order_id for report in result.orders]
    assert ids == ["6000001", "6000003", "6000001-SL", "6000001-TP"]
    entry, close, stop, target = result.orders
    assert entry.status == ReportStatus.FILLED
    assert trade_ids(entry) == ("7000001",)
    assert entry.client_order_id == entry_id(FIRST)
    assert entry.avg_price == Decimal("85287.21")
    assert entry.contingency == Contingency.OTO
    assert entry.linked_order_ids == (stop_id(FIRST), target_id(FIRST))
    assert close.reduce_only
    assert close.order_type == ExternalType.MARKET
    assert close.client_order_id is None
    assert stop.status == ReportStatus.FILLED
    assert trade_ids(stop) == ("7000003",)
    assert stop.client_order_id == stop_id(FIRST)
    assert stop.parent_order_id == entry_id(FIRST)
    assert stop.order_type == ExternalType.STOP_MARKET
    assert target.status == ReportStatus.CANCELED
    assert target.client_order_id == target_id(FIRST)
    # Cancelled when the stop-loss closed the position.
    assert target.ts_ms == stop.fills[0].ts_ms
    assert result.positions == ()


def test_foreign_closed_position_uses_real_ids() -> None:
    result = closed_first(mine=False)

    assert [report.venue_order_id for report in result.orders] == ["6000001", "6000003", "6000002"]
    protective = result.orders[2]
    assert protective.order_type == ExternalType.STOP_MARKET
    assert protective.trigger_price == Decimal("85206.20")
    assert protective.reduce_only
    assert protective.status == ReportStatus.FILLED
    assert trade_ids(protective) == ("7000003",)
    assert all(report.client_order_id is None for report in result.orders)
    assert all(report.contingency is None for report in result.orders)


def test_open_position_reports_live_legs() -> None:
    snapshot, histories, deals = open_first(mine=True)

    result = run(snapshot, histories, deals)

    reports = by_id(result)
    stop, target = reports["6000001-SL"], reports["6000001-TP"]
    (protective,) = (o for o in snapshot.order if o.orderType == om.STOP_LOSS_TAKE_PROFIT)
    levels = levels_of(protective, 2)
    assert stop.status == target.status == ReportStatus.ACCEPTED
    assert stop.trigger_price == levels[Level.STOP_LOSS]
    assert target.price == levels[Level.TAKE_PROFIT]
    assert stop.units == target.units == units_of(99)
    assert stop.contingency == target.contingency == Contingency.OUO
    assert stop.linked_order_ids == (target_id(FIRST),)
    assert target.linked_order_ids == (stop_id(FIRST),)
    assert stop.ts_ms == protective.utcLastUpdateTimestamp
    assert reports["6000001"].status == ReportStatus.FILLED
    assert reports["6000003"].reduce_only
    (position,) = result.positions
    assert position.venue_position_id == str(FIRST)
    assert position.units == units_of(99)
    assert position.avg_price == Decimal("85287.21")


def test_a_removed_level_cancels_its_leg() -> None:
    snapshot, histories, deals = open_first(mine=True, at=228.8)

    reports = by_id(run(snapshot, histories, deals))

    assert any(o.orderType == om.STOP_LOSS_TAKE_PROFIT for o in snapshot.order)
    assert reports["6000001-TP"].status == ReportStatus.CANCELED
    assert reports["6000001-SL"].status == ReportStatus.ACCEPTED


def test_reports_are_chronological_across_positions() -> None:
    snapshot = as_ours([snapshot_at(EMPTY_AT)], [FIRST, SECOND])[0]
    histories = {pid: ours(history(pid), pid) for pid in (FIRST, SECOND)}

    result = run(snapshot, histories, window(FIRST, SECOND))

    times = [r.fills[0].ts_ms if r.fills else r.ts_ms for r in result.orders]
    assert times == sorted(times)
    for pid in (FIRST, SECOND):
        mine = [(i, r) for i, r in enumerate(result.orders) if r.venue_position_id == str(pid)]
        entry_at = next(i for i, r in mine if r.client_order_id == entry_id(pid))
        assert all(entry_at < i for i, r in mine if r.reduce_only)
    # SECOND closed at its take-profit.
    reports = by_id(result)
    assert reports["6000004-TP"].status == ReportStatus.FILLED
    assert reports["6000004-SL"].status == ReportStatus.CANCELED


class OneClose:
    """The node's close of `volume` on one position is in flight; every ask names it again."""

    def __init__(self, position_id: int = FIRST, volume: int = 1, close_id: str = "O-C-1") -> None:
        self._close = (position_id, volume)
        self._close_id = close_id

    def amending(self, position_id: int) -> bool:
        return False

    def closing(self, position_id: int, volume: int, created_ms: int) -> str | None:
        return self._close_id if (position_id, volume) == self._close else None


def test_a_close_in_flight_is_named_by_the_operation() -> None:
    snapshot = as_ours([snapshot_at(CLOSED_AT)], [FIRST])[0]
    histories = {FIRST: ours(history(FIRST), FIRST)}

    in_flight = run(snapshot, histories, window(FIRST), operations=OneClose())
    matched = run(snapshot, histories, window(FIRST), known_closes={6000003: "O-C-2"})
    # A close id the model already gave to another broker order is not claimed again.
    taken = run(
        snapshot,
        histories,
        window(FIRST),
        known_closes={6999999: "O-C-1"},
        operations=OneClose(),
    )

    assert by_id(in_flight)["6000003"].client_order_id == "O-C-1"
    assert by_id(matched)["6000003"].client_order_id == "O-C-2"
    assert by_id(taken)["6000003"].client_order_id is None


def test_unparsable_comment_reports_no_legs() -> None:
    snapshot, histories, deals = open_first(mine=True)
    orders = [copy.deepcopy(order) for order in histories[FIRST].orders]
    for order in orders:
        if order.orderId == 6000001:
            order.tradeData.comment = "ntca1|sl"
    histories = {FIRST: PositionHistory(tuple(orders), histories[FIRST].deals)}

    result = run(snapshot, histories, deals)

    assert [r.venue_order_id for r in result.orders] == ["6000001", "6000003"]
    entry = result.orders[0]
    assert entry.client_order_id == entry_id(FIRST)
    assert entry.contingency is None
    assert entry.linked_order_ids == ()
    assert len(result.notices) == 1


def test_a_position_without_history_is_skipped_with_a_notice() -> None:
    result = run(as_ours([snapshot_at(CLOSED_AT)], [FIRST])[0], {}, window(FIRST))

    assert result.orders == ()
    assert result.positions == ()
    assert len(result.notices) == 1
    assert str(FIRST) in result.notices[0].text


def test_unloaded_symbols_are_not_reported() -> None:
    # The third position traded a symbol the node has not loaded.
    deals = window(FIRST, SECOND, UNLOADED)
    histories = {pid: history(pid) for pid in (FIRST, SECOND)}

    result = run(snapshot_at(EMPTY_AT), histories, deals)

    assert result.notices == ()
    assert {r.venue_position_id for r in result.orders} == {str(FIRST), str(SECOND)}

    snapshot, histories, deals = open_first(mine=True)
    nothing = reconcile(snapshot, histories, deals, lambda symbol_id: None, {}, NOTHING)
    assert nothing == Reconciliation((), (), ())


def test_open_protective_order_is_never_a_report() -> None:
    for mine in (True, False):
        snapshot, histories, deals = open_first(mine=mine)

        result = run(snapshot, histories, deals)

        assert "6000002" not in by_id(result)
        assert {r.venue_order_id for r in result.orders} >= {"6000001", "6000003"}


def test_pending_order_is_reported_accepted_without_id() -> None:
    snapshot = snapshot_at(697.3)

    result = run(snapshot, {}, ())

    (order,) = snapshot.order
    (report,) = result.orders
    assert report == unfilled_order(order, 2, None)
    assert report.venue_order_id == "6000006"
    assert report.status == ReportStatus.ACCEPTED
    assert report.client_order_id is None
    assert report.order_type == ExternalType.LIMIT
    assert report.price == Decimal("85100.00")
    assert report.units == units_of(100)
    assert report.filled_units == 0
    assert not report.reduce_only
    assert result.positions == ()


def test_reconcile_changes_nothing_given() -> None:
    snapshot, histories, deals = open_first(mine=True)
    known_closes = {6999999: "O-C-9"}
    before = copy.deepcopy((snapshot, histories, deals, known_closes))

    run(snapshot, histories, deals, known_closes=known_closes)

    assert (snapshot, histories, deals, known_closes) == before


# Hand-built lists, for what the recording did not hold.


def reconcile_res(positions=(), orders=()) -> oa.ProtoOAReconcileRes:
    snapshot = oa.ProtoOAReconcileRes(ctidTraderAccountId=1_000_001)
    snapshot.position.extend(positions)
    snapshot.order.extend(orders)
    return snapshot


def filled(order: om.ProtoOAOrder, deal_id: int, volume: int, ts: int) -> om.ProtoOADeal:
    side = order.tradeData.tradeSide
    return make_deal(
        deal_id, order.orderId, order.positionId, side=side, volume=volume, price=85000.0, ts=ts
    )


def test_an_open_position_without_an_entry_fill_is_reported_with_a_notice() -> None:
    pid = 5_000_101
    snapshot = reconcile_res([make_position(pid)])
    found = PositionHistory((our_entry(pid, 6_000_101),), ())

    result = run(snapshot, {pid: found}, ())

    assert result.orders == ()
    assert [p.venue_position_id for p in result.positions] == [str(pid)]
    assert len(result.notices) == 1


def test_only_a_close_in_the_window_can_take_the_close_in_flight() -> None:
    pid = 5_000_102
    entry = our_entry(pid, 6_000_110, volume=300)
    old = make_order(6_000_111, pid, side=om.SELL, volume=100, closing=True, utc=2000)
    new = make_order(6_000_112, pid, side=om.SELL, volume=100, closing=True, utc=3000)
    entry_deal = filled(entry, 7_000_110, 300, 1000)
    old_deal, new_deal = filled(old, 7_000_111, 100, 2000), filled(new, 7_000_112, 100, 3000)
    snapshot = reconcile_res([make_position(pid, volume=100)])
    both = PositionHistory((entry, old, new), (entry_deal, old_deal, new_deal))
    only_old = PositionHistory((entry, old), (entry_deal, old_deal))
    in_flight = OneClose(pid, 100)

    reports = by_id(run(snapshot, {pid: both}, (new_deal,), operations=in_flight))
    alone = by_id(run(snapshot, {pid: only_old}, (), operations=in_flight))
    matched = by_id(run(snapshot, {pid: only_old}, (), known_closes={6_000_111: "O-C-2"}))

    assert reports["6000112"].client_order_id == "O-C-1"
    assert reports["6000111"].client_order_id is None
    assert alone["6000111"].client_order_id is None
    assert matched["6000111"].client_order_id == "O-C-2"


def test_a_partly_filled_close_still_listed_is_reported_once() -> None:
    pid = 5_000_103
    entry = our_entry(pid, 6_000_120, volume=200)
    close = make_order(6_000_121, pid, side=om.SELL, volume=200, closing=True, utc=2000)
    close_deal = filled(close, 7_000_121, 100, 2000)
    snapshot = reconcile_res([make_position(pid, volume=100)], [close])
    found = PositionHistory((entry, close), (filled(entry, 7_000_120, 200, 1000), close_deal))

    result = run(snapshot, {pid: found}, (close_deal,))

    (report,) = (r for r in result.orders if r.venue_order_id == "6000121")
    assert report.status == ReportStatus.PARTIALLY_FILLED
    assert trade_ids(report) == ("7000121",)


def test_unfilled_orders_carry_the_node_id_when_they_are_the_node_s() -> None:
    # An entry of ours not filled yet, a close of ours not filled yet on an open position of
    # ours, and someone else's pending order.
    pending_entry = our_entry(5_000_104, 6_000_130)
    pid = 5_000_105
    entry = our_entry(pid, 6_000_131)
    close = make_order(6_000_132, pid, side=om.SELL, volume=100, closing=True, utc=2000)
    foreign = make_order(6_000_133, 5_000_106, order_type=om.LIMIT, limit=85000.0)
    snapshot = reconcile_res([make_position(pid)], [pending_entry, close, foreign])
    found = PositionHistory((entry, close), (filled(entry, 7_000_130, 100, 1000),))

    result = run(snapshot, {pid: found}, (), operations=OneClose(pid, 100))

    reports = by_id(result)
    assert reports["6000130"].client_order_id == entry_id(5_000_104)
    assert reports["6000130"].status == ReportStatus.ACCEPTED
    assert reports["6000132"].client_order_id == "O-C-1"
    assert reports["6000132"].reduce_only
    assert reports["6000133"].client_order_id is None
    assert result.notices == ()


def test_one_position_is_reported_from_its_own_lists_alone() -> None:
    # Another open position, with no lists given: left out, not reported with a notice.
    snapshot = as_ours([snapshot_at(OPEN_AT)], [FIRST])[0]
    snapshot.position.append(make_position(SECOND))
    found = ours(history(FIRST, until_ms=snapshot.position[0].utcLastUpdateTimestamp), FIRST)

    result = one_position(snapshot, FIRST, found, precision, {}, NOTHING)

    assert result.notices == ()
    assert {r.venue_position_id for r in result.orders} == {str(FIRST)}
    assert [p.venue_position_id for p in result.positions] == [str(FIRST)]


def test_one_position_takes_a_closed_position_with_no_window() -> None:
    found = ours(history(FIRST), FIRST)

    result = one_position(snapshot_at(CLOSED_AT), FIRST, found, precision, {}, NOTHING)

    assert [r.venue_order_id for r in result.orders] == [
        r.venue_order_id for r in closed_first(mine=True).orders
    ]


def test_the_entry_is_never_a_protective_or_closing_order_carrying_its_ids() -> None:
    label = order_record.encode_label(entry_id(FIRST))
    protective = make_order(
        6_000_150,
        FIRST,
        order_type=om.STOP_LOSS_TAKE_PROFIT,
        label=label,
        client_order_id=entry_id(FIRST),
    )
    close = make_order(
        6_000_151, FIRST, side=om.SELL, closing=True, label=label, client_order_id=entry_id(FIRST)
    )
    # The id without the node's record is somebody else's order.
    unlabelled = make_order(6_000_152, FIRST, client_order_id=entry_id(FIRST))
    orders = [protective, close, unlabelled, our_entry(FIRST, 6_000_153)]

    assert entry_named(orders, entry_id(FIRST)).orderId == 6_000_153
    assert entry_named(orders[:3], entry_id(FIRST)) is None
    assert entry_named(orders, entry_id(SECOND)) is None


# -- The lists, read after the snapshot, decide open or closed -------------------------------


def test_open_volume_nets_the_entry_side_against_the_other() -> None:
    until = snapshot_at(OPEN_AT).position[0].utcLastUpdateTimestamp

    assert open_volume(history(FIRST, until_ms=until)) == 99
    assert open_volume(history(FIRST)) == 0
    assert open_volume(PositionHistory((), history(FIRST).deals)) == 0


def test_a_position_its_deals_close_is_closed_whatever_the_snapshot_says() -> None:
    # The snapshot still holds it open; its lists, read later, hold the stop-loss's fill.
    snapshot = as_ours([snapshot_at(OPEN_AT)], [FIRST])[0]

    result = run(snapshot, {FIRST: ours(history(FIRST), FIRST)}, window(FIRST))

    assert result == closed_first(mine=True)


def test_a_position_its_deals_leave_open_that_the_snapshot_lacks_is_skipped() -> None:
    _, histories, deals = open_first(mine=True)

    result = run(as_ours([snapshot_at(CLOSED_AT)], [FIRST])[0], histories, deals)

    assert result.orders == ()
    assert result.positions == ()
    (notice,) = result.notices
    assert str(FIRST) in notice.text
    assert "snapshot" in notice.text


def test_an_open_position_is_sized_by_its_own_deals() -> None:
    # The snapshot from before the trader's partial close, the lists from after it.
    snapshot = as_ours([snapshot_at(256.5)], [FIRST])[0]
    _, histories, deals = open_first(mine=True)

    (position,) = run(snapshot, histories, deals).positions

    assert snapshot.position[0].tradeData.volume == 100
    assert position.units == units_of(99)
    assert position.avg_price == Decimal("85287.21")


def test_a_deal_list_cut_short_never_closes_what_the_snapshot_holds_open() -> None:
    snapshot = as_ours([snapshot_at(OPEN_AT)], [FIRST])[0]
    found = ours(history(FIRST), FIRST)
    cut_short = PositionHistory(found.orders, found.deals, complete=False)

    result = run(snapshot, {FIRST: cut_short}, window(FIRST))

    (position,) = result.positions
    assert position.units == units_of(99)
    assert by_id(result)["6000001-TP"].status != ReportStatus.CANCELED


def test_a_position_in_the_histories_alone_is_taken() -> None:
    # Closed before the fill window: neither the snapshot nor the window names it.
    snapshot = as_ours([snapshot_at(CLOSED_AT)], [FIRST])[0]

    result = run(snapshot, {FIRST: ours(history(FIRST), FIRST)}, ())

    assert [r.venue_order_id for r in result.orders] == [
        r.venue_order_id for r in closed_first(mine=True).orders
    ]
    assert result.positions == ()
