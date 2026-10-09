"""Reconciliation records: what the broker's lists say stands now and happened in the window.

Built from one snapshot, each position's own order and deal lists, and the window's deals, with
the venue model's parsing. Pure: it reads no cache, keeps no state and changes nothing it is
given, so the same input always gives the same records.
"""

from __future__ import annotations

from collections.abc import Callable, Collection, Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from decimal import ROUND_HALF_EVEN, Decimal
from typing import Protocol

from nautilus_ctrader.common import order_record
from nautilus_ctrader.common.venue_book import (
    EXTERNAL_TYPE,
    created_of,
    entry_of,
    levels_of,
    opening_orders,
    remaining_of,
    which_level,
)
from nautilus_ctrader.common.venue_records import (
    Contingency,
    ExternalType,
    Fill,
    Level,
    Notice,
    Operations,
    ReportedOrder,
    ReportedPosition,
    ReportStatus,
    leg_venue_order_id,
    money_of,
    parse_leg_venue_order_id,
    price_of,
    units_of,
)
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as om

_SIDE = {om.BUY: "BUY", om.SELL: "SELL"}
_OPPOSITE = {"BUY": "SELL", "SELL": "BUY"}
_LEVELS = (Level.STOP_LOSS, Level.TAKE_PROFIT)
_ENDED = {
    om.ORDER_STATUS_CANCELLED: ReportStatus.CANCELED,
    om.ORDER_STATUS_EXPIRED: ReportStatus.EXPIRED,
    om.ORDER_STATUS_REJECTED: ReportStatus.REJECTED,
}
_GOOD_TILL_CANCEL = om.ProtoOATimeInForce.Name(om.GOOD_TILL_CANCEL)


@dataclass(frozen=True)
class PositionHistory:
    """One position's order and deal lists, as the broker returned them.

    `complete` is false when the deal list did not end, so some deals may be missing.
    """

    orders: tuple[om.ProtoOAOrder, ...]
    deals: tuple[om.ProtoOADeal, ...]
    complete: bool = True


class HeldOrders(Protocol):
    """What Nautilus holds of the legs of positions the node did not open, by venue order id."""

    def closed(self, venue_order_id: str) -> bool:
        """Whether Nautilus holds the order as a closed order."""
        ...

    def trade_ids(self, venue_order_id: str) -> Collection[str]:
        """The trade ids of the fills Nautilus holds for the order."""
        ...

    def open_legs(self, entry_order_id: int) -> Collection[str]:
        """The venue order ids of the open legs Nautilus holds for the entry `entry_order_id`."""
        ...


class NothingHeld:
    """Nautilus holds no order: a start without a persistent cache."""

    def closed(self, venue_order_id: str) -> bool:
        return False

    def trade_ids(self, venue_order_id: str) -> Collection[str]:
        return ()

    def open_legs(self, entry_order_id: int) -> Collection[str]:
        return ()


NOTHING_HELD = NothingHeld()


@dataclass(frozen=True)
class Reconciliation:
    """`orders` is chronological, as `reconcile` describes."""

    orders: tuple[ReportedOrder, ...]
    positions: tuple[ReportedPosition, ...]
    notices: tuple[Notice, ...]


def reconcile(
    snapshot: oa.ProtoOAReconcileRes,
    histories: Mapping[int, PositionHistory],
    window_deals: Sequence[om.ProtoOADeal],
    precision: Callable[[int], int | None],
    known_closes: Mapping[int, str],
    operations: Operations,
    held: HeldOrders = NOTHING_HELD,
) -> Reconciliation:
    """The reports for everything the broker holds now and every position traded in the window.

    `snapshot` is asked with protection orders. `histories` holds each position's own lists, by
    position id. `known_closes` maps a broker order id to the node's close the venue model has
    matched it to. `precision` gives a symbol's price precision, `None` for an unloaded one.
    `held` says what Nautilus holds of the legs of positions the node did not open.

    Positions taken: every open one in `snapshot`, every one with a deal in `window_deals`, and
    every one in `histories`.

    - One on an unloaded symbol is skipped silently: the unloaded exposure covers it.
    - One without a history, or whose history lists no entry, is skipped with a `Notice`.
    - One whose entry never filled is never reported.

    Whether a position is open, and how much of it, is decided by its own deals
    (`open_volume`), read after `snapshot`:

    - one they leave at zero is closed whatever `snapshot` says;
    - one they leave open that `snapshot` lacks is skipped with a `Notice`;
    - an open one is reported with the volume they leave, at `snapshot`'s price and time.

    A history that is not `complete` never closes a position `snapshot` holds open, and its
    position is reported with `snapshot`'s volume.

    What a taken position reports:

    - Its entry, as filled with its own deals: of the orders neither closing nor protective, the
      earliest created that carries the node's record, else the earliest (`entry_of`).
      `avg_price` is their volume-weighted price. When the entry's `label` is the node's record
      the report carries that id, contingency `OTO` and the legs named in `comment`. A foreign
      entry carries neither.
    - Each other such order with a deal, one that raised the position, as filled with its own
      deals and no client order id, the node's position included: the node never raises one.
    - Each closing order with a deal, as a reduce-only `MARKET`. Its client order id is the
      matched close from `known_closes`, else, for a close with a deal in `window_deals`, the
      node's close of the same volume on that position still in flight (`operations.closing`),
      never claiming one id twice, else none.
    - For the node's position, each leg named in `comment`, under `leg_venue_order_id`, with the
      entry as parent, contingency `OUO` and the other leg linked. A stop leg is a
      `STOP_MARKET` with `trigger_price`; a take-profit leg is a `LIMIT` with `price`.
      - Open position: `ACCEPTED` at the broker's level, for the protective order's remaining
        volume, while the level stands; `CANCELED` once it is gone.
      - Closed position: `FILLED` with the protective order's deal `which_level` attributes to
        it, or `CANCELED` with what it had if that falls short of the leg; any other leg is
        `CANCELED` at the closing deal's time.
    - For a position that is not the node's, each level as a leg with no client order id, no
      parent and no links, typed as the node's legs are, under `leg_venue_order_id` of its
      entry at a generation chosen from `held`:
      - A level that stands, or one with a fill, takes the lowest generation Nautilus does not
        hold closed, or one that holds a fill of the level's.
      - Open position: `ACCEPTED` for each standing level; a level gone is reported only if it
        filled in part, `CANCELED` with its fills.
      - Closed position: each level of the protective order that filled, else of the last one
        listed, `FILLED` or `CANCELED` as for the node's legs. An unfilled one is reported only
        at generation 1 or when Nautilus holds it open: a later generation unheld means Nautilus
        already holds the level's last leg ended.
      - Every other leg of the entry Nautilus holds open is `CANCELED`, at the price it holds.
    - A protective order's deal no leg takes (no leg of that level, or no level known): an
      external reduce-only order under the protective order's own id, typed by `which_level`
      (`STOP_MARKET` or `LIMIT`), or `MARKET` with no level known.
    - An open position's `ReportedPosition`, even when its lists show no entry fill (with a
      `Notice`): the position stands at the broker whatever its lists say.

    Never reported: an open protective order (it is its position's levels) and a closing order in
    a position's lists with no deal. Every other order in `snapshot` on a loaded symbol not
    reported above is reported as it stands, with the node's id when it is the node's: for an
    entry, the one in its `label`; for a closing order, the matched or in-flight close, as above
    but with no deal.

    `orders` is sorted by the first fill's time, or `ts_ms` for a report with no fill, so a
    cancelled leg of a closed position sorts at the closing deal. Ties put a non-closing order
    first, then a position's entry, then go by venue order id. No report of a position sorts
    before its entry: a fill Nautilus meets before the entry's would open the position the other
    way.
    """
    open_positions = {position.positionId: position for position in snapshot.position}
    symbols = {pid: position.tradeData.symbolId for pid, position in open_positions.items()}
    for deal in window_deals:
        symbols.setdefault(deal.positionId, deal.symbolId)
    for position_id, found in histories.items():
        symbol_id = _symbol_of(found)
        if symbol_id is not None:
            symbols.setdefault(position_id, symbol_id)
    live_protective = {
        order.positionId: order
        for order in snapshot.order
        if order.orderType == om.STOP_LOSS_TAKE_PROFIT
    }
    claimed = set(known_closes.values())
    in_window = {str(deal.dealId) for deal in window_deals}
    keyed: list[tuple[tuple[int, bool, bool, str], ReportedOrder]] = []
    positions: list[ReportedPosition] = []
    notices: list[Notice] = []
    for position_id in sorted(symbols):
        digits = precision(symbols[position_id])
        if digits is None:
            continue
        found = histories.get(position_id)
        if found is None:
            notices.append(
                Notice(f"position {position_id} has no order and deal lists; it is not reported")
            )
            continue
        entry = entry_of(found.orders)
        if entry is None:
            notices.append(
                Notice(f"position {position_id} lists no entry order; it is not reported")
            )
            continue
        venue_position = open_positions.get(position_id)
        built = _Position(position_id, entry, found, digits)
        if not built.entry_fills:
            # Created and never filled: nothing to tell Nautilus, unless the broker holds it open.
            if venue_position is not None:
                notices.append(
                    Notice(
                        f"open position {position_id} lists no entry fill; only the position is "
                        "reported",
                    )
                )
                positions.append(_position_report(venue_position, digits))
            continue
        volume = open_volume(found)
        still_open = volume > 0
        if venue_position is not None and not still_open and found.complete:
            # Closed after the snapshot was taken.
            venue_position = None
        elif venue_position is None and still_open:
            notices.append(
                Notice(
                    f"position {position_id} is open by its own deals but not in the snapshot; "
                    "it is not reported, and its execution events carry it",
                )
            )
            continue
        reports, said = built.reports(
            venue_position,
            live_protective.get(position_id),
            known_closes,
            operations,
            claimed,
            in_window,
            held,
        )
        notices += said
        entry_ts = _sort_key(reports[0])[0]
        keyed += [
            (_sort_key(report, not_before=entry_ts, entry=index == 0), report)
            for index, report in enumerate(reports)
        ]
        if venue_position is not None:
            units = units_of(volume) if found.complete and still_open else None
            positions.append(_position_report(venue_position, digits, units))
    reported = {report.venue_order_id for _, report in keyed}
    for order in snapshot.order:
        if order.orderType == om.STOP_LOSS_TAKE_PROFIT or str(order.orderId) in reported:
            continue
        digits = precision(order.tradeData.symbolId)
        if digits is None:
            continue
        if order.closingOrder:
            client_order_id = _close_id(order, known_closes, operations, claimed, ask=True)
        else:
            client_order_id = order_record.parse_label(order.tradeData.label)
        report = unfilled_order(order, digits, client_order_id)
        keyed.append((_sort_key(report), report))
    keyed.sort(key=lambda item: item[0])
    return Reconciliation(
        orders=tuple(report for _, report in keyed),
        positions=tuple(positions),
        notices=tuple(notices),
    )


def one_position(
    snapshot: oa.ProtoOAReconcileRes,
    position_id: int,
    found: PositionHistory,
    precision: Callable[[int], int | None],
    known_closes: Mapping[int, str],
    operations: Operations,
    held: HeldOrders = NOTHING_HELD,
) -> Reconciliation:
    """`reconcile` of position `position_id` alone, open or closed, from its own lists `found`.

    Each of its deals counts as in the window, so a closed position is taken too.
    """
    alone = oa.ProtoOAReconcileRes(
        ctidTraderAccountId=snapshot.ctidTraderAccountId,
        position=[p for p in snapshot.position if p.positionId == position_id],
        order=[o for o in snapshot.order if o.positionId == position_id],
    )
    return reconcile(
        alone, {position_id: found}, found.deals, precision, known_closes, operations, held
    )


def open_volume(found: PositionHistory) -> int:
    """The venue volume a position's own deals leave open: its entry's side less the other's.

    Zero when its lists show no entry.
    """
    entry = entry_of(found.orders)
    if entry is None:
        return 0
    side = entry.tradeData.tradeSide
    # TODO(verify): that a deal which did not fill carries no `filledVolume`; every recorded
    # deal filled.
    return sum(
        deal.filledVolume if deal.tradeSide == side else -deal.filledVolume for deal in found.deals
    )


def _symbol_of(found: PositionHistory) -> int | None:
    entry = entry_of(found.orders)
    if entry is not None:
        return entry.tradeData.symbolId
    return found.deals[0].symbolId if found.deals else None


def entry_named(orders: Iterable[om.ProtoOAOrder], client_order_id: str) -> om.ProtoOAOrder | None:
    """The broker order that the node's entry or market order `client_order_id` became.

    Told by the node's record in its `label`. A protective or closing order is never taken,
    though either may carry the entry's ids.
    """
    for order in orders:
        if order.closingOrder or order.orderType == om.STOP_LOSS_TAKE_PROFIT:
            continue
        if order_record.parse_label(order.tradeData.label) == client_order_id:
            return order
    return None


def unfilled_order(
    order: om.ProtoOAOrder, precision: int, client_order_id: str | None
) -> ReportedOrder:
    """An order with no fill, at its own status: a pending order, or one that ended unfilled."""
    return _order_report(
        order, precision, (), client_order_id=client_order_id, reduce_only=order.closingOrder
    )


def _close_id(
    order: om.ProtoOAOrder,
    known_closes: Mapping[int, str],
    operations: Operations,
    claimed: set[str],
    *,
    ask: bool,
) -> str | None:
    """The node's id for a closing order, if it is the node's; `ask` offers it to `operations`."""
    close_id = known_closes.get(order.orderId)
    # A stop-out is the broker's own close, never the node's.
    if close_id is None and ask and not order.isStopOut:
        candidate = operations.closing(
            order.positionId, order.tradeData.volume, created_of(order), order.orderId
        )
        if candidate is not None and candidate not in claimed:
            claimed.add(candidate)
            close_id = candidate
    return close_id


def _sort_key(
    report: ReportedOrder, not_before: int = 0, *, entry: bool = False
) -> tuple[int, bool, bool, str]:
    first = report.fills[0].ts_ms if report.fills else report.ts_ms
    return max(first, not_before), report.reduce_only, not entry, report.venue_order_id


class _Position:
    """One taken position's reports, built from its own lists."""

    def __init__(
        self,
        position_id: int,
        entry: om.ProtoOAOrder,
        found: PositionHistory,
        precision: int,
    ) -> None:
        self.position_id = position_id
        self.entry = entry
        self.precision = precision
        self.side = _SIDE[entry.tradeData.tradeSide]
        self.deals = sorted(found.deals, key=lambda deal: (deal.executionTimestamp, deal.dealId))
        self.fills: dict[int, list[Fill]] = {}
        for deal in self.deals:
            self.fills.setdefault(deal.orderId, []).append(_fill(deal, precision))
        self.entry_fills = self.fills.get(entry.orderId, [])
        self.raises = [
            order
            for order in opening_orders(found.orders)
            if order.orderId != entry.orderId and order.orderId in self.fills
        ]
        self.orders = sorted(found.orders, key=lambda order: order.orderId)
        self.protective = [o for o in self.orders if o.orderType == om.STOP_LOSS_TAKE_PROFIT]

    def reports(
        self,
        venue_position: om.ProtoOAPosition | None,
        live_protective: om.ProtoOAOrder | None,
        known_closes: Mapping[int, str],
        operations: Operations,
        claimed: set[str],
        in_window: set[str],
        held: HeldOrders,
    ) -> tuple[list[ReportedOrder], list[Notice]]:
        """The entry's report first, then the rest in no particular order."""
        notices: list[Notice] = []
        entry_id = order_record.parse_label(self.entry.tradeData.label)
        leg_ids: dict[Level, str] = {}
        if entry_id is not None:
            legs = order_record.parse_comment(self.entry.tradeData.comment)
            if legs is None:
                notices.append(
                    Notice(
                        f"position {self.position_id} is the node's own, but its legs' record "
                        "cannot be read; its levels stay at the broker without legs",
                    )
                )
            else:
                found = zip(_LEVELS, (legs.stop_loss, legs.take_profit), strict=True)
                leg_ids = {level: leg_id for level, leg_id in found if leg_id is not None}
        entry = _order_report(
            self.entry,
            self.precision,
            tuple(self.entry_fills),
            client_order_id=entry_id,
            reduce_only=False,
        )
        if leg_ids:
            entry = replace(
                entry, linked_order_ids=tuple(leg_ids.values()), contingency=Contingency.OTO
            )
        raised = [
            _order_report(
                order,
                self.precision,
                tuple(self.fills[order.orderId]),
                client_order_id=None,
                reduce_only=False,
            )
            for order in self.raises
        ]
        reports = [entry, *raised, *self._closes(known_closes, operations, claimed, in_window)]
        # A foreign position's every level is a leg.
        leg_fills, external, said = self._triggers(_LEVELS if entry_id is None else leg_ids)
        notices += said
        if entry_id is None:
            reports += self._foreign_legs(leg_fills, venue_position, live_protective, held)
        for level, leg_id in leg_ids.items():
            linked = tuple(other for key, other in leg_ids.items() if key != level)
            reports.append(
                self._leg(
                    level,
                    leg_id,
                    entry_id,
                    linked,
                    leg_fills.get(level, []),
                    venue_position,
                    live_protective,
                )
            )
        reports += external
        return reports, notices

    def _closes(
        self,
        known_closes: Mapping[int, str],
        operations: Operations,
        claimed: set[str],
        in_window: set[str],
    ) -> list[ReportedOrder]:
        closes = [
            order
            for order in self.orders
            if order.closingOrder
            and order.orderType != om.STOP_LOSS_TAKE_PROFIT
            and order.orderId in self.fills
        ]
        # Newest first: a close in flight whose answer was lost is the latest of its volume.
        closes.sort(key=lambda order: (order.utcLastUpdateTimestamp, order.orderId), reverse=True)
        reports = []
        for order in closes:
            # Only a recent close can be the answer to a close still in flight.
            recent = any(fill.trade_id in in_window for fill in self.fills[order.orderId])
            close_id = _close_id(order, known_closes, operations, claimed, ask=recent)
            reports.append(
                _order_report(
                    order,
                    self.precision,
                    tuple(self.fills[order.orderId]),
                    client_order_id=close_id,
                    reduce_only=True,
                    order_type=ExternalType.MARKET,
                )
            )
        return reports

    def _triggers(
        self, with_legs: Collection[Level]
    ) -> tuple[dict[Level, list[Fill]], list[ReportedOrder], list[Notice]]:
        """The protective orders' deals: fills for the levels `with_legs`, reports for the rest."""
        leg_fills: dict[Level, list[Fill]] = {}
        external: list[ReportedOrder] = []
        notices: list[Notice] = []
        for order in self.protective:
            fills = self.fills.get(order.orderId, [])
            if not fills:
                continue
            levels = levels_of(order, self.precision)
            untaken: list[Fill] = []
            order_type = ExternalType.MARKET
            for fill in fills:
                if not levels:
                    untaken.append(fill)
                    continue
                level, said = which_level(self.side, levels, fill.price)
                notices += [notice for notice in said if isinstance(notice, Notice)]
                if level in with_legs:
                    leg_fills.setdefault(level, []).append(fill)
                else:
                    untaken.append(fill)
                    order_type = (
                        ExternalType.STOP_MARKET if level == Level.STOP_LOSS else ExternalType.LIMIT
                    )
            if untaken:
                external.append(
                    _order_report(
                        order,
                        self.precision,
                        tuple(untaken),
                        client_order_id=None,
                        reduce_only=True,
                        order_type=order_type,
                    )
                )
        return leg_fills, external, notices

    def _foreign_legs(
        self,
        leg_fills: Mapping[Level, list[Fill]],
        venue_position: om.ProtoOAPosition | None,
        live_protective: om.ProtoOAOrder | None,
        held: HeldOrders,
    ) -> list[ReportedOrder]:
        """A foreign position's levels as its legs, as `reconcile` describes."""
        held_open = held.open_legs(self.entry.orderId)
        reports: list[ReportedOrder] = []
        for level in _LEVELS:
            fills = leg_fills.get(level, [])
            if venue_position is not None:
                levels = _position_levels(venue_position, self.precision)
            else:
                source = self._closing_source(fills)
                levels = {} if source is None else levels_of(source, self.precision)
            if level not in levels and not fills:
                continue
            stands = venue_position is not None and level in levels
            venue_order_id, generation, fills = self._generation(level, fills, held, stands=stands)
            report = self._leg(
                level,
                None,
                None,
                (),
                fills,
                venue_position,
                live_protective,
                venue_order_id=venue_order_id,
            )
            ended_unfilled = report.status == ReportStatus.CANCELED and not fills
            if ended_unfilled and generation > 1 and venue_order_id not in held_open:
                continue
            reports.append(report)
        reported = {report.venue_order_id for report in reports}
        for venue_order_id in sorted(held_open):
            parsed = parse_leg_venue_order_id(venue_order_id)
            if venue_order_id in reported or parsed is None:
                continue
            gone = self._leg(
                parsed[1],
                None,
                None,
                (),
                [],
                venue_position,
                live_protective,
                venue_order_id=venue_order_id,
            )
            if venue_position is not None:
                ts_ms = venue_position.utcLastUpdateTimestamp
            else:
                ts_ms = self.deals[-1].executionTimestamp
            # Any level the broker lists now is another leg's: the price is the one held.
            reports.append(
                replace(
                    gone,
                    status=ReportStatus.CANCELED,
                    ts_ms=max(ts_ms, gone.ts_accepted_ms),
                    price=None,
                    trigger_price=None,
                )
            )
        return reports

    def _generation(
        self, level: Level, fills: list[Fill], held: HeldOrders, *, stands: bool
    ) -> tuple[str, int, list[Fill]]:
        """A foreign leg's venue order id, its generation, and the fills it takes of `fills`.

        - A level that `stands` takes the lowest generation Nautilus does not hold closed, as
          `VenueBook.load` does, and leaves the fills Nautilus holds under an earlier one.
        - An ended level takes the lowest generation Nautilus does not hold closed or that holds
          one of `fills`, and takes them all.
        """
        trade_ids = {fill.trade_id for fill in fills}
        taken: set[str] = set()
        generation = 1
        while True:
            venue_order_id = leg_venue_order_id(self.entry.orderId, level, generation)
            held_trades = trade_ids.intersection(held.trade_ids(venue_order_id))
            if not held.closed(venue_order_id) or (held_trades and not stands):
                return venue_order_id, generation, [f for f in fills if f.trade_id not in taken]
            taken |= held_trades
            generation += 1

    def _closing_source(self, fills: list[Fill]) -> om.ProtoOAOrder | None:
        """The protective order that filled for a level, else the last one listed."""
        return next(
            (o for o in self.protective if o.orderId in self._fill_orders(fills)),
            max(self.protective, key=lambda o: o.utcLastUpdateTimestamp, default=None),
        )

    def _leg(
        self,
        level: Level,
        leg_id: str | None,
        entry_id: str | None,
        linked: tuple[str, ...],
        fills: list[Fill],
        venue_position: om.ProtoOAPosition | None,
        live_protective: om.ProtoOAOrder | None,
        *,
        venue_order_id: str | None = None,
    ) -> ReportedOrder:
        """A leg as the broker's lists leave it; `leg_id` is the node's, `None` for a foreign one.

        `venue_order_id` defaults to the leg's first generation.
        """
        filled = sum((fill.units for fill in fills), Decimal(0))
        first_fill = self.entry_fills[0].ts_ms
        if venue_position is not None:
            source = live_protective
            levels = _position_levels(venue_position, self.precision)
            if live_protective is not None:
                rest = units_of(remaining_of(live_protective))
                ts_ms = live_protective.utcLastUpdateTimestamp
            else:
                rest = units_of(venue_position.tradeData.volume)
                ts_ms = venue_position.utcLastUpdateTimestamp
            if level in levels:
                status = ReportStatus.PARTIALLY_FILLED if fills else ReportStatus.ACCEPTED
            else:
                status, ts_ms = ReportStatus.CANCELED, venue_position.utcLastUpdateTimestamp
            units = filled + rest
        else:
            source = self._closing_source(fills)
            levels = {} if source is None else levels_of(source, self.precision)
            opened = [*self.entry_fills, *(f for o in self.raises for f in self.fills[o.orderId])]
            leg_units = (
                units_of(source.tradeData.volume)
                if source is not None
                else sum((fill.units for fill in opened), Decimal(0))
            )
            units = max(leg_units, filled)
            # TODO(verify): whether a partly filled protective order's `volume` is its total or
            # its rest, the question `remaining_of` has; read as the total here, as one reduced
            # by a partial close reports it (confirmed live). A level that closes part of a
            # position would settle it.
            if fills and filled >= leg_units:
                status, ts_ms = ReportStatus.FILLED, fills[-1].ts_ms
            else:
                status, ts_ms = ReportStatus.CANCELED, self.deals[-1].executionTimestamp
        accepted = first_fill
        if source is not None and source.tradeData.HasField("openTimestamp"):
            # A leg is accepted once its level is set, never before the entry filled.
            accepted = max(source.tradeData.openTimestamp, first_fill)
        level_price = levels.get(level)
        stop = level == Level.STOP_LOSS
        return ReportedOrder(
            venue_order_id=venue_order_id or leg_venue_order_id(self.entry.orderId, level),
            client_order_id=leg_id,
            symbol_id=self.entry.tradeData.symbolId,
            side=_OPPOSITE[self.side],
            order_type=ExternalType.STOP_MARKET if stop else ExternalType.LIMIT,
            status=status,
            units=units,
            filled_units=filled,
            reduce_only=True,
            venue_position_id=str(self.position_id),
            ts_accepted_ms=accepted,
            ts_ms=max(ts_ms, accepted),
            avg_price=_average(fills, self.precision),
            price=None if stop else level_price,
            trigger_price=level_price if stop else None,
            time_in_force=_GOOD_TILL_CANCEL,
            parent_order_id=entry_id,
            linked_order_ids=linked,
            contingency=None if leg_id is None else Contingency.OUO,
            fills=tuple(fills),
        )

    def _fill_orders(self, fills: list[Fill]) -> set[int]:
        trade_ids = {fill.trade_id for fill in fills}
        return {deal.orderId for deal in self.deals if str(deal.dealId) in trade_ids}


def _fill(deal: om.ProtoOADeal, precision: int) -> Fill:
    return Fill(
        trade_id=str(deal.dealId),
        venue_position_id=str(deal.positionId),
        side=_SIDE[deal.tradeSide],
        units=units_of(deal.filledVolume),
        price=price_of(deal.executionPrice, precision),
        commission=money_of(deal.commission, deal.moneyDigits),
        ts_ms=deal.executionTimestamp,
    )


def _average(fills: Sequence[Fill], precision: int) -> Decimal | None:
    units = sum((fill.units for fill in fills), Decimal(0))
    if not units:
        return None
    total = sum((fill.price * fill.units for fill in fills), Decimal(0))
    return (total / units).quantize(Decimal(1).scaleb(-precision), rounding=ROUND_HALF_EVEN)


def _position_levels(position: om.ProtoOAPosition, precision: int) -> dict[Level, Decimal]:
    levels: dict[Level, Decimal] = {}
    if position.HasField("stopLoss"):
        levels[Level.STOP_LOSS] = price_of(position.stopLoss, precision)
    if position.HasField("takeProfit"):
        levels[Level.TAKE_PROFIT] = price_of(position.takeProfit, precision)
    return levels


def _status(order: om.ProtoOAOrder, fills: Sequence[Fill]) -> ReportStatus:
    if order.orderStatus == om.ORDER_STATUS_FILLED:
        return ReportStatus.FILLED
    if order.orderStatus in _ENDED:
        return _ENDED[order.orderStatus]
    return ReportStatus.PARTIALLY_FILLED if fills else ReportStatus.ACCEPTED


def _order_report(
    order: om.ProtoOAOrder,
    precision: int,
    fills: tuple[Fill, ...],
    *,
    client_order_id: str | None,
    reduce_only: bool,
    order_type: ExternalType | None = None,
) -> ReportedOrder:
    """`order` as it stands, with `fills` its own deals in time order."""
    kind = order_type or EXTERNAL_TYPE.get(order.orderType, ExternalType.MARKET)
    prices = levels_of(order, precision)
    priced = kind in (ExternalType.LIMIT, ExternalType.STOP_LIMIT)
    triggered = kind in (ExternalType.STOP_MARKET, ExternalType.STOP_LIMIT)
    data = order.tradeData
    if data.HasField("openTimestamp"):
        accepted = data.openTimestamp
    else:
        accepted = fills[0].ts_ms if fills else order.utcLastUpdateTimestamp
    last = max(order.utcLastUpdateTimestamp, fills[-1].ts_ms if fills else 0)
    return ReportedOrder(
        venue_order_id=str(order.orderId),
        client_order_id=client_order_id,
        symbol_id=data.symbolId,
        side=_SIDE[data.tradeSide],
        order_type=kind,
        status=_status(order, fills),
        units=units_of(data.volume),
        filled_units=sum((fill.units for fill in fills), Decimal(0)),
        reduce_only=reduce_only,
        venue_position_id=str(order.positionId) if order.HasField("positionId") else None,
        ts_accepted_ms=accepted,
        ts_ms=last,
        avg_price=_average(fills, precision),
        price=prices.get(Level.TAKE_PROFIT) if priced else None,
        trigger_price=prices.get(Level.STOP_LOSS) if triggered else None,
        time_in_force=(
            om.ProtoOATimeInForce.Name(order.timeInForce) if order.HasField("timeInForce") else None
        ),
        expire_ts_ms=order.expirationTimestamp if order.HasField("expirationTimestamp") else None,
        fills=fills,
    )


def _position_report(
    position: om.ProtoOAPosition, precision: int, units: Decimal | None = None
) -> ReportedPosition:
    """`position` as the snapshot holds it, with `units` instead of its volume when given."""
    return ReportedPosition(
        venue_position_id=str(position.positionId),
        symbol_id=position.tradeData.symbolId,
        side=_SIDE[position.tradeData.tradeSide],
        units=units_of(position.tradeData.volume) if units is None else units,
        avg_price=price_of(position.price, precision),
        ts_ms=position.utcLastUpdateTimestamp,
    )
