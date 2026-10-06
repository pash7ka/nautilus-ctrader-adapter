"""Reconciliation records: what the broker's lists say stands now and happened in the window.

Built from one snapshot, each position's own order and deal lists, and the window's deals, with
the venue model's parsing. Pure: it reads no cache, keeps no state and changes nothing it is
given, so the same input always gives the same records.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from decimal import ROUND_HALF_EVEN, Decimal

from nautilus_ctrader.common import order_record
from nautilus_ctrader.common.venue_book import (
    EXTERNAL_TYPE,
    created_of,
    entry_of,
    levels_of,
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
    """One position's order and deal lists, as the broker returned them."""

    orders: tuple[om.ProtoOAOrder, ...]
    deals: tuple[om.ProtoOADeal, ...]


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
) -> Reconciliation:
    """The reports for everything the broker holds now and every position traded in the window.

    `snapshot` is asked with protection orders. `histories` holds each position's own lists, by
    position id. `known_closes` maps a broker order id to the node's close the venue model has
    matched it to. `precision` gives a symbol's price precision, `None` for an unloaded one.

    Positions taken: every open one in `snapshot`, every one with a deal in `window_deals`, and
    every one in `histories`.

    - One on an unloaded symbol is skipped silently: the unloaded exposure covers it.
    - One without a history, or whose history lists no entry, is skipped with a `Notice`.
    - One whose entry never filled is never reported.

    Whether a position is open is decided by its own deals (`open_volume`), read after
    `snapshot`: one they leave at zero is closed whatever `snapshot` says, and one they leave
    open that `snapshot` lacks is skipped with a `Notice`.

    What a taken position reports:

    - Its entry, the one order neither closing nor protective, as filled with its own deals.
      `avg_price` is their volume-weighted price. When the entry's `label` is the node's record
      the report carries that id, contingency `OTO` and the legs named in `comment`. A foreign
      entry carries neither.
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
    - A protective order's deal no leg takes (a foreign position, or no leg of that level): an
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
    first, then go by venue order id. No report of a position sorts before its entry: a fill
    Nautilus meets before the entry's would open the position the other way.
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
    keyed: list[tuple[tuple[int, bool, str], ReportedOrder]] = []
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
        still_open = open_volume(found) > 0
        if venue_position is not None and not still_open:
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
        )
        notices += said
        entry_ts = _sort_key(reports[0])[0]
        keyed += [(_sort_key(report, not_before=entry_ts), report) for report in reports]
        if venue_position is not None:
            positions.append(_position_report(venue_position, digits))
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
) -> Reconciliation:
    """`reconcile` of position `position_id` alone, open or closed, from its own lists `found`.

    Each of its deals counts as in the window, so a closed position is taken too.
    """
    alone = oa.ProtoOAReconcileRes(
        ctidTraderAccountId=snapshot.ctidTraderAccountId,
        position=[p for p in snapshot.position if p.positionId == position_id],
        order=[o for o in snapshot.order if o.positionId == position_id],
    )
    return reconcile(alone, {position_id: found}, found.deals, precision, known_closes, operations)


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
        candidate = operations.closing(order.positionId, order.tradeData.volume, created_of(order))
        if candidate is not None and candidate not in claimed:
            claimed.add(candidate)
            close_id = candidate
    return close_id


def _sort_key(report: ReportedOrder, not_before: int = 0) -> tuple[int, bool, str]:
    first = report.fills[0].ts_ms if report.fills else report.ts_ms
    return max(first, not_before), report.reduce_only, report.venue_order_id


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
        reports = [entry, *self._closes(known_closes, operations, claimed, in_window)]
        leg_fills, external, said = self._triggers(leg_ids)
        notices += said
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
        self, leg_ids: Mapping[Level, str]
    ) -> tuple[dict[Level, list[Fill]], list[ReportedOrder], list[Notice]]:
        """The protective orders' deals: the legs' fills, and reports for those no leg takes."""
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
                if level in leg_ids:
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

    def _leg(
        self,
        level: Level,
        leg_id: str,
        entry_id: str | None,
        linked: tuple[str, ...],
        fills: list[Fill],
        venue_position: om.ProtoOAPosition | None,
        live_protective: om.ProtoOAOrder | None,
    ) -> ReportedOrder:
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
            # The protective order that filled for this level, else the last one listed.
            source = next(
                (o for o in self.protective if o.orderId in self._fill_orders(fills)),
                max(self.protective, key=lambda o: o.utcLastUpdateTimestamp, default=None),
            )
            levels = {} if source is None else levels_of(source, self.precision)
            leg_units = (
                units_of(source.tradeData.volume)
                if source is not None
                else sum((fill.units for fill in self.entry_fills), Decimal(0))
            )
            units = max(leg_units, filled)
            # TODO(verify): whether a partly filled protective order's `volume` is its total or
            # its rest, the question `remaining_of` has; read as the total here. A level that
            # closes part of a position would settle it.
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
            venue_order_id=leg_venue_order_id(self.entry.orderId, level),
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
            contingency=Contingency.OUO,
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


def _position_report(position: om.ProtoOAPosition, precision: int) -> ReportedPosition:
    return ReportedPosition(
        venue_position_id=str(position.positionId),
        symbol_id=position.tradeData.symbolId,
        side=_SIDE[position.tradeData.tradeSide],
        units=units_of(position.tradeData.volume),
        avg_price=price_of(position.price, precision),
        ts_ms=position.utcLastUpdateTimestamp,
    )
