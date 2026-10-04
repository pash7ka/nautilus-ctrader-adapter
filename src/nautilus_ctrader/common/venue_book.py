"""The venue model: what the broker holds, changed only by the broker's own data.

It is built from a snapshot on every connect and changed by execution events, never ahead of the
broker by a command. For each event it returns records saying what Nautilus must be told; it
does no I/O and holds no Nautilus object. What it relies on, from a recorded session:

- a position's stop-loss and take-profit live in one protective order (`STOP_LOSS_TAKE_PROFIT`)
  that the broker creates after the fill, replaces on every level change and fills when a level
  triggers;
- `isServerEvent` separates what the broker did on its own from what a client asked for.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from decimal import Decimal

from nautilus_ctrader.common import order_record
from nautilus_ctrader.common.order_record import LegIds
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
    Operations,
    OrderEvent,
    OrderEventKind,
    ProtectionMissing,
    Record,
    leg_venue_order_id,
    money_of,
    price_of,
    units_of,
)
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as om

_SIDE = {om.BUY: "BUY", om.SELL: "SELL"}
_EXTERNAL_TYPE = {
    om.MARKET: ExternalType.MARKET,
    om.MARKET_RANGE: ExternalType.MARKET,
    om.LIMIT: ExternalType.LIMIT,
    om.STOP: ExternalType.STOP_MARKET,
    om.STOP_LIMIT: ExternalType.STOP_LIMIT,
}
_FILLS = (om.ORDER_FILLED, om.ORDER_PARTIAL_FILL)
_ENDED = {
    om.ORDER_CANCELLED: OrderEventKind.CANCELED,
    om.ORDER_EXPIRED: OrderEventKind.EXPIRED,
    om.ORDER_REJECTED: OrderEventKind.REJECTED,
}
_LEVELS = (Level.STOP_LOSS, Level.TAKE_PROFIT)


@dataclass
class _Leg:
    """A leg as Nautilus knows it; `quantity` and `filled` are venue volumes."""

    client_order_id: str
    alive: bool = True
    accepted: bool = False
    quantity: int = 0
    filled: int = 0


@dataclass
class _Position:
    position_id: int
    symbol_id: int
    side: str
    volume: int = 0
    open: bool = False
    entry_order_id: int | None = None
    entry_client_order_id: str | None = None
    entry_accepted: bool = False
    legs: dict[Level, _Leg] = field(default_factory=dict)
    awaiting_protection: bool = False
    protective_order_id: int | None = None
    protective_volume: int = 0
    levels: dict[Level, Decimal] = field(default_factory=dict)

    @property
    def ours(self) -> bool:
        return self.entry_client_order_id is not None


@dataclass(frozen=True)
class PositionView:
    """A read-only copy of what the model holds for one position."""

    position_id: int
    symbol_id: int
    side: str
    units: Decimal
    open: bool
    ours: bool
    entry_order_id: int | None
    entry_client_order_id: str | None
    protective_order_id: int | None
    levels: dict[Level, Decimal]
    legs: dict[Level, tuple[str, bool]]


def _entry_of(orders: Sequence[om.ProtoOAOrder]) -> om.ProtoOAOrder | None:
    # The protective order and closing orders carry the entry's client order id too.
    for order in orders:
        if not order.closingOrder and order.orderType != om.STOP_LOSS_TAKE_PROFIT:
            return order
    return None


def _opposite(side: int) -> int:
    return om.SELL if side == om.BUY else om.BUY


def _levels_of(order: om.ProtoOAOrder, precision: int) -> dict[Level, Decimal]:
    levels: dict[Level, Decimal] = {}
    if order.HasField("stopPrice"):
        levels[Level.STOP_LOSS] = price_of(order.stopPrice, precision)
    if order.HasField("limitPrice"):
        levels[Level.TAKE_PROFIT] = price_of(order.limitPrice, precision)
    return levels


def _which_level(
    side: str,
    levels: dict[Level, Decimal],
    fill_price: Decimal,
) -> tuple[Level, list[Record]]:
    """The level a protective order's fill triggered, by the order's own semantics."""
    stop, target = levels.get(Level.STOP_LOSS), levels.get(Level.TAKE_PROFIT)
    if target is None:
        return Level.STOP_LOSS, []
    if stop is None:
        return Level.TAKE_PROFIT, []
    long = side == "BUY"
    # TODO(verify): that a take-profit never fills worse than its price; the rule rests on it.
    hit_target = fill_price >= target if long else fill_price <= target
    level = Level.TAKE_PROFIT if hit_target else Level.STOP_LOSS
    inverted = stop >= target if long else stop <= target
    if not inverted:
        return level, []
    return level, [
        Notice(
            f"a protective order with inverted levels (stop-loss {stop}, take-profit {target}) "
            f"filled at {fill_price}; read as the {level.name.lower().replace('_', '-')}",
        ),
    ]


class VenueBook:
    """What the broker holds for one account, and what each of its events means to Nautilus.

    `price_precision` gives a symbol's price precision, or `None` for a symbol the node has not
    loaded: activity there becomes an `Activity`, never an order record.
    """

    def __init__(self, price_precision: Callable[[int], int | None]) -> None:
        self._precision = price_precision
        self._positions: dict[int, _Position] = {}
        # Broker orders Nautilus knows besides the node's entries: its closes, by broker id, and
        # external orders reported once.
        self._closes: dict[int, str] = {}
        self._closes_accepted: set[int] = set()
        self._reported: set[int] = set()
        self._seen: set[tuple] = set()

    def view(self, position_id: int) -> PositionView | None:
        position = self._positions.get(position_id)
        if position is None:
            return None
        return PositionView(
            position_id=position.position_id,
            symbol_id=position.symbol_id,
            side=position.side,
            units=units_of(position.volume),
            open=position.open,
            ours=position.ours,
            entry_order_id=position.entry_order_id,
            entry_client_order_id=position.entry_client_order_id,
            protective_order_id=position.protective_order_id,
            levels=dict(position.levels),
            legs={level: (leg.client_order_id, leg.alive) for level, leg in position.legs.items()},
        )

    def load(
        self,
        snapshot: oa.ProtoOAReconcileRes,
        position_orders: Mapping[int, Sequence[om.ProtoOAOrder]],
    ) -> list[Record]:
        """Rebuild the model from a snapshot asked with protection orders.

        `position_orders` holds each open position's own order list. Reporting the rebuilt state
        to Nautilus is reconciliation's job; this returns only notices.
        """
        self._positions = {}
        self._reported = set()
        notices: list[Record] = []
        for venue_position in snapshot.position:
            position = self._new_position(
                venue_position.positionId,
                venue_position.tradeData.symbolId,
                venue_position.tradeData.tradeSide,
            )
            position.open = True
            position.volume = venue_position.tradeData.volume
            precision = self._precision(position.symbol_id)
            if precision is not None:
                if venue_position.HasField("stopLoss"):
                    position.levels[Level.STOP_LOSS] = price_of(venue_position.stopLoss, precision)
                if venue_position.HasField("takeProfit"):
                    position.levels[Level.TAKE_PROFIT] = price_of(
                        venue_position.takeProfit, precision
                    )
            entry = _entry_of(position_orders.get(position.position_id, ()))
            if entry is not None:
                notices += self._adopt(position, entry, restored=True)
        for order in snapshot.order:
            if order.orderType == om.STOP_LOSS_TAKE_PROFIT:
                position = self._positions.get(order.positionId)
                if position is not None:
                    position.protective_order_id = order.orderId
                    position.protective_volume = order.tradeData.volume
                    for leg in position.legs.values():
                        if leg.accepted:
                            leg.quantity = order.tradeData.volume
            else:
                # Reconciliation reports every other open order, so Nautilus knows it from then on.
                self._reported.add(order.orderId)
        return notices

    def apply(self, event: oa.ProtoOAExecutionEvent, operations: Operations) -> list[Record]:
        """What `event` means to Nautilus; an event seen before means nothing new."""
        if not event.HasField("order"):
            # Swap and cash-flow events change the account, not an order.
            return []
        key = self._key(event)
        if key in self._seen:
            return []
        records = self._handle(event, operations)
        # Marked only once handled, so an event whose handling raised can be applied again.
        self._seen.add(key)
        return records

    @staticmethod
    def _key(event: oa.ProtoOAExecutionEvent) -> tuple:
        order = event.order
        if event.HasField("deal"):
            return (order.orderId, event.executionType, event.deal.dealId)
        # A level change has no deal, and two can share a millisecond: the content tells them
        # apart from a repeat.
        return (
            order.orderId,
            event.executionType,
            order.utcLastUpdateTimestamp,
            order.stopPrice if order.HasField("stopPrice") else None,
            order.limitPrice if order.HasField("limitPrice") else None,
            order.tradeData.volume if order.HasField("tradeData") else None,
        )

    def _handle(self, event: oa.ProtoOAExecutionEvent, operations: Operations) -> list[Record]:
        order = event.order
        precision = self._precision(order.tradeData.symbolId)
        if precision is None:
            return self._unloaded(event)
        if order.orderType == om.STOP_LOSS_TAKE_PROFIT:
            return self._protective(event, operations, precision)
        if order.closingOrder:
            return self._closing(event, operations, precision)
        return self._opening(event, precision)

    def reject_entry(
        self,
        client_order_id: str,
        legs: LegIds,
        reason: str,
        ts_ms: int,
    ) -> list[Record]:
        """An entry the broker refused before giving it an id; its legs go with it."""
        records: list[Record] = [
            OrderEvent(OrderEventKind.REJECTED, None, client_order_id, ts_ms, reason=reason),
        ]
        for leg_id in (legs.stop_loss, legs.take_profit):
            if leg_id is not None:
                records.append(OrderEvent(OrderEventKind.CANCELED, None, leg_id, ts_ms))
        return records

    def protection_timed_out(self, position_id: int) -> list[Record]:
        """Called when the wait for a filled entry's protective order ends.

        Reports the live legs the broker holds no level for: all of them without a protective
        order, and those never accepted with one.
        """
        position = self._positions.get(position_id)
        if position is None or not position.ours or not position.open:
            return []
        unprotected = position.protective_order_id is None
        missing = {
            level: leg.client_order_id
            for level, leg in position.legs.items()
            if leg.alive and (unprotected or not leg.accepted)
        }
        if not missing:
            return []
        return [
            ProtectionMissing(
                position_id, missing.get(Level.STOP_LOSS), missing.get(Level.TAKE_PROFIT)
            ),
        ]

    # Positions

    def _new_position(self, position_id: int, symbol_id: int, side: int) -> _Position:
        position = _Position(position_id, symbol_id, _SIDE[side])
        self._positions[position_id] = position
        return position

    def _position_for(self, event: oa.ProtoOAExecutionEvent) -> _Position:
        """The event's position, created from the event if the model never saw it.

        An order without a `positionId` gets a position of its own that is not kept.
        """
        order = event.order
        # TODO(verify): whether a rejected order carries a positionId; none was recorded.
        kept = order.HasField("positionId")
        if kept and order.positionId in self._positions:
            return self._positions[order.positionId]
        if event.HasField("position"):
            symbol_id, side = event.position.tradeData.symbolId, event.position.tradeData.tradeSide
        else:
            symbol_id, side = order.tradeData.symbolId, order.tradeData.tradeSide
            if order.closingOrder:
                side = _opposite(side)
        if kept:
            return self._new_position(order.positionId, symbol_id, side)
        return _Position(order.positionId, symbol_id, _SIDE[side])

    @staticmethod
    def _sync(position: _Position, event: oa.ProtoOAExecutionEvent) -> None:
        if event.HasField("position"):
            position.volume = event.position.tradeData.volume
            position.open = event.position.positionStatus == om.POSITION_STATUS_OPEN
        elif (
            event.order.closingOrder
            and event.executionType in _FILLS
            and event.HasField("deal")
            and position.open
        ):
            # TODO(verify): whether a closing fill always carries the position; until then the
            # volumes tell a full close.
            position.volume = max(position.volume - event.deal.filledVolume, 0)
            position.open = position.volume > 0

    def _adopt(
        self, position: _Position, entry: om.ProtoOAOrder, *, restored: bool
    ) -> list[Record]:
        """Make `position` the node's if its entry carries the node's record."""
        entry_id = order_record.parse_label(entry.tradeData.label)
        if entry_id is None:
            return []
        position.entry_order_id = entry.orderId
        position.entry_client_order_id = entry_id
        position.entry_accepted = restored
        legs = order_record.parse_comment(entry.tradeData.comment)
        if legs is None:
            return [
                Notice(
                    f"position {position.position_id} is the node's own, but its legs' record "
                    "cannot be read; its levels stay at the broker without legs",
                ),
            ]
        for level, client_order_id in zip(_LEVELS, (legs.stop_loss, legs.take_profit), strict=True):
            if client_order_id is not None:
                # After a restart a leg lives exactly while its level does.
                alive = level in position.levels if restored else True
                position.legs[level] = _Leg(
                    client_order_id,
                    alive=alive,
                    accepted=restored and alive,
                    quantity=position.volume if restored and alive else 0,
                )
        return []

    # Records

    def _fill(self, deal: om.ProtoOADeal, precision: int) -> Fill:
        return Fill(
            trade_id=str(deal.dealId),
            venue_position_id=str(deal.positionId),
            side=_SIDE[deal.tradeSide],
            units=units_of(deal.filledVolume),
            price=price_of(deal.executionPrice, precision),
            commission=money_of(deal.commission, deal.moneyDigits),
            ts_ms=deal.executionTimestamp,
        )

    @staticmethod
    def _leg_id(position: _Position, level: Level) -> str | None:
        if position.entry_order_id is None:
            return None
        return leg_venue_order_id(position.entry_order_id, level)

    def _leg_event(
        self,
        kind: OrderEventKind,
        position: _Position,
        level: Level,
        ts_ms: int,
        *,
        quantity: Decimal | None = None,
        price: Decimal | None = None,
        fill: Fill | None = None,
    ) -> OrderEvent:
        stop = level == Level.STOP_LOSS
        return OrderEvent(
            kind,
            self._leg_id(position, level),
            position.legs[level].client_order_id,
            ts_ms,
            fill=fill,
            quantity=quantity,
            price=None if stop else price,
            trigger_price=price if stop else None,
        )

    def _cancel_legs(self, position: _Position, ts_ms: int) -> list[Record]:
        records: list[Record] = []
        for level, leg in position.legs.items():
            if leg.alive:
                leg.alive = False
                records.append(self._leg_event(OrderEventKind.CANCELED, position, level, ts_ms))
        return records

    def _closed_by(self, position: _Position, deal: om.ProtoOADeal) -> list[Record]:
        """What is left of a position a deal has closed."""
        position.protective_order_id = None
        position.levels = {}
        # The deal is what ended the legs, so their cancels take its time, as reconciliation's do.
        return self._cancel_legs(position, deal.executionTimestamp)

    @staticmethod
    def _activity(
        kind: ActivityKind,
        position: _Position,
        action: Action,
        ts_ms: int,
        units: Decimal | None = None,
    ) -> Activity:
        return Activity(
            kind,
            position.symbol_id,
            "position",
            position.side,
            units_of(position.volume) if units is None else units,
            action,
            ts_ms,
        )

    def _external_report(
        self,
        order: om.ProtoOAOrder,
        precision: int,
        *,
        reduce_only: bool,
        fills: tuple[Fill, ...] = (),
        order_type: ExternalType | None = None,
        levels: dict[Level, Decimal] | None = None,
    ) -> ExternalOrder:
        """`order` as an external order; `levels` stands in for its own prices if given.

        A stop price is read from `STOP_LOSS` and a limit price from `TAKE_PROFIT`, as
        `_levels_of` reads `stopPrice` and `limitPrice`.
        """
        kind = order_type or _EXTERNAL_TYPE.get(order.orderType, ExternalType.MARKET)
        prices = _levels_of(order, precision) if levels is None else levels
        priced = kind in (ExternalType.LIMIT, ExternalType.STOP_LIMIT)
        triggered = kind in (ExternalType.STOP_MARKET, ExternalType.STOP_LIMIT)
        self._reported.add(order.orderId)
        return ExternalOrder(
            venue_order_id=str(order.orderId),
            symbol_id=order.tradeData.symbolId,
            side=_SIDE[order.tradeData.tradeSide],
            order_type=kind,
            units=units_of(order.tradeData.volume),
            reduce_only=reduce_only,
            venue_position_id=str(order.positionId) if order.HasField("positionId") else None,
            ts_ms=order.utcLastUpdateTimestamp,
            price=prices.get(Level.TAKE_PROFIT) if priced else None,
            trigger_price=prices.get(Level.STOP_LOSS) if triggered else None,
            fills=fills,
        )

    def _external_event(
        self,
        event: oa.ProtoOAExecutionEvent,
        precision: int,
        *,
        reduce_only: bool,
    ) -> list[Record]:
        """An order Nautilus learns of only from reports: reported once, then its events."""
        order, kind = event.order, event.executionType
        venue_order_id, ts = str(order.orderId), order.utcLastUpdateTimestamp
        known = order.orderId in self._reported
        if kind == om.ORDER_ACCEPTED:
            return (
                [] if known else [self._external_report(order, precision, reduce_only=reduce_only)]
            )
        if kind in _FILLS:
            fill = self._fill(event.deal, precision)
            if known:
                return [
                    OrderEvent(OrderEventKind.FILLED, venue_order_id, None, fill.ts_ms, fill=fill)
                ]
            return [self._external_report(order, precision, reduce_only=reduce_only, fills=(fill,))]
        if not known:
            return []
        if kind == om.ORDER_REPLACED:
            report = self._external_report(order, precision, reduce_only=reduce_only)
            return [
                OrderEvent(
                    OrderEventKind.UPDATED,
                    venue_order_id,
                    None,
                    ts,
                    quantity=report.units,
                    price=report.price,
                    trigger_price=report.trigger_price,
                ),
            ]
        if kind in _ENDED:
            return [
                OrderEvent(_ENDED[kind], venue_order_id, None, ts, reason=event.errorCode or None),
            ]
        return []

    # Event kinds

    def _opening(self, event: oa.ProtoOAExecutionEvent, precision: int) -> list[Record]:
        order = event.order
        position = self._position_for(event)
        records: list[Record] = []
        if position.entry_order_id is None:
            records += self._adopt(position, order, restored=False)
        self._sync(position, event)
        if position.ours and position.entry_order_id == order.orderId:
            return records + self._entry_event(event, position, precision)
        return records + self._external_event(event, precision, reduce_only=False)

    def _entry_event(
        self,
        event: oa.ProtoOAExecutionEvent,
        position: _Position,
        precision: int,
    ) -> list[Record]:
        order, kind = event.order, event.executionType
        venue_order_id, entry_id = str(order.orderId), position.entry_client_order_id
        ts = order.utcLastUpdateTimestamp
        if kind == om.ORDER_ACCEPTED:
            # A repeat past the dedupe key, or a late one after the fill, says nothing new.
            if position.entry_accepted:
                return []
            position.entry_accepted = True
            return [OrderEvent(OrderEventKind.ACCEPTED, venue_order_id, entry_id, ts)]
        if kind in _FILLS:
            position.entry_accepted = True
            fill = self._fill(event.deal, precision)
            records: list[Record] = [
                OrderEvent(OrderEventKind.FILLED, venue_order_id, entry_id, fill.ts_ms, fill=fill),
            ]
            waiting = position.legs and position.protective_order_id is None
            if waiting and not position.awaiting_protection:
                position.awaiting_protection = True
                records.append(AwaitProtection(position.position_id))
            return records
        if kind in _ENDED:
            ended = _ENDED[kind]
            records = [
                OrderEvent(ended, venue_order_id, entry_id, ts, reason=event.errorCode or None),
            ]
            # A remainder cancelled after a fill leaves the position, and its legs, standing.
            if ended == OrderEventKind.REJECTED or position.volume == 0:
                records += self._cancel_legs(position, ts)
            return records
        return []

    def _protective(
        self,
        event: oa.ProtoOAExecutionEvent,
        operations: Operations,
        precision: int,
    ) -> list[Record]:
        order, kind = event.order, event.executionType
        position = self._position_for(event)
        if kind in _FILLS:
            return self._triggered(event, position, precision)
        current = position.protective_order_id
        if kind != om.ORDER_ACCEPTED and current is not None and order.orderId != current:
            # A late event of a protective order the broker has since replaced with a new one.
            return []
        old_levels = dict(position.levels)
        if kind in (om.ORDER_ACCEPTED, om.ORDER_REPLACED):
            position.protective_order_id = order.orderId
            position.protective_volume = order.tradeData.volume
            position.levels = _levels_of(order, precision)
            position.awaiting_protection = False
        elif kind == om.ORDER_CANCELLED:
            position.protective_order_id = None
            position.levels = {}
        else:
            return []
        self._sync(position, event)
        if not position.ours:
            return []
        manual = not event.isServerEvent and not operations.amending(position.position_id)
        return self._level_changes(position, old_levels, manual, order.utcLastUpdateTimestamp)

    def _level_changes(
        self,
        position: _Position,
        old_levels: dict[Level, Decimal],
        manual: bool,
        ts: int,
    ) -> list[Record]:
        records: list[Record] = []
        for level in _LEVELS:
            old, new = old_levels.get(level), position.levels.get(level)
            leg = position.legs.get(level)
            alive = leg is not None and leg.alive
            if new is not None and alive and not leg.accepted:
                leg.accepted = True
                leg.quantity = leg.filled + position.protective_volume
                records.append(
                    self._leg_event(
                        OrderEventKind.ACCEPTED,
                        position,
                        level,
                        ts,
                        quantity=units_of(leg.quantity),
                        price=new,
                    ),
                )
                if manual:
                    action = Action.LEVEL_ADDED if old is None else Action.LEVEL_MOVED
                    records.append(self._activity(ActivityKind.MANUAL_CHANGE, position, action, ts))
            elif old == new:
                continue
            elif new is None:
                if alive:
                    leg.alive = False
                    records.append(self._leg_event(OrderEventKind.CANCELED, position, level, ts))
                if manual:
                    records.append(
                        self._activity(
                            ActivityKind.MANUAL_CHANGE, position, Action.LEVEL_REMOVED, ts
                        )
                    )
            elif old is None:
                # A level re-added after its leg was cancelled is not the node's.
                if manual:
                    records.append(
                        self._activity(ActivityKind.MANUAL_CHANGE, position, Action.LEVEL_ADDED, ts)
                    )
            else:
                if alive:
                    records.append(
                        self._leg_event(OrderEventKind.UPDATED, position, level, ts, price=new)
                    )
                if manual:
                    records.append(
                        self._activity(ActivityKind.MANUAL_CHANGE, position, Action.LEVEL_MOVED, ts)
                    )
        # The protective order follows the position's volume, and so do the legs; a leg already
        # partly filled keeps what it filled.
        for level, leg in position.legs.items():
            quantity = leg.filled + position.protective_volume
            if leg.alive and leg.accepted and leg.quantity != quantity:
                leg.quantity = quantity
                records.append(
                    self._leg_event(
                        OrderEventKind.UPDATED, position, level, ts, quantity=units_of(quantity)
                    ),
                )
        return records

    def _triggered(
        self,
        event: oa.ProtoOAExecutionEvent,
        position: _Position,
        precision: int,
    ) -> list[Record]:
        order, deal = event.order, event.deal
        fill = self._fill(deal, precision)
        level: Level | None = None
        levels = _levels_of(order, precision) or dict(position.levels)
        if levels:
            level, records = _which_level(position.side, levels, fill.price)
        else:
            records = [
                Notice(
                    f"a protective order filled at {fill.price} with no level known; "
                    "read as a market close",
                ),
            ]
        self._sync(position, event)
        # TODO(verify): whether a protective order can fill partially; none was recorded.
        position.protective_volume = max(position.protective_volume - deal.filledVolume, 0)
        leg = position.legs.get(level) if level is not None else None
        if position.ours and leg is not None and leg.alive:
            records.append(
                self._leg_event(OrderEventKind.FILLED, position, level, fill.ts_ms, fill=fill)
            )
            leg.filled += deal.filledVolume
            # A leg left with a remainder lives while the position does.
            if leg.filled >= leg.quantity:
                leg.alive = False
        elif order.orderId in self._reported:
            records.append(
                OrderEvent(OrderEventKind.FILLED, str(order.orderId), None, fill.ts_ms, fill=fill)
            )
        else:
            order_type = {
                None: ExternalType.MARKET,
                Level.STOP_LOSS: ExternalType.STOP_MARKET,
                Level.TAKE_PROFIT: ExternalType.LIMIT,
            }[level]
            records.append(
                self._external_report(
                    order,
                    precision,
                    reduce_only=True,
                    fills=(fill,),
                    order_type=order_type,
                    levels=levels,
                ),
            )
        if not position.open:
            records += self._closed_by(position, deal)
        return records

    def _closing(
        self,
        event: oa.ProtoOAExecutionEvent,
        operations: Operations,
        precision: int,
    ) -> list[Record]:
        order, kind = event.order, event.executionType
        position = self._position_for(event)
        self._sync(position, event)
        close_id = self._closes.get(order.orderId)
        # A close the broker made itself is never the node's, whatever the node has in flight.
        asked = not event.isServerEvent and not order.isStopOut
        if close_id is None and asked and order.orderId not in self._reported:
            # TODO(verify): that the broker's closing order carries the volume the node's close
            # asked for; the match rests on it.
            candidate = operations.closing(position.position_id, order.tradeData.volume)
            # One close id names one broker order; a second match is somebody else's close.
            if candidate is not None and candidate not in self._closes.values():
                close_id = self._closes[order.orderId] = candidate
        if close_id is not None:
            records = self._node_close(event, close_id, precision)
        else:
            records = self._external_event(event, precision, reduce_only=True)
            if kind in _FILLS:
                records += self._close_activity(event, position)
        if kind in _FILLS and not position.open:
            records += self._closed_by(position, event.deal)
        return records

    def _node_close(
        self,
        event: oa.ProtoOAExecutionEvent,
        close_id: str,
        precision: int,
    ) -> list[Record]:
        order, kind = event.order, event.executionType
        venue_order_id, ts = str(order.orderId), order.utcLastUpdateTimestamp
        if kind == om.ORDER_ACCEPTED:
            if order.orderId in self._closes_accepted:
                return []
            self._closes_accepted.add(order.orderId)
            return [OrderEvent(OrderEventKind.ACCEPTED, venue_order_id, close_id, ts)]
        if kind in _FILLS:
            self._closes_accepted.add(order.orderId)
            fill = self._fill(event.deal, precision)
            return [
                OrderEvent(OrderEventKind.FILLED, venue_order_id, close_id, fill.ts_ms, fill=fill)
            ]
        if kind in _ENDED:
            return [
                OrderEvent(
                    _ENDED[kind], venue_order_id, close_id, ts, reason=event.errorCode or None
                )
            ]
        return []

    def _close_activity(
        self,
        event: oa.ProtoOAExecutionEvent,
        position: _Position,
    ) -> list[Record]:
        units = units_of(event.deal.filledVolume)
        ts = event.deal.executionTimestamp
        action = Action.PARTIALLY_CLOSED if position.open else Action.CLOSED
        # TODO(verify): that a stop-out's closing order carries `isStopOut`; none was recorded.
        if event.order.isStopOut:
            return [self._activity(ActivityKind.STOP_OUT, position, action, ts, units)]
        if position.ours:
            return [self._activity(ActivityKind.MANUAL_CHANGE, position, action, ts, units)]
        return []

    def _unloaded(self, event: oa.ProtoOAExecutionEvent) -> list[Record]:
        """Anything on a symbol the node has not loaded: activity, never an order."""
        order, kind = event.order, event.executionType
        data = order.tradeData
        has_position = event.HasField("position")
        if has_position:
            position_side = _SIDE[event.position.tradeData.tradeSide]
        else:
            # A closing order trades against its position.
            position_side = _SIDE[
                _opposite(data.tradeSide) if order.closingOrder else data.tradeSide
            ]
        if kind in _FILLS:
            if order.closingOrder:
                open_after = (
                    has_position and event.position.positionStatus == om.POSITION_STATUS_OPEN
                )
                action = Action.PARTIALLY_CLOSED if open_after else Action.CLOSED
            else:
                action = Action.OPENED
            subject, side = "position", position_side
            units, ts = units_of(event.deal.filledVolume), event.deal.executionTimestamp
        elif order.orderType == om.STOP_LOSS_TAKE_PROFIT:
            if kind not in (om.ORDER_ACCEPTED, om.ORDER_REPLACED, om.ORDER_CANCELLED):
                return []
            subject, side, action = "position", position_side, Action.CHANGED
            volume = event.position.tradeData.volume if has_position else data.volume
            units, ts = units_of(volume), order.utcLastUpdateTimestamp
        elif kind in (om.ORDER_ACCEPTED, om.ORDER_REPLACED, om.ORDER_CANCELLED, om.ORDER_EXPIRED):
            action = {
                om.ORDER_ACCEPTED: Action.OPENED,
                om.ORDER_REPLACED: Action.CHANGED,
            }.get(kind, Action.CLOSED)
            subject, side = "order", _SIDE[data.tradeSide]
            units, ts = units_of(data.volume), order.utcLastUpdateTimestamp
        else:
            return []
        kind_of_activity = (
            ActivityKind.STOP_OUT if order.isStopOut else ActivityKind.UNLOADED_SYMBOL
        )
        return [Activity(kind_of_activity, data.symbolId, subject, side, units, action, ts)]
