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
from dataclasses import dataclass, field, replace
from decimal import Decimal

from nautilus_ctrader.common import order_record
from nautilus_ctrader.common.order_record import LegIds
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
    Operations,
    OrderEvent,
    OrderEventKind,
    ProtectionMissing,
    Record,
    leg_venue_order_id,
    money_of,
    parse_leg_venue_order_id,
    price_of,
    units_of,
)
from nautilus_ctrader.constants import PENDING_ORDER_TYPES
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as om

_SIDE = {om.BUY: "BUY", om.SELL: "SELL"}
EXTERNAL_TYPE = {
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
class _ForeignLeg:
    """A level of a position the node did not open, as the external order Nautilus holds.

    `quantity` and `filled` are venue volumes.
    """

    venue_order_id: str
    generation: int
    quantity: int
    alive: bool = True
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
    # A foreign position's current or last leg of each level.
    foreign_legs: dict[Level, _ForeignLeg] = field(default_factory=dict)
    entry_unknown_told: bool = False
    awaiting_protection: bool = False
    protective_order_id: int | None = None
    protective_volume: int = 0
    protective_opened_ms: int | None = None
    # When the entry first filled, if the model saw it.
    entry_filled_ms: int | None = None
    # Protective orders replaced by a new id or cancelled: any later event of theirs is stale.
    retired_protective_ids: set[int] = field(default_factory=set)
    levels: dict[Level, Decimal] = field(default_factory=dict)
    # `None` until a state of the position is seen.
    terms: LevelTerms | None = None
    # The `utcLastUpdateTimestamp` of the position state last applied; a created position has
    # none and counts as 0.
    updated_ms: int = -1

    @property
    def ours(self) -> bool:
        return self.entry_client_order_id is not None


@dataclass(frozen=True)
class PositionView:
    """A read-only copy of what the model holds for one position.

    - `legs`: the node's legs, by client order id and whether each is alive.
    - `leg_units`: the quantity Nautilus has for each accepted live leg, the node's or foreign.
    - `foreign_legs`: the venue order id of each live leg of a position the node did not open.
    - `terms`: how the levels work, from the last position state seen; `None` before any.
    """

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
    leg_units: dict[Level, Decimal]
    foreign_legs: dict[Level, str] = field(default_factory=dict)
    terms: LevelTerms | None = None


def entry_of(orders: Sequence[om.ProtoOAOrder]) -> om.ProtoOAOrder | None:
    """The entry among a position's orders."""
    # The protective order and closing orders carry the entry's client order id too.
    for order in orders:
        if not order.closingOrder and order.orderType != om.STOP_LOSS_TAKE_PROFIT:
            return order
    return None


def created_of(order: om.ProtoOAOrder) -> int:
    """When the broker created `order`, in ms; its last change if it carries no creation time."""
    # TODO(verify): that a closing order's `openTimestamp` is on the same clock as the broker's
    # other timestamps; every recorded closing order carries it.
    if order.tradeData.HasField("openTimestamp"):
        return order.tradeData.openTimestamp
    return order.utcLastUpdateTimestamp


def opened_of(order: om.ProtoOAOrder) -> int | None:
    """When the broker created `order`, in ms, if it says."""
    data = order.tradeData
    return data.openTimestamp if data.HasField("openTimestamp") else None


def _opposite(side: int) -> int:
    return om.SELL if side == om.BUY else om.BUY


def levels_of(order: om.ProtoOAOrder, precision: int) -> dict[Level, Decimal]:
    """The levels a protective order holds: its stop price and its limit price."""
    levels: dict[Level, Decimal] = {}
    if order.HasField("stopPrice"):
        levels[Level.STOP_LOSS] = price_of(order.stopPrice, precision)
    if order.HasField("limitPrice"):
        levels[Level.TAKE_PROFIT] = price_of(order.limitPrice, precision)
    return levels


def terms_of(position: om.ProtoOAPosition) -> LevelTerms:
    """How `position`'s levels work; a field the venue leaves out is the schema's default."""
    return LevelTerms(
        trailing_stop_loss=position.trailingStopLoss,
        guaranteed_stop_loss=position.guaranteedStopLoss,
        stop_loss_trigger_method=position.stopLossTriggerMethod,
    )


def _copied(order: om.ProtoOAOrder) -> om.ProtoOAOrder:
    copy = om.ProtoOAOrder()
    copy.CopyFrom(order)
    return copy


def remaining_of(order: om.ProtoOAOrder) -> int:
    """What is left of an order, as a venue volume."""
    # TODO(verify): whether the broker reports a partly filled protective order's total volume or
    # its rest; none was recorded. A replace after a partial trigger would settle it.
    executed = order.executedVolume if order.HasField("executedVolume") else 0
    return max(order.tradeData.volume - executed, 0)


def foreign_leg(
    venue_order_id: str,
    level: Level,
    *,
    symbol_id: int,
    position_id: int,
    position_side: str,
    price: Decimal,
    volume: int,
    ts_ms: int,
    accepted_ms: int,
) -> ExternalOrder:
    """A level of a position the node did not open, as the external order Nautilus learns.

    A stop-loss is a reduce-only `STOP_MARKET` at its `trigger_price`, a take-profit a reduce-only
    `LIMIT` at its `price`, both against the position, good till cancelled. `volume` is a venue
    volume; `accepted_ms` is when the level was set, never before the entry filled.
    """
    stop = level == Level.STOP_LOSS
    return ExternalOrder(
        venue_order_id=venue_order_id,
        symbol_id=symbol_id,
        side="SELL" if position_side == "BUY" else "BUY",
        order_type=ExternalType.STOP_MARKET if stop else ExternalType.LIMIT,
        units=units_of(volume),
        reduce_only=True,
        venue_position_id=str(position_id),
        ts_ms=ts_ms,
        price=None if stop else price,
        trigger_price=price if stop else None,
        time_in_force="GOOD_TILL_CANCEL",
        ts_accepted_ms=accepted_ms,
    )


def which_level(
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

    - `price_precision` gives a symbol's price precision, or `None` for a symbol the node has not
      loaded: activity there becomes an `Activity`, never an order record.
    - `held_closed` says whether Nautilus holds a venue order id as a closed order. A leg of a
      position the node did not open takes the first generation of its id that Nautilus does not
      hold closed, and that this model has not ended since it was last loaded.
    """

    def __init__(
        self,
        price_precision: Callable[[int], int | None],
        held_closed: Callable[[str], bool] = lambda _venue_order_id: False,
    ) -> None:
        self._precision = price_precision
        self._held_closed = held_closed
        self._positions: dict[int, _Position] = {}
        # Broker orders Nautilus knows besides the node's entries: its closes, by broker id, and
        # external orders reported once.
        self._closes: dict[int, str] = {}
        self._closes_accepted: set[int] = set()
        # The position each matched close belongs to, by broker order id.
        self._close_positions: dict[int, int] = {}
        # Pending orders on symbols the node has not loaded, by broker order id.
        self._unloaded_orders: dict[int, Exposure] = {}
        # Never-opened entries that ended, dropped from `_positions`.
        self._ended_entries: set[int] = set()
        self._reported: set[int] = set()
        # The last known state of each pending order among them while open, by broker order id:
        # an amend sends it again.
        self._open_orders: dict[int, om.ProtoOAOrder] = {}
        self._seen: set[tuple] = set()
        self._synced_deals: set[int] = set()

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
            leg_units={
                **{
                    level: units_of(leg.quantity)
                    for level, leg in position.legs.items()
                    if leg.alive and leg.accepted
                },
                **{
                    level: units_of(leg.quantity)
                    for level, leg in position.foreign_legs.items()
                    if leg.alive
                },
            },
            foreign_legs={
                level: leg.venue_order_id
                for level, leg in position.foreign_legs.items()
                if leg.alive
            },
            terms=position.terms,
        )

    def exposure(self) -> tuple[Exposure, ...]:
        """What stands on symbols the node has not loaded, sorted."""
        positions = (
            Exposure(position.symbol_id, "position", position.side, units_of(position.volume))
            for position in self._positions.values()
            if position.open and self._precision(position.symbol_id) is None
        )
        return tuple(sorted((*positions, *self._unloaded_orders.values())))

    def updated_ms(self, position_id: int) -> int:
        """The broker's time of the last position state applied, or -1 for none."""
        position = self._positions.get(position_id)
        return -1 if position is None else position.updated_ms

    def known_closes(self) -> dict[int, str]:
        """The node's closes the model has matched: broker order id to the node's close id."""
        return dict(self._closes)

    def close_order(self, client_order_id: str) -> tuple[int, int] | None:
        """The broker order id and position id of the node's matched close `client_order_id`."""
        for order_id, close_id in self._closes.items():
            if close_id == client_order_id:
                return order_id, self._close_positions[order_id]
        return None

    def match_close(self, order_id: int, position_id: int, close_id: str) -> None:
        """Take broker order `order_id` for the node's close `close_id`, as a query found it.

        Its later events are then the node's close's, even once the close is no longer in
        flight. Nothing changes when either is already matched.
        """
        if order_id in self._closes or close_id in self._closes.values():
            return
        self._closes[order_id] = close_id
        self._close_positions[order_id] = position_id
        # Listed by the broker, so accepted: a late acceptance adds nothing.
        self._closes_accepted.add(order_id)

    def load(
        self,
        snapshot: oa.ProtoOAReconcileRes,
        position_orders: Mapping[int, Sequence[om.ProtoOAOrder]],
    ) -> list[Record]:
        """Rebuild the model from a snapshot asked with protection orders.

        `position_orders` holds each open position's own order list. Reporting the rebuilt state
        to Nautilus is reconciliation's job; this returns only notices, and an `EntryUnknown` for
        each foreign position with a level whose order list names no entry.
        """
        self._positions = {}
        self._unloaded_orders = {}
        self._reported = set()
        self._open_orders = {}
        notices: list[Record] = []
        for venue_position in snapshot.position:
            position = self._new_position(
                venue_position.positionId,
                venue_position.tradeData.symbolId,
                venue_position.tradeData.tradeSide,
            )
            position.open = True
            position.volume = venue_position.tradeData.volume
            position.updated_ms = venue_position.utcLastUpdateTimestamp
            position.terms = terms_of(venue_position)
            precision = self._precision(position.symbol_id)
            if precision is not None:
                if venue_position.HasField("stopLoss"):
                    position.levels[Level.STOP_LOSS] = price_of(venue_position.stopLoss, precision)
                if venue_position.HasField("takeProfit"):
                    position.levels[Level.TAKE_PROFIT] = price_of(
                        venue_position.takeProfit, precision
                    )
            entry = entry_of(position_orders.get(position.position_id, ()))
            if entry is not None:
                notices += self._adopt(position, entry, restored=True)
        for order in snapshot.order:
            if order.orderType == om.STOP_LOSS_TAKE_PROFIT:
                position = self._positions.get(order.positionId)
                if position is not None:
                    position.protective_order_id = order.orderId
                    position.protective_volume = remaining_of(order)
                    position.protective_opened_ms = opened_of(order)
                    for leg in position.legs.values():
                        if leg.accepted:
                            leg.quantity = position.protective_volume
            else:
                # Reconciliation reports every other open order, so Nautilus knows it from then on.
                self._reported.add(order.orderId)
                if self._precision(order.tradeData.symbolId) is None:
                    self._unloaded_orders[order.orderId] = self._pending(order)
                elif order.orderType in PENDING_ORDER_TYPES:
                    self._open_orders[order.orderId] = _copied(order)
        for position in self._positions.values():
            if not position.ours and position.levels:
                # Reconciliation reports the standing levels' legs, at the generation chosen here
                # from the same cache, so this says nothing of them.
                notices += [
                    record
                    for record in self._foreign_levels(position, {}, position.updated_ms)
                    if isinstance(record, EntryUnknown)
                ]
        return notices

    def apply(self, event: oa.ProtoOAExecutionEvent, operations: Operations) -> list[Record]:
        """What `event` means to Nautilus; an event seen before means nothing new.

        Account state the event carries (`usedMargin`, `closePositionDetail.balance`) is not in
        the records: the execution client reads it from the event it passed in.
        """
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

    def trailing_stop_moved(self, event: oa.ProtoOATrailingSLChangedEvent) -> list[Record]:
        """A trailing stop-loss the broker moved: the stop-loss leg follows it.

        An event seen before, or one of a position or protective order the model does not hold,
        means nothing.
        """
        # TODO(verify): whether a trailing move also arrives as an execution event; none was
        # recorded. A second one finds the level already there and says nothing.
        position = self._positions.get(event.positionId)
        precision = None if position is None else self._precision(position.symbol_id)
        key = ("trailing", event.orderId, event.utcLastUpdateTimestamp, event.stopPrice)
        if precision is None or key in self._seen:
            return []
        # A retired id is never current.
        if event.orderId != position.protective_order_id:
            return []
        old_levels = dict(position.levels)
        position.levels[Level.STOP_LOSS] = price_of(event.stopPrice, precision)
        ts = event.utcLastUpdateTimestamp
        if position.ours:
            # The broker's own move: no trader's change.
            records = self._level_changes(position, old_levels, False, ts)
        else:
            records = self._foreign_levels(position, old_levels, ts)
        self._seen.add(key)
        return records

    def entry_found(self, position_id: int, orders: Sequence[om.ProtoOAOrder]) -> list[Record]:
        """The order list of a position whose entry the model did not know, read afterwards.

        A foreign position's standing levels get their legs. The node's own entry is taken as
        its opening event would take it, and its legs wait for that entry's events. Nothing
        changes for a position the model does not hold, one whose entry it knows, or a list
        that names no entry.
        """
        position = self._positions.get(position_id)
        entry = entry_of(orders)
        if position is None or position.entry_order_id is not None or entry is None:
            return []
        records = self._adopt(position, entry, restored=False)
        if not position.ours:
            records += self._foreign_levels(position, {}, entry.utcLastUpdateTimestamp)
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
        # TODO(verify): whether a stop-out's order carries `closingOrder`; none was recorded.
        # `_opening` ends the legs of a position its fill closed, so they do not hang on it.
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

    def leg_position(self, client_order_id: str) -> tuple[int, Level] | None:
        """The position and level of the node's leg `client_order_id`, alive or not."""
        for position in self._positions.values():
            for level, leg in position.legs.items():
                if leg.client_order_id == client_order_id:
                    return position.position_id, level
        return None

    def foreign_leg_position(self, venue_order_id: str) -> tuple[int, Level] | None:
        """The position and level of a leg of a position the node did not open, by venue order id.

        Any generation of the leg's id is found, alive or not: whether it is the live one is
        `view().foreign_legs`' to say.
        """
        parsed = parse_leg_venue_order_id(venue_order_id)
        if parsed is None:
            return None
        entry_order_id, level, _generation = parsed
        position_id = self.entry_position(entry_order_id)
        if position_id is None or self._positions[position_id].ours:
            return None
        return position_id, level

    def open_order(self, order_id: int) -> om.ProtoOAOrder | None:
        """The last known state of an open pending order Nautilus knows from reports."""
        order = self._open_orders.get(order_id)
        return None if order is None else _copied(order)

    def standing_order(self, order_id: int, ts_ms: int) -> list[Record]:
        """An open pending order Nautilus knows from reports, as held; nothing if not held."""
        order = self._open_orders.get(order_id)
        precision = None if order is None else self._precision(order.tradeData.symbolId)
        if precision is None:
            return []
        return [self._order_updated(order, precision, ts_ms)]

    def entry_position(self, entry_order_id: int) -> int | None:
        """The position broker order `entry_order_id` opened, open or closed since the last load."""
        for position in self._positions.values():
            if position.entry_order_id == entry_order_id:
                return position.position_id
        return None

    def cancel_leg(self, position_id: int, level: Level, ts_ms: int) -> list[Record]:
        """The node cancels a live leg whose level the broker does not hold: no request is needed.

        Says nothing for a leg already ended, or one whose level stands: that takes an amend.
        """
        position = self._positions.get(position_id)
        leg = None if position is None else position.legs.get(level)
        if leg is None or not leg.alive or level in position.levels:
            return []
        leg.alive = False
        return [self._leg_event(OrderEventKind.CANCELED, position, level, ts_ms)]

    def reject_legs(self, position_id: int, reason: str, ts_ms: int) -> list[Record]:
        """The broker refused the levels of the live legs it never accepted; they are rejected."""
        position = self._positions.get(position_id)
        if position is None:
            return []
        records: list[Record] = []
        for level, leg in position.legs.items():
            if leg.alive and not leg.accepted:
                leg.alive = False
                records.append(
                    OrderEvent(
                        OrderEventKind.REJECTED,
                        self._leg_id(position, level),
                        leg.client_order_id,
                        ts_ms,
                        reason=reason,
                    ),
                )
        return records

    # Positions

    def _new_position(self, position_id: int, symbol_id: int, side: int) -> _Position:
        position = _Position(position_id, symbol_id, _SIDE[side])
        self._positions[position_id] = position
        return position

    def _position_for(self, event: oa.ProtoOAExecutionEvent) -> _Position:
        """The event's position, created from the event if the model never saw it.

        An order without a `positionId` gets a position of its own that is not kept, and the
        position of an entry that ended before it opened is dropped when the entry ends.
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

    def _sync(self, position: _Position, event: oa.ProtoOAExecutionEvent) -> None:
        if event.HasField("position"):
            updated = event.position.utcLastUpdateTimestamp
            # A response can be applied after a later event of its order: its older position
            # state must not undo the newer one.
            # TODO(verify): that a position's `utcLastUpdateTimestamp` grows in event order and
            # is set on every state but a created one; a recording of a reconnect, or of events
            # racing each other, would confirm both.
            if updated < position.updated_ms:
                return
            position.updated_ms = updated
            position.terms = terms_of(event.position)
            position.volume = event.position.tradeData.volume
            position.open = event.position.positionStatus == om.POSITION_STATUS_OPEN
        elif event.executionType in _FILLS and event.HasField("deal"):
            # An event whose handling raised is not marked seen and may come again; its deal must
            # not move the volume twice.
            if event.deal.dealId in self._synced_deals:
                return
            self._synced_deals.add(event.deal.dealId)
            # TODO(verify): whether a fill always carries the position; until then the deal's
            # volume moves the one known.
            filled = event.deal.filledVolume
            if not event.order.closingOrder:
                position.volume += filled
                position.open = True
            elif position.open:
                position.volume = max(position.volume - filled, 0)
                position.open = position.volume > 0

    def _adopt(
        self, position: _Position, entry: om.ProtoOAOrder, *, restored: bool
    ) -> list[Record]:
        """Take `entry` as `position`'s; the position is the node's if `entry` has its record."""
        position.entry_order_id = entry.orderId
        entry_id = order_record.parse_label(entry.tradeData.label)
        if entry_id is None:
            return []
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
        # A protective order's cancel that follows finds no live leg and no level, and so says
        # nothing.
        position.protective_order_id = None
        position.levels = {}
        # The deal is what ended the legs, so their cancels take its time, as reconciliation's do.
        ts = deal.executionTimestamp
        records = self._cancel_legs(position, ts)
        for leg in position.foreign_legs.values():
            if leg.alive:
                leg.alive = False
                records.append(OrderEvent(OrderEventKind.CANCELED, leg.venue_order_id, None, ts))
        return records

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
        `levels_of` reads `stopPrice` and `limitPrice`. Building the record marks nothing: the
        caller that reports it adds the id to `_reported`.
        """
        kind = order_type or EXTERNAL_TYPE.get(order.orderType, ExternalType.MARKET)
        prices = levels_of(order, precision) if levels is None else levels
        priced = kind in (ExternalType.LIMIT, ExternalType.STOP_LIMIT)
        triggered = kind in (ExternalType.STOP_MARKET, ExternalType.STOP_LIMIT)
        data = order.tradeData
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
            time_in_force=(
                om.ProtoOATimeInForce.Name(order.timeInForce)
                if order.HasField("timeInForce")
                else None
            ),
            expire_ts_ms=(
                order.expirationTimestamp if order.HasField("expirationTimestamp") else None
            ),
            ts_accepted_ms=data.openTimestamp if data.HasField("openTimestamp") else None,
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
        taken = self._keep_open_order(event, known)
        if kind == om.ORDER_ACCEPTED:
            if known:
                return []
            self._reported.add(order.orderId)
            return [self._external_report(order, precision, reduce_only=reduce_only)]
        if kind in _FILLS:
            fill = self._fill(event.deal, precision)
            if known:
                return [
                    OrderEvent(OrderEventKind.FILLED, venue_order_id, None, fill.ts_ms, fill=fill)
                ]
            self._reported.add(order.orderId)
            return [self._external_report(order, precision, reduce_only=reduce_only, fills=(fill,))]
        if not known:
            return []
        if kind == om.ORDER_REPLACED:
            # A pending order's state older than the one held, or of an order since ended,
            # would take Nautilus back.
            if order.orderType in PENDING_ORDER_TYPES and not taken:
                return []
            return [self._order_updated(order, precision, ts)]
        if kind in _ENDED:
            return [
                OrderEvent(_ENDED[kind], venue_order_id, None, ts, reason=event.errorCode or None),
            ]
        return []

    def _order_updated(self, order: om.ProtoOAOrder, precision: int, ts_ms: int) -> OrderEvent:
        report = self._external_report(order, precision, reduce_only=False)
        return OrderEvent(
            OrderEventKind.UPDATED,
            str(order.orderId),
            None,
            ts_ms,
            quantity=report.units,
            price=report.price,
            trigger_price=report.trigger_price,
        )

    def _keep_open_order(self, event: oa.ProtoOAExecutionEvent, known: bool) -> bool:
        """Keep the latest state of a pending order reported to Nautilus while it is open.

        Returns whether the event's state was taken as the latest.
        """
        order, kind = event.order, event.executionType
        if order.orderType not in PENDING_ORDER_TYPES:
            return False
        if kind == om.ORDER_FILLED or kind in _ENDED:
            self._open_orders.pop(order.orderId, None)
            return False
        if kind in (om.ORDER_ACCEPTED, om.ORDER_PARTIAL_FILL) and not known:
            self._open_orders[order.orderId] = _copied(order)
            return True
        if kind in (om.ORDER_REPLACED, om.ORDER_PARTIAL_FILL):
            held = self._open_orders.get(order.orderId)
            # An ended order is not held, so a late event never brings it back.
            if held is not None and order.utcLastUpdateTimestamp >= held.utcLastUpdateTimestamp:
                self._open_orders[order.orderId] = _copied(order)
                return True
        return False

    # Event kinds

    def _opening(self, event: oa.ProtoOAExecutionEvent, precision: int) -> list[Record]:
        order, kind = event.order, event.executionType
        filled = kind in _FILLS
        ended = order.orderId in self._ended_entries
        if ended and not filled:
            return []
        position = self._position_for(event)
        records: list[Record] = []
        learnt = position.entry_order_id is None
        if learnt:
            records += self._adopt(position, order, restored=False)
        if ended:
            # A fill is never dropped. Nautilus holds the entry and its legs ended: the fill is
            # legal after that, the legs are not live again.
            records.append(Notice(f"entry {order.orderId} filled after it ended"))
            position.entry_accepted = True
            for leg in position.legs.values():
                leg.alive = False
        was_open, volume_before = position.open, position.volume
        self._sync(position, event)
        if filled and not was_open and position.entry_order_id == order.orderId:
            position.entry_filled_ms = event.deal.executionTimestamp
        reduced = was_open and (not position.open or position.volume < volume_before)
        # A triggered pending order is a server event too; only one that took volume away is
        # a close.
        if filled and event.isServerEvent and not order.isStopOut and reduced:
            records.append(self._unnamed_close(position, event.deal))
        if position.ours and position.entry_order_id == order.orderId:
            records += self._entry_event(event, position, precision)
        else:
            records += self._external_event(event, precision, reduce_only=False)
            if learnt and position.levels:
                # Levels seen before their entry get their legs now, after the entry's report.
                records += self._foreign_levels(position, {}, order.utcLastUpdateTimestamp)
        if filled and order.isStopOut:
            action = Action.PARTIALLY_CLOSED if position.open else Action.CLOSED
            records.append(
                self._activity(
                    ActivityKind.STOP_OUT,
                    position,
                    action,
                    event.deal.executionTimestamp,
                    units_of(event.deal.filledVolume),
                )
            )
        if filled and (position.legs or position.foreign_legs) and not position.open:
            records += self._closed_by(position, event.deal)
        return records

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
            # Built first: a fill that raises must leave the acceptance to be told next time.
            fill = self._fill(event.deal, precision)
            records: list[Record] = []
            if not position.entry_accepted:
                # The acceptance can be applied after the fill, or never come: it is told here.
                position.entry_accepted = True
                records.append(OrderEvent(OrderEventKind.ACCEPTED, venue_order_id, entry_id, ts))
            records.append(
                OrderEvent(OrderEventKind.FILLED, venue_order_id, entry_id, fill.ts_ms, fill=fill),
            )
            if position.protective_order_id is not None:
                # The protective order came first: its levels accept the legs now, at the deal's
                # time so that they never predate the fill.
                records += self._level_changes(position, {}, False, fill.ts_ms)
            waiting = any(leg.alive and not leg.accepted for leg in position.legs.values())
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
            if position.volume == 0 and not position.open:
                self._ended_entries.add(order.orderId)
                if self._positions.get(position.position_id) is position:
                    del self._positions[position.position_id]
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
        retired = order.orderId in position.retired_protective_ids
        if kind in _FILLS:
            # A deal is never stale, whatever became of the order's id.
            notices: list[Record] = []
            if retired:
                notices.append(
                    Notice(
                        f"protective order {order.orderId} filled after it was replaced or "
                        "cancelled; the fill is reported all the same",
                    )
                )
            return notices + self._triggered(event, position, precision)
        # TODO(verify): that a replaced or cancelled protective id has no later live event but a
        # fill, and whether the broker ever replaces the id at all. If it does, an out-of-order
        # ACCEPTED of an id never current leaves stale levels until the next event.
        if retired:
            return []
        current = position.protective_order_id
        if kind != om.ORDER_ACCEPTED and current is not None and order.orderId != current:
            # A late event of a protective order the broker has since replaced with a new one.
            return []
        old_levels = dict(position.levels)
        if kind in (om.ORDER_ACCEPTED, om.ORDER_REPLACED):
            if current is not None and order.orderId != current:
                position.retired_protective_ids.add(current)
            position.protective_order_id = order.orderId
            position.protective_volume = remaining_of(order)
            position.protective_opened_ms = opened_of(order)
            position.levels = levels_of(order, precision)
            position.awaiting_protection = False
        elif kind == om.ORDER_CANCELLED:
            position.retired_protective_ids.add(order.orderId)
            position.protective_order_id = None
            position.levels = {}
        else:
            return []
        self._sync(position, event)
        if not position.ours:
            return self._foreign_levels(position, old_levels, order.utcLastUpdateTimestamp)
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
                    leg.quantity = leg.filled + position.protective_volume
                    records.append(
                        self._leg_event(
                            OrderEventKind.UPDATED,
                            position,
                            level,
                            ts,
                            quantity=units_of(leg.quantity),
                            price=new,
                        )
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

    def _foreign_levels(
        self,
        position: _Position,
        old_levels: dict[Level, Decimal],
        ts: int,
    ) -> list[Record]:
        """A foreign position's level changes, as its legs: external orders reported once.

        A level seen with no live leg opens a new one; a moved level updates it, a removed one
        cancels it. Each live leg's quantity follows the protective order's volume.
        """
        if position.entry_order_id is None:
            if not position.levels or position.entry_unknown_told:
                return []
            position.entry_unknown_told = True
            return [EntryUnknown(position.position_id)]
        records: list[Record] = []
        for level in _LEVELS:
            old, new = old_levels.get(level), position.levels.get(level)
            leg = position.foreign_legs.get(level)
            live = leg is not None and leg.alive
            if new is not None and not live:
                records.append(self._new_foreign_leg(position, level, new, ts))
            elif new is None and live:
                leg.alive = False
                records.append(OrderEvent(OrderEventKind.CANCELED, leg.venue_order_id, None, ts))
            elif live and new != old:
                leg.quantity = leg.filled + self._foreign_rest(position)
                records.append(
                    self._foreign_event(
                        OrderEventKind.UPDATED, leg, level, ts, units_of(leg.quantity), new
                    )
                )
        for leg in position.foreign_legs.values():
            quantity = leg.filled + self._foreign_rest(position)
            if leg.alive and leg.quantity != quantity:
                leg.quantity = quantity
                records.append(
                    OrderEvent(
                        OrderEventKind.UPDATED,
                        leg.venue_order_id,
                        None,
                        ts,
                        quantity=units_of(quantity),
                    )
                )
        return records

    @staticmethod
    def _foreign_rest(position: _Position) -> int:
        """What a foreign leg covers besides its fills: the whole position's protection."""
        if position.protective_order_id is None:
            return position.volume
        return position.protective_volume

    def _new_foreign_leg(
        self, position: _Position, level: Level, price: Decimal, ts: int
    ) -> ExternalOrder:
        assert position.entry_order_id is not None
        last = position.foreign_legs.get(level)
        # A generation this model ended may not have reached Nautilus's cache yet.
        generation = 1 if last is None else last.generation + 1
        venue_order_id = leg_venue_order_id(position.entry_order_id, level, generation)
        while self._held_closed(venue_order_id):
            generation += 1
            venue_order_id = leg_venue_order_id(position.entry_order_id, level, generation)
        leg = _ForeignLeg(venue_order_id, generation, self._foreign_rest(position))
        position.foreign_legs[level] = leg
        accepted = self._leg_accepted_ms(position, ts)
        return foreign_leg(
            venue_order_id,
            level,
            symbol_id=position.symbol_id,
            position_id=position.position_id,
            position_side=position.side,
            price=price,
            volume=leg.quantity,
            ts_ms=max(ts, accepted),
            accepted_ms=accepted,
        )

    @staticmethod
    def _leg_accepted_ms(position: _Position, ts: int) -> int:
        """When a foreign leg was accepted: once its level was set, never before the entry filled.

        The same as reconciliation reads from the broker's lists, but where the model lacks what
        they hold: without the entry's fill, as for a position loaded open, the protective order's
        creation stands alone; without either, the event's time `ts`.
        """
        opened, filled = position.protective_opened_ms, position.entry_filled_ms
        if opened is None:
            return ts if filled is None else filled
        return opened if filled is None else max(opened, filled)

    @staticmethod
    def _foreign_event(
        kind: OrderEventKind,
        leg: _ForeignLeg,
        level: Level,
        ts: int,
        quantity: Decimal,
        price: Decimal,
    ) -> OrderEvent:
        stop = level == Level.STOP_LOSS
        return OrderEvent(
            kind,
            leg.venue_order_id,
            None,
            ts,
            quantity=quantity,
            price=None if stop else price,
            trigger_price=price if stop else None,
        )

    @staticmethod
    def _foreign_fill(leg: _ForeignLeg, volume: int, fill: Fill) -> list[Record]:
        """A foreign leg's fill; its quantity is raised to cover it first.

        The protective order's smaller volume after a partial close can come after the trigger,
        and a fill above the leg's quantity would be an overfill to Nautilus.
        """
        records: list[Record] = []
        if leg.filled + volume > leg.quantity:
            leg.quantity = leg.filled + volume
            records.append(
                OrderEvent(
                    OrderEventKind.UPDATED,
                    leg.venue_order_id,
                    None,
                    fill.ts_ms,
                    quantity=units_of(leg.quantity),
                )
            )
        records.append(
            OrderEvent(OrderEventKind.FILLED, leg.venue_order_id, None, fill.ts_ms, fill=fill)
        )
        leg.filled += volume
        # A leg left with a remainder lives while the position does.
        if leg.filled >= leg.quantity:
            leg.alive = False
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
        levels = levels_of(order, precision) or dict(position.levels)
        if levels:
            level, records = which_level(position.side, levels, fill.price)
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
        foreign = position.foreign_legs.get(level) if level is not None else None
        if position.ours and leg is not None and leg.alive:
            records.append(
                self._leg_event(OrderEventKind.FILLED, position, level, fill.ts_ms, fill=fill)
            )
            leg.filled += deal.filledVolume
            # A leg left with a remainder lives while the position does.
            if leg.filled >= leg.quantity:
                leg.alive = False
        elif not position.ours and foreign is not None and foreign.alive:
            records += self._foreign_fill(foreign, deal.filledVolume, fill)
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
            self._reported.add(order.orderId)
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
        # TODO(verify): that a stop-out is marked by `isServerEvent` or `isStopOut`; none was
        # recorded.
        asked = not event.isServerEvent and not order.isStopOut
        if close_id is None and asked and order.orderId not in self._reported:
            # TODO(verify): that the broker's closing order carries the volume the node's close
            # asked for; the match rests on it.
            candidate = operations.closing(
                position.position_id, order.tradeData.volume, created_of(order), order.orderId
            )
            # One close id names one broker order; a second match is somebody else's close.
            if candidate is not None and candidate not in self._closes.values():
                close_id = self._closes[order.orderId] = candidate
                self._close_positions[order.orderId] = position.position_id
        records: list[Record] = []
        if kind in _FILLS and event.isServerEvent and not order.isStopOut:
            records.append(self._unnamed_close(position, event.deal))
        if close_id is not None:
            records += self._node_close(event, close_id, precision)
        else:
            records += self._external_event(event, precision, reduce_only=True)
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
            fill = self._fill(event.deal, precision)
            records: list[Record] = []
            if order.orderId not in self._closes_accepted:
                # As for an entry: the acceptance can be applied after the fill.
                self._closes_accepted.add(order.orderId)
                records.append(OrderEvent(OrderEventKind.ACCEPTED, venue_order_id, close_id, ts))
            records.append(
                OrderEvent(OrderEventKind.FILLED, venue_order_id, close_id, fill.ts_ms, fill=fill)
            )
            return records
        if kind in _ENDED:
            return [
                OrderEvent(
                    _ENDED[kind], venue_order_id, close_id, ts, reason=event.errorCode or None
                )
            ]
        return []

    @staticmethod
    def _unnamed_close(position: _Position, deal: om.ProtoOADeal) -> Notice:
        return Notice(
            f"the broker closed {units_of(deal.filledVolume)} of position "
            f"{position.position_id} on its own, for a reason the event does not name",
        )

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
        if position.ours and not event.isServerEvent:
            return [self._activity(ActivityKind.MANUAL_CHANGE, position, action, ts, units)]
        return []

    def _unloaded(self, event: oa.ProtoOAExecutionEvent) -> list[Record]:
        """Anything on a symbol the node has not loaded: activity, never an order."""
        self._track_unloaded(event)
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

    @staticmethod
    def _pending(order: om.ProtoOAOrder) -> Exposure:
        return Exposure(
            order.tradeData.symbolId,
            "order",
            _SIDE[order.tradeData.tradeSide],
            units_of(remaining_of(order)),
        )

    def _track_unloaded(self, event: oa.ProtoOAExecutionEvent) -> None:
        """Keep the position and the pending orders `exposure()` reports current."""
        order, kind = event.order, event.executionType
        if event.HasField("position") or kind in _FILLS:
            self._sync(self._position_for(event), event)
        if order.orderType == om.STOP_LOSS_TAKE_PROFIT:
            return
        order_id = order.orderId
        if kind in (om.ORDER_ACCEPTED, om.ORDER_REPLACED):
            self._unloaded_orders[order_id] = self._pending(order)
        elif kind == om.ORDER_PARTIAL_FILL and order_id in self._unloaded_orders:
            held = self._unloaded_orders[order_id]
            left = held.units - units_of(event.deal.filledVolume)
            if left > 0:
                self._unloaded_orders[order_id] = replace(held, units=left)
            else:
                del self._unloaded_orders[order_id]
        elif kind == om.ORDER_FILLED or kind in _ENDED:
            self._unloaded_orders.pop(order_id, None)
