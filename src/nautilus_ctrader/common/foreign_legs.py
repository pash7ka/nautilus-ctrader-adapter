"""The legs of positions the node did not open, as the external orders Nautilus holds.

Such a position has no legs of the node's: each of its levels is told to Nautilus as a reduce-only
external order named after the position's entry (`leg_venue_order_id`). A level set again after
its leg ended is a new order, at the next generation of that id. The venue model calls these
rules with one of its positions; they change only that position's `foreign_legs` and
`entry_unknown_told`, and do no I/O.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol

from nautilus_ctrader.common.venue_records import (
    EntryUnknown,
    ExternalOrder,
    ExternalType,
    Fill,
    Level,
    OrderEvent,
    OrderEventKind,
    Record,
    leg_prices,
    leg_venue_order_id,
    units_of,
)

_LEVELS = (Level.STOP_LOSS, Level.TAKE_PROFIT)


@dataclass
class ForeignLeg:
    """A level of a position the node did not open, as the external order Nautilus holds.

    `quantity` and `filled` are venue volumes.
    """

    venue_order_id: str
    generation: int
    quantity: int
    alive: bool = True
    filled: int = 0


class ForeignPosition(Protocol):
    """What the rules here read of a position the node did not open."""

    position_id: int
    symbol_id: int
    side: str
    volume: int
    entry_order_id: int | None
    levels: dict[Level, Decimal]
    # The current or last leg of each level.
    foreign_legs: dict[Level, ForeignLeg]
    entry_unknown_told: bool
    protective_order_id: int | None
    protective_volume: int
    protective_opened_ms: int | None
    entry_filled_ms: int | None


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
    limit, trigger = leg_prices(level, price)
    return ExternalOrder(
        venue_order_id=venue_order_id,
        symbol_id=symbol_id,
        side="SELL" if position_side == "BUY" else "BUY",
        order_type=ExternalType.STOP_MARKET if level == Level.STOP_LOSS else ExternalType.LIMIT,
        units=units_of(volume),
        reduce_only=True,
        venue_position_id=str(position_id),
        ts_ms=ts_ms,
        price=limit,
        trigger_price=trigger,
        time_in_force="GOOD_TILL_CANCEL",
        ts_accepted_ms=accepted_ms,
    )


def open_legs(position: ForeignPosition, held_closed: Callable[[str], bool]) -> list[Record]:
    """Open a leg for each standing level of a position just loaded, telling Nautilus nothing.

    Reconciliation reports these legs, at the generation chosen here from the same
    `held_closed`. Returns an `EntryUnknown` instead when the position's entry is not known.
    """
    if position.entry_order_id is None:
        return _entry_unknown(position)
    for level in _LEVELS:
        leg = position.foreign_legs.get(level)
        if level in position.levels and (leg is None or not leg.alive):
            _open(position, level, held_closed)
    return []


def level_changes(
    position: ForeignPosition,
    old_levels: dict[Level, Decimal],
    ts: int,
    held_closed: Callable[[str], bool],
) -> list[Record]:
    """A foreign position's level changes, as its legs: external orders reported once.

    A level seen with no live leg opens a new one; a moved level updates it, a removed one
    cancels it. Each live leg's quantity follows the protective order's volume. `held_closed`
    says whether Nautilus holds a venue order id as a closed order.
    """
    if position.entry_order_id is None:
        return _entry_unknown(position)
    records: list[Record] = []
    for level in _LEVELS:
        old, new = old_levels.get(level), position.levels.get(level)
        leg = position.foreign_legs.get(level)
        live = leg is not None and leg.alive
        if new is not None and not live:
            records.append(_new_leg(position, level, new, ts, held_closed))
        elif new is None and live:
            leg.alive = False
            records.append(OrderEvent(OrderEventKind.CANCELED, leg.venue_order_id, None, ts))
        elif live and new != old:
            leg.quantity = leg.filled + _rest(position)
            records.append(_moved(leg, level, new, ts))
    for leg in position.foreign_legs.values():
        quantity = leg.filled + _rest(position)
        if leg.alive and leg.quantity != quantity:
            leg.quantity = quantity
            records.append(_resized(leg, ts))
    return records


def filled(leg: ForeignLeg, volume: int, fill: Fill) -> list[Record]:
    """A foreign leg's fill; its quantity is raised to cover it first.

    The protective order's smaller volume after a partial close can come after the trigger,
    and a fill above the leg's quantity would be an overfill to Nautilus.
    """
    records: list[Record] = []
    if leg.filled + volume > leg.quantity:
        leg.quantity = leg.filled + volume
        records.append(_resized(leg, fill.ts_ms))
    records.append(
        OrderEvent(OrderEventKind.FILLED, leg.venue_order_id, None, fill.ts_ms, fill=fill)
    )
    leg.filled += volume
    # A leg left with a remainder lives while the position does.
    if leg.filled >= leg.quantity:
        leg.alive = False
    return records


def cancel_all(position: ForeignPosition, ts: int) -> list[Record]:
    """End every live leg of a position that closed."""
    records: list[Record] = []
    for leg in position.foreign_legs.values():
        if leg.alive:
            leg.alive = False
            records.append(OrderEvent(OrderEventKind.CANCELED, leg.venue_order_id, None, ts))
    return records


def _rest(position: ForeignPosition) -> int:
    """What a foreign leg covers besides its fills: the whole position's protection."""
    if position.protective_order_id is None:
        return position.volume
    return position.protective_volume


def _entry_unknown(position: ForeignPosition) -> list[Record]:
    """An `EntryUnknown` the first time a position with levels has no known entry."""
    if not position.levels or position.entry_unknown_told:
        return []
    position.entry_unknown_told = True
    return [EntryUnknown(position.position_id)]


def _open(
    position: ForeignPosition, level: Level, held_closed: Callable[[str], bool]
) -> ForeignLeg:
    """A new leg for `level`, at the first generation Nautilus does not hold closed."""
    assert position.entry_order_id is not None
    last = position.foreign_legs.get(level)
    # A generation this model ended may not have reached Nautilus's cache yet.
    generation = 1 if last is None else last.generation + 1
    venue_order_id = leg_venue_order_id(position.entry_order_id, level, generation)
    while held_closed(venue_order_id):
        generation += 1
        venue_order_id = leg_venue_order_id(position.entry_order_id, level, generation)
    leg = ForeignLeg(venue_order_id, generation, _rest(position))
    position.foreign_legs[level] = leg
    return leg


def _new_leg(
    position: ForeignPosition,
    level: Level,
    price: Decimal,
    ts: int,
    held_closed: Callable[[str], bool],
) -> ExternalOrder:
    leg = _open(position, level, held_closed)
    accepted = _accepted_ms(position, ts)
    return foreign_leg(
        leg.venue_order_id,
        level,
        symbol_id=position.symbol_id,
        position_id=position.position_id,
        position_side=position.side,
        price=price,
        volume=leg.quantity,
        ts_ms=max(ts, accepted),
        accepted_ms=accepted,
    )


def _accepted_ms(position: ForeignPosition, ts: int) -> int:
    """When a foreign leg was accepted: once its level was set, never before the entry filled.

    The same as reconciliation reads from the broker's lists, but where the model lacks what
    they hold: without the entry's fill, as for a position loaded open, the protective order's
    creation stands alone; without either, the event's time `ts`.
    """
    opened, entry_filled = position.protective_opened_ms, position.entry_filled_ms
    if opened is None:
        return ts if entry_filled is None else entry_filled
    return opened if entry_filled is None else max(opened, entry_filled)


def _resized(leg: ForeignLeg, ts: int) -> OrderEvent:
    """The leg updated to its quantity, its price unchanged."""
    return OrderEvent(
        OrderEventKind.UPDATED, leg.venue_order_id, None, ts, quantity=units_of(leg.quantity)
    )


def _moved(leg: ForeignLeg, level: Level, price: Decimal, ts: int) -> OrderEvent:
    """The leg updated to its quantity, its level at `price`."""
    limit, trigger = leg_prices(level, price)
    return OrderEvent(
        OrderEventKind.UPDATED,
        leg.venue_order_id,
        None,
        ts,
        quantity=units_of(leg.quantity),
        price=limit,
        trigger_price=trigger,
    )
