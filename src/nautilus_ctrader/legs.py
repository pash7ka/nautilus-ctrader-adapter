"""Tying a stop-loss or take-profit leg to the position it protects."""

from __future__ import annotations

from nautilus_trader.cache.base import CacheFacade
from nautilus_trader.model.identifiers import PositionId, VenueOrderId
from nautilus_trader.model.orders import Order

from nautilus_ctrader.common.venue_records import parse_leg_venue_order_id


def leg_position_id(cache: CacheFacade, order: Order) -> PositionId | None:
    """The Nautilus position a stop-loss or take-profit leg belongs to, or `None`.

    A leg carries no `position_id` while it is open: Nautilus caches an order made from a report
    without one, and only a fill sets it. The answer comes from, in turn:

    - the order's own `position_id`, if set (a filled leg has one);
    - otherwise the entry order its venue order id names (`<entry>-SL`, `<entry>-TP`, then
      `-SL-2`, `-TP-3`, ...), looked up in `cache`, and that entry's `position_id`. The entry is
      the position's earliest opening order.

    `None` when the order is not a leg, or its entry is not in `cache` or has no position yet.
    Works the same for the node's own legs and for a foreign position's. Pure over `cache`;
    never raises for an ordinary order.
    """
    if order.position_id is not None:
        return order.position_id
    if order.venue_order_id is None:
        return None
    parsed = parse_leg_venue_order_id(order.venue_order_id.value)
    if parsed is None:
        return None
    entry_order_id, _level, _generation = parsed
    client_order_id = cache.client_order_id(VenueOrderId(str(entry_order_id)))
    if client_order_id is None:
        return None
    entry = cache.order(client_order_id)
    return None if entry is None else entry.position_id
