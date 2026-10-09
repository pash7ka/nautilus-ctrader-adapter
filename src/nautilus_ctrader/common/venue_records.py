"""What the venue model hands the execution client, and the conversions it relies on.

The model speaks in broker facts, not Nautilus objects: prices, volumes and money are `Decimal`,
ids are the broker's or the node's own strings, times are milliseconds. The execution client
turns each record into a Nautilus event or report.

Conventions across the records:

- `side` is `"BUY"` or `"SELL"`, and `units` is a positive magnitude, never signed.
- A broker position id is an `int` where it identifies a position for the model
  (`AwaitProtection`, `ProtectionMissing`, `Operations`), and its decimal `str` where it
  travels to Nautilus (`venue_position_id`).
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from decimal import ROUND_HALF_EVEN, Decimal, InvalidOperation
from enum import Enum
from typing import Protocol, runtime_checkable

from nautilus_ctrader.common.parsing import volume_to_units


class Level(Enum):
    STOP_LOSS = "SL"
    TAKE_PROFIT = "TP"


@dataclass(frozen=True)
class LevelTerms:
    """How a position's levels work besides their prices, as its last known state says.

    An amend sends them again, so that it changes only the prices it means to change.
    `stop_loss_trigger_method` is the protocol's `ProtoOAOrderTriggerMethod` value.
    """

    trailing_stop_loss: bool
    guaranteed_stop_loss: bool
    stop_loss_trigger_method: int


def leg_venue_order_id(entry_order_id: int, level: Level, generation: int = 1) -> str:
    """A leg's venue order id: `<entry>-SL`, then `<entry>-SL-2`, `<entry>-SL-3`, ...

    Both of a position's levels live in one broker order, and Nautilus maps a venue order id to
    a single order, so each leg is named after its entry, whose id never changes. A level put
    back after its leg closed is a new order, since Nautilus cannot reopen a closed one: the
    next `generation`.
    """
    if generation < 1:
        raise ValueError(f"a leg's generation starts at 1, got {generation}")
    suffix = "" if generation == 1 else f"-{generation}"
    return f"{entry_order_id}-{level.value}{suffix}"


_LEG_ID = re.compile(r"([1-9][0-9]*)-(SL|TP)(?:-([2-9]|[1-9][0-9]+))?")


def parse_leg_venue_order_id(text: str) -> tuple[int, Level, int] | None:
    """The entry order id, level and generation of a leg's venue order id; `None` for any other.

    Strict: only exactly what `leg_venue_order_id()` writes is read.
    """
    found = _LEG_ID.fullmatch(text)
    if found is None:
        return None
    entry, level, generation = found.groups()
    return int(entry), Level(level), 1 if generation is None else int(generation)


def price_of(value: float, precision: int) -> Decimal:
    """A price the venue sent as a double, at `precision` decimals.

    Built from the double's shortest decimal form, not its binary value, so `85197.2` is
    `85197.20` and not `85197.199999…`. The venue sends prices at the symbol's own digits, so
    any extra digit is noise of the double; it is rounded half-even rather than refused, because
    a refusal inside a live event's handling would stop the model.

    Raises `ValueError` for a non-finite value or one too large to hold at `precision`.
    """
    if not math.isfinite(value):
        raise ValueError(f"a price must be finite, got {value!r}")
    exponent = Decimal(1).scaleb(-precision)
    try:
        price = Decimal(repr(value)).quantize(exponent, rounding=ROUND_HALF_EVEN)
    except InvalidOperation:
        raise ValueError(f"price {value!r} does not fit at {precision} decimals") from None
    # A negative zero would print as "-0.00".
    return exponent * 0 if price == 0 else price


def units_of(volume: int) -> Decimal:
    """A venue volume, in hundredths of a unit, as units."""
    return volume_to_units(volume)


def money_of(amount: int, money_digits: int) -> Decimal:
    """A venue money amount, scaled by its own message's `moneyDigits`.

    Keep the `Decimal`, not its `str`: a small amount prints in exponent form.
    """
    return Decimal(amount).scaleb(-money_digits)


class OrderEventKind(Enum):
    ACCEPTED = "accepted"
    FILLED = "filled"
    UPDATED = "updated"
    CANCELED = "canceled"
    REJECTED = "rejected"
    EXPIRED = "expired"


@dataclass(frozen=True)
class Fill:
    """One deal.

    `commission` is in the account's deposit currency, and a charge is negative, as the broker
    reports it. Nautilus counts a charge positive: the execution client flips the sign.
    """

    trade_id: str
    venue_position_id: str
    side: str
    units: Decimal
    price: Decimal
    commission: Decimal
    ts_ms: int


@dataclass(frozen=True)
class OrderEvent:
    """An event of an order Nautilus already knows.

    The node's own orders are named by `client_order_id`; an external order reported earlier
    has none and is named by `venue_order_id` alone. An entry refused before the broker gave it
    an id has no `venue_order_id`. A stop-loss leg's price is `trigger_price`; any other order's
    is `price`.
    """

    kind: OrderEventKind
    venue_order_id: str | None
    client_order_id: str | None
    ts_ms: int
    fill: Fill | None = None
    quantity: Decimal | None = None
    price: Decimal | None = None
    trigger_price: Decimal | None = None
    reason: str | None = None


class ExternalType(Enum):
    MARKET = "market"
    LIMIT = "limit"
    STOP_MARKET = "stop_market"
    STOP_LIMIT = "stop_limit"


@dataclass(frozen=True)
class ExternalOrder:
    """An order Nautilus does not know yet, as it stood before any fill; `fills` follow it.

    Reported with its pre-fill status so that Nautilus never infers a fill of its own. A `LIMIT`
    fills `price`; a `STOP_MARKET`, `trigger_price`; a `STOP_LIMIT`, both; a `MARKET`, neither.

    - `time_in_force`: the broker's `ProtoOATimeInForce` name, e.g. `"GOOD_TILL_CANCEL"`, or
      `None` when the order does not carry one.
    - `expire_ts_ms`: the order's expiry, when it has one.
    - `ts_accepted_ms`: when the broker opened the order; `ts_ms` is its last update, which for an
      order first seen at its fill is the fill's time.
    """

    venue_order_id: str
    symbol_id: int
    side: str
    order_type: ExternalType
    units: Decimal
    reduce_only: bool
    venue_position_id: str | None
    ts_ms: int
    price: Decimal | None = None
    trigger_price: Decimal | None = None
    fills: tuple[Fill, ...] = ()
    time_in_force: str | None = None
    expire_ts_ms: int | None = None
    ts_accepted_ms: int | None = None


class ActivityKind(Enum):
    UNLOADED_SYMBOL = "unloaded_symbol"
    MANUAL_CHANGE = "manual_change"
    STOP_OUT = "stop_out"


class Action(Enum):
    OPENED = "opened"
    CHANGED = "changed"
    CLOSED = "closed"
    PARTIALLY_CLOSED = "partially_closed"
    LEVEL_MOVED = "level_moved"
    LEVEL_REMOVED = "level_removed"
    LEVEL_ADDED = "level_added"


@dataclass(frozen=True)
class Activity:
    """Account activity the node did not start: published for applications, never an order.

    `subject` is `"order"` or `"position"`.
    """

    kind: ActivityKind
    symbol_id: int
    subject: str
    side: str
    units: Decimal
    action: Action
    ts_ms: int


@dataclass(frozen=True, order=True)
class Exposure:
    """An open position or pending order on a symbol the node has not loaded.

    `subject` is `"position"` or `"order"`.
    """

    symbol_id: int
    subject: str
    side: str
    units: Decimal


class ReportStatus(Enum):
    ACCEPTED = "accepted"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELED = "canceled"
    EXPIRED = "expired"
    REJECTED = "rejected"


class Contingency(Enum):
    OTO = "oto"
    OUO = "ouo"


@dataclass(frozen=True)
class ReportedOrder:
    """An order as reconciliation reports it, from the broker's lists.

    Unlike `ExternalOrder` it carries its status as it stands now, with every fill it had. A
    `LIMIT` fills `price`; a `STOP_MARKET`, `trigger_price`; a `STOP_LIMIT`, both.

    - `client_order_id`: the node's id, or `None` for an order that is not the node's.
    - `units`: the order's quantity; `filled_units`: what its fills add up to.
    - `avg_price`: the volume-weighted price of `fills`, `None` without one.
    - `ts_accepted_ms`: when the broker accepted the order; `ts_ms`: its last change.
    - `parent_order_id`, `linked_order_ids`, `contingency`: the bracket links, all client order
      ids.
    """

    venue_order_id: str
    client_order_id: str | None
    symbol_id: int
    side: str
    order_type: ExternalType
    status: ReportStatus
    units: Decimal
    filled_units: Decimal
    reduce_only: bool
    venue_position_id: str | None
    ts_accepted_ms: int
    ts_ms: int
    avg_price: Decimal | None = None
    price: Decimal | None = None
    trigger_price: Decimal | None = None
    time_in_force: str | None = None
    expire_ts_ms: int | None = None
    parent_order_id: str | None = None
    linked_order_ids: tuple[str, ...] = ()
    contingency: Contingency | None = None
    fills: tuple[Fill, ...] = ()


@dataclass(frozen=True)
class ReportedPosition:
    """An open position; `avg_price` is the broker's position price."""

    venue_position_id: str
    symbol_id: int
    side: str
    units: Decimal
    avg_price: Decimal
    ts_ms: int


@dataclass(frozen=True)
class AwaitProtection:
    """The node's entry filled with legs, and the broker's protective order should follow."""

    position_id: int


@dataclass(frozen=True)
class ProtectionMissing:
    """No protective order followed the fill in time: the legs' levels must be set by an amend."""

    position_id: int
    stop_loss_id: str | None
    take_profit_id: str | None


@dataclass(frozen=True)
class Notice:
    """Something a person should know, for a WARNING. Never holds a credential."""

    text: str


@dataclass(frozen=True)
class EntryUnknown:
    """A position the node did not open has a level, but the model knows no entry for it.

    Its legs are named after the entry, so it has none until the entry is known: the position's
    own order list names it.
    """

    position_id: int


Record = (
    OrderEvent
    | ExternalOrder
    | Activity
    | AwaitProtection
    | ProtectionMissing
    | Notice
    | EntryUnknown
)


@runtime_checkable
class Operations(Protocol):
    """What the node has asked of the broker and not yet heard back about."""

    def amending(self, position_id: int) -> bool:
        """Whether the node's own level amend of that position is in flight."""
        ...

    def closing(self, position_id: int, volume: int, created_ms: int, order_id: int) -> str | None:
        """The client order id of the node's close of `volume` on that position, if in flight.

        `order_id` is the broker's closing order and `created_ms` its creation time, on the
        broker's clock. A close whose own answer named `order_id` is returned; otherwise only a
        close sent after the broker's last time the node had seen before `created_ms`. The id
        identifies one close; the execution client consumes it once matched.
        """
        ...
