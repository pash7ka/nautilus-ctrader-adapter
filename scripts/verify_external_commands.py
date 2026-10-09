"""Cancel and amend orders the node did not place, through the adapter, for the account owner.

**This script changes real orders on a real account.** It is for the account owner to run by
hand, only with `--send-commands`; it then prints what it will change and asks for the symbol to
be typed again before anything starts. Nothing in the test suite runs it against a broker.

Before the run, the owner places by hand in the trading terminal, on one symbol:

- one pending LIMIT order far from the market, of the minimum volume; with a stop-loss, a
  take-profit and an expiration attached, so that more of what an amend keeps can be checked;
- one position of the minimum volume with a stop-loss and a take-profit, the stop-loss trailing.

The script runs the adapter's execution client inside a Nautilus execution engine, with no
trading node, lets Nautilus reconcile the account, and finds both as Nautilus sees them: the
pending order as an external order, the position's levels as its legs. It refuses to act unless
it finds exactly one of each on the symbol, neither placed by the node, both of the minimum
volume. Then, through Nautilus commands:

1. moves the pending order's limit price ten price steps further from the market;
2. raises its volume by one volume step;
3. cancels it, then asks the broker for the ended order's details;
4. asks the broker to cancel an order id that does not exist, which it refuses;
5. moves the stop-loss ten price steps further from the market;
6. removes the take-profit;
7. listens for `--trailing-wait-secs` for the broker moving the trailing stop.

It never opens or closes a position and never places an order. Every request the session sends
goes through `Guard`, which lets through the reads in `READ_REQUESTS`, and a cancel or an amend
only of the two objects found (and the one cancel of a missing id); anything else raises before
it reaches the socket. The position stays open with its stop-loss: the owner closes it by hand.

    uv run python scripts/verify_external_commands.py --symbol EURUSD --send-commands

The report has one `OK` / `DIFFERS` / `UNKNOWN` line per step and per open protocol question,
with the broker's answer. Every message the broker sent is kept, unscrubbed, in a raw recording
under `--log-dir` (by default `tests/recordings/`, which git ignores);
`scripts/record_execution.py --rescrub` turns it into a scrubbed fixture.

Credentials and the trader login come from `.env` in the repository root. The script does no
token refresh, so it refuses to start with an access token whose `CTRADER_TOKEN_EXPIRES_AT` has
passed. No token, client id or secret is ever printed. The report and the progress lines name no
account identifier; warnings the adapter itself logs are relayed to stderr as they are.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import datetime
import importlib.util
import pathlib
import re
import sys
import time
from collections import Counter
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from decimal import Decimal

from google.protobuf.message import Message
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock, MessageBus
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.messages import CancelOrder, ModifyOrder
from nautilus_trader.live.config import LiveExecEngineConfig
from nautilus_trader.live.execution_engine import LiveExecutionEngine
from nautilus_trader.model.events import (
    OrderCanceled,
    OrderCancelRejected,
    OrderModifyRejected,
    OrderPendingCancel,
    OrderPendingUpdate,
    OrderUpdated,
)
from nautilus_trader.model.identifiers import ClientOrderId, InstrumentId, TraderId
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.model.objects import Price, Quantity
from nautilus_trader.model.orders import Order
from nautilus_trader.portfolio.portfolio import Portfolio

from nautilus_ctrader.common import codec
from nautilus_ctrader.common.account import AccountCredentials, CTraderAccountClient
from nautilus_ctrader.common.connection import CTraderConnection
from nautilus_ctrader.common.errors import (
    CTraderError,
    CTraderProtocolError,
    CTraderRequestError,
    CTraderTimeoutError,
)
from nautilus_ctrader.common.order_record import parse_label
from nautilus_ctrader.common.order_translation import Unsupported, volume_from_quantity
from nautilus_ctrader.common.session import CTraderSession
from nautilus_ctrader.common.venue_book import entry_of
from nautilus_ctrader.common.venue_records import Level, parse_leg_venue_order_id, units_of
from nautilus_ctrader.config import CTraderExecClientConfig
from nautilus_ctrader.constants import (
    DEMO_HOST,
    LIVE_HOST,
    PENDING_ORDER_TYPES,
    PROTOBUF_PORT,
)
from nautilus_ctrader.execution import CTraderExecutionClient
from nautilus_ctrader.messages import OpenApiCommonModelMessages_pb2 as common_model
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as om

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]


def _load_sibling(name: str):
    """scripts/ is not a package: a sibling script is loaded by file path.

    `record_execution` has the same helper, but loading that module needs this one first.
    """
    spec = importlib.util.spec_from_file_location(
        name,
        pathlib.Path(__file__).resolve().with_name(f"{name}.py"),
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


record_execution = _load_sibling("record_execution")
first_orders = _load_sibling("first_orders")
get_tokens = _load_sibling("get_tokens")

_TRADER_LOGIN_KEY = "CTRADER_TRADER_LOGIN"

# What the session may send besides the commands below: the recorder's reads, less the account
# list (read only on the account client's pre-connection), plus the reads the clients make at
# connect. Each reads, authenticates or subscribes to prices; none changes the account.
READ_REQUESTS: frozenset[type[Message]] = (
    record_execution.READ_ONLY_REQUESTS - {oa.ProtoOAGetAccountListByAccessTokenReq}
) | {
    oa.ProtoOAAssetListReq,
    oa.ProtoOASymbolsListReq,
    oa.ProtoOASymbolByIdReq,
    oa.ProtoOASymbolsForConversionReq,
    oa.ProtoOAOrderDetailsReq,
    oa.ProtoOASubscribeSpotsReq,
    oa.ProtoOAUnsubscribeSpotsReq,
}
# Allowed only once `Guard.targets` is set, and only on those targets.
COMMAND_REQUESTS: frozenset[type[Message]] = frozenset(
    {oa.ProtoOACancelOrderReq, oa.ProtoOAAmendOrderReq, oa.ProtoOAAmendPositionSLTPReq},
)
ALLOWED_REQUESTS = READ_REQUESTS | COMMAND_REQUESTS

OK = "OK"
DIFFERS = "DIFFERS"
UNKNOWN = "UNKNOWN"

Decision = tuple[str, tuple[str, ...]]

# How many price steps each move takes a price away from the market.
_MOVE_STEPS = 10
# Added to the highest order id seen to name an order that does not exist yet.
_NO_SUCH_ORDER_OFFSET = 1_000_000_000
_POLL_SECS = 0.05
# Relative slack for comparing prices the venue sent as doubles with ones sent back.
_EPSILON = 1e-9
_TRADER_ID = TraderId("CHECK-001")
# An adapter-made refusal reason starts with the broker's code; the description after it is
# the broker's free text, which this script does not print.
_CODE = re.compile(r"[A-Z][A-Z0-9_]+")


# -- The guard and the recorder ------------------------------------------------------------


class CommandRefused(RuntimeError):
    """A request this script may not send was about to be sent."""


@dataclass(frozen=True)
class Targets:
    """The only objects a command may touch, and how far."""

    pending_order_id: int
    position_id: int
    no_such_order_id: int
    # The pending order's volume may not go above this.
    max_volume: int
    pending_is_buy: bool
    # The pending order's price at the start: an amend may only move it away from the market.
    start_limit_price: float
    position_is_long: bool
    # The position's levels at the start. The stop-loss may only move away from the market from
    # where the broker last reported it; the take-profit may only stay or go.
    start_stop_loss: float
    start_take_profit: float


@dataclass
class Exchange:
    """One request the session sent, and how the broker answered it."""

    request: Message
    sent_at: float
    answer: Message | None = None
    error_code: str | None = None
    timed_out: bool = False
    answered_at: float | None = None
    # The error type, for a request that got no answer: one that failed before the guard logged
    # it, or one the guard logged that was lost with the connection.
    failed: str | None = None


@dataclass(frozen=True)
class Inbound:
    """One message the broker sent; `pushed` when it answered no request."""

    t: float
    message: Message
    pushed: bool


class Recorder:
    """Keeps every message the broker sends in a recording `record_execution.py` can scrub."""

    def __init__(self, recording, clock: Callable[[], float] = time.monotonic) -> None:
        self.recording = recording
        self.inbound: list[Inbound] = []
        self._clock = clock
        self._start = clock()

    def now(self) -> float:
        return self._clock() - self._start

    def mark(self, note: str) -> None:
        self.recording.add("marker", note, None, self.now())

    def tap(self, connection: CTraderConnection) -> None:
        """Record each frame `connection` reads, before the connection acts on it."""
        dispatch = connection._dispatch

        def tapped(envelope) -> None:
            self._take(envelope)
            dispatch(envelope)

        # The connection has no public hook for inbound frames; its read loop calls this.
        connection._dispatch = tapped

    def _take(self, envelope) -> None:
        if envelope.payloadType == common_model.HEARTBEAT_EVENT:
            return
        try:
            message = codec.parse_payload(envelope)
        except CTraderProtocolError:
            return
        pushed = not envelope.clientMsgId
        t = self.now()
        self.inbound.append(Inbound(t, message, pushed))
        self.recording.add(
            "event" if pushed else "snapshot", "" if pushed else "answer", message, t
        )
        if isinstance(message, oa.ProtoOATraderRes) and message.trader.HasField("brokerName"):
            self.recording.add_name(message.trader.brokerName)


class Guard:
    """The one place every request of the session passes; it refuses all but what is allowed.

    Before `targets` is set, no command passes at all.
    """

    def __init__(self, recorder: Recorder) -> None:
        self.recorder = recorder
        self.targets: Targets | None = None
        self.exchanges: list[Exchange] = []
        self.refused: list[str] = []

    def check(self, payload: Message) -> None:
        reason = self._refusal(payload)
        if reason is not None:
            self.refused.append(reason)
            raise CommandRefused(reason)

    def _refusal(self, payload: Message) -> str | None:
        name = type(payload).__name__
        if type(payload) not in ALLOWED_REQUESTS:
            return f"{name} is not a request this script may send"
        if type(payload) in READ_REQUESTS:
            return None
        targets = self.targets
        if targets is None:
            return f"{name} before the owner's orders were found"
        if isinstance(payload, oa.ProtoOACancelOrderReq):
            if payload.orderId not in (targets.pending_order_id, targets.no_such_order_id):
                return f"{name} of an order other than the owner's pending one"
            return None
        if isinstance(payload, oa.ProtoOAAmendOrderReq):
            if payload.orderId != targets.pending_order_id:
                return f"{name} of an order other than the owner's pending one"
            if payload.HasField("volume") and payload.volume > targets.max_volume:
                return f"{name} raising the volume above the minimum plus one step"
            if payload.HasField("limitPrice") and _toward(
                payload.limitPrice, targets.start_limit_price, up=targets.pending_is_buy
            ):
                return f"{name} moving the limit price toward the market"
            return None
        if payload.positionId != targets.position_id:
            return f"{name} of a position other than the owner's"
        if not payload.HasField("stopLoss"):
            return f"{name} removing the stop-loss"
        if _toward(payload.stopLoss, self._reported_stop(targets), up=targets.position_is_long):
            return f"{name} moving the stop-loss toward the market"
        if payload.HasField("takeProfit") and not _same(
            payload.takeProfit, targets.start_take_profit
        ):
            return f"{name} changing the take-profit"
        return None

    def _reported_stop(self, targets: Targets) -> float:
        """The position's stop-loss as the broker last reported it: a trailing stop moves."""
        stop = targets.start_stop_loss
        for inbound in self.recorder.inbound:
            message = inbound.message
            if isinstance(message, oa.ProtoOATrailingSLChangedEvent):
                if message.positionId == targets.position_id:
                    stop = message.stopPrice
                continue
            if isinstance(message, oa.ProtoOAExecutionEvent) and message.HasField("position"):
                positions = [message.position]
            elif isinstance(message, oa.ProtoOAReconcileRes):
                positions = list(message.position)
            else:
                continue
            for position in positions:
                if position.positionId == targets.position_id and position.HasField("stopLoss"):
                    stop = position.stopLoss
        return stop

    def covers(self, connection: CTraderConnection) -> bool:
        """Whether `connection` still sends through this guard."""
        return all(
            getattr(method, "guarded_by", None) is self
            for method in (connection.request, connection.send)
        )

    def instrument(self, connection: CTraderConnection) -> None:
        """Make `connection` check every request and send through this guard, and log them."""
        request, send = connection.request, connection.send

        async def guarded_request(payload: Message, **kwargs) -> Message:
            self.check(payload)
            self.recorder.mark(f"request {type(payload).__name__}")
            exchange = Exchange(payload, self.recorder.now())
            self.exchanges.append(exchange)
            try:
                exchange.answer = await request(payload, **kwargs)
            except CTraderRequestError as e:
                exchange.error_code = e.error_code
                raise
            except CTraderTimeoutError:
                exchange.timed_out = True
                raise
            except CTraderError as e:
                exchange.failed = type(e).__name__
                raise
            finally:
                exchange.answered_at = self.recorder.now()
            return exchange.answer

        async def guarded_send(payload: Message, **kwargs) -> None:
            self.check(payload)
            self.recorder.mark(f"send {type(payload).__name__}")
            await send(payload, **kwargs)

        guarded_request.guarded_by = self
        guarded_send.guarded_by = self
        connection.request = guarded_request
        connection.send = guarded_send


def _same(price: float, other: float) -> bool:
    return abs(price - other) <= _EPSILON * max(1.0, abs(other))


def _toward(price: float, bound: float, *, up: bool) -> bool:
    """Whether `price` lies past `bound` upward when `up`, else downward."""
    if _same(price, bound):
        return False
    return price > bound if up else price < bound


class GuardedAccount(CTraderAccountClient):
    """The account client, with its session's connection behind the guard and the recorder."""

    def __init__(self, *, guard: Guard, **kwargs) -> None:
        super().__init__(**kwargs)
        self._guard = guard

    def _build_session(self, host: str) -> CTraderSession:
        session = super()._build_session(host)
        # The session's connection carries authentication and every request of every client.
        connection = session._connection
        self._guard.instrument(connection)
        self._guard.recorder.tap(connection)
        return session


class ScriptLogger:
    """The Nautilus `Logger` interface for the clients: warnings and errors go to stderr.

    Only the exception's type is printed: its message could carry the broker's free text.
    """

    def __init__(self) -> None:
        self.lines: list[tuple[str, str]] = []

    def debug(self, message: str, color=None) -> None:
        self.lines.append(("debug", message))

    def info(self, message: str, color=None) -> None:
        self.lines.append(("info", message))

    def warning(self, message: str, color=None) -> None:
        self.lines.append(("warning", message))
        print(f"warning: {message}", file=sys.stderr)

    def error(self, message: str, color=None) -> None:
        self.lines.append(("error", message))
        print(f"error: {message}", file=sys.stderr)

    def exception(self, message: str, ex: BaseException) -> None:
        self.lines.append(("error", message))
        print(f"error: {message}: {type(ex).__name__}", file=sys.stderr)


class _Client(CTraderExecutionClient):
    """The execution client, logging through `ScriptLogger`: the Nautilus logger is not set up."""

    def __init__(self, *args, logger: ScriptLogger, **kwargs) -> None:
        self.__dict__["_script_logger"] = logger
        super().__init__(*args, **kwargs)

    @property
    def _log(self):
        return self.__dict__["_script_logger"]


# -- Finding the owner's orders ------------------------------------------------------------


@dataclass(frozen=True)
class Refusal:
    reasons: tuple[str, ...]


@dataclass(frozen=True)
class Choice:
    pending: om.ProtoOAOrder
    position: om.ProtoOAPosition


@dataclass(frozen=True)
class Found:
    pending: om.ProtoOAOrder
    position: om.ProtoOAPosition
    entry_id: int
    pending_order: ClientOrderId
    stop_loss: ClientOrderId
    take_profit: ClientOrderId


def choose(
    snapshot: oa.ProtoOAReconcileRes, *, symbol_id: int, min_volume: int
) -> Choice | Refusal:
    """The owner's pending order and position on the symbol, from the broker's snapshot."""
    pending = [
        order
        for order in snapshot.order
        if order.tradeData.symbolId == symbol_id and order.orderType in PENDING_ORDER_TYPES
    ]
    positions = [p for p in snapshot.position if p.tradeData.symbolId == symbol_id]
    minimum = units_of(min_volume)
    reasons: list[str] = []
    if len(pending) != 1:
        reasons.append(f"{len(pending)} pending orders on the symbol, not exactly one")
    else:
        order = pending[0]
        if order.orderType != om.LIMIT:
            name = om.ProtoOAOrderType.Name(order.orderType)
            reasons.append(f"the pending order is a {name} order, not a LIMIT order")
        if parse_label(order.tradeData.label) is not None:
            reasons.append("the pending order was placed by the node: it carries its label")
        if order.tradeData.volume != min_volume:
            volume = units_of(order.tradeData.volume)
            reasons.append(f"the pending order's volume is {volume}, not the minimum {minimum}")
    if len(positions) != 1:
        reasons.append(f"{len(positions)} positions on the symbol, not exactly one")
    else:
        position = positions[0]
        if not position.HasField("stopLoss"):
            reasons.append("the position has no stop-loss")
        if not position.HasField("takeProfit"):
            reasons.append("the position has no take-profit")
        if position.tradeData.volume != min_volume:
            volume = units_of(position.tradeData.volume)
            reasons.append(f"the position's volume is {volume}, not the minimum {minimum}")
    if reasons:
        return Refusal(tuple(reasons))
    return Choice(pending[0], positions[0])


def confirm(
    choice: Choice,
    position_orders: Sequence[om.ProtoOAOrder],
    nautilus_orders: Iterable[Order],
) -> Found | Refusal:
    """`choice` as Nautilus holds it: the pending order, and the position's two open legs.

    `position_orders` are the broker's orders of the position; its entry tells whether the node
    opened it.
    """
    reasons: list[str] = []
    entry = entry_of(position_orders)
    if entry is None:
        reasons.append("the position's order list names no entry, so it has no legs")
    elif parse_label(entry.tradeData.label) is not None:
        reasons.append("the position was opened by the node: its entry carries its label")
    open_orders = {
        order.venue_order_id.value: order
        for order in nautilus_orders
        if order.venue_order_id is not None and order.is_open
    }
    pending = open_orders.get(str(choice.pending.orderId))
    if pending is None:
        reasons.append("Nautilus holds no open order for the pending order")
    legs: dict[Level, list[Order]] = {Level.STOP_LOSS: [], Level.TAKE_PROFIT: []}
    if entry is not None:
        for venue_order_id, order in open_orders.items():
            parsed = parse_leg_venue_order_id(venue_order_id)
            if parsed is not None and parsed[0] == entry.orderId:
                legs[parsed[1]].append(order)
    for level, name in ((Level.STOP_LOSS, "stop-loss"), (Level.TAKE_PROFIT, "take-profit")):
        if entry is not None and len(legs[level]) != 1:
            reasons.append(f"Nautilus holds {len(legs[level])} open {name} legs, not one")
    if reasons:
        return Refusal(tuple(reasons))
    return Found(
        pending=choice.pending,
        position=choice.position,
        entry_id=entry.orderId,
        pending_order=pending.client_order_id,
        stop_loss=legs[Level.STOP_LOSS][0].client_order_id,
        take_profit=legs[Level.TAKE_PROFIT][0].client_order_id,
    )


def no_such_order_id(known: Iterable[int]) -> int:
    """An order id far past every id seen, so that no order has it yet."""
    return max(known, default=0) + _NO_SUCH_ORDER_OFFSET


def moved_away(price: Decimal, increment: Decimal, *, lower: bool) -> Decimal | None:
    """`price` moved `_MOVE_STEPS` increments down when `lower`, else up; `None` if not positive."""
    moved = price - increment * _MOVE_STEPS if lower else price + increment * _MOVE_STEPS
    return moved if moved > 0 else None


# -- Decisions -----------------------------------------------------------------------------
#
# Each turns what a run observed into a verdict and the lines behind it, with no I/O, so the
# report a live run prints can be checked offline.


@dataclass(frozen=True)
class Outcome:
    """How Nautilus ended a command: done, rejected with a reason, or neither in time."""

    done: bool = False
    rejection: str | None = None
    skipped: str | None = None


def code_only(reason: str) -> str:
    """A refusal reason without the broker's description: its code, or the adapter's own words."""
    head, sep, _ = reason.partition(":")
    return head if sep and _CODE.fullmatch(head) else reason


def answer_text(exchange: Exchange | None) -> str:
    """What the broker answered a request, by type and code only."""
    if exchange is None:
        return "not sent"
    if exchange.failed is not None:
        # Only an exchange the guard logged has an end.
        sent = exchange.answered_at is not None
        return f"{'no answer' if sent else 'not sent'}: {exchange.failed}"
    if exchange.error_code is not None:
        return f"ProtoOAErrorRes {exchange.error_code}"
    if exchange.timed_out or exchange.answer is None:
        return "no answer carrying the request's id"
    answer = exchange.answer
    name = type(answer).__name__
    if isinstance(answer, oa.ProtoOAExecutionEvent):
        kind = om.ProtoOAExecutionType.Name(answer.executionType)
        code = f" {answer.errorCode}" if answer.HasField("errorCode") else ""
        return f"{name} {kind}{code}"
    if isinstance(answer, oa.ProtoOAOrderErrorEvent):
        return f"{name} {answer.errorCode}"
    if isinstance(answer, oa.ProtoOAOrderDetailsRes):
        status = om.ProtoOAOrderStatus.Name(answer.order.orderStatus)
        return f"{name}, order {status}, {len(answer.deal)} deals"
    return name


def refusal_of(exchange: Exchange | None) -> str | None:
    """The broker's refusal code, when its answer refused the request."""
    if exchange is None:
        return None
    if exchange.error_code is not None:
        return exchange.error_code
    answer = exchange.answer
    if isinstance(answer, oa.ProtoOAOrderErrorEvent):
        return answer.errorCode
    if isinstance(answer, oa.ProtoOAExecutionEvent) and answer.executionType in (
        om.ORDER_REJECTED,
        om.ORDER_CANCEL_REJECTED,
    ):
        return answer.errorCode or om.ProtoOAExecutionType.Name(answer.executionType)
    return None


def decide_step(outcome: Outcome, exchange: Exchange | None, done: str) -> Decision:
    """A command Nautilus sent: `done` says what Nautilus shows once it is carried out."""
    broker = f"broker: {answer_text(exchange)}"
    if outcome.skipped is not None:
        return UNKNOWN, (f"not run: {outcome.skipped}",)
    if outcome.done:
        return OK, (f"Nautilus: {done}", broker)
    if outcome.rejection is not None:
        return DIFFERS, (f"Nautilus: rejected: {outcome.rejection}", broker)
    return UNKNOWN, ("Nautilus heard nothing within the wait", broker)


def accepted(exchange: Exchange | None) -> bool:
    """Whether the broker answered the request without refusing it."""
    return exchange is not None and exchange.answer is not None and refusal_of(exchange) is None


def decide_order_replaced(exchanges: Sequence[Exchange]) -> Decision:
    """Whether each accepted amend of the pending order was answered as a replaced order."""
    if not exchanges:
        return UNKNOWN, ("no amend of the pending order was sent",)
    lines = tuple(f"answered: {answer_text(x)}" for x in exchanges)
    taken = [x for x in exchanges if accepted(x)]
    if not taken:
        return UNKNOWN, ("no amend was accepted", *lines)
    if all(
        isinstance(x.answer, oa.ProtoOAExecutionEvent)
        and x.answer.executionType == om.ORDER_REPLACED
        for x in taken
    ):
        return OK, lines
    return DIFFERS, lines


# What an amend of the pending order must keep. A value is kept or lost: absent differs from
# any value.
_OPTIONAL_KEPT = (
    "expirationTimestamp",
    "stopLoss",
    "takeProfit",
    "relativeStopLoss",
    "relativeTakeProfit",
    "slippageInPoints",
)
# A flag or a method is read with its schema default when absent, so stating the default is no
# change.
_DEFAULTED_KEPT = ("trailingStopLoss", "stopTriggerMethod", "timeInForce", "guaranteedStopLoss")


def _field(order: om.ProtoOAOrder, name: str) -> object:
    if name == "guaranteedStopLoss":
        return order.tradeData.guaranteedStopLoss
    if name in _DEFAULTED_KEPT:
        return getattr(order, name)
    return getattr(order, name) if order.HasField(name) else None


def decide_order_kept(
    before: om.ProtoOAOrder | None,
    after: om.ProtoOAOrder | None,
    *,
    amended: bool,
) -> Decision:
    """Whether the amends kept what they re-sent: expiration, attached levels, trigger method."""
    if not amended:
        return UNKNOWN, ("no amend of the pending order was accepted",)
    if before is None or after is None:
        return UNKNOWN, ("the pending order was not listed after its amends",)
    # A level held as a distance is what the adapter re-sends; its price follows the moved limit
    # price, so only the distance must stay.
    following = [
        absolute
        for relative, absolute in (
            ("relativeStopLoss", "stopLoss"),
            ("relativeTakeProfit", "takeProfit"),
        )
        if before.HasField(relative)
    ]
    compared = [name for name in (*_OPTIONAL_KEPT, *_DEFAULTED_KEPT) if name not in following]
    changed = [
        f"{name}: {_field(before, name)} -> {_field(after, name)}"
        for name in compared
        if _field(before, name) != _field(after, name)
    ]
    if changed:
        return DIFFERS, tuple(changed)
    held = [name for name in _OPTIONAL_KEPT if name in compared and before.HasField(name)]
    if not held:
        return UNKNOWN, (
            "the order carried no expiration and no attached level, so the amends kept "
            "only the time in force and the trigger method",
        )
    lines = [f"kept: {', '.join(held)} and the order's other fields"]
    lines += [
        f"{name} not compared: it follows the moved limit price"
        for name in following
        if before.HasField(name)
    ]
    return OK, tuple(lines)


def decide_level_form(order: om.ProtoOAOrder | None) -> Decision:
    """Which form, a distance or a price, the pending order reports its attached levels in."""
    if order is None:
        return UNKNOWN, ("the pending order was not read",)
    lines = []
    for name, relative, absolute in (
        ("stop-loss", "relativeStopLoss", "stopLoss"),
        ("take-profit", "relativeTakeProfit", "takeProfit"),
    ):
        forms = [f for f in (relative, absolute) if order.HasField(f)]
        if forms:
            lines.append(f"{name}: {' and '.join(forms)}")
    if not lines:
        return UNKNOWN, ("the pending order carries no stop-loss or take-profit",)
    if any(" and " in line for line in lines):
        lines.append("both forms are reported; the adapter re-sends the distance")
    return OK, tuple(lines)


def decide_refusal_form(exchange: Exchange | None) -> Decision:
    """How the broker refuses a cancel: an order error, a rejection event, or an error answer."""
    if exchange is None or exchange.failed is not None:
        return UNKNOWN, (answer_text(exchange),)
    text = answer_text(exchange)
    if refusal_of(exchange) is not None:
        return OK, (f"refused with {text}",)
    if exchange.answer is None:
        return DIFFERS, (f"{text}: the adapter waits for a correlated answer",)
    return DIFFERS, (f"answered with {text}, not a refusal",)


def decide_details_of_ended(exchange: Exchange | None) -> Decision:
    """Whether the order details request answers an order that has ended."""
    if exchange is None:
        return UNKNOWN, ("the pending order was not cancelled, so not asked",)
    if exchange.failed is not None:
        return UNKNOWN, (answer_text(exchange),)
    if isinstance(exchange.answer, oa.ProtoOAOrderDetailsRes):
        return OK, (f"answered: {answer_text(exchange)}",)
    return DIFFERS, (
        f"answered: {answer_text(exchange)}",
        "a query of an ended order falls back to the order list",
    )


def decide_details_of_unknown(exchange: Exchange | None) -> Decision:
    """What the order details request answers for an id no order has."""
    if exchange is None or exchange.failed is not None:
        return UNKNOWN, (answer_text(exchange),)
    if refusal_of(exchange) is not None:
        return OK, (f"refused with {answer_text(exchange)}",)
    return DIFFERS, (f"answered: {answer_text(exchange)}",)


_POSITION_TERMS = ("stopLossTriggerMethod", "trailingStopLoss", "guaranteedStopLoss")


def decide_terms_accepted(exchange: Exchange | None) -> Decision:
    """Whether an amend explicitly carrying the stop-loss's terms is accepted."""
    if exchange is None:
        return UNKNOWN, ("the stop-loss was not moved",)
    request = exchange.request
    missing = [name for name in _POSITION_TERMS if not request.HasField(name)]
    if missing:
        return UNKNOWN, (f"the amend did not carry {', '.join(missing)}",)
    sent = ", ".join(f"{name}={getattr(request, name)}" for name in _POSITION_TERMS)
    refused = refusal_of(exchange)
    if refused is not None:
        return DIFFERS, (f"sent {sent}", f"refused with {answer_text(exchange)}")
    if exchange.answer is None:
        return UNKNOWN, (f"sent {sent}", answer_text(exchange))
    return OK, (f"sent {sent}", f"answered: {answer_text(exchange)}")


def decide_trailing_kept(
    *,
    trailing_at_start: bool,
    moved: bool,
    after: om.ProtoOAPosition | None,
) -> Decision:
    """Whether a trailing stop-loss is still trailing after the stop-loss is moved."""
    if not trailing_at_start:
        return UNKNOWN, ("the stop-loss was not trailing at the start",)
    if not moved:
        return UNKNOWN, ("the stop-loss was not moved",)
    if after is None:
        return UNKNOWN, ("the position was not listed after the move",)
    level = f"stop-loss now {after.stopLoss}" if after.HasField("stopLoss") else "no stop-loss"
    if after.trailingStopLoss:
        return OK, (f"still trailing, {level}",)
    return DIFFERS, (f"no longer trailing, {level}",)


def decide_level_left_out(*, removed: bool, after: om.ProtoOAPosition | None) -> Decision:
    """Whether leaving the take-profit out of an amend removes it."""
    if not removed:
        return UNKNOWN, ("the take-profit's removal was not answered",)
    if after is None:
        return UNKNOWN, ("the position was not listed after the amend",)
    if after.HasField("takeProfit"):
        return DIFFERS, (f"the take-profit stays at {after.takeProfit}",)
    return OK, ("the position has no take-profit",)


def decide_stop_kept(
    *,
    removed: bool,
    trailing_at_start: bool,
    after: om.ProtoOAPosition | None,
) -> Decision:
    """Whether the stop-loss and its trailing stay once the take-profit is removed."""
    if not removed:
        return UNKNOWN, ("the take-profit's removal was not answered",)
    if after is None:
        return UNKNOWN, ("the position was not listed after the amend",)
    if not after.HasField("stopLoss"):
        return DIFFERS, ("the stop-loss went with the take-profit",)
    lines = [f"stop-loss at {after.stopLoss}"]
    if not trailing_at_start:
        return OK, (*lines, "it was not trailing at the start, so trailing was not checked")
    if not after.trailingStopLoss:
        return DIFFERS, (*lines, "no longer trailing")
    return OK, (*lines, "still trailing")


def decide_besides_answers(seen: Sequence[tuple[str, Sequence[str]]]) -> Decision:
    """Whether an accepted command is followed by execution events besides its answer.

    `seen` holds each accepted command's name and the pushed execution events about its order or
    position that came in the short wait after its answer.
    """
    if not seen:
        return UNKNOWN, ("no command was accepted",)
    extra = [(name, events) for name, events in seen if events]
    if not extra:
        return OK, (f"only the answer, after each of: {', '.join(name for name, _ in seen)}",)
    return DIFFERS, tuple(f"{name}: also {', '.join(events)}" for name, events in extra)


def decide_trailing_moves(
    *,
    trailing_events: int,
    execution_events: int,
    wait_secs: float,
    trailing_at_start: bool,
) -> Decision:
    """How a trailing stop's moves arrive: trailing events, execution events, or both."""
    counts = f"{trailing_events} trailing events, {execution_events} execution events"
    if not trailing_at_start:
        return UNKNOWN, ("the stop-loss was not trailing at the start",)
    if trailing_events == 0 and execution_events == 0:
        return UNKNOWN, (f"the market did not move the stop-loss within {wait_secs:g} s",)
    if execution_events == 0:
        return OK, (f"{counts}: each move as its trailing event only",)
    if trailing_events == 0:
        return DIFFERS, (f"{counts}: moves arrive only as execution events",)
    return DIFFERS, (f"{counts}: moves arrive both ways; the second of each changes nothing",)


def decide_volume_after_partial_fill() -> Decision:
    return UNKNOWN, (
        "settled only by an amend of a partly filled pending order; this run fills nothing",
    )


def left_open(
    snapshot: oa.ProtoOAReconcileRes | None,
    *,
    symbol: str,
    pending_order_id: int,
    position_id: int,
) -> tuple[str, ...]:
    """What of the owner's two objects the broker still holds, as lines telling what to do."""
    if snapshot is None:
        return (
            f"The broker could not be read at the end: check {symbol} in the terminal, close "
            "the position by hand and cancel the pending order if it is still there.",
        )
    lines = []
    for position in snapshot.position:
        if position.positionId != position_id:
            continue
        if not position.HasField("stopLoss"):
            lines.append(
                f"The position on {symbol} stays open WITHOUT a stop-loss: close it by hand now.",
            )
            continue
        trailing = ", trailing" if position.trailingStopLoss else ""
        target = (
            f", take-profit at {position.takeProfit}" if position.HasField("takeProfit") else ""
        )
        lines.append(
            f"The position on {symbol} stays open with its stop-loss at {position.stopLoss}"
            f"{trailing}{target}. Close it by hand in the terminal.",
        )
    if any(order.orderId == pending_order_id for order in snapshot.order):
        lines.append(f"The pending order on {symbol} is still open: cancel it by hand.")
    if not lines:
        lines.append(f"Nothing of the two is left open on {symbol}.")
    return tuple(lines)


# -- The run -------------------------------------------------------------------------------


@dataclass(frozen=True)
class Finding:
    item: int
    title: str
    status: str
    detail: tuple[str, ...] = ()


@dataclass(frozen=True)
class Settings:
    symbol: str
    trailing_wait_secs: float = 60.0
    answer_wait_secs: float = 30.0
    # How long to watch for events after each command's answer.
    settle_secs: float = 2.0
    log_dir: pathlib.Path = _REPO_ROOT / "tests" / "recordings"


@dataclass(frozen=True)
class Address:
    demo_host: str = DEMO_HOST
    live_host: str = LIVE_HOST
    port: int = PROTOBUF_PORT
    tls: bool = True


@dataclass
class Result:
    findings: list[Finding] = field(default_factory=list)
    refusal: tuple[str, ...] = ()
    left_open: tuple[str, ...] = ()
    raw: pathlib.Path | None = None
    guard: Guard | None = None
    logger: ScriptLogger | None = None
    # The type of the error that ended the run early, if one did.
    failure: str | None = None


def _say(text: str) -> None:
    print(f">>> {text}")


class Check:
    """The steps, carried out through the engine as a strategy would send them."""

    def __init__(
        self,
        *,
        settings: Settings,
        account: GuardedAccount,
        client: _Client,
        engine: LiveExecutionEngine,
        cache: Cache,
        instrument: Instrument,
        symbol_id: int,
        guard: Guard,
        clock: LiveClock,
        status: Callable[[str], None],
    ) -> None:
        self._settings = settings
        self._clock = clock
        self._account = account
        self._client = client
        self._engine = engine
        self._cache = cache
        self._instrument = instrument
        self._symbol_id = symbol_id
        self._guard = guard
        self._recorder = guard.recorder
        self._status = status
        # Each accepted command's label and exchange, to look at the events after its answer.
        self._accepted: list[tuple[str, Exchange]] = []

    async def run(self, result: Result, listening: asyncio.Event | None) -> None:
        """Find the objects, carry out the steps, then say what is left open, however it ends."""
        found = await self._find(result)
        if found is None:
            return
        self._status("Found the pending order and the position. Sending the commands.")
        try:
            await self._steps(found, listening, result)
        except Exception as e:
            result.failure = type(e).__name__
        final = None
        with contextlib.suppress(Exception):
            final = await self._snapshot()
        result.left_open = left_open(
            final,
            symbol=self._settings.symbol,
            pending_order_id=found.pending.orderId,
            position_id=found.position.positionId,
        )

    # -- finding --

    async def _find(self, result: Result) -> Found | None:
        snapshot = await self._snapshot()
        if snapshot is None:
            result.refusal = ("the broker's snapshot could not be read",)
            return None
        try:
            min_volume = volume_from_quantity(self._min_quantity())
            max_volume = volume_from_quantity(self._raised_quantity())
        except Unsupported as e:
            result.refusal = (f"the instrument's minimum volume: {e}",)
            return None
        choice = choose(snapshot, symbol_id=self._symbol_id, min_volume=min_volume)
        if isinstance(choice, Refusal):
            result.refusal = choice.reasons
            return None
        listed = await self._ask(
            oa.ProtoOAOrderListByPositionIdReq(
                ctidTraderAccountId=self._account.account_id,
                positionId=choice.position.positionId,
            ),
        )
        if not isinstance(listed.answer, oa.ProtoOAOrderListByPositionIdRes):
            result.refusal = ("the position's order list could not be read",)
            return None
        found = confirm(
            choice,
            listed.answer.order,
            self._cache.orders(instrument_id=self._instrument.id),
        )
        if isinstance(found, Refusal):
            result.refusal = found.reasons
            return None
        known = [o.orderId for o in (*snapshot.order, *listed.answer.order)]
        self._guard.targets = Targets(
            pending_order_id=found.pending.orderId,
            position_id=found.position.positionId,
            no_such_order_id=no_such_order_id(known),
            max_volume=max_volume,
            pending_is_buy=found.pending.tradeData.tradeSide == om.BUY,
            start_limit_price=found.pending.limitPrice,
            position_is_long=found.position.tradeData.tradeSide == om.BUY,
            start_stop_loss=found.position.stopLoss,
            start_take_profit=found.position.takeProfit,
        )
        return found

    def _min_quantity(self) -> Quantity:
        minimum = self._instrument.min_quantity
        return self._instrument.size_increment if minimum is None else minimum

    def _raised_quantity(self) -> Quantity:
        step = self._instrument.size_increment.as_decimal()
        return self._instrument.make_qty(self._min_quantity().as_decimal() + step)

    # -- the steps --

    async def _steps(self, found: Found, listening: asyncio.Event | None, result: Result) -> None:
        """The steps; each step's finding is added to `result` as soon as the step ends."""
        targets = self._guard.targets

        def step(item: int, title: str, sent: tuple[Outcome, Exchange | None], done: str) -> None:
            result.findings.append(Finding(item, title, *decide_step(*sent, done)))

        position_id = found.position.positionId
        increment = self._instrument.price_increment.as_decimal()
        trailing_at_start = found.position.trailingStopLoss

        # 1-2. Price, then volume, of the pending order.
        pending = self._cache.order(found.pending_order)
        price = moved_away(pending.price.as_decimal(), increment, lower=targets.pending_is_buy)
        priced = await self._command(
            "price amend",
            found.pending_order,
            oa.ProtoOAAmendOrderReq,
            price=None if price is None else self._instrument.make_price(price),
            skip=None if price is not None else "the moved price would not be positive",
        )
        step(1, "The pending order's limit price moves away", priced, f"price {price}")
        raised = self._raised_quantity()
        sized = await self._command(
            "volume amend", found.pending_order, oa.ProtoOAAmendOrderReq, quantity=raised
        )
        step(2, "The pending order's volume rises one step", sized, f"quantity {raised}")
        after_amends = await self._snapshot()
        amended_pending = _order(after_amends, found.pending.orderId)

        # 3. Cancel the pending order, then ask for its details.
        cancelled = await self._command(
            "cancel", found.pending_order, oa.ProtoOACancelOrderReq, cancel=True
        )
        step(3, "The pending order is cancelled", cancelled, "CANCELED")
        details_ended = None
        if cancelled[0].done:
            details_ended = await self._ask(
                oa.ProtoOAOrderDetailsReq(
                    ctidTraderAccountId=self._account.account_id,
                    orderId=found.pending.orderId,
                ),
            )

        # 4. A cancel the broker refuses, and the details of the same missing order.
        refused = None
        if self._blocked() is None:
            self._status("Asking the broker to cancel an order id that does not exist.")
            refused = await self._ask(
                oa.ProtoOACancelOrderReq(
                    ctidTraderAccountId=self._account.account_id,
                    orderId=targets.no_such_order_id,
                ),
            )
        details_unknown = await self._ask(
            oa.ProtoOAOrderDetailsReq(
                ctidTraderAccountId=self._account.account_id,
                orderId=targets.no_such_order_id,
            ),
        )

        # 5. Move the stop-loss away from the market.
        stop_leg = self._cache.order(found.stop_loss)
        long = found.position.tradeData.tradeSide == om.BUY
        trigger = moved_away(stop_leg.trigger_price.as_decimal(), increment, lower=long)
        moved = await self._command(
            "stop-loss move",
            found.stop_loss,
            oa.ProtoOAAmendPositionSLTPReq,
            trigger_price=None if trigger is None else self._instrument.make_price(trigger),
            skip=None if trigger is not None else "the moved stop-loss would not be positive",
        )
        step(4, "The stop-loss moves away", moved, f"trigger price {trigger}")
        after_move = _position(await self._snapshot(), position_id)

        # 6. Remove the take-profit.
        removed = await self._command(
            "take-profit removal", found.take_profit, oa.ProtoOAAmendPositionSLTPReq, cancel=True
        )
        step(5, "The take-profit is removed", removed, "take-profit leg CANCELED")
        after_removal = _position(await self._snapshot(), position_id)

        # 7. Listen for the broker moving the trailing stop, unless the run was stopped.
        window = []
        if self._blocked() is None:
            self._status(
                f"Listening {self._settings.trailing_wait_secs:g} s for trailing stop moves. "
                "Change nothing in the terminal meanwhile.",
            )
            start = self._recorder.now()
            if listening is not None:
                listening.set()
            await asyncio.sleep(self._settings.trailing_wait_secs)
            window = [i for i in self._recorder.inbound if i.pushed and i.t >= start]
        trailing_events = sum(
            isinstance(i.message, oa.ProtoOATrailingSLChangedEvent)
            and i.message.positionId == position_id
            for i in window
        )
        execution_events = sum(
            _concerns(i.message, order_ids=(), position_id=position_id) for i in window
        )

        sent_amends = [x for _, x in (priced, sized) if x is not None]
        decisions: list[tuple[str, Decision]] = [
            (
                "A pending order's amend is answered ORDER_REPLACED",
                decide_order_replaced(sent_amends),
            ),
            (
                "A pending order's amend keeps its expiration, attached levels and trigger method",
                decide_order_kept(
                    found.pending,
                    amended_pending,
                    amended=any(accepted(x) for x in sent_amends),
                ),
            ),
            (
                "The form a pending order reports its attached levels in",
                decide_level_form(found.pending),
            ),
            ("How the broker refuses a cancel", decide_refusal_form(refused)),
            ("Order details of an order that has ended", decide_details_of_ended(details_ended)),
            (
                "Order details of an order id that does not exist",
                decide_details_of_unknown(details_unknown),
            ),
            (
                "A position amend carrying the trigger method, trailing and guaranteed flags is "
                "accepted",
                decide_terms_accepted(moved[1]),
            ),
            (
                "A trailing stop-loss stays trailing after it is moved",
                decide_trailing_kept(
                    trailing_at_start=trailing_at_start,
                    moved=moved[0].done,
                    after=after_move,
                ),
            ),
            (
                "Leaving the take-profit out of an amend removes it",
                decide_level_left_out(removed=removed[0].done, after=after_removal),
            ),
            (
                "Once the take-profit is removed, the stop-loss and its trailing stay",
                decide_stop_kept(
                    removed=removed[0].done,
                    trailing_at_start=trailing_at_start,
                    after=after_removal,
                ),
            ),
            (
                "Execution events besides an accepted command's answer",
                decide_besides_answers(self._besides_answers(found)),
            ),
            (
                "A trailing stop's moves arrive as trailing events, execution events or both",
                decide_trailing_moves(
                    trailing_events=trailing_events,
                    execution_events=execution_events,
                    wait_secs=self._settings.trailing_wait_secs,
                    trailing_at_start=trailing_at_start,
                ),
            ),
            (
                "An amend's volume after a partial fill: the whole order or its rest",
                decide_volume_after_partial_fill(),
            ),
        ]
        result.findings += [
            Finding(item, title, status, detail)
            for item, (title, (status, detail)) in enumerate(decisions, start=6)
        ]

    def _besides_answers(self, found: Found) -> list[tuple[str, list[str]]]:
        seen = []
        for name, exchange in self._accepted:
            start = exchange.answered_at
            end = start + self._settings.settle_secs
            events = [
                om.ProtoOAExecutionType.Name(i.message.executionType)
                for i in self._recorder.inbound
                if i.pushed
                and start <= i.t <= end
                and _concerns(
                    i.message,
                    order_ids=(found.pending.orderId,),
                    position_id=found.position.positionId,
                )
            ]
            seen.append((name, events))
        return seen

    # -- commands through the engine --

    async def _command(
        self,
        label: str,
        client_order_id: ClientOrderId,
        request_type: type[Message],
        *,
        cancel: bool = False,
        price: Price | None = None,
        trigger_price: Price | None = None,
        quantity: Quantity | None = None,
        skip: str | None = None,
    ) -> tuple[Outcome, Exchange | None]:
        """A cancel, or else a modify, of the order, sent as a strategy sends it."""
        blocked = self._blocked()
        if blocked is not None:
            return Outcome(skipped=blocked), None
        order = self._cache.order(client_order_id)
        if skip is not None:
            return Outcome(skipped=skip), None
        if order is None or not order.is_open:
            return Outcome(skipped="the order is no longer open in Nautilus"), None
        self._status(f"Sending the {label}.")
        start = len(order.events)
        sent = len(self._guard.exchanges)
        now = self._clock.timestamp_ns()
        pending = OrderPendingCancel if cancel else OrderPendingUpdate
        self._engine.process(
            pending(
                _TRADER_ID,
                order.strategy_id,
                order.instrument_id,
                order.client_order_id,
                order.venue_order_id,
                self._client.account_id,
                UUID4(),
                now,
                now,
            ),
        )
        ids = {
            "trader_id": _TRADER_ID,
            "strategy_id": order.strategy_id,
            "instrument_id": order.instrument_id,
            "client_order_id": order.client_order_id,
            "venue_order_id": order.venue_order_id,
            "command_id": UUID4(),
            "ts_init": now,
        }
        if cancel:
            self._engine.execute(CancelOrder(**ids))
        else:
            self._engine.execute(
                ModifyOrder(**ids, quantity=quantity, price=price, trigger_price=trigger_price),
            )

        def done(events: list) -> bool:
            if cancel:
                return any(isinstance(e, OrderCanceled) for e in events)
            return any(
                isinstance(e, OrderUpdated)
                and (price is None or e.price == price)
                and (trigger_price is None or e.trigger_price == trigger_price)
                and (quantity is None or e.quantity == quantity)
                for e in events
            )

        return await self._outcome(label, client_order_id, start, sent, request_type, done)

    async def _outcome(
        self,
        label: str,
        client_order_id: ClientOrderId,
        start: int,
        sent: int,
        request_type: type[Message],
        done: Callable[[list], bool],
    ) -> tuple[Outcome, Exchange | None]:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._settings.answer_wait_secs
        outcome = Outcome()
        while True:
            events = self._cache.order(client_order_id).events[start:]
            rejected = [
                e for e in events if isinstance(e, (OrderModifyRejected, OrderCancelRejected))
            ]
            if rejected:
                outcome = Outcome(rejection=code_only(rejected[-1].reason))
                break
            if done(events):
                outcome = Outcome(done=True)
                break
            if loop.time() >= deadline:
                break
            await asyncio.sleep(_POLL_SECS)
        exchanges = [x for x in self._guard.exchanges[sent:] if isinstance(x.request, request_type)]
        exchange = exchanges[-1] if exchanges else None
        await asyncio.sleep(self._settings.settle_secs)
        if outcome.done and exchange is not None and exchange.answered_at is not None:
            self._accepted.append((label, exchange))
        return outcome, exchange

    def _blocked(self) -> str | None:
        """Why no further command may be sent, if one may not."""
        if self._guard.refused:
            return "the guard refused a request; no further command is sent"
        session = self._account.session
        if session is None or not self._guard.covers(session._connection):
            reason = "the session's connection no longer sends through the guard"
            self._guard.refused.append(reason)
            return reason
        return None

    # -- reads --

    async def _ask(self, payload: Message) -> Exchange:
        """Send `payload` through the guard; its exchange, however the broker answered."""
        failed = None
        try:
            await self._account.request(payload, timeout_secs=self._settings.answer_wait_secs)
        except CTraderError as e:
            # The exchange keeps the answer or the error; the guard's refusal still raises.
            failed = type(e).__name__
        for exchange in reversed(self._guard.exchanges):
            if exchange.request is payload:
                return exchange
        # Failed before the guard saw it, as when the session is reconnecting.
        return Exchange(payload, self._recorder.now(), failed=failed or "no request sent")

    async def _snapshot(self) -> oa.ProtoOAReconcileRes | None:
        exchange = await self._ask(
            oa.ProtoOAReconcileReq(
                ctidTraderAccountId=self._account.account_id,
                returnProtectionOrders=True,
            ),
        )
        answer = exchange.answer
        return answer if isinstance(answer, oa.ProtoOAReconcileRes) else None


def _order(snapshot: oa.ProtoOAReconcileRes | None, order_id: int) -> om.ProtoOAOrder | None:
    if snapshot is None:
        return None
    return next((o for o in snapshot.order if o.orderId == order_id), None)


def _position(
    snapshot: oa.ProtoOAReconcileRes | None,
    position_id: int,
) -> om.ProtoOAPosition | None:
    if snapshot is None:
        return None
    return next((p for p in snapshot.position if p.positionId == position_id), None)


def _concerns(message: Message, *, order_ids: Sequence[int], position_id: int) -> bool:
    """Whether `message` is an execution event about one of `order_ids` or the position."""
    if not isinstance(message, oa.ProtoOAExecutionEvent):
        return False
    if message.HasField("position") and message.position.positionId == position_id:
        return True
    order = message.order if message.HasField("order") else None
    return order is not None and (order.orderId in order_ids or order.positionId == position_id)


async def run_check(
    settings: Settings,
    credentials: AccountCredentials,
    trader_login: int,
    *,
    address: Address | None = None,
    status: Callable[[str], None] = _say,
    listening: asyncio.Event | None = None,
) -> Result:
    """Connect, find the owner's orders, carry out the steps and keep the broker's messages.

    The raw recording is written to `settings.log_dir` however the run ends, if it holds any
    message. `listening` is set when the run starts listening for trailing moves.
    """
    address = address or Address()
    recorder = Recorder(record_execution.Recording(int(time.time() * 1000)))
    guard = Guard(recorder)
    logger = ScriptLogger()
    result = Result(guard=guard, logger=logger)
    instrument_id = first_orders.instrument_id_of(settings.symbol)
    account = GuardedAccount(
        guard=guard,
        trader_login=trader_login,
        credentials=credentials,
        environment="auto",
        logger=logger,
        demo_host=address.demo_host,
        live_host=address.live_host,
        port=address.port,
        tls=address.tls,
    )
    provider = account.get_instrument_provider(
        config=InstrumentProviderConfig(load_ids=frozenset([str(instrument_id)])),
        asset_class_overrides={},
        fail_on_instrument_error=False,
        logger=logger,
    )
    loop = asyncio.get_running_loop()
    clock = LiveClock()
    msgbus = MessageBus(trader_id=_TRADER_ID, clock=clock)
    cache = Cache()
    Portfolio(msgbus, cache, clock)
    engine = LiveExecutionEngine(
        loop=loop,
        msgbus=msgbus,
        cache=cache,
        clock=clock,
        config=LiveExecEngineConfig(reconciliation=False, inflight_check_interval_ms=0),
    )
    client = _Client(
        loop=loop,
        account=account,
        msgbus=msgbus,
        cache=cache,
        clock=clock,
        instrument_provider=provider,
        config=CTraderExecClientConfig(
            client_id=credentials.client_id,
            client_secret=credentials.client_secret,
            access_token=credentials.access_token,
            trader_login=trader_login,
        ),
        logger=logger,
    )
    engine.register_client(client)
    engine.start()
    try:
        await _connect_and_check(
            settings,
            status,
            listening,
            result=result,
            account=account,
            client=client,
            engine=engine,
            cache=cache,
            clock=clock,
            instrument_id=instrument_id,
        )
    except Exception as e:
        result.failure = type(e).__name__
        if guard.targets is not None and not result.left_open:
            result.left_open = left_open(
                None,
                symbol=settings.symbol,
                pending_order_id=guard.targets.pending_order_id,
                position_id=guard.targets.position_id,
            )
    finally:
        try:
            await client._disconnect()
        finally:
            if engine.is_running:
                engine.stop()
            result.raw = _write_raw(recorder, settings.log_dir, account, trader_login)
            if result.raw is not None:
                status(f"The broker's messages are kept in {result.raw}")
    return result


async def _connect_and_check(
    settings: Settings,
    status: Callable[[str], None],
    listening: asyncio.Event | None,
    *,
    result: Result,
    account: GuardedAccount,
    client: _Client,
    engine: LiveExecutionEngine,
    cache: Cache,
    clock: LiveClock,
    instrument_id: InstrumentId,
) -> None:
    """Connect, let Nautilus reconcile the account, then run the check."""
    provider = account.instrument_provider
    guard = result.guard
    status("Connecting and reconciling the account.")
    await client._connect()
    for instrument in provider.list_all():
        cache.add_instrument(instrument)
    instrument = cache.instrument(instrument_id)
    if instrument is None:
        result.refusal = (f"{settings.symbol} did not load",)
        return
    mass_status = await client.generate_mass_status()
    if mass_status is not None:
        engine.reconcile_execution_mass_status(mass_status)
    await asyncio.sleep(settings.settle_secs)
    check = Check(
        settings=settings,
        account=account,
        client=client,
        engine=engine,
        cache=cache,
        instrument=instrument,
        symbol_id=provider.symbol_id(instrument_id),
        guard=guard,
        clock=clock,
        status=status,
    )
    await check.run(result, listening)


def _write_raw(
    recorder: Recorder,
    log_dir: pathlib.Path,
    account: CTraderAccountClient,
    trader_login: int,
) -> pathlib.Path | None:
    recording = recorder.recording
    if not any(True for _ in recording.messages()):
        return None
    try:
        account_id = account.account_id
    except Exception:
        return None
    stamp = datetime.datetime.now(datetime.UTC).strftime("%Y%m%dT%H%M%SZ")
    path = log_dir / f"verify_external_commands-{stamp}.raw.json"
    log_dir.mkdir(parents=True, exist_ok=True)
    path.write_bytes(
        record_execution.encode_raw(recording, account_id=account_id, login=trader_login)
    )
    return path


# -- The report ----------------------------------------------------------------------------


def plan(symbol: str) -> str:
    """What the run changes at the broker, printed before the owner confirms."""
    return "\n".join(
        [
            f"This run changes real orders at the broker, on {symbol} only.",
            "Place these by hand in the terminal first, and nothing else on that symbol:",
            "- one pending LIMIT order far from the market, of the minimum volume; attach a",
            "  stop-loss, a take-profit and an expiration to it if you can;",
            "- one position of the minimum volume with a stop-loss and a take-profit, the",
            "  stop-loss trailing.",
            "The run then, through the adapter:",
            f"1. moves the pending order's limit price {_MOVE_STEPS} price steps further from the "
            "market;",
            "2. raises its volume by one volume step, to the minimum plus one step;",
            "3. cancels it;",
            "4. asks to cancel an order id that does not exist (the broker refuses; nothing "
            "changes);",
            f"5. moves the position's stop-loss {_MOVE_STEPS} price steps further from the market;",
            "6. removes the position's take-profit;",
            "7. listens for trailing stop moves; change nothing in the terminal meanwhile.",
            "It never opens or closes a position and never places an order.",
            "Afterwards the position STAYS OPEN with its stop-loss: close it by hand in the "
            "terminal.",
        ],
    )


def format_report(result: Result) -> str:
    """The whole report: the findings, and what is left open. Names no identifier."""
    lines = ["Commands on orders the node did not place (live check)", ""]
    if result.failure is not None:
        lines += [f"The run ended early on {result.failure}; below is what it reached.", ""]
    if result.refusal:
        lines.append("Refused to act; nothing was changed at the broker:")
        lines.extend(f"- {reason}" for reason in result.refusal)
    for finding in result.findings:
        lines.append(f"{finding.status:<8} {finding.item}. {finding.title}")
        lines.extend(f"         {line}" for line in finding.detail)
    if result.findings:
        counts = Counter(f.status for f in result.findings)
        lines += [
            "",
            f"{OK} {counts[OK]}, {DIFFERS} {counts[DIFFERS]}, {UNKNOWN} {counts[UNKNOWN]}",
        ]
    if result.guard is not None and result.guard.refused:
        lines += ["", "The guard refused these requests:"]
        lines.extend(f"- {reason}" for reason in result.guard.refused)
    if result.left_open:
        lines += ["", *result.left_open]
    if result.raw is not None:
        lines += [
            "",
            f"Every broker message, unscrubbed (never commit it): {result.raw}",
            "Turn it into a scrubbed fixture with:",
            f"  uv run python scripts/record_execution.py --rescrub {result.raw} "
            "--output tests/fixtures/<name>.json",
        ]
    return "\n".join(lines)


def exit_code(result: Result) -> int:
    if result.failure is not None or result.refusal:
        return 1
    if result.guard is not None and result.guard.refused:
        return 1
    return 1 if any(f.status == DIFFERS for f in result.findings) else 0


# -- Command line --------------------------------------------------------------------------


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Cancel and amend the owner's orders on a LIVE account through the adapter.",
    )
    parser.add_argument(
        "--symbol",
        required=True,
        help="the broker's symbol name, as the terminal shows it",
    )
    parser.add_argument(
        "--send-commands",
        action="store_true",
        help="required: confirms that real orders are to be changed",
    )
    parser.add_argument(
        "--trailing-wait-secs",
        type=first_orders._positive_float,
        default=60.0,
        help="how long to listen for trailing stop moves at the end (default: 60)",
    )
    parser.add_argument(
        "--answer-wait-secs",
        type=first_orders._positive_float,
        default=30.0,
        help="how long to wait for each command's outcome (default: 30)",
    )
    parser.add_argument(
        "--log-dir",
        type=pathlib.Path,
        default=_REPO_ROOT / "tests" / "recordings",
        help="where the raw recording goes (default: tests/recordings/, ignored by git)",
    )
    return parser


def _credentials(env: dict[str, str]) -> tuple[AccountCredentials, int]:
    """The credentials and trader login from the env file; raises `MissingCredentials`."""
    missing_credentials = first_orders.MissingCredentials
    keys = (
        get_tokens.CLIENT_ID_KEY,
        get_tokens.CLIENT_SECRET_KEY,
        get_tokens.ACCESS_TOKEN_KEY,
        _TRADER_LOGIN_KEY,
    )
    missing = [key for key in keys if not env.get(key)]
    if missing:
        raise missing_credentials(f"missing in .env: {', '.join(missing)}")
    try:
        trader_login = int(env[_TRADER_LOGIN_KEY])
    except ValueError:
        raise missing_credentials(f"{_TRADER_LOGIN_KEY} in .env is not a number") from None
    # No refresh token: a refresh would rotate it, and this script does not write the new pair
    # back, so the one in .env would stop working. A run is minutes long. It also keeps the
    # account client's pre-connection, which the guard does not cover, to its application auth
    # and account list: only a refresh token would add a token refresh to it.
    credentials = AccountCredentials(
        client_id=env[get_tokens.CLIENT_ID_KEY],
        client_secret=env[get_tokens.CLIENT_SECRET_KEY],
        access_token=env[get_tokens.ACCESS_TOKEN_KEY],
        refresh_token=None,
        token_expires_at=None,
    )
    return credentials, trader_login


def main(argv: list[str] | None = None) -> int:
    try:
        args = build_arg_parser().parse_args(argv)
    except SystemExit as e:
        return e.code if isinstance(e.code, int) else 2
    # A presence check rather than `required=True`, so the refusal can say what it is for.
    if not args.send_commands:
        print(
            "Refused: this script changes real orders. Add --send-commands to run it.",
            file=sys.stderr,
        )
        return 2
    print(plan(args.symbol))
    try:
        typed = input(f"Type {args.symbol} again to go on: ")
    except EOFError:
        typed = ""
    if typed.strip() != args.symbol:
        print("Refused: the symbol typed does not match. Nothing was sent.", file=sys.stderr)
        return 2

    try:
        env = get_tokens.load_env(_REPO_ROOT / ".env")
        expired = first_orders.token_expired(env, time.time())
        credentials, trader_login = _credentials(env)
    except (OSError, first_orders.MissingCredentials) as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    if expired:
        print(
            "Refused: the access token in .env has expired, and this script does not refresh "
            "it. Issue a new one with scripts/get_tokens.py. Nothing was sent.",
            file=sys.stderr,
        )
        return 2

    settings = Settings(
        symbol=args.symbol,
        trailing_wait_secs=args.trailing_wait_secs,
        answer_wait_secs=args.answer_wait_secs,
        log_dir=args.log_dir,
    )
    try:
        result = asyncio.run(run_check(settings, credentials, trader_login))
    except KeyboardInterrupt:
        unread = left_open(None, symbol=args.symbol, pending_order_id=0, position_id=0)
        print(f"Stopped by hand. {unread[0]}")
        return 130
    except Exception as e:
        # The exception's own message could carry the broker's free text: only its type.
        print(f"error: {type(e).__name__}: the run failed; check the terminal", file=sys.stderr)
        return 1
    print(format_report(result))
    return exit_code(result)


if __name__ == "__main__":
    sys.exit(main())
