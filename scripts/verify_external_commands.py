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

With `--watch-partial-close` instead, the script changes nothing at the broker: its guard lets
through only reads and authentication. The owner places one position of at least twice the
minimum volume with a stop-loss and a take-profit, and nothing else on the symbol. The script
finds it as the commands do, checks that Nautilus holds its two legs, then asks the owner to
close half of it by hand in the terminal and listens for up to `--close-wait-secs`. It reports
how the broker told of the close and of the protective order's smaller volume, and whether the
legs in Nautilus followed. The rest of the position stays open: the owner closes it by hand.

    uv run python scripts/verify_external_commands.py --symbol EURUSD --watch-partial-close

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
from nautilus_ctrader.common.venue_book import entry_of, remaining_of
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

    Before `targets` is set, no command passes at all; a guard given only `READ_REQUESTS` never
    lets one pass.
    """

    def __init__(
        self, recorder: Recorder, allowed: frozenset[type[Message]] = ALLOWED_REQUESTS
    ) -> None:
        self.recorder = recorder
        self.allowed = allowed
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
        if type(payload) not in self.allowed:
            return f"{name} is not a request this run may send"
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
    reasons += _protected_position_reasons(positions)
    if len(positions) == 1 and positions[0].tradeData.volume != min_volume:
        volume = units_of(positions[0].tradeData.volume)
        reasons.append(f"the position's volume is {volume}, not the minimum {minimum}")
    if reasons:
        return Refusal(tuple(reasons))
    return Choice(pending[0], positions[0])


def _protected_position_reasons(positions: Sequence[om.ProtoOAPosition]) -> list[str]:
    """Why `positions` is not exactly one position with both levels."""
    if len(positions) != 1:
        return [f"{len(positions)} positions on the symbol, not exactly one"]
    reasons = []
    if not positions[0].HasField("stopLoss"):
        reasons.append("the position has no stop-loss")
    if not positions[0].HasField("takeProfit"):
        reasons.append("the position has no take-profit")
    return reasons


def choose_watched(
    snapshot: oa.ProtoOAReconcileRes, *, symbol_id: int, min_volume: int
) -> om.ProtoOAPosition | Refusal:
    """The owner's position on the symbol to watch, from the broker's snapshot.

    It must be at least twice the minimum, so that half of it can be closed, and alone on the
    symbol: no pending order may fill while the close is watched.
    """
    pending = [
        order
        for order in snapshot.order
        if order.tradeData.symbolId == symbol_id and order.orderType in PENDING_ORDER_TYPES
    ]
    positions = [p for p in snapshot.position if p.tradeData.symbolId == symbol_id]
    reasons: list[str] = []
    if pending:
        reasons.append(f"{len(pending)} pending orders on the symbol, not none")
    reasons += _protected_position_reasons(positions)
    if len(positions) == 1 and positions[0].tradeData.volume < 2 * min_volume:
        volume, least = units_of(positions[0].tradeData.volume), units_of(2 * min_volume)
        reasons.append(f"the position's volume is {volume}, under twice the minimum, {least}")
    if reasons:
        return Refusal(tuple(reasons))
    return positions[0]


def _open_by_venue_id(nautilus_orders: Iterable[Order]) -> dict[str, Order]:
    return {
        order.venue_order_id.value: order
        for order in nautilus_orders
        if order.venue_order_id is not None and order.is_open
    }


def _owner_legs(
    position_orders: Sequence[om.ProtoOAOrder],
    open_orders: dict[str, Order],
) -> tuple[om.ProtoOAOrder | None, dict[Level, list[Order]], list[str]]:
    """The position's entry, its open legs in Nautilus, and why they are not the owner's two.

    `position_orders` are the broker's orders of the position; its entry tells whether the node
    opened it.
    """
    reasons: list[str] = []
    entry = entry_of(position_orders)
    if entry is None:
        reasons.append("the position's order list names no entry, so it has no legs")
    elif parse_label(entry.tradeData.label) is not None:
        reasons.append("the position was opened by the node: its entry carries its label")
    legs: dict[Level, list[Order]] = {Level.STOP_LOSS: [], Level.TAKE_PROFIT: []}
    if entry is not None:
        for venue_order_id, order in open_orders.items():
            parsed = parse_leg_venue_order_id(venue_order_id)
            if parsed is not None and parsed[0] == entry.orderId:
                legs[parsed[1]].append(order)
        for level, name in ((Level.STOP_LOSS, "stop-loss"), (Level.TAKE_PROFIT, "take-profit")):
            if len(legs[level]) != 1:
                reasons.append(f"Nautilus holds {len(legs[level])} open {name} legs, not one")
    return entry, legs, reasons


def confirm(
    choice: Choice,
    position_orders: Sequence[om.ProtoOAOrder],
    nautilus_orders: Iterable[Order],
) -> Found | Refusal:
    """`choice` as Nautilus holds it: the pending order, and the position's two open legs."""
    open_orders = _open_by_venue_id(nautilus_orders)
    entry, legs, reasons = _owner_legs(position_orders, open_orders)
    pending = open_orders.get(str(choice.pending.orderId))
    if pending is None:
        reasons.append("Nautilus holds no open order for the pending order")
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


@dataclass(frozen=True)
class Watched:
    position: om.ProtoOAPosition
    # The protective order the broker listed at the start, if it listed one.
    protective: om.ProtoOAOrder | None
    stop_loss: ClientOrderId
    take_profit: ClientOrderId


def confirm_watched(
    position: om.ProtoOAPosition,
    protective: om.ProtoOAOrder | None,
    position_orders: Sequence[om.ProtoOAOrder],
    nautilus_orders: Iterable[Order],
) -> Watched | Refusal:
    """The position to watch as Nautilus holds it: its two open legs."""
    _, legs, reasons = _owner_legs(position_orders, _open_by_venue_id(nautilus_orders))
    if reasons:
        return Refusal(tuple(reasons))
    return Watched(
        position=position,
        protective=protective,
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
    lines = [
        _still_open(position, symbol)
        for position in snapshot.position
        if position.positionId == position_id
    ]
    if any(order.orderId == pending_order_id for order in snapshot.order):
        lines.append(f"The pending order on {symbol} is still open: cancel it by hand.")
    if not lines:
        lines.append(f"Nothing of the two is left open on {symbol}.")
    return tuple(lines)


def _still_open(position: om.ProtoOAPosition, symbol: str, *, volume: bool = False) -> str:
    """The line telling the owner to close `position` by hand; with its volume if `volume`."""
    size = f" at {units_of(position.tradeData.volume)}" if volume else ""
    if not position.HasField("stopLoss"):
        return (
            f"The position on {symbol} stays open{size} WITHOUT a stop-loss: close it by hand now."
        )
    trailing = ", trailing" if position.trailingStopLoss else ""
    target = f", take-profit at {position.takeProfit}" if position.HasField("takeProfit") else ""
    return (
        f"The position on {symbol} stays open{size} with its stop-loss at {position.stopLoss}"
        f"{trailing}{target}. Close it by hand in the terminal."
    )


def left_open_watched(
    snapshot: oa.ProtoOAReconcileRes | None, *, symbol: str, position_id: int
) -> tuple[str, ...]:
    """What is left of the watched position, as a line telling what to do."""
    if snapshot is None:
        return (
            f"The broker could not be read at the end: check {symbol} in the terminal and close "
            "the position by hand.",
        )
    position = _position(snapshot, position_id)
    if position is None:
        return (f"The position on {symbol} is closed; nothing is left open.",)
    return (_still_open(position, symbol, volume=True),)


# -- Watching a partial close --------------------------------------------------------------
#
# What the watch observed, picked out of the broker's pushed messages, and the verdicts on it;
# no I/O, as above.

_FILL_TYPES = (om.ORDER_FILLED, om.ORDER_PARTIAL_FILL)

# Where in the adapter each watch finding's answer is used.
SETTLES_DEAL = (
    "venue_book.py `_sync` TODO(verify): whether a fill always carries the position; and when "
    "the protective order's smaller volume comes relative to the closing deal"
)
SETTLES_ARRIVAL = (
    "venue_book.py `_foreign_fill`: whether, and when, the protective order's smaller volume "
    "arrives after a partial close; `_protective` TODO(verify): whether the broker replaces the "
    "protective order's id"
)
SETTLES_REMAINING = (
    "venue_book.py `remaining_of` TODO(verify), and its twin in reconciliation.py: whether a "
    "protective order's volume is its total or its rest, for a protective order reduced by a "
    "close (a partial trigger is still unrecorded)"
)
SETTLES_LEGS = (
    "the same question as the arrival, end to end: the legs follow the protective order's "
    "volume with no overfill"
)
SETTLES_POSITION = (
    "venue_book.py `_sync` TODO(verify): the event's position against the broker's own list"
)


def half_of(volume: int, step: int) -> int:
    """Half of a venue volume, rounded down to a whole number of volume steps."""
    return volume // 2 // step * step


def _executions(inbound: Sequence[Inbound], since: float) -> list[Inbound]:
    return [
        i
        for i in inbound
        if i.pushed and i.t >= since and isinstance(i.message, oa.ProtoOAExecutionEvent)
    ]


def _is_closing_deal(event: oa.ProtoOAExecutionEvent, position_id: int) -> bool:
    """A deal on the position by an order other than its protective one: a level did not fill."""
    return (
        event.HasField("deal")
        and event.deal.positionId == position_id
        and event.order.orderType != om.STOP_LOSS_TAKE_PROFIT
    )


def _is_reduction(event: oa.ProtoOAExecutionEvent, position_id: int, start_volume: int) -> bool:
    order = event.order
    return (
        event.executionType not in _FILL_TYPES
        and order.orderType == om.STOP_LOSS_TAKE_PROFIT
        and order.positionId == position_id
        and order.tradeData.volume < start_volume
    )


def _is_smaller(event: oa.ProtoOAExecutionEvent, position_id: int, start_volume: int) -> bool:
    position = event.position
    return (
        event.HasField("position")
        and position.positionId == position_id
        and (
            position.tradeData.volume < start_volume
            or position.positionStatus == om.POSITION_STATUS_CLOSED
        )
    )


def closing_deal(inbound: Sequence[Inbound], *, position_id: int, since: float) -> Inbound | None:
    """The first execution event since `since` carrying a closing deal of the position."""
    return next(
        (i for i in _executions(inbound, since) if _is_closing_deal(i.message, position_id)),
        None,
    )


def reduced_protective(
    inbound: Sequence[Inbound], *, position_id: int, start_volume: int, since: float
) -> Inbound | None:
    """The first execution event since `since` of the protective order with a smaller volume."""
    return next(
        (
            i
            for i in _executions(inbound, since)
            if _is_reduction(i.message, position_id, start_volume)
        ),
        None,
    )


def first_drop(
    inbound: Sequence[Inbound], *, position_id: int, start_volume: int, since: float
) -> Inbound | None:
    """The first message since `since` telling that the position became smaller."""
    for i in _executions(inbound, since):
        event = i.message
        if (
            _is_closing_deal(event, position_id)
            or _is_reduction(event, position_id, start_volume)
            or _is_smaller(event, position_id, start_volume)
        ):
            return i
    return None


def last_position_volume(
    inbound: Sequence[Inbound], *, position_id: int, since: float
) -> int | None:
    """The position's volume in the last execution event since `since` that carried it."""
    volume = None
    for i in _executions(inbound, since):
        if i.message.HasField("position") and i.message.position.positionId == position_id:
            volume = i.message.position.tradeData.volume
    return volume


def _event_text(event: oa.ProtoOAExecutionEvent) -> str:
    order = event.order
    kind = om.ProtoOAExecutionType.Name(event.executionType)
    closing = " closing" if order.closingOrder else ""
    return (
        f"{kind} of a{closing} {om.ProtoOAOrderType.Name(order.orderType)} order, "
        f"isServerEvent={event.isServerEvent}"
    )


def _nothing_closed(wait_secs: float) -> str:
    return f"the position's volume did not drop within {wait_secs:g} s"


def decide_closing_deal(
    deal: Inbound | None, *, start_volume: int, remaining: int | None, wait_secs: float
) -> Decision:
    """Whether the close arrived as an execution event carrying its deal, of the volume closed."""
    if deal is None:
        if remaining is not None and remaining < start_volume:
            return DIFFERS, (
                f"the broker lists {units_of(remaining)} of {units_of(start_volume)} now, but no "
                "execution event carried a closing deal",
            )
        return UNKNOWN, (_nothing_closed(wait_secs),)
    event = deal.message
    lines = [
        f"t={deal.t:.2f} s: {_event_text(event)}",
        f"deal volume {units_of(event.deal.volume)}, filled {units_of(event.deal.filledVolume)}",
        (
            f"the event carries the position, at {units_of(event.position.tradeData.volume)}"
            if event.HasField("position")
            else "the event carries no position"
        ),
    ]
    if remaining is not None and event.deal.filledVolume != start_volume - remaining:
        lines.append(
            f"but the position went from {units_of(start_volume)} to {units_of(remaining)}"
        )
        return DIFFERS, tuple(lines)
    return OK, tuple(lines)


def decide_reduction_arrives(
    reduced: Inbound | None,
    deal: Inbound | None,
    *,
    closed: bool,
    protective_id: int | None,
    listed: om.ProtoOAOrder | None,
    settle_secs: float,
    wait_secs: float,
) -> Decision:
    """Whether the protective order's smaller volume arrived as ORDER_REPLACED after the deal.

    `listed` is the protective order as the broker lists it at the end.
    """
    if not closed:
        return UNKNOWN, (_nothing_closed(wait_secs),)
    if reduced is None:
        return DIFFERS, (
            "no execution event of the protective order with a smaller volume within "
            f"{settle_secs:g} s of the close",
            (
                f"the broker now lists it at {units_of(listed.tradeData.volume)}"
                if listed is not None
                else "the broker now lists no protective order for the position"
            ),
        )
    event = reduced.message
    volume = units_of(event.order.tradeData.volume)
    lines = [f"t={reduced.t:.2f} s: {_event_text(event)}, volume {volume}"]
    after = deal is not None and reduced.t >= deal.t
    if deal is None:
        lines.append("no closing deal arrived to time it against")
    elif after:
        lines.append(f"{reduced.t - deal.t:.2f} s after the deal")
    else:
        lines.append(f"{deal.t - reduced.t:.2f} s before the deal")
    if protective_id is None:
        lines.append("the protective order was not listed at the start")
    elif event.order.orderId == protective_id:
        lines.append("the same protective order id as at the start")
    else:
        lines.append("a new protective order id")
    status = OK if after and event.executionType == om.ORDER_REPLACED else DIFFERS
    return status, tuple(lines)


def decide_reduced_total(
    reduced: Inbound | None, *, start_volume: int, remaining: int | None
) -> Decision:
    """Whether the smaller protective order's `tradeData.volume` is what is left of the position."""
    if reduced is None:
        return UNKNOWN, ("no protective order with a smaller volume arrived",)
    if remaining is None:
        return UNKNOWN, ("the position's remaining volume was not read",)
    volume = reduced.message.order.tradeData.volume
    line = f"tradeData.volume {units_of(volume)}; the position holds {units_of(remaining)}"
    if volume == remaining:
        return OK, (line, "the reduced total, not the volume closed")
    if volume == start_volume - remaining:
        return DIFFERS, (line, "the volume closed, not what is left")
    return DIFFERS, (line, "neither what is left nor what was closed")


def decide_executed_volume(
    reduced: Inbound | None,
    listed: om.ProtoOAOrder | None,
    *,
    closed: bool,
    remaining: int | None,
    wait_secs: float,
) -> Decision:
    """Whether `executedVolume` is set on the protective order, and what `remaining_of` reads."""
    if not closed:
        return UNKNOWN, (_nothing_closed(wait_secs),)
    orders = [
        (where, order)
        for where, order in (
            ("in the event", None if reduced is None else reduced.message.order),
            ("in the broker's list", listed),
        )
        if order is not None
    ]
    if not orders:
        return UNKNOWN, ("the protective order was neither pushed smaller nor listed",)
    if remaining is None:
        return UNKNOWN, ("the position's remaining volume was not read",)
    lines = []
    for where, order in orders:
        executed = (
            f"executedVolume {units_of(order.executedVolume)}"
            if order.HasField("executedVolume")
            else "executedVolume not set"
        )
        lines.append(f"{where}: {executed}; the adapter reads {units_of(remaining_of(order))} left")
    lines.append(f"the position holds {units_of(remaining)}")
    agree = all(remaining_of(order) == remaining for _, order in orders)
    return (OK if agree else DIFFERS), tuple(lines)


@dataclass(frozen=True)
class LegState:
    """A leg as Nautilus holds it; `quantity` is `None` when Nautilus holds no such order."""

    name: str
    quantity: Decimal | None
    is_open: bool


def decide_legs_follow(
    legs: Sequence[LegState],
    *,
    closed: bool,
    remaining: int | None,
    errors: int,
    wait_secs: float,
) -> Decision:
    """Whether each leg's quantity is the position's remaining volume, with no ERROR logged."""
    if not closed:
        return UNKNOWN, (_nothing_closed(wait_secs),)
    if remaining is None:
        return UNKNOWN, ("the position's remaining volume was not read",)
    expected = units_of(remaining)
    lines = [
        f"{leg.name}: "
        + ("not held" if leg.quantity is None else f"quantity {leg.quantity}")
        + ("" if leg.is_open else ", not open")
        for leg in legs
    ]
    lines.append(f"the position holds {expected}")
    lines.append(f"{errors} ERROR or overfill lines logged" + (" (see stderr)" if errors else ""))
    follow = all(leg.is_open and leg.quantity == expected for leg in legs)
    return (OK if follow and errors == 0 else DIFFERS), tuple(lines)


def decide_remaining(
    *,
    start_volume: int,
    half: int,
    closed: bool,
    remaining: int | None,
    last_event: int | None,
    wait_secs: float,
) -> Decision:
    """The position's remaining volume as the broker lists it, against its execution events."""
    if remaining is None:
        return UNKNOWN, ("the broker's snapshot was not read at the end",)
    lines = [
        f"from {units_of(start_volume)}, {units_of(half)} asked to close",
        (
            f"the broker lists {units_of(remaining)}"
            if remaining
            else "the broker lists the position no more"
        ),
    ]
    if last_event is not None:
        lines.append(f"the last execution event carried {units_of(last_event)}")
    if remaining == start_volume:
        if closed:
            return DIFFERS, (*lines, "the broker still lists the whole volume")
        return UNKNOWN, (_nothing_closed(wait_secs),)
    if remaining == 0:
        return DIFFERS, (*lines, "the whole position was closed, not part of it")
    if last_event is not None and last_event != remaining:
        return DIFFERS, tuple(lines)
    return OK, tuple(lines)


# -- The run -------------------------------------------------------------------------------


@dataclass(frozen=True)
class Finding:
    item: int
    title: str
    status: str
    detail: tuple[str, ...] = ()
    # The open question in the adapter the finding answers.
    settles: str = ""


@dataclass(frozen=True)
class Settings:
    symbol: str
    trailing_wait_secs: float = 60.0
    answer_wait_secs: float = 30.0
    # How long to watch for events after each command's answer.
    settle_secs: float = 2.0
    # How long the watch waits for the owner's partial close, then for what follows it.
    close_wait_secs: float = 120.0
    close_settle_secs: float = 10.0
    log_dir: pathlib.Path = _REPO_ROOT / "tests" / "recordings"


@dataclass(frozen=True)
class Address:
    demo_host: str = DEMO_HOST
    live_host: str = LIVE_HOST
    port: int = PROTOBUF_PORT
    tls: bool = True


_CHECK_TITLE = "Commands on orders the node did not place (live check)"
_WATCH_TITLE = "A partial close of the owner's position (live watch)"


@dataclass
class Result:
    title: str = _CHECK_TITLE
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


class _Run:
    """What both modes share: the clients, the guard, and the reads made through it."""

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

    def _min_quantity(self) -> Quantity:
        minimum = self._instrument.min_quantity
        return self._instrument.size_increment if minimum is None else minimum

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

    async def _position_orders(self, position_id: int) -> oa.ProtoOAOrderListByPositionIdRes | None:
        listed = await self._ask(
            oa.ProtoOAOrderListByPositionIdReq(
                ctidTraderAccountId=self._account.account_id,
                positionId=position_id,
            ),
        )
        answer = listed.answer
        return answer if isinstance(answer, oa.ProtoOAOrderListByPositionIdRes) else None


class Check(_Run):
    """The steps, carried out through the engine as a strategy would send them."""

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
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
        listed = await self._position_orders(choice.position.positionId)
        if listed is None:
            result.refusal = ("the position's order list could not be read",)
            return None
        found = confirm(
            choice,
            listed.order,
            self._cache.orders(instrument_id=self._instrument.id),
        )
        if isinstance(found, Refusal):
            result.refusal = found.reasons
            return None
        known = [o.orderId for o in (*snapshot.order, *listed.order)]
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


class Watch(_Run):
    """The watch of a partial close the owner makes by hand. Its guard lets no command pass."""

    async def run(self, result: Result, listening: asyncio.Event | None) -> None:
        """Find the position, watch its close, then say what is left open, however it ends."""
        watched = await self._find(result)
        if watched is None:
            return
        symbol, position_id = self._settings.symbol, watched.position.positionId
        # What a break-off says, until the broker is read at the end.
        result.left_open = left_open_watched(None, symbol=symbol, position_id=position_id)
        try:
            await self._watch(watched, listening, result)
        except Exception as e:
            result.failure = type(e).__name__
        final = None
        with contextlib.suppress(Exception):
            final = await self._snapshot()
        result.left_open = left_open_watched(final, symbol=symbol, position_id=position_id)

    async def _find(self, result: Result) -> Watched | None:
        snapshot = await self._snapshot()
        if snapshot is None:
            result.refusal = ("the broker's snapshot could not be read",)
            return None
        try:
            min_volume = volume_from_quantity(self._min_quantity())
        except Unsupported as e:
            result.refusal = (f"the instrument's minimum volume: {e}",)
            return None
        position = choose_watched(snapshot, symbol_id=self._symbol_id, min_volume=min_volume)
        if isinstance(position, Refusal):
            result.refusal = position.reasons
            return None
        listed = await self._position_orders(position.positionId)
        if listed is None:
            result.refusal = ("the position's order list could not be read",)
            return None
        watched = confirm_watched(
            position,
            _protective(snapshot, position.positionId),
            listed.order,
            self._cache.orders(instrument_id=self._instrument.id),
        )
        if isinstance(watched, Refusal):
            result.refusal = watched.reasons
            return None
        return watched

    async def _watch(
        self, watched: Watched, listening: asyncio.Event | None, result: Result
    ) -> None:
        settings = self._settings
        position_id = watched.position.positionId
        start_volume = watched.position.tradeData.volume
        half = half_of(start_volume, volume_from_quantity(self._instrument.size_increment))
        logged_from = len(result.logger.lines) if result.logger is not None else 0
        self._status(
            f"Found the position and its two legs. Now close {units_of(half)} of the position "
            f"by hand in the terminal. Listening up to {settings.close_wait_secs:g} s.",
        )
        start = self._recorder.now()
        if listening is not None:
            listening.set()
        loop = asyncio.get_running_loop()
        deadline = loop.time() + settings.close_wait_secs
        seen = None
        while seen is None and loop.time() < deadline and self._blocked() is None:
            await asyncio.sleep(_POLL_SECS)
            seen = first_drop(
                self._recorder.inbound,
                position_id=position_id,
                start_volume=start_volume,
                since=start,
            )
        if seen is not None:
            self._status(
                f"The position became smaller. Listening {settings.close_settle_secs:g} s more.",
            )
            await asyncio.sleep(settings.close_settle_secs)

        after = await self._snapshot()
        inbound = self._recorder.inbound
        deal = closing_deal(inbound, position_id=position_id, since=start)
        reduced = reduced_protective(
            inbound, position_id=position_id, start_volume=start_volume, since=start
        )
        remaining = None
        if after is not None:
            position = _position(after, position_id)
            remaining = 0 if position is None else position.tradeData.volume
        closed = seen is not None or (remaining is not None and remaining < start_volume)
        listed = _protective(after, position_id)
        legs = [
            _leg_state(name, self._cache.order(client_order_id))
            for name, client_order_id in (
                ("stop-loss leg", watched.stop_loss),
                ("take-profit leg", watched.take_profit),
            )
        ]
        logged = result.logger.lines[logged_from:] if result.logger is not None else []
        errors = sum(level == "error" or "overfill" in line.lower() for level, line in logged)
        wait = settings.close_wait_secs
        protective_id = None if watched.protective is None else watched.protective.orderId
        decisions: list[tuple[str, str, Decision]] = [
            (
                "The close arrives as an execution event carrying its deal",
                SETTLES_DEAL,
                decide_closing_deal(
                    deal, start_volume=start_volume, remaining=remaining, wait_secs=wait
                ),
            ),
            (
                "The protective order's smaller volume arrives as ORDER_REPLACED after the deal",
                SETTLES_ARRIVAL,
                decide_reduction_arrives(
                    reduced,
                    deal,
                    closed=closed,
                    protective_id=protective_id,
                    listed=listed,
                    settle_secs=settings.close_settle_secs,
                    wait_secs=wait,
                ),
            ),
            (
                "The smaller protective order's tradeData.volume is the reduced total",
                SETTLES_REMAINING,
                decide_reduced_total(reduced, start_volume=start_volume, remaining=remaining),
            ),
            (
                "executedVolume on the protective order, as remaining_of reads it",
                SETTLES_REMAINING,
                decide_executed_volume(
                    reduced, listed, closed=closed, remaining=remaining, wait_secs=wait
                ),
            ),
            (
                "The legs in Nautilus hold the position's remaining volume, with no ERROR",
                SETTLES_LEGS,
                decide_legs_follow(
                    legs, closed=closed, remaining=remaining, errors=errors, wait_secs=wait
                ),
            ),
            (
                "The position's remaining volume as the broker lists it",
                SETTLES_POSITION,
                decide_remaining(
                    start_volume=start_volume,
                    half=half,
                    closed=closed,
                    remaining=remaining,
                    last_event=last_position_volume(inbound, position_id=position_id, since=start),
                    wait_secs=wait,
                ),
            ),
        ]
        result.findings += [
            Finding(item, title, status, detail, settles)
            for item, (title, settles, (status, detail)) in enumerate(decisions, start=1)
        ]


def _leg_state(name: str, order: Order | None) -> LegState:
    if order is None:
        return LegState(name, None, is_open=False)
    return LegState(name, order.quantity.as_decimal(), is_open=order.is_open)


def _protective(
    snapshot: oa.ProtoOAReconcileRes | None, position_id: int
) -> om.ProtoOAOrder | None:
    if snapshot is None:
        return None
    return next(
        (
            o
            for o in snapshot.order
            if o.positionId == position_id and o.orderType == om.STOP_LOSS_TAKE_PROFIT
        ),
        None,
    )


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
    watch: bool = False,
) -> Result:
    """Connect, find the owner's orders, carry out the steps and keep the broker's messages.

    With `watch`, watch the owner's partial close instead, behind a guard that lets no command
    pass. The raw recording is written to `settings.log_dir` however the run ends, if it holds
    any message. `listening` is set when the run starts listening: for trailing moves, or for
    the close.
    """
    address = address or Address()
    recorder = Recorder(record_execution.Recording(int(time.time() * 1000)))
    guard = Guard(recorder, READ_REQUESTS if watch else ALLOWED_REQUESTS)
    logger = ScriptLogger()
    result = Result(title=_WATCH_TITLE if watch else _CHECK_TITLE, guard=guard, logger=logger)
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
            run_type=Watch if watch else Check,
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
            name = "watch_partial_close" if watch else "verify_external_commands"
            result.raw = _write_raw(recorder, settings.log_dir, account, trader_login, name)
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
    run_type: type[Check | Watch],
) -> None:
    """Connect, let Nautilus reconcile the account, then run the check or the watch."""
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
    check = run_type(
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
    name: str,
) -> pathlib.Path | None:
    recording = recorder.recording
    if not any(True for _ in recording.messages()):
        return None
    try:
        account_id = account.account_id
    except Exception:
        return None
    stamp = datetime.datetime.now(datetime.UTC).strftime("%Y%m%dT%H%M%SZ")
    path = log_dir / f"{name}-{stamp}.raw.json"
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


def watch_plan(symbol: str, close_wait_secs: float) -> str:
    """What the watch asks of the owner, printed before the owner confirms."""
    return "\n".join(
        [
            f"This run watches a partial close on {symbol}. The script itself changes nothing at "
            "the broker:",
            "it sends no order, amend, cancel or close; it only reads and listens.",
            "Place this by hand in the terminal first, and nothing else on that symbol:",
            "- one position of at least twice the minimum volume, with a stop-loss and a",
            "  take-profit.",
            "The run finds it and checks that Nautilus holds its two levels. Then it asks you to",
            "close half of the position by hand in the terminal, and listens up to "
            f"{close_wait_secs:g} s.",
            "Afterwards the rest of the position STAYS OPEN with its levels: close it by hand in "
            "the terminal.",
        ],
    )


def format_report(result: Result) -> str:
    """The whole report: the findings, and what is left open. Names no identifier."""
    lines = [result.title, ""]
    if result.failure is not None:
        lines += [f"The run ended early on {result.failure}; below is what it reached.", ""]
    if result.refusal:
        lines.append("Refused to act; nothing was changed at the broker:")
        lines.extend(f"- {reason}" for reason in result.refusal)
    for finding in result.findings:
        lines.append(f"{finding.status:<8} {finding.item}. {finding.title}")
        lines.extend(f"         {line}" for line in finding.detail)
        if finding.settles:
            lines.append(f"         settles: {finding.settles}")
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
        description=(
            "Cancel and amend the owner's orders on a LIVE account through the adapter, or "
            "watch the owner's partial close of a position."
        ),
    )
    parser.add_argument(
        "--symbol",
        required=True,
        help="the broker's symbol name, as the terminal shows it",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--send-commands",
        action="store_true",
        help="confirms that real orders are to be changed",
    )
    mode.add_argument(
        "--watch-partial-close",
        action="store_true",
        help="watch a partial close made by hand; sends no command",
    )
    parser.add_argument(
        "--close-wait-secs",
        type=first_orders._positive_float,
        default=120.0,
        help="how long the watch waits for the partial close (default: 120)",
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
    if not (args.send_commands or args.watch_partial_close):
        print(
            "Refused: this script changes real orders. Add --send-commands to run it, or "
            "--watch-partial-close to watch a partial close made by hand.",
            file=sys.stderr,
        )
        return 2
    watch = args.watch_partial_close
    print(watch_plan(args.symbol, args.close_wait_secs) if watch else plan(args.symbol))
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
        close_wait_secs=args.close_wait_secs,
        log_dir=args.log_dir,
    )
    try:
        result = asyncio.run(run_check(settings, credentials, trader_login, watch=watch))
    except KeyboardInterrupt:
        if watch:
            unread = left_open_watched(None, symbol=args.symbol, position_id=0)
        else:
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
