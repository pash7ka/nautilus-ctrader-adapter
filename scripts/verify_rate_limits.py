"""Read-only live probe of the venue's request limits.

Measures, against one live account in one run of a minute or two:

1. how many historical (trendbar) requests the venue answers at once, and whether it counts
   them in a sliding one-second window or per calendar second;
2. which history lists are limited the same way;
3. whether a refusal blocks only its own payload type or every limited one, tested against
   another type the run showed limited (tick data, if no list is);
4. where the general limit starts refusing, with the small `ProtoOATraderReq`;
5. what `retryAfter` says, and whether waiting it out is enough.

**Read-only by construction.** Every request passes through `ReadOnlyGuard`, which refuses any
payload class outside `ALLOWED_REQUESTS` before it is written. Nothing that creates, amends or
closes an order is in that set.

**No local pacing.** The connection is built without a `RateLimiter`, so each request leaves
exactly when the probe schedules it, and every refusal is the venue's own.

**Bounded.** A cap on requests per run and per step; between steps at least 3 s, and at least
the step's largest `retryAfter` plus 2 s; a hard time limit. Ctrl+C stops at the next send and
still prints what was measured; a second Ctrl+C quits at once.

Run from the repository root; credentials and, by default, the trader login come from `.env`:

    uv run python scripts/verify_rate_limits.py --dry-run
    uv run python scripts/verify_rate_limits.py

The plan is printed first. The account's host is then looked up from the account list, and
nothing is probed until `y` is typed. Each request's payload type, timing, outcome and
`retryAfter` are appended to `logs/verify_rate_limits-<stamp>.raw.jsonl`; no credential and no
account identifier is written there. `--from-log <path>` prints that run's report again,
offline.

Exit code: 0 when every step ran or was skipped for a stated reason, 1 otherwise, 2 when the
plan is invalid or the run was declined.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import dataclasses
import datetime
import importlib.util
import json
import math
import pathlib
import signal
import sys
import time
from collections import Counter
from collections.abc import Awaitable, Callable, Collection, Iterator, Sequence
from dataclasses import dataclass
from typing import TextIO

from google.protobuf.message import Message

from nautilus_ctrader.common.account import (
    AccountCredentials,
    account_host,
    request_granted_accounts,
)
from nautilus_ctrader.common.connection import CTraderConnection
from nautilus_ctrader.common.errors import (
    CTraderConnectionError,
    CTraderRequestError,
    CTraderTimeoutError,
)
from nautilus_ctrader.constants import DEMO_HOST, LIVE_HOST, PROTOBUF_PORT
from nautilus_ctrader.messages import OpenApiCommonMessages_pb2 as common
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as om

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]

# scripts/ is not a package; get_tokens.py is loaded by file path, as verify_live_data.py does.
_GET_TOKENS_SPEC = importlib.util.spec_from_file_location(
    "get_tokens",
    pathlib.Path(__file__).resolve().with_name("get_tokens.py"),
)
get_tokens = importlib.util.module_from_spec(_GET_TOKENS_SPEC)
sys.modules[_GET_TOKENS_SPEC.name] = get_tokens
_GET_TOKENS_SPEC.loader.exec_module(get_tokens)

_TRADER_LOGIN_KEY = "CTRADER_TRADER_LOGIN"

OTHER_SESSIONS = (
    "Stop any other session using these credentials or this account (for example a running "
    "node) for the duration of the run: its requests count against the same limits."
)

# The only payload classes this script may send. Nothing that creates, amends or closes an
# order is listed, so a read-only run is a property of this set rather than of each call site.
ALLOWED_REQUESTS: frozenset[type[Message]] = frozenset(
    {
        oa.ProtoOAApplicationAuthReq,
        oa.ProtoOAGetAccountListByAccessTokenReq,
        oa.ProtoOAAccountAuthReq,
        oa.ProtoOAVersionReq,
        oa.ProtoOATraderReq,
        # Only to resolve `--symbol` at setup.
        oa.ProtoOASymbolsListReq,
        oa.ProtoOASymbolByIdReq,
        oa.ProtoOAGetTrendbarsReq,
        oa.ProtoOAGetTickDataReq,
        oa.ProtoOADealListReq,
        oa.ProtoOAOrderListReq,
        oa.ProtoOACashFlowHistoryListReq,
        oa.ProtoOAOrderDetailsReq,
        common.ProtoHeartbeatEvent,
    },
)

# What a probed request asks for, by the name the report uses.
TRENDBARS = "trendbars"
TICK_DATA = "tick data"
DEAL_LIST = "deal list"
ORDER_LIST = "order list"
CASH_FLOW = "cash-flow list"
ORDER_DETAILS = "order details"
TRADER = "trader"
# A plan placeholder, replaced at run time by the first other type shown to be limited.
SIBLING = "limited sibling"

PROBED_REQUESTS: dict[str, type[Message]] = {
    TRENDBARS: oa.ProtoOAGetTrendbarsReq,
    TICK_DATA: oa.ProtoOAGetTickDataReq,
    DEAL_LIST: oa.ProtoOADealListReq,
    ORDER_LIST: oa.ProtoOAOrderListReq,
    CASH_FLOW: oa.ProtoOACashFlowHistoryListReq,
    ORDER_DETAILS: oa.ProtoOAOrderDetailsReq,
    TRADER: oa.ProtoOATraderReq,
}

# An attempt's outcome is `ANSWERED`, `TIMEOUT`, or the venue's error code.
ANSWERED = "answered"
TIMEOUT = "timeout"
BLOCKED = "BLOCKED_PAYLOAD_TYPE"
REFUSALS = frozenset({BLOCKED, "REQUEST_FREQUENCY_EXCEEDED"})

NO_SIBLING = "no other limited type to test with"
IMPLAUSIBLE = "implausible; unit unconfirmed (seconds expected)"

# Step kinds.
BURST = "burst"
STREAM = "stream"
BLOCK = "block"

# What a request was for within its step.
ROLE_BURST = "burst"
ROLE_STREAM = "stream"
ROLE_TRIGGER = "trigger"
ROLE_FOLLOW_UP = "follow-up"
ROLE_AFTER_WAIT = "after-wait"

# The measurement a step feeds.
GROUP_HISTORICAL_BURSTS = "historical bursts"
GROUP_HISTORICAL_STREAMS = "historical streams"
GROUP_TYPES = "types"
GROUP_TICK_DATA = "tick data"
GROUP_SHARING = "sharing"
GROUP_SHARING_REVERSE = "sharing, reversed"
GROUP_DEFAULT = "default"

# Step statuses.
RAN = "ran"
SKIPPED = "skipped"
STOPPED = "stopped"
NOT_RUN = "not run"

SLIDING = "sliding"
FIXED = "fixed"
PER_TYPE = "per payload type"
SHARED = "shared"

_HISTORICAL_BURSTS = (5, 6, 8, 10)
# (rate, start phase), chosen so the two window shapes separate at any clock offset; see
# `test_the_window_shape_is_told_at_any_clock_offset`.
_HISTORICAL_STREAMS = ((5.0, 0.5), (6.0, 0.5), (7.0, 0.85))
_TYPE_BURST = 8
_TYPE_KINDS = (DEAL_LIST, ORDER_LIST, CASH_FLOW, ORDER_DETAILS)
# A live start-up was refused after 12 trendbar requests in about 2 s.
_BLOCK_TRIGGER = 12
_DEFAULT_BURSTS = (50, 60, 80)
# Application auth and the account list on the demo host; application auth, account auth and
# the symbols list that resolves `--symbol` on the account's host.
_SETUP_REQUESTS = 5

_TRENDBAR_WINDOW_MS = 3_600_000
_TICK_WINDOW_MS = 60_000
_LIST_WINDOW_MS = 2 * 86_400_000
# The `retryAfter` the plan's run-time estimate assumes.
_ESTIMATE_RETRY_AFTER_SECS = 1.0

_WINDOW_SECS = 1.0
# How far a local send time may sit from the venue's arrival time. Generous to both window
# shapes, so jitter can make a verdict inconclusive but never wrong.
_TIMING_TOLERANCE_SECS = 0.05
# Calendar-second phases tried, in hundredths of a second.
_PHASE_GRID = 100
_PAUSE_SLICE_SECS = 0.25
_MAX_LISTED_TIMES = 12


@dataclass(frozen=True)
class Endpoints:
    demo_host: str = DEMO_HOST
    live_host: str = LIVE_HOST
    port: int = PROTOBUF_PORT
    tls: bool = True


_VENUE_ENDPOINTS = Endpoints()


@dataclass(frozen=True)
class Settings:
    trader_login: int | None = None
    symbol: str = "EURUSD"
    max_requests: int = 400
    max_step_requests: int = 100
    min_gap_secs: float = 3.0
    gap_margin_secs: float = 2.0
    retry_margin_secs: float = 0.5
    max_wait_secs: float = 60.0
    max_run_secs: float = 600.0
    request_timeout_secs: float = 10.0
    stream_secs: float = 3.0


@dataclass(frozen=True)
class Step:
    name: str
    kind: str
    request: str
    count: int
    group: str
    rate: float | None = None
    follow_up: str | None = None
    # The fraction of a local second the step starts at. Mid-second keeps a burst inside one
    # calendar second.
    phase: float = 0.5

    @property
    def planned(self) -> int:
        """The most this step sends: a block step adds a follow-up and a check after the wait."""
        return self.count + (2 if self.kind == BLOCK else 0)


@dataclass(frozen=True)
class Attempt:
    """One request: when it left, when its answer came, and what the answer was.

    Times are the probe clock's, in seconds. `index` is the send order within the step.
    """

    step: str
    request: str
    role: str
    index: int
    sent_at: float
    answered_at: float
    outcome: str
    retry_after: int | None = None


@dataclass(frozen=True)
class StepResult:
    step: Step
    attempts: tuple[Attempt, ...]
    status: str
    note: str = ""


@dataclass(frozen=True)
class Finding:
    number: int
    title: str
    measured: str
    evidence: tuple[str, ...]
    conclusion: str


class ReadOnlyViolation(RuntimeError):
    """A payload outside `ALLOWED_REQUESTS` reached the guard."""


class SetupError(RuntimeError):
    """The run cannot start. The message names no identifier and is safe to print."""


class RunStopped(Exception):
    """The run ends here; the message says why, and what was measured so far is kept."""


class StopRequested(RunStopped):
    pass


class RequestCapReached(RunStopped):
    pass


class StepCapReached(RunStopped):
    pass


class WaitTooLong(RunStopped):
    pass


class RunTooLong(RunStopped):
    pass


class ReadOnlyGuard:
    """The read-only guarantee and the run's request cap, checked before every write.

    `stop_requested` is set by Ctrl+C; the next send then raises `StopRequested`.
    """

    def __init__(self, max_requests: int) -> None:
        self.max_requests = max_requests
        self.sent = 0
        self.stop_requested = False

    def check(self, payload: Message) -> None:
        if type(payload) not in ALLOWED_REQUESTS:
            raise ReadOnlyViolation(
                f"{type(payload).__name__} is not one of this script's read-only requests",
            )
        if self.stop_requested:
            raise StopRequested("stopped by Ctrl+C")
        if self.sent >= self.max_requests:
            raise RequestCapReached(f"the run's cap of {self.max_requests} requests is reached")
        self.sent += 1


class GuardedConnection:
    """A connection whose every request passes `ReadOnlyGuard.check()` first."""

    def __init__(self, connection: CTraderConnection, guard: ReadOnlyGuard) -> None:
        self._connection = connection
        self._guard = guard

    async def request(self, payload: Message, *, timeout_secs: float | None = None) -> Message:
        self._guard.check(payload)
        return await self._connection.request(payload, timeout_secs=timeout_secs)


class _StderrLogger:
    """Enough of the Nautilus `Logger` for `CTraderConnection`; only warnings and errors show."""

    def debug(self, message: str, color=None) -> None:
        pass

    def info(self, message: str, color=None) -> None:
        pass

    def warning(self, message: str, color=None) -> None:
        print(f"warning: {message}", file=sys.stderr)

    def error(self, message: str, color=None) -> None:
        print(f"error: {message}", file=sys.stderr)

    def exception(self, message: str, ex: BaseException) -> None:
        print(f"error: {message}: {type(ex).__name__}", file=sys.stderr)


# -- The plan -------------------------------------------------------------------------------


def build_plan(settings: Settings) -> tuple[Step, ...]:
    secs = settings.stream_secs
    steps = [
        Step(f"trendbars, burst of {n}", BURST, TRENDBARS, n, GROUP_HISTORICAL_BURSTS)
        for n in _HISTORICAL_BURSTS
    ]
    steps += [
        Step(
            f"trendbars, {rate:g}/s for {secs:g} s",
            STREAM,
            TRENDBARS,
            max(1, round(rate * secs)),
            GROUP_HISTORICAL_STREAMS,
            rate=rate,
            phase=phase,
        )
        for rate, phase in _HISTORICAL_STREAMS
    ]
    steps += [
        Step(f"{kind}, burst of {_TYPE_BURST}", BURST, kind, _TYPE_BURST, GROUP_TYPES)
        for kind in _TYPE_KINDS
    ]
    steps += [
        Step(
            f"{TICK_DATA}, burst of {_TYPE_BURST}",
            BURST,
            TICK_DATA,
            _TYPE_BURST,
            GROUP_TICK_DATA,
        ),
        Step(
            f"trendbars burst of {_BLOCK_TRIGGER}, then one {SIBLING} request while blocked",
            BLOCK,
            TRENDBARS,
            _BLOCK_TRIGGER,
            GROUP_SHARING,
            follow_up=SIBLING,
        ),
        Step(
            f"{SIBLING} burst of {_BLOCK_TRIGGER}, then one trendbars request while blocked",
            BLOCK,
            SIBLING,
            _BLOCK_TRIGGER,
            GROUP_SHARING_REVERSE,
            follow_up=TRENDBARS,
        ),
    ]
    steps += [
        Step(f"{TRADER}, burst of {n}", BURST, TRADER, n, GROUP_DEFAULT) for n in _DEFAULT_BURSTS
    ]
    return tuple(steps)


def estimated_run_secs(plan: Sequence[Step], settings: Settings) -> float:
    """Every step run, each refusal answered with `_ESTIMATE_RETRY_AFTER_SECS`."""
    retry_after = _ESTIMATE_RETRY_AFTER_SECS
    gap = max(settings.min_gap_secs, retry_after + settings.gap_margin_secs)
    total = 0.0
    for step in plan:
        # The gap, then on average half a second until the step's start phase.
        total += gap + 0.5
        if step.kind == STREAM and step.rate:
            total += step.count / step.rate
        elif step.kind == BLOCK:
            total += retry_after + settings.retry_margin_secs
    return total


def validate_plan(plan: Sequence[Step], settings: Settings) -> None:
    """Raise `ValueError` if the plan could break either cap."""
    for step in plan:
        if step.planned > settings.max_step_requests:
            raise ValueError(
                f"step '{step.name}' sends up to {step.planned} requests, over the per-step "
                f"cap of {settings.max_step_requests}",
            )
    total = _SETUP_REQUESTS + sum(step.planned for step in plan)
    if total > settings.max_requests:
        raise ValueError(
            f"the plan sends up to {total} requests, over the cap of {settings.max_requests}",
        )


def format_plan(plan: Sequence[Step], settings: Settings) -> str:
    login = settings.trader_login or f"from {_TRADER_LOGIN_KEY} in .env"
    lines = [
        "cTrader request-limit probe: plan (read-only, no local pacing)",
        f"account: trader login {login}; symbol {settings.symbol}",
        f"setup: {_SETUP_REQUESTS} requests (authentication, account list, symbols list)",
    ]
    for number, step in enumerate(plan, 1):
        extra = ""
        if step.kind == BLOCK:
            extra = " (+1 while blocked, +1 after retryAfter)"
        if SIBLING in (step.request, step.follow_up):
            extra += f"; the {SIBLING} is the first type above shown limited, if any"
        if step.group == GROUP_TICK_DATA:
            extra += ", only if no list type above is limited"
        if step.request == ORDER_DETAILS:
            extra += ", only if the order list served an order id"
        lines.append(f"  {number:2d}. {step.name}: {step.count} requests{extra}")
    total = _SETUP_REQUESTS + sum(step.planned for step in plan)
    estimate = 5 * math.ceil(estimated_run_secs(plan, settings) / 5)
    lines += [
        f"at most {total} requests (cap {settings.max_requests}, at most "
        f"{settings.max_step_requests} per step)",
        f"between steps: at least {settings.min_gap_secs:g} s, and at least the step's largest "
        f"retryAfter + {settings.gap_margin_secs:g} s; a retryAfter over "
        f"{settings.max_wait_secs:g} s stops the run",
        f"expected run time: about {estimate} s with retryAfter = "
        f"{_ESTIMATE_RETRY_AFTER_SECS:g} s; hard limit {settings.max_run_secs:g} s",
        OTHER_SESSIONS,
    ]
    return "\n".join(lines)


# -- The run --------------------------------------------------------------------------------


class RequestFactory:
    """Builds each probed request, and keeps the first order id an order list serves."""

    def __init__(self, account_id: int, symbol_id: int, clock: Callable[[], float]) -> None:
        self._account_id = account_id
        self._symbol_id = symbol_id
        self._clock = clock
        self.order_id: int | None = None

    def make(self, kind: str) -> Message:
        now_ms = int(self._clock() * 1000)
        window = {"fromTimestamp": now_ms - _LIST_WINDOW_MS, "toTimestamp": now_ms}
        account = self._account_id
        if kind == TRENDBARS:
            return oa.ProtoOAGetTrendbarsReq(
                ctidTraderAccountId=account,
                symbolId=self._symbol_id,
                period=om.M1,
                fromTimestamp=now_ms - _TRENDBAR_WINDOW_MS,
                toTimestamp=now_ms,
                count=1,
            )
        if kind == DEAL_LIST:
            return oa.ProtoOADealListReq(ctidTraderAccountId=account, maxRows=1, **window)
        if kind == ORDER_LIST:
            return oa.ProtoOAOrderListReq(ctidTraderAccountId=account, **window)
        if kind == CASH_FLOW:
            return oa.ProtoOACashFlowHistoryListReq(ctidTraderAccountId=account, **window)
        if kind == ORDER_DETAILS:
            if self.order_id is None:
                raise ValueError("no order id is known")
            return oa.ProtoOAOrderDetailsReq(ctidTraderAccountId=account, orderId=self.order_id)
        if kind == TICK_DATA:
            return oa.ProtoOAGetTickDataReq(
                ctidTraderAccountId=account,
                symbolId=self._symbol_id,
                type=om.BID,
                fromTimestamp=now_ms - _TICK_WINDOW_MS,
                toTimestamp=now_ms,
            )
        if kind == TRADER:
            return oa.ProtoOATraderReq(ctidTraderAccountId=account)
        raise ValueError(f"unknown request kind {kind!r}")

    def note(self, response: Message | None) -> None:
        if (
            self.order_id is None
            and isinstance(response, oa.ProtoOAOrderListRes)
            and response.order
        ):
            self.order_id = response.order[0].orderId


class AttemptLog:
    """The JSONL record of a run: payload type, timing, outcome and `retryAfter`, nothing else.

    Times are seconds since `origin`. Each line is flushed, so a run killed outright keeps
    everything sent before it.
    """

    def __init__(self, path: pathlib.Path, origin: float) -> None:
        self.path = path
        self._origin = origin
        self._file: TextIO | None = None

    def __enter__(self) -> AttemptLog:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._file = self.path.open("a", encoding="utf-8")
        return self

    def __exit__(self, *exc_info: object) -> None:
        if self._file is not None:
            self._file.close()
            self._file = None

    def attempt(self, attempt: Attempt) -> None:
        self._write(
            {
                "step": attempt.step,
                "payload": PROBED_REQUESTS[attempt.request].__name__,
                "role": attempt.role,
                "index": attempt.index,
                "sent_at": round(attempt.sent_at - self._origin, 4),
                "answered_at": round(attempt.answered_at - self._origin, 4),
                "outcome": attempt.outcome,
                "retry_after": attempt.retry_after,
            },
        )

    def step(self, result: StepResult) -> None:
        self._write({"step": result.step.name, "status": result.status, "note": result.note})

    def _write(self, record: dict) -> None:
        if self._file is not None:
            self._file.write(json.dumps(record) + "\n")
            self._file.flush()


def read_log(path: pathlib.Path, plan: Sequence[Step]) -> list[StepResult]:
    """The step results a run's log records, as the run itself returned them.

    The sibling is chosen as the run chose it. A plan step the log never reached is `NOT_RUN`.
    """
    kinds = {cls.__name__: kind for kind, cls in PROBED_REQUESTS.items()}
    attempts: dict[str, list[Attempt]] = {}
    ends: dict[str, dict] = {}
    with path.open(encoding="utf-8") as file:
        for line in file:
            record = json.loads(line)
            if "status" in record:
                ends[record["step"]] = record
                continue
            attempts.setdefault(record["step"], []).append(
                Attempt(
                    record["step"],
                    kinds[record["payload"]],
                    record["role"],
                    record["index"],
                    record["sent_at"],
                    record["answered_at"],
                    record["outcome"],
                    record["retry_after"],
                ),
            )
    results: list[StepResult] = []
    for step in plan:
        if SIBLING in (step.request, step.follow_up) and (sibling := choose_sibling(results)):
            step = _with_sibling(step, sibling)
        end = ends.get(step.name)
        if end is None:
            results.append(StepResult(step, (), NOT_RUN))
        else:
            step_attempts = tuple(attempts.get(step.name, ()))
            results.append(StepResult(step, step_attempts, end["status"], end["note"]))
    return results


class Prober:
    """Runs the plan's steps one by one, recording every attempt as it ends.

    A `RunStopped` or a lost connection ends the run: the step it hit is `STOPPED`, the rest
    `NOT_RUN`, and everything recorded so far is kept.
    """

    def __init__(
        self,
        connection: GuardedConnection,
        guard: ReadOnlyGuard,
        requests: RequestFactory,
        settings: Settings,
        log: AttemptLog,
        *,
        clock: Callable[[], float],
        sleep: Callable[[float], Awaitable[None]],
    ) -> None:
        self._connection = connection
        self._guard = guard
        self._requests = requests
        self._settings = settings
        self._log = log
        self._clock = clock
        self._sleep = sleep
        self._attempts: list[Attempt] = []
        self._step_first = 0
        self._step_sent = 0
        self._deadline = clock() + settings.max_run_secs

    async def run(self, plan: Sequence[Step]) -> list[StepResult]:
        results: list[StepResult] = []
        stopped = False
        previous: tuple[Attempt, ...] = ()
        for step in plan:
            if SIBLING in (step.request, step.follow_up) and (sibling := choose_sibling(results)):
                step = _with_sibling(step, sibling)
            if stopped:
                result = StepResult(step, (), NOT_RUN)
            elif skip := self._skip_reason(step, results):
                result = StepResult(step, (), SKIPPED, skip)
            else:
                self._step_first = len(self._attempts)
                self._step_sent = 0
                status, note = RAN, ""
                try:
                    await self._gap(previous)
                    await self._until(math.floor(self._clock() - step.phase) + 1 + step.phase)
                    await self._run_step(step)
                except (RunStopped, CTraderConnectionError) as e:
                    stopped = True
                    status, note = STOPPED, _reason(e)
                result = StepResult(step, tuple(self._attempts[self._step_first :]), status, note)
                previous = result.attempts
            results.append(result)
            self._log.step(result)
        return results

    def _skip_reason(self, step: Step, results: Sequence[StepResult]) -> str:
        if step.request == ORDER_DETAILS and self._requests.order_id is None:
            return "the order list served no order, so there is no order id to ask about"
        if step.group == GROUP_TICK_DATA and (sibling := choose_sibling(results)):
            return f"not needed: the {sibling} is limited"
        if SIBLING in (step.request, step.follow_up):
            return NO_SIBLING
        return ""

    async def _run_step(self, step: Step) -> None:
        if step.kind == BURST:
            await self._burst(step, step.request, step.count, ROLE_BURST)
        elif step.kind == STREAM:
            await self._stream(step)
        else:
            await self._block(step)

    async def _burst(self, step: Step, kind: str, count: int, role: str) -> None:
        outcomes = await asyncio.gather(
            *(self._send(step, kind, role, index) for index in range(count)),
            return_exceptions=True,
        )
        _raise_first(outcomes)

    async def _stream(self, step: Step) -> None:
        assert step.rate is not None
        start = self._clock()
        tasks: list[asyncio.Task] = []
        try:
            for index in range(step.count):
                await self._until(start + index / step.rate)
                tasks.append(
                    asyncio.create_task(self._send(step, step.request, ROLE_STREAM, index)),
                )
        finally:
            outcomes = await asyncio.gather(*tasks, return_exceptions=True)
        _raise_first(outcomes)

    async def _block(self, step: Step) -> None:
        """Trigger a refusal, ask the other type while blocked, then ask again once unblocked."""
        assert step.follow_up is not None
        await self._burst(step, step.request, step.count, ROLE_TRIGGER)
        if not any(a.outcome in REFUSALS for a in self._step_attempts()):
            return
        await self._send(step, step.follow_up, ROLE_FOLLOW_UP, step.count)
        refusals = [
            a for a in self._step_attempts() if a.outcome in REFUSALS and a.retry_after is not None
        ]
        if not refusals:
            return
        self._check_retry_after(max(a.retry_after for a in refusals))
        target = max(a.answered_at + a.retry_after for a in refusals)
        await self._until(target + self._settings.retry_margin_secs)
        await self._send(step, step.request, ROLE_AFTER_WAIT, step.count + 1)

    async def _gap(self, previous: Sequence[Attempt]) -> None:
        retry_after = max((a.retry_after or 0 for a in previous), default=0)
        self._check_retry_after(retry_after)
        wait = max(self._settings.min_gap_secs, retry_after + self._settings.gap_margin_secs)
        last = max((a.answered_at for a in previous), default=self._clock())
        await self._until(last + wait)

    def _check_retry_after(self, retry_after: int) -> None:
        if retry_after > self._settings.max_wait_secs:
            raise WaitTooLong(
                f"retryAfter = {retry_after}: {IMPLAUSIBLE}; the run stops rather than wait it out",
            )

    async def _until(self, target: float) -> None:
        while (left := target - self._clock()) > 0:
            self._check_running()
            await self._sleep(min(left, _PAUSE_SLICE_SECS))
        self._check_running()

    def _check_running(self) -> None:
        if self._guard.stop_requested:
            raise StopRequested("stopped by Ctrl+C")
        if self._clock() > self._deadline:
            raise RunTooLong(f"the run's hard limit of {self._settings.max_run_secs:g} s")

    def _step_attempts(self) -> list[Attempt]:
        return self._attempts[self._step_first :]

    async def _send(self, step: Step, kind: str, role: str, index: int) -> None:
        self._step_sent += 1
        if self._step_sent > self._settings.max_step_requests:
            raise StepCapReached(
                f"the per-step cap of {self._settings.max_step_requests} requests is reached",
            )
        payload = self._requests.make(kind)
        sent_at = self._clock()
        response: Message | None = None
        retry_after: int | None = None
        try:
            response = await self._connection.request(
                payload,
                timeout_secs=self._settings.request_timeout_secs,
            )
            outcome = ANSWERED
        except CTraderRequestError as e:
            outcome, retry_after = e.error_code, e.retry_after_secs
        except CTraderTimeoutError:
            outcome = TIMEOUT
        attempt = Attempt(
            step.name,
            kind,
            role,
            index,
            sent_at,
            self._clock(),
            outcome,
            retry_after,
        )
        self._attempts.append(attempt)
        self._log.attempt(attempt)
        self._requests.note(response)


def _with_sibling(step: Step, sibling: str) -> Step:
    def resolved(kind: str | None) -> str | None:
        return sibling if kind == SIBLING else kind

    return dataclasses.replace(
        step,
        name=step.name.replace(SIBLING, sibling),
        request=resolved(step.request),
        follow_up=resolved(step.follow_up),
    )


def _raise_first(outcomes: Sequence[object]) -> None:
    for outcome in outcomes:
        if isinstance(outcome, BaseException):
            raise outcome


def _reason(error: BaseException) -> str:
    if isinstance(error, CTraderConnectionError):
        return "the connection was lost"
    return str(error)


# -- Inference ------------------------------------------------------------------------------
#
# Pure functions from recorded attempts to a measured value, or to `None` with the reason it
# cannot be told. Nothing here guesses: a timeline that fits more than one reading, or none, is
# reported as inconclusive.


@dataclass(frozen=True)
class BurstSummary:
    size: int
    answered: int
    refused: int
    other: tuple[str, ...]
    # 1-based send position of the first refusal.
    first_refused: int | None
    refusal_codes: tuple[str, ...] = ()

    @property
    def complete(self) -> bool:
        return self.answered + self.refused + len(self.other) == self.size


def summarize_burst(size: int, attempts: Sequence[Attempt]) -> BurstSummary:
    ordered = sorted(attempts, key=lambda a: a.index)
    refused = [a for a in ordered if a.outcome in REFUSALS]
    return BurstSummary(
        size=size,
        answered=sum(1 for a in ordered if a.outcome == ANSWERED),
        refused=len(refused),
        other=tuple(
            a.outcome for a in ordered if a.outcome != ANSWERED and a.outcome not in REFUSALS
        ),
        first_refused=refused[0].index + 1 if refused else None,
        refusal_codes=tuple(sorted({a.outcome for a in refused})),
    )


def describe_burst(summary: BurstSummary) -> str:
    text = f"burst of {summary.size}: {summary.answered} answered"
    if summary.refused:
        codes = ", ".join(summary.refusal_codes)
        text += f", {summary.refused} refused ({codes}) from request {summary.first_refused}"
    if summary.other:
        counts = Counter(summary.other)
        text += ", " + ", ".join(f"{n} {code}" for code, n in sorted(counts.items()))
    sent = summary.answered + summary.refused + len(summary.other)
    if sent != summary.size:
        text += f" (only {sent} sent)"
    return text


def infer_allowance(bursts: Sequence[BurstSummary]) -> tuple[int | None, str]:
    """How many requests sent at once are answered, or `None` and why it cannot be told."""
    if not bursts:
        return None, "no burst ran"
    if any(not b.complete or b.other for b in bursts):
        return None, "a burst ended in something other than answers and refusals"
    refusing = [b for b in bursts if b.refused]
    if not refusing:
        return None, f"no burst was refused, up to {max(b.size for b in bursts)} at once"
    answered = sorted({b.answered for b in refusing})
    if len(answered) > 1:
        return None, f"refused bursts answered different numbers: {answered}"
    allowance = answered[0]
    larger = [b.size for b in bursts if not b.refused and b.size > allowance]
    if larger:
        return (
            None,
            f"a burst of {larger[0]} was answered in full, yet others stopped at {allowance}",
        )
    return allowance, ""


def most_within(times: Sequence[float], span: float) -> int:
    """The most of `times` that fit in any interval shorter than `span`."""
    ordered = sorted(times)
    best = 0
    start = 0
    for end, t in enumerate(ordered):
        while t - ordered[start] >= span:
            start += 1
        best = max(best, end - start + 1)
    return best


def _blocked(refusal: Attempt, refusals: Sequence[Attempt]) -> bool:
    """Whether `refusal` fell inside the block an earlier refusal's `retryAfter` announced."""
    return any(
        q.sent_at < refusal.sent_at
        and q.retry_after is not None
        and refusal.sent_at < q.answered_at + q.retry_after + _TIMING_TOLERANCE_SECS
        for q in refusals
    )


def fits_sliding(attempts: Sequence[Attempt], allowance: int) -> bool:
    """Whether the timeline fits "`allowance` answered within any 1 s", plus blocks."""
    answered = sorted(a.sent_at for a in attempts if a.outcome == ANSWERED)
    if most_within(answered, _WINDOW_SECS - _TIMING_TOLERANCE_SECS) > allowance:
        return False
    refusals = [a for a in attempts if a.outcome in REFUSALS]
    for r in refusals:
        if _blocked(r, refusals):
            continue
        before = sum(
            1
            for t in answered
            if r.sent_at - _WINDOW_SECS - _TIMING_TOLERANCE_SECS < t <= r.sent_at
        )
        if before < allowance:
            return False
    return True


def fits_fixed(attempts: Sequence[Attempt], allowance: int, phase: float) -> bool:
    """Whether the timeline fits "`allowance` per second starting at `phase`", plus blocks."""

    def window(t: float) -> int:
        return math.floor(t - phase)

    answered = [a.sent_at for a in attempts if a.outcome == ANSWERED]
    if any(n > allowance for n in Counter(window(t) for t in answered).values()):
        return False
    refusals = [a for a in attempts if a.outcome in REFUSALS]
    for r in refusals:
        if _blocked(r, refusals):
            continue
        current = window(r.sent_at)
        before = sum(1 for t in answered if window(t) == current and t <= r.sent_at)
        if before < allowance:
            return False
    return True


def infer_window_shape(
    streams: Sequence[Sequence[Attempt]],
    allowance: int | None,
) -> tuple[str | None, str]:
    """`SLIDING` or `FIXED` when exactly one fits every stream; otherwise `None` and why."""
    if allowance is None:
        return None, "the allowance at once is unknown, so the streams cannot be read"
    if not any(streams):
        return None, "no stream ran"
    if any(a.outcome != ANSWERED and a.outcome not in REFUSALS for s in streams for a in s):
        return None, "a stream ended in something other than answers and refusals"
    if not any(a.outcome in REFUSALS for s in streams for a in s):
        return None, "no stream was refused, so neither shape showed"
    sliding = all(fits_sliding(s, allowance) for s in streams)
    phases = [
        k
        for k in range(_PHASE_GRID)
        if all(fits_fixed(s, allowance, k / _PHASE_GRID) for s in streams)
    ]
    if sliding and not phases:
        return SLIDING, "only a sliding 1 s window fits the streams"
    if phases and not sliding:
        return FIXED, (
            "only calendar seconds fit the streams, with seconds starting at local-clock "
            f"fractions {_phase_ranges(phases)}"
        )
    if sliding:
        return None, "the streams fit both a sliding window and calendar seconds"
    return None, "the streams fit neither a sliding window nor calendar seconds"


def _phase_ranges(phases: Sequence[int]) -> str:
    ranges: list[list[int]] = []
    for k in sorted(phases):
        if ranges and k == ranges[-1][1] + 1:
            ranges[-1][1] = k
        else:
            ranges.append([k, k])
    return ", ".join(
        f"{a / _PHASE_GRID:.2f}" if a == b else f"{a / _PHASE_GRID:.2f}-{b / _PHASE_GRID:.2f}"
        for a, b in ranges
    )


def describe_stream(label: str, attempts: Sequence[Attempt]) -> str:
    if not attempts:
        return f"{label}: nothing sent"
    start = min(a.sent_at for a in attempts)
    answered = [a.sent_at for a in attempts if a.outcome == ANSWERED]
    refused = sorted(a.sent_at - start for a in attempts if a.outcome in REFUSALS)
    codes = ", ".join(sorted({a.outcome for a in attempts if a.outcome in REFUSALS}))
    other = len(attempts) - len(answered) - len(refused)
    text = (
        f"{label}: {len(attempts)} sent, {len(answered)} answered, {len(refused)} refused"
        f"{f' ({codes})' if codes else ''}{f', {other} other' if other else ''}; at most "
        f"{most_within(answered, _WINDOW_SECS)} answered within any 1 s"
    )
    if refused:
        shown = ", ".join(f"+{t:.2f}" for t in refused[:_MAX_LISTED_TIMES])
        more = ", ..." if len(refused) > _MAX_LISTED_TIMES else ""
        text += f"; refused at {shown}{more} s"
    return text


def classify_type(summary: BurstSummary | None, allowance: int | None) -> tuple[bool | None, str]:
    """Whether a burst of one payload type was limited: `None` when it cannot be told."""
    if summary is None:
        return None, "not run"
    if not summary.complete or summary.other:
        return None, f"inconclusive ({describe_burst(summary)})"
    if summary.refused:
        same = (
            " - like trendbars" if allowance is not None and summary.answered == allowance else ""
        )
        counts = f"{summary.answered} answered, {summary.refused} refused"
        return True, f"limited at {summary.size}: {counts}{same}"
    return False, f"NOT limited at {summary.size}"


def choose_sibling(results: Sequence[StepResult]) -> str | None:
    """The first type other than trendbars that a burst showed limited, or `None`."""
    for kind in (*_TYPE_KINDS, TICK_DATA):
        if classify_type(_type_summary(results, kind), None)[0] is True:
            return kind
    return None


def infer_sharing(
    attempts: Sequence[Attempt],
    limited: Collection[str],
) -> tuple[str | None, str]:
    """`PER_TYPE` or `SHARED` from one block step; otherwise `None` and why.

    `limited` holds the types a burst showed limited. A follow-up answered while another type
    is blocked says nothing about sharing unless its own type is limited too.
    """
    triggers = [a for a in attempts if a.role == ROLE_TRIGGER]
    follow = next((a for a in attempts if a.role == ROLE_FOLLOW_UP), None)
    trigger = triggers[0].request if triggers else "trigger"
    refusals = [a for a in triggers if a.outcome in REFUSALS]
    if not refusals:
        return None, f"no {trigger} request was refused in a burst of {len(triggers)}"
    if follow is None:
        return None, "no follow-up request was sent"
    unshown = [k for k in (trigger, follow.request) if k != TRENDBARS and k not in limited]
    if unshown:
        return None, f"the {unshown[0]} type was not shown to be limited"
    ends = [q.answered_at + q.retry_after for q in refusals if q.retry_after is not None]
    if ends and follow.sent_at >= max(ends):
        return None, f"the {follow.request} request left after the {trigger} block had ended"
    after = follow.sent_at - min(q.answered_at for q in refusals)
    lead = f"one {follow.request} request sent {after:.2f} s after the first {trigger} refusal"
    if follow.outcome == ANSWERED:
        return PER_TYPE, f"{lead} was answered"
    if follow.outcome in REFUSALS:
        return SHARED, f"{lead} was refused with {follow.outcome}"
    return None, f"{lead} ended with {follow.outcome}"


def infer_retry_after(
    attempts: Sequence[Attempt],
    margin_secs: float,
    max_wait_secs: float,
) -> tuple[str | None, bool | None, str]:
    """What `retryAfter` said, whether waiting it out was enough, and the evidence.

    The value is `None` when no refusal carried one; "enough" is `None` when it was not checked.
    A value over `max_wait_secs` is reported raw and marked implausible.
    """
    refusals = [a for a in attempts if a.outcome in REFUSALS]
    if not refusals:
        return None, None, "no refusal was seen"
    values = sorted({a.retry_after for a in refusals if a.retry_after is not None})
    missing = sum(1 for a in refusals if a.retry_after is None)
    if not values:
        return None, None, f"none of the {len(refusals)} refusals carried retryAfter"
    value = f"retryAfter = {values[0]} s" if len(values) == 1 else f"retryAfter in {values} s"
    if values[-1] > max_wait_secs:
        value = f"retryAfter = {values[-1]}: {IMPLAUSIBLE}"
    if missing:
        value += f" ({missing} of {len(refusals)} refusals carried none)"
    checks = [a for a in attempts if a.role == ROLE_AFTER_WAIT]
    if not checks:
        return value, None, "no request was sent after waiting it out"
    failed = [a for a in checks if a.outcome != ANSWERED]
    sent = f"requests sent retryAfter + {margin_secs:g} s after a refusal"
    if failed:
        detail = f"{len(failed)} of {len(checks)} {sent} ended with {failed[0].outcome}"
        return value, False, detail
    return value, True, f"{len(checks)} of {len(checks)} {sent} were answered"


def _ran(results: Sequence[StepResult], group: str) -> list[StepResult]:
    return [r for r in results if r.step.group == group and r.status == RAN]


def _type_summary(results: Sequence[StepResult], kind: str) -> BurstSummary | None:
    result = next(
        (
            r
            for r in [*_ran(results, GROUP_TYPES), *_ran(results, GROUP_TICK_DATA)]
            if r.step.request == kind
        ),
        None,
    )
    return None if result is None else summarize_burst(result.step.count, result.attempts)


def _not_ran(results: Sequence[StepResult], group: str) -> list[str]:
    return [
        f"{r.step.name}: {r.status}{f' ({r.note})' if r.note else ''}"
        for r in results
        if r.step.group == group and r.status != RAN
    ]


def build_findings(results: Sequence[StepResult], settings: Settings) -> list[Finding]:
    findings = []

    bursts = [
        summarize_burst(r.step.count, r.attempts) for r in _ran(results, GROUP_HISTORICAL_BURSTS)
    ]
    streams = _ran(results, GROUP_HISTORICAL_STREAMS)
    allowance, why = infer_allowance(bursts)
    shape, shape_detail = infer_window_shape([r.attempts for r in streams], allowance)
    evidence = [describe_burst(b) for b in bursts]
    evidence += [describe_stream(f"{r.step.rate:g}/s", r.attempts) for r in streams]
    evidence += _not_ran(results, GROUP_HISTORICAL_BURSTS)
    evidence += _not_ran(results, GROUP_HISTORICAL_STREAMS)
    if allowance is None:
        measured, conclusion = f"inconclusive: {why}", "historical: inconclusive"
    elif shape is None:
        evidence.append(shape_detail)
        measured = f"{allowance} at once; window shape inconclusive: {shape_detail}"
        conclusion = f"historical: {allowance} at once, window shape inconclusive"
    else:
        evidence.append(shape_detail)
        if shape == SLIDING:
            measured = f"{allowance} within any 1 s (sliding window)"
            conclusion = f"historical: {allowance} within any 1 s (sliding)"
        else:
            measured = f"{allowance} per calendar second (fixed window)"
            conclusion = f"historical: {allowance} per calendar second (fixed)"
    findings.append(
        Finding(1, "Historical limit (trendbars)", measured, tuple(evidence), conclusion)
    )

    # (kind, limited or `None`, evidence, conclusion)
    verdicts: list[tuple[str, bool | None, str, str]] = []
    for kind in (*_TYPE_KINDS, TICK_DATA):
        result = next(
            (
                r
                for r in results
                if r.step.group in (GROUP_TYPES, GROUP_TICK_DATA) and r.step.request == kind
            ),
            None,
        )
        if result is not None and result.status == SKIPPED:
            verdicts.append((kind, None, f"skipped: {result.note}", "skipped"))
            continue
        if result is not None and result.status != RAN:
            verdicts.append((kind, None, result.status, "inconclusive"))
            continue
        limited, text = classify_type(_type_summary(results, kind), allowance)
        short = {
            True: f"limited at {_TYPE_BURST}",
            False: f"NOT limited at {_TYPE_BURST}",
            None: "inconclusive",
        }[limited]
        verdicts.append((kind, limited, text, short))
    evidence = [f"{kind}: {text}" for kind, _, text, _ in verdicts]
    measured = "; ".join(
        f"{kind}: {'limited' if limited else 'NOT limited' if limited is False else 'unknown'}"
        for kind, limited, _, short in verdicts
        if short != "skipped"
    )
    conclusion = "; ".join(f"{kind}: {short}" for kind, _, _, short in verdicts)
    findings.append(
        Finding(2, "Which payload types are historical", measured, tuple(evidence), conclusion)
    )

    limited_kinds = {kind for kind, limited, _, _ in verdicts if limited}
    readings = []
    evidence = []
    for group in (GROUP_SHARING, GROUP_SHARING_REVERSE):
        for found in _ran(results, group):
            value, detail = infer_sharing(found.attempts, limited_kinds)
            readings.append(value)
            evidence.append(detail)
    evidence += _not_ran(results, GROUP_SHARING) + _not_ran(results, GROUP_SHARING_REVERSE)
    settled = {r for r in readings if r is not None}
    skipped = [r for r in results if r.step.group == GROUP_SHARING and r.status == SKIPPED]
    if not readings and skipped:
        measured, conclusion = (
            f"inconclusive: {skipped[0].note}",
            "per type or shared: inconclusive",
        )
    elif not readings:
        measured, conclusion = "inconclusive: no block step ran", "per type or shared: inconclusive"
    elif None in readings:
        unsettled = evidence[readings.index(None)]
        measured, conclusion = f"inconclusive: {unsettled}", "per type or shared: inconclusive"
    elif len(settled) > 1:
        measured, conclusion = (
            "inconclusive: the two directions disagree",
            "per type or shared: inconclusive",
        )
    else:
        value = settled.pop()
        measured = f"the limit is {value}" if value == SHARED else f"the limit is counted {value}"
        conclusion = f"limit is {value}"
    findings.append(Finding(3, "Per payload type or shared", measured, tuple(evidence), conclusion))

    defaults = [summarize_burst(r.step.count, r.attempts) for r in _ran(results, GROUP_DEFAULT)]
    default_allowance, why = infer_allowance(defaults)
    evidence = [describe_burst(b) for b in defaults] + _not_ran(results, GROUP_DEFAULT)
    if default_allowance is not None:
        measured = f"{default_allowance} at once; refusals from request {default_allowance + 1}"
        conclusion = f"default: refusals from request {default_allowance + 1}"
    else:
        measured, conclusion = f"inconclusive: {why}", f"default: inconclusive ({why})"
    findings.append(
        Finding(4, f"Default limit ({TRADER} requests)", measured, tuple(evidence), conclusion)
    )

    every = [a for r in results for a in r.attempts]
    value, enough, detail = infer_retry_after(
        every, settings.retry_margin_secs, settings.max_wait_secs
    )
    if value is None:
        measured, conclusion = f"inconclusive: {detail}", "retryAfter: inconclusive"
    else:
        waiting = {True: "enough", False: "NOT enough", None: "unchecked"}[enough]
        measured, conclusion = value, f"{value}, waiting it out {waiting}"
    findings.append(Finding(5, "retryAfter", measured, (detail,), conclusion))
    return findings


def format_report(
    findings: Sequence[Finding], results: Sequence[StepResult], *, sent: int, settings: Settings
) -> str:
    statuses = Counter(r.status for r in results)
    lines = [
        "cTrader request-limit probe: report",
        f"requests sent: {sent} (cap {settings.max_requests}); steps: "
        + ", ".join(f"{statuses[s]} {s}" for s in (RAN, SKIPPED, STOPPED, NOT_RUN) if statuses[s]),
        "",
    ]
    for f in findings:
        lines.append(f"{f.number}. {f.title}")
        lines.append(f"   measured:   {f.measured}")
        for index, line in enumerate(f.evidence):
            lines.append(f"   {'evidence:' if index == 0 else '':<11} {line}")
        lines.append(f"   conclusion: {f.conclusion}")
        lines.append("")
    stopped = [r for r in results if r.status == STOPPED]
    if stopped:
        lines.append(f"The run stopped at '{stopped[0].step.name}': {stopped[0].note}")
    lines.append("Summary: " + "; ".join(f.conclusion for f in findings))
    return "\n".join(lines)


# -- Entry points ---------------------------------------------------------------------------


async def resolve_account(
    trader_login: int,
    credentials: AccountCredentials,
    guard: ReadOnlyGuard,
    endpoints: Endpoints,
) -> tuple[str, int]:
    """The account's host and its `ctidTraderAccountId`, from the account list."""
    connection = CTraderConnection(
        endpoints.demo_host,
        endpoints.port,
        logger=_StderrLogger(),
        tls=endpoints.tls,
    )
    guarded = GuardedConnection(connection, guard)
    await connection.connect()
    try:
        await guarded.request(
            oa.ProtoOAApplicationAuthReq(
                clientId=credentials.client_id,
                clientSecret=credentials.client_secret,
            ),
        )
        granted = await request_granted_accounts(guarded, credentials.access_token)
    finally:
        await connection.close()
    matched = [a for a in granted.accounts if a.trader_login == trader_login]
    if len(matched) != 1:
        raise SetupError(
            f"the access token grants {len(matched)} accounts with that trader login, not one",
        )
    account = matched[0]
    host = account_host(
        account.is_live, demo_host=endpoints.demo_host, live_host=endpoints.live_host
    )
    return host, account.ctid_trader_account_id


@contextlib.contextmanager
def _stop_on_interrupt(guard: ReadOnlyGuard, *, enabled: bool) -> Iterator[None]:
    """First Ctrl+C: stop at the next send. Second: quit at once."""
    if not enabled:
        yield
        return

    def on_interrupt(signum, frame) -> None:
        if guard.stop_requested:
            raise KeyboardInterrupt
        guard.stop_requested = True
        print("Stopping at the next send; Ctrl+C again quits at once.", file=sys.stderr)

    previous = signal.signal(signal.SIGINT, on_interrupt)
    try:
        yield
    finally:
        signal.signal(signal.SIGINT, previous)


@contextlib.contextmanager
def _interrupt_raises(*, enabled: bool) -> Iterator[None]:
    """Ctrl+C raises `KeyboardInterrupt` here, rather than cancelling the whole run."""
    if not enabled:
        yield
        return
    previous = signal.signal(signal.SIGINT, signal.default_int_handler)
    try:
        yield
    finally:
        signal.signal(signal.SIGINT, previous)


async def run_probe(
    settings: Settings,
    credentials: AccountCredentials,
    plan: Sequence[Step],
    *,
    guard: ReadOnlyGuard,
    endpoints: Endpoints,
    log_dir: pathlib.Path,
    confirm: Callable[[str], str] = input,
    clock: Callable[[], float] = time.time,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    handle_interrupt: bool = False,
) -> int:
    """Resolve the account, ask for `y`, run the plan, and print the report. Returns the exit code.

    `clock` must be wall-clock seconds in a live run: calendar seconds are read from it.
    """
    assert settings.trader_login is not None
    host, account_id = await resolve_account(settings.trader_login, credentials, guard, endpoints)
    print(f"account: trader login {settings.trader_login}, host {host}")
    print(OTHER_SESSIONS)
    try:
        with _interrupt_raises(enabled=handle_interrupt):
            answer = confirm("Type y to start probing: ")
    except (EOFError, KeyboardInterrupt):
        answer = ""
    if answer.strip().lower() != "y":
        print("Declined: nothing was probed.")
        return 2

    stamp = datetime.datetime.now(datetime.UTC).strftime("%Y%m%dT%H%M%SZ")
    log_path = log_dir / f"verify_rate_limits-{stamp}.raw.jsonl"
    connection = CTraderConnection(host, endpoints.port, logger=_StderrLogger(), tls=endpoints.tls)
    guarded = GuardedConnection(connection, guard)
    with (
        AttemptLog(log_path, origin=clock()) as log,
        _stop_on_interrupt(guard, enabled=handle_interrupt),
    ):
        await connection.connect()
        try:
            await guarded.request(
                oa.ProtoOAApplicationAuthReq(
                    clientId=credentials.client_id,
                    clientSecret=credentials.client_secret,
                ),
            )
            await guarded.request(
                oa.ProtoOAAccountAuthReq(
                    ctidTraderAccountId=account_id,
                    accessToken=credentials.access_token,
                ),
            )
            listed = await guarded.request(oa.ProtoOASymbolsListReq(ctidTraderAccountId=account_id))
            symbol_id = next(
                (s.symbolId for s in listed.symbol if s.symbolName == settings.symbol),
                None,
            )
            if symbol_id is None:
                raise SetupError(f"the account does not offer {settings.symbol}")
            prober = Prober(
                guarded,
                guard,
                RequestFactory(account_id, symbol_id, clock),
                settings,
                log,
                clock=clock,
                sleep=sleep,
            )
            results = await prober.run(plan)
        finally:
            await connection.close()

    print(
        format_report(
            build_findings(results, settings), results, sent=guard.sent, settings=settings
        )
    )
    print(f"log: {log_path}")
    return 0 if all(r.status in (RAN, SKIPPED) for r in results) else 1


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Measure the venue's request limits on a live account, read-only.",
    )
    parser.add_argument(
        "--trader-login",
        type=int,
        help=f"the account number the broker gave you (default: {_TRADER_LOGIN_KEY} in .env)",
    )
    parser.add_argument(
        "--symbol",
        default="EURUSD",
        help="the symbol the trendbar requests ask about (default: EURUSD)",
    )
    parser.add_argument(
        "--max-requests",
        type=int,
        default=Settings.max_requests,
        help=f"hard cap on requests for the whole run (default: {Settings.max_requests})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="print the plan and send nothing",
    )
    parser.add_argument(
        "--from-log",
        type=pathlib.Path,
        metavar="PATH",
        help="print the report of an earlier run from its log, and send nothing",
    )
    return parser


def main(
    argv: list[str] | None = None,
    *,
    endpoints: Endpoints = _VENUE_ENDPOINTS,
    env_path: pathlib.Path = _REPO_ROOT / ".env",
    log_dir: pathlib.Path = _REPO_ROOT / "logs",
) -> int:
    args = build_arg_parser().parse_args(argv)
    settings = Settings(
        trader_login=args.trader_login,
        symbol=args.symbol,
        max_requests=args.max_requests,
    )
    plan = build_plan(settings)
    try:
        validate_plan(plan, settings)
    except ValueError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2
    if args.dry_run:
        print(format_plan(plan, settings))
        print("Dry run: nothing was sent.")
        return 0
    if args.from_log is not None:
        results = read_log(args.from_log, plan)
        sent = sum(len(r.attempts) for r in results)
        print(
            format_report(build_findings(results, settings), results, sent=sent, settings=settings)
        )
        return 0

    try:
        env = get_tokens.load_env(env_path)
        credentials = AccountCredentials(
            client_id=env["CTRADER_CLIENT_ID"],
            client_secret=env["CTRADER_CLIENT_SECRET"],
            access_token=env["CTRADER_ACCESS_TOKEN"],
            refresh_token=None,
            token_expires_at=None,
        )
        login = settings.trader_login or int(env[_TRADER_LOGIN_KEY])
    except (OSError, KeyError, ValueError):
        print(
            f"error: {env_path.name} must hold CTRADER_CLIENT_ID, CTRADER_CLIENT_SECRET, "
            f"CTRADER_ACCESS_TOKEN and, without --trader-login, {_TRADER_LOGIN_KEY}",
            file=sys.stderr,
        )
        return 1
    settings = dataclasses.replace(settings, trader_login=login)
    print(format_plan(plan, settings))

    try:
        return asyncio.run(
            run_probe(
                settings,
                credentials,
                plan,
                guard=ReadOnlyGuard(settings.max_requests),
                endpoints=endpoints,
                log_dir=log_dir,
                handle_interrupt=True,
            ),
        )
    except KeyboardInterrupt:
        print("Quit: the report was not printed; the log holds what was sent.", file=sys.stderr)
        return 1
    except SetupError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        # Never the exception's message: a venue rejection's carries the venue's description.
        code = f" {e.error_code}" if isinstance(e, CTraderRequestError) else ""
        print(f"error: {type(e).__name__}{code}: the probe failed", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
