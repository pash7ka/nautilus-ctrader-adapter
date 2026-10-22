"""Outbound rate limiting.

Spotware's SDK drains a send queue on a one-second timer (`TcpProtocol._sendStrings`, run by
`LoopingCall(...).start(1)`). Here a caller awaits its turn, so it is delayed rather than parked
behind a timer, and a message is either sent or its caller sees an error.

`RequestScheduler` paces each group of payload types to a sliding window and serves the waiting
requests by priority.

The venue reports a breach as `BLOCKED_PAYLOAD_TYPE` carrying `retryAfter`, so the scheduler
does not have to guess the real limits after a refusal: it pauses for what the venue asked, up
to a cap.
"""

from __future__ import annotations

import asyncio
import heapq
import itertools
import math
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import IntEnum
from typing import NoReturn

from google.protobuf.message import Message
from nautilus_trader.common.component import Logger

from nautilus_ctrader.common.errors import (
    CTraderNotSentConnectionError,
    CTraderQueueTimeoutError,
    CTraderRequestError,
)
from nautilus_ctrader.constants import (
    GROUP_HISTORICAL,
    GROUP_OTHER,
    HISTORICAL_PAYLOAD_TYPES,
    RATE_LIMIT_MAX_PAUSE_SECS,
    RATE_LIMIT_PAUSE_MARGIN_SECS,
    RATE_LIMIT_RETRIES,
    REQUEST_QUEUE_TIMEOUT_SECS,
    REQUEST_WINDOW_SECS,
    VENUE_HISTORICAL_REQUESTS_PER_SEC,
    VENUE_REQUESTS_PER_SEC,
)


class RequestPriority(IntEnum):
    """Lower values leave the queue first."""

    EXECUTION = 0
    CHECKPOINT = 1
    DATA = 2
    BACKFILL = 3


@dataclass(frozen=True)
class Pacing:
    """How the scheduler paces requests.

    - `requests_per_window`, `historical_requests_per_window`: the most requests of the other
      and the historical group granted within any `window_secs`;
    - `retries`: resends of a historical request the venue refused for its rate limit;
    - `queue_timeout_secs`: the longest a request may wait in the queue, retries included;
    - `max_pause_secs`: the longest pause after a rate-limit refusal, whatever the venue asks.
    """

    requests_per_window: int = VENUE_REQUESTS_PER_SEC
    historical_requests_per_window: int = VENUE_HISTORICAL_REQUESTS_PER_SEC
    window_secs: float = REQUEST_WINDOW_SECS
    retries: int = RATE_LIMIT_RETRIES
    queue_timeout_secs: float = REQUEST_QUEUE_TIMEOUT_SECS
    max_pause_secs: float = RATE_LIMIT_MAX_PAUSE_SECS

    def __post_init__(self) -> None:
        if self.requests_per_window < 1 or self.historical_requests_per_window < 1:
            raise ValueError("a window must allow at least one request")
        if not (self.window_secs > 0):
            raise ValueError("window_secs must be positive")
        if not (self.queue_timeout_secs > 0):
            raise ValueError("queue_timeout_secs must be positive")
        if not (self.max_pause_secs > 0):
            raise ValueError("max_pause_secs must be positive")
        if self.retries < 0:
            raise ValueError("retries must not be negative")


def group_of(payload: Message) -> str:
    """The pacing group of a request payload."""
    return GROUP_HISTORICAL if type(payload) in HISTORICAL_PAYLOAD_TYPES else GROUP_OTHER


class _Entry:
    __slots__ = ("future", "queued_at")

    def __init__(self, future: asyncio.Future[None], queued_at: float) -> None:
        self.future = future
        self.queued_at = queued_at


class _Group:
    def __init__(self, name: str, limit: int) -> None:
        self.name = name
        self.limit = limit
        # `(priority, sequence, entry)`; an entry whose future is done is dead and skipped.
        self.heap: list[tuple[int, int, _Entry]] = []
        self.granted: deque[float] = deque()
        self.paused_until = -math.inf
        self.dispatcher: asyncio.Task[None] | None = None


class RequestScheduler:
    """Grants requests their turn to be sent, paced per group and served by priority.

    Each group grants at most its count within any `window_secs`, and none while a venue
    refusal has paused it. Waiting requests leave by priority, then in arrival order.
    `clock` and `sleep` default to the running loop's `time()` and `asyncio.sleep`.
    """

    def __init__(
        self,
        pacing: Pacing,
        *,
        logger: Logger,
        clock: Callable[[], float] | None = None,
        sleep: Callable[[float], Awaitable[None]] | None = None,
    ) -> None:
        self._pacing = pacing
        self._log = logger
        self._clock = clock or (lambda: asyncio.get_running_loop().time())
        self._sleep = sleep or asyncio.sleep
        self._sequence = itertools.count()
        self._failed: CTraderNotSentConnectionError | None = None
        self._groups = {
            GROUP_HISTORICAL: _Group(GROUP_HISTORICAL, pacing.historical_requests_per_window),
            GROUP_OTHER: _Group(GROUP_OTHER, pacing.requests_per_window),
        }

    async def submit[T](
        self,
        group: str,
        priority: RequestPriority,
        send: Callable[[], Awaitable[T]],
    ) -> T:
        """Wait for the turn of a `group` request, then return what `send()` returns.

        A rate-limit refusal (a `CTraderRequestError` with `retry_after_secs`) pauses the group,
        for no longer than `Pacing.max_pause_secs`. A historical request is then resent, ahead
        of its level, up to `Pacing.retries` times; any other is not. Every other error from
        `send()` propagates untouched.

        Raises
        ------
        CTraderQueueTimeoutError
            The request waited `Pacing.queue_timeout_secs` in the queue, retries included, and
            was dropped unsent. Time inside `send()` does not count.
        Exception
            A copy of the error given to `fail_all()`, for a request waiting then, or queued (a
            retry included) after it and before `reset()`.
        """
        state = self._groups[group]
        sequence = next(self._sequence)
        retries = self._pacing.retries if group == GROUP_HISTORICAL else 0
        waited = 0.0
        attempt = 0
        while True:
            waited += await self._wait_turn(state, priority, sequence, waited)
            try:
                return await send()
            except CTraderRequestError as error:
                if error.retry_after_secs is None:
                    raise
                cap = self._pacing.max_pause_secs
                pause = min(error.retry_after_secs + RATE_LIMIT_PAUSE_MARGIN_SECS, cap)
                state.paused_until = max(state.paused_until, self._clock() + pause)
                attempt += 1
                retrying = attempt <= retries
                if self._failed is not None:
                    # The requeue below raises the latched error instead.
                    outcome = "not retried: the connection was lost"
                elif retrying:
                    outcome = f"retry {attempt} of {retries}"
                elif retries:
                    outcome = "no retry left"
                else:
                    outcome = "not retried"
                refused = (
                    f"Venue rate limit refused a {state.name} request ({error.error_code}, "
                    f"retryAfter {error.retry_after_secs} s)"
                )
                if error.retry_after_secs > cap:
                    self._log.error(
                        f"{refused}, above the {cap:g} s cap: group paused only {pause:g} s, "
                        f"{outcome}",
                    )
                else:
                    self._log.warning(f"{refused}: group paused {pause:g} s, {outcome}")
                if not retrying:
                    raise

    def fail_all(self, error: CTraderNotSentConnectionError) -> None:
        """Fail every waiting request with `error`, and every later one until `reset()`.

        Each request gets its own instance of `error`'s type, built from `error.args`, which is
        exact only for this error type.
        """
        self._failed = error
        for state in self._groups.values():
            self._stop_dispatcher(state)
            for _, _, entry in state.heap:
                if not entry.future.done():
                    entry.future.set_exception(self._failure())
            state.heap.clear()

    def _failure(self) -> CTraderNotSentConnectionError:
        """A fresh copy of the `fail_all()` error: a shared instance would gather tracebacks."""
        assert self._failed is not None
        return type(self._failed)(*self._failed.args)

    def reset(self) -> None:
        """Forget send times, pauses and failure, for a new connection; waiting requests stay."""
        self._failed = None
        for state in self._groups.values():
            self._stop_dispatcher(state)
            state.granted.clear()
            state.paused_until = -math.inf
            if state.heap:
                self._start_dispatcher(state)

    async def _wait_turn(
        self,
        state: _Group,
        priority: RequestPriority,
        sequence: int,
        waited: float,
    ) -> float:
        """Queue one attempt and wait for its grant; returns the time it waited."""
        if self._failed is not None:
            raise self._failure()
        budget = self._pacing.queue_timeout_secs - waited
        if budget <= 0:
            self._raise_queue_timeout()
        queued_at = self._clock()
        entry = _Entry(asyncio.get_running_loop().create_future(), queued_at)
        heapq.heappush(state.heap, (priority, sequence, entry))
        if state.dispatcher is None or state.dispatcher.done():
            self._start_dispatcher(state)

        timer = asyncio.ensure_future(self._sleep(budget))
        try:
            await asyncio.wait((entry.future, timer), return_when=asyncio.FIRST_COMPLETED)
        finally:
            timer.cancel()
            granted = entry.future.done()
            # Leaves a dead entry for the dispatcher to skip, whether timed out or cancelled.
            entry.future.cancel()
        if not granted:
            self._raise_queue_timeout()
        entry.future.result()
        return self._clock() - queued_at

    def _raise_queue_timeout(self) -> NoReturn:
        raise CTraderQueueTimeoutError(
            f"waited {self._pacing.queue_timeout_secs:g} s in the request queue",
        )

    def _start_dispatcher(self, state: _Group) -> None:
        state.dispatcher = asyncio.get_running_loop().create_task(self._dispatch(state))

    @staticmethod
    def _stop_dispatcher(state: _Group) -> None:
        if state.dispatcher is not None:
            state.dispatcher.cancel()
            state.dispatcher = None

    async def _dispatch(self, state: _Group) -> None:
        window = self._pacing.window_secs
        while True:
            while state.heap and state.heap[0][2].future.done():
                heapq.heappop(state.heap)
            if not state.heap:
                return

            now = self._clock()
            # The same sum as `ready_at` below: `now - t >= window` can disagree with it by a
            # rounding error, and then the full window grants without limit.
            while state.granted and state.granted[0] + window <= now:
                state.granted.popleft()
            ready_at = state.paused_until
            if len(state.granted) >= state.limit:
                ready_at = max(ready_at, state.granted[0] + window)
            if ready_at > now:
                # A refusal may pause the group meanwhile, so everything is checked again.
                await self._sleep(ready_at - now)
                continue

            priority, _, entry = heapq.heappop(state.heap)
            state.granted.append(now)
            entry.future.set_result(None)
            waiting = sum(1 for _, _, e in state.heap if not e.future.done())
            self._log.debug(
                f"Granted a {state.name} request at {RequestPriority(priority).name} after "
                f"{now - entry.queued_at:.3f} s in the queue, {waiting} still queued",
            )
