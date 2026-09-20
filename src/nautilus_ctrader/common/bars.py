"""Pure state machine deciding when a live bar is closed, from the stream or a timer fallback.

No network I/O: `clock`, `fetch` and `fetch_range` are the only points of contact with the
outside world, all injected by the caller. Every closed bar is emitted exactly once, in
ascending order, whichever of the stream, timer or backfill path closes it.
"""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Protocol

from nautilus_ctrader.common.errors import CTraderConnectionError


@dataclass(frozen=True)
class RawBar:
    """One trendbar in venue units: prices as deltas from `low`, volume in venue units."""

    boundary_secs: int
    low: int
    delta_open: int
    delta_high: int
    delta_close: int
    volume: int


class Clock(Protocol):
    def now(self) -> float: ...  # epoch seconds

    def call_later(
        self, delay_secs: float, callback: Callable[[], None]
    ) -> asyncio.TimerHandle | Any: ...


class Logger(Protocol):
    def debug(self, message: str) -> None: ...
    def info(self, message: str) -> None: ...
    def warning(self, message: str) -> None: ...
    def error(self, message: str) -> None: ...
    def exception(self, message: str, ex: BaseException) -> None: ...


Fetch = Callable[[int], Awaitable[RawBar | None]]
Emit = Callable[[RawBar], None]

# How many times `backfill` re-fetches a growing range before it fetches the rest once and stops.
_MAX_BACKFILL_ROUNDS = 5


@dataclass(eq=False)
class _Pending:
    """A closed boundary waiting its turn in the queue.

    `streamed` is final when `needs_history` is false, otherwise only the fallback.
    """

    boundary: int
    streamed: RawBar | None
    needs_history: bool


class BarCloser:
    """
    Decides when one live bar type's forming bar is closed.

    Closing a bar never emits directly and never cancels anything: it only queues the boundary.
    The queue is kept in ascending order and drained strictly front to back, by a single
    resolver task whenever an entry needs history, so a later bar can never go out before an
    earlier one.

    - A stream update for a newer boundary closes the previous one with its last streamed state.
    - Otherwise a timer at `boundary + period + grace` queues it for history, which is asked up
      to `1 + history_retries` times, `grace` apart; if it never has the bar, the last streamed
      state is emitted instead.
    - While there is no connection there is no history to ask, so that is not a spent attempt:
      the entry stays queued until `backfill` or a later closed bar resumes it. A bar that
      closed during an outage is therefore never emitted from its partial streamed state.
    - The first update after construction, `on_disconnect` or `backfill` is a baseline: if its
      bar has already closed, it is queued for history with that state as the fallback.
    - No boundary before the bar forming at construction is ever emitted.
    - `close()` stops all of it permanently.

    `on_update`, `mark_emitted`, `close` and the clock's timer callbacks must run on the event
    loop thread: queueing a boundary may start the resolver task.

    The warn-once fallback flags - one for a failed history request, one for history simply
    having no bar - are per closer instance, i.e. per subscription.
    """

    def __init__(
        self,
        *,
        period_secs: int,
        grace_secs: float,
        history_retries: int,
        clock: Clock,
        fetch: Fetch,
        emit: Emit,
        logger: Logger,
        label: str,
    ) -> None:
        self._period = period_secs
        self._grace = grace_secs
        self._retries = history_retries
        self._clock = clock
        self._fetch = fetch
        self._emit_cb = emit
        self._log = logger
        self._label = label
        self._floor = self._floor_boundary(clock.now())
        self._last_emitted: int | None = None
        self._current: RawBar | None = None
        self._timer: asyncio.TimerHandle | Any | None = None
        self._queue: deque[_Pending] = deque()
        self._resolver: asyncio.Task | None = None
        self._drained = asyncio.Event()
        self._drained.set()
        self._holding = False
        self._held: RawBar | None = None
        self._baseline = True
        self._closed = False
        self._warned_failed = False
        self._warned_empty = False

    @property
    def last_emitted(self) -> int | None:
        return self._last_emitted

    def mark_emitted(self, boundary_secs: int) -> None:
        if self._last_emitted is None or boundary_secs > self._last_emitted:
            self._last_emitted = boundary_secs
        while self._queue and self._is_covered(self._queue[0].boundary):
            self._queue.popleft()
        if not self._queue and self._resolver is None:
            self._drained.set()

    def on_update(self, bar: RawBar) -> None:
        boundary = bar.boundary_secs
        if self._closed or boundary < self._floor or self._is_covered(boundary):
            return
        if self._holding:
            self._held = bar
            return
        if self._find(boundary) is not None:
            self._enqueue(boundary, bar, needs_history=True)  # only attaches the fallback
            return
        if self._baseline:
            self._baseline = False
            if boundary + self._period <= self._clock.now():
                self._enqueue(boundary, bar, needs_history=True)
                return
        current = self._current
        if current is None:
            self._current = bar
            self._arm(boundary)
        elif boundary == current.boundary_secs:
            self._current = bar
        elif boundary > current.boundary_secs:
            self._current = bar
            self._arm(boundary)
            self._enqueue(current.boundary_secs, current, needs_history=False)

    def on_disconnect(self) -> None:
        # Queued bars have already closed and still need resolving, so the queue stays.
        if self._closed:
            return
        self._cancel_timer()
        self._current = None
        self._held = None
        self._baseline = True

    def close(self) -> None:
        self._closed = True
        self._cancel_timer()
        self._current = None
        self._held = None
        self._queue.clear()
        if self._resolver is not None:
            self._resolver.cancel()
            self._resolver = None
        self._drained.set()

    async def backfill(self, fetch_range: Callable[[int, int], Awaitable[list[RawBar]]]) -> None:
        """Queue every bar closed since the last one, then resume the stream as a baseline.

        `fetch_range(start, end)` returns bars for the **inclusive** range `[start, end]`; both
        are period-aligned boundaries. Bars outside the range are ignored. `start` continues
        from `last_emitted`, never earlier than the bar forming at construction; `end` is the
        last boundary already closed by the clock.

        - If the closed range advances while a fetch and its emits are in flight, the new part
          is fetched again, for up to `_MAX_BACKFILL_ROUNDS` rounds; past that, a WARNING is
          logged and the rest is fetched once more, with no further rounds.
        - History can lag behind a just-closed bar, so if the newest closed boundary is still
          missing afterwards, it is queued for history alone. Earlier gaps in a served range
          are genuinely empty periods and are not retried.
        - After a capped final fetch, every boundary that closed while it ran is also queued
          for history alone, since no fetch covered it.
        - Stream updates are held meanwhile; the latest is replayed afterwards.
        """
        if self._closed:
            return
        if self._holding:
            self._log.debug(f"{self._label}: backfill already running; ignoring the second call")
            return
        self._holding = True
        self._held = None
        self._cancel_timer()
        self._current = None
        # There is a connection again, so an entry the resolver left queued when it went down
        # can be asked for now. Done here because the rounds below may enqueue nothing.
        self._pump()
        try:
            end, capped = await self._backfill_rounds(fetch_range)
            if self._closed:
                return
            newest = self._last_closed_boundary() if capped else end
            for boundary in range(end, newest + 1, self._period):
                self._enqueue(boundary, None, needs_history=True)
            if newest > end:
                self._log.debug(
                    f"{self._label}: queued {(newest - end) // self._period} bars that closed "
                    "during the final backfill fetch for history"
                )
        finally:
            self._holding = False
            self._baseline = True
        held, self._held = self._held, None
        if held is not None:
            self.on_update(held)

    async def _backfill_rounds(
        self, fetch_range: Callable[[int, int], Awaitable[list[RawBar]]]
    ) -> tuple[int, bool]:
        """Run the fetch rounds; return the last `end` fetched up to and whether the cap hit."""
        rounds = 0
        while True:
            end = self._last_closed_boundary()
            if not await self._fetch_and_drain(fetch_range, end) or self._closed:
                return end, False
            rounds += 1
            if self._last_closed_boundary() <= end:
                return end, False
            if rounds >= _MAX_BACKFILL_ROUNDS:
                self._log.warning(
                    f"{self._label}: history fetch is slower than the bar period; "
                    f"fetching the rest once after {rounds} rounds"
                )
                end = self._last_closed_boundary()
                await self._fetch_and_drain(fetch_range, end)
                return end, True

    async def _fetch_and_drain(
        self, fetch_range: Callable[[int, int], Awaitable[list[RawBar]]], end: int
    ) -> bool:
        """Fetch `[start, end]`, queue the result and wait until it is emitted.

        Return false if there was nothing to fetch.
        """
        start = self._floor
        if self._last_emitted is not None:
            start = max(start, self._last_emitted + self._period)
        if start > end:
            return False
        bars = await fetch_range(start, end)
        if self._closed:
            return True
        for bar in sorted(bars, key=lambda b: b.boundary_secs):
            if start <= bar.boundary_secs <= end:
                self._enqueue(bar.boundary_secs, bar, needs_history=False)
        await self._wait_drained()
        return True

    async def _wait_drained(self) -> None:
        """Wait for the queue to drain, but never longer than its own retries can take.

        A history request that never returns must not block `backfill`, and with it the
        caller's restore path, forever. The queued entries and the resolver are left alone.
        """
        if self._drained.is_set():
            return
        queued = len(self._queue)
        bound_secs = (1 + self._retries) * self._grace * queued + self._grace
        drained = asyncio.ensure_future(self._drained.wait())
        expiry = asyncio.ensure_future(self._sleep(bound_secs))
        try:
            await asyncio.wait({drained, expiry}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            drained.cancel()
            expiry.cancel()
        if not self._drained.is_set():
            self._log.warning(
                f"{self._label}: still resolving {len(self._queue)} closed bars after "
                f"{bound_secs} s; continuing the backfill"
            )

    def _floor_boundary(self, t: float) -> int:
        return (int(t) // self._period) * self._period

    def _last_closed_boundary(self) -> int:
        return self._floor_boundary(self._clock.now()) - self._period

    def _is_covered(self, boundary: int) -> bool:
        return self._last_emitted is not None and boundary <= self._last_emitted

    def _find(self, boundary: int) -> _Pending | None:
        return next((p for p in self._queue if p.boundary == boundary), None)

    def _arm(self, boundary: int) -> None:
        self._cancel_timer()
        # TODO(verify): local clock vs venue time; skew larger than grace could close a
        # still-forming bar.
        delay = boundary + self._period + self._grace - self._clock.now()
        self._timer = self._clock.call_later(max(delay, 0.0), lambda: self._on_timer(boundary))

    def _cancel_timer(self) -> None:
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None

    def _on_timer(self, boundary: int) -> None:
        current = self._current
        if self._closed or current is None or current.boundary_secs != boundary:
            return
        self._timer = None
        self._current = None
        self._enqueue(boundary, current, needs_history=True)

    def _enqueue(self, boundary: int, streamed: RawBar | None, *, needs_history: bool) -> None:
        if self._closed or boundary < self._floor or self._is_covered(boundary):
            return
        existing = self._find(boundary)
        if existing is not None:
            if existing.streamed is None:
                existing.streamed = streamed
            return
        index = sum(1 for p in self._queue if p.boundary < boundary)
        self._queue.insert(index, _Pending(boundary, streamed, needs_history))
        self._drained.clear()
        self._pump()

    def _pump(self) -> None:
        """Emit ready entries inline; hand over to the resolver at the first one needing history.

        Emitting inline is only done while no resolver is running, so the order is the same.
        """
        if self._resolver is not None:
            return
        if self._emit_ready() is None:
            self._drained.set()
        elif self._resolver is None:  # an emit callback may have re-entered and started one
            self._resolver = asyncio.get_running_loop().create_task(self._resolve())

    def _emit_ready(self) -> _Pending | None:
        """Pop and emit front entries until one needs history; return that one, if any."""
        while self._queue and not self._closed:
            front = self._queue[0]
            if front.needs_history and not self._is_covered(front.boundary):
                return front
            self._queue.popleft()
            if front.streamed is not None:
                self._emit(front.streamed)
        return None

    async def _resolve(self) -> None:
        try:
            while (pending := self._emit_ready()) is not None:
                try:
                    result = await self._from_history(pending)
                except CTraderConnectionError as e:
                    # With no connection there is nothing to ask, so the entry is left exactly
                    # as it is and this task stops. Whatever queues the next boundary restarts
                    # it - the reconnect's `backfill` at the latest - and by then history can
                    # serve the bar that closed during the outage.
                    self._log.debug(f"{self._label}: no connection for history ({e!r})")
                    return
                if self._closed:
                    return
                # Resolved: the entry now carries its final bar, or none if no bar exists.
                pending.streamed = result
                pending.needs_history = False
        finally:
            if asyncio.current_task() is self._resolver:
                self._resolver = None
            if not self._queue:
                self._drained.set()

    async def _from_history(self, pending: _Pending) -> RawBar | None:
        """Ask history for `pending`'s bar; fall back to its streamed state.

        Return `None` if nothing should be emitted. Raises `CTraderConnectionError` for the
        caller to leave the entry queued: see the class docstring.
        """
        boundary = pending.boundary
        error: Exception | None = None
        for attempt in range(1 + self._retries):
            if attempt:
                await self._sleep(self._grace)
            if not self._wanted(pending):
                return None
            try:
                result = await self._fetch(boundary)
            except CTraderConnectionError:
                # Kept ahead of the catch-all below, which would count it as an attempt.
                raise
            except Exception as e:
                # Any history failure is retried, then falls back to the streamed state.
                error = e
                self._log.debug(f"{self._label}: history request failed ({e!r})")
                continue
            error = None
            if result is not None and result.boundary_secs != boundary:
                self._log.debug(
                    f"{self._label}: history returned a bar for {result.boundary_secs}, "
                    f"expected {boundary}; treating it as no bar"
                )
                result = None
            if result is not None:
                return result
        if not self._wanted(pending):
            return None
        fallback = pending.streamed
        if error is not None:
            if fallback is not None:
                first_line = f"{self._label}: history request failed ({error!r}); " + (
                    "emitting the streamed state"
                )
                later_line = (
                    f"{self._label}: closed bar from streamed state "
                    f"(history request failed: {error!r})"
                )
            else:
                first_line = f"{self._label}: history request failed ({error!r}); no bar emitted"
                later_line = first_line
            self._warn_once("failed", first_line, later_line)
        elif fallback is not None:
            self._warn_once(
                "empty",
                f"{self._label}: history had no closed bar; emitting the streamed state",
                f"{self._label}: closed bar from streamed state",
            )
        return fallback

    def _wanted(self, pending: _Pending) -> bool:
        return (
            not self._closed
            and not self._is_covered(pending.boundary)
            and any(p is pending for p in self._queue)
        )

    def _warn_once(self, kind: str, first_line: str, later_line: str) -> None:
        if kind == "failed":
            already, self._warned_failed = self._warned_failed, True
        else:
            already, self._warned_empty = self._warned_empty, True
        if already:
            self._log.debug(later_line)
        else:
            self._log.warning(first_line)

    async def _sleep(self, delay_secs: float) -> None:
        # Driven by the injected clock, not `asyncio.sleep`, so a fake clock controls it.
        future = asyncio.get_running_loop().create_future()

        def wake() -> None:
            if not future.done():
                future.set_result(None)

        handle = self._clock.call_later(delay_secs, wake)
        try:
            await future
        finally:
            handle.cancel()

    def _emit(self, bar: RawBar) -> None:
        if self._is_covered(bar.boundary_secs):
            return
        self._last_emitted = bar.boundary_secs
        try:
            self._emit_cb(bar)
        except Exception as e:
            self._log.exception(f"{self._label}: emit callback raised", e)
