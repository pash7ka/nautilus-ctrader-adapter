"""Pure state machine deciding when a live bar is closed, from the stream or a timer fallback.

No network I/O: `clock`, `fetch` and `fetch_range` are the only points of contact with the
outside world, all injected by the caller. Every closed bar is emitted exactly once, whichever
of the stream, timer or backfill path closes it.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Protocol


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

# How many times `backfill` re-fetches a growing range before giving up on catching up and
# leaving the rest to the tail-boundary retry and ordinary live streaming.
_MAX_BACKFILL_ROUNDS = 5


class BarCloser:
    """
    Decides when one live bar type's forming bar is closed.

    - A stream update for a newer boundary closes the previous one immediately.
    - Otherwise a timer at `boundary + period + grace` asks history for the closed bar,
      retrying `history_retries` times, `grace` apart; if history never has it, the last
      streamed state is emitted instead.
    - The first update after construction, after `on_disconnect`, or after `backfill` is a
      baseline: a bar already past its end (the venue sends the last bar on subscribe even when
      the market is closed) is discarded instead of being treated as newly forming.
    - `close()` stops all of it permanently: further updates, timers and `backfill` calls are
      no-ops.

    The warn-once fallback flags - one for a failed history request, one for history simply
    having no bar - are per closer instance, i.e. per subscription: one bar type's history
    gaps do not silence another's, and one kind of failure does not silence the other.
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
        self._created_at = clock.now()
        self._current: RawBar | None = None
        self._last_emitted: int | None = None
        self._timer: asyncio.TimerHandle | Any | None = None
        self._task: asyncio.Task | None = None
        self._baseline = True
        self._holding = False
        self._held_update: RawBar | None = None
        self._fallback_warned_failed = False
        self._fallback_warned_empty = False
        self._closed = False

    @property
    def last_emitted(self) -> int | None:
        return self._last_emitted

    def mark_emitted(self, boundary_secs: int) -> None:
        if self._last_emitted is None or boundary_secs > self._last_emitted:
            self._last_emitted = boundary_secs

    def on_update(self, bar: RawBar) -> None:
        if self._closed:
            return
        if self._holding:
            # Keep only the latest; it is replayed once `backfill` releases the hold.
            self._held_update = bar
            return
        if self._last_emitted is not None and bar.boundary_secs <= self._last_emitted:
            return
        if self._baseline:
            self._baseline = False
            if bar.boundary_secs + self._period <= self._clock.now():
                return
        current = self._current
        if current is None:
            self._current = bar
            self._arm(bar.boundary_secs)
        elif bar.boundary_secs == current.boundary_secs:
            self._current = bar
        elif bar.boundary_secs > current.boundary_secs:
            self._emit(current)
            self._current = bar
            self._arm(bar.boundary_secs)

    def on_disconnect(self) -> None:
        if self._closed:
            return
        self._current = None
        self._cancel()
        self._baseline = True

    def close(self) -> None:
        self._closed = True
        self._current = None
        self._cancel()

    async def backfill(self, fetch_range: Callable[[int, int], Awaitable[list[RawBar]]]) -> None:
        """Emit every bar closed since the last one, then resume the stream as a baseline.

        `fetch_range(start, end)` returns bars for the **inclusive** range `[start, end]`, in
        ascending boundary order (the result is defensively re-sorted here regardless);
        `start` and `end` are both period-aligned boundaries. `end` is the last boundary already
        closed by the clock. `start` continues from `last_emitted`, or, if nothing has been
        emitted yet, from the bar that was forming when this closer was constructed - so a bar
        that closed during a reconnect gap is not lost, and nothing that closed before the
        subscription existed is emitted.

        If the closed range advances again while a fetch is in flight, it is fetched again from
        where the previous one left off, until stable, for up to `_MAX_BACKFILL_ROUNDS`; past
        that, a WARNING is logged and refining stops. Either way, if the most recent closed
        boundary still has no bar afterwards, history may simply not have served it yet (it can
        lag behind a bar's close): a stream update held for exactly that boundary is promoted to
        `_current` and retried through the normal history path instead of being discarded by the
        baseline rule; with no such update, a history-only retry is armed, emitting nothing if it
        stays empty, since a bar with no ticks does not exist. Earlier gaps in the range are
        treated as genuinely empty periods. Stream updates are held while this runs; the latest
        one still unused afterwards is replayed through the normal `on_update` path.
        """
        if self._closed:
            return
        self._current = None
        self._cancel()
        self._holding = True
        self._held_update = None
        try:
            rounds = 0
            while True:
                end = self._last_closed_boundary()
                start = (
                    self._last_emitted + self._period
                    if self._last_emitted is not None
                    else self._floor_boundary(self._created_at)
                )
                if start > end:
                    break
                closed_bars = await fetch_range(start, end)
                if self._closed:
                    return
                for closed_bar in sorted(closed_bars, key=lambda b: b.boundary_secs):
                    if closed_bar.boundary_secs + self._period <= self._clock.now():
                        self._emit_during_backfill(closed_bar)
                rounds += 1
                if self._last_closed_boundary() <= end:
                    break
                if rounds >= _MAX_BACKFILL_ROUNDS:
                    self._log.warning(
                        f"{self._label}: history fetch is slower than the bar period; "
                        f"stopped refining the backfill range after {rounds} rounds"
                    )
                    break
        finally:
            self._holding = False
            self._baseline = True
        held, self._held_update = self._held_update, None
        tail = self._last_closed_boundary()
        if self._last_emitted is None or tail > self._last_emitted:
            if held is not None and held.boundary_secs == tail:
                self._current = held
                held = None
            self._arm_immediately(tail)
        if held is not None:
            self.on_update(held)

    def _floor_boundary(self, t: float) -> int:
        return (int(t) // self._period) * self._period

    def _last_closed_boundary(self) -> int:
        return self._floor_boundary(self._clock.now()) - self._period

    def _arm(self, boundary: int) -> None:
        self._cancel()
        # TODO(verify): local clock vs venue time; skew larger than grace could close a
        # still-forming bar.
        delay = boundary + self._period + self._grace - self._clock.now()
        self._timer = self._clock.call_later(max(delay, 0.0), lambda: self._on_timer(boundary, 0))

    def _arm_immediately(self, boundary: int) -> None:
        """Like `_arm`, but for a boundary that is already overdue: retry history right away."""
        self._cancel()
        self._timer = self._clock.call_later(0.0, lambda: self._on_timer(boundary, 0))

    def _on_timer(self, boundary: int, attempt: int) -> None:
        if self._closed or self._holding:
            return
        # `_current` may be `None` here: a history-only tail retry (see `backfill`) has no
        # streamed state to track, only a boundary to keep asking history about.
        if self._current is not None and self._current.boundary_secs != boundary:
            return
        loop = asyncio.get_running_loop()
        self._task = loop.create_task(self._close_from_history(boundary, attempt))

    async def _close_from_history(self, boundary: int, attempt: int) -> None:
        if self._closed:
            return
        error: Exception | None = None
        try:
            result = await self._fetch(boundary)
        except Exception as e:
            # Any history failure falls back to the streamed state rather than leaving the
            # bar unclosed.
            error = e
            result = None
        else:
            if result is not None and result.boundary_secs != boundary:
                self._log.debug(
                    f"{self._label}: history returned a bar for {result.boundary_secs}, "
                    f"expected {boundary}; treating it as no bar"
                )
                result = None
        if self._closed:
            return
        if result is not None:
            self._safe_emit(result)
            return
        if error is None and attempt < self._retries:
            self._timer = self._clock.call_later(
                self._grace, lambda: self._on_timer(boundary, attempt + 1)
            )
            return
        current = self._current
        if current is None or current.boundary_secs != boundary:
            return
        if self._last_emitted is not None and current.boundary_secs <= self._last_emitted:
            # Already covered by another path (e.g. a concurrent stream close or backfill);
            # nothing to report, and this must not count as a fallback.
            return
        if error is not None:
            first_line = (
                f"{self._label}: history request failed ({error!r}); emitting the streamed state"
            )
            later_line = (
                f"{self._label}: closed bar from streamed state (history request failed: {error!r})"
            )
            already_warned = self._fallback_warned_failed
            self._fallback_warned_failed = True
        else:
            first_line = f"{self._label}: history had no closed bar; emitting the streamed state"
            later_line = f"{self._label}: closed bar from streamed state"
            already_warned = self._fallback_warned_empty
            self._fallback_warned_empty = True
        if not already_warned:
            self._log.warning(first_line)
        else:
            self._log.debug(later_line)
        self._safe_emit(current)

    def _emit(self, bar: RawBar) -> None:
        if self._closed:
            return
        if self._last_emitted is not None and bar.boundary_secs <= self._last_emitted:
            return
        self._last_emitted = bar.boundary_secs
        if self._current is not None and self._current.boundary_secs == bar.boundary_secs:
            self._current = None
            self._cancel()
        self._emit_cb(bar)

    def _safe_emit(self, bar: RawBar) -> None:
        # Runs inside `self._task`: an uncaught exception here would just be a "Task exception
        # was never retrieved" log line, invisible to the caller.
        try:
            self._emit(bar)
        except Exception as e:
            self._log.error(f"{self._label}: emit callback raised {e!r}")

    def _emit_during_backfill(self, bar: RawBar) -> None:
        # A bad callback must not abort the rest of the range or the held-update replay that
        # follows `backfill`'s loop.
        try:
            self._emit(bar)
        except Exception as e:
            self._log.exception(f"{self._label}: emit callback raised during backfill", e)

    def _cancel(self) -> None:
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        if (
            self._task is not None
            and not self._task.done()
            and self._task is not asyncio.current_task()
        ):
            self._task.cancel()
        self._task = None
