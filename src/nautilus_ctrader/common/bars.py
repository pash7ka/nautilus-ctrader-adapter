"""Pure state machine deciding when a live bar is closed, from the stream or a timer fallback.

No network I/O: `clock`, `fetch` and `fetch_range` are the only points of contact with the
outside world, all injected by the caller.
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


Fetch = Callable[[int], Awaitable[RawBar | None]]
Emit = Callable[[RawBar], None]


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
        self._current: RawBar | None = None
        self._last_emitted: int | None = None
        self._timer: asyncio.TimerHandle | Any | None = None
        self._task: asyncio.Task | None = None
        self._baseline = True
        self._holding = False
        self._fallback_warned = False

    @property
    def last_emitted(self) -> int | None:
        return self._last_emitted

    def mark_emitted(self, boundary_secs: int) -> None:
        if self._last_emitted is None or boundary_secs > self._last_emitted:
            self._last_emitted = boundary_secs

    def on_update(self, bar: RawBar) -> None:
        if self._holding:
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
        self._current = None
        self._cancel()
        self._baseline = True

    def close(self) -> None:
        self._cancel()

    async def backfill(self, fetch_range: Callable[[int, int], Awaitable[list[RawBar]]]) -> None:
        """Emit closed bars with boundary in `(last_emitted, now - period]`, in order.

        Stream updates are held until this returns; the next one after is a baseline.
        """
        self._holding = True
        try:
            end = int(self._clock.now()) - self._period
            start = self._last_emitted + self._period if self._last_emitted is not None else end
            for bar in await fetch_range(start, end):
                if bar.boundary_secs + self._period <= self._clock.now():
                    self._emit(bar)
        finally:
            self._holding = False
            self._baseline = True

    def _arm(self, boundary: int) -> None:
        self._cancel()
        delay = boundary + self._period + self._grace - self._clock.now()
        self._timer = self._clock.call_later(max(delay, 0.0), lambda: self._on_timer(boundary, 0))

    def _on_timer(self, boundary: int, attempt: int) -> None:
        if self._current is None or self._current.boundary_secs != boundary:
            return
        loop = asyncio.get_running_loop()
        self._task = loop.create_task(self._close_from_history(boundary, attempt))

    async def _close_from_history(self, boundary: int, attempt: int) -> None:
        try:
            bar = await self._fetch(boundary)
        except Exception as e:
            # Any history failure falls back to the streamed state rather than leaving the
            # bar unclosed.
            self._log.warning(f"{self._label}: history request for closing bar failed ({e!r})")
            bar = None
            attempt = self._retries
        if bar is not None:
            self._emit(bar)
            return
        if attempt < self._retries:
            self._timer = self._clock.call_later(
                self._grace, lambda: self._on_timer(boundary, attempt + 1)
            )
            return
        current = self._current
        if current is not None and current.boundary_secs == boundary:
            if not self._fallback_warned:
                self._fallback_warned = True
                self._log.warning(
                    f"{self._label}: history had no closed bar; emitting the streamed state"
                )
            else:
                self._log.debug(f"{self._label}: closed bar from streamed state")
            self._emit(current)

    def _emit(self, bar: RawBar) -> None:
        if self._last_emitted is not None and bar.boundary_secs <= self._last_emitted:
            return
        self._last_emitted = bar.boundary_secs
        if self._current is not None and self._current.boundary_secs == bar.boundary_secs:
            self._current = None
            self._cancel()
        self._emit_cb(bar)

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
