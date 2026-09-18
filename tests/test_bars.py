"""Tests for the bar-close state machine.

`FakeClock` stands in for `Clock`: `advance` fires any due timers, then yields a few event-loop
turns so the tasks those timers wake (the resolver and its history fetches) get to run.

Probe tests use offsets from `T0`, a 60 s period, grace 1 s, 2 retries, and a `History` feed that
serves a bar `lag` seconds after it closes.
"""

from __future__ import annotations

import asyncio

from nautilus_ctrader.common.bars import BarCloser, RawBar
from tests.recording_logger import RecordingLogger

PERIOD = 60
GRACE = 1.0
RETRIES = 2
T0 = 1_800_000_000 - 1_800_000_000 % 60


class FakeClock:
    def __init__(self, now: float) -> None:
        self.t = now
        self.timers: list[list] = []  # [due, callback, cancelled]

    def now(self) -> float:
        return self.t

    def call_later(self, delay, cb):
        entry = [self.t + delay, cb, False]
        self.timers.append(entry)

        class H:
            def cancel(_self):
                entry[2] = True

        return H()

    async def advance(self, secs: float) -> None:
        self.t += secs
        for entry in sorted(self.timers, key=lambda e: e[0]):
            if not entry[2] and entry[0] <= self.t:
                entry[2] = True
                entry[1]()
        for _ in range(5):
            await asyncio.sleep(0)


async def settle() -> None:
    for _ in range(10):
        await asyncio.sleep(0)


def bar(boundary: int, tag: int | str = 0) -> RawBar:
    # `delta_close` carries a tag so tests can tell bars apart by identity, not just boundary.
    return RawBar(
        boundary_secs=boundary, low=0, delta_open=0, delta_high=0, delta_close=tag, volume=0
    )


def h(offset: int) -> RawBar:
    """The history version of the bar at `T0 + offset`."""
    return bar(T0 + offset, tag=f"h{offset}")


def s(offset: int) -> RawBar:
    """A streamed state of the bar at `T0 + offset`."""
    return bar(T0 + offset, tag=f"s{offset}")


def offsets(emitted: list[RawBar]) -> list[int]:
    return [b.boundary_secs - T0 for b in emitted]


class FetchStub:
    """An async `Fetch` returning queued results in order, `None` once exhausted."""

    def __init__(self, results: list[RawBar | Exception | None]) -> None:
        self._results = list(results)
        self.calls: list[int] = []

    async def __call__(self, boundary_secs: int) -> RawBar | None:
        self.calls.append(boundary_secs)
        result = self._results.pop(0) if self._results else None
        if isinstance(result, Exception):
            raise result
        return result


class History:
    """A history feed that serves a bar `lag` seconds after it closes.

    `range_cost_secs` advances the clock on every `fetch_range` call; `range_gate`, if set,
    suspends the call until it is released.
    """

    def __init__(self, clock: FakeClock, lag: float, range_cost_secs: float = 0.0) -> None:
        self._clock = clock
        self._lag = lag
        self._cost = range_cost_secs
        self.range_gate: asyncio.Event | None = None
        self.calls: list[int] = []
        self.range_calls: list[tuple[int, int]] = []

    def _served(self, boundary: int) -> bool:
        return boundary + PERIOD + self._lag <= self._clock.now()

    async def fetch(self, boundary_secs: int) -> RawBar | None:
        self.calls.append(boundary_secs)
        return h(boundary_secs - T0) if self._served(boundary_secs) else None

    async def fetch_range(self, start: int, end: int) -> list[RawBar]:
        self.range_calls.append((start, end))
        if self.range_gate is not None:
            await self.range_gate.wait()
        self._clock.t += self._cost
        return [h(b - T0) for b in range(start, end + 1, PERIOD) if self._served(b)]


def make_closer(
    clock, fetch, emitted: list[RawBar], logger: RecordingLogger, label: str = "L"
) -> BarCloser:
    return BarCloser(
        period_secs=PERIOD,
        grace_secs=GRACE,
        history_retries=RETRIES,
        clock=clock,
        fetch=fetch,
        emit=emitted.append,
        logger=logger,
        label=label,
    )


def warnings_of(logger: RecordingLogger) -> list[str]:
    return [line for level, line in logger.lines if level == "warning"]


def debugs_of(logger: RecordingLogger) -> list[str]:
    return [line for level, line in logger.lines if level == "debug"]


# --- Stream and timer paths ---------------------------------------------------------------------


def test_a_stream_update_for_a_newer_bar_closes_the_previous_one() -> None:
    clock = FakeClock(T0)
    emitted: list[RawBar] = []
    fetch = FetchStub([])
    closer = make_closer(clock, fetch, emitted, RecordingLogger())

    closer.on_update(bar(T0, tag=1))
    closer.on_update(bar(T0, tag=2))
    closer.on_update(bar(T0 + 60, tag=3))

    assert emitted == [bar(T0, tag=2)]
    assert fetch.calls == []


async def test_a_timer_closes_via_history_fetch_after_grace() -> None:
    clock = FakeClock(T0 + 10)
    emitted: list[RawBar] = []
    history_bar = bar(T0, tag="history")
    fetch = FetchStub([history_bar])
    closer = make_closer(clock, fetch, emitted, RecordingLogger())

    closer.on_update(bar(T0, tag="stream"))
    await clock.advance(51)  # now T0+61: boundary + period + grace

    assert fetch.calls == [T0]
    assert emitted == [history_bar]


async def test_empty_history_retries_grace_apart_then_falls_back_to_the_streamed_state() -> None:
    clock = FakeClock(T0)
    fetch = FetchStub([])  # always None
    emitted: list[RawBar] = []
    logger = RecordingLogger()
    closer = make_closer(clock, fetch, emitted, logger)
    stream_bar = bar(T0, tag="stream")

    closer.on_update(stream_bar)
    await clock.advance(61)  # first attempt, at T0+61
    pending = [e[0] for e in clock.timers if not e[2]]
    assert pending == [T0 + 61 + GRACE]  # first retry armed exactly `grace` later

    await clock.advance(1)  # first retry fires at T0+62
    pending = [e[0] for e in clock.timers if not e[2]]
    assert pending == [T0 + 62 + GRACE]  # second retry armed exactly `grace` later

    await clock.advance(1)  # second retry fires at T0+63, exhausted -> fallback

    assert fetch.calls == [T0, T0, T0]
    assert emitted == [stream_bar]
    assert warnings_of(logger) == [
        "L: history had no closed bar; emitting the streamed state",
    ]


async def test_a_second_fallback_in_the_same_session_logs_debug_not_warning() -> None:
    clock = FakeClock(T0)
    fetch = FetchStub([])  # always None
    emitted: list[RawBar] = []
    logger = RecordingLogger()
    closer = make_closer(clock, fetch, emitted, logger)

    first_bar = bar(T0, tag="first")
    closer.on_update(first_bar)
    await clock.advance(61)
    await clock.advance(1)
    await clock.advance(1)

    second_bar = bar(T0 + 60, tag="second")
    closer.on_update(second_bar)
    await clock.advance(58)  # (T0+60) + period + grace - now
    await clock.advance(1)
    await clock.advance(1)

    assert emitted == [first_bar, second_bar]
    assert len(warnings_of(logger)) == 1
    assert debugs_of(logger) == ["L: closed bar from streamed state"]


async def test_a_fetch_that_keeps_raising_is_retried_then_falls_back() -> None:
    clock = FakeClock(T0)
    boom = RuntimeError("boom")
    fetch = FetchStub([boom, boom, boom])
    emitted: list[RawBar] = []
    logger = RecordingLogger()
    stream_bar = bar(T0, tag="stream")
    closer = make_closer(clock, fetch, emitted, logger)

    closer.on_update(stream_bar)
    await clock.advance(61)
    await clock.advance(1)
    await clock.advance(1)

    assert fetch.calls == [T0, T0, T0]
    assert emitted == [stream_bar]
    assert warnings_of(logger) == [
        "L: history request failed (RuntimeError('boom')); emitting the streamed state",
    ]


async def test_a_fetch_that_raises_once_is_retried_and_history_still_wins() -> None:
    clock = FakeClock(T0)
    history_bar = bar(T0, tag="history")
    fetch = FetchStub([RuntimeError("boom"), history_bar])
    emitted: list[RawBar] = []
    logger = RecordingLogger()
    closer = make_closer(clock, fetch, emitted, logger)

    closer.on_update(bar(T0, tag="stream"))
    await clock.advance(61)
    await clock.advance(1)

    assert fetch.calls == [T0, T0]
    assert emitted == [history_bar]
    assert warnings_of(logger) == []


def test_baseline_discards_a_first_update_already_past_its_end() -> None:
    clock = FakeClock(T0 + 120)
    fetch = FetchStub([])
    emitted: list[RawBar] = []
    closer = make_closer(clock, fetch, emitted, RecordingLogger())

    closer.on_update(bar(T0, tag="stale"))
    assert emitted == []
    assert clock.timers == []  # no timer armed for a bar that was never treated as forming

    # A later, still-forming bar behaves normally.
    closer.on_update(bar(T0 + 120, tag="live"))
    closer.on_update(bar(T0 + 180, tag="live2"))
    assert emitted == [bar(T0 + 120, tag="live")]


async def test_a_history_fetch_in_flight_does_not_double_emit_the_closed_bar() -> None:
    clock = FakeClock(T0 + 10)
    emitted: list[RawBar] = []
    gate = asyncio.Event()
    calls: list[int] = []
    history_bar = bar(T0, tag="history")

    async def fetch(boundary_secs: int) -> RawBar | None:
        calls.append(boundary_secs)
        await gate.wait()
        return history_bar

    closer = make_closer(clock, fetch, emitted, RecordingLogger())
    closer.on_update(bar(T0, tag="stream"))
    await clock.advance(51)  # timer fires, the resolver suspends on the gate

    assert calls == [T0]
    assert emitted == []

    closer.on_update(bar(T0 + 60, tag="next"))  # the next bar starts forming
    assert emitted == []

    gate.set()
    await settle()
    assert emitted == [history_bar]

    closer.on_update(bar(T0 + 120, tag="after"))
    assert emitted == [history_bar, bar(T0 + 60, tag="next")]  # T0 exactly once
    assert calls == [T0]


def test_mark_emitted_makes_a_stream_update_at_or_before_it_a_no_op() -> None:
    clock = FakeClock(T0)
    fetch = FetchStub([])
    emitted: list[RawBar] = []
    closer = make_closer(clock, fetch, emitted, RecordingLogger())

    closer.mark_emitted(T0)
    closer.on_update(bar(T0, tag="stale"))
    assert emitted == []

    closer.on_update(bar(T0 + 60, tag="live"))
    closer.on_update(bar(T0 + 120, tag="live2"))
    assert emitted == [bar(T0 + 60, tag="live")]


async def test_on_disconnect_discards_the_partial_bar_and_queues_a_closed_baseline() -> None:
    clock = FakeClock(T0)
    fetch = FetchStub([])  # history never has it
    emitted: list[RawBar] = []
    closer = make_closer(clock, fetch, emitted, RecordingLogger())

    closer.on_update(bar(T0, tag="partial"))
    assert not clock.timers[0][2]  # armed before disconnect

    closer.on_disconnect()
    assert clock.timers[0][2]  # the pending close timer was cancelled

    await clock.advance(70)  # past T0's end; nothing fires
    assert fetch.calls == []

    # The first update after reconnect is a baseline for a bar that has already closed: it is
    # queued for history, with this state as the fallback, never the partial one.
    closer.on_update(bar(T0, tag="late"))
    await clock.advance(0)
    await clock.advance(1)
    await clock.advance(1)
    assert fetch.calls == [T0, T0, T0]
    assert emitted == [bar(T0, tag="late")]

    closer.on_update(bar(T0 + 60, tag="fresh"))
    closer.on_update(bar(T0 + 120, tag="fresh2"))
    assert emitted == [bar(T0, tag="late"), bar(T0 + 60, tag="fresh")]


async def test_fallback_is_silent_and_preserves_the_warn_once_flag_when_already_covered() -> None:
    clock = FakeClock(T0)
    fetch = FetchStub([])  # always None
    emitted: list[RawBar] = []
    logger = RecordingLogger()
    stream_bar = bar(T0, tag="stream")
    closer = make_closer(clock, fetch, emitted, logger)

    closer.on_update(stream_bar)
    await clock.advance(61)
    await clock.advance(1)

    closer.mark_emitted(T0)  # something else has already covered this boundary

    await clock.advance(1)  # exhausted -> would fall back, but is now covered

    assert emitted == []
    assert logger.lines == []  # neither logged nor counted as a fallback

    second_bar = bar(T0 + 60, tag="second")
    closer.on_update(second_bar)
    await clock.advance(58)
    await clock.advance(1)
    await clock.advance(1)

    assert emitted == [second_bar]
    assert len(warnings_of(logger)) == 1  # the warn-once flag was not spent by the silent fallback
    assert debugs_of(logger) == []


async def test_close_stops_updates_timers_and_backfill() -> None:
    clock = FakeClock(T0)
    fetch = FetchStub([bar(T0, tag="history")])
    emitted: list[RawBar] = []
    closer = make_closer(clock, fetch, emitted, RecordingLogger())

    closer.on_update(bar(T0, tag="stream"))
    closer.close()
    assert closer.last_emitted is None

    await clock.advance(61)  # the pending close timer must not fire into a fetch
    assert emitted == []
    assert fetch.calls == []

    closer.on_update(bar(T0 + 60, tag="ignored"))
    assert emitted == []

    async def fetch_range(start: int, end: int) -> list[RawBar]:
        raise AssertionError("fetch_range must not be called after close")

    await closer.backfill(fetch_range)
    assert emitted == []


async def test_close_stops_a_resolver_that_is_retrying() -> None:
    clock = FakeClock(T0)
    fetch = FetchStub([])  # always None
    emitted: list[RawBar] = []
    logger = RecordingLogger()
    closer = make_closer(clock, fetch, emitted, logger)

    closer.on_update(bar(T0, tag="stream"))
    await clock.advance(61)  # first attempt; the resolver now waits `grace` for the retry
    assert fetch.calls == [T0]

    closer.close()
    await clock.advance(1)
    await clock.advance(1)

    assert fetch.calls == [T0]
    assert emitted == []
    assert logger.lines == []


async def test_an_emit_callback_exception_from_the_history_path_is_caught_and_logged() -> None:
    clock = FakeClock(T0 + 10)
    history_bar = bar(T0, tag="history")
    fetch = FetchStub([history_bar])
    logger = RecordingLogger()

    def bad_emit(_bar: RawBar) -> None:
        raise RuntimeError("emit blew up")

    closer = BarCloser(
        period_secs=PERIOD,
        grace_secs=GRACE,
        history_retries=RETRIES,
        clock=clock,
        fetch=fetch,
        emit=bad_emit,
        logger=logger,
        label="L",
    )
    closer.on_update(bar(T0, tag="stream"))
    await clock.advance(51)  # timer fires, the resolver fetches, emit raises

    assert len(logger.errors()) == 1
    assert closer.last_emitted == T0  # state stayed consistent despite the callback failing


async def test_an_emit_callback_exception_does_not_stop_the_queue() -> None:
    clock = FakeClock(T0)
    logger = RecordingLogger()
    emitted: list[RawBar] = []

    def emit(b: RawBar) -> None:
        if b.boundary_secs == T0:
            raise RuntimeError("emit blew up")
        emitted.append(b)

    closer = BarCloser(
        period_secs=PERIOD,
        grace_secs=GRACE,
        history_retries=RETRIES,
        clock=clock,
        fetch=FetchStub([]),
        emit=emit,
        logger=logger,
        label="L",
    )
    closer.on_update(bar(T0, tag="a"))
    closer.on_update(bar(T0 + 60, tag="b"))  # stream close of T0 raises
    closer.on_update(bar(T0 + 120, tag="c"))  # stream close of T0+60 still goes out

    assert emitted == [bar(T0 + 60, tag="b")]
    assert logger.errors() == ["L: emit callback raised"]


async def test_a_history_bar_for_a_newer_boundary_is_treated_as_no_bar() -> None:
    clock = FakeClock(T0)
    wrong = bar(T0 + 60, tag="wrong-newer")
    fetch = FetchStub([wrong, wrong, wrong])
    emitted: list[RawBar] = []
    logger = RecordingLogger()
    stream_bar = bar(T0, tag="stream")
    closer = make_closer(clock, fetch, emitted, logger)

    closer.on_update(stream_bar)
    await clock.advance(61)
    await clock.advance(1)
    await clock.advance(1)

    assert fetch.calls == [T0, T0, T0]
    assert emitted == [stream_bar]  # the mismatched bar was never emitted
    assert len(debugs_of(logger)) == 3  # one mismatch note per attempt
    assert len(warnings_of(logger)) == 1  # the eventual fallback, not the mismatches


async def test_a_history_bar_for_an_older_boundary_is_treated_as_no_bar() -> None:
    clock = FakeClock(T0)
    wrong = bar(T0 - 60, tag="wrong-older")
    fetch = FetchStub([wrong, wrong, wrong])
    emitted: list[RawBar] = []
    logger = RecordingLogger()
    stream_bar = bar(T0, tag="stream")
    closer = make_closer(clock, fetch, emitted, logger)

    closer.on_update(stream_bar)
    await clock.advance(61)
    await clock.advance(1)
    await clock.advance(1)

    assert fetch.calls == [T0, T0, T0]
    assert emitted == [stream_bar]
    assert len(debugs_of(logger)) == 3
    assert len(warnings_of(logger)) == 1


async def test_the_warn_once_flag_is_separate_per_fallback_kind() -> None:
    clock = FakeClock(T0)
    boom = RuntimeError("boom")
    fetch = FetchStub([boom, boom, boom])  # first bar: failures; second: empty history
    emitted: list[RawBar] = []
    logger = RecordingLogger()
    closer = make_closer(clock, fetch, emitted, logger)

    first_bar = bar(T0, tag="first")
    closer.on_update(first_bar)
    await clock.advance(61)
    await clock.advance(1)
    await clock.advance(1)  # exhausted on failures -> WARNING (failed kind)

    second_bar = bar(T0 + 60, tag="second")
    closer.on_update(second_bar)
    await clock.advance(58)
    await clock.advance(1)
    await clock.advance(1)  # exhausted on empty history -> WARNING (empty kind, still fresh)

    assert emitted == [first_bar, second_bar]
    assert len(warnings_of(logger)) == 2  # one kind's warning does not silence the other's


async def test_the_stream_close_of_a_later_bar_waits_behind_an_earlier_history_entry() -> None:
    clock = FakeClock(T0 + 5)
    emitted: list[RawBar] = []
    gate = asyncio.Event()

    async def fetch(boundary_secs: int) -> RawBar | None:
        await gate.wait()
        return h(boundary_secs - T0)

    async def fetch_range(start: int, end: int) -> list[RawBar]:
        return []  # history has not served T0+60 yet

    closer = make_closer(clock, fetch, emitted, RecordingLogger())
    closer.mark_emitted(T0)
    clock.t = T0 + 130
    await closer.backfill(fetch_range)  # queues T0+60 as a history-only tail
    await settle()  # the resolver is now waiting on history for T0+60

    closer.on_update(s(120))
    clock.t = T0 + 180.5
    closer.on_update(s(180))  # the stream closes T0+120 while T0+60 is still unresolved
    assert emitted == []

    gate.set()
    await settle()
    assert emitted == [h(60), s(120)]
    closer.close()


# --- Backfill -----------------------------------------------------------------------------------


async def test_backfill_emits_closed_bars_in_order_and_holds_updates() -> None:
    clock = FakeClock(T0 + 5)
    fetch = FetchStub([])
    emitted: list[RawBar] = []
    closer = make_closer(clock, fetch, emitted, RecordingLogger())
    closer.mark_emitted(T0)
    clock.t = T0 + 180  # an aligned boundary

    gate = asyncio.Event()
    calls: list[tuple[int, int]] = []
    b1 = bar(T0 + 60, tag="h1")
    b2 = bar(T0 + 120, tag="h2")
    still_forming = bar(T0 + 300, tag="still-forming")  # outside the range: filtered out

    async def fetch_range(start: int, end: int) -> list[RawBar]:
        calls.append((start, end))
        await gate.wait()
        return [b1, b2, still_forming]

    task = asyncio.create_task(closer.backfill(fetch_range))
    await asyncio.sleep(0)  # backfill starts and suspends on the gate

    # end = floor(now/period)*period - period = T0+180-60 = T0+120 (now already aligned)
    assert calls == [(T0 + 60, T0 + 120)]

    closer.on_update(bar(T0 + 60, tag="held"))  # held while backfill is in flight
    assert emitted == []

    gate.set()
    await task

    assert emitted == [b1, b2]  # each closed bar exactly once; the held update deduped
    assert closer.last_emitted == T0 + 120
    assert fetch.calls == []

    closer.on_update(bar(T0 + 180, tag="live"))  # the forming bar, after backfill
    closer.on_update(bar(T0 + 240, tag="live2"))
    assert emitted == [b1, b2, bar(T0 + 180, tag="live")]


async def test_backfill_that_spans_a_boundary_replays_and_dedupes_the_held_update() -> None:
    """`end` is recomputed when it advances while the fetch is in flight."""
    clock = FakeClock(T0 + 5)
    fetch = FetchStub([])
    emitted: list[RawBar] = []
    closer = make_closer(clock, fetch, emitted, RecordingLogger())
    closer.mark_emitted(T0)
    clock.t = T0 + 200

    gate1 = asyncio.Event()
    calls: list[tuple[int, int]] = []

    async def fetch_range(start: int, end: int) -> list[RawBar]:
        calls.append((start, end))
        if len(calls) == 1:
            await gate1.wait()
            clock.t = T0 + 245  # time passes during the request
            return [bar(T0 + 60, tag="h1"), bar(T0 + 120, tag="h2")]
        return [bar(T0 + 180, tag="h3")]

    task = asyncio.create_task(closer.backfill(fetch_range))
    await asyncio.sleep(0)  # backfill starts: fetch_range(T0+60, T0+120), suspends on gate1

    assert calls == [(T0 + 60, T0 + 120)]

    closer.on_update(bar(T0 + 180, tag="stream"))  # held while backfill is in flight

    gate1.set()
    await task

    # The advanced range (T0+120, T0+180] is re-fetched once the first call returns.
    assert calls == [(T0 + 60, T0 + 120), (T0 + 180, T0 + 180)]
    assert emitted == [
        bar(T0 + 60, tag="h1"),
        bar(T0 + 120, tag="h2"),
        bar(T0 + 180, tag="h3"),
    ]
    assert closer.last_emitted == T0 + 180  # exactly once, in order; the held update deduped


async def test_backfill_cancels_the_forming_bar_timer() -> None:
    clock = FakeClock(T0 + 10)
    fetch = FetchStub([])
    emitted: list[RawBar] = []
    closer = make_closer(clock, fetch, emitted, RecordingLogger())
    closer.on_update(bar(T0, tag="stream"))  # arms a timer for T0, due at T0+61

    clock.t = T0 + 130  # time passes; backfill now has a real gap to fetch
    gate = asyncio.Event()

    async def fetch_range(start: int, end: int) -> list[RawBar]:
        await gate.wait()
        return []

    task = asyncio.create_task(closer.backfill(fetch_range))
    await asyncio.sleep(0)  # backfill started and is suspended on the gate

    await clock.advance(0)  # the T0 timer is overdue; it must have been cancelled
    assert fetch.calls == []
    assert emitted == []

    gate.set()
    await task
    await clock.advance(0)
    assert T0 not in fetch.calls  # only the history-only tail (T0+60) is asked about
    assert emitted == []
    closer.close()


async def test_backfill_starts_from_the_bar_forming_at_creation_when_never_emitted() -> None:
    clock = FakeClock(T0 + 5)
    fetch = FetchStub([])
    emitted: list[RawBar] = []
    closer = make_closer(clock, fetch, emitted, RecordingLogger())  # created_at = T0+5

    clock.t = T0 + 200
    calls: list[tuple[int, int]] = []
    h1 = bar(T0, tag="h1")

    async def fetch_range(start: int, end: int) -> list[RawBar]:
        calls.append((start, end))
        return [h1]

    await closer.backfill(fetch_range)

    # end = floor(200/60)*60 - 60 = T0+120; start = floor(5/60)*60 = T0 (the bar forming at
    # creation, so nothing that closed before the subscription existed is emitted).
    assert calls == [(T0, T0 + 120)]
    assert emitted == [h1]
    assert closer.last_emitted == T0
    closer.close()


async def test_backfill_skips_the_fetch_and_still_resets_baseline_when_nothing_has_closed() -> None:
    clock = FakeClock(T0 + 240)  # an aligned boundary
    fetch = FetchStub([])
    emitted: list[RawBar] = []
    closer = make_closer(clock, fetch, emitted, RecordingLogger())
    closer.mark_emitted(T0 + 180)  # an aligned boundary

    calls: list[tuple[int, int]] = []

    async def fetch_range(start: int, end: int) -> list[RawBar]:
        calls.append((start, end))
        return []

    await closer.backfill(fetch_range)

    # end = T0+180; start = last_emitted + period = T0+240 > end -> nothing to fetch, and
    # nothing closed since last_emitted, so no tail-boundary retry is queued either.
    assert calls == []
    assert emitted == []
    assert clock.timers == []

    clock.t = T0 + 400  # time passes before the post-backfill baseline update arrives
    closer.on_update(bar(T0 + 200, tag="stale"))  # before the subscription: ignored
    assert emitted == []
    closer.on_update(bar(T0 + 400, tag="live"))
    closer.on_update(bar(T0 + 460, tag="live2"))
    assert emitted == [bar(T0 + 400, tag="live")]


async def test_backfill_recovers_the_most_recent_boundary_when_history_still_lags() -> None:
    """Probe D: the held update is the closing state of the tail boundary."""
    clock = FakeClock(T0 + 5)
    hist = History(clock, lag=3)
    hist.range_gate = asyncio.Event()
    emitted: list[RawBar] = []
    closer = make_closer(clock, hist.fetch, emitted, RecordingLogger())
    closer.mark_emitted(T0)
    clock.t = T0 + 241

    task = asyncio.create_task(closer.backfill(hist.fetch_range))
    await asyncio.sleep(0)
    assert hist.range_calls == [(T0 + 60, T0 + 180)]

    closer.on_update(s(180))  # held; the closing snapshot of T0+180
    hist.range_gate.set()
    await task

    # History served 60 and 120; 180 lags by 3 s and is retried from history.
    assert emitted == [h(60), h(120)]

    await clock.advance(0)  # T0+241: still lagging
    await clock.advance(1)  # T0+242: still lagging
    assert emitted == [h(60), h(120)]
    await clock.advance(1)  # T0+243: served
    assert emitted == [h(60), h(120), h(180)]
    assert hist.calls == [T0 + 180] * 3

    closer.on_update(s(240))
    closer.on_update(s(300))
    assert emitted == [h(60), h(120), h(180), s(240)]


async def test_backfill_queues_a_history_only_retry_for_a_missing_unheld_tail() -> None:
    clock = FakeClock(T0 + 5)
    emitted: list[RawBar] = []
    logger = RecordingLogger()
    calls: list[int] = []

    async def fetch(boundary_secs: int) -> RawBar | None:
        calls.append(boundary_secs)
        return None  # the bar never shows up in history: it had no ticks

    async def fetch_range(start: int, end: int) -> list[RawBar]:
        return []  # nothing closed has been served yet

    closer = make_closer(clock, fetch, emitted, logger)
    closer.mark_emitted(T0)
    clock.t = T0 + 130

    await closer.backfill(fetch_range)

    # end = floor(130/60)*60-60 = T0+60, which is above last_emitted -> a tail retry is queued.
    await clock.advance(0)
    await clock.advance(GRACE)
    await clock.advance(GRACE)

    assert calls == [T0 + 60] * 3
    assert emitted == []  # a bar with no ticks does not exist
    assert logger.lines == []  # silent: there was never a streamed state to fall back to


async def test_backfill_caps_refining_then_fetches_the_rest_once_and_warns() -> None:
    clock = FakeClock(T0)
    fetch = FetchStub([])
    emitted: list[RawBar] = []
    logger = RecordingLogger()
    closer = make_closer(clock, fetch, emitted, logger)

    clock.t = T0 + 500  # give it a real gap to work through

    calls: list[tuple[int, int]] = []

    async def fetch_range(start: int, end: int) -> list[RawBar]:
        calls.append((start, end))
        clock.t += PERIOD  # history is perpetually one period further behind
        return []

    await closer.backfill(fetch_range)

    assert len(calls) == 6  # five rounds, then one final fetch; never an endless loop
    assert emitted == []
    warnings = warnings_of(logger)
    assert len(warnings) == 1
    assert "slower than the bar period" in warnings[0]
    closer.close()


async def test_an_emit_callback_exception_during_backfill_is_caught_and_replay_continues() -> None:
    clock = FakeClock(T0 + 5)
    fetch = FetchStub([])
    logger = RecordingLogger()
    emitted: list[RawBar] = []

    bad_boundary = T0 + 60
    b1 = bar(bad_boundary, tag="bad")
    b2 = bar(T0 + 120, tag="good")

    def emit(b: RawBar) -> None:
        if b.boundary_secs == bad_boundary:
            raise RuntimeError("emit blew up")
        emitted.append(b)

    closer = BarCloser(
        period_secs=PERIOD,
        grace_secs=GRACE,
        history_retries=RETRIES,
        clock=clock,
        fetch=fetch,
        emit=emit,
        logger=logger,
        label="L",
    )
    closer.mark_emitted(T0)
    clock.t = T0 + 200

    gate = asyncio.Event()

    async def fetch_range(start: int, end: int) -> list[RawBar]:
        await gate.wait()
        return [b1, b2]

    task = asyncio.create_task(closer.backfill(fetch_range))
    await asyncio.sleep(0)

    closer.on_update(bar(T0 + 300, tag="held"))  # a still-forming bar, held during backfill

    gate.set()
    await task

    assert logger.errors() == ["L: emit callback raised"]
    assert emitted == [b2]  # the rest of the range was still emitted
    assert closer.last_emitted == T0 + 120  # recorded despite the callback failing

    # The held update was still replayed once backfill released the hold.
    closer.on_update(bar(T0 + 360, tag="held2"))
    assert emitted == [b2, bar(T0 + 300, tag="held")]


# --- Reviewer probes ----------------------------------------------------------------------------


async def test_probe_a_the_first_forming_update_after_backfill_does_not_drop_the_tail() -> None:
    clock = FakeClock(T0 + 5)
    hist = History(clock, lag=3)
    emitted: list[RawBar] = []
    closer = make_closer(clock, hist.fetch, emitted, RecordingLogger())
    closer.mark_emitted(T0)
    clock.t = T0 + 241

    await closer.backfill(hist.fetch_range)  # 180 is not served yet: tail
    assert emitted == [h(60), h(120)]

    closer.on_update(s(240))  # the forming bar, first update after backfill
    await clock.advance(0)
    await clock.advance(1)
    await clock.advance(1)  # T0+243: 180 served
    await clock.advance(57.5)
    closer.on_update(s(300))  # closes 240 from the stream

    assert emitted == [h(60), h(120), h(180), s(240)]


async def test_probe_b_disconnect_does_not_cancel_a_closed_bar_waiting_for_history() -> None:
    clock = FakeClock(T0 + 5)
    hist = History(clock, lag=3)
    emitted: list[RawBar] = []
    closer = make_closer(clock, hist.fetch, emitted, RecordingLogger())

    clock.t = T0 + 70
    closer.on_update(s(60))
    await clock.advance(51)  # T0+121: timer queues 60; history lags
    assert hist.calls == [T0 + 60]

    clock.t += 0.5
    closer.on_disconnect()
    clock.t += 0.1
    closer.on_update(s(120))  # the baseline after reconnect is the forming bar

    await clock.advance(0.4)  # T0+122: retry, still lagging
    await clock.advance(1)  # T0+123: served
    await clock.advance(57.5)
    closer.on_update(s(180))

    assert emitted == [h(60), s(120)]


async def test_probe_c_a_range_result_outside_the_requested_range_is_not_emitted() -> None:
    clock = FakeClock(T0 + 10)
    emitted: list[RawBar] = []
    closer = make_closer(clock, FetchStub([]), emitted, RecordingLogger())
    clock.t = T0 + 130

    async def fetch_range(start: int, end: int) -> list[RawBar]:
        return [bar(T0 - 60, tag="before-subscription"), h(0), h(60), bar(T0 + 120, tag="forming")]

    await closer.backfill(fetch_range)

    assert emitted == [h(0), h(60)]


async def test_probe_e_a_held_forming_update_does_not_drop_the_tail() -> None:
    clock = FakeClock(T0 + 5)
    hist = History(clock, lag=3)
    hist.range_gate = asyncio.Event()
    emitted: list[RawBar] = []
    closer = make_closer(clock, hist.fetch, emitted, RecordingLogger())
    closer.mark_emitted(T0)
    clock.t = T0 + 241

    task = asyncio.create_task(closer.backfill(hist.fetch_range))
    await asyncio.sleep(0)
    closer.on_update(s(240))  # kept while holding: the forming bar T0+240
    hist.range_gate.set()
    await task

    await clock.advance(0)
    await clock.advance(1)
    await clock.advance(1)  # T0+243: 180 served
    await clock.advance(57.5)
    closer.on_update(s(300))
    await clock.advance(60)
    closer.on_update(s(360))

    assert emitted == [h(60), h(120), h(180), s(240), s(300)]


async def test_probe_f_a_forming_update_right_after_backfill_keeps_the_tail_in_order() -> None:
    clock = FakeClock(T0 + 5)
    hist = History(clock, lag=3)
    hist.range_gate = asyncio.Event()
    emitted: list[RawBar] = []
    closer = make_closer(clock, hist.fetch, emitted, RecordingLogger())
    closer.mark_emitted(T0)
    clock.t = T0 + 241

    task = asyncio.create_task(closer.backfill(hist.fetch_range))
    await asyncio.sleep(0)
    closer.on_update(s(180))  # kept while holding: the closing state of the tail
    hist.range_gate.set()
    await task
    closer.on_update(s(240))  # the first forming-bar update, right after backfill

    await clock.advance(0)
    await clock.advance(1)
    await clock.advance(1)  # T0+243: 180 served by history
    await clock.advance(57.5)
    closer.on_update(s(300))

    assert emitted == [h(60), h(120), h(180), s(240)]


async def test_probe_g_nothing_from_before_the_subscription_is_emitted() -> None:
    clock = FakeClock(T0 + 10)
    hist = History(clock, lag=0)
    emitted: list[RawBar] = []
    closer = make_closer(clock, hist.fetch, emitted, RecordingLogger())

    closer.on_update(s(-60))  # the venue's last bar on subscribe, already closed
    closer.on_disconnect()
    clock.t = T0 + 40
    await closer.backfill(hist.fetch_range)
    await clock.advance(5)
    await clock.advance(1)

    assert emitted == []
    assert hist.calls == []
    assert hist.range_calls == []

    closer.on_update(s(0))  # the stream resumes normally
    clock.t = T0 + 60.5
    closer.on_update(s(60))
    assert emitted == [s(0)]


async def test_probe_h_hitting_the_round_cap_skips_no_bar() -> None:
    clock = FakeClock(T0 + 5)
    hist = History(clock, lag=3, range_cost_secs=130)
    emitted: list[RawBar] = []
    logger = RecordingLogger()
    closer = make_closer(clock, hist.fetch, emitted, logger)
    closer.mark_emitted(T0)
    clock.t = T0 + 200

    await closer.backfill(hist.fetch_range)
    await clock.advance(0)
    await clock.advance(1)
    await clock.advance(1)

    assert hist.range_calls[-1] == (T0 + 720, T0 + 780)  # the one final fetch after the cap
    assert len(hist.range_calls) == 6
    assert offsets(emitted) == list(range(60, 781, 60))
    assert len(warnings_of(logger)) == 1
