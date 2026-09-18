"""Tests for the bar-close state machine.

`FakeClock` stands in for `Clock`: `advance` fires any due timers, then yields a few event-loop
turns so the tasks those timers schedule (history fetches) get to run.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

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


def bar(boundary: int, tag: int = 0) -> RawBar:
    # `delta_close` carries a tag so tests can tell bars apart by identity, not just boundary.
    return RawBar(
        boundary_secs=boundary, low=0, delta_open=0, delta_high=0, delta_close=tag, volume=0
    )


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


async def test_empty_history_retries_then_falls_back_to_the_streamed_state() -> None:
    clock = FakeClock(T0)
    fetch = FetchStub([])  # always None
    emitted: list[RawBar] = []
    logger = RecordingLogger()
    closer = make_closer(clock, fetch, emitted, logger)
    stream_bar = bar(T0, tag="stream")

    closer.on_update(stream_bar)
    await clock.advance(61)  # first attempt
    await clock.advance(1)  # first retry, one grace later
    await clock.advance(1)  # second retry, exhausted -> fallback

    assert fetch.calls == [T0, T0, T0]
    assert emitted == [stream_bar]
    assert [line for level, line in logger.lines if level == "warning"] == [
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
    warnings = [line for level, line in logger.lines if level == "warning"]
    debugs = [line for level, line in logger.lines if level == "debug"]
    assert len(warnings) == 1
    assert debugs == ["L: closed bar from streamed state"]


async def test_a_fetch_that_raises_falls_back_immediately_without_retries() -> None:
    clock = FakeClock(T0)
    fetch = FetchStub([RuntimeError("boom")])
    emitted: list[RawBar] = []
    logger = RecordingLogger()
    stream_bar = bar(T0, tag="stream")
    closer = make_closer(clock, fetch, emitted, logger)

    closer.on_update(stream_bar)
    await clock.advance(61)

    assert fetch.calls == [T0]
    assert emitted == [stream_bar]
    warnings = [line for level, line in logger.lines if level == "warning"]
    assert warnings == [
        "L: history request for closing bar failed (RuntimeError('boom'))",
        "L: history had no closed bar; emitting the streamed state",
    ]


def test_baseline_discards_a_first_update_already_past_its_end() -> None:
    clock = FakeClock(T0 + 120)
    fetch = FetchStub([])
    emitted: list[RawBar] = []
    closer = make_closer(clock, fetch, emitted, RecordingLogger())

    closer.on_update(bar(T0, tag="stale"))
    assert emitted == []

    # A later, still-forming bar behaves normally.
    closer.on_update(bar(T0 + 120, tag="live"))
    closer.on_update(bar(T0 + 180, tag="live2"))
    assert emitted == [bar(T0 + 120, tag="live")]


async def test_a_history_fetch_in_flight_does_not_double_emit_a_stream_closed_bar() -> None:
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
    await clock.advance(51)  # timer fires, task starts, suspends on the gate

    assert calls == [T0]
    assert emitted == []

    closer.on_update(bar(T0 + 60, tag="next"))  # closes T0 from the stream instead
    assert emitted == [bar(T0, tag="stream")]

    gate.set()
    for _ in range(5):
        await asyncio.sleep(0)

    assert emitted == [bar(T0, tag="stream")]  # the in-flight history result was not appended


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


async def test_on_disconnect_discards_the_partial_bar_and_resets_to_baseline() -> None:
    clock = FakeClock(T0)
    fetch = FetchStub([])
    emitted: list[RawBar] = []
    closer = make_closer(clock, fetch, emitted, RecordingLogger())

    closer.on_update(bar(T0, tag="partial"))
    closer.on_disconnect()

    await clock.advance(70)  # past T0's end; the pending timer was cancelled anyway
    closer.on_update(bar(T0, tag="stale-again"))  # baseline: already past its end, discarded
    assert emitted == []

    closer.on_update(bar(T0 + 120, tag="fresh"))
    closer.on_update(bar(T0 + 180, tag="fresh2"))
    assert emitted == [bar(T0 + 120, tag="fresh")]


async def test_backfill_emits_closed_bars_in_order_and_holds_stream_updates() -> None:
    clock = FakeClock(T0 + 200)
    fetch = FetchStub([])
    emitted: list[RawBar] = []
    closer = make_closer(clock, fetch, emitted, RecordingLogger())
    closer.mark_emitted(T0)

    gate = asyncio.Event()
    calls: list[tuple[int, int]] = []
    b1 = bar(T0 + 60, tag="h1")
    b2 = bar(T0 + 140, tag="h2")

    async def fetch_range(start: int, end: int) -> list[RawBar]:
        calls.append((start, end))
        await gate.wait()
        return [b1, b2]

    fetch_range_typed: Callable[[int, int], Awaitable[list[RawBar]]] = fetch_range
    task = asyncio.create_task(closer.backfill(fetch_range_typed))
    await asyncio.sleep(0)  # let backfill start and suspend on the gate

    assert calls == [(T0 + 60, T0 + 140)]

    closer.on_update(bar(T0 + 60, tag="dropped"))  # held while backfill is in flight
    assert emitted == []

    gate.set()
    await task

    assert emitted == [b1, b2]
    assert closer.last_emitted == T0 + 140
