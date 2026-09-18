"""Tests for the bar-close state machine.

`FakeClock` stands in for `Clock`: `advance` fires any due timers, then yields a few event-loop
turns so the tasks those timers schedule (history fetches) get to run.
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


def bar(boundary: int, tag: int | str = 0) -> RawBar:
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
        "L: history request failed (RuntimeError('boom')); emitting the streamed state",
    ]


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
    assert not clock.timers[0][2]  # armed before disconnect

    closer.on_disconnect()
    assert clock.timers[0][2]  # the pending close timer was cancelled

    await clock.advance(70)  # past T0's end; nothing to fire anyway
    closer.on_update(bar(T0, tag="stale-again"))  # baseline: already past its end, discarded
    assert emitted == []

    closer.on_update(bar(T0 + 120, tag="fresh"))
    closer.on_update(bar(T0 + 180, tag="fresh2"))
    assert emitted == [bar(T0 + 120, tag="fresh")]


async def test_backfill_emits_closed_bars_in_order_holds_updates_and_resets_baseline() -> None:
    clock = FakeClock(T0 + 180)  # an aligned boundary
    fetch = FetchStub([])
    emitted: list[RawBar] = []
    closer = make_closer(clock, fetch, emitted, RecordingLogger())
    closer.mark_emitted(T0)

    gate = asyncio.Event()
    calls: list[tuple[int, int]] = []
    b1 = bar(T0 + 60, tag="h1")
    b2 = bar(T0 + 120, tag="h2")
    still_forming = bar(T0 + 300, tag="still-forming")  # defensive: must be filtered out

    async def fetch_range(start: int, end: int) -> list[RawBar]:
        calls.append((start, end))
        await gate.wait()
        return [b1, b2, still_forming]

    task = asyncio.create_task(closer.backfill(fetch_range))
    await asyncio.sleep(0)  # backfill starts and suspends on the gate

    # end = floor(now/period)*period - period = T0+180-60 = T0+120 (now already aligned)
    assert calls == [(T0 + 60, T0 + 120)]

    closer.on_update(bar(T0 + 60, tag="dropped"))  # held while backfill is in flight
    assert emitted == []

    gate.set()
    await task

    assert emitted == [b1, b2]  # still_forming filtered out; each closed bar exactly once
    assert closer.last_emitted == T0 + 120

    clock.t = T0 + 300  # time passes before the post-backfill baseline update arrives
    closer.on_update(bar(T0 + 180, tag="stale"))  # past its end -> discarded, not forming
    assert emitted == [b1, b2]
    closer.on_update(bar(T0 + 300, tag="live"))
    closer.on_update(bar(T0 + 360, tag="live2"))
    assert emitted == [b1, b2, bar(T0 + 300, tag="live")]


async def test_backfill_that_spans_a_boundary_replays_and_dedupes_the_held_update() -> None:
    """Reviewer's probe: `end` must be recomputed if it advances while the fetch is in flight."""
    clock = FakeClock(T0 + 200)
    fetch = FetchStub([])
    emitted: list[RawBar] = []
    closer = make_closer(clock, fetch, emitted, RecordingLogger())
    closer.mark_emitted(T0)

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


async def test_on_timer_is_a_no_op_while_backfill_is_holding() -> None:
    clock = FakeClock(T0 + 10)
    fetch = FetchStub([bar(T0, tag="should-not-be-fetched")])
    emitted: list[RawBar] = []
    closer = make_closer(clock, fetch, emitted, RecordingLogger())
    closer.on_update(bar(T0, tag="stream"))  # arms a timer for T0

    clock.t = T0 + 130  # time passes; backfill now has a real gap to fetch

    gate = asyncio.Event()

    async def fetch_range(start: int, end: int) -> list[RawBar]:
        await gate.wait()
        return []

    task = asyncio.create_task(closer.backfill(fetch_range))
    await asyncio.sleep(0)  # backfill started, cleared current, and is suspended on the gate

    # Simulate the timer racing in regardless, as a belt-and-suspenders guard.
    closer._current = bar(T0, tag="race")
    closer._on_timer(T0, 0)
    await asyncio.sleep(0)

    assert emitted == []
    assert fetch.calls == []  # the old timer's fetch was never invoked

    gate.set()
    await task
    assert emitted == []
    assert fetch.calls == []


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
    debugs = [line for level, line in logger.lines if level == "debug"]
    assert len(debugs) == 3  # one mismatch note per attempt
    warnings = [line for level, line in logger.lines if level == "warning"]
    assert len(warnings) == 1  # the eventual fallback, not the mismatches


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
    debugs = [line for level, line in logger.lines if level == "debug"]
    assert len(debugs) == 3
    warnings = [line for level, line in logger.lines if level == "warning"]
    assert len(warnings) == 1


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


async def test_backfill_skips_the_fetch_and_still_resets_baseline_when_nothing_has_closed() -> None:
    clock = FakeClock(T0 + 200)
    fetch = FetchStub([])
    emitted: list[RawBar] = []
    closer = make_closer(clock, fetch, emitted, RecordingLogger())
    closer.mark_emitted(T0 + 80)

    calls: list[tuple[int, int]] = []

    async def fetch_range(start: int, end: int) -> list[RawBar]:
        calls.append((start, end))
        return []

    await closer.backfill(fetch_range)

    # end = T0+120; start = last_emitted + period = T0+140 > end -> nothing to fetch.
    assert calls == []
    assert emitted == []

    closer.on_update(bar(T0 + 120, tag="stale"))  # past its end -> discarded, not forming
    assert emitted == []
    closer.on_update(bar(T0 + 200, tag="live"))
    closer.on_update(bar(T0 + 260, tag="live2"))
    assert emitted == [bar(T0 + 200, tag="live")]


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
    warnings = [line for level, line in logger.lines if level == "warning"]
    debugs = [line for level, line in logger.lines if level == "debug"]
    assert len(warnings) == 1  # the warn-once flag was not spent by the silent fallback
    assert debugs == []


async def test_close_stops_updates_timers_and_backfill() -> None:
    clock = FakeClock(T0)
    fetch = FetchStub([bar(T0, tag="history")])
    emitted: list[RawBar] = []
    closer = make_closer(clock, fetch, emitted, RecordingLogger())

    closer.on_update(bar(T0, tag="stream"))
    closer.close()
    assert closer.last_emitted is None

    await clock.advance(61)  # the pending close timer must not fire into a fetch task
    assert emitted == []
    assert fetch.calls == []

    closer.on_update(bar(T0 + 60, tag="ignored"))
    assert emitted == []

    async def fetch_range(start: int, end: int) -> list[RawBar]:
        raise AssertionError("fetch_range must not be called after close")

    await closer.backfill(fetch_range)
    assert emitted == []


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
    await clock.advance(51)  # timer fires, task runs, fetch returns, emit raises

    errors = [line for level, line in logger.lines if level == "error"]
    assert len(errors) == 1
    assert closer.last_emitted == T0  # state stayed consistent despite the callback failing
