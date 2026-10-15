"""Tests for the request scheduler, on a virtual clock.

Every scenario runs under `VirtualClock.run`: time moves only when nothing on the event loop
can run, and then jumps to the earliest sleeper. A scenario of minutes takes milliseconds, and
grant times are exact.
"""

from __future__ import annotations

import asyncio
import heapq
import itertools
import random
from collections.abc import Awaitable, Callable, Coroutine
from typing import Any

import pytest

from nautilus_ctrader.common.errors import (
    CTraderConnectionError,
    CTraderRequestError,
    CTraderTimeoutError,
)
from nautilus_ctrader.common.rate_limit import (
    Pacing,
    RequestPriority,
    RequestScheduler,
    group_of,
)
from nautilus_ctrader.constants import GROUP_HISTORICAL, GROUP_OTHER
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as oa_model
from tests.recording_logger import RecordingLogger
from tests.venue_limiter import FakeLimiter

EXECUTION = RequestPriority.EXECUTION
CHECKPOINT = RequestPriority.CHECKPOINT
DATA = RequestPriority.DATA
BACKFILL = RequestPriority.BACKFILL

# Far beyond any scenario here: reaching it means something sleeps in a loop.
_TIME_LIMIT_SECS = 100_000.0


class VirtualClock:
    """A clock and `sleep` for the scheduler; time jumps only when the loop is idle."""

    def __init__(self) -> None:
        self.now = 0.0
        self._sleepers: list[tuple[float, int, asyncio.Future[None]]] = []
        self._order = itertools.count()

    def time(self) -> float:
        return self.now

    async def sleep(self, secs: float) -> None:
        wake = asyncio.get_running_loop().create_future()
        heapq.heappush(self._sleepers, (self.now + max(0.0, secs), next(self._order), wake))
        try:
            await wake
        finally:
            wake.cancel()

    async def at(self, when: float) -> None:
        await self.sleep(when - self.now)

    async def run[T](self, scenario: Coroutine[Any, Any, T]) -> T:
        """Run `scenario`, then let every task it left behind finish."""
        loop = asyncio.get_running_loop()
        main = asyncio.ensure_future(scenario)
        while True:
            await asyncio.sleep(0)
            # `_ready` holds the callbacks due on this loop iteration; the public API has no
            # way to ask whether anything else can still run.
            if loop._ready:  # type: ignore[attr-defined]
                continue
            while self._sleepers and self._sleepers[0][2].done():
                heapq.heappop(self._sleepers)
            if not self._sleepers:
                break
            self.now = self._sleepers[0][0]
            assert self.now < _TIME_LIMIT_SECS, "the scenario never settles"
            while self._sleepers and self._sleepers[0][0] <= self.now:
                _, _, wake = heapq.heappop(self._sleepers)
                if not wake.done():
                    wake.set_result(None)
        assert main.done(), "the scenario waits on something that never happens"
        return main.result()


def scheduler(
    clock: VirtualClock,
    pacing: Pacing | None = None,
    logger: RecordingLogger | None = None,
) -> RequestScheduler:
    return RequestScheduler(
        pacing or Pacing(),
        logger=logger or RecordingLogger(),
        clock=clock.time,
        sleep=clock.sleep,
    )


class Sends:
    """Makes `send` callables that record which request was sent, and when."""

    def __init__(self, clock: VirtualClock) -> None:
        self.clock = clock
        self.log: list[tuple[str, float]] = []

    def ok(self, name: str, *, takes: float = 0.0) -> Callable[[], Awaitable[str]]:
        async def send() -> str:
            self.log.append((name, self.clock.now))
            if takes:
                await self.clock.sleep(takes)
            return name

        return send

    def failing(
        self,
        name: str,
        errors: list[Exception],
        *,
        takes: float = 0.0,
    ) -> Callable[[], Awaitable[str]]:
        """Raises the next of `errors` on each call; answers once they run out."""

        async def send() -> str:
            self.log.append((name, self.clock.now))
            if takes:
                await self.clock.sleep(takes)
            if errors:
                raise errors.pop(0)
            return name

        return send

    def names(self) -> list[str]:
        return [name for name, _ in self.log]

    def times(self, name: str) -> list[float]:
        return [t for n, t in self.log if n == name]


def refusal(retry_after: int | None = 1) -> CTraderRequestError:
    return CTraderRequestError("BLOCKED_PAYLOAD_TYPE", "refused", retry_after_secs=retry_after)


def spawn(s: RequestScheduler, group: str, priority: RequestPriority, send) -> asyncio.Task:
    return asyncio.ensure_future(s.submit(group, priority, send))


async def fill_window(s: RequestScheduler, sends: Sends, group: str, count: int) -> None:
    await asyncio.gather(
        *(s.submit(group, DATA, sends.ok(f"fill{i}")) for i in range(count)),
    )


def most_within(times: list[float], span: float) -> int:
    """The most of `times` that fall in any half-open span `[t, t + span)`."""
    ordered = sorted(times)
    most = 0
    start = 0
    for end, t in enumerate(ordered):
        # The scheduler's own rounding: `t - start >= span` can differ from it by one ulp.
        while ordered[start] + span <= t:
            start += 1
        most = max(most, end - start + 1)
    return most


# -- The window -----------------------------------------------------------------------------


@pytest.mark.parametrize("seed", [1, 2, 3])
async def test_the_window_is_never_exceeded(seed) -> None:
    rng = random.Random(seed)
    clock = VirtualClock()
    s = scheduler(clock)
    granted: dict[str, list[float]] = {GROUP_HISTORICAL: [], GROUP_OTHER: []}
    # Three bursts of about a hundred, so both groups' windows fill up.
    bursts = [0.0, 40.0, 80.0]

    async def one(group: str, priority: RequestPriority, arrival: float, takes: float) -> None:
        await clock.at(arrival)

        async def send() -> None:
            granted[group].append(clock.now)
            await clock.sleep(takes)

        await s.submit(group, priority, send)

    async def scenario() -> None:
        await asyncio.gather(
            *(
                one(
                    GROUP_OTHER if rng.random() < 0.6 else GROUP_HISTORICAL,
                    rng.choice(list(RequestPriority)),
                    rng.choice(bursts) + rng.uniform(0.0, 1.0),
                    rng.uniform(0.0, 0.2),
                )
                for _ in range(300)
            ),
        )

    await clock.run(scenario())

    assert most_within(granted[GROUP_HISTORICAL], 1.25) == 5
    assert most_within(granted[GROUP_OTHER], 1.25) == 50
    assert len(granted[GROUP_HISTORICAL]) + len(granted[GROUP_OTHER]) == 300


async def test_a_full_window_of_five_goes_at_once_and_the_sixth_waits_the_window() -> None:
    clock = VirtualClock()
    s = scheduler(clock)
    sends = Sends(clock)

    async def scenario() -> list[str]:
        return await asyncio.gather(
            *(s.submit(GROUP_HISTORICAL, DATA, sends.ok(str(i))) for i in range(6)),
        )

    results = await clock.run(scenario())

    assert results == ["0", "1", "2", "3", "4", "5"]
    assert [t for _, t in sends.log] == [0.0] * 5 + [pytest.approx(1.25)]


# -- Priorities -----------------------------------------------------------------------------


async def test_higher_priority_goes_first_and_fifo_within_a_level() -> None:
    clock = VirtualClock()
    s = scheduler(clock)
    sends = Sends(clock)

    async def scenario() -> None:
        await fill_window(s, sends, GROUP_HISTORICAL, 5)
        await asyncio.gather(
            spawn(s, GROUP_HISTORICAL, DATA, sends.ok("1")),
            spawn(s, GROUP_HISTORICAL, BACKFILL, sends.ok("2")),
            spawn(s, GROUP_HISTORICAL, EXECUTION, sends.ok("3")),
            spawn(s, GROUP_HISTORICAL, DATA, sends.ok("4")),
        )

    await clock.run(scenario())

    assert sends.names()[5:] == ["3", "1", "4", "2"]


async def test_a_lower_priority_never_overtakes_a_waiting_higher() -> None:
    clock = VirtualClock()
    s = scheduler(clock)
    sends = Sends(clock)

    async def scenario() -> None:
        # One window slot frees every 0.2 s from 1.25 on.
        for i in range(5):
            await clock.at(0.2 * i)
            await s.submit(GROUP_HISTORICAL, DATA, sends.ok(f"fill{i}"))
        await clock.at(0.9)
        execution = spawn(s, GROUP_HISTORICAL, EXECUTION, sends.ok("execution"))
        await clock.at(0.95)
        backfill = spawn(s, GROUP_HISTORICAL, BACKFILL, sends.ok("backfill"))
        await asyncio.gather(execution, backfill)

    await clock.run(scenario())

    assert sends.times("execution") == [pytest.approx(1.25)]
    assert sends.times("backfill") == [pytest.approx(1.45)]


async def test_groups_are_paced_independently() -> None:
    clock = VirtualClock()
    s = scheduler(clock)
    sends = Sends(clock)

    async def scenario() -> None:
        await fill_window(s, sends, GROUP_HISTORICAL, 5)
        waiting = spawn(s, GROUP_HISTORICAL, EXECUTION, sends.ok("historical"))
        await clock.at(0.1)
        await s.submit(GROUP_OTHER, BACKFILL, sends.ok("other"))
        await waiting

    await clock.run(scenario())

    assert sends.times("other") == [pytest.approx(0.1)]
    assert sends.times("historical") == [pytest.approx(1.25)]


# -- Refusals -------------------------------------------------------------------------------


async def test_a_historical_refusal_pauses_the_group_and_retries_in_place() -> None:
    clock = VirtualClock()
    logger = RecordingLogger()
    s = scheduler(clock, logger=logger)
    sends = Sends(clock)

    async def scenario() -> list[str]:
        await fill_window(s, sends, GROUP_HISTORICAL, 4)
        refused = spawn(
            s, GROUP_HISTORICAL, DATA, sends.failing("refused", [refusal(1)], takes=0.1)
        )
        behind = spawn(s, GROUP_HISTORICAL, DATA, sends.ok("behind"))
        return await asyncio.gather(refused, behind)

    results = await clock.run(scenario())

    assert results == ["refused", "behind"]
    # Refused on arrival at 0.1: paused until 0.1 + 1 + 0.25.
    assert sends.times("refused") == [0.0, pytest.approx(1.35)]
    assert sends.names()[-2:] == ["refused", "behind"]
    (warning,) = logger.warnings()
    assert GROUP_HISTORICAL in warning
    assert "1.25 s" in warning
    assert "retry 1 of 3" in warning


async def test_a_historical_request_fails_after_three_retries() -> None:
    clock = VirtualClock()
    logger = RecordingLogger()
    s = scheduler(clock, logger=logger)
    sends = Sends(clock)
    errors = [refusal(1) for _ in range(10)]

    async def scenario() -> None:
        await s.submit(GROUP_HISTORICAL, DATA, sends.failing("refused", errors))

    with pytest.raises(CTraderRequestError) as raised:
        await clock.run(scenario())

    assert raised.value.retry_after_secs == 1
    assert sends.times("refused") == [
        0.0,
        pytest.approx(1.25),
        pytest.approx(2.5),
        pytest.approx(3.75),
    ]
    assert len(logger.warnings()) == 4


async def test_an_other_group_refusal_fails_at_once_and_the_rest_wait_the_pause() -> None:
    clock = VirtualClock()
    logger = RecordingLogger()
    s = scheduler(clock, logger=logger)
    sends = Sends(clock)

    async def scenario() -> None:
        refused = spawn(s, GROUP_OTHER, EXECUTION, sends.failing("refused", [refusal(1)]))
        with pytest.raises(CTraderRequestError):
            await refused
        await clock.at(0.5)
        await s.submit(GROUP_OTHER, EXECUTION, sends.ok("next"))

    await clock.run(scenario())

    assert sends.times("refused") == [0.0]
    assert sends.times("next") == [pytest.approx(1.25)]
    (warning,) = logger.warnings()
    assert GROUP_OTHER in warning


@pytest.mark.parametrize(
    "error",
    [refusal(None), CTraderTimeoutError("no answer")],
    ids=["refusal without retryAfter", "timeout"],
)
async def test_an_error_without_retry_after_neither_pauses_nor_retries(error) -> None:
    clock = VirtualClock()
    logger = RecordingLogger()
    s = scheduler(clock, logger=logger)
    sends = Sends(clock)

    async def scenario() -> None:
        with pytest.raises(type(error)) as raised:
            await s.submit(GROUP_HISTORICAL, DATA, sends.failing("failed", [error]))
        assert raised.value is error
        await s.submit(GROUP_HISTORICAL, DATA, sends.ok("next"))

    await clock.run(scenario())

    assert sends.log == [("failed", 0.0), ("next", 0.0)]
    assert logger.warnings() == []


async def test_a_pause_set_during_a_window_wait_is_honoured() -> None:
    clock = VirtualClock()
    s = scheduler(clock, Pacing(retries=0))
    sends = Sends(clock)

    async def scenario() -> None:
        await fill_window(s, sends, GROUP_HISTORICAL, 4)
        # The fifth fills the window and is refused at 0.5, while the sixth waits for room.
        refused = spawn(
            s, GROUP_HISTORICAL, DATA, sends.failing("refused", [refusal(1)], takes=0.5)
        )
        waiting = spawn(s, GROUP_HISTORICAL, DATA, sends.ok("waiting"))
        with pytest.raises(CTraderRequestError):
            await refused
        await waiting

    await clock.run(scenario())

    # Window room at 1.25, but the pause runs until 0.5 + 1 + 0.25.
    assert sends.times("waiting") == [pytest.approx(1.75)]


# -- The ceiling ----------------------------------------------------------------------------


async def test_the_ceiling_drops_a_request_unsent() -> None:
    clock = VirtualClock()
    s = scheduler(clock, Pacing(queue_timeout_secs=60.0))
    sends = Sends(clock)
    failed_at: list[float] = []

    async def scenario() -> None:
        with pytest.raises(CTraderRequestError):
            await s.submit(GROUP_OTHER, EXECUTION, sends.failing("refused", [refusal(1000)]))
        with pytest.raises(CTraderTimeoutError, match="waited 60 s in the request queue"):
            await s.submit(GROUP_OTHER, EXECUTION, sends.ok("dropped"))
        failed_at.append(clock.now)

    await clock.run(scenario())

    assert failed_at == [pytest.approx(60.0)]
    assert sends.times("dropped") == []


async def test_the_ceiling_counts_retry_waits() -> None:
    clock = VirtualClock()
    s = scheduler(clock, Pacing(queue_timeout_secs=60.0))
    sends = Sends(clock)
    errors = [refusal(30) for _ in range(3)]
    failed_at: list[float] = []

    async def scenario() -> None:
        with pytest.raises(CTraderTimeoutError):
            await s.submit(GROUP_HISTORICAL, DATA, sends.failing("refused", errors))
        failed_at.append(clock.now)

    await clock.run(scenario())

    # Each pause is 30.25 s: the second retry would leave at 60.5.
    assert sends.times("refused") == [0.0, pytest.approx(30.25)]
    assert failed_at == [pytest.approx(60.0)]


# -- Cancellation, connection loss, reset ---------------------------------------------------


async def test_a_cancelled_waiter_is_never_granted_and_blocks_nobody() -> None:
    clock = VirtualClock()
    s = scheduler(clock, Pacing(historical_requests_per_window=1))
    sends = Sends(clock)

    async def scenario() -> None:
        await fill_window(s, sends, GROUP_HISTORICAL, 1)
        cancelled = spawn(s, GROUP_HISTORICAL, EXECUTION, sends.ok("cancelled"))
        behind = spawn(s, GROUP_HISTORICAL, EXECUTION, sends.ok("behind"))
        await clock.at(0.5)
        cancelled.cancel()
        with pytest.raises(asyncio.CancelledError):
            await cancelled
        await behind

    await clock.run(scenario())

    assert sends.times("cancelled") == []
    # The slot the cancelled one would have taken.
    assert sends.times("behind") == [pytest.approx(1.25)]


async def test_fail_all_fails_every_waiter() -> None:
    clock = VirtualClock()
    s = scheduler(clock)
    sends = Sends(clock)
    lost = CTraderConnectionError("lost")

    async def scenario() -> list[BaseException]:
        await fill_window(s, sends, GROUP_HISTORICAL, 5)
        with pytest.raises(CTraderRequestError):
            await s.submit(GROUP_OTHER, EXECUTION, sends.failing("refused", [refusal(5)]))
        waiters = [
            spawn(s, GROUP_HISTORICAL, EXECUTION, sends.ok("historical")),
            spawn(s, GROUP_HISTORICAL, BACKFILL, sends.ok("historical")),
            spawn(s, GROUP_OTHER, DATA, sends.ok("other")),
        ]
        await clock.at(0.5)
        s.fail_all(lost)
        return await asyncio.gather(*waiters, return_exceptions=True)

    results = await clock.run(scenario())

    assert all(result is lost for result in results)
    assert sends.times("historical") == sends.times("other") == []


async def test_after_fail_all_a_submit_fails_at_once_until_reset() -> None:
    clock = VirtualClock()
    s = scheduler(clock)
    sends = Sends(clock)
    lost = CTraderConnectionError("lost")
    failed_at: list[float] = []

    async def scenario() -> None:
        await clock.at(1.0)
        s.fail_all(lost)
        for group in (GROUP_HISTORICAL, GROUP_OTHER):
            with pytest.raises(CTraderConnectionError) as raised:
                await s.submit(group, EXECUTION, sends.ok("refused"))
            assert raised.value is lost
        failed_at.append(clock.now)
        s.reset()
        await s.submit(GROUP_OTHER, EXECUTION, sends.ok("after reset"))

    await clock.run(scenario())

    assert failed_at == [1.0]
    assert sends.names() == ["after reset"]


async def test_a_refusal_after_fail_all_is_not_requeued() -> None:
    # The request was already inside `send()` when the connection was lost.
    clock = VirtualClock()
    s = scheduler(clock)
    sends = Sends(clock)
    lost = CTraderConnectionError("lost")

    async def scenario() -> None:
        refused = spawn(
            s, GROUP_HISTORICAL, DATA, sends.failing("refused", [refusal(1)], takes=0.5)
        )
        await clock.at(0.1)
        s.fail_all(lost)
        with pytest.raises(CTraderConnectionError) as raised:
            await refused
        assert raised.value is lost

    await clock.run(scenario())

    assert sends.times("refused") == [0.0]


async def test_a_historical_pause_does_not_delay_the_other_group() -> None:
    clock = VirtualClock()
    s = scheduler(clock, Pacing(retries=0))
    sends = Sends(clock)

    async def scenario() -> None:
        with pytest.raises(CTraderRequestError):
            await s.submit(GROUP_HISTORICAL, DATA, sends.failing("refused", [refusal(1)]))
        await clock.at(0.1)
        await s.submit(GROUP_OTHER, BACKFILL, sends.ok("other"))

    await clock.run(scenario())

    assert sends.times("other") == [pytest.approx(0.1)]


async def test_reset_forgets_the_window_and_the_pause() -> None:
    clock = VirtualClock()
    s = scheduler(clock)
    sends = Sends(clock)

    async def scenario() -> None:
        await fill_window(s, sends, GROUP_HISTORICAL, 5)
        with pytest.raises(CTraderRequestError):
            await s.submit(GROUP_OTHER, EXECUTION, sends.failing("refused", [refusal(5)]))
        waiting = spawn(s, GROUP_HISTORICAL, DATA, sends.ok("waiting"))
        await clock.at(0.1)
        s.reset()
        await asyncio.gather(
            waiting,
            s.submit(GROUP_HISTORICAL, DATA, sends.ok("historical")),
            s.submit(GROUP_OTHER, DATA, sends.ok("other")),
        )

    await clock.run(scenario())

    assert sends.times("waiting") == [pytest.approx(0.1)]
    assert sends.times("historical") == [pytest.approx(0.1)]
    assert sends.times("other") == [pytest.approx(0.1)]


# -- Pacing ---------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "overrides",
    [
        {"requests_per_window": 0},
        {"historical_requests_per_window": 0},
        {"window_secs": 0.0},
        {"queue_timeout_secs": 0.0},
        {"retries": -1},
    ],
    ids=["count", "historical count", "window", "ceiling", "retries"],
)
def test_pacing_rejects_impossible_values(overrides) -> None:
    with pytest.raises(ValueError):
        Pacing(**overrides)


# -- Against the measured venue -------------------------------------------------------------


async def test_the_measured_venue_refuses_nothing_at_start_up() -> None:
    clock = VirtualClock()
    rng = random.Random(7)
    trendbars = oa_model.PROTO_OA_GET_TRENDBARS_REQ
    venue = FakeLimiter(clock.time, {trendbars: ("historical", 5)}, shape="sliding")
    s = scheduler(clock)
    sent: list[RequestPriority] = []
    refusals: list[int] = []

    def request(priority: RequestPriority) -> Callable[[], Awaitable[None]]:
        async def send() -> None:
            sent.append(priority)
            await clock.sleep(rng.uniform(0.0, 0.1))
            retry_after = venue.admit(trendbars)
            if retry_after is not None:
                refusals.append(retry_after)
                raise refusal(retry_after)

        return send

    async def scenario() -> None:
        startup = [EXECUTION] * 8 + [CHECKPOINT] * 3 + [DATA] * 12
        rng.shuffle(startup)
        await asyncio.gather(
            *(s.submit(GROUP_HISTORICAL, p, request(p)) for p in startup),
        )

    await clock.run(scenario())

    assert refusals == []
    assert sent == [EXECUTION] * 8 + [CHECKPOINT] * 3 + [DATA] * 12


# -- Groups ---------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("payload", "group"),
    [
        (oa.ProtoOAGetTrendbarsReq(), GROUP_HISTORICAL),
        (oa.ProtoOADealListReq(), GROUP_HISTORICAL),
        (oa.ProtoOACashFlowHistoryListReq(), GROUP_HISTORICAL),
        (oa.ProtoOAOrderDetailsReq(), GROUP_OTHER),
        (oa.ProtoOANewOrderReq(), GROUP_OTHER),
    ],
    ids=["trendbars", "deal list", "cash-flow list", "order details", "new order"],
)
def test_group_of_classifies_by_payload_type(payload, group) -> None:
    assert group_of(payload) == group
