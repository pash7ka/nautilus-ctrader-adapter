"""Tests for the read-only request-limit probe.

`scripts/verify_rate_limits.py` is developer tooling, imported by file path as the other script
tests do. The inference is checked on synthetic timelines; the whole run is checked against the
fake venue with a fake limiter that refuses with `BLOCKED_PAYLOAD_TYPE` by a configurable rule.

The end-to-end runs use a virtual clock: time only moves when the probe sleeps, and only once
the venue has read every request already sent, so each request reaches the fake limiter at the
virtual time the probe recorded for it. A run of minutes takes well under a second.
"""

from __future__ import annotations

import asyncio
import importlib.util
import itertools
import json
import math
import pathlib
import random
import shutil
import signal
import subprocess
import sys
from collections import defaultdict
from collections.abc import Callable

import pytest

from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as oa_model
from tests import account_venue
from tests.execution_replay import make_order
from tests.fake_server import FakeCTraderServer
from tests.secrecy import assert_secret_free

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
_SCRIPT_PATH = _REPO_ROOT / "scripts" / "verify_rate_limits.py"
_SPEC = importlib.util.spec_from_file_location("verify_rate_limits", _SCRIPT_PATH)
verify_rate_limits = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = verify_rate_limits
_SPEC.loader.exec_module(verify_rate_limits)

r = verify_rate_limits

TRENDBARS_TYPE = oa_model.PROTO_OA_GET_TRENDBARS_REQ
DEAL_LIST_TYPE = oa_model.PROTO_OA_DEAL_LIST_REQ
TICK_DATA_TYPE = oa_model.PROTO_OA_GET_TICKDATA_REQ
TRADER_TYPE = oa_model.PROTO_OA_TRADER_REQ

CLIENT_ID = "client-id-7f3a"
CLIENT_SECRET = "client-secret-91c2"
ACCESS_TOKEN = "access-token-5d8e"
VENUE_DESCRIPTION = "venue-description-e4b0"


# -- A fake limiter -------------------------------------------------------------------------


class FakeLimiter:
    """Refuses with `BLOCKED_PAYLOAD_TYPE` and `retryAfter`, by one of two window shapes.

    `groups` maps a payload type to the key it is counted under and that key's limit: payload
    types sharing a key share one limit. A refusal blocks its key for `retry_after` seconds.
    """

    def __init__(
        self,
        clock: Callable[[], float],
        groups: dict[int, tuple[str, int]],
        *,
        shape: str,
        retry_after: int = 1,
    ) -> None:
        self._clock = clock
        self._groups = groups
        self._shape = shape
        self._retry_after = retry_after
        self._admitted: dict[str, list[float]] = defaultdict(list)
        self._blocked_until: dict[str, float] = {}

    def admit(self, payload_type: int) -> int | None:
        """`None` if the request is answered, else the `retryAfter` it is refused with."""
        if payload_type not in self._groups:
            return None
        key, limit = self._groups[payload_type]
        now = self._clock()
        if now < self._blocked_until.get(key, -math.inf):
            return self._retry_after
        admitted = self._admitted[key]
        if self._shape == r.SLIDING:
            used = sum(1 for t in admitted if now - t < 1.0)
        else:
            used = sum(1 for t in admitted if math.floor(t) == math.floor(now))
        if used >= limit:
            self._blocked_until[key] = now + self._retry_after
            return self._retry_after
        admitted.append(now)
        return None


def limiter_groups(
    *,
    shared: bool,
    siblings: tuple[int, ...] = (DEAL_LIST_TYPE,),
) -> dict[int, tuple[str, int]]:
    """Trendbars and `siblings` limited at 5, trader requests at 50, the rest not at all."""
    groups = {TRENDBARS_TYPE: ("historical", 5), TRADER_TYPE: ("trader", 50)}
    for payload_type in siblings:
        groups[payload_type] = ("historical" if shared else str(payload_type), 5)
    return groups


class _Clock:
    def __init__(self, now: float = 0.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now


def simulate(shape: str, times: list[float], *, limit: int = 5, retry_after: int = 1) -> list:
    """The attempts a limiter of `shape` gives for trendbar requests sent at `times`."""
    clock = _Clock()
    limiter = FakeLimiter(
        clock,
        {TRENDBARS_TYPE: ("historical", limit)},
        shape=shape,
        retry_after=retry_after,
    )
    attempts = []
    for index, t in enumerate(times):
        clock.now = t
        refused = limiter.admit(TRENDBARS_TYPE)
        attempts.append(
            attempt(
                t, r.ANSWERED if refused is None else r.BLOCKED, index=index, retry_after=refused
            ),
        )
    return attempts


def stream_times(rate: float, *, start: float = 1000.5, secs: float = 3.0) -> list[float]:
    return [start + i / rate for i in range(round(rate * secs))]


def attempt(
    sent_at: float,
    outcome: str = "answered",
    *,
    role: str = "stream",
    index: int = 0,
    retry_after: int | None = None,
    answered_at: float | None = None,
    request: str = "trendbars",
):
    return r.Attempt(
        "step",
        request,
        role,
        index,
        sent_at,
        sent_at if answered_at is None else answered_at,
        outcome,
        retry_after,
    )


def burst(size: int, answered: int, *, other: tuple[str, ...] = ()) -> r.BurstSummary:
    attempts = [
        attempt(0.0, r.ANSWERED if i < answered else r.BLOCKED, role=r.ROLE_BURST, index=i)
        for i in range(size - len(other))
    ]
    attempts += [
        attempt(0.0, code, role=r.ROLE_BURST, index=size - len(other) + i)
        for i, code in enumerate(other)
    ]
    return r.summarize_burst(size, attempts)


# -- The read-only guard --------------------------------------------------------------------


class _StubConnection:
    def __init__(self) -> None:
        self.sent: list[object] = []

    async def request(self, payload, *, timeout_secs=None):
        self.sent.append(payload)
        return oa.ProtoOAVersionRes(version="1")


@pytest.mark.parametrize(
    "payload",
    [
        oa.ProtoOANewOrderReq(
            ctidTraderAccountId=1, symbolId=1, orderType=1, tradeSide=1, volume=1
        ),
        oa.ProtoOAClosePositionReq(ctidTraderAccountId=1, positionId=1, volume=1),
        oa.ProtoOACancelOrderReq(ctidTraderAccountId=1, orderId=1),
        oa.ProtoOASubscribeSpotsReq(ctidTraderAccountId=1, symbolId=[1]),
    ],
    ids=["new order", "close position", "cancel order", "spot subscription"],
)
async def test_the_guard_refuses_anything_outside_the_allow_list_before_it_is_sent(payload) -> None:
    connection = _StubConnection()
    guard = r.ReadOnlyGuard(10)

    with pytest.raises(r.ReadOnlyViolation):
        await r.GuardedConnection(connection, guard).request(payload)

    assert connection.sent == []
    assert guard.sent == 0


def test_the_allow_list_holds_only_authentication_reference_and_history_requests() -> None:
    assert {cls.__name__ for cls in r.ALLOWED_REQUESTS} == {
        "ProtoOAApplicationAuthReq",
        "ProtoOAGetAccountListByAccessTokenReq",
        "ProtoOAAccountAuthReq",
        "ProtoOAVersionReq",
        "ProtoOATraderReq",
        "ProtoOASymbolsListReq",
        "ProtoOASymbolByIdReq",
        "ProtoOAGetTrendbarsReq",
        "ProtoOAGetTickDataReq",
        "ProtoOADealListReq",
        "ProtoOAOrderListReq",
        "ProtoOACashFlowHistoryListReq",
        "ProtoOAOrderDetailsReq",
        "ProtoHeartbeatEvent",
    }
    assert set(r.PROBED_REQUESTS.values()) <= r.ALLOWED_REQUESTS
    # The symbols list only resolves `--symbol`; it is never probed.
    assert oa.ProtoOASymbolsListReq not in set(r.PROBED_REQUESTS.values())


async def test_the_guard_stops_at_its_cap_and_on_request() -> None:
    connection = _StubConnection()
    guard = r.ReadOnlyGuard(2)
    guarded = r.GuardedConnection(connection, guard)
    version = oa.ProtoOAVersionReq()

    await guarded.request(version)
    await guarded.request(version)
    with pytest.raises(r.RequestCapReached):
        await guarded.request(version)
    assert len(connection.sent) == 2

    guard = r.ReadOnlyGuard(10)
    guard.stop_requested = True
    with pytest.raises(r.StopRequested):
        await r.GuardedConnection(connection, guard).request(version)
    assert len(connection.sent) == 2


# -- The plan -------------------------------------------------------------------------------


def test_the_default_plan_fits_its_caps() -> None:
    settings = r.Settings()
    plan = r.build_plan(settings)

    r.validate_plan(plan, settings)
    assert max(step.planned for step in plan) <= settings.max_step_requests


@pytest.mark.parametrize(
    ("overrides", "message"),
    [({"max_step_requests": 60}, "per-step cap"), ({"max_requests": 300}, "cap of 300")],
)
def test_a_plan_over_a_cap_is_refused(overrides, message) -> None:
    settings = r.Settings(**overrides)

    with pytest.raises(ValueError, match=message):
        r.validate_plan(r.build_plan(settings), settings)


# -- Inference: the allowance at once -------------------------------------------------------


def test_the_allowance_is_what_every_refused_burst_answered() -> None:
    assert r.infer_allowance([burst(5, 5), burst(6, 5), burst(8, 5), burst(10, 5)]) == (5, "")


@pytest.mark.parametrize(
    ("bursts", "reason"),
    [
        ([], "no burst ran"),
        ([burst(5, 5), burst(10, 10)], "no burst was refused, up to 10"),
        ([burst(8, 5), burst(10, 6)], "different numbers"),
        ([burst(6, 4), burst(5, 5)], "answered in full"),
        ([burst(8, 5, other=("timeout",))], "something other than"),
    ],
    ids=["none", "never refused", "disagreeing", "contradicted", "timeout"],
)
def test_an_allowance_that_cannot_be_told_is_inconclusive(bursts, reason) -> None:
    allowance, why = r.infer_allowance(bursts)

    assert allowance is None
    assert reason in why


def test_a_burst_summary_gives_the_first_refused_position() -> None:
    summary = burst(8, 5)

    assert (summary.answered, summary.refused, summary.first_refused) == (5, 3, 6)
    assert r.describe_burst(summary) == (
        "burst of 8: 5 answered, 3 refused (BLOCKED_PAYLOAD_TYPE) from request 6"
    )


def test_request_frequency_exceeded_counts_as_a_refusal_and_keeps_its_code() -> None:
    attempts = [attempt(0.0, role=r.ROLE_BURST, index=i) for i in range(5)]
    attempts += [
        attempt(0.0, "REQUEST_FREQUENCY_EXCEEDED", role=r.ROLE_BURST, index=5),
        attempt(0.0, r.BLOCKED, role=r.ROLE_BURST, index=6),
    ]

    summary = r.summarize_burst(7, attempts)

    assert (summary.refused, summary.other) == (2, ())
    assert r.infer_allowance([summary]) == (5, "")
    assert "2 refused (BLOCKED_PAYLOAD_TYPE, REQUEST_FREQUENCY_EXCEEDED)" in r.describe_burst(
        summary
    )


# -- Inference: the window shape ------------------------------------------------------------


def planned_streams() -> list[r.Step]:
    return [s for s in r.build_plan(r.Settings()) if s.group == r.GROUP_HISTORICAL_STREAMS]


@pytest.mark.parametrize("shape", [r.SLIDING, r.FIXED])
@pytest.mark.parametrize(
    "offset", [0.0, 0.2, -0.3], ids=["in step", "venue later", "venue earlier"]
)
def test_the_planned_streams_tell_the_window_shape(shape, offset) -> None:
    # `offset` moves the streams against the venue's seconds, as a clock difference would.
    streams = [
        simulate(shape, stream_times(s.rate, start=1000 + 10 * i + s.phase + offset))
        for i, s in enumerate(planned_streams())
    ]

    found, detail = r.infer_window_shape(streams, 5)

    assert found == shape, detail


@pytest.mark.parametrize("shape", [r.SLIDING, r.FIXED])
@pytest.mark.parametrize("jitter", [0.0, 0.03])
@pytest.mark.parametrize("retry_after", [1, 2])
def test_the_window_shape_is_told_at_any_clock_offset(shape, jitter, retry_after) -> None:
    # Venue seconds offset from ours by 0 to 0.95 s, send times jittered by up to `jitter`.
    rng = random.Random(1)
    for offset in [k / 20 for k in range(20)]:
        streams = [
            simulate(
                shape,
                [
                    t + rng.uniform(-jitter, jitter)
                    for t in stream_times(s.rate, start=1000 + 10 * i + s.phase + offset)
                ],
                retry_after=retry_after,
            )
            for i, s in enumerate(planned_streams())
        ]

        found, detail = r.infer_window_shape(streams, 5)

        assert found == shape, (offset, detail)


def test_a_fixed_window_names_the_phase_its_seconds_start_at() -> None:
    streams = [
        simulate(r.FIXED, stream_times(s.rate, start=1000 + 10 * i + s.phase))
        for i, s in enumerate(planned_streams())
    ]

    _, detail = r.infer_window_shape(streams, 5)

    assert "fractions 0.00" in detail


def test_more_than_the_allowance_within_one_second_rules_out_a_sliding_window() -> None:
    answered = [attempt(1000.5 + i / 7) for i in range(7)]

    assert not r.fits_sliding(answered, 5)
    assert r.fits_sliding(answered[:5], 5)


@pytest.mark.parametrize(
    ("streams", "allowance", "reason"),
    [
        ([[attempt(1.0)]], None, "allowance at once is unknown"),
        ([[]], 5, "no stream ran"),
        ([[attempt(1.0, "timeout")]], 5, "something other than"),
        ([[attempt(1.0 + i / 4) for i in range(8)]], 5, "no stream was refused"),
        # Five answers and a refusal inside one second: both shapes explain it.
        (
            [[attempt(1000.0 + i / 6) for i in range(5)] + [attempt(1000.0 + 5 / 6, r.BLOCKED)]],
            5,
            "fit both",
        ),
        # A refusal with nothing before it and no retryAfter: neither shape explains it.
        ([[attempt(1000.5, r.BLOCKED), attempt(1000.7)]], 5, "fit neither"),
    ],
    ids=["no allowance", "empty", "timeout", "never refused", "both", "neither"],
)
def test_a_window_shape_that_cannot_be_told_is_inconclusive(streams, allowance, reason) -> None:
    found, detail = r.infer_window_shape(streams, allowance)

    assert found is None
    assert reason in detail


def test_most_within_counts_only_times_closer_than_the_span() -> None:
    assert r.most_within([0.0, 0.5, 0.99, 1.0, 1.5], 1.0) == 3
    assert r.most_within([], 1.0) == 0


# -- Inference: payload types, sharing, retryAfter ------------------------------------------


@pytest.mark.parametrize(
    ("summary", "limited", "text"),
    [
        (burst(8, 5), True, "limited at 8: 5 answered, 3 refused - like trendbars"),
        (burst(8, 6), True, "limited at 8: 6 answered, 2 refused"),
        (burst(8, 8), False, "NOT limited at 8"),
        (burst(8, 7, other=("timeout",)), None, "inconclusive"),
        (None, None, "not run"),
    ],
    ids=["like trendbars", "differently", "not limited", "timeout", "not run"],
)
def test_a_payload_type_is_classified_by_its_burst(summary, limited, text) -> None:
    found, detail = r.classify_type(summary, 5)

    assert found is limited
    assert text in detail


def _block_attempts(follow_outcome: str, *, follow_at: float = 0.05) -> list:
    refusal = attempt(0.0, r.BLOCKED, role=r.ROLE_TRIGGER, retry_after=1, answered_at=0.02)
    answered = [attempt(0.0, role=r.ROLE_TRIGGER, answered_at=0.02) for _ in range(5)]
    follow = attempt(follow_at, follow_outcome, role=r.ROLE_FOLLOW_UP, request="deal list")
    return [*answered, refusal, follow]


@pytest.mark.parametrize(
    ("follow_outcome", "expected"),
    [(r.ANSWERED, r.PER_TYPE), (r.BLOCKED, r.SHARED), ("timeout", None)],
)
def test_the_follow_up_while_blocked_tells_per_type_from_shared(follow_outcome, expected) -> None:
    found, detail = r.infer_sharing(_block_attempts(follow_outcome), {r.DEAL_LIST})

    assert found == expected, detail


def test_sharing_is_inconclusive_without_a_refusal_or_after_the_block() -> None:
    no_refusal = [attempt(0.0, role=r.ROLE_TRIGGER) for _ in range(12)]
    assert r.infer_sharing(no_refusal, {r.DEAL_LIST})[0] is None

    late = _block_attempts(r.ANSWERED, follow_at=1.5)
    found, detail = r.infer_sharing(late, {r.DEAL_LIST})
    assert found is None
    assert "after the trendbars block had ended" in detail


def test_an_answer_from_a_type_not_shown_limited_never_means_per_type() -> None:
    found, detail = r.infer_sharing(_block_attempts(r.ANSWERED), set())

    assert found is None
    assert detail == "the deal list type was not shown to be limited"


def test_retry_after_reports_its_value_and_whether_waiting_it_out_was_enough() -> None:
    refusal = attempt(0.0, r.BLOCKED, retry_after=1)

    def infer(attempts):
        return r.infer_retry_after(attempts, 0.5, 60.0)

    assert infer([])[0] is None
    assert infer([attempt(0.0, r.BLOCKED)])[0] is None
    assert infer([refusal])[:2] == ("retryAfter = 1 s", None)
    answered = attempt(1.6, role=r.ROLE_AFTER_WAIT)
    assert infer([refusal, answered])[:2] == ("retryAfter = 1 s", True)
    refused = attempt(1.6, r.BLOCKED, role=r.ROLE_AFTER_WAIT, retry_after=2)
    assert infer([refusal, refused])[:2] == ("retryAfter in [1, 2] s", False)
    huge = attempt(0.0, r.BLOCKED, retry_after=1000)
    assert infer([huge])[0] == "retryAfter = 1000: implausible; unit unconfirmed (seconds expected)"


def test_two_directions_that_disagree_leave_sharing_inconclusive() -> None:
    settings = r.Settings()
    plan = r.build_plan(settings)
    by_group = {step.group: step for step in plan}
    deal_list = next(s for s in plan if s.request == r.DEAL_LIST and s.group == r.GROUP_TYPES)
    burst_attempts = [
        attempt(0.0, r.ANSWERED if i < 5 else r.BLOCKED, role=r.ROLE_BURST, index=i)
        for i in range(8)
    ]
    results = [
        r.StepResult(deal_list, tuple(burst_attempts), r.RAN),
        r.StepResult(by_group[r.GROUP_SHARING], tuple(_block_attempts(r.ANSWERED)), r.RAN),
        r.StepResult(by_group[r.GROUP_SHARING_REVERSE], tuple(_block_attempts(r.BLOCKED)), r.RAN),
    ]

    finding = r.build_findings(results, settings)[2]

    assert finding.measured == "inconclusive: the two directions disagree"


# -- End to end against a limiting fake venue -----------------------------------------------


class VirtualTime:
    """A clock that moves only when the probe sleeps, once the venue has read what was sent."""

    def __init__(self, start: float = 1_000_000.3) -> None:
        self.now = start
        self.server: FakeCTraderServer | None = None
        self.guard = None

    def clock(self) -> float:
        return self.now

    async def sleep(self, secs: float) -> None:
        # Lets a request task started just before this sleep write its frame first.
        await asyncio.sleep(0)
        # The fake server has no event to wait on, and `wait_until` polls too slowly here.
        async with asyncio.timeout(5):
            while len(self.server.received) < self.guard.sent:  # noqa: ASYNC110
                await asyncio.sleep(0.001)
        self.now += max(0.0, secs)


def limiting_venue(
    limiter: FakeLimiter,
    *,
    on_request: Callable[[FakeCTraderServer], None] | None = None,
) -> FakeCTraderServer:
    server = account_venue.venue()
    order = make_order(91, 81)
    replies = {
        TRENDBARS_TYPE: lambda q: oa.ProtoOAGetTrendbarsRes(
            ctidTraderAccountId=q.ctidTraderAccountId,
            period=q.period,
            symbolId=q.symbolId,
        ),
        DEAL_LIST_TYPE: lambda q: oa.ProtoOADealListRes(
            ctidTraderAccountId=q.ctidTraderAccountId,
            hasMore=False,
        ),
        oa_model.PROTO_OA_ORDER_LIST_REQ: lambda q: oa.ProtoOAOrderListRes(
            ctidTraderAccountId=q.ctidTraderAccountId,
            order=[order],
            hasMore=False,
        ),
        oa_model.PROTO_OA_CASH_FLOW_HISTORY_LIST_REQ: lambda q: oa.ProtoOACashFlowHistoryListRes(
            ctidTraderAccountId=q.ctidTraderAccountId,
        ),
        oa_model.PROTO_OA_ORDER_DETAILS_REQ: lambda q: oa.ProtoOAOrderDetailsRes(
            ctidTraderAccountId=q.ctidTraderAccountId,
            order=order,
        ),
        TICK_DATA_TYPE: lambda q: oa.ProtoOAGetTickDataRes(
            ctidTraderAccountId=q.ctidTraderAccountId,
            hasMore=False,
        ),
        TRADER_TYPE: lambda q: account_venue.for_account(
            account_venue.RECORDED["trader"][0],
            q.ctidTraderAccountId,
        ),
    }

    def limited(request, payload_type, reply):
        if on_request is not None:
            on_request(server)
        retry_after = limiter.admit(payload_type)
        if retry_after is None:
            return reply(request)
        return oa.ProtoOAErrorRes(
            ctidTraderAccountId=request.ctidTraderAccountId,
            errorCode="BLOCKED_PAYLOAD_TYPE",
            description=VENUE_DESCRIPTION,
            retryAfter=retry_after,
        )

    for payload_type, reply in replies.items():
        server.on(payload_type, lambda q, t=payload_type, f=reply: limited(q, t, f))
    return server


def credentials() -> r.AccountCredentials:
    return r.AccountCredentials(CLIENT_ID, CLIENT_SECRET, ACCESS_TOKEN, None, None)


async def run_against(
    tmp_path: pathlib.Path,
    *,
    shape: str = r.SLIDING,
    shared: bool = False,
    siblings: tuple[int, ...] = (DEAL_LIST_TYPE,),
    retry_after: int = 1,
    settings: r.Settings | None = None,
    plan=None,
    confirm: Callable[[str], str] = lambda _prompt: "y",
    stop_after: int | None = None,
    on_request: Callable[[FakeCTraderServer], None] | None = None,
    handle_interrupt: bool = False,
):
    """Run the probe against a limiting venue; returns the exit code, the venue and the log."""
    vt = VirtualTime()
    limiter = FakeLimiter(
        vt.clock,
        limiter_groups(shared=shared, siblings=siblings),
        shape=shape,
        retry_after=retry_after,
    )
    settings = settings or r.Settings(trader_login=account_venue.TRADER_LOGIN)
    guard = r.ReadOnlyGuard(settings.max_requests)

    def hook(server: FakeCTraderServer) -> None:
        if stop_after is not None and len(server.received) >= stop_after:
            guard.stop_requested = True
        if on_request is not None:
            on_request(server)

    server = limiting_venue(limiter, on_request=hook)
    vt.server, vt.guard = server, guard
    await server.start()
    try:
        code = await r.run_probe(
            settings,
            credentials(),
            r.build_plan(settings) if plan is None else plan,
            guard=guard,
            endpoints=r.Endpoints(server.host, server.host, server.port, tls=False),
            log_dir=tmp_path,
            confirm=confirm,
            clock=vt.clock,
            sleep=vt.sleep,
            handle_interrupt=handle_interrupt,
        )
    finally:
        await server.stop()
    return code, server, guard, read_log(tmp_path)


def read_log(directory: pathlib.Path) -> list[dict]:
    logs = list(directory.glob("verify_rate_limits-*.raw.jsonl"))
    return [json.loads(line) for line in logs[0].read_text().splitlines()] if logs else []


async def test_a_run_against_a_sliding_per_type_venue_reports_what_it_enforces(
    tmp_path,
    capsys,
) -> None:
    code, _, _, _ = await run_against(tmp_path, shape=r.SLIDING, shared=False)

    out = capsys.readouterr().out
    assert code == 0
    summary = out.split("Summary: ", 1)[1]
    assert "historical: 5 within any 1 s (sliding)" in summary
    assert (
        "deal list: limited at 8; order list: NOT limited at 8; cash-flow list: NOT limited at 8; "
        "order details: NOT limited at 8; tick data: skipped" in summary
    )
    assert "limit is per payload type" in summary
    assert "default: refusals from request 51" in summary
    assert "retryAfter = 1 s, waiting it out enough" in summary
    assert "one deal list request sent 0.00 s after the first trendbars refusal" in out


async def test_with_no_list_type_limited_tick_data_is_the_sibling(tmp_path, capsys) -> None:
    code, _, _, records = await run_against(tmp_path, siblings=(TICK_DATA_TYPE,))

    out = capsys.readouterr().out
    summary = out.split("Summary: ", 1)[1]
    assert code == 0
    assert "deal list: NOT limited at 8" in summary
    assert "tick data: limited at 8" in summary
    assert "limit is per payload type" in summary
    assert "one tick data request sent 0.00 s after the first trendbars refusal" in out
    assert any(rec.get("payload") == "ProtoOAGetTickDataReq" for rec in records)


async def test_with_no_other_limited_type_sharing_stays_inconclusive(tmp_path, capsys) -> None:
    code, server, _, _ = await run_against(tmp_path, siblings=())

    out = capsys.readouterr().out
    assert code == 0
    assert "measured:   inconclusive: no other limited type to test with" in out
    assert "per type or shared: inconclusive" in out.split("Summary: ", 1)[1]
    # The block steps were skipped: no trendbar burst of 12 went out.
    assert sum(isinstance(m, oa.ProtoOAGetTrendbarsReq) for m in server.received) == 29 + 54


async def test_a_run_against_a_fixed_shared_venue_reports_what_it_enforces(
    tmp_path, capsys
) -> None:
    code, _, _, _ = await run_against(tmp_path, shape=r.FIXED, shared=True)

    summary = capsys.readouterr().out.split("Summary: ", 1)[1]
    assert code == 0
    assert "historical: 5 per calendar second (fixed)" in summary
    assert "limit is shared" in summary


async def test_a_run_keeps_its_caps_and_waits_between_steps(tmp_path, capsys) -> None:
    settings = r.Settings(trader_login=account_venue.TRADER_LOGIN)

    _, server, guard, records = await run_against(tmp_path, settings=settings)

    attempts = [rec for rec in records if "outcome" in rec]
    assert guard.sent == len(server.received) <= settings.max_requests
    by_step: dict[str, list[dict]] = defaultdict(list)
    for rec in attempts:
        by_step[rec["step"]].append(rec)
    assert max(len(recs) for recs in by_step.values()) <= settings.max_step_requests
    steps = list(by_step.values())
    for before, after in itertools.pairwise(steps):
        retry_after = max((rec["retry_after"] or 0 for rec in before), default=0)
        wanted = max(settings.min_gap_secs, retry_after + settings.gap_margin_secs)
        gap = min(rec["sent_at"] for rec in after) - max(rec["answered_at"] for rec in before)
        assert gap >= wanted - 1e-6, (before[0]["step"], gap)


async def test_the_log_and_the_output_carry_no_credential_or_account_id(tmp_path, capsys) -> None:
    _, _, _, records = await run_against(tmp_path)

    captured = capsys.readouterr()
    log_text = "\n".join(json.dumps(rec) for rec in records)
    assert records
    assert_secret_free(
        None,
        CLIENT_ID,
        CLIENT_SECRET,
        ACCESS_TOKEN,
        VENUE_DESCRIPTION,
        stderr=log_text + captured.out + captured.err,
    )
    for identifier in (account_venue.ACCOUNT_ID, account_venue.TRADER_LOGIN):
        assert str(identifier) not in log_text
    assert {rec["payload"] for rec in records if "payload" in rec} <= {
        cls.__name__ for cls in r.PROBED_REQUESTS.values()
    }


async def test_a_declined_run_probes_nothing(tmp_path, capsys) -> None:
    code, server, _, records = await run_against(tmp_path, confirm=lambda _prompt: "n")

    out = capsys.readouterr().out
    assert code == 2
    assert [type(m).__name__ for m in server.received] == [
        "ProtoOAApplicationAuthReq",
        "ProtoOAGetAccountListByAccessTokenReq",
    ]
    assert records == []
    assert r.OTHER_SESSIONS in out


async def test_ctrl_c_at_the_prompt_declines(tmp_path, capsys) -> None:
    def interrupted(_prompt: str) -> str:
        signal.raise_signal(signal.SIGINT)
        return "y"

    try:
        code, server, _, records = await run_against(
            tmp_path,
            confirm=interrupted,
            handle_interrupt=True,
        )
    except KeyboardInterrupt:
        pytest.fail("Ctrl+C at the prompt escaped the probe")

    captured = capsys.readouterr()
    assert code == 2
    assert len(server.received) == 2
    assert records == []
    assert "Declined: nothing was probed." in captured.out
    assert "the log holds" not in captured.out + captured.err


async def test_a_real_sigint_stops_the_run_at_the_next_send(tmp_path, capsys) -> None:
    raised = []
    before = signal.getsignal(signal.SIGINT)

    def interrupt_once(server: FakeCTraderServer) -> None:
        if raised or len(server.received) < 20:
            return
        handler = signal.getsignal(signal.SIGINT)
        # Never raise into pytest's own handler: that would end the whole session.
        raised.append(handler is not signal.default_int_handler)
        if raised[0]:
            signal.raise_signal(signal.SIGINT)

    code, server, guard, _ = await run_against(
        tmp_path,
        on_request=interrupt_once,
        handle_interrupt=True,
    )

    captured = capsys.readouterr()
    assert raised == [True], "the probe did not install its own SIGINT handler"
    assert code == 1
    assert guard.stop_requested
    assert len(server.received) < 40
    assert "stopped by Ctrl+C" in captured.out
    assert "Stopping at the next send" in captured.err
    assert signal.getsignal(signal.SIGINT) is before


async def test_an_implausible_retry_after_is_reported_raw_and_stops_the_run(
    tmp_path,
    capsys,
) -> None:
    code, _, _, _ = await run_against(tmp_path, retry_after=1000)

    out = capsys.readouterr().out
    assert code == 1
    assert "retryAfter = 1000: implausible; unit unconfirmed (seconds expected)" in out
    assert "The run stopped at" in out


async def test_a_stop_ends_the_run_at_the_next_send_and_still_reports(tmp_path, capsys) -> None:
    code, server, guard, _ = await run_against(tmp_path, stop_after=20)

    out = capsys.readouterr().out
    assert code == 1
    assert guard.stop_requested
    assert len(server.received) < 40
    assert "The run stopped at 'trendbars, burst of 10': stopped by Ctrl+C" in out
    assert "not run" in out
    assert "Summary: historical:" in out


async def test_a_step_over_its_cap_is_stopped_by_the_probe_itself(tmp_path, capsys) -> None:
    settings = r.Settings(trader_login=account_venue.TRADER_LOGIN, max_step_requests=3)
    plan = (r.Step("trendbars, burst of 5", r.BURST, r.TRENDBARS, 5, r.GROUP_HISTORICAL_BURSTS),)

    code, server, _, _ = await run_against(tmp_path, settings=settings, plan=plan)

    assert code == 1
    assert sum(isinstance(m, oa.ProtoOAGetTrendbarsReq) for m in server.received) == 3
    assert "per-step cap of 3" in capsys.readouterr().out


async def test_a_dry_run_sends_nothing(tmp_path, capsys) -> None:
    server = account_venue.venue()
    await server.start()
    try:
        code = r.main(
            ["--dry-run", "--trader-login", "1"],
            endpoints=r.Endpoints(server.host, server.host, server.port, tls=False),
            env_path=tmp_path / "absent.env",
            log_dir=tmp_path,
        )
    finally:
        await server.stop()

    out = capsys.readouterr().out
    assert code == 0
    assert server.connection_count == 0
    assert "Dry run: nothing was sent." in out
    assert "at most 346 requests (cap 400" in out
    assert "expected run time: about 75 s with retryAfter = 1 s" in out
    assert r.OTHER_SESSIONS in out
    assert list(tmp_path.iterdir()) == []


@pytest.mark.skipif(shutil.which("git") is None, reason="git is not available")
def test_the_log_file_name_is_ignored_by_git() -> None:
    result = subprocess.run(
        ["git", "check-ignore", "-q", "logs/verify_rate_limits-20260101T000000Z.raw.jsonl"],
        cwd=_REPO_ROOT,
        check=False,
    )

    assert result.returncode == 0
