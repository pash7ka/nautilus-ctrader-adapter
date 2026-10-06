"""The balance checkpoint in the cache, against the fake venue.

The recorded history (a deposit, then closing deals up to balance version 6) is moved in time so
that its last change sits where a test needs it relative to the real clock. The checkpoint hour
is half a day away from now, so no checkpoint passes while a test runs.
"""

from __future__ import annotations

import asyncio
import json
import time
from datetime import UTC, datetime
from functools import partial
from zoneinfo import ZoneInfo

from nautilus_ctrader.common import balance_checkpoint
from nautilus_ctrader.common.balance_checkpoint import BalanceCheckpoint
from nautilus_ctrader.common.balance_history import OFF, checkpoint_at, next_checkpoint, value_json
from nautilus_ctrader.constants import BALANCE_CHECKPOINT_KEY, BUCKET_HISTORICAL
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as om
from tests.account_venue import ACCOUNT_ID
from tests.execution_replay import RECORDING
from tests.execution_venue import WEEK_MS, ExecutionVenue, exec_config, harness, trader
from tests.polling import wait_until

UTC_ZONE = ZoneInfo("UTC")
DAY_MS = 86_400_000
LAST_CHANGE_MS = 1_600_000_658_056  # version 6, the trader's
DEPOSIT_MS = 1_599_143_632_165  # version 2
BALANCE = 5_122_378_460
AVAILABLE = {"balance": "51223784.60", "currency": "USD", "first_deposit": "51224029.64"}


def now_ms() -> int:
    return int(time.time() * 1000)


def moment(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, UTC)


def as_ms(at: datetime) -> int:
    return round(at.timestamp() * 1000)


def iso(ms: int) -> str:
    return moment(ms).strftime("%Y-%m-%dT%H:%M:%S.") + f"{ms % 1000:03d}Z"


HOUR = (moment(now_ms()).hour + 12) % 24


def last_t() -> int:
    return as_ms(checkpoint_at(moment(now_ms()), HOUR, UTC_ZONE))


def config(**overrides):
    return exec_config(balance_checkpoint_hour=HOUR, **overrides)


def recorded_deals() -> list[om.ProtoOADeal]:
    return [deal for page in RECORDING["closing"]["account_deals"] for deal in page.deal]


def recorded_cash_flow() -> list[om.ProtoOADepositWithdraw]:
    return [op for page in RECORDING["closing"]["cash_flow"] for op in page.depositWithdraw]


def history_venue(
    *, last_at: int, registration: int | None = None, venue: ExecutionVenue | None = None
) -> ExecutionVenue:
    """A venue whose history is the recorded one, its last change moved to `last_at`."""
    venue = venue or ExecutionVenue()
    shift = last_at - LAST_CHANGE_MS
    venue.deals = []
    for recorded in recorded_deals():
        deal = om.ProtoOADeal()
        deal.CopyFrom(recorded)
        deal.executionTimestamp += shift
        deal.createTimestamp += shift
        venue.deals.append(deal)
    venue.cash_flow = []
    for recorded in recorded_cash_flow():
        operation = om.ProtoOADepositWithdraw()
        operation.CopyFrom(recorded)
        operation.changeBalanceTimestamp += shift
        venue.cash_flow.append(operation)
    fields = {"balance": BALANCE, "balanceVersion": 6, "moneyDigits": 2}
    if registration is not None:
        fields["registrationTimestamp"] = registration
    venue.trader = trader(**fields)
    return venue


def deposit_at(last_at: int) -> int:
    return DEPOSIT_MS + last_at - LAST_CHANGE_MS


def key(h) -> dict | None:
    value = h.cache.get(BALANCE_CHECKPOINT_KEY)
    return None if value is None else json.loads(value)


async def test_off_writes_the_explicit_off_value() -> None:
    async with harness() as h:
        assert h.cache.get(BALANCE_CHECKPOINT_KEY) == value_json(OFF, None)
        assert key(h) == {
            "status": "off",
            "checkpoint": None,
            "balance": None,
            "currency": None,
            "reason": None,
            "first_deposit": None,
        }
        assert h.client._checkpoint._timer is None
        assert h.received(oa.ProtoOACashFlowHistoryListReq) == []
        assert h.received(oa.ProtoOADealListReq) == []


async def test_written_before_connect_returns() -> None:
    t_ms = last_t()
    last_at = t_ms - DAY_MS
    venue = history_venue(last_at=last_at, registration=deposit_at(last_at) - 1000)
    async with harness(execution_venue=venue, config=config(), connect=False) as h:
        await h.client._connect()

        assert key(h) == {
            "status": "available",
            "checkpoint": iso(t_ms),
            "reason": None,
            **AVAILABLE,
        }
        assert h.client._checkpoint._timer is not None


def with_a_close_after(venue: ExecutionVenue, at: int) -> None:
    """Add a closing deal at `at`, version 7, and move the trader to it."""
    deal = om.ProtoOADeal()
    deal.CopyFrom(next(d for d in venue.deals if d.closePositionDetail.balanceVersion == 6))
    deal.dealId += 100
    deal.executionTimestamp = at
    deal.createTimestamp = at - 100
    detail = deal.closePositionDetail
    detail.balanceVersion = 7
    detail.balance = (
        BALANCE + detail.grossProfit + detail.swap + detail.commission + detail.pnlConversionFee
    )
    venue.deals.append(deal)
    venue.trader = trader(
        balance=detail.balance,
        balanceVersion=7,
        moneyDigits=2,
        registrationTimestamp=venue.trader.trader.registrationTimestamp,
    )


async def test_the_value_survives_a_restart_mid_day() -> None:
    t_ms = last_t()
    last_at = t_ms - DAY_MS
    registration = deposit_at(last_at) - 1000
    async with harness(
        execution_venue=history_venue(last_at=last_at, registration=registration), config=config()
    ) as h:
        before = h.cache.get(BALANCE_CHECKPOINT_KEY)

    # The node restarts after a close made since the checkpoint.
    venue = history_venue(last_at=last_at, registration=registration)
    with_a_close_after(venue, t_ms + 1000)
    async with harness(execution_venue=venue, config=config()) as h:
        after = h.cache.get(BALANCE_CHECKPOINT_KEY)

    assert json.loads(before)["status"] == "available"
    assert after == before


async def test_rewritten_on_reconnect() -> None:
    t_ms = last_t()
    venue = history_venue(last_at=t_ms - DAY_MS)
    venue.fail = {om.PROTO_OA_CASH_FLOW_HISTORY_LIST_REQ}
    async with harness(execution_venue=venue, config=config()) as h:
        assert key(h)["reason"] == "history request failed"

        venue.fail = set()
        await h.server.drop_connections()
        await wait_until(lambda: key(h)["status"] == "available", timeout_secs=10)

        assert key(h)["checkpoint"] == iso(t_ms)
        assert key(h)["balance"] == AVAILABLE["balance"]


def anchor_windows(h) -> list:
    """The deal lists the anchor walk asked: a week each, unlike the fill window's."""
    return [
        r for r in h.received(oa.ProtoOADealListReq) if r.toTimestamp - r.fromTimestamp == WEEK_MS
    ]


async def test_the_reconnect_walk_is_bounded_then_completes_in_background() -> None:
    t_ms = last_t()
    venue = history_venue(last_at=t_ms - 12 * WEEK_MS)
    venue.fail = {om.PROTO_OA_CASH_FLOW_HISTORY_LIST_REQ}
    async with harness(execution_venue=venue, config=config()) as h:
        assert key(h)["reason"] == "history request failed"
        deals_before = len(anchor_windows(h))
        cash_flow_before = len(h.received(oa.ProtoOACashFlowHistoryListReq))
        at_release: list[tuple[int, int, dict]] = []
        release = h.client._release_buffer

        def counted_release() -> None:
            at_release.append(
                (
                    len(anchor_windows(h)) - deals_before,
                    len(h.received(oa.ProtoOACashFlowHistoryListReq)) - cash_flow_before,
                    key(h),
                ),
            )
            release()

        h.client._release_buffer = counted_release
        venue.fail = set()

        await h.server.drop_connections()
        await wait_until(lambda: key(h)["status"] == "available", timeout_secs=20)

        deals, cash_flow, value = at_release[0]
        assert 0 < deals <= 8
        assert 0 < cash_flow <= 8
        assert value["reason"] == "history incomplete"
        assert key(h)["balance"] == AVAILABLE["balance"]
        assert key(h)["checkpoint"] == iso(t_ms)


def direct(h, clock: dict, written: list, request=None) -> BalanceCheckpoint:
    """A checkpoint on the harness's connection, with a clock the test sets."""
    return BalanceCheckpoint(
        request=request or partial(h.client._request, bucket=BUCKET_HISTORICAL),
        account_id=ACCOUNT_ID,
        hour=HOUR,
        zone=UTC_ZONE,
        currency="USD",
        write=written.append,
        loop=asyncio.get_running_loop(),
        now_ms=lambda: clock["now"],
        log=h.logger,
    )


async def test_timer_fires_after_the_grace_and_recomputes() -> None:
    real_now = now_ms()
    t_ms = last_t()
    last_at = t_ms - DAY_MS
    venue = history_venue(last_at=last_at, registration=deposit_at(last_at) - 1000)
    async with harness(execution_venue=venue) as h:
        clock = {"now": real_now}
        written: list[bytes] = []
        loop = asyncio.get_running_loop()
        checkpoint = direct(h, clock, written)
        try:
            await checkpoint.refresh(venue.trader.trader, bounded=False)
            assert json.loads(written[-1])["checkpoint"] == iso(t_ms)

            firings: list[int] = []
            fire = checkpoint._fire
            checkpoint._fire = lambda t: (firings.append(t), fire(t))
            next_t = as_ms(next_checkpoint(moment(real_now), HOUR, UTC_ZONE))
            clock["now"] = next_t + 30_000 - 200
            checkpoint.schedule()
            # The node slept past the following checkpoint before the timer fired.
            clock["now"] = next_t + DAY_MS + 60_000
            await wait_until(lambda: len(written) == 2, timeout_secs=5)

            fired = json.loads(written[-1])
            assert fired["checkpoint"] == iso(next_t + DAY_MS)
            assert fired["status"] == "available"
            # The next one, a day on, and no second firing for the one slept past.
            assert checkpoint._timer is not None
            assert checkpoint._timer.when() - loop.time() > DAY_MS / 1000 - 60
            assert firings == [next_t]
        finally:
            checkpoint.stop()


async def test_a_refresh_overtaken_by_the_next_checkpoint_writes_nothing() -> None:
    real_now = now_ms()
    venue = history_venue(last_at=last_t() - DAY_MS)
    async with harness(execution_venue=venue) as h:
        held, opened = asyncio.Event(), asyncio.Event()
        plain = partial(h.client._request, bucket=BUCKET_HISTORICAL)

        async def request(payload):
            # The slow refresh's first deal list waits; every later request goes through.
            if isinstance(payload, oa.ProtoOADealListReq) and not held.is_set():
                held.set()
                await opened.wait()
            return await plain(payload)

        clock = {"now": real_now}
        written: list[bytes] = []
        checkpoint = direct(h, clock, written, request)
        try:
            slow = asyncio.create_task(checkpoint.refresh(venue.trader.trader, bounded=False))
            await asyncio.wait_for(held.wait(), timeout=5)
            next_t = as_ms(next_checkpoint(moment(real_now), HOUR, UTC_ZONE))
            clock["now"] = next_t + 30_000 - 200
            checkpoint.schedule()
            await wait_until(lambda: len(written) == 1, timeout_secs=5)
            assert json.loads(written[0])["checkpoint"] == iso(next_t)

            opened.set()
            await asyncio.wait_for(slow, timeout=5)

            assert len(written) == 1
            assert any("later checkpoint" in m for level, m in h.logger.lines if level == "debug")
        finally:
            checkpoint.stop()


async def test_overlapping_refreshes_read_each_deposit_window_once() -> None:
    t_ms = last_t()
    last_at = t_ms - DAY_MS
    registration = deposit_at(last_at) - 5 * WEEK_MS - DAY_MS
    venue = history_venue(last_at=last_at, registration=registration)
    async with harness(execution_venue=venue) as h:
        clock = {"now": now_ms()}
        written: list[bytes] = []
        checkpoint = direct(h, clock, written)
        try:
            await asyncio.gather(
                checkpoint.refresh(venue.trader.trader, bounded=False),
                checkpoint.refresh(venue.trader.trader, bounded=False),
            )
        finally:
            checkpoint.stop()

        deposit_windows = [
            (r.fromTimestamp, r.toTimestamp)
            for r in h.received(oa.ProtoOACashFlowHistoryListReq)
            if (r.fromTimestamp - registration) % WEEK_MS == 0
        ]
        assert len(deposit_windows) == 6
        assert len(set(deposit_windows)) == len(deposit_windows)
        assert checkpoint._deposit_windows == 6
        assert json.loads(written[-1])["first_deposit"] == AVAILABLE["first_deposit"]


async def test_an_unexpected_failure_is_unavailable_and_connect_succeeds(monkeypatch) -> None:
    def broken(*_args, **_kwargs):
        raise RuntimeError("broken rule")

    monkeypatch.setattr(balance_checkpoint, "balance_at", broken)
    t_ms = last_t()
    venue = history_venue(last_at=t_ms - DAY_MS)
    async with harness(execution_venue=venue, config=config(), connect=False) as h:
        await h.client._connect()

        assert key(h)["status"] == "unavailable"
        assert key(h)["reason"] == "history request failed"
        assert key(h)["checkpoint"] == iso(t_ms)
        assert any("could not be rebuilt" in line for line in h.logger.errors())


async def test_a_failed_history_request_is_unavailable() -> None:
    t_ms = last_t()
    venue = history_venue(last_at=t_ms - DAY_MS)
    venue.fail = {om.PROTO_OA_DEAL_LIST_REQ}
    async with harness(execution_venue=venue, config=config()) as h:
        assert key(h) == {
            "status": "unavailable",
            "checkpoint": iso(t_ms),
            "balance": None,
            "currency": "USD",
            "reason": "history request failed",
            "first_deposit": None,
        }
        assert any("history request failed" in line for line in h.logger.warnings())


async def test_start_walk_is_bounded_then_completes_in_background() -> None:
    t_ms = last_t()
    last_at = t_ms - 12 * WEEK_MS
    registration = deposit_at(last_at) - 20 * WEEK_MS
    venue = history_venue(last_at=last_at, registration=registration)
    async with harness(execution_venue=venue, config=config(), connect=False) as h:
        await h.client._connect()

        cash_flow = h.received(oa.ProtoOACashFlowHistoryListReq)
        deposit = [r for r in cash_flow if (r.fromTimestamp - registration) % WEEK_MS == 0]
        assert 0 < len(deposit) <= 8
        assert 0 < len(cash_flow) - len(deposit) <= 8
        assert 0 < len(h.received(oa.ProtoOADealListReq)) <= 8
        assert key(h)["status"] == "unavailable"
        assert key(h)["reason"] == "history incomplete"
        assert key(h)["first_deposit"] is None

        await wait_until(
            lambda: key(h)["status"] == "available" and key(h)["first_deposit"] is not None,
            timeout_secs=20,
        )
        assert key(h) == {
            "status": "available",
            "checkpoint": iso(t_ms),
            "reason": None,
            **AVAILABLE,
        }


async def test_first_deposit_found_late_rewrites_the_key() -> None:
    t_ms = last_t()
    last_at = t_ms - DAY_MS
    registration = deposit_at(last_at) - 10 * WEEK_MS - DAY_MS
    venue = history_venue(last_at=last_at, registration=registration)
    async with harness(execution_venue=venue, config=config(), connect=False) as h:
        await h.client._connect()

        assert key(h)["status"] == "available"
        assert key(h)["first_deposit"] is None

        await wait_until(lambda: key(h)["first_deposit"] is not None, timeout_secs=15)
        assert key(h) == {
            "status": "available",
            "checkpoint": iso(t_ms),
            "reason": None,
            **AVAILABLE,
        }


async def test_cash_flow_windows_never_exceed_a_week() -> None:
    t_ms = last_t()
    last_at = t_ms - 2 * WEEK_MS
    venue = history_venue(last_at=last_at, registration=deposit_at(last_at) - 3 * WEEK_MS)
    async with harness(execution_venue=venue, config=config()) as h:
        asked = h.received(oa.ProtoOACashFlowHistoryListReq)

        assert len(asked) > 3
        assert all(0 < r.toTimestamp - r.fromTimestamp <= WEEK_MS for r in asked)
        assert key(h)["status"] == "available"
        assert key(h)["first_deposit"] == AVAILABLE["first_deposit"]


async def test_an_item_without_a_version_does_not_chain() -> None:
    t_ms = last_t()
    venue = history_venue(last_at=t_ms - DAY_MS)
    for deal in venue.deals:
        if deal.HasField("closePositionDetail"):
            deal.closePositionDetail.ClearField("balanceVersion")
    async with harness(execution_venue=venue, config=config()) as h:
        assert key(h)["status"] == "unavailable"
        assert key(h)["reason"] == "balance versions do not chain"
        assert key(h)["balance"] is None
        assert any("balanceVersion" in line for line in h.logger.warnings())


async def test_an_item_without_a_scale_is_mixed_scales() -> None:
    t_ms = last_t()
    venue = history_venue(last_at=t_ms - DAY_MS)
    (operation,) = venue.cash_flow
    operation.ClearField("moneyDigits")
    # Within the first week read, so the anchor walk meets it.
    operation.changeBalanceTimestamp = t_ms - 2 * DAY_MS
    async with harness(execution_venue=venue, config=config()) as h:
        assert key(h)["status"] == "unavailable"
        assert key(h)["reason"] == "mixed money scales"
        assert key(h)["first_deposit"] is None
        assert any("moneyDigits" in line for line in h.logger.warnings())
