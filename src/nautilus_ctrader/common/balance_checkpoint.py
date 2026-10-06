"""The balance at the daily checkpoint, kept in the cache key `ctrader.balance_checkpoint`.

Reads the history the rule in `balance_history` needs, writes the key, and writes it again at
each checkpoint:

- **The anchor walk** reads closing deals and cash flows a week at a time, back from now, until
  the rule has its answer, at most 156 weeks.
- **The first-deposit walk** reads cash flows a week at a time, forward from the account's
  registration, until it finds the deposit; what it finds is kept for the session.
- **At start and on a reconnect** each walk reads at most 8 weeks, so neither is held up. The
  key is written with what is known (`history incomplete`, or no first deposit yet), and the
  walks finish in the background, writing the key again.

A walk cut short by a lost connection writes nothing: the key keeps its previous value, with its
own checkpoint, until the reconnect writes it. Nor does a walk whose checkpoint is older than
the one last written: the newer value stands.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from zoneinfo import ZoneInfo

from nautilus_trader.common.component import Logger

from nautilus_ctrader.common.balance_history import (
    NEED_EARLIER,
    OFF,
    BalanceChange,
    CheckpointValue,
    MissingField,
    Reason,
    balance_at,
    change_of_cash_flow,
    change_of_deal,
    checkpoint_at,
    first_deposit,
    next_checkpoint,
    value_json,
)
from nautilus_ctrader.common.errors import CTraderConnectionError, CTraderError
from nautilus_ctrader.common.history import WEEK_MS, Request, deals_between, weekly_windows
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as om

START_WINDOWS = 8
MAX_WINDOWS = 156
# The node's clock may run ahead of the broker's.
GRACE_MS = 30_000


@dataclass
class _Walk:
    """The history read back toward checkpoint `t_ms`, covering `covered_from_ms` to `upper_ms`.

    `changes` is keyed by kind and id, since neighbouring windows share their edge.
    """

    t_ms: int
    upper_ms: int
    covered_from_ms: int
    windows: int = 0
    changes: dict[tuple[bool, int], BalanceChange] = field(default_factory=dict)
    settled: bool = False
    failure: Reason | None = None


class BalanceCheckpoint:
    """
    Keeps the balance at the most recent checkpoint in the cache, and rewrites it at the next.

    Parameters
    ----------
    request : Request
        Sends a request; the caller binds it to the historical rate limit.
    account_id : int
        The `ctidTraderAccountId` of the account.
    hour : int, optional
        The checkpoint hour, 0-23; `None` writes the off value and arms no timer.
    zone : ZoneInfo
        The zone `hour` is in.
    currency : str
        The deposit asset's name, written beside the amounts.
    write : Callable[[bytes], None]
        Stores the key's value.
    loop : asyncio.AbstractEventLoop
        Runs the timer and the background walks.
    now_ms : Callable[[], int]
        The current Unix time in milliseconds.
    log : Logger
        The client's logger.

    """

    def __init__(
        self,
        *,
        request: Request,
        account_id: int,
        hour: int | None,
        zone: ZoneInfo,
        currency: str,
        write: Callable[[bytes], None],
        loop: asyncio.AbstractEventLoop,
        now_ms: Callable[[], int],
        log: Logger,
    ) -> None:
        self._request = request
        self._account_id = account_id
        self._hour = hour
        self._zone = zone
        self._currency = currency
        self._write = write
        self._loop = loop
        self._now_ms = now_ms
        self._log = log
        self._timer: asyncio.TimerHandle | None = None
        self._task: asyncio.Task | None = None
        # Set for good by `stop()`: a refresh still running then writes nothing, so it starts no
        # background walk either.
        self._stopped = False
        self._deposit: BalanceChange | None = None
        # Where the first-deposit walk goes on from; `None` until it starts.
        self._deposit_from_ms: int | None = None
        self._deposit_windows = 0
        # The checkpoint of the value last written.
        self._written_ms: int | None = None
        # Overlapping refreshes share the walk, so none reads a window twice or counts it twice.
        self._deposit_lock = asyncio.Lock()

    async def refresh(self, trader: om.ProtoOATrader, *, bounded: bool) -> None:
        """Write the balance at the most recent checkpoint, `trader` being read just before.

        A walk still running in the background is given up for this one. `bounded` walks 8
        windows each here and the rest in the background; otherwise the walks run to their end
        here. Never raises but for cancellation: a failed history request is written as
        `history request failed`, and a failed write is logged.
        """
        self._cancel_task()
        if self._hour is None:
            self._write(value_json(OFF, None))
            return
        await self._run(trader, bounded=bounded)

    def schedule(self) -> None:
        """Arm the timer for the next checkpoint plus the grace; it recomputes the checkpoint."""
        if self._hour is not None and not self._stopped:
            # A checkpoint whose grace has not yet run out is still the next one due.
            self._arm(self._now_ms() - GRACE_MS)

    def stop(self) -> None:
        """Cancel the timer and any walk in progress; nothing is written or started after this."""
        self._stopped = True
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        self._cancel_task()

    def _arm(self, after_ms: int) -> None:
        """Arm the timer for the first checkpoint after `after_ms`."""
        assert self._hour is not None
        if self._timer is not None:
            self._timer.cancel()
        t_ms = _ms(next_checkpoint(_moment(after_ms), self._hour, self._zone))
        delay_ms = max(0, t_ms + GRACE_MS - self._now_ms())
        self._timer = self._loop.call_later(delay_ms / 1000, self._fire, t_ms)

    def _fire(self, t_ms: int) -> None:
        self._timer = None
        # Not before the checkpoint just due: a wall clock behind the loop's would otherwise arm
        # it again.
        self._arm(max(t_ms, self._now_ms() - GRACE_MS))
        self._start_task(self._run(None, bounded=False))

    def _start_task(self, work) -> None:
        # One walk in the background at a time: an orphan would outlive `stop()`.
        self._cancel_task()
        self._task = self._loop.create_task(work)
        self._task.add_done_callback(self._task_done)

    def _task_done(self, task: asyncio.Task) -> None:
        if not task.cancelled() and task.exception() is not None:
            self._log.error(f"Balance checkpoint not written: {task.exception()!r}")

    def _cancel_task(self) -> None:
        if self._task is not None:
            self._task.cancel()
            self._task = None

    async def _run(self, trader: om.ProtoOATrader | None, *, bounded: bool) -> None:
        assert self._hour is not None
        now = self._now_ms()
        t_ms = _ms(checkpoint_at(_moment(now), self._hour, self._zone))
        walk = _Walk(t_ms, upper_ms=now, covered_from_ms=now)
        limit = START_WINDOWS if bounded else MAX_WINDOWS
        trader = await self._settle(walk, trader, limit)
        if trader is not None and bounded and self._unfinished(walk, trader):
            self._start_task(self._settle(walk, trader, MAX_WINDOWS))

    async def _settle(
        self, walk: _Walk, trader: om.ProtoOATrader | None, limit: int
    ) -> om.ProtoOATrader | None:
        """Walk up to `limit` windows each and write the key; returns the trader it read first.

        Writes nothing and returns `None` when the connection is lost, or when a later
        checkpoint was written meanwhile.
        """
        digits = None
        try:
            if trader is None:
                trader = await self._trader()
            digits = trader.moneyDigits if trader.HasField("moneyDigits") else None
            await self._walk_back(walk, trader, limit)
            await self._walk_deposit(_registration(trader), limit)
            # Read after the lists: a change made while they were read must chain too.
            latest = _newer(trader, await self._trader())
            await self._top_up(walk, latest)
            value = self._value(walk, latest)
        except CTraderConnectionError as e:
            self._log.info(f"Balance checkpoint not written, the connection was lost: {e}")
            return None
        except CTraderError as e:
            self._log.warning(f"Balance checkpoint unavailable, a history request failed: {e}")
            walk.failure = Reason.REQUEST_FAILED
            value = CheckpointValue(
                "unavailable", walk.t_ms, None, Reason.REQUEST_FAILED, None, digits
            )
        except Exception as e:
            self._log.exception("Balance checkpoint could not be rebuilt", e)
            walk.failure = Reason.REQUEST_FAILED
            value = CheckpointValue(
                "unavailable", walk.t_ms, None, Reason.REQUEST_FAILED, None, digits
            )
        if self._stopped:
            self._log.debug("Balance checkpoint dropped: stopped")
            return None
        # A value for its own checkpoint stands even once a later one is due, until that one
        # is written.
        if self._written_ms is not None and self._written_ms > walk.t_ms:
            self._log.debug("Balance checkpoint dropped: a later checkpoint is written")
            return None
        try:
            if trader is not None:
                value = self._with_deposit(value, _registration(trader))
            self._write(value_json(value, self._currency))
        except Exception as e:
            self._log.exception("Balance checkpoint could not be written", e)
            return None
        self._written_ms = walk.t_ms
        reason = "" if value.reason is None else f" ({value.reason.value})"
        self._log.info(f"Balance checkpoint written: {value.status}{reason}")
        return trader

    def _unfinished(self, walk: _Walk, trader: om.ProtoOATrader) -> bool:
        if walk.failure is not None:
            return False
        return not walk.settled or self._deposit_unfinished(_registration(trader))

    async def _trader(self) -> om.ProtoOATrader:
        response = await self._request(oa.ProtoOATraderReq(ctidTraderAccountId=self._account_id))
        return response.trader

    async def _walk_back(self, walk: _Walk, trader: om.ProtoOATrader, limit: int) -> None:
        registration = _registration(trader)
        digits = trader.moneyDigits
        while not walk.settled and walk.failure is None and walk.windows < limit:
            end = walk.covered_from_ms
            start = max(0, end - WEEK_MS)
            await self._read(walk, start, end)
            walk.covered_from_ms = start
            walk.windows += 1
            if walk.failure is not None:
                return
            # Only whether to read earlier is asked here; the trader's balance plays no part.
            found = balance_at(
                walk.t_ms,
                list(walk.changes.values()),
                covered_from_ms=walk.covered_from_ms,
                registration_ms=registration,
                trader_balance=0,
                trader_version=0,
                trader_money_digits=digits,
                reached_cap=walk.windows >= MAX_WINDOWS,
            )
            walk.settled = found is not NEED_EARLIER

    async def _top_up(self, walk: _Walk, trader: om.ProtoOATrader) -> None:
        """Read the history since the walk began, if the trader has moved past what it read."""
        if not walk.settled or walk.failure is not None or not trader.HasField("balanceVersion"):
            return
        seen = max((change.version for change in walk.changes.values()), default=-1)
        if trader.balanceVersion <= seen:
            return
        now = self._now_ms()
        for start, end in weekly_windows(walk.upper_ms, now):
            await self._read(walk, start, end)
        walk.upper_ms = now

    async def _read(self, walk: _Walk, start: int, end: int) -> None:
        """Add one window's closing deals and cash flows to `walk`."""
        # TODO(verify): whether a list includes items exactly at a window's edges; both edges are
        # assumed, and a repeat is dropped by its id.
        deals, complete = await deals_between(self._request, self._account_id, start, end)
        if not complete:
            self._log.warning("Balance checkpoint: a deal list did not end; history incomplete")
            walk.failure = Reason.INCOMPLETE
            return
        # TODO(verify): that the cash-flow list has no pages and takes at most a week, as the
        # schema states; a week with many operations shows whether a list is cut.
        response = await self._request(
            oa.ProtoOACashFlowHistoryListReq(
                ctidTraderAccountId=self._account_id, fromTimestamp=start, toTimestamp=end
            ),
        )
        try:
            for deal in deals:
                change = change_of_deal(deal)
                if change is not None:
                    walk.changes[(False, deal.dealId)] = change
            for operation in response.depositWithdraw:
                walk.changes[(True, operation.balanceHistoryId)] = change_of_cash_flow(operation)
        except MissingField as e:
            self._log.warning(f"Balance checkpoint: a history item has no {e.field}")
            walk.failure = Reason.NO_CHAIN if e.field == "balanceVersion" else Reason.MIXED_SCALES

    async def _walk_deposit(self, registration_ms: int | None, limit: int) -> None:
        async with self._deposit_lock:
            await self._walk_deposit_locked(registration_ms, limit)

    async def _walk_deposit_locked(self, registration_ms: int | None, limit: int) -> None:
        # TODO(verify): that an account's first funding is a `BALANCE_DEPOSIT`; one recorded
        # account only.
        if not self._deposit_unfinished(registration_ms):
            return
        if self._deposit_from_ms is None:
            self._deposit_from_ms = registration_ms
        for _ in range(limit):
            now = self._now_ms()
            if not self._deposit_unfinished(registration_ms):
                return
            start = self._deposit_from_ms
            end = min(start + WEEK_MS, now)
            response = await self._request(
                oa.ProtoOACashFlowHistoryListReq(
                    ctidTraderAccountId=self._account_id, fromTimestamp=start, toTimestamp=end
                ),
            )
            self._deposit_from_ms = end
            self._deposit_windows += 1
            try:
                found = first_deposit([change_of_cash_flow(op) for op in response.depositWithdraw])
            except MissingField as e:
                self._log.warning(f"Balance checkpoint: a cash flow has no {e.field}")
                # Given up for the session: no first deposit is stated.
                self._deposit_windows = MAX_WINDOWS
                return
            if found is not None:
                self._deposit = found
                return

    def _deposit_unfinished(self, registration_ms: int | None) -> bool:
        """Whether the first-deposit walk has more to read now."""
        if registration_ms is None or self._deposit is not None:
            return False
        if self._deposit_windows >= MAX_WINDOWS:
            return False
        start = registration_ms if self._deposit_from_ms is None else self._deposit_from_ms
        return start < self._now_ms()

    def _value(self, walk: _Walk, trader: om.ProtoOATrader) -> CheckpointValue:
        digits = trader.moneyDigits if trader.HasField("moneyDigits") else None
        reason = walk.failure
        if reason is None and not trader.HasField("balanceVersion"):
            self._log.warning("Balance checkpoint: the trader has no balanceVersion")
            reason = Reason.NO_CHAIN
        if reason is None and digits is None:
            self._log.warning("Balance checkpoint: the trader has no moneyDigits")
            reason = Reason.MIXED_SCALES
        if reason is not None:
            return CheckpointValue("unavailable", walk.t_ms, None, reason, None, digits)
        value = balance_at(
            walk.t_ms,
            list(walk.changes.values()),
            covered_from_ms=walk.covered_from_ms,
            registration_ms=_registration(trader),
            trader_balance=trader.balance,
            trader_version=trader.balanceVersion,
            trader_money_digits=digits,
            reached_cap=not walk.settled or walk.windows >= MAX_WINDOWS,
        )
        assert isinstance(value, CheckpointValue)
        return value

    def _with_deposit(self, value: CheckpointValue, registration_ms: int | None) -> CheckpointValue:
        """`value` with the first deposit the forward walk found.

        Without a registration time there is no forward walk, and the rule's own stands.
        """
        if registration_ms is None or value.reason is Reason.MIXED_SCALES:
            return value
        found = self._deposit
        if found is None or found.money_digits != value.money_digits:
            return replace(value, first_deposit=None)
        return replace(value, first_deposit=found.balance)


def _registration(trader: om.ProtoOATrader) -> int | None:
    return trader.registrationTimestamp if trader.HasField("registrationTimestamp") else None


def _newer(first: om.ProtoOATrader, second: om.ProtoOATrader) -> om.ProtoOATrader:
    def version(trader: om.ProtoOATrader) -> int:
        return trader.balanceVersion if trader.HasField("balanceVersion") else -1

    return second if version(second) >= version(first) else first


def _moment(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, UTC)


def _ms(moment: datetime) -> int:
    return round(moment.timestamp() * 1000)
