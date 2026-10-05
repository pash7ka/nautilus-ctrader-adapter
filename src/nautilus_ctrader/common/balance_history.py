"""The account balance at a daily checkpoint, rebuilt from the broker's own history.

The protocol has no request for a past balance, so the balance at a past moment is read back from
the closing deals and cash-flow operations. The rule is a literal reading of that history; nothing
here judges the value.

- **The checkpoint** `T` is the most recent occurrence of a configured hour in a configured time
  zone. An hour that a clock change skips is the first instant after it that exists; an hour that
  occurs twice is its first occurrence.
- **A balance change** is a closing deal, at its execution time, or a cash-flow operation, at its
  own time. Opening a position does not change the balance, and an open position's floating
  result never counts.
- **The balance at `T`** is the balance after the last change strictly before `T`, at millisecond
  precision. Changes are ordered by their balance version, since several can share a millisecond.
- **It is trusted** only when the versions run without a gap from that change to the newest one
  seen, which reaches the trader's current version, and each balance follows from the one before:
  a closing deal adds its gross profit, swap, commission and conversion fee; a cash flow adds or
  subtracts its amount.
- **With no change before `T`**, the history is read further back until it reaches the account's
  registration. An account registered after `T` did not exist at `T`. One registered at or before
  `T` held zero, provided the chain from zero holds through every change, the first being a
  deposit on a zero balance.

Pure: no I/O and no clock; every function takes the moment it needs.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from enum import Enum
from typing import Final
from zoneinfo import ZoneInfo

from nautilus_ctrader.common.venue_records import money_of
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as om

_DAY = timedelta(days=1)
_EPOCH = datetime(1970, 1, 1)


@dataclass(frozen=True)
class BalanceChange:
    """One change of the account balance; amounts are raw, scaled by `money_digits`.

    - `ts_ms`: a closing deal's `executionTimestamp`, a cash flow's `changeBalanceTimestamp`.
    - `balance`: the balance after the change.
    - `delta`: a closing deal's `grossProfit + swap + commission + pnlConversionFee`; a cash
      flow's own `delta`, whose sign the chain decides.
    """

    ts_ms: int
    version: int
    balance: int
    delta: int | None
    money_digits: int
    cash_flow: bool


def change_of_deal(deal: om.ProtoOADeal) -> BalanceChange | None:
    """The change a closing deal made, or `None` for a deal that closed nothing.

    Raises `ValueError` if the closing detail lacks `balanceVersion` or `moneyDigits`.
    """
    if not deal.HasField("closePositionDetail"):
        return None
    detail = deal.closePositionDetail
    return BalanceChange(
        ts_ms=deal.executionTimestamp,
        version=_required(detail, "balanceVersion"),
        balance=detail.balance,
        delta=detail.grossProfit + detail.swap + detail.commission + detail.pnlConversionFee,
        money_digits=_required(detail, "moneyDigits"),
        cash_flow=False,
    )


def change_of_cash_flow(operation: om.ProtoOADepositWithdraw) -> BalanceChange:
    """The change a cash-flow operation made.

    Raises `ValueError` if the operation lacks `balanceVersion` or `moneyDigits`.
    """
    return BalanceChange(
        ts_ms=operation.changeBalanceTimestamp,
        version=_required(operation, "balanceVersion"),
        balance=operation.balance,
        delta=operation.delta,
        money_digits=_required(operation, "moneyDigits"),
        cash_flow=True,
    )


def _required(message, field: str) -> int:
    if not message.HasField(field):
        raise ValueError(f"{type(message).__name__} without {field}")
    return getattr(message, field)


def checkpoint_at(now: datetime, hour: int, zone: ZoneInfo) -> datetime:
    """The most recent checkpoint at or before `now`, in UTC.

    Raises `ValueError` for a naive `now` or an hour outside 0-23.
    """
    day = _local_day(now, hour, zone)
    for back in range(3):
        checkpoint = _occurrence(day - back * _DAY, hour, zone)
        if checkpoint <= now:
            return checkpoint
    raise AssertionError("a checkpoint within three local days")


def next_checkpoint(now: datetime, hour: int, zone: ZoneInfo) -> datetime:
    """The first checkpoint after `now`, in UTC.

    Raises `ValueError` for a naive `now` or an hour outside 0-23.
    """
    day = _local_day(now, hour, zone)
    for ahead in range(3):
        checkpoint = _occurrence(day + ahead * _DAY, hour, zone)
        if checkpoint > now:
            return checkpoint
    raise AssertionError("a checkpoint within three local days")


def _local_day(now: datetime, hour: int, zone: ZoneInfo) -> date:
    if now.utcoffset() is None:
        raise ValueError(f"now must be timezone-aware, got {now!r}")
    if not 0 <= hour <= 23:
        raise ValueError(f"the checkpoint hour must be 0-23, got {hour}")
    return now.astimezone(zone).date()


def _occurrence(day: date, hour: int, zone: ZoneInfo) -> datetime:
    """`hour` on `day` in `zone`, as a UTC instant."""
    # fold=0: the first of a repeated hour.
    wall = datetime.combine(day, time(hour), tzinfo=zone)
    instant = wall.astimezone(UTC)
    naive = wall.replace(tzinfo=None)
    if instant.astimezone(zone).replace(tzinfo=None) == naive:
        return instant
    # Skipped: find the clock change, the first instant whose wall time is not before the hour.
    # A skipped time maps before the change with fold=1 and at or after it with fold=0.
    low = int(wall.replace(fold=1).astimezone(UTC).timestamp())
    high = int(instant.timestamp())
    while low < high:
        middle = (low + high) // 2
        if datetime.fromtimestamp(middle, zone).replace(tzinfo=None) >= naive:
            high = middle
        else:
            low = middle + 1
    return datetime.fromtimestamp(low, UTC)


class Reason(Enum):
    REQUEST_FAILED = "history request failed"
    INCOMPLETE = "history incomplete"
    NO_CHAIN = "balance versions do not chain"
    OPENED_AFTER = "account opened after the checkpoint"
    MIXED_SCALES = "mixed money scales"


@dataclass(frozen=True)
class CheckpointValue:
    """The balance at a checkpoint, as the cache key carries it.

    - `status`: `"available"`, `"unavailable"` (with a `reason`) or `"off"`.
    - `balance`, `first_deposit`: raw amounts, scaled by `money_digits`.
    """

    status: str
    checkpoint_ms: int | None
    balance: int | None
    reason: Reason | None
    first_deposit: int | None
    money_digits: int | None


OFF: Final = CheckpointValue("off", None, None, None, None, None)

NEED_EARLIER: Final = object()
"""The history given holds no change before the checkpoint yet: read further back."""


def balance_at(
    t_ms: int,
    changes: Sequence[BalanceChange],
    *,
    covered_from_ms: int,
    registration_ms: int | None,
    trader_balance: int,
    trader_version: int,
    trader_money_digits: int,
    reached_cap: bool,
) -> CheckpointValue | object:
    """The balance immediately before `t_ms`, or `NEED_EARLIER`.

    - `changes`: every balance change from `covered_from_ms` to now, in any order; a version
      given twice must be the same change.
    - `registration_ms`: the account's registration time, `None` when the broker did not give one.
    - `trader_balance`, `trader_version`, `trader_money_digits`: the trader as read before the
      history.
    - `reached_cap`: the history cannot be read further back than `covered_from_ms`.

    `first_deposit` is filled whenever a deposit on a zero balance is among `changes`, whatever
    the status.
    """
    digits = trader_money_digits
    if any(change.money_digits != digits for change in changes):
        return CheckpointValue("unavailable", t_ms, None, Reason.MIXED_SCALES, None, digits)
    deposit = first_deposit(changes)
    deposit_balance = None if deposit is None else deposit.balance

    def result(balance: int | None, reason: Reason | None) -> CheckpointValue:
        if reason is not None:
            return CheckpointValue("unavailable", t_ms, None, reason, deposit_balance, digits)
        return CheckpointValue("available", t_ms, balance, None, deposit_balance, digits)

    ordered = _by_version(changes)
    if ordered is None:
        return result(None, Reason.NO_CHAIN)

    before = [index for index, change in enumerate(ordered) if change.ts_ms < t_ms]
    if before:
        anchor = ordered[before[-1]]
        after = ordered[before[-1] + 1 :]
        reason = _chain(anchor.balance, anchor.version, after, trader_balance, trader_version)
        return result(anchor.balance, reason)

    if registration_ms is not None and registration_ms > t_ms:
        return result(None, Reason.OPENED_AFTER)
    if reached_cap:
        return result(None, Reason.INCOMPLETE)
    if registration_ms is None:
        return result(None, Reason.INCOMPLETE) if covered_from_ms <= 0 else NEED_EARLIER
    if covered_from_ms > registration_ms:
        return NEED_EARLIER
    if ordered and not _on_zero(ordered[0]):
        return result(None, Reason.NO_CHAIN)
    return result(0, _chain(0, None, ordered, trader_balance, trader_version))


def first_deposit(changes: Sequence[BalanceChange]) -> BalanceChange | None:
    """The earliest cash flow made on a zero balance, if `changes` hold one."""
    deposits = [change for change in changes if _on_zero(change)]
    return min(deposits, key=lambda change: change.version, default=None)


def _on_zero(change: BalanceChange) -> bool:
    return change.cash_flow and change.delta is not None and change.balance == change.delta


def _by_version(changes: Sequence[BalanceChange]) -> list[BalanceChange] | None:
    """`changes` ordered by version, a repeated copy dropped; `None` if two disagree."""
    by_version: dict[int, BalanceChange] = {}
    for change in changes:
        if by_version.setdefault(change.version, change) != change:
            return None
    return [by_version[version] for version in sorted(by_version)]


def _chain(
    balance: int,
    version: int | None,
    after: Sequence[BalanceChange],
    trader_balance: int,
    trader_version: int,
) -> Reason | None:
    """Why the changes `after` a balance do not lead to the trader, or `None` when they do.

    `version` is `None` for the chain from zero, whose first change has no predecessor.
    """
    for change in after:
        if version is not None and change.version != version + 1:
            return Reason.NO_CHAIN
        if not _follows(balance, change):
            return Reason.NO_CHAIN
        if change.version == trader_version and change.balance != trader_balance:
            return Reason.NO_CHAIN
        balance, version = change.balance, change.version
    if (version or 0) < trader_version:
        return Reason.INCOMPLETE
    return None


def _follows(balance: int, change: BalanceChange) -> bool:
    if change.delta is None:
        return False
    if change.cash_flow:
        # TODO(verify): the sign of a withdrawal's delta; confirmed once a withdrawal is recorded.
        return change.balance - balance in (change.delta, -change.delta)
    return balance + change.delta == change.balance


def value_json(value: CheckpointValue, currency: str | None) -> bytes:
    """`value` as UTF-8 JSON with the keys `status`, `checkpoint`, `balance`, `currency`,
    `reason` and `first_deposit`.

    - Amounts are decimal text with exactly `money_digits` fraction digits, never an exponent.
    - `checkpoint` is `YYYY-MM-DDTHH:MM:SS.sssZ`.
    - The off value carries nulls only, `currency` included.
    """

    def money(amount: int | None) -> str | None:
        return None if amount is None else format(money_of(amount, value.money_digits), "f")

    checkpoint = None
    if value.checkpoint_ms is not None:
        moment = _EPOCH + timedelta(milliseconds=value.checkpoint_ms)
        checkpoint = moment.isoformat(timespec="milliseconds") + "Z"
    return json.dumps(
        {
            "status": value.status,
            "checkpoint": checkpoint,
            "balance": money(value.balance),
            "currency": None if value.status == "off" else currency,
            "reason": None if value.reason is None else value.reason.value,
            "first_deposit": money(value.first_deposit),
        }
    ).encode("utf-8")
