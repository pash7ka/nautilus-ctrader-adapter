"""The balance at a daily checkpoint, rebuilt from the recorded deal and cash-flow history.

The recorded account: a deposit (version 2), then four closing deals (versions 3 to 6); the
trader stands at version 6. The recording holds no registration time, so each test that needs
one states it.
"""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime
from zoneinfo import ZoneInfo

import pytest

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
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as om
from tests.execution_replay import RECORDING

TRADER_BALANCE = 5_122_378_460
TRADER_VERSION = 6
DEPOSIT = 5_122_402_964
WEEK_MS = 604_800_000
NEW_YORK = ZoneInfo("America/New_York")


def deals():
    return [deal for page in RECORDING["closing"]["account_deals"] for deal in page.deal]


def operations():
    return [
        operation
        for page in RECORDING["closing"]["cash_flow"]
        for operation in page.depositWithdraw
    ]


def recorded_changes() -> list[BalanceChange]:
    closing = [change for change in map(change_of_deal, deals()) if change is not None]
    return closing + [change_of_cash_flow(operation) for operation in operations()]


def value(t_ms: int, changes=None, **overrides):
    arguments = {
        "covered_from_ms": 0,
        "registration_ms": None,
        "trader_balance": TRADER_BALANCE,
        "trader_version": TRADER_VERSION,
        "trader_money_digits": 2,
        "reached_cap": False,
    }
    arguments.update(overrides)
    return balance_at(t_ms, recorded_changes() if changes is None else changes, **arguments)


def without_version(version: int) -> list[BalanceChange]:
    return [change for change in recorded_changes() if change.version != version]


def utc(*fields) -> datetime:
    return datetime(*fields, tzinfo=UTC)


def utc_ms(*fields) -> int:
    return int(utc(*fields).timestamp() * 1000)


def as_json(checkpoint: CheckpointValue, currency: str | None = "USD") -> dict:
    return json.loads(value_json(checkpoint, currency))


def test_the_recorded_changes():
    changes = sorted(recorded_changes(), key=lambda change: change.version)
    assert [(change.version, change.balance, change.delta) for change in changes] == [
        (2, DEPOSIT, DEPOSIT),
        (3, 5_122_399_809, -2621 - 515 - 19),
        (4, 5_122_399_685, -68 - 56),
        (5, 5_122_386_119, -8081 - 5485),
        (6, TRADER_BALANCE, -2121 - 5538),
    ]
    assert [change.cash_flow for change in changes] == [True, False, False, False, False]
    assert {change.money_digits for change in changes} == {2}
    assert changes[1].ts_ms == 1_599_531_088_355  # the execution, not the creation


def test_checkpoint_between_partial_close_and_stop():
    checkpoint = value(1_600_000_400_000)

    assert checkpoint == CheckpointValue(
        status="available",
        checkpoint_ms=1_600_000_400_000,
        balance=5_122_399_685,
        reason=None,
        first_deposit=DEPOSIT,
        money_digits=2,
    )
    assert as_json(checkpoint) == {
        "status": "available",
        "checkpoint": "2020-09-13T12:33:20.000Z",
        "balance": "51223996.85",
        "currency": "USD",
        "reason": None,
        "first_deposit": "51224029.64",
    }


def test_a_deal_exactly_at_t_belongs_after():
    assert value(1_600_000_355_547).balance == 5_122_399_809
    assert value(1_600_000_355_548).balance == 5_122_399_685


def test_anchor_is_the_deposit():
    checkpoint = value(1_599_200_000_000)

    assert checkpoint.status == "available"
    assert checkpoint.balance == DEPOSIT


def test_deal_created_before_t_executed_after_it():
    assert value(1_600_000_406_400).balance == 5_122_399_685


def test_an_open_position_floating_result_never_counts():
    opening = [deal for deal in deals() if not deal.HasField("closePositionDetail")]

    assert [deal.dealId for deal in opening] == [7_000_004, 7_000_001, 7_000_007]
    assert [change_of_deal(deal) for deal in opening] == [None, None, None]
    # Open from 1600000442966 to 1600000658056: the balance stays the previous close's.
    assert value(1_600_000_500_000).balance == 5_122_386_119


def test_before_the_first_deposit():
    t_ms = 1_599_100_000_000

    existing = value(t_ms, registration_ms=1_599_000_000_000)
    assert existing == CheckpointValue("available", t_ms, 0, None, DEPOSIT, 2)
    assert as_json(existing)["balance"] == "0.00"
    assert as_json(existing)["first_deposit"] == "51224029.64"

    opened_after = value(t_ms, registration_ms=1_599_140_000_000)
    assert opened_after == CheckpointValue(
        "unavailable", t_ms, None, Reason.OPENED_AFTER, DEPOSIT, 2
    )
    assert as_json(opened_after)["reason"] == "account opened after the checkpoint"
    assert as_json(opened_after)["first_deposit"] == "51224029.64"


def test_the_chain_from_zero_must_hold():
    t_ms, registration = 1_599_100_000_000, 1_599_000_000_000

    assert value(t_ms, without_version(4), registration_ms=registration).reason == Reason.NO_CHAIN
    no_deposit = [change for change in recorded_changes() if not change.cash_flow]
    assert value(t_ms, no_deposit, registration_ms=registration).reason == Reason.NO_CHAIN


def test_several_changes_in_one_millisecond_order_by_version():
    changes = recorded_changes()
    same_ms = next(change for change in changes if change.version == 4).ts_ms
    changes = [
        replace(change, ts_ms=same_ms) if change.version == 5 else change for change in changes
    ]

    assert value(same_ms + 1, list(reversed(changes))).balance == 5_122_386_119
    assert value(same_ms + 1, changes).balance == 5_122_386_119
    assert value(same_ms, changes).balance == 5_122_399_809


def test_a_missing_version_does_not_chain():
    checkpoint = value(1_600_000_400_000, without_version(5))

    assert checkpoint == CheckpointValue(
        "unavailable", 1_600_000_400_000, None, Reason.NO_CHAIN, DEPOSIT, 2
    )


def test_a_balance_that_does_not_follow_does_not_chain():
    changes = [
        replace(change, balance=change.balance + 1) if change.version == 5 else change
        for change in recorded_changes()
    ]

    assert value(1_600_000_400_000, changes).reason == Reason.NO_CHAIN


def test_a_conflicting_copy_of_a_version_does_not_chain():
    changes = recorded_changes()
    duplicate = next(change for change in changes if change.version == 3)

    assert value(1_600_000_400_000, [*changes, duplicate]).balance == 5_122_399_685
    conflicting = replace(duplicate, balance=duplicate.balance + 1)
    assert value(1_600_000_400_000, [*changes, conflicting]).reason == Reason.NO_CHAIN


def test_a_cash_flow_chains_with_either_sign():
    withdrawal = BalanceChange(
        ts_ms=1_600_000_700_000,
        version=7,
        balance=TRADER_BALANCE - 1000,
        delta=1000,
        money_digits=2,
        cash_flow=True,
        deposit=False,
    )
    trader = {"trader_version": 7, "trader_balance": TRADER_BALANCE - 1000}

    for delta in (1000, -1000):
        changes = [*recorded_changes(), replace(withdrawal, delta=delta)]
        assert value(1_600_000_400_000, changes, **trader).balance == 5_122_399_685
    changes = [*recorded_changes(), replace(withdrawal, delta=999)]
    assert value(1_600_000_400_000, changes, **trader).reason == Reason.NO_CHAIN


def test_the_trader_ahead_of_the_lists_is_incomplete():
    checkpoint = value(1_600_000_400_000, trader_version=7, trader_balance=TRADER_BALANCE - 1)

    assert checkpoint.status == "unavailable"
    assert checkpoint.reason == Reason.INCOMPLETE


def test_the_lists_ahead_of_the_trader_still_chain():
    checkpoint = value(1_600_000_400_000, trader_version=5, trader_balance=5_122_386_119)

    assert checkpoint.balance == 5_122_399_685


def test_the_trader_balance_must_match_its_version():
    checkpoint = value(1_600_000_400_000, trader_balance=TRADER_BALANCE + 1)

    assert checkpoint.reason == Reason.NO_CHAIN


def test_the_trader_balance_must_match_an_anchor_at_its_version():
    # After the last change, so that change is the anchor itself.
    assert value(1_600_000_700_000).balance == TRADER_BALANCE
    assert value(1_600_000_700_000, trader_balance=TRADER_BALANCE + 1).reason == Reason.NO_CHAIN


def test_no_change_in_the_week_needs_earlier():
    t_ms = 1_599_100_000_000
    week = {"covered_from_ms": t_ms - WEEK_MS}

    assert value(t_ms, **week) is NEED_EARLIER
    assert value(t_ms, registration_ms=t_ms - 2 * WEEK_MS, **week) is NEED_EARLIER
    assert value(t_ms, reached_cap=True, **week).reason == Reason.INCOMPLETE


def test_the_walk_stops_at_the_registration():
    t_ms = 1_599_100_000_000
    registration = t_ms - 2 * WEEK_MS

    reached = value(t_ms, covered_from_ms=registration, registration_ms=registration)
    assert reached.status == "available"
    assert reached.balance == 0


def test_unknown_registration_is_incomplete():
    checkpoint = value(1_599_100_000_000)

    assert checkpoint == CheckpointValue(
        "unavailable", 1_599_100_000_000, None, Reason.INCOMPLETE, DEPOSIT, 2
    )


def test_mixed_money_scales():
    changes = [
        replace(change, money_digits=3) if change.version == 6 else change
        for change in recorded_changes()
    ]

    assert value(1_600_000_400_000, changes).reason == Reason.MIXED_SCALES
    assert value(1_600_000_400_000, trader_money_digits=3).reason == Reason.MIXED_SCALES
    # No single scale to state it in.
    assert value(1_600_000_400_000, changes).first_deposit is None


def test_first_deposit():
    assert first_deposit(recorded_changes()).balance == DEPOSIT
    assert first_deposit([change for change in recorded_changes() if not change.cash_flow]) is None


def test_the_first_deposit_is_told_by_the_operation_type():
    (operation,) = operations()
    assert operation.operationType == om.BALANCE_DEPOSIT
    assert change_of_cash_flow(operation).deposit is True
    assert {change_of_deal(deal).deposit for deal in deals() if change_of_deal(deal)} == {None}

    other = om.ProtoOADepositWithdraw()
    other.CopyFrom(operation)
    other.operationType = om.BALANCE_DEPOSIT_TRANSFER
    changes = [change_of_cash_flow(other) if c.cash_flow else c for c in recorded_changes()]

    # Its balance equals its delta, yet it is not a deposit.
    assert change_of_cash_flow(other).deposit is False
    assert first_deposit(changes) is None
    t_ms, registration = 1_599_100_000_000, 1_599_000_000_000
    assert value(t_ms, changes, registration_ms=registration).reason == Reason.NO_CHAIN


@pytest.mark.parametrize("field", ["balanceVersion", "moneyDigits"])
def test_an_item_without_a_version_or_scale_is_refused_by_name(field):
    deal = om.ProtoOADeal()
    deal.CopyFrom(next(deal for deal in deals() if deal.HasField("closePositionDetail")))
    deal.closePositionDetail.ClearField(field)
    operation = om.ProtoOADepositWithdraw()
    operation.CopyFrom(operations()[0])
    operation.ClearField(field)

    for convert, item in ((change_of_deal, deal), (change_of_cash_flow, operation)):
        with pytest.raises(MissingField) as refused:
            convert(item)
        assert refused.value.field == field
        assert isinstance(refused.value, ValueError)


def test_checkpoint_hour_in_utc():
    t = checkpoint_at(utc(2020, 9, 13, 12, 33, 20), 12, ZoneInfo("UTC"))

    assert t == utc(2020, 9, 13, 12)
    assert t.tzinfo is UTC
    assert int(t.timestamp() * 1000) == 1_599_998_400_000
    assert value(1_599_998_400_000).balance == 5_122_399_809


def test_a_checkpoint_at_now_is_the_current_one():
    zone = ZoneInfo("UTC")

    assert checkpoint_at(utc(2020, 9, 13, 12), 12, zone) == utc(2020, 9, 13, 12)
    assert checkpoint_at(utc(2020, 9, 13, 11, 59), 12, zone) == utc(2020, 9, 12, 12)
    assert next_checkpoint(utc(2020, 9, 13, 12), 12, zone) == utc(2020, 9, 14, 12)
    assert next_checkpoint(utc(2020, 9, 13, 11, 59), 12, zone) == utc(2020, 9, 13, 12)


def test_daylight_saving_moves_the_checkpoint_in_utc():
    assert checkpoint_at(utc(2026, 7, 1, 23), 17, NEW_YORK) == utc(2026, 7, 1, 21)
    assert checkpoint_at(utc(2026, 12, 1, 23), 17, NEW_YORK) == utc(2026, 12, 1, 22)


def test_checkpoint_hour_missing_on_spring_forward():
    # 02:00 does not exist on 2026-03-08: the clocks go from 01:59:59 EST to 03:00 EDT.
    assert checkpoint_at(utc(2026, 3, 8, 12), 2, NEW_YORK) == utc(2026, 3, 8, 7)
    assert next_checkpoint(utc(2026, 3, 8, 6, 30), 2, NEW_YORK) == utc(2026, 3, 8, 7)
    assert checkpoint_at(utc(2026, 3, 8, 6, 59, 59), 2, NEW_YORK) == utc(2026, 3, 7, 7)


def test_a_skipped_day_takes_the_first_instant_after_it():
    apia = ZoneInfo("Pacific/Apia")
    # 2011-12-30 does not exist there: 2011-12-29 23:59:59 (-10) is followed by 2011-12-31 00:00.
    jump = utc(2011, 12, 30, 10)

    assert checkpoint_at(utc(2011, 12, 30, 13), 5, apia) == jump
    assert next_checkpoint(utc(2011, 12, 30, 9), 5, apia) == jump


def test_checkpoint_hour_repeated_on_fall_back():
    # 01:00 occurs at 05:00 UTC (EDT) and again at 06:00 UTC (EST) on 2026-11-01.
    assert checkpoint_at(utc(2026, 11, 1, 6, 30), 1, NEW_YORK) == utc(2026, 11, 1, 5)
    assert next_checkpoint(utc(2026, 11, 1, 4), 1, NEW_YORK) == utc(2026, 11, 1, 5)
    assert next_checkpoint(utc(2026, 11, 1, 5), 1, NEW_YORK) == utc(2026, 11, 2, 6)


def test_next_checkpoint_after_a_missed_day():
    # Last written for 2026-10-05; the node wakes on 2026-10-07 before the day's checkpoint.
    now = utc(2026, 10, 7, 15)

    assert checkpoint_at(now, 17, NEW_YORK) == utc(2026, 10, 6, 21)
    assert next_checkpoint(now, 17, NEW_YORK) == utc(2026, 10, 7, 21)


def test_checkpoint_arguments_are_checked():
    with pytest.raises(ValueError, match="timezone-aware"):
        checkpoint_at(datetime(2020, 9, 13, 12), 12, NEW_YORK)
    with pytest.raises(ValueError, match="timezone-aware"):
        next_checkpoint(datetime(2020, 9, 13, 12), 12, NEW_YORK)
    for hour in (-1, 24):
        with pytest.raises(ValueError, match="hour"):
            checkpoint_at(utc(2020, 9, 13, 12), hour, NEW_YORK)


KEYS = ["status", "checkpoint", "balance", "currency", "reason", "first_deposit"]


def test_value_json_shapes():
    t_ms = utc_ms(2020, 9, 13, 12)
    available = CheckpointValue("available", t_ms, 5_122_399_809, None, None, 2)

    assert list(as_json(available)) == KEYS
    assert as_json(available) == {
        "status": "available",
        "checkpoint": "2020-09-13T12:00:00.000Z",
        "balance": "51223998.09",
        "currency": "USD",
        "reason": None,
        "first_deposit": None,
    }

    texts = {
        Reason.REQUEST_FAILED: "history request failed",
        Reason.INCOMPLETE: "history incomplete",
        Reason.NO_CHAIN: "balance versions do not chain",
        Reason.OPENED_AFTER: "account opened after the checkpoint",
        Reason.MIXED_SCALES: "mixed money scales",
    }
    assert set(texts) == set(Reason)
    for reason, text in texts.items():
        unavailable = CheckpointValue("unavailable", t_ms + 7, None, reason, DEPOSIT, 2)
        assert list(as_json(unavailable)) == KEYS
        assert as_json(unavailable) == {
            "status": "unavailable",
            "checkpoint": "2020-09-13T12:00:00.007Z",
            "balance": None,
            "currency": "USD",
            "reason": text,
            "first_deposit": "51224029.64",
        }

    assert OFF.status == "off"
    assert list(as_json(OFF)) == KEYS
    assert as_json(OFF, "USD") == dict.fromkeys(KEYS) | {"status": "off"}
    assert value_json(OFF, None) == value_json(OFF, "USD")


def test_value_json_decimals():
    def balance(amount: int, digits: int) -> str:
        return as_json(CheckpointValue("available", 0, amount, None, None, digits))["balance"]

    assert balance(-50, 2) == "-0.50"
    assert balance(0, 2) == "0.00"
    assert balance(1, 8) == "0.00000001"
    assert balance(123, 0) == "123"
    assert balance(10**20, 2) == "1000000000000000000.00"
    assert as_json(CheckpointValue("available", 0, 0, None, None, 2))["checkpoint"] == (
        "1970-01-01T00:00:00.000Z"
    )
