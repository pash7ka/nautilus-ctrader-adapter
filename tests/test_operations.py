"""The operations-in-flight table and the brackets awaiting their exact levels."""

from __future__ import annotations

from decimal import Decimal

from nautilus_ctrader.common.operations import OperationsInFlight, PendingBracket, PendingBrackets
from nautilus_ctrader.common.venue_records import Level, Operations


def test_the_table_is_what_the_venue_model_asks() -> None:
    assert isinstance(OperationsInFlight(), Operations)


def test_an_amend_is_in_flight_until_each_one_begun_has_ended() -> None:
    table = OperationsInFlight()
    table.begin_amend(1)
    table.begin_amend(1)

    table.end_amend(1)
    assert table.amending(1)
    table.end_amend(1)
    assert not table.amending(1)
    assert not table.amending(2)


def test_ending_an_amend_never_begun_changes_nothing() -> None:
    table = OperationsInFlight()
    table.end_amend(1)

    assert not table.amending(1)


LOST = {Level.STOP_LOSS: Decimal("85150.00"), Level.TAKE_PROFIT: Decimal("85400.00")}


def test_a_lost_amend_is_matched_once_by_its_levels_on_its_position() -> None:
    table = OperationsInFlight()
    table.lost_amend(1, LOST)

    assert not table.late_amend(2, LOST)
    same = {Level.STOP_LOSS: Decimal("85150"), Level.TAKE_PROFIT: Decimal("85400.0")}
    assert table.late_amend(1, same)
    assert not table.late_amend(1, LOST)


def test_a_lost_amend_ends_at_another_change_or_when_forgotten() -> None:
    table = OperationsInFlight()
    table.lost_amend(1, LOST)
    table.lost_amend(2, LOST)

    assert not table.late_amend(1, {Level.STOP_LOSS: Decimal("85150.00")})
    assert not table.late_amend(1, LOST)
    table.forget_lost_amends()
    assert not table.late_amend(2, LOST)


# The newest broker time the node had seen when it sent the close.
ANCHOR = 1_000
LATER = ANCHOR + 1
ORDER = 6_000_001


def test_a_close_is_found_by_its_position_and_volume_until_it_ends() -> None:
    table = OperationsInFlight()
    table.begin_close("O-C-1", 1, 100, ANCHOR)

    assert table.closing(1, 100, LATER, ORDER) == "O-C-1"
    assert table.closing(1, 50, LATER, ORDER) is None
    assert table.closing(2, 100, LATER, ORDER) is None
    table.end_close("O-C-1")
    assert table.closing(1, 100, LATER, ORDER) is None


def test_a_close_takes_no_broker_order_created_by_the_time_it_was_sent() -> None:
    table = OperationsInFlight()
    table.begin_close("O-C-1", 1, 100, ANCHOR)

    assert table.closing(1, 100, ANCHOR, ORDER) is None
    assert table.closing(1, 100, ANCHOR - 60_000, ORDER) is None
    assert table.closing(1, 100, LATER, ORDER) == "O-C-1"


def test_a_close_sent_before_any_broker_time_has_no_bound() -> None:
    table = OperationsInFlight()
    table.begin_close("O-C-1", 1, 100, -1)

    assert table.closing(1, 100, 0, ORDER) == "O-C-1"


def test_the_order_a_close_s_own_answer_names_is_the_close_s_whatever_its_time() -> None:
    table = OperationsInFlight()
    table.begin_close("O-C-1", 1, 100, ANCHOR)
    table.answered("O-C-1", ORDER)

    assert table.closing(1, 100, ANCHOR - 60_000, ORDER) == "O-C-1"
    # Answered with that order, the close takes no other.
    assert table.closing(1, 100, LATER, ORDER + 1) is None
    table.end_close("O-C-1")
    assert table.closing(1, 100, ANCHOR - 60_000, ORDER) is None


def test_a_close_in_flight_names_its_position_until_it_ends() -> None:
    table = OperationsInFlight()
    table.begin_close("O-C-1", 1, 100, ANCHOR)

    assert table.close_position("O-C-1") == 1
    assert table.close_position("O-C-2") is None
    table.end_close("O-C-1")
    assert table.close_position("O-C-1") is None


def test_two_closes_of_one_volume_are_matched_one_at_a_time() -> None:
    table = OperationsInFlight()
    table.begin_close("O-C-1", 1, 100, ANCHOR)
    table.begin_close("O-C-2", 1, 100, ANCHOR)

    assert table.closing(1, 100, LATER, ORDER) == "O-C-1"
    table.end_close("O-C-1")
    assert table.closing(1, 100, LATER, ORDER) == "O-C-2"


def bracket(entry: str = "O-E") -> PendingBracket:
    return PendingBracket(
        entry_id=entry,
        legs={Level.STOP_LOSS: f"{entry}-SL", Level.TAKE_PROFIT: f"{entry}-TP"},
        requested={Level.STOP_LOSS: Decimal("85000.00"), Level.TAKE_PROFIT: Decimal("85500.00")},
    )


def test_a_bracket_is_found_by_its_entry_and_by_each_leg() -> None:
    brackets = PendingBrackets()
    one = bracket()
    brackets.add(one)

    assert brackets.by_entry("O-E") is one
    assert brackets.by_leg("O-E-SL") == (one, Level.STOP_LOSS)
    assert brackets.by_leg("O-E-TP") == (one, Level.TAKE_PROFIT)
    assert brackets.by_leg("O-E") is None
    assert len(brackets) == 1


def test_a_removed_bracket_is_gone_and_iteration_survives_removal() -> None:
    brackets = PendingBrackets()
    brackets.add(bracket("O-1"))
    brackets.add(bracket("O-2"))

    for each in brackets:
        brackets.remove(each.entry_id)

    assert len(brackets) == 0
    assert brackets.by_entry("O-1") is None
    assert brackets.by_leg("O-2-SL") is None


def test_a_new_bracket_records_no_change_yet() -> None:
    one = bracket()

    assert one.cancels == set()
    assert one.modified == set()
    assert not one.correcting
    assert one.rounds == 0
