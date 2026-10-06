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


SENT = 1_000


def test_a_close_is_found_by_its_position_and_volume_until_it_ends() -> None:
    table = OperationsInFlight()
    table.begin_close("O-C-1", 1, 100, SENT)

    assert table.closing(1, 100, SENT) == "O-C-1"
    assert table.closing(1, 50, SENT) is None
    assert table.closing(2, 100, SENT) is None
    table.end_close("O-C-1")
    assert table.closing(1, 100, SENT) is None


def test_a_close_takes_no_broker_order_created_before_it_was_sent() -> None:
    table = OperationsInFlight()
    table.begin_close("O-C-1", 1, 100, SENT)

    assert table.closing(1, 100, SENT - 1) is None
    assert table.closing(1, 100, SENT + 1) == "O-C-1"


def test_a_close_in_flight_names_its_position_until_it_ends() -> None:
    table = OperationsInFlight()
    table.begin_close("O-C-1", 1, 100, SENT)

    assert table.close_position("O-C-1") == 1
    assert table.close_position("O-C-2") is None
    table.end_close("O-C-1")
    assert table.close_position("O-C-1") is None


def test_two_closes_of_one_volume_are_matched_one_at_a_time() -> None:
    table = OperationsInFlight()
    table.begin_close("O-C-1", 1, 100, SENT)
    table.begin_close("O-C-2", 1, 100, SENT)

    assert table.closing(1, 100, SENT) == "O-C-1"
    table.end_close("O-C-1")
    assert table.closing(1, 100, SENT) == "O-C-2"


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
