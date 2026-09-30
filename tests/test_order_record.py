"""The adapter's own records in an order's `label` and `comment`.

`label` decides whether a position is the node's own, so a record that is malformed in any way
must read as foreign: the safe mistake is a false "not ours".
"""

from __future__ import annotations

import pytest

from nautilus_ctrader.common import order_record as rec
from nautilus_ctrader.common.order_record import LegIds

ENTRY = "O-20260929-120000-001-GER40-1"
STOP = "O-20260929-120000-001-GER40-2"
TARGET = "O-20260929-120000-001-GER40-3"


def test_a_label_round_trips_the_entry_id() -> None:
    label = rec.encode_label(ENTRY)

    assert label == f"ntca1:{ENTRY}"
    assert rec.parse_label(label) == ENTRY


@pytest.mark.parametrize(
    "label",
    [
        "",
        "ntca1",
        "ntca1:",
        "ntca2:" + ENTRY,
        "NTCA1:" + ENTRY,
        " ntca1:" + ENTRY,
        "ntca1:" + ENTRY + " ",
        "ntca1: " + ENTRY,
        "ntca1:O-1 O-2",
        "my robot",
        ENTRY,
        "xntca1:" + ENTRY,
        "ntca1|" + ENTRY,
        "ntca1::" + ENTRY + "|x",
        "ntca1:" + ENTRY + "\n",
        "ntca1:\u00a0" + ENTRY,
        "ntca1:" + ENTRY + "\u3000",
        "ntca1:" + "9" * rec.LABEL_MAX,
    ],
)
def test_parse_label_reads_anything_malformed_as_foreign(label: str) -> None:
    assert rec.parse_label(label) is None


def test_a_comment_round_trips_both_legs() -> None:
    comment = rec.encode_comment(LegIds(stop_loss=STOP, take_profit=TARGET))

    assert comment == f"ntca1|sl={STOP}|tp={TARGET}"
    assert rec.parse_comment(comment) == LegIds(stop_loss=STOP, take_profit=TARGET)


@pytest.mark.parametrize(
    "legs",
    [
        LegIds(stop_loss=STOP, take_profit=None),
        LegIds(stop_loss=None, take_profit=TARGET),
        LegIds(stop_loss=None, take_profit=None),
    ],
)
def test_a_comment_round_trips_a_missing_leg(legs: LegIds) -> None:
    assert rec.parse_comment(rec.encode_comment(legs)) == legs


def test_a_comment_with_no_legs_still_carries_the_marker() -> None:
    assert rec.encode_comment(LegIds(None, None)) == "ntca1"


@pytest.mark.parametrize(
    "comment",
    [
        "",
        "ntca2|sl=" + STOP,
        "ntca1|",
        "ntca1|sl=",
        "ntca1|sl=" + STOP + "|sl=" + STOP,
        "ntca1|xx=" + STOP,
        "ntca1|sl" + STOP,
        "ntca1|sl=a b",
        "ntca1 |sl=" + STOP,
        "a private note",
        "NTCA1|sl=" + STOP,
        " ntca1",
        "ntca1 ",
        "ntca1\n",
        "ntca1:sl=" + STOP,
        "ntca1;sl=" + STOP,
        "ntca1||sl=" + STOP,
        "ntca1|sl=" + STOP + "|",
        "ntca1|SL=" + STOP,
        "ntca1|sl=" + STOP + "|tp=" + TARGET + "|tp=" + TARGET,
        "ntca1|sl=" + STOP + "|tp=" + TARGET + "|extra=1",
        "ntca1|sl=" + STOP + "\u00a0",
        "ntca1|tp=" + TARGET + "|sl=" + STOP,
        "ntca1|sl=" + "9" * rec.COMMENT_MAX,
    ],
)
def test_parse_comment_refuses_anything_malformed(comment: str) -> None:
    assert rec.parse_comment(comment) is None


@pytest.mark.parametrize("bad", ["", "a b", "a|b", "a\tb", "a\nb"])
def test_an_id_that_cannot_be_recorded_is_refused_when_encoding(bad: str) -> None:
    with pytest.raises(ValueError):
        rec.encode_label(bad)
    with pytest.raises(ValueError):
        rec.encode_comment(LegIds(stop_loss=bad, take_profit=None))


def test_a_record_too_long_for_its_field_is_refused() -> None:
    with pytest.raises(rec.RecordTooLong):
        rec.encode_label("O-" + "9" * rec.LABEL_MAX)
    with pytest.raises(rec.RecordTooLong):
        rec.encode_comment(LegIds(stop_loss="O-" + "9" * rec.COMMENT_MAX, take_profit=None))
    with pytest.raises(rec.RecordTooLong):
        rec.check_client_order_id("O-" + "9" * rec.CLIENT_ORDER_ID_MAX)


def test_a_client_order_id_that_fits_is_returned_unchanged() -> None:
    assert rec.check_client_order_id(ENTRY) == ENTRY
    # The three fields a real order needs all fit with room to spare.
    assert len(rec.encode_label(ENTRY)) <= rec.LABEL_MAX
    assert len(rec.encode_comment(LegIds(STOP, TARGET))) <= rec.COMMENT_MAX


def test_a_record_at_the_field_limit_still_round_trips() -> None:
    entry = "9" * (rec.LABEL_MAX - len("ntca1:"))
    assert rec.parse_label(rec.encode_label(entry)) == entry

    stop = "9" * (rec.COMMENT_MAX - len("ntca1|sl="))
    legs = LegIds(stop_loss=stop, take_profit=None)
    assert rec.parse_comment(rec.encode_comment(legs)) == legs
