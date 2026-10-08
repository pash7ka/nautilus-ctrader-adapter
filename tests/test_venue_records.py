"""The venue model's conversions: what the broker sends, as exact decimals."""

from __future__ import annotations

import math
from decimal import Decimal

import pytest

from nautilus_ctrader.common.venue_records import (
    ExternalOrder,
    ExternalType,
    Level,
    leg_venue_order_id,
    money_of,
    parse_leg_venue_order_id,
    price_of,
    units_of,
)


@pytest.mark.parametrize(
    ("value", "precision", "expected"),
    [
        (85197.2, 2, "85197.20"),
        (85179.3, 2, "85179.30"),
        (1.1005, 5, "1.10050"),
        (0.1 + 0.2, 1, "0.3"),
        (152.123, 3, "152.123"),
        (1e-05, 5, "0.00001"),
        (1e-05, 8, "0.00001000"),
        (1e16, 0, "10000000000000000"),
        (1e16, 2, "10000000000000000.00"),
        (85197.0, 0, "85197"),
        (-85197.2, 2, "-85197.20"),
        (-0.0, 2, "0.00"),
        (0.125, 2, "0.12"),
        (0.135, 2, "0.14"),
    ],
)
def test_a_price_is_its_shortest_decimal_at_the_instruments_precision(
    value, precision, expected
) -> None:
    assert str(price_of(value, precision)) == expected


@pytest.mark.parametrize("value", [math.nan, math.inf, -math.inf])
def test_a_non_finite_price_is_refused(value) -> None:
    with pytest.raises(ValueError, match="finite"):
        price_of(value, 2)


def test_a_price_too_large_for_its_precision_is_refused_as_a_value_error() -> None:
    with pytest.raises(ValueError, match="does not fit"):
        price_of(1e30, 5)


@pytest.mark.parametrize(
    ("volume", "expected"), [(100, Decimal("1")), (99, Decimal("0.99")), (1, Decimal("0.01"))]
)
def test_a_volume_in_hundredths_is_units(volume, expected) -> None:
    assert units_of(volume) == expected


@pytest.mark.parametrize(
    ("amount", "digits", "expected"),
    [(-2772, 2, "-27.72"), (-28, 2, "-0.28"), (10053099944, 8, "100.53099944"), (0, 2, "0")],
)
def test_money_is_scaled_by_its_own_digits(amount, digits, expected) -> None:
    assert money_of(amount, digits) == Decimal(expected)


def test_a_legs_venue_order_id_is_named_after_its_entry() -> None:
    assert leg_venue_order_id(6000001, Level.STOP_LOSS) == "6000001-SL"
    assert leg_venue_order_id(6000001, Level.TAKE_PROFIT) == "6000001-TP"


@pytest.mark.parametrize(
    ("entry", "level", "generation", "text"),
    [
        (6000001, Level.STOP_LOSS, 1, "6000001-SL"),
        (6000001, Level.TAKE_PROFIT, 1, "6000001-TP"),
        (6000001, Level.STOP_LOSS, 2, "6000001-SL-2"),
        (6000001, Level.TAKE_PROFIT, 10, "6000001-TP-10"),
    ],
)
def test_a_legs_generation_is_written_from_the_second_and_read_back(
    entry, level, generation, text
) -> None:
    assert leg_venue_order_id(entry, level, generation) == text
    assert parse_leg_venue_order_id(text) == (entry, level, generation)


def test_a_legs_generation_starts_at_one() -> None:
    with pytest.raises(ValueError, match="starts at 1"):
        leg_venue_order_id(6000001, Level.STOP_LOSS, 0)


@pytest.mark.parametrize(
    "text",
    [
        "6000001",
        "6000001-SL-1",
        "6000001-SL-02",
        "6000001-SL-",
        "6000001-XX",
        "06000001-SL",
        "-SL",
        "6000001-sl",
        "6000001-SL-2-3",
        " 6000001-SL",
        "6000001-SL\n",
        # A non-ASCII digit.
        "٦000001-SL",
        "O-SL-5000001",
        "",
    ],
)
def test_anything_but_a_legs_venue_order_id_is_not_read_as_one(text) -> None:
    assert parse_leg_venue_order_id(text) is None


def test_a_legs_venue_order_id_is_never_read_as_a_spread_leg() -> None:
    # Nautilus treats a fill whose venue order id contains this as a spread leg's.
    for level in Level:
        for generation in (1, 2):
            assert "-LEG-" not in leg_venue_order_id(6000001, level, generation)


def test_an_external_orders_added_fields_come_last_and_default_to_none() -> None:
    # Built positionally from the fields it had first, as existing callers do.
    order = ExternalOrder("1", 2, "BUY", ExternalType.MARKET, Decimal("1"), False, None, 3)

    assert (order.time_in_force, order.expire_ts_ms, order.ts_accepted_ms) == (None, None, None)
