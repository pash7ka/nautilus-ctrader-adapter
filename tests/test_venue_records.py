"""The venue model's conversions: what the broker sends, as exact decimals."""

from __future__ import annotations

from decimal import Decimal

import pytest

from nautilus_ctrader.common.venue_records import (
    Level,
    leg_venue_order_id,
    money_of,
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
    ],
)
def test_a_price_is_its_shortest_decimal_at_the_instruments_precision(
    value, precision, expected
) -> None:
    assert str(price_of(value, precision)) == expected


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


def test_a_legs_venue_order_id_is_never_read_as_a_spread_leg() -> None:
    # Nautilus treats a fill whose venue order id contains this as a spread leg's.
    for level in Level:
        assert "-LEG-" not in leg_venue_order_id(6000001, level)
