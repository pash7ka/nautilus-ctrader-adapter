"""`ProtoOATrendbarPeriod` and its mapping to a Nautilus `BarType`.

Only the periods cTrader's historical trendbar endpoint actually serves are covered: M1..M30,
H1, H4, H12, D1. W1 and MN1 exist in the protocol but are out of scope here.
"""

from __future__ import annotations

from nautilus_trader.model.data import BarType
from nautilus_trader.model.enums import AggregationSource, BarAggregation, PriceType

# ProtoOATrendbarPeriod value -> period length in seconds.
PERIOD_SECS: dict[int, int] = {
    1: 60,  # M1
    2: 120,  # M2
    3: 180,  # M3
    4: 240,  # M4
    5: 300,  # M5
    6: 600,  # M10
    7: 900,  # M15
    8: 1_800,  # M30
    9: 3_600,  # H1
    10: 14_400,  # H4
    11: 43_200,  # H12
    12: 86_400,  # D1
}

_MINUTE_STEPS: dict[int, int] = {1: 1, 2: 2, 3: 3, 4: 4, 5: 5, 10: 6, 15: 7, 30: 8}
_HOUR_STEPS: dict[int, int] = {1: 9, 4: 10, 12: 11}
_DAY_STEPS: dict[int, int] = {1: 12}

_STEPS_BY_AGGREGATION: dict[BarAggregation, dict[int, int]] = {
    BarAggregation.MINUTE: _MINUTE_STEPS,
    BarAggregation.HOUR: _HOUR_STEPS,
    BarAggregation.DAY: _DAY_STEPS,
}


def trendbar_period_for(bar_type: BarType) -> int:
    """ProtoOATrendbarPeriod for a bar type.

    Raises `ValueError` unless aggregation source is EXTERNAL, price type is BID, and
    step/aggregation is one of 1,2,3,4,5,10,15,30-MINUTE, 1,4,12-HOUR, 1-DAY.
    """
    spec = bar_type.spec
    if spec.price_type != PriceType.BID:
        raise ValueError(f"{bar_type}: trendbars are only available for the BID price type")
    if bar_type.aggregation_source != AggregationSource.EXTERNAL:
        raise ValueError(f"{bar_type}: trendbars are always EXTERNAL aggregation")
    steps = _STEPS_BY_AGGREGATION.get(spec.aggregation)
    period = steps.get(spec.step) if steps is not None else None
    if period is None:
        raise ValueError(f"{bar_type}: step/aggregation combination is not supported")
    return period
