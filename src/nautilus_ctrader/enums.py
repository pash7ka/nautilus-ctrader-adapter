"""`ProtoOATrendbarPeriod` and its mapping to a Nautilus `BarType`.

Only the periods cTrader's historical trendbar endpoint actually serves are covered: M1..M30,
H1, H4, H12, D1. W1 and MN1 exist in the protocol but are out of scope here.
"""

from __future__ import annotations

from nautilus_trader.model.data import BarType
from nautilus_trader.model.enums import AggregationSource, BarAggregation, PriceType

from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as om

_period = om.ProtoOATrendbarPeriod.Value

# ProtoOATrendbarPeriod value -> period length in seconds.
PERIOD_SECS: dict[int, int] = {
    _period("M1"): 60,
    _period("M2"): 120,
    _period("M3"): 180,
    _period("M4"): 240,
    _period("M5"): 300,
    _period("M10"): 600,
    _period("M15"): 900,
    _period("M30"): 1_800,
    _period("H1"): 3_600,
    _period("H4"): 14_400,
    _period("H12"): 43_200,
    _period("D1"): 86_400,
}

_MINUTE_STEPS: dict[int, int] = {
    1: _period("M1"),
    2: _period("M2"),
    3: _period("M3"),
    4: _period("M4"),
    5: _period("M5"),
    10: _period("M10"),
    15: _period("M15"),
    30: _period("M30"),
}
_HOUR_STEPS: dict[int, int] = {1: _period("H1"), 4: _period("H4"), 12: _period("H12")}
_DAY_STEPS: dict[int, int] = {1: _period("D1")}

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
