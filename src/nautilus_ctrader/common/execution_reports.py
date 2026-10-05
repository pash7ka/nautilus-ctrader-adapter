"""Venue records as Nautilus values, reports and account state.

Prices and quantities are built from their decimal text, never through a float, so a price the
venue sent as `85197.2` reaches Nautilus as exactly `85197.20`.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from decimal import Decimal

from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.reports import FillReport, OrderStatusReport
from nautilus_trader.model.enums import (
    AccountType,
    LiquiditySide,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
    TriggerType,
)
from nautilus_trader.model.events import AccountState
from nautilus_trader.model.identifiers import (
    AccountId,
    InstrumentId,
    PositionId,
    TradeId,
    VenueOrderId,
)
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.model.objects import (
    AccountBalance,
    Currency,
    MarginBalance,
    Money,
    Price,
    Quantity,
)

from nautilus_ctrader.activity import CTraderAccountActivity
from nautilus_ctrader.common.venue_records import Activity, ExternalOrder, ExternalType, Fill

_NANOS_PER_MILLI = 1_000_000

_ORDER_TYPE = {
    ExternalType.MARKET: OrderType.MARKET,
    ExternalType.LIMIT: OrderType.LIMIT,
    ExternalType.STOP_MARKET: OrderType.STOP_MARKET,
    ExternalType.STOP_LIMIT: OrderType.STOP_LIMIT,
}
# A good-till-date order is told by its expiry, below.
_TIME_IN_FORCE = {
    "GOOD_TILL_CANCEL": TimeInForce.GTC,
    "IMMEDIATE_OR_CANCEL": TimeInForce.IOC,
    "FILL_OR_KILL": TimeInForce.FOK,
    "MARKET_ON_OPEN": TimeInForce.AT_THE_OPEN,
}


def nanos(ms: int) -> int:
    return ms * _NANOS_PER_MILLI


def order_side(side: str) -> OrderSide:
    return OrderSide.BUY if side == "BUY" else OrderSide.SELL


def _text_at(value: Decimal, precision: int, what: str) -> str:
    exact = value.quantize(Decimal(1).scaleb(-precision))
    if exact != value:
        raise ValueError(f"{what} {value} does not fit {precision} decimals")
    return format(exact, "f")


def price(value: Decimal, instrument: Instrument) -> Price:
    """`value` at the instrument's price precision; raises `ValueError` if it does not fit."""
    return Price.from_str(_text_at(value, instrument.price_precision, "price"))


def quantity(units: Decimal, instrument: Instrument) -> Quantity:
    """`units` at the instrument's size precision; raises `ValueError` if they do not fit."""
    return Quantity.from_str(_text_at(units, instrument.size_precision, "quantity"))


def commission(fill: Fill, currency: Currency) -> Money:
    """The deal's commission as Nautilus counts it, a charge positive.

    Rounded to the currency's precision where the venue reports finer amounts.
    """
    return Money(-fill.commission, currency)


def order_status_report(
    record: ExternalOrder,
    instrument: Instrument,
    account_id: AccountId,
    ts_init: int,
) -> OrderStatusReport:
    """An order Nautilus does not know yet, as it stood before any fill: accepted, none filled.

    Its fills follow as their own reports. A filled status alone would make Nautilus infer a fill
    of its own, without the commission, and refuse the real one.
    """
    time_in_force = _TIME_IN_FORCE.get(record.time_in_force or "", TimeInForce.GTC)
    expire_time = None
    if record.time_in_force == "GOOD_TILL_DATE" and record.expire_ts_ms is not None:
        time_in_force = TimeInForce.GTD
        expire_time = datetime.fromtimestamp(record.expire_ts_ms / 1000, tz=UTC)
    accepted_ms = record.ts_ms if record.ts_accepted_ms is None else record.ts_accepted_ms
    return OrderStatusReport(
        account_id=account_id,
        instrument_id=instrument.id,
        venue_order_id=VenueOrderId(record.venue_order_id),
        order_side=order_side(record.side),
        order_type=_ORDER_TYPE[record.order_type],
        time_in_force=time_in_force,
        order_status=OrderStatus.ACCEPTED,
        quantity=quantity(record.units, instrument),
        filled_qty=Quantity.zero(instrument.size_precision),
        report_id=UUID4(),
        ts_accepted=nanos(accepted_ms),
        ts_last=nanos(record.ts_ms),
        ts_init=ts_init,
        venue_position_id=(
            None if record.venue_position_id is None else PositionId(record.venue_position_id)
        ),
        expire_time=expire_time,
        price=None if record.price is None else price(record.price, instrument),
        trigger_price=(
            None if record.trigger_price is None else price(record.trigger_price, instrument)
        ),
        trigger_type=TriggerType.NO_TRIGGER
        if record.trigger_price is None
        else TriggerType.DEFAULT,
        # Never left at its default: Nautilus would take a close for an opening order.
        reduce_only=record.reduce_only,
    )


def fill_report(
    fill: Fill,
    venue_order_id: str,
    instrument: Instrument,
    account_id: AccountId,
    currency: Currency,
    ts_init: int,
) -> FillReport:
    return FillReport(
        account_id=account_id,
        instrument_id=instrument.id,
        venue_order_id=VenueOrderId(venue_order_id),
        trade_id=TradeId(fill.trade_id),
        order_side=order_side(fill.side),
        last_qty=quantity(fill.units, instrument),
        last_px=price(fill.price, instrument),
        commission=commission(fill, currency),
        # The venue does not say which side of the book a deal took.
        liquidity_side=LiquiditySide.NO_LIQUIDITY_SIDE,
        report_id=UUID4(),
        ts_event=nanos(fill.ts_ms),
        ts_init=ts_init,
        venue_position_id=PositionId(fill.venue_position_id),
    )


def account_activity(record: Activity, symbol: str, ts_init: int) -> CTraderAccountActivity:
    return CTraderAccountActivity(
        kind=record.kind.value,
        symbol=symbol,
        subject=record.subject,
        side=record.side,
        volume=record.units,
        action=record.action.value,
        ts_event=nanos(record.ts_ms),
        ts_init=ts_init,
    )


def account_state(
    account_id: AccountId,
    currency: Currency,
    balance: Decimal,
    margins: Mapping[InstrumentId | None, Decimal],
    ts_event: int,
    ts_init: int,
) -> AccountState:
    """The account as the broker states it: its balance, and the margin its positions use.

    `margins` holds the margin per instrument, and under `None` that of positions on instruments
    the node has not loaded. Nothing is locked: the venue reports no such figure.
    """
    total = Money(balance, currency)
    ordered = sorted(margins.items(), key=lambda item: (item[0] is None, str(item[0])))
    return AccountState(
        account_id=account_id,
        account_type=AccountType.MARGIN,
        base_currency=currency,
        reported=True,
        balances=[AccountBalance(total=total, locked=Money(0, currency), free=total)],
        # The venue gives one figure per position; Nautilus replaces its whole set each time.
        margins=[
            MarginBalance(
                initial=Money(margin, currency),
                maintenance=Money(margin, currency),
                instrument_id=instrument_id,
            )
            for instrument_id, margin in ordered
        ],
        info={},
        event_id=UUID4(),
        ts_event=ts_event,
        ts_init=ts_init,
    )
