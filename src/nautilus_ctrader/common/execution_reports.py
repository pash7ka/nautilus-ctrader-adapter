"""Venue records as Nautilus values, reports and account state.

Prices and quantities are built from their decimal text, never through a float, so a price the
venue sent as `85197.2` reaches Nautilus as exactly `85197.20`.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import replace
from datetime import UTC, datetime
from decimal import ROUND_HALF_EVEN, Decimal

from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.reports import (
    ExecutionMassStatus,
    FillReport,
    OrderStatusReport,
    PositionStatusReport,
)
from nautilus_trader.model.enums import (
    AccountType,
    ContingencyType,
    LiquiditySide,
    OrderSide,
    OrderStatus,
    OrderType,
    PositionSide,
    TimeInForce,
    TriggerType,
)
from nautilus_trader.model.events import AccountState
from nautilus_trader.model.identifiers import (
    AccountId,
    ClientId,
    ClientOrderId,
    InstrumentId,
    PositionId,
    TradeId,
    Venue,
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
from nautilus_trader.model.orders import Order

from nautilus_ctrader.activity import CTraderAccountActivity
from nautilus_ctrader.common.reconciliation import Reconciliation
from nautilus_ctrader.common.venue_records import (
    Activity,
    Contingency,
    Exposure,
    ExternalOrder,
    ExternalType,
    Fill,
    OrderEvent,
    OrderEventKind,
    ReportedOrder,
    ReportedPosition,
)

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
_CONTINGENCY = {Contingency.OTO: ContingencyType.OTO, Contingency.OUO: ContingencyType.OUO}


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


def _time_in_force(
    name: str | None, expire_ts_ms: int | None
) -> tuple[TimeInForce, datetime | None]:
    if name == "GOOD_TILL_DATE" and expire_ts_ms is not None:
        return TimeInForce.GTD, datetime.fromtimestamp(expire_ts_ms / 1000, tz=UTC)
    return _TIME_IN_FORCE.get(name or "", TimeInForce.GTC), None


def order_status_report(
    record: ExternalOrder,
    instrument: Instrument,
    account_id: AccountId,
    ts_init: int,
    client_order_id: str | None = None,
) -> OrderStatusReport:
    """An external order as it stood before any fill: accepted, none filled.

    Its fills follow as their own reports. A filled status alone would make Nautilus infer a fill
    of its own, without the commission, and refuse the real one. `client_order_id` is the id
    Nautilus gave the order, when it holds it already.
    """
    time_in_force, expire_time = _time_in_force(record.time_in_force, record.expire_ts_ms)
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
        client_order_id=None if client_order_id is None else ClientOrderId(client_order_id),
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
    client_order_id: str | None = None,
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
        client_order_id=None if client_order_id is None else ClientOrderId(client_order_id),
        venue_position_id=PositionId(fill.venue_position_id),
    )


_ENDED_STATUS = {
    OrderEventKind.CANCELED: OrderStatus.CANCELED,
    OrderEventKind.EXPIRED: OrderStatus.EXPIRED,
    OrderEventKind.REJECTED: OrderStatus.REJECTED,
}


def held_order_report(
    order: Order,
    record: OrderEvent,
    instrument: Instrument,
    account_id: AccountId,
    ts_init: int,
) -> OrderStatusReport:
    """An external order Nautilus holds, as `record` leaves it; not for a fill.

    What the record does not change is taken from `order`, its fills included: they reach
    Nautilus as their own reports, so a filled quantity above Nautilus's would make it infer one.
    """
    if record.kind == OrderEventKind.FILLED:
        raise ValueError("a fill is reported by `fill_report()`")
    filled = order.filled_qty
    if record.kind in _ENDED_STATUS:
        status = _ENDED_STATUS[record.kind]
    else:
        status = OrderStatus.PARTIALLY_FILLED if filled.as_decimal() > 0 else OrderStatus.ACCEPTED
    held_price = getattr(order, "price", None)
    held_trigger = getattr(order, "trigger_price", None)
    return OrderStatusReport(
        account_id=account_id,
        instrument_id=instrument.id,
        client_order_id=order.client_order_id,
        venue_order_id=order.venue_order_id,
        order_side=order.side,
        order_type=order.order_type,
        time_in_force=order.time_in_force,
        order_status=status,
        quantity=order.quantity
        if record.quantity is None
        else quantity(record.quantity, instrument),
        filled_qty=filled,
        report_id=UUID4(),
        ts_accepted=order.ts_accepted,
        ts_last=nanos(record.ts_ms),
        ts_init=ts_init,
        expire_time=getattr(order, "expire_time", None),
        price=held_price if record.price is None else price(record.price, instrument),
        trigger_price=held_trigger
        if record.trigger_price is None
        else price(record.trigger_price, instrument),
        trigger_type=getattr(order, "trigger_type", TriggerType.NO_TRIGGER),
        avg_px=None if filled.as_decimal() == 0 else Decimal(str(order.avg_px)),
        cancel_reason=record.reason,
        reduce_only=order.is_reduce_only,
    )


def changes_terms(order: Order, report: OrderStatusReport) -> bool:
    """Whether `report` gives `order` another quantity, price or trigger price, its fills alike.

    A report with other fills is not one: sent alone, Nautilus would infer a fill or refuse it.
    """
    if report.filled_qty != order.filled_qty:
        return False
    if report.quantity != order.quantity:
        return True
    if order.has_price and report.price is not None and report.price != order.price:
        return True
    return (
        order.has_trigger_price
        and report.trigger_price is not None
        and report.trigger_price != order.trigger_price
    )


def reported_order(
    record: ReportedOrder,
    instrument: Instrument,
    account_id: AccountId,
    currency: Currency,
    ts_init: int,
) -> tuple[OrderStatusReport, list[FillReport]]:
    """A reconciliation record as Nautilus's order report, with its fills under the same ids.

    Raises `ValueError` if a price or quantity does not fit the instrument.
    """
    time_in_force, expire_time = _time_in_force(record.time_in_force, record.expire_ts_ms)
    linked = [ClientOrderId(order_id) for order_id in record.linked_order_ids]
    # Nautilus refuses a contingency with nothing linked, as on a bracket's only leg; the
    # parent link alone still ties that leg to its entry.
    contingency = (
        _CONTINGENCY[record.contingency]
        if record.contingency is not None and linked
        else ContingencyType.NO_CONTINGENCY
    )
    triggered = record.order_type in (ExternalType.STOP_MARKET, ExternalType.STOP_LIMIT)
    report = OrderStatusReport(
        account_id=account_id,
        instrument_id=instrument.id,
        venue_order_id=VenueOrderId(record.venue_order_id),
        order_side=order_side(record.side),
        order_type=_ORDER_TYPE[record.order_type],
        time_in_force=time_in_force,
        order_status=OrderStatus[record.status.name],
        quantity=quantity(record.units, instrument),
        filled_qty=quantity(record.filled_units, instrument),
        report_id=UUID4(),
        ts_accepted=nanos(record.ts_accepted_ms),
        ts_last=nanos(record.ts_ms),
        ts_init=ts_init,
        client_order_id=(
            None if record.client_order_id is None else ClientOrderId(record.client_order_id)
        ),
        venue_position_id=(
            None if record.venue_position_id is None else PositionId(record.venue_position_id)
        ),
        linked_order_ids=linked or None,
        parent_order_id=(
            None if record.parent_order_id is None else ClientOrderId(record.parent_order_id)
        ),
        contingency_type=contingency,
        expire_time=expire_time,
        price=None if record.price is None else price(record.price, instrument),
        trigger_price=(
            None if record.trigger_price is None else price(record.trigger_price, instrument)
        ),
        trigger_type=TriggerType.DEFAULT if triggered else TriggerType.NO_TRIGGER,
        avg_px=record.avg_price if record.fills else None,
        reduce_only=record.reduce_only,
    )
    fills = [
        fill_report(
            fill,
            record.venue_order_id,
            instrument,
            account_id,
            currency,
            ts_init,
            client_order_id=record.client_order_id,
        )
        for fill in record.fills
    ]
    return report, fills


def position_report(
    record: ReportedPosition,
    instrument: Instrument,
    account_id: AccountId,
    ts_init: int,
) -> PositionStatusReport:
    return PositionStatusReport(
        account_id=account_id,
        instrument_id=instrument.id,
        position_side=PositionSide.LONG if record.side == "BUY" else PositionSide.SHORT,
        quantity=quantity(record.units, instrument),
        report_id=UUID4(),
        ts_last=nanos(record.ts_ms),
        ts_init=ts_init,
        venue_position_id=PositionId(record.venue_position_id),
        avg_px_open=record.avg_price,
    )


def mass_status(
    client_id: ClientId,
    account_id: AccountId,
    venue: Venue,
    reconciliation: Reconciliation,
    instrument_for: Callable[[int], Instrument | None],
    currency: Currency,
    ts_init: int,
    *,
    held_price: Callable[[str], Decimal | None],
    known_id: Callable[[str], str | None] = lambda _venue_order_id: None,
) -> tuple[ExecutionMassStatus, tuple[ReportedOrder, ...]]:
    """The reconciliation records as one mass status, orders in record order.

    - `instrument_for`: a symbol's instrument, `None` for one not loaded; its records are left
      out silently.
    - `held_price`: the price Nautilus holds for a leg, by client order id: the trigger price of
      a stop, the limit price otherwise, `None` for an order it does not hold.
    - `known_id`: the client order id Nautilus gave an external order, by venue order id, `None`
      for one it does not hold; a leg that is not the node's finds its held price through it.
      The report itself names no client order id for such an order: Nautilus finds it by its
      venue order id, and drops a report naming a cached order whose status it already holds,
      a changed price or quantity with it.

    A leg whose level the broker no longer lists has no price of its own. It is reported at
    the held price: no price change was seen, and Nautilus would otherwise emit an update
    without a price. Failing that, a leg with fills is reported at their average price; one
    without is left out. What is left out is unlinked from the orders kept, since Nautilus
    fails on a linked order it cannot find.

    Returns the mass status and the records left out for want of a price.
    """
    status = ExecutionMassStatus(client_id, account_id, venue, UUID4(), ts_init)
    kept: list[tuple[ReportedOrder, Instrument]] = []
    left_out: list[ReportedOrder] = []
    for record in reconciliation.orders:
        instrument = instrument_for(record.symbol_id)
        if instrument is None:
            continue
        priced = _priced(record, held_price, known_id, instrument.price_precision)
        if priced is None:
            left_out.append(record)
        else:
            kept.append((priced, instrument))
    gone = {record.client_order_id for record in left_out}
    order_reports: list[OrderStatusReport] = []
    fill_reports: list[FillReport] = []
    for record, instrument in kept:
        if gone.intersection(record.linked_order_ids):
            linked = tuple(i for i in record.linked_order_ids if i not in gone)
            record = replace(record, linked_order_ids=linked)
        report, fills = reported_order(record, instrument, account_id, currency, ts_init)
        order_reports.append(report)
        fill_reports += fills
    position_reports = [
        position_report(record, instrument, account_id, ts_init)
        for record in reconciliation.positions
        if (instrument := instrument_for(record.symbol_id)) is not None
    ]
    status.add_order_reports(order_reports)
    status.add_fill_reports(fill_reports)
    status.add_position_reports(position_reports)
    return status, tuple(left_out)


def _priced(
    record: ReportedOrder,
    held_price: Callable[[str], Decimal | None],
    known_id: Callable[[str], str | None],
    precision: int,
) -> ReportedOrder | None:
    """`record` with every price its type needs, or `None` if one is missing and unknown."""
    if record.order_type == ExternalType.LIMIT and record.price is None:
        field = "price"
    elif record.order_type == ExternalType.STOP_MARKET and record.trigger_price is None:
        field = "trigger_price"
    elif record.order_type == ExternalType.STOP_LIMIT and None in (
        record.price,
        record.trigger_price,
    ):
        return None
    else:
        return record
    client_order_id = record.client_order_id or known_id(record.venue_order_id)
    held = None if client_order_id is None else held_price(client_order_id)
    if held is None and record.fills:
        units = sum((fill.units for fill in record.fills), Decimal(0))
        total = sum((fill.price * fill.units for fill in record.fills), Decimal(0))
        held = (total / units).quantize(Decimal(1).scaleb(-precision), rounding=ROUND_HALF_EVEN)
    return None if held is None else replace(record, **{field: held})


def exposure_json(items: Sequence[Exposure], symbol_name: Callable[[int], str]) -> bytes:
    """`items` as UTF-8 JSON, sorted by symbol name, then subject, side and volume.

    Each item is `{"symbol", "subject", "side", "volume"}`, the volume as plain decimal text.
    """
    named = sorted(
        (symbol_name(item.symbol_id), item.subject, item.side, item.units) for item in items
    )
    return json.dumps(
        [
            {"symbol": symbol, "subject": subject, "side": side, "volume": format(units, "f")}
            for symbol, subject, side, units in named
        ]
    ).encode("utf-8")


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
