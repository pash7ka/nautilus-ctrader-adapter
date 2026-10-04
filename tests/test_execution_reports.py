"""Venue records turned into Nautilus values, reports, account state and account activity."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.enums import (
    AccountType,
    LiquiditySide,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
    TriggerType,
)
from nautilus_trader.model.identifiers import AccountId, PositionId, TradeId, VenueOrderId
from nautilus_trader.model.objects import Money, Price, Quantity

from nautilus_ctrader.activity import ACCOUNT_ACTIVITY_TOPIC, CTraderAccountActivity
from nautilus_ctrader.common import execution_reports as reports
from nautilus_ctrader.common import parsing
from nautilus_ctrader.common.venue_records import (
    Action,
    Activity,
    ActivityKind,
    ExternalOrder,
    ExternalType,
    Fill,
    price_of,
)
from tests.fixtures import load_recorded

REC = load_recorded()
ASSETS = {a.assetId: a for a in REC["assets"][0].asset}
LIGHT = {s.symbolName: s for s in REC["symbols"][0].symbol}
SPECS = {s.symbolId: s for s in REC["symbol_specs"][0].symbol}


def instrument(name: str):
    light = LIGHT[name]
    return parsing.instrument_from_symbol(SPECS[light.symbolId], light, ASSETS, {}, ts_init=0)


US100 = instrument("US100.cash")  # 2-decimal prices, quantities in hundredths
EURUSD = instrument("EURUSD")  # 5-decimal prices
ACCOUNT = AccountId("CTRADER-001")


def fill(**overrides) -> Fill:
    values = {
        "trade_id": "7000003",
        "venue_position_id": "5000001",
        "side": "SELL",
        "units": Decimal("0.99"),
        "price": Decimal("85205.58"),
        "commission": Decimal("-27.41"),
        "ts_ms": 1_600_000_406_510,
    }
    values.update(overrides)
    return Fill(**values)


def test_a_price_sent_as_a_double_reaches_nautilus_exactly_at_the_instruments_precision() -> None:
    assert reports.price(price_of(85197.2, 2), US100) == Price.from_str("85197.20")
    assert reports.price(price_of(85197.2, 5), EURUSD) == Price.from_str("85197.20000")
    assert reports.price(price_of(1.17, 5), EURUSD).precision == 5


def test_a_value_finer_than_the_instrument_is_refused_not_rounded() -> None:
    with pytest.raises(ValueError, match="does not fit"):
        reports.price(Decimal("85197.205"), US100)
    with pytest.raises(ValueError, match="does not fit"):
        reports.quantity(Decimal("0.995"), US100)


def test_a_quantity_takes_the_instruments_size_precision() -> None:
    assert reports.quantity(Decimal("1"), US100) == Quantity.from_str("1.00")
    assert reports.quantity(Decimal("0.99"), US100).precision == 2


def test_a_charged_commission_is_positive_in_the_deposit_currency() -> None:
    assert reports.commission(fill(), USD) == Money(Decimal("27.41"), USD)
    assert reports.commission(fill(commission=Decimal("1.50")), USD) == Money(Decimal("-1.50"), USD)


def test_an_external_order_is_reported_accepted_with_nothing_filled() -> None:
    record = ExternalOrder(
        venue_order_id="6000002",
        symbol_id=275,
        side="SELL",
        order_type=ExternalType.STOP_MARKET,
        units=Decimal("0.99"),
        reduce_only=True,
        venue_position_id="5000001",
        ts_ms=1_600_000_406_510,
        trigger_price=Decimal("85206.20"),
        fills=(fill(),),
        time_in_force="GOOD_TILL_CANCEL",
        ts_accepted_ms=1_600_000_124_775,
    )

    report = reports.order_status_report(record, US100, ACCOUNT, ts_init=1)

    assert report.account_id == ACCOUNT
    assert report.instrument_id == US100.id
    assert report.venue_order_id == VenueOrderId("6000002")
    assert report.client_order_id is None
    assert report.order_side == OrderSide.SELL
    assert report.order_type == OrderType.STOP_MARKET
    assert report.time_in_force == TimeInForce.GTC
    assert report.order_status == OrderStatus.ACCEPTED
    assert report.quantity == Quantity.from_str("0.99")
    assert report.filled_qty == Quantity.from_str("0.00")
    assert report.trigger_price == Price.from_str("85206.20")
    assert report.trigger_type == TriggerType.DEFAULT
    assert report.price is None
    assert report.reduce_only
    assert report.venue_position_id == PositionId("5000001")
    assert report.ts_accepted == 1_600_000_124_775_000_000
    assert report.ts_last == 1_600_000_406_510_000_000


def test_an_external_good_till_date_order_carries_its_expiry() -> None:
    record = ExternalOrder(
        venue_order_id="6000006",
        symbol_id=275,
        side="BUY",
        order_type=ExternalType.LIMIT,
        units=Decimal("1"),
        reduce_only=False,
        venue_position_id="5000003",
        ts_ms=1_600_000_694_986,
        price=Decimal("85100.00"),
        time_in_force="GOOD_TILL_DATE",
        expire_ts_ms=1_600_003_600_000,
    )

    report = reports.order_status_report(record, US100, ACCOUNT, ts_init=1)

    assert report.time_in_force == TimeInForce.GTD
    assert report.expire_time == datetime.fromtimestamp(1_600_003_600, tz=UTC)
    assert report.price == Price.from_str("85100.00")
    assert report.trigger_type == TriggerType.NO_TRIGGER
    assert not report.reduce_only
    assert report.ts_accepted == report.ts_last


def test_an_external_market_close_without_a_time_in_force_is_good_till_cancel() -> None:
    record = ExternalOrder(
        venue_order_id="6000003",
        symbol_id=275,
        side="SELL",
        order_type=ExternalType.MARKET,
        units=Decimal("0.01"),
        reduce_only=True,
        venue_position_id="5000001",
        ts_ms=1,
    )

    report = reports.order_status_report(record, US100, ACCOUNT, ts_init=1)

    assert report.order_type == OrderType.MARKET
    assert report.time_in_force == TimeInForce.GTC


def test_a_fill_report_carries_the_deal() -> None:
    report = reports.fill_report(fill(), "6000002", US100, ACCOUNT, USD, ts_init=1)

    assert report.venue_order_id == VenueOrderId("6000002")
    assert report.trade_id == TradeId("7000003")
    assert report.order_side == OrderSide.SELL
    assert report.last_qty == Quantity.from_str("0.99")
    assert report.last_px == Price.from_str("85205.58")
    assert report.commission == Money(Decimal("27.41"), USD)
    assert report.liquidity_side == LiquiditySide.NO_LIQUIDITY_SIDE
    assert report.venue_position_id == PositionId("5000001")
    assert report.ts_event == 1_600_000_406_510_000_000
    assert report.client_order_id is None


def test_account_state_sums_margins_per_instrument_and_pools_unloaded_symbols() -> None:
    state = reports.account_state(
        ACCOUNT,
        USD,
        Decimal("10000.00"),
        {US100.id: Decimal("50.00"), None: Decimal("7.25"), EURUSD.id: Decimal("12.50")},
        ts_event=5,
        ts_init=6,
    )

    assert state.account_type == AccountType.MARGIN
    assert state.base_currency == USD
    assert state.is_reported
    (balance,) = state.balances
    assert balance.total == Money(Decimal("10000.00"), USD)
    assert balance.locked == Money(0, USD)
    assert balance.free == balance.total
    margins = {m.instrument_id: (m.initial, m.maintenance) for m in state.margins}
    assert margins == {
        US100.id: (Money(Decimal("50.00"), USD), Money(Decimal("50.00"), USD)),
        EURUSD.id: (Money(Decimal("12.50"), USD), Money(Decimal("12.50"), USD)),
        None: (Money(Decimal("7.25"), USD), Money(Decimal("7.25"), USD)),
    }
    assert state.ts_event == 5
    assert state.ts_init == 6


def test_account_activity_carries_the_record_and_the_symbol_name() -> None:
    record = Activity(
        ActivityKind.STOP_OUT,
        275,
        "position",
        "BUY",
        Decimal("0.99"),
        Action.CLOSED,
        1_600_000_000_000,
    )

    activity = reports.account_activity(record, "US100.cash", ts_init=7)

    assert activity == CTraderAccountActivity(
        kind="stop_out",
        symbol="US100.cash",
        subject="position",
        side="BUY",
        volume=Decimal("0.99"),
        action="closed",
        ts_event=1_600_000_000_000_000_000,
        ts_init=7,
    )
    with pytest.raises(FrozenInstanceError):
        activity.kind = "manual_change"
    assert ACCOUNT_ACTIVITY_TOPIC == "ctrader.account_activity"
