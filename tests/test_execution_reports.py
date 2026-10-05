"""Venue records turned into Nautilus values, reports, account state and account activity."""

from __future__ import annotations

import json
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from nautilus_trader.model.currencies import USD
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
from nautilus_trader.model.identifiers import (
    AccountId,
    ClientId,
    ClientOrderId,
    PositionId,
    TradeId,
    VenueOrderId,
)
from nautilus_trader.model.objects import Money, Price, Quantity

from nautilus_ctrader.activity import ACCOUNT_ACTIVITY_TOPIC, CTraderAccountActivity
from nautilus_ctrader.common import execution_reports as reports
from nautilus_ctrader.common import parsing
from nautilus_ctrader.common.reconciliation import Reconciliation
from nautilus_ctrader.common.venue_records import (
    Action,
    Activity,
    ActivityKind,
    Contingency,
    Exposure,
    ExternalOrder,
    ExternalType,
    Fill,
    ReportedOrder,
    ReportedPosition,
    ReportStatus,
    price_of,
)
from nautilus_ctrader.constants import CTRADER_VENUE
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


# Reconciliation records.

ENTRY, STOP, TARGET = "O-E-5000001", "O-SL-5000001", "O-TP-5000001"


def leg(**overrides) -> ReportedOrder:
    values = {
        "venue_order_id": "6000001-SL",
        "client_order_id": STOP,
        "symbol_id": 275,
        "side": "SELL",
        "order_type": ExternalType.STOP_MARKET,
        "status": ReportStatus.ACCEPTED,
        "units": Decimal("0.99"),
        "filled_units": Decimal(0),
        "reduce_only": True,
        "venue_position_id": "5000001",
        "ts_accepted_ms": 1_600_000_124_775,
        "ts_ms": 1_600_000_358_000,
        "trigger_price": Decimal("85200.20"),
        "time_in_force": "GOOD_TILL_CANCEL",
        "parent_order_id": ENTRY,
        "linked_order_ids": (TARGET,),
        "contingency": Contingency.OUO,
    }
    values.update(overrides)
    return ReportedOrder(**values)


def target_leg(**overrides) -> ReportedOrder:
    values = {
        "venue_order_id": "6000001-TP",
        "client_order_id": TARGET,
        "order_type": ExternalType.LIMIT,
        "trigger_price": None,
        "price": Decimal("85353.42"),
        "linked_order_ids": (STOP,),
    }
    values.update(overrides)
    return leg(**values)


def entry(**overrides) -> ReportedOrder:
    values = {
        "venue_order_id": "6000001",
        "client_order_id": ENTRY,
        "symbol_id": 275,
        "side": "BUY",
        "order_type": ExternalType.MARKET,
        "status": ReportStatus.FILLED,
        "units": Decimal("1.00"),
        "filled_units": Decimal("1.00"),
        "reduce_only": False,
        "venue_position_id": "5000001",
        "ts_accepted_ms": 1_600_000_100_000,
        "ts_ms": 1_600_000_100_500,
        "avg_price": Decimal("85287.21"),
        "time_in_force": "IMMEDIATE_OR_CANCEL",
        "linked_order_ids": (STOP, TARGET),
        "contingency": Contingency.OTO,
        "fills": (
            fill(trade_id="7000001", side="BUY", units=Decimal("0.40"), ts_ms=1_600_000_100_400),
            fill(trade_id="7000009", side="BUY", units=Decimal("0.60"), ts_ms=1_600_000_100_500),
        ),
    }
    values.update(overrides)
    return ReportedOrder(**values)


def test_reported_leg_carries_its_links() -> None:
    report, fills = reports.reported_order(leg(), US100, ACCOUNT, USD, ts_init=1)

    assert report.client_order_id == ClientOrderId(STOP)
    assert report.venue_order_id == VenueOrderId("6000001-SL")
    assert report.parent_order_id == ClientOrderId(ENTRY)
    assert report.contingency_type == ContingencyType.OUO
    assert report.linked_order_ids == [ClientOrderId(TARGET)]
    assert report.order_type == OrderType.STOP_MARKET
    assert report.trigger_price == Price.from_str("85200.20")
    assert report.trigger_type == TriggerType.DEFAULT
    assert report.price is None
    assert report.order_status == OrderStatus.ACCEPTED
    assert report.quantity == Quantity.from_str("0.99")
    assert report.filled_qty == Quantity.from_str("0.00")
    assert report.avg_px is None
    assert report.reduce_only
    assert report.time_in_force == TimeInForce.GTC
    assert report.venue_position_id == PositionId("5000001")
    assert report.ts_accepted == 1_600_000_124_775_000_000
    assert report.ts_last == 1_600_000_358_000_000_000
    assert fills == []

    target, _ = reports.reported_order(target_leg(), US100, ACCOUNT, USD, ts_init=1)
    assert target.order_type == OrderType.LIMIT
    assert target.price == Price.from_str("85353.42")
    assert target.trigger_type == TriggerType.NO_TRIGGER


def test_a_leg_of_a_one_leg_bracket_has_no_contingency() -> None:
    # Nautilus refuses OUO without a linked order; the parent link is what ties it to its entry.
    report, _ = reports.reported_order(leg(linked_order_ids=()), US100, ACCOUNT, USD, ts_init=1)

    assert report.contingency_type == ContingencyType.NO_CONTINGENCY
    assert report.linked_order_ids is None
    assert report.parent_order_id == ClientOrderId(ENTRY)


def test_reported_entry_has_avg_px_and_fills_under_its_id() -> None:
    report, fills = reports.reported_order(entry(), US100, ACCOUNT, USD, ts_init=1)

    assert report.order_status == OrderStatus.FILLED
    assert report.filled_qty == Quantity.from_str("1.00")
    assert report.avg_px == Decimal("85287.21")
    assert report.contingency_type == ContingencyType.OTO
    assert report.linked_order_ids == [ClientOrderId(STOP), ClientOrderId(TARGET)]
    assert report.parent_order_id is None
    assert report.time_in_force == TimeInForce.IOC
    assert not report.reduce_only
    assert [f.trade_id for f in fills] == [TradeId("7000001"), TradeId("7000009")]
    assert all(f.client_order_id == ClientOrderId(ENTRY) for f in fills)
    assert all(f.venue_order_id == VenueOrderId("6000001") for f in fills)
    assert fills[0].last_qty == Quantity.from_str("0.40")
    assert fills[0].commission == Money(Decimal("27.41"), USD)


def test_an_order_not_the_nodes_carries_no_client_id_nor_links() -> None:
    close = entry(
        venue_order_id="6000003",
        client_order_id=None,
        side="SELL",
        units=Decimal("0.01"),
        filled_units=Decimal("0.01"),
        reduce_only=True,
        avg_price=Decimal("85300.00"),
        linked_order_ids=(),
        contingency=None,
        time_in_force=None,
        fills=(fill(trade_id="7000002", units=Decimal("0.01"), price=Decimal("85300.00")),),
    )

    report, fills = reports.reported_order(close, US100, ACCOUNT, USD, ts_init=1)

    assert report.client_order_id is None
    assert report.contingency_type == ContingencyType.NO_CONTINGENCY
    assert report.linked_order_ids is None
    assert report.reduce_only
    assert report.time_in_force == TimeInForce.GTC
    assert [f.client_order_id for f in fills] == [None]


def test_position_report_carries_side_quantity_and_id() -> None:
    record = ReportedPosition("5000001", 275, "SELL", Decimal("0.99"), Decimal("85287.21"), 5)

    report = reports.position_report(record, US100, ACCOUNT, ts_init=6)

    assert report.instrument_id == US100.id
    assert report.position_side == PositionSide.SHORT
    assert report.quantity == Quantity.from_str("0.99")
    assert report.venue_position_id == PositionId("5000001")
    assert report.avg_px_open == Decimal("85287.21")
    assert report.ts_last == 5_000_000
    assert report.ts_init == 6


def build(reconciliation: Reconciliation, held=lambda client_order_id: None):
    return reports.mass_status(
        ClientId("CTRADER"),
        ACCOUNT,
        CTRADER_VENUE,
        reconciliation,
        lambda symbol_id: US100 if symbol_id == 275 else None,
        USD,
        ts_init=1,
        held_price=held,
    )


def test_mass_status_keeps_record_order() -> None:
    close = entry(
        venue_order_id="6000003",
        client_order_id=None,
        side="SELL",
        reduce_only=True,
        linked_order_ids=(),
        contingency=None,
        fills=(fill(trade_id="7000002", units=Decimal("1.00")),),
    )
    unloaded = leg(venue_order_id="6000099", client_order_id=None, symbol_id=999)
    position = ReportedPosition("5000001", 275, "BUY", Decimal("1.00"), Decimal("85287.21"), 5)
    records = (close, entry(), unloaded, target_leg(), leg())

    status, left_out = build(Reconciliation(records, (position, position), ()))

    assert list(status.order_reports) == [
        VenueOrderId("6000003"),
        VenueOrderId("6000001"),
        VenueOrderId("6000001-TP"),
        VenueOrderId("6000001-SL"),
    ]
    assert list(status.fill_reports) == [VenueOrderId("6000003"), VenueOrderId("6000001")]
    assert len(status.fill_reports[VenueOrderId("6000001")]) == 2
    assert [r.venue_position_id for r in status.position_reports[US100.id]] == [
        PositionId("5000001"),
        PositionId("5000001"),
    ]
    assert status.client_id == ClientId("CTRADER")
    assert status.account_id == ACCOUNT
    assert status.venue == CTRADER_VENUE
    assert left_out == ()


def test_a_priceless_leg_takes_the_price_nautilus_holds_or_is_left_out() -> None:
    # A leg cancelled while its position stays open: the broker no longer lists its level.
    stop = leg(status=ReportStatus.CANCELED, trigger_price=None)
    target = target_leg(status=ReportStatus.CANCELED, price=None)
    records = Reconciliation((entry(), stop, target), (), ())
    held = {STOP: Decimal("85197.20"), TARGET: Decimal("85387.22")}

    priced, none_left = build(records, held.get)
    unpriced, left_out = build(records)

    assert none_left == ()
    by_venue_id = priced.order_reports
    assert by_venue_id[VenueOrderId("6000001-SL")].trigger_price == Price.from_str("85197.20")
    assert by_venue_id[VenueOrderId("6000001-SL")].order_status == OrderStatus.CANCELED
    assert by_venue_id[VenueOrderId("6000001-TP")].price == Price.from_str("85387.22")
    assert left_out == (stop, target)
    assert list(unpriced.order_reports) == [VenueOrderId("6000001")]


def test_exposure_json_is_sorted_and_plain() -> None:
    names = {275: "US100.cash", 1: "EURUSD"}
    items = [
        Exposure(275, "position", "BUY", Decimal("2")),
        Exposure(1, "position", "SELL", Decimal("0.01")),
        Exposure(275, "order", "SELL", Decimal("1.5")),
        Exposure(275, "position", "BUY", Decimal("0.10")),
        Exposure(1, "order", "BUY", Decimal("1E-2")),
    ]

    encoded = reports.exposure_json(items, names.__getitem__)

    assert reports.exposure_json([], names.__getitem__) == b"[]"
    assert json.loads(encoded) == [
        {"symbol": "EURUSD", "subject": "order", "side": "BUY", "volume": "0.01"},
        {"symbol": "EURUSD", "subject": "position", "side": "SELL", "volume": "0.01"},
        {"symbol": "US100.cash", "subject": "order", "side": "SELL", "volume": "1.5"},
        {"symbol": "US100.cash", "subject": "position", "side": "BUY", "volume": "0.10"},
        {"symbol": "US100.cash", "subject": "position", "side": "BUY", "volume": "2"},
    ]
    assert b'"volume": "0.01"' in encoded
