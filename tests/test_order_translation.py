"""Nautilus orders to cTrader requests.

Orders are real Nautilus orders from an `OrderFactory`, and instruments come from the recorded
symbol specifications, so a change in either shows up here rather than against the venue.
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from nautilus_trader.common.component import TestClock
from nautilus_trader.common.factories import OrderFactory
from nautilus_trader.model.enums import OrderSide, TimeInForce
from nautilus_trader.model.identifiers import ClientOrderId, StrategyId, TraderId
from nautilus_trader.model.objects import Price, Quantity

from nautilus_ctrader.common import order_translation as tr
from nautilus_ctrader.common import parsing
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as om
from tests.fixtures import load_recorded

ACCOUNT_ID = 7654321
REC = load_recorded()
ASSETS = {a.assetId: a for a in REC["assets"][0].asset}
LIGHT = {s.symbolName: s for s in REC["symbols"][0].symbol}
SPECS = {s.symbolId: s for s in REC["symbol_specs"][0].symbol}


def instrument(name: str):
    light = LIGHT[name]
    return parsing.instrument_from_symbol(SPECS[light.symbolId], light, ASSETS, {}, ts_init=0)


EURUSD = instrument("EURUSD")
GER40 = instrument("GER40.cash")
BID, ASK = Decimal("1.10990"), Decimal("1.11000")


def factory() -> OrderFactory:
    return OrderFactory(
        trader_id=TraderId("TESTER-001"),
        strategy_id=StrategyId("S-001"),
        clock=TestClock(),
    )


def eurusd_bracket(
    side: OrderSide = OrderSide.BUY, *, stop: str, target: str, quantity: int = 1000
):
    return (
        factory()
        .bracket(
            instrument_id=EURUSD.id,
            order_side=side,
            quantity=Quantity.from_int(quantity),
            sl_trigger_price=Price.from_str(stop),
            tp_price=Price.from_str(target),
        )
        .orders
    )


def test_a_buy_bracket_is_one_market_order_with_relative_levels() -> None:
    orders = eurusd_bracket(stop="1.10000", target="1.12000")
    entry, stop, target = orders

    built = tr.bracket(ACCOUNT_ID, EURUSD, orders, bid=BID, ask=ASK)
    request = built.request

    assert request.ctidTraderAccountId == ACCOUNT_ID
    assert request.symbolId == EURUSD.info["symbol_id"]
    assert request.orderType == om.MARKET
    assert request.tradeSide == om.BUY
    assert request.volume == 100_000  # 1 000 units in hundredths of a unit
    # A buy fills at the ask, so both distances are measured from it, in 1/100000 of a price.
    assert request.relativeStopLoss == 1_000
    assert request.relativeTakeProfit == 1_000
    # Absolute levels are not accepted on a market order, so they are never set.
    assert not request.HasField("stopLoss") and not request.HasField("takeProfit")
    assert request.clientOrderId == entry.client_order_id.value
    assert request.label == f"ntca1:{entry.client_order_id.value}"
    assert request.comment == (
        f"ntca1|sl={stop.client_order_id.value}|tp={target.client_order_id.value}"
    )
    assert built.stop_loss == Price.from_str("1.10000")
    assert built.take_profit == Price.from_str("1.12000")
    assert built.stop_loss_id == stop.client_order_id
    assert built.take_profit_id == target.client_order_id


def test_a_sell_bracket_measures_its_levels_from_the_bid() -> None:
    orders = eurusd_bracket(OrderSide.SELL, stop="1.12000", target="1.10000")

    request = tr.bracket(ACCOUNT_ID, EURUSD, orders, bid=BID, ask=ASK).request

    assert request.tradeSide == om.SELL
    assert request.relativeStopLoss == 1_010  # 1.12000 - 1.10990
    assert request.relativeTakeProfit == 990  # 1.10990 - 1.10000


def test_a_fractional_contract_quantity_is_scaled_exactly() -> None:
    orders = (
        factory()
        .bracket(
            instrument_id=GER40.id,
            order_side=OrderSide.BUY,
            quantity=Quantity.from_str("0.01"),
            sl_trigger_price=Price.from_str("17000.00"),
            tp_price=Price.from_str("19000.00"),
        )
        .orders
    )

    request = tr.bracket(
        ACCOUNT_ID, GER40, orders, bid=Decimal("18000.00"), ask=Decimal("18001.00")
    ).request

    assert request.volume == 1
    assert request.relativeStopLoss == 100_100_000  # 1 001.00 in 1/100000


@pytest.mark.parametrize(
    ("side", "stop", "target"),
    [
        (OrderSide.BUY, "1.11000", "1.12000"),  # stop exactly at the ask
        (OrderSide.BUY, "1.11500", "1.12000"),  # stop above the market
        (OrderSide.BUY, "1.10000", "1.11000"),  # target exactly at the ask
        (OrderSide.SELL, "1.10990", "1.10000"),  # stop exactly at the bid
        (OrderSide.SELL, "1.12000", "1.11500"),  # target above the market
    ],
)
def test_a_level_on_the_wrong_side_of_the_market_is_refused(side, stop, target) -> None:
    with pytest.raises(tr.Unsupported, match="wrong side of the market"):
        tr.bracket(
            ACCOUNT_ID, EURUSD, eurusd_bracket(side, stop=stop, target=target), bid=BID, ask=ASK
        )


def test_a_quantity_finer_than_the_venue_unit_is_refused() -> None:
    with pytest.raises(tr.Unsupported, match="hundredths of a unit"):
        tr.volume_from_quantity(Quantity.from_str("0.005"))


def test_a_pending_entry_is_refused_by_name() -> None:
    entry = factory().limit(
        instrument_id=EURUSD.id,
        order_side=OrderSide.BUY,
        quantity=Quantity.from_int(1000),
        price=Price.from_str("1.10000"),
    )
    _, stop, target = eurusd_bracket(stop="1.09000", target="1.12000")

    with pytest.raises(tr.Unsupported, match="LIMIT"):
        tr.bracket(ACCOUNT_ID, EURUSD, [entry, stop, target], bid=BID, ask=ASK)


def test_a_second_stop_on_one_position_is_refused() -> None:
    entry, stop, _ = eurusd_bracket(stop="1.10000", target="1.12000")
    second = factory().stop_market(
        instrument_id=EURUSD.id,
        order_side=OrderSide.SELL,
        quantity=Quantity.from_int(1000),
        trigger_price=Price.from_str("1.09500"),
        reduce_only=True,
    )

    with pytest.raises(tr.Unsupported, match="one stop-loss"):
        tr.bracket(ACCOUNT_ID, EURUSD, [entry, stop, second], bid=BID, ask=ASK)


def test_a_leg_on_the_entrys_side_is_refused() -> None:
    entry, _, target = eurusd_bracket(stop="1.10000", target="1.12000")
    same_side = factory().stop_market(
        instrument_id=EURUSD.id,
        order_side=OrderSide.BUY,
        quantity=Quantity.from_int(1000),
        trigger_price=Price.from_str("1.10000"),
        reduce_only=True,
    )

    with pytest.raises(tr.Unsupported, match="opposite side"):
        tr.bracket(ACCOUNT_ID, EURUSD, [entry, same_side, target], bid=BID, ask=ASK)


def test_a_leg_sized_differently_from_the_entry_is_refused() -> None:
    entry, _, target = eurusd_bracket(stop="1.10000", target="1.12000")
    smaller = factory().stop_market(
        instrument_id=EURUSD.id,
        order_side=OrderSide.SELL,
        quantity=Quantity.from_int(2000),
        trigger_price=Price.from_str("1.10000"),
        reduce_only=True,
    )

    with pytest.raises(tr.Unsupported, match="whole position"):
        tr.bracket(ACCOUNT_ID, EURUSD, [entry, smaller, target], bid=BID, ask=ASK)


def test_a_time_in_force_other_than_gtc_is_refused() -> None:
    order = factory().market(
        instrument_id=EURUSD.id,
        order_side=OrderSide.BUY,
        quantity=Quantity.from_int(1000),
        time_in_force=TimeInForce.FOK,
    )

    with pytest.raises(tr.Unsupported, match="FOK"):
        tr.market_order(ACCOUNT_ID, EURUSD, order)


def test_a_plain_market_order_carries_the_record_and_no_levels() -> None:
    order = factory().market(
        instrument_id=EURUSD.id,
        order_side=OrderSide.SELL,
        quantity=Quantity.from_int(2000),
    )

    request = tr.market_order(ACCOUNT_ID, EURUSD, order)

    assert request.orderType == om.MARKET and request.tradeSide == om.SELL
    assert request.volume == 200_000
    assert request.clientOrderId == order.client_order_id.value
    assert request.label == f"ntca1:{order.client_order_id.value}"
    assert request.comment == "ntca1"
    assert not request.HasField("relativeStopLoss")
    assert not request.HasField("relativeTakeProfit")


def test_a_reduce_only_market_order_is_not_an_opening_order() -> None:
    order = factory().market(
        instrument_id=EURUSD.id,
        order_side=OrderSide.SELL,
        quantity=Quantity.from_int(1000),
        reduce_only=True,
    )

    with pytest.raises(tr.Unsupported, match="must name its position"):
        tr.market_order(ACCOUNT_ID, EURUSD, order)


def test_a_close_names_the_position_and_the_volume() -> None:
    order = factory().market(
        instrument_id=EURUSD.id,
        order_side=OrderSide.SELL,
        quantity=Quantity.from_int(1000),
        reduce_only=True,
    )

    request = tr.close_position(ACCOUNT_ID, 5_000_001, order)

    assert request.ctidTraderAccountId == ACCOUNT_ID
    assert request.positionId == 5_000_001
    assert request.volume == 100_000


def test_a_close_must_be_a_reduce_only_market_order() -> None:
    order = factory().market(
        instrument_id=EURUSD.id,
        order_side=OrderSide.SELL,
        quantity=Quantity.from_int(1000),
    )

    with pytest.raises(tr.Unsupported, match="reduce-only"):
        tr.close_position(ACCOUNT_ID, 5_000_001, order)


def test_an_amend_sets_the_levels_it_is_given() -> None:
    request = tr.amend_levels(
        ACCOUNT_ID,
        5_000_001,
        stop_loss=Price.from_str("1.10000"),
        take_profit=Price.from_str("1.12000"),
    )

    assert request.positionId == 5_000_001
    assert request.stopLoss == pytest.approx(1.1)
    assert request.takeProfit == pytest.approx(1.12)


def test_an_amend_leaves_out_a_level_that_is_to_be_removed() -> None:
    request = tr.amend_levels(
        ACCOUNT_ID,
        5_000_001,
        stop_loss=None,
        take_profit=Price.from_str("1.12000"),
    )

    assert not request.HasField("stopLoss")
    assert request.HasField("takeProfit")


def test_an_id_too_long_for_the_venue_is_refused() -> None:
    orders = eurusd_bracket(stop="1.10000", target="1.12000")
    # Set explicitly: the factory builds its ids from the strategy's short tag, so a long
    # strategy name does not make a long id.
    entry = factory().market(
        instrument_id=EURUSD.id,
        order_side=OrderSide.BUY,
        quantity=Quantity.from_int(1000),
        client_order_id=ClientOrderId("O-" + "9" * 60),
    )

    with pytest.raises(tr.Unsupported, match="characters"):
        tr.bracket(ACCOUNT_ID, EURUSD, [entry, orders[1], orders[2]], bid=BID, ask=ASK)
