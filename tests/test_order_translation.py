"""Nautilus orders to cTrader requests.

Orders are real Nautilus orders from an `OrderFactory`, and instruments come from the recorded
symbol specifications, so a change in either shows up here rather than against the venue.
"""

from __future__ import annotations

import itertools
from decimal import Decimal

import pytest
from nautilus_trader.common.component import TestClock
from nautilus_trader.common.factories import OrderFactory
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.model.enums import (
    ContingencyType,
    OrderSide,
    OrderType,
    PositionSide,
    TimeInForce,
    TriggerType,
)
from nautilus_trader.model.identifiers import (
    ClientOrderId,
    ExecAlgorithmId,
    OrderListId,
    StrategyId,
    TraderId,
)
from nautilus_trader.model.objects import Price, Quantity
from nautilus_trader.model.orders import LimitOrder, MarketOrder, StopMarketOrder

from nautilus_ctrader.common import order_translation as tr
from nautilus_ctrader.common import parsing
from nautilus_ctrader.common.venue_records import LevelTerms
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
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
GER_BID, GER_ASK = Decimal("18000.00"), Decimal("18001.00")

_TRADER = TraderId("TESTER-001")
_STRATEGY = StrategyId("S-001")
_LEG_SEQ = itertools.count(1)


def factory() -> OrderFactory:
    return OrderFactory(trader_id=_TRADER, strategy_id=_STRATEGY, clock=TestClock())


def eurusd_bracket(
    side: OrderSide = OrderSide.BUY,
    *,
    stop: str,
    target: str,
    quantity: int = 1000,
    **overrides,
):
    return (
        factory()
        .bracket(
            instrument_id=EURUSD.id,
            order_side=side,
            quantity=Quantity.from_int(quantity),
            sl_trigger_price=Price.from_str(stop),
            tp_price=Price.from_str(target),
            **overrides,
        )
        .orders
    )


def market_entry(side: OrderSide = OrderSide.BUY, *, quantity: int = 1000, **overrides):
    return factory().market(
        instrument_id=EURUSD.id,
        order_side=side,
        quantity=Quantity.from_int(quantity),
        **overrides,
    )


def _opposite(side: OrderSide) -> OrderSide:
    return OrderSide.SELL if side == OrderSide.BUY else OrderSide.BUY


def stop_leg(entry, trigger: str = "1.10000", **overrides) -> StopMarketOrder:
    """A stop-loss leg as a bracket builds it, with any attribute replaced by `overrides`."""
    args = {
        "trader_id": _TRADER,
        "strategy_id": _STRATEGY,
        "instrument_id": entry.instrument_id,
        "client_order_id": ClientOrderId(f"L-{next(_LEG_SEQ)}"),
        "order_side": _opposite(entry.side),
        "quantity": entry.quantity,
        "trigger_price": Price.from_str(trigger),
        "trigger_type": TriggerType.DEFAULT,
        "init_id": UUID4(),
        "ts_init": 0,
        "reduce_only": True,
        "parent_order_id": entry.client_order_id,
    }
    args.update(overrides)
    return StopMarketOrder(**args)


def target_leg(entry, price: str = "1.12000", **overrides) -> LimitOrder:
    """A take-profit leg as a bracket builds it, with any attribute replaced by `overrides`."""
    args = {
        "trader_id": _TRADER,
        "strategy_id": _STRATEGY,
        "instrument_id": entry.instrument_id,
        "client_order_id": ClientOrderId(f"L-{next(_LEG_SEQ)}"),
        "order_side": _opposite(entry.side),
        "quantity": entry.quantity,
        "price": Price.from_str(price),
        "init_id": UUID4(),
        "ts_init": 0,
        "post_only": True,
        "reduce_only": True,
        "parent_order_id": entry.client_order_id,
    }
    args.update(overrides)
    return LimitOrder(**args)


def build(orders, instrument=EURUSD, *, bid=BID, ask=ASK):
    return tr.bracket(ACCOUNT_ID, instrument, orders, bid=bid, ask=ask)


def test_a_buy_bracket_is_one_market_order_with_relative_levels() -> None:
    orders = eurusd_bracket(stop="1.10000", target="1.12000")
    entry, stop, target = orders

    built = build(orders)
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

    request = build(orders).request

    assert request.tradeSide == om.SELL
    assert request.relativeStopLoss == 1_010  # 1.12000 - 1.10990
    assert request.relativeTakeProfit == 990  # 1.10990 - 1.10000


def test_a_fractional_contract_buy_bracket_is_scaled_exactly() -> None:
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

    request = build(orders, GER40, bid=GER_BID, ask=GER_ASK).request

    assert request.volume == 1
    assert request.relativeStopLoss == 100_100_000  # 1 001.00 in 1/100000
    assert request.relativeTakeProfit == 99_900_000  # 999.00 in 1/100000


def test_a_fractional_contract_sell_bracket_is_scaled_exactly() -> None:
    orders = (
        factory()
        .bracket(
            instrument_id=GER40.id,
            order_side=OrderSide.SELL,
            quantity=Quantity.from_str("0.05"),
            sl_trigger_price=Price.from_str("19010.50"),
            tp_price=Price.from_str("17000.25"),
        )
        .orders
    )

    request = build(orders, GER40, bid=GER_BID, ask=GER_ASK).request

    assert request.tradeSide == om.SELL
    assert request.volume == 5
    assert request.relativeStopLoss == 101_050_000  # 19010.50 - 18000.00
    assert request.relativeTakeProfit == 99_975_000  # 18000.00 - 17000.25


@pytest.mark.parametrize(
    ("side", "stop", "target"),
    [
        (OrderSide.BUY, "1.11000", "1.12000"),  # stop exactly at the ask
        (OrderSide.BUY, "1.11500", "1.12000"),  # stop above the market
        (OrderSide.BUY, "1.10000", "1.11000"),  # target exactly at the ask
        (OrderSide.BUY, "1.10000", "1.10500"),  # target strictly below the ask
        (OrderSide.SELL, "1.10990", "1.10000"),  # stop exactly at the bid
        (OrderSide.SELL, "1.10500", "1.10000"),  # stop strictly below the bid
        (OrderSide.SELL, "1.12000", "1.11500"),  # target above the market
        (OrderSide.SELL, "1.12000", "1.10990"),  # target exactly at the bid
    ],
)
def test_a_level_on_the_wrong_side_of_the_market_is_refused(side, stop, target) -> None:
    with pytest.raises(tr.Unsupported, match="wrong side of the market"):
        build(eurusd_bracket(side, stop=stop, target=target))


def test_a_quantity_finer_than_the_venue_unit_is_refused() -> None:
    with pytest.raises(tr.Unsupported, match="hundredths of a unit"):
        tr.volume_from_quantity(Quantity.from_str("0.005"))


def test_a_level_finer_than_the_venue_price_scale_is_refused() -> None:
    with pytest.raises(tr.Unsupported, match="finer than the venue's price scale"):
        tr.relative_level(Decimal("1.2"), Decimal("1.000001"), below=True)


def test_a_level_finer_than_the_instruments_price_precision_is_refused() -> None:
    orders = (
        factory()
        .bracket(
            instrument_id=GER40.id,
            order_side=OrderSide.BUY,
            quantity=Quantity.from_str("0.01"),
            sl_trigger_price=Price.from_str("17000.123"),
            tp_price=Price.from_str("19000.00"),
        )
        .orders
    )

    with pytest.raises(tr.Unsupported, match="price precision"):
        build(orders, GER40, bid=GER_BID, ask=GER_ASK)


def test_a_level_written_with_spare_zeros_is_on_the_instruments_grid() -> None:
    orders = (
        factory()
        .bracket(
            instrument_id=GER40.id,
            order_side=OrderSide.BUY,
            quantity=Quantity.from_str("0.01"),
            sl_trigger_price=Price.from_str("17000.100"),
            tp_price=Price.from_str("19000.00"),
        )
        .orders
    )

    request = build(orders, GER40, bid=GER_BID, ask=GER_ASK).request

    assert request.relativeStopLoss == 100_090_000  # 18001.00 - 17000.10


@pytest.mark.parametrize(
    ("side", "bid", "ask"),
    [
        (OrderSide.BUY, BID, Decimal("1.1100005")),
        (OrderSide.SELL, Decimal("1.1099005"), ASK),
    ],
)
def test_a_reference_price_off_the_venue_grid_is_refused_by_name(side, bid, ask) -> None:
    orders = (
        eurusd_bracket(stop="1.10000", target="1.12000")
        if side == OrderSide.BUY
        else eurusd_bracket(OrderSide.SELL, stop="1.12000", target="1.10000")
    )

    with pytest.raises(tr.Unsupported, match="reference price"):
        build(orders, bid=bid, ask=ask)


def test_a_non_positive_reference_price_is_refused() -> None:
    with pytest.raises(tr.Unsupported, match="reference price"):
        tr.relative_level(Decimal("0"), Decimal("1.1"), below=False)


def test_a_pending_entry_is_refused_by_name() -> None:
    orders = eurusd_bracket(
        stop="1.09000",
        target="1.12000",
        entry_order_type=OrderType.LIMIT,
        entry_price=Price.from_str("1.10000"),
    )

    with pytest.raises(tr.Unsupported, match="LIMIT"):
        build(orders)


def test_a_second_stop_on_one_position_is_refused() -> None:
    entry, stop, _ = eurusd_bracket(stop="1.10000", target="1.12000")

    with pytest.raises(tr.Unsupported, match="one stop-loss"):
        build([entry, stop, stop_leg(entry, "1.09500")])


def test_a_second_take_profit_on_one_position_is_refused() -> None:
    entry, _, target = eurusd_bracket(stop="1.10000", target="1.12000")

    with pytest.raises(tr.Unsupported, match="one take-profit"):
        build([entry, target, target_leg(entry, "1.13000")])


def test_a_leg_of_an_unsupported_type_is_refused_by_name() -> None:
    entry, _, target = eurusd_bracket(stop="1.10000", target="1.12000")
    stop_limit = factory().stop_limit(
        instrument_id=EURUSD.id,
        order_side=OrderSide.SELL,
        quantity=entry.quantity,
        price=Price.from_str("1.09900"),
        trigger_price=Price.from_str("1.10000"),
    )

    with pytest.raises(tr.Unsupported, match="STOP_LIMIT"):
        build([entry, stop_limit, target])


def test_a_stop_with_a_trigger_type_other_than_default_is_refused() -> None:
    entry, _, target = eurusd_bracket(stop="1.10000", target="1.12000")

    with pytest.raises(tr.Unsupported, match="LAST_PRICE"):
        build([entry, stop_leg(entry, trigger_type=TriggerType.LAST_PRICE), target])


def test_a_leg_on_the_entrys_side_is_refused() -> None:
    entry, _, target = eurusd_bracket(stop="1.10000", target="1.12000")

    with pytest.raises(tr.Unsupported, match="opposite side"):
        build([entry, stop_leg(entry, order_side=entry.side), target])


def test_a_leg_larger_than_the_entry_is_refused() -> None:
    entry, _, target = eurusd_bracket(stop="1.10000", target="1.12000")
    larger = stop_leg(entry, quantity=Quantity.from_int(2000))

    with pytest.raises(tr.Unsupported, match="whole position"):
        build([entry, larger, target])


def test_a_leg_that_is_not_reduce_only_is_refused() -> None:
    entry, _, target = eurusd_bracket(stop="1.10000", target="1.12000")

    with pytest.raises(tr.Unsupported, match="reduce-only"):
        build([entry, stop_leg(entry, reduce_only=False), target])


def test_a_leg_of_another_parent_is_refused() -> None:
    entry, _, target = eurusd_bracket(stop="1.10000", target="1.12000")
    foreign = stop_leg(entry, parent_order_id=ClientOrderId("O-elsewhere"))

    with pytest.raises(tr.Unsupported, match="parent"):
        build([entry, foreign, target])


def test_a_leg_time_in_force_other_than_gtc_is_refused() -> None:
    orders = eurusd_bracket(stop="1.10000", target="1.12000", tp_time_in_force=TimeInForce.IOC)

    with pytest.raises(tr.Unsupported, match="IOC"):
        build(orders)


def test_a_time_in_force_other_than_gtc_is_refused() -> None:
    order = market_entry(time_in_force=TimeInForce.FOK)

    with pytest.raises(tr.Unsupported, match="FOK"):
        tr.market_order(ACCOUNT_ID, EURUSD, order)


@pytest.mark.parametrize("orders", [[], [market_entry()]])
def test_a_bracket_needs_an_entry_and_a_protective_leg(orders) -> None:
    with pytest.raises(tr.Unsupported, match="entry and one or two protective legs"):
        build(orders)


def test_a_bracket_of_more_than_three_orders_is_refused() -> None:
    entry, stop, target = eurusd_bracket(stop="1.10000", target="1.12000")

    with pytest.raises(tr.Unsupported, match="entry and one or two protective legs"):
        build([entry, stop, target, stop_leg(entry)])


def test_a_bracket_may_carry_a_single_leg() -> None:
    entry = market_entry()

    request = build([entry, stop_leg(entry, "1.10000")]).request

    assert request.relativeStopLoss == 1_000
    assert not request.HasField("relativeTakeProfit")
    assert request.comment.startswith("ntca1|sl=") and "|tp=" not in request.comment


def test_an_order_for_another_instrument_is_refused() -> None:
    entry = factory().market(
        instrument_id=GER40.id, order_side=OrderSide.BUY, quantity=Quantity.from_str("0.01")
    )

    with pytest.raises(tr.Unsupported, match="instrument"):
        tr.market_order(ACCOUNT_ID, EURUSD, entry)


def test_a_leg_for_another_instrument_is_refused() -> None:
    entry, _, target = eurusd_bracket(stop="1.10000", target="1.12000")
    other = stop_leg(entry, instrument_id=GER40.id)

    with pytest.raises(tr.Unsupported, match="instrument"):
        build([entry, other, target])


def test_a_plain_market_order_carries_the_record_and_no_levels() -> None:
    order = market_entry(OrderSide.SELL, quantity=2000)

    request = tr.market_order(ACCOUNT_ID, EURUSD, order)

    assert request.orderType == om.MARKET and request.tradeSide == om.SELL
    assert request.volume == 200_000
    assert request.clientOrderId == order.client_order_id.value
    assert request.label == f"ntca1:{order.client_order_id.value}"
    assert request.comment == "ntca1"
    assert not request.HasField("relativeStopLoss")
    assert not request.HasField("relativeTakeProfit")


def test_a_reduce_only_market_order_is_not_an_opening_order() -> None:
    order = market_entry(OrderSide.SELL, reduce_only=True)

    with pytest.raises(tr.Unsupported, match="must name the position"):
        tr.market_order(ACCOUNT_ID, EURUSD, order)


def test_a_reduce_only_entry_of_a_bracket_is_refused_like_a_market_order() -> None:
    entry = market_entry(reduce_only=True)

    with pytest.raises(tr.Unsupported, match="must name the position"):
        build([entry, stop_leg(entry), target_leg(entry)])


def test_a_quantity_in_the_quote_currency_is_refused_on_a_market_order() -> None:
    order = market_entry(quote_quantity=True)

    with pytest.raises(tr.Unsupported, match="quote currency"):
        tr.market_order(ACCOUNT_ID, EURUSD, order)


def test_a_quantity_in_the_quote_currency_is_refused_on_a_bracket() -> None:
    orders = eurusd_bracket(stop="1.10000", target="1.12000", quote_quantity=True)

    with pytest.raises(tr.Unsupported, match="quote currency"):
        build(orders)


def test_a_quantity_in_the_quote_currency_is_refused_on_a_leg() -> None:
    entry, _, target = eurusd_bracket(stop="1.10000", target="1.12000")

    with pytest.raises(tr.Unsupported, match="quote currency"):
        build([entry, stop_leg(entry, quote_quantity=True), target])


def test_an_emulated_leg_is_refused() -> None:
    orders = eurusd_bracket(stop="1.10000", target="1.12000", emulation_trigger=TriggerType.DEFAULT)

    with pytest.raises(tr.Unsupported, match="emulat"):
        build(orders)


def test_an_order_for_an_execution_algorithm_is_refused() -> None:
    order = market_entry(exec_algorithm_id=ExecAlgorithmId("ALGO-1"))

    with pytest.raises(tr.Unsupported, match="execution algorithm"):
        tr.market_order(ACCOUNT_ID, EURUSD, order)


def test_a_leg_for_an_execution_algorithm_is_refused() -> None:
    orders = eurusd_bracket(
        stop="1.10000", target="1.12000", sl_exec_algorithm_id=ExecAlgorithmId("ALGO-1")
    )

    with pytest.raises(tr.Unsupported, match="execution algorithm"):
        build(orders)


def test_a_market_order_linked_to_others_is_refused() -> None:
    order = MarketOrder(
        trader_id=_TRADER,
        strategy_id=_STRATEGY,
        instrument_id=EURUSD.id,
        client_order_id=ClientOrderId("O-1"),
        order_side=OrderSide.BUY,
        quantity=Quantity.from_int(1000),
        init_id=UUID4(),
        ts_init=0,
        contingency_type=ContingencyType.OTO,
        order_list_id=OrderListId("OL-1"),
        linked_order_ids=[ClientOrderId("O-2")],
    )

    with pytest.raises(tr.Unsupported, match="bracket"):
        tr.market_order(ACCOUNT_ID, EURUSD, order)


def test_a_display_quantity_on_the_take_profit_is_refused() -> None:
    entry, stop, _ = eurusd_bracket(stop="1.10000", target="1.12000")
    iceberg = target_leg(entry, display_qty=Quantity.from_int(100))

    with pytest.raises(tr.Unsupported, match="display quantity"):
        build([entry, stop, iceberg])


def test_a_take_profit_translates_the_same_whether_or_not_it_is_post_only() -> None:
    made = {
        flag: build(eurusd_bracket(stop="1.10000", target="1.12000", tp_post_only=flag))
        for flag in (True, False)
    }

    # Ids differ per factory call; everything the venue is asked for must not.
    for built in made.values():
        built.request.ClearField("clientOrderId")
        built.request.ClearField("label")
        built.request.ClearField("comment")
    assert made[True].request == made[False].request
    assert made[True].take_profit == made[False].take_profit


def test_a_close_names_the_position_and_the_volume() -> None:
    order = market_entry(OrderSide.SELL, reduce_only=True)

    request = tr.close_position(ACCOUNT_ID, 5_000_001, order, position_side=PositionSide.LONG)

    assert request.ctidTraderAccountId == ACCOUNT_ID
    assert request.positionId == 5_000_001
    assert request.volume == 100_000


def test_a_buy_closes_a_short_position() -> None:
    order = market_entry(OrderSide.BUY, reduce_only=True)

    request = tr.close_position(ACCOUNT_ID, 5_000_001, order, position_side=PositionSide.SHORT)

    assert request.positionId == 5_000_001


@pytest.mark.parametrize(
    ("side", "position_side"),
    [(OrderSide.BUY, PositionSide.LONG), (OrderSide.SELL, PositionSide.SHORT)],
)
def test_a_close_that_would_add_to_the_position_is_refused(side, position_side) -> None:
    order = market_entry(side, reduce_only=True)

    with pytest.raises(tr.Unsupported, match="would not reduce"):
        tr.close_position(ACCOUNT_ID, 5_000_001, order, position_side=position_side)


@pytest.mark.parametrize("position_side", [PositionSide.FLAT, PositionSide.NO_POSITION_SIDE])
def test_a_close_of_a_position_that_is_not_open_is_refused(position_side) -> None:
    order = market_entry(OrderSide.SELL, reduce_only=True)

    with pytest.raises(tr.Unsupported, match="cannot be reduced"):
        tr.close_position(ACCOUNT_ID, 5_000_001, order, position_side=position_side)


def test_a_close_must_be_a_reduce_only_market_order() -> None:
    order = market_entry(OrderSide.SELL)

    with pytest.raises(tr.Unsupported, match="reduce-only"):
        tr.close_position(ACCOUNT_ID, 5_000_001, order, position_side=PositionSide.LONG)


def test_a_close_of_another_order_type_is_refused() -> None:
    order = factory().limit(
        instrument_id=EURUSD.id,
        order_side=OrderSide.SELL,
        quantity=Quantity.from_int(1000),
        price=Price.from_str("1.12000"),
        reduce_only=True,
    )

    with pytest.raises(tr.Unsupported, match="reduce-only MARKET"):
        tr.close_position(ACCOUNT_ID, 5_000_001, order, position_side=PositionSide.LONG)


def test_a_close_in_the_quote_currency_is_refused() -> None:
    order = market_entry(OrderSide.SELL, reduce_only=True, quote_quantity=True)

    with pytest.raises(tr.Unsupported, match="quote currency"):
        tr.close_position(ACCOUNT_ID, 5_000_001, order, position_side=PositionSide.LONG)


def test_a_close_with_a_time_in_force_other_than_gtc_or_ioc_is_refused() -> None:
    order = market_entry(OrderSide.SELL, reduce_only=True, time_in_force=TimeInForce.FOK)

    with pytest.raises(tr.Unsupported, match="FOK"):
        tr.close_position(ACCOUNT_ID, 5_000_001, order, position_side=PositionSide.LONG)


TRAILING = LevelTerms(
    trailing_stop_loss=True,
    guaranteed_stop_loss=False,
    stop_loss_trigger_method=om.OPPOSITE,
)


def test_an_amend_sets_the_levels_it_is_given() -> None:
    request = tr.amend_levels(
        ACCOUNT_ID,
        5_000_001,
        stop_loss=Price.from_str("1.10000"),
        take_profit=Price.from_str("1.12000"),
        terms=None,
    )

    assert request.positionId == 5_000_001
    assert request.stopLoss == 1.1
    assert request.takeProfit == 1.12


def test_an_amend_leaves_out_a_level_that_is_to_be_removed() -> None:
    request = tr.amend_levels(
        ACCOUNT_ID,
        5_000_001,
        stop_loss=None,
        take_profit=Price.from_str("1.12000"),
        terms=None,
    )

    assert not request.HasField("stopLoss")
    assert request.HasField("takeProfit")


def test_an_amend_with_no_level_removes_both() -> None:
    request = tr.amend_levels(ACCOUNT_ID, 5_000_001, stop_loss=None, take_profit=None, terms=None)

    assert request.positionId == 5_000_001
    assert not request.HasField("stopLoss")
    assert not request.HasField("takeProfit")


@pytest.mark.parametrize(
    "terms",
    [
        TRAILING,
        LevelTerms(
            trailing_stop_loss=False,
            guaranteed_stop_loss=True,
            stop_loss_trigger_method=om.DOUBLE_TRADE,
        ),
        # The schema's defaults are sent too: an omitted field may read as a change.
        LevelTerms(
            trailing_stop_loss=False,
            guaranteed_stop_loss=False,
            stop_loss_trigger_method=om.TRADE,
        ),
    ],
)
def test_an_amend_keeps_how_the_positions_stop_loss_works(terms: LevelTerms) -> None:
    request = tr.amend_levels(
        ACCOUNT_ID,
        5_000_001,
        stop_loss=Price.from_str("1.10000"),
        take_profit=None,
        terms=terms,
    )

    assert request.HasField("trailingStopLoss")
    assert request.HasField("guaranteedStopLoss")
    assert request.HasField("stopLossTriggerMethod")
    assert request.trailingStopLoss == terms.trailing_stop_loss
    assert request.guaranteedStopLoss == terms.guaranteed_stop_loss
    assert request.stopLossTriggerMethod == terms.stop_loss_trigger_method


def test_an_amend_removing_the_stop_loss_sends_no_trailing_or_guaranteed_stop() -> None:
    request = tr.amend_levels(
        ACCOUNT_ID,
        5_000_001,
        stop_loss=None,
        take_profit=Price.from_str("1.12000"),
        terms=TRAILING,
    )

    assert not request.HasField("trailingStopLoss")
    assert not request.HasField("guaranteedStopLoss")
    # It applies to the take-profit as well.
    assert request.stopLossTriggerMethod == om.OPPOSITE


def test_an_amend_of_a_position_never_seen_sends_no_terms() -> None:
    request = tr.amend_levels(
        ACCOUNT_ID,
        5_000_001,
        stop_loss=Price.from_str("1.10000"),
        take_profit=None,
        terms=None,
    )

    assert not request.HasField("trailingStopLoss")
    assert not request.HasField("guaranteedStopLoss")
    assert not request.HasField("stopLossTriggerMethod")


def test_an_id_too_long_for_the_venue_is_refused() -> None:
    # Set explicitly: the factory builds its ids from the strategy's short tag, so a long
    # strategy name does not make a long id.
    orders = eurusd_bracket(
        stop="1.10000",
        target="1.12000",
        entry_client_order_id=ClientOrderId("O-" + "9" * 60),
    )

    with pytest.raises(tr.Unsupported, match="characters"):
        build(orders)


def test_leg_ids_that_overflow_the_comment_are_refused() -> None:
    entry = market_entry()
    legs = [
        stop_leg(entry, client_order_id=ClientOrderId("S" * 300)),
        target_leg(entry, client_order_id=ClientOrderId("T" * 300)),
    ]

    with pytest.raises(tr.Unsupported, match="comment record"):
        build([entry, *legs])


@pytest.mark.parametrize("bad", ["O 1", "O|1"])
def test_an_entry_id_the_record_cannot_hold_is_refused(bad) -> None:
    entry = market_entry(client_order_id=ClientOrderId(bad))

    with pytest.raises(tr.Unsupported, match="client order id"):
        tr.market_order(ACCOUNT_ID, EURUSD, entry)
    with pytest.raises(tr.Unsupported, match="client order id"):
        build([entry, stop_leg(entry), target_leg(entry)])


@pytest.mark.parametrize("bad", ["L 1", "L|1"])
def test_a_leg_id_the_record_cannot_hold_is_refused(bad) -> None:
    entry = market_entry()
    stop = stop_leg(entry, client_order_id=ClientOrderId(bad))
    target = target_leg(entry, client_order_id=ClientOrderId(bad))

    with pytest.raises(tr.Unsupported, match="client order id"):
        build([entry, stop, target_leg(entry)])
    with pytest.raises(tr.Unsupported, match="client order id"):
        build([entry, stop_leg(entry), target])


def test_a_market_entry_goes_out_immediate_or_cancel() -> None:
    request = tr.market_order(ACCOUNT_ID, EURUSD, market_entry())

    assert request.timeInForce == om.IMMEDIATE_OR_CANCEL


def test_a_bracket_entry_goes_out_immediate_or_cancel() -> None:
    bracket = build(eurusd_bracket(stop="1.10000", target="1.12000"))

    assert bracket.request.timeInForce == om.IMMEDIATE_OR_CANCEL


@pytest.mark.parametrize("time_in_force", [TimeInForce.GTC, TimeInForce.IOC])
def test_a_market_order_with_gtc_or_ioc_is_accepted(time_in_force) -> None:
    request = tr.market_order(ACCOUNT_ID, EURUSD, market_entry(time_in_force=time_in_force))

    assert request.timeInForce == om.IMMEDIATE_OR_CANCEL


@pytest.mark.parametrize("time_in_force", [TimeInForce.FOK, TimeInForce.DAY])
def test_a_market_order_with_another_time_in_force_is_refused(time_in_force) -> None:
    order = market_entry(time_in_force=time_in_force)

    with pytest.raises(tr.Unsupported, match=TimeInForce(time_in_force).name):
        tr.market_order(ACCOUNT_ID, EURUSD, order)


def test_a_close_with_ioc_is_accepted() -> None:
    order = market_entry(OrderSide.SELL, reduce_only=True, time_in_force=TimeInForce.IOC)

    request = tr.close_position(ACCOUNT_ID, 5_000_001, order, position_side=PositionSide.LONG)

    assert request.positionId == 5_000_001


# -- Pending orders held at the broker -----------------------------------------------------------

PENDING_ID = 6_800_001


def pending(kind: int, **fields) -> om.ProtoOAOrder:
    """A broker's pending EURUSD buy of 1000 units; `fields` set on the order or its trade data."""
    order = om.ProtoOAOrder(
        orderId=PENDING_ID,
        orderType=kind,
        orderStatus=om.ORDER_STATUS_ACCEPTED,
    )
    order.tradeData.symbolId = EURUSD.info["symbol_id"]
    order.tradeData.volume = 100_000
    order.tradeData.tradeSide = om.BUY
    for name, value in fields.items():
        target = order.tradeData if name == "guaranteedStopLoss" else order
        setattr(target, name, value)
    return order


def amend(order: om.ProtoOAOrder, *, quantity=None, price=None, trigger_price=None):
    return tr.amend_order(
        ACCOUNT_ID,
        EURUSD,
        order,
        quantity=None if quantity is None else Quantity.from_str(quantity),
        price=None if price is None else Price.from_str(price),
        trigger_price=None if trigger_price is None else Price.from_str(trigger_price),
    )


def amend_request(**fields) -> oa.ProtoOAAmendOrderReq:
    return oa.ProtoOAAmendOrderReq(ctidTraderAccountId=ACCOUNT_ID, orderId=PENDING_ID, **fields)


@pytest.mark.parametrize("kind", [om.LIMIT, om.STOP, om.STOP_LIMIT])
def test_a_cancel_names_the_pending_order(kind) -> None:
    request = tr.cancel_order(ACCOUNT_ID, pending(kind))

    assert request == oa.ProtoOACancelOrderReq(ctidTraderAccountId=ACCOUNT_ID, orderId=PENDING_ID)


@pytest.mark.parametrize("kind", [om.MARKET, om.MARKET_RANGE])
def test_a_cancel_of_a_market_order_is_refused(kind) -> None:
    with pytest.raises(tr.Unsupported, match="a market order fills at once"):
        tr.cancel_order(ACCOUNT_ID, pending(kind))


@pytest.mark.parametrize(
    ("order", "change", "expected"),
    [
        pytest.param(
            pending(om.LIMIT, limitPrice=1.1),
            {"price": "1.10500"},
            amend_request(volume=100_000, limitPrice=1.105),
            id="limit-price",
        ),
        pytest.param(
            pending(om.LIMIT, limitPrice=1.1),
            {"quantity": "2000"},
            amend_request(volume=200_000, limitPrice=1.1),
            id="limit-quantity",
        ),
        pytest.param(
            pending(om.STOP, stopPrice=1.12, stopTriggerMethod=om.OPPOSITE),
            {"trigger_price": "1.12500"},
            amend_request(volume=100_000, stopPrice=1.125, stopTriggerMethod=om.OPPOSITE),
            id="stop-trigger",
        ),
        pytest.param(
            # The trigger method the schema defaults to is sent too.
            pending(om.STOP, stopPrice=1.12),
            {"quantity": "500"},
            amend_request(volume=50_000, stopPrice=1.12, stopTriggerMethod=om.TRADE),
            id="stop-quantity",
        ),
        pytest.param(
            pending(om.STOP_LIMIT, stopPrice=1.12, slippageInPoints=30),
            {"trigger_price": "1.12500"},
            amend_request(
                volume=100_000, stopPrice=1.125, slippageInPoints=30, stopTriggerMethod=om.TRADE
            ),
            id="stop-limit-trigger",
        ),
        pytest.param(
            pending(
                om.LIMIT,
                limitPrice=1.1,
                expirationTimestamp=1_700_000_000_000,
                stopLoss=1.09,
                takeProfit=1.13,
                trailingStopLoss=True,
                guaranteedStopLoss=True,
            ),
            {"price": "1.10500"},
            amend_request(
                volume=100_000,
                limitPrice=1.105,
                expirationTimestamp=1_700_000_000_000,
                stopLoss=1.09,
                takeProfit=1.13,
                trailingStopLoss=True,
                guaranteedStopLoss=True,
            ),
            id="attached-absolute",
        ),
        pytest.param(
            pending(om.STOP, stopPrice=1.12, relativeStopLoss=500, relativeTakeProfit=1000),
            {"trigger_price": "1.12500"},
            amend_request(
                volume=100_000,
                stopPrice=1.125,
                stopTriggerMethod=om.TRADE,
                relativeStopLoss=500,
                relativeTakeProfit=1000,
                trailingStopLoss=False,
                guaranteedStopLoss=False,
            ),
            id="attached-relative",
        ),
        pytest.param(
            # The trailing flag describes a stop-loss, so with none it is not sent.
            pending(om.LIMIT, limitPrice=1.1, takeProfit=1.13, trailingStopLoss=True),
            {"price": "1.10500"},
            amend_request(volume=100_000, limitPrice=1.105, takeProfit=1.13),
            id="take-profit-only",
        ),
    ],
)
def test_an_order_amend_changes_what_is_asked_and_sends_the_rest_again(
    order, change, expected
) -> None:
    assert amend(order, **change) == expected


@pytest.mark.parametrize(
    ("order", "change"),
    [
        pytest.param(pending(om.LIMIT, limitPrice=1.1), {}, id="nothing"),
        pytest.param(pending(om.LIMIT, limitPrice=1.1), {"price": "1.10000"}, id="same-price"),
        pytest.param(pending(om.LIMIT, limitPrice=1.1), {"quantity": "1000"}, id="same-quantity"),
        pytest.param(
            pending(om.STOP, stopPrice=1.12), {"trigger_price": "1.12"}, id="same-trigger"
        ),
    ],
)
def test_an_order_amend_that_changes_nothing_is_none(order, change) -> None:
    assert amend(order, **change) is None


@pytest.mark.parametrize(
    ("order", "change", "reason"),
    [
        pytest.param(
            pending(om.LIMIT, limitPrice=1.1),
            {"price": "1.105001"},
            "finer than the instrument's price precision",
            id="off-grid",
        ),
        pytest.param(
            pending(om.STOP, stopPrice=1.12),
            {"trigger_price": "1.125001"},
            "finer than the instrument's price precision",
            id="trigger-off-grid",
        ),
        pytest.param(
            pending(om.LIMIT, limitPrice=1.1),
            {"quantity": "0.001"},
            "hundredths of a unit",
            id="quantity-off-grid",
        ),
        pytest.param(
            pending(om.LIMIT, limitPrice=1.1),
            {"trigger_price": "1.12000"},
            "cannot set the trigger price of a LIMIT order",
            id="trigger-on-limit",
        ),
        pytest.param(
            pending(om.STOP_LIMIT, stopPrice=1.12, slippageInPoints=30),
            {"price": "1.12100"},
            "cannot set the limit price of a STOP_LIMIT order",
            id="price-on-stop-limit",
        ),
        pytest.param(
            pending(om.MARKET),
            {"quantity": "2000"},
            "a market order fills at once",
            id="market",
        ),
        pytest.param(
            pending(om.MARKET_RANGE, slippageInPoints=30),
            {"quantity": "2000"},
            "a market order fills at once",
            id="market-range",
        ),
    ],
)
def test_an_order_amend_the_venue_cannot_express_is_refused(order, change, reason) -> None:
    with pytest.raises(tr.Unsupported, match=reason):
        amend(order, **change)
