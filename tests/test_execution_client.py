"""Tests for `CTraderExecutionClient` against the fake venue, inside a Nautilus engine."""

from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock, MessageBus
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.messages import (
    BatchCancelOrders,
    CancelAllOrders,
    CancelOrder,
    ModifyOrder,
    QueryAccount,
    SubmitOrder,
)
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.enums import OrderSide, OrderStatus, TimeInForce
from nautilus_trader.model.events import (
    OrderFilled,
    OrderModifyRejected,
    OrderPendingCancel,
    OrderRejected,
)
from nautilus_trader.model.identifiers import (
    AccountId,
    ClientOrderId,
    PositionId,
    TradeId,
    VenueOrderId,
)
from nautilus_trader.model.objects import Money, Price, Quantity

from nautilus_ctrader.activity import CTraderAccountActivity
from nautilus_ctrader.common import order_record
from nautilus_ctrader.common.account import account_client_from_config
from nautilus_ctrader.common.errors import CTraderAccountError
from nautilus_ctrader.common.order_record import LegIds
from nautilus_ctrader.common.venue_records import Level
from nautilus_ctrader.execution import CTraderExecutionClient
from nautilus_ctrader.factories import CTraderLiveExecClientFactory
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as om
from tests.account_venue import ACCOUNT_ID, HeldReplies
from tests.execution_replay import (
    FIRST,
    as_ours,
    make_deal,
    make_event,
    make_order,
    make_position,
    position_orders,
    snapshot_with_protection,
)
from tests.execution_venue import (
    ENTRY,
    FIRST_EVENTS,
    STOP,
    STRATEGY_ID,
    TARGET,
    TRADER_ID,
    US100_ID,
    US100_SYMBOL_ID,
    ExecutionVenue,
    bracket,
    exec_config,
    harness,
    on_us100,
    push,
    push_spot,
    status,
    submit_bracket,
    submitted,
    sync,
    trader,
)
from tests.polling import wait_until
from tests.recording_logger import RecordingLogger

ACCOUNT = AccountId("CTRADER-001")


def test_the_config_carries_no_instrument_settings() -> None:
    with pytest.raises(ValueError, match="instrument"):
        exec_config(instrument_provider=InstrumentProviderConfig(load_all=True))


@pytest.mark.parametrize(
    "overrides",
    [
        {"environment": "staging"},
        {"reference_price_max_age_secs": 0.0},
        {"protective_order_timeout_secs": -1.0},
        {"order_request_timeout_secs": 0.0},
        {"reconciliation_default_lookback_mins": 0},
    ],
)
def test_the_config_refuses_bad_values(overrides) -> None:
    with pytest.raises(ValueError):
        exec_config(**overrides)


def test_the_config_defaults() -> None:
    config = exec_config()

    assert config.reference_price_max_age_secs == 10.0
    assert config.protective_order_timeout_secs == 2.0
    assert config.order_request_timeout_secs == 30.0
    assert config.reconciliation_default_lookback_mins == 1440
    assert config.environment == "auto"


async def test_a_provider_that_is_not_the_accounts_own_fails_construction() -> None:
    config = exec_config()
    logger = RecordingLogger()
    account = account_client_from_config(config, logger)
    account.get_instrument_provider(
        config=InstrumentProviderConfig(),
        asset_class_overrides={},
        fail_on_instrument_error=False,
        logger=logger,
    )
    other = account_client_from_config(exec_config(trader_login=1), logger)
    foreign = other.get_instrument_provider(
        config=InstrumentProviderConfig(),
        asset_class_overrides={},
        fail_on_instrument_error=False,
        logger=logger,
    )
    clock = LiveClock()

    with pytest.raises(ValueError, match="account's own"):
        CTraderExecutionClient(
            loop=asyncio.get_running_loop(),
            account=account,
            msgbus=MessageBus(trader_id=TRADER_ID, clock=clock),
            cache=Cache(),
            clock=clock,
            instrument_provider=foreign,
            config=config,
        )


async def test_the_factory_needs_the_accounts_provider_built_first() -> None:
    clock = LiveClock()
    kwargs = {
        "loop": asyncio.get_running_loop(),
        "name": "CTRADER",
        "config": exec_config(),
        "msgbus": MessageBus(trader_id=TRADER_ID, clock=clock),
        "cache": Cache(),
        "clock": clock,
    }

    with pytest.raises(ValueError, match="data client"):
        CTraderLiveExecClientFactory.create(**kwargs)

    account = account_client_from_config(kwargs["config"], RecordingLogger())
    provider = account.get_instrument_provider(
        config=InstrumentProviderConfig(),
        asset_class_overrides={},
        fail_on_instrument_error=False,
        logger=RecordingLogger(),
    )
    client = CTraderLiveExecClientFactory.create(**kwargs)

    assert client.instrument_provider is provider
    assert client.account_id == ACCOUNT


async def test_connect_states_the_account_and_holds_the_spots_brackets_are_measured_from() -> None:
    async with harness() as h:
        (state,) = h.states
        recorded = trader().trader
        assert state.account_id == ACCOUNT
        assert state.base_currency == USD
        assert state.balances[0].total == Money(
            Decimal(recorded.balance).scaleb(-recorded.moneyDigits), USD
        )
        assert state.margins == []
        assert [list(r.symbolId) for r in h.received(oa.ProtoOASubscribeSpotsReq)] == [
            [US100_SYMBOL_ID]
        ]
        assert h.received(oa.ProtoOAReconcileReq)[0].returnProtectionOrders
        assert h.cache.account(ACCOUNT) is not None


@pytest.mark.parametrize(
    ("overrides", "reason"),
    [
        ({"accountType": om.NETTED}, "NETTED"),
        ({"accessRights": om.CLOSE_ONLY}, "CLOSE_ONLY"),
        ({"isLimitedRisk": True}, "limited-risk"),
    ],
)
async def test_connect_refuses_an_account_this_adapter_cannot_trade(overrides, reason) -> None:
    execution_venue = ExecutionVenue()
    execution_venue.trader = trader(**overrides)
    async with harness(execution_venue=execution_venue, connect=False) as h:
        with pytest.raises(CTraderAccountError, match=reason):
            await h.client._connect()

        assert h.account.session is None
        assert h.states == []


def our_open_position(execution_venue: ExecutionVenue) -> None:
    """The recorded first position, made the node's, open with both levels at the broker."""
    (execution_venue.snapshot,) = on_us100(as_ours([snapshot_with_protection(127.187)], [FIRST]))
    execution_venue.position_orders = {
        position_id: on_us100(as_ours(orders, [FIRST]))
        for position_id, orders in position_orders().items()
    }


async def test_connect_loads_an_open_position_and_states_its_margin() -> None:
    execution_venue = ExecutionVenue()
    our_open_position(execution_venue)
    async with harness(execution_venue=execution_venue) as h:
        view = h.client._book.view(FIRST)
        assert view.ours
        assert view.open
        (state,) = h.states
        (margin,) = state.margins
        position = execution_venue.snapshot.position[0]
        assert margin.instrument_id == US100_ID
        assert margin.initial == Money(
            Decimal(position.usedMargin).scaleb(-position.moneyDigits), USD
        )
        assert margin.maintenance == margin.initial
        assert h.received(oa.ProtoOAOrderListByPositionIdReq)[0].positionId == FIRST


async def test_an_unreadable_leg_record_at_load_is_a_warning() -> None:
    execution_venue = ExecutionVenue()
    our_open_position(execution_venue)
    for order in execution_venue.position_orders[FIRST]:
        if order.tradeData.comment:
            order.tradeData.comment = "not a record"
    async with harness(execution_venue=execution_venue) as h:
        assert any("legs' record cannot be read" in line for line in h.logger.warnings())


async def test_the_models_prices_take_the_loaded_instruments_precision() -> None:
    async with harness() as h:
        assert h.client._price_precision(US100_SYMBOL_ID) == 2
        assert h.client._price_precision(324) is None


async def test_disconnect_releases_the_spots_and_the_account() -> None:
    async with harness() as h:
        await h.client._disconnect()

        assert [list(r.symbolId) for r in h.received(oa.ProtoOAUnsubscribeSpotsReq)] == [
            [US100_SYMBOL_ID]
        ]
        assert h.account.session is None


async def test_disconnect_releases_only_the_account_user_this_client_holds() -> None:
    execution_venue = ExecutionVenue()
    execution_venue.trader = trader(accountType=om.NETTED)
    async with harness(execution_venue=execution_venue, connect=False) as h:
        # Another client of the same account, which must keep its session.
        await h.account.connect()
        try:
            with pytest.raises(CTraderAccountError):
                await h.client._connect()
            await h.client._disconnect()
            assert h.account.session is not None

            execution_venue.trader = trader()
            await h.client._connect()
            await h.client._disconnect()
            await h.client._disconnect()
            assert h.account.session is not None
        finally:
            await h.account.disconnect()
        assert h.account.session is None


async def test_the_nodes_entry_and_legs_reach_nautilus_under_their_ids() -> None:
    async with harness() as h:
        await submitted(h)
        await push(h, *FIRST_EVENTS[:3])
        await wait_until(lambda: status(h, TARGET) == OrderStatus.ACCEPTED)

        assert status(h, ENTRY) == OrderStatus.FILLED
        assert status(h, STOP) == OrderStatus.ACCEPTED
        assert h.cache.order(ClientOrderId(ENTRY)).venue_order_id == VenueOrderId("6000001")
        assert h.cache.order(ClientOrderId(STOP)).venue_order_id == VenueOrderId("6000001-SL")
        assert h.cache.order(ClientOrderId(TARGET)).venue_order_id == VenueOrderId("6000001-TP")
        (filled,) = [e for e in h.events_of(ENTRY) if isinstance(e, OrderFilled)]
        assert filled.trade_id == TradeId("7000001")
        assert filled.last_px == Price.from_str("85287.21")
        assert filled.last_qty == Quantity.from_str("1.00")
        assert filled.commission == Money(Decimal("27.72"), USD)
        assert filled.position_id == PositionId("5000001")
        position = h.cache.position(PositionId("5000001"))
        assert position.is_open
        assert position.quantity == Quantity.from_str("1.00")
        assert h.logger.errors() == []


async def test_the_first_recorded_position_reaches_nautilus_step_by_step() -> None:
    async with harness() as h:
        await submitted(h)
        await push(h, *FIRST_EVENTS)
        await wait_until(lambda: status(h, STOP) == OrderStatus.FILLED)

        assert h.kinds_of(ENTRY) == ["OrderSubmitted", "OrderAccepted", "OrderFilled"]
        assert h.kinds_of(STOP) == [
            "OrderSubmitted",
            "OrderAccepted",
            "OrderUpdated",  # moved by hand
            "OrderUpdated",  # its quantity follows the partial close
            "OrderUpdated",  # moved by hand again
            "OrderFilled",
        ]
        assert h.kinds_of(TARGET) == ["OrderSubmitted", "OrderAccepted", "OrderCanceled"]
        stop = h.cache.order(ClientOrderId(STOP))
        assert stop.trigger_price == Price.from_str("85206.20")
        assert stop.filled_qty == Quantity.from_str("0.99")
        (stop_fill,) = [e for e in h.events_of(STOP) if isinstance(e, OrderFilled)]
        assert stop_fill.venue_order_id == VenueOrderId("6000001-SL")
        assert stop_fill.last_px == Price.from_str("85205.58")
        # The trader's partial close: reported as an external closing order, then filled.
        (report,) = h.reports
        assert report.venue_order_id == VenueOrderId("6000003")
        assert report.reduce_only
        assert report.order_status == OrderStatus.ACCEPTED
        external = h.cache.order(h.cache.client_order_id(VenueOrderId("6000003")))
        assert external.status == OrderStatus.FILLED
        assert external.is_reduce_only
        assert h.cache.position(PositionId("5000001")).is_closed
        assert [(a.kind, a.action) for a in h.activity] == [
            ("manual_change", "level_moved"),
            ("manual_change", "level_removed"),
            ("manual_change", "level_added"),
            ("manual_change", "partially_closed"),
            ("manual_change", "level_moved"),
        ]
        assert all(isinstance(a, CTraderAccountActivity) for a in h.activity)
        assert h.activity[3].symbol == "US100.cash"
        assert h.activity[3].volume == Decimal("0.01")
        assert h.logger.errors() == []


async def test_a_response_applied_after_a_later_event_reports_each_step_once() -> None:
    async with harness() as h:
        await submitted(h)
        accepted, filled, protected = FIRST_EVENTS[:3]
        # The transport hands buffered frames to the event handlers before the waiting request
        # resumes, so the order's acceptance can come after its fill.
        h.client._on_execution_event(filled)
        h.client._on_execution_event(accepted)
        h.client._on_execution_event(protected)
        h.client._on_execution_event(filled)
        await wait_until(lambda: status(h, TARGET) == OrderStatus.ACCEPTED)

        assert h.kinds_of(ENTRY) == ["OrderSubmitted", "OrderAccepted", "OrderFilled"]
        assert h.kinds_of(STOP) == ["OrderSubmitted", "OrderAccepted"]


async def test_events_arriving_while_the_model_is_rebuilt_are_applied_after_it() -> None:
    async with harness() as h:
        await submitted(h)
        held = HeldReplies(h.server, om.PROTO_OA_RECONCILE_REQ, lambda _r: h.venue.snapshot)
        await h.server.drop_connections()
        await asyncio.wait_for(held.arrived.wait(), timeout=10)

        for message in FIRST_EVENTS[:3]:
            await h.server.push(message)
        await held.stop_holding()
        await wait_until(lambda: status(h, TARGET) == OrderStatus.ACCEPTED, timeout_secs=10)

        assert status(h, ENTRY) == OrderStatus.FILLED
        # Applied before the rebuild, the events would be wiped by the older, empty snapshot.
        assert h.client._book.view(FIRST).open
        assert any("Rebuilding the venue model" in line for line in h.logger.warnings())


async def test_events_held_by_a_rebuild_the_client_left_are_dropped() -> None:
    async with harness() as h:
        await submitted(h)
        held = HeldReplies(h.server, om.PROTO_OA_RECONCILE_REQ, lambda _r: h.venue.snapshot)
        await h.server.drop_connections()
        await asyncio.wait_for(held.arrived.wait(), timeout=10)
        for message in FIRST_EVENTS[:3]:
            await h.server.push(message)
        await wait_until(lambda: len(h.client._buffer or []) == 3)

        await h.client._disconnect()
        await wait_until(lambda: h.client._buffer is None)

        assert h.kinds_of(ENTRY) == ["OrderSubmitted"]
        assert status(h, ENTRY) == OrderStatus.SUBMITTED


async def test_an_event_the_model_cannot_apply_is_an_error_and_the_client_carries_on() -> None:
    async with harness() as h:
        await submitted(h)
        accepted, filled, _ = FIRST_EVENTS[:3]
        broken = type(filled)()
        broken.CopyFrom(filled)
        broken.deal.executionPrice = float("nan")
        h.client._on_execution_event(accepted)
        h.client._on_execution_event(broken)
        h.client._on_execution_event(filled)
        await wait_until(lambda: status(h, ENTRY) == OrderStatus.FILLED)

        assert any("could not be applied" in line for line in h.logger.errors())
        assert h.cache.position(PositionId("5000001")).quantity == Quantity.from_str("1.00")


async def test_an_event_for_an_order_nautilus_does_not_hold_is_a_warning() -> None:
    async with harness() as h:
        await push(h, FIRST_EVENTS[0])

        assert any("No Nautilus order" in line for line in h.logger.warnings())
        assert h.events == []


async def test_a_record_of_an_unknown_type_is_a_warning() -> None:
    async with harness() as h:
        h.client._handle_records([object()])

        assert any("object" in line for line in h.logger.warnings())
        assert h.logger.errors() == []


async def test_an_order_error_nobody_waits_for_is_a_warning() -> None:
    async with harness() as h:
        await push(
            h,
            oa.ProtoOAOrderErrorEvent(
                ctidTraderAccountId=1, errorCode="POSITION_NOT_FOUND", description="gone"
            ),
        )

        assert any("POSITION_NOT_FOUND: gone" in line for line in h.logger.warnings())


async def test_activity_on_an_unloaded_symbol_is_published_with_its_name() -> None:
    async with harness() as h:
        order = make_order(6_900_001, 5_900_001, order_type=om.LIMIT, limit=1.1, symbol=1)
        await push(h, make_event(om.ORDER_ACCEPTED, order))

        assert h.activity == [
            CTraderAccountActivity(
                kind="unloaded_symbol",
                symbol="EURUSD",
                subject="order",
                side="BUY",
                volume=Decimal("1"),
                action="opened",
                ts_event=1_000_000,
                ts_init=h.activity[0].ts_init,
            ),
        ]
        assert h.events == []
        assert h.reports == []


def balance_of(state) -> Decimal:
    return state.balances[0].total.as_decimal()


def deposit(balance: int, version: int) -> oa.ProtoOAExecutionEvent:
    return oa.ProtoOAExecutionEvent(
        ctidTraderAccountId=ACCOUNT_ID,
        executionType=om.DEPOSIT_WITHDRAW,
        depositWithdraw=om.ProtoOADepositWithdraw(
            operationType=om.BALANCE_DEPOSIT,
            balanceHistoryId=version,
            balance=balance,
            delta=100,
            changeBalanceTimestamp=1_600_000_000_000 + version,
            balanceVersion=version,
            moneyDigits=2,
        ),
    )


async def test_the_account_follows_margins_and_closing_deals() -> None:
    async with harness() as h:
        await submitted(h)
        await push(h, *FIRST_EVENTS)
        await wait_until(lambda: h.cache.position(PositionId("5000001")).is_closed)

        opened = FIRST_EVENTS[1].position
        assert any(
            s.margins
            and s.margins[0].initial
            == Money(Decimal(opened.usedMargin).scaleb(-opened.moneyDigits), USD)
            for s in h.states
        )
        closing = [e.deal for e in FIRST_EVENTS if e.deal.HasField("closePositionDetail")]
        last = closing[-1].closePositionDetail
        assert balance_of(h.states[-1]) == Decimal(last.balance).scaleb(-last.moneyDigits)
        assert h.states[-1].margins == []
        assert h.states[-1].ts_event == closing[-1].executionTimestamp * 1_000_000


async def test_a_balance_older_than_the_one_held_is_ignored() -> None:
    async with harness() as h:
        await push(h, deposit(1_000_000, version=10), deposit(900_000, version=9))

        assert [balance_of(s) for s in h.states[1:]] == [Decimal("10000.00")]


async def test_the_same_balance_states_nothing_new() -> None:
    async with harness() as h:
        await push(h, deposit(1_000_000, version=10), deposit(1_000_000, version=11))

        assert len(h.states) == 2


async def test_a_trader_update_states_its_balance() -> None:
    async with harness() as h:
        updated = trader(balance=123_456, balanceVersion=50)
        await push(
            h,
            oa.ProtoOATraderUpdatedEvent(ctidTraderAccountId=ACCOUNT_ID, trader=updated.trader),
        )

        assert balance_of(h.states[-1]) == Decimal("1234.56")


async def test_a_margin_change_of_a_known_position_states_it() -> None:
    execution_venue = ExecutionVenue()
    our_open_position(execution_venue)
    async with harness(execution_venue=execution_venue) as h:
        await push(
            h,
            oa.ProtoOAMarginChangedEvent(
                ctidTraderAccountId=ACCOUNT_ID,
                positionId=FIRST,
                usedMargin=1_234,
                moneyDigits=2,
            ),
            oa.ProtoOAMarginChangedEvent(
                ctidTraderAccountId=ACCOUNT_ID,
                positionId=999,
                usedMargin=5_000,
                moneyDigits=2,
            ),
        )

        assert len(h.states) == 2
        assert h.states[-1].margins[0].initial == Money(Decimal("12.34"), USD)


async def test_a_query_reads_the_account_again() -> None:
    async with harness() as h:
        h.venue.trader = trader(balance=777_700, balanceVersion=99)
        await h.client._query_account(
            QueryAccount(
                trader_id=TRADER_ID,
                account_id=AccountId("CTRADER-001"),
                command_id=UUID4(),
                ts_init=0,
            ),
        )

        assert balance_of(h.states[-1]) == Decimal("7777.00")


async def test_a_query_answered_with_an_older_balance_never_takes_it_back() -> None:
    async with harness() as h:
        await push(h, deposit(1_000_000, version=10))
        h.venue.trader = trader(balance=777_700, balanceVersion=9)
        await h.client._query_account(
            QueryAccount(
                trader_id=TRADER_ID,
                account_id=AccountId("CTRADER-001"),
                command_id=UUID4(),
                ts_init=0,
            ),
        )

        assert balance_of(h.states[-1]) == Decimal("10000.00")


async def test_an_account_event_during_the_account_read_is_stated_once_the_account_is_known() -> (
    None
):
    async with harness(connect=False) as h:
        # Another user of the account brings it up, so the client's own trader read is the
        # first one held.
        await h.account.connect()
        try:
            await h.client.instrument_provider.initialize()
            held = HeldReplies(h.server, om.PROTO_OA_TRADER_REQ, lambda _r: h.venue.trader)
            connecting = asyncio.create_task(h.client._connect())
            await asyncio.wait_for(held.arrived.wait(), timeout=5)
            await h.server.push(deposit(1_000_000, version=10))
            await held.release()
            await held.stop_holding()
            await connecting

            assert h.logger.errors() == []
            assert all(state.base_currency == USD for state in h.states)
            assert balance_of(h.states[-1]) == Decimal("10000.00")
        finally:
            await h.account.disconnect()


# The recorded entry filled at 85287.21: an ask there gives the recorded distances.
BID, ASK = 8_528_600_000, 8_528_721_000
MARKET_ID, CLOSE_ID = "O-M-1", "O-C-1"
MARKET_POSITION = 5_100_001


def at(position: om.ProtoOAPosition, utc: int) -> om.ProtoOAPosition:
    position.utcLastUpdateTimestamp = utc
    return position


def market_events() -> list:
    """A plain market order of the node, accepted then filled (hand-built)."""

    def order(utc: int) -> om.ProtoOAOrder:
        return make_order(
            6_100_001,
            MARKET_POSITION,
            utc=utc,
            label=order_record.encode_label(MARKET_ID),
            comment=order_record.encode_comment(LegIds(None, None)),
            client_order_id=MARKET_ID,
        )

    return on_us100(
        [
            make_event(
                om.ORDER_ACCEPTED,
                order(10),
                position=make_position(
                    MARKET_POSITION, volume=0, status=om.POSITION_STATUS_CREATED
                ),
            ),
            make_event(
                om.ORDER_FILLED,
                order(20),
                position=at(make_position(MARKET_POSITION), 20),
                deal=make_deal(
                    7_100_001,
                    6_100_001,
                    MARKET_POSITION,
                    side=om.BUY,
                    volume=100,
                    price=85250.0,
                    ts=20,
                    commission=-2770,
                ),
            ),
        ],
    )


def close_events() -> list:
    """The node's close of that position, accepted then filled (hand-built)."""

    def order(utc: int) -> om.ProtoOAOrder:
        return make_order(6_100_002, MARKET_POSITION, side=om.SELL, closing=True, utc=utc)

    deal = make_deal(
        7_100_002,
        6_100_002,
        MARKET_POSITION,
        side=om.SELL,
        volume=100,
        price=85260.0,
        ts=40,
        commission=-2770,
    )
    deal.closePositionDetail.CopyFrom(
        om.ProtoOAClosePositionDetail(
            entryPrice=85250.0,
            grossProfit=1000,
            swap=0,
            commission=-5540,
            balance=1_000_000,
            balanceVersion=100,
            moneyDigits=2,
        ),
    )
    return on_us100(
        [
            make_event(
                om.ORDER_ACCEPTED, order(30), position=at(make_position(MARKET_POSITION), 20)
            ),
            make_event(
                om.ORDER_FILLED,
                order(40),
                position=at(
                    make_position(MARKET_POSITION, volume=0, status=om.POSITION_STATUS_CLOSED), 40
                ),
                deal=deal,
            ),
        ],
    )


def market(
    h,
    *,
    side: OrderSide = OrderSide.BUY,
    client_order_id: str = MARKET_ID,
    reduce_only: bool = False,
    time_in_force: TimeInForce = TimeInForce.GTC,
):
    return h.factory.market(
        US100_ID,
        side,
        Quantity.from_str("1.00"),
        time_in_force=time_in_force,
        reduce_only=reduce_only,
        client_order_id=ClientOrderId(client_order_id),
    )


async def submit(h, order, position_id: PositionId | None = None) -> None:
    h.cache.add_order(order, position_id=position_id)
    await h.client._submit_order(
        SubmitOrder(
            trader_id=TRADER_ID,
            strategy_id=STRATEGY_ID,
            order=order,
            command_id=UUID4(),
            ts_init=0,
            position_id=position_id,
        ),
    )


def rejection(h, client_order_id: str) -> str:
    (event,) = [e for e in h.events_of(client_order_id) if isinstance(e, OrderRejected)]
    return event.reason


def answered(answer) -> ExecutionVenue:
    """A venue answering every new order with `answer`."""
    execution_venue = ExecutionVenue()
    execution_venue.server.on(om.PROTO_OA_NEW_ORDER_REQ, lambda _r: answer)
    return execution_venue


async def test_a_bracket_goes_out_as_one_market_order_measured_from_the_ask() -> None:
    async with harness(execution_venue=answered(FIRST_EVENTS[:3])) as h:
        await push_spot(h, BID, ASK)
        await submit_bracket(h, bracket(h))
        await wait_until(lambda: status(h, TARGET) == OrderStatus.ACCEPTED)

        (request,) = h.received(oa.ProtoOANewOrderReq)
        assert request.orderType == om.MARKET
        assert request.timeInForce == om.IMMEDIATE_OR_CANCEL
        assert request.tradeSide == om.BUY
        assert request.volume == 100
        assert request.clientOrderId == ENTRY
        assert request.label == order_record.encode_label(ENTRY)
        assert request.comment == order_record.encode_comment(LegIds(STOP, TARGET))
        assert request.relativeStopLoss == 9_001_000
        assert request.relativeTakeProfit == 10_001_000
        assert status(h, ENTRY) == OrderStatus.FILLED
        assert status(h, STOP) == OrderStatus.ACCEPTED


async def test_a_market_order_goes_out_without_levels_and_opens_a_position() -> None:
    async with harness(execution_venue=answered(market_events())) as h:
        await submit(h, market(h))
        await wait_until(lambda: status(h, MARKET_ID) == OrderStatus.FILLED)

        (request,) = h.received(oa.ProtoOANewOrderReq)
        assert not request.HasField("relativeStopLoss")
        assert not request.HasField("relativeTakeProfit")
        assert request.timeInForce == om.IMMEDIATE_OR_CANCEL
        assert h.cache.position(PositionId(str(MARKET_POSITION))).is_open


async def test_a_bracket_without_a_fresh_price_is_refused_before_sending() -> None:
    async with harness() as h:
        await submit_bracket(h, bracket(h))
        await wait_until(lambda: status(h, TARGET) == OrderStatus.CANCELED)

        assert "no price" in rejection(h, ENTRY)
        assert h.kinds_of(ENTRY) == ["OrderRejected"]
        assert h.kinds_of(STOP) == ["OrderCanceled"]
        assert h.received(oa.ProtoOANewOrderReq) == []


async def test_a_stale_price_measures_nothing() -> None:
    async with harness(config=exec_config(reference_price_max_age_secs=0.05)) as h:
        await push_spot(h, BID, ASK)
        await asyncio.sleep(0.1)
        await submit_bracket(h, bracket(h))
        await wait_until(lambda: status(h, ENTRY) == OrderStatus.REJECTED)

        assert "no price" in rejection(h, ENTRY)


async def test_a_level_on_the_wrong_side_of_the_market_is_refused() -> None:
    async with harness() as h:
        await push_spot(h, BID, ASK)
        await submit_bracket(h, bracket(h, stop_price="85300.00"))
        await wait_until(lambda: status(h, ENTRY) == OrderStatus.REJECTED)

        assert "wrong side" in rejection(h, ENTRY)
        assert h.received(oa.ProtoOANewOrderReq) == []


async def test_a_fill_or_kill_market_order_is_refused() -> None:
    async with harness() as h:
        await submit(h, market(h, time_in_force=TimeInForce.FOK))
        await wait_until(lambda: status(h, MARKET_ID) == OrderStatus.REJECTED)

        assert "FOK" in rejection(h, MARKET_ID)


async def test_an_order_is_refused_unsent_while_the_connection_is_down() -> None:
    async with harness() as h:
        await h.server.drop_connections()
        await wait_until(lambda: not h.account.session.is_ready)
        await submit(h, market(h))
        await wait_until(lambda: status(h, MARKET_ID) == OrderStatus.REJECTED)

        assert rejection(h, MARKET_ID) == "not connected to the venue"
        assert h.kinds_of(MARKET_ID) == ["OrderRejected"]
        assert h.received(oa.ProtoOANewOrderReq) == []


async def test_a_bracket_is_refused_unsent_while_the_connection_is_down() -> None:
    async with harness() as h:
        await push_spot(h, BID, ASK)
        await h.server.drop_connections()
        await wait_until(lambda: not h.account.session.is_ready)
        await submit_bracket(h, bracket(h))
        await wait_until(lambda: status(h, TARGET) == OrderStatus.CANCELED)

        assert rejection(h, ENTRY) == "not connected to the venue"
        assert h.kinds_of(ENTRY) == ["OrderRejected"]
        assert h.kinds_of(STOP) == ["OrderCanceled"]
        assert h.kinds_of(TARGET) == ["OrderCanceled"]
        assert h.received(oa.ProtoOANewOrderReq) == []
        assert len(h.client._brackets) == 0


@pytest.mark.parametrize(
    "answer",
    [
        oa.ProtoOAOrderErrorEvent(
            ctidTraderAccountId=ACCOUNT_ID,
            errorCode="NOT_ENOUGH_MONEY",
            description="Not enough money",
        ),
        oa.ProtoOAErrorRes(
            ctidTraderAccountId=ACCOUNT_ID,
            errorCode="NOT_ENOUGH_MONEY",
            description="Not enough money",
        ),
    ],
)
async def test_a_bracket_the_broker_refuses_is_one_rejection(answer) -> None:
    async with harness(execution_venue=answered(answer)) as h:
        await push_spot(h, BID, ASK)
        await submit_bracket(h, bracket(h))
        await wait_until(lambda: status(h, TARGET) == OrderStatus.CANCELED)

        assert rejection(h, ENTRY) == "NOT_ENOUGH_MONEY: Not enough money"
        assert h.kinds_of(STOP) == ["OrderSubmitted", "OrderCanceled"]
        assert h.kinds_of(TARGET) == ["OrderSubmitted", "OrderCanceled"]
        assert len(h.client._brackets) == 0


async def test_an_order_without_an_answer_is_never_sent_again() -> None:
    async with harness() as h:
        h.server.close_after_next_request = True
        await submit(h, market(h))
        await wait_until(lambda: h.account.session.is_ready, timeout_secs=10)
        await sync(h)

        assert len(h.received(oa.ProtoOANewOrderReq)) == 1
        assert status(h, MARKET_ID) == OrderStatus.SUBMITTED
        assert any("not resent" in line for line in h.logger.warnings())


async def test_an_order_waits_for_its_answer_as_long_as_configured() -> None:
    # The venue never answers a new order.
    async with harness(config=exec_config(order_request_timeout_secs=0.3)) as h:
        loop = asyncio.get_running_loop()
        started = loop.time()
        await submit(h, market(h))
        waited = loop.time() - started

        assert 0.3 <= waited < 2.0
        assert status(h, MARKET_ID) == OrderStatus.SUBMITTED
        assert any("not resent" in line for line in h.logger.warnings())


def closing_venue() -> ExecutionVenue:
    """A venue that opens the market order, and answers a close with its acceptance only.

    Answered together, the fill can reach the client before the acceptance, which then adds
    nothing; the test pushes the fill itself to fix the order.
    """
    execution_venue = answered(market_events())
    execution_venue.server.on(om.PROTO_OA_CLOSE_POSITION_REQ, lambda _r: close_events()[0])
    return execution_venue


async def opened_market_position(h) -> None:
    await submit(h, market(h))
    await wait_until(lambda: h.cache.position(PositionId(str(MARKET_POSITION))) is not None)


def close_order(h, side: OrderSide = OrderSide.SELL):
    return market(h, side=side, client_order_id=CLOSE_ID, reduce_only=True)


async def test_a_close_names_its_position_and_fills_under_its_own_id() -> None:
    async with harness(execution_venue=closing_venue()) as h:
        await opened_market_position(h)
        await submit(h, close_order(h), position_id=PositionId(str(MARKET_POSITION)))
        await push(h, close_events()[1])
        await wait_until(lambda: status(h, CLOSE_ID) == OrderStatus.FILLED)

        (request,) = h.received(oa.ProtoOAClosePositionReq)
        assert request.positionId == MARKET_POSITION
        assert request.volume == 100
        assert h.kinds_of(CLOSE_ID) == ["OrderSubmitted", "OrderAccepted", "OrderFilled"]
        assert h.cache.position(PositionId(str(MARKET_POSITION))).is_closed
        assert h.client._operations.closing(MARKET_POSITION, 100) is None
        # The node's own close is never reported as somebody else's order.
        assert h.reports == []


async def test_a_close_without_its_position_is_refused() -> None:
    async with harness() as h:
        await submit(h, close_order(h))
        await wait_until(lambda: status(h, CLOSE_ID) == OrderStatus.REJECTED)

        assert "must name its position" in rejection(h, CLOSE_ID)


async def test_a_close_of_a_position_not_open_at_the_venue_is_refused() -> None:
    async with harness() as h:
        await submit(h, close_order(h), position_id=PositionId(str(MARKET_POSITION)))
        await wait_until(lambda: status(h, CLOSE_ID) == OrderStatus.REJECTED)

        assert "not open at the venue" in rejection(h, CLOSE_ID)
        assert h.received(oa.ProtoOAClosePositionReq) == []


async def test_a_close_is_refused_unsent_while_the_connection_is_down() -> None:
    async with harness(execution_venue=closing_venue()) as h:
        await opened_market_position(h)
        await h.server.drop_connections()
        await wait_until(lambda: not h.account.session.is_ready)
        await submit(h, close_order(h), position_id=PositionId(str(MARKET_POSITION)))
        await wait_until(lambda: status(h, CLOSE_ID) == OrderStatus.REJECTED)

        assert rejection(h, CLOSE_ID) == "not connected to the venue"
        assert h.kinds_of(CLOSE_ID) == ["OrderRejected"]
        assert h.received(oa.ProtoOAClosePositionReq) == []
        assert h.client._operations.closing(MARKET_POSITION, 100) is None


async def test_a_close_that_would_add_to_the_position_is_refused() -> None:
    async with harness(execution_venue=closing_venue()) as h:
        await opened_market_position(h)
        await submit(
            h, close_order(h, side=OrderSide.BUY), position_id=PositionId(str(MARKET_POSITION))
        )
        await wait_until(lambda: status(h, CLOSE_ID) == OrderStatus.REJECTED)

        assert "would not reduce" in rejection(h, CLOSE_ID)


async def test_a_close_the_broker_refuses_is_rejected_and_no_longer_in_flight() -> None:
    execution_venue = answered(market_events())
    execution_venue.server.on(
        om.PROTO_OA_CLOSE_POSITION_REQ,
        lambda _r: oa.ProtoOAOrderErrorEvent(
            ctidTraderAccountId=ACCOUNT_ID, errorCode="POSITION_NOT_FOUND", description="gone"
        ),
    )
    async with harness(execution_venue=execution_venue) as h:
        await opened_market_position(h)
        await submit(h, close_order(h), position_id=PositionId(str(MARKET_POSITION)))
        await wait_until(lambda: status(h, CLOSE_ID) == OrderStatus.REJECTED)

        assert rejection(h, CLOSE_ID) == "POSITION_NOT_FOUND: gone"
        assert h.client._operations.closing(MARKET_POSITION, 100) is None


AMEND_FROM = 1_600_000_200_000
REFUSED_STOPS = oa.ProtoOAOrderErrorEvent(
    ctidTraderAccountId=ACCOUNT_ID, errorCode="TRADING_BAD_STOPS", description="Invalid stops"
)


def protective(
    stop: float | None,
    limit: float | None,
    *,
    utc: int,
    kind: int = om.ORDER_REPLACED,
) -> oa.ProtoOAExecutionEvent:
    """The first position's protective order as the broker answers an amend (hand-built)."""
    event = type(FIRST_EVENTS[2])()
    event.CopyFrom(FIRST_EVENTS[2])
    event.executionType = kind
    event.isServerEvent = False
    event.order.ClearField("stopPrice")
    event.order.ClearField("limitPrice")
    if stop is not None:
        event.order.stopPrice = stop
    if limit is not None:
        event.order.limitPrice = limit
    event.order.utcLastUpdateTimestamp = utc
    event.position.utcLastUpdateTimestamp = utc
    return event


def amend_echo(amends: list, *, kind: int = om.ORDER_REPLACED):
    """The broker's answer setting whatever levels an amend asks for; keeps each in `amends`."""

    def answer(request):
        amends.append(request)
        stop = request.stopLoss if request.HasField("stopLoss") else None
        limit = request.takeProfit if request.HasField("takeProfit") else None
        utc = AMEND_FROM + len(amends)
        if stop is None and limit is None:
            return protective(None, None, utc=utc, kind=om.ORDER_CANCELLED)
        return protective(stop, limit, utc=utc, kind=kind)

    return answer


def echo_amends(execution_venue: ExecutionVenue, *, kind: int = om.ORDER_REPLACED) -> list:
    """The broker sets whatever levels an amend asks for; returns the amends it received."""
    amends: list = []
    execution_venue.server.on(om.PROTO_OA_AMEND_POSITION_SLTP_REQ, amend_echo(amends, kind=kind))
    return amends


def cancel(client_order_id: str) -> CancelOrder:
    return CancelOrder(
        trader_id=TRADER_ID,
        strategy_id=STRATEGY_ID,
        instrument_id=US100_ID,
        client_order_id=ClientOrderId(client_order_id),
        venue_order_id=None,
        command_id=UUID4(),
        ts_init=0,
    )


def modify(
    client_order_id: str,
    *,
    price: str | None = None,
    trigger_price: str | None = None,
    quantity: str | None = None,
) -> ModifyOrder:
    return ModifyOrder(
        trader_id=TRADER_ID,
        strategy_id=STRATEGY_ID,
        instrument_id=US100_ID,
        client_order_id=ClientOrderId(client_order_id),
        venue_order_id=None,
        quantity=None if quantity is None else Quantity.from_str(quantity),
        price=None if price is None else Price.from_str(price),
        trigger_price=None if trigger_price is None else Price.from_str(trigger_price),
        command_id=UUID4(),
        ts_init=0,
    )


def last_kind(h, client_order_id: str) -> str | None:
    kinds = h.kinds_of(client_order_id)
    return kinds[-1] if kinds else None


async def opened_bracket(h, **levels) -> None:
    await push_spot(h, BID, ASK)
    await submit_bracket(h, bracket(h, **levels))
    await wait_until(
        lambda: (
            status(h, STOP) == OrderStatus.ACCEPTED and status(h, TARGET) == OrderStatus.ACCEPTED
        ),
    )
    await sync(h)


async def test_the_levels_are_set_exactly_once_the_protective_order_has_come() -> None:
    execution_venue = answered(FIRST_EVENTS[:3])
    amends = echo_amends(execution_venue)
    async with harness(execution_venue=execution_venue) as h:
        await push_spot(h, BID, ASK)
        await submit_bracket(h, bracket(h, stop_price="85190.00", target_price="85400.00"))
        await wait_until(lambda: last_kind(h, TARGET) == "OrderUpdated")

        (amend,) = amends
        assert amend.positionId == FIRST
        assert amend.stopLoss == 85190.0
        assert amend.takeProfit == 85400.0
        assert h.kinds_of(STOP) == ["OrderSubmitted", "OrderAccepted", "OrderUpdated"]
        assert h.client._book.view(FIRST).levels == {
            Level.STOP_LOSS: Decimal("85190.00"),
            Level.TAKE_PROFIT: Decimal("85400.00"),
        }
        assert len(h.client._brackets) == 0
        # The node's own amend is no trader's change.
        assert h.activity == []


async def test_no_amend_when_the_broker_set_the_levels_asked_for() -> None:
    execution_venue = answered(FIRST_EVENTS[:3])
    amends = echo_amends(execution_venue)
    async with harness(execution_venue=execution_venue) as h:
        await opened_bracket(h)

        assert amends == []
        assert len(h.client._brackets) == 0


async def test_a_refused_correction_leaves_each_leg_where_the_broker_holds_it() -> None:
    execution_venue = answered(FIRST_EVENTS[:3])
    execution_venue.server.on(om.PROTO_OA_AMEND_POSITION_SLTP_REQ, lambda _r: REFUSED_STOPS)
    async with harness(execution_venue=execution_venue) as h:
        await push_spot(h, BID, ASK)
        await submit_bracket(h, bracket(h, stop_price="85190.00", target_price="85400.00"))
        await wait_until(lambda: last_kind(h, TARGET) == "OrderUpdated")

        # The engine applies the events it is given from a queue, after they are recorded.
        await wait_until(
            lambda: (
                h.cache.order(ClientOrderId(STOP)).trigger_price == Price.from_str("85197.20")
                and h.cache.order(ClientOrderId(TARGET)).price == Price.from_str("85387.22")
            ),
        )
        assert any("stay where the broker set them" in line for line in h.logger.warnings())


async def in_flight(h, execution_venue_held: HeldReplies) -> asyncio.Task:
    """A bracket sent and still unanswered."""
    await push_spot(h, BID, ASK)
    task = asyncio.create_task(submit_bracket(h, bracket(h)))
    await asyncio.wait_for(execution_venue_held.arrived.wait(), timeout=5)
    return task


async def test_a_leg_cancelled_while_its_entry_is_in_flight_goes_with_the_correction() -> None:
    execution_venue = ExecutionVenue()
    held = HeldReplies(
        execution_venue.server, om.PROTO_OA_NEW_ORDER_REQ, lambda _r: FIRST_EVENTS[0]
    )
    amends = echo_amends(execution_venue)
    async with harness(execution_venue=execution_venue) as h:
        sending = await in_flight(h, held)
        await h.client._cancel_order(cancel(TARGET))
        assert amends == []

        await held.release()
        await sending
        await push(h, *FIRST_EVENTS[1:3])
        await wait_until(lambda: status(h, TARGET) == OrderStatus.CANCELED)

        (amend,) = amends
        assert amend.stopLoss == 85197.2
        assert not amend.HasField("takeProfit")
        assert h.kinds_of(TARGET) == ["OrderSubmitted", "OrderAccepted", "OrderCanceled"]
        assert status(h, STOP) == OrderStatus.ACCEPTED


async def test_a_leg_modified_while_its_entry_is_in_flight_goes_with_the_correction() -> None:
    execution_venue = ExecutionVenue()
    held = HeldReplies(
        execution_venue.server, om.PROTO_OA_NEW_ORDER_REQ, lambda _r: FIRST_EVENTS[0]
    )
    amends = echo_amends(execution_venue)
    async with harness(execution_venue=execution_venue) as h:
        sending = await in_flight(h, held)
        await h.client._modify_order(modify(STOP, trigger_price="85150.00"))

        await held.release()
        await sending
        await push(h, *FIRST_EVENTS[1:3])
        await wait_until(lambda: last_kind(h, STOP) == "OrderUpdated")

        (amend,) = amends
        assert amend.stopLoss == 85150.0
        assert amend.takeProfit == 85387.22
        await wait_until(
            lambda: h.cache.order(ClientOrderId(STOP)).trigger_price == Price.from_str("85150.00"),
        )


async def test_a_modify_waiting_on_a_rejected_entry_is_rejected_before_the_leg_is_cancelled() -> (
    None
):
    execution_venue = ExecutionVenue()
    refusal = oa.ProtoOAOrderErrorEvent(
        ctidTraderAccountId=ACCOUNT_ID, errorCode="NOT_ENOUGH_MONEY", description="Not enough"
    )
    held = HeldReplies(execution_venue.server, om.PROTO_OA_NEW_ORDER_REQ, lambda _r: refusal)
    async with harness(execution_venue=execution_venue) as h:
        sending = await in_flight(h, held)
        await h.client._modify_order(modify(STOP, trigger_price="85150.00"))

        await held.release()
        await sending
        await wait_until(lambda: status(h, STOP) == OrderStatus.CANCELED)

        assert h.kinds_of(STOP) == ["OrderSubmitted", "OrderModifyRejected", "OrderCanceled"]


def closed_by_hand() -> oa.ProtoOAExecutionEvent:
    """The first position closed whole by hand, with no level set yet (hand-built)."""
    event = type(FIRST_EVENTS[7])()
    event.CopyFrom(FIRST_EVENTS[7])
    event.order.tradeData.volume = 100
    event.order.executedVolume = 100
    event.deal.volume = 100
    event.deal.filledVolume = 100
    event.deal.closePositionDetail.closedVolume = 100
    event.position.tradeData.volume = 0
    event.position.positionStatus = om.POSITION_STATUS_CLOSED
    event.position.ClearField("stopLoss")
    event.position.ClearField("takeProfit")
    return event


async def test_a_bracket_whose_position_closed_before_its_correction_is_dropped() -> None:
    execution_venue = ExecutionVenue()
    held = HeldReplies(
        execution_venue.server, om.PROTO_OA_NEW_ORDER_REQ, lambda _r: FIRST_EVENTS[0]
    )
    config = exec_config(protective_order_timeout_secs=30.0)
    async with harness(execution_venue=execution_venue, config=config) as h:
        sending = await in_flight(h, held)
        await h.client._modify_order(modify(STOP, trigger_price="85150.00"))
        await held.release()
        await sending

        await push(h, FIRST_EVENTS[1], closed_by_hand())
        await wait_until(lambda: "OrderModifyRejected" in h.kinds_of(STOP))

        assert len(h.client._brackets) == 0
        (rejected,) = [e for e in h.events_of(STOP) if isinstance(e, OrderModifyRejected)]
        assert rejected.reason == "the position closed before its levels were set"
        assert status(h, STOP) == OrderStatus.CANCELED
        assert status(h, TARGET) == OrderStatus.CANCELED
        assert h.received(oa.ProtoOAAmendPositionSLTPReq) == []


async def test_a_leg_is_cancelled_by_removing_its_level_alone() -> None:
    execution_venue = answered(FIRST_EVENTS[:3])
    amends = echo_amends(execution_venue)
    async with harness(execution_venue=execution_venue) as h:
        await opened_bracket(h)
        await h.client._cancel_order(cancel(STOP))
        await wait_until(lambda: status(h, STOP) == OrderStatus.CANCELED)

        (amend,) = amends
        assert not amend.HasField("stopLoss")
        assert amend.takeProfit == 85387.22
        assert status(h, TARGET) == OrderStatus.ACCEPTED

        await h.client._cancel_order(cancel(STOP))
        await wait_until(lambda: last_kind(h, STOP) == "OrderCancelRejected")
        assert len(amends) == 1


async def test_a_leg_is_moved_by_an_amend_keeping_the_other_level() -> None:
    execution_venue = answered(FIRST_EVENTS[:3])
    amends = echo_amends(execution_venue)
    async with harness(execution_venue=execution_venue) as h:
        await opened_bracket(h)
        await h.client._modify_order(modify(TARGET, price="85400.00"))
        await wait_until(lambda: last_kind(h, TARGET) == "OrderUpdated")

        (amend,) = amends
        assert amend.stopLoss == 85197.2
        assert amend.takeProfit == 85400.0
        await wait_until(
            lambda: h.cache.order(ClientOrderId(TARGET)).price == Price.from_str("85400.00"),
        )


async def test_a_legs_quantity_follows_its_position_and_cannot_be_set() -> None:
    execution_venue = answered(FIRST_EVENTS[:3])
    amends = echo_amends(execution_venue)
    async with harness(execution_venue=execution_venue) as h:
        await opened_bracket(h)
        await h.client._modify_order(modify(TARGET, quantity="1.00"))
        await wait_until(lambda: last_kind(h, TARGET) == "OrderUpdated")
        await h.client._modify_order(modify(TARGET, quantity="0.50"))
        await wait_until(lambda: last_kind(h, TARGET) == "OrderModifyRejected")

        assert amends == []


async def test_a_market_entry_cannot_be_cancelled() -> None:
    async with harness(execution_venue=answered(FIRST_EVENTS[:3])) as h:
        await opened_bracket(h)
        await h.client._cancel_order(cancel(ENTRY))
        await wait_until(lambda: last_kind(h, ENTRY) == "OrderCancelRejected")

        (event,) = [e for e in h.events_of(ENTRY) if type(e).__name__ == "OrderCancelRejected"]
        assert "market order" in event.reason


async def test_a_cancel_the_broker_refuses_is_rejected_and_the_level_stays() -> None:
    execution_venue = answered(FIRST_EVENTS[:3])
    execution_venue.server.on(om.PROTO_OA_AMEND_POSITION_SLTP_REQ, lambda _r: REFUSED_STOPS)
    async with harness(execution_venue=execution_venue) as h:
        await opened_bracket(h)
        await h.client._cancel_order(cancel(STOP))
        await wait_until(lambda: last_kind(h, STOP) == "OrderCancelRejected")

        assert status(h, STOP) == OrderStatus.ACCEPTED
        assert Level.STOP_LOSS in h.client._book.view(FIRST).levels


def keep_levels(execution_venue: ExecutionVenue) -> list:
    """The broker answers every amend by replacing the protective order, its levels unchanged."""
    amends: list = []

    def answer(request):
        amends.append(request)
        return protective(85197.2, 85387.22, utc=AMEND_FROM + len(amends))

    execution_venue.server.on(om.PROTO_OA_AMEND_POSITION_SLTP_REQ, answer)
    return amends


async def test_a_cancel_whose_level_the_broker_keeps_is_rejected() -> None:
    execution_venue = answered(FIRST_EVENTS[:3])
    amends = keep_levels(execution_venue)
    async with harness(execution_venue=execution_venue) as h:
        await opened_bracket(h)
        await h.client._cancel_order(cancel(STOP))
        await wait_until(lambda: last_kind(h, STOP) == "OrderCancelRejected")

        assert len(amends) == 1
        assert "OrderCanceled" not in h.kinds_of(STOP)
        assert status(h, STOP) == OrderStatus.ACCEPTED


async def test_a_modify_whose_level_the_broker_keeps_is_rejected() -> None:
    execution_venue = answered(FIRST_EVENTS[:3])
    amends = keep_levels(execution_venue)
    async with harness(execution_venue=execution_venue) as h:
        await opened_bracket(h)
        await h.client._modify_order(modify(TARGET, price="85400.00"))
        await wait_until(lambda: last_kind(h, TARGET) == "OrderModifyRejected")

        assert len(amends) == 1
        (event,) = [e for e in h.events_of(TARGET) if type(e).__name__ == "OrderModifyRejected"]
        assert "85387.22" in event.reason
        assert "OrderUpdated" not in h.kinds_of(TARGET)
        assert h.cache.order(ClientOrderId(TARGET)).price == Price.from_str("85387.22")


async def test_cancel_all_removes_a_positions_levels_in_one_amend() -> None:
    execution_venue = answered(FIRST_EVENTS[:3])
    amends = echo_amends(execution_venue)
    async with harness(execution_venue=execution_venue) as h:
        await opened_bracket(h)
        await h.client._cancel_all_orders(
            CancelAllOrders(
                trader_id=TRADER_ID,
                strategy_id=STRATEGY_ID,
                instrument_id=US100_ID,
                order_side=OrderSide.NO_ORDER_SIDE,
                command_id=UUID4(),
                ts_init=0,
            ),
        )
        await wait_until(
            lambda: (
                status(h, STOP) == OrderStatus.CANCELED
                and status(h, TARGET) == OrderStatus.CANCELED
            ),
        )

        (amend,) = amends
        assert not amend.HasField("stopLoss")
        assert not amend.HasField("takeProfit")


async def test_cancel_all_answers_an_order_both_open_and_in_flight_once() -> None:
    async with harness(execution_venue=answered(market_events()[0])) as h:
        await submit(h, market(h))
        await wait_until(lambda: status(h, MARKET_ID) == OrderStatus.ACCEPTED)
        order = h.cache.order(ClientOrderId(MARKET_ID))
        h.engine.process(
            OrderPendingCancel(
                TRADER_ID,
                STRATEGY_ID,
                US100_ID,
                order.client_order_id,
                order.venue_order_id,
                ACCOUNT,
                UUID4(),
                0,
                0,
            ),
        )
        await wait_until(lambda: status(h, MARKET_ID) == OrderStatus.PENDING_CANCEL)

        await h.client._cancel_all_orders(
            CancelAllOrders(
                trader_id=TRADER_ID,
                strategy_id=STRATEGY_ID,
                instrument_id=US100_ID,
                order_side=OrderSide.NO_ORDER_SIDE,
                command_id=UUID4(),
                ts_init=0,
            ),
        )

        assert h.kinds_of(MARKET_ID).count("OrderCancelRejected") == 1


async def test_a_batch_cancel_removes_a_positions_levels_in_one_amend() -> None:
    execution_venue = answered(FIRST_EVENTS[:3])
    amends = echo_amends(execution_venue)
    async with harness(execution_venue=execution_venue) as h:
        await opened_bracket(h)
        await h.client._batch_cancel_orders(
            BatchCancelOrders(
                trader_id=TRADER_ID,
                strategy_id=STRATEGY_ID,
                instrument_id=US100_ID,
                cancels=[cancel(STOP), cancel(TARGET)],
                command_id=UUID4(),
                ts_init=0,
            ),
        )
        await wait_until(
            lambda: (
                status(h, STOP) == OrderStatus.CANCELED
                and status(h, TARGET) == OrderStatus.CANCELED
            ),
        )

        assert len(amends) == 1


async def test_two_amends_of_one_position_never_undo_each_other() -> None:
    execution_venue = answered(FIRST_EVENTS[:3])
    amends = echo_amends(execution_venue)
    async with harness(execution_venue=execution_venue) as h:
        await opened_bracket(h)
        await asyncio.gather(
            h.client._cancel_order(cancel(STOP)),
            h.client._modify_order(modify(TARGET, price="85400.00")),
        )
        await wait_until(
            lambda: (
                status(h, STOP) == OrderStatus.CANCELED and last_kind(h, TARGET) == "OrderUpdated"
            ),
        )

        assert len(amends) == 2
        assert not amends[-1].HasField("stopLoss")
        assert amends[-1].takeProfit == 85400.0


async def test_missing_protection_is_set_by_an_amend_once_the_wait_ends() -> None:
    execution_venue = answered(FIRST_EVENTS[:2])  # no protective order follows the fill
    amends = echo_amends(execution_venue, kind=om.ORDER_ACCEPTED)
    config = exec_config(protective_order_timeout_secs=0.05)
    async with harness(execution_venue=execution_venue, config=config) as h:
        await push_spot(h, BID, ASK)
        await submit_bracket(h, bracket(h))
        await wait_until(
            lambda: (
                status(h, STOP) == OrderStatus.ACCEPTED
                and status(h, TARGET) == OrderStatus.ACCEPTED
            ),
        )

        (amend,) = amends
        assert amend.stopLoss == 85197.2
        assert amend.takeProfit == 85387.22
        assert any("No protective order followed" in line for line in h.logger.warnings())
        assert h.client._protection_timers == set()


async def test_a_positions_amend_lock_goes_once_the_position_has_closed() -> None:
    execution_venue = answered(FIRST_EVENTS[:3])
    echo_amends(execution_venue)
    async with harness(execution_venue=execution_venue) as h:
        await opened_bracket(h)
        await h.client._modify_order(modify(TARGET, price="85400.00"))
        assert FIRST in h.client._amend_locks

        await push(h, *FIRST_EVENTS[3:])

        assert not h.client._book.view(FIRST).open
        assert FIRST not in h.client._amend_locks


async def test_levels_the_broker_refuses_after_the_wait_reject_the_legs_with_an_error() -> None:
    execution_venue = answered(FIRST_EVENTS[:2])
    execution_venue.server.on(om.PROTO_OA_AMEND_POSITION_SLTP_REQ, lambda _r: REFUSED_STOPS)
    config = exec_config(protective_order_timeout_secs=0.05)
    async with harness(execution_venue=execution_venue, config=config) as h:
        await push_spot(h, BID, ASK)
        await submit_bracket(h, bracket(h))
        await wait_until(
            lambda: (
                status(h, STOP) == OrderStatus.REJECTED
                and status(h, TARGET) == OrderStatus.REJECTED
            ),
        )

        assert any("stands without its levels" in line for line in h.logger.errors())


async def test_an_amend_without_an_answer_is_sent_again_once_reconnected() -> None:
    execution_venue = answered(FIRST_EVENTS[:3])
    amends = echo_amends(execution_venue)
    async with harness(execution_venue=execution_venue) as h:
        await opened_bracket(h)
        # What the broker holds after the reconnect, for the model's rebuild.
        our_open_position(execution_venue)
        h.server.close_after_next_request = True
        await h.client._modify_order(modify(TARGET, price="85400.00"))
        await wait_until(lambda: last_kind(h, TARGET) == "OrderUpdated", timeout_secs=10)

        assert len(h.received(oa.ProtoOAAmendPositionSLTPReq)) == 2
        assert len(amends) == 1


async def test_a_bracket_settles_once_the_model_is_rebuilt_with_its_protective_order() -> None:
    execution_venue = ExecutionVenue()
    amends = echo_amends(execution_venue)
    async with harness(execution_venue=execution_venue) as h:
        await push_spot(h, BID, ASK)
        # The broker took the order and protected the position while the connection was down.
        our_open_position(execution_venue)
        h.server.close_after_next_request = True
        sending = asyncio.create_task(submit_bracket(h, bracket(h)))
        await wait_until(lambda: len(h.client._brackets) == 1)
        await h.client._cancel_order(cancel(TARGET))
        await sending
        await wait_until(lambda: status(h, TARGET) == OrderStatus.CANCELED, timeout_secs=10)

        (amend,) = amends
        assert amend.stopLoss == 85197.2
        assert not amend.HasField("takeProfit")
        assert len(h.client._brackets) == 0


async def test_a_correction_leaves_the_level_of_a_leg_no_longer_alive() -> None:
    execution_venue = answered(FIRST_EVENTS[:2])
    amends = echo_amends(execution_venue)
    config = exec_config(protective_order_timeout_secs=30.0)
    async with harness(execution_venue=execution_venue, config=config) as h:
        await push_spot(h, BID, ASK)
        await submit_bracket(h, bracket(h))
        await wait_until(lambda: status(h, ENTRY) == OrderStatus.FILLED)
        # Rebuilt before the protective order came: the position has no level, so no live leg.
        our_open_position(execution_venue)
        snapshot = execution_venue.snapshot
        snapshot.position[0].ClearField("stopLoss")
        snapshot.position[0].ClearField("takeProfit")
        del snapshot.order[:]
        await h.client._load()
        assert not any(alive for _, alive in h.client._book.view(FIRST).legs.values())

        await push(h, FIRST_EVENTS[2])

        # Settled on the event itself: no correction was started.
        assert len(h.client._brackets) == 0
        assert amends == []


async def test_a_wait_ending_with_nothing_to_set_leaves_the_bracket_to_its_correction() -> None:
    execution_venue = ExecutionVenue()
    held = HeldReplies(
        execution_venue.server, om.PROTO_OA_NEW_ORDER_REQ, lambda _r: FIRST_EVENTS[0]
    )
    amends: list = []
    held_amends = HeldReplies(
        execution_venue.server, om.PROTO_OA_AMEND_POSITION_SLTP_REQ, amend_echo(amends)
    )
    config = exec_config(protective_order_timeout_secs=30.0)
    async with harness(execution_venue=execution_venue, config=config) as h:
        sending = await in_flight(h, held)
        await h.client._cancel_order(cancel(TARGET))
        await held.release()
        await sending
        # The protective order brings the stop-loss alone, away from its asked price.
        stop_only = protective(85190.0, None, utc=AMEND_FROM, kind=om.ORDER_ACCEPTED)
        stop_only.isServerEvent = True
        await push(h, FIRST_EVENTS[1], stop_only)
        await asyncio.wait_for(held_amends.arrived.wait(), timeout=5)
        await h.client._modify_order(modify(STOP, trigger_price="85150.00"))

        # The wait ends while the correction is out; the cancelled leg is the one missing.
        h.client._protection_waited(FIRST)
        await wait_until(lambda: status(h, TARGET) == OrderStatus.CANCELED)
        assert len(h.client._brackets) == 1

        await held_amends.stop_holding()
        await wait_until(
            lambda: h.cache.order(ClientOrderId(STOP)).trigger_price == Price.from_str("85150.00"),
        )
        assert amends[-1].stopLoss == 85150.0
        assert not amends[-1].HasField("takeProfit")
        assert len(h.client._brackets) == 0
