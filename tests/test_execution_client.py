"""Tests for `CTraderExecutionClient` against the fake venue, inside a Nautilus engine."""

from __future__ import annotations

import asyncio
import struct
import time
from collections.abc import Callable
from decimal import Decimal

import pytest
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock, MessageBus
from nautilus_trader.config import InstrumentProviderConfig, StrategyConfig
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.messages import (
    BatchCancelOrders,
    CancelAllOrders,
    CancelOrder,
    ModifyOrder,
    QueryAccount,
    QueryOrder,
    SubmitOrder,
)
from nautilus_trader.execution.reports import FillReport, OrderStatusReport
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.enums import OrderSide, OrderStatus, TimeInForce
from nautilus_trader.model.events import (
    OrderCancelRejected,
    OrderFilled,
    OrderModifyRejected,
    OrderPendingCancel,
    OrderPendingUpdate,
    OrderRejected,
    OrderUpdated,
)
from nautilus_trader.model.identifiers import (
    AccountId,
    ClientOrderId,
    PositionId,
    StrategyId,
    TradeId,
    VenueOrderId,
)
from nautilus_trader.model.objects import Money, Price, Quantity
from nautilus_trader.trading.strategy import Strategy

from nautilus_ctrader.activity import CTraderAccountActivity
from nautilus_ctrader.common import order_record, order_translation
from nautilus_ctrader.common.account import account_client_from_config
from nautilus_ctrader.common.errors import CTraderAccountError
from nautilus_ctrader.common.order_record import LegIds
from nautilus_ctrader.common.venue_book import terms_of
from nautilus_ctrader.common.venue_records import Level, LevelTerms
from nautilus_ctrader.constants import LENGTH_PREFIX_FORMAT, MAX_FRAME_BYTES
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
    FOREIGN_EVENTS,
    STOP,
    STRATEGY_ID,
    TARGET,
    TRADER_ID,
    TRADER_LOGIN,
    US100_ID,
    US100_SYMBOL_ID,
    ExecutionVenue,
    bracket,
    exec_config,
    harness,
    on_us100,
    pending_event,
    push,
    push_spot,
    serve,
    started,
    status,
    submit_bracket,
    submitted,
    sync,
    trader,
)
from tests.fake_server import Pushed
from tests.polling import wait_until
from tests.recording_logger import RecordingLogger

ACCOUNT = AccountId(f"CTRADER-{TRADER_LOGIN}")


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


async def test_the_account_id_carries_the_trader_login() -> None:
    logger = RecordingLogger()
    clock = LiveClock()

    def client_for(trader_login: int) -> CTraderExecutionClient:
        config = exec_config(trader_login=trader_login)
        account = account_client_from_config(config, logger)
        provider = account.get_instrument_provider(
            config=InstrumentProviderConfig(),
            asset_class_overrides={},
            fail_on_instrument_error=False,
            logger=logger,
        )
        return CTraderExecutionClient(
            loop=asyncio.get_running_loop(),
            account=account,
            msgbus=MessageBus(trader_id=TRADER_ID, clock=clock),
            cache=Cache(),
            clock=clock,
            instrument_provider=provider,
            config=config,
        )

    first = client_for(TRADER_LOGIN)
    second = client_for(TRADER_LOGIN + 1)

    assert first.account_id == AccountId(f"CTRADER-{TRADER_LOGIN}")
    assert second.account_id == AccountId(f"CTRADER-{TRADER_LOGIN + 1}")
    assert first.account_id != second.account_id


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
        # The trader's partial close: reported as an external closing order, then its fill.
        report, fill = h.reports
        assert isinstance(report, OrderStatusReport)
        assert report.venue_order_id == VenueOrderId("6000003")
        assert report.reduce_only
        assert report.order_status == OrderStatus.ACCEPTED
        assert isinstance(fill, FillReport)
        assert fill.venue_order_id == VenueOrderId("6000003")
        assert not any(
            isinstance(e, OrderFilled) and e.venue_order_id == VenueOrderId("6000003")
            for e in h.events
        )
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
                account_id=ACCOUNT,
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
                account_id=ACCOUNT,
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
    """The node's close of that position, accepted then filled (hand-built).

    The broker's clock runs a minute behind the node's: its answer is the close's all the same.
    """
    created = int(time.time() * 1000) - 60_000

    def order(utc: int) -> om.ProtoOAOrder:
        found = make_order(6_100_002, MARKET_POSITION, side=om.SELL, closing=True, utc=utc)
        found.tradeData.openTimestamp = created
        return found

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
        assert h.client._operations.close_position(CLOSE_ID) is None
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
        assert h.client._operations.close_position(CLOSE_ID) is None


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
        assert h.client._operations.close_position(CLOSE_ID) is None


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
    """The broker's answer setting whatever an amend asks for; keeps each in `amends`."""

    def answer(request):
        amends.append(request)
        stop = request.stopLoss if request.HasField("stopLoss") else None
        limit = request.takeProfit if request.HasField("takeProfit") else None
        utc = AMEND_FROM + len(amends)
        if stop is None and limit is None:
            event = protective(None, None, utc=utc, kind=om.ORDER_CANCELLED)
        else:
            event = protective(stop, limit, utc=utc, kind=kind)
        for name in ("trailingStopLoss", "guaranteedStopLoss", "stopLossTriggerMethod"):
            if request.HasField(name):
                setattr(event.position, name, getattr(request, name))
        return event

    return answer


def echo_amends(execution_venue: ExecutionVenue, *, kind: int = om.ORDER_REPLACED) -> list:
    """The broker sets whatever levels an amend asks for; returns the amends it received."""
    amends: list = []
    execution_venue.server.on(om.PROTO_OA_AMEND_POSITION_SLTP_REQ, amend_echo(amends, kind=kind))
    return amends


def kept_terms(amend: oa.ProtoOAAmendPositionSLTPReq, terms: LevelTerms) -> bool:
    """Whether `amend` sends the position's terms again: the stop-loss flags only with one."""
    with_stop = amend.HasField("stopLoss")
    return (
        amend.HasField("stopLossTriggerMethod")
        and amend.stopLossTriggerMethod == terms.stop_loss_trigger_method
        and amend.HasField("trailingStopLoss") == with_stop
        and amend.HasField("guaranteedStopLoss") == with_stop
        and (not with_stop or amend.trailingStopLoss == terms.trailing_stop_loss)
        and (not with_stop or amend.guaranteedStopLoss == terms.guaranteed_stop_loss)
    )


# How the first position's levels work, as recorded.
FIRST_TERMS = terms_of(FIRST_EVENTS[2].position)


def cancel(client_order_id: str, *, strategy_id: StrategyId = STRATEGY_ID) -> CancelOrder:
    return CancelOrder(
        trader_id=TRADER_ID,
        strategy_id=strategy_id,
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
    strategy_id: StrategyId = STRATEGY_ID,
) -> ModifyOrder:
    return ModifyOrder(
        trader_id=TRADER_ID,
        strategy_id=strategy_id,
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
        assert h.client._book.view(FIRST).terms == FIRST_TERMS
        assert kept_terms(amend, FIRST_TERMS)
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
        # The legs' cancels come behind the trader's close, which waits for the entry's fill.
        await wait_until(lambda: status(h, TARGET) == OrderStatus.CANCELED)

        assert len(h.client._brackets) == 0
        (rejected,) = [e for e in h.events_of(STOP) if isinstance(e, OrderModifyRejected)]
        assert rejected.reason == "the position closed before its levels were set"
        assert status(h, STOP) == OrderStatus.CANCELED
        assert status(h, TARGET) == OrderStatus.CANCELED
        assert h.received(oa.ProtoOAAmendPositionSLTPReq) == []


def entry_ended(kind: int, status: int) -> oa.ProtoOAExecutionEvent:
    """The first entry ended by the broker with nothing filled (hand-built)."""
    event = type(FIRST_EVENTS[0])()
    event.CopyFrom(FIRST_EVENTS[0])
    event.executionType = kind
    event.order.orderStatus = status
    event.order.utcLastUpdateTimestamp += 1
    return event


@pytest.mark.parametrize(
    ("kind", "order_status", "reason"),
    [
        (om.ORDER_CANCELLED, om.ORDER_STATUS_CANCELLED, "the entry canceled"),
        (om.ORDER_EXPIRED, om.ORDER_STATUS_EXPIRED, "the entry expired"),
        (om.ORDER_REJECTED, om.ORDER_STATUS_REJECTED, "the entry rejected"),
    ],
)
async def test_a_bracket_whose_entry_ends_unfilled_answers_its_waiting_modify(
    kind: int, order_status: int, reason: str
) -> None:
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

        await push(h, entry_ended(kind, order_status))

        assert len(h.client._brackets) == 0
        await wait_until(lambda: status(h, STOP) == OrderStatus.CANCELED)
        await wait_until(lambda: status(h, TARGET) == OrderStatus.CANCELED)
        (rejected,) = [e for e in h.events_of(STOP) if isinstance(e, OrderModifyRejected)]
        assert rejected.reason == reason
        # Each leg's events in the order Nautilus applied them: the answer, then the cancel.
        assert h.kinds_of(STOP)[-2:] == ["OrderModifyRejected", "OrderCanceled"]
        assert [type(e).__name__ for e in h.cache.order(ClientOrderId(STOP)).events][-2:] == [
            "OrderModifyRejected",
            "OrderCanceled",
        ]
        assert h.kinds_of(TARGET)[-1] == "OrderCanceled"
        assert h.logger.errors() == []


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
        assert kept_terms(amend, FIRST_TERMS)
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
        assert kept_terms(amend, FIRST_TERMS)
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
        assert kept_terms(amend, FIRST_TERMS)


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


# A position the node did not open: its levels are external legs, and a command on one is
# carried out as on the node's own. Nautilus hears of the outcome from the broker's answer, as
# reports; the client itself sends no event of such an order but a rejection.

FOREIGN_SL, FOREIGN_TP = "6000001-SL", "6000001-TP"
EXTERNAL = StrategyId("EXTERNAL")
FOREIGN_TERMS = terms_of(FOREIGN_EVENTS[2].position)


def foreign_leg(h, venue_order_id: str):
    client_order_id = h.cache.client_order_id(VenueOrderId(venue_order_id))
    return None if client_order_id is None else h.cache.order(client_order_id)


def protected_by_hand(*, trailing: bool, guaranteed: bool = False) -> list:
    """The first position as a trader opened and protected it."""
    events = list(FOREIGN_EVENTS[:3])
    if trailing or guaranteed:
        protected = type(events[2])()
        protected.CopyFrom(events[2])
        if trailing:
            protected.position.trailingStopLoss = True
            protected.position.stopLossTriggerMethod = om.OPPOSITE
        if guaranteed:
            protected.position.guaranteedStopLoss = True
        events[2] = protected
    return events


async def foreign_legs(h, *, trailing: bool = False, guaranteed: bool = False) -> tuple:
    await push(h, *protected_by_hand(trailing=trailing, guaranteed=guaranteed))
    await wait_until(
        lambda: foreign_leg(h, FOREIGN_SL) is not None and foreign_leg(h, FOREIGN_TP) is not None,
        description="the foreign legs",
    )
    return foreign_leg(h, FOREIGN_SL), foreign_leg(h, FOREIGN_TP)


def reports_of(h, venue_order_id: str) -> list[OrderStatusReport]:
    return [
        r
        for r in h.reports
        if isinstance(r, OrderStatusReport) and r.venue_order_id == VenueOrderId(venue_order_id)
    ]


def sent_about(h, order) -> list:
    """The events the client itself sent Nautilus about `order`."""
    return [e for e in h.events if e.client_order_id == order.client_order_id]


def levels(amend: oa.ProtoOAAmendPositionSLTPReq) -> tuple:
    return (
        amend.stopLoss if amend.HasField("stopLoss") else None,
        amend.takeProfit if amend.HasField("takeProfit") else None,
    )


@pytest.mark.parametrize(
    ("leg_id", "other_id", "sent"),
    [(FOREIGN_SL, FOREIGN_TP, (None, 85387.22)), (FOREIGN_TP, FOREIGN_SL, (85197.2, None))],
)
async def test_a_foreign_leg_is_cancelled_by_removing_its_level(leg_id, other_id, sent) -> None:
    execution_venue = ExecutionVenue()
    amends = echo_amends(execution_venue)
    async with harness(execution_venue=execution_venue) as h:
        await foreign_legs(h)
        order = foreign_leg(h, leg_id)
        await h.client._cancel_order(cancel(order.client_order_id.value))
        await wait_until(lambda: foreign_leg(h, leg_id).status == OrderStatus.CANCELED)

        (amend,) = amends
        assert amend.positionId == FIRST
        assert levels(amend) == sent
        assert kept_terms(amend, FOREIGN_TERMS)
        assert foreign_leg(h, other_id).status == OrderStatus.ACCEPTED
        assert reports_of(h, leg_id)[-1].order_status == OrderStatus.CANCELED
        assert sent_about(h, order) == []
        assert h.logger.errors() == []

        await h.client._cancel_order(cancel(order.client_order_id.value))
        await wait_until(lambda: len(sent_about(h, order)) == 1)
        (rejected,) = sent_about(h, order)
        assert isinstance(rejected, OrderCancelRejected)
        assert rejected.reason == "the leg is already closed"
        assert len(amends) == 1


@pytest.mark.parametrize(
    ("leg_id", "change", "sent"),
    [
        (FOREIGN_SL, {"trigger_price": "85150.00"}, (85150.0, 85387.22)),
        (FOREIGN_TP, {"price": "85400.00"}, (85197.2, 85400.0)),
    ],
)
async def test_a_foreign_leg_is_moved_by_an_amend_keeping_the_other_level(
    leg_id, change, sent
) -> None:
    execution_venue = ExecutionVenue()
    amends = echo_amends(execution_venue)
    async with harness(execution_venue=execution_venue) as h:
        await foreign_legs(h)
        order = foreign_leg(h, leg_id)
        wanted = Price.from_str(next(iter(change.values())))
        await h.client._modify_order(modify(order.client_order_id.value, **change))

        def moved() -> bool:
            held = foreign_leg(h, leg_id)
            return (held.trigger_price if leg_id == FOREIGN_SL else held.price) == wanted

        await wait_until(moved, description="the leg moved")
        (amend,) = amends
        assert levels(amend) == sent
        assert kept_terms(amend, FOREIGN_TERMS)
        assert foreign_leg(h, leg_id).status == OrderStatus.ACCEPTED
        assert foreign_leg(h, leg_id).quantity == Quantity.from_str("1.00")
        assert reports_of(h, leg_id)[-1].order_status == OrderStatus.ACCEPTED
        assert sent_about(h, order) == []
        assert h.logger.errors() == []


async def test_a_modify_of_a_trailing_stop_loss_keeps_it_trailing() -> None:
    execution_venue = ExecutionVenue()
    amends = echo_amends(execution_venue)
    async with harness(execution_venue=execution_venue) as h:
        stop, _ = await foreign_legs(h, trailing=True)
        await h.client._modify_order(modify(stop.client_order_id.value, trigger_price="85150.00"))
        await wait_until(
            lambda: foreign_leg(h, FOREIGN_SL).trigger_price == Price.from_str("85150.00"),
        )

        (amend,) = amends
        assert levels(amend) == (85150.0, 85387.22)
        assert amend.HasField("trailingStopLoss") and amend.trailingStopLoss
        assert amend.HasField("stopLossTriggerMethod")
        assert amend.stopLossTriggerMethod == om.OPPOSITE
        assert amend.HasField("guaranteedStopLoss") and not amend.guaranteedStopLoss


async def test_commands_the_broker_refuses_are_rejected_under_the_orders_strategy() -> None:
    execution_venue = ExecutionVenue()
    execution_venue.server.on(om.PROTO_OA_AMEND_POSITION_SLTP_REQ, lambda _r: REFUSED_STOPS)
    async with harness(execution_venue=execution_venue) as h:
        stop, target = await foreign_legs(h)
        # Sent by a strategy that is not the orders': Nautilus checks no ownership.
        await h.client._cancel_order(cancel(stop.client_order_id.value))
        await h.client._modify_order(modify(target.client_order_id.value, price="85400.00"))
        await wait_until(lambda: sent_about(h, stop) and sent_about(h, target))

        (cancel_rejected,) = sent_about(h, stop)
        (modify_rejected,) = sent_about(h, target)
        assert isinstance(cancel_rejected, OrderCancelRejected)
        assert isinstance(modify_rejected, OrderModifyRejected)
        for rejected in (cancel_rejected, modify_rejected):
            assert rejected.strategy_id == EXTERNAL
            assert rejected.reason == "TRADING_BAD_STOPS: Invalid stops"
        assert foreign_leg(h, FOREIGN_SL).status == OrderStatus.ACCEPTED
        assert foreign_leg(h, FOREIGN_TP).price == Price.from_str("85387.22")
        assert set(h.client._book.view(FIRST).levels) == {Level.STOP_LOSS, Level.TAKE_PROFIT}


async def test_a_quantity_change_of_a_foreign_leg_is_refused() -> None:
    execution_venue = ExecutionVenue()
    amends = echo_amends(execution_venue)
    async with harness(execution_venue=execution_venue) as h:
        stop, _ = await foreign_legs(h)
        await h.client._modify_order(
            modify(
                stop.client_order_id.value,
                trigger_price="85150.00",
                quantity="0.50",
                strategy_id=StrategyId("S-002"),
            ),
        )
        await wait_until(lambda: sent_about(h, stop))

        (rejected,) = sent_about(h, stop)
        assert isinstance(rejected, OrderModifyRejected)
        assert rejected.strategy_id == EXTERNAL
        assert "covers the whole position (1.00)" in rejected.reason
        assert amends == []


@pytest.mark.parametrize(
    "change",
    [{"trigger_price": "85197.20"}, {"quantity": "1.00"}],
    ids=["same-level", "same-quantity"],
)
async def test_a_modify_that_changes_nothing_is_answered_as_the_broker_holds_the_leg(
    change,
) -> None:
    execution_venue = ExecutionVenue()
    amends = echo_amends(execution_venue)
    async with harness(execution_venue=execution_venue) as h:
        stop, _ = await foreign_legs(h)
        h.engine.process(
            OrderPendingUpdate(
                TRADER_ID,
                EXTERNAL,
                US100_ID,
                stop.client_order_id,
                stop.venue_order_id,
                ACCOUNT,
                UUID4(),
                0,
                0,
            ),
        )
        await wait_until(lambda: foreign_leg(h, FOREIGN_SL).status == OrderStatus.PENDING_UPDATE)
        before = len(reports_of(h, FOREIGN_SL))

        await h.client._modify_order(modify(stop.client_order_id.value, **change))
        await wait_until(lambda: foreign_leg(h, FOREIGN_SL).status == OrderStatus.ACCEPTED)

        assert amends == []
        assert len(reports_of(h, FOREIGN_SL)) == before + 1
        assert foreign_leg(h, FOREIGN_SL).trigger_price == Price.from_str("85197.20")
        assert sent_about(h, stop) == []


async def test_cancel_all_of_a_claiming_strategy_cancels_the_foreign_legs_it_claimed() -> None:
    execution_venue = ExecutionVenue()
    amends = echo_amends(execution_venue)
    async with harness(execution_venue=execution_venue) as h:
        claiming = Strategy(
            StrategyConfig(
                strategy_id="S-CLAIM",
                order_id_tag="001",
                external_order_claims=[str(US100_ID)],
            ),
        )
        h.engine.register_external_order_claims(claiming)
        stop, target = await foreign_legs(h)
        assert stop.strategy_id == target.strategy_id == claiming.id

        def cancel_all(strategy_id: StrategyId) -> CancelAllOrders:
            return CancelAllOrders(
                trader_id=TRADER_ID,
                strategy_id=strategy_id,
                instrument_id=US100_ID,
                order_side=OrderSide.NO_ORDER_SIDE,
                command_id=UUID4(),
                ts_init=0,
            )

        # Another strategy's selection holds neither.
        await h.client._cancel_all_orders(cancel_all(STRATEGY_ID))
        assert amends == []

        await h.client._cancel_all_orders(cancel_all(claiming.id))
        await wait_until(
            lambda: (
                foreign_leg(h, FOREIGN_SL).status == OrderStatus.CANCELED
                and foreign_leg(h, FOREIGN_TP).status == OrderStatus.CANCELED
            ),
        )
        (amend,) = amends
        assert levels(amend) == (None, None)
        assert sent_about(h, stop) == sent_about(h, target) == []
        assert h.logger.errors() == []


async def test_commands_whose_level_the_broker_keeps_are_rejected() -> None:
    execution_venue = ExecutionVenue()
    amends = keep_levels(execution_venue)
    async with harness(execution_venue=execution_venue) as h:
        stop, target = await foreign_legs(h)
        await h.client._cancel_order(cancel(stop.client_order_id.value))
        await h.client._modify_order(modify(target.client_order_id.value, price="85400.00"))
        await wait_until(lambda: sent_about(h, stop) and sent_about(h, target))

        assert len(amends) == 2
        (cancel_rejected,) = sent_about(h, stop)
        (modify_rejected,) = sent_about(h, target)
        assert cancel_rejected.reason == "the broker kept the level"
        assert modify_rejected.reason == "the broker kept the level at 85387.22"
        assert cancel_rejected.strategy_id == modify_rejected.strategy_id == EXTERNAL
        assert foreign_leg(h, FOREIGN_SL).status == OrderStatus.ACCEPTED
        assert foreign_leg(h, FOREIGN_TP).price == Price.from_str("85387.22")


# A trader's pending order: a cancel or a modify of it is carried out at the broker, whoever sends
# it. Nautilus hears of the outcome from the broker's answer, as reports, as for a foreign leg.

RESTING, RESTING_POSITION = 6_800_001, 5_800_001
EXPIRES = 1_900_000_000_000


def resting(kind: int = om.LIMIT, **fields) -> om.ProtoOAOrder:
    """A trader's pending buy of 2.00 `US100.cash`: a limit at 84000.00 or a stop at 86000.00."""
    order = make_order(
        RESTING,
        RESTING_POSITION,
        order_type=kind,
        utc=int(time.time() * 1000) - 60_000,
        limit=84000.0 if kind == om.LIMIT else None,
        stop=None if kind == om.LIMIT else 86000.0,
        volume=200,
        symbol=US100_SYMBOL_ID,
    )
    order.timeInForce = om.GOOD_TILL_CANCEL
    for name, value in fields.items():
        setattr(order, name, value)
    return order


def resting_venue(order: om.ProtoOAOrder | None = None) -> ExecutionVenue:
    execution_venue = ExecutionVenue()
    execution_venue.snapshot.order.append(order or resting())
    return execution_venue


async def held_resting(h):
    """The trader's order, as Nautilus learnt it from the start's reconciliation."""
    await started(h)
    order = foreign_leg(h, str(RESTING))
    assert order is not None and order.status == OrderStatus.ACCEPTED
    return order


def order_amends(h) -> list[oa.ProtoOAAmendOrderReq]:
    return h.received(oa.ProtoOAAmendOrderReq)


async def test_a_pending_external_order_is_cancelled_at_the_broker() -> None:
    async with harness(execution_venue=resting_venue()) as h:
        order = await held_resting(h)
        await h.client._cancel_order(cancel(order.client_order_id.value))
        await wait_until(lambda: foreign_leg(h, str(RESTING)).status == OrderStatus.CANCELED)

        assert h.received(oa.ProtoOACancelOrderReq) == [
            oa.ProtoOACancelOrderReq(ctidTraderAccountId=ACCOUNT_ID, orderId=RESTING)
        ]
        assert reports_of(h, str(RESTING))[-1].order_status == OrderStatus.CANCELED
        assert reports_of(h, str(RESTING))[-1].client_order_id == order.client_order_id
        assert sent_about(h, order) == []
        assert h.client._book.open_order(RESTING) is None

        # The broker holds it no longer, so nothing is sent.
        await h.client._cancel_order(cancel(order.client_order_id.value))
        await wait_until(lambda: sent_about(h, order))
        (rejected,) = sent_about(h, order)
        assert isinstance(rejected, OrderCancelRejected)
        assert rejected.reason == "the order is not open at the venue"
        assert len(h.received(oa.ProtoOACancelOrderReq)) == 1
        assert h.logger.errors() == []


@pytest.mark.parametrize(
    ("kind", "change", "sent", "held"),
    [
        pytest.param(
            om.LIMIT,
            {"price": "84100.00"},
            {"volume": 200, "limitPrice": 84100.0},
            {"price": Price.from_str("84100.00")},
            id="price",
        ),
        pytest.param(
            om.STOP,
            {"trigger_price": "86100.00"},
            {"volume": 200, "stopPrice": 86100.0, "stopTriggerMethod": om.TRADE},
            {"trigger_price": Price.from_str("86100.00")},
            id="trigger-price",
        ),
        pytest.param(
            om.LIMIT,
            {"quantity": "3.00"},
            {"volume": 300, "limitPrice": 84000.0},
            {"quantity": Quantity.from_str("3.00"), "price": Price.from_str("84000.00")},
            id="quantity",
        ),
    ],
)
async def test_a_pending_external_order_is_amended_at_the_broker(kind, change, sent, held) -> None:
    async with harness(execution_venue=resting_venue(resting(kind))) as h:
        order = await held_resting(h)
        # Sent by a strategy that is not the order's: Nautilus checks no ownership.
        await h.client._modify_order(modify(order.client_order_id.value, **change))

        def amended() -> bool:
            now = foreign_leg(h, str(RESTING))
            return all(getattr(now, name) == value for name, value in held.items())

        await wait_until(amended, description="the order amended")
        (amend,) = order_amends(h)
        assert amend == oa.ProtoOAAmendOrderReq(
            ctidTraderAccountId=ACCOUNT_ID, orderId=RESTING, **sent
        )
        assert foreign_leg(h, str(RESTING)).status == OrderStatus.ACCEPTED
        assert reports_of(h, str(RESTING))[-1].order_status == OrderStatus.ACCEPTED
        assert sent_about(h, order) == []
        assert h.logger.errors() == []


async def test_an_amend_of_a_pending_order_sends_its_attached_levels_again() -> None:
    order = resting(
        stopLoss=83000.0,
        takeProfit=85500.0,
        trailingStopLoss=True,
        expirationTimestamp=EXPIRES,
        timeInForce=om.GOOD_TILL_DATE,
    )
    order.tradeData.guaranteedStopLoss = True
    async with harness(execution_venue=resting_venue(order)) as h:
        held = await held_resting(h)
        await h.client._modify_order(modify(held.client_order_id.value, price="84100.00"))
        await wait_until(lambda: order_amends(h))

        (amend,) = order_amends(h)
        assert amend == oa.ProtoOAAmendOrderReq(
            ctidTraderAccountId=ACCOUNT_ID,
            orderId=RESTING,
            volume=200,
            limitPrice=84100.0,
            expirationTimestamp=EXPIRES,
            stopLoss=83000.0,
            takeProfit=85500.0,
            trailingStopLoss=True,
            guaranteedStopLoss=True,
        )
        await wait_until(
            lambda: foreign_leg(h, str(RESTING)).price == Price.from_str("84100.00"),
        )
        # The broker's answer is what the next amend starts from.
        assert h.client._book.open_order(RESTING).limitPrice == 84100.0


async def test_a_pending_order_learnt_live_is_amended_from_its_last_state() -> None:
    async with harness() as h:
        order = resting()
        await push(h, pending_event(om.ORDER_ACCEPTED, order))
        await wait_until(lambda: foreign_leg(h, str(RESTING)) is not None)
        moved = resting(limitPrice=84050.0)
        moved.utcLastUpdateTimestamp = order.utcLastUpdateTimestamp + 1
        h.venue.snapshot.order.append(moved)
        await push(h, pending_event(om.ORDER_REPLACED, moved))
        await wait_until(
            lambda: foreign_leg(h, str(RESTING)).price == Price.from_str("84050.00"),
        )

        held = foreign_leg(h, str(RESTING))
        await h.client._modify_order(modify(held.client_order_id.value, quantity="1.00"))
        await wait_until(
            lambda: foreign_leg(h, str(RESTING)).quantity == Quantity.from_str("1.00"),
        )

        (amend,) = order_amends(h)
        assert amend.volume == 100
        assert amend.limitPrice == 84050.0


REFUSED_PRICE = oa.ProtoOAOrderErrorEvent(
    ctidTraderAccountId=ACCOUNT_ID, errorCode="TRADING_BAD_PRICES", description="Invalid price"
)


@pytest.mark.parametrize("by_event", [False, True], ids=["order-error", "execution-event"])
async def test_commands_on_a_pending_order_the_broker_refuses_are_rejected(by_event) -> None:
    execution_venue = resting_venue()

    def refuse(kind: int):
        def answer(_request):
            if not by_event:
                return REFUSED_PRICE
            event = pending_event(kind, execution_venue.snapshot.order[0])
            event.errorCode = "TRADING_BAD_PRICES"
            return event

        return answer

    execution_venue.server.on(om.PROTO_OA_CANCEL_ORDER_REQ, refuse(om.ORDER_CANCEL_REJECTED))
    execution_venue.server.on(om.PROTO_OA_AMEND_ORDER_REQ, refuse(om.ORDER_REJECTED))
    async with harness(execution_venue=execution_venue) as h:
        order = await held_resting(h)
        await h.client._cancel_order(cancel(order.client_order_id.value))
        await h.client._modify_order(modify(order.client_order_id.value, price="84100.00"))
        await wait_until(lambda: len(sent_about(h, order)) == 2)

        cancel_rejected, modify_rejected = sent_about(h, order)
        assert isinstance(cancel_rejected, OrderCancelRejected)
        assert isinstance(modify_rejected, OrderModifyRejected)
        for rejected in (cancel_rejected, modify_rejected):
            assert rejected.strategy_id == EXTERNAL
            assert rejected.reason.startswith("TRADING_BAD_PRICES")
        await sync(h)
        held = foreign_leg(h, str(RESTING))
        assert held.status == OrderStatus.ACCEPTED
        assert held.price == Price.from_str("84000.00")
        assert h.client._book.open_order(RESTING) is not None


@pytest.mark.parametrize(
    ("change", "reason"),
    [
        ({"price": "84000.005"}, "finer than the instrument's price precision"),
        ({"trigger_price": "85000.00"}, "cannot set the trigger price of a LIMIT order"),
    ],
)
async def test_a_modify_the_venue_cannot_express_is_refused_unsent(change, reason) -> None:
    async with harness(execution_venue=resting_venue()) as h:
        order = await held_resting(h)
        await h.client._modify_order(modify(order.client_order_id.value, **change))
        await wait_until(lambda: sent_about(h, order))

        (rejected,) = sent_about(h, order)
        assert isinstance(rejected, OrderModifyRejected)
        assert reason in rejected.reason
        assert order_amends(h) == []


async def test_a_modify_that_changes_nothing_is_answered_as_the_broker_holds_the_order() -> None:
    async with harness(execution_venue=resting_venue()) as h:
        order = await held_resting(h)
        h.engine.process(
            OrderPendingUpdate(
                TRADER_ID,
                EXTERNAL,
                US100_ID,
                order.client_order_id,
                order.venue_order_id,
                ACCOUNT,
                UUID4(),
                0,
                0,
            ),
        )
        await wait_until(lambda: foreign_leg(h, str(RESTING)).status == OrderStatus.PENDING_UPDATE)
        before = len(reports_of(h, str(RESTING)))

        await h.client._modify_order(modify(order.client_order_id.value, price="84000.00"))
        await wait_until(lambda: foreign_leg(h, str(RESTING)).status == OrderStatus.ACCEPTED)

        assert order_amends(h) == []
        assert len(reports_of(h, str(RESTING))) == before + 1
        assert sent_about(h, order) == []


async def test_a_market_order_can_be_neither_cancelled_nor_modified() -> None:
    async with harness(execution_venue=answered(FIRST_EVENTS[:3])) as h:
        await opened_bracket(h)
        await h.client._cancel_order(cancel(ENTRY))
        await h.client._modify_order(modify(ENTRY, quantity="2.00"))
        await wait_until(lambda: last_kind(h, ENTRY) == "OrderModifyRejected")

        cancel_rejected, modify_rejected = (
            e for e in h.events_of(ENTRY) if "Rejected" in type(e).__name__
        )
        assert cancel_rejected.reason == "a market order fills at once and cannot be cancelled"
        assert modify_rejected.reason == "a market order fills at once and cannot be modified"
        assert h.received(oa.ProtoOACancelOrderReq) == order_amends(h) == []


def kinds(order) -> list[str]:
    """The kinds of the events Nautilus applied to `order`, oldest first."""
    return [type(e).__name__ for e in order.events]


def engine_cancel(h, order) -> None:
    """A cancel of `order` by strategy `S-002`, through the engine as a strategy sends one."""
    h.engine.process(
        OrderPendingCancel(
            TRADER_ID,
            order.strategy_id,
            order.instrument_id,
            order.client_order_id,
            order.venue_order_id,
            ACCOUNT,
            UUID4(),
            0,
            0,
        ),
    )
    h.engine.execute(
        CancelOrder(
            trader_id=TRADER_ID,
            strategy_id=StrategyId("S-002"),
            instrument_id=order.instrument_id,
            client_order_id=order.client_order_id,
            venue_order_id=order.venue_order_id,
            command_id=UUID4(),
            ts_init=0,
        ),
    )


async def test_a_cancel_through_the_engine_ends_the_pending_cancel() -> None:
    async with harness(execution_venue=resting_venue()) as h:
        order = await held_resting(h)
        engine_cancel(h, order)

        await wait_until(lambda: foreign_leg(h, str(RESTING)).status == OrderStatus.CANCELED)
        assert kinds(foreign_leg(h, str(RESTING)))[-2:] == ["OrderPendingCancel", "OrderCanceled"]
        assert len(h.received(oa.ProtoOACancelOrderReq)) == 1
        assert sent_about(h, order) == []


async def test_a_refused_cancel_of_a_foreign_leg_through_the_engine_ends_its_pending_cancel() -> (
    None
):
    execution_venue = ExecutionVenue()
    execution_venue.server.on(om.PROTO_OA_AMEND_POSITION_SLTP_REQ, lambda _r: REFUSED_STOPS)
    async with harness(execution_venue=execution_venue) as h:
        stop, _ = await foreign_legs(h)
        engine_cancel(h, stop)

        await wait_until(lambda: kinds(foreign_leg(h, FOREIGN_SL))[-1] == "OrderCancelRejected")
        assert foreign_leg(h, FOREIGN_SL).status == OrderStatus.ACCEPTED
        assert kinds(foreign_leg(h, FOREIGN_SL))[-2:] == [
            "OrderPendingCancel",
            "OrderCancelRejected",
        ]
        (rejected,) = [
            e for e in h.events_of(stop.client_order_id.value) if isinstance(e, OrderCancelRejected)
        ]
        assert rejected.strategy_id == EXTERNAL
        assert rejected.reason == "TRADING_BAD_STOPS: Invalid stops"


async def test_a_modify_of_a_guaranteed_stop_loss_keeps_it_guaranteed() -> None:
    execution_venue = ExecutionVenue()
    amends = echo_amends(execution_venue)
    async with harness(execution_venue=execution_venue) as h:
        stop, _ = await foreign_legs(h, guaranteed=True)
        await h.client._modify_order(modify(stop.client_order_id.value, trigger_price="85150.00"))
        await wait_until(
            lambda: foreign_leg(h, FOREIGN_SL).trigger_price == Price.from_str("85150.00"),
        )

        (amend,) = amends
        assert levels(amend) == (85150.0, 85387.22)
        assert amend.HasField("guaranteedStopLoss") and amend.guaranteedStopLoss
        assert amend.HasField("trailingStopLoss") and not amend.trailingStopLoss


async def test_commands_on_a_pending_order_the_broker_answers_unchanged_are_rejected() -> None:
    execution_venue = resting_venue()

    def unchanged(_request):
        order = execution_venue.snapshot.order[0]
        order.utcLastUpdateTimestamp += 1
        return pending_event(om.ORDER_REPLACED, order)

    execution_venue.server.on(om.PROTO_OA_CANCEL_ORDER_REQ, unchanged)
    execution_venue.server.on(om.PROTO_OA_AMEND_ORDER_REQ, unchanged)
    async with harness(execution_venue=execution_venue) as h:
        order = await held_resting(h)
        await h.client._cancel_order(cancel(order.client_order_id.value))
        await h.client._modify_order(modify(order.client_order_id.value, price="84100.00"))
        await wait_until(lambda: len(sent_about(h, order)) == 2)

        cancel_rejected, modify_rejected = sent_about(h, order)
        assert cancel_rejected.reason == "the broker kept the order open"
        assert modify_rejected.reason == "the broker did not amend the order as asked"
        assert cancel_rejected.strategy_id == modify_rejected.strategy_id == EXTERNAL
        await sync(h)
        held = foreign_leg(h, str(RESTING))
        assert held.status == OrderStatus.ACCEPTED
        assert held.price == Price.from_str("84000.00")


async def test_two_amends_of_one_pending_order_never_undo_each_other() -> None:
    async with harness(execution_venue=resting_venue()) as h:
        order = await held_resting(h)
        await asyncio.gather(
            h.client._modify_order(modify(order.client_order_id.value, price="84100.00")),
            h.client._modify_order(modify(order.client_order_id.value, quantity="3.00")),
        )
        await wait_until(
            lambda: foreign_leg(h, str(RESTING)).quantity == Quantity.from_str("3.00"),
        )

        first, second = order_amends(h)
        assert (first.volume, first.limitPrice) == (200, 84100.0)
        assert (second.volume, second.limitPrice) == (300, 84100.0)
        assert foreign_leg(h, str(RESTING)).price == Price.from_str("84100.00")


def amend_then(execution_venue: ExecutionVenue, trader_change: int):
    """The broker amends as asked, but a trader's change reaches the client before its answer.

    `trader_change` is `ORDER_REPLACED` (the trader moves the limit to 84200.00) or
    `ORDER_CANCELLED`.
    """

    def answer(request):
        ours = execution_venue.replies_to_amend(request)
        theirs = om.ProtoOAOrder()
        theirs.CopyFrom(execution_venue.snapshot.order[0])
        theirs.utcLastUpdateTimestamp += 1
        if trader_change == om.ORDER_REPLACED:
            theirs.limitPrice = 84200.0
            execution_venue.snapshot.order[0].CopyFrom(theirs)
        else:
            theirs.orderStatus = om.ORDER_STATUS_CANCELLED
            del execution_venue.snapshot.order[:]
        return [Pushed(pending_event(trader_change, theirs)), ours]

    execution_venue.server.on(om.PROTO_OA_AMEND_ORDER_REQ, answer)


async def test_a_trader_change_newer_than_the_amends_answer_is_what_nautilus_holds() -> None:
    execution_venue = resting_venue()
    amend_then(execution_venue, om.ORDER_REPLACED)
    async with harness(execution_venue=execution_venue) as h:
        order = await held_resting(h)
        await h.client._modify_order(modify(order.client_order_id.value, price="84100.00"))
        await sync(h)

        await wait_until(lambda: not h.client._outbox)
        assert foreign_leg(h, str(RESTING)).price == Price.from_str("84200.00")
        assert h.client._book.open_order(RESTING).limitPrice == 84200.0
        # The amend's own answer, older than the trader's change, is never reported.
        assert Price.from_str("84100.00") not in [r.price for r in reports_of(h, str(RESTING))]
        assert sent_about(h, order) == []
        assert h.logger.errors() == []


async def test_a_trader_cancel_before_the_amends_answer_reports_nothing_after_it() -> None:
    execution_venue = resting_venue()
    amend_then(execution_venue, om.ORDER_CANCELLED)
    async with harness(execution_venue=execution_venue) as h:
        order = await held_resting(h)
        await h.client._modify_order(modify(order.client_order_id.value, price="84100.00"))
        await sync(h)

        await wait_until(lambda: foreign_leg(h, str(RESTING)).status == OrderStatus.CANCELED)
        await wait_until(lambda: not h.client._outbox)
        assert reports_of(h, str(RESTING))[-1].order_status == OrderStatus.CANCELED
        assert foreign_leg(h, str(RESTING)).price == Price.from_str("84000.00")
        assert sent_about(h, order) == []
        assert not any("is not reported" in line for line in h.logger.warnings())
        assert h.logger.errors() == []


async def test_a_pending_orders_amend_lock_goes_once_the_order_has_ended() -> None:
    async with harness(execution_venue=resting_venue()) as h:
        order = await held_resting(h)
        await h.client._modify_order(modify(order.client_order_id.value, price="84100.00"))
        assert RESTING in h.client._order_locks

        await h.client._cancel_order(cancel(order.client_order_id.value))

        assert h.client._book.open_order(RESTING) is None
        assert RESTING not in h.client._order_locks


def answer_amend_with(execution_venue: ExecutionVenue, kind: int) -> None:
    """The broker answers an amend of the pending order with an event of `kind`, unchanged.

    `ORDER_EXPIRED` ends the order; any other kind leaves it open.
    """

    def answer(_request):
        order = execution_venue.snapshot.order[0]
        order.utcLastUpdateTimestamp += 1
        stated = om.ProtoOAOrder()
        stated.CopyFrom(order)
        if kind == om.ORDER_EXPIRED:
            stated.orderStatus = om.ORDER_STATUS_EXPIRED
            del execution_venue.snapshot.order[:]
        return pending_event(kind, stated)

    execution_venue.server.on(om.PROTO_OA_AMEND_ORDER_REQ, answer)


async def test_an_amend_answered_by_the_order_ending_is_no_refusal() -> None:
    execution_venue = resting_venue()
    answer_amend_with(execution_venue, om.ORDER_EXPIRED)
    async with harness(execution_venue=execution_venue) as h:
        order = await held_resting(h)
        await h.client._modify_order(modify(order.client_order_id.value, price="84100.00"))
        await wait_until(lambda: foreign_leg(h, str(RESTING)).status == OrderStatus.EXPIRED)
        await sync(h)

        assert sent_about(h, order) == []
        assert h.logger.errors() == []


async def test_an_amend_answered_other_than_by_a_replace_is_no_refusal() -> None:
    # Only a replace states the amended order; another answer's values are not the amend's.
    execution_venue = resting_venue()
    answer_amend_with(execution_venue, om.ORDER_ACCEPTED)
    async with harness(execution_venue=execution_venue) as h:
        order = await held_resting(h)
        before = len(reports_of(h, str(RESTING)))
        await h.client._modify_order(modify(order.client_order_id.value, price="84100.00"))
        await wait_until(lambda: len(reports_of(h, str(RESTING))) > before)
        await sync(h)

        assert len(order_amends(h)) == 1
        assert sent_about(h, order) == []
        assert foreign_leg(h, str(RESTING)).status == OrderStatus.ACCEPTED


# A modify that names the price the leg does not move by.
WRONG_PRICE = [
    pytest.param(
        Level.STOP_LOSS,
        {"price": "85150.00"},
        "a stop-loss leg moves by its trigger price",
        id="stop-loss",
    ),
    pytest.param(
        Level.TAKE_PROFIT,
        {"trigger_price": "85400.00"},
        "a take-profit leg moves by its price",
        id="take-profit",
    ),
]


@pytest.mark.parametrize(("level", "change", "reason"), WRONG_PRICE)
async def test_a_foreign_leg_modified_by_the_wrong_price_is_refused(level, change, reason) -> None:
    execution_venue = ExecutionVenue()
    amends = echo_amends(execution_venue)
    async with harness(execution_venue=execution_venue) as h:
        legs = dict(zip((Level.STOP_LOSS, Level.TAKE_PROFIT), await foreign_legs(h), strict=True))
        order = legs[level]
        await h.client._modify_order(modify(order.client_order_id.value, **change))
        await wait_until(lambda: sent_about(h, order))

        (rejected,) = sent_about(h, order)
        assert isinstance(rejected, OrderModifyRejected)
        assert rejected.reason == reason
        assert rejected.strategy_id == EXTERNAL
        assert amends == []


@pytest.mark.parametrize(("level", "change", "reason"), WRONG_PRICE)
async def test_a_leg_modified_by_the_wrong_price_is_refused(level, change, reason) -> None:
    execution_venue = answered(FIRST_EVENTS[:3])
    amends = echo_amends(execution_venue)
    async with harness(execution_venue=execution_venue) as h:
        await opened_bracket(h)
        leg = STOP if level == Level.STOP_LOSS else TARGET
        await h.client._modify_order(modify(leg, **change))
        await wait_until(lambda: last_kind(h, leg) == "OrderModifyRejected")

        (rejected,) = [e for e in h.events_of(leg) if isinstance(e, OrderModifyRejected)]
        assert rejected.reason == reason
        assert amends == []


def trailed_at(stop: float, *, utc: int = AMEND_FROM + 100) -> oa.ProtoOATrailingSLChangedEvent:
    """The broker's move of the first position's trailing stop-loss, by default after any amend's
    answer."""
    return oa.ProtoOATrailingSLChangedEvent(
        ctidTraderAccountId=ACCOUNT_ID,
        positionId=FIRST,
        orderId=FOREIGN_EVENTS[2].order.orderId,
        stopPrice=stop,
        utcLastUpdateTimestamp=utc,
    )


@pytest.mark.parametrize("foreign", [True, False], ids=["foreign", "own"])
async def test_a_trailing_move_after_the_amends_answer_is_no_refusal(
    foreign, monkeypatch: pytest.MonkeyPatch
) -> None:
    execution_venue = ExecutionVenue() if foreign else answered(FIRST_EVENTS[:3])
    amends = echo_amends(execution_venue)
    async with harness(execution_venue=execution_venue) as h:
        if foreign:
            stop, _ = await foreign_legs(h, trailing=True)
            leg = stop.client_order_id.value
        else:
            await opened_bracket(h)
            leg = STOP
        amend = h.client._amend

        async def then_trailed(*args, **kwargs):
            outcome = await amend(*args, **kwargs)
            # The market moves the stop-loss before the modify reads the answer.
            h.client._on_trailing_stop(trailed_at(85160.0))
            return outcome

        monkeypatch.setattr(h.client, "_amend", then_trailed)
        await h.client._modify_order(modify(leg, trigger_price="85150.00"))
        await wait_until(
            lambda: h.cache.order(ClientOrderId(leg)).trigger_price == Price.from_str("85160.00"),
            description="the trailing move reported",
        )
        await sync(h)

        assert len(amends) == 1
        assert not any(isinstance(e, OrderModifyRejected) for e in h.events_of(leg))


@pytest.mark.parametrize("foreign", [True, False], ids=["foreign", "own"])
async def test_a_trailing_move_ahead_of_the_amends_older_answer_is_kept(foreign) -> None:
    execution_venue = ExecutionVenue() if foreign else answered(FIRST_EVENTS[:3])
    amends: list = []
    echo = amend_echo(amends)
    # The broker moves the stop-loss after the amend, and that move overtakes the answer.
    execution_venue.server.on(
        om.PROTO_OA_AMEND_POSITION_SLTP_REQ,
        lambda request: [Pushed(trailed_at(85160.0)), echo(request)],
    )
    async with harness(execution_venue=execution_venue) as h:
        if foreign:
            stop, target = await foreign_legs(h, trailing=True)
            leg, other = stop.client_order_id.value, target.client_order_id.value
        else:
            await opened_bracket(h)
            leg, other = STOP, TARGET
        await h.client._modify_order(modify(leg, trigger_price="85150.00"))
        await sync(h)

        order = h.cache.order(ClientOrderId(leg))
        assert order.trigger_price == Price.from_str("85160.00")
        moves = [e.trigger_price for e in order.events if isinstance(e, OrderUpdated)]
        assert Price.from_str("85150.00") not in moves
        assert h.client._book.view(FIRST).levels[Level.STOP_LOSS] == Decimal("85160.00")
        assert levels(amends[0])[0] == 85150.0
        assert not any(isinstance(e, OrderModifyRejected) for e in h.events_of(leg))
        assert h.logger.errors() == []

        # The next level amend starts from the trailed stop-loss.
        await h.client._cancel_order(cancel(other))
        await wait_until(lambda: len(amends) == 2)
        assert levels(amends[1]) == (85160.0, None)


@pytest.mark.parametrize("foreign", [True, False], ids=["foreign", "own"])
async def test_a_trailing_move_made_before_the_amend_and_delivered_after_its_answer_is_dropped(
    foreign,
) -> None:
    execution_venue = ExecutionVenue() if foreign else answered(FIRST_EVENTS[:3])
    amends = echo_amends(execution_venue)
    async with harness(execution_venue=execution_venue) as h:
        if foreign:
            stop, target = await foreign_legs(h, trailing=True)
            leg, other = stop.client_order_id.value, target.client_order_id.value
        else:
            await opened_bracket(h)
            leg, other = STOP, TARGET
        await h.client._modify_order(modify(leg, trigger_price="85150.00"))
        # The answer is stamped `AMEND_FROM + 1`; the move was made a millisecond earlier.
        await push(h, trailed_at(85160.0, utc=AMEND_FROM))

        order = h.cache.order(ClientOrderId(leg))
        assert order.trigger_price == Price.from_str("85150.00")
        moves = [e.trigger_price for e in order.events if isinstance(e, OrderUpdated)]
        assert Price.from_str("85160.00") not in moves
        assert h.client._book.view(FIRST).levels[Level.STOP_LOSS] == Decimal("85150.00")
        assert h.logger.errors() == []

        # The next level amend starts from the amended stop-loss.
        await h.client._cancel_order(cancel(other))
        await wait_until(lambda: len(amends) == 2)
        assert levels(amends[1]) == (85150.0, None)


def unload_us100(h) -> None:
    h.client._instrument_provider.remove_failed(US100_ID, "unloaded for the test")


async def test_commands_on_a_foreign_leg_of_an_unloaded_instrument_are_refused() -> None:
    execution_venue = ExecutionVenue()
    amends = echo_amends(execution_venue)
    async with harness(execution_venue=execution_venue) as h:
        stop, target = await foreign_legs(h)
        unload_us100(h)
        await h.client._modify_order(modify(stop.client_order_id.value, trigger_price="85150.00"))
        await h.client._cancel_order(cancel(target.client_order_id.value))
        await wait_until(lambda: sent_about(h, stop) and sent_about(h, target))

        (modify_rejected,) = sent_about(h, stop)
        (cancel_rejected,) = sent_about(h, target)
        assert isinstance(modify_rejected, OrderModifyRejected)
        assert isinstance(cancel_rejected, OrderCancelRejected)
        for rejected in (modify_rejected, cancel_rejected):
            assert "is not loaded" in rejected.reason
        assert amends == []


async def test_commands_on_a_leg_of_an_unloaded_instrument_are_refused() -> None:
    execution_venue = answered(FIRST_EVENTS[:3])
    amends = echo_amends(execution_venue)
    async with harness(execution_venue=execution_venue) as h:
        await opened_bracket(h)
        unload_us100(h)
        await h.client._modify_order(modify(STOP, trigger_price="85150.00"))
        await h.client._cancel_order(cancel(TARGET))
        await wait_until(
            lambda: (
                last_kind(h, STOP) == "OrderModifyRejected"
                and last_kind(h, TARGET) == "OrderCancelRejected"
            ),
        )

        for leg in (STOP, TARGET):
            (rejected,) = [
                e
                for e in h.events_of(leg)
                if isinstance(e, (OrderModifyRejected, OrderCancelRejected))
            ]
            assert "is not loaded" in rejected.reason
        assert amends == []


@pytest.mark.parametrize("foreign", [True, False], ids=["foreign", "own"])
async def test_a_leg_price_finer_than_the_instrument_is_refused_unsent(foreign) -> None:
    # Sent to the client directly: Nautilus's risk engine would refuse it first.
    execution_venue = ExecutionVenue() if foreign else answered(FIRST_EVENTS[:3])
    amends = echo_amends(execution_venue)
    async with harness(execution_venue=execution_venue) as h:
        if foreign:
            stop, _ = await foreign_legs(h)
            leg = stop.client_order_id.value
        else:
            await opened_bracket(h)
            leg = STOP
        await h.client._modify_order(modify(leg, trigger_price="85150.005"))
        await wait_until(lambda: last_kind(h, leg) == "OrderModifyRejected")

        (rejected,) = [e for e in h.events_of(leg) if isinstance(e, OrderModifyRejected)]
        assert "finer than the instrument's price precision" in rejected.reason
        assert amends == []


async def test_a_command_that_fails_unexpectedly_is_refused_with_an_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with harness(execution_venue=resting_venue()) as h:
        order = await held_resting(h)

        def broken(_order_id):
            raise RuntimeError("broken for the test")

        monkeypatch.setattr(h.client._book, "open_order", broken)
        await h.client._modify_order(modify(order.client_order_id.value, price="84100.00"))
        await h.client._cancel_order(cancel(order.client_order_id.value))
        await wait_until(lambda: len(sent_about(h, order)) == 2)

        modify_rejected, cancel_rejected = sent_about(h, order)
        assert isinstance(modify_rejected, OrderModifyRejected)
        assert isinstance(cancel_rejected, OrderCancelRejected)
        for rejected in (modify_rejected, cancel_rejected):
            assert "RuntimeError" in rejected.reason
            assert rejected.strategy_id == EXTERNAL
        assert len(h.logger.errors()) == 2
        assert all("RuntimeError" in line for line in h.logger.errors())


async def test_a_leg_command_that_fails_before_its_send_is_refused_unsent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    execution_venue = answered(FIRST_EVENTS[:3])
    amends = echo_amends(execution_venue)
    async with harness(execution_venue=execution_venue) as h:
        await opened_bracket(h)
        monkeypatch.setattr(h.client, "_leg_alive", broken)
        await h.client._modify_order(modify(TARGET, price="85400.00"))
        await h.client._cancel_order(cancel(STOP))
        await wait_until(
            lambda: (
                last_kind(h, TARGET) == "OrderModifyRejected"
                and last_kind(h, STOP) == "OrderCancelRejected"
            ),
        )

        for leg in (STOP, TARGET):
            (rejected,) = [
                e
                for e in h.events_of(leg)
                if isinstance(e, (OrderModifyRejected, OrderCancelRejected))
            ]
            assert "RuntimeError" in rejected.reason
        assert amends == []
        assert len(h.logger.errors()) == 2


# -- A command the broker has received is never refused -----------------------------------------

OPEN_AT = 358.0  # FIRST open with 0.99 after a manual partial close, both levels set


def broken(*_args, **_kwargs):
    raise RuntimeError("broken for the test")


async def made_pending(h, order, *, cancelling: bool) -> None:
    """Put `order` in `PENDING_CANCEL` or `PENDING_UPDATE`, as the engine does with a command."""
    kind = OrderPendingCancel if cancelling else OrderPendingUpdate
    h.engine.process(
        kind(
            TRADER_ID,
            order.strategy_id,
            order.instrument_id,
            order.client_order_id,
            order.venue_order_id,
            ACCOUNT,
            UUID4(),
            0,
            0,
        ),
    )
    wanted = OrderStatus.PENDING_CANCEL if cancelling else OrderStatus.PENDING_UPDATE
    await wait_until(lambda: h.cache.order(order.client_order_id).status == wanted)


def query_of(order) -> QueryOrder:
    return QueryOrder(
        trader_id=TRADER_ID,
        strategy_id=order.strategy_id,
        instrument_id=order.instrument_id,
        client_order_id=order.client_order_id,
        venue_order_id=order.venue_order_id,
        command_id=UUID4(),
        ts_init=0,
    )


def refusals(h, order) -> list:
    return [
        e for e in sent_about(h, order) if isinstance(e, (OrderModifyRejected, OrderCancelRejected))
    ]


def assert_unknown(h, order, wanted: OrderStatus) -> None:
    """The command is neither refused nor answered: an ERROR says so, and the order waits."""
    assert refusals(h, order) == []
    assert h.cache.order(order.client_order_id).status == wanted
    (error,) = h.logger.errors()
    assert "RuntimeError" in error
    assert order.client_order_id.value in error


@pytest.mark.parametrize("cancelling", [True, False], ids=["cancel", "modify"])
async def test_a_leg_command_that_fails_after_its_send_waits_for_a_query(
    monkeypatch: pytest.MonkeyPatch, cancelling: bool
) -> None:
    execution_venue = answered(FIRST_EVENTS[:3])
    amends = echo_amends(execution_venue)
    async with harness(execution_venue=execution_venue) as h:
        await opened_bracket(h)
        leg = h.cache.order(ClientOrderId(STOP if cancelling else TARGET))
        await made_pending(h, leg, cancelling=cancelling)
        # Fails while the broker's answer is handled.
        monkeypatch.setattr(h.client, "_bookkeep", broken)
        if cancelling:
            await h.client._cancel_order(cancel(STOP))
        else:
            await h.client._modify_order(modify(TARGET, price="85400.00"))
        await sync(h)

        assert len(amends) == 1
        pending = OrderStatus.PENDING_CANCEL if cancelling else OrderStatus.PENDING_UPDATE
        assert_unknown(h, leg, pending)

        # The broker holds what the amend asked for.
        monkeypatch.undo()
        serve(execution_venue, OPEN_AT)
        position = execution_venue.snapshot.position[0]
        if cancelling:
            position.ClearField("stopLoss")
        else:
            position.takeProfit = 85400.0
        await h.client._query_order(query_of(leg))

        if cancelling:
            await wait_until(lambda: status(h, STOP) == OrderStatus.CANCELED)
        else:
            await wait_until(lambda: status(h, TARGET) == OrderStatus.ACCEPTED)
            assert h.cache.order(ClientOrderId(TARGET)).price == Price.from_str("85400.00")
        assert refusals(h, leg) == []


@pytest.mark.parametrize("cancelling", [True, False], ids=["cancel", "modify"])
async def test_a_pending_order_command_that_fails_after_its_send_waits_for_a_query(
    monkeypatch: pytest.MonkeyPatch, cancelling: bool
) -> None:
    async with harness(execution_venue=resting_venue()) as h:
        order = await held_resting(h)
        await made_pending(h, order, cancelling=cancelling)
        monkeypatch.setattr(h.client, "_bookkeep", broken)
        if cancelling:
            await h.client._cancel_order(cancel(order.client_order_id.value))
        else:
            await h.client._modify_order(modify(order.client_order_id.value, price="84100.00"))
        await sync(h)

        if cancelling:
            assert len(h.received(oa.ProtoOACancelOrderReq)) == 1
            assert_unknown(h, order, OrderStatus.PENDING_CANCEL)
        else:
            assert len(order_amends(h)) == 1
            assert_unknown(h, order, OrderStatus.PENDING_UPDATE)

        monkeypatch.undo()
        await h.client._query_order(query_of(order))

        if cancelling:
            await wait_until(lambda: foreign_leg(h, str(RESTING)).status == OrderStatus.CANCELED)
        else:
            await wait_until(lambda: foreign_leg(h, str(RESTING)).status == OrderStatus.ACCEPTED)
            assert foreign_leg(h, str(RESTING)).price == Price.from_str("84100.00")
        assert refusals(h, order) == []


async def test_a_leg_cancel_failing_once_its_answer_is_applied_is_not_refused(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    execution_venue = answered(FIRST_EVENTS[:3])
    echo_amends(execution_venue)
    async with harness(execution_venue=execution_venue) as h:
        await opened_bracket(h)
        stop = h.cache.order(ClientOrderId(STOP))
        monkeypatch.setattr(h.client._book, "cancel_leg", broken)
        await h.client._cancel_order(cancel(STOP))
        await sync(h)

        assert refusals(h, stop) == []
        (error,) = h.logger.errors()
        assert "RuntimeError" in error
        assert STOP in error


async def test_a_pending_order_amend_failing_once_its_answer_is_applied_is_not_refused(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with harness(execution_venue=resting_venue()) as h:
        order = await held_resting(h)
        monkeypatch.setattr(order_translation, "carries", broken)
        await h.client._modify_order(modify(order.client_order_id.value, price="84100.00"))
        await sync(h)

        assert len(order_amends(h)) == 1
        assert refusals(h, order) == []
        (error,) = h.logger.errors()
        assert "RuntimeError" in error
        assert order.client_order_id.value in error


async def test_a_leg_modify_failing_when_nothing_was_sent_is_refused(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    execution_venue = answered(FIRST_EVENTS[:3])
    amends = echo_amends(execution_venue)
    async with harness(execution_venue=execution_venue) as h:
        await opened_bracket(h)
        monkeypatch.setattr(h.client, "_leg_updated", broken)
        # The price the leg already holds: the levels stand, so no amend goes out.
        await h.client._modify_order(modify(TARGET, price="85387.22"))
        await wait_until(lambda: last_kind(h, TARGET) == "OrderModifyRejected")

        (rejected,) = [e for e in h.events_of(TARGET) if isinstance(e, OrderModifyRejected)]
        assert "RuntimeError" in rejected.reason
        assert amends == []


@pytest.mark.parametrize("cancelling", [True, False], ids=["cancel", "modify"])
@pytest.mark.parametrize("pending_order", [False, True], ids=["leg", "pending-order"])
async def test_a_command_failing_as_its_request_is_built_is_refused_unsent(
    monkeypatch: pytest.MonkeyPatch, cancelling: bool, pending_order: bool
) -> None:
    execution_venue = resting_venue() if pending_order else answered(FIRST_EVENTS[:3])
    echo_amends(execution_venue)
    async with harness(execution_venue=execution_venue) as h:
        if pending_order:
            order = await held_resting(h)
            build = "cancel_order" if cancelling else "amend_order"
            change = {"price": "84100.00"}
        else:
            await opened_bracket(h)
            order = h.cache.order(ClientOrderId(STOP if cancelling else TARGET))
            build = "amend_levels"
            change = {"price": "85400.00"}
        before = len(h.server.received)
        monkeypatch.setattr(order_translation, build, broken)
        if cancelling:
            await h.client._cancel_order(cancel(order.client_order_id.value))
        else:
            await h.client._modify_order(modify(order.client_order_id.value, **change))
        await wait_until(lambda: refusals(h, order))

        (rejected,) = refusals(h, order)
        assert isinstance(rejected, OrderCancelRejected if cancelling else OrderModifyRejected)
        assert "RuntimeError" in rejected.reason
        assert len(h.server.received) == before


@pytest.mark.parametrize("cancelling", [True, False], ids=["cancel", "modify"])
async def test_a_foreign_leg_command_that_fails_after_its_send_is_not_refused(
    monkeypatch: pytest.MonkeyPatch, cancelling: bool
) -> None:
    execution_venue = ExecutionVenue()
    amends = echo_amends(execution_venue)
    async with harness(execution_venue=execution_venue) as h:
        stop, _ = await foreign_legs(h)
        await made_pending(h, stop, cancelling=cancelling)
        monkeypatch.setattr(h.client, "_bookkeep", broken)
        if cancelling:
            await h.client._cancel_order(cancel(stop.client_order_id.value))
        else:
            await h.client._modify_order(
                modify(stop.client_order_id.value, trigger_price="85150.00")
            )
        await sync(h)

        assert len(amends) == 1
        pending = OrderStatus.PENDING_CANCEL if cancelling else OrderStatus.PENDING_UPDATE
        assert_unknown(h, stop, pending)


async def test_a_correction_failing_after_its_send_still_answers_the_modify_it_carried(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    execution_venue = ExecutionVenue()
    held = HeldReplies(
        execution_venue.server, om.PROTO_OA_NEW_ORDER_REQ, lambda _r: FIRST_EVENTS[0]
    )
    amends = echo_amends(execution_venue)
    async with harness(execution_venue=execution_venue) as h:
        sending = await in_flight(h, held)
        await h.client._modify_order(modify(STOP, trigger_price="85150.00"))
        bookkeep = h.client._bookkeep

        def failing_in_the_amend(record) -> None:
            if h.client._operations.amending(FIRST):
                raise RuntimeError("broken for the test")
            bookkeep(record)

        monkeypatch.setattr(h.client, "_bookkeep", failing_in_the_amend)
        await held.release()
        await sending
        await push(h, *FIRST_EVENTS[1:3])
        await wait_until(lambda: last_kind(h, STOP) == "OrderUpdated")

        assert len(amends) == 1
        assert "OrderModifyRejected" not in h.kinds_of(STOP)
        assert len(h.client._brackets) == 0
        (error,) = h.logger.errors()
        assert "RuntimeError" in error
        await wait_until(
            lambda: h.cache.order(ClientOrderId(STOP)).trigger_price == Price.from_str("85150.00"),
        )


# -- An amend whose answer was lost is never refused afterwards ---------------------------------


def lose_amends(h, monkeypatch: pytest.MonkeyPatch, steps: list) -> None:
    """The first level amends go as `steps` say: "lost" gets no answer, an exception is raised.

    Amends past the steps reach the venue.
    """
    send = h.client._send

    async def scripted(request):
        if isinstance(request, oa.ProtoOAAmendPositionSLTPReq) and steps:
            step = steps.pop(0)
            if step == "lost":
                return None
            raise step
        return await send(request)

    monkeypatch.setattr(h.client, "_send", scripted)


@pytest.mark.parametrize(
    ("steps", "venue_refuses"),
    [
        pytest.param(["lost"], True, id="then-refused"),
        pytest.param(["lost", "lost", "lost"], False, id="every-attempt-lost"),
        pytest.param(["lost", RuntimeError("broken for the test")], False, id="then-failed"),
    ],
)
async def test_an_amend_that_got_no_answer_ends_unknown_not_refused(
    monkeypatch: pytest.MonkeyPatch, steps: list, venue_refuses: bool
) -> None:
    execution_venue = answered(FIRST_EVENTS[:3])
    amends = echo_amends(execution_venue)
    async with harness(execution_venue=execution_venue) as h:
        await opened_bracket(h)
        if venue_refuses:
            execution_venue.server.on(om.PROTO_OA_AMEND_POSITION_SLTP_REQ, lambda _r: REFUSED_STOPS)
        target = h.cache.order(ClientOrderId(TARGET))
        await made_pending(h, target, cancelling=False)
        failed = any(isinstance(step, Exception) for step in steps)
        lose_amends(h, monkeypatch, steps)
        await h.client._modify_order(modify(TARGET, price="85400.00"))
        await sync(h)

        assert refusals(h, target) == []
        assert status(h, TARGET) == OrderStatus.PENDING_UPDATE
        assert len(h.received(oa.ProtoOAAmendPositionSLTPReq)) == (1 if venue_refuses else 0)
        assert amends == []
        if failed:
            (error,) = h.logger.errors()
            assert "RuntimeError" in error
            assert TARGET in error
        else:
            assert h.logger.errors() == []
            assert any(TARGET in line and "got no answer" in line for line in h.logger.warnings())


async def test_missing_levels_whose_amend_got_no_answer_leave_the_legs_with_an_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    execution_venue = answered(FIRST_EVENTS[:2])  # no protective order follows the fill
    config = exec_config(protective_order_timeout_secs=0.05)
    async with harness(execution_venue=execution_venue, config=config) as h:
        lose_amends(h, monkeypatch, ["lost", "lost", "lost"])
        await push_spot(h, BID, ASK)
        await submit_bracket(h, bracket(h))
        await wait_until(
            lambda: any("whether its levels were set is unknown" in e for e in h.logger.errors()),
        )

        assert OrderStatus.REJECTED not in (status(h, STOP), status(h, TARGET))
        assert len(h.client._brackets) == 1


async def test_a_correction_that_got_no_answer_is_not_resent_and_waits_for_the_late_answer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    execution_venue = ExecutionVenue()
    held = HeldReplies(
        execution_venue.server, om.PROTO_OA_NEW_ORDER_REQ, lambda _r: FIRST_EVENTS[0]
    )
    # Taken as refusing an amend that changes nothing, since the lost one may have applied.
    execution_venue.server.on(om.PROTO_OA_AMEND_POSITION_SLTP_REQ, lambda _r: REFUSED_STOPS)
    async with harness(execution_venue=execution_venue) as h:
        sending = await in_flight(h, held)
        await h.client._modify_order(modify(STOP, trigger_price="85150.00"))
        lose_amends(h, monkeypatch, ["lost"])
        await held.release()
        await sending
        await push(h, *FIRST_EVENTS[1:3])
        await wait_until(lambda: h.received(oa.ProtoOAAmendPositionSLTPReq))
        await sync(h)

        stop = h.cache.order(ClientOrderId(STOP))
        assert refusals(h, stop) == []
        assert h.kinds_of(STOP) == ["OrderSubmitted", "OrderAccepted"]
        assert len(h.received(oa.ProtoOAAmendPositionSLTPReq)) == 1
        assert len(h.client._brackets) == 1
        (error,) = h.logger.errors()
        assert "whether its levels were set is unknown" in error

        # The lost amend's answer, late.
        await push(h, protective(85150.0, 85387.22, utc=AMEND_FROM))
        await wait_until(lambda: last_kind(h, STOP) == "OrderUpdated")

        await wait_until(lambda: stop.trigger_price == Price.from_str("85150.00"))
        assert refusals(h, stop) == []
        assert len(h.client._brackets) == 0


async def test_an_amend_answered_after_a_lost_attempt_is_reported() -> None:
    execution_venue = answered(FIRST_EVENTS[:3])
    amends = echo_amends(execution_venue)
    async with harness(execution_venue=execution_venue) as h:
        await opened_bracket(h)
        with pytest.MonkeyPatch.context() as monkeypatch:
            lose_amends(h, monkeypatch, ["lost"])
            await h.client._modify_order(modify(TARGET, price="85400.00"))
        await wait_until(lambda: last_kind(h, TARGET) == "OrderUpdated")

        assert len(amends) == 1
        assert "OrderModifyRejected" not in h.kinds_of(TARGET)


async def test_an_amend_failing_before_any_attempt_was_lost_is_refused(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    execution_venue = answered(FIRST_EVENTS[:3])
    amends = echo_amends(execution_venue)
    async with harness(execution_venue=execution_venue) as h:
        await opened_bracket(h)
        lose_amends(h, monkeypatch, [RuntimeError("broken for the test")])
        await h.client._modify_order(modify(TARGET, price="85400.00"))
        await wait_until(lambda: last_kind(h, TARGET) == "OrderModifyRejected")

        assert amends == []


# -- An answer the adapter cannot read --------------------------------------------------------

UNREADABLE = struct.pack(LENGTH_PREFIX_FORMAT, MAX_FRAME_BYTES + 1)


def answer_unreadably(server, times: int) -> Callable:
    """A reply for the first `times` requests: a frame too long to read, which drops the link."""
    pushes: list[asyncio.Task] = []

    def reply(request):
        if len(pushes) >= times:
            return None
        pushes.append(asyncio.get_running_loop().create_task(server.push_raw(UNREADABLE)))
        return None

    return reply


async def test_a_pending_order_cancel_answered_unreadably_is_not_refused() -> None:
    execution_venue = resting_venue()
    async with harness(execution_venue=execution_venue) as h:
        order = await held_resting(h)
        execution_venue.server.on(
            om.PROTO_OA_CANCEL_ORDER_REQ, answer_unreadably(execution_venue.server, 1)
        )
        await h.client._cancel_order(cancel(order.client_order_id.value))

        assert refusals(h, order) == []
        assert any("no answer, so its outcome is unknown" in line for line in h.logger.warnings())


async def test_a_leg_amend_answered_unreadably_is_sent_again_and_never_refused() -> None:
    execution_venue = answered(FIRST_EVENTS[:3])
    amends: list = []
    echo = amend_echo(amends)
    unreadable = answer_unreadably(execution_venue.server, 1)
    asked: list = []

    def reply(request):
        asked.append(request)
        return unreadable(request) if len(asked) == 1 else echo(request)

    execution_venue.server.on(om.PROTO_OA_AMEND_POSITION_SLTP_REQ, reply)
    async with harness(execution_venue=execution_venue) as h:
        await opened_bracket(h)
        # What the broker holds after the reconnect, for the model's rebuild.
        our_open_position(execution_venue)
        await h.client._modify_order(modify(TARGET, price="85400.00"))
        await wait_until(lambda: last_kind(h, TARGET) == "OrderUpdated", timeout_secs=10)

        assert len(asked) == 2
        assert len(amends) == 1
        assert "OrderModifyRejected" not in h.kinds_of(TARGET)
