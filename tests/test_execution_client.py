"""Tests for `CTraderExecutionClient` against the fake venue, inside a Nautilus engine."""

from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock, MessageBus
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.messages import QueryAccount
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.events import OrderFilled
from nautilus_trader.model.identifiers import (
    AccountId,
    ClientOrderId,
    PositionId,
    TradeId,
    VenueOrderId,
)
from nautilus_trader.model.objects import Money, Price, Quantity

from nautilus_ctrader.activity import CTraderAccountActivity
from nautilus_ctrader.common.account import account_client_from_config
from nautilus_ctrader.common.errors import CTraderAccountError
from nautilus_ctrader.execution import CTraderExecutionClient
from nautilus_ctrader.factories import CTraderLiveExecClientFactory
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as om
from tests.account_venue import ACCOUNT_ID, HeldReplies
from tests.execution_replay import (
    FIRST,
    as_ours,
    make_event,
    make_order,
    position_orders,
    snapshot_with_protection,
)
from tests.execution_venue import (
    ENTRY,
    FIRST_EVENTS,
    STOP,
    TARGET,
    TRADER_ID,
    US100_ID,
    US100_SYMBOL_ID,
    ExecutionVenue,
    exec_config,
    harness,
    on_us100,
    push,
    status,
    submitted,
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
    ],
)
def test_the_config_refuses_bad_values(overrides) -> None:
    with pytest.raises(ValueError):
        exec_config(**overrides)


def test_the_config_defaults() -> None:
    config = exec_config()

    assert config.reference_price_max_age_secs == 10.0
    assert config.protective_order_timeout_secs == 2.0
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

        assert h.kinds_of(ENTRY) == ["OrderSubmitted", "OrderFilled"]
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
