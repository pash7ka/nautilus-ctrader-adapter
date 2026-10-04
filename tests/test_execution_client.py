"""Tests for `CTraderExecutionClient` against the fake venue, inside a Nautilus engine."""

from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock, MessageBus
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.identifiers import AccountId
from nautilus_trader.model.objects import Money

from nautilus_ctrader.common.account import account_client_from_config
from nautilus_ctrader.common.errors import CTraderAccountError
from nautilus_ctrader.execution import CTraderExecutionClient
from nautilus_ctrader.factories import CTraderLiveExecClientFactory
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as om
from tests.execution_replay import FIRST, as_ours, position_orders, snapshot_with_protection
from tests.execution_venue import (
    TRADER_ID,
    US100_ID,
    US100_SYMBOL_ID,
    ExecutionVenue,
    exec_config,
    harness,
    on_us100,
    trader,
)
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
