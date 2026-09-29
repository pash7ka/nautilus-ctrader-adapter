"""Tests for `CTraderLiveDataClientFactory` and the package's public exports.

The factory reaches the venue only through `get_cached_ctrader_account_client`, so the tests
seed that cache with a client pointed at the fake server - which is also the sequence the
application follows to register a token listener before the node is built.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.live.factories import LiveDataClientFactory
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.test_kit.stubs.component import TestComponentStubs

import nautilus_ctrader
from nautilus_ctrader.common.account import (
    CTraderAccountClient,
    get_cached_ctrader_account_client,
)
from nautilus_ctrader.config import CTraderDataClientConfig
from nautilus_ctrader.data import CTraderDataClient
from nautilus_ctrader.factories import CTraderLiveDataClientFactory
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as om
from nautilus_ctrader.providers import CTraderInstrumentProvider
from tests.account_venue import TRADER_LOGIN, venue
from tests.fake_server import FakeCTraderServer
from tests.recording_logger import RecordingLogger

NEW_ACCESS_TOKEN = "new-access-token"
NEW_REFRESH_TOKEN = "new-refresh-token"


def config(**overrides) -> CTraderDataClientConfig:
    values = {
        "client_id": "client-id",
        "client_secret": "client-secret",
        "access_token": "access-token",
        "refresh_token": "refresh-token",
        "token_expires_at": 4_102_444_800.0,
        "trader_login": TRADER_LOGIN,
        "instrument_provider": InstrumentProviderConfig(),
    }
    values.update(overrides)
    return CTraderDataClientConfig(**values)


def factory_venue() -> FakeCTraderServer:
    server = venue()
    server.on(
        om.PROTO_OA_REFRESH_TOKEN_REQ,
        lambda _r: oa.ProtoOARefreshTokenRes(
            accessToken=NEW_ACCESS_TOKEN,
            tokenType="bearer",
            expiresIn=2_592_000,
            refreshToken=NEW_REFRESH_TOKEN,
        ),
    )
    return server


def seed_account(
    server: FakeCTraderServer,
    client_config: CTraderDataClientConfig,
) -> CTraderAccountClient:
    """The account client the factory will find in the cache, pointed at the fake server."""
    return get_cached_ctrader_account_client(
        trader_login=client_config.trader_login,
        credentials=client_config.credentials(),
        environment=client_config.environment,
        logger=RecordingLogger(),
        demo_host=server.host,
        live_host=server.host,
        port=server.port,
        tls=False,
    )


def create(
    client_config: CTraderDataClientConfig,
    *,
    name: str | None = None,
) -> CTraderDataClient:
    return CTraderLiveDataClientFactory.create(
        loop=asyncio.get_running_loop(),
        name=name or "CTRADER",
        config=client_config,
        msgbus=TestComponentStubs.msgbus(),
        cache=TestComponentStubs.cache(),
        clock=TestComponentStubs.clock(),
    )


@asynccontextmanager
async def running_server() -> AsyncIterator[FakeCTraderServer]:
    server = factory_venue()
    await server.start()
    try:
        yield server
    finally:
        await server.stop()


async def test_create_builds_a_data_client_over_the_cached_account() -> None:
    async with running_server() as server:
        client_config = config()
        account = seed_account(server, client_config)

        client = create(client_config)

        assert isinstance(client, CTraderDataClient)
        assert isinstance(client.instrument_provider, CTraderInstrumentProvider)
        assert client._account is account


async def test_create_builds_the_account_from_the_config_when_none_is_cached() -> None:
    """No `seed_account()`: the factory's own arguments reach `CTraderAccountClient`."""
    client_config = config(
        environment="demo",
        connect_timeout_secs=11.0,
        restore_retry_interval_secs=7.0,
    )

    account = create(client_config)._account

    assert account.trader_login == TRADER_LOGIN
    assert account._environment == "demo"
    assert account._connect_timeout_secs == 11.0
    assert account._restore_retry_interval_secs == 7.0


async def test_create_honours_the_client_name() -> None:
    async with running_server() as server:
        client_config = config()
        seed_account(server, client_config)

        client = create(client_config, name="CTRADER-001")

        assert client.id == ClientId("CTRADER-001")


async def test_two_clients_for_one_account_share_a_session() -> None:
    async with running_server() as server:
        client_config = config()
        seed_account(server, client_config)

        first = create(client_config, name="CTRADER-001")
        second = create(client_config, name="CTRADER-002")

        assert first._account is second._account


async def test_a_listener_registered_before_create_sees_the_refresh() -> None:
    async with running_server() as server:
        client_config = config()
        account = seed_account(server, client_config)
        persisted: list[tuple[str, str, float]] = []
        account.add_token_listener(lambda a, r, e: persisted.append((a, r, e)))

        client = create(client_config)
        await client._connect()
        try:
            assert account.session is not None
            await account.session.refresh_tokens()
        finally:
            await client._disconnect()

        assert [(a, r) for a, r, _ in persisted] == [(NEW_ACCESS_TOKEN, NEW_REFRESH_TOKEN)]


def test_the_factory_is_a_nautilus_data_client_factory() -> None:
    assert issubclass(CTraderLiveDataClientFactory, LiveDataClientFactory)


def test_the_package_exports_what_an_application_needs() -> None:
    exported = {
        "CTRADER",
        "CTRADER_VENUE",
        "AccountCredentials",
        "CTraderDataClientConfig",
        "CTraderInstrumentProvider",
        "CTraderLiveDataClientFactory",
        "account_client_from_config",
        "get_cached_ctrader_account_client",
    }

    assert exported <= set(nautilus_ctrader.__all__)
    for name in nautilus_ctrader.__all__:
        assert getattr(nautilus_ctrader, name) is not None
