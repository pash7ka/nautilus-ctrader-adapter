"""`TradingNode` factories for the cTrader clients.

The node hands a config to `create()` and gets back a client. Everything the account needs -
host selection, authentication, token refresh, the instrument provider - hangs off the account
client behind `get_cached_ctrader_account_client`, so every client configured for the same
account shares one connection and one provider.

An application that persists refreshed tokens must call `account_client_from_config` itself
and register its listener **before** building the node; the factory then finds that same
instance.
"""

from __future__ import annotations

import asyncio

from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock, Logger, MessageBus
from nautilus_trader.live.factories import LiveDataClientFactory, LiveExecClientFactory

from nautilus_ctrader.common.account import account_client_from_config
from nautilus_ctrader.config import (
    CTraderDataClientConfig,
    CTraderExecClientConfig,
    parse_asset_class_overrides,
)
from nautilus_ctrader.data import CTraderDataClient
from nautilus_ctrader.execution import CTraderExecutionClient


class CTraderLiveDataClientFactory(LiveDataClientFactory):
    """
    Provides a cTrader live market data client factory.
    """

    @staticmethod
    def create(  # type: ignore[override]
        loop: asyncio.AbstractEventLoop,
        name: str,
        config: CTraderDataClientConfig,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
    ) -> CTraderDataClient:
        """
        Create a new cTrader data client.

        Parameters
        ----------
        loop : asyncio.AbstractEventLoop
            The event loop for the client.
        name : str
            The custom client ID.
        config : CTraderDataClientConfig
            The client configuration.
        msgbus : MessageBus
            The message bus for the client.
        cache : Cache
            The cache for the client.
        clock : LiveClock
            The clock for the client.

        Returns
        -------
        CTraderDataClient

        Raises
        ------
        ValueError
            If `config.asset_class_overrides` holds a value that is not an `AssetClass` name,
            or the account's instrument provider was already built from other instrument
            settings (`instrument_provider`, `asset_class_overrides`,
            `fail_on_instrument_error`).

        Notes
        -----
        An account client already cached for this account is returned as is, and the
        application's own `account_client_from_config` call is the authoritative one: the
        connection settings of every later config for that account are ignored, and the
        client keeps the `Logger(name)` of the first `create`, so its account-level lines
        carry that client's component name. The instrument provider is the account's too,
        built from the first config that asks for it.

        """
        logger = Logger(name)
        account = account_client_from_config(config, logger)
        provider = account.get_instrument_provider(
            config=config.instrument_provider,
            asset_class_overrides=parse_asset_class_overrides(config.asset_class_overrides),
            fail_on_instrument_error=config.fail_on_instrument_error,
            logger=logger,
        )
        return CTraderDataClient(
            loop=loop,
            account=account,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=provider,
            config=config,
            name=name,
        )


class CTraderLiveExecClientFactory(LiveExecClientFactory):
    """
    Provides a cTrader live execution client factory.
    """

    @staticmethod
    def create(  # type: ignore[override]
        loop: asyncio.AbstractEventLoop,
        name: str,
        config: CTraderExecClientConfig,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
    ) -> CTraderExecutionClient:
        """
        Create a new cTrader execution client.

        Parameters
        ----------
        loop : asyncio.AbstractEventLoop
            The event loop for the client.
        name : str
            The custom client ID.
        config : CTraderExecClientConfig
            The client configuration.
        msgbus : MessageBus
            The message bus for the client.
        cache : Cache
            The cache for the client.
        clock : LiveClock
            The clock for the client.

        Returns
        -------
        CTraderExecutionClient

        Raises
        ------
        ValueError
            If the account has no instrument provider yet: a cTrader data client for the same
            account must be configured, and the node builds data clients first.

        """
        logger = Logger(name)
        account = account_client_from_config(config, logger)
        provider = account.instrument_provider
        if provider is None:
            raise ValueError(
                "the account has no instrument provider: configure a cTrader data client for "
                "the same account, which builds it",
            )
        return CTraderExecutionClient(
            loop=loop,
            account=account,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=provider,
            config=config,
            name=name,
        )
