"""`TradingNode` factories for the cTrader clients.

The node hands a config to `create()` and gets back a client. Everything the account needs -
host selection, authentication, token refresh - hangs off the account client behind
`get_cached_ctrader_account_client`, so every client configured for the same account shares
one connection.

An application that persists refreshed tokens must call `get_cached_ctrader_account_client`
itself and register its listener **before** building the node; the factory then finds that
same instance.
"""

from __future__ import annotations

import asyncio

from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock, Logger, MessageBus
from nautilus_trader.live.factories import LiveDataClientFactory

from nautilus_ctrader.common.account import get_cached_ctrader_account_client
from nautilus_ctrader.config import CTraderDataClientConfig, parse_asset_class_overrides
from nautilus_ctrader.data import CTraderDataClient
from nautilus_ctrader.providers import CTraderInstrumentProvider


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
            or `config.environment` is not one of "auto", "demo", "live".

        Notes
        -----
        An account client already cached for this account is returned as it is: the connection
        settings of the second config for one account are ignored.

        """
        logger = Logger(name)
        account = get_cached_ctrader_account_client(
            account_id=config.account_id,
            credentials=config.credentials(),
            environment=config.environment,
            logger=logger,
            connect_timeout_secs=config.connect_timeout_secs,
            restore_retry_interval_secs=config.restore_retry_interval_secs,
        )
        provider = CTraderInstrumentProvider(
            account,
            config.instrument_provider,
            parse_asset_class_overrides(config.asset_class_overrides),
            config.fail_on_instrument_error,
            logger,
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
