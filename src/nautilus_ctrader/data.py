"""`CTraderDataClient`: the Nautilus live market-data client for one cTrader account.

It translates and routes only. Which instruments to load, whether to price them in the
account currency, and what to do with a quote are all the application's decisions, taken
through the config object and the Nautilus subscription commands.
"""

from __future__ import annotations

import asyncio
import functools
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal

from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock, MessageBus
from nautilus_trader.data.messages import (
    RequestInstrument,
    RequestInstruments,
    SubscribeQuoteTicks,
    UnsubscribeQuoteTicks,
)
from nautilus_trader.live.data_client import LiveMarketDataClient
from nautilus_trader.model.identifiers import ClientId, InstrumentId
from nautilus_trader.model.instruments import Instrument

from nautilus_ctrader.common.account import CTraderAccountClient
from nautilus_ctrader.common.errors import CTraderError, CTraderRequestError
from nautilus_ctrader.common.parsing import quote_from_prices
from nautilus_ctrader.common.session import CTraderSession
from nautilus_ctrader.config import CTraderDataClientConfig, parse_asset_class_overrides
from nautilus_ctrader.constants import CTRADER_VENUE
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.providers import CTraderInstrumentProvider, InstrumentLoadError

CONVERSION_CONSUMER = "conversion"

SpotListener = Callable[[oa.ProtoOASpotEvent], None]


def _quote_consumer(instrument_id: InstrumentId) -> str:
    return f"quotes:{instrument_id}"


def _conversion_message(quote: str, deposit: str, symbols: list[str]) -> str:
    """The INFO line reporting a subscribed currency-conversion chain."""
    return f"Conversion {quote}->{deposit}: subscribed {', '.join(symbols)}"


@dataclass
class _Book:
    """The last bid and ask seen for one symbol.

    A spot event carries only the side that moved, so both are kept until a tick can be built.
    """

    bid: int | None = None
    ask: int | None = None


@dataclass(frozen=True)
class _QuoteSubscription:
    symbol_id: int
    listener: SpotListener


class CTraderDataClient(LiveMarketDataClient):
    """
    Market data for one cTrader account: instruments, currency conversion and quote ticks.

    Parameters
    ----------
    loop : asyncio.AbstractEventLoop
        The event loop for the client.
    account : CTraderAccountClient
        The account's connection, shared with every other client of the same account.
    msgbus : MessageBus
        The message bus for the client.
    cache : Cache
        The cache for the client.
    clock : LiveClock
        The clock for the client.
    instrument_provider : CTraderInstrumentProvider
        The instrument provider, built over the same `account`.
    config : CTraderDataClientConfig
        The configuration for the client.
    name : str, optional
        The custom client ID.

    Raises
    ------
    ValueError
        If `config.asset_class_overrides` holds a value that is not an `AssetClass` name.

    """

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        account: CTraderAccountClient,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
        instrument_provider: CTraderInstrumentProvider,
        config: CTraderDataClientConfig,
        name: str | None = None,
    ) -> None:
        super().__init__(
            loop=loop,
            client_id=ClientId(name or CTRADER_VENUE.value),
            venue=CTRADER_VENUE,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=instrument_provider,
        )
        self._instrument_provider: CTraderInstrumentProvider = instrument_provider
        self._account = account
        self._config = config
        # Parsed here so a misspelled class name fails at construction, not at the first load.
        self._asset_class_overrides = parse_asset_class_overrides(config.asset_class_overrides)
        self._quote_size = (
            Decimal(0)
            if config.synthetic_quote_size is None
            else Decimal(str(config.synthetic_quote_size))
        )

        self._session: CTraderSession | None = None
        self._books: dict[int, _Book] = {}
        self._quote_subscriptions: dict[InstrumentId, _QuoteSubscription] = {}
        # Every (symbol_id, consumer) the registry may still count for this client. The
        # registry records a cancelled or timed-out subscribe, so a hold is added before the
        # request goes out and dropped only once its unsubscribe has returned.
        self._spot_holds: set[tuple[int, str]] = set()
        self._converted: set[InstrumentId] = set()

    @property
    def instrument_provider(self) -> CTraderInstrumentProvider:
        return self._instrument_provider

    async def _connect(self) -> None:
        # A chain is venue data that can change, so it is re-queried once per connection.
        self._instrument_provider.reset_conversion_cache()
        self._converted.clear()

        await self._account.connect()
        await self._instrument_provider.initialize()

        session = self._account.session
        assert session is not None  # `connect()` returned, so the session is up
        self._session = session
        session.add_event_handler(oa.ProtoOASymbolChangedEvent, self._on_symbol_changed)

        # Conversion first: an instrument whose value in the account currency cannot be priced
        # is dropped here, and must never reach the data engine or the cache.
        await self._connect_conversions()

        for instrument in self._instrument_provider.list_all():
            self._handle_data(instrument)

    async def _disconnect(self) -> None:
        try:
            for subscription in self._quote_subscriptions.values():
                self._remove_quote_listener(subscription)
            self._quote_subscriptions.clear()
            if self._session is not None:
                self._session.remove_event_handler(
                    oa.ProtoOASymbolChangedEvent,
                    self._on_symbol_changed,
                )
                self._session = None
            # A snapshot: releasing a hold drops it from the set.
            for symbol_id, consumer in sorted(self._spot_holds):
                await self._release_spots(symbol_id, consumer)
        finally:
            await self._account.disconnect()

    # -- Instruments ------------------------------------------------------------------------

    async def _request_instrument(self, request: RequestInstrument) -> None:
        instrument = self._instrument_provider.find(request.instrument_id)
        if instrument is None:
            self._log.error(f"Cannot request instrument: {request.instrument_id} is not loaded")
            return
        self._handle_instrument(
            instrument,
            request.id,
            request.start,
            request.end,
            request.params,
        )

    async def _request_instruments(self, request: RequestInstruments) -> None:
        self._handle_instruments(
            request.venue,
            self._instrument_provider.list_all(),
            request.id,
            request.start,
            request.end,
            request.params,
        )

    def _on_symbol_changed(self, event: oa.ProtoOASymbolChangedEvent) -> None:
        for symbol_id in event.symbolId:
            self.create_task(
                self._reload_symbol(symbol_id),
                log_msg=f"reload symbol {symbol_id}",
            )

    async def _reload_symbol(self, symbol_id: int) -> None:
        provider = self._instrument_provider
        changed = provider.instrument_for_symbol_id(symbol_id)
        name = f"symbol id {symbol_id}" if changed is None else changed.id.symbol.value
        self._log.warning(f"Symbol changed at the venue: {name}; reloading")
        if provider.reset_conversion_cache(symbol_id):
            # A leg of a cached chain changed, so every chain is resolved again on demand.
            self._converted.clear()
        try:
            reloaded = await provider.reload(symbol_id)
        except (InstrumentLoadError, CTraderError) as e:
            self._log.error(f"Reload of {name} failed: {e}")
            return
        self._handle_data(reloaded)

    # -- Conversion -------------------------------------------------------------------------

    async def _connect_conversions(self) -> None:
        if not self._config.subscribe_conversion_quotes:
            return
        for instrument_id in self._requested_instrument_ids():
            instrument = self._instrument_provider.find(instrument_id)
            if instrument is not None:
                await self._prepare_conversion(instrument)

    def _requested_instrument_ids(self) -> list[InstrumentId]:
        """The configured `load_ids`, whose conversion is resolved during connect.

        Instruments that arrived through `load_all` are converted on their first subscription
        instead: resolving a chain for every symbol the broker offers would be pointless work.
        """
        requested = self._config.instrument_provider.load_ids or ()
        return [i if isinstance(i, InstrumentId) else InstrumentId.from_str(i) for i in requested]

    async def _prepare_conversion(self, instrument: Instrument) -> list[Instrument] | None:
        """Subscribe the chain pricing `instrument`'s quote currency in the deposit currency.

        Returns the chain's instruments for the caller to publish, an empty list if there was
        nothing to do, or `None` if the chain could not be resolved. In that last case
        `instrument` has been dropped from the provider, so nothing downstream can trade an
        instrument whose value in the account currency is unknown.

        Raises `InstrumentLoadError` or `CTraderError` instead of returning `None` when
        `fail_on_instrument_error` is set.
        """
        if instrument.id in self._converted:
            return []
        try:
            chain = await self._instrument_provider.conversion_instruments_for(instrument)
        except (InstrumentLoadError, CTraderError) as e:
            self._instrument_provider.remove_failed(instrument.id, f"conversion failed: {e}")
            if self._config.fail_on_instrument_error:
                raise
            return None
        self._converted.add(instrument.id)
        if not chain:
            return []
        for leg in chain:
            await self._hold_spots(leg.info["symbol_id"], CONVERSION_CONSUMER)
        self._log.info(
            _conversion_message(
                instrument.quote_currency.code,
                self._account.deposit_asset.name,
                [leg.id.symbol.value for leg in chain],
            ),
        )
        return chain

    # -- Quotes -----------------------------------------------------------------------------

    async def _subscribe_quote_ticks(self, command: SubscribeQuoteTicks) -> None:
        instrument_id = command.instrument_id
        if instrument_id in self._quote_subscriptions:
            return
        instrument = self._instrument_provider.find(instrument_id)
        if instrument is None:
            self._log.error(f"Cannot subscribe quotes: {instrument_id} is not loaded")
            return

        if self._config.subscribe_conversion_quotes:
            # Before the venue subscription, so no quote is ever published for an instrument
            # that cannot be priced in the account currency.
            chain = await self._prepare_conversion(instrument)
            if chain is None:
                self._log.error(f"Cannot subscribe quotes: {instrument_id} has no conversion")
                return
            for leg in chain:
                self._handle_data(leg)

        symbol_id = instrument.info["symbol_id"]
        listener = functools.partial(self._on_quote_spot, instrument_id)
        self._quote_subscriptions[instrument_id] = _QuoteSubscription(symbol_id, listener)
        self._account.subscriptions.add_spot_listener(symbol_id, listener)
        await self._hold_spots(symbol_id, _quote_consumer(instrument_id))

    async def _unsubscribe_quote_ticks(self, command: UnsubscribeQuoteTicks) -> None:
        instrument_id = command.instrument_id
        subscription = self._quote_subscriptions.pop(instrument_id, None)
        if subscription is None:
            return
        self._remove_quote_listener(subscription)
        await self._release_spots(subscription.symbol_id, _quote_consumer(instrument_id))

    def _remove_quote_listener(self, subscription: _QuoteSubscription) -> None:
        self._account.subscriptions.remove_spot_listener(
            subscription.symbol_id,
            subscription.listener,
        )
        self._books.pop(subscription.symbol_id, None)

    def _on_quote_spot(self, instrument_id: InstrumentId, event: oa.ProtoOASpotEvent) -> None:
        # Looked up per event rather than captured, so a reloaded instrument's precisions apply.
        instrument = self._instrument_provider.find(instrument_id)
        if instrument is None:
            return
        book = self._books.setdefault(event.symbolId, _Book())
        if event.HasField("bid"):
            book.bid = event.bid
        if event.HasField("ask"):
            book.ask = event.ask
        if book.bid is None or book.ask is None:
            return
        ts_init = self._clock.timestamp_ns()
        self._handle_data(
            quote_from_prices(
                instrument_id,
                book.bid,
                book.ask,
                instrument.price_precision,
                self._quote_size,
                instrument.size_precision,
                event.timestamp * 1_000_000 if event.HasField("timestamp") else ts_init,
                ts_init,
            ),
        )

    # -- Subscription bookkeeping -----------------------------------------------------------

    async def _hold_spots(self, symbol_id: int, consumer: str) -> None:
        hold = (symbol_id, consumer)
        # Recorded before the request: the registry counts a subscribe whose outcome is
        # unknown, and only a venue refusal leaves it uncounted.
        self._spot_holds.add(hold)
        try:
            await self._account.subscriptions.subscribe_spots(symbol_id, consumer)
        except CTraderRequestError:
            self._spot_holds.discard(hold)
            raise

    async def _release_spots(self, symbol_id: int, consumer: str) -> None:
        await self._account.subscriptions.unsubscribe_spots(symbol_id, consumer)
        self._spot_holds.discard((symbol_id, consumer))
