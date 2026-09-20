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
from nautilus_ctrader.common.errors import (
    CTraderError,
    CTraderProtocolError,
    CTraderRequestError,
)
from nautilus_ctrader.common.parsing import quote_from_prices
from nautilus_ctrader.common.session import CTraderSession
from nautilus_ctrader.config import CTraderDataClientConfig, parse_asset_class_overrides
from nautilus_ctrader.constants import CTRADER_VENUE
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.providers import CTraderInstrumentProvider, InstrumentLoadError

CONVERSION_CONSUMER = "conversion"
# Restore key for the reset that a reconnect needs; also the reason it must run first.
_BOOK_RESET_RESTORE = "quote_books"

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


class CTraderDataClient(LiveMarketDataClient):
    """
    Market data for one cTrader account: instruments, currency conversion and quote ticks.

    Quote ticks are published for subscribed instruments and for every leg of a
    currency-conversion chain, because Nautilus builds its exchange rates from the quotes in
    the cache, not from the subscriptions.

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
        # The consumers holding each symbol's spot subscription. A hold is recorded before its
        # request goes out, because the registry counts a subscribe whose outcome is unknown,
        # and dropped once its unsubscribe starts or the venue refuses the subscribe outright.
        self._spot_holds: dict[int, set[str]] = {}
        # Holds whose unsubscribe is in flight. They no longer publish, but a cancelled release
        # must still be repeated, so they stay here until the request returns.
        self._releasing: set[tuple[int, str]] = set()
        # One listener per symbol however many holds it has: a symbol that is both subscribed
        # and a conversion leg would otherwise emit every quote once per listener.
        self._spot_listeners: dict[int, SpotListener] = {}
        self._converted: set[InstrumentId] = set()
        self._conversion_legs: dict[InstrumentId, frozenset[int]] = {}
        # Keys already reported as undeliverable; a repeat is logged at DEBUG, since a broken
        # scale would otherwise flood the log on every tick.
        self._quote_errors: set[InstrumentId] = set()
        self._unknown_symbols: set[int] = set()

    @property
    def instrument_provider(self) -> CTraderInstrumentProvider:
        return self._instrument_provider

    async def _connect(self) -> None:
        # A chain is venue data that can change, so it is re-queried once per connection.
        self._instrument_provider.reset_conversion_cache()
        self._converted.clear()
        self._conversion_legs.clear()
        # A problem that survives a reconnect is worth reporting at full volume again.
        self._quote_errors.clear()
        self._unknown_symbols.clear()

        await self._account.connect()
        await self._instrument_provider.initialize()

        session = self._account.session
        assert session is not None  # `connect()` returned, so the session is up
        self._session = session
        session.add_event_handler(oa.ProtoOASymbolChangedEvent, self._on_symbol_changed)
        # Registered before any subscription: restores run in registration order, so this one
        # empties the books before the first re-subscribed spot of a new connection arrives.
        session.add_restore(_BOOK_RESET_RESTORE, self._reset_books)

        # Conversion first: an instrument whose value in the account currency cannot be priced
        # is dropped here, and must never reach the data engine or the cache.
        await self._connect_conversions()

        for instrument in self._instrument_provider.list_all():
            self._handle_data(instrument)

    async def _disconnect(self) -> None:
        try:
            if self._session is not None:
                self._session.remove_event_handler(
                    oa.ProtoOASymbolChangedEvent,
                    self._on_symbol_changed,
                )
                self._session.remove_restore(_BOOK_RESET_RESTORE)
                self._session = None
            # A snapshot: releasing a hold drops it from the mapping.
            for symbol_id, consumer in sorted(self._all_holds()):
                await self._release_spots(symbol_id, consumer)
            self._conversion_legs.clear()
            self._converted.clear()
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
            # The holds stay until then, and re-resolution releases the ones that drop out.
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
                await self._prepare_conversion(instrument, during_connect=True)

    def _requested_instrument_ids(self) -> list[InstrumentId]:
        """The configured `load_ids`, whose conversion is resolved during connect.

        Instruments that arrived through `load_all` are converted on their first subscription
        instead: resolving a chain for every symbol the broker offers would be pointless work.
        """
        requested = self._config.instrument_provider.load_ids or ()
        return [i if isinstance(i, InstrumentId) else InstrumentId.from_str(i) for i in requested]

    async def _prepare_conversion(
        self,
        instrument: Instrument,
        *,
        during_connect: bool = False,
    ) -> list[Instrument] | None:
        """Subscribe the chain pricing `instrument`'s quote currency in the deposit currency.

        Returns the chain's instruments for the caller to publish, an empty list if there was
        nothing to do, or `None` if the chain is not usable, for one of two reasons:

        - the chain does not exist, or a leg cannot be built. The instrument can never be
          priced in the account currency, so it is dropped from the provider (D6), and
          `fail_on_instrument_error` raises instead of returning.
        - the venue refused the subscription. The instrument is still tradable, so it stays
          loaded and unconverted and the next subscribe tries the chain again - unless this is
          the bring-up and `fail_on_instrument_error` is set, which asks for an instrument that
          cannot be priced to fail the connect rather than start unpriced.
        """
        if instrument.id in self._converted:
            return []
        # Read before the first await: a symbol change during it resets the chain cache, and
        # what this call resolved must then not be recorded as current.
        generation = self._instrument_provider.conversion_generation
        try:
            chain = await self._instrument_provider.conversion_instruments_for(instrument)
        except (InstrumentLoadError, CTraderError) as e:
            self._instrument_provider.remove_failed(instrument.id, f"conversion failed: {e}")
            if self._config.fail_on_instrument_error:
                raise
            return None
        try:
            await self._hold_chain(chain)
        except CTraderError as e:
            if during_connect and self._config.fail_on_instrument_error:
                raise
            detail = e.error_code if isinstance(e, CTraderRequestError) else type(e).__name__
            self._log.warning(
                f"Conversion for {instrument.id} not subscribed ({detail}); "
                f"retrying on the next subscribe",
            )
            return None

        legs = frozenset(leg.info["symbol_id"] for leg in chain)
        await self._retire_old_legs(instrument.id, legs)
        # Only now, and only if the chain is still the current one: neither a half-subscribed
        # nor a stale chain may count as converted.
        if generation == self._instrument_provider.conversion_generation:
            self._converted.add(instrument.id)
        if not chain:
            return []
        self._log.info(
            _conversion_message(
                instrument.quote_currency.code,
                self._account.deposit_asset.name,
                [leg.id.symbol.value for leg in chain],
            ),
        )
        return chain

    async def _hold_chain(self, chain: list[Instrument]) -> None:
        """Subscribe every leg's spots, releasing the ones this call added if one is refused."""
        added: list[int] = []
        try:
            for leg in chain:
                symbol_id = leg.info["symbol_id"]
                if await self._hold_spots(symbol_id, CONVERSION_CONSUMER):
                    added.append(symbol_id)
        except CTraderError:
            for symbol_id in added:
                await self._release_spots(symbol_id, CONVERSION_CONSUMER)
            raise

    async def _retire_old_legs(self, instrument_id: InstrumentId, legs: frozenset[int]) -> None:
        """Release the legs this instrument no longer converts through and nothing else holds."""
        previous = self._conversion_legs.get(instrument_id, frozenset())
        self._conversion_legs[instrument_id] = legs
        for symbol_id in previous - legs:
            if not any(symbol_id in held for held in self._conversion_legs.values()):
                await self._release_spots(symbol_id, CONVERSION_CONSUMER)

    # -- Quotes -----------------------------------------------------------------------------

    async def _subscribe_quote_ticks(self, command: SubscribeQuoteTicks) -> None:
        instrument_id = command.instrument_id
        instrument = self._instrument_provider.find(instrument_id)
        if instrument is None:
            self._log.error(f"Cannot subscribe quotes: {instrument_id} is not loaded")
            return

        if self._config.subscribe_conversion_quotes:
            # Before the venue subscription, so no quote is ever published for an instrument
            # that cannot be priced in the account currency.
            chain = await self._prepare_conversion(instrument)
            if chain is None:
                self._report_missing_conversion(instrument_id)
                return
            for leg in chain:
                self._handle_data(leg)

        await self._hold_spots(instrument.info["symbol_id"], _quote_consumer(instrument_id))

    def _report_missing_conversion(self, instrument_id: InstrumentId) -> None:
        """Say which of the two ways the conversion failed, because they need different acts."""
        if self._instrument_provider.find(instrument_id) is None:
            self._log.error(
                f"Cannot subscribe quotes: {instrument_id} cannot be priced in the account "
                f"currency and has been dropped",
            )
        else:
            self._log.warning(
                f"Not subscribing quotes for {instrument_id}: the venue refused its conversion "
                f"chain; subscribe again to retry",
            )

    async def _unsubscribe_quote_ticks(self, command: UnsubscribeQuoteTicks) -> None:
        # Found by consumer name rather than through the instrument's symbol id, because the
        # instrument may have been dropped since and the hold must be released even then.
        consumer = _quote_consumer(command.instrument_id)
        symbol_id = next((s for s, c in self._all_holds() if c == consumer), None)
        if symbol_id is not None:
            await self._release_spots(symbol_id, consumer)

    def _on_quote_spot(self, symbol_id: int, event: oa.ProtoOASpotEvent) -> None:
        # Looked up per event rather than captured, so a reloaded instrument's precisions apply.
        instrument = self._instrument_provider.instrument_for_symbol_id(symbol_id)
        if instrument is None:
            # The instrument was dropped while its spots were still held: nothing is published
            # for this symbol any more, and the holder has not noticed.
            self._log_once(
                self._unknown_symbols,
                symbol_id,
                f"Dropped spot for symbol {symbol_id}: its instrument is no longer loaded",
                self._log.warning,
            )
            return
        instrument_id = instrument.id
        book = self._books.setdefault(event.symbolId, _Book())
        if event.HasField("bid"):
            book.bid = event.bid
        if event.HasField("ask"):
            book.ask = event.ask
        if book.bid is None or book.ask is None:
            return
        ts_init = self._clock.timestamp_ns()
        try:
            quote = quote_from_prices(
                instrument_id,
                book.bid,
                book.ask,
                instrument.price_precision,
                self._quote_size,
                instrument.size_precision,
                event.timestamp * 1_000_000 if event.HasField("timestamp") else ts_init,
                ts_init,
            )
        except CTraderProtocolError as e:
            # Raising here would reach the account's shared spot dispatcher and disable this
            # listener for every later event.
            self._log_once(
                self._quote_errors,
                instrument_id,
                f"Dropped quote for {instrument_id}: {e}",
                self._log.error,
            )
            return
        self._handle_data(quote)

    def _log_once(
        self,
        seen: set,
        key: object,
        message: str,
        first: Callable[[str], None],
    ) -> None:
        """Report `message` through `first` the first time `key` reports it, at DEBUG after.

        These conditions repeat on every tick, so the full level is spent on the first one.
        """
        if key in seen:
            self._log.debug(message)
            return
        seen.add(key)
        first(message)

    async def _reset_books(self) -> None:
        """Forget every remembered bid and ask.

        Run by the session's restores: after a reconnect a side from before the outage must
        never be paired with one from after it.
        """
        self._books.clear()

    # -- Subscription bookkeeping -----------------------------------------------------------

    async def _hold_spots(self, symbol_id: int, consumer: str) -> bool:
        """Subscribe `symbol_id`'s spots for `consumer`; returns whether this call added it."""
        consumers = self._spot_holds.setdefault(symbol_id, set())
        if consumer in consumers:
            return False
        consumers.add(consumer)
        self._sync_listener(symbol_id)
        try:
            await self._account.subscriptions.subscribe_spots(symbol_id, consumer)
        except CTraderRequestError:
            # A venue refusal is the one outcome the registry does not count.
            self._discard_hold(symbol_id, consumer)
            self._sync_listener(symbol_id)
            raise
        return True

    async def _release_spots(self, symbol_id: int, consumer: str) -> None:
        key = (symbol_id, consumer)
        if consumer not in self._spot_holds.get(symbol_id, ()) and key not in self._releasing:
            return
        # Dropped before the request, so nothing is published while it is in flight and a
        # release of another consumer cannot re-attach a listener for it meanwhile. The key
        # stays in `_releasing` until the request returns, so a cancelled release is repeated.
        self._discard_hold(symbol_id, consumer)
        self._releasing.add(key)
        self._sync_listener(symbol_id)
        await self._account.subscriptions.unsubscribe_spots(symbol_id, consumer)
        self._releasing.discard(key)

    def _discard_hold(self, symbol_id: int, consumer: str) -> None:
        consumers = self._spot_holds.get(symbol_id)
        if consumers is None:
            return
        consumers.discard(consumer)
        if not consumers:
            del self._spot_holds[symbol_id]

    def _all_holds(self) -> set[tuple[int, str]]:
        """Every `(symbol id, consumer)` the registry may still count for this client."""
        held = {(s, c) for s, consumers in self._spot_holds.items() for c in consumers}
        return held | self._releasing

    def _sync_listener(self, symbol_id: int) -> None:
        """Keep exactly one listener on `symbol_id` while any hold remains."""
        held = symbol_id in self._spot_holds
        listener = self._spot_listeners.get(symbol_id)
        if held and listener is None:
            listener = functools.partial(self._on_quote_spot, symbol_id)
            self._spot_listeners[symbol_id] = listener
            self._account.subscriptions.add_spot_listener(symbol_id, listener)
        elif not held and listener is not None:
            del self._spot_listeners[symbol_id]
            self._account.subscriptions.remove_spot_listener(symbol_id, listener)
            self._books.pop(symbol_id, None)
