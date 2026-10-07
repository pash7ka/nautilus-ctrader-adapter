"""`CTraderDataClient`: the Nautilus live market-data client for one cTrader account.

It translates and routes only. Which instruments to load, whether to price them in the
account currency, and what to do with a quote are all the application's decisions, taken
through the config object and the Nautilus subscription commands.
"""

from __future__ import annotations

import asyncio
import functools
import time
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal

from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock, MessageBus
from nautilus_trader.data.messages import (
    RequestBars,
    RequestInstrument,
    RequestInstruments,
    SubscribeBars,
    SubscribeQuoteTicks,
    UnsubscribeBars,
    UnsubscribeQuoteTicks,
)
from nautilus_trader.live.data_client import LiveMarketDataClient
from nautilus_trader.model.data import BarType
from nautilus_trader.model.identifiers import ClientId, InstrumentId
from nautilus_trader.model.instruments import Instrument

from nautilus_ctrader.common.account import CTraderAccountClient
from nautilus_ctrader.common.bars import BarCloser, Clock, RawBar
from nautilus_ctrader.common.errors import (
    CTraderConnectionError,
    CTraderError,
    CTraderProtocolError,
    CTraderRequestError,
)
from nautilus_ctrader.common.parsing import (
    bar_boundary_secs,
    bar_from_trendbar,
    is_unaligned_boundary,
    quote_from_prices,
)
from nautilus_ctrader.common.session import CTraderSession
from nautilus_ctrader.config import CTraderDataClientConfig, parse_asset_class_overrides
from nautilus_ctrader.constants import CTRADER_VENUE
from nautilus_ctrader.enums import PERIOD_SECS, trendbar_period_for
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as om
from nautilus_ctrader.providers import CTraderInstrumentProvider, InstrumentLoadError

SpotListener = Callable[[oa.ProtoOASpotEvent], None]


def _quote_consumer(instrument_id: InstrumentId) -> str:
    return f"quotes:{instrument_id}"


def _conversion_consumer(instrument_id: InstrumentId) -> str:
    """One name per instrument, so the registry counts every instrument a shared leg serves."""
    return f"conversion:{instrument_id}"


def _bar_consumer(bar_type: BarType) -> str:
    return f"bars:{bar_type}"


def _bar_restore_key(bar_type: BarType) -> tuple[str, str]:
    return ("bar_backfill", str(bar_type))


def _conversion_message(quote: str, deposit: str, symbols: list[str]) -> str:
    """The INFO line reporting a subscribed currency-conversion chain."""
    return f"Conversion {quote}->{deposit}: subscribed {', '.join(symbols)}"


def _raw_bar(trendbar: om.ProtoOATrendbar) -> RawBar:
    """A trendbar in the closer's own units."""
    return RawBar(
        boundary_secs=bar_boundary_secs(trendbar.utcTimestampInMinutes),
        low=trendbar.low,
        delta_open=trendbar.deltaOpen,
        delta_high=trendbar.deltaHigh,
        delta_close=trendbar.deltaClose,
        volume=trendbar.volume,
    )


def _trendbar(raw: RawBar) -> om.ProtoOATrendbar:
    """`raw` back as a trendbar, with no period: `bar_from_trendbar` takes it from the bar type."""
    return om.ProtoOATrendbar(
        volume=raw.volume,
        low=raw.low,
        deltaOpen=raw.delta_open,
        deltaHigh=raw.delta_high,
        deltaClose=raw.delta_close,
        utcTimestampInMinutes=raw.boundary_secs // 60,
    )


@dataclass
class _Book:
    """The last bid and ask seen for one symbol.

    A spot event carries only the side that moved, so both are kept until a tick can be built.
    """

    bid: int | None = None
    ask: int | None = None


@dataclass(frozen=True)
class _LoopClock:
    """The production `Clock` for the bar closers: wall time, and the event loop for timers."""

    loop: asyncio.AbstractEventLoop

    def now(self) -> float:
        return time.time()

    def call_later(self, delay_secs: float, callback: Callable[[], None]) -> asyncio.TimerHandle:
        return self.loop.call_later(delay_secs, callback)


@dataclass(frozen=True)
class _BarSub:
    """One live bar subscription: where its trendbars come from, and what closes them."""

    symbol_id: int
    period: int
    closer: BarCloser


class CTraderDataClient(LiveMarketDataClient):
    """
    Market data for one cTrader account: instruments, currency conversion, quote ticks and bars.

    Quote ticks are published for subscribed instruments and for every leg of a
    currency-conversion chain, because Nautilus builds its exchange rates from the quotes in
    the cache, not from the subscriptions.

    Bars arrive as trendbars inside the same spot events, so a bar subscription holds the spot
    subscription the venue requires for it; which of them is closed, and when, is decided by a
    `BarCloser` per bar type.

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
        The account's own provider, from `account.get_instrument_provider()`.
    config : CTraderDataClientConfig
        The configuration for the client.
    name : str, optional
        The custom client ID.

    Raises
    ------
    ValueError
        If `instrument_provider` is not the account's own, or `config.asset_class_overrides`
        holds a value that is not an `AssetClass` name.

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
        # Identity, not equality: a second provider would miss the account's chain reset, and
        # an instrument it unloads would stay loaded for every other client of the account.
        if instrument_provider is not account.instrument_provider:
            raise ValueError(
                "instrument_provider must be the account's own, from "
                "account.get_instrument_provider()",
            )
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

        # Scopes this client's consumer names in the account's registry, which every client of
        # the account shares. The prefix because a data and an execution client may share a
        # `ClientId`; a string rather than the object so a client rebuilt under the same id
        # releases what an earlier instance left behind.
        self._owner = f"data:{self.id}"
        self._session: CTraderSession | None = None
        self._books: dict[int, _Book] = {}
        # The session and its `bring_up_generation` that `_books` were filled under. A session
        # is part of it because each one counts its bring-ups from zero.
        self._books_generation: tuple[CTraderSession, int] | None = None
        # One listener per symbol however many holds it has: a symbol that is both subscribed
        # and a conversion leg would otherwise emit every quote once per listener.
        self._spot_listeners: dict[int, SpotListener] = {}
        self._converted: set[InstrumentId] = set()
        # The provider's `conversion_generation` that `_converted` is valid for. The provider is
        # the account's, which resets it on a bring-up and on a symbol change.
        self._converted_generation = instrument_provider.conversion_generation
        # One preparation per instrument at a time: its legs are held under one consumer name,
        # so an older preparation finishing last would retire the legs of a newer chain.
        self._conversion_locks: dict[InstrumentId, asyncio.Lock] = {}
        # Subscribes still preparing their conversion, by consumer name. An unsubscribe drops
        # the entry, and a subscribe whose marker is gone then takes no hold.
        self._preparing: dict[str, set[object]] = {}
        # Advanced by every `_connect` and `_disconnect`, so a subscribe that awaited across one
        # knows its preparation belongs to a connection that is gone.
        self._connection_generation = 0
        self._bars: dict[BarType, _BarSub] = {}
        # The same subscriptions by the `(symbol id, period)` a live trendbar identifies itself
        # with, which is how a spot event's trendbars are routed.
        self._bar_routes: dict[tuple[int, int], _BarSub] = {}
        self._bar_clock: Clock = _LoopClock(loop)
        # Keys already reported as undeliverable; a repeat is logged at DEBUG, since a broken
        # scale would otherwise flood the log on every tick.
        self._quote_errors: set[InstrumentId] = set()
        self._bar_errors: set[BarType] = set()
        self._bar_phases: set[BarType] = set()
        self._unknown_symbols: set[int] = set()
        # The account counts its users without knowing who releases, and Nautilus calls
        # `_disconnect` even after a failed `_connect`: only a user this client holds is released.
        self._holds_account = False

    @property
    def instrument_provider(self) -> CTraderInstrumentProvider:
        return self._instrument_provider

    async def _connect(self) -> None:
        self._connection_generation += 1
        # Nothing from an earlier connection may survive here, in this client or in the
        # registry: a `_disconnect` cut short would otherwise make a later subscribe a silent
        # no-op against a session that is gone.
        self._forget_subscriptions()
        await self._release_subscriptions(None)
        self._converted.clear()
        # A problem that survives a reconnect is worth reporting at full volume again.
        self._quote_errors.clear()
        self._bar_errors.clear()
        self._bar_phases.clear()
        self._unknown_symbols.clear()

        await self._account.connect()
        self._holds_account = True
        await self._instrument_provider.initialize()

        session = self._account.session
        assert session is not None  # `connect()` returned, so the session is up
        self._session = session

        # Conversion first: an instrument whose value in the account currency cannot be priced
        # is dropped here, and must never reach the data engine or the cache.
        await self._connect_conversions()

        # Only once nothing above can fail, so a failed connect leaves no listener behind.
        self._account.add_reload_listener(self._on_instrument_reloaded)
        for instrument in self._instrument_provider.list_all():
            self._handle_data(instrument)

    async def _disconnect(self) -> None:
        self._connection_generation += 1
        self._preparing.clear()
        session, self._session = self._session, None
        self._account.remove_reload_listener(self._on_instrument_reloaded)
        try:
            await self._release_subscriptions(session)
            self._converted.clear()
        finally:
            if self._holds_account:
                self._holds_account = False
                await self._account.disconnect()

    async def _release_subscriptions(self, session: CTraderSession | None) -> None:
        """Release everything the registry still counts for this client.

        Cancelled releases included: the registry keeps counting those until one runs its
        course.
        """
        subscriptions = self._account.subscriptions
        for bar_type in sorted(self._bars, key=str):
            await self._drop_bars(bar_type, session)
        for symbol_id, period, consumer in sorted(subscriptions.trendbar_holds(self._owner)):
            await subscriptions.unsubscribe_trendbars(symbol_id, period, consumer, self._owner)
        for symbol_id, consumer in sorted(subscriptions.spot_holds(self._owner)):
            await self._release_spots(symbol_id, consumer)

    def _forget_subscriptions(self) -> None:
        """Drop this client's own routing of what was subscribed, without touching the venue.

        The backfill restores go too, from the account's session: another client may have
        kept it running.
        """
        session = self._account.session
        for bar_type, sub in self._bars.items():
            sub.closer.close()
            if session is not None:
                session.remove_restore(_bar_restore_key(bar_type))
        self._bars.clear()
        self._bar_routes.clear()
        for symbol_id, listener in self._spot_listeners.items():
            self._account.subscriptions.remove_spot_listener(symbol_id, listener)
        self._spot_listeners.clear()
        self._books.clear()
        self._preparing.clear()

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

    def _on_instrument_reloaded(self, instrument: Instrument) -> None:
        # The account has already dropped the chains through the changed symbol; whether a
        # conversion is prepared again is left to the generation check in `_prepare_conversion`.
        self._handle_data(instrument)

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
          priced in the account currency, so it is unloaded rather than left tradable by
          accident, and `fail_on_instrument_error` raises instead of returning.
        - the request could not be made or was refused. Nothing is known about the chain yet,
          so the instrument stays loaded and unconverted and the next subscribe tries again -
          unless this is the bring-up and `fail_on_instrument_error` is set, which asks for an
          instrument that cannot be priced to fail the connect rather than start unpriced.
        """
        lock = self._conversion_locks.setdefault(instrument.id, asyncio.Lock())
        async with lock:
            return await self._prepare_conversion_locked(instrument, during_connect=during_connect)

    async def _prepare_conversion_locked(
        self,
        instrument: Instrument,
        *,
        during_connect: bool,
    ) -> list[Instrument] | None:
        # Read before the first await: a symbol change during it resets the chain cache, and
        # what this call resolved must then not be recorded as current.
        generation = self._instrument_provider.conversion_generation
        if generation != self._converted_generation:
            # A reset since these were prepared: each chain is resolved again on demand. The
            # holds stay until then, and re-resolution releases the ones that drop out.
            self._converted.clear()
            self._converted_generation = generation
        if instrument.id in self._converted:
            return []
        try:
            chain = await self._instrument_provider.conversion_instruments_for(instrument)
        except (InstrumentLoadError, CTraderRequestError, CTraderProtocolError) as e:
            # Only these say something about the chain itself: no route, a leg that cannot be
            # built, or an answer that could not be read.
            self._instrument_provider.remove_failed(instrument.id, f"conversion failed: {e}")
            if self._config.fail_on_instrument_error:
                raise
            return None
        except CTraderError as e:
            # Everything else - a lost connection, a timeout, a session that cannot
            # authenticate - says nothing about the instrument, and dropping it would lose it
            # for the whole process over one reconnect.
            self._conversion_not_subscribed(instrument.id, e, during_connect=during_connect)
            return None
        consumer = _conversion_consumer(instrument.id)
        try:
            await self._hold_chain(chain, consumer)
        except CTraderError as e:
            self._conversion_not_subscribed(instrument.id, e, during_connect=during_connect)
            return None

        await self._retire_old_legs(consumer, {leg.info["symbol_id"] for leg in chain})
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

    def _conversion_not_subscribed(
        self,
        instrument_id: InstrumentId,
        error: CTraderError,
        *,
        during_connect: bool,
    ) -> None:
        """Report a chain left unsubscribed; raise instead if the bring-up asked to fail on it."""
        if during_connect and self._config.fail_on_instrument_error:
            raise error
        detail = (
            error.error_code if isinstance(error, CTraderRequestError) else type(error).__name__
        )
        self._log.warning(
            f"Conversion for {instrument_id} not subscribed ({detail}); "
            f"retrying on the next subscribe",
        )

    async def _hold_chain(self, chain: list[Instrument], consumer: str) -> None:
        """Subscribe every leg's spots, releasing the ones this call added if one is refused."""
        added: list[int] = []
        try:
            for leg in chain:
                symbol_id = leg.info["symbol_id"]
                if await self._hold_spots(symbol_id, consumer):
                    added.append(symbol_id)
        except CTraderError:
            for symbol_id in added:
                await self._release_spots(symbol_id, consumer)
            raise

    async def _retire_old_legs(self, consumer: str, legs: set[int]) -> None:
        """Release `consumer`'s holds on the symbols that are no longer among its `legs`.

        Another instrument's hold on the same symbol is its own, so the venue subscription stays
        while any instrument still converts through it.
        """
        held = self._account.subscriptions.spot_holds(self._owner)
        for symbol_id in sorted(s for s, c in held if c == consumer and s not in legs):
            await self._release_spots(symbol_id, consumer)

    # -- Quotes -----------------------------------------------------------------------------

    async def _subscribe_quote_ticks(self, command: SubscribeQuoteTicks) -> None:
        instrument_id = command.instrument_id
        instrument = self._instrument_provider.find(instrument_id)
        if instrument is None:
            self._log.error(f"Cannot subscribe quotes: {instrument_id} is not loaded")
            return

        consumer = _quote_consumer(instrument_id)
        if not await self._convert_before_subscribing(instrument, "quotes", consumer):
            return

        await self._hold_spots(instrument.info["symbol_id"], consumer)

    async def _convert_before_subscribing(
        self,
        instrument: Instrument,
        what: str,
        consumer: str,
    ) -> bool:
        """Resolve and publish `instrument`'s conversion chain; report and refuse if it fails.

        Run before the venue subscription, so nothing is ever published for an instrument that
        cannot be priced in the account currency. `what` names the subscription being refused.

        Also refuses, silently, if `consumer` was unsubscribed meanwhile: that unsubscribe found
        no hold to release. The chain stays held, as after a completed subscribe and unsubscribe.

        Refuses silently too if the client connected or disconnected meanwhile, releasing the
        instrument's chain: the release that came with it did not see what was still being held.
        """
        if not self._config.subscribe_conversion_quotes:
            return True
        connection = self._connection_generation
        marker = object()
        self._preparing.setdefault(consumer, set()).add(marker)
        try:
            chain = await self._prepare_conversion(instrument)
        finally:
            markers = self._preparing.get(consumer, set())
            wanted = marker in markers
            markers.discard(marker)
            if not markers:
                self._preparing.pop(consumer, None)
        if connection != self._connection_generation:
            # Before the release's first await, so a preparation queued behind this one for the
            # same instrument does not take the chain as converted.
            self._converted.discard(instrument.id)
            await self._retire_old_legs(_conversion_consumer(instrument.id), set())
            return False
        if chain is None:
            self._report_missing_conversion(instrument.id, what)
            return False
        for leg in chain:
            self._handle_data(leg)
        return wanted

    def _report_missing_conversion(self, instrument_id: InstrumentId, what: str) -> None:
        """Say which of the two ways the conversion failed, because they need different acts."""
        if self._instrument_provider.find(instrument_id) is None:
            self._log.error(
                f"Cannot subscribe {what}: {instrument_id} cannot be priced in the account "
                f"currency and has been dropped",
            )
        else:
            self._log.warning(
                f"Not subscribing {what} for {instrument_id}: its conversion chain was not "
                f"subscribed; subscribe again to retry",
            )

    async def _unsubscribe_quote_ticks(self, command: UnsubscribeQuoteTicks) -> None:
        # Found by consumer name rather than through the instrument's symbol id, because the
        # instrument may have been dropped since and the hold must be released even then.
        consumer = _quote_consumer(command.instrument_id)
        self._preparing.pop(consumer, None)
        held = self._account.subscriptions.spot_holds(self._owner)
        symbol_id = next((s for s, c in held if c == consumer), None)
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
        book = self._book(event.symbolId)
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

    def _book(self, symbol_id: int) -> _Book:
        """`symbol_id`'s book, every book emptied first if the session has been brought up since.

        After a reconnect a side from before the outage must never be paired with one from
        after it. Decided by generation rather than by a restore, because restores run in the
        order their keys were first registered, and another client's may run first.
        """
        session = self._account.session
        generation = None if session is None else (session, session.bring_up_generation)
        if generation != self._books_generation:
            self._books.clear()
            self._books_generation = generation
        return self._books.setdefault(symbol_id, _Book())

    # -- Bars -------------------------------------------------------------------------------

    async def _subscribe_bars(self, command: SubscribeBars) -> None:
        bar_type = command.bar_type
        # Enough: Nautilus records it before this task, and its engine gates on `subscribed_bars()`.
        if bar_type in self._bars:
            return
        try:
            period = trendbar_period_for(bar_type)
        except ValueError as e:
            self._log.error(f"Cannot subscribe bars: {e}")
            return
        instrument = self._instrument_provider.find(bar_type.instrument_id)
        if instrument is None:
            self._log.error(f"Cannot subscribe bars: {bar_type.instrument_id} is not loaded")
            return
        # A bars-only subscriber needs the account valued just as much as a quote subscriber
        # does. Before anything below is recorded, so a refused chain leaves nothing behind.
        consumer = _bar_consumer(bar_type)
        if not await self._convert_before_subscribing(instrument, "bars", consumer):
            return

        symbol_id = instrument.info["symbol_id"]
        # Created before the venue request: the closer ignores every boundary below the bar
        # forming at its construction, so building it afterwards would drop a bar that closed
        # while the request was in flight.
        sub = _BarSub(symbol_id, period, self._make_closer(bar_type, symbol_id, period))
        self._bars[bar_type] = sub
        self._bar_routes[(symbol_id, period)] = sub
        self._sync_listener(symbol_id)
        subscriptions = self._account.subscriptions
        try:
            await subscriptions.subscribe_trendbars(symbol_id, period, consumer, self._owner)
        finally:
            # What is kept here must match what the registry counts, whatever the outcome:
            # - a counted trendbar leg, unknown outcomes included, keeps the bars and needs the
            #   backfill restore;
            # - without one there are no live bars, so the bar type is freed for another try;
            #   a spot leg left counted is released with this client's other holds;
            # - an unsubscribe that ran meanwhile has already forgotten `sub`.
            if self._bars.get(bar_type) is sub:
                if consumer in subscriptions.trendbar_consumers(symbol_id, period, self._owner):
                    if self._session is not None:
                        # Added after the registry's own restore for this key, and so run after
                        # it: the backfill's history needs the reconnected subscription in place.
                        self._session.add_restore(
                            _bar_restore_key(bar_type),
                            functools.partial(self._restore_bars, sub),
                        )
                else:
                    self._forget_bars(bar_type, sub)

    async def _unsubscribe_bars(self, command: UnsubscribeBars) -> None:
        self._preparing.pop(_bar_consumer(command.bar_type), None)
        await self._drop_bars(command.bar_type, self._session)

    async def _drop_bars(self, bar_type: BarType, session: CTraderSession | None) -> None:
        subscriptions = self._account.subscriptions
        consumer = _bar_consumer(bar_type)
        sub = self._bars.get(bar_type)
        if sub is not None:
            self._forget_bars(bar_type, sub)
            symbol_id, period = sub.symbol_id, sub.period
        else:
            # Only what a cancellation cut short can be left - a release, or a subscribe that
            # got no further than its spot leg - and the registry still counts it.
            held = subscriptions.trendbar_holds(self._owner)
            found = next(((s, p) for s, p, c in held if c == consumer), None)
            if found is None:
                return
            symbol_id, period = found
        if session is not None:
            session.remove_restore(_bar_restore_key(bar_type))
        await subscriptions.unsubscribe_trendbars(symbol_id, period, consumer, self._owner)

    def _forget_bars(self, bar_type: BarType, sub: _BarSub) -> None:
        """Stop routing and closing `bar_type`, before its venue subscription is given up."""
        del self._bars[bar_type]
        self._bar_routes.pop((sub.symbol_id, sub.period), None)
        sub.closer.close()
        self._sync_listener(sub.symbol_id)

    def _make_closer(self, bar_type: BarType, symbol_id: int, period: int) -> BarCloser:
        return BarCloser(
            period_secs=PERIOD_SECS[period],
            grace_secs=self._config.bar_close_grace_secs,
            history_retries=self._config.bar_close_history_retries,
            clock=self._bar_clock,
            fetch=functools.partial(self._fetch_bar, symbol_id, period),
            emit=functools.partial(self._emit_bar, bar_type, symbol_id, PERIOD_SECS[period]),
            logger=self._log,
            label=str(bar_type),
        )

    async def _restore_bars(self, sub: _BarSub) -> None:
        sub.closer.on_disconnect()
        await sub.closer.backfill(functools.partial(self._fetch_bar_range, sub))

    def _on_bar_spot(self, symbol_id: int, event: oa.ProtoOASpotEvent) -> None:
        for trendbar in event.trendbar:
            # Routed by the bar's own period: a spot event carries one forming bar per
            # subscribed period. A historical bar leaves the field unset, where it would read
            # as M1, so an unset one is never routed.
            if not trendbar.HasField("period"):
                continue
            sub = self._bar_routes.get((symbol_id, trendbar.period))
            if sub is not None:
                sub.closer.on_update(_raw_bar(trendbar))

    def _emit_bar(self, bar_type: BarType, symbol_id: int, period_secs: int, raw: RawBar) -> None:
        self._note_bar_phase(bar_type, period_secs, raw.boundary_secs)
        # Looked up per bar rather than captured, so a reloaded instrument's precisions apply.
        instrument = self._instrument_provider.instrument_for_symbol_id(symbol_id)
        if instrument is None:
            self._log_once(
                self._bar_errors,
                bar_type,
                f"Dropped bar for {bar_type}: its instrument is no longer loaded",
                self._log.error,
            )
            return
        try:
            bar = bar_from_trendbar(
                _trendbar(raw),
                bar_type,
                instrument.price_precision,
                instrument.size_precision,
                self._clock.timestamp_ns(),
            )
        except CTraderProtocolError as e:
            self._log_once(
                self._bar_errors,
                bar_type,
                f"Dropped bar for {bar_type}: {e}",
                self._log.error,
            )
            return
        self._handle_data(bar)

    def _note_bar_phase(self, bar_type: BarType, period_secs: int, boundary_secs: int) -> None:
        """Report an open time that breaks the epoch alignment expected of the period.

        Reported, never corrected: the venue's open time is the bar's own, and a period whose
        boundaries carry the trading day's phase is what flooring used to get wrong.
        """
        if not is_unaligned_boundary(boundary_secs, period_secs):
            return
        self._log_once(
            self._bar_phases,
            bar_type,
            f"Open time {boundary_secs} of a {bar_type} bar is not a multiple of its period; "
            "published as the venue sent it",
            self._log.warning,
        )

    async def _request_bars(self, request: RequestBars) -> None:
        bar_type = request.bar_type
        try:
            period = trendbar_period_for(bar_type)
        except ValueError as e:
            self._log.error(f"Cannot request bars: {e}")
            return
        instrument = self._instrument_provider.find(bar_type.instrument_id)
        if instrument is None:
            self._log.error(f"Cannot request bars: {bar_type.instrument_id} is not loaded")
            return

        period_secs = PERIOD_SECS[period]
        now_secs = int(self._bar_clock.now())
        served = await self._page_trendbars(
            instrument.info["symbol_id"],
            period,
            start_secs=None if request.start is None else int(request.start.timestamp()),
            end_secs=now_secs if request.end is None else int(request.end.timestamp()),
            limit=request.limit or None,
            closed_secs=now_secs,
        )
        # History serves the forming bar too, which is not a bar yet.
        closed = [raw for raw in served if raw.boundary_secs + period_secs <= now_secs]
        if request.limit:
            closed = closed[-request.limit :]

        sub = self._bars.get(bar_type)
        if sub is not None and closed:
            # What this request delivered must not go out a second time from the stream.
            sub.closer.mark_emitted(closed[-1].boundary_secs)

        ts_init = self._clock.timestamp_ns()
        bars = [
            bar_from_trendbar(
                _trendbar(raw),
                bar_type,
                instrument.price_precision,
                instrument.size_precision,
                ts_init,
            )
            for raw in closed
        ]
        self._handle_bars(bar_type, bars, request.id, request.start, request.end, request.params)

    async def _page_trendbars(
        self,
        symbol_id: int,
        period: int,
        *,
        start_secs: int | None,
        end_secs: int,
        limit: int | None,
        closed_secs: int,
    ) -> list[RawBar]:
        """Bars opening in `[start_secs, end_secs)`, ascending, paging backwards from the end.

        `limit` counts only bars closed by `closed_secs`, so the forming bar history also
        serves never takes a closed bar's place. With neither a `start_secs` nor a `limit`
        exactly one page is asked for: the caller wanted the most recent bars, not the whole
        history.

        The venue counts a page's bars back from its `toTimestamp` without stopping at its
        `fromTimestamp`, so a page can reach past `start_secs`; those bars are dropped here.

        Only an empty page proves a window holds nothing: whenever one comes back non-empty,
        the next window ends at the oldest boundary it served rather than at the start of the
        window asked for. A venue that cut the page short - whether it says so through
        `hasMore` or not - therefore skips nothing, and a full page changes nothing, since it
        reaches back to the window's own start.
        """
        period_secs = PERIOD_SECS[period]
        span = self._page_size() * period_secs
        bars: dict[int, RawBar] = {}
        to_secs = end_secs
        while True:
            from_secs = to_secs - span
            if start_secs is not None:
                from_secs = max(start_secs, from_secs)
            page, truncated = await self._get_trendbars(
                symbol_id,
                period,
                from_secs=from_secs,
                to_secs=to_secs,
                count=self._page_size() + 1,
            )
            for raw in page:
                # The venue includes a bar opening on `toTimestamp`, and serves bars before
                # `fromTimestamp`.
                if raw.boundary_secs < end_secs and (
                    start_secs is None or raw.boundary_secs >= start_secs
                ):
                    bars.setdefault(raw.boundary_secs, raw)
            oldest = min((raw.boundary_secs for raw in page), default=None)
            if oldest is None:
                reached = from_secs
            else:
                # Never above the window's own last boundary, so every page makes progress.
                reached = min(oldest, to_secs - period_secs)
                if reached > from_secs:
                    self._log.debug(
                        f"History served symbol {symbol_id} only back to {oldest}, not to "
                        f"{from_secs}{' (cut short)' if truncated else ''}; continuing there",
                    )
            if start_secs is None and limit is None:
                break
            if limit is not None and self._closed_count(bars, period_secs, closed_secs) >= limit:
                break
            if start_secs is not None:
                # Only stop once a page has actually reached `start_secs`: one cut short above
                # it leaves the bar at `start_secs` unserved.
                if reached <= start_secs and (oldest is None or oldest <= start_secs):
                    break
            elif not page:
                # Without a lower bound, an empty page is the only sign history has run out.
                break
            to_secs = reached
        return [bars[boundary] for boundary in sorted(bars)]

    @staticmethod
    def _closed_count(bars: dict[int, RawBar], period_secs: int, closed_secs: int) -> int:
        return sum(1 for boundary in bars if boundary + period_secs <= closed_secs)

    def _page_size(self) -> int:
        return max(1, self._config.history_page_size)

    async def _fetch_bar(self, symbol_id: int, period: int, boundary_secs: int) -> RawBar | None:
        """The one closed bar opening at `boundary_secs`, or `None` if history has none yet."""
        period_secs = PERIOD_SECS[period]
        bars, _ = await self._get_trendbars(
            symbol_id,
            period,
            from_secs=boundary_secs,
            to_secs=boundary_secs + period_secs,
            # Two for a one-bar window: the venue counts `count` back from `toTimestamp` and
            # includes a bar opening on it, so asking for one would hand back the next bar.
            count=2,
        )
        return next((raw for raw in bars if raw.boundary_secs == boundary_secs), None)

    async def _fetch_bar_range(self, sub: _BarSub, start_secs: int, end_secs: int) -> list[RawBar]:
        """Every bar history has for the inclusive range `[start_secs, end_secs]`."""
        period_secs = PERIOD_SECS[sub.period]
        # Paged backwards, like every other historical request, because that is the end the
        # venue counts `count` from.
        return await self._page_trendbars(
            sub.symbol_id,
            sub.period,
            start_secs=start_secs,
            end_secs=end_secs + period_secs,
            limit=None,
            closed_secs=end_secs + period_secs,
        )

    async def _get_trendbars(
        self,
        symbol_id: int,
        period: int,
        *,
        from_secs: int,
        to_secs: int,
        count: int,
    ) -> tuple[list[RawBar], bool]:
        """The window's bars, and whether the venue served fewer than the window holds."""
        session = self._session
        if session is None:
            raise CTraderConnectionError("data client is not connected")
        # Confirmed live: the venue serves up to `count` bars counted back from `toTimestamp`
        # by open time, including a bar opening on it, and `fromTimestamp` does not bound the
        # answer - a one-minute window came back with ten bars. The window's start is still
        # sent: the schema makes it optional, but leaving it out is untested, and a venue that
        # did honour it would only serve less per page, which the paging continues from.
        # TODO(verify): whether `fromTimestamp` bounds the answer in any case the live probe
        # did not cover; it had plenty of history behind its window. Nothing relies on it.
        # The venue's caps on `count` and on a window's span are unknown but harmless here: a
        # live run served 5000 bars for one request and truncated a 400-day window without an
        # error, and both look like a page that did not reach its window's start.
        response = await session.request(
            oa.ProtoOAGetTrendbarsReq(
                ctidTraderAccountId=self._account.account_id,
                symbolId=symbol_id,
                period=period,
                fromTimestamp=from_secs * 1_000,
                toTimestamp=to_secs * 1_000,
                count=count,
            ),
            timeout_secs=self._config.history_request_timeout_secs,
        )
        bars = [_raw_bar(trendbar) for trendbar in response.trendbar]
        return bars, response.hasMore or len(bars) >= count

    # -- Subscription bookkeeping -----------------------------------------------------------

    async def _hold_spots(self, symbol_id: int, consumer: str) -> bool:
        """Subscribe `symbol_id`'s spots for `consumer`; returns whether this call added it."""
        subscriptions = self._account.subscriptions
        if consumer in subscriptions.active_consumers(symbol_id, self._owner):
            return False
        # Attached first: the registry makes the consumer active as the call starts, so a spot
        # arriving before the answer is published.
        self._attach_listener(symbol_id)
        try:
            await subscriptions.subscribe_spots(symbol_id, consumer, self._owner)
        finally:
            # Detaches it again if the registry did not count the subscribe.
            self._sync_listener(symbol_id)
        return True

    async def _release_spots(self, symbol_id: int, consumer: str) -> None:
        subscriptions = self._account.subscriptions
        if subscriptions.active_consumers(symbol_id, self._owner) <= {consumer}:
            # The last quote hold goes as the release starts, and nothing updates the book
            # after that, so it is dropped now rather than when the release returns.
            self._books.pop(symbol_id, None)
        try:
            # The registry makes `consumer` inactive as the call starts, so nothing is published
            # for it while the request is in flight; a cancelled release stays held for the next
            # one to repeat.
            await subscriptions.unsubscribe_spots(symbol_id, consumer, self._owner)
        finally:
            self._sync_listener(symbol_id)

    def _quoting(self, symbol_id: int) -> bool:
        return self._account.subscriptions.has_active_consumer(symbol_id, self._owner)

    def _sync_listener(self, symbol_id: int) -> None:
        """Keep exactly one listener on `symbol_id` while a quote hold or a bar needs it."""
        quoting = self._quoting(symbol_id)
        if not quoting:
            # Tied to the quote holds, not to the listener a bar subscription also keeps alive:
            # a side from before a gap in the subscription must never be paired with one after.
            self._books.pop(symbol_id, None)
        if quoting or any(sub.symbol_id == symbol_id for sub in self._bars.values()):
            self._attach_listener(symbol_id)
            return
        listener = self._spot_listeners.pop(symbol_id, None)
        if listener is not None:
            self._account.subscriptions.remove_spot_listener(symbol_id, listener)

    def _attach_listener(self, symbol_id: int) -> None:
        if symbol_id not in self._spot_listeners:
            listener = functools.partial(self._on_spot, symbol_id)
            self._spot_listeners[symbol_id] = listener
            self._account.subscriptions.add_spot_listener(symbol_id, listener)

    def _on_spot(self, symbol_id: int, event: oa.ProtoOASpotEvent) -> None:
        """Route one spot event: its prices to the quote path, its trendbars to the closers."""
        if self._quoting(symbol_id):
            self._on_quote_spot(symbol_id, event)
        self._on_bar_spot(symbol_id, event)
