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
    CTraderTimeoutError,
)
from nautilus_ctrader.common.parsing import (
    bar_boundary_secs,
    bar_from_trendbar,
    quote_from_prices,
)
from nautilus_ctrader.common.session import CTraderSession
from nautilus_ctrader.config import CTraderDataClientConfig, parse_asset_class_overrides
from nautilus_ctrader.constants import CTRADER_VENUE
from nautilus_ctrader.enums import PERIOD_SECS, trendbar_period_for
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as om
from nautilus_ctrader.providers import CTraderInstrumentProvider, InstrumentLoadError

CONVERSION_CONSUMER = "conversion"
# Restore key for the reset that a reconnect needs; also the reason it must run first.
_BOOK_RESET_RESTORE = "quote_books"

SpotListener = Callable[[oa.ProtoOASpotEvent], None]


def _quote_consumer(instrument_id: InstrumentId) -> str:
    return f"quotes:{instrument_id}"


def _bar_consumer(bar_type: BarType) -> str:
    return f"bars:{bar_type}"


def _bar_restore_key(bar_type: BarType) -> tuple[str, str]:
    return ("bar_backfill", str(bar_type))


def _conversion_message(quote: str, deposit: str, symbols: list[str]) -> str:
    """The INFO line reporting a subscribed currency-conversion chain."""
    return f"Conversion {quote}->{deposit}: subscribed {', '.join(symbols)}"


def _raw_bar(trendbar: om.ProtoOATrendbar, period_secs: int) -> RawBar:
    """A trendbar in the closer's own units. The period is the caller's, never the message's."""
    return RawBar(
        boundary_secs=bar_boundary_secs(trendbar.utcTimestampInMinutes, period_secs),
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
        # The quote consumers holding each symbol's spot subscription. A hold is recorded
        # before its request goes out, because the registry counts a subscribe whose outcome is
        # unknown, and dropped once its unsubscribe starts or the venue refuses the subscribe.
        self._spot_holds: dict[int, set[str]] = {}
        # Holds whose unsubscribe is in flight. They no longer publish, but a cancelled release
        # must still be repeated, so they stay here until the request returns.
        self._releasing: set[tuple[int, str]] = set()
        # One listener per symbol however many holds it has: a symbol that is both subscribed
        # and a conversion leg would otherwise emit every quote once per listener.
        self._spot_listeners: dict[int, SpotListener] = {}
        self._converted: set[InstrumentId] = set()
        self._conversion_legs: dict[InstrumentId, frozenset[int]] = {}
        self._bars: dict[BarType, _BarSub] = {}
        # The same subscriptions by the `(symbol id, period)` a live trendbar identifies itself
        # with, which is how a spot event's trendbars are routed.
        self._bar_routes: dict[tuple[int, int], _BarSub] = {}
        # Bar subscriptions whose unsubscribe is in flight, kept for the same reason as
        # `_releasing`: a cancelled one must still be repeated.
        self._releasing_bars: dict[BarType, _BarSub] = {}
        self._bar_clock: Clock = _LoopClock(loop)
        # Keys already reported as undeliverable; a repeat is logged at DEBUG, since a broken
        # scale would otherwise flood the log on every tick.
        self._quote_errors: set[InstrumentId] = set()
        self._bar_errors: set[BarType] = set()
        self._unknown_symbols: set[int] = set()

    @property
    def instrument_provider(self) -> CTraderInstrumentProvider:
        return self._instrument_provider

    async def _connect(self) -> None:
        # Nothing from an earlier connection may survive here: a `_disconnect` cut short would
        # otherwise make a later subscribe a silent no-op against a session that is gone.
        self._forget_subscriptions()
        # A chain is venue data that can change, so it is re-queried once per connection.
        self._instrument_provider.reset_conversion_cache()
        self._converted.clear()
        self._conversion_legs.clear()
        # A problem that survives a reconnect is worth reporting at full volume again.
        self._quote_errors.clear()
        self._bar_errors.clear()
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
        session, self._session = self._session, None
        try:
            if session is not None:
                session.remove_event_handler(
                    oa.ProtoOASymbolChangedEvent,
                    self._on_symbol_changed,
                )
                session.remove_restore(_BOOK_RESET_RESTORE)
            for bar_type in sorted(self._all_bars(), key=str):
                await self._drop_bars(bar_type, session)
            # A snapshot: releasing a hold drops it from the mapping.
            for symbol_id, consumer in sorted(self._all_holds()):
                await self._release_spots(symbol_id, consumer)
            self._conversion_legs.clear()
            self._converted.clear()
        finally:
            await self._account.disconnect()

    def _forget_subscriptions(self) -> None:
        """Drop every record of what was subscribed, without touching the venue."""
        for sub in self._bars.values():
            sub.closer.close()
        for sub in self._releasing_bars.values():
            sub.closer.close()
        self._bars.clear()
        self._bar_routes.clear()
        self._releasing_bars.clear()
        self._spot_holds.clear()
        self._releasing.clear()
        for symbol_id, listener in self._spot_listeners.items():
            self._account.subscriptions.remove_spot_listener(symbol_id, listener)
        self._spot_listeners.clear()
        self._books.clear()

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
          priced in the account currency, so it is unloaded rather than left tradable by
          accident, and `fail_on_instrument_error` raises instead of returning.
        - the request could not be made or was refused. Nothing is known about the chain yet,
          so the instrument stays loaded and unconverted and the next subscribe tries again -
          unless this is the bring-up and `fail_on_instrument_error` is set, which asks for an
          instrument that cannot be priced to fail the connect rather than start unpriced.
        """
        if instrument.id in self._converted:
            return []
        # Read before the first await: a symbol change during it resets the chain cache, and
        # what this call resolved must then not be recorded as current.
        generation = self._instrument_provider.conversion_generation
        try:
            chain = await self._instrument_provider.conversion_instruments_for(instrument)
        except (InstrumentLoadError, CTraderRequestError, CTraderProtocolError) as e:
            # Only these say something about the chain itself: no route, a leg that cannot be
            # built, or an answer that could not be read.
            self._instrument_provider.remove_failed(instrument.id, f"conversion failed: {e}")
            if self._config.fail_on_instrument_error:
                raise
            return None
        except (CTraderConnectionError, CTraderTimeoutError) as e:
            # A stalled or unsent request says nothing about the instrument, so dropping it
            # would lose it for the whole process over one reconnect.
            self._conversion_not_subscribed(instrument.id, e, during_connect=during_connect)
            return None
        try:
            await self._hold_chain(chain)
        except CTraderError as e:
            self._conversion_not_subscribed(instrument.id, e, during_connect=during_connect)
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

    # -- Bars -------------------------------------------------------------------------------

    async def _subscribe_bars(self, command: SubscribeBars) -> None:
        bar_type = command.bar_type
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

        symbol_id = instrument.info["symbol_id"]
        # Created before the venue request: the closer ignores every boundary below the bar
        # forming at its construction, so building it afterwards would drop a bar that closed
        # while the request was in flight.
        sub = _BarSub(symbol_id, period, self._make_closer(bar_type, symbol_id, period))
        consumer = _bar_consumer(bar_type)
        self._bars[bar_type] = sub
        self._bar_routes[(symbol_id, period)] = sub
        self._sync_listener(symbol_id)
        try:
            await self._account.subscriptions.subscribe_trendbars(symbol_id, period, consumer)
        finally:
            # Whatever the outcome - success, refusal, cancellation, a lost connection - what
            # is kept here must match what the registry counts. It counts a subscribe whose
            # outcome is unknown, and those still need the restore; a refused or unrecorded one
            # leaves nothing behind, and the bar type must stay free for another try.
            if consumer in self._account.subscriptions.trendbar_consumers(symbol_id, period):
                if self._session is not None:
                    # Added after the registry's own restore for this key, and so run after it:
                    # the backfill's history needs the reconnected subscription in place.
                    self._session.add_restore(
                        _bar_restore_key(bar_type),
                        functools.partial(self._restore_bars, sub),
                    )
            else:
                self._forget_bars(bar_type, sub)

    async def _unsubscribe_bars(self, command: UnsubscribeBars) -> None:
        await self._drop_bars(command.bar_type, self._session)

    async def _drop_bars(self, bar_type: BarType, session: CTraderSession | None) -> None:
        sub = self._bars.get(bar_type) or self._releasing_bars.get(bar_type)
        if sub is None:
            return
        if bar_type in self._bars:
            self._forget_bars(bar_type, sub)
        # Recorded until the request returns, so a cancelled unsubscribe is repeated at
        # disconnect rather than leaving the venue sending trendbars nobody listens to.
        self._releasing_bars[bar_type] = sub
        if session is not None:
            session.remove_restore(_bar_restore_key(bar_type))
        await self._account.subscriptions.unsubscribe_trendbars(
            sub.symbol_id,
            sub.period,
            _bar_consumer(bar_type),
        )
        self._releasing_bars.pop(bar_type, None)

    def _forget_bars(self, bar_type: BarType, sub: _BarSub) -> None:
        """Stop routing and closing `bar_type`, before its venue subscription is given up."""
        del self._bars[bar_type]
        self._bar_routes.pop((sub.symbol_id, sub.period), None)
        sub.closer.close()
        self._sync_listener(sub.symbol_id)

    def _all_bars(self) -> set[BarType]:
        """Every bar type the registry may still count a trendbar reference for."""
        return set(self._bars) | set(self._releasing_bars)

    def _make_closer(self, bar_type: BarType, symbol_id: int, period: int) -> BarCloser:
        return BarCloser(
            period_secs=PERIOD_SECS[period],
            grace_secs=self._config.bar_close_grace_secs,
            history_retries=self._config.bar_close_history_retries,
            clock=self._bar_clock,
            fetch=functools.partial(self._fetch_bar, symbol_id, period),
            emit=functools.partial(self._emit_bar, bar_type, symbol_id),
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
                sub.closer.on_update(_raw_bar(trendbar, PERIOD_SECS[sub.period]))

    def _emit_bar(self, bar_type: BarType, symbol_id: int, raw: RawBar) -> None:
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
        """Bars opening before `end_secs`, ascending, paging backwards from it.

        `limit` counts only bars closed by `closed_secs`, so the forming bar history also
        serves never takes a closed bar's place. With neither a `start_secs` nor a `limit`
        exactly one page is asked for: the caller wanted the most recent bars, not the whole
        history.

        Only an empty page proves a window holds nothing: whenever one comes back non-empty,
        the next window ends at the oldest boundary it served rather than at the start of the
        window asked for. A venue that cut the page short - whether it says so through
        `hasMore` or not - therefore skips nothing, and a full page changes nothing, since its
        oldest boundary is the window's own start.
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
                # An inclusive `toTimestamp` would otherwise carry a bar past the end in.
                if raw.boundary_secs < end_secs:
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
            # Two for a one-bar window: the venue counts `count` back from `toTimestamp`, so
            # asking for one could hand back a bar at the far edge instead of the one wanted.
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
        # TODO(verify): whether fromTimestamp and toTimestamp are inclusive, and what the
        # venue's caps on `count` and on a request's time span are. The paging absorbs all of
        # them: an inclusive `toTimestamp` only repeats a boundary the caller dedupes, and a
        # cap shows up as a page that did not reach its window's start. An *exclusive*
        # `fromTimestamp` would not be absorbed - the bar at the window's start would be lost
        # on every page - which is why this is the assumption to check first.
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
        # The period is the response's, not each bar's: a historical trendbar leaves its own
        # `period` unset.
        period_secs = PERIOD_SECS[period]
        bars = [_raw_bar(trendbar, period_secs) for trendbar in response.trendbar]
        return bars, response.hasMore or len(bars) >= count

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
        """Keep exactly one listener on `symbol_id` while a quote hold or a bar needs it."""
        quoting = symbol_id in self._spot_holds
        if not quoting:
            # Tied to the quote holds, not to the listener a bar subscription also keeps alive:
            # a side from before a gap in the subscription must never be paired with one after.
            self._books.pop(symbol_id, None)
        wanted = quoting or any(sub.symbol_id == symbol_id for sub in self._bars.values())
        listener = self._spot_listeners.get(symbol_id)
        if wanted and listener is None:
            listener = functools.partial(self._on_spot, symbol_id)
            self._spot_listeners[symbol_id] = listener
            self._account.subscriptions.add_spot_listener(symbol_id, listener)
        elif not wanted and listener is not None:
            del self._spot_listeners[symbol_id]
            self._account.subscriptions.remove_spot_listener(symbol_id, listener)

    def _on_spot(self, symbol_id: int, event: oa.ProtoOASpotEvent) -> None:
        """Route one spot event: its prices to the quote path, its trendbars to the closers."""
        if symbol_id in self._spot_holds:
            self._on_quote_spot(symbol_id, event)
        self._on_bar_spot(symbol_id, event)
