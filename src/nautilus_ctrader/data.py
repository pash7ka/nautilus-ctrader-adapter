"""`CTraderDataClient`: the Nautilus live market-data client for one cTrader account.

It translates and routes only. Which instruments to load, whether to price them in the
account currency, and what to do with a quote are all the application's decisions, taken
through the config object and the Nautilus subscription commands.
"""

from __future__ import annotations

import asyncio
import functools
import math
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum, auto

from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock, MessageBus
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.data.messages import (
    RequestBars,
    RequestData,
    RequestFundingRates,
    RequestInstrument,
    RequestInstruments,
    RequestOrderBookDeltas,
    RequestOrderBookDepth,
    RequestOrderBookSnapshot,
    RequestQuoteTicks,
    RequestTradeTicks,
    SubscribeBars,
    SubscribeQuoteTicks,
    UnsubscribeBars,
    UnsubscribeQuoteTicks,
)
from nautilus_trader.live.data_client import LiveMarketDataClient
from nautilus_trader.model.data import Bar, BarType, DataType
from nautilus_trader.model.identifiers import ClientId, InstrumentId
from nautilus_trader.model.instruments import Instrument

from nautilus_ctrader.common.account import RESTORE_RETRY_ERROR_ATTEMPTS, CTraderAccountClient
from nautilus_ctrader.common.bars import BarCloser, Clock, RawBar
from nautilus_ctrader.common.errors import (
    CTraderConnectionError,
    CTraderError,
    CTraderNotSentError,
    CTraderProtocolError,
    CTraderRequestError,
    CTraderTimeoutError,
)
from nautilus_ctrader.common.parsing import (
    bar_boundary_secs,
    bar_from_trendbar,
    is_unaligned_boundary,
    quote_from_prices,
)
from nautilus_ctrader.common.rate_limit import RequestPriority
from nautilus_ctrader.common.session import CTraderSession
from nautilus_ctrader.config import CTraderDataClientConfig, parse_asset_class_overrides
from nautilus_ctrader.constants import CTRADER_VENUE
from nautilus_ctrader.enums import PERIOD_SECS, trendbar_period_for
from nautilus_ctrader.failures import REQUEST_FAILED_TOPIC, CTraderRequestFailed
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


def _failure_reason(error: Exception) -> str:
    """The published reason for `error`: a fixed phrase or a venue error code, never its text."""
    if isinstance(error, CTraderRequestError):
        return f"venue refused: {error.error_code}"
    if isinstance(error, CTraderTimeoutError):
        return "timed out"
    if isinstance(error, CTraderConnectionError):
        return "connection lost"
    # Before the protocol check: a request that could not be encoded is both.
    if isinstance(error, CTraderNotSentError):
        return "not sent"
    if isinstance(error, CTraderProtocolError):
        return "invalid venue response"
    if isinstance(error, CTraderError):
        return "request failed"
    return "internal error"


def _bars_data_type(bar_type: BarType) -> DataType:
    return DataType(Bar, metadata={"bar_type": bar_type})


def _answer_data_type(request: RequestData) -> DataType:
    """The `data_type` Nautilus puts on a response to `request`.

    Not `request.data_type`, which carries no metadata for the built-in requests.
    """
    if isinstance(request, RequestBars):
        return _bars_data_type(request.bar_type)
    if isinstance(request, RequestInstruments):
        return DataType(Instrument, metadata={"venue": request.venue})
    if type(request) is RequestData:
        return request.data_type
    return DataType(request.data_type.type, metadata={"instrument_id": request.instrument_id})


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


@dataclass(eq=False)
class _BarSub:
    """One live bar subscription: where its trendbars come from, and what closes them."""

    bar_type: BarType
    symbol_id: int
    period: int
    closer: BarCloser
    # The latest backfill, the session bring-up it was started for, and the failures in a row
    # within that bring-up.
    backfill: asyncio.Task | None = None
    backfill_generation: tuple[CTraderSession, int] | None = None
    backfill_failures: int = 0
    # Set while the live trendbars are not restored: the venue confirming them, or the first one
    # to arrive if that comes sooner, starts a backfill.
    owes_backfill: bool = False

    def cancel_backfill(self) -> None:
        if self.backfill is not None:
            self.backfill.cancel()


class _Conversion(Enum):
    """How a conversion ended, and so whether to subscribe now, retry later, or neither."""

    READY = auto()
    REFUSED = auto()
    # The chain can never be built, and the instrument has been dropped.
    DROPPED = auto()
    # Not wanted any more, or the client connected or disconnected meanwhile.
    ABANDONED = auto()


@dataclass(eq=False)
class _ConversionRetry:
    """The timer retrying one instrument's refused conversion chain."""

    # The subscribes waiting on it by consumer name: what each names, and what completes it.
    # An unsubscribe removes its own; an attempt that succeeds completes the rest.
    waiting: dict[str, tuple[str, Callable[[Instrument], Awaitable[None]]]] = field(
        default_factory=dict,
    )
    task: asyncio.Task | None = None
    # Set while an attempt runs. The retry then stays registered with nobody waiting, so a
    # disconnect still cancels it, rather than an unsubscribe cancelling it half-way through.
    attempting: bool = False
    failures: int = 0

    def running(self) -> bool:
        return self.task is not None and not self.task.done()


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
        # Instruments whose conversion chain was refused during a subscribe. Nautilus keeps those
        # subscriptions on record and passes a repeat over, so only the retry can complete them.
        self._conversion_retries: dict[InstrumentId, _ConversionRetry] = {}
        # Advanced by every `_connect` and `_disconnect`, so a subscribe that awaited across one
        # knows its preparation belongs to a connection that is gone.
        self._connection_generation = 0
        # Requests handed to a task whose handler has not begun yet, by id; see `_submit`.
        self._unanswered: dict[UUID4, RequestData] = {}
        self._task_count_warned = False
        self._bars: dict[BarType, _BarSub] = {}
        # Every backfill task not yet done, whichever subscription it was started for.
        self._backfills: set[asyncio.Task] = set()
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
        await self._cancel_conversion_retries()
        await self._cancel_backfills()
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
        self._account.subscriptions.add_trendbar_recovery_listener(self._on_trendbars_recovered)
        for instrument in self._instrument_provider.list_all():
            self._handle_data(instrument)

    async def _disconnect(self) -> None:
        self._connection_generation += 1
        self._preparing.clear()
        session, self._session = self._session, None
        self._account.remove_reload_listener(self._on_instrument_reloaded)
        self._account.subscriptions.remove_trendbar_recovery_listener(self._on_trendbars_recovered)
        try:
            # First, so the release below also covers what a cancelled attempt took.
            await self._cancel_conversion_retries()
            await self._release_subscriptions(session)
            self._converted.clear()
        finally:
            # Releasing cancels them, unless it failed first. Either way none outlives this.
            try:
                await self._cancel_backfills()
            finally:
                if self._holds_account:
                    self._holds_account = False
                    await self._account.disconnect()

    async def _cancel_backfills(self) -> None:
        """Cancel every backfill and wait until each has ended, those started meanwhile too."""
        while running := {task for task in self._backfills if not task.done()}:
            for task in running:
                task.cancel()
            await asyncio.wait(running)

    async def _cancel_conversion_retries(self) -> None:
        """Cancel every conversion retry, one in the middle of an attempt too, and wait for each."""
        retries, self._conversion_retries = self._conversion_retries, {}
        running = {r.task for r in retries.values() if r.task is not None and r.running()}
        for task in running:
            task.cancel()
        if running:
            await asyncio.wait(running)

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
            sub.cancel_backfill()
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
        self._claim(request)
        instrument = self._instrument_provider.find(request.instrument_id)
        if instrument is None:
            self._log.error(f"Cannot request instrument: {request.instrument_id} is not loaded")
            self._request_failed(
                request,
                _answer_data_type(request),
                f"instrument {request.instrument_id} is not loaded",
            )
            return
        self._handle_instrument(
            instrument,
            request.id,
            request.start,
            request.end,
            request.params,
        )

    async def _request_instruments(self, request: RequestInstruments) -> None:
        self._claim(request)
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

    # -- Failed requests --------------------------------------------------------------------

    def _request_failed(self, request: RequestData, data_type: DataType, reason: str) -> None:
        """Publish `CTraderRequestFailed`, then answer `request` with no data.

        Published first, so a subscriber knows of the failure before the requester's callback
        fires. A raising subscriber is logged rather than allowed to keep the response back.
        """
        failure = CTraderRequestFailed(
            # The engine hands the client a request of its own, carrying the requester's id
            # as its correlation id.
            request_id=request.correlation_id or request.id,
            data_type=data_type,
            reason=reason,
        )
        try:
            self._msgbus.publish(topic=REQUEST_FAILED_TOPIC, msg=failure)
        except Exception as e:
            self._log.exception(f"A {REQUEST_FAILED_TOPIC} subscriber raised", e)
        self._handle_data_response(
            data_type,
            [],
            request.id,
            request.start,
            request.end,
            request.params,
        )

    def _submit(self, submit: Callable[[RequestData], None], request: RequestData) -> None:
        """Hand `request` to the base class's `submit`, which runs its handler in a task.

        A task cancelled before its first step never runs its coroutine at all, so the
        handler's own cancellation path cannot answer it. This happens when a disconnect
        cancels the client's tasks in the same loop turn the request arrived in. The task's
        done-callback answers such a request instead.

        The handler takes `request` out of `_unanswered` on its first line; a done-callback
        answers only a request still there. So the cancellation path never adds a second
        answer: a task's first step is queued when it is created, ahead of the done-callback of
        any task cancelled afterwards. Nautilus 1.231 creates one task per request; with several
        whose handlers all ran, each handler would answer.
        """
        self._unanswered[request.id] = request
        before = set(self._tasks)
        submit(request)
        tasks = set(self._tasks) - before
        if len(tasks) != 1 and request.id in self._unanswered and not self._task_count_warned:
            self._task_count_warned = True
            if tasks:
                self._log.warning(
                    f"Request {request.id} was given {len(tasks)} tasks instead of one; each "
                    "handler that runs answers it",
                )
            else:
                self._log.warning(
                    f"Request {request.id} was given no task; it may go unanswered",
                )
        if not tasks:
            # Never scheduled: nothing will claim it.
            self._unanswered.pop(request.id, None)
        for task in tasks:
            task.add_done_callback(functools.partial(self._on_request_done, request.id))

    def _claim(self, request: RequestData) -> None:
        """Take `request` for its handler, which answers it from here on."""
        self._unanswered.pop(request.id, None)

    def _on_request_done(self, request_id: UUID4, task: asyncio.Task) -> None:
        if not task.cancelled():
            return
        request = self._unanswered.pop(request_id, None)
        if request is not None:
            self._log.warning(f"Request {request_id} was cancelled before it started")
            self._request_failed(request, _answer_data_type(request), "connection closed")

    def request_instrument(self, request: RequestInstrument) -> None:
        self._submit(super().request_instrument, request)

    def request_instruments(self, request: RequestInstruments) -> None:
        self._submit(super().request_instruments, request)

    def request_bars(self, request: RequestBars) -> None:
        self._submit(super().request_bars, request)

    def request_quote_ticks(self, request: RequestQuoteTicks) -> None:
        self._submit(super().request_quote_ticks, request)

    def request_trade_ticks(self, request: RequestTradeTicks) -> None:
        self._submit(super().request_trade_ticks, request)

    def request_funding_rates(self, request: RequestFundingRates) -> None:
        self._submit(super().request_funding_rates, request)

    def request_order_book_deltas(self, request: RequestOrderBookDeltas) -> None:
        self._submit(super().request_order_book_deltas, request)

    def request_order_book_depth(self, request: RequestOrderBookDepth) -> None:
        self._submit(super().request_order_book_depth, request)

    def request_order_book_snapshot(self, request: RequestOrderBookSnapshot) -> None:
        self._submit(super().request_order_book_snapshot, request)

    def request(self, request: RequestData) -> None:
        self._submit(super().request, request)

    # The handlers below are answered here because the base class raises
    # `NotImplementedError`, which leaves the request pending for ever.

    def _unsupported(self, request: RequestData) -> None:
        self._claim(request)
        data_type = _answer_data_type(request)
        name = data_type.type.__name__
        self._log.error(f"Cannot request {name}: not supported by this adapter")
        self._request_failed(request, data_type, f"{name} requests are not supported")

    async def _request_quote_ticks(self, request: RequestQuoteTicks) -> None:
        self._unsupported(request)

    async def _request_trade_ticks(self, request: RequestTradeTicks) -> None:
        self._unsupported(request)

    async def _request_funding_rates(self, request: RequestFundingRates) -> None:
        self._unsupported(request)

    async def _request_order_book_deltas(self, request: RequestOrderBookDeltas) -> None:
        self._unsupported(request)

    async def _request_order_book_depth(self, request: RequestOrderBookDepth) -> None:
        self._unsupported(request)

    async def _request_order_book_snapshot(self, request: RequestOrderBookSnapshot) -> None:
        self._unsupported(request)

    async def _request(self, request: RequestData) -> None:
        self._unsupported(request)

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
        retry: _ConversionRetry | None = None,
    ) -> list[Instrument] | None:
        """Subscribe the chain pricing `instrument`'s quote currency in the deposit currency.

        Returns the chain's instruments for the caller to publish, an empty list if there was
        nothing to do, or `None` if the chain is not usable, for one of two reasons:

        - the chain does not exist, or a leg cannot be built. The instrument can never be
          priced in the account currency, so it is unloaded rather than left tradable by
          accident, and `fail_on_instrument_error` raises instead of returning.
        - the request could not be made or was refused. Nothing is known about the chain yet,
          so the instrument stays loaded and unconverted, and a subscribe retries on a timer -
          unless this is the bring-up and `fail_on_instrument_error` is set, which asks for an
          instrument that cannot be priced to fail the connect rather than start unpriced.

        A refusal is counted against `retry`, if this is its attempt.
        """
        lock = self._conversion_locks.setdefault(instrument.id, asyncio.Lock())
        async with lock:
            return await self._prepare_conversion_locked(
                instrument,
                during_connect=during_connect,
                retry=retry,
            )

    async def _prepare_conversion_locked(
        self,
        instrument: Instrument,
        *,
        during_connect: bool,
        retry: _ConversionRetry | None,
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
        except InstrumentLoadError as e:
            # Only this says something about the chain itself: no route, or a leg that cannot
            # be built.
            # TODO(verify): whether the venue answers an impossible pair with an empty chain or
            # with an error code. One live conversion request for such a pair would settle it.
            self._instrument_provider.remove_failed(instrument.id, f"conversion failed: {e}")
            if self._config.fail_on_instrument_error:
                raise
            return None
        except CTraderError as e:
            # Everything else - a venue refusal, a lost connection, a timeout - says nothing
            # about the instrument, and dropping it would lose it for the whole process.
            self._conversion_not_subscribed(instrument.id, e, during_connect, retry)
            return None
        consumer = _conversion_consumer(instrument.id)
        try:
            await self._hold_chain(chain, consumer)
        except CTraderError as e:
            self._conversion_not_subscribed(instrument.id, e, during_connect, retry)
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
        during_connect: bool,
        retry: _ConversionRetry | None,
    ) -> None:
        """Report a chain left unsubscribed; raise instead if the bring-up asked to fail on it.

        A failed retry is a WARNING, and the third in a row an ERROR, as for a failed restore.
        """
        if during_connect and self._config.fail_on_instrument_error:
            raise error
        detail = (
            error.error_code if isinstance(error, CTraderRequestError) else type(error).__name__
        )
        if retry is None:
            # A subscribe says how it retries itself.
            suffix = "; retrying on the next subscribe" if during_connect else ""
            self._log.warning(f"Conversion for {instrument_id} not subscribed ({detail}){suffix}")
            return
        retry.failures += 1
        waiting = ", ".join(sorted(retry.waiting)) or "nothing"
        if retry.failures == RESTORE_RETRY_ERROR_ATTEMPTS:
            self._log.error(
                f"Conversion for {instrument_id} still not subscribed after "
                f"{RESTORE_RETRY_ERROR_ATTEMPTS} retries ({detail}); waiting: {waiting}",
            )
        else:
            self._log.warning(
                f"Conversion for {instrument_id} not subscribed on a retry ({detail}); "
                f"waiting: {waiting}",
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

        await self._subscribe_converted(
            instrument,
            "quotes",
            _quote_consumer(instrument_id),
            self._hold_quotes,
        )

    async def _hold_quotes(self, instrument: Instrument) -> None:
        await self._hold_spots(instrument.info["symbol_id"], _quote_consumer(instrument.id))

    async def _subscribe_converted(
        self,
        instrument: Instrument,
        what: str,
        consumer: str,
        subscribe: Callable[[Instrument], Awaitable[None]],
    ) -> None:
        """Run `subscribe` once `instrument`'s conversion chain is subscribed.

        A refused chain is retried every `restore_retry_interval_secs`, once for every
        subscription of the instrument, until it succeeds and completes them all, or the last
        one is unsubscribed, or the client disconnects. Nautilus keeps each subscription on
        record meanwhile, so the retry serves every subscriber its engine passed over.
        """
        interval = self._config.restore_retry_interval_secs
        retry = self._conversion_retries.get(instrument.id)
        if retry is not None and retry.running():
            retry.waiting[consumer] = (what, subscribe)
            self._log.warning(
                f"Not subscribing {what} for {instrument.id} yet: its conversion chain is being "
                f"retried every {interval:g} s",
            )
            return
        conversion = await self._convert_before_subscribing(instrument, what, consumer)
        if conversion is _Conversion.READY:
            await subscribe(instrument)
            return
        if conversion is not _Conversion.REFUSED:
            return
        if self._session is None:
            # `_connect` forgets every subscription anyway.
            self._log.warning(
                f"Not subscribing {what} for {instrument.id}: its conversion chain was not "
                f"subscribed, and the client is not connected",
            )
            return
        self._log.warning(
            f"Not subscribing {what} for {instrument.id} yet: its conversion chain was not "
            f"subscribed; retrying every {interval:g} s",
        )
        retry = self._conversion_retries.get(instrument.id)
        if retry is None or not retry.running():
            # None started during the conversion above.
            retry = _ConversionRetry()
            self._conversion_retries[instrument.id] = retry
            retry.task = asyncio.get_running_loop().create_task(
                self._retry_conversion(retry, instrument.id),
            )
            retry.task.add_done_callback(
                functools.partial(self._forget_conversion_retry, instrument.id, retry),
            )
        retry.waiting[consumer] = (what, subscribe)

    async def _retry_conversion(self, retry: _ConversionRetry, instrument_id: InstrumentId) -> None:
        while True:
            await asyncio.sleep(self._config.restore_retry_interval_secs)
            # Not an attempt: an outage must not count towards the ERROR.
            if self._session is None or not self._session.is_ready:
                continue
            instrument = self._instrument_provider.find(instrument_id)
            if instrument is None:
                # Whatever dropped it has reported it.
                self._log.debug(f"Conversion retry for {instrument_id} ended: no longer loaded")
                return
            retry.attempting = True
            try:
                conversion = await self._convert_chain(instrument, retry)
                if conversion is _Conversion.READY:
                    await self._complete_waiting(instrument, retry)
            except Exception as e:
                self._log.exception(f"Conversion retry for {instrument_id} raised", e)
                return
            finally:
                retry.attempting = False
            if conversion is _Conversion.DROPPED and retry.waiting:
                self._log.error(
                    f"Cannot subscribe {', '.join(sorted(retry.waiting))}: {instrument_id} "
                    f"cannot be priced in the account currency and has been dropped",
                )
            if conversion is not _Conversion.REFUSED or not retry.waiting:
                return

    async def _complete_waiting(self, instrument: Instrument, retry: _ConversionRetry) -> None:
        """Complete every subscribe still waiting on `retry`, those joining meanwhile too."""
        while retry.waiting:
            consumer, (what, subscribe) = retry.waiting.popitem()
            try:
                await subscribe(instrument)
            except Exception as e:
                # One failed subscription is no reason to leave the others waiting.
                self._log.exception(f"Subscribing {consumer} after a conversion retry raised", e)
                continue
            self._log.info(
                f"Subscribed {what} for {instrument.id} on a retry: its conversion chain is "
                f"subscribed now",
            )

    def _stop_waiting(self, instrument_id: InstrumentId, consumer: str) -> None:
        """Take `consumer` off its instrument's retry, and stop the retry once nobody waits.

        A retry in the middle of an attempt runs it to the end, and so stays registered for a
        disconnect to cancel.
        """
        retry = self._conversion_retries.get(instrument_id)
        if retry is None or retry.waiting.pop(consumer, None) is None or retry.waiting:
            return
        if not retry.attempting:
            del self._conversion_retries[instrument_id]
            if retry.task is not None:
                retry.task.cancel()

    def _forget_conversion_retry(
        self,
        instrument_id: InstrumentId,
        retry: _ConversionRetry,
        _task: asyncio.Task,
    ) -> None:
        if self._conversion_retries.get(instrument_id) is retry:
            del self._conversion_retries[instrument_id]

    async def _convert_before_subscribing(
        self,
        instrument: Instrument,
        what: str,
        consumer: str,
    ) -> _Conversion:
        """`_convert_chain()` for one subscribe, which `consumer` may unsubscribe meanwhile.

        Returns `ABANDONED` if it was: that unsubscribe found no hold to release. The chain
        stays held, as after a completed subscribe and unsubscribe. A dropped instrument is
        reported here, at ERROR, and returned as `ABANDONED` too.
        """
        marker = object()
        self._preparing.setdefault(consumer, set()).add(marker)
        try:
            conversion = await self._convert_chain(instrument)
        finally:
            markers = self._preparing.get(consumer, set())
            wanted = marker in markers
            markers.discard(marker)
            if not markers:
                self._preparing.pop(consumer, None)
        if conversion is _Conversion.DROPPED:
            self._log.error(
                f"Cannot subscribe {what}: {instrument.id} cannot be priced in the account "
                f"currency and has been dropped",
            )
            return _Conversion.ABANDONED
        return conversion if wanted else _Conversion.ABANDONED

    async def _convert_chain(
        self,
        instrument: Instrument,
        retry: _ConversionRetry | None = None,
    ) -> _Conversion:
        """Resolve and publish `instrument`'s conversion chain before it is subscribed.

        Run before the venue subscription, so nothing is ever published for an instrument that
        cannot be priced in the account currency. Returns `ABANDONED`, releasing the chain, if
        the client connected or disconnected meanwhile: the release that came with it did not
        see what was still being held.
        """
        if not self._config.subscribe_conversion_quotes:
            return _Conversion.READY
        connection = self._connection_generation
        chain = await self._prepare_conversion(instrument, retry=retry)
        if connection != self._connection_generation:
            # Before the release's first await, so a preparation queued behind this one for the
            # same instrument does not take the chain as converted.
            self._converted.discard(instrument.id)
            await self._retire_old_legs(_conversion_consumer(instrument.id), set())
            return _Conversion.ABANDONED
        if chain is None:
            if self._instrument_provider.find(instrument.id) is None:
                return _Conversion.DROPPED
            return _Conversion.REFUSED
        for leg in chain:
            self._handle_data(leg)
        return _Conversion.READY

    async def _unsubscribe_quote_ticks(self, command: UnsubscribeQuoteTicks) -> None:
        # Found by consumer name rather than through the instrument's symbol id, because the
        # instrument may have been dropped since and the hold must be released even then.
        consumer = _quote_consumer(command.instrument_id)
        self._stop_waiting(command.instrument_id, consumer)
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
            self._log.error(f"Cannot subscribe bars {bar_type}: {e}")
            return
        instrument = self._instrument_provider.find(bar_type.instrument_id)
        if instrument is None:
            self._log.error(f"Cannot subscribe bars: {bar_type.instrument_id} is not loaded")
            return
        # A bars-only subscriber needs the account valued just as much as a quote subscriber
        # does. Before the bars are recorded, so a refused chain leaves nothing behind.
        await self._subscribe_converted(
            instrument,
            "bars",
            _bar_consumer(bar_type),
            functools.partial(self._subscribe_trendbars, bar_type, period),
        )

    async def _subscribe_trendbars(
        self,
        bar_type: BarType,
        period: int,
        instrument: Instrument,
    ) -> None:
        if bar_type in self._bars:
            # Another subscribe completed it during the conversion.
            return
        consumer = _bar_consumer(bar_type)
        symbol_id = instrument.info["symbol_id"]
        # Created before the venue request: the closer ignores every boundary below the bar
        # forming at its construction, so building it afterwards would drop a bar that closed
        # while the request was in flight.
        sub = _BarSub(bar_type, symbol_id, period, self._make_closer(bar_type, symbol_id, period))
        self._bars[bar_type] = sub
        self._bar_routes[(symbol_id, period)] = sub
        self._sync_listener(symbol_id)
        subscriptions = self._account.subscriptions
        try:
            await subscriptions.subscribe_trendbars(symbol_id, period, consumer, self._owner)
        finally:
            # What is kept here must match what the registry counts, whatever the outcome:
            # - a counted trendbar leg, unknown outcomes included, keeps the bars and needs the
            #   backfill restore; an unknown outcome also owes a backfill once they are confirmed;
            # - without one there are no live bars, so the bar type is freed for another try;
            #   a spot leg left counted is released with this client's other holds;
            # - an unsubscribe that ran meanwhile has already forgotten `sub`.
            if self._bars.get(bar_type) is sub:
                if consumer in subscriptions.trendbar_consumers(symbol_id, period, self._owner):
                    sub.owes_backfill = subscriptions.trendbar_restore_failed(symbol_id, period)
                    if self._session is not None:
                        # Added after the registry's own restore for this key, and so run after
                        # it: the backfill's history needs the reconnected subscription in place.
                        self._session.add_restore(
                            _bar_restore_key(bar_type),
                            functools.partial(self._restore_bars, bar_type, sub),
                        )
                else:
                    self._forget_bars(bar_type, sub)

    async def _unsubscribe_bars(self, command: UnsubscribeBars) -> None:
        consumer = _bar_consumer(command.bar_type)
        self._stop_waiting(command.bar_type.instrument_id, consumer)
        self._preparing.pop(consumer, None)
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
        sub.cancel_backfill()
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

    async def _restore_bars(self, bar_type: BarType, sub: _BarSub) -> None:
        """Start backfilling what `bar_type` missed, and return without waiting for it.

        The session becomes ready only once every restore has returned, and every command waits
        for that. A backfill still running from an earlier restore is replaced: the closer would
        ignore a second one, leaving the new gap unfilled.

        If the trendbar restore failed, this backfill ends with no stream to take over from it,
        so the retry that restores them starts another: see `_on_trendbars_recovered()`.
        """
        # In a bring-up the trendbar restore has run by now: restores run in the order their keys
        # were added.
        sub.owes_backfill = self._account.subscriptions.trendbar_restore_failed(
            sub.symbol_id,
            sub.period,
        )
        sub.closer.on_disconnect()
        self._start_backfill(sub)

    def _start_backfill(self, sub: _BarSub) -> None:
        generation = self._backfill_generation()
        if generation != sub.backfill_generation:
            # The first backfill of this bring-up: a failure is the first since the reconnect.
            sub.backfill_generation = generation
            sub.backfill_failures = 0
        previous = sub.backfill
        sub.cancel_backfill()
        # Eager, so the closer holds stream updates before this returns.
        task = asyncio.Task(
            self._backfill(sub.bar_type, sub, previous, generation),
            loop=asyncio.get_running_loop(),
            eager_start=True,
        )
        sub.backfill = task
        self._backfills.add(task)
        task.add_done_callback(self._backfills.discard)

    def _backfill_generation(self) -> tuple[CTraderSession, int] | None:
        """The session and its `bring_up_generation`, which a backfill is started for."""
        session = self._session
        return None if session is None else (session, session.bring_up_generation)

    async def _backfill(
        self,
        bar_type: BarType,
        sub: _BarSub,
        previous: asyncio.Task | None,
        generation: tuple[CTraderSession, int] | None,
    ) -> None:
        """Run one backfill; a failure goes to the session's failed restores, to be retried.

        Once the session has been brought up again, the outcome is stale and ignored: that
        bring-up runs its own backfill.
        """
        if previous is not None and not previous.done():
            # Until it has unwound, the closer still counts it as running.
            await asyncio.wait({previous})
        try:
            await sub.closer.backfill(functools.partial(self._fetch_bar_range, sub))
        except CTraderConnectionError as e:
            # The next reconnect starts another.
            self._log.debug(f"Backfill of {bar_type} ended with the connection: {e}")
            return
        except CTraderError as e:
            if self._backfill_generation() != generation:
                self._log.debug(f"Backfill of {bar_type} from an earlier bring-up failed: {e!r}")
                return
            sub.backfill_failures += 1
            detail = e.error_code if isinstance(e, CTraderRequestError) else repr(e)
            message = f"Backfill of {bar_type} failed: {detail}"
            # The levels of a failed restore: ERROR for the first failure after a reconnect,
            # WARNING for a failed retry, ERROR again once the retries keep failing.
            if sub.backfill_failures == 1:
                self._log.error(message)
            elif sub.backfill_failures == 1 + RESTORE_RETRY_ERROR_ATTEMPTS:
                self._log.error(
                    f"{message}; still failing after {RESTORE_RETRY_ERROR_ATTEMPTS} retries",
                )
            else:
                self._log.warning(message)
            self._mark_backfill_failed(bar_type)
            return
        except Exception as e:
            self._log.exception(f"Backfill of {bar_type} raised", e)
            if self._backfill_generation() == generation:
                self._mark_backfill_failed(bar_type)
            return
        if self._backfill_generation() == generation:
            sub.backfill_failures = 0

    def _mark_backfill_failed(self, bar_type: BarType) -> None:
        if self._session is not None:
            self._session.mark_restore_failed(_bar_restore_key(bar_type))

    def _on_bar_spot(self, symbol_id: int, event: oa.ProtoOASpotEvent) -> None:
        for trendbar in event.trendbar:
            # Routed by the bar's own period: a spot event carries one forming bar per
            # subscribed period. A historical bar leaves the field unset, where it would read
            # as M1, so an unset one is never routed.
            if not trendbar.HasField("period"):
                continue
            sub = self._bar_routes.get((symbol_id, trendbar.period))
            if sub is None:
                continue
            # A subscribe whose answer was lost may still have reached the venue, so its stream
            # can come before any retry confirms it. The backfill must then run before that
            # stream emits a bar: it never reaches back behind the last one emitted.
            self._pay_backfill(sub)
            sub.closer.on_update(_raw_bar(trendbar))

    def _on_trendbars_recovered(self, symbol_id: int, period: int) -> None:
        sub = self._bar_routes.get((symbol_id, period))
        if sub is not None:
            self._pay_backfill(sub)

    def _pay_backfill(self, sub: _BarSub) -> None:
        """Start the backfill `sub` owes, once, whichever of its two triggers comes first."""
        if not sub.owes_backfill:
            return
        sub.owes_backfill = False
        # A running backfill covers the gap already: its tail reaches past this moment.
        if sub.backfill is None or sub.backfill.done():
            self._start_backfill(sub)

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
        """Answer `request` exactly once: with its bars, or failed and with none.

        A failure part way through delivers none of the bars already served, so a requester
        is never handed a shortened history that looks complete.
        """
        self._claim(request)
        bar_type = request.bar_type
        data_type = _bars_data_type(bar_type)
        try:
            period = trendbar_period_for(bar_type)
        except ValueError as e:
            self._log.error(f"Cannot request bars {bar_type}: {e}")
            # The text is this adapter's own: short, and with nothing secret in it.
            self._request_failed(request, data_type, str(e))
            return
        if self._session is None:
            # Before the instrument check: the instruments may not be loaded yet either.
            self._log.warning(f"Cannot request bars {bar_type}: not connected")
            self._request_failed(request, data_type, "not connected")
            return
        instrument = self._instrument_provider.find(bar_type.instrument_id)
        if instrument is None:
            self._log.error(f"Cannot request bars: {bar_type.instrument_id} is not loaded")
            self._request_failed(
                request,
                data_type,
                f"instrument {bar_type.instrument_id} is not loaded",
            )
            return

        connection = self._connection_generation
        try:
            bars = await self._historical_bars(request, instrument, period)
        except asyncio.CancelledError:
            self._log.warning(f"Cannot request bars for {bar_type}: cancelled")
            self._request_failed(request, data_type, "connection closed")
            raise
        except Exception as e:
            reason = _failure_reason(e)
            if isinstance(e, CTraderConnectionError) and connection != self._connection_generation:
                # Closed by this client's own disconnect, not lost.
                reason = "connection closed"
            if isinstance(e, (CTraderTimeoutError, CTraderConnectionError)):
                self._log.warning(f"Cannot request bars for {bar_type}: {e}")
            elif isinstance(e, CTraderError):
                self._log.error(f"Cannot request bars for {bar_type}: {e}")
            self._request_failed(request, data_type, reason)
            if not isinstance(e, CTraderError):
                # A bug: Nautilus logs it with its traceback.
                raise
            return
        # Outside the `try`: a response must never be followed by a failure for the same request.
        self._handle_bars(bar_type, bars, request.id, request.start, request.end, request.params)

    async def _historical_bars(
        self,
        request: RequestBars,
        instrument: Instrument,
        period: int,
    ) -> list[Bar]:
        """`request`'s closed bars, ascending; nothing is recorded unless all are returned."""
        bar_type = request.bar_type
        period_secs = PERIOD_SECS[period]
        now_secs = int(self._bar_clock.now())
        served = await self._page_trendbars(
            instrument.info["symbol_id"],
            period,
            # Rounded up, so a fractional start never lets in the bar opening just before it.
            start_secs=None if request.start is None else math.ceil(request.start.timestamp()),
            end_secs=now_secs if request.end is None else int(request.end.timestamp()),
            limit=request.limit or None,
            closed_secs=now_secs,
            priority=RequestPriority.DATA,
        )
        # History serves the forming bar too, which is not a bar yet.
        closed = [raw for raw in served if raw.boundary_secs + period_secs <= now_secs]
        if request.limit:
            closed = closed[-request.limit :]
        elif request.start is None:
            # One page of the newest bars, although a page asks the venue for two more.
            closed = closed[-self._page_size() :]

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
        # Nautilus never hands a response to live bar subscribers, so a live closer is left alone.
        return bars

    async def _page_trendbars(
        self,
        symbol_id: int,
        period: int,
        *,
        start_secs: int | None,
        end_secs: int,
        limit: int | None,
        closed_secs: int,
        priority: RequestPriority,
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
                # Two more than the page: one for the bar the venue repeats at `toTimestamp` or
                # the forming bar, and one for an answer a bar short of `count`, which the venue
                # gives at times.
                count=self._page_size() + 2,
                priority=priority,
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
            priority=RequestPriority.DATA,
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
            priority=RequestPriority.BACKFILL,
        )

    async def _get_trendbars(
        self,
        symbol_id: int,
        period: int,
        *,
        from_secs: int,
        to_secs: int,
        count: int,
        priority: RequestPriority,
    ) -> tuple[list[RawBar], bool]:
        """The window's bars, and whether the venue served fewer than the window holds."""
        session = self._session
        if session is None:
            raise CTraderConnectionError("data client is not connected")
        # Confirmed live: the venue serves up to `count` bars counted back from `toTimestamp`
        # by open time, including a bar opening on it, and `fromTimestamp` did not bound the
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
            priority=priority,
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
        self._publish_last_spot(symbol_id)
        return True

    def _publish_last_spot(self, symbol_id: int) -> None:
        """Publish the account's last spot of a symbol this client has no quote of yet.

        Joining a live subscription sends the venue nothing, so without it the first quote
        would wait for the price to move. The quote carries the spot's own broker time.
        """
        if not self._quoting(symbol_id):
            return
        book = self._book(symbol_id)
        spot = self._account.subscriptions.last_spot(symbol_id)
        if (book.bid is not None and book.ask is not None) or spot is None:
            return
        if spot.bid is None or spot.ask is None:
            return
        event = oa.ProtoOASpotEvent(
            ctidTraderAccountId=self._account.account_id,
            symbolId=symbol_id,
            bid=spot.bid,
            ask=spot.ask,
        )
        if spot.timestamp is not None:
            event.timestamp = spot.timestamp
        self._on_quote_spot(symbol_id, event)

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
