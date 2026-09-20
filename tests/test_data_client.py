"""Tests for `CTraderDataClient`: connect, instruments, conversion quotes, quote ticks and bars.

The bar-close state machine itself is covered by `test_bars.py`; what is covered here is the
client half of it - the venue subscriptions, the routing of live trendbars, the historical
requests and the reconnect backfill.

Everything runs against the fake server replaying `tests/fixtures/m2_recorded.json`. The
client's own `Logger` writes from Rust and is invisible to pytest, so log *text* is asserted
against the formatter that produces it and log *effects* against what the venue received.
"""

from __future__ import annotations

import asyncio
import functools
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime

import pytest
from nautilus_trader.cache.cache import Cache
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.data.engine import DataEngine
from nautilus_trader.data.messages import (
    RequestBars,
    RequestInstrument,
    RequestInstruments,
    SubscribeBars,
    SubscribeQuoteTicks,
    UnsubscribeBars,
    UnsubscribeQuoteTicks,
)
from nautilus_trader.model.currencies import EUR, USD
from nautilus_trader.model.data import Bar, BarType, QuoteTick
from nautilus_trader.model.identifiers import InstrumentId, Symbol
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.test_kit.stubs.component import TestComponentStubs

from nautilus_ctrader.common.account import CTraderAccountClient
from nautilus_ctrader.common.errors import CTraderProtocolError, CTraderRequestError
from nautilus_ctrader.config import CTraderDataClientConfig, parse_asset_class_overrides
from nautilus_ctrader.constants import CTRADER_VENUE
from nautilus_ctrader.data import CONVERSION_CONSUMER, CTraderDataClient, _conversion_message
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as om
from nautilus_ctrader.providers import CTraderInstrumentProvider, InstrumentLoadError
from tests.account_venue import (
    ACCOUNT_ID,
    RECORDED,
    HeldReplies,
    account_client,
    received,
    venue,
)
from tests.fake_server import FakeCTraderServer
from tests.polling import wait_until
from tests.recording_logger import RecordingLogger

EURUSD_ID = InstrumentId(Symbol("EURUSD"), CTRADER_VENUE)
GER40_ID = InstrumentId(Symbol("GER40.cash"), CTRADER_VENUE)
EURUSD_SYMBOL_ID = 1
GER40_SYMBOL_ID = 279

EURUSD_M1 = BarType.from_str(f"{EURUSD_ID}-1-MINUTE-BID-EXTERNAL")
EURUSD_H1 = BarType.from_str(f"{EURUSD_ID}-1-HOUR-BID-EXTERNAL")
EURUSD_LAST = BarType.from_str(f"{EURUSD_ID}-1-MINUTE-LAST-EXTERNAL")
GER40_M1 = BarType.from_str(f"{GER40_ID}-1-MINUTE-BID-EXTERNAL")

M1 = om.ProtoOATrendbarPeriod.Value("M1")
H1 = om.ProtoOATrendbarPeriod.Value("H1")

SPOTS = RECORDED["spot_events"]
# Recorded EURUSD spots carrying one side only, the second consistent with the first.
BID_ONLY = SPOTS[17]
ASK_ONLY = SPOTS[19]
TWO_SIDED = SPOTS[0]
GER40_SPOT = SPOTS[1]

# The first minute the recorded spot events carry a trendbar for, and the last bar of the
# recorded H1 response. Both in `utcTimestampInMinutes`, as the venue reports them.
FIRST_M1_MINUTE = 29_829_284
LAST_H1_MINUTE = 29_829_180


def config(**overrides) -> CTraderDataClientConfig:
    values = {
        "client_id": "client-id",
        "client_secret": "client-secret",
        "access_token": "access-token",
        "refresh_token": "refresh-token",
        "token_expires_at": 4_102_444_800.0,
        "account_id": ACCOUNT_ID,
        "instrument_provider": InstrumentProviderConfig(load_ids=frozenset({GER40_ID})),
    }
    values.update(overrides)
    return CTraderDataClientConfig(**values)


@dataclass
class Harness:
    server: FakeCTraderServer
    account: CTraderAccountClient
    provider: CTraderInstrumentProvider
    client: CTraderDataClient
    cache: Cache
    published: list
    responses: list

    def instruments(self) -> list[Instrument]:
        return [d for d in self.published if isinstance(d, Instrument)]

    def quotes(self) -> list[QuoteTick]:
        return [d for d in self.published if isinstance(d, QuoteTick)]

    def bars(self) -> list[Bar]:
        return [d for d in self.published if isinstance(d, Bar)]

    def subscribed_symbol_ids(self) -> list[int]:
        return [i for r in received(self.server, oa.ProtoOASubscribeSpotsReq) for i in r.symbolId]

    def unsubscribed_symbol_ids(self) -> list[int]:
        return [i for r in received(self.server, oa.ProtoOAUnsubscribeSpotsReq) for i in r.symbolId]


@asynccontextmanager
async def harness(
    *,
    client_config: CTraderDataClientConfig | None = None,
    server: FakeCTraderServer | None = None,
) -> AsyncIterator[Harness]:
    client_config = client_config or config()
    server = server or data_venue()
    await server.start()
    logger = RecordingLogger()
    account = account_client(server, logger=logger, credentials=client_config.credentials())
    provider = CTraderInstrumentProvider(
        account,
        client_config.instrument_provider,
        parse_asset_class_overrides(client_config.asset_class_overrides),
        client_config.fail_on_instrument_error,
        logger,
    )
    published: list = []
    responses: list = []
    msgbus = TestComponentStubs.msgbus()
    cache = TestComponentStubs.cache()
    clock = TestComponentStubs.clock()
    # The real engine, so published data lands in the cache exactly as it would in
    # production, wrapped so the list also keeps what the client sent. Responses are only
    # captured: routing one needs a request the engine never issued here.
    engine = DataEngine(msgbus, cache, clock)

    def process(data) -> None:
        published.append(data)
        engine.process(data)

    msgbus.deregister(endpoint="DataEngine.process", handler=engine.process)
    msgbus.deregister(endpoint="DataEngine.response", handler=engine.response)
    msgbus.register(endpoint="DataEngine.process", handler=process)
    msgbus.register(endpoint="DataEngine.response", handler=responses.append)
    client = CTraderDataClient(
        loop=asyncio.get_running_loop(),
        account=account,
        msgbus=msgbus,
        cache=cache,
        clock=clock,
        instrument_provider=provider,
        config=client_config,
    )
    try:
        yield Harness(server, account, provider, client, cache, published, responses)
    finally:
        await account.disconnect()
        await server.stop()


async def subscribe_quotes(h: Harness, instrument_id: InstrumentId) -> None:
    await h.client._subscribe_quote_ticks(
        SubscribeQuoteTicks(
            instrument_id=instrument_id,
            client_id=None,
            venue=CTRADER_VENUE,
            command_id=UUID4(),
            ts_init=0,
        ),
    )


async def unsubscribe_quotes(h: Harness, instrument_id: InstrumentId) -> None:
    await h.client._unsubscribe_quote_ticks(
        UnsubscribeQuoteTicks(
            instrument_id=instrument_id,
            client_id=None,
            venue=CTRADER_VENUE,
            command_id=UUID4(),
            ts_init=0,
        ),
    )


async def push_spot(h: Harness, event: oa.ProtoOASpotEvent) -> None:
    """Push a spot event and return once the client has dispatched it.

    The round-trip afterwards shares the connection, so it cannot overtake the event, and the
    client's reader dispatches inbound frames in order.
    """
    await h.server.push(event)
    await h.account.request(oa.ProtoOATraderReq(ctidTraderAccountId=ACCOUNT_ID))


def data_venue() -> FakeCTraderServer:
    """The reference-data venue, also accepting every spot (un)subscription."""
    server = venue()
    for payload_type, response_class in (
        (om.PROTO_OA_SUBSCRIBE_SPOTS_REQ, oa.ProtoOASubscribeSpotsRes),
        (om.PROTO_OA_UNSUBSCRIBE_SPOTS_REQ, oa.ProtoOAUnsubscribeSpotsRes),
    ):
        server.on(
            payload_type,
            lambda r, cls=response_class: cls(ctidTraderAccountId=r.ctidTraderAccountId),
        )
    return server


def refusing_venue(symbol_id: int) -> FakeCTraderServer:
    """A venue that refuses to subscribe one symbol's spots."""
    server = data_venue()
    server.on(
        om.PROTO_OA_SUBSCRIBE_SPOTS_REQ,
        lambda r: (
            oa.ProtoOAErrorRes(ctidTraderAccountId=r.ctidTraderAccountId, errorCode="NO_QUOTES")
            if symbol_id in r.symbolId
            else oa.ProtoOASubscribeSpotsRes(ctidTraderAccountId=r.ctidTraderAccountId)
        ),
    )
    return server


def empty_conversion_venue() -> FakeCTraderServer:
    server = data_venue()
    server.on(
        om.PROTO_OA_SYMBOLS_FOR_CONVERSION_REQ,
        lambda r: oa.ProtoOASymbolsForConversionRes(ctidTraderAccountId=r.ctidTraderAccountId),
    )
    return server


# -- Bar fixtures ---------------------------------------------------------------------------


def _live_m1_bars(symbol_id: int) -> dict[int, om.ProtoOATrendbar]:
    """The closing state of every M1 trendbar the recorded spot events carry for `symbol_id`.

    A forming bar is re-sent on every tick, so the last state recorded for a minute is the one
    history would serve for it.
    """
    bars: dict[int, om.ProtoOATrendbar] = {}
    for event in SPOTS:
        if event.symbolId == symbol_id:
            for trendbar in event.trendbar:
                bars[trendbar.utcTimestampInMinutes] = trendbar
    return bars


# What the venue's history serves, by `(symbol id, period)` and then by open minute.
RECORDED_HISTORY: dict[tuple[int, int], dict[int, om.ProtoOATrendbar]] = {
    (EURUSD_SYMBOL_ID, M1): _live_m1_bars(EURUSD_SYMBOL_ID),
    (GER40_SYMBOL_ID, M1): _live_m1_bars(GER40_SYMBOL_ID),
    (EURUSD_SYMBOL_ID, H1): {
        trendbar.utcTimestampInMinutes: trendbar
        for trendbar in RECORDED["trendbars_h1"][0].trendbar
    },
}


def _serve_trendbars(
    request: oa.ProtoOAGetTrendbarsReq,
    *,
    inclusive_to: bool,
    chunk: int | None,
) -> oa.ProtoOAGetTrendbarsRes:
    """Serve the recorded bars in the requested window, keeping the newest when capped.

    `inclusive_to` and `chunk` stand in for the two things the live endpoint has not settled:
    whether `toTimestamp` is inclusive, and a per-request cap below the asked-for `count`.
    """
    source = RECORDED_HISTORY.get((request.symbolId, request.period), {})
    bars = [
        trendbar
        for minute, trendbar in sorted(source.items())
        if request.fromTimestamp <= minute * 60_000 <= request.toTimestamp
        and (inclusive_to or minute * 60_000 != request.toTimestamp)
    ]
    cap = min(request.count or len(bars), len(bars) if chunk is None else chunk)
    truncated = len(bars) > cap
    return oa.ProtoOAGetTrendbarsRes(
        ctidTraderAccountId=request.ctidTraderAccountId,
        symbolId=request.symbolId,
        period=request.period,
        trendbar=bars[-cap:],
        hasMore=truncated,
    )


def trendbar_venue(*, inclusive_to: bool = False, chunk: int | None = None) -> FakeCTraderServer:
    """The data venue, also accepting live trendbar subscriptions and serving recorded history."""
    server = data_venue()
    for payload_type, response_class in (
        (om.PROTO_OA_SUBSCRIBE_LIVE_TRENDBAR_REQ, oa.ProtoOASubscribeLiveTrendbarRes),
        (om.PROTO_OA_UNSUBSCRIBE_LIVE_TRENDBAR_REQ, oa.ProtoOAUnsubscribeLiveTrendbarRes),
    ):
        server.on(
            payload_type,
            lambda r, cls=response_class: cls(ctidTraderAccountId=r.ctidTraderAccountId),
        )
    server.on(
        om.PROTO_OA_GET_TRENDBARS_REQ,
        functools.partial(_serve_trendbars, inclusive_to=inclusive_to, chunk=chunk),
    )
    return server


class PinnedClock:
    """A `Clock` for the client's bar closers whose `now()` the test sets.

    The fixtures carry the venue's own timestamps, so a recorded bar only counts as live while
    the closer is told the time it was recorded at. Timers still go to the event loop, so the
    delays the closer computes are real ones.
    """

    def __init__(self, now_secs: float) -> None:
        self.t = now_secs

    def now(self) -> float:
        return self.t

    def call_later(self, delay_secs: float, callback: Callable[[], None]) -> asyncio.TimerHandle:
        return asyncio.get_running_loop().call_later(delay_secs, callback)


def pin_clock(h: Harness, minute: int, offset_secs: float = 5.0) -> PinnedClock:
    """Put the client's bar clock `offset_secs` into `minute`, and return it for later moves."""
    clock = PinnedClock(minute * 60 + offset_secs)
    h.client._bar_clock = clock
    return clock


def at_minute(minute: int) -> datetime:
    return datetime.fromtimestamp(minute * 60, tz=UTC)


def close_ns(minute: int, period_secs: int) -> int:
    """`ts_event` of the closed bar opening at `minute`: its close time in nanoseconds."""
    return (minute * 60 + period_secs) * 1_000_000_000


def spots_until(symbol_id: int, minute: int) -> list[oa.ProtoOASpotEvent]:
    """Recorded spot events for `symbol_id`, up to and including the first one opening `minute`."""
    events = []
    for event in SPOTS:
        if event.symbolId != symbol_id or not event.trendbar:
            continue
        events.append(event)
        if event.trendbar[0].utcTimestampInMinutes == minute:
            break
    return events


async def subscribe_bars(h: Harness, bar_type: BarType) -> None:
    await h.client._subscribe_bars(
        SubscribeBars(
            bar_type=bar_type,
            client_id=None,
            venue=CTRADER_VENUE,
            command_id=UUID4(),
            ts_init=0,
        ),
    )


async def unsubscribe_bars(h: Harness, bar_type: BarType) -> None:
    await h.client._unsubscribe_bars(
        UnsubscribeBars(
            bar_type=bar_type,
            client_id=None,
            venue=CTRADER_VENUE,
            command_id=UUID4(),
            ts_init=0,
        ),
    )


async def request_bars(
    h: Harness,
    bar_type: BarType,
    *,
    limit: int = 0,
    start: datetime | None = None,
    end: datetime | None = None,
) -> list[Bar]:
    await h.client._request_bars(
        RequestBars(
            bar_type=bar_type,
            start=start,
            end=end,
            limit=limit,
            client_id=None,
            venue=CTRADER_VENUE,
            callback=None,
            request_id=UUID4(),
            ts_init=0,
            params=None,
        ),
    )
    return list(h.responses[-1].data)


# -- Config ---------------------------------------------------------------------------------


def test_credentials_carry_the_configured_tokens() -> None:
    credentials = config().credentials()

    assert credentials.client_id == "client-id"
    assert credentials.access_token == "access-token"
    assert credentials.refresh_token == "refresh-token"
    assert credentials.token_expires_at == 4_102_444_800.0


async def test_an_unknown_asset_class_override_fails_construction() -> None:
    async with harness(client_config=config()) as h:
        with pytest.raises(ValueError, match="NOT_A_CLASS"):
            CTraderDataClient(
                loop=asyncio.get_running_loop(),
                account=h.account,
                msgbus=TestComponentStubs.msgbus(),
                cache=TestComponentStubs.cache(),
                clock=TestComponentStubs.clock(),
                instrument_provider=h.provider,
                config=config(asset_class_overrides={"GER40.cash": "NOT_A_CLASS"}),
            )


# -- Connect --------------------------------------------------------------------------------


async def test_connect_publishes_every_loaded_instrument() -> None:
    async with harness() as h:
        await h.client._connect()

        published = {i.id for i in h.instruments()}
        assert GER40_ID in published
        assert EURUSD_ID in published


async def test_connect_subscribes_the_conversion_chain() -> None:
    async with harness() as h:
        await h.client._connect()

        assert EURUSD_SYMBOL_ID in h.subscribed_symbol_ids()
        assert "conversion" in h.account.subscriptions.consumers(EURUSD_SYMBOL_ID)


def test_the_conversion_message_names_the_currencies_and_the_symbols() -> None:
    assert _conversion_message("EUR", "USD", ["EURUSD"]) == "Conversion EUR->USD: subscribed EURUSD"


async def test_conversion_can_be_switched_off() -> None:
    async with harness(client_config=config(subscribe_conversion_quotes=False)) as h:
        await h.client._connect()

        assert not received(h.server, oa.ProtoOASymbolsForConversionReq)
        assert EURUSD_SYMBOL_ID not in h.subscribed_symbol_ids()


async def test_a_broken_conversion_drops_the_instrument() -> None:
    async with harness(server=empty_conversion_venue()) as h:
        await h.client._connect()

        assert h.provider.find(GER40_ID) is None
        assert GER40_ID not in {i.id for i in h.instruments()}
        assert any(f.symbol == "GER40.cash" for f in h.provider.failures)


async def test_a_broken_conversion_fails_connect_when_configured_to() -> None:
    async with harness(
        client_config=config(fail_on_instrument_error=True),
        server=empty_conversion_venue(),
    ) as h:
        with pytest.raises(InstrumentLoadError):
            await h.client._connect()


async def test_connect_re_queries_the_conversion_chain_every_time() -> None:
    async with harness() as h:
        await h.client._connect()
        await h.client._disconnect()
        await h.client._connect()

        assert len(received(h.server, oa.ProtoOASymbolsForConversionReq)) == 2


async def test_the_nautilus_lifecycle_connects_and_disconnects_the_client() -> None:
    async with harness() as h:
        h.client.connect()
        await wait_until(lambda: h.client.is_connected, description="client connected")

        h.client.disconnect()
        await wait_until(lambda: not h.client.is_connected, description="client disconnected")


# -- Quotes ---------------------------------------------------------------------------------


async def test_a_one_sided_spot_emits_nothing_until_both_sides_are_known() -> None:
    async with harness() as h:
        await h.client._connect()
        await subscribe_quotes(h, EURUSD_ID)

        await push_spot(h, BID_ONLY)

        assert not h.quotes()


async def test_a_quote_carries_the_recorded_prices_and_timestamp() -> None:
    async with harness() as h:
        await h.client._connect()
        await subscribe_quotes(h, EURUSD_ID)

        await push_spot(h, BID_ONLY)
        await push_spot(h, ASK_ONLY)

        quote = h.quotes()[-1]
        assert str(quote.bid_price) == "1.14813"
        assert str(quote.ask_price) == "1.14814"
        assert quote.ts_event == ASK_ONLY.timestamp * 1_000_000


async def test_quote_sizes_are_zero_without_a_synthetic_size() -> None:
    async with harness() as h:
        await h.client._connect()
        await subscribe_quotes(h, EURUSD_ID)

        await push_spot(h, TWO_SIDED)

        quote = h.quotes()[-1]
        assert quote.bid_size == quote.ask_size
        assert float(quote.bid_size) == 0.0


async def test_the_synthetic_quote_size_is_used_for_both_sides() -> None:
    async with harness(client_config=config(synthetic_quote_size=1_000_000)) as h:
        await h.client._connect()
        await subscribe_quotes(h, EURUSD_ID)

        await push_spot(h, TWO_SIDED)

        quote = h.quotes()[-1]
        assert float(quote.bid_size) == 1_000_000.0
        assert float(quote.ask_size) == 1_000_000.0


async def test_subscribing_quotes_holds_the_spot_subscription_for_that_instrument() -> None:
    async with harness() as h:
        await h.client._connect()

        await subscribe_quotes(h, GER40_ID)

        assert f"quotes:{GER40_ID}" in h.account.subscriptions.consumers(GER40_SYMBOL_ID)


async def test_unsubscribing_quotes_releases_the_subscription_and_stops_the_ticks() -> None:
    # GER40.cash, because EURUSD keeps its own ticks as a conversion leg.
    async with harness() as h:
        await h.client._connect()
        await subscribe_quotes(h, GER40_ID)

        await unsubscribe_quotes(h, GER40_ID)

        assert not h.account.subscriptions.consumers(GER40_SYMBOL_ID)
        await push_spot(h, GER40_SPOT)
        assert not h.quotes()


async def test_a_load_all_instrument_converts_before_its_first_subscription() -> None:
    client_config = config(instrument_provider=InstrumentProviderConfig(load_all=True))
    async with harness(client_config=client_config) as h:
        await h.client._connect()
        assert not received(h.server, oa.ProtoOASymbolsForConversionReq)

        await subscribe_quotes(h, GER40_ID)

        order = [
            type(m).__name__
            for m in h.server.received
            if isinstance(m, oa.ProtoOASymbolsForConversionReq | oa.ProtoOASubscribeSpotsReq)
        ]
        assert order[0] == "ProtoOASymbolsForConversionReq"


async def test_disconnect_releases_every_subscription_it_holds() -> None:
    async with harness() as h:
        await h.client._connect()
        await subscribe_quotes(h, GER40_ID)

        await h.client._disconnect()

        released = h.unsubscribed_symbol_ids()
        assert EURUSD_SYMBOL_ID in released
        assert GER40_SYMBOL_ID in released


# -- Instruments ----------------------------------------------------------------------------


async def test_a_symbol_change_reloads_and_republishes_the_instrument() -> None:
    async with harness() as h:
        await h.client._connect()
        before = len(h.instruments())

        await h.server.push(
            oa.ProtoOASymbolChangedEvent(
                ctidTraderAccountId=ACCOUNT_ID,
                symbolId=[GER40_SYMBOL_ID],
            ),
        )

        await wait_until(
            lambda: any(i.id == GER40_ID for i in h.instruments()[before:]),
            description="GER40.cash republished",
        )


async def test_a_symbol_change_on_a_chain_leg_re_queries_the_chain() -> None:
    client_config = config(instrument_provider=InstrumentProviderConfig(load_all=True))
    async with harness(client_config=client_config) as h:
        await h.client._connect()
        await subscribe_quotes(h, GER40_ID)
        assert len(received(h.server, oa.ProtoOASymbolsForConversionReq)) == 1

        await h.server.push(
            oa.ProtoOASymbolChangedEvent(
                ctidTraderAccountId=ACCOUNT_ID,
                symbolId=[EURUSD_SYMBOL_ID],
            ),
        )
        await wait_until(
            lambda: len(received(h.server, oa.ProtoOASymbolByIdReq)) > 1,
            description="EURUSD reloaded",
        )
        await unsubscribe_quotes(h, GER40_ID)
        await subscribe_quotes(h, GER40_ID)

        assert len(received(h.server, oa.ProtoOASymbolsForConversionReq)) == 2


async def test_request_instrument_answers_from_the_provider() -> None:
    async with harness() as h:
        await h.client._connect()

        await h.client._request_instrument(
            RequestInstrument(
                instrument_id=GER40_ID,
                start=None,
                end=None,
                client_id=None,
                venue=CTRADER_VENUE,
                callback=None,
                request_id=UUID4(),
                ts_init=0,
                params=None,
            ),
        )

        assert [i.id for i in h.responses[-1].data] == [GER40_ID]


async def test_request_instruments_answers_with_every_loaded_instrument() -> None:
    async with harness() as h:
        await h.client._connect()

        await h.client._request_instruments(
            RequestInstruments(
                start=None,
                end=None,
                client_id=None,
                venue=CTRADER_VENUE,
                callback=None,
                request_id=UUID4(),
                ts_init=0,
                params=None,
            ),
        )

        assert {i.id for i in h.responses[-1].data} == {GER40_ID, EURUSD_ID}


# -- Conversion quotes and rollback -----------------------------------------------------------


async def test_conversion_quotes_price_the_deposit_currency() -> None:
    async with harness() as h:
        await h.client._connect()

        await push_spot(h, TWO_SIDED)

        assert [q.instrument_id for q in h.quotes()] == [EURUSD_ID]
        assert h.cache.get_xrate(CTRADER_VENUE, EUR, USD) is not None


async def test_a_refused_quote_subscribe_can_be_retried() -> None:
    async with harness(server=refusing_venue(GER40_SYMBOL_ID)) as h:
        await h.client._connect()

        with pytest.raises(CTraderRequestError):
            await subscribe_quotes(h, GER40_ID)
        assert not h.account.subscriptions.consumers(GER40_SYMBOL_ID)

        # The venue relents; the retry must not be a silent no-op.
        h.server.on(
            om.PROTO_OA_SUBSCRIBE_SPOTS_REQ,
            lambda r: oa.ProtoOASubscribeSpotsRes(ctidTraderAccountId=r.ctidTraderAccountId),
        )
        await subscribe_quotes(h, GER40_ID)

        assert f"quotes:{GER40_ID}" in h.account.subscriptions.consumers(GER40_SYMBOL_ID)


async def test_a_refused_conversion_leg_keeps_the_instrument_and_retries() -> None:
    async with harness(server=refusing_venue(EURUSD_SYMBOL_ID)) as h:
        await h.client._connect()

        # A refusal is the venue saying no to one request, not a reason to drop the instrument.
        assert h.provider.find(GER40_ID) is not None
        assert not any(f.symbol == "GER40.cash" for f in h.provider.failures)
        assert GER40_ID not in h.client._converted
        assert not h.account.subscriptions.consumers(EURUSD_SYMBOL_ID)

        h.server.on(
            om.PROTO_OA_SUBSCRIBE_SPOTS_REQ,
            lambda r: oa.ProtoOASubscribeSpotsRes(ctidTraderAccountId=r.ctidTraderAccountId),
        )
        await subscribe_quotes(h, GER40_ID)

        assert "conversion" in h.account.subscriptions.consumers(EURUSD_SYMBOL_ID)
        assert GER40_ID in h.client._converted


async def test_a_conversion_leg_that_is_also_subscribed_emits_one_quote() -> None:
    async with harness() as h:
        await h.client._connect()
        await subscribe_quotes(h, EURUSD_ID)

        await push_spot(h, TWO_SIDED)

        assert len(h.quotes()) == 1
        assert len(h.cache.quote_ticks(EURUSD_ID)) == 1


async def test_a_subscribed_symbol_pulled_in_as_a_leg_emits_one_quote() -> None:
    client_config = config(instrument_provider=InstrumentProviderConfig(load_all=True))
    async with harness(client_config=client_config) as h:
        await h.client._connect()
        await subscribe_quotes(h, EURUSD_ID)
        # GER40.cash is EUR-quoted, so this adds EURUSD a second time, as a conversion leg.
        await subscribe_quotes(h, GER40_ID)

        await push_spot(h, TWO_SIDED)

        assert len(h.quotes()) == 1
        assert len(h.cache.quote_ticks(EURUSD_ID)) == 1


async def test_dropping_one_hold_on_a_symbol_leaves_the_other_emitting() -> None:
    async with harness() as h:
        await h.client._connect()
        await subscribe_quotes(h, EURUSD_ID)

        await unsubscribe_quotes(h, EURUSD_ID)
        await push_spot(h, TWO_SIDED)

        # Still held for the conversion chain, so still exactly one tick.
        assert "conversion" in h.account.subscriptions.consumers(EURUSD_SYMBOL_ID)
        assert len(h.quotes()) == 1


async def test_a_bid_from_before_a_reconnect_is_not_paired_with_a_later_ask(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("nautilus_ctrader.common.session.STABLE_SESSION_SECS", 0.0)
    monkeypatch.setattr("nautilus_ctrader.common.session.BACKOFF_BASE_SECS", 0.01)
    async with harness() as h:
        await h.client._connect()
        await subscribe_quotes(h, EURUSD_ID)
        await push_spot(h, BID_ONLY)
        assert not h.quotes()

        await h.server.drop_connections()
        await wait_until(
            lambda: h.subscribed_symbol_ids().count(EURUSD_SYMBOL_ID) >= 2,
            timeout_secs=10.0,
            description="spots re-subscribed after the reconnect",
        )

        await push_spot(h, ASK_ONLY)

        assert not h.quotes()


# -- Live bars ------------------------------------------------------------------------------


async def test_a_change_of_open_time_closes_exactly_one_bar() -> None:
    async with harness(server=trendbar_venue()) as h:
        await h.client._connect()
        pin_clock(h, FIRST_M1_MINUTE)
        await subscribe_bars(h, EURUSD_M1)

        for event in spots_until(EURUSD_SYMBOL_ID, FIRST_M1_MINUTE + 1):
            await push_spot(h, event)

        assert len(h.bars()) == 1
        bar = h.bars()[0]
        assert bar.bar_type == EURUSD_M1
        assert bar.ts_event == close_ns(FIRST_M1_MINUTE, 60)
        # The recorded closing state of that minute, field by field.
        assert [str(bar.open), str(bar.high), str(bar.low), str(bar.close)] == [
            "1.14814",
            "1.14818",
            "1.14809",
            "1.14809",
        ]
        assert float(bar.volume) == 84.0


async def test_a_quote_unsubscribe_forgets_the_book_although_the_bars_go_on() -> None:
    # No conversion, so the quote subscription is the symbol's only spot hold.
    client_config = config(
        subscribe_conversion_quotes=False,
        instrument_provider=InstrumentProviderConfig(load_ids=frozenset({EURUSD_ID})),
    )
    async with harness(client_config=client_config, server=trendbar_venue()) as h:
        await h.client._connect()
        pin_clock(h, FIRST_M1_MINUTE)
        # The bar subscription keeps the symbol's listener attached throughout.
        await subscribe_bars(h, EURUSD_M1)
        await subscribe_quotes(h, EURUSD_ID)
        await push_spot(h, BID_ONLY)

        await unsubscribe_quotes(h, EURUSD_ID)
        await subscribe_quotes(h, EURUSD_ID)
        await push_spot(h, ASK_ONLY)

        # The bid is from before the gap in the subscription, so it may not be paired.
        assert not h.quotes()


async def test_subscribing_bars_holds_the_trendbar_subscription() -> None:
    async with harness(server=trendbar_venue()) as h:
        await h.client._connect()
        pin_clock(h, FIRST_M1_MINUTE)

        await subscribe_bars(h, EURUSD_M1)

        subscribed = received(h.server, oa.ProtoOASubscribeLiveTrendbarReq)
        assert [(r.symbolId, r.period) for r in subscribed] == [(EURUSD_SYMBOL_ID, M1)]


async def test_bars_reach_a_symbol_that_has_no_quote_subscription() -> None:
    async with harness(server=trendbar_venue()) as h:
        await h.client._connect()
        pin_clock(h, FIRST_M1_MINUTE)
        await subscribe_bars(h, GER40_M1)

        for event in spots_until(GER40_SYMBOL_ID, FIRST_M1_MINUTE + 1):
            await push_spot(h, event)

        assert [b.bar_type for b in h.bars()] == [GER40_M1]
        # Spots are held for the trendbars, not for quoting, so no tick is published.
        assert not h.quotes()


async def test_an_unsupported_price_type_is_refused_without_a_venue_subscription() -> None:
    async with harness(server=trendbar_venue()) as h:
        await h.client._connect()

        await subscribe_bars(h, EURUSD_LAST)

        assert not received(h.server, oa.ProtoOASubscribeLiveTrendbarReq)
        assert EURUSD_LAST not in h.client._bars


async def test_unsubscribing_bars_releases_the_subscription_and_stops_the_bars() -> None:
    async with harness(server=trendbar_venue()) as h:
        await h.client._connect()
        pin_clock(h, FIRST_M1_MINUTE)
        await subscribe_bars(h, EURUSD_M1)

        await unsubscribe_bars(h, EURUSD_M1)

        released = received(h.server, oa.ProtoOAUnsubscribeLiveTrendbarReq)
        assert [(r.symbolId, r.period) for r in released] == [(EURUSD_SYMBOL_ID, M1)]
        for event in spots_until(EURUSD_SYMBOL_ID, FIRST_M1_MINUTE + 1):
            await push_spot(h, event)
        assert not h.bars()


async def test_a_cancelled_subscribe_still_gets_its_backfill_restore(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("nautilus_ctrader.common.session.STABLE_SESSION_SECS", 0.0)
    monkeypatch.setattr("nautilus_ctrader.common.session.BACKOFF_BASE_SECS", 0.01)
    async with harness(server=trendbar_venue()) as h:
        await h.client._connect()
        clock = pin_clock(h, FIRST_M1_MINUTE)
        held = HeldReplies(
            h.server,
            om.PROTO_OA_SUBSCRIBE_LIVE_TRENDBAR_REQ,
            lambda r: oa.ProtoOASubscribeLiveTrendbarRes(ctidTraderAccountId=r.ctidTraderAccountId),
        )

        cancelled = asyncio.create_task(subscribe_bars(h, EURUSD_M1))
        await held.arrived.wait()
        cancelled.cancel()
        with pytest.raises(asyncio.CancelledError):
            await cancelled
        await held.stop_holding()

        # The registry counts a subscribe whose outcome is unknown, so the bars are live and
        # the reconnect must still drop the partial bar and backfill.
        assert EURUSD_M1 in h.client._bars
        await h.server.drop_connections()
        clock.t = (FIRST_M1_MINUTE + 3) * 60 + 5
        await wait_until(
            lambda: len(h.bars()) >= 3,
            timeout_secs=10.0,
            description="the missed bars were backfilled",
        )


async def test_a_subscribe_the_registry_did_not_record_frees_the_bar_type(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with harness(server=trendbar_venue()) as h:
        await h.client._connect()
        pin_clock(h, FIRST_M1_MINUTE)

        async def raise_protocol_error(*_args: object) -> None:
            raise CTraderProtocolError("unreadable response")

        monkeypatch.setattr(
            h.account.subscriptions,
            "subscribe_trendbars",
            raise_protocol_error,
        )
        with pytest.raises(CTraderProtocolError):
            await subscribe_bars(h, EURUSD_M1)
        assert EURUSD_M1 not in h.client._bars

        # Nothing was recorded anywhere, so the next subscribe must not be a silent no-op.
        monkeypatch.undo()
        await subscribe_bars(h, EURUSD_M1)

        assert received(h.server, oa.ProtoOASubscribeLiveTrendbarReq)


async def test_a_cancelled_bar_unsubscribe_is_repeated_at_disconnect() -> None:
    async with harness(server=trendbar_venue()) as h:
        await h.client._connect()
        pin_clock(h, FIRST_M1_MINUTE)
        await subscribe_bars(h, EURUSD_M1)
        held = HeldReplies(
            h.server,
            om.PROTO_OA_UNSUBSCRIBE_LIVE_TRENDBAR_REQ,
            lambda r: oa.ProtoOAUnsubscribeLiveTrendbarRes(
                ctidTraderAccountId=r.ctidTraderAccountId,
            ),
        )

        cancelled = asyncio.create_task(unsubscribe_bars(h, EURUSD_M1))
        await held.arrived.wait()
        cancelled.cancel()
        with pytest.raises(asyncio.CancelledError):
            await cancelled
        await held.stop_holding()

        assert EURUSD_M1 not in h.client._bars  # no longer routed or closed
        assert EURUSD_M1 in h.client._releasing_bars  # still on record for the repeat
        for event in spots_until(EURUSD_SYMBOL_ID, FIRST_M1_MINUTE + 1):
            await push_spot(h, event)
        assert not h.bars()

        await h.client._disconnect()

        assert not h.client._releasing_bars


async def test_connect_forgets_bar_subscriptions_left_by_an_interrupted_disconnect() -> None:
    async with harness(server=trendbar_venue()) as h:
        await h.client._connect()
        pin_clock(h, FIRST_M1_MINUTE)
        await subscribe_bars(h, EURUSD_M1)
        await subscribe_quotes(h, GER40_ID)
        await h.account.disconnect()  # a disconnect that never reached the client

        await h.client._connect()

        # Nothing from the old connection is left to make a later subscribe a silent no-op;
        # only what this connect subscribed itself is on record.
        assert not h.client._bars
        assert not h.client._bar_routes
        assert GER40_SYMBOL_ID not in h.client._spot_holds
        assert h.client._spot_holds == {EURUSD_SYMBOL_ID: {CONVERSION_CONSUMER}}


async def test_disconnect_releases_the_trendbar_subscription() -> None:
    async with harness(server=trendbar_venue()) as h:
        await h.client._connect()
        pin_clock(h, FIRST_M1_MINUTE)
        await subscribe_bars(h, EURUSD_M1)

        await h.client._disconnect()

        assert received(h.server, oa.ProtoOAUnsubscribeLiveTrendbarReq)
        assert not h.client._bars


# -- Historical bars ------------------------------------------------------------------------


async def test_request_bars_returns_the_last_closed_bars_ascending() -> None:
    async with harness(client_config=config(history_page_size=20), server=trendbar_venue()) as h:
        await h.client._connect()

        bars = await request_bars(h, EURUSD_H1, limit=30, end=at_minute(LAST_H1_MINUTE + 60))

        assert len(bars) == 30
        assert [b.ts_event for b in bars] == sorted(b.ts_event for b in bars)
        assert bars[-1].ts_event == close_ns(LAST_H1_MINUTE, 3600)
        assert all(b.bar_type == EURUSD_H1 for b in bars)
        # Twenty bars a page, so the thirtieth is only reached on the second one.
        assert len(received(h.server, oa.ProtoOAGetTrendbarsReq)) == 2


async def test_request_bars_without_a_start_or_a_limit_asks_for_one_page() -> None:
    async with harness(client_config=config(history_page_size=20), server=trendbar_venue()) as h:
        await h.client._connect()

        bars = await request_bars(h, EURUSD_H1, end=at_minute(LAST_H1_MINUTE + 60))

        assert len(received(h.server, oa.ProtoOAGetTrendbarsReq)) == 1
        assert len(bars) == 20


async def test_request_bars_stops_at_the_requested_start() -> None:
    async with harness(client_config=config(history_page_size=20), server=trendbar_venue()) as h:
        await h.client._connect()

        bars = await request_bars(
            h,
            EURUSD_H1,
            start=at_minute(LAST_H1_MINUTE - 120),
            end=at_minute(LAST_H1_MINUTE + 60),
        )

        assert [b.ts_event for b in bars] == [
            close_ns(LAST_H1_MINUTE - 120, 3600),
            close_ns(LAST_H1_MINUTE - 60, 3600),
            close_ns(LAST_H1_MINUTE, 3600),
        ]


async def test_request_bars_counts_only_closed_bars_towards_the_limit() -> None:
    async with harness(client_config=config(history_page_size=20), server=trendbar_venue()) as h:
        await h.client._connect()
        # Half way into the bar opening at LAST_H1_MINUTE, which is therefore still forming.
        h.client._bar_clock = PinnedClock(LAST_H1_MINUTE * 60 + 1800)

        bars = await request_bars(h, EURUSD_H1, limit=20)

        assert len(bars) == 20
        assert bars[-1].ts_event == close_ns(LAST_H1_MINUTE - 60, 3600)


async def test_an_inclusive_to_timestamp_neither_duplicates_nor_skips_a_bar() -> None:
    client_config = config(history_page_size=20)
    async with harness(client_config=client_config, server=trendbar_venue(inclusive_to=True)) as h:
        await h.client._connect()

        bars = await request_bars(h, EURUSD_H1, limit=30, end=at_minute(LAST_H1_MINUTE + 60))

        assert [b.ts_event for b in bars] == [
            close_ns(LAST_H1_MINUTE - 60 * offset, 3600) for offset in reversed(range(30))
        ]


async def test_a_page_the_venue_cuts_short_is_continued_from_what_it_served() -> None:
    client_config = config(history_page_size=20)
    async with harness(client_config=client_config, server=trendbar_venue(chunk=5)) as h:
        await h.client._connect()

        bars = await request_bars(h, EURUSD_H1, limit=30, end=at_minute(LAST_H1_MINUTE + 60))

        assert [b.ts_event for b in bars] == [
            close_ns(LAST_H1_MINUTE - 60 * offset, 3600) for offset in reversed(range(30))
        ]


async def test_a_one_bar_page_on_an_inclusive_edge_still_walks_backwards() -> None:
    client_config = config(history_page_size=20)
    server = trendbar_venue(inclusive_to=True, chunk=1)
    async with harness(client_config=client_config, server=server) as h:
        await h.client._connect()

        bars = await request_bars(h, EURUSD_H1, limit=3, end=at_minute(LAST_H1_MINUTE + 60))

        assert [b.ts_event for b in bars] == [
            close_ns(LAST_H1_MINUTE - 60 * offset, 3600) for offset in reversed(range(3))
        ]


async def test_a_bar_the_stream_did_not_close_is_fetched_from_history() -> None:
    client_config = config(bar_close_grace_secs=0.05, bar_close_history_retries=0)
    async with harness(client_config=client_config, server=trendbar_venue()) as h:
        await h.client._connect()
        # Just short of the close, so the closer's timer is due almost at once.
        h.client._bar_clock = PinnedClock(FIRST_M1_MINUTE * 60 + 59.9)
        await subscribe_bars(h, EURUSD_M1)

        await push_spot(h, SPOTS[2])  # the forming bar, never closed by a later open time
        await wait_until(lambda: bool(h.bars()), description="the closed bar was fetched")

        request = received(h.server, oa.ProtoOAGetTrendbarsReq)[-1]
        assert request.symbolId == EURUSD_SYMBOL_ID
        assert request.period == M1
        assert request.fromTimestamp == FIRST_M1_MINUTE * 60 * 1_000
        assert request.toTimestamp == (FIRST_M1_MINUTE * 60 + 60) * 1_000
        # One more than the window holds, so the bar wanted is never the one truncated away.
        assert request.count == 2
        assert [b.ts_event for b in h.bars()] == [close_ns(FIRST_M1_MINUTE, 60)]
        assert float(h.bars()[0].volume) == 84.0  # history's state, not the first tick's


async def test_a_history_bar_for_another_boundary_is_discarded() -> None:
    client_config = config(bar_close_grace_secs=0.05, bar_close_history_retries=0)
    server = trendbar_venue()
    # Whatever is asked for, the venue answers with the next minute's bar.
    server.on(
        om.PROTO_OA_GET_TRENDBARS_REQ,
        lambda r: oa.ProtoOAGetTrendbarsRes(
            ctidTraderAccountId=r.ctidTraderAccountId,
            symbolId=r.symbolId,
            period=r.period,
            trendbar=[RECORDED_HISTORY[(EURUSD_SYMBOL_ID, M1)][FIRST_M1_MINUTE + 1]],
        ),
    )
    async with harness(client_config=client_config, server=server) as h:
        await h.client._connect()
        h.client._bar_clock = PinnedClock(FIRST_M1_MINUTE * 60 + 59.9)
        await subscribe_bars(h, EURUSD_M1)

        await push_spot(h, SPOTS[2])
        await wait_until(lambda: bool(h.bars()), description="the closed bar was emitted")

        # The streamed state of the right minute, never the wrong bar history offered.
        assert [b.ts_event for b in h.bars()] == [close_ns(FIRST_M1_MINUTE, 60)]
        assert float(h.bars()[0].volume) == float(SPOTS[2].trendbar[0].volume)


async def test_a_warm_up_request_is_not_repeated_by_the_stream() -> None:
    async with harness(server=trendbar_venue()) as h:
        await h.client._connect()
        warm_up = await request_bars(h, EURUSD_M1, end=at_minute(FIRST_M1_MINUTE + 1))
        assert [b.ts_event for b in warm_up] == [close_ns(FIRST_M1_MINUTE, 60)]

        # Subscribed in the next minute, as a warm-up ending at that bar's close implies.
        pin_clock(h, FIRST_M1_MINUTE + 1)
        await subscribe_bars(h, EURUSD_M1)
        for event in spots_until(EURUSD_SYMBOL_ID, FIRST_M1_MINUTE + 2):
            await push_spot(h, event)

        assert [b.ts_event for b in h.bars()] == [close_ns(FIRST_M1_MINUTE + 1, 60)]


async def test_a_request_covering_a_live_subscription_suppresses_the_streamed_bar() -> None:
    async with harness(server=trendbar_venue()) as h:
        await h.client._connect()
        clock = pin_clock(h, FIRST_M1_MINUTE)
        await subscribe_bars(h, EURUSD_M1)

        clock.t = (FIRST_M1_MINUTE + 1) * 60 + 5
        served = await request_bars(h, EURUSD_M1, end=at_minute(FIRST_M1_MINUTE + 1))
        assert [b.ts_event for b in served] == [close_ns(FIRST_M1_MINUTE, 60)]

        for event in spots_until(EURUSD_SYMBOL_ID, FIRST_M1_MINUTE + 1):
            await push_spot(h, event)

        # The request already delivered the bar the stream would now close.
        assert not h.bars()


# -- Reconnect ------------------------------------------------------------------------------


async def test_a_reconnect_drops_the_partial_bar_and_backfills_what_was_missed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("nautilus_ctrader.common.session.STABLE_SESSION_SECS", 0.0)
    monkeypatch.setattr("nautilus_ctrader.common.session.BACKOFF_BASE_SECS", 0.01)
    async with harness(server=trendbar_venue()) as h:
        await h.client._connect()
        clock = pin_clock(h, FIRST_M1_MINUTE)
        await subscribe_bars(h, EURUSD_M1)
        for event in spots_until(EURUSD_SYMBOL_ID, FIRST_M1_MINUTE):
            await push_spot(h, event)
        assert not h.bars()

        await h.server.drop_connections()
        # Three bars closed while the connection was down.
        clock.t = (FIRST_M1_MINUTE + 3) * 60 + 5
        await wait_until(
            lambda: len(h.bars()) >= 3,
            timeout_secs=10.0,
            description="the missed bars were backfilled",
        )

        assert [b.ts_event for b in h.bars()] == [
            close_ns(FIRST_M1_MINUTE, 60),
            close_ns(FIRST_M1_MINUTE + 1, 60),
            close_ns(FIRST_M1_MINUTE + 2, 60),
        ]
        # The backfill's history requests must follow the re-subscription, not precede it.
        order = [
            type(m).__name__
            for m in h.server.received
            if isinstance(m, oa.ProtoOASubscribeLiveTrendbarReq | oa.ProtoOAGetTrendbarsReq)
        ]
        assert order[:3] == [
            "ProtoOASubscribeLiveTrendbarReq",
            "ProtoOASubscribeLiveTrendbarReq",
            "ProtoOAGetTrendbarsReq",
        ]


# -- Carry-over fixes -----------------------------------------------------------------------


async def test_a_refused_conversion_fails_connect_when_configured_to() -> None:
    async with harness(
        client_config=config(fail_on_instrument_error=True),
        server=refusing_venue(EURUSD_SYMBOL_ID),
    ) as h:
        with pytest.raises(CTraderRequestError):
            await h.client._connect()


async def test_a_refused_conversion_outside_connect_still_retries() -> None:
    client_config = config(
        fail_on_instrument_error=True,
        instrument_provider=InstrumentProviderConfig(load_all=True),
    )
    async with harness(client_config=client_config, server=refusing_venue(EURUSD_SYMBOL_ID)) as h:
        await h.client._connect()

        await subscribe_quotes(h, GER40_ID)

        assert h.provider.find(GER40_ID) is not None
        assert GER40_ID not in h.client._converted


async def test_a_cancelled_release_does_not_let_a_later_one_re_attach_the_listener() -> None:
    async with harness() as h:
        await h.client._connect()  # EURUSD is held for the conversion chain
        held = HeldReplies(
            h.server,
            om.PROTO_OA_UNSUBSCRIBE_SPOTS_REQ,
            lambda r: oa.ProtoOAUnsubscribeSpotsRes(ctidTraderAccountId=r.ctidTraderAccountId),
        )

        cancelled = asyncio.create_task(
            h.client._release_spots(EURUSD_SYMBOL_ID, CONVERSION_CONSUMER),
        )
        await held.arrived.wait()
        cancelled.cancel()
        with pytest.raises(asyncio.CancelledError):
            await cancelled
        await held.stop_holding()

        # Another consumer comes and goes; with the cancelled one gone too, nobody is left.
        await subscribe_quotes(h, EURUSD_ID)
        await unsubscribe_quotes(h, EURUSD_ID)
        await push_spot(h, TWO_SIDED)

        assert not h.quotes()
        # The cancelled release is still on record, so a disconnect repeats its unsubscribe.
        assert (EURUSD_SYMBOL_ID, CONVERSION_CONSUMER) in h.client._all_holds()


async def test_disconnect_forgets_which_instruments_were_converted() -> None:
    async with harness() as h:
        await h.client._connect()

        await h.client._disconnect()

        assert not h.client._converted
