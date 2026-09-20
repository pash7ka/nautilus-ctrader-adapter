"""Tests for `CTraderDataClient`: connect, instruments, conversion quotes and quote ticks.

Bars belong to `test_bars.py` and to the bar half of the client, which is not covered here.

Everything runs against the fake server replaying `tests/fixtures/m2_recorded.json`. The
client's own `Logger` writes from Rust and is invisible to pytest, so log *text* is asserted
against the formatter that produces it and log *effects* against what the venue received.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass

import pytest
from nautilus_trader.cache.cache import Cache
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.data.engine import DataEngine
from nautilus_trader.data.messages import (
    RequestInstrument,
    RequestInstruments,
    SubscribeQuoteTicks,
    UnsubscribeQuoteTicks,
)
from nautilus_trader.model.currencies import EUR, USD
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.identifiers import InstrumentId, Symbol
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.test_kit.stubs.component import TestComponentStubs

from nautilus_ctrader.common.account import CTraderAccountClient
from nautilus_ctrader.common.errors import CTraderRequestError
from nautilus_ctrader.config import CTraderDataClientConfig, parse_asset_class_overrides
from nautilus_ctrader.constants import CTRADER_VENUE
from nautilus_ctrader.data import CTraderDataClient, _conversion_message
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as om
from nautilus_ctrader.providers import CTraderInstrumentProvider, InstrumentLoadError
from tests.account_venue import ACCOUNT_ID, RECORDED, account_client, received, venue
from tests.fake_server import FakeCTraderServer
from tests.polling import wait_until
from tests.recording_logger import RecordingLogger

EURUSD_ID = InstrumentId(Symbol("EURUSD"), CTRADER_VENUE)
GER40_ID = InstrumentId(Symbol("GER40.cash"), CTRADER_VENUE)
EURUSD_SYMBOL_ID = 1
GER40_SYMBOL_ID = 279

SPOTS = RECORDED["spot_events"]
# Recorded EURUSD spots carrying one side only, the second consistent with the first.
BID_ONLY = SPOTS[17]
ASK_ONLY = SPOTS[19]
TWO_SIDED = SPOTS[0]
GER40_SPOT = SPOTS[1]


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
