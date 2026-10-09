"""A failed data request: one `CTraderRequestFailed`, then one empty response.

Every test drives the request the way a strategy does - `Actor.request_*` with a callback -
through a real `DataEngine` or `LiveDataEngine`, so what is asserted is what the strategy sees:
the failure message, then the callback, and no historical data.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import timedelta

import pytest
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.actor import Actor
from nautilus_trader.common.component import LiveClock, MessageBus
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.data.engine import DataEngine
from nautilus_trader.live.data_engine import LiveDataEngine
from nautilus_trader.model.data import Bar, BarType, DataType, QuoteTick
from nautilus_trader.model.identifiers import ClientId, InstrumentId, Symbol
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.portfolio.portfolio import Portfolio
from nautilus_trader.test_kit.stubs.identifiers import TestIdStubs

from nautilus_ctrader import REQUEST_FAILED_TOPIC, CTraderRequestFailed
from nautilus_ctrader.config import CTraderDataClientConfig, parse_asset_class_overrides
from nautilus_ctrader.constants import CTRADER_VENUE
from nautilus_ctrader.data import CTraderDataClient
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as om
from tests.account_venue import HeldReplies, account_client, received
from tests.fake_server import FakeCTraderServer
from tests.polling import wait_until
from tests.recording_logger import RecordingLogger
from tests.test_data_client import (
    EURUSD_H1,
    EURUSD_ID,
    EURUSD_LAST,
    LAST_H1_MINUTE,
    at_minute,
    close_ns,
    config,
    trendbar_venue,
)

USDJPY_ID = InstrumentId(Symbol("USDJPY"), CTRADER_VENUE)
# The fake venue's account carries no USDJPY, so nothing loads it.
USDJPY_H1 = BarType.from_str(f"{USDJPY_ID}-1-HOUR-BID-EXTERNAL")
REFUSAL = "INVALID_REQUEST"


class Requester(Actor):
    """Records, in order, what a strategy observes of its requests."""

    def __init__(self) -> None:
        super().__init__()
        self.events: list[tuple[str, object]] = []

    def on_start(self) -> None:
        self.msgbus.subscribe(topic=REQUEST_FAILED_TOPIC, handler=self.on_request_failed)

    def on_request_failed(self, failure: CTraderRequestFailed) -> None:
        self.events.append(("failed", failure))

    def on_historical_data(self, data) -> None:
        self.events.append(("data", data))

    def on_answered(self, request_id: UUID4) -> None:
        self.events.append(("callback", request_id))

    def of(self, kind: str) -> list:
        return [event for k, event in self.events if k == kind]


@dataclass
class Node:
    server: FakeCTraderServer
    client: CTraderDataClient
    msgbus: MessageBus
    clock: LiveClock
    actor: Requester
    # Every response the client sent the engine, whatever the engine then made of it.
    responses: list

    async def answered(self) -> None:
        await wait_until(lambda: self.actor.of("callback"), description="the callback fired")
        await wait_until(
            lambda: all(task.done() for task in self.client._tasks),
            description="the client's request task finished",
        )

    def assert_failed_once(self, request_id: UUID4) -> CTraderRequestFailed:
        """The failure, then the callback, each once, with nothing historical and one response."""
        assert [kind for kind, _ in self.actor.events] == ["failed", "callback"]
        failure = self.actor.of("failed")[0]
        assert failure.request_id == request_id
        assert self.actor.of("callback") == [request_id]
        assert len(self.responses) == 1
        assert self.responses[0].data == []
        return failure


@asynccontextmanager
async def node(
    *,
    live: bool = True,
    server: FakeCTraderServer | None = None,
    client_config: CTraderDataClientConfig | None = None,
) -> AsyncIterator[Node]:
    client_config = client_config or config()
    server = server or trendbar_venue()
    await server.start()
    logger = RecordingLogger()
    account = account_client(server, logger=logger, credentials=client_config.credentials())
    provider = account.get_instrument_provider(
        config=client_config.instrument_provider,
        asset_class_overrides=parse_asset_class_overrides(client_config.asset_class_overrides),
        fail_on_instrument_error=client_config.fail_on_instrument_error,
        logger=logger,
    )
    loop = asyncio.get_running_loop()
    clock = LiveClock()
    msgbus = MessageBus(trader_id=TestIdStubs.trader_id(), clock=clock)
    cache = Cache(database=None)
    engine = (
        LiveDataEngine(loop, msgbus, cache, clock) if live else DataEngine(msgbus, cache, clock)
    )
    responses: list = []

    def respond(response) -> None:
        responses.append(response)
        engine.response(response)

    msgbus.deregister(endpoint="DataEngine.response", handler=engine.response)
    msgbus.register(endpoint="DataEngine.response", handler=respond)
    client = CTraderDataClient(
        loop=loop,
        account=account,
        msgbus=msgbus,
        cache=cache,
        clock=clock,
        instrument_provider=provider,
        config=client_config,
    )
    engine.register_client(client)
    engine.start()
    actor = Requester()
    actor.register_base(Portfolio(msgbus, cache, clock), msgbus, cache, clock)
    actor.start()
    await client._connect()
    try:
        yield Node(server, client, msgbus, clock, actor, responses)
    finally:
        actor.stop()
        if live:
            engine.kill()
        else:
            engine.stop()
        await account.disconnect()
        await server.stop()


def refusing_history(server: FakeCTraderServer, *, answer_first: int = 0) -> FakeCTraderServer:
    """`server`, refusing every trendbar request after the first `answer_first`."""
    serve = server._handlers[om.PROTO_OA_GET_TRENDBARS_REQ]
    answered = 0

    def reply(request: oa.ProtoOAGetTrendbarsReq):
        nonlocal answered
        if answered < answer_first:
            answered += 1
            return serve(request)
        return oa.ProtoOAErrorRes(
            ctidTraderAccountId=request.ctidTraderAccountId,
            errorCode=REFUSAL,
            description="a venue description that must not reach the reason",
        )

    server.on(om.PROTO_OA_GET_TRENDBARS_REQ, reply)
    return server


def bars_data_type(bar_type: BarType) -> DataType:
    return DataType(Bar, metadata={"bar_type": bar_type})


# -- The requester's id ---------------------------------------------------------------------


@pytest.mark.parametrize("live", [True, False], ids=["LiveDataEngine", "DataEngine"])
@pytest.mark.parametrize("window", ["start and limit", "start and end"])
async def test_the_failure_carries_the_id_the_strategy_callback_receives(
    live: bool,
    window: str,
) -> None:
    async with node(live=live, server=refusing_history(trendbar_venue())) as n:
        now = n.clock.utc_now()
        # A warm-up of 500 bars, reaching back three times as far as they span.
        start = now - 3 * 500 * timedelta(hours=1)
        if window == "start and limit":
            request_id = n.actor.request_bars(
                EURUSD_H1,
                start=start,
                limit=500,
                callback=n.actor.on_answered,
            )
        else:
            request_id = n.actor.request_bars(
                EURUSD_H1,
                start=start,
                end=now,
                callback=n.actor.on_answered,
            )
        await n.answered()

        failure = n.assert_failed_once(request_id)
        # The client was asked through a request of the engine's own, not the strategy's.
        assert n.responses[0].correlation_id != request_id
        assert failure.reason == f"venue refused: {REFUSAL}"
        assert failure.data_type == bars_data_type(EURUSD_H1)
        assert failure.data_type.metadata["bar_type"] == EURUSD_H1


# -- Every failure of a bar request ---------------------------------------------------------


async def test_a_bar_type_with_no_trendbar_period_fails() -> None:
    async with node() as n:
        start = n.clock.utc_now() - timedelta(hours=1)
        request_id = n.actor.request_bars(EURUSD_LAST, start=start, callback=n.actor.on_answered)
        await n.answered()

        failure = n.assert_failed_once(request_id)
        assert failure.reason == f"no trendbar period for {EURUSD_LAST}"
        assert not received(n.server, oa.ProtoOAGetTrendbarsReq)


async def test_bars_of_an_instrument_that_is_not_loaded_fail() -> None:
    async with node() as n:
        start = n.clock.utc_now() - timedelta(hours=1)
        request_id = n.actor.request_bars(USDJPY_H1, start=start, callback=n.actor.on_answered)
        await n.answered()

        failure = n.assert_failed_once(request_id)
        assert failure.reason == f"instrument {USDJPY_ID} is not loaded"
        assert failure.data_type == bars_data_type(USDJPY_H1)


async def test_a_bar_request_the_venue_does_not_answer_times_out() -> None:
    server = trendbar_venue()
    server.on(om.PROTO_OA_GET_TRENDBARS_REQ, lambda _r: None)
    async with node(server=server, client_config=config(history_request_timeout_secs=0.2)) as n:
        start = n.clock.utc_now() - timedelta(hours=1)
        request_id = n.actor.request_bars(EURUSD_H1, start=start, callback=n.actor.on_answered)
        await n.answered()

        assert n.assert_failed_once(request_id).reason == "timed out"


async def test_a_bar_request_whose_connection_drops_fails() -> None:
    async with node() as n:
        n.server.close_after_next_request = True
        start = n.clock.utc_now() - timedelta(hours=1)
        request_id = n.actor.request_bars(EURUSD_H1, start=start, callback=n.actor.on_answered)
        await n.answered()

        assert n.assert_failed_once(request_id).reason == "connection lost"


async def test_a_bar_request_cancelled_at_disconnect_fails() -> None:
    server = trendbar_venue()
    held = HeldReplies(server, om.PROTO_OA_GET_TRENDBARS_REQ, lambda _r: None)
    async with node(server=server) as n:
        start = n.clock.utc_now() - timedelta(hours=1)
        request_id = n.actor.request_bars(EURUSD_H1, start=start, callback=n.actor.on_answered)
        await asyncio.wait_for(held.arrived.wait(), timeout=3.0)

        await n.client.cancel_pending_tasks()
        await n.answered()

        assert n.assert_failed_once(request_id).reason == "connection closed"


async def test_a_bar_request_in_flight_when_the_client_disconnects_fails() -> None:
    server = trendbar_venue()
    held = HeldReplies(server, om.PROTO_OA_GET_TRENDBARS_REQ, lambda _r: None)
    async with node(server=server) as n:
        start = n.clock.utc_now() - timedelta(hours=1)
        request_id = n.actor.request_bars(EURUSD_H1, start=start, callback=n.actor.on_answered)
        await asyncio.wait_for(held.arrived.wait(), timeout=3.0)

        n.client.disconnect()
        await n.answered()

        # Failed by the closing socket, before Nautilus cancels the client's tasks.
        assert n.assert_failed_once(request_id).reason == "connection closed"


async def test_a_failure_after_a_page_was_served_delivers_no_bar() -> None:
    server = refusing_history(trendbar_venue(), answer_first=1)
    async with node(server=server, client_config=config(history_page_size=20)) as n:
        request_id = n.actor.request_bars(
            EURUSD_H1,
            start=at_minute(LAST_H1_MINUTE - 60 * 40),
            end=at_minute(LAST_H1_MINUTE + 60),
            callback=n.actor.on_answered,
        )
        await n.answered()

        assert n.assert_failed_once(request_id).reason == f"venue refused: {REFUSAL}"
        assert len(received(n.server, oa.ProtoOAGetTrendbarsReq)) == 2


async def test_a_raising_failure_subscriber_does_not_stop_the_response() -> None:
    async with node() as n:

        def broken(_failure: CTraderRequestFailed) -> None:
            raise RuntimeError("subscriber bug")

        # Ahead of the strategy's own handler, so the publish is cut short before it.
        n.msgbus.subscribe(topic=REQUEST_FAILED_TOPIC, handler=broken, priority=10)
        start = n.clock.utc_now() - timedelta(hours=1)
        request_id = n.actor.request_bars(EURUSD_LAST, start=start, callback=n.actor.on_answered)
        await n.answered()

        assert n.actor.of("callback") == [request_id]
        assert not n.actor.of("data")
        assert len(n.responses) == 1


# -- Success --------------------------------------------------------------------------------


@pytest.mark.parametrize("live", [True, False], ids=["LiveDataEngine", "DataEngine"])
async def test_a_served_bar_request_publishes_no_failure(live: bool) -> None:
    async with node(live=live) as n:
        request_id = n.actor.request_bars(
            EURUSD_H1,
            start=at_minute(LAST_H1_MINUTE - 120),
            end=at_minute(LAST_H1_MINUTE + 60),
            callback=n.actor.on_answered,
        )
        await n.answered()

        assert [kind for kind, _ in n.actor.events] == ["data", "data", "data", "callback"]
        assert [bar.ts_event for bar in n.actor.of("data")] == [
            close_ns(LAST_H1_MINUTE - 120, 3600),
            close_ns(LAST_H1_MINUTE - 60, 3600),
            close_ns(LAST_H1_MINUTE, 3600),
        ]
        assert n.actor.of("callback") == [request_id]
        assert len(n.responses) == 1


# -- Other requests -------------------------------------------------------------------------


async def test_an_instrument_that_is_not_loaded_fails() -> None:
    async with node() as n:
        request_id = n.actor.request_instrument(USDJPY_ID, callback=n.actor.on_answered)
        await n.answered()

        failure = n.assert_failed_once(request_id)
        assert failure.reason == f"instrument {USDJPY_ID} is not loaded"
        assert failure.data_type == DataType(Instrument, metadata={"instrument_id": USDJPY_ID})


def _ask(n: Node, what: str) -> UUID4:
    start = n.clock.utc_now() - timedelta(hours=1)
    callback = n.actor.on_answered
    actor = n.actor
    requests: dict[str, Callable[[], UUID4]] = {
        "quote ticks": lambda: actor.request_quote_ticks(EURUSD_ID, start, callback=callback),
        "trade ticks": lambda: actor.request_trade_ticks(EURUSD_ID, start, callback=callback),
        "funding rates": lambda: actor.request_funding_rates(EURUSD_ID, start, callback=callback),
        "order book deltas": lambda: actor.request_order_book_deltas(
            EURUSD_ID,
            start,
            callback=callback,
        ),
        "order book depth": lambda: actor.request_order_book_depth(
            EURUSD_ID,
            start,
            callback=callback,
        ),
        "order book snapshot": lambda: actor.request_order_book_snapshot(
            EURUSD_ID,
            callback=callback,
        ),
        "custom data": lambda: actor.request_data(
            DataType(QuoteTick, metadata={"instrument_id": EURUSD_ID}),
            ClientId(CTRADER_VENUE.value),
            start=start,
            callback=callback,
        ),
    }
    return requests[what]()


@pytest.mark.parametrize(
    "what",
    [
        "quote ticks",
        "trade ticks",
        "funding rates",
        "order book deltas",
        "order book depth",
        "order book snapshot",
        "custom data",
    ],
)
async def test_a_request_the_venue_has_no_history_for_fails(what: str) -> None:
    async with node() as n:
        request_id = _ask(n, what)
        await n.answered()

        failure = n.assert_failed_once(request_id)
        assert failure.reason.endswith("requests are not supported")
