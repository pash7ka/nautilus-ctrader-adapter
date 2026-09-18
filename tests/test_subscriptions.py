"""The subscription registry: reference counts, reconnect restores and spot routing."""

import asyncio

import pytest
from google.protobuf.message import Message

from nautilus_ctrader.common.account import CTraderAccountClient
from nautilus_ctrader.common.errors import CTraderConnectionError, CTraderRequestError
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as oa_model
from tests.account_venue import RECORDED, account_client, for_account, received, venue
from tests.fake_server import FakeCTraderServer
from tests.polling import wait_until
from tests.recording_logger import RecordingLogger

EURUSD = 1
GBPUSD = 2
M15 = oa_model.M15

_SUBSCRIPTION_TYPES = (
    oa.ProtoOASubscribeSpotsReq,
    oa.ProtoOAUnsubscribeSpotsReq,
    oa.ProtoOASubscribeLiveTrendbarReq,
    oa.ProtoOAUnsubscribeLiveTrendbarReq,
)


def _error(request: Message, code: str) -> oa.ProtoOAErrorRes:
    return oa.ProtoOAErrorRes(ctidTraderAccountId=request.ctidTraderAccountId, errorCode=code)


def _subscription_venue() -> FakeCTraderServer:
    """The reference-data venue, also accepting every spot and trendbar (un)subscription."""
    server = venue()
    # Request type -> error code the venue answers it with.
    server.refuse = {}

    def reply(response_class: type[Message]):
        def handle(request: Message) -> Message:
            code = server.refuse.get(type(request))
            if code is not None:
                return _error(request, code)
            return response_class(ctidTraderAccountId=request.ctidTraderAccountId)

        return handle

    for payload_type, response_class in (
        (oa_model.PROTO_OA_SUBSCRIBE_SPOTS_REQ, oa.ProtoOASubscribeSpotsRes),
        (oa_model.PROTO_OA_UNSUBSCRIBE_SPOTS_REQ, oa.ProtoOAUnsubscribeSpotsRes),
        (oa_model.PROTO_OA_SUBSCRIBE_LIVE_TRENDBAR_REQ, oa.ProtoOASubscribeLiveTrendbarRes),
        (oa_model.PROTO_OA_UNSUBSCRIBE_LIVE_TRENDBAR_REQ, oa.ProtoOAUnsubscribeLiveTrendbarRes),
    ):
        server.on(payload_type, reply(response_class))
    return server


def _subscriptions(server: FakeCTraderServer) -> list[tuple]:
    """Every (un)subscription the server saw, as `(request name, symbol ids[, period])`."""
    seen = []
    for message in server.received:
        if isinstance(message, oa.ProtoOASubscribeSpotsReq | oa.ProtoOAUnsubscribeSpotsReq):
            seen.append((type(message).__name__, tuple(message.symbolId)))
        elif isinstance(message, _SUBSCRIPTION_TYPES):
            seen.append((type(message).__name__, (message.symbolId,), message.period))
    return seen


def _spot(symbol_id: int) -> oa.ProtoOASpotEvent:
    event = for_account(RECORDED["spot_events"][0], 1)
    event.symbolId = symbol_id
    return event


@pytest.fixture
async def server():
    server = _subscription_venue()
    await server.start()
    yield server
    await server.stop()


@pytest.fixture
def logger() -> RecordingLogger:
    return RecordingLogger()


@pytest.fixture
async def client(server: FakeCTraderServer, logger: RecordingLogger):
    client = account_client(server, logger=logger)
    await client.connect()
    yield client
    await client.disconnect()


async def test_two_consumers_share_one_venue_subscription(
    server: FakeCTraderServer,
    client: CTraderAccountClient,
) -> None:
    registry = client.subscriptions

    await registry.subscribe_spots(EURUSD, "a")
    await registry.subscribe_spots(EURUSD, "b")
    assert registry.consumers(EURUSD) == frozenset({"a", "b"})
    [request] = received(server, oa.ProtoOASubscribeSpotsReq)
    assert request.ctidTraderAccountId == client.account_id
    assert request.subscribeToSpotTimestamp is True

    await registry.unsubscribe_spots(EURUSD, "a")
    assert received(server, oa.ProtoOAUnsubscribeSpotsReq) == []

    await registry.unsubscribe_spots(EURUSD, "b")
    assert registry.consumers(EURUSD) == frozenset()
    assert _subscriptions(server) == [
        ("ProtoOASubscribeSpotsReq", (EURUSD,)),
        ("ProtoOAUnsubscribeSpotsReq", (EURUSD,)),
    ]


async def test_the_same_consumer_counts_once(
    server: FakeCTraderServer,
    client: CTraderAccountClient,
) -> None:
    registry = client.subscriptions

    await registry.subscribe_spots(EURUSD, "a")
    await registry.subscribe_spots(EURUSD, "a")
    await registry.unsubscribe_spots(EURUSD, "a")

    assert _subscriptions(server) == [
        ("ProtoOASubscribeSpotsReq", (EURUSD,)),
        ("ProtoOAUnsubscribeSpotsReq", (EURUSD,)),
    ]


async def test_concurrent_first_subscriptions_send_one_request(
    server: FakeCTraderServer,
    client: CTraderAccountClient,
) -> None:
    server.reply_delay_secs = 0.05

    await asyncio.gather(
        client.subscriptions.subscribe_spots(EURUSD, "a"),
        client.subscriptions.subscribe_spots(EURUSD, "b"),
    )

    assert len(received(server, oa.ProtoOASubscribeSpotsReq)) == 1
    assert client.subscriptions.consumers(EURUSD) == frozenset({"a", "b"})


async def test_trendbars_take_a_spot_reference_and_release_it_last(
    server: FakeCTraderServer,
    client: CTraderAccountClient,
) -> None:
    registry = client.subscriptions

    await registry.subscribe_trendbars(EURUSD, M15, "bars")
    assert registry.consumers(EURUSD) == frozenset({f"trendbar:{M15}:bars"})
    await registry.unsubscribe_trendbars(EURUSD, M15, "bars")

    assert registry.consumers(EURUSD) == frozenset()
    assert _subscriptions(server) == [
        ("ProtoOASubscribeSpotsReq", (EURUSD,)),
        ("ProtoOASubscribeLiveTrendbarReq", (EURUSD,), M15),
        ("ProtoOAUnsubscribeLiveTrendbarReq", (EURUSD,), M15),
        ("ProtoOAUnsubscribeSpotsReq", (EURUSD,)),
    ]


async def test_trendbar_release_keeps_spots_another_consumer_holds(
    server: FakeCTraderServer,
    client: CTraderAccountClient,
) -> None:
    registry = client.subscriptions
    await registry.subscribe_spots(EURUSD, "quotes")
    await registry.subscribe_trendbars(EURUSD, M15, "bars")

    await registry.unsubscribe_trendbars(EURUSD, M15, "bars")

    assert registry.consumers(EURUSD) == frozenset({"quotes"})
    assert received(server, oa.ProtoOAUnsubscribeSpotsReq) == []


async def test_already_subscribed_counts_as_success(
    server: FakeCTraderServer,
    client: CTraderAccountClient,
) -> None:
    server.refuse[oa.ProtoOASubscribeSpotsReq] = "ALREADY_SUBSCRIBED"
    server.refuse[oa.ProtoOASubscribeLiveTrendbarReq] = "ALREADY_SUBSCRIBED"

    await client.subscriptions.subscribe_trendbars(EURUSD, M15, "bars")

    assert client.subscriptions.consumers(EURUSD) == frozenset({f"trendbar:{M15}:bars"})


async def test_a_refused_subscription_is_not_counted(
    server: FakeCTraderServer,
    client: CTraderAccountClient,
) -> None:
    server.refuse[oa.ProtoOASubscribeSpotsReq] = "SYMBOL_NOT_FOUND"

    with pytest.raises(CTraderRequestError):
        await client.subscriptions.subscribe_spots(EURUSD, "a")

    assert client.subscriptions.consumers(EURUSD) == frozenset()
    assert client.session.failed_restores == frozenset()


async def test_a_refused_trendbar_releases_its_spot_reference(
    server: FakeCTraderServer,
    client: CTraderAccountClient,
) -> None:
    server.refuse[oa.ProtoOASubscribeLiveTrendbarReq] = "NOT_SUBSCRIBED_TO_SPOTS"

    with pytest.raises(CTraderRequestError):
        await client.subscriptions.subscribe_trendbars(EURUSD, M15, "bars")

    assert client.subscriptions.consumers(EURUSD) == frozenset()
    assert _subscriptions(server)[-1] == ("ProtoOAUnsubscribeSpotsReq", (EURUSD,))


async def test_a_failed_unsubscribe_still_drops_the_reference(
    server: FakeCTraderServer,
    client: CTraderAccountClient,
    logger: RecordingLogger,
) -> None:
    await client.subscriptions.subscribe_spots(EURUSD, "a")
    server.refuse[oa.ProtoOAUnsubscribeSpotsReq] = "NOT_SUBSCRIBED_TO_SPOTS"

    await client.subscriptions.unsubscribe_spots(EURUSD, "a")

    assert client.subscriptions.consumers(EURUSD) == frozenset()
    warnings = [m for level, m in logger.lines if level == "warning"]
    assert any("NOT_SUBSCRIBED_TO_SPOTS" in m for m in warnings)


async def test_subscribing_without_a_session_raises_and_counts_nothing(
    server: FakeCTraderServer,
    logger: RecordingLogger,
) -> None:
    client = account_client(server, logger=logger)

    with pytest.raises(CTraderConnectionError):
        await client.subscriptions.subscribe_spots(EURUSD, "a")
    with pytest.raises(CTraderConnectionError):
        await client.subscriptions.unsubscribe_spots(EURUSD, "a")

    assert client.subscriptions.consumers(EURUSD) == frozenset()
    assert _subscriptions(server) == []


async def test_live_keys_are_restored_after_a_reconnect(
    monkeypatch: pytest.MonkeyPatch,
    server: FakeCTraderServer,
    client: CTraderAccountClient,
) -> None:
    monkeypatch.setattr("nautilus_ctrader.common.session.STABLE_SESSION_SECS", 0.0)
    registry = client.subscriptions
    await registry.subscribe_spots(GBPUSD, "quotes")
    await registry.subscribe_trendbars(EURUSD, M15, "bars")
    server.received.clear()
    connections = server.connection_count

    await server.drop_connections()
    await wait_until(
        lambda: server.connection_count > connections and client.session.is_ready,
        description="session ready again",
    )

    restored = _subscriptions(server)
    assert ("ProtoOASubscribeSpotsReq", (GBPUSD,)) in restored
    trendbar = ("ProtoOASubscribeLiveTrendbarReq", (EURUSD,), M15)
    eurusd_spots = ("ProtoOASubscribeSpotsReq", (EURUSD,))
    assert trendbar in restored
    assert restored.index(eurusd_spots) < restored.index(trendbar)
    assert client.session.failed_restores == frozenset()


async def test_a_refused_restore_is_reported_and_the_others_still_work(
    monkeypatch: pytest.MonkeyPatch,
    server: FakeCTraderServer,
    client: CTraderAccountClient,
) -> None:
    monkeypatch.setattr("nautilus_ctrader.common.session.STABLE_SESSION_SECS", 0.0)
    registry = client.subscriptions
    await registry.subscribe_spots(GBPUSD, "quotes")
    await registry.subscribe_trendbars(EURUSD, M15, "bars")
    spots = []
    registry.add_spot_listener(GBPUSD, spots.append)
    server.refuse[oa.ProtoOASubscribeLiveTrendbarReq] = "TRADING_DISABLED"
    connections = server.connection_count

    await server.drop_connections()
    await wait_until(
        lambda: server.connection_count > connections and client.session.is_ready,
        description="session ready again",
    )

    assert client.session.failed_restores == frozenset({("trendbar", EURUSD, M15)})
    await server.push(_spot(GBPUSD))
    await wait_until(lambda: len(spots) == 1, description="spot delivered after reconnect")


async def test_restores_accept_already_subscribed(
    monkeypatch: pytest.MonkeyPatch,
    server: FakeCTraderServer,
    client: CTraderAccountClient,
) -> None:
    monkeypatch.setattr("nautilus_ctrader.common.session.STABLE_SESSION_SECS", 0.0)
    await client.subscriptions.subscribe_trendbars(EURUSD, M15, "bars")
    server.refuse[oa.ProtoOASubscribeSpotsReq] = "ALREADY_SUBSCRIBED"
    server.refuse[oa.ProtoOASubscribeLiveTrendbarReq] = "ALREADY_SUBSCRIBED"
    connections = server.connection_count

    await server.drop_connections()
    await wait_until(
        lambda: server.connection_count > connections and client.session.is_ready,
        description="session ready again",
    )

    assert client.session.failed_restores == frozenset()


async def test_live_keys_survive_a_disconnect_and_connect(
    server: FakeCTraderServer,
    client: CTraderAccountClient,
) -> None:
    await client.subscriptions.subscribe_trendbars(EURUSD, M15, "bars")
    await client.disconnect()
    server.received.clear()

    await client.connect()

    # The trendbar restore re-sends its spot subscription, whatever order the restores run in.
    assert _subscriptions(server) == [
        ("ProtoOASubscribeSpotsReq", (EURUSD,)),
        ("ProtoOASubscribeSpotsReq", (EURUSD,)),
        ("ProtoOASubscribeLiveTrendbarReq", (EURUSD,), M15),
    ]
    spots = []
    client.subscriptions.add_spot_listener(EURUSD, spots.append)
    await server.push(_spot(EURUSD))
    await wait_until(lambda: len(spots) == 1, description="spot delivered on the new session")


async def test_spots_reach_only_their_symbol_listener(
    server: FakeCTraderServer,
    client: CTraderAccountClient,
    logger: RecordingLogger,
) -> None:
    registry = client.subscriptions
    await registry.subscribe_spots(EURUSD, "a")
    eurusd, gbpusd = [], []
    registry.add_spot_listener(EURUSD, eurusd.append)
    registry.add_spot_listener(GBPUSD, gbpusd.append)

    await server.push(_spot(EURUSD))
    await wait_until(lambda: len(eurusd) == 1, description="spot delivered")

    assert eurusd[0].symbolId == EURUSD
    assert gbpusd == []


async def test_a_spot_with_no_listener_is_dropped_at_debug(
    server: FakeCTraderServer,
    client: CTraderAccountClient,
    logger: RecordingLogger,
) -> None:
    listened = []
    client.subscriptions.add_spot_listener(EURUSD, listened.append)
    client.subscriptions.remove_spot_listener(EURUSD, listened.append)
    logged_before = len(logger.lines)

    await server.push(_spot(EURUSD))
    await wait_until(
        lambda: any("no listener" in m for _l, m in logger.lines[logged_before:]),
        description="dropped spot logged",
    )

    assert listened == []
    assert {level for level, _m in logger.lines[logged_before:]} == {"debug"}
