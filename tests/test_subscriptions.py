"""The subscription registry: reference counts, reconnect restores and spot routing."""

import asyncio
import functools

import pytest
from google.protobuf.message import Message

from nautilus_ctrader.common import account as account_module
from nautilus_ctrader.common.account import CTraderAccountClient
from nautilus_ctrader.common.errors import CTraderProtocolError, CTraderRequestError
from nautilus_ctrader.common.subscriptions import SubscriptionRegistry
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as oa_model
from tests.account_venue import (
    RECORDED,
    HeldReplies,
    account_client,
    for_account,
    hold_account_auth,
    received,
    venue,
)
from tests.fake_server import FakeCTraderServer
from tests.polling import wait_until
from tests.recording_logger import RecordingLogger

EURUSD = 1
GBPUSD = 2
# Pushed last to prove every earlier spot has been dispatched.
BARRIER = 41
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


async def _pass_barrier(server: FakeCTraderServer, client: CTraderAccountClient) -> None:
    """Return once a spot pushed now is delivered, so every spot pushed before it is too."""
    arrived = []
    client.subscriptions.add_spot_listener(BARRIER, arrived.append)
    await server.push(_spot(BARRIER))
    await wait_until(lambda: arrived, description="barrier spot delivered")
    client.subscriptions.remove_spot_listener(BARRIER, arrived.append)


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

    await registry.subscribe_spots(EURUSD, "a", "data")
    await registry.subscribe_spots(EURUSD, "b", "data")
    assert registry.consumers(EURUSD) == frozenset({"a", "b"})
    [request] = received(server, oa.ProtoOASubscribeSpotsReq)
    assert request.ctidTraderAccountId == client.account_id
    assert request.subscribeToSpotTimestamp is True

    await registry.unsubscribe_spots(EURUSD, "a", "data")
    assert received(server, oa.ProtoOAUnsubscribeSpotsReq) == []

    await registry.unsubscribe_spots(EURUSD, "b", "data")
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

    await registry.subscribe_spots(EURUSD, "a", "data")
    await registry.subscribe_spots(EURUSD, "a", "data")
    await registry.unsubscribe_spots(EURUSD, "a", "data")

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
        client.subscriptions.subscribe_spots(EURUSD, "a", "data"),
        client.subscriptions.subscribe_spots(EURUSD, "b", "data"),
    )

    assert len(received(server, oa.ProtoOASubscribeSpotsReq)) == 1
    assert client.subscriptions.consumers(EURUSD) == frozenset({"a", "b"})
    await asyncio.sleep(0.1)
    assert len(received(server, oa.ProtoOASubscribeSpotsReq)) == 1


async def test_trendbars_take_a_spot_reference_and_release_it_last(
    server: FakeCTraderServer,
    client: CTraderAccountClient,
) -> None:
    registry = client.subscriptions

    await registry.subscribe_trendbars(EURUSD, M15, "bars", "data")
    assert registry.consumers(EURUSD) == frozenset({f"trendbar:{M15}:bars"})
    await registry.unsubscribe_trendbars(EURUSD, M15, "bars", "data")

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
    await registry.subscribe_spots(EURUSD, "quotes", "data")
    await registry.subscribe_trendbars(EURUSD, M15, "bars", "data")

    await registry.unsubscribe_trendbars(EURUSD, M15, "bars", "data")

    assert registry.consumers(EURUSD) == frozenset({"quotes"})
    assert received(server, oa.ProtoOAUnsubscribeSpotsReq) == []


async def test_already_subscribed_counts_as_success(
    server: FakeCTraderServer,
    client: CTraderAccountClient,
) -> None:
    server.refuse[oa.ProtoOASubscribeSpotsReq] = "ALREADY_SUBSCRIBED"
    server.refuse[oa.ProtoOASubscribeLiveTrendbarReq] = "ALREADY_SUBSCRIBED"

    await client.subscriptions.subscribe_trendbars(EURUSD, M15, "bars", "data")

    assert client.subscriptions.consumers(EURUSD) == frozenset({f"trendbar:{M15}:bars"})


async def test_a_refused_subscription_is_not_counted(
    server: FakeCTraderServer,
    client: CTraderAccountClient,
) -> None:
    server.refuse[oa.ProtoOASubscribeSpotsReq] = "SYMBOL_NOT_FOUND"

    with pytest.raises(CTraderRequestError):
        await client.subscriptions.subscribe_spots(EURUSD, "a", "data")

    assert client.subscriptions.consumers(EURUSD) == frozenset()
    assert client.session.failed_restores == frozenset()


async def test_a_refused_trendbar_releases_its_spot_reference(
    server: FakeCTraderServer,
    client: CTraderAccountClient,
) -> None:
    server.refuse[oa.ProtoOASubscribeLiveTrendbarReq] = "NOT_SUBSCRIBED_TO_SPOTS"

    with pytest.raises(CTraderRequestError):
        await client.subscriptions.subscribe_trendbars(EURUSD, M15, "bars", "data")

    assert client.subscriptions.consumers(EURUSD) == frozenset()
    assert _subscriptions(server)[-1] == ("ProtoOAUnsubscribeSpotsReq", (EURUSD,))


async def test_a_failed_unsubscribe_still_drops_the_reference(
    server: FakeCTraderServer,
    client: CTraderAccountClient,
    logger: RecordingLogger,
) -> None:
    await client.subscriptions.subscribe_spots(EURUSD, "a", "data")
    server.refuse[oa.ProtoOAUnsubscribeSpotsReq] = "NOT_SUBSCRIBED_TO_SPOTS"

    await client.subscriptions.unsubscribe_spots(EURUSD, "a", "data")

    assert client.subscriptions.consumers(EURUSD) == frozenset()
    warnings = [m for level, m in logger.lines if level == "warning"]
    assert any("NOT_SUBSCRIBED_TO_SPOTS" in m for m in warnings)


async def test_subscriptions_while_detached_are_recorded_and_restored_on_connect(
    server: FakeCTraderServer,
    logger: RecordingLogger,
) -> None:
    client = account_client(server, logger=logger)
    registry = client.subscriptions

    await registry.subscribe_spots(EURUSD, "a", "data")
    await registry.subscribe_spots(GBPUSD, "b", "data")
    assert registry.consumers(EURUSD) == frozenset({"a"})
    assert _subscriptions(server) == []

    await client.connect()
    try:
        assert _subscriptions(server) == [
            ("ProtoOASubscribeSpotsReq", (EURUSD,)),
            ("ProtoOASubscribeSpotsReq", (GBPUSD,)),
        ]
        await client.disconnect()

        await registry.unsubscribe_spots(EURUSD, "a", "data")
        assert registry.consumers(EURUSD) == frozenset()
        server.received.clear()

        await client.connect()
        assert _subscriptions(server) == [("ProtoOASubscribeSpotsReq", (GBPUSD,))]
    finally:
        await client.disconnect()


async def _reconnect_held(server: FakeCTraderServer) -> HeldReplies:
    """Drop the connection and hold the next bring-up in account authentication."""
    held = hold_account_auth(server)
    await server.drop_connections()
    await asyncio.wait_for(held.arrived.wait(), 2.0)
    return held


async def test_a_subscribe_while_reconnecting_is_sent_by_the_next_bring_up(
    monkeypatch: pytest.MonkeyPatch,
    server: FakeCTraderServer,
    client: CTraderAccountClient,
) -> None:
    monkeypatch.setattr("nautilus_ctrader.common.session.STABLE_SESSION_SECS", 0.0)
    held = await _reconnect_held(server)

    await client.subscriptions.subscribe_trendbars(EURUSD, M15, "bars", "data")
    assert client.subscriptions.consumers(EURUSD) == frozenset({f"trendbar:{M15}:bars"})
    assert _subscriptions(server) == []

    await held.stop_holding()
    await wait_until(lambda: client.session.is_ready, description="session ready again")
    assert ("ProtoOASubscribeLiveTrendbarReq", (EURUSD,), M15) in _subscriptions(server)
    assert client.session.failed_restores == frozenset()


async def test_an_unsubscribe_while_reconnecting_is_not_restored(
    monkeypatch: pytest.MonkeyPatch,
    server: FakeCTraderServer,
    client: CTraderAccountClient,
) -> None:
    monkeypatch.setattr("nautilus_ctrader.common.session.STABLE_SESSION_SECS", 0.0)
    await client.subscriptions.subscribe_trendbars(EURUSD, M15, "bars", "data")
    held = await _reconnect_held(server)
    server.received.clear()

    await client.subscriptions.unsubscribe_trendbars(EURUSD, M15, "bars", "data")
    assert client.subscriptions.consumers(EURUSD) == frozenset()

    await held.stop_holding()
    await wait_until(lambda: client.session.is_ready, description="session ready again")
    assert _subscriptions(server) == []


async def test_a_restore_racing_the_last_unsubscribe_leaves_no_subscription(
    monkeypatch: pytest.MonkeyPatch,
    server: FakeCTraderServer,
    client: CTraderAccountClient,
    logger: RecordingLogger,
) -> None:
    monkeypatch.setattr("nautilus_ctrader.common.session.STABLE_SESSION_SECS", 0.0)
    registry = client.subscriptions
    await registry.subscribe_trendbars(EURUSD, M15, "bars", "data")
    # The spots restore passes; the trendbar restore's own spots request is held.
    held = HeldReplies(
        server,
        oa_model.PROTO_OA_SUBSCRIBE_SPOTS_REQ,
        lambda r: oa.ProtoOASubscribeSpotsRes(ctidTraderAccountId=r.ctidTraderAccountId),
        answer_first=1,
    )
    # The new connection never had them, so the venue refuses the unsubscribes.
    server.refuse[oa.ProtoOAUnsubscribeLiveTrendbarReq] = "NOT_SUBSCRIBED_TO_SPOTS"
    server.refuse[oa.ProtoOAUnsubscribeSpotsReq] = "NOT_SUBSCRIBED_TO_SPOTS"
    server.received.clear()
    await server.drop_connections()
    await asyncio.wait_for(held.arrived.wait(), 2.0)

    unsubscribing = asyncio.create_task(registry.unsubscribe_trendbars(EURUSD, M15, "bars", "data"))
    # One tick suffices: the task blocks on the key lock at its first statement.
    await asyncio.sleep(0)
    assert not unsubscribing.done()
    await held.stop_holding()
    await unsubscribing
    await wait_until(lambda: client.session.is_ready, description="session ready again")

    assert registry.consumers(EURUSD) == frozenset()
    sent = _subscriptions(server)
    assert ("ProtoOASubscribeLiveTrendbarReq", (EURUSD,), M15) not in sent
    assert sent[-1] == ("ProtoOAUnsubscribeSpotsReq", (EURUSD,))
    assert [m for level, m in logger.lines if level == "warning" and "Unsubscribe" in m] == []


async def test_interleaved_trendbar_unsubscribe_and_subscribe_end_subscribed(
    server: FakeCTraderServer,
    client: CTraderAccountClient,
) -> None:
    registry = client.subscriptions
    await registry.subscribe_trendbars(EURUSD, M15, "bars", "data")
    server.reply_delay_secs = 0.02

    await asyncio.gather(
        registry.unsubscribe_trendbars(EURUSD, M15, "bars", "data"),
        registry.subscribe_trendbars(EURUSD, M15, "bars", "data"),
    )

    assert registry.consumers(EURUSD) == frozenset({f"trendbar:{M15}:bars"})
    sent = [entry[0] for entry in _subscriptions(server)]
    assert [n for n in sent if "Trendbar" in n][-1] == "ProtoOASubscribeLiveTrendbarReq"
    assert [n for n in sent if "Spots" in n][-1] == "ProtoOASubscribeSpotsReq"


@pytest.mark.parametrize("outcome", ["cancelled", "timed out"])
async def test_an_unknown_trendbar_outcome_is_recorded_and_cleaned_up_by_unsubscribe(
    monkeypatch: pytest.MonkeyPatch,
    server: FakeCTraderServer,
    logger: RecordingLogger,
    outcome: str,
) -> None:
    monkeypatch.setattr(
        account_module,
        "CTraderSession",
        functools.partial(account_module.CTraderSession, request_timeout_secs=0.2),
    )
    HeldReplies(
        server,
        oa_model.PROTO_OA_SUBSCRIBE_LIVE_TRENDBAR_REQ,
        lambda r: oa.ProtoOASubscribeLiveTrendbarRes(ctidTraderAccountId=r.ctidTraderAccountId),
    )
    client = account_client(server, logger=logger)
    await client.connect()
    try:
        registry = client.subscriptions
        subscribing = asyncio.create_task(registry.subscribe_trendbars(EURUSD, M15, "bars", "data"))
        await wait_until(
            lambda: bool(received(server, oa.ProtoOASubscribeLiveTrendbarReq)),
            description="trendbar request sent",
        )
        if outcome == "cancelled":
            subscribing.cancel()
            with pytest.raises(asyncio.CancelledError):
                await subscribing
        else:
            await subscribing
        assert ("trendbar", EURUSD, M15) in client.session.failed_restores

        await registry.unsubscribe_trendbars(EURUSD, M15, "bars", "data")

        assert registry.consumers(EURUSD) == frozenset()
        sent = [entry[0] for entry in _subscriptions(server)]
        assert [n for n in sent if "Trendbar" in n][-1] == "ProtoOAUnsubscribeLiveTrendbarReq"
        assert [n for n in sent if "Spots" in n][-1] == "ProtoOAUnsubscribeSpotsReq"
        assert client.session.failed_restores == frozenset()
    finally:
        await client.disconnect()


async def test_a_timed_out_subscribe_is_retried_by_the_restore_retry(
    monkeypatch: pytest.MonkeyPatch,
    server: FakeCTraderServer,
    logger: RecordingLogger,
) -> None:
    monkeypatch.setattr(
        account_module,
        "CTraderSession",
        functools.partial(account_module.CTraderSession, request_timeout_secs=0.2),
    )
    held = HeldReplies(
        server,
        oa_model.PROTO_OA_SUBSCRIBE_LIVE_TRENDBAR_REQ,
        lambda r: oa.ProtoOASubscribeLiveTrendbarRes(ctidTraderAccountId=r.ctidTraderAccountId),
    )
    client = account_client(server, logger=logger)
    await client.connect()
    try:
        await client.subscriptions.subscribe_trendbars(EURUSD, M15, "bars", "data")
        assert client.session.failed_restores == frozenset({("trendbar", EURUSD, M15)})

        await held.stop_holding()
        await client.session.retry_failed_restores()

        assert client.session.failed_restores == frozenset()
        assert len(received(server, oa.ProtoOASubscribeLiveTrendbarReq)) == 2
    finally:
        await client.disconnect()


async def test_a_cancelled_unsubscribe_keeps_the_reference_and_a_repeat_re_sends_it(
    server: FakeCTraderServer,
    client: CTraderAccountClient,
) -> None:
    registry = client.subscriptions
    await registry.subscribe_spots(EURUSD, "a", "data")
    held = HeldReplies(
        server,
        oa_model.PROTO_OA_UNSUBSCRIBE_SPOTS_REQ,
        lambda r: oa.ProtoOAUnsubscribeSpotsRes(ctidTraderAccountId=r.ctidTraderAccountId),
    )

    releasing = asyncio.create_task(registry.unsubscribe_spots(EURUSD, "a", "data"))
    await asyncio.wait_for(held.arrived.wait(), 2.0)
    releasing.cancel()
    with pytest.raises(asyncio.CancelledError):
        await releasing

    # The venue may or may not have processed it, so the reference stays until one does.
    assert registry.consumers(EURUSD) == frozenset({"a"})

    await held.stop_holding()
    await registry.unsubscribe_spots(EURUSD, "a", "data")

    assert registry.consumers(EURUSD) == frozenset()
    assert len(received(server, oa.ProtoOAUnsubscribeSpotsReq)) == 2


async def test_a_cancelled_spots_leg_is_released_by_unsubscribe_trendbars(
    server: FakeCTraderServer,
    client: CTraderAccountClient,
) -> None:
    held = HeldReplies(
        server,
        oa_model.PROTO_OA_SUBSCRIBE_SPOTS_REQ,
        lambda r: oa.ProtoOASubscribeSpotsRes(ctidTraderAccountId=r.ctidTraderAccountId),
    )
    registry = client.subscriptions
    subscribing = asyncio.create_task(registry.subscribe_trendbars(EURUSD, M15, "bars", "data"))
    await asyncio.wait_for(held.arrived.wait(), 2.0)
    subscribing.cancel()
    with pytest.raises(asyncio.CancelledError):
        await subscribing

    await registry.unsubscribe_trendbars(EURUSD, M15, "bars", "data")

    assert registry.consumers(EURUSD) == frozenset()
    assert client.session.failed_restores == frozenset()
    sent = _subscriptions(server)
    assert ("ProtoOASubscribeLiveTrendbarReq", (EURUSD,), M15) not in sent
    assert sent[-1] == ("ProtoOAUnsubscribeSpotsReq", (EURUSD,))


async def test_a_loss_surfacing_as_another_error_records_the_intent(
    monkeypatch: pytest.MonkeyPatch,
    server: FakeCTraderServer,
    client: CTraderAccountClient,
) -> None:
    monkeypatch.setattr("nautilus_ctrader.common.session.STABLE_SESSION_SECS", 0.0)
    session = client.session
    real_request = session.request
    lost: list[HeldReplies] = []

    async def lost_on_first_subscribe(payload, **kwargs):
        if isinstance(payload, oa.ProtoOASubscribeSpotsReq) and not lost:
            lost.append(await _reconnect_held(server))
            raise CTraderProtocolError("frame cut short")
        return await real_request(payload, **kwargs)

    monkeypatch.setattr(session, "request", lost_on_first_subscribe)
    await client.subscriptions.subscribe_spots(EURUSD, "a", "data")

    assert client.subscriptions.consumers(EURUSD) == frozenset({"a"})
    await lost[0].stop_holding()
    await wait_until(
        lambda: bool(received(server, oa.ProtoOASubscribeSpotsReq)) and session.is_ready,
        description="subscription restored",
    )


async def test_live_keys_are_restored_after_a_reconnect(
    monkeypatch: pytest.MonkeyPatch,
    server: FakeCTraderServer,
    client: CTraderAccountClient,
) -> None:
    monkeypatch.setattr("nautilus_ctrader.common.session.STABLE_SESSION_SECS", 0.0)
    registry = client.subscriptions
    await registry.subscribe_spots(GBPUSD, "quotes", "data")
    await registry.subscribe_trendbars(EURUSD, M15, "bars", "data")
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
    await registry.subscribe_spots(GBPUSD, "quotes", "data")
    await registry.subscribe_trendbars(EURUSD, M15, "bars", "data")
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
    await _pass_barrier(server, client)
    assert len(spots) == 1


async def test_restores_accept_already_subscribed(
    monkeypatch: pytest.MonkeyPatch,
    server: FakeCTraderServer,
    client: CTraderAccountClient,
) -> None:
    monkeypatch.setattr("nautilus_ctrader.common.session.STABLE_SESSION_SECS", 0.0)
    await client.subscriptions.subscribe_trendbars(EURUSD, M15, "bars", "data")
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
    await client.subscriptions.subscribe_trendbars(EURUSD, M15, "bars", "data")
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
    await _pass_barrier(server, client)
    assert len(spots) == 1


async def test_spots_reach_only_their_symbol_listener(
    server: FakeCTraderServer,
    client: CTraderAccountClient,
    logger: RecordingLogger,
) -> None:
    registry = client.subscriptions
    await registry.subscribe_spots(EURUSD, "a", "data")
    eurusd, gbpusd = [], []
    registry.add_spot_listener(EURUSD, eurusd.append)
    registry.add_spot_listener(GBPUSD, gbpusd.append)

    await server.push(_spot(EURUSD))
    await wait_until(lambda: len(eurusd) == 1, description="spot delivered")
    await _pass_barrier(server, client)
    assert len(eurusd) == 1

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


async def test_a_raising_listener_is_logged_once_and_does_not_stop_the_others(
    server: FakeCTraderServer,
    client: CTraderAccountClient,
    logger: RecordingLogger,
) -> None:
    def broken(_event: oa.ProtoOASpotEvent) -> None:
        raise ValueError("boom")

    delivered = []
    client.subscriptions.add_spot_listener(EURUSD, broken)
    client.subscriptions.add_spot_listener(EURUSD, delivered.append)

    await server.push(_spot(EURUSD))
    await server.push(_spot(EURUSD))
    await wait_until(lambda: len(delivered) == 2, description="both spots delivered")

    listener_lines = [level for level, m in logger.lines if "Spot listener" in m]
    assert listener_lines == ["error", "debug"]


# -- Owners, and consumers being subscribed or released ---------------------------------------


def _spot_reply(request: Message) -> Message:
    return oa.ProtoOASubscribeSpotsRes(ctidTraderAccountId=request.ctidTraderAccountId)


def _spot_unsubscribe_reply(request: Message) -> Message:
    return oa.ProtoOAUnsubscribeSpotsRes(ctidTraderAccountId=request.ctidTraderAccountId)


async def test_the_same_name_under_two_owners_is_two_consumers(
    server: FakeCTraderServer,
    client: CTraderAccountClient,
) -> None:
    registry = client.subscriptions
    await registry.subscribe_spots(EURUSD, "quotes", "data")
    await registry.subscribe_spots(EURUSD, "quotes", "execution")

    await registry.unsubscribe_spots(EURUSD, "quotes", "data")

    assert received(server, oa.ProtoOAUnsubscribeSpotsReq) == []
    assert registry.spot_holds("data") == frozenset()
    assert registry.spot_holds("execution") == frozenset({(EURUSD, "quotes")})
    assert registry.active_consumers(EURUSD, "execution") == frozenset({"quotes"})


async def test_a_consumer_is_active_and_held_while_its_subscribe_is_in_flight(
    server: FakeCTraderServer,
    client: CTraderAccountClient,
) -> None:
    registry = client.subscriptions
    held = HeldReplies(server, oa_model.PROTO_OA_SUBSCRIBE_SPOTS_REQ, _spot_reply)

    subscribing = asyncio.create_task(registry.subscribe_spots(EURUSD, "a", "data"))
    await asyncio.wait_for(held.arrived.wait(), 2.0)

    # Not counted yet, but already on record: a spot may arrive before the answer does.
    assert registry.consumers(EURUSD) == frozenset()
    assert registry.active_consumers(EURUSD, "data") == frozenset({"a"})
    assert registry.spot_holds("data") == frozenset({(EURUSD, "a")})

    await held.stop_holding()
    await subscribing
    assert registry.active_consumers(EURUSD, "data") == frozenset({"a"})


async def test_has_active_consumer_answers_for_its_owner_only(
    server: FakeCTraderServer,
    client: CTraderAccountClient,
) -> None:
    registry = client.subscriptions
    await registry.subscribe_spots(EURUSD, "a", "data")

    assert registry.has_active_consumer(EURUSD, "data")
    assert not registry.has_active_consumer(EURUSD, "execution")

    await registry.unsubscribe_spots(EURUSD, "a", "data")
    assert not registry.has_active_consumer(EURUSD, "data")


async def test_a_refused_subscribe_leaves_nothing_active_or_held(
    server: FakeCTraderServer,
    client: CTraderAccountClient,
) -> None:
    server.refuse[oa.ProtoOASubscribeSpotsReq] = "SYMBOL_NOT_FOUND"

    with pytest.raises(CTraderRequestError):
        await client.subscriptions.subscribe_spots(EURUSD, "a", "data")

    assert client.subscriptions.active_consumers(EURUSD, "data") == frozenset()
    assert client.subscriptions.spot_holds("data") == frozenset()


async def test_a_subscribe_the_registry_did_not_record_leaves_nothing_active_or_held(
    monkeypatch: pytest.MonkeyPatch,
    server: FakeCTraderServer,
    client: CTraderAccountClient,
) -> None:
    async def unreadable(*_args: object, **_kwargs: object) -> Message:
        raise CTraderProtocolError("unreadable response")

    # The session stays up, so this is not taken for a loss.
    monkeypatch.setattr(client.session, "request", unreadable)

    with pytest.raises(CTraderProtocolError):
        await client.subscriptions.subscribe_spots(EURUSD, "a", "data")

    assert client.subscriptions.consumers(EURUSD) == frozenset()
    assert client.subscriptions.active_consumers(EURUSD, "data") == frozenset()
    assert client.subscriptions.spot_holds("data") == frozenset()


async def test_a_consumer_stops_being_active_as_its_release_starts(
    server: FakeCTraderServer,
    client: CTraderAccountClient,
) -> None:
    registry = client.subscriptions
    await registry.subscribe_spots(EURUSD, "a", "data")
    held = HeldReplies(server, oa_model.PROTO_OA_UNSUBSCRIBE_SPOTS_REQ, _spot_unsubscribe_reply)

    releasing = asyncio.create_task(registry.unsubscribe_spots(EURUSD, "a", "data"))
    await asyncio.wait_for(held.arrived.wait(), 2.0)

    assert registry.active_consumers(EURUSD, "data") == frozenset()
    assert registry.spot_holds("data") == frozenset({(EURUSD, "a")})

    await held.stop_holding()
    await releasing
    assert registry.spot_holds("data") == frozenset()


async def test_a_cancelled_release_stays_held_but_inactive_until_repeated(
    server: FakeCTraderServer,
    client: CTraderAccountClient,
) -> None:
    registry = client.subscriptions
    await registry.subscribe_spots(EURUSD, "a", "data")
    held = HeldReplies(server, oa_model.PROTO_OA_UNSUBSCRIBE_SPOTS_REQ, _spot_unsubscribe_reply)

    releasing = asyncio.create_task(registry.unsubscribe_spots(EURUSD, "a", "data"))
    await asyncio.wait_for(held.arrived.wait(), 2.0)
    releasing.cancel()
    with pytest.raises(asyncio.CancelledError):
        await releasing
    await held.stop_holding()

    assert registry.active_consumers(EURUSD, "data") == frozenset()
    assert registry.spot_holds("data") == frozenset({(EURUSD, "a")})

    await registry.unsubscribe_spots(EURUSD, "a", "data")

    assert registry.spot_holds("data") == frozenset()
    assert len(received(server, oa.ProtoOAUnsubscribeSpotsReq)) == 2


async def test_a_release_cancelled_before_it_runs_stays_inactive(
    server: FakeCTraderServer,
    client: CTraderAccountClient,
) -> None:
    registry = client.subscriptions
    held = HeldReplies(server, oa_model.PROTO_OA_SUBSCRIBE_SPOTS_REQ, _spot_reply)
    subscribing = asyncio.create_task(registry.subscribe_spots(EURUSD, "a", "data"))
    await asyncio.wait_for(held.arrived.wait(), 2.0)

    # Queued behind the subscribe on the key lock, and cancelled there.
    releasing = asyncio.create_task(registry.unsubscribe_spots(EURUSD, "a", "data"))
    await asyncio.sleep(0)
    releasing.cancel()
    with pytest.raises(asyncio.CancelledError):
        await releasing
    await held.stop_holding()
    await subscribing

    # Subscribed after all, and still to be released.
    assert registry.consumers(EURUSD) == frozenset({"a"})
    assert registry.active_consumers(EURUSD, "data") == frozenset()
    assert registry.spot_holds("data") == frozenset({(EURUSD, "a")})


async def test_a_subscribe_after_a_cancelled_release_makes_it_active_again(
    server: FakeCTraderServer,
    client: CTraderAccountClient,
) -> None:
    registry = client.subscriptions
    await registry.subscribe_spots(EURUSD, "a", "data")
    held = HeldReplies(server, oa_model.PROTO_OA_UNSUBSCRIBE_SPOTS_REQ, _spot_unsubscribe_reply)
    releasing = asyncio.create_task(registry.unsubscribe_spots(EURUSD, "a", "data"))
    await asyncio.wait_for(held.arrived.wait(), 2.0)
    releasing.cancel()
    with pytest.raises(asyncio.CancelledError):
        await releasing
    await held.stop_holding()

    await registry.subscribe_spots(EURUSD, "a", "data")

    assert registry.active_consumers(EURUSD, "data") == frozenset({"a"})


async def _cancel_spot_release_the_venue_processes(
    server: FakeCTraderServer,
    registry: SubscriptionRegistry,
) -> None:
    """Cancel the release of `a`'s spots once its unsubscribe is sent; the venue then honours it."""
    held = HeldReplies(server, oa_model.PROTO_OA_UNSUBSCRIBE_SPOTS_REQ, _spot_unsubscribe_reply)
    releasing = asyncio.create_task(registry.unsubscribe_spots(EURUSD, "a", "data"))
    await asyncio.wait_for(held.arrived.wait(), 2.0)
    releasing.cancel()
    with pytest.raises(asyncio.CancelledError):
        await releasing
    await held.stop_holding()


@pytest.mark.parametrize("owner", ["data", "execution"], ids=["same owner", "another owner"])
async def test_a_subscribe_after_a_cancelled_release_is_sent_to_the_venue(
    server: FakeCTraderServer,
    client: CTraderAccountClient,
    owner: str,
) -> None:
    registry = client.subscriptions
    await registry.subscribe_spots(EURUSD, "a", "data")
    await _cancel_spot_release_the_venue_processes(server, registry)

    await registry.subscribe_spots(EURUSD, "a", owner)

    assert len(received(server, oa.ProtoOASubscribeSpotsReq)) == 2
    assert registry.active_consumers(EURUSD, owner) == frozenset({"a"})

    # That subscribe settled the key, so the next consumer joins without a request.
    await registry.subscribe_spots(EURUSD, "b", "data")
    assert len(received(server, oa.ProtoOASubscribeSpotsReq)) == 2


async def test_a_subscribe_re_sent_after_a_cancelled_release_accepts_already_subscribed(
    server: FakeCTraderServer,
    client: CTraderAccountClient,
) -> None:
    registry = client.subscriptions
    await registry.subscribe_spots(EURUSD, "a", "data")
    await _cancel_spot_release_the_venue_processes(server, registry)
    server.refuse[oa.ProtoOASubscribeSpotsReq] = "ALREADY_SUBSCRIBED"

    await registry.subscribe_spots(EURUSD, "b", "data")

    assert len(received(server, oa.ProtoOASubscribeSpotsReq)) == 2
    assert registry.consumers(EURUSD) == frozenset({"a", "b"})
    await registry.subscribe_spots(EURUSD, "c", "data")
    assert len(received(server, oa.ProtoOASubscribeSpotsReq)) == 2


async def test_a_trendbar_subscribe_after_a_cancelled_trendbar_release_is_sent_to_the_venue(
    server: FakeCTraderServer,
    client: CTraderAccountClient,
) -> None:
    registry = client.subscriptions
    await registry.subscribe_trendbars(EURUSD, M15, "bars", "data")
    held = HeldReplies(
        server,
        oa_model.PROTO_OA_UNSUBSCRIBE_LIVE_TRENDBAR_REQ,
        lambda r: oa.ProtoOAUnsubscribeLiveTrendbarRes(ctidTraderAccountId=r.ctidTraderAccountId),
    )
    releasing = asyncio.create_task(registry.unsubscribe_trendbars(EURUSD, M15, "bars", "data"))
    await asyncio.wait_for(held.arrived.wait(), 2.0)
    releasing.cancel()
    with pytest.raises(asyncio.CancelledError):
        await releasing
    await held.stop_holding()
    server.received.clear()

    await registry.subscribe_trendbars(EURUSD, M15, "bars", "data")

    assert ("ProtoOASubscribeLiveTrendbarReq", (EURUSD,), M15) in _subscriptions(server)
    assert registry.trendbar_consumers(EURUSD, M15, "data") == frozenset({"bars"})
    assert registry.consumers(EURUSD) == frozenset({f"trendbar:{M15}:bars"})


async def test_a_completed_release_settles_a_cancelled_one(
    server: FakeCTraderServer,
    client: CTraderAccountClient,
) -> None:
    registry = client.subscriptions
    await registry.subscribe_spots(EURUSD, "a", "data")
    await _cancel_spot_release_the_venue_processes(server, registry)
    await registry.unsubscribe_spots(EURUSD, "a", "data")

    await registry.subscribe_spots(EURUSD, "a", "data")
    await registry.subscribe_spots(EURUSD, "b", "data")

    assert len(received(server, oa.ProtoOASubscribeSpotsReq)) == 2


async def test_a_reconnect_settles_a_cancelled_release(
    server: FakeCTraderServer,
    client: CTraderAccountClient,
) -> None:
    registry = client.subscriptions
    await registry.subscribe_spots(EURUSD, "a", "data")
    await _cancel_spot_release_the_venue_processes(server, registry)

    await client.disconnect()
    await client.connect()
    assert len(received(server, oa.ProtoOASubscribeSpotsReq)) == 2

    # The restore re-subscribed it on the new connection, so this joins without a request.
    await registry.subscribe_spots(EURUSD, "b", "data")
    assert len(received(server, oa.ProtoOASubscribeSpotsReq)) == 2


async def test_a_release_started_during_the_subscribe_ends_with_nothing_held(
    server: FakeCTraderServer,
    client: CTraderAccountClient,
) -> None:
    registry = client.subscriptions
    held = HeldReplies(server, oa_model.PROTO_OA_SUBSCRIBE_SPOTS_REQ, _spot_reply)
    subscribing = asyncio.create_task(registry.subscribe_spots(EURUSD, "a", "data"))
    await asyncio.wait_for(held.arrived.wait(), 2.0)

    releasing = asyncio.create_task(registry.unsubscribe_spots(EURUSD, "a", "data"))
    await asyncio.sleep(0)
    assert registry.active_consumers(EURUSD, "data") == frozenset()

    await held.stop_holding()
    await asyncio.gather(subscribing, releasing)

    assert registry.consumers(EURUSD) == frozenset()
    assert registry.spot_holds("data") == frozenset()


async def test_trendbar_holds_list_the_trendbar_consumer_but_not_its_spot_leg(
    server: FakeCTraderServer,
    client: CTraderAccountClient,
) -> None:
    registry = client.subscriptions

    await registry.subscribe_trendbars(EURUSD, M15, "bars", "data")

    assert registry.trendbar_holds("data") == frozenset({(EURUSD, M15, "bars")})
    assert registry.trendbar_holds("execution") == frozenset()
    assert registry.spot_holds("data") == frozenset()
    assert registry.active_consumers(EURUSD, "data") == frozenset()


async def test_a_cancelled_trendbar_release_stays_held_until_repeated(
    server: FakeCTraderServer,
    client: CTraderAccountClient,
) -> None:
    registry = client.subscriptions
    await registry.subscribe_trendbars(EURUSD, M15, "bars", "data")
    held = HeldReplies(
        server,
        oa_model.PROTO_OA_UNSUBSCRIBE_LIVE_TRENDBAR_REQ,
        lambda r: oa.ProtoOAUnsubscribeLiveTrendbarRes(ctidTraderAccountId=r.ctidTraderAccountId),
    )

    releasing = asyncio.create_task(registry.unsubscribe_trendbars(EURUSD, M15, "bars", "data"))
    await asyncio.wait_for(held.arrived.wait(), 2.0)
    releasing.cancel()
    with pytest.raises(asyncio.CancelledError):
        await releasing
    await held.stop_holding()

    assert registry.trendbar_holds("data") == frozenset({(EURUSD, M15, "bars")})

    await registry.unsubscribe_trendbars(EURUSD, M15, "bars", "data")

    assert registry.trendbar_holds("data") == frozenset()
    assert len(received(server, oa.ProtoOAUnsubscribeLiveTrendbarReq)) == 2


async def test_a_trendbar_release_cancelled_in_its_spots_leg_stays_held_until_repeated(
    server: FakeCTraderServer,
    client: CTraderAccountClient,
) -> None:
    registry = client.subscriptions
    await registry.subscribe_trendbars(EURUSD, M15, "bars", "data")
    held = HeldReplies(server, oa_model.PROTO_OA_UNSUBSCRIBE_SPOTS_REQ, _spot_unsubscribe_reply)

    releasing = asyncio.create_task(registry.unsubscribe_trendbars(EURUSD, M15, "bars", "data"))
    await asyncio.wait_for(held.arrived.wait(), 2.0)
    releasing.cancel()
    with pytest.raises(asyncio.CancelledError):
        await releasing
    await held.stop_holding()

    # The trendbar leg is gone, the spot leg is not: the owner still has it to release.
    assert registry.trendbar_holds("data") == frozenset({(EURUSD, M15, "bars")})
    assert registry.trendbar_consumers(EURUSD, M15, "data") == frozenset()
    assert registry.consumers(EURUSD) == frozenset({f"trendbar:{M15}:bars"})

    await registry.unsubscribe_trendbars(EURUSD, M15, "bars", "data")

    assert registry.trendbar_holds("data") == frozenset()
    assert registry.consumers(EURUSD) == frozenset()
    assert len(received(server, oa.ProtoOAUnsubscribeLiveTrendbarReq)) == 1
    assert len(received(server, oa.ProtoOAUnsubscribeSpotsReq)) == 2


async def test_a_trendbar_subscribe_cancelled_in_its_spots_leg_stays_held(
    server: FakeCTraderServer,
    client: CTraderAccountClient,
) -> None:
    registry = client.subscriptions
    held = HeldReplies(server, oa_model.PROTO_OA_SUBSCRIBE_SPOTS_REQ, _spot_reply)

    subscribing = asyncio.create_task(registry.subscribe_trendbars(EURUSD, M15, "bars", "data"))
    await asyncio.wait_for(held.arrived.wait(), 2.0)
    subscribing.cancel()
    with pytest.raises(asyncio.CancelledError):
        await subscribing
    await held.stop_holding()

    assert registry.trendbar_holds("data") == frozenset({(EURUSD, M15, "bars")})
    assert registry.trendbar_consumers(EURUSD, M15, "data") == frozenset()


async def test_a_refused_trendbar_whose_spot_rollback_is_cancelled_stays_held(
    server: FakeCTraderServer,
    client: CTraderAccountClient,
) -> None:
    registry = client.subscriptions
    server.refuse[oa.ProtoOASubscribeLiveTrendbarReq] = "TRADING_DISABLED"
    held = HeldReplies(server, oa_model.PROTO_OA_UNSUBSCRIBE_SPOTS_REQ, _spot_unsubscribe_reply)

    subscribing = asyncio.create_task(registry.subscribe_trendbars(EURUSD, M15, "bars", "data"))
    await asyncio.wait_for(held.arrived.wait(), 2.0)
    subscribing.cancel()
    with pytest.raises(asyncio.CancelledError):
        await subscribing
    await held.stop_holding()

    assert registry.trendbar_holds("data") == frozenset({(EURUSD, M15, "bars")})
    assert registry.trendbar_consumers(EURUSD, M15, "data") == frozenset()
    assert registry.consumers(EURUSD) == frozenset({f"trendbar:{M15}:bars"})

    await registry.unsubscribe_trendbars(EURUSD, M15, "bars", "data")

    assert registry.trendbar_holds("data") == frozenset()
    assert registry.consumers(EURUSD) == frozenset()
