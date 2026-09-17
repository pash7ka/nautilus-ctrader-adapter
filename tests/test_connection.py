"""The connection layer: framing over a real socket, correlation, heartbeat, failure."""

import asyncio
import contextlib
import struct

import pytest
from nautilus_trader.common.component import Logger

from nautilus_ctrader.common import codec
from nautilus_ctrader.common.connection import CTraderConnection
from nautilus_ctrader.common.errors import (
    CTraderConnectionError,
    CTraderProtocolError,
    CTraderRequestError,
    CTraderTimeoutError,
)
from nautilus_ctrader.common.rate_limit import RateLimiter
from nautilus_ctrader.constants import LENGTH_PREFIX_FORMAT, MAX_FRAME_BYTES
from nautilus_ctrader.messages import OpenApiCommonMessages_pb2 as common
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as oa_model
from tests.fake_server import FakeCTraderServer
from tests.polling import wait_until
from tests.recording_logger import RecordingLogger


async def _connected(server: FakeCTraderServer, **kwargs) -> CTraderConnection:
    connection = CTraderConnection(
        host=server.host,
        port=server.port,
        logger=Logger("test"),
        tls=False,
        **kwargs,
    )
    await connection.connect()
    return connection


async def test_request_returns_the_correlated_response() -> None:
    server = FakeCTraderServer()
    server.on(
        oa_model.PROTO_OA_APPLICATION_AUTH_REQ,
        lambda _r: oa.ProtoOAApplicationAuthRes(),
    )
    await server.start()
    connection = await _connected(server)
    try:
        response = await connection.request(
            oa.ProtoOAApplicationAuthReq(clientId="i", clientSecret="s"),
        )
        assert isinstance(response, oa.ProtoOAApplicationAuthRes)
    finally:
        await connection.close()
        await server.stop()


async def test_concurrent_requests_do_not_cross_their_responses() -> None:
    server = FakeCTraderServer()
    server.on(
        oa_model.PROTO_OA_SUBSCRIBE_SPOTS_REQ,
        lambda request: oa.ProtoOASubscribeSpotsRes(
            ctidTraderAccountId=request.ctidTraderAccountId,
        ),
    )
    await server.start()
    # Replies are delayed so all three requests are in flight at once; if correlation were
    # broken, the responses would come back attached to the wrong callers.
    server.reply_delay_secs = 0.05
    connection = await _connected(server)
    try:
        responses = await asyncio.gather(
            *(
                connection.request(oa.ProtoOASubscribeSpotsReq(ctidTraderAccountId=n))
                for n in (11, 22, 33)
            ),
        )
        assert [r.ctidTraderAccountId for r in responses] == [11, 22, 33]
    finally:
        await connection.close()
        await server.stop()


async def test_an_error_response_rejects_its_caller() -> None:
    server = FakeCTraderServer()
    server.on(
        oa_model.PROTO_OA_TRADER_REQ,
        lambda _r: oa.ProtoOAErrorRes(errorCode="ENTITY_NOT_FOUND", description="nope"),
    )
    await server.start()
    connection = await _connected(server)
    try:
        with pytest.raises(CTraderRequestError) as caught:
            await connection.request(oa.ProtoOATraderReq(ctidTraderAccountId=1))
        assert caught.value.error_code == "ENTITY_NOT_FOUND"
    finally:
        await connection.close()
        await server.stop()


async def test_a_rate_limit_rejection_pauses_that_bucket() -> None:
    server = FakeCTraderServer()
    server.on(
        oa_model.PROTO_OA_TRADER_REQ,
        lambda _r: oa.ProtoOAErrorRes(errorCode="BLOCKED_PAYLOAD_TYPE", retryAfter=1),
    )
    await server.start()
    limiter = RateLimiter({"default": 1000.0, "historical": 1000.0})
    connection = await _connected(server, rate_limiter=limiter)
    try:
        with pytest.raises(CTraderRequestError):
            await connection.request(oa.ProtoOATraderReq(ctidTraderAccountId=1))

        # The bucket is now paused, so the next acquire must not return immediately.
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(limiter.acquire("default"), timeout=0.2)
    finally:
        await connection.close()
        await server.stop()


async def test_a_silent_server_times_out_the_request() -> None:
    server = FakeCTraderServer()
    await server.start()
    connection = await _connected(server, request_timeout_secs=0.2)
    try:
        with pytest.raises(CTraderTimeoutError):
            await connection.request(oa.ProtoOATraderReq(ctidTraderAccountId=1))
    finally:
        await connection.close()
        await server.stop()


async def test_a_heartbeat_is_sent_when_idle_and_never_loops() -> None:
    # The fake server echoes heartbeats by default. Answering those echoes once made the two
    # sides ping-pong thousands of times a second; the upper bound pins that down.
    server = FakeCTraderServer()
    await server.start()
    connection = await _connected(server, heartbeat_idle_secs=0.1)
    try:
        await asyncio.sleep(0.5)
        assert 2 <= server.heartbeats_received <= 10
    finally:
        await connection.close()
        await server.stop()


async def test_a_silent_venue_is_treated_as_a_lost_connection() -> None:
    # Our own heartbeats go out on the 50ms idle timer, but the server never answers and never
    # sends anything of its own: a stand-in for a half-open TCP connection, where writes keep
    # succeeding while nothing ever arrives.
    server = FakeCTraderServer()
    server.answer_heartbeats = False
    await server.start()
    connection = await _connected(server, heartbeat_idle_secs=0.05, inbound_silence_secs=0.3)
    losses: list[Exception] = []
    connection.set_disconnect_handler(losses.append)
    try:
        await wait_until(lambda: len(losses) >= 1, timeout_secs=2.0, description="loss reported")
        # Give any duplicate report a chance to show up before asserting there is only one.
        await asyncio.sleep(0.1)
        assert len(losses) == 1
        assert isinstance(losses[0], CTraderConnectionError)
        assert connection.is_connected is False
    finally:
        await connection.close()
        await server.stop()


async def test_a_frame_arriving_after_a_silence_loss_is_not_delivered() -> None:
    # The read loop outlives the loss until the owner closes the connection.
    server = FakeCTraderServer()
    server.answer_heartbeats = False
    await server.start()
    logger = RecordingLogger()
    connection = CTraderConnection(
        host=server.host,
        port=server.port,
        logger=logger,
        tls=False,
        heartbeat_idle_secs=0.05,
        inbound_silence_secs=0.3,
    )
    await connection.connect()
    seen: list[object] = []
    losses: list[Exception] = []
    connection.set_event_handler(seen.append)
    connection.set_disconnect_handler(losses.append)
    try:
        await server.wait_for_connections()
        await wait_until(lambda: len(losses) >= 1, timeout_secs=2.0, description="loss reported")

        await server.push(oa.ProtoOAAccountsTokenInvalidatedEvent(reason="late"))
        # The receive line is logged in the same synchronous step as any delivery.
        await wait_until(
            lambda: any(m.startswith("recv payloadType=") for _level, m in logger.lines),
            description="late frame read",
        )
        assert seen == []
    finally:
        await connection.close()
        await server.stop()


async def test_inbound_traffic_keeps_the_connection_alive() -> None:
    server = FakeCTraderServer()
    await server.start()
    connection = await _connected(server, heartbeat_idle_secs=0.05, inbound_silence_secs=0.3)
    losses: list[Exception] = []
    connection.set_disconnect_handler(losses.append)
    try:
        # A fixed wait is correct here: this checks that a loss never happens.
        await asyncio.sleep(1.0)
        assert losses == []
        assert connection.is_connected is True
        assert server.heartbeats_received >= 1
    finally:
        await connection.close()
        await server.stop()


async def test_a_long_heartbeat_interval_does_not_delay_the_silence_check() -> None:
    server = FakeCTraderServer()
    server.answer_heartbeats = False
    await server.start()
    connection = await _connected(server, heartbeat_idle_secs=3600.0, inbound_silence_secs=0.3)
    losses: list[Exception] = []
    connection.set_disconnect_handler(losses.append)
    try:
        await wait_until(lambda: len(losses) >= 1, timeout_secs=2.0, description="loss reported")
        assert isinstance(losses[0], CTraderConnectionError)
    finally:
        await connection.close()
        await server.stop()


async def test_an_inbound_heartbeat_is_neither_answered_nor_surfaced() -> None:
    server = FakeCTraderServer()
    await server.start()
    # Long enough that no heartbeat of our own goes out during the test.
    connection = await _connected(server, heartbeat_idle_secs=60.0)
    seen: list[object] = []
    connection.set_event_handler(seen.append)
    try:
        await server.wait_for_connections()
        await server.push(common.ProtoHeartbeatEvent())
        await asyncio.sleep(0.2)
        assert server.heartbeats_received == 0
        assert seen == []
    finally:
        await connection.close()
        await server.stop()


async def test_an_unsolicited_message_reaches_the_event_handler() -> None:
    server = FakeCTraderServer()
    await server.start()
    connection = await _connected(server)
    seen: list[object] = []
    connection.set_event_handler(seen.append)
    try:
        await server.wait_for_connections()
        await server.push(oa.ProtoOAAccountsTokenInvalidatedEvent(reason="recalled"))
        await asyncio.sleep(0.2)
        assert len(seen) == 1
        assert isinstance(seen[0], oa.ProtoOAAccountsTokenInvalidatedEvent)
    finally:
        await connection.close()
        await server.stop()


async def test_a_lost_connection_rejects_pending_requests_and_notifies() -> None:
    server = FakeCTraderServer()
    await server.start()
    connection = await _connected(server, request_timeout_secs=5.0)
    losses: list[Exception] = []
    connection.set_disconnect_handler(losses.append)
    try:
        pending = asyncio.create_task(
            connection.request(oa.ProtoOATraderReq(ctidTraderAccountId=1)),
        )
        # Drop only once the request is in flight, so this proves a pending request is
        # rejected - not one refused before it was ever sent.
        for _ in range(200):
            if server.received:
                break
            await asyncio.sleep(0.01)
        assert server.received, "request never reached the server"
        await server.drop_connections()

        with pytest.raises(CTraderConnectionError):
            await asyncio.wait_for(pending, timeout=2.0)
        assert len(losses) == 1
        assert connection.is_connected is False
    finally:
        await connection.close()
        await server.stop()


async def test_a_cancelled_close_still_releases_everything() -> None:
    server = FakeCTraderServer()
    await server.start()
    connection = await _connected(server, request_timeout_secs=30.0)
    try:
        await server.wait_for_connections()
        # No handler: the request stays pending.
        pending = asyncio.create_task(
            connection.request(oa.ProtoOATraderReq(ctidTraderAccountId=1)),
        )
        await wait_until(lambda: bool(server.received), description="request in flight")

        close_task = asyncio.create_task(connection.close())
        # One yield puts `close()` inside its wait for the cancelled tasks.
        await asyncio.sleep(0)
        close_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await close_task

        with pytest.raises(CTraderConnectionError):
            await asyncio.wait_for(pending, timeout=2.0)
        assert connection.is_connected is False
        await wait_until(
            lambda: server.open_connection_count == 0,
            description="server sees the connection closed",
        )
    finally:
        await connection.close()
        await server.stop()


async def test_a_request_waiting_across_a_reconnect_is_never_sent() -> None:
    # A request that failed must not reach the next connection: for an order that would be
    # a duplicate the caller never knows about.
    server = FakeCTraderServer()
    await server.start()
    limiter = RateLimiter({"default": 1000.0, "historical": 1000.0})
    connection = await _connected(server, rate_limiter=limiter)
    try:
        await server.wait_for_connections()
        limiter.pause("default", 0.3)
        pending = asyncio.create_task(
            connection.request(oa.ProtoOATraderReq(ctidTraderAccountId=1)),
        )
        await asyncio.sleep(0.05)
        assert not pending.done(), "request should be waiting on the paused bucket"

        await connection.close()
        await connection.connect()
        await server.wait_for_connections()

        with pytest.raises(CTraderConnectionError):
            await asyncio.wait_for(pending, timeout=2.0)
        # A fixed wait is right here: this checks that the request never arrives.
        await asyncio.sleep(0.5)
        assert not any(isinstance(m, oa.ProtoOATraderReq) for m in server.received)
    finally:
        await connection.close()
        await server.stop()


async def test_credentials_never_reach_the_log() -> None:
    # DEBUG carries payload type, correlation id and byte length, never payload bytes.
    # Authentication payloads are where the secrets are.
    server = FakeCTraderServer()
    server.on(
        oa_model.PROTO_OA_APPLICATION_AUTH_REQ,
        lambda _r: oa.ProtoOAApplicationAuthRes(),
    )
    await server.start()
    logger = RecordingLogger()
    connection = CTraderConnection(
        host=server.host,
        port=server.port,
        logger=logger,
        tls=False,
    )
    await connection.connect()
    try:
        await connection.request(
            oa.ProtoOAApplicationAuthReq(
                clientId="super-secret-client-id",
                clientSecret="super-secret-client-secret",
            ),
        )
        await asyncio.sleep(0.1)

        # The outbound auth request must have passed through the logger, or the absence of
        # secrets below would prove nothing.
        messages = [message for _level, message in logger.lines]
        sent = [m for m in messages if m.startswith("send payloadType=2100")]
        assert sent, f"auth request was never logged; lines were {logger.lines}"

        logged = "\n".join(messages)
        assert "super-secret-client-id" not in logged
        assert "super-secret-client-secret" not in logged
    finally:
        await connection.close()
        await server.stop()


async def test_a_request_before_connecting_fails_fast() -> None:
    connection = CTraderConnection(
        host="127.0.0.1",
        port=1,
        logger=Logger("test"),
        tls=False,
    )
    with pytest.raises(CTraderConnectionError, match="not connected"):
        await connection.request(oa.ProtoOATraderReq(ctidTraderAccountId=1))


async def _serve_bytes(data: bytes) -> tuple[asyncio.Server, int]:
    """A server that sends `data` once, then holds the connection until the client leaves."""

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        writer.write(data)
        await writer.drain()
        with contextlib.suppress(ConnectionError):
            await reader.read()
        writer.close()
        with contextlib.suppress(ConnectionError, OSError):
            await writer.wait_closed()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    return server, server.sockets[0].getsockname()[1]


async def test_tls_is_attempted_by_default() -> None:
    # The first frame carries the client secret, so an unconfigured connection must not
    # open in plaintext.
    first_bytes: list[bytes] = []

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        with contextlib.suppress(ConnectionError):
            first_bytes.append(await reader.read(64))
        writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    connection = CTraderConnection(host="127.0.0.1", port=port, logger=Logger("test"))
    try:
        with pytest.raises(CTraderConnectionError):
            await asyncio.wait_for(connection.connect(), timeout=5.0)
        assert first_bytes, "server received nothing"
        assert first_bytes[0][:1] == b"\x16", "first byte is not a TLS handshake record"
    finally:
        await connection.close()
        server.close()
        await server.wait_closed()


async def test_a_silent_peer_times_out_the_connect() -> None:
    # A peer that accepts and never answers the TLS handshake stands in for a black-holed host.
    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        with contextlib.suppress(ConnectionError):
            await reader.read()
        writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    connection = CTraderConnection(
        host="127.0.0.1",
        port=port,
        logger=Logger("test"),
        connect_timeout_secs=0.2,
    )
    try:
        with pytest.raises(CTraderConnectionError, match="timed out"):
            await asyncio.wait_for(connection.connect(), timeout=2.0)
    finally:
        await connection.close()
        server.close()
        await server.wait_closed()


async def test_an_oversized_frame_drops_the_connection_as_a_protocol_error() -> None:
    server, port = await _serve_bytes(struct.pack(LENGTH_PREFIX_FORMAT, MAX_FRAME_BYTES + 1))
    connection = CTraderConnection(host="127.0.0.1", port=port, logger=Logger("test"), tls=False)
    losses: list[Exception] = []
    connection.set_disconnect_handler(losses.append)
    await connection.connect()
    try:
        for _ in range(200):
            if losses:
                break
            await asyncio.sleep(0.01)
        assert len(losses) == 1
        assert isinstance(losses[0], CTraderProtocolError)
        assert connection.is_connected is False
    finally:
        await connection.close()
        server.close()
        await server.wait_closed()


async def test_an_unknown_payload_type_is_ignored_and_the_connection_survives() -> None:
    # The schema grows; a message type we do not know must not take the connection down.
    envelope = common.ProtoMessage(payloadType=999_999, payload=b"").SerializeToString()
    unknown = struct.pack(LENGTH_PREFIX_FORMAT, len(envelope)) + envelope
    # A known frame right behind it marks the point by which the unknown one was handled.
    known = codec.encode_frame(oa.ProtoOAAccountsTokenInvalidatedEvent(reason="marker"))
    server, port = await _serve_bytes(unknown + known)
    connection = CTraderConnection(host="127.0.0.1", port=port, logger=Logger("test"), tls=False)
    seen: list[object] = []
    losses: list[Exception] = []
    connection.set_event_handler(seen.append)
    connection.set_disconnect_handler(losses.append)
    await connection.connect()
    try:
        await wait_until(lambda: len(seen) >= 1, description="known frame delivered")
        assert len(seen) == 1
        assert isinstance(seen[0], oa.ProtoOAAccountsTokenInvalidatedEvent)
        assert connection.is_connected is True
        assert losses == []
    finally:
        await connection.close()
        server.close()
        await server.wait_closed()


async def test_a_raising_event_handler_does_not_break_the_connection() -> None:
    server = FakeCTraderServer()
    server.on(
        oa_model.PROTO_OA_SUBSCRIBE_SPOTS_REQ,
        lambda request: oa.ProtoOASubscribeSpotsRes(
            ctidTraderAccountId=request.ctidTraderAccountId,
        ),
    )
    await server.start()
    connection = await _connected(server)

    calls: list[object] = []

    def explode(payload: object) -> None:
        calls.append(payload)
        raise RuntimeError("handler bug")

    connection.set_event_handler(explode)
    try:
        await server.wait_for_connections()
        await server.push(oa.ProtoOAAccountsTokenInvalidatedEvent(reason="recalled"))
        await asyncio.sleep(0.1)

        response = await connection.request(oa.ProtoOASubscribeSpotsReq(ctidTraderAccountId=7))
        assert response.ctidTraderAccountId == 7
        assert connection.is_connected is True
        assert len(calls) == 1
    finally:
        await connection.close()
        await server.stop()


async def test_a_raising_disconnect_handler_still_marks_the_connection_down() -> None:
    server = FakeCTraderServer()
    await server.start()
    connection = await _connected(server)
    calls: list[Exception] = []

    def explode(error: Exception) -> None:
        calls.append(error)
        raise RuntimeError("handler bug")

    connection.set_disconnect_handler(explode)
    try:
        await server.wait_for_connections()
        await server.drop_connections()
        for _ in range(200):
            if calls:
                break
            await asyncio.sleep(0.01)
        assert len(calls) == 1
        assert connection.is_connected is False
    finally:
        await connection.close()
        await server.stop()


async def test_send_delivers_and_passes_the_client_msg_id_through() -> None:
    server = FakeCTraderServer()
    await server.start()
    connection = await _connected(server)
    try:
        await connection.send(
            oa.ProtoOASubscribeSpotsReq(ctidTraderAccountId=5),
            client_msg_id="fire-1",
        )
        for _ in range(200):
            if server.received:
                break
            await asyncio.sleep(0.01)
        assert isinstance(server.received[0], oa.ProtoOASubscribeSpotsReq)
        assert server.received[0].ctidTraderAccountId == 5
        assert server.received_client_msg_ids == ["fire-1"]
    finally:
        await connection.close()
        await server.stop()


async def test_send_without_a_client_msg_id_sends_none() -> None:
    # The counterpart that makes the test above discriminating: an id appears only when given.
    server = FakeCTraderServer()
    await server.start()
    connection = await _connected(server)
    try:
        await connection.send(oa.ProtoOASubscribeSpotsReq(ctidTraderAccountId=5))
        for _ in range(200):
            if server.received:
                break
            await asyncio.sleep(0.01)
        assert server.received_client_msg_ids == [None]
    finally:
        await connection.close()
        await server.stop()


@pytest.mark.parametrize(
    ("heartbeat_idle_secs", "inbound_silence_secs"),
    [(10.0, 0.0), (10.0, -1.0)],
)
def test_an_invalid_silence_threshold_is_rejected(
    heartbeat_idle_secs: float,
    inbound_silence_secs: float,
) -> None:
    with pytest.raises(ValueError, match="inbound_silence_secs"):
        CTraderConnection(
            host="127.0.0.1",
            port=1,
            logger=Logger("test"),
            tls=False,
            heartbeat_idle_secs=heartbeat_idle_secs,
            inbound_silence_secs=inbound_silence_secs,
        )


async def test_send_before_connecting_fails_fast() -> None:
    connection = CTraderConnection(
        host="127.0.0.1",
        port=1,
        logger=Logger("test"),
        tls=False,
    )
    with pytest.raises(CTraderConnectionError, match="not connected"):
        await connection.send(oa.ProtoOATraderReq(ctidTraderAccountId=1))
