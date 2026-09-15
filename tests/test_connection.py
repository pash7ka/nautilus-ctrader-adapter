"""The connection layer: framing over a real socket, correlation, heartbeat, failure."""

import asyncio
import contextlib
import struct

import pytest
from nautilus_trader.common.component import Logger

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


async def _connected(server: FakeCTraderServer, **kwargs) -> CTraderConnection:
    connection = CTraderConnection(
        host=server.host,
        port=server.port,
        logger=Logger("test"),
        ssl_context=None,
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


async def test_an_inbound_heartbeat_is_neither_answered_nor_surfaced() -> None:
    server = FakeCTraderServer()
    await server.start()
    connection = await _connected(server, heartbeat_idle_secs=3600.0)
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


class _RecordingLogger:
    """Stands in for the Nautilus Logger, whose output is written from Rust and is invisible
    to pytest's caplog - asserting against caplog would pass without checking anything."""

    def __init__(self) -> None:
        self.lines: list[str] = []

    def debug(self, message: str) -> None:
        self.lines.append(message)

    info = warning = error = exception = debug


async def test_credentials_never_reach_the_log() -> None:
    # Spec section 9: DEBUG carries payload type, correlation id and byte length, never payload
    # bytes. Authentication payloads are where the secrets are.
    server = FakeCTraderServer()
    server.on(
        oa_model.PROTO_OA_APPLICATION_AUTH_REQ,
        lambda _r: oa.ProtoOAApplicationAuthRes(),
    )
    await server.start()
    logger = _RecordingLogger()
    connection = CTraderConnection(
        host=server.host,
        port=server.port,
        logger=logger,
        ssl_context=None,
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
        sent = [line for line in logger.lines if line.startswith("send payloadType=2100")]
        assert sent, f"auth request was never logged; lines were {logger.lines}"

        logged = "\n".join(logger.lines)
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
        ssl_context=None,
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
        with contextlib.suppress(ConnectionError):
            await writer.wait_closed()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    return server, server.sockets[0].getsockname()[1]


async def test_an_oversized_frame_drops_the_connection_as_a_protocol_error() -> None:
    server, port = await _serve_bytes(struct.pack(LENGTH_PREFIX_FORMAT, MAX_FRAME_BYTES + 1))
    connection = CTraderConnection(
        host="127.0.0.1", port=port, logger=Logger("test"), ssl_context=None
    )
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
    server, port = await _serve_bytes(struct.pack(LENGTH_PREFIX_FORMAT, len(envelope)) + envelope)
    connection = CTraderConnection(
        host="127.0.0.1", port=port, logger=Logger("test"), ssl_context=None
    )
    seen: list[object] = []
    losses: list[Exception] = []
    connection.set_event_handler(seen.append)
    connection.set_disconnect_handler(losses.append)
    await connection.connect()
    try:
        await asyncio.sleep(0.2)
        assert connection.is_connected is True
        assert seen == []
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


async def test_send_before_connecting_fails_fast() -> None:
    connection = CTraderConnection(
        host="127.0.0.1",
        port=1,
        logger=Logger("test"),
        ssl_context=None,
    )
    with pytest.raises(CTraderConnectionError, match="not connected"):
        await connection.send(oa.ProtoOATraderReq(ctidTraderAccountId=1))
