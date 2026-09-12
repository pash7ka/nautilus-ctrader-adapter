"""The fake server must speak the real framing, or nothing built on it means anything."""

import asyncio

import pytest

from nautilus_ctrader.common import codec
from nautilus_ctrader.messages import OpenApiCommonMessages_pb2 as common
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as oa_model
from tests.fake_server import FakeCTraderServer


async def _read_frame(reader: asyncio.StreamReader) -> common.ProtoMessage:
    length = codec.decode_length(await reader.readexactly(4))
    return codec.decode_envelope(await reader.readexactly(length))


async def test_it_answers_a_request_on_the_correlated_client_msg_id() -> None:
    server = FakeCTraderServer()
    server.on(
        oa_model.PROTO_OA_APPLICATION_AUTH_REQ,
        lambda _request: oa.ProtoOAApplicationAuthRes(),
    )
    await server.start()
    try:
        reader, writer = await asyncio.open_connection(server.host, server.port)
        writer.write(
            codec.encode_frame(
                oa.ProtoOAApplicationAuthReq(clientId="i", clientSecret="s"),
                "abc",
            ),
        )
        await writer.drain()

        envelope = await asyncio.wait_for(_read_frame(reader), timeout=2.0)
        assert envelope.payloadType == oa_model.PROTO_OA_APPLICATION_AUTH_RES
        assert envelope.clientMsgId == "abc"
        assert len(server.received) == 1

        writer.close()
        await writer.wait_closed()
    finally:
        await server.stop()


async def test_it_counts_heartbeats_and_can_stay_silent() -> None:
    server = FakeCTraderServer()
    server.answer_heartbeats = False
    await server.start()
    try:
        reader, writer = await asyncio.open_connection(server.host, server.port)
        writer.write(codec.encode_frame(common.ProtoHeartbeatEvent()))
        await writer.drain()
        await asyncio.sleep(0.1)

        assert server.heartbeats_received == 1
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(reader.readexactly(1), timeout=0.2)

        writer.close()
        await writer.wait_closed()
    finally:
        await server.stop()


async def test_it_can_push_an_unsolicited_event() -> None:
    server = FakeCTraderServer()
    await server.start()
    try:
        reader, writer = await asyncio.open_connection(server.host, server.port)
        await server.wait_for_connections()
        await server.push(oa.ProtoOAAccountsTokenInvalidatedEvent(reason="recalled"))

        envelope = await asyncio.wait_for(_read_frame(reader), timeout=2.0)
        assert envelope.payloadType == oa_model.PROTO_OA_ACCOUNTS_TOKEN_INVALIDATED_EVENT

        writer.close()
        await writer.wait_closed()
    finally:
        await server.stop()


async def test_it_can_truncate_a_frame() -> None:
    server = FakeCTraderServer()
    server.on(
        oa_model.PROTO_OA_SUBSCRIBE_SPOTS_REQ,
        lambda request: oa.ProtoOASubscribeSpotsRes(
            ctidTraderAccountId=request.ctidTraderAccountId,
        ),
    )
    server.truncate_next_frame = True
    await server.start()
    try:
        reader, writer = await asyncio.open_connection(server.host, server.port)
        writer.write(
            codec.encode_frame(oa.ProtoOASubscribeSpotsReq(ctidTraderAccountId=1), "t"),
        )
        await writer.drain()

        # A truncated frame declares more bytes than it delivers, so reading the declared
        # length must never complete.
        declared = codec.decode_length(await asyncio.wait_for(reader.readexactly(4), 2.0))
        with pytest.raises((TimeoutError, asyncio.IncompleteReadError)):
            await asyncio.wait_for(reader.readexactly(declared), timeout=0.5)

        writer.close()
        await writer.wait_closed()
    finally:
        await server.stop()


async def test_dropping_connections_forces_a_reconnect() -> None:
    server = FakeCTraderServer()
    await server.start()
    try:
        reader, writer = await asyncio.open_connection(server.host, server.port)
        await server.wait_for_connections()
        assert server.connection_count == 1

        await server.drop_connections()
        assert await reader.read(1) == b""

        writer.close()
        await writer.wait_closed()
    finally:
        await server.stop()


async def test_push_with_no_connected_client_is_an_error() -> None:
    # Pushing to nobody must fail here rather than pass silently and break an assertion
    # somewhere else.
    server = FakeCTraderServer()
    await server.start()
    try:
        with pytest.raises(RuntimeError, match="no connected client"):
            await server.push(oa.ProtoOAAccountsTokenInvalidatedEvent(reason="recalled"))
    finally:
        await server.stop()
