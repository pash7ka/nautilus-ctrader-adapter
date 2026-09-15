"""An asyncio server speaking the real cTrader framing, for offline tests.

Scenarios are scripted as handlers keyed by inbound payload type, so a test states what the
venue does rather than how bytes are laid out. The hostile behaviours - truncating a frame,
staying silent, closing mid-stream, replying slowly - are first-class options, because those
are exactly the paths that otherwise run for the first time in production.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Callable

from google.protobuf.message import Message

from nautilus_ctrader.common import codec
from nautilus_ctrader.constants import LENGTH_PREFIX_BYTES
from nautilus_ctrader.messages import OpenApiCommonMessages_pb2 as common
from nautilus_ctrader.messages import OpenApiCommonModelMessages_pb2 as common_model

Handler = Callable[[Message], Message | list[Message] | None]


class FakeCTraderServer:
    def __init__(self, host: str = "127.0.0.1") -> None:
        self.host = host
        self.port = 0
        self.received: list[Message] = []
        self.received_client_msg_ids: list[str | None] = []
        self.heartbeats_received = 0

        # Scenario switches.
        self.reply_delay_secs = 0.0
        self.truncate_next_frame = False
        self.close_after_next_request = False
        self.answer_heartbeats = True

        self._handlers: dict[int, Handler] = {}
        self._server: asyncio.Server | None = None
        self._writers: list[asyncio.StreamWriter] = []
        self._connection_count = 0
        self._connection_event = asyncio.Event()

    @property
    def connection_count(self) -> int:
        return self._connection_count

    def on(self, payload_type: int, handler: Handler) -> None:
        """Register what the venue does when it receives `payload_type`."""
        self._handlers[payload_type] = handler

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._serve, self.host, 0)
        self.port = self._server.sockets[0].getsockname()[1]

    async def stop(self) -> None:
        await self.drop_connections()
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

    async def drop_connections(self) -> None:
        """Close every open connection without warning, to force a client reconnect."""
        writers, self._writers = self._writers, []
        for writer in writers:
            writer.close()
        for writer in writers:
            with contextlib.suppress(ConnectionError, OSError):
                await writer.wait_closed()

    async def wait_for_connections(self, count: int = 1, timeout_secs: float = 2.0) -> None:
        """Wait until `count` clients are connected, failing after `timeout_secs`.

        A client's `open_connection` returns before the server has registered it, so a push
        issued straight after connecting can reach nobody. Tests wait on this instead. The
        bound matters: without it, a client that never connects would hang the suite.
        """
        async with asyncio.timeout(timeout_secs):
            while len(self._writers) < count:
                self._connection_event.clear()
                await self._connection_event.wait()

    async def push(self, message: Message, client_msg_id: str | None = None) -> None:
        """Send an unsolicited event to every connected client."""
        if not self._writers:
            raise RuntimeError("push with no connected client; await wait_for_connections()")
        for writer in list(self._writers):
            await self._write(writer, message, client_msg_id)

    async def push_raw(self, data: bytes) -> None:
        """Send raw bytes to every connected client, bypassing the framing."""
        if not self._writers:
            raise RuntimeError("push with no connected client; await wait_for_connections()")
        for writer in list(self._writers):
            writer.write(data)
            with contextlib.suppress(ConnectionError, OSError):
                await writer.drain()

    async def _serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self._connection_count += 1
        self._writers.append(writer)
        self._connection_event.set()
        try:
            while True:
                prefix = await reader.readexactly(LENGTH_PREFIX_BYTES)
                body = await reader.readexactly(codec.decode_length(prefix))
                envelope = codec.decode_envelope(body)
                await self._handle(writer, envelope)
        except (asyncio.IncompleteReadError, ConnectionError, asyncio.CancelledError):
            pass
        finally:
            if writer in self._writers:
                self._writers.remove(writer)
            writer.close()

    async def _handle(self, writer: asyncio.StreamWriter, envelope: common.ProtoMessage) -> None:
        if envelope.payloadType == common_model.HEARTBEAT_EVENT:
            self.heartbeats_received += 1
            if self.answer_heartbeats:
                await self._write(writer, common.ProtoHeartbeatEvent())
            return

        payload = codec.parse_payload(envelope)
        self.received.append(payload)
        self.received_client_msg_ids.append(envelope.clientMsgId or None)

        if self.close_after_next_request:
            self.close_after_next_request = False
            writer.close()
            return

        handler = self._handlers.get(envelope.payloadType)
        if handler is None:
            return

        if self.reply_delay_secs:
            await asyncio.sleep(self.reply_delay_secs)

        replies = handler(payload)
        if replies is None:
            return
        for reply in replies if isinstance(replies, list) else [replies]:
            await self._write(writer, reply, envelope.clientMsgId or None)

    async def _write(
        self,
        writer: asyncio.StreamWriter,
        message: Message,
        client_msg_id: str | None = None,
    ) -> None:
        frame = codec.encode_frame(message, client_msg_id)
        if self.truncate_next_frame:
            self.truncate_next_frame = False
            # Keep the declared length but deliver fewer bytes than promised.
            frame = frame[: LENGTH_PREFIX_BYTES + max(1, (len(frame) - LENGTH_PREFIX_BYTES) // 2)]
        writer.write(frame)
        with contextlib.suppress(ConnectionError, OSError):
            await writer.drain()
